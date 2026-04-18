package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"runtime"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/api/option"
)

// 데이터 구조 정의
type EmbeddingRow struct {
	FileCode      string
	TimelineIndex int
	Embedding[]float64
	Stage         string
}

// Avro Union(Nullable) 필드에서 값을 안전하게 추출하는 헬퍼 함수
func extractAvroString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case map[string]interface{}: // BQ Nullable 필드는 map 형태로 내려옴
		if str, ok := val["string"].(string); ok {
			return str
		}
	}
	return ""
}

func extractAvroInt(v interface{}) int {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return int(val)
	case map[string]interface{}:
		if num, ok := val["long"].(int64); ok { // BQ INT64 -> Avro long
			return int(num)
		}
	}
	return 0
}

func extractAvroFloatArray(v interface{})[]float64 {
	if v == nil {
		return nil
	}
	var arr []interface{}
	switch val := v.(type) {
	case[]interface{}:
		arr = val
	case map[string]interface{}:
		if a, ok := val["array"].([]interface{}); ok {
			arr = a
		}
	}
	result := make([]float64, len(arr))
	for i, item := range arr {
		result[i] = item.(float64)
	}
	return result
}

func main() {
	fmt.Println("[Test] BigQuery Storage Read API 기반 병렬 데이터 로드 시작...")

	ctx := context.Background()
	projectID := "testprojects-453622"
	datasetID := "Psychological_counseling_data"
	keyPath := "/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json"
	authOpt := option.WithServiceAccountFile(keyPath)

	// 1. 표준 BigQuery 클라이언트 초기화 (쿼리 실행 및 임시 테이블 관리용)
	bqClient, err := bigquery.NewClient(ctx, projectID, authOpt)
	if err != nil {
		log.Fatalf("Failed to create BQ client: %v", err)
	}
	defer bqClient.Close()

	// 2. Storage Read API 클라이언트 초기화 (데이터 병렬 다운로드용)
	storageClient, err := storage.NewBigQueryReadClient(ctx, authOpt)
	if err != nil {
		log.Fatalf("Failed to create Storage API client: %v", err)
	}
	defer storageClient.Close()

	// ==========================================
	//[시간 측정 시작]
	startTime := time.Now()
	// ==========================================

	// 3. 쿼리 실행 후 임시 테이블에 결과 저장
	tempTableID := fmt.Sprintf("temp_result_%d", time.Now().UnixNano())
	tempTable := bqClient.Dataset(datasetID).Table(tempTableID)
	
	// 임시 테이블은 1시간 뒤 자동 삭제되도록 설정(안전장치)
	defer tempTable.Delete(ctx)

	queryStr := `WITH Target_Processed_Data AS (SELECT file_code, ANY_VALUE(stage) AS stage FROM testprojects-453622.Psychological_counseling_data.processed_data GROUP BY file_code) SELECT e.file_code, e.timeline_index, e.embedding, p.stage FROM testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding AS e JOIN Target_Processed_Data AS p ON e.file_code = p.file_code ORDER BY e.file_code, e.timeline_index ASC`
	
	fmt.Println("[1/4] 임시 테이블에 쿼리 결과 생성 중...")
	query := bqClient.Query(queryStr)
	query.QueryConfig.Dst = tempTable // 쿼리 결과를 임시 테이블로 지정
	
	job, err := query.Run(ctx)
	if err != nil {
		log.Fatalf("Failed to run query: %v", err)
	}
	// 쿼리 작업 완료 대기
	status, err := job.Wait(ctx)
	if err != nil || status.Err() != nil {
		log.Fatalf("Query job failed: %v", status.Err())
	}

	// 4. Storage API Read Session 생성
	fmt.Println("[2/4] Storage Read API 세션 및 스트림 생성 중...")
	tableRef := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tempTableID)
	
	// 코어 수만큼 최대 스트림 개수 요청
	maxStreams := int32(runtime.NumCPU() * 2) 
	
	sessionReq := &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &storagepb.ReadSession{
			Table:      tableRef,
			DataFormat: storagepb.DataFormat_AVRO, // Go에서는 Avro 파싱이 상대적으로 수월함
		},
		MaxStreamCount: maxStreams,
	}

	session, err := storageClient.CreateReadSession(ctx, sessionReq)
	if err != nil {
		log.Fatalf("Failed to create read session: %v", err)
	}

	fmt.Printf("[3/4] %d개의 스트림으로 데이터 병렬 다운로드 시작...\n", len(session.Streams))

	// Avro 스키마 코덱 생성
	avroSchema := session.GetAvroSchema().GetSchema()
	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		log.Fatalf("Failed to parse avro schema: %v", err)
	}

	// 결과를 모을 채널과 워커(스트림) 동기화 WaitGroup
	resultsChan := make(chan[]EmbeddingRow, len(session.Streams))
	var wg sync.WaitGroup

	// 5. 각 스트림을 병렬 고루틴으로 읽기
	for i, stream := range session.Streams {
		wg.Add(1)
		go func(streamName string, streamIndex int) {
			defer wg.Done()

			rowClient, err := storageClient.ReadRows(ctx, &storagepb.ReadRowsRequest{
				ReadStream: streamName,
			})
			if err != nil {
				log.Printf("Stream %d failed to initiate: %v", streamIndex, err)
				return
			}
var localRows[]EmbeddingRow

			for {
				resp, err := rowClient.Recv()
				if err == io.EOF {
					break // 스트림 끝
				}
				if err != nil {
					log.Printf("Stream %d read error: %v", streamIndex, err)
					break
				}

				// 가져온 직렬화된 Avro 바이너리 데이터 블록 ([]byte)
				binaryBuffer := resp.GetAvroRows().GetSerializedBinaryRows()
				
				// 바이너리 버퍼에 데이터가 남아있는 동안 계속 파싱 (1 Row씩 추출)
				for len(binaryBuffer) > 0 {
					// NativeFromBinary는 파싱된 데이터(native), 남은 데이터(remaining), 에러를 반환합니다.
					native, remaining, err := codec.NativeFromBinary(binaryBuffer)
					if err != nil {
						log.Printf("Avro decoding error in stream %d: %v", streamIndex, err)
						break // 디코딩 에러 발생 시 해당 청크 파싱 중단
					}

					// 버퍼를 남은 데이터로 갱신 (다음 루프에서 사용)
					binaryBuffer = remaining

					// 파싱된 데이터를 map 형태로 변환
					record, ok := native.(map[string]interface{})
					if !ok {
						continue
					}

					localRows = append(localRows, EmbeddingRow{
						FileCode:      extractAvroString(record["file_code"]),
						TimelineIndex: extractAvroInt(record["timeline_index"]),
						Embedding:     extractAvroFloatArray(record["embedding"]),
						Stage:         extractAvroString(record["stage"]),
					})
				}
			}
			resultsChan <- localRows

		}(stream.Name, i)
	}

	// 모든 스트림이 완료되면 채널 닫기
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// 6. 데이터 병합
	fmt.Println("[4/4] 수집된 데이터 병합 중...")
	var allRows[]EmbeddingRow
	for localBatch := range resultsChan {
		allRows = append(allRows, localBatch...)
	}

	// ==========================================
	//[시간 측정 종료]
	elapsedTime := time.Since(startTime)
	// ==========================================

	fmt.Printf("\n[결과 요약]\n")
	fmt.Printf("- 로드된 총 행(Rows) 개수: %d 개\n", len(allRows))
	fmt.Printf("- 총 소요 시간 (쿼리실행 + 병렬다운로드 + 디코딩): %s\n", elapsedTime)
	fmt.Println("※ 완료 후 임시 테이블은 자동 삭제됩니다.")
}