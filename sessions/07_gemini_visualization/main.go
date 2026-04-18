package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/linkedin/goavro/v2"
	"github.com/schollz/progressbar/v3"
	"google.golang.org/api/option"
)

// --- 1. 데이터 구조 및 헬퍼 함수 정의 ---
type EmbeddingRow struct {
	FileCode      string
	TimelineIndex int
	Embedding[]float64
	Stage         string
}

type Job struct {
	FileCode string
	Rows[]EmbeddingRow
}

type Result struct {
	Group     string
	Distances[]float64
}

// Avro Nullable 필드 추출 헬퍼
func extractAvroString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case map[string]interface{}:
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
		if num, ok := val["long"].(int64); ok {
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

// --- 2. 비즈니스 로직 (거리 계산 워커) ---
func calculateDistance(v1, v2[]float64) float64 {
	sum := 0.0
	minLen := len(v1)
	if len(v2) < minLen {
		minLen = len(v2)
	}
	for i := 0; i < minLen; i++ {
		diff := v1[i] - v2[i]
		sum += diff * diff
	}
	return math.Sqrt(sum)
}

func worker(jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		if len(job.Rows) < 2 {
			continue
		}

		var dists[]float64
		group := "Normal"

		for i := 1; i < len(job.Rows); i++ {
			dist := calculateDistance(job.Rows[i].Embedding, job.Rows[i-1].Embedding)
			dists = append(dists, dist)

			if job.Rows[i].Stage != "정상" {
				group = "Depressed"
			}
		}

		results <- Result{
			Group:     group,
			Distances: dists,
		}
	}
}

// --- 3. 메인 프로세스 ---
func main() {
	fmt.Println("[Processing] Step 7: Storage Read API + Gemini Embedding 분산 분석 시작...")

	ctx := context.Background()
	projectID := "testprojects-453622"
	datasetID := "Psychological_counseling_data"
	keyPath := "/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json"
	authOpt := option.WithServiceAccountFile(keyPath)

	// [Phase 1] 클라이언트 초기화 및 쿼리 실행 -> 임시 테이블 생성
	bqClient, err := bigquery.NewClient(ctx, projectID, authOpt)
	if err != nil {
		log.Fatalf("Failed to create BQ client: %v", err)
	}
	defer bqClient.Close()

	storageClient, err := storage.NewBigQueryReadClient(ctx, authOpt)
	if err != nil {
		log.Fatalf("Failed to create Storage API client: %v", err)
	}
	defer storageClient.Close()

	tempTableID := fmt.Sprintf("temp_result_%d", time.Now().UnixNano())
	tempTable := bqClient.Dataset(datasetID).Table(tempTableID)
	defer tempTable.Delete(ctx) // 완료 시 임시 테이블 자동 삭제

	queryStr := `WITH Target_Processed_Data AS (SELECT file_code, ANY_VALUE(stage) AS stage FROM testprojects-453622.Psychological_counseling_data.processed_data GROUP BY file_code) SELECT e.file_code, e.timeline_index, e.embedding, p.stage FROM testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding AS e JOIN Target_Processed_Data AS p ON e.file_code = p.file_code`
	
	fmt.Println("\n[1/5] BigQuery 쿼리 실행 및 임시 테이블 생성 중...")
	query := bqClient.Query(queryStr)
	query.QueryConfig.Dst = tempTable
	job, err := query.Run(ctx)
	if err != nil {
		log.Fatalf("Failed to run query: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil || status.Err() != nil {
		log.Fatalf("Query job failed: %v", status.Err())
	}

	// [Phase 2] Storage Read API로 데이터 병렬 다운로드
	fmt.Println("\n[2/5] Storage Read API를 통한 데이터 병렬 다운로드 시작...")
	tableRef := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tempTableID)
	maxStreams := int32(runtime.NumCPU() * 2)

	sessionReq := &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &storagepb.ReadSession{
			Table:      tableRef,
			DataFormat: storagepb.DataFormat_AVRO,
		},
		MaxStreamCount: maxStreams,
	}

	session, err := storageClient.CreateReadSession(ctx, sessionReq)
	if err != nil {
		log.Fatalf("Failed to create read session: %v", err)
	}

	codec, err := goavro.NewCodec(session.GetAvroSchema().GetSchema())
	if err != nil {
		log.Fatalf("Failed to parse avro schema: %v", err)
	}

	// 스트림 결과를 받는 채널
	streamResultsChan := make(chan[]EmbeddingRow, len(session.Streams))
	var streamWg sync.WaitGroup

	for i, stream := range session.Streams {
		streamWg.Add(1)
		go func(streamName string, streamIndex int) {
			defer streamWg.Done()
			rowClient, err := storageClient.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: streamName})
			if err != nil {
				log.Printf("Stream error: %v", err)
				return
			}
			var localRows[]EmbeddingRow
			for {
				resp, err := rowClient.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					break
				}
				binaryBuffer := resp.GetAvroRows().GetSerializedBinaryRows()
				for len(binaryBuffer) > 0 {
					native, remaining, err := codec.NativeFromBinary(binaryBuffer)
					if err != nil {
						break
					}
					binaryBuffer = remaining
					if record, ok := native.(map[string]interface{}); ok {
						localRows = append(localRows, EmbeddingRow{
							FileCode:      extractAvroString(record["file_code"]),
							TimelineIndex: extractAvroInt(record["timeline_index"]),
							Embedding:     extractAvroFloatArray(record["embedding"]),
							Stage:         extractAvroString(record["stage"]),
						})
					}
				}
			}
			streamResultsChan <- localRows
		}(stream.Name, i)
	}

	// [Phase 3] 다운로드된 데이터를 수집하고 정렬 (중요)
	go func() {
		streamWg.Wait()
		close(streamResultsChan)
	}()

	groupedData := make(map[string][]EmbeddingRow)
	rowCount := 0
	for localBatch := range streamResultsChan {
		for _, row := range localBatch {
			groupedData[row.FileCode] = append(groupedData[row.FileCode], row)
			rowCount++
		}
	}

	fmt.Printf("[Processing] 총 %d행 다운로드 완료. (파일 그룹: %d개)\n", rowCount, len(groupedData))
	
	fmt.Println("\n[3/5] 타임라인 인덱스 기준 메모리 정렬(Sorting) 중...")
	for fileCode := range groupedData {
		sort.Slice(groupedData[fileCode], func(i, j int) bool {
			return groupedData[fileCode][i].TimelineIndex < groupedData[fileCode][j].TimelineIndex
		})
	}

	// [Phase 4] 분산 Worker Pool을 통한 감정 기복 계산
	fmt.Println("\n[4/5] 감정 기복(Euclidean Distance) 분산 병렬 계산 중...")
	barCalc := progressbar.Default(int64(len(groupedData)), "Calculating")

	numWorkers := runtime.NumCPU()
	jobs := make(chan Job, len(groupedData))
	resultsChan := make(chan Result, len(groupedData))
	var calcWg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		calcWg.Add(1)
		go worker(jobs, resultsChan, &calcWg)
	}

	go func() {
		for fc, rows := range groupedData {
			jobs <- Job{FileCode: fc, Rows: rows}
		}
		close(jobs)
	}()

	go func() {
		calcWg.Wait()
		close(resultsChan)
	}()

	results := make(map[string][]float64)
	for res := range resultsChan {
		results[res.Group] = append(results[res.Group], res.Distances...)
		_ = barCalc.Add(1)
	}

	// [Phase 5] 차트 시각화 및 HTML 렌더링
	smoothData := func(data []float64) []opts.LineData {
		var smoothed[]opts.LineData
		windowSize := 3
		if len(data) < windowSize {
			return smoothed
		}
		for i := 0; i <= len(data)-windowSize; i++ {
			sum := 0.0
			for j := 0; j < windowSize; j++ {
				sum += data[i+j]
			}
			smoothed = append(smoothed, opts.LineData{Value: sum / float64(windowSize)})
		}
		return smoothed
	}

	fmt.Println("\n\n[5/5] 차트 시각화(Go-Echarts) 및 렌더링 중...")
	line := charts.NewLine()
	line.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    "감정 기복률 시계열 분석 (Storage Read API + Gemini)",
			Subtitle: "정상군 vs 우울군 대조 분석 (Sliding Window: 3-turns)",
		}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true)}),
		charts.WithLegendOpts(opts.Legend{Right: "10%"}),
	)

	xAxis :=[]int{}
	for i := 0; i < 50; i++ {
		xAxis = append(xAxis, i)
	}

	getSafeSlice := func(data[]float64, maxLen int)[]float64 {
		if len(data) > maxLen {
			return data[:maxLen]
		}
		return data
	}

	line.SetXAxis(xAxis).
		AddSeries("정상군", smoothData(getSafeSlice(results["Normal"], 60))).
		AddSeries("우울군/불안장애", smoothData(getSafeSlice(results["Depressed"], 60)))

	f, err := os.Create("step7_emotional_volatility.html")
	if err != nil {
		log.Fatalf("Failed to create HTML file: %v", err)
	}
	defer f.Close()
	line.Render(f)

	fmt.Println("\n[Success] 시각화 결과가 step7_emotional_volatility.html 에 저장되었습니다! 🚀")
}