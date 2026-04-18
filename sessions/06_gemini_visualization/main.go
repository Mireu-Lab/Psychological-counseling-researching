package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/schollz/progressbar/v3"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// 데이터 구조 정의
type EmbeddingRow struct {
	FileCode      string    `bigquery:"file_code"`
	TimelineIndex int       `bigquery:"timeline_index"`
	Embedding     []float64 `bigquery:"embedding"`
	Stage         string    `bigquery:"stage"` // 정상/우울 여부 구분용
}

func main() {
	fmt.Println("[Processing] Step 6: Gemini Embedding 기반 감정 기복 분석 시작...")

	ctx := context.Background()
	projectID := "testprojects-453622"
	keyPath := "/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json"

	// 1. BigQuery 클라이언트 초기화
	client, err := bigquery.NewClient(ctx, projectID, option.WithServiceAccountFile(keyPath))
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}
	defer client.Close()

	// 2. 데이터 쿼리
	queryStr := `WITH Target_Processed_Data AS (SELECT file_code, ANY_VALUE(stage) AS stage FROM testprojects-453622.Psychological_counseling_data.processed_data GROUP BY file_code) SELECT e.file_code, e.timeline_index, e.embedding, p.stage FROM testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding AS e JOIN Target_Processed_Data AS p ON e.file_code = p.file_code ORDER BY e.file_code, e.timeline_index ASC`
	query := client.Query(queryStr)
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("Query execution failed: %v", err)
	}

	// [tqdm 1] 데이터 로드 진행률 표시
	// BigQuery의 총 행 갯수를 가져옵니다 (지원되지 않는 경우 -1로 설정되어 스피너 모드로 작동)
	totalRows := int64(-1)
	if it.TotalRows != 0 {
		totalRows = int64(it.TotalRows)
	}

	fmt.Println("\n[1/3] BigQuery 데이터 로드 중...")
	barLoad := progressbar.Default(totalRows, "Downloading")

	var rows []EmbeddingRow
	for {
		var row EmbeddingRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("\nError fetching rows: %v", err)
		}
		rows = append(rows, row)
		_ = barLoad.Add(1) // 진행률 1 증가
	}
	fmt.Printf("\n[Processing] %d개의 데이터 로드 완료\n", len(rows))

	// 3. 감정 기복 계산
	calculateDistance := func(v1, v2 []float64) float64 {
		sum := 0.0
		for i := range v1 {
			diff := v1[i] - v2[i]
			sum += diff * diff
		}
		return math.Sqrt(sum)
	}

	results := make(map[string][]float64)

	// [tqdm 2] 감정 기복 계산 진행률 표시
	fmt.Println("\n[2/3] 감정 기복(Euclidean Distance) 계산 중...")
	barCalc := progressbar.Default(int64(len(rows)-1), "Calculating")

	for i := 1; i < len(rows); i++ {
		if rows[i].FileCode == rows[i-1].FileCode {
			dist := calculateDistance(rows[i].Embedding, rows[i-1].Embedding)
			group := "Normal"
			if rows[i].Stage != "정상" {
				group = "Depressed"
			}
			results[group] = append(results[group], dist)
		}
		_ = barCalc.Add(1) // 진행률 1 증가
	}
	fmt.Println("\n[Processing] 기복 계산 완료")

	// 4. Sliding Window (3-turns) 처리
	smoothData := func(data []float64) []opts.LineData {
		var smoothed []opts.LineData
		windowSize := 3
		if len(data) < windowSize {
			return smoothed // 데이터가 윈도우 사이즈보다 작으면 빈 배열 반환
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

	// 5. 시각화 (Go-Echarts)
	fmt.Println("\n[3/3] 차트 시각화(Go-Echarts) 및 파일 렌더링 중...")
	line := charts.NewLine()
	line.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    "감정 기복률 시계열 분석 (Gemini Embedding)",
			Subtitle: "정상군 vs 우울군 대조 분석 (Sliding Window: 3-turns)",
		}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true)}),
		charts.WithLegendOpts(opts.Legend{Right: "10%"}),
	)

	xAxis := []int{}
	for i := 0; i < 50; i++ {
		xAxis = append(xAxis, i)
	}

	// Index out of range 에러를 방지하기 위해 배열 자르기 시 길이 체크
	getSafeSlice := func(data []float64, maxLen int) []float64 {
		if len(data) > maxLen {
			return data[:maxLen]
		}
		return data
	}

	line.SetXAxis(xAxis).
		AddSeries("정상군", smoothData(getSafeSlice(results["Normal"], 60))).
		AddSeries("우울군/불안장애", smoothData(getSafeSlice(results["Depressed"], 60)))

	// 차트 렌더링
	f, err := os.Create("step6_emotional_volatility.html")
	if err != nil {
		log.Fatalf("Failed to create HTML file: %v", err)
	}
	defer f.Close()
	line.Render(f)

	fmt.Println("\n[Success] 시각화 결과가 step6_emotional_volatility.html 에 성공적으로 저장되었습니다. 🎉")
}
