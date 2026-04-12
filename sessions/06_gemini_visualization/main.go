package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
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

	// 2. 데이터 쿼리 (Gemini 임베딩 및 메타데이터 JOIN)
	// 정상군(Normal)과 우울군(Depressed)을 대조하기 위해 stage 필드를 활용함
	queryStr := `
		SELECT
			e.file_code, e.timeline_index, e.embedding, p.stage
		FROM
			` + "`testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding`" + ` AS e
		JOIN
			` + "`testprojects-453622.Psychological_counseling_data.processed_data`" + ` AS p
		ON
			e.file_code = p.file_code
		ORDER BY 
			e.file_code, e.timeline_index ASC`

	query := client.Query(queryStr)
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("Query execution failed: %v", err)
	}

	var rows []EmbeddingRow
	for {
		var row EmbeddingRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Error fetching rows: %v", err)
		}
		rows = append(rows, row)
	}
	fmt.Printf("[Processing] %d개의 데이터 로드 완료\n", len(rows))

	// 3. 감정 기복 계산 (Euclidean Distance & Sliding Window)
	// Gota DataFrame 대신 슬라이스로 처리 후 분석
	type AnalysisResult struct {
		Index      int
		Volatility float64
		Group      string
	}

	calculateDistance := func(v1, v2 []float64) float64 {
		sum := 0.0
		for i := range v1 {
			diff := v1[i] - v2[i]
			sum += diff * diff
		}
		return math.Sqrt(sum)
	}

	results := make(map[string][]float64)

	// 데이터 그룹화 및 기복 계산 루프 (단순화된 예시)
	for i := 1; i < len(rows); i++ {
		if rows[i].FileCode == rows[i-1].FileCode {
			dist := calculateDistance(rows[i].Embedding, rows[i-1].Embedding)
			group := "Normal"
			if rows[i].Stage != "정상" { // 가상의 조건
				group = "Depressed"
			}
			results[group] = append(results[group], dist)
		}
	}

	// 4. Sliding Window (3-turns) 처리
	smoothData := func(data []float64) []opts.LineData {
		var smoothed []opts.LineData
		windowSize := 3
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
	for i := 0; i < 50; i++ { // 최대 50 Turn까지 시각화
		xAxis = append(xAxis, i)
	}

	line.SetXAxis(xAxis).
		AddSeries("정상군", smoothData(results["Normal"][:60])).
		AddSeries("우울군/불안장애", smoothData(results["Depressed"][:60]))

	// 차트 렌더링
	f, _ := os.Create("step6_emotional_volatility.html")
	line.Render(f)

	fmt.Println("[Success] 시각화 결과가 step6_emotional_volatility.html 에 저장되었습니다.")
}
