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
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func CosineDistance(a, b []float64) float64 {
	if len(a) != len(b) {
		return 1.0 // Maximum distance if mismatched
	}
	var dotProduct, normA, normB float64
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 1.0
	}
	similarity := dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
	// Cosine Distance = 1 - Cosine Similarity
	return 1.0 - similarity
}

func main() {
	fmt.Println("[1/3] 특정 내담자 벡터 추출")

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "testprojects-453622", option.WithCredentialsFile("/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json"))
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	// Using a sample file_code
	fileCode := "A008"

	queryStr := fmt.Sprintf(`
		SELECT timeline_index, embedding, split_contents
		FROM `+"`testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding`"+`
		WHERE file_code = '%s' and speaker = '내담자'
		ORDER BY timeline_index ASC
	`, fileCode)

	q := client.Query(queryStr)
	it, err := q.Read(ctx)
	if err != nil {
		log.Fatalf("Query.Read: %v", err)
	}

	type Row struct {
		TimelineIndex int64     `bigquery:"timeline_index"`
		Embedding     []float64 `bigquery:"embedding"`
		SplitContents string    `bigquery:"split_contents"`
	}

	var rows []Row
	for {
		var row Row
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("it.Next: %v", err)
		}
		rows = append(rows, row)
	}

	fmt.Println("[2/3] 코사인 변위 계산")

	var indices []int
	var distances []float64

	for i := 0; i < len(rows)-1; i++ {
		dist := CosineDistance(rows[i].Embedding, rows[i+1].Embedding)
		indices = append(indices, int(rows[i].TimelineIndex))
		distances = append(distances, dist)
	}

	// Sliding Window (3-turns MA) using gota dataframe
	df := dataframe.New(
		series.New(indices, series.Int, "TimelineIndex"),
		series.New(distances, series.Float, "Distance"),
	)

	// Calculate 3-turns moving average
	maDistances := make([]float64, df.Nrow())
	distCol := df.Col("Distance").Float()
	for i := range distCol {
		if i < 2 {
			maDistances[i] = distCol[i] // Not enough points for MA, use raw
		} else {
			maDistances[i] = (distCol[i] + distCol[i-1] + distCol[i-2]) / 3.0
		}
	}

	df = df.Mutate(series.New(maDistances, series.Float, "MA_Distance"))

	fmt.Println("[3/3] 시계열 그래프 생성")

	line := charts.NewLine()
	line.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    fmt.Sprintf("Emotional Volatility (File: %s)", fileCode),
			Subtitle: "Gemini Embedding 1 - Cosine Distance Moving Average",
		}),
		charts.WithInitializationOpts(opts.Initialization{
			Theme: "macarons",
		}),
	)

	xAxis := make([]int, df.Nrow())
	idxCol, _ := df.Col("TimelineIndex").Int()
	for i, val := range idxCol {
		xAxis[i] = val
	}

	yAxisDataRaw := make([]opts.LineData, 0)
	for _, val := range distCol {
		yAxisDataRaw = append(yAxisDataRaw, opts.LineData{Value: val})
	}

	yAxisDataMA := make([]opts.LineData, 0)
	for _, val := range maDistances {
		// Flight of ideas if moving average is high (e.g., > threshold)
		symbol := "circle"
		symbolSize := 4
		if val > 0.4 { // arbitrary threshold
			symbol = "triangle"
			symbolSize = 10
		}
		yAxisDataMA = append(yAxisDataMA, opts.LineData{Value: val, Symbol: symbol, SymbolSize: symbolSize})
	}

	line.SetXAxis(xAxis).
		AddSeries("Volatility", yAxisDataRaw).
		AddSeries("3-Turn MA (Flight of Ideas = Triangle)", yAxisDataMA)

	f, _ := os.Create("volatility.html")
	line.Render(f)
	fmt.Println("Done. Output written to volatility.html")
}
