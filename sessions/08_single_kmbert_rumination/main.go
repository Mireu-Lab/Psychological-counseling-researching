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
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func CosineDistance(a, b []float64) float64 {
	if len(a) != len(b) {
		return 1.0
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
	return 1.0 - (dotProduct / (math.Sqrt(normA) * math.Sqrt(normB)))
}

func main() {
	fmt.Println("[1/3] BERT 임베딩 로드")

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "testprojects-453622", option.WithCredentialsFile("/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json"))
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	fileCode := "A008"

	queryStr := fmt.Sprintf(`
		SELECT timeline_index, content_embedding as embedding, split_contents
		FROM `+"`testprojects-453622.Psychological_counseling_data.morpheme_classification_kmbert_embedding`"+`
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

	fmt.Println("[2/3] 고착/비약 지표 정량화")

	var indices []int
	var distances []float64

	for i := 0; i < len(rows)-1; i++ {
		dist := CosineDistance(rows[i].Embedding, rows[i+1].Embedding)
		indices = append(indices, int(rows[i].TimelineIndex))
		distances = append(distances, dist)
	}

	fmt.Println("[3/3] Area Chart 시각화")

	line := charts.NewLine()
	line.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    fmt.Sprintf("Cognitive Rumination Index (%s)", fileCode),
			Subtitle: "KM-BERT Embedding",
		}),
		charts.WithXAxisOpts(opts.XAxis{
			Name: "Timeline Index",
		}),
		charts.WithYAxisOpts(opts.YAxis{
			Name: "Distance",
		}),
		charts.WithInitializationOpts(opts.Initialization{
			Theme: "macarons",
		}),
	)

	xAxis := make([]int, len(indices))
	yAxisDataRaw := make([]opts.LineData, 0)

	for i, dist := range distances {
		xAxis[i] = indices[i]
		yAxisDataRaw = append(yAxisDataRaw, opts.LineData{
			Value: dist,
		})
	}

	line.SetXAxis(xAxis).AddSeries(
		"Cognitive Distance",
		yAxisDataRaw,
		charts.WithAreaStyleOpts(opts.AreaStyle{Opacity: opts.Float(0.5)}),
		charts.WithLineChartOpts(opts.LineChart{Smooth: opts.Bool(true)}),
	)

	f, _ := os.Create("rumination.html")
	line.Render(f)

	fmt.Println("Done. Output written to rumination.html")
}
