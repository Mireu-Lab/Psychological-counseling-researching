package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/api/option"
)

const (
	projectID   = "testprojects-453622"
	datasetID   = "Psychological_counseling_data"
	credPath    = "/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json"
	fontPath    = "/workspaces/Psychological-counseling-researching/.fonts/NotoSansKR-Bold.ttf"
	outputHTML  = "swamp_landscape_merge.html"
	totalStages = 4
)

type EmbeddingRow struct {
	TimelineIndex   int64     `bigquery:"timeline_index"`
	KMbertEmbedding []float64 `bigquery:"kmbert_embedding"`
	GeminiEmbedding []float64 `bigquery:"gemini_embedding"`
	SplitContents   string    `bigquery:"split_contents"`
}

type ProgressBar struct {
	Prefix string
	Total  int
	Width  int
	Start  time.Time
	last   time.Time
}

func newProgressBar(prefix string, total int) *ProgressBar {
	return &ProgressBar{
		Prefix: prefix,
		Total:  total,
		Width:  28,
		Start:  time.Now(),
	}
}

func (p *ProgressBar) Render(current int, info string) {
	if p.Total <= 0 {
		return
	}
	if current < 0 {
		current = 0
	}
	if current > p.Total {
		current = p.Total
	}

	now := time.Now()
	if current != p.Total && now.Sub(p.last) < 80*time.Millisecond {
		return
	}
	p.last = now

	ratio := float64(current) / float64(p.Total)
	filled := int(ratio * float64(p.Width))
	if filled > p.Width {
		filled = p.Width
	}
	bar := strings.Repeat("#", filled) + strings.Repeat("-", p.Width-filled)

	elapsed := now.Sub(p.Start)
	eta := "--"
	if current > 0 {
		remaining := time.Duration(float64(elapsed) * float64(p.Total-current) / float64(current))
		eta = remaining.Round(100 * time.Millisecond).String()
	}

	fmt.Printf("\r    %s [%s] %6.2f%% (%d/%d) elapsed=%s eta=%s %s",
		p.Prefix,
		bar,
		ratio*100.0,
		current,
		p.Total,
		elapsed.Round(100*time.Millisecond),
		eta,
		info,
	)

	if current == p.Total {
		fmt.Print("\n")
	}
}

func renderSpinner(prefix string, count int, start time.Time) {
	frames := []string{"|", "/", "-", "\\"}
	idx := (count / 17) % len(frames)
	fmt.Printf("\r    %s %s rows=%d elapsed=%s", prefix, frames[idx], count, time.Since(start).Round(100*time.Millisecond))
}

func printStageHeader(stage int, total int, title string) time.Time {
	now := time.Now()
	fmt.Printf("\n[%d/%d] %s\n", stage, total, title)
	fmt.Printf("    start=%s\n", now.Format("15:04:05"))
	return now
}

func printStageDone(title string, stageStart time.Time) {
	fmt.Printf("    완료: %s (elapsed=%s)\n", title, time.Since(stageStart).Round(100*time.Millisecond))
}

func extractAvroString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case map[string]interface{}:
		if sv, ok := val["string"].(string); ok {
			return sv
		}
	}
	return ""
}

func extractAvroInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case map[string]interface{}:
		if lv, ok := val["long"].(int64); ok {
			return lv
		}
	}
	return 0
}

func extractAvroFloatArray(v interface{}) []float64 {
	if v == nil {
		return nil
	}

	arr, ok := v.([]interface{})
	if !ok {
		if mv, mapOk := v.(map[string]interface{}); mapOk {
			if av, arrOk := mv["array"].([]interface{}); arrOk {
				arr = av
			} else {
				return nil
			}
		} else {
			return nil
		}
	}

	out := make([]float64, 0, len(arr))
	for _, item := range arr {
		switch val := item.(type) {
		case float64:
			out = append(out, val)
		case float32:
			out = append(out, float64(val))
		case int64:
			out = append(out, float64(val))
		case int:
			out = append(out, float64(val))
		}
	}

	return out
}

func runSinkQueryToTempTable(
	ctx context.Context,
	bqClient *bigquery.Client,
	fileCode string,
	sessionNo int64,
	speaker string,
) (string, func(), error) {
	tempTableID := fmt.Sprintf("temp_swamp_landscape_%d", time.Now().UnixNano())
	tempTable := bqClient.Dataset(datasetID).Table(tempTableID)
	cleanup := func() {
		if err := tempTable.Delete(ctx); err != nil {
			log.Printf("warn: failed to delete temp table %s: %v", tempTableID, err)
		}
	}

	query := `
		SELECT
		  k.timeline_index,
		  k.content_embedding AS kmbert_embedding,
		  g.embedding AS gemini_embedding,
		  k.split_contents
		FROM ` + "`testprojects-453622.Psychological_counseling_data.morpheme_classification_kmbert_embedding`" + ` AS k
		JOIN ` + "`testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding`" + ` AS g
		  ON k.file_code = g.file_code
		 AND k.session_no = g.session_no
		 AND k.timeline_index = g.timeline_index
		 AND k.speaker = g.speaker
		WHERE k.file_code = @fileCode
		  AND k.session_no < @sessionNo
		  AND k.speaker = @speaker
		ORDER BY k.timeline_index ASC
	`

	q := bqClient.Query(query)
	q.QueryConfig.Dst = tempTable
	q.Parameters = []bigquery.QueryParameter{
		{Name: "fileCode", Value: fileCode},
		{Name: "sessionNo", Value: sessionNo},
		{Name: "speaker", Value: speaker},
	}

	job, err := q.Run(ctx)
	if err != nil {
		return "", cleanup, fmt.Errorf("failed to run sink query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return "", cleanup, fmt.Errorf("failed waiting sink query: %w", err)
	}
	if err := status.Err(); err != nil {
		return "", cleanup, fmt.Errorf("sink query failed: %w", err)
	}

	return tempTableID, cleanup, nil
}

func readByStorageAPI(ctx context.Context, storageClient *storage.BigQueryReadClient, tempTableID string) ([]EmbeddingRow, error) {
	tableRef := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tempTableID)
	maxStreams := int32(runtime.NumCPU() * 2)
	if maxStreams < 1 {
		maxStreams = 1
	}

	sessionReq := &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &storagepb.ReadSession{
			Table:      tableRef,
			DataFormat: storagepb.DataFormat_AVRO,
		},
		MaxStreamCount: maxStreams,
	}

	readSession, err := storageClient.CreateReadSession(ctx, sessionReq)
	if err != nil {
		return nil, fmt.Errorf("failed to create read session: %w", err)
	}

	if len(readSession.Streams) == 0 {
		return nil, nil
	}

	avroSchema := readSession.GetAvroSchema().GetSchema()
	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse avro schema: %w", err)
	}

	fmt.Printf("    Storage Read Session: streams=%d requested_max_streams=%d\n", len(readSession.Streams), maxStreams)

	resultsChan := make(chan []EmbeddingRow, len(readSession.Streams))
	errorChan := make(chan error, len(readSession.Streams))
	var wg sync.WaitGroup

	for i, stream := range readSession.Streams {
		wg.Add(1)
		go func(streamName string, streamIndex int) {
			defer wg.Done()

			rowClient, err := storageClient.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: streamName})
			if err != nil {
				errorChan <- fmt.Errorf("stream %d init failed: %w", streamIndex, err)
				return
			}

			localRows := make([]EmbeddingRow, 0, 512)
			for {
				resp, err := rowClient.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					errorChan <- fmt.Errorf("stream %d read failed: %w", streamIndex, err)
					return
				}

				buffer := resp.GetAvroRows().GetSerializedBinaryRows()
				for len(buffer) > 0 {
					native, remaining, err := codec.NativeFromBinary(buffer)
					if err != nil {
						errorChan <- fmt.Errorf("stream %d avro decode failed: %w", streamIndex, err)
						return
					}
					buffer = remaining

					record, ok := native.(map[string]interface{})
					if !ok {
						continue
					}

					localRows = append(localRows, EmbeddingRow{
						TimelineIndex:   extractAvroInt64(record["timeline_index"]),
						KMbertEmbedding: extractAvroFloatArray(record["kmbert_embedding"]),
						GeminiEmbedding: extractAvroFloatArray(record["gemini_embedding"]),
						SplitContents:   extractAvroString(record["split_contents"]),
					})
				}
			}

			resultsChan <- localRows
		}(stream.Name, i)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
		close(errorChan)
	}()

	allRows := make([]EmbeddingRow, 0, 2048)
	streamBatchCount := 0
	mergeStart := time.Now()
	for batch := range resultsChan {
		streamBatchCount++
		allRows = append(allRows, batch...)
		renderSpinner("Storage stream 병합", len(allRows), mergeStart)
	}
	fmt.Print("\n")
	fmt.Printf("    스트림 배치 병합 완료: batch_count=%d rows=%d\n", streamBatchCount, len(allRows))

	for err := range errorChan {
		if err != nil {
			return nil, err
		}
	}

	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].TimelineIndex < allRows[j].TimelineIndex
	})

	return allRows, nil
}

func cosineDistance(a, b []float64) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 1.0
	}

	var dot, normA, normB float64
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 1.0
	}

	return 1.0 - (dot / (math.Sqrt(normA) * math.Sqrt(normB)))
}

func movingAverage(values []float64, window int) []float64 {
	if len(values) == 0 {
		return nil
	}
	if window <= 1 {
		cloned := make([]float64, len(values))
		copy(cloned, values)
		return cloned
	}

	out := make([]float64, len(values))
	for i := range values {
		start := i - window + 1
		if start < 0 {
			start = 0
		}

		var sum float64
		count := 0
		for j := start; j <= i; j++ {
			sum += values[j]
			count++
		}
		out[i] = sum / float64(count)
	}

	return out
}

func meanStd(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0.0, 0.0
	}

	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))

	if len(values) == 1 {
		return mean, 0.0
	}

	var varianceSum float64
	for _, v := range values {
		delta := v - mean
		varianceSum += delta * delta
	}
	std := math.Sqrt(varianceSum / float64(len(values)))

	return mean, std
}

func getenvOrDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func getenvIntOrDefault(key string, fallback int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func injectFontCSS(html string) string {
	fontCSS := fmt.Sprintf(`<style>
@font-face {
  font-family: 'NotoSansKRBold';
  src: url('%s') format('truetype');
  font-weight: 700;
  font-style: normal;
}
html, body, div, span, p, h1, h2, h3, h4, h5, h6 {
  font-family: 'NotoSansKRBold', sans-serif !important;
}
html, body {
	width: 100%%;
	height: 100%%;
	margin: 0;
	padding: 0;
	overflow-x: hidden;
}
.container {
	width: 100vw !important;
	margin: 0 !important;
	padding: 0 !important;
}
</style>`, fontPath)

	if strings.Contains(html, "</head>") {
		return strings.Replace(html, "</head>", fontCSS+"\n</head>", 1)
	}

	return fontCSS + "\n" + html
}

func main() {
	jobStart := time.Now()
	fileCode := getenvOrDefault("FILE_CODE", "A008")
	sessionNo := getenvIntOrDefault("SESSION_NO", 8)
	speaker := getenvOrDefault("SPEAKER", "내담자")
	fmt.Printf("Run Config | file_code=%s session_no=%d speaker=%s\n", fileCode, sessionNo, speaker)

	stageStart := printStageHeader(1, totalStages, "멀티 임베딩 Sinking + Storage Read API 로드")

	ctx := context.Background()
	authOpt := option.WithCredentialsFile(credPath)

	bqClient, err := bigquery.NewClient(ctx, projectID, authOpt)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer bqClient.Close()

	storageClient, err := storage.NewBigQueryReadClient(ctx, authOpt)
	if err != nil {
		log.Fatalf("storage.NewBigQueryReadClient: %v", err)
	}
	defer storageClient.Close()

	tempTableID, cleanupTempTable, err := runSinkQueryToTempTable(ctx, bqClient, fileCode, sessionNo, speaker)
	if err != nil {
		log.Fatalf("runSinkQueryToTempTable: %v", err)
	}
	defer cleanupTempTable()
	fmt.Printf("    임시 테이블 생성 완료: %s\n", tempTableID)

	rows, err := readByStorageAPI(ctx, storageClient, tempTableID)
	if err != nil {
		log.Fatalf("readByStorageAPI: %v", err)
	}

	printStageDone("멀티 임베딩 Sinking + Storage Read API 로드", stageStart)
	fmt.Printf("    수집된 행 수: %d\n", len(rows))

	if len(rows) < 2 {
		log.Fatalf("not enough rows to compute distance: %d", len(rows))
	}

	stageStart = printStageHeader(2, totalStages, "고착도(늪) 및 변동성(풍경) 산출")

	indices := make([]int, 0, len(rows)-1)
	kmbertDistances := make([]float64, 0, len(rows)-1)
	geminiDistances := make([]float64, 0, len(rows)-1)
	distanceBar := newProgressBar("distance", len(rows)-1)

	for i := 0; i < len(rows)-1; i++ {
		indices = append(indices, int(rows[i+1].TimelineIndex))
		kmbertDistances = append(kmbertDistances, cosineDistance(rows[i].KMbertEmbedding, rows[i+1].KMbertEmbedding))
		geminiDistances = append(geminiDistances, cosineDistance(rows[i].GeminiEmbedding, rows[i+1].GeminiEmbedding))
		distanceBar.Render(i+1, "코사인 변위 계산")
	}

	kmbertMA5 := movingAverage(kmbertDistances, 5)
	geminiMA3 := movingAverage(geminiDistances, 3)

	swampIndex := make([]float64, len(kmbertMA5))
	for i, v := range kmbertMA5 {
		s := 1.0 - v
		if s < 0 {
			s = 0
		}
		if s > 1 {
			s = 1
		}
		swampIndex[i] = s
	}

	volatilityIndex := make([]float64, len(geminiMA3))
	copy(volatilityIndex, geminiMA3)

	mean, std := meanStd(volatilityIndex)
	spikeThreshold := mean + (2.0 * std)
	printStageDone("고착도(늪) 및 변동성(풍경) 산출", stageStart)
	fmt.Printf("    통계: mean=%.6f std=%.6f spike_threshold=%.6f\n", mean, std, spikeThreshold)

	stageStart = printStageHeader(3, totalStages, "이중 축(Dual-Axis) 차트 생성")

	line := charts.NewLine()
	line.ExtendYAxis(opts.YAxis{Name: "Volatility Index (Gemini)", Min: 0, Max: 1})
	line.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: "shine", Width: "100vw", Height: "78vh"}),
		charts.WithTitleOpts(opts.Title{
			Title:    fmt.Sprintf("늪과 풍경 (File: %s / Session: %d)", fileCode, sessionNo),
			Subtitle: fmt.Sprintf("Swamp=1-MA5(KM-BERT Distance), Landscape=MA3(Gemini Distance), Spike > %.4f", spikeThreshold),
		}),
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true)}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true), Trigger: "axis"}),
		charts.WithXAxisOpts(opts.XAxis{Name: "Timeline Index"}),
		charts.WithYAxisOpts(opts.YAxis{Name: "Swamp Index (KM-BERT)", Min: 0, Max: 1}),
		charts.WithDataZoomOpts(
			opts.DataZoom{Type: "inside", XAxisIndex: []int{0}, FilterMode: "none"},
			opts.DataZoom{Type: "slider", XAxisIndex: []int{0}, Start: 0, End: 100, FilterMode: "none"},
		),
		charts.WithToolboxOpts(opts.Toolbox{
			Show: opts.Bool(true),
			Feature: &opts.ToolBoxFeature{
				Restore:     &opts.ToolBoxFeatureRestore{Show: opts.Bool(true), Title: "초기화"},
				SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{Show: opts.Bool(true), Name: "swamp_landscape_merge"},
			},
		}),
	)

	swampData := make([]opts.LineData, 0, len(swampIndex))
	swampBar := newProgressBar("swamp-area", len(swampIndex))
	for _, v := range swampIndex {
		swampData = append(swampData, opts.LineData{Value: v})
		swampBar.Render(len(swampData), "Area 데이터 준비")
	}
	printStageDone("이중 축(Dual-Axis) 차트 생성", stageStart)

	stageStart = printStageHeader(4, totalStages, "융합 심리 패턴 마킹")

	volatilityData := make([]opts.LineData, 0, len(volatilityIndex))
	markBar := newProgressBar("landscape-mark", len(volatilityIndex))
	spikeCount := 0
	for _, v := range volatilityIndex {
		symbol := "circle"
		symbolSize := 5
		if v > spikeThreshold {
			symbol = "triangle"
			symbolSize = 12
			spikeCount++
		}
		volatilityData = append(volatilityData, opts.LineData{
			Value:      v,
			Symbol:     symbol,
			SymbolSize: symbolSize,
		})
		markBar.Render(len(volatilityData), "Spike 마커 설정")
	}

	line.SetXAxis(indices).
		AddSeries(
			"Swamp Index (KM-BERT, Area)",
			swampData,
			charts.WithLineChartOpts(opts.LineChart{Smooth: opts.Bool(true), YAxisIndex: 0, ShowSymbol: opts.Bool(false)}),
			charts.WithAreaStyleOpts(opts.AreaStyle{Opacity: opts.Float(0.5), Color: "rgba(47,79,79,0.75)"}),
			charts.WithLineStyleOpts(opts.LineStyle{Color: "#2F4F4F", Width: 2}),
		).
		AddSeries(
			"Landscape Index (Gemini, Line)",
			volatilityData,
			charts.WithLineChartOpts(opts.LineChart{Smooth: opts.Bool(true), YAxisIndex: 1}),
			charts.WithLineStyleOpts(opts.LineStyle{Color: "#D62728", Width: 3}),
		)

	var rendered bytes.Buffer
	if err := line.Render(&rendered); err != nil {
		log.Fatalf("chart render failed: %v", err)
	}

	styledHTML := injectFontCSS(rendered.String())
	if err := os.WriteFile(outputHTML, []byte(styledHTML), 0o644); err != nil {
		log.Fatalf("failed to write html: %v", err)
	}
	printStageDone("융합 심리 패턴 마킹", stageStart)

	fmt.Printf("Done. Output written to %s\n", outputHTML)
	fmt.Printf("Summary | spikes=%d total_points=%d total_elapsed=%s\n", spikeCount, len(volatilityIndex), time.Since(jobStart).Round(100*time.Millisecond))
}
