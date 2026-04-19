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
	outputHTML  = "multi_gemini_relative_volatility.html"
	totalStages = 4
)

type EmbeddingRow struct {
	FileCode      string    `bigquery:"file_code"`
	TimelineIndex int64     `bigquery:"timeline_index"`
	Embedding     []float64 `bigquery:"embedding"`
}

type SessionCountRow struct {
	FileCode     string `bigquery:"file_code"`
	SessionCount int64  `bigquery:"session_count"`
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
	case float64:
		return int64(val)
	case map[string]interface{}:
		if lv, ok := val["long"].(int64); ok {
			return lv
		}
		if iv, ok := val["int"].(int32); ok {
			return int64(iv)
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
	tablePrefix string,
	query string,
	params []bigquery.QueryParameter,
) (string, func(), error) {
	tempTableID := fmt.Sprintf("%s_%d", tablePrefix, time.Now().UnixNano())
	tempTable := bqClient.Dataset(datasetID).Table(tempTableID)
	cleanup := func() {
		if err := tempTable.Delete(ctx); err != nil {
			log.Printf("warn: failed to delete temp table %s: %v", tempTableID, err)
		}
	}

	q := bqClient.Query(query)
	q.QueryConfig.Dst = tempTable
	q.Parameters = params

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

func readAvroNativeRows(ctx context.Context, storageClient *storage.BigQueryReadClient, tempTableID string) ([]map[string]interface{}, error) {
	tableRef := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tempTableID)
	maxStreams := int32(runtime.NumCPU() * 2)
	if maxStreams < 1 {
		maxStreams = 1
	}

	readSession, err := storageClient.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
		ReadSession: &storagepb.ReadSession{
			Table:      tableRef,
			DataFormat: storagepb.DataFormat_AVRO,
		},
		MaxStreamCount: maxStreams,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create read session: %w", err)
	}

	if len(readSession.Streams) == 0 {
		return nil, nil
	}
	fmt.Printf("    Storage Read Session: streams=%d requested_max_streams=%d\n", len(readSession.Streams), maxStreams)

	codec, err := goavro.NewCodec(readSession.GetAvroSchema().GetSchema())
	if err != nil {
		return nil, fmt.Errorf("failed to parse avro schema: %w", err)
	}

	resultsChan := make(chan []map[string]interface{}, len(readSession.Streams))
	errorChan := make(chan error, len(readSession.Streams))
	var wg sync.WaitGroup

	for streamIndex, stream := range readSession.Streams {
		wg.Add(1)
		go func(idx int, streamName string) {
			defer wg.Done()

			rowClient, err := storageClient.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: streamName})
			if err != nil {
				errorChan <- fmt.Errorf("stream %d init failed: %w", idx, err)
				return
			}

			localRows := make([]map[string]interface{}, 0, 512)
			for {
				resp, err := rowClient.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					errorChan <- fmt.Errorf("stream %d read failed: %w", idx, err)
					return
				}

				buffer := resp.GetAvroRows().GetSerializedBinaryRows()
				for len(buffer) > 0 {
					native, remaining, err := codec.NativeFromBinary(buffer)
					if err != nil {
						errorChan <- fmt.Errorf("stream %d avro decode failed: %w", idx, err)
						return
					}
					buffer = remaining

					record, ok := native.(map[string]interface{})
					if !ok {
						continue
					}
					localRows = append(localRows, record)
				}
			}

			resultsChan <- localRows
		}(streamIndex, stream.Name)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
		close(errorChan)
	}()

	allRows := make([]map[string]interface{}, 0, 2048)
	batchCount := 0
	mergeStart := time.Now()
	for batch := range resultsChan {
		batchCount++
		allRows = append(allRows, batch...)
		renderSpinner("Storage stream 병합", len(allRows), mergeStart)
	}
	fmt.Print("\n")
	fmt.Printf("    스트림 배치 병합 완료: batch_count=%d rows=%d\n", batchCount, len(allRows))

	for err := range errorChan {
		if err != nil {
			return nil, err
		}
	}

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

	var variance float64
	for _, v := range values {
		d := v - mean
		variance += d * d
	}

	return mean, math.Sqrt(variance / float64(len(values)))
}

func quantiles(values []float64) (float64, float64, float64, float64, float64) {
	if len(values) == 0 {
		return 0, 0, 0, 0, 0
	}
	cloned := make([]float64, len(values))
	copy(cloned, values)
	sort.Float64s(cloned)

	q := func(p float64) float64 {
		if len(cloned) == 1 {
			return cloned[0]
		}
		idx := p * float64(len(cloned)-1)
		lo := int(math.Floor(idx))
		hi := int(math.Ceil(idx))
		if lo == hi {
			return cloned[lo]
		}
		frac := idx - float64(lo)
		return cloned[lo] + (cloned[hi]-cloned[lo])*frac
	}

	return cloned[0], q(0.25), q(0.50), q(0.75), cloned[len(cloned)-1]
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
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

func main() {
	jobStart := time.Now()
	targetFileCode := getenvOrDefault("FILE_CODE", "A008")
	speaker := getenvOrDefault("SPEAKER", "내담자")
	weightK := float64(getenvIntOrDefault("RELIABILITY_K", 5))
	if weightK <= 0 {
		weightK = 5
	}

	fmt.Printf("Run Config | file_code=%s speaker=%s reliability_k=%.0f\n", targetFileCode, speaker, weightK)

	ctx := context.Background()
	authOpt := option.WithCredentialsFile(credPath)
	client, err := bigquery.NewClient(ctx, projectID, authOpt)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	storageClient, err := storage.NewBigQueryReadClient(ctx, authOpt)
	if err != nil {
		log.Fatalf("storage.NewBigQueryReadClient: %v", err)
	}
	defer storageClient.Close()

	fmt.Println("\n[1/4] 전수 데이터 변동성 집계")
	stageStart := time.Now()

	queryAll := `
		SELECT file_code, timeline_index, embedding
		FROM ` + "`testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding`" + `
		WHERE speaker = @speaker
		ORDER BY file_code, timeline_index ASC
	`

	tempEmbeddingsTable, cleanupEmbeddings, err := runSinkQueryToTempTable(
		ctx,
		client,
		"temp_step10_embeddings",
		queryAll,
		[]bigquery.QueryParameter{{Name: "speaker", Value: speaker}},
	)
	if err != nil {
		log.Fatalf("runSinkQueryToTempTable(embeddings): %v", err)
	}
	defer cleanupEmbeddings()

	embeddingNativeRows, err := readAvroNativeRows(ctx, storageClient, tempEmbeddingsTable)
	if err != nil {
		log.Fatalf("readAvroNativeRows(embeddings): %v", err)
	}

	embeddingRows := make([]EmbeddingRow, 0, len(embeddingNativeRows))
	decodeBar := newProgressBar("decode-embedding", len(embeddingNativeRows))
	for _, native := range embeddingNativeRows {
		embeddingRows = append(embeddingRows, EmbeddingRow{
			FileCode:      extractAvroString(native["file_code"]),
			TimelineIndex: extractAvroInt64(native["timeline_index"]),
			Embedding:     extractAvroFloatArray(native["embedding"]),
		})
		decodeBar.Render(len(embeddingRows), "Avro -> EmbeddingRow")
	}

	sort.Slice(embeddingRows, func(i, j int) bool {
		if embeddingRows[i].FileCode == embeddingRows[j].FileCode {
			return embeddingRows[i].TimelineIndex < embeddingRows[j].TimelineIndex
		}
		return embeddingRows[i].FileCode < embeddingRows[j].FileCode
	})

	vectorsByFile := make(map[string][][]float64)
	groupBar := newProgressBar("group-file", len(embeddingRows))
	groupCurrent := 0
	for _, row := range embeddingRows {
		vectorsByFile[row.FileCode] = append(vectorsByFile[row.FileCode], row.Embedding)
		groupCurrent++
		groupBar.Render(groupCurrent, "file_code 그룹화")
	}
	rowCount := len(embeddingRows)

	querySessions := `
		SELECT file_code, COUNT(DISTINCT session_no) AS session_count
		FROM ` + "`testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding`" + `
		WHERE speaker = @speaker
		GROUP BY file_code
	`

	tempSessionsTable, cleanupSessions, err := runSinkQueryToTempTable(
		ctx,
		client,
		"temp_step10_sessions",
		querySessions,
		[]bigquery.QueryParameter{{Name: "speaker", Value: speaker}},
	)
	if err != nil {
		log.Fatalf("runSinkQueryToTempTable(session count): %v", err)
	}
	defer cleanupSessions()

	sessionNativeRows, err := readAvroNativeRows(ctx, storageClient, tempSessionsTable)
	if err != nil {
		log.Fatalf("readAvroNativeRows(session count): %v", err)
	}

	sessionsByFile := make(map[string]int64)
	sessionBar := newProgressBar("session-agg", len(sessionNativeRows))
	sessionCurrent := 0
	for _, native := range sessionNativeRows {
		row := SessionCountRow{
			FileCode:     extractAvroString(native["file_code"]),
			SessionCount: extractAvroInt64(native["session_count"]),
		}
		sessionsByFile[row.FileCode] = row.SessionCount
		sessionCurrent++
		sessionBar.Render(sessionCurrent, "session_count 매핑")
	}

	volatilityByFile := make(map[string]float64)
	volBar := newProgressBar("volatility", len(vectorsByFile))
	volCurrent := 0
	for fileCode, vecs := range vectorsByFile {
		if len(vecs) < 2 {
			volCurrent++
			volBar.Render(volCurrent, "변동성 계산")
			continue
		}
		dists := make([]float64, 0, len(vecs)-1)
		for i := 0; i < len(vecs)-1; i++ {
			dists = append(dists, cosineDistance(vecs[i], vecs[i+1]))
		}
		mu, _ := meanStd(dists)
		volatilityByFile[fileCode] = mu
		volCurrent++
		volBar.Render(volCurrent, "변동성 계산")
	}

	fmt.Printf("    완료: 전수 데이터 변동성 집계 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("    수집 행 수=%d, 환자 수=%d\n", rowCount, len(volatilityByFile))

	targetVol, ok := volatilityByFile[targetFileCode]
	if !ok {
		log.Fatalf("target file_code not found or insufficient rows: %s", targetFileCode)
	}

	allVols := make([]float64, 0, len(volatilityByFile))
	volCollectBar := newProgressBar("collect-vol", len(volatilityByFile))
	collectCurrent := 0
	for _, v := range volatilityByFile {
		allVols = append(allVols, v)
		collectCurrent++
		volCollectBar.Render(collectCurrent, "분포 벡터 준비")
	}

	fmt.Println("\n[2/4] Z-Score 정규화")
	stageStart = time.Now()
	globalMean, globalStd := meanStd(allVols)
	zScore := 0.0
	if globalStd > 0 {
		zScore = (targetVol - globalMean) / globalStd
	}
	fmt.Printf("    완료: Z-Score 정규화 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("    통계: mean=%.6f std=%.6f target_volatility=%.6f z=%.6f\n", globalMean, globalStd, targetVol, zScore)

	fmt.Println("\n[3/4] 상담 횟수 기반 신뢰도 보정")
	stageStart = time.Now()
	sessionCount := sessionsByFile[targetFileCode]
	if sessionCount < 0 {
		sessionCount = 0
	}
	weight := float64(sessionCount) / (float64(sessionCount) + weightK)
	adjustedVol := globalMean + weight*(targetVol-globalMean)
	adjustedZ := zScore * weight
	fmt.Printf("    완료: 상담 횟수 기반 신뢰도 보정 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("    보정치: N=%d W=%.6f adjusted_volatility=%.6f adjusted_z=%.6f\n", sessionCount, weight, adjustedVol, adjustedZ)

	fmt.Println("\n[4/4] 상대 비교 분포도 생성")
	stageStart = time.Now()

	minV, q1, median, q3, maxV := quantiles(allVols)
	box := charts.NewBoxPlot()
	box.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: "shine", Width: "100vw", Height: "78vh"}),
		charts.WithTitleOpts(opts.Title{
			Title:    "다중 내담자 상대 변동성 분포 (Gemini)",
			Subtitle: fmt.Sprintf("Target=%s | Z=%.4f -> Adjusted Z=%.4f (W=%.4f)", targetFileCode, zScore, adjustedZ, weight),
		}),
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true)}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true), Trigger: "item"}),
		charts.WithXAxisOpts(opts.XAxis{Name: "Population"}),
		charts.WithYAxisOpts(opts.YAxis{Name: "Volatility (Cosine Distance Mean)"}),
		charts.WithToolboxOpts(opts.Toolbox{
			Show: opts.Bool(true),
			Feature: &opts.ToolBoxFeature{
				Restore:     &opts.ToolBoxFeatureRestore{Show: opts.Bool(true), Title: "초기화"},
				SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{Show: opts.Bool(true), Name: "multi_gemini_relative_volatility"},
			},
		}),
	)

	box.SetXAxis([]string{"전체 내담자 분포"}).
		AddSeries("Volatility BoxPlot", []opts.BoxPlotData{{Value: []float64{minV, q1, median, q3, maxV}}})

	scatter := charts.NewScatter()
	scatter.SetGlobalOptions(
		charts.WithXAxisOpts(opts.XAxis{Type: "category"}),
		charts.WithYAxisOpts(opts.YAxis{Type: "value"}),
	)
	scatter.SetXAxis([]string{"전체 내담자 분포"}).
		AddSeries("대상 내담자(보정 변동성)", []opts.ScatterData{{
			Name:       targetFileCode,
			Value:      adjustedVol,
			Symbol:     "diamond",
			SymbolSize: 20,
		}}, charts.WithItemStyleOpts(opts.ItemStyle{Color: "#D62728"}))

	box.Overlap(scatter)

	var rendered bytes.Buffer
	if err := box.Render(&rendered); err != nil {
		log.Fatalf("chart render failed: %v", err)
	}

	styledHTML := injectFontCSS(rendered.String())
	if err := os.WriteFile(outputHTML, []byte(styledHTML), 0o644); err != nil {
		log.Fatalf("failed to write html: %v", err)
	}

	fmt.Printf("    완료: 상대 비교 분포도 생성 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("Done. Output written to %s\n", outputHTML)
	fmt.Printf("Summary | target=%s sessions=%d patients=%d total_elapsed=%s\n", targetFileCode, sessionCount, len(volatilityByFile), time.Since(jobStart).Round(100*time.Millisecond))
}
