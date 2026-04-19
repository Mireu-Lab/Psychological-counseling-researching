package main

import (
	"bytes"
	"context"
	"encoding/json"
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
	"gonum.org/v1/gonum/stat"
	"google.golang.org/api/option"
)

const (
	projectID   = "testprojects-453622"
	datasetID   = "Psychological_counseling_data"
	credPath    = "/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json"
	fontPath    = "/workspaces/Psychological-counseling-researching/.fonts/NotoSansKR-Bold.ttf"
	outputHTML  = "multi_psychological_signature_vector.html"
	outputJSON  = "session12_signature_vectors.json"
	totalStages = 3
)

type JoinedEmbeddingRow struct {
	FileCode      string
	SessionNo     int64
	TimelineIndex int64
	KMEmbedding   []float64
	GemEmbedding  []float64
}

type SignatureVector struct {
	FileCode              string  `json:"file_code"`
	SwampDensity          float64 `json:"swamp_density"`
	LandscapeVolatility   float64 `json:"landscape_volatility"`
	SpikeFrequency        float64 `json:"spike_frequency"`
	DissonanceScore       float64 `json:"dissonance_score"`
	NormSwampDensity      float64 `json:"norm_swamp_density"`
	NormLandscapeVol      float64 `json:"norm_landscape_volatility"`
	NormSpikeFrequency    float64 `json:"norm_spike_frequency"`
	NormDissonanceScore   float64 `json:"norm_dissonance_score"`
	TurnCount             int     `json:"turn_count"`
	DistanceCount         int     `json:"distance_count"`
	SpikeCount            int     `json:"spike_count"`
	LandscapeSpikeCutoff  float64 `json:"landscape_spike_cutoff"`
	LandscapeMean         float64 `json:"landscape_mean"`
	LandscapeStd          float64 `json:"landscape_std"`
	CorrelationSampleSize int     `json:"correlation_sample_size"`
}

type ProgressBar struct {
	Prefix string
	Total  int
	Width  int
	Start  time.Time
	last   time.Time
}

func newProgressBar(prefix string, total int) *ProgressBar {
	return &ProgressBar{Prefix: prefix, Total: total, Width: 28, Start: time.Now()}
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

	var dot float64
	var normA float64
	var normB float64
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

		count := i - start + 1
		sum := 0.0
		for j := start; j <= i; j++ {
			sum += values[j]
		}
		out[i] = sum / float64(count)
	}

	return out
}

func meanStd(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	if len(values) == 1 {
		return values[0], 0
	}
	return stat.Mean(values, nil), stat.StdDev(values, nil)
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func minMaxScale(v, minV, maxV float64) float64 {
	if maxV-minV == 0 {
		return 0.5
	}
	return (v - minV) / (maxV - minV)
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

func computeSignature(fileCode string, rows []JoinedEmbeddingRow) (SignatureVector, bool) {
	if len(rows) < 2 {
		return SignatureVector{}, false
	}

	kmDist := make([]float64, 0, len(rows)-1)
	gemDist := make([]float64, 0, len(rows)-1)
	for i := 0; i < len(rows)-1; i++ {
		kmDist = append(kmDist, cosineDistance(rows[i].KMEmbedding, rows[i+1].KMEmbedding))
		gemDist = append(gemDist, cosineDistance(rows[i].GemEmbedding, rows[i+1].GemEmbedding))
	}

	swampIdx := movingAverage(kmDist, 5)
	for i := range swampIdx {
		swampIdx[i] = clamp01(1.0 - swampIdx[i])
	}
	landscapeIdx := movingAverage(gemDist, 3)

	if len(swampIdx) < 2 || len(landscapeIdx) < 2 {
		return SignatureVector{}, false
	}

	meanLand, stdLand := meanStd(landscapeIdx)
	spikeThreshold := meanLand + (2.0 * stdLand)
	spikeCount := 0
	for _, v := range landscapeIdx {
		if v > spikeThreshold {
			spikeCount++
		}
	}

	dissonance := stat.Correlation(swampIdx, landscapeIdx, nil)
	if math.IsNaN(dissonance) || math.IsInf(dissonance, 0) {
		dissonance = 0
	}

	return SignatureVector{
		FileCode:              fileCode,
		SwampDensity:          stat.Mean(swampIdx, nil),
		LandscapeVolatility:   stat.StdDev(landscapeIdx, nil),
		SpikeFrequency:        float64(spikeCount) / float64(len(landscapeIdx)),
		DissonanceScore:       dissonance,
		TurnCount:             len(rows),
		DistanceCount:         len(kmDist),
		SpikeCount:            spikeCount,
		LandscapeSpikeCutoff:  spikeThreshold,
		LandscapeMean:         meanLand,
		LandscapeStd:          stdLand,
		CorrelationSampleSize: len(swampIdx),
	}, true
}

func loadJoinedEmbeddingsByStorageAPI(
	ctx context.Context,
	bqClient *bigquery.Client,
	storageClient *storage.BigQueryReadClient,
	speaker string,
) ([]JoinedEmbeddingRow, error) {
	query := `
		SELECT
		  k.file_code,
		  k.session_no,
		  k.timeline_index,
		  k.content_embedding AS kmbert_embedding,
		  g.embedding AS gemini_embedding
		FROM ` + "`testprojects-453622.Psychological_counseling_data.morpheme_classification_kmbert_embedding`" + ` AS k
		JOIN ` + "`testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding`" + ` AS g
		  ON k.file_code = g.file_code
		 AND k.session_no = g.session_no
		 AND k.timeline_index = g.timeline_index
		 AND k.speaker = g.speaker
		WHERE k.speaker = @speaker
		ORDER BY k.file_code, k.session_no, k.timeline_index ASC
	`

	tempTable, cleanup, err := runSinkQueryToTempTable(
		ctx,
		bqClient,
		"temp_step12_join",
		query,
		[]bigquery.QueryParameter{{Name: "speaker", Value: speaker}},
	)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	nativeRows, err := readAvroNativeRows(ctx, storageClient, tempTable)
	if err != nil {
		return nil, err
	}

	joinedRows := make([]JoinedEmbeddingRow, 0, len(nativeRows))
	decodeBar := newProgressBar("decode-joined", len(nativeRows))
	for _, native := range nativeRows {
		joinedRows = append(joinedRows, JoinedEmbeddingRow{
			FileCode:      extractAvroString(native["file_code"]),
			SessionNo:     extractAvroInt64(native["session_no"]),
			TimelineIndex: extractAvroInt64(native["timeline_index"]),
			KMEmbedding:   extractAvroFloatArray(native["kmbert_embedding"]),
			GemEmbedding:  extractAvroFloatArray(native["gemini_embedding"]),
		})
		decodeBar.Render(len(joinedRows), "Avro -> JoinedEmbeddingRow")
	}

	sort.Slice(joinedRows, func(i, j int) bool {
		if joinedRows[i].FileCode != joinedRows[j].FileCode {
			return joinedRows[i].FileCode < joinedRows[j].FileCode
		}
		if joinedRows[i].SessionNo != joinedRows[j].SessionNo {
			return joinedRows[i].SessionNo < joinedRows[j].SessionNo
		}
		return joinedRows[i].TimelineIndex < joinedRows[j].TimelineIndex
	})

	return joinedRows, nil
}

func main() {
	jobStart := time.Now()
	speaker := getenvOrDefault("SPEAKER", "내담자")
	minTurns := int(getenvIntOrDefault("MIN_TURNS", 20))
	if minTurns < 2 {
		minTurns = 2
	}

	fmt.Printf("Run Config | speaker=%s min_turns=%d\n", speaker, minTurns)

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

	fmt.Println("\n[1/3] 내담자별 시계열 통계량 집계")
	stageStart := time.Now()

	joinedRows, err := loadJoinedEmbeddingsByStorageAPI(ctx, bqClient, storageClient, speaker)
	if err != nil {
		log.Fatalf("loadJoinedEmbeddingsByStorageAPI(step12): %v", err)
	}

	rowsByFile := make(map[string][]JoinedEmbeddingRow)
	groupBar := newProgressBar("group-file", len(joinedRows))
	for i, row := range joinedRows {
		rowsByFile[row.FileCode] = append(rowsByFile[row.FileCode], row)
		groupBar.Render(i+1, "file_code 그룹화")
	}

	signatures := make([]SignatureVector, 0, len(rowsByFile))
	featureBar := newProgressBar("feature", len(rowsByFile))
	featureDone := 0
	for fileCode, rows := range rowsByFile {
		if len(rows) >= minTurns {
			if vec, ok := computeSignature(fileCode, rows); ok {
				signatures = append(signatures, vec)
			}
		}
		featureDone++
		featureBar.Render(featureDone, "4차원 피처 계산")
	}

	if len(signatures) == 0 {
		log.Fatalf("no signatures generated (file_count=%d, min_turns=%d)", len(rowsByFile), minTurns)
	}

	sort.Slice(signatures, func(i, j int) bool { return signatures[i].FileCode < signatures[j].FileCode })
	fmt.Printf("    완료: 내담자별 시계열 통계량 집계 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("    전체 행=%d, file_code=%d, 유효 벡터=%d\n", len(joinedRows), len(rowsByFile), len(signatures))

	fmt.Println("\n[2/3] 피처 정규화(Min-Max Scaling)")
	stageStart = time.Now()

	minSwamp, maxSwamp := signatures[0].SwampDensity, signatures[0].SwampDensity
	minLand, maxLand := signatures[0].LandscapeVolatility, signatures[0].LandscapeVolatility
	minSpike, maxSpike := signatures[0].SpikeFrequency, signatures[0].SpikeFrequency
	minDis, maxDis := signatures[0].DissonanceScore, signatures[0].DissonanceScore

	for _, s := range signatures[1:] {
		if s.SwampDensity < minSwamp {
			minSwamp = s.SwampDensity
		}
		if s.SwampDensity > maxSwamp {
			maxSwamp = s.SwampDensity
		}
		if s.LandscapeVolatility < minLand {
			minLand = s.LandscapeVolatility
		}
		if s.LandscapeVolatility > maxLand {
			maxLand = s.LandscapeVolatility
		}
		if s.SpikeFrequency < minSpike {
			minSpike = s.SpikeFrequency
		}
		if s.SpikeFrequency > maxSpike {
			maxSpike = s.SpikeFrequency
		}
		if s.DissonanceScore < minDis {
			minDis = s.DissonanceScore
		}
		if s.DissonanceScore > maxDis {
			maxDis = s.DissonanceScore
		}
	}

	normBar := newProgressBar("normalize", len(signatures))
	for i := range signatures {
		signatures[i].NormSwampDensity = clamp01(minMaxScale(signatures[i].SwampDensity, minSwamp, maxSwamp))
		signatures[i].NormLandscapeVol = clamp01(minMaxScale(signatures[i].LandscapeVolatility, minLand, maxLand))
		signatures[i].NormSpikeFrequency = clamp01(minMaxScale(signatures[i].SpikeFrequency, minSpike, maxSpike))
		signatures[i].NormDissonanceScore = clamp01(minMaxScale(signatures[i].DissonanceScore, minDis, maxDis))
		normBar.Render(i+1, "Min-Max 스케일링")
	}

	fmt.Printf("    완료: 피처 정규화(Min-Max Scaling) (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("    범위: swamp=[%.4f, %.4f], landscape=[%.4f, %.4f], spike=[%.4f, %.4f], dissonance=[%.4f, %.4f]\n",
		minSwamp, maxSwamp, minLand, maxLand, minSpike, maxSpike, minDis, maxDis)

	fmt.Println("\n[3/3] 내담자별 4차원 피처 벡터 생성")
	stageStart = time.Now()

	jsonBytes, err := json.MarshalIndent(signatures, "", "  ")
	if err != nil {
		log.Fatalf("json marshal failed: %v", err)
	}
	if err := os.WriteFile(outputJSON, jsonBytes, 0o644); err != nil {
		log.Fatalf("failed to write %s: %v", outputJSON, err)
	}

	scatter := charts.NewScatter()
	scatter.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: "shine", Width: "100vw", Height: "78vh"}),
		charts.WithTitleOpts(opts.Title{
			Title:    "Step 12 | 심리 지형 4차원 시그니처 벡터",
			Subtitle: "X=Swamp Density, Y=Landscape Volatility, Size=Spike Frequency, Group=Dissonance",
		}),
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true)}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true), Trigger: "item"}),
		charts.WithXAxisOpts(opts.XAxis{Name: "Normalized Swamp Density", Type: "value", Min: 0, Max: 1}),
		charts.WithYAxisOpts(opts.YAxis{Name: "Normalized Landscape Volatility", Type: "value", Min: 0, Max: 1}),
		charts.WithDataZoomOpts(
			opts.DataZoom{Type: "inside", XAxisIndex: []int{0}, YAxisIndex: []int{0}, FilterMode: "none"},
			opts.DataZoom{Type: "slider", XAxisIndex: []int{0}, FilterMode: "none"},
		),
		charts.WithToolboxOpts(opts.Toolbox{
			Show: opts.Bool(true),
			Feature: &opts.ToolBoxFeature{
				Restore:     &opts.ToolBoxFeatureRestore{Show: opts.Bool(true), Title: "초기화"},
				SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{Show: opts.Bool(true), Name: "multi_psychological_signature_vector"},
			},
		}),
	)

	scatter.SetXAxis([]float64{})

	negativeSeries := make([]opts.ScatterData, 0)
	neutralSeries := make([]opts.ScatterData, 0)
	positiveSeries := make([]opts.ScatterData, 0)

	seriesBar := newProgressBar("series", len(signatures))
	for i := range signatures {
		s := signatures[i]
		point := opts.ScatterData{
			Name: fmt.Sprintf("%s | raw=[%.4f, %.4f, %.4f, %.4f] norm=[%.4f, %.4f, %.4f, %.4f]",
				s.FileCode,
				s.SwampDensity,
				s.LandscapeVolatility,
				s.SpikeFrequency,
				s.DissonanceScore,
				s.NormSwampDensity,
				s.NormLandscapeVol,
				s.NormSpikeFrequency,
				s.NormDissonanceScore,
			),
			Value:      []float64{s.NormSwampDensity, s.NormLandscapeVol},
			Symbol:     "circle",
			SymbolSize: int(math.Round(8 + (14 * s.NormSpikeFrequency))),
		}

		if s.DissonanceScore <= -0.2 {
			negativeSeries = append(negativeSeries, point)
		} else if s.DissonanceScore >= 0.2 {
			positiveSeries = append(positiveSeries, point)
		} else {
			neutralSeries = append(neutralSeries, point)
		}
		seriesBar.Render(i+1, "시각화 포인트 생성")
	}

	scatter.AddSeries("Dissonance 음의 상관", negativeSeries, charts.WithItemStyleOpts(opts.ItemStyle{Color: "#2f7f5f"}))
	scatter.AddSeries("Dissonance 중립", neutralSeries, charts.WithItemStyleOpts(opts.ItemStyle{Color: "#8c8c8c"}))
	scatter.AddSeries("Dissonance 양의 상관", positiveSeries, charts.WithItemStyleOpts(opts.ItemStyle{Color: "#d9480f"}))

	var rendered bytes.Buffer
	if err := scatter.Render(&rendered); err != nil {
		log.Fatalf("chart render failed: %v", err)
	}

	styledHTML := injectFontCSS(rendered.String())
	if err := os.WriteFile(outputHTML, []byte(styledHTML), 0o644); err != nil {
		log.Fatalf("failed to write html: %v", err)
	}

	fmt.Printf("    완료: 내담자별 4차원 피처 벡터 생성 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("Done. Outputs written to %s, %s\n", outputHTML, outputJSON)
	fmt.Printf("Summary | vectors=%d total_elapsed=%s\n", len(signatures), time.Since(jobStart).Round(100*time.Millisecond))
}
