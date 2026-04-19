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
	outputHTML  = "multi_kmbert_cluster_scatter.html"
	totalStages = 4
)

type EmbeddingRow struct {
	FileCode      string    `bigquery:"file_code"`
	TimelineIndex int64     `bigquery:"timeline_index"`
	Embedding     []float64 `bigquery:"embedding"`
}

type ReliabilityMeta struct {
	FileCode        string `bigquery:"file_code"`
	SpeakerRowCount int64  `bigquery:"speaker_row_count"`
	SessionNo       int64  `bigquery:"session_no"`
}

type PatientFeature struct {
	FileCode         string
	Volatility       float64
	Affect           float64
	MeanDistance     float64
	StdDistance      float64
	SpeakerRowCount  int64
	SessionNo        int64
	SampleCount      int
	Cluster          int
	OutlierDistance  float64
	ReliabilityLabel string
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

func euclidean(a, b [2]float64) float64 {
	dx := a[0] - b[0]
	dy := a[1] - b[1]
	return math.Sqrt(dx*dx + dy*dy)
}

func kmeans2D(points [][2]float64, k int, maxIter int) ([]int, [][2]float64) {
	n := len(points)
	if n == 0 || k <= 0 {
		return nil, nil
	}
	if k > n {
		k = n
	}

	centroids := make([][2]float64, k)
	for i := 0; i < k; i++ {
		centroids[i] = points[i]
	}

	assign := make([]int, n)
	for i := range assign {
		assign[i] = -1
	}

	for iter := 0; iter < maxIter; iter++ {
		changed := false

		for i, p := range points {
			best := 0
			bestDist := euclidean(p, centroids[0])
			for c := 1; c < k; c++ {
				d := euclidean(p, centroids[c])
				if d < bestDist {
					bestDist = d
					best = c
				}
			}
			if assign[i] != best {
				assign[i] = best
				changed = true
			}
		}

		sums := make([][2]float64, k)
		counts := make([]int, k)
		for i, cid := range assign {
			sums[cid][0] += points[i][0]
			sums[cid][1] += points[i][1]
			counts[cid]++
		}

		for c := 0; c < k; c++ {
			if counts[c] == 0 {
				continue
			}
			centroids[c][0] = sums[c][0] / float64(counts[c])
			centroids[c][1] = sums[c][1] / float64(counts[c])
		}

		if !changed {
			break
		}
	}

	return assign, centroids
}

func reliabilityLabel(speakerRows int64, sessionNo int64) string {
	rowScore := math.Min(1.0, float64(speakerRows)/200.0)
	sessionScore := math.Min(1.0, float64(sessionNo)/10.0)
	score := (rowScore + sessionScore) / 2.0

	if score >= 0.67 {
		return "상"
	}
	if score >= 0.40 {
		return "중"
	}
	return "하"
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
	kClusters := int(getenvIntOrDefault("K_CLUSTERS", 3))
	if kClusters < 1 {
		kClusters = 1
	}

	fmt.Printf("Run Config | file_code=%s speaker=%s k_clusters=%d\n", targetFileCode, speaker, kClusters)

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

	fmt.Println("\n[1/4] 군집화 중심점 계산")
	stageStart := time.Now()

	queryEmbeddings := `
		SELECT file_code, timeline_index, content_embedding AS embedding
		FROM ` + "`testprojects-453622.Psychological_counseling_data.morpheme_classification_kmbert_embedding`" + `
		WHERE speaker = @speaker
		ORDER BY file_code, timeline_index ASC
	`

	tempEmbeddingsTable, cleanupEmbeddings, err := runSinkQueryToTempTable(
		ctx,
		client,
		"temp_step11_embeddings",
		queryEmbeddings,
		[]bigquery.QueryParameter{{Name: "speaker", Value: speaker}},
	)
	if err != nil {
		log.Fatalf("runSinkQueryToTempTable(kmbert embeddings): %v", err)
	}
	defer cleanupEmbeddings()

	embeddingNativeRows, err := readAvroNativeRows(ctx, storageClient, tempEmbeddingsTable)
	if err != nil {
		log.Fatalf("readAvroNativeRows(kmbert embeddings): %v", err)
	}

	embeddingRows := make([]EmbeddingRow, 0, len(embeddingNativeRows))
	decodeBar := newProgressBar("decode-kmbert", len(embeddingNativeRows))
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

	queryMeta := `
		SELECT file_code, MAX(speaker_row_count) AS speaker_row_count, MAX(session_no) AS session_no
		FROM ` + "`testprojects-453622.Psychological_counseling_data.processed_data`" + `
		WHERE speaker = @speaker
		GROUP BY file_code
	`

	tempMetaTable, cleanupMeta, err := runSinkQueryToTempTable(
		ctx,
		client,
		"temp_step11_meta",
		queryMeta,
		[]bigquery.QueryParameter{{Name: "speaker", Value: speaker}},
	)
	if err != nil {
		log.Fatalf("runSinkQueryToTempTable(reliability meta): %v", err)
	}
	defer cleanupMeta()

	metaNativeRows, err := readAvroNativeRows(ctx, storageClient, tempMetaTable)
	if err != nil {
		log.Fatalf("readAvroNativeRows(reliability meta): %v", err)
	}

	metaByFile := make(map[string]ReliabilityMeta)
	metaBar := newProgressBar("meta-map", len(metaNativeRows))
	metaCurrent := 0
	for _, native := range metaNativeRows {
		row := ReliabilityMeta{
			FileCode:        extractAvroString(native["file_code"]),
			SpeakerRowCount: extractAvroInt64(native["speaker_row_count"]),
			SessionNo:       extractAvroInt64(native["session_no"]),
		}
		metaByFile[row.FileCode] = row
		metaCurrent++
		metaBar.Render(metaCurrent, "신뢰도 메타 매핑")
	}

	features := make([]PatientFeature, 0, len(vectorsByFile))
	featureBar := newProgressBar("feature", len(vectorsByFile))
	featureCurrent := 0
	for fileCode, vecs := range vectorsByFile {
		if len(vecs) < 2 {
			featureCurrent++
			featureBar.Render(featureCurrent, "환자별 특징량 계산")
			continue
		}
		dists := make([]float64, 0, len(vecs)-1)
		for i := 0; i < len(vecs)-1; i++ {
			dists = append(dists, cosineDistance(vecs[i], vecs[i+1]))
		}
		meanDist, stdDist := meanStd(dists)
		affect := 1.0 - meanDist
		if affect < 0 {
			affect = 0
		}
		if affect > 1 {
			affect = 1
		}

		meta := metaByFile[fileCode]
		features = append(features, PatientFeature{
			FileCode:        fileCode,
			Volatility:      stdDist,
			Affect:          affect,
			MeanDistance:    meanDist,
			StdDistance:     stdDist,
			SpeakerRowCount: meta.SpeakerRowCount,
			SessionNo:       meta.SessionNo,
			SampleCount:     len(dists),
		})
		featureCurrent++
		featureBar.Render(featureCurrent, "환자별 특징량 계산")
	}

	if len(features) < 2 {
		log.Fatalf("not enough patients for clustering: %d", len(features))
	}

	points := make([][2]float64, 0, len(features))
	for _, f := range features {
		points = append(points, [2]float64{f.Volatility, f.Affect})
	}

	assign, centroids := kmeans2D(points, kClusters, 30)
	for i := range features {
		features[i].Cluster = assign[i]
	}

	fmt.Printf("    완료: 군집화 중심점 계산 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("    수집 행 수=%d, 환자 수=%d, centroid 수=%d\n", rowCount, len(features), len(centroids))

	fmt.Println("\n[2/4] 이상치 거리 산출")
	stageStart = time.Now()

	clusterSizes := make(map[int]int)
	clusterBar := newProgressBar("cluster-size", len(features))
	clusterCurrent := 0
	for _, f := range features {
		clusterSizes[f.Cluster]++
		clusterCurrent++
		clusterBar.Render(clusterCurrent, "클러스터 분포 집계")
	}

	normalCluster := -1
	maxSize := -1
	for cid, size := range clusterSizes {
		if size > maxSize {
			maxSize = size
			normalCluster = cid
		}
	}
	if normalCluster < 0 {
		log.Fatalf("failed to determine normal cluster")
	}

	normalCentroid := centroids[normalCluster]
	outlierBar := newProgressBar("outlier", len(features))
	outlierCurrent := 0
	for i := range features {
		features[i].OutlierDistance = euclidean(
			[2]float64{features[i].Volatility, features[i].Affect},
			normalCentroid,
		)
		outlierCurrent++
		outlierBar.Render(outlierCurrent, "정상군 중심 이탈거리 계산")
	}

	targetOutlier := 0.0
	for _, f := range features {
		if f.FileCode == targetFileCode {
			targetOutlier = f.OutlierDistance
			break
		}
	}

	fmt.Printf("    완료: 이상치 거리 산출 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("    정상군 클러스터=%d(size=%d), 대상 이탈거리=%.6f\n", normalCluster, maxSize, targetOutlier)

	fmt.Println("\n[3/4] 신뢰도 라벨링")
	stageStart = time.Now()

	reliabilityCounts := map[string]int{"상": 0, "중": 0, "하": 0}
	labelBar := newProgressBar("label", len(features))
	labelCurrent := 0
	for i := range features {
		if features[i].SpeakerRowCount <= 0 {
			features[i].SpeakerRowCount = int64(features[i].SampleCount)
		}
		if features[i].SessionNo <= 0 {
			features[i].SessionNo = 1
		}
		features[i].ReliabilityLabel = reliabilityLabel(features[i].SpeakerRowCount, features[i].SessionNo)
		reliabilityCounts[features[i].ReliabilityLabel]++
		labelCurrent++
		labelBar.Render(labelCurrent, "신뢰도 라벨링")
	}

	fmt.Printf("    완료: 신뢰도 라벨링 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("    라벨 분포: 상=%d 중=%d 하=%d\n", reliabilityCounts["상"], reliabilityCounts["중"], reliabilityCounts["하"])

	fmt.Println("\n[4/4] 다차원 분포 시각화")
	stageStart = time.Now()

	sort.Slice(features, func(i, j int) bool {
		if features[i].Volatility == features[j].Volatility {
			return features[i].FileCode < features[j].FileCode
		}
		return features[i].Volatility < features[j].Volatility
	})

	scatter := charts.NewScatter()
	scatter.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: "shine", Width: "100vw", Height: "78vh"}),
		charts.WithTitleOpts(opts.Title{
			Title:    "다중 내담자 KM-BERT 군집 분포 (Volatility vs Affect)",
			Subtitle: fmt.Sprintf("Normal Cluster=%d | Target=%s Outlier Distance=%.4f", normalCluster, targetFileCode, targetOutlier),
		}),
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true)}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true), Trigger: "item"}),
		charts.WithXAxisOpts(opts.XAxis{Name: "감정 기복 (Volatility, Std Distance)", Type: "value"}),
		charts.WithYAxisOpts(opts.YAxis{Name: "정서가 (Affect, 1-Mean Distance)", Type: "value", Min: 0, Max: 1}),
		charts.WithDataZoomOpts(
			opts.DataZoom{Type: "inside", XAxisIndex: []int{0}, YAxisIndex: []int{0}, FilterMode: "none"},
			opts.DataZoom{Type: "slider", XAxisIndex: []int{0}, FilterMode: "none"},
		),
		charts.WithToolboxOpts(opts.Toolbox{
			Show: opts.Bool(true),
			Feature: &opts.ToolBoxFeature{
				Restore:     &opts.ToolBoxFeatureRestore{Show: opts.Bool(true), Title: "초기화"},
				SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{Show: opts.Bool(true), Name: "multi_kmbert_cluster_scatter"},
			},
		}),
	)

	scatter.SetXAxis([]float64{})

	highSeries := make([]opts.ScatterData, 0)
	midSeries := make([]opts.ScatterData, 0)
	lowSeries := make([]opts.ScatterData, 0)
	targetSeries := make([]opts.ScatterData, 0)

	for _, f := range features {
		point := opts.ScatterData{
			Name:       fmt.Sprintf("%s | cluster=%d | outlier=%.4f", f.FileCode, f.Cluster, f.OutlierDistance),
			Value:      []float64{f.Volatility, f.Affect},
			Symbol:     "circle",
			SymbolSize: 10,
		}

		switch f.ReliabilityLabel {
		case "상":
			highSeries = append(highSeries, point)
		case "중":
			midSeries = append(midSeries, point)
		default:
			lowSeries = append(lowSeries, point)
		}

		if f.FileCode == targetFileCode {
			targetSeries = append(targetSeries, opts.ScatterData{
				Name:       fmt.Sprintf("%s (Target) | outlier=%.4f", f.FileCode, f.OutlierDistance),
				Value:      []float64{f.Volatility, f.Affect},
				Symbol:     "diamond",
				SymbolSize: 20,
			})
		}
	}

	seriesBar := newProgressBar("series", len(features))
	seriesBar.Render(len(features), "시각화 시리즈 준비")

	scatter.AddSeries("신뢰도 상", highSeries, charts.WithItemStyleOpts(opts.ItemStyle{Color: "#1f77b4"}))
	scatter.AddSeries("신뢰도 중", midSeries, charts.WithItemStyleOpts(opts.ItemStyle{Color: "#ff7f0e"}))
	scatter.AddSeries("신뢰도 하", lowSeries, charts.WithItemStyleOpts(opts.ItemStyle{Color: "#7f7f7f"}))
	if len(targetSeries) > 0 {
		scatter.AddSeries("분석 대상", targetSeries, charts.WithItemStyleOpts(opts.ItemStyle{Color: "#d62728"}))
	}

	var rendered bytes.Buffer
	if err := scatter.Render(&rendered); err != nil {
		log.Fatalf("chart render failed: %v", err)
	}

	styledHTML := injectFontCSS(rendered.String())
	if err := os.WriteFile(outputHTML, []byte(styledHTML), 0o644); err != nil {
		log.Fatalf("failed to write html: %v", err)
	}

	fmt.Printf("    완료: 다차원 분포 시각화 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("Done. Output written to %s\n", outputHTML)
	fmt.Printf("Summary | target=%s patients=%d total_elapsed=%s\n", targetFileCode, len(features), time.Since(jobStart).Round(100*time.Millisecond))
}
