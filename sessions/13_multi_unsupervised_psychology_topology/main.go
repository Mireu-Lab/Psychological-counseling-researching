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
	"gonum.org/v1/gonum/mat"
	"gonum.org/v1/gonum/stat"
	"google.golang.org/api/option"
)

const (
	projectID   = "testprojects-453622"
	datasetID   = "Psychological_counseling_data"
	credPath    = "/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json"
	fontPath    = "/workspaces/Psychological-counseling-researching/.fonts/NotoSansKR-Bold.ttf"
	outputHTML  = "multi_unsupervised_psychology_topology.html"
	outputJSON  = "session13_cluster_result.json"
	totalStages = 4
)

type JoinedEmbeddingRow struct {
	FileCode      string
	SessionNo     int64
	TimelineIndex int64
	KMEmbedding   []float64
	GemEmbedding  []float64
}

type SignatureVector struct {
	FileCode            string  `json:"file_code"`
	SwampDensity        float64 `json:"swamp_density"`
	LandscapeVolatility float64 `json:"landscape_volatility"`
	SpikeFrequency      float64 `json:"spike_frequency"`
	DissonanceScore     float64 `json:"dissonance_score"`
	NormSwampDensity    float64 `json:"norm_swamp_density"`
	NormLandscapeVol    float64 `json:"norm_landscape_volatility"`
	NormSpikeFrequency  float64 `json:"norm_spike_frequency"`
	NormDissonance      float64 `json:"norm_dissonance_score"`
	TurnCount           int     `json:"turn_count"`
}

type ClusteredPoint struct {
	FileCode            string  `json:"file_code"`
	ClusterID           int     `json:"cluster_id"`
	ClusterLabel        string  `json:"cluster_label"`
	NormSwampDensity    float64 `json:"norm_swamp_density"`
	NormLandscapeVol    float64 `json:"norm_landscape_volatility"`
	NormSpikeFrequency  float64 `json:"norm_spike_frequency"`
	NormDissonance      float64 `json:"norm_dissonance_score"`
	PCA1                float64 `json:"pca1"`
	PCA2                float64 `json:"pca2"`
	DistanceToCentroid  float64 `json:"distance_to_centroid"`
	RawSwampDensity     float64 `json:"raw_swamp_density"`
	RawLandscapeVol     float64 `json:"raw_landscape_volatility"`
	RawSpikeFrequency   float64 `json:"raw_spike_frequency"`
	RawDissonance       float64 `json:"raw_dissonance_score"`
	TrajectoryTurnCount int     `json:"trajectory_turn_count"`
}

type ClusterCentroid struct {
	ClusterID        int     `json:"cluster_id"`
	Size             int     `json:"size"`
	NormSwampDensity float64 `json:"norm_swamp_density"`
	NormLandscapeVol float64 `json:"norm_landscape_volatility"`
	NormSpikeFreq    float64 `json:"norm_spike_frequency"`
	NormDissonance   float64 `json:"norm_dissonance_score"`
	PCA1             float64 `json:"pca1"`
	PCA2             float64 `json:"pca2"`
	Label            string  `json:"label"`
}

type ClusterResult struct {
	SelectedK        int                        `json:"selected_k"`
	SSEByK           map[int]float64            `json:"sse_by_k"`
	PCAEvalues       []float64                  `json:"pca_eigen_values"`
	Centroids        []ClusterCentroid          `json:"centroids"`
	Samples          []ClusteredPoint           `json:"samples"`
	ClusterLabelInfo map[int]string             `json:"cluster_label_info"`
	GeneratedAt      string                     `json:"generated_at"`
	Metadata         map[string]json.RawMessage `json:"metadata"`
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
		FileCode:            fileCode,
		SwampDensity:        stat.Mean(swampIdx, nil),
		LandscapeVolatility: stat.StdDev(landscapeIdx, nil),
		SpikeFrequency:      float64(spikeCount) / float64(len(landscapeIdx)),
		DissonanceScore:     dissonance,
		TurnCount:           len(rows),
	}, true
}

func squaredEuclidean(a, b []float64) float64 {
	if len(a) != len(b) {
		return math.Inf(1)
	}
	sum := 0.0
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

func kmeans(data [][]float64, k int, maxIter int) ([]int, [][]float64, float64) {
	n := len(data)
	if n == 0 || k <= 0 {
		return nil, nil, 0
	}
	if k > n {
		k = n
	}

	dim := len(data[0])
	centroids := make([][]float64, k)
	for i := 0; i < k; i++ {
		centroids[i] = make([]float64, dim)
		copy(centroids[i], data[i%n])
	}

	assign := make([]int, n)
	for i := range assign {
		assign[i] = -1
	}

	for iter := 0; iter < maxIter; iter++ {
		changed := false

		for i := 0; i < n; i++ {
			best := 0
			bestDist := squaredEuclidean(data[i], centroids[0])
			for c := 1; c < k; c++ {
				d := squaredEuclidean(data[i], centroids[c])
				if d < bestDist {
					best = c
					bestDist = d
				}
			}
			if assign[i] != best {
				assign[i] = best
				changed = true
			}
		}

		sums := make([][]float64, k)
		counts := make([]int, k)
		for c := 0; c < k; c++ {
			sums[c] = make([]float64, dim)
		}
		for i := 0; i < n; i++ {
			cid := assign[i]
			counts[cid]++
			for d := 0; d < dim; d++ {
				sums[cid][d] += data[i][d]
			}
		}

		for c := 0; c < k; c++ {
			if counts[c] == 0 {
				copy(centroids[c], data[c%n])
				continue
			}
			for d := 0; d < dim; d++ {
				centroids[c][d] = sums[c][d] / float64(counts[c])
			}
		}

		if !changed {
			break
		}
	}

	sse := 0.0
	for i := 0; i < n; i++ {
		sse += squaredEuclidean(data[i], centroids[assign[i]])
	}
	return assign, centroids, sse
}

func chooseKByElbow(sseByK map[int]float64, kMin int, kMax int) int {
	if len(sseByK) == 0 {
		return kMin
	}
	if kMax-kMin < 2 {
		return kMin
	}

	bestK := kMin
	bestScore := math.Inf(-1)
	for k := kMin + 1; k <= kMax-1; k++ {
		prev, ok1 := sseByK[k-1]
		curr, ok2 := sseByK[k]
		next, ok3 := sseByK[k+1]
		if !ok1 || !ok2 || !ok3 {
			continue
		}
		score := (prev - curr) - (curr - next)
		if score > bestScore {
			bestScore = score
			bestK = k
		}
	}

	if bestScore <= 0 {
		return kMin
	}
	return bestK
}

func pca2D(data [][]float64) ([][2]float64, []float64, error) {
	n := len(data)
	if n == 0 {
		return nil, nil, fmt.Errorf("empty data")
	}
	d := len(data[0])
	if d < 2 {
		return nil, nil, fmt.Errorf("dimension must be >= 2")
	}

	means := make([]float64, d)
	for j := 0; j < d; j++ {
		col := make([]float64, n)
		for i := 0; i < n; i++ {
			col[i] = data[i][j]
		}
		means[j] = stat.Mean(col, nil)
	}

	centered := mat.NewDense(n, d, nil)
	for i := 0; i < n; i++ {
		for j := 0; j < d; j++ {
			centered.Set(i, j, data[i][j]-means[j])
		}
	}

	cov := mat.NewSymDense(d, nil)
	for i := 0; i < d; i++ {
		for j := i; j < d; j++ {
			sum := 0.0
			for r := 0; r < n; r++ {
				sum += centered.At(r, i) * centered.At(r, j)
			}
			denom := float64(n - 1)
			if n <= 1 {
				denom = 1
			}
			cov.SetSym(i, j, sum/denom)
		}
	}

	var eig mat.EigenSym
	ok := eig.Factorize(cov, true)
	if !ok {
		return nil, nil, fmt.Errorf("eigen factorization failed")
	}

	vals := eig.Values(nil)
	vecs := mat.NewDense(d, d, nil)
	eig.VectorsTo(vecs)

	indices := make([]int, d)
	for i := 0; i < d; i++ {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return vals[indices[i]] > vals[indices[j]]
	})

	pc1 := indices[0]
	pc2 := indices[1]

	coords := make([][2]float64, n)
	for i := 0; i < n; i++ {
		x := 0.0
		y := 0.0
		for j := 0; j < d; j++ {
			v := centered.At(i, j)
			x += v * vecs.At(j, pc1)
			y += v * vecs.At(j, pc2)
		}
		coords[i] = [2]float64{x, y}
	}

	return coords, []float64{vals[pc1], vals[pc2]}, nil
}

func clusterSemanticLabel(swamp, landscape float64) string {
	if swamp >= 0.60 && landscape < 0.40 {
		return "Cluster A (우울/반추 중심군 매핑)"
	}
	if swamp < 0.40 && landscape >= 0.60 {
		return "Cluster B (양극성/사고 비약군 매핑)"
	}
	if swamp >= 0.60 && landscape >= 0.60 {
		return "Cluster C (융합 심리군: 경계선/얀데레 패턴 후보)"
	}
	return "Cluster M (혼합/비정형군)"
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
		"temp_step13_join",
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
	speaker := getenvOrDefault("SPEAKER", "내담자")
	minTurns := int(getenvIntOrDefault("MIN_TURNS", 20))
	maxKEnv := int(getenvIntOrDefault("MAX_K", 6))
	if minTurns < 2 {
		minTurns = 2
	}

	fmt.Printf("Run Config | speaker=%s min_turns=%d max_k=%d\n", speaker, minTurns, maxKEnv)

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

	joinedRows, err := loadJoinedEmbeddingsByStorageAPI(ctx, bqClient, storageClient, speaker)
	if err != nil {
		log.Fatalf("loadJoinedEmbeddingsByStorageAPI(step13): %v", err)
	}

	rowsByFile := make(map[string][]JoinedEmbeddingRow)
	for _, row := range joinedRows {
		rowsByFile[row.FileCode] = append(rowsByFile[row.FileCode], row)
	}

	signatures := make([]SignatureVector, 0, len(rowsByFile))
	for fileCode, rows := range rowsByFile {
		if len(rows) >= minTurns {
			if vec, ok := computeSignature(fileCode, rows); ok {
				signatures = append(signatures, vec)
			}
		}
	}
	if len(signatures) < 3 {
		log.Fatalf("not enough vectors for clustering: %d", len(signatures))
	}

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

	for i := range signatures {
		signatures[i].NormSwampDensity = clamp01(minMaxScale(signatures[i].SwampDensity, minSwamp, maxSwamp))
		signatures[i].NormLandscapeVol = clamp01(minMaxScale(signatures[i].LandscapeVolatility, minLand, maxLand))
		signatures[i].NormSpikeFrequency = clamp01(minMaxScale(signatures[i].SpikeFrequency, minSpike, maxSpike))
		signatures[i].NormDissonance = clamp01(minMaxScale(signatures[i].DissonanceScore, minDis, maxDis))
	}

	sort.Slice(signatures, func(i, j int) bool { return signatures[i].FileCode < signatures[j].FileCode })

	data := make([][]float64, len(signatures))
	for i := range signatures {
		data[i] = []float64{
			signatures[i].NormSwampDensity,
			signatures[i].NormLandscapeVol,
			signatures[i].NormSpikeFrequency,
			signatures[i].NormDissonance,
		}
	}

	fmt.Println("\n[1/4] K-Means 클러스터링 실행")
	stageStart := time.Now()

	kMin := 2
	kMax := maxKEnv
	if kMax > len(data)-1 {
		kMax = len(data) - 1
	}
	if kMax < kMin {
		kMax = kMin
	}

	sseByK := make(map[int]float64)
	elbowBar := newProgressBar("elbow", kMax-kMin+1)
	progressIdx := 0
	for k := kMin; k <= kMax; k++ {
		_, _, sse := kmeans(data, k, 60)
		sseByK[k] = sse
		progressIdx++
		elbowBar.Render(progressIdx, fmt.Sprintf("k=%d sse=%.4f", k, sse))
	}

	selectedK := chooseKByElbow(sseByK, kMin, kMax)
	assign, centroids, sse := kmeans(data, selectedK, 100)

	fmt.Printf("    완료: K-Means 클러스터링 실행 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("    벡터=%d selected_k=%d final_sse=%.6f\n", len(data), selectedK, sse)

	fmt.Println("\n[2/4] 차원 축소(PCA) 기반 좌표 변환")
	stageStart = time.Now()

	pcaCoords, eigVals, err := pca2D(data)
	if err != nil {
		log.Fatalf("pca2D failed: %v", err)
	}

	fmt.Printf("    완료: 차원 축소(PCA) 기반 좌표 변환 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("    PCA eigen values: [%.6f, %.6f]\n", eigVals[0], eigVals[1])

	fmt.Println("\n[3/4] 클러스터별 중심점(Centroid) 산출")
	stageStart = time.Now()

	clusterSizes := make(map[int]int)
	sumPCA := make(map[int][2]float64)
	for i, cid := range assign {
		clusterSizes[cid]++
		acc := sumPCA[cid]
		acc[0] += pcaCoords[i][0]
		acc[1] += pcaCoords[i][1]
		sumPCA[cid] = acc
	}

	labelByCluster := make(map[int]string)
	centroidRows := make([]ClusterCentroid, 0, len(centroids))
	centroidBar := newProgressBar("centroid", len(centroids))
	for cid := range centroids {
		label := clusterSemanticLabel(centroids[cid][0], centroids[cid][1])
		labelByCluster[cid] = label
		sz := clusterSizes[cid]
		pc := sumPCA[cid]
		pca1, pca2 := 0.0, 0.0
		if sz > 0 {
			pca1 = pc[0] / float64(sz)
			pca2 = pc[1] / float64(sz)
		}
		centroidRows = append(centroidRows, ClusterCentroid{
			ClusterID:        cid,
			Size:             sz,
			NormSwampDensity: centroids[cid][0],
			NormLandscapeVol: centroids[cid][1],
			NormSpikeFreq:    centroids[cid][2],
			NormDissonance:   centroids[cid][3],
			PCA1:             pca1,
			PCA2:             pca2,
			Label:            label,
		})
		centroidBar.Render(cid+1, "클러스터 중심 계산")
	}
	sort.Slice(centroidRows, func(i, j int) bool { return centroidRows[i].ClusterID < centroidRows[j].ClusterID })

	fmt.Printf("    완료: 클러스터별 중심점(Centroid) 산출 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	for _, c := range centroidRows {
		fmt.Printf("    cluster=%d size=%d label=%s\n", c.ClusterID, c.Size, c.Label)
	}

	fmt.Println("\n[4/4] 군집 위상도 시각화")
	stageStart = time.Now()

	clustered := make([]ClusteredPoint, 0, len(signatures))
	pointBar := newProgressBar("point", len(signatures))
	for i := range signatures {
		cid := assign[i]
		dist := math.Sqrt(squaredEuclidean(data[i], centroids[cid]))
		clustered = append(clustered, ClusteredPoint{
			FileCode:            signatures[i].FileCode,
			ClusterID:           cid,
			ClusterLabel:        labelByCluster[cid],
			NormSwampDensity:    signatures[i].NormSwampDensity,
			NormLandscapeVol:    signatures[i].NormLandscapeVol,
			NormSpikeFrequency:  signatures[i].NormSpikeFrequency,
			NormDissonance:      signatures[i].NormDissonance,
			PCA1:                pcaCoords[i][0],
			PCA2:                pcaCoords[i][1],
			DistanceToCentroid:  dist,
			RawSwampDensity:     signatures[i].SwampDensity,
			RawLandscapeVol:     signatures[i].LandscapeVolatility,
			RawSpikeFrequency:   signatures[i].SpikeFrequency,
			RawDissonance:       signatures[i].DissonanceScore,
			TrajectoryTurnCount: signatures[i].TurnCount,
		})
		pointBar.Render(i+1, "시각화 포인트 생성")
	}

	palette := []string{"#1b9e77", "#d95f02", "#7570b3", "#e7298a", "#66a61e", "#e6ab02", "#a6761d", "#666666"}

	scatter := charts.NewScatter()
	scatter.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: "shine", Width: "100vw", Height: "78vh"}),
		charts.WithTitleOpts(opts.Title{
			Title:    "Step 13 | 비지도 이상 심리 군집 위상도",
			Subtitle: fmt.Sprintf("K=%d | X=Swamp Density | Y=Landscape Volatility | Color=Cluster", selectedK),
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
				SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{Show: opts.Bool(true), Name: "multi_unsupervised_psychology_topology"},
			},
		}),
	)
	scatter.SetXAxis([]float64{})

	seriesByCluster := make(map[int][]opts.ScatterData)
	for _, p := range clustered {
		seriesByCluster[p.ClusterID] = append(seriesByCluster[p.ClusterID], opts.ScatterData{
			Name: fmt.Sprintf("%s | %s | pca=(%.4f, %.4f) | dist=%.4f",
				p.FileCode,
				p.ClusterLabel,
				p.PCA1,
				p.PCA2,
				p.DistanceToCentroid,
			),
			Value:      []float64{p.NormSwampDensity, p.NormLandscapeVol},
			Symbol:     "circle",
			SymbolSize: int(math.Round(9 + (12 * p.NormSpikeFrequency))),
		})
	}

	for cid := 0; cid < selectedK; cid++ {
		name := fmt.Sprintf("Cluster %d", cid)
		if label, ok := labelByCluster[cid]; ok {
			name = fmt.Sprintf("Cluster %d | %s", cid, label)
		}
		color := palette[cid%len(palette)]
		scatter.AddSeries(name, seriesByCluster[cid], charts.WithItemStyleOpts(opts.ItemStyle{Color: color}))
	}

	result := ClusterResult{
		SelectedK:        selectedK,
		SSEByK:           sseByK,
		PCAEvalues:       eigVals,
		Centroids:        centroidRows,
		Samples:          clustered,
		ClusterLabelInfo: labelByCluster,
		GeneratedAt:      time.Now().Format(time.RFC3339),
		Metadata: map[string]json.RawMessage{
			"run_config": json.RawMessage(fmt.Sprintf(`{"speaker":"%s","min_turns":%d,"max_k":%d}`, speaker, minTurns, maxKEnv)),
		},
	}

	resultBytes, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Fatalf("json marshal failed: %v", err)
	}
	if err := os.WriteFile(outputJSON, resultBytes, 0o644); err != nil {
		log.Fatalf("failed to write %s: %v", outputJSON, err)
	}

	var rendered bytes.Buffer
	if err := scatter.Render(&rendered); err != nil {
		log.Fatalf("chart render failed: %v", err)
	}
	styledHTML := injectFontCSS(rendered.String())
	if err := os.WriteFile(outputHTML, []byte(styledHTML), 0o644); err != nil {
		log.Fatalf("failed to write html: %v", err)
	}

	fmt.Printf("    완료: 군집 위상도 시각화 (elapsed=%s)\n", time.Since(stageStart).Round(100*time.Millisecond))
	fmt.Printf("Done. Outputs written to %s, %s\n", outputHTML, outputJSON)
	fmt.Printf("Summary | vectors=%d clusters=%d total_elapsed=%s\n", len(signatures), selectedK, time.Since(jobStart).Round(100*time.Millisecond))
}
