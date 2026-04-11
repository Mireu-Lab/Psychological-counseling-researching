package main

import (
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "time"

    "cloud.google.com/go/bigquery"
    "golang.org/x/oauth2"
    "golang.org/x/oauth2/google"
    "google.golang.org/api/iterator"
)

const (
    projectID  = "testprojects-453622"
    datasetID  = "Psychological_counseling_data"
    sourceTB   = "morpheme_classification"
    targetTB   = "morpheme_classification_gemini_embedding"
    location   = "asia-northeast3" // 필요 시 asia-northeast3 등으로 변경
    modelName  = "gemini-embedding-001"
    outputDim  = 3072 // 128~3072 범위에서 조정 가능
    batchSize  = 100

    keyPath  = "/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json"
    fontPath = "/workspaces/Psychological-counseling-researching/.fonts/NotoSansKR-Bold.ttf"
)

type SourceRow struct {
    FileCode        string `bigquery:"file_code"`
    Speaker         string `bigquery:"speaker"`
    SessionNo       int64  `bigquery:"session_no"`
    TimelineIndex   int64  `bigquery:"timeline_index"`
    EndContentIndex int64  `bigquery:"end_content_index"`
    SplitRowIndex   int64  `bigquery:"split_row_index"`
    SplitContents   string `bigquery:"split_contents"`
}

type DestRow struct {
    FileCode        string    `bigquery:"file_code"`
    Speaker         string    `bigquery:"speaker"`
    SessionNo       int64     `bigquery:"session_no"`
    TimelineIndex   int64     `bigquery:"timeline_index"`
    EndContentIndex int64     `bigquery:"end_content_index"`
    SplitRowIndex   int64     `bigquery:"split_row_index"`
    SplitContents   string    `bigquery:"split_contents"`
    Embedding       []float64 `bigquery:"embedding"`
    Model           string    `bigquery:"model"`
    Dimension       int64     `bigquery:"dimension"`
    CreatedAt       time.Time `bigquery:"created_at"`
}

type predictRequest struct {
    Instances  []map[string]any `json:"instances"`
    Parameters map[string]any   `json:"parameters,omitempty"`
}

type predictResponse struct {
    Predictions []struct {
        Embeddings struct {
            Values []float64 `json:"values"`
        } `json:"embeddings"`
    } `json:"predictions"`
}

func main() {
    ctx := context.Background()

    // 필수 경로 적용
    _ = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", keyPath)

    if _, err := os.Stat(keyPath); err != nil {
        log.Fatalf("GCP 키 파일 확인 실패: %v", err)
    }
    if _, err := os.Stat(fontPath); err != nil {
        log.Fatalf("폰트 파일 확인 실패: %v", err)
    }
    log.Printf("폰트 확인 완료: %s", fontPath)

    bq, err := bigquery.NewClient(ctx, projectID)
    if err != nil {
        log.Fatalf("BigQuery client 생성 실패: %v", err)
    }
    defer bq.Close()

    if err := ensureTargetTable(ctx, bq); err != nil {
        log.Fatalf("타겟 테이블 준비 실패: %v", err)
    }

    httpClient, err := newAuthedHTTPClient(ctx, keyPath)
    if err != nil {
        log.Fatalf("인증 HTTP 클라이언트 생성 실패: %v", err)
    }

    total, err := countSourceRows(ctx, bq)
    if err != nil {
        log.Fatalf("원본 행 개수 조회 실패: %v", err)
    }
    log.Printf("총 처리 대상: %d", total)

    query := fmt.Sprintf(`
        SELECT file_code, speaker, session_no, timeline_index, end_content_index, split_row_index, split_contents
        FROM %s.%s.%s
        ORDER BY file_code, split_row_index
    `, "`"+projectID+"`", "`"+datasetID+"`", "`"+sourceTB+"`")

    it, err := bq.Query(query).Read(ctx)
    if err != nil {
        log.Fatalf("원본 조회 실패: %v", err)
    }

    inserter := bq.Dataset(datasetID).Table(targetTB).Inserter()
    buffer := make([]*DestRow, 0, batchSize)

    var processed int64
    start := time.Now()

    for {
        var src SourceRow
        err := it.Next(&src)
        if errors.Is(err, iterator.Done) {
            break
        }
        if err != nil {
            log.Fatalf("원본 iterator 실패: %v", err)
        }

        emb, err := embedWithRetry(ctx, httpClient, src.SplitContents, 3)
        if err != nil {
            log.Printf("[WARN] 임베딩 실패(file_code=%s, row=%d): %v", src.FileCode, src.SplitRowIndex, err)
            continue
        }

        buffer = append(buffer, &DestRow{
            FileCode:        src.FileCode,
            Speaker:         src.Speaker,
            SessionNo:       src.SessionNo,
            TimelineIndex:   src.TimelineIndex,
            EndContentIndex: src.EndContentIndex,
            SplitRowIndex:   src.SplitRowIndex,
            SplitContents:   src.SplitContents,
            Embedding:       emb,
            Model:           modelName,
            Dimension:       int64(len(emb)),
            CreatedAt:       time.Now(),
        })

        processed++
        if (processed % 50) == 0 || processed == total {
            log.Printf("Processing... %d/%d (%.2f%%)", processed, total, (float64(processed)/float64(max(total, 1)))*100.0)
        }

        if len(buffer) >= batchSize {
            if err := inserter.Put(ctx, buffer); err != nil {
                log.Fatalf("BigQuery 적재 실패: %v", err)
            }
            buffer = buffer[:0]
        }
    }

    if len(buffer) > 0 {
        if err := inserter.Put(ctx, buffer); err != nil {
            log.Fatalf("마지막 배치 적재 실패: %v", err)
        }
    }

    log.Printf("완료: 처리=%d, 소요=%s", processed, time.Since(start))
}

func ensureTargetTable(ctx context.Context, bq *bigquery.Client) error {
    tb := bq.Dataset(datasetID).Table(targetTB)
    _, err := tb.Metadata(ctx)
    if err == nil {
        return nil
    }

    schema, err := bigquery.InferSchema(DestRow{})
    if err != nil {
        return err
    }

    return tb.Create(ctx, &bigquery.TableMetadata{
        Schema: schema,
    })
}

func countSourceRows(ctx context.Context, bq *bigquery.Client) (int64, error) {
    q := fmt.Sprintf("SELECT COUNT(*) AS cnt FROM `%s.%s.%s`", projectID, datasetID, sourceTB)
    it, err := bq.Query(q).Read(ctx)
    if err != nil {
        return 0, err
    }
    var row struct {
        Cnt int64 `bigquery:"cnt"`
    }
    if err := it.Next(&row); err != nil {
        return 0, err
    }
    return row.Cnt, nil
}

func newAuthedHTTPClient(ctx context.Context, serviceAccountPath string) (*http.Client, error) {
    keyBytes, err := os.ReadFile(serviceAccountPath)
    if err != nil {
        return nil, err
    }
    creds, err := google.CredentialsFromJSON(ctx, keyBytes, "https://www.googleapis.com/auth/cloud-platform")
    if err != nil {
        return nil, err
    }
    return oauth2.NewClient(ctx, creds.TokenSource), nil
}

func embedWithRetry(ctx context.Context, c *http.Client, text string, retries int) ([]float64, error) {
    var lastErr error
    for i := 0; i <= retries; i++ {
        emb, err := embed(ctx, c, text)
        if err == nil {
            return emb, nil
        }
        lastErr = err
        time.Sleep(time.Duration(i+1) * 700 * time.Millisecond)
    }
    return nil, lastErr
}

func embed(ctx context.Context, c *http.Client, text string) ([]float64, error) {
    endpoint := fmt.Sprintf(
        "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
        location, projectID, location, modelName,
    )

    reqBody := predictRequest{
        Instances: []map[string]any{
            {"content": text},
        },
        Parameters: map[string]any{
            "outputDimensionality": outputDim,
        },
    }

    b, _ := json.Marshal(reqBody)
    req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
    if err != nil {
        return nil, err
    }
    req.Header.Set("Content-Type", "application/json")

    resp, err := c.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    body, _ := io.ReadAll(resp.Body)
    if resp.StatusCode < 200 || resp.StatusCode >= 300 {
        return nil, fmt.Errorf("vertex ai 오류(%d): %s", resp.StatusCode, string(body))
    }

    var pr predictResponse
    if err := json.Unmarshal(body, &pr); err != nil {
        return nil, err
    }
    if len(pr.Predictions) == 0 || len(pr.Predictions[0].Embeddings.Values) == 0 {
        return nil, fmt.Errorf("임베딩 응답 비어있음")
    }
    return pr.Predictions[0].Embeddings.Values, nil
}

func max(a, b int64) int64 {
    if a > b {
        return a
    }
    return b
}