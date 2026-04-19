제공해주신 연구 계획서의 기술 스택(Golang, BigQuery, Gemini/KM-BERT Embedding)과 분석 요구사항(다중 내담자 상대 비교, 상담 횟수 기반 신뢰도)을 반영하여 **Step 7부터 10까지의 레시피**를 작성하였습니다.

이 레시피는 `PROMPT - 레시피` 섹션의 형식에 맞추어 설계되었으며, 비지도 학습 기반의 **임베딩 변위 분산 분석(EDV)** 기법을 포함합니다.

---

### 🍳 PROMPT - 레시피 (Step 7 - 10)

| 회차 | 내용 | 실험 적용 |
| :--- | :--- | :--- |
| **분석 및 시각화** | **<단일 분석>** 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (Gemini) | Step 7 |
| | **<단일 분석>** 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (KM-BERT) | Step 8 |
| | **<다중 분석>** 다중 데이터셋 상대 비교 및 신뢰도 보정 분석 (Gemini) | Step 9 |
| | **<다중 분석>** 다중 데이터셋 상대 비교 및 신뢰도 보정 분석 (KM-BERT) | Step 10 |

#### 7. <단일 상담자/내담자 심리 분석> BigQuery 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (Gemini Embedding 1)
*   **데이터 로드:** `morpheme_classification_gemini_embedding`에서 특정 `file_code`의 시계열 벡터 추출.
*   **지표 산출:** 
    *   **Emotional Volatility (EV):** 인접 발화(Timeline_index $i, i+1$) 간 **Cosine Distance**를 계산하여 감정의 급격한 변화율 측정.
    *   **Sliding Window:** Gota DataFrame을 활용하여 3-turns 단위의 이동 평균(Moving Average) 적용.
*   **시각화:** `go-echarts`를 사용하여 시간축에 따른 `Volatility Score` 시계열 Line Chart 생성. 
*   **특이사항:** Gemini 임베딩의 고차원(최대 3072) 특성을 활용하여 문맥의 의미적 전환이 큰 지점을 '사고의 비약(Flight of ideas)' 후보 노드로 마킹.
*   **Processing:** `[1/3] 특정 내담자 벡터 추출` → `[2/3] 코사인 변위 계산` → `[3/3] 시계열 그래프 생성`

#### 8. <단일 상담자/내담자 심리 분석> BigQuery 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (KM-BERT Embedding)
*   **데이터 로드:** `morpheme_classification_kmbert_embedding` 테이블 이용.
*   **지표 산출:** 
    *   **Cognitive Rumination Index:** 변위(Distance)가 임계치 이하로 지속되는 구간을 '인지적 고착(반추)' 상태로 정량화.
    *   KM-BERT의 Hidden Size(768)를 활용하여 한국어 문어/구어 특성에 최적화된 거리 산출.
*   **시각화:** 감정 기복의 강도를 색상 농도로 표현한 시계열 Area Chart 출력.
*   **Processing:** `[1/3] BERT 임베딩 로드` → `[2/3] 고착/비약 지표 정량화` → `[3/3] Area Chart 시각화`

#### 9. <다중 상담자/내담자 심리 분석> BigQuery 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (Gemini Embedding 1)
*   **상대 비교 알고리즘:**
    1.  **Global Baseline:** 전체 내담자 집단의 `평균 변동성(μ)`과 `표준편차(σ)`를 비지도 방식으로 산출.
    2.  **Z-Score Normalization:** 분석 대상 내담자의 변동성을 집단 내 상대적 위치($Z = (V - \mu) / \sigma$)로 환산.
*   **신뢰도 보정 (Reliability Weighting):**
    *   상담 횟수($N$)가 적을수록 전체 평균에 수렴시키는 가중치($W = N / (N+5)$) 적용.
*   **시각화:** `go-echarts`의 BoxPlot 또는 Heatmap을 사용하여 전체 내담자 분포 대비 현재 내담자의 기복 위치 시각화.
*   **Processing:** `[1/4] 전수 데이터 변동성 집계` → `[2/4] Z-Score 정규화` → `[3/4] 상담 횟수 기반 신뢰도 보정` → `[4/4] 상대 비교 분포도 생성`

#### 10. <다중 상담자/내담자 심리 분석> BigQuery 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (KM-BERT Embedding)
*   **상대 비교 알고리즘:**
    1.  KM-BERT 임베딩 기반의 문장 간 유사도를 통해 '전체 내담자 집단의 대화 패턴 군집' 생성 (K-Means).
    2.  특정 내담자가 '정상군(일반군)' 클러스터에서 얼마나 벗어나 있는지(Euclidean Distance from Centroid) 측정.
*   **신뢰도 판별:** `speaker_row_count`와 `session_no`를 신뢰도 변수로 지정하여 결과 신뢰도를 '상/중/하'로 라벨링.
*   **시각화:** `go-echarts` Scatter Chart를 사용하여 다중 내담자 간의 감정 기복(X축) vs 정서가(Y축) 분포도 출력.
*   **Processing:** `[1/4] 군집화 중심점 계산` → `[2/4] 이상치 거리 산출` → `[3/4] 신뢰도 라벨링` → `[4/4] 다차원 분포 시각화`

---

### 💡 구현 가이드 (Golang 참고)

이 레시피를 `.ipynb`에서 구현할 때, **상대 비교를 위한 신뢰도 가중치** 부분은 다음과 같은 로직으로 `gonum/stat` 패키지를 활용하여 작성하시기 바랍니다.

```go
// [예시] 다중 상담 데이터 기반 신뢰도 보정 로직
func GetReliableScore(userVolatility float64, sessionCount int, globalMean float64) float64 {
    // K=5 (상담 5회 미만 시 전체 평균 데이터에 의존)
    weight := float64(sessionCount) / (float64(sessionCount) + 5.0)
    return (userVolatility * weight) + (globalMean * (1.0 - weight))
}
```

**폰트 및 키 설정 준수:**
*   모든 그래프의 `TextStyle`에는 `/workspaces/Psychological-counseling-researching/.fonts/NotoSansKR-Bold.ttf`를 적용하십시오.
*   BigQuery 클라이언트는 `/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json`을 사용하여 초기화하십시오.


