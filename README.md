# Psychological-counseling-researching
심리 분석 및 추론 연구


## 목표
상담자와 내담자, 질문과 대답의 행동/언어적 내용을 보고 기계적으로 이상 심리적 형태를 파악할수있는 로보틱스 + 챗봇 서비스를 기획으로 개발하고자 한다.


우리가 가지고 있는 리소스를 최소화 하여 사물인터넷에서 local로 구동 할수있는 Psychology NLP Model를 구현하는것에 현제 이의를 두고자 한다.


현재 정신 질환 진단를 판별하기에는 상담자의 교육수준, 문맥 이해 등 여러 매개변수가 존재하여 대략적인 병명을 진단 할수 있지만, 새부화적인 병명을 진단하기에는 여러 방법의 검사를 거쳐서 이해 할수있는 모델을 구축할수있게 된다.


해당 매개변수에 흔들리지 않는 통계적인 기계로 적용하였을때, 정확하게 병명을 진단하고 해당 연구가 좋은 결과물로 이어 졌을때 해당 기술로 예방 및 치료로 적용할수있는 방법을 고안하고자 한다.


### 개발 환경


|**조건**|**도구**|
|:---:|:---:|
|시스템|Linux codespaces 6.8.0-1044-azure 22.04.1-Ubuntu SMP x86_64 GNU/Linux|
|언어|Golang 1.25.4|
|jupyter 커널|Golang (gonb)|
|프레임워크|[pkg.go.dev/github.com/go-gota/gota/dataframe](https://pkg.go.dev/github.com/go-gota/gota/dataframe),<br>[pkg.go.dev/gorgonia.org/gorgonia](https://pkg.go.dev/gorgonia.org/gorgonia),<br>[pkg.go.dev/gonum.org/v1/gonum](https://pkg.go.dev/gonum.org/v1/gonum),<br>[pkg.go.dev/gonum.org/v1/plot](https://pkg.go.dev/gonum.org/v1/plot),<br>[pkg.go.dev/github.com/go-echarts/go-echarts/v2](https://pkg.go.dev/github.com/go-echarts/go-echarts/v2)|




## 레시피


| 회차 | 내용 | 실험 적용 |
| :---- | :---- | :---- |
| **데이터셋 관리 과정** | 파일 구조 분류 | 전체 적용 |
| **전처리 과정** | 텍스트 문장 분류 (텍스트 토크나이저) |  |
|  | 오디오 대화별 분류 (오디오 편집) | 오디오/텍스트 심리 상담 데이터를 기반으로 행동/이상 심리 분석 및 예측 모델 구현 |
|  | 오디오 대화별 오디오 STT (Speech to text)로 텍스트 생성 |  |
| **임베딩 과정** | 텍스트 데이터를 기반으로 벡터 임베딩 처리 | 전체 적용 |
|  | 오디오 데이터를 기반으로 벡터 임베딩 처리 | 오디오/텍스트 심리 상담 데이터를 기반으로 행동/이상 심리 분석 및 예측 모델 구현 |
| **시각화 과정** | 임베딩 벡터기반으로 데이터 셋 상담 데이터 클러스터링 처리 | 전체 적용 |
| **전처리 과정** | 비 정규화 (오디오) 데이터 및 정규화 (텍스트) 임베딩 Sinking 처리 | 오디오/텍스트 심리 상담 데이터를 기반으로 행동/이상 심리 분석 및 예측 모델 구현 |
| **시각화 과정** | 임베딩 데이터 시각화 및 군집 확인 | 전체 적용 |
| **모델 구현 과정** | 그래프 구조화 및 데이터베이스 적재후 GNN 모델 학습 |  |


### 제조 과정 (step)


[ **주의** ] : 반드시 Golang을 기반하여 코드를 제시 하여 `.go`에서 구동하여야한다.
[ **주의** ] : 각 세션 마다 파일을 따로 생성하여 관리 하여야한다.
[ **주의** ] : 주석 처리된 레시피 제외한 나머지를 구상 하여야한다.
[ **주의** ] : 구현 완료를 제외한 나머지를 구상하여야한다.


[ **제시** ] : font는 `/workspaces/Psychological-counseling-researching/.fonts/NotoSansKR-Bold.ttf`에 있는 폰트를 사용하여야한다.
[ **제시** ] : GCP Accsee Key는 `/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json`에 있는 JSON파일을 사용하여야한다.
[ **제시** ] : Processing (실행) 표시창도 출력 하여야한다.




1. **(구현 완료) 텍스트 데이터 파일 정렬후 BigQuery에 저장**
    *Schema(INDEX:INTEGER, FILEPATH:STRING, SPEAKER:STRING, CONTENT: STRING)* 으로 구성 된 데이터를 BigQuery Table에 저장.
    **구상된 Query** : sql```SELECT * FROM `testprojects-453622.Psychological_counseling_data.all_raw` ```


2. **(구현 완료) 텍스트 내용 및 파일 정렬 데이터 BigQuery Table에 저장**
    *Schema(index:INTEGER, FilePath:STRING, Speaker:STRING, Content:STRING, FileName:STRING, prefix:STRING, topic:STRING, session_no:INTEGER, stage:STRING, file_code:STRING, file_code_row_count:INTEGER, speaker_row_count:INTEGER, file_code_ratio_pct:FLOAT, speaker_ratio_pct:FLOAT)* 으로 구성된 데이터를 BigQuery Table에 저장 하였음.
    **구상된 Query** : sql```SELECT * FROM `testprojects-453622.Psychological_counseling_data.processed_data` ```


3. **(구현 완료) BigQuery에 있는 텍스트 문장 분류**
    Kiwi를 가지고 Text Tokenizer진행후, *Schema(file_code : STRING, speaker : STRING, session_no : INTEGER, timeline_index : INTEGER, end_content_index : INTEGER, split_row_index : INTEGER, split_contents : STRING)* 으로 구성된 데이터를 BigQuery BigQuery에 morpheme_classification Table에 저장 하였음.


    * 문장 종료 규칙: 종결 어미(EF) 뒤에 종결 부호/줄임표/붙임표(SF/SE/SO)가 나올 때만 종료
    **구상된 Query** : sql```SELECT * FROM `testprojects-453622.Psychological_counseling_data.morpheme_classification` ```


4. **(구현 완료) BigQuery에 있는 텍스트 문장 임베딩 후 BigQuery에 저장**
    *Schema(file_code : STRING, speaker : STRING, session_no : INTEGER, timeline_index : INTEGER, end_content_index : INTEGER, split_row_index : INTEGER, split_contents : STRING)
    KM-BERT를 가지고 Text Embedding 진행후, BigQuery에 morpheme_classification_kmbert_embedding 저장


    * KM-BERT의 Hidden Size 768
    **구상된 Query** : sql```SELECT * FROM `testprojects-453622.Psychological_counseling_data.morpheme_classification_kmbert_embedding` ```


5. **(구현 완료) BigQuery에 있는 텍스트 문장 임베딩 후 BigQuery에 저장**
    *Schema(file_code: STRING, speaker: STRING, session_no: INTEGER, timeline_index: INTEGER, end_content_index: INTEGER, split_row_index: INTEGER, split_contents: STRING, embedding: FLOAT, model: STRING, dimension: INTEGER, created_at: TIMESTAMP)


    Gemini API에서 Gemini Embedding 1를 가지고 Text Embedding 진행후, BigQuery에 morpheme_classification_gemini_embedding 저장
    * Gemini Embedding 1은 Gemini API Docs에서 `gemini-embedding-001` 코드명으로 사용.
    * Gemini Embedding 1의 Hidden Size 128 - 3072
    **구상된 Query** : sql```SELECT * FROM `testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding`


6. **(구현 완료) 임베딩 기반 의미적 문단 감정 기복 분석 시각화 위한 BigQuery Storage Read API 기반 데이터 로드**
    `processed_data`에서 `file_code`별 `stage`를 추출한 뒤, `morpheme_classification_gemini_embedding`과 JOIN하여
    *(file_code:STRING, timeline_index:INTEGER, embedding:FLOAT64[], stage:STRING)* 스키마로 정렬된 결과를 생성.
    생성된 쿼리 결과는 임시 테이블로 저장 후, BigQuery Storage Read API의 다중 스트림 병렬 읽기(코어 기반 MaxStreamCount)로 다운로드.
    Avro 포맷으로 수신한 `serialized_binary_rows`를 디코딩하여 `EmbeddingRow` 구조체로 병합하고,
    전체 행 수 및 총 소요 시간(쿼리 실행 + 병렬 다운로드 + 디코딩)을 출력.
    * Processing 출력 단계: `[1/4] 임시 테이블 생성` → `[2/4] Read Session/Stream 생성` → `[3/4] 병렬 다운로드` → `[4/4] 데이터 병합`
    **구상된 Query** : sql```WITH Target_Processed_Data AS ( SELECT file_code, ANY_VALUE(stage) AS stage FROM testprojects-453622.Psychological_counseling_data.processed_data GROUP BY file_code ) SELECT e.file_code, e.timeline_index, e.embedding, p.stage FROM testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding AS e JOIN Target_Processed_Data AS p ON e.file_code = p.file_code ORDER BY e.file_code, e.timeline_index ASC```




7. **(구현 완료) <단일 상담자/내담자 심리 분석> BigQuery 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (Gemini Embedding 1)**
    *   **데이터 로드:** `morpheme_classification_gemini_embedding`에서 특정 `file_code`의 시계열 벡터 추출.
    *   **지표 산출:**
        *   **Emotional Volatility (EV):** 인접 발화(Timeline_index $i, i+1$) 간 **Cosine Distance**를 계산하여 감정의 급격한 변화율 측정.
        *   **Sliding Window:** Gota DataFrame을 활용하여 3-turns 단위의 이동 평균(Moving Average) 적용.
    *   **시각화:** `go-echarts`를 사용하여 시간축에 따른 `Volatility Score` 시계열 Line Chart 생성.
    *   **특이사항:** Gemini 임베딩의 고차원(최대 3072) 특성을 활용하여 문맥의 의미적 전환이 큰 지점을 '사고의 비약(Flight of ideas)' 후보 노드로 마킹.
    *   **Processing:** `[1/3] 특정 내담자 벡터 추출` → `[2/3] 코사인 변위 계산` → `[3/3] 시계열 그래프 생성`


8. **(구현 완료) <단일 상담자/내담자 심리 분석> BigQuery 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (KM-BERT Embedding)**
    *   **데이터 로드:** `morpheme_classification_kmbert_embedding` 테이블 이용.
    *   **지표 산출:**
        *   **Cognitive Rumination Index:** 변위(Distance)가 임계치 이하로 지속되는 구간을 '인지적 고착(반추)' 상태로 정량화.
        *   KM-BERT의 Hidden Size(768)를 활용하여 한국어 문어/구어 특성에 최적화된 거리 산출.
    *   **시각화:** 감정 기복의 강도를 색상 농도로 표현한 시계열 Area Chart 출력.
    *   **Processing:** `[1/3] BERT 임베딩 로드` → `[2/3] 고착/비약 지표 정량화` → `[3/3] Area Chart 시각화`



9. **<단일 상담자/내담자 심리 분석> 늪과 풍경: KM-BERT(Area) + Gemini(Line) Merge 시각화**
    **[목표]**
    의학적 고착 상태를 나타내는 **'늪(Swamp)'**과 문맥적 비약을 나타내는 **'풍경(Landscape)'**을 중첩(Overlap)시켜, 
    내담자가 특정 병리적 주제에 갇힌 채(KM-BERT) 얼마나 비정상적인 인지적 널뛰기(Gemini)를 수행하는지 분석함.

    *   **데이터 로드:** BigQuery에서 동일한 `file_code`와 `session_no`를 가진 KM-BERT 및 Gemini 임베딩 데이터를 JOIN하여 추출.
    *   **지표 정량화 (The Hybrid Logic):**
        1.  **Swamp Index (늪):** KM-BERT 임베딩의 코사인 거리를 **역전(Invert)**하고 5-Turn 이동평균 적용. 
            *   *식:* $Swamp\_Idx = 1.0 - \text{MovingAvg}(\text{Distance}_{kmbert})$
            *   *의미:* 값이 높고 면적이 넓을수록 인지적 고착(늪)이 깊음.
        2.  **Landscape Index (풍경):** Gemini 임베딩의 코사인 변위와 3-Turn 이동평균 적용.
            *   *식:* $Volatility\_Idx = \text{MovingAvg}(\text{Distance}_{gemini})$
            *   *의미:* 선이 뾰족하게 튀어 오를수록 사고의 비약(풍경의 정점)이 발생함.
    *   **시각화 전략 (Merge Chart):**
        *   **Bottom (Area Chart):** KM-BERT의 `Swamp Index`를 반투명한 어두운 색상(예: Deep Green/Grey)의 영역 차트로 배치.
        *   **Top (Line Chart):** Gemini의 `Volatility Index`를 선명한 색상(예: Red/Yellow)의 꺾은선으로 중첩.
        *   **Overlay 마커:** Gemini 변위가 $\mu + 2\sigma$를 넘는 지점에 삼각형(▲) 마커를 표시하여 "늪 위에서 발생하는 번개(비약)"를 시각화.
    *   **Processing 단계:** `[1/4] 멀티 임베딩 Sinking` $\rightarrow$ `[2/4] 고착도(늪) 및 변동성(풍경) 산출` $\rightarrow$ `[3/4] 이중 축(Dual-Axis) 차트 생성` $\rightarrow$ `[4/4] 융합 심리 패턴 마킹`
    


10. **<다중 상담자/내담자 심리 분석> BigQuery 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (Gemini Embedding 1)**
    *   **상대 비교 알고리즘:**
        1.  **Global Baseline:** 전체 내담자 집단의 `평균 변동성(μ)`과 `표준편차(σ)`를 비지도 방식으로 산출.
        2.  **Z-Score Normalization:** 분석 대상 내담자의 변동성을 집단 내 상대적 위치($Z = (V - \mu) / \sigma$)로 환산.
    *   **신뢰도 보정 (Reliability Weighting):**
        *   상담 횟수($N$)가 적을수록 전체 평균에 수렴시키는 가중치($W = N / (N+5)$) 적용.
    *   **시각화:** `go-echarts`의 BoxPlot 또는 Heatmap을 사용하여 전체 내담자 분포 대비 현재 내담자의 기복 위치 시각화.
    *   **Processing:** `[1/4] 전수 데이터 변동성 집계` → `[2/4] Z-Score 정규화` → `[3/4] 상담 횟수 기반 신뢰도 보정` → `[4/4] 상대 비교 분포도 생성`


11. **<다중 상담자/내담자 심리 분석> BigQuery 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (KM-BERT Embedding)**
    *   **상대 비교 알고리즘:**
        1.  KM-BERT 임베딩 기반의 문장 간 유사도를 통해 '전체 내담자 집단의 대화 패턴 군집' 생성 (K-Means).
        2.  특정 내담자가 '정상군(일반군)' 클러스터에서 얼마나 벗어나 있는지(Euclidean Distance from Centroid) 측정.
    *   **신뢰도 판별:** `speaker_row_count`와 `session_no`를 신뢰도 변수로 지정하여 결과 신뢰도를 '상/중/하'로 라벨링.
    *   **시각화:** `go-echarts` Scatter Chart를 사용하여 다중 내담자 간의 감정 기복(X축) vs 정서가(Y축) 분포도 출력.
    *   **Processing:** `[1/4] 군집화 중심점 계산` → `[2/4] 이상치 거리 산출` → `[3/4] 신뢰도 라벨링` → `[4/4] 다차원 분포 시각화`


12. Multi-view Embedding 기법
    1. **임베딩 Sinking 및 결합 (Early Fusion):**
        BigQuery에 적재된 `morpheme_classification_kmbert_embedding`과 `morpheme_classification_gemini_embedding`을 JOIN하여, 병리-문맥 복합 벡터(Concatenated Vector)를 생성.


    2. **교차 검증 및 평가 (Model Evaluation):**
        * 모델 A: KM-BERT 임베딩만 사용한 GNN
        * 모델 B: Gemini 임베딩만 사용한 GNN
        * 모델 C: 두 임베딩을 결합(또는 비교)한 Ensemble GNN
        * 위 3가지 모델의 F1-Score를 비교하여, 복합/융합 심리 장애(예: Bipolar, 조현병의 연상 이완 등) 탐지에 있어 다중 차원 임베딩이 얼마나 정확도를 향상시키는지 정량적으로 증명.