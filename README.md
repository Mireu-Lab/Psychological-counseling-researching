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

[ **주의** ] : 반드시 Golang을 기반하여 코드를 제시 하여 `.ipynb`에서 구동하여야한다.
[ **주의** ] : 각 세션 마다 파일을 따로 생성하여 관리 하여야한다.
[ **주의** ] : 주석 처리된 레시피 제외한 나머지를 구상 하여야한다.
[ **주의** ] : 구현 완료를 제외한 나머지를 구상하여야한다.

[ **제시** ] : font는 `/workspaces/Psychological-counseling-researching/.fonts/NotoSansKR-Bold.ttf`에 있는 폰트를 사용하여야한다.
[ **제시** ] : GCP Accsee Key는 `/workspaces/Psychological-counseling-researching/.key/testprojects-453622-d1f78fcce8b7.json`에 있는 JSON파일을 사용하여야한다.
[ **제시** ] : Processing (실행) 표시창도 출력 하여야한다.


1. **(구현 완료) 텍스트 데이터 파일 정렬후 BigQuery에 저장**
    *Schema(INDEX:INTEGER, FILEPATH:STRING, SPEAKER:STRING, CONTENT: STRING)* 으로 구성 된 데이터를 BigQuery Table에 저장.
    **구상된 Table** : sql```SELECT * FROM `testprojects-453622.Psychological_counseling_data.all_raw` ```

2. **(구현 완료) 텍스트 내용 및 파일 정렬 데이터 BigQuery Table에 저장**
    *Schema(index:INTEGER, FilePath:STRING, Speaker:STRING, Content:STRING, FileName:STRING, prefix:STRING, topic:STRING, session_no:INTEGER, stage:STRING, file_code:STRING, file_code_row_count:INTEGER, speaker_row_count:INTEGER, file_code_ratio_pct:FLOAT, speaker_ratio_pct:FLOAT)* 으로 구성된 데이터를 BigQuery Table에 저장 하였음.
    **구상된 Table** : sql```SELECT * FROM `testprojects-453622.Psychological_counseling_data.processed_data` ```

3. **(구현 완료) BigQuery에 있는 텍스트 문장 분류**
    Kiwi를 가지고 Text Tokenizer진행후, *Schema(file_code : STRING, speaker : STRING, session_no : INTEGER, timeline_index : INTEGER, end_content_index : INTEGER, split_row_index : INTEGER, split_contents : STRING)* 으로 구성된 데이터를 BigQuery BigQuery에 morpheme_classification Table에 저장 하였음.

    * 문장 종료 규칙: 종결 어미(EF) 뒤에 종결 부호/줄임표/붙임표(SF/SE/SO)가 나올 때만 종료
    **구상된 Table** : sql```SELECT * FROM `testprojects-453622.Psychological_counseling_data.morpheme_classification` ```

4. **(구현 완료) BigQuery에 있는 텍스트 문장 임베딩 후 BigQuery에 저장**
    KM-BERT를 가지고 Text Embedding 진행후
    BigQuery에 morpheme_classification_kmbert_embedding 저장
    * KM-BERT의 Hidden Size 768
    **구상된 Table** : sql```SELECT * FROM `testprojects-453622.Psychological_counseling_data.morpheme_classification_kmbert_embedding` ```

5. **(구현 완료) BigQuery에 있는 텍스트 문장 임베딩 후 BigQuery에 저장**
    Gemini API에서 Gemini Embedding 1를 가지고 Text Embedding 진행후
    BigQuery에 morpheme_classification_gemini_embedding 저장
    * Gemini Embedding 1은 Gemini API Docs에서 `gemini-embedding-001` 코드명으로 사용.
    * Gemini Embedding 1의 Hidden Size 128 - 3072

6. **BigQuery 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (Gemini Embedding 1)**
    임베딩 벡터 시계열(Timeline_index)을 활용하여 발화자 간 감정 거리(Euclidean/Cosine Distance) 및 가가-각성(V-A) 궤적 이동 속도 계산.
    * GNN 노드 피처 구성을 위한 인지적 고착(Rumination) 및 사고의 비약(Flight of ideas) 지표 정량화
    * DataFrame(Gota) 활용 시계열 윈도우(Sliding Window) 크기를 3-turns 단위로 설정.
    * Go-Echarts를 이용해 시간축에 따른 감정 기복률(Emotional Volatility Rate) 시계열 꺾은선 그래프(Line Chart) 출력 (정상군 vs 우울군/불안장애군 대조 시각화).

7. **BigQuery 임베딩 기반 의미적 문단 감정 기복 분석 시각화 (KM-BERT Embedding)**
    임베딩 벡터 시계열(Timeline_index)을 활용하여 발화자 간 감정 거리(Euclidean/Cosine Distance) 및 가가-각성(V-A) 궤적 이동 속도 계산.
    * GNN 노드 피처 구성을 위한 인지적 고착(Rumination) 및 사고의 비약(Flight of ideas) 지표 정량화
    * DataFrame(Gota) 활용 시계열 윈도우(Sliding Window) 크기를 3-turns 단위로 설정.
    * Go-Echarts를 이용해 시간축에 따른 감정 기복률(Emotional Volatility Rate) 시계열 꺾은선 그래프(Line Chart) 출력 (정상군 vs 우울군/불안장애군 대조 시각화).


8. Multi-view Embedding 기법
    1. **임베딩 Sinking 및 결합 (Early Fusion):** 
        BigQuery에 적재된 `morpheme_classification_kmbert_embedding`과 `morpheme_classification_gemini_embedding`을 JOIN하여, 병리-문맥 복합 벡터(Concatenated Vector)를 생성.

    2. **교차 검증 및 평가 (Model Evaluation):**
        * 모델 A: KM-BERT 임베딩만 사용한 GNN
        * 모델 B: Gemini 임베딩만 사용한 GNN
        * 모델 C: 두 임베딩을 결합(또는 비교)한 Ensemble GNN
        * 위 3가지 모델의 F1-Score를 비교하여, 복합/융합 심리 장애(예: Bipolar, 조현병의 연상 이완 등) 탐지에 있어 다중 차원 임베딩이 얼마나 정확도를 향상시키는지 정량적으로 증명.