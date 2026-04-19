SELECT split_contents FROM `testprojects-453622.Psychological_counseling_data.morpheme_classification`WHERE split_contents IS NOT NULL




-- [Step 1] 복구 시점 확인을 위한 로그 조회
-- 이 쿼리를 먼저 실행하여 'creation_time'을 확인하십시오.
SELECT creation_time, query
FROM `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS
WHERE query LIKE '%morpheme_classification_gemini_embedding%'
  AND statement_type = 'CREATE_TABLE_AS_SELECT'
ORDER BY creation_time DESC LIMIT 5;

-- [Step 2] 실행 전 상태로 테이블 복구 (Rollback)
-- 주의: 이전 쿼리에서 'embedding' 컬럼을 누락하여 발생한 오류를 해결합니다.
DECLARE restore_ts TIMESTAMP DEFAULT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE); 

CREATE OR REPLACE TABLE `testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding` AS
SELECT *
FROM `testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding`
FOR SYSTEM_TIME AS OF restore_ts;

-- [Step 3] morpheme_classification_gemini_embedding에서 file_code, speaker, session_no, timeline_index, end_content_index, split_row_index, split_contents 중복시 1개의 행을 제외한 나머지 제거후 Teble에 다시 저장
CREATE OR REPLACE TABLE
  `testprojects-453622`.`Psychological_counseling_data`.`morpheme_classification_gemini_embedding` AS
SELECT
  file_code,
  speaker,
  session_no,
  timeline_index,
  end_content_index,
  split_row_index,
  split_contents,
  embedding,
  MODEL,
  dimension,
  created_at
FROM
  `testprojects-453622`.`Psychological_counseling_data`.`morpheme_classification_gemini_embedding`
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY file_code, speaker, session_no, timeline_index, end_content_index, split_row_index, split_contents ORDER BY created_at ) = 1;

