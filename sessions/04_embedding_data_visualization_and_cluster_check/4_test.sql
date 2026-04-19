-- # Column Schema
SELECT column_name, data_type
FROM `testprojects-453622.Psychological_counseling_data.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'morpheme_classification'
ORDER BY ordinal_position

-- ## 실행 결과

-- |컬럼명|데이터타입|
-- |:---:|:---:|
-- |file_code|STRING|
-- |speaker|STRING|
-- |session_no|INT64|
-- |timeline_index|INT64|
-- |end_content_index|INT64|
-- |split_row_index|INT64|
-- |split_contents|STRING|

-- 예시 데이터셋
SELECT * 
FROM `Psychological_counseling_data`.morpheme_classification_kmbert_embedding 
LIMIT 100

