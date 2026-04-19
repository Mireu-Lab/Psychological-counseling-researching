
-- index    :   INTEGER
-- FilePath :   STRING
-- Speaker  :   STRING
-- Content  :   STRING

-- # 목표1: FilePath 마다 Index가 0-n까지 조직 되어있으므로 FilePath기준으로 Index 내림차순으로 작성 하여야함.

SELECT * 
FROM `testprojects-453622.Psychological_counseling_data.all_raw`
ORDER BY FilePath ASC, Index DESC
-- LIMIT 1000