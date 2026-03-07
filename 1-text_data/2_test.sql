-- index	:	INT64   :   상담 시계열 순서
-- FilePath	:	STRING  :   상담 RAW 파일명
-- Speaker	:	STRING  :   화자
-- Content	:	STRING  :   맨트
-- FileName	:	STRING  :   파일명
-- prefix	:	STRING  :   resource
-- topic	:	STRING  :   이상심리 분류 케이스
-- session_no	:	INT64   :   상담횟수
-- stage	:	STRING  :   check
-- file_code	:	STRING  :   상담 Code
-- file_code_row_count	:	INT64   :   상담 맨트 행 개수
-- speaker_row_count	:	INT64   :   화자 멘트 행 개수
-- file_code_ratio_pct	:	FLOAT64 :   테이블 중 상담 Code 비중
-- speaker_ratio_pct	:	FLOAT64 :   테이블 중 화자 멘트 비중

-- 목표 1 : file_code에 있는 고유한 값을 출력하시오
SELECT DISTINCT file_code
FROM `testprojects-453622.Psychological_counseling_data.processed_data`;

-- 목표 2 : file_code마다 최대 상담 횟수를 출력하시오
SELECT
  file_code,
  MAX(session_no) AS max_session_no
FROM
  `testprojects-453622.Psychological_counseling_data.processed_data`
WHERE
    session_no>=2
GROUP BY
  file_code
ORDER BY 
    max_session_no ASC;


-- 예시 : 특정 상담 Code에서 첫번째 상담 데이터를 출력하시오.
-- SELECT 
--     `index`, 
--     `Speaker`, 
--     `Content`
-- FROM 
--     `Psychological_counseling_data.processed_data`
-- WHERE 
--     file_code="A131" AND 
--     session_no=1
-- ORDER BY 
--     index ASC;

