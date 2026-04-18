WITH Target_Processed_Data AS (
    SELECT 
        file_code, 
        ANY_VALUE(stage) AS stage 
    FROM 
        testprojects-453622.Psychological_counseling_data.processed_data 
    GROUP BY file_code
) SELECT 
    e.file_code, 
    e.timeline_index, 
    e.embedding, 
    p.stage 
FROM 
    testprojects-453622.Psychological_counseling_data.morpheme_classification_gemini_embedding AS e 
    JOIN Target_Processed_Data AS p ON e.file_code = p.file_code 
ORDER BY e.file_code, e.timeline_index ASC