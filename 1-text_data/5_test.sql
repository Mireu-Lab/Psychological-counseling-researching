SELECT 
    file_code, 
    session_no, 
    timeline_index, 
    split_row_index, 
    split_contents, 
    content_embedding
FROM 
    `Psychological_counseling_data`.morpheme_classification_kmbert_embedding
ORDER BY 
    file_code, session_no, timeline_index, split_row_index