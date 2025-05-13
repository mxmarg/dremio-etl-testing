SELECT
    "partition",
    partition_date,
    partition_hour,
    SUM(record_count) AS total_record_count,
    SUM(file_size_in_bytes)/1024.0/1024.0/1024.0 AS total_size_in_gb,
    COUNT(*) AS total_file_count
FROM (
    SELECT 
        DATE_ADD('1970-01-01', CAST(REGEXP_EXTRACT("partition", 'date=(\d+),', 1) AS INTEGER)) AS partition_date,
        CAST(REPLACE(SUBSTRING("partition" FROM 19), '}', '') AS INTEGER) as partition_hour,
        "partition", 
        record_count,
        file_size_in_bytes
    FROM table(table_files('<INSERT_SOURCE_PATH_HERE>."<INSERT_RUN_ID_HERE>".processed_table'))
)
GROUP BY 1, 2, 3
ORDER BY 2, 3