-- ============================================================================
-- File Sizing: Distribution Query
-- ============================================================================
-- Snowflake recommends 100-250MB compressed files for optimal loading.
-- These queries help analyze your file sizes before loading.

-- ----------------------------------------------------------------------------
-- Check file sizes in a stage
-- ----------------------------------------------------------------------------

-- List files (captures size in bytes)
LIST @external_stage/data/;

-- Analyze size distribution from LIST results
SELECT 
    CASE 
        WHEN "size" < 1000000 THEN 'Under 1MB'
        WHEN "size" BETWEEN 1000000 AND 10000000 THEN '1-10MB'
        WHEN "size" BETWEEN 10000000 AND 100000000 THEN '10-100MB'
        WHEN "size" BETWEEN 100000000 AND 250000000 THEN '100-250MB (optimal)'
        WHEN "size" BETWEEN 250000000 AND 500000000 THEN '250-500MB'
        ELSE 'Over 500MB'
    END as size_bucket,
    COUNT(*) as file_count,
    ROUND(SUM("size") / 1e9, 2) as total_gb,
    ROUND(AVG("size") / 1e6, 2) as avg_mb,
    ROUND(MIN("size") / 1e6, 2) as min_mb,
    ROUND(MAX("size") / 1e6, 2) as max_mb
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
GROUP BY size_bucket
ORDER BY 
    CASE size_bucket
        WHEN 'Under 1MB' THEN 1
        WHEN '1-10MB' THEN 2
        WHEN '10-100MB' THEN 3
        WHEN '100-250MB (optimal)' THEN 4
        WHEN '250-500MB' THEN 5
        ELSE 6
    END;

-- ----------------------------------------------------------------------------
-- Detailed file analysis
-- ----------------------------------------------------------------------------

-- Run LIST first
LIST @external_stage/data/;

-- Then analyze
WITH file_data AS (
    SELECT 
        "name" as file_path,
        "size" as size_bytes,
        "size" / 1e6 as size_mb,
        "last_modified" as modified_at
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
)
SELECT 
    COUNT(*) as total_files,
    ROUND(SUM(size_bytes) / 1e9, 2) as total_gb,
    ROUND(AVG(size_mb), 2) as avg_mb,
    ROUND(MEDIAN(size_mb), 2) as median_mb,
    ROUND(MIN(size_mb), 2) as min_mb,
    ROUND(MAX(size_mb), 2) as max_mb,
    COUNT_IF(size_mb < 10) as files_under_10mb,
    COUNT_IF(size_mb BETWEEN 100 AND 250) as files_optimal_range,
    COUNT_IF(size_mb > 500) as files_over_500mb
FROM file_data;

-- ----------------------------------------------------------------------------
-- Identify problematic small files
-- ----------------------------------------------------------------------------

LIST @external_stage/data/;

SELECT 
    "name" as file_path,
    ROUND("size" / 1e6, 2) as size_mb
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "size" < 10000000  -- Under 10MB
ORDER BY "size"
LIMIT 100;

-- ----------------------------------------------------------------------------
-- Check files by date partition
-- ----------------------------------------------------------------------------

-- Useful when files are organized by date
LIST @external_stage/data/2025/01/;

WITH file_data AS (
    SELECT 
        SPLIT_PART("name", '/', 3) as year,
        SPLIT_PART("name", '/', 4) as month,
        SPLIT_PART("name", '/', 5) as day,
        "size" as size_bytes
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
)
SELECT 
    year,
    month,
    day,
    COUNT(*) as file_count,
    ROUND(SUM(size_bytes) / 1e6, 2) as total_mb,
    ROUND(AVG(size_bytes) / 1e6, 2) as avg_mb
FROM file_data
GROUP BY year, month, day
ORDER BY year, month, day;

-- ----------------------------------------------------------------------------
-- Recommendations based on analysis
-- ----------------------------------------------------------------------------

/*
File size impacts:

UNDER 1MB:
- High per-file overhead
- Snowpipe: Each file triggers separate load
- COPY INTO: Many small files = many metadata operations
- Fix: Batch files upstream before delivery

1-10MB:
- Still suboptimal but workable
- Consider combining files in ETL pipeline
- If stuck with small files, ensure warehouse isn't oversized

10-100MB:
- Acceptable range
- Good for Snowpipe workloads
- COPY INTO handles these efficiently

100-250MB (OPTIMAL):
- Best balance of parallelism and overhead
- Target this range for batch loads
- Snowflake's internal optimizations work best here

250-500MB:
- Still good, slight reduction in parallelism
- Fine for most workloads

OVER 500MB:
- Reduced parallelism (fewer threads per file)
- Consider splitting large files
- May be fine for daily batch of few large files
*/

-- ----------------------------------------------------------------------------
-- Monitor file sizes over time
-- ----------------------------------------------------------------------------

-- Track if file sizes are drifting from optimal
-- Requires capturing LIST results to a table periodically

CREATE TABLE IF NOT EXISTS ops.file_size_history (
    check_date DATE,
    stage_name STRING,
    file_count INTEGER,
    avg_size_mb FLOAT,
    median_size_mb FLOAT,
    pct_under_10mb FLOAT,
    pct_optimal FLOAT
);

-- Insert daily snapshot (run via task or scheduled job)
LIST @external_stage/data/;

INSERT INTO ops.file_size_history
WITH file_data AS (
    SELECT "size" / 1e6 as size_mb
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
)
SELECT 
    CURRENT_DATE() as check_date,
    '@external_stage/data/' as stage_name,
    COUNT(*) as file_count,
    AVG(size_mb) as avg_size_mb,
    MEDIAN(size_mb) as median_size_mb,
    COUNT_IF(size_mb < 10) * 100.0 / COUNT(*) as pct_under_10mb,
    COUNT_IF(size_mb BETWEEN 100 AND 250) * 100.0 / COUNT(*) as pct_optimal
FROM file_data;

-- View trends
SELECT * FROM ops.file_size_history
ORDER BY check_date DESC;
