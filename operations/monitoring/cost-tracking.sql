-- ============================================================================
-- Cost Tracking Queries
-- ============================================================================
-- Track credit consumption across all data loading methods.

-- ----------------------------------------------------------------------------
-- Daily loading costs by method
-- ----------------------------------------------------------------------------

WITH snowpipe_costs AS (
    SELECT 
        DATE_TRUNC('day', start_time) as day,
        'Snowpipe' as method,
        SUM(credits_used) as credits
    FROM snowflake.account_usage.pipe_usage_history
    WHERE start_time > dateadd('day', -30, current_date())
    GROUP BY day
),
streaming_costs AS (
    SELECT 
        DATE_TRUNC('day', start_time) as day,
        'Streaming' as method,
        SUM(credits_used) as credits
    FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
    WHERE start_time > dateadd('day', -30, current_date())
    GROUP BY day
),
copy_into_costs AS (
    -- COPY INTO uses warehouse compute
    -- This estimates based on query execution time
    SELECT 
        DATE_TRUNC('day', start_time) as day,
        'COPY INTO' as method,
        -- Rough estimate: execution_time in ms, warehouse credits vary by size
        SUM(execution_time / 3600000.0 * 2) as credits  -- Assumes ~2 credits/hr (Medium)
    FROM snowflake.account_usage.query_history
    WHERE query_type = 'COPY'
      AND start_time > dateadd('day', -30, current_date())
    GROUP BY day
)
SELECT 
    day,
    method,
    ROUND(credits, 4) as credits
FROM (
    SELECT * FROM snowpipe_costs
    UNION ALL
    SELECT * FROM streaming_costs
    UNION ALL
    SELECT * FROM copy_into_costs
)
ORDER BY day DESC, method;

-- ----------------------------------------------------------------------------
-- Monthly cost summary
-- ----------------------------------------------------------------------------

WITH all_costs AS (
    SELECT 'Snowpipe' as method, start_time, credits_used as credits
    FROM snowflake.account_usage.pipe_usage_history
    WHERE start_time > dateadd('month', -6, current_date())
    
    UNION ALL
    
    SELECT 'Streaming' as method, start_time, credits_used as credits
    FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
    WHERE start_time > dateadd('month', -6, current_date())
)
SELECT 
    DATE_TRUNC('month', start_time) as month,
    method,
    ROUND(SUM(credits), 2) as monthly_credits
FROM all_costs
GROUP BY month, method
ORDER BY month DESC, method;

-- ----------------------------------------------------------------------------
-- Snowpipe cost per table
-- ----------------------------------------------------------------------------

-- Join pipe usage with pipe definitions to get target tables
WITH pipe_tables AS (
    SELECT 
        pipe_name,
        REGEXP_SUBSTR(definition, 'INTO\\s+([\\w.]+)', 1, 1, 'ie', 1) as target_table
    FROM information_schema.pipes
)
SELECT 
    pt.target_table,
    SUM(puh.credits_used) as total_credits,
    SUM(puh.files_inserted) as total_files,
    SUM(puh.bytes_inserted) / 1e9 as total_gb,
    ROUND(SUM(puh.credits_used) / NULLIF(SUM(puh.files_inserted), 0), 6) as credits_per_file
FROM snowflake.account_usage.pipe_usage_history puh
JOIN pipe_tables pt ON puh.pipe_name = pt.pipe_name
WHERE puh.start_time > dateadd('day', -30, current_date())
GROUP BY pt.target_table
ORDER BY total_credits DESC;

-- ----------------------------------------------------------------------------
-- Cost efficiency metrics
-- ----------------------------------------------------------------------------

-- Snowpipe: credits per file and per GB
SELECT 
    pipe_name,
    SUM(credits_used) as total_credits,
    SUM(files_inserted) as total_files,
    SUM(bytes_inserted) / 1e9 as total_gb,
    ROUND(SUM(credits_used) / NULLIF(SUM(files_inserted), 0), 6) as credits_per_file,
    ROUND(SUM(credits_used) / NULLIF(SUM(bytes_inserted) / 1e9, 0), 4) as credits_per_gb
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY pipe_name
ORDER BY total_credits DESC;

-- Streaming: credits per million rows
SELECT 
    table_name,
    SUM(credits_used) as total_credits,
    SUM(num_rows_migrated) as total_rows,
    SUM(num_bytes_migrated) / 1e9 as total_gb,
    ROUND(SUM(credits_used) / NULLIF(SUM(num_rows_migrated) / 1e6, 0), 4) as credits_per_million_rows
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY table_name
ORDER BY total_credits DESC;

-- ----------------------------------------------------------------------------
-- Cost anomaly detection
-- ----------------------------------------------------------------------------

-- Day-over-day cost changes
WITH daily_costs AS (
    SELECT 
        DATE_TRUNC('day', start_time) as day,
        SUM(credits_used) as credits
    FROM snowflake.account_usage.pipe_usage_history
    WHERE start_time > dateadd('day', -14, current_date())
    GROUP BY day
),
with_prev AS (
    SELECT 
        day,
        credits,
        LAG(credits) OVER (ORDER BY day) as prev_day_credits
    FROM daily_costs
)
SELECT 
    day,
    credits,
    prev_day_credits,
    credits - prev_day_credits as change,
    ROUND((credits - prev_day_credits) / NULLIF(prev_day_credits, 0) * 100, 1) as pct_change
FROM with_prev
WHERE prev_day_credits IS NOT NULL
  AND ABS(credits - prev_day_credits) / NULLIF(prev_day_credits, 0) > 0.5  -- >50% change
ORDER BY day DESC;

-- ----------------------------------------------------------------------------
-- Warehouse costs for COPY INTO
-- ----------------------------------------------------------------------------

-- Detailed COPY INTO cost by warehouse
SELECT 
    warehouse_name,
    warehouse_size,
    DATE_TRUNC('day', start_time) as day,
    COUNT(*) as copy_operations,
    SUM(execution_time) / 1000 as total_execution_seconds,
    -- Credit rates vary by warehouse size
    SUM(execution_time) / 3600000.0 * 
        CASE warehouse_size
            WHEN 'X-Small' THEN 1
            WHEN 'Small' THEN 2
            WHEN 'Medium' THEN 4
            WHEN 'Large' THEN 8
            WHEN 'X-Large' THEN 16
            ELSE 4  -- Default estimate
        END as estimated_credits
FROM snowflake.account_usage.query_history
WHERE query_type = 'COPY'
  AND start_time > dateadd('day', -30, current_date())
GROUP BY warehouse_name, warehouse_size, day
ORDER BY day DESC, estimated_credits DESC;

-- ----------------------------------------------------------------------------
-- Total data loading spend
-- ----------------------------------------------------------------------------

SELECT 
    DATE_TRUNC('month', start_time) as month,
    'Snowpipe' as category,
    SUM(credits_used) as credits
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time > dateadd('month', -6, current_date())
GROUP BY month

UNION ALL

SELECT 
    DATE_TRUNC('month', start_time) as month,
    'Streaming' as category,
    SUM(credits_used) as credits
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('month', -6, current_date())
GROUP BY month

UNION ALL

SELECT 
    DATE_TRUNC('month', start_time) as month,
    'COPY Warehouse' as category,
    -- Estimate based on query time
    SUM(execution_time / 3600000.0 * 4) as credits
FROM snowflake.account_usage.query_history
WHERE query_type = 'COPY'
  AND start_time > dateadd('month', -6, current_date())
GROUP BY month

ORDER BY month DESC, category;

-- ----------------------------------------------------------------------------
-- Cost optimization opportunities
-- ----------------------------------------------------------------------------

-- Small file inefficiency (Snowpipe)
SELECT 
    pipe_name,
    AVG(bytes_inserted / NULLIF(files_inserted, 0)) / 1e6 as avg_mb_per_file,
    SUM(files_inserted) as total_files,
    SUM(credits_used) as total_credits,
    CASE 
        WHEN AVG(bytes_inserted / NULLIF(files_inserted, 0)) < 10000000 THEN 'SMALL FILES - Consider batching'
        WHEN AVG(bytes_inserted / NULLIF(files_inserted, 0)) < 100000000 THEN 'MODERATE - Acceptable'
        ELSE 'GOOD - Well-sized files'
    END as recommendation
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time > dateadd('day', -7, current_date())
GROUP BY pipe_name
ORDER BY avg_mb_per_file ASC;
