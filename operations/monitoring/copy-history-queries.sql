-- ============================================================================
-- COPY History Queries
-- ============================================================================
-- Monitor COPY INTO operations and Snowpipe loads using information schema
-- and account usage views.

-- ----------------------------------------------------------------------------
-- Recent load history (Information Schema - real-time)
-- ----------------------------------------------------------------------------

-- Load history for specific table (last 24 hours)
SELECT 
    table_name,
    file_name,
    stage_location,
    status,
    row_count,
    row_parsed,
    first_error_message,
    first_error_line_number,
    last_load_time
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('hour', -24, current_timestamp())
))
ORDER BY last_load_time DESC;

-- ----------------------------------------------------------------------------
-- Load status summary
-- ----------------------------------------------------------------------------

SELECT 
    status,
    COUNT(*) as file_count,
    SUM(row_count) as total_rows_loaded,
    SUM(row_parsed) as total_rows_parsed,
    SUM(row_parsed) - SUM(row_count) as rows_skipped
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -7, current_timestamp())
))
GROUP BY status;

-- ----------------------------------------------------------------------------
-- Failed loads investigation
-- ----------------------------------------------------------------------------

SELECT 
    file_name,
    status,
    first_error_message,
    first_error_line_number,
    first_error_column_name,
    row_parsed,
    row_count,
    last_load_time
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -7, current_timestamp())
))
WHERE status != 'LOADED'
ORDER BY last_load_time DESC;

-- ----------------------------------------------------------------------------
-- Hourly load volume
-- ----------------------------------------------------------------------------

SELECT 
    DATE_TRUNC('hour', last_load_time) as load_hour,
    COUNT(*) as files_loaded,
    SUM(row_count) as rows_loaded,
    AVG(row_count) as avg_rows_per_file
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -1, current_timestamp())
))
WHERE status = 'LOADED'
GROUP BY load_hour
ORDER BY load_hour DESC;

-- ----------------------------------------------------------------------------
-- Account-level copy history (Account Usage - 45 min latency)
-- ----------------------------------------------------------------------------

-- All COPY INTO activity across account
SELECT 
    table_name,
    table_schema,
    table_catalog as database_name,
    file_name,
    status,
    row_count,
    last_load_time
FROM snowflake.account_usage.copy_history
WHERE last_load_time > dateadd('day', -7, current_date())
ORDER BY last_load_time DESC
LIMIT 1000;

-- ----------------------------------------------------------------------------
-- Daily load summary by table
-- ----------------------------------------------------------------------------

SELECT 
    table_catalog as database_name,
    table_schema,
    table_name,
    DATE_TRUNC('day', last_load_time) as load_date,
    COUNT(*) as files_loaded,
    SUM(row_count) as total_rows,
    COUNT_IF(status != 'LOADED') as failed_files
FROM snowflake.account_usage.copy_history
WHERE last_load_time > dateadd('day', -30, current_date())
GROUP BY database_name, table_schema, table_name, load_date
ORDER BY load_date DESC, total_rows DESC;

-- ----------------------------------------------------------------------------
-- Error pattern analysis
-- ----------------------------------------------------------------------------

-- Common error messages
SELECT 
    REGEXP_SUBSTR(first_error_message, '^[^:]+') as error_type,
    COUNT(*) as occurrence_count,
    COUNT(DISTINCT file_name) as affected_files,
    MAX(last_load_time) as most_recent
FROM snowflake.account_usage.copy_history
WHERE status != 'LOADED'
  AND last_load_time > dateadd('day', -30, current_date())
GROUP BY error_type
ORDER BY occurrence_count DESC;

-- ----------------------------------------------------------------------------
-- Duplicate file detection
-- ----------------------------------------------------------------------------

-- Files loaded multiple times (potential issue)
SELECT 
    file_name,
    table_name,
    COUNT(*) as load_count,
    MIN(last_load_time) as first_load,
    MAX(last_load_time) as last_load
FROM snowflake.account_usage.copy_history
WHERE last_load_time > dateadd('day', -30, current_date())
  AND status = 'LOADED'
GROUP BY file_name, table_name
HAVING COUNT(*) > 1
ORDER BY load_count DESC;

-- ----------------------------------------------------------------------------
-- Load SLA monitoring
-- ----------------------------------------------------------------------------

-- Check if loads are happening on schedule
-- Adjust expected_interval based on your load frequency

WITH load_times AS (
    SELECT 
        table_name,
        last_load_time,
        LAG(last_load_time) OVER (PARTITION BY table_name ORDER BY last_load_time) as prev_load_time
    FROM TABLE(information_schema.copy_history(
        table_name => 'raw.events',
        start_time => dateadd('day', -1, current_timestamp())
    ))
    WHERE status = 'LOADED'
)
SELECT 
    table_name,
    last_load_time,
    prev_load_time,
    DATEDIFF('minute', prev_load_time, last_load_time) as minutes_since_last,
    CASE 
        WHEN DATEDIFF('minute', prev_load_time, last_load_time) > 60 THEN 'LATE'
        ELSE 'OK'
    END as sla_status
FROM load_times
WHERE prev_load_time IS NOT NULL
ORDER BY last_load_time DESC;

-- ----------------------------------------------------------------------------
-- Data freshness check
-- ----------------------------------------------------------------------------

-- When was data last loaded to each table?
SELECT 
    table_catalog as database_name,
    table_schema,
    table_name,
    MAX(last_load_time) as last_load,
    DATEDIFF('hour', MAX(last_load_time), CURRENT_TIMESTAMP()) as hours_since_load
FROM snowflake.account_usage.copy_history
WHERE last_load_time > dateadd('day', -7, current_date())
  AND status = 'LOADED'
GROUP BY database_name, table_schema, table_name
ORDER BY hours_since_load DESC;
