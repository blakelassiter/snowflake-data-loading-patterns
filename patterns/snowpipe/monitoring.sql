-- ============================================================================
-- Snowpipe: Monitoring Queries
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Pipe status check
-- ----------------------------------------------------------------------------

-- Quick status check for a specific pipe
SELECT SYSTEM$PIPE_STATUS('raw.events_pipe');

-- Returns JSON with:
-- executionState: RUNNING, PAUSED, STALLED
-- pendingFileCount: Files queued for loading
-- notificationChannelName: SQS/Pub/Sub queue
-- lastReceivedMessageTimestamp: Last event received

-- Parse the JSON for easier reading
SELECT 
    PARSE_JSON(SYSTEM$PIPE_STATUS('raw.events_pipe')):executionState::STRING as execution_state,
    PARSE_JSON(SYSTEM$PIPE_STATUS('raw.events_pipe')):pendingFileCount::INTEGER as pending_files,
    PARSE_JSON(SYSTEM$PIPE_STATUS('raw.events_pipe')):lastReceivedMessageTimestamp::TIMESTAMP as last_message;

-- ----------------------------------------------------------------------------
-- List all pipes with status
-- ----------------------------------------------------------------------------

SHOW PIPES;

-- Get status for all pipes in a schema
SELECT 
    pipe_name,
    pipe_schema,
    is_enabled,
    PARSE_JSON(SYSTEM$PIPE_STATUS(pipe_catalog || '.' || pipe_schema || '.' || pipe_name)):executionState::STRING as state,
    PARSE_JSON(SYSTEM$PIPE_STATUS(pipe_catalog || '.' || pipe_schema || '.' || pipe_name)):pendingFileCount::INTEGER as pending
FROM information_schema.pipes
WHERE pipe_schema = 'RAW';

-- ----------------------------------------------------------------------------
-- Copy history for Snowpipe loads
-- ----------------------------------------------------------------------------

-- Recent load history for a table
SELECT 
    pipe_name,
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
    pipe_name,
    status,
    COUNT(*) as file_count,
    SUM(row_count) as total_rows,
    MIN(last_load_time) as earliest_load,
    MAX(last_load_time) as latest_load
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -7, current_timestamp())
))
GROUP BY pipe_name, status
ORDER BY pipe_name, status;

-- ----------------------------------------------------------------------------
-- Failed loads investigation
-- ----------------------------------------------------------------------------

SELECT 
    pipe_name,
    file_name,
    status,
    first_error_message,
    first_error_line_number,
    first_error_column_name,
    last_load_time
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -7, current_timestamp())
))
WHERE status != 'LOADED'
ORDER BY last_load_time DESC;

-- ----------------------------------------------------------------------------
-- Load latency analysis
-- ----------------------------------------------------------------------------

-- Check how long files take from creation to load completion
-- Note: Requires metadata capture during load or external file metadata

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
ORDER BY load_hour;

-- ----------------------------------------------------------------------------
-- Account-level pipe usage (Account Usage views)
-- ----------------------------------------------------------------------------

-- Pipe usage history (last 365 days, 45-minute latency)
SELECT 
    pipe_name,
    DATE_TRUNC('day', start_time) as load_date,
    SUM(credits_used) as daily_credits,
    SUM(files_inserted) as daily_files,
    SUM(bytes_inserted) as daily_bytes
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY pipe_name, load_date
ORDER BY pipe_name, load_date DESC;

-- ----------------------------------------------------------------------------
-- Pipe credit consumption trends
-- ----------------------------------------------------------------------------

SELECT 
    pipe_name,
    DATE_TRUNC('week', start_time) as week,
    SUM(credits_used) as weekly_credits,
    SUM(files_inserted) as weekly_files,
    ROUND(SUM(credits_used) / NULLIF(SUM(files_inserted), 0), 6) as credits_per_file
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time > dateadd('month', -3, current_date())
GROUP BY pipe_name, week
ORDER BY pipe_name, week DESC;

-- ----------------------------------------------------------------------------
-- Compare Snowpipe vs COPY INTO costs
-- ----------------------------------------------------------------------------

-- Snowpipe credits
SELECT 
    'Snowpipe' as method,
    DATE_TRUNC('day', start_time) as load_date,
    SUM(credits_used) as credits
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY load_date

UNION ALL

-- Warehouse credits for COPY INTO (estimate based on query history)
SELECT 
    'COPY INTO' as method,
    DATE_TRUNC('day', start_time) as load_date,
    SUM(credits_used_cloud_services) + 
    (SUM(execution_time) / 3600000.0 * 2) as credits  -- Rough estimate for Medium warehouse
FROM snowflake.account_usage.query_history
WHERE query_type = 'COPY'
  AND start_time > dateadd('day', -30, current_date())
GROUP BY load_date

ORDER BY load_date, method;
