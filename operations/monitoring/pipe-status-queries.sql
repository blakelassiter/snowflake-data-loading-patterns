-- ============================================================================
-- Pipe Status Queries
-- ============================================================================
-- Monitor Snowpipe health, pending files, and execution state.

-- ----------------------------------------------------------------------------
-- Quick pipe health check
-- ----------------------------------------------------------------------------

-- Single pipe status
SELECT SYSTEM$PIPE_STATUS('raw.events_pipe');

-- Parse status JSON for readability
SELECT 
    'raw.events_pipe' as pipe_name,
    PARSE_JSON(SYSTEM$PIPE_STATUS('raw.events_pipe')):executionState::STRING as state,
    PARSE_JSON(SYSTEM$PIPE_STATUS('raw.events_pipe')):pendingFileCount::INTEGER as pending_files,
    PARSE_JSON(SYSTEM$PIPE_STATUS('raw.events_pipe')):lastReceivedMessageTimestamp::TIMESTAMP as last_message,
    PARSE_JSON(SYSTEM$PIPE_STATUS('raw.events_pipe')):notificationChannelName::STRING as notification_channel;

-- ----------------------------------------------------------------------------
-- All pipes status
-- ----------------------------------------------------------------------------

SHOW PIPES;

-- Get status for all pipes in a schema
SELECT 
    pipe_name,
    pipe_schema,
    pipe_catalog as database_name,
    definition,
    is_autoingest_enabled,
    notification_channel,
    owner,
    PARSE_JSON(SYSTEM$PIPE_STATUS(
        pipe_catalog || '.' || pipe_schema || '.' || pipe_name
    )):executionState::STRING as execution_state,
    PARSE_JSON(SYSTEM$PIPE_STATUS(
        pipe_catalog || '.' || pipe_schema || '.' || pipe_name
    )):pendingFileCount::INTEGER as pending_files
FROM information_schema.pipes
WHERE pipe_schema = 'RAW';

-- ----------------------------------------------------------------------------
-- Pipes with issues
-- ----------------------------------------------------------------------------

-- Find stalled or paused pipes
WITH pipe_status AS (
    SELECT 
        pipe_name,
        pipe_schema,
        pipe_catalog as database_name,
        SYSTEM$PIPE_STATUS(pipe_catalog || '.' || pipe_schema || '.' || pipe_name) as status_json
    FROM information_schema.pipes
)
SELECT 
    database_name,
    pipe_schema,
    pipe_name,
    PARSE_JSON(status_json):executionState::STRING as state,
    PARSE_JSON(status_json):pendingFileCount::INTEGER as pending_files,
    PARSE_JSON(status_json):lastReceivedMessageTimestamp::TIMESTAMP as last_message
FROM pipe_status
WHERE PARSE_JSON(status_json):executionState::STRING IN ('PAUSED', 'STALLED')
   OR PARSE_JSON(status_json):pendingFileCount::INTEGER > 1000;

-- ----------------------------------------------------------------------------
-- Pipe load history
-- ----------------------------------------------------------------------------

-- Recent loads via specific pipe
SELECT 
    pipe_name,
    file_name,
    status,
    row_count,
    first_error_message,
    last_load_time
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('hour', -24, current_timestamp())
))
WHERE pipe_name IS NOT NULL
ORDER BY last_load_time DESC;

-- ----------------------------------------------------------------------------
-- Pipe throughput analysis
-- ----------------------------------------------------------------------------

SELECT 
    pipe_name,
    DATE_TRUNC('hour', last_load_time) as hour,
    COUNT(*) as files_loaded,
    SUM(row_count) as rows_loaded,
    AVG(row_count) as avg_rows_per_file,
    COUNT_IF(status != 'LOADED') as failed_files
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -1, current_timestamp())
))
WHERE pipe_name IS NOT NULL
GROUP BY pipe_name, hour
ORDER BY hour DESC;

-- ----------------------------------------------------------------------------
-- Pipe usage history (Account Usage - credits consumed)
-- ----------------------------------------------------------------------------

-- Daily credit consumption by pipe
SELECT 
    pipe_name,
    DATE_TRUNC('day', start_time) as day,
    SUM(credits_used) as daily_credits,
    SUM(files_inserted) as files_loaded,
    SUM(bytes_inserted) / 1e9 as gb_loaded,
    ROUND(SUM(credits_used) / NULLIF(SUM(files_inserted), 0), 6) as credits_per_file
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY pipe_name, day
ORDER BY pipe_name, day DESC;

-- ----------------------------------------------------------------------------
-- Pipe cost trends
-- ----------------------------------------------------------------------------

SELECT 
    pipe_name,
    DATE_TRUNC('week', start_time) as week,
    SUM(credits_used) as weekly_credits,
    SUM(files_inserted) as weekly_files,
    SUM(bytes_inserted) / 1e9 as weekly_gb
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time > dateadd('month', -3, current_date())
GROUP BY pipe_name, week
ORDER BY pipe_name, week DESC;

-- ----------------------------------------------------------------------------
-- Notification channel health
-- ----------------------------------------------------------------------------

-- Check if events are being received
WITH pipe_messages AS (
    SELECT 
        pipe_name,
        PARSE_JSON(SYSTEM$PIPE_STATUS(
            pipe_catalog || '.' || pipe_schema || '.' || pipe_name
        )):lastReceivedMessageTimestamp::TIMESTAMP as last_message
    FROM information_schema.pipes
    WHERE is_autoingest_enabled = 'YES'
)
SELECT 
    pipe_name,
    last_message,
    DATEDIFF('minute', last_message, CURRENT_TIMESTAMP()) as minutes_since_last_message,
    CASE 
        WHEN DATEDIFF('minute', last_message, CURRENT_TIMESTAMP()) > 60 THEN 'WARNING'
        WHEN DATEDIFF('minute', last_message, CURRENT_TIMESTAMP()) > 15 THEN 'CHECK'
        ELSE 'OK'
    END as health_status
FROM pipe_messages
ORDER BY minutes_since_last_message DESC;

-- ----------------------------------------------------------------------------
-- Pending file investigation
-- ----------------------------------------------------------------------------

-- High pending file count may indicate:
-- 1. Schema mismatch between files and table
-- 2. Malformed files blocking the queue
-- 3. Insufficient processing capacity

-- Check for patterns in failing files
SELECT 
    pipe_name,
    REGEXP_SUBSTR(file_name, '[^/]+$') as file_name_only,
    status,
    first_error_message,
    COUNT(*) as occurrence
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('hour', -24, current_timestamp())
))
WHERE pipe_name IS NOT NULL
  AND status != 'LOADED'
GROUP BY pipe_name, file_name_only, status, first_error_message
ORDER BY occurrence DESC;

-- ----------------------------------------------------------------------------
-- Pipe management operations
-- ----------------------------------------------------------------------------

-- Pause pipe
ALTER PIPE raw.events_pipe SET PIPE_EXECUTION_PAUSED = TRUE;

-- Resume pipe
ALTER PIPE raw.events_pipe SET PIPE_EXECUTION_PAUSED = FALSE;

-- Refresh pipe (load files not yet loaded)
ALTER PIPE raw.events_pipe REFRESH;

-- Refresh specific prefix
ALTER PIPE raw.events_pipe REFRESH PREFIX = '2025/01/';
