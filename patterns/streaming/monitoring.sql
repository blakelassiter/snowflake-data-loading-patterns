-- ============================================================================
-- Snowpipe Streaming: Monitoring Queries
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Streaming file migration history (Account Usage)
-- ----------------------------------------------------------------------------
-- When streaming data accumulates, Snowflake migrates it to optimized storage.
-- This view tracks those migrations.

SELECT 
    table_name,
    database_name,
    schema_name,
    start_time,
    end_time,
    credits_used,
    num_bytes_migrated,
    num_rows_migrated
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -1, current_date())
ORDER BY start_time DESC;

-- ----------------------------------------------------------------------------
-- Streaming credit consumption
-- ----------------------------------------------------------------------------

SELECT 
    DATE_TRUNC('hour', start_time) as hour,
    SUM(credits_used) as hourly_credits,
    SUM(num_rows_migrated) as rows_migrated,
    SUM(num_bytes_migrated) / 1e9 as gb_migrated
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -7, current_date())
GROUP BY hour
ORDER BY hour DESC;

-- ----------------------------------------------------------------------------
-- Daily streaming costs
-- ----------------------------------------------------------------------------

SELECT 
    DATE_TRUNC('day', start_time) as day,
    SUM(credits_used) as daily_credits,
    SUM(num_rows_migrated) as total_rows,
    ROUND(SUM(credits_used) / NULLIF(SUM(num_rows_migrated), 0) * 1000000, 4) as credits_per_million_rows
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY day
ORDER BY day DESC;

-- ----------------------------------------------------------------------------
-- Streaming latency check (requires metadata columns)
-- ----------------------------------------------------------------------------
-- If your target table captures insertion metadata, you can measure latency.

-- Example: Table with record_metadata_time (from SDK) and loaded_at columns
/*
SELECT 
    DATE_TRUNC('minute', loaded_at) as minute,
    COUNT(*) as row_count,
    AVG(DATEDIFF('second', record_metadata_time, loaded_at)) as avg_latency_seconds,
    MAX(DATEDIFF('second', record_metadata_time, loaded_at)) as max_latency_seconds
FROM your_streaming_table
WHERE loaded_at > dateadd('hour', -1, current_timestamp())
GROUP BY minute
ORDER BY minute DESC;
*/

-- ----------------------------------------------------------------------------
-- Channel-level latency (if channel name captured)
-- ----------------------------------------------------------------------------

/*
SELECT 
    channel_name,
    AVG(DATEDIFF('second', record_metadata_time, loaded_at)) as avg_latency_seconds,
    MIN(DATEDIFF('second', record_metadata_time, loaded_at)) as min_latency_seconds,
    MAX(DATEDIFF('second', record_metadata_time, loaded_at)) as max_latency_seconds,
    COUNT(*) as row_count
FROM your_streaming_table
WHERE loaded_at > dateadd('hour', -1, current_timestamp())
GROUP BY channel_name
ORDER BY avg_latency_seconds DESC;
*/

-- ----------------------------------------------------------------------------
-- Streaming vs Snowpipe cost comparison
-- ----------------------------------------------------------------------------

-- Streaming credits
SELECT 
    'Streaming' as method,
    DATE_TRUNC('day', start_time) as day,
    SUM(credits_used) as credits
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY day

UNION ALL

-- Snowpipe credits
SELECT 
    'Snowpipe' as method,
    DATE_TRUNC('day', start_time) as day,
    SUM(credits_used) as credits
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY day

ORDER BY day DESC, method;

-- ----------------------------------------------------------------------------
-- Streaming throughput analysis
-- ----------------------------------------------------------------------------

SELECT 
    DATE_TRUNC('hour', start_time) as hour,
    SUM(num_rows_migrated) as rows,
    SUM(num_bytes_migrated) / 1e6 as mb,
    ROUND(SUM(num_rows_migrated) / 3600.0, 2) as rows_per_second,
    ROUND(SUM(num_bytes_migrated) / 1e6 / 3600.0, 2) as mb_per_second
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -1, current_date())
GROUP BY hour
ORDER BY hour DESC;

-- ----------------------------------------------------------------------------
-- Tables receiving streaming data
-- ----------------------------------------------------------------------------

SELECT 
    database_name,
    schema_name,
    table_name,
    COUNT(*) as migration_count,
    SUM(credits_used) as total_credits,
    SUM(num_rows_migrated) as total_rows,
    MAX(end_time) as last_migration
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -7, current_date())
GROUP BY database_name, schema_name, table_name
ORDER BY total_credits DESC;

-- ----------------------------------------------------------------------------
-- Detect streaming issues
-- ----------------------------------------------------------------------------

-- Long migration times may indicate issues
SELECT 
    table_name,
    start_time,
    end_time,
    DATEDIFF('minute', start_time, end_time) as duration_minutes,
    num_rows_migrated,
    credits_used
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -1, current_date())
  AND DATEDIFF('minute', start_time, end_time) > 10
ORDER BY duration_minutes DESC;

-- ----------------------------------------------------------------------------
-- Kafka connector monitoring (if using Kafka)
-- ----------------------------------------------------------------------------

-- The Kafka connector logs to RECORD_METADATA table if configured
-- Check connector documentation for specific monitoring tables

-- General approach: query target tables for recent data
/*
SELECT 
    DATE_TRUNC('minute', _inserted_at) as minute,
    COUNT(*) as records
FROM your_kafka_target_table
WHERE _inserted_at > dateadd('hour', -1, current_timestamp())
GROUP BY minute
ORDER BY minute DESC;
*/
