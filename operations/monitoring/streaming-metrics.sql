-- ============================================================================
-- Streaming Metrics Queries
-- ============================================================================
-- Monitor Snowpipe Streaming performance, costs, and health.

-- ----------------------------------------------------------------------------
-- Streaming file migration history
-- ----------------------------------------------------------------------------

-- When streaming data accumulates, Snowflake migrates it to optimized storage.
-- This view tracks those background migrations.

SELECT 
    database_name,
    schema_name,
    table_name,
    start_time,
    end_time,
    DATEDIFF('second', start_time, end_time) as duration_seconds,
    credits_used,
    num_bytes_migrated / 1e6 as mb_migrated,
    num_rows_migrated
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -1, current_date())
ORDER BY start_time DESC;

-- ----------------------------------------------------------------------------
-- Hourly streaming throughput
-- ----------------------------------------------------------------------------

SELECT 
    DATE_TRUNC('hour', start_time) as hour,
    COUNT(*) as migration_count,
    SUM(credits_used) as hourly_credits,
    SUM(num_rows_migrated) as rows_migrated,
    SUM(num_bytes_migrated) / 1e9 as gb_migrated,
    ROUND(SUM(num_rows_migrated) / 3600.0, 0) as avg_rows_per_second
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
    SUM(num_bytes_migrated) / 1e9 as total_gb,
    ROUND(SUM(credits_used) / NULLIF(SUM(num_rows_migrated), 0) * 1000000, 4) as credits_per_million_rows
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY day
ORDER BY day DESC;

-- ----------------------------------------------------------------------------
-- Cost by table
-- ----------------------------------------------------------------------------

SELECT 
    database_name,
    schema_name,
    table_name,
    COUNT(*) as migration_count,
    SUM(credits_used) as total_credits,
    SUM(num_rows_migrated) as total_rows,
    SUM(num_bytes_migrated) / 1e9 as total_gb,
    MAX(end_time) as last_migration
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY database_name, schema_name, table_name
ORDER BY total_credits DESC;

-- ----------------------------------------------------------------------------
-- Migration duration analysis
-- ----------------------------------------------------------------------------

-- Long migrations may indicate performance issues
SELECT 
    table_name,
    start_time,
    end_time,
    DATEDIFF('minute', start_time, end_time) as duration_minutes,
    num_rows_migrated,
    num_bytes_migrated / 1e6 as mb_migrated,
    credits_used
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -7, current_date())
  AND DATEDIFF('minute', start_time, end_time) > 5
ORDER BY duration_minutes DESC;

-- ----------------------------------------------------------------------------
-- Streaming vs other methods comparison
-- ----------------------------------------------------------------------------

-- Compare daily costs across loading methods
WITH streaming_costs AS (
    SELECT 
        DATE_TRUNC('day', start_time) as day,
        'Streaming' as method,
        SUM(credits_used) as credits
    FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
    WHERE start_time > dateadd('day', -30, current_date())
    GROUP BY day
),
pipe_costs AS (
    SELECT 
        DATE_TRUNC('day', start_time) as day,
        'Snowpipe' as method,
        SUM(credits_used) as credits
    FROM snowflake.account_usage.pipe_usage_history
    WHERE start_time > dateadd('day', -30, current_date())
    GROUP BY day
),
warehouse_copy_costs AS (
    -- Estimate COPY INTO costs from query history
    SELECT 
        DATE_TRUNC('day', start_time) as day,
        'COPY INTO' as method,
        SUM(credits_used_cloud_services) as credits
    FROM snowflake.account_usage.query_history
    WHERE query_type = 'COPY'
      AND start_time > dateadd('day', -30, current_date())
    GROUP BY day
)
SELECT * FROM streaming_costs
UNION ALL
SELECT * FROM pipe_costs
UNION ALL
SELECT * FROM warehouse_copy_costs
ORDER BY day DESC, method;

-- ----------------------------------------------------------------------------
-- Streaming latency estimation
-- ----------------------------------------------------------------------------

-- If target table has audit columns capturing insertion time,
-- compare against source event time to measure end-to-end latency

/*
-- Example: Table with source_timestamp and _inserted_at columns
SELECT 
    DATE_TRUNC('minute', _inserted_at) as minute,
    COUNT(*) as row_count,
    AVG(DATEDIFF('second', source_timestamp, _inserted_at)) as avg_latency_sec,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY 
        DATEDIFF('second', source_timestamp, _inserted_at)
    ) as p95_latency_sec,
    MAX(DATEDIFF('second', source_timestamp, _inserted_at)) as max_latency_sec
FROM raw.streaming.events
WHERE _inserted_at > dateadd('hour', -1, current_timestamp())
GROUP BY minute
ORDER BY minute DESC;
*/

-- ----------------------------------------------------------------------------
-- Channel health (requires application-level tracking)
-- ----------------------------------------------------------------------------

-- If you track channel names during insertion, query by channel:

/*
SELECT 
    channel_name,
    COUNT(*) as rows_ingested,
    MIN(_inserted_at) as first_insert,
    MAX(_inserted_at) as last_insert,
    AVG(DATEDIFF('second', source_timestamp, _inserted_at)) as avg_latency_sec
FROM raw.streaming.events
WHERE _inserted_at > dateadd('hour', -1, current_timestamp())
GROUP BY channel_name
ORDER BY last_insert DESC;
*/

-- ----------------------------------------------------------------------------
-- Alerting queries
-- ----------------------------------------------------------------------------

-- No streaming data in last N minutes
SELECT 
    MAX(end_time) as last_migration,
    DATEDIFF('minute', MAX(end_time), CURRENT_TIMESTAMP()) as minutes_since_last
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
HAVING DATEDIFF('minute', MAX(end_time), CURRENT_TIMESTAMP()) > 30;

-- Streaming costs exceeding threshold
SELECT 
    DATE_TRUNC('day', start_time) as day,
    SUM(credits_used) as daily_credits
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -1, current_date())
GROUP BY day
HAVING SUM(credits_used) > 100;  -- Adjust threshold as needed

-- ----------------------------------------------------------------------------
-- Kafka connector monitoring (if applicable)
-- ----------------------------------------------------------------------------

-- The Kafka connector doesn't expose direct metrics in Snowflake.
-- Monitor via:
-- 1. Kafka Connect JMX metrics
-- 2. Target table row counts over time
-- 3. Consumer lag in Kafka

-- Simple freshness check for Kafka-sourced table:
/*
SELECT 
    MAX(_kafka_timestamp) as latest_kafka_time,
    MAX(_inserted_at) as latest_snowflake_time,
    DATEDIFF('second', MAX(_kafka_timestamp), MAX(_inserted_at)) as processing_lag_sec,
    COUNT(*) as recent_rows
FROM raw.kafka.events
WHERE _inserted_at > dateadd('minute', -5, current_timestamp());
*/
