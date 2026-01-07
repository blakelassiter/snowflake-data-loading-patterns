-- ============================================================================
-- dbt Source Freshness Checks
-- ============================================================================
-- Patterns for monitoring data freshness and integrating with dbt's
-- source freshness feature.

-- ----------------------------------------------------------------------------
-- Supporting infrastructure
-- ----------------------------------------------------------------------------

-- Create schema for operations
CREATE SCHEMA IF NOT EXISTS ops;

-- Table to track source freshness history
CREATE OR REPLACE TABLE ops.source_freshness_history (
    source_name STRING,
    table_name STRING,
    max_loaded_at TIMESTAMP_NTZ,
    snapshotted_at TIMESTAMP_NTZ,
    freshness_status STRING,  -- 'pass', 'warn', 'error'
    error_after_minutes INTEGER,
    warn_after_minutes INTEGER,
    actual_freshness_minutes INTEGER
);

-- ----------------------------------------------------------------------------
-- dbt source configuration
-- ----------------------------------------------------------------------------

/*
Configure source freshness in your dbt project:

# models/staging/sources.yml
version: 2

sources:
  - name: raw
    database: RAW
    schema: LOADING
    
    freshness:
      warn_after: {count: 1, period: hour}
      error_after: {count: 2, period: hour}
    
    loaded_at_field: _loaded_at  # Column tracking when data was loaded
    
    tables:
      - name: events
        freshness:
          warn_after: {count: 15, period: minute}
          error_after: {count: 30, period: minute}
      
      - name: customers
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 12, period: hour}
      
      - name: orders
        # Uses default freshness from source

Run with: dbt source freshness
*/

-- ----------------------------------------------------------------------------
-- Query freshness directly (without dbt)
-- ----------------------------------------------------------------------------

-- Check freshness of all raw tables
SELECT 
    'raw' as source,
    table_name,
    MAX(_loaded_at) as max_loaded_at,
    CURRENT_TIMESTAMP() as checked_at,
    DATEDIFF('minute', MAX(_loaded_at), CURRENT_TIMESTAMP()) as minutes_stale
FROM (
    SELECT 'events' as table_name, MAX(_loaded_at) as _loaded_at FROM raw.events
    UNION ALL
    SELECT 'customers', MAX(_loaded_at) FROM raw.customers
    UNION ALL
    SELECT 'orders', MAX(_loaded_at) FROM raw.orders
)
GROUP BY table_name
ORDER BY minutes_stale DESC;

-- ----------------------------------------------------------------------------
-- Freshness monitoring view
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW ops.source_freshness AS
WITH freshness_config AS (
    -- Define freshness thresholds (mirrors dbt source config)
    SELECT 'events' as table_name, 15 as warn_minutes, 30 as error_minutes
    UNION ALL SELECT 'customers', 360, 720
    UNION ALL SELECT 'orders', 60, 120
),
current_freshness AS (
    SELECT 'events' as table_name, MAX(_loaded_at) as max_loaded_at FROM raw.events
    UNION ALL
    SELECT 'customers', MAX(_loaded_at) FROM raw.customers
    UNION ALL
    SELECT 'orders', MAX(_loaded_at) FROM raw.orders
)
SELECT 
    cf.table_name,
    cf.max_loaded_at,
    CURRENT_TIMESTAMP() as checked_at,
    DATEDIFF('minute', cf.max_loaded_at, CURRENT_TIMESTAMP()) as minutes_stale,
    fc.warn_minutes,
    fc.error_minutes,
    CASE 
        WHEN DATEDIFF('minute', cf.max_loaded_at, CURRENT_TIMESTAMP()) >= fc.error_minutes THEN 'error'
        WHEN DATEDIFF('minute', cf.max_loaded_at, CURRENT_TIMESTAMP()) >= fc.warn_minutes THEN 'warn'
        ELSE 'pass'
    END as freshness_status
FROM current_freshness cf
JOIN freshness_config fc ON cf.table_name = fc.table_name;

-- ----------------------------------------------------------------------------
-- Record freshness history
-- ----------------------------------------------------------------------------

-- Scheduled task to snapshot freshness
CREATE OR REPLACE TASK ops.record_freshness
    WAREHOUSE = ops_wh
    SCHEDULE = '15 MINUTE'
AS
INSERT INTO ops.source_freshness_history
SELECT 
    'raw' as source_name,
    table_name,
    max_loaded_at,
    CURRENT_TIMESTAMP() as snapshotted_at,
    freshness_status,
    error_minutes as error_after_minutes,
    warn_minutes as warn_after_minutes,
    minutes_stale as actual_freshness_minutes
FROM ops.source_freshness;

ALTER TASK ops.record_freshness RESUME;

-- ----------------------------------------------------------------------------
-- Freshness alerts
-- ----------------------------------------------------------------------------

-- Alert when any source is stale
CREATE OR REPLACE ALERT ops.stale_data_alert
    WAREHOUSE = ops_wh
    SCHEDULE = '15 MINUTE'
IF (
    EXISTS (
        SELECT 1 
        FROM ops.source_freshness 
        WHERE freshness_status = 'error'
    )
)
THEN
    CALL ops.send_freshness_alert();

ALTER ALERT ops.stale_data_alert RESUME;

-- Procedure to send alert
CREATE OR REPLACE PROCEDURE ops.send_freshness_alert()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    stale_tables STRING;
BEGIN
    SELECT LISTAGG(table_name || ' (' || minutes_stale || 'm)', ', ') INTO :stale_tables
    FROM ops.source_freshness
    WHERE freshness_status = 'error';
    
    -- Log the alert (integrate with your notification system)
    INSERT INTO ops.alert_log (alert_type, message, created_at)
    VALUES ('stale_data', 'Stale tables: ' || :stale_tables, CURRENT_TIMESTAMP());
    
    RETURN 'Alert sent for: ' || :stale_tables;
END;
$$;

-- ----------------------------------------------------------------------------
-- Conditional dbt run based on freshness
-- ----------------------------------------------------------------------------

-- Only run dbt if data is fresh
CREATE OR REPLACE PROCEDURE ops.run_dbt_if_fresh(selector STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    stale_count INTEGER;
BEGIN
    -- Check for any error-level staleness
    SELECT COUNT(*) INTO :stale_count
    FROM ops.source_freshness
    WHERE freshness_status = 'error';
    
    IF (stale_count > 0) THEN
        RETURN 'Skipped: ' || :stale_count || ' stale sources';
    ELSE
        CALL ops.trigger_dbt_run(:selector);
        RETURN 'Triggered dbt for: ' || :selector;
    END IF;
END;
$$;

-- ----------------------------------------------------------------------------
-- Freshness analysis queries
-- ----------------------------------------------------------------------------

-- Average freshness by table over last 7 days
SELECT 
    table_name,
    AVG(actual_freshness_minutes) as avg_minutes_stale,
    MAX(actual_freshness_minutes) as max_minutes_stale,
    COUNT_IF(freshness_status = 'error') as error_count,
    COUNT_IF(freshness_status = 'warn') as warn_count,
    COUNT(*) as total_checks
FROM ops.source_freshness_history
WHERE snapshotted_at > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY table_name
ORDER BY avg_minutes_stale DESC;

-- Freshness by hour of day (find patterns)
SELECT 
    table_name,
    HOUR(snapshotted_at) as hour_of_day,
    AVG(actual_freshness_minutes) as avg_staleness
FROM ops.source_freshness_history
WHERE snapshotted_at > DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY table_name, HOUR(snapshotted_at)
ORDER BY table_name, hour_of_day;

-- Find when sources tend to be stale
SELECT 
    table_name,
    DAYNAME(snapshotted_at) as day_of_week,
    COUNT_IF(freshness_status IN ('warn', 'error')) as stale_count,
    COUNT(*) as total_checks,
    ROUND(100.0 * stale_count / total_checks, 1) as stale_pct
FROM ops.source_freshness_history
WHERE snapshotted_at > DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY table_name, DAYNAME(snapshotted_at)
ORDER BY table_name, stale_pct DESC;

-- ----------------------------------------------------------------------------
-- Load tracking integration
-- ----------------------------------------------------------------------------

-- Combine copy_history with freshness for full pipeline view
CREATE OR REPLACE VIEW ops.load_freshness_combined AS
SELECT 
    ch.table_name,
    ch.file_name,
    ch.row_count,
    ch.last_load_time,
    sf.max_loaded_at,
    sf.minutes_stale,
    sf.freshness_status,
    DATEDIFF('minute', ch.last_load_time, sf.max_loaded_at) as load_to_freshness_minutes
FROM TABLE(information_schema.copy_history(
    start_time => dateadd('hour', -24, current_timestamp())
)) ch
LEFT JOIN ops.source_freshness sf 
    ON UPPER(ch.table_name) = UPPER(sf.table_name);
