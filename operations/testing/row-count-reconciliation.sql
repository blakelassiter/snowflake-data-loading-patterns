-- ============================================================================
-- Testing: Row Count Reconciliation
-- ============================================================================
-- Verify that the number of rows loaded matches expectations from source.

-- ----------------------------------------------------------------------------
-- Basic count comparison
-- ----------------------------------------------------------------------------

-- Count rows in staged files vs loaded table
WITH staged AS (
    SELECT COUNT(*) as row_count
    FROM @external_stage/events/2025/01/15/
    (FILE_FORMAT => 'json_format')
),
loaded AS (
    SELECT COUNT(*) as row_count
    FROM raw.events
    WHERE DATE(loaded_at) = '2025-01-15'
)
SELECT 
    staged.row_count as staged_rows,
    loaded.row_count as loaded_rows,
    staged.row_count - loaded.row_count as difference,
    CASE 
        WHEN staged.row_count = loaded.row_count THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM staged, loaded;

-- ----------------------------------------------------------------------------
-- Reconciliation with copy_history
-- ----------------------------------------------------------------------------

-- Compare what COPY INTO reported vs what's in the table
WITH copy_stats AS (
    SELECT 
        SUM(row_count) as reported_loaded,
        SUM(row_parsed) as reported_parsed,
        SUM(row_parsed) - SUM(row_count) as reported_errors
    FROM TABLE(information_schema.copy_history(
        table_name => 'raw.events',
        start_time => dateadd('hour', -1, current_timestamp())
    ))
),
table_stats AS (
    SELECT COUNT(*) as actual_rows
    FROM raw.events
    WHERE loaded_at > dateadd('hour', -1, current_timestamp())
)
SELECT 
    c.reported_loaded,
    c.reported_parsed,
    c.reported_errors,
    t.actual_rows,
    c.reported_loaded - t.actual_rows as discrepancy,
    CASE 
        WHEN c.reported_loaded = t.actual_rows THEN 'PASS'
        ELSE 'FAIL - Check for deletes or duplicates'
    END as status
FROM copy_stats c, table_stats t;

-- ----------------------------------------------------------------------------
-- File-level reconciliation
-- ----------------------------------------------------------------------------

-- Check each file loaded the expected number of rows
WITH file_expectations AS (
    -- This would come from a manifest or external system
    SELECT 'events/batch_001.json' as file_name, 10000 as expected_rows
    UNION ALL
    SELECT 'events/batch_002.json', 15000
    UNION ALL
    SELECT 'events/batch_003.json', 12000
),
file_actuals AS (
    SELECT 
        file_name,
        row_count as actual_rows
    FROM TABLE(information_schema.copy_history(
        table_name => 'raw.events',
        start_time => dateadd('hour', -1, current_timestamp())
    ))
)
SELECT 
    e.file_name,
    e.expected_rows,
    COALESCE(a.actual_rows, 0) as actual_rows,
    e.expected_rows - COALESCE(a.actual_rows, 0) as difference,
    CASE 
        WHEN e.expected_rows = COALESCE(a.actual_rows, 0) THEN 'PASS'
        WHEN a.actual_rows IS NULL THEN 'FAIL - File not loaded'
        ELSE 'FAIL - Row count mismatch'
    END as status
FROM file_expectations e
LEFT JOIN file_actuals a ON e.file_name = a.file_name;

-- ----------------------------------------------------------------------------
-- Reconciliation with external manifest
-- ----------------------------------------------------------------------------

-- If source system provides a manifest file with expected counts
-- Step 1: Load manifest
CREATE OR REPLACE TEMPORARY TABLE load_manifest AS
SELECT 
    $1:file_name::STRING as file_name,
    $1:row_count::INTEGER as expected_rows,
    $1:checksum::STRING as expected_checksum
FROM @external_stage/manifests/2025-01-15.json
(FILE_FORMAT => 'json_format');

-- Step 2: Compare to actuals
SELECT 
    m.file_name,
    m.expected_rows,
    COALESCE(h.row_count, 0) as actual_rows,
    CASE 
        WHEN m.expected_rows = COALESCE(h.row_count, 0) THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM load_manifest m
LEFT JOIN TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -1, current_timestamp())
)) h ON m.file_name LIKE '%' || h.file_name || '%';

-- ----------------------------------------------------------------------------
-- Count by partition/date
-- ----------------------------------------------------------------------------

-- Ensure each date partition has expected volume
WITH daily_counts AS (
    SELECT 
        DATE(event_timestamp) as event_date,
        COUNT(*) as row_count
    FROM raw.events
    WHERE event_timestamp >= '2025-01-01'
    GROUP BY event_date
),
expected_counts AS (
    -- Historical average or explicit expectations
    SELECT 
        DATE(event_timestamp) as event_date,
        AVG(daily_count) OVER (ORDER BY event_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) as expected_rows
    FROM (
        SELECT DATE(event_timestamp) as event_date, COUNT(*) as daily_count
        FROM raw.events
        GROUP BY event_date
    )
)
SELECT 
    d.event_date,
    d.row_count,
    ROUND(e.expected_rows) as expected_rows,
    ROUND((d.row_count - e.expected_rows) / e.expected_rows * 100, 1) as pct_diff,
    CASE 
        WHEN ABS(d.row_count - e.expected_rows) / e.expected_rows < 0.2 THEN 'PASS'
        ELSE 'WARN - >20% deviation from expected'
    END as status
FROM daily_counts d
JOIN expected_counts e ON d.event_date = e.event_date
ORDER BY d.event_date DESC;

-- ----------------------------------------------------------------------------
-- Duplicate detection
-- ----------------------------------------------------------------------------

-- Check for unexpected duplicates after load
SELECT 
    event_id,
    COUNT(*) as occurrence_count
FROM raw.events
WHERE loaded_at > dateadd('hour', -1, current_timestamp())
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 100;

-- Duplicate summary
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT event_id) as unique_ids,
    COUNT(*) - COUNT(DISTINCT event_id) as duplicate_rows,
    ROUND((COUNT(*) - COUNT(DISTINCT event_id)) * 100.0 / COUNT(*), 2) as duplicate_pct
FROM raw.events
WHERE loaded_at > dateadd('hour', -1, current_timestamp());

-- ----------------------------------------------------------------------------
-- Automated reconciliation procedure
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE ops.reconcile_load(
    table_name STRING,
    expected_rows INTEGER,
    hours_back INTEGER DEFAULT 1
)
RETURNS TABLE (metric STRING, value INTEGER, status STRING)
LANGUAGE SQL
AS
$$
DECLARE
    actual_rows INTEGER;
    result RESULTSET;
BEGIN
    -- Get actual row count
    SELECT COUNT(*) INTO actual_rows
    FROM IDENTIFIER(:table_name)
    WHERE loaded_at > dateadd('hour', -:hours_back, current_timestamp());
    
    result := (
        SELECT 
            'row_count' as metric,
            :actual_rows as value,
            CASE 
                WHEN :actual_rows = :expected_rows THEN 'PASS'
                WHEN :actual_rows > :expected_rows THEN 'WARN - More rows than expected'
                ELSE 'FAIL - Missing rows'
            END as status
    );
    
    RETURN TABLE(result);
END;
$$;

-- Usage
CALL ops.reconcile_load('raw.events', 50000, 1);

-- ----------------------------------------------------------------------------
-- Reconciliation audit log
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ops.reconciliation_log (
    reconciliation_id INTEGER AUTOINCREMENT,
    check_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    table_name STRING,
    expected_rows INTEGER,
    actual_rows INTEGER,
    difference INTEGER,
    status STRING,
    notes STRING
);

-- Log reconciliation results
INSERT INTO ops.reconciliation_log (table_name, expected_rows, actual_rows, difference, status)
SELECT 
    'raw.events',
    50000,
    COUNT(*),
    50000 - COUNT(*),
    CASE WHEN COUNT(*) = 50000 THEN 'PASS' ELSE 'FAIL' END
FROM raw.events
WHERE loaded_at > dateadd('hour', -1, current_timestamp());

-- Review reconciliation history
SELECT * FROM ops.reconciliation_log
WHERE check_time > dateadd('day', -7, current_timestamp())
ORDER BY check_time DESC;
