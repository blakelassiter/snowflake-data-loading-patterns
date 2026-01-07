-- ============================================================================
-- Error Handling: Recovery Procedures
-- ============================================================================
-- Patterns for recovering from load failures, reloading data, and backfilling.

-- ----------------------------------------------------------------------------
-- Identify failed files
-- ----------------------------------------------------------------------------

-- Find files that failed to load
SELECT 
    file_name,
    stage_location,
    status,
    first_error_message,
    first_error_line_number,
    last_load_time
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -7, current_timestamp())
))
WHERE status IN ('LOAD_FAILED', 'PARTIALLY_LOADED')
ORDER BY last_load_time DESC;

-- Get distinct failed file paths for retry
SELECT DISTINCT file_name
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -1, current_timestamp())
))
WHERE status = 'LOAD_FAILED';

-- ----------------------------------------------------------------------------
-- Reload specific failed files
-- ----------------------------------------------------------------------------

-- Option 1: Reload by file list
COPY INTO raw.events
FROM @external_stage/events/
FILES = (
    'events/2025/01/15/batch_001.json',
    'events/2025/01/15/batch_002.json'
)
FILE_FORMAT = (TYPE = 'JSON')
FORCE = TRUE;  -- Required to reload previously attempted files

-- Option 2: Reload by pattern (if failed files share a pattern)
COPY INTO raw.events
FROM @external_stage/events/
PATTERN = '.*2025/01/15.*[.]json'
FILE_FORMAT = (TYPE = 'JSON')
FORCE = TRUE;

-- ----------------------------------------------------------------------------
-- Reload after fixing source data
-- ----------------------------------------------------------------------------

-- Scenario: Bad files were fixed and re-uploaded to stage

-- Step 1: Verify fixed files are in stage
LIST @external_stage/events/fixed/;

-- Step 2: Load fixed files
COPY INTO raw.events
FROM @external_stage/events/fixed/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'ABORT_STATEMENT';  -- Strict since these should be clean

-- Step 3: Confirm success
SELECT status, COUNT(*) 
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
GROUP BY status;

-- Step 4: Archive or delete fixed files from stage
-- (Handle via external process or REMOVE command if internal stage)

-- ----------------------------------------------------------------------------
-- Backfill missing data
-- ----------------------------------------------------------------------------

-- Scenario: Data gap discovered, need to reload a date range

-- Step 1: Identify the gap
SELECT 
    DATE_TRUNC('day', event_timestamp) as event_date,
    COUNT(*) as row_count
FROM raw.events
WHERE event_timestamp BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY event_date
ORDER BY event_date;

-- Step 2: Check what files exist for missing dates
LIST @external_stage/events/2025/01/15/;

-- Step 3: Force reload for the gap period
COPY INTO raw.events
FROM @external_stage/events/
PATTERN = '.*2025/01/15.*'
FILE_FORMAT = (TYPE = 'JSON')
FORCE = TRUE;

-- ----------------------------------------------------------------------------
-- Handling duplicates during recovery
-- ----------------------------------------------------------------------------

-- If reloading might cause duplicates, use a staging approach

-- Step 1: Load into staging table
CREATE OR REPLACE TEMPORARY TABLE raw.events_reload_staging AS
SELECT * FROM raw.events WHERE 1=0;  -- Empty table with same schema

COPY INTO raw.events_reload_staging
FROM @external_stage/events/
PATTERN = '.*2025/01/15.*'
FILE_FORMAT = (TYPE = 'JSON')
FORCE = TRUE;

-- Step 2: Check for duplicates
SELECT 
    COUNT(*) as staged_rows,
    COUNT(DISTINCT event_id) as unique_ids,
    COUNT(*) - COUNT(DISTINCT event_id) as duplicates_in_staged
FROM raw.events_reload_staging;

SELECT COUNT(*) as already_exists
FROM raw.events_reload_staging s
WHERE EXISTS (
    SELECT 1 FROM raw.events e WHERE e.event_id = s.event_id
);

-- Step 3: Insert only new records
INSERT INTO raw.events
SELECT s.*
FROM raw.events_reload_staging s
WHERE NOT EXISTS (
    SELECT 1 FROM raw.events e WHERE e.event_id = s.event_id
);

-- Step 4: Clean up
DROP TABLE raw.events_reload_staging;

-- ----------------------------------------------------------------------------
-- Recovery with MERGE (upsert pattern)
-- ----------------------------------------------------------------------------

-- For idempotent reloads where duplicates should update existing records

-- Step 1: Load to staging
CREATE OR REPLACE TEMPORARY TABLE raw.orders_reload AS
SELECT 
    $1:order_id::STRING as order_id,
    $1:customer_id::STRING as customer_id,
    $1:order_total::NUMBER(12,2) as order_total,
    $1:status::STRING as status,
    $1:updated_at::TIMESTAMP_NTZ as updated_at
FROM @external_stage/orders/recovery/
(FILE_FORMAT => 'json_format');

-- Step 2: Merge into target
MERGE INTO raw.orders t
USING raw.orders_reload s
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
    UPDATE SET 
        customer_id = s.customer_id,
        order_total = s.order_total,
        status = s.status,
        updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_total, status, updated_at)
    VALUES (s.order_id, s.customer_id, s.order_total, s.status, s.updated_at);

-- ----------------------------------------------------------------------------
-- Snowpipe recovery
-- ----------------------------------------------------------------------------

-- Refresh pipe to catch missed files
ALTER PIPE raw.events_pipe REFRESH;

-- Refresh specific prefix
ALTER PIPE raw.events_pipe REFRESH PREFIX = 'events/2025/01/15/';

-- Check what got picked up
SELECT * FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('minute', -10, current_timestamp())
))
ORDER BY last_load_time DESC;

-- If pipe is stalled, recreate it
-- (capture definition first)
SHOW PIPES LIKE 'events_pipe';

DROP PIPE raw.events_pipe;

CREATE PIPE raw.events_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO raw.events
    FROM @external_stage/events/
    FILE_FORMAT = (TYPE = 'JSON');

-- Refresh to load files from recreation gap
ALTER PIPE raw.events_pipe REFRESH;

-- ----------------------------------------------------------------------------
-- Automated recovery procedure
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE ops.retry_failed_loads(
    table_name STRING,
    stage_name STRING,
    hours_back INTEGER DEFAULT 24
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    failed_files STRING;
    retry_count INTEGER;
BEGIN
    -- Get failed files
    SELECT LISTAGG(DISTINCT file_name, ',') INTO failed_files
    FROM TABLE(information_schema.copy_history(
        table_name => :table_name,
        start_time => dateadd('hour', -:hours_back, current_timestamp())
    ))
    WHERE status = 'LOAD_FAILED';
    
    IF (failed_files IS NULL) THEN
        RETURN 'No failed files found';
    END IF;
    
    -- Log the retry attempt
    INSERT INTO ops.recovery_log (table_name, files, attempted_at)
    VALUES (:table_name, :failed_files, current_timestamp());
    
    -- The actual COPY with FORCE would need dynamic SQL
    -- This is a simplified example
    
    RETURN 'Found failed files: ' || failed_files;
END;
$$;

-- ----------------------------------------------------------------------------
-- Recovery audit trail
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ops.recovery_log (
    recovery_id INTEGER AUTOINCREMENT,
    table_name STRING,
    files STRING,
    attempted_at TIMESTAMP_NTZ,
    completed_at TIMESTAMP_NTZ,
    rows_recovered INTEGER,
    status STRING,
    notes STRING
);

-- Log recovery attempts for audit
INSERT INTO ops.recovery_log (table_name, files, attempted_at, status, notes)
VALUES (
    'raw.events',
    'events/2025/01/15/batch_001.json',
    CURRENT_TIMESTAMP(),
    'COMPLETED',
    'Manual recovery after schema fix'
);
