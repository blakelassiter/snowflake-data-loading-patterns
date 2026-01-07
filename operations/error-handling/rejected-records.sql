-- ============================================================================
-- Error Handling: Rejected Records
-- ============================================================================
-- Capturing, analyzing, and processing rows that fail during load.

-- ----------------------------------------------------------------------------
-- Create rejected records table
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ops.rejected_records (
    rejection_id INTEGER AUTOINCREMENT,
    rejection_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    target_table STRING,
    source_file STRING,
    source_line INTEGER,
    error_message STRING,
    error_column STRING,
    rejected_data VARIANT,
    retry_status STRING DEFAULT 'PENDING',
    retry_count INTEGER DEFAULT 0,
    resolved_at TIMESTAMP_NTZ
);

-- ----------------------------------------------------------------------------
-- Capture rejected records workflow
-- ----------------------------------------------------------------------------

-- Step 1: Validate and capture errors
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = 'RETURN_ALL_ERRORS';

-- Step 2: Insert validation errors into rejected records
INSERT INTO ops.rejected_records (
    target_table,
    source_file,
    source_line,
    error_message,
    error_column
)
SELECT 
    'raw.events',
    file,
    line,
    error,
    column_name
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE error IS NOT NULL;

-- Step 3: Now load with CONTINUE (errors will be skipped)
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

-- ----------------------------------------------------------------------------
-- Capture rejected data content
-- ----------------------------------------------------------------------------

-- More comprehensive capture including the actual data

-- Step 1: Query staged files to find rows that will fail
-- This requires knowing which rows have issues

-- For JSON files, attempt to capture raw content:
INSERT INTO ops.rejected_records (
    target_table,
    source_file,
    source_line,
    error_message,
    rejected_data
)
SELECT 
    'raw.events',
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    CASE 
        WHEN TRY_CAST($1:event_id AS STRING) IS NULL THEN 'Invalid event_id'
        WHEN TRY_TO_TIMESTAMP($1:timestamp::STRING) IS NULL THEN 'Invalid timestamp'
        ELSE 'Unknown error'
    END,
    $1  -- Capture the raw VARIANT
FROM @external_stage/events/
WHERE TRY_CAST($1:event_id AS STRING) IS NULL
   OR TRY_TO_TIMESTAMP($1:timestamp::STRING) IS NULL;

-- ----------------------------------------------------------------------------
-- Analyze rejection patterns
-- ----------------------------------------------------------------------------

-- Most common errors
SELECT 
    error_message,
    COUNT(*) as occurrence_count,
    MIN(rejection_time) as first_seen,
    MAX(rejection_time) as last_seen
FROM ops.rejected_records
WHERE rejection_time > dateadd('day', -7, current_timestamp())
GROUP BY error_message
ORDER BY occurrence_count DESC;

-- Errors by source file
SELECT 
    source_file,
    COUNT(*) as error_count,
    LISTAGG(DISTINCT error_message, ', ') as error_types
FROM ops.rejected_records
WHERE rejection_time > dateadd('day', -1, current_timestamp())
GROUP BY source_file
ORDER BY error_count DESC;

-- Errors by column
SELECT 
    error_column,
    error_message,
    COUNT(*) as count
FROM ops.rejected_records
WHERE error_column IS NOT NULL
GROUP BY error_column, error_message
ORDER BY count DESC;

-- Rejection rate over time
SELECT 
    DATE_TRUNC('hour', rejection_time) as hour,
    COUNT(*) as rejected_count
FROM ops.rejected_records
WHERE rejection_time > dateadd('day', -7, current_timestamp())
GROUP BY hour
ORDER BY hour;

-- ----------------------------------------------------------------------------
-- Process rejected records
-- ----------------------------------------------------------------------------

-- Fix and retry workflow

-- Step 1: Identify fixable records
SELECT 
    rejection_id,
    error_message,
    rejected_data,
    -- Attempt to fix common issues
    CASE 
        WHEN error_message LIKE '%timestamp%' THEN
            TRY_TO_TIMESTAMP(rejected_data:timestamp::STRING, 'MM/DD/YYYY')
        ELSE NULL
    END as fixed_timestamp
FROM ops.rejected_records
WHERE retry_status = 'PENDING'
  AND rejected_data IS NOT NULL;

-- Step 2: Insert fixed records
INSERT INTO raw.events (event_id, event_type, event_timestamp, payload)
SELECT 
    rejected_data:event_id::STRING,
    rejected_data:event_type::STRING,
    TRY_TO_TIMESTAMP(rejected_data:timestamp::STRING, 'MM/DD/YYYY'),
    rejected_data:payload::VARIANT
FROM ops.rejected_records
WHERE retry_status = 'PENDING'
  AND TRY_TO_TIMESTAMP(rejected_data:timestamp::STRING, 'MM/DD/YYYY') IS NOT NULL;

-- Step 3: Mark as resolved
UPDATE ops.rejected_records
SET retry_status = 'RESOLVED',
    resolved_at = CURRENT_TIMESTAMP()
WHERE retry_status = 'PENDING'
  AND TRY_TO_TIMESTAMP(rejected_data:timestamp::STRING, 'MM/DD/YYYY') IS NOT NULL;

-- Step 4: Mark unfixable as permanent failures
UPDATE ops.rejected_records
SET retry_status = 'PERMANENT_FAILURE',
    retry_count = retry_count + 1
WHERE retry_status = 'PENDING'
  AND rejection_time < dateadd('day', -7, current_timestamp());

-- ----------------------------------------------------------------------------
-- Dead letter queue pattern
-- ----------------------------------------------------------------------------

-- For records that can't be fixed, move to dead letter table
CREATE TABLE IF NOT EXISTS ops.dead_letter_queue (
    dlq_id INTEGER AUTOINCREMENT,
    original_rejection_id INTEGER,
    target_table STRING,
    source_file STRING,
    error_message STRING,
    rejected_data VARIANT,
    moved_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    notes STRING
);

-- Move permanent failures to DLQ
INSERT INTO ops.dead_letter_queue (
    original_rejection_id,
    target_table,
    source_file,
    error_message,
    rejected_data
)
SELECT 
    rejection_id,
    target_table,
    source_file,
    error_message,
    rejected_data
FROM ops.rejected_records
WHERE retry_status = 'PERMANENT_FAILURE'
  AND rejection_id NOT IN (SELECT original_rejection_id FROM ops.dead_letter_queue);

-- ----------------------------------------------------------------------------
-- Alerting on rejection rates
-- ----------------------------------------------------------------------------

-- Create alert for high rejection rate
CREATE OR REPLACE ALERT ops.alert_high_rejection_rate
    WAREHOUSE = ops_wh
    SCHEDULE = 'USING CRON 0 * * * * UTC'
    IF (EXISTS (
        SELECT 1
        FROM ops.rejected_records
        WHERE rejection_time > dateadd('hour', -1, current_timestamp())
        HAVING COUNT(*) > 100
    ))
    THEN
        CALL system$send_email(
            'data_alerts',
            'data-team@company.com',
            'High Rejection Rate Alert',
            'More than 100 records rejected in the last hour.'
        );

-- ----------------------------------------------------------------------------
-- Cleanup old rejected records
-- ----------------------------------------------------------------------------

-- Archive resolved records older than 30 days
CREATE TABLE IF NOT EXISTS ops.rejected_records_archive AS
SELECT * FROM ops.rejected_records WHERE 1=0;

INSERT INTO ops.rejected_records_archive
SELECT * FROM ops.rejected_records
WHERE retry_status IN ('RESOLVED', 'PERMANENT_FAILURE')
  AND rejection_time < dateadd('day', -30, current_timestamp());

DELETE FROM ops.rejected_records
WHERE retry_status IN ('RESOLVED', 'PERMANENT_FAILURE')
  AND rejection_time < dateadd('day', -30, current_timestamp());

-- ----------------------------------------------------------------------------
-- Reporting
-- ----------------------------------------------------------------------------

-- Daily rejection summary
SELECT 
    DATE_TRUNC('day', rejection_time) as day,
    target_table,
    retry_status,
    COUNT(*) as count
FROM ops.rejected_records
WHERE rejection_time > dateadd('day', -30, current_timestamp())
GROUP BY day, target_table, retry_status
ORDER BY day DESC, target_table;

-- Resolution rate
SELECT 
    DATE_TRUNC('week', rejection_time) as week,
    COUNT(*) as total_rejected,
    COUNT_IF(retry_status = 'RESOLVED') as resolved,
    COUNT_IF(retry_status = 'PERMANENT_FAILURE') as permanent_failures,
    COUNT_IF(retry_status = 'PENDING') as pending,
    ROUND(COUNT_IF(retry_status = 'RESOLVED') * 100.0 / COUNT(*), 1) as resolution_rate_pct
FROM ops.rejected_records
GROUP BY week
ORDER BY week DESC;
