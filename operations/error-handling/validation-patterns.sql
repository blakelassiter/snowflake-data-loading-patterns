-- ============================================================================
-- Error Handling: Validation Patterns
-- ============================================================================
-- Validate data before committing to tables. Catch schema mismatches,
-- type conversion errors, and malformed records without loading anything.

-- ----------------------------------------------------------------------------
-- VALIDATION_MODE options
-- ----------------------------------------------------------------------------

-- RETURN_ERRORS: Return first error per file (fast check)
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = 'RETURN_ERRORS';

-- Check results
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- RETURN_ALL_ERRORS: Return all errors (comprehensive but slower)
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = 'RETURN_ALL_ERRORS';

-- RETURN_<n>_ROWS: Validate and preview first n rows
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = 'RETURN_10_ROWS';

-- ----------------------------------------------------------------------------
-- Pre-load validation workflow
-- ----------------------------------------------------------------------------

-- Step 1: Check for errors without loading
COPY INTO raw.orders
FROM @external_stage/orders/
FILE_FORMAT = (TYPE = 'PARQUET')
VALIDATION_MODE = 'RETURN_ALL_ERRORS';

-- Step 2: Capture validation results
CREATE OR REPLACE TEMPORARY TABLE validation_results AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Step 3: Analyze errors
SELECT 
    error,
    file,
    line,
    character,
    column_name,
    COUNT(*) as occurrence_count
FROM validation_results
WHERE error IS NOT NULL
GROUP BY error, file, line, character, column_name
ORDER BY occurrence_count DESC;

-- Step 4: If clean, load for real
COPY INTO raw.orders
FROM @external_stage/orders/
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR = 'ABORT_STATEMENT';  -- Fail if any errors (should be none)

-- ----------------------------------------------------------------------------
-- Preview data before loading
-- ----------------------------------------------------------------------------

-- See what the data looks like without loading
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = 'RETURN_100_ROWS';

-- Inspect the preview
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Check data types and values
SELECT 
    TYPEOF($1:event_id) as event_id_type,
    TYPEOF($1:timestamp) as timestamp_type,
    $1:event_id::STRING as sample_event_id,
    $1:timestamp::STRING as sample_timestamp
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
LIMIT 5;

-- ----------------------------------------------------------------------------
-- Validate schema compatibility
-- ----------------------------------------------------------------------------

-- Compare source schema to target table
-- Step 1: Infer schema from files
SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@external_stage/new_data/',
        FILE_FORMAT => 'my_parquet_format'
    )
);

-- Step 2: Compare to existing table
WITH source_schema AS (
    SELECT COLUMN_NAME, TYPE
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@external_stage/new_data/',
            FILE_FORMAT => 'my_parquet_format'
        )
    )
),
target_schema AS (
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'RAW' AND table_name = 'EVENTS'
)
SELECT 
    COALESCE(s.COLUMN_NAME, t.column_name) as column_name,
    s.TYPE as source_type,
    t.data_type as target_type,
    CASE 
        WHEN s.COLUMN_NAME IS NULL THEN 'Missing in source'
        WHEN t.column_name IS NULL THEN 'New column in source'
        WHEN s.TYPE != t.data_type THEN 'Type mismatch'
        ELSE 'OK'
    END as status
FROM source_schema s
FULL OUTER JOIN target_schema t 
    ON UPPER(s.COLUMN_NAME) = UPPER(t.column_name)
WHERE s.COLUMN_NAME IS NULL 
   OR t.column_name IS NULL 
   OR s.TYPE != t.data_type;

-- ----------------------------------------------------------------------------
-- Validate file format compatibility
-- ----------------------------------------------------------------------------

-- Quick check: Can we read the files at all?
SELECT $1 FROM @external_stage/data/ LIMIT 5;

-- JSON: Check structure
SELECT 
    $1 as raw_json,
    OBJECT_KEYS($1) as top_level_keys
FROM @external_stage/json_data/
LIMIT 5;

-- CSV: Check column count and sample values
SELECT 
    $1 as col1, $2 as col2, $3 as col3, $4 as col4
FROM @external_stage/csv_data/
(FILE_FORMAT => 'csv_format')
LIMIT 5;

-- ----------------------------------------------------------------------------
-- Validate data quality rules
-- ----------------------------------------------------------------------------

-- Check for nulls in required fields
SELECT 
    METADATA$FILENAME as file,
    COUNT(*) as total_rows,
    COUNT_IF($1:customer_id IS NULL) as null_customer_ids,
    COUNT_IF($1:order_total IS NULL) as null_order_totals
FROM @external_stage/orders/
GROUP BY file;

-- Check for invalid values
SELECT 
    METADATA$FILENAME as file,
    COUNT_IF($1:order_total::NUMBER < 0) as negative_totals,
    COUNT_IF($1:quantity::NUMBER <= 0) as zero_or_negative_qty,
    COUNT_IF(TRY_TO_TIMESTAMP($1:order_date::STRING) IS NULL) as invalid_dates
FROM @external_stage/orders/
GROUP BY file;

-- ----------------------------------------------------------------------------
-- Automated validation procedure
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE ops.validate_before_load(
    stage_path STRING,
    target_table STRING,
    file_format STRING
)
RETURNS TABLE (check_name STRING, status STRING, details STRING)
LANGUAGE SQL
AS
$$
DECLARE
    result_table RESULTSET;
BEGIN
    -- This is a simplified example; production would be more comprehensive
    
    result_table := (
        WITH validation_checks AS (
            -- Check 1: Can we read files?
            SELECT 
                'file_readable' as check_name,
                CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as status,
                COUNT(*) || ' rows readable' as details
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        )
        SELECT * FROM validation_checks
    );
    
    RETURN TABLE(result_table);
END;
$$;

-- ----------------------------------------------------------------------------
-- Validation for incremental loads
-- ----------------------------------------------------------------------------

-- Check for duplicates that would be loaded
WITH staged_keys AS (
    SELECT DISTINCT $1:id::STRING as id
    FROM @external_stage/incremental/
),
existing_keys AS (
    SELECT DISTINCT id FROM raw.events
)
SELECT 
    COUNT(*) as duplicate_count,
    LISTAGG(s.id, ', ') WITHIN GROUP (ORDER BY s.id) as duplicate_ids
FROM staged_keys s
INNER JOIN existing_keys e ON s.id = e.id;

-- If duplicates found, decide: skip, update, or error?
