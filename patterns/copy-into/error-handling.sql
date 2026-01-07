-- ============================================================================
-- COPY INTO: Error Handling Patterns
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ON_ERROR options
-- ----------------------------------------------------------------------------

-- CONTINUE: Skip rows with errors, load valid rows
-- Best for: Data where some bad records are acceptable
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

-- SKIP_FILE: Skip entire file if any row has an error
-- Best for: When partial file loads would cause data integrity issues
COPY INTO raw.orders
FROM @external_stage/orders/
FILE_FORMAT = (TYPE = 'CSV')
ON_ERROR = 'SKIP_FILE';

-- SKIP_FILE_<num>: Skip file if error count exceeds threshold
-- Best for: Allowing small number of errors but catching corrupted files
COPY INTO raw.transactions
FROM @external_stage/txns/
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR = 'SKIP_FILE_10';  -- Skip if more than 10 errors in file

-- SKIP_FILE_<num>%: Skip file if error percentage exceeds threshold
COPY INTO raw.logs
FROM @external_stage/logs/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'SKIP_FILE_5%';  -- Skip if more than 5% of rows have errors

-- ABORT_STATEMENT: Stop entire load on first error
-- Best for: Critical data where any error requires investigation
COPY INTO raw.financial_data
FROM @external_stage/financial/
FILE_FORMAT = (TYPE = 'CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- ----------------------------------------------------------------------------
-- Validation mode - test before loading
-- ----------------------------------------------------------------------------

-- RETURN_ERRORS: Return first error per file without loading
COPY INTO raw.test_table
FROM @external_stage/test/
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = 'RETURN_ERRORS';

-- RETURN_ALL_ERRORS: Return all errors without loading
COPY INTO raw.test_table
FROM @external_stage/test/
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = 'RETURN_ALL_ERRORS';

-- RETURN_<n>_ROWS: Validate and return first n rows without loading
-- Useful for checking data looks correct before committing
COPY INTO raw.test_table
FROM @external_stage/test/
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = 'RETURN_10_ROWS';

-- ----------------------------------------------------------------------------
-- Check load results
-- ----------------------------------------------------------------------------

-- After COPY INTO completes, check what happened
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Returns columns:
-- file                  - File path
-- status                - 'LOADED', 'LOAD_FAILED', 'PARTIALLY_LOADED'
-- rows_parsed           - Rows read from file
-- rows_loaded           - Rows successfully loaded
-- error_limit           - Max errors allowed
-- errors_seen           - Errors encountered
-- first_error           - First error message
-- first_error_line      - Line number of first error
-- first_error_character - Character position of first error
-- first_error_column_name - Column where error occurred

-- ----------------------------------------------------------------------------
-- Query copy history for recent loads
-- ----------------------------------------------------------------------------

-- Check load status from information schema
SELECT 
    table_name,
    file_name,
    status,
    row_count,
    row_parsed,
    first_error_message,
    first_error_line_number
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('hour', -24, current_timestamp())
))
ORDER BY last_load_time DESC;

-- ----------------------------------------------------------------------------
-- Capture rejected records
-- ----------------------------------------------------------------------------

-- Create a table to capture rejected rows for analysis
CREATE TABLE IF NOT EXISTS raw.rejected_records (
    rejection_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    error_message STRING,
    rejected_record VARIANT
);

-- Load with error capture using a two-step process:
-- Step 1: Attempt load with VALIDATION_MODE to identify errors
-- Step 2: Parse error output and insert into rejected_records
-- Step 3: Load valid records with ON_ERROR = 'CONTINUE'

-- Example: Manual error capture flow
-- First validate
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = 'RETURN_ALL_ERRORS';

-- Capture validation results
CREATE OR REPLACE TEMPORARY TABLE validation_errors AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Then load, skipping bad records
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

-- ----------------------------------------------------------------------------
-- Common error patterns and fixes
-- ----------------------------------------------------------------------------

-- Error: "Number of columns in file does not match"
-- Fix: Check SKIP_HEADER, column count, or use explicit column list
COPY INTO raw.csv_data (col1, col2, col3)
FROM (SELECT $1, $2, $3 FROM @stage/data/)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- Error: "Numeric value 'abc' is not recognized"
-- Fix: Use TRY_CAST or TRY_TO_NUMBER to handle bad values
COPY INTO raw.numeric_data
FROM (
    SELECT 
        $1 as id,
        TRY_TO_NUMBER($2) as amount  -- Returns NULL for non-numeric
    FROM @stage/data/
)
FILE_FORMAT = (TYPE = 'CSV');

-- Error: "Timestamp 'invalid' is not recognized"
-- Fix: Use TRY_TO_TIMESTAMP with explicit format
COPY INTO raw.dated_data
FROM (
    SELECT 
        $1 as id,
        TRY_TO_TIMESTAMP($2, 'YYYY-MM-DD HH24:MI:SS') as event_time
    FROM @stage/data/
)
FILE_FORMAT = (TYPE = 'CSV');

-- Error: "String is too long for column"
-- Fix: Truncate or increase column size
COPY INTO raw.text_data
FROM (
    SELECT 
        $1 as id,
        LEFT($2::STRING, 255) as description  -- Truncate to fit
    FROM @stage/data/
)
FILE_FORMAT = (TYPE = 'CSV');
