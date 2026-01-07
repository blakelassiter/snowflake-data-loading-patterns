-- ============================================================================
-- Error Handling: ON_ERROR Options Deep Dive
-- ============================================================================
-- Detailed exploration of each ON_ERROR option with use cases and trade-offs.

-- ----------------------------------------------------------------------------
-- ABORT_STATEMENT (default)
-- ----------------------------------------------------------------------------
-- Stops the entire load on first error. No data is committed.

COPY INTO raw.financial_data
FROM @external_stage/financial/
FILE_FORMAT = (TYPE = 'CSV')
ON_ERROR = 'ABORT_STATEMENT';

/*
Use when:
- Data integrity is critical (financial, compliance)
- Any error indicates a systemic problem
- You want to investigate before any data lands
- Downstream systems can't handle partial loads

Trade-offs:
+ Zero risk of bad data entering the system
+ Forces investigation of every issue
- Single bad row blocks entire load
- May cause significant delays
- Requires robust retry/alerting
*/

-- ----------------------------------------------------------------------------
-- CONTINUE
-- ----------------------------------------------------------------------------
-- Skips rows with errors, loads valid rows. Most permissive option.

COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

-- Check what was skipped
SELECT 
    file,
    status,
    rows_parsed,
    rows_loaded,
    rows_parsed - rows_loaded as rows_skipped,
    first_error,
    first_error_line
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE rows_parsed > rows_loaded;

/*
Use when:
- Some data loss is acceptable
- Bad records are expected (dirty source data)
- Volume matters more than completeness
- You have a separate process to handle rejects

Trade-offs:
+ Load continues despite errors
+ Maximizes data throughput
- Silent data loss if not monitored
- Skipped rows may be important
- Requires reject tracking
*/

-- ----------------------------------------------------------------------------
-- SKIP_FILE
-- ----------------------------------------------------------------------------
-- Skips entire file if ANY row has an error. File-level atomicity.

COPY INTO raw.orders
FROM @external_stage/orders/
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR = 'SKIP_FILE';

/*
Use when:
- Files should load completely or not at all
- Partial file loads would cause data integrity issues
- Files represent logical units (daily batches, transactions)
- You'll retry/fix failed files separately

Trade-offs:
+ File-level consistency
+ Clear success/failure per file
- One bad row fails entire file
- May lose significant good data
- Requires file-level retry logic
*/

-- ----------------------------------------------------------------------------
-- SKIP_FILE_<n> (error count threshold)
-- ----------------------------------------------------------------------------
-- Skips file only if error count exceeds n. Tolerates some errors.

COPY INTO raw.logs
FROM @external_stage/logs/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'SKIP_FILE_10';  -- Skip if > 10 errors

/*
Use when:
- Small number of errors is acceptable
- Want to catch corrupted files (many errors)
- But tolerate occasional bad records
- Files vary in quality

Trade-offs:
+ Balances tolerance with protection
+ Catches catastrophically bad files
- Arbitrary threshold (why 10 vs 20?)
- Still partial data if under threshold
*/

-- Example thresholds by use case:
-- SKIP_FILE_1   : Almost like SKIP_FILE (1 error allowed)
-- SKIP_FILE_10  : Small tolerance for known-dirty sources
-- SKIP_FILE_100 : High tolerance, catch only major issues
-- SKIP_FILE_1000: Very permissive, effectively CONTINUE for most files

-- ----------------------------------------------------------------------------
-- SKIP_FILE_<n>% (error percentage threshold)
-- ----------------------------------------------------------------------------
-- Skips file if error percentage exceeds n%. Scales with file size.

COPY INTO raw.transactions
FROM @external_stage/txns/
FILE_FORMAT = (TYPE = 'CSV')
ON_ERROR = 'SKIP_FILE_5%';  -- Skip if > 5% errors

/*
Use when:
- Error tolerance should scale with volume
- 10 errors in 100 rows is bad; 10 in 100,000 is fine
- Files vary significantly in size
- Want proportional quality threshold

Trade-offs:
+ Adapts to file size
+ More intuitive threshold
- Small files may exceed % easily
- Percentage calculation has overhead
*/

-- Percentage thresholds:
-- SKIP_FILE_1%  : Very strict (99% must succeed)
-- SKIP_FILE_5%  : Moderate tolerance
-- SKIP_FILE_10% : Permissive
-- SKIP_FILE_50% : Only skip if majority fails

-- ----------------------------------------------------------------------------
-- Combining with RETURN_FAILED_ONLY
-- ----------------------------------------------------------------------------
-- When using CONTINUE, only return info about files with errors

COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;

-- Result only shows files that had skipped rows
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Useful when loading many files and you only care about problems

-- ----------------------------------------------------------------------------
-- Decision matrix
-- ----------------------------------------------------------------------------

/*
                    | Single bad row | Many bad rows | Corrupted file
--------------------|----------------|---------------|----------------
ABORT_STATEMENT     | Stop all       | Stop all      | Stop all
CONTINUE            | Skip row       | Skip rows     | Load partial
SKIP_FILE           | Skip file      | Skip file     | Skip file
SKIP_FILE_10        | Load file      | Skip file     | Skip file
SKIP_FILE_5%        | Load file*     | Skip file*    | Skip file

* Depends on file size and error distribution
*/

-- ----------------------------------------------------------------------------
-- Monitoring error rates over time
-- ----------------------------------------------------------------------------

-- Track error rates to tune ON_ERROR settings
SELECT 
    DATE_TRUNC('day', last_load_time) as load_date,
    status,
    COUNT(*) as file_count,
    SUM(row_parsed) as total_parsed,
    SUM(row_count) as total_loaded,
    ROUND((SUM(row_parsed) - SUM(row_count)) * 100.0 / 
          NULLIF(SUM(row_parsed), 0), 2) as error_pct
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -30, current_timestamp())
))
GROUP BY load_date, status
ORDER BY load_date DESC;

-- If error_pct is consistently < 1%, SKIP_FILE_5% is reasonable
-- If error_pct spikes randomly, investigate those files

-- ----------------------------------------------------------------------------
-- Environment-specific settings
-- ----------------------------------------------------------------------------

-- Development: Strict, catch everything
COPY INTO dev.events
FROM @dev_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'ABORT_STATEMENT';

-- Staging: Moderate, test real-world tolerance
COPY INTO staging.events
FROM @staging_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'SKIP_FILE_10';

-- Production: Balanced, don't block pipeline
COPY INTO prod.events
FROM @prod_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';  -- With robust reject monitoring
