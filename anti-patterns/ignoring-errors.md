# Anti-Pattern: Ignoring Errors

Using `ON_ERROR = CONTINUE` without monitoring what gets skipped.

## The Problem

`ON_ERROR = CONTINUE` is useful - it keeps pipelines flowing when source data has quality issues. But without monitoring, you have no idea what data is silently being lost.

Over time, this creates:
- Data gaps that compound downstream
- Incorrect analytics from incomplete data
- False confidence in data completeness
- Difficult-to-diagnose issues months later

## Cost Impact

| Impact Type | Consequence |
|-------------|-------------|
| Data completeness | 1-10% of records may be missing |
| Downstream accuracy | Wrong aggregations, skewed metrics |
| Debugging time | Hours/days finding why numbers don't match |
| Trust | Stakeholders lose confidence in data |

The compute cost isn't the problem - it's the hidden data quality debt.

## Detection

### Check for skipped rows in load history

```sql
-- Files with skipped rows
SELECT 
    file_name,
    row_parsed,
    row_count,
    row_parsed - row_count as skipped_rows,
    ROUND((row_parsed - row_count) * 100.0 / row_parsed, 2) as skip_pct,
    first_error_message
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -7, current_timestamp())
))
WHERE row_parsed > row_count
ORDER BY skipped_rows DESC;
```

### Calculate overall skip rate

```sql
-- Weekly skip rate trend
SELECT 
    DATE_TRUNC('week', last_load_time) as week,
    SUM(row_parsed) as total_parsed,
    SUM(row_count) as total_loaded,
    SUM(row_parsed - row_count) as total_skipped,
    ROUND(SUM(row_parsed - row_count) * 100.0 / SUM(row_parsed), 2) as skip_pct
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -30, current_timestamp())
))
GROUP BY week
ORDER BY week DESC;
```

### Find tables with no error monitoring

```sql
-- Tables using CONTINUE without reject tracking
-- (Manual audit - check if ops.rejected_records captures from each table)
SELECT DISTINCT table_name
FROM TABLE(information_schema.copy_history(
    start_time => dateadd('day', -7, current_timestamp())
))
WHERE row_parsed > row_count
  AND table_name NOT IN (
    SELECT DISTINCT target_table FROM ops.rejected_records
  );
```

## Root Causes

1. **Set and forget** - Configured CONTINUE during development, never added monitoring
2. **Assuming data is clean** - "We trust the source, errors won't happen"
3. **Not understanding the option** - Didn't realize CONTINUE silently skips
4. **Missing observability** - No alerting or dashboards for load health
5. **Pressure to ship** - Monitoring deferred to "later"

## Remediation

### Option 1: Monitor skip rates

Always check load results, even with CONTINUE:

```sql
-- After every load, check what happened
SELECT 
    status,
    SUM(row_parsed) as parsed,
    SUM(row_count) as loaded,
    SUM(row_parsed - row_count) as skipped
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
GROUP BY status;
```

### Option 2: Capture rejected records

Implement systematic reject tracking:

```sql
-- Create rejected records table
CREATE TABLE IF NOT EXISTS ops.rejected_records (
    rejection_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    target_table STRING,
    source_file STRING,
    error_message STRING,
    sample_data VARIANT
);

-- Workflow: Validate first, capture errors, then load
-- Step 1: Validate
COPY INTO raw.events
FROM @stage/events/
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = 'RETURN_ALL_ERRORS';

-- Step 2: Capture errors
INSERT INTO ops.rejected_records (target_table, source_file, error_message)
SELECT 'raw.events', file, error
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE error IS NOT NULL;

-- Step 3: Load with CONTINUE
COPY INTO raw.events
FROM @stage/events/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';
```

### Option 3: Alert on thresholds

```sql
-- Alert if skip rate exceeds threshold
CREATE OR REPLACE ALERT ops.alert_high_skip_rate
    WAREHOUSE = ops_wh
    SCHEDULE = 'USING CRON 0 * * * * UTC'
    IF (EXISTS (
        SELECT 1
        FROM TABLE(information_schema.copy_history(
            table_name => 'raw.events',
            start_time => dateadd('hour', -1, current_timestamp())
        ))
        HAVING SUM(row_parsed - row_count) * 100.0 / NULLIF(SUM(row_parsed), 0) > 5
    ))
    THEN
        CALL system$send_email(
            'data_alerts',
            'data-team@company.com',
            'High Skip Rate Alert',
            'More than 5% of rows skipped in the last hour.'
        );

ALTER ALERT ops.alert_high_skip_rate RESUME;
```

### Option 4: Use stricter settings

Consider whether CONTINUE is actually appropriate:

```sql
-- For critical data, use ABORT_STATEMENT
COPY INTO raw.financial_transactions
FROM @stage/transactions/
FILE_FORMAT = (TYPE = 'CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- For tolerant pipelines, use SKIP_FILE with threshold
COPY INTO raw.events
FROM @stage/events/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'SKIP_FILE_5%';  -- Skip file only if > 5% errors
```

### Option 5: Build observability dashboard

Create a view for monitoring:

```sql
CREATE OR REPLACE VIEW ops.load_health AS
SELECT 
    DATE_TRUNC('day', last_load_time) as load_date,
    table_name,
    COUNT(*) as load_count,
    SUM(row_parsed) as total_parsed,
    SUM(row_count) as total_loaded,
    SUM(row_parsed - row_count) as total_skipped,
    ROUND(SUM(row_parsed - row_count) * 100.0 / NULLIF(SUM(row_parsed), 0), 2) as skip_pct,
    COUNT_IF(status = 'LOAD_FAILED') as failed_loads
FROM TABLE(information_schema.copy_history(
    start_time => dateadd('day', -30, current_timestamp())
))
GROUP BY load_date, table_name;

-- Query for issues
SELECT * FROM ops.load_health
WHERE skip_pct > 1 OR failed_loads > 0
ORDER BY load_date DESC;
```

## Acceptable Use of CONTINUE

CONTINUE is appropriate when:

1. **Source is known-dirty** - External vendor with bad data you can't fix
2. **Best-effort is acceptable** - Analytics where 99% is good enough
3. **Rejects are captured** - You know what's being skipped
4. **Alerts exist** - You're notified when skip rates spike
5. **Downstream handles gaps** - Missing records don't break anything

The key is awareness, not avoidance.

## Related

- [ON_ERROR options](../operations/error-handling/on-error-options.sql)
- [Rejected records](../operations/error-handling/rejected-records.sql)
- [Testing patterns](../operations/testing/)
