# Anti-Pattern: Missing Partitions

Querying external tables without proper partition pruning, causing full scans.

## The Problem

External tables read from cloud storage on demand. Without partition definitions, every query scans every file - regardless of filters in the WHERE clause.

A query for "yesterday's data" shouldn't read 5 years of history, but that's exactly what happens without partition pruning.

## Cost Impact

**Scenario: 1TB external table, query for single day**

| Setup | Files Scanned | Time | Relative Cost |
|-------|---------------|------|---------------|
| No partitions | All (1TB) | Minutes | 100x |
| Date partitioned | Day only (~3GB) | Seconds | 1x |

The impact scales with data volume. Multi-TB tables without partitions become essentially unusable.

## Detection

### Check if external table has partitions

```sql
-- Show external table definition
SHOW EXTERNAL TABLES LIKE 'your_table';

-- Look for partition_columns in output
-- Empty = no partitions defined
```

### Check query profile for full scans

```sql
-- Run a filtered query
SELECT COUNT(*) FROM lake.events_archive
WHERE event_date = '2025-01-15';

-- Check query profile
SELECT 
    query_id,
    query_text,
    bytes_scanned,
    partitions_scanned,
    partitions_total
FROM snowflake.account_usage.query_history
WHERE query_id = LAST_QUERY_ID();

-- If partitions_scanned = partitions_total, no pruning happened
```

### Check external table queries for efficiency

```sql
-- Find external table queries with poor pruning
SELECT 
    query_id,
    query_text,
    bytes_scanned / 1e9 as gb_scanned,
    partitions_scanned,
    partitions_total,
    ROUND(partitions_scanned * 100.0 / NULLIF(partitions_total, 0), 1) as pct_scanned
FROM snowflake.account_usage.query_history
WHERE query_text ILIKE '%your_external_table%'
  AND start_time > dateadd('day', -7, current_date())
  AND partitions_total > 100
  AND partitions_scanned * 1.0 / partitions_total > 0.5  -- More than 50% scanned
ORDER BY bytes_scanned DESC;
```

## Root Causes

1. **Partition columns not defined** - External table created without PARTITION BY
2. **Filters on wrong columns** - Filtering on non-partition columns
3. **Function on partition column** - `WHERE DATE(timestamp) = ...` instead of `WHERE date_col = ...`
4. **Storage not partitioned** - Files in flat structure, not date/key folders

## Remediation

### Add partition columns to external table

```sql
-- Recreate external table with partitions
-- Data stored as: s3://bucket/events/year=2025/month=01/day=15/*.parquet

DROP EXTERNAL TABLE IF EXISTS lake.events_archive;

CREATE EXTERNAL TABLE lake.events_archive (
    event_id STRING AS (VALUE:event_id::STRING),
    event_type STRING AS (VALUE:event_type::STRING),
    event_timestamp TIMESTAMP_NTZ AS (VALUE:event_timestamp::TIMESTAMP_NTZ),
    -- Partition columns extracted from file path
    year INTEGER AS (SPLIT_PART(METADATA$FILENAME, '/', 3)::INTEGER),
    month INTEGER AS (SPLIT_PART(METADATA$FILENAME, '/', 4)::INTEGER),
    day INTEGER AS (SPLIT_PART(METADATA$FILENAME, '/', 5)::INTEGER)
)
PARTITION BY (year, month, day)
WITH LOCATION = @archive_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = FALSE;

-- Refresh to pick up partitions
ALTER EXTERNAL TABLE lake.events_archive REFRESH;
```

### Query with partition filters

```sql
-- GOOD: Filter on partition columns
SELECT * FROM lake.events_archive
WHERE year = 2025 AND month = 1 AND day = 15;

-- BAD: Filter on non-partition column (still scans everything)
SELECT * FROM lake.events_archive
WHERE event_timestamp::DATE = '2025-01-15';

-- BETTER: Combine partition filter with detailed filter
SELECT * FROM lake.events_archive
WHERE year = 2025 AND month = 1 AND day = 15
  AND event_timestamp >= '2025-01-15 10:00:00'
  AND event_timestamp < '2025-01-15 11:00:00';
```

### Restructure storage

If files are in a flat structure, reorganize:

```
-- Before (bad):
s3://bucket/events/events_001.parquet
s3://bucket/events/events_002.parquet
...

-- After (good):
s3://bucket/events/year=2025/month=01/day=15/events_001.parquet
s3://bucket/events/year=2025/month=01/day=15/events_002.parquet
...
```

This requires external processing (Spark, AWS Glue, etc.) to reorganize existing files.

### Consider loading hot data

For frequently-queried date ranges, load into native tables:

```sql
-- Load recent data into native table
CREATE TABLE raw.events_recent AS
SELECT * FROM lake.events_archive
WHERE year = 2025 AND month >= 1;

-- Query native table for recent data (fast)
-- Query external table for archive (acceptable slowness)
```

## Monitoring

```sql
-- Track external table query efficiency
CREATE TABLE IF NOT EXISTS ops.external_table_efficiency (
    check_date DATE,
    table_name STRING,
    query_count INTEGER,
    avg_pct_scanned FLOAT,
    total_gb_scanned FLOAT
);

-- Daily check
INSERT INTO ops.external_table_efficiency
SELECT 
    CURRENT_DATE(),
    'lake.events_archive',
    COUNT(*),
    AVG(partitions_scanned * 100.0 / NULLIF(partitions_total, 0)),
    SUM(bytes_scanned) / 1e9
FROM snowflake.account_usage.query_history
WHERE query_text ILIKE '%events_archive%'
  AND start_time > dateadd('day', -1, current_date())
  AND partitions_total > 0;

-- Alert if efficiency drops
SELECT * FROM ops.external_table_efficiency
WHERE avg_pct_scanned > 50
ORDER BY check_date DESC;
```

## Related

- [External table partitioning](../../patterns/external-tables/partitioning.sql)
- [File sizing guidance](../file-sizing/)
