-- ============================================================================
-- External Tables: Partitioning
-- ============================================================================
-- Partition pruning is critical for external table performance. Without it,
-- queries scan all files. With proper partitioning, queries only read
-- relevant partitions.

-- ----------------------------------------------------------------------------
-- Directory-based partitioning (most common)
-- ----------------------------------------------------------------------------

-- Data organized as: s3://bucket/events/year=2025/month=01/day=15/*.parquet

CREATE OR REPLACE EXTERNAL TABLE lake.events_partitioned (
    event_id STRING AS (VALUE:event_id::STRING),
    event_type STRING AS (VALUE:event_type::STRING),
    event_timestamp TIMESTAMP_NTZ AS (VALUE:event_timestamp::TIMESTAMP_NTZ),
    payload VARIANT AS (VALUE:payload::VARIANT),
    -- Partition columns extracted from path
    year INTEGER AS (SPLIT_PART(METADATA$FILENAME, '/', 2)::INTEGER),
    month INTEGER AS (SPLIT_PART(METADATA$FILENAME, '/', 3)::INTEGER),
    day INTEGER AS (SPLIT_PART(METADATA$FILENAME, '/', 4)::INTEGER)
)
PARTITION BY (year, month, day)
WITH LOCATION = @archive_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = FALSE;

-- Query with partition pruning
SELECT * FROM lake.events_partitioned
WHERE year = 2025 AND month = 1 AND day = 15;
-- Only scans files in year=2025/month=01/day=15/

-- ----------------------------------------------------------------------------
-- Hive-style partitions
-- ----------------------------------------------------------------------------

-- Path: s3://bucket/data/region=us-east/date=2025-01-15/*.parquet

CREATE OR REPLACE EXTERNAL TABLE lake.regional_data (
    id STRING AS (VALUE:id::STRING),
    value DOUBLE AS (VALUE:value::DOUBLE),
    -- Extract Hive partition values
    region STRING AS (
        REGEXP_SUBSTR(METADATA$FILENAME, 'region=([^/]+)', 1, 1, 'e')
    ),
    date DATE AS (
        REGEXP_SUBSTR(METADATA$FILENAME, 'date=([^/]+)', 1, 1, 'e')::DATE
    )
)
PARTITION BY (region, date)
WITH LOCATION = @archive_stage/data/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = FALSE;

-- Query specific region and date
SELECT * FROM lake.regional_data
WHERE region = 'us-east' AND date = '2025-01-15';

-- ----------------------------------------------------------------------------
-- Adding partitions manually
-- ----------------------------------------------------------------------------

-- For external tables without auto-refresh, add partitions explicitly
-- after new data arrives

ALTER EXTERNAL TABLE lake.events_partitioned
ADD PARTITION (year = 2025, month = 2, day = 1)
LOCATION 'year=2025/month=02/day=01/';

-- Add multiple partitions
ALTER EXTERNAL TABLE lake.events_partitioned ADD PARTITION
    (year = 2025, month = 2, day = 2) LOCATION 'year=2025/month=02/day=02/',
    (year = 2025, month = 2, day = 3) LOCATION 'year=2025/month=02/day=03/';

-- ----------------------------------------------------------------------------
-- Viewing partitions
-- ----------------------------------------------------------------------------

-- List partitions for an external table
SHOW EXTERNAL TABLES LIKE 'events_partitioned';

-- Check partition metadata
SELECT 
    partition_columns,
    partition_values,
    row_count
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ----------------------------------------------------------------------------
-- Removing partitions
-- ----------------------------------------------------------------------------

-- Remove specific partition
ALTER EXTERNAL TABLE lake.events_partitioned
DROP PARTITION (year = 2024, month = 1, day = 1);

-- ----------------------------------------------------------------------------
-- Partition pruning verification
-- ----------------------------------------------------------------------------

-- Check if query uses partition pruning
EXPLAIN SELECT * FROM lake.events_partitioned
WHERE year = 2025 AND month = 1;
-- Look for "partition pruning" in explain output

-- Compare scan with and without partition filter
-- With pruning (fast):
SELECT COUNT(*) FROM lake.events_partitioned
WHERE year = 2025 AND month = 1 AND day = 15;

-- Without pruning (slow - scans everything):
SELECT COUNT(*) FROM lake.events_partitioned
WHERE event_type = 'click';

-- ----------------------------------------------------------------------------
-- Computed partition columns
-- ----------------------------------------------------------------------------

-- Derive partition from data columns for better ergonomics
CREATE OR REPLACE EXTERNAL TABLE lake.events_computed (
    event_id STRING AS (VALUE:event_id::STRING),
    event_type STRING AS (VALUE:event_type::STRING),
    event_timestamp TIMESTAMP_NTZ AS (VALUE:event_timestamp::TIMESTAMP_NTZ),
    -- Compute date parts from path
    event_date DATE AS (
        TO_DATE(
            SPLIT_PART(METADATA$FILENAME, '/', 2) || '-' ||
            SPLIT_PART(METADATA$FILENAME, '/', 3) || '-' ||
            SPLIT_PART(METADATA$FILENAME, '/', 4),
            'YYYY-MM-DD'
        )
    )
)
PARTITION BY (event_date)
WITH LOCATION = @archive_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = FALSE;

-- Query using computed date column
SELECT * FROM lake.events_computed
WHERE event_date = '2025-01-15';

-- ----------------------------------------------------------------------------
-- Best practices for partitioning
-- ----------------------------------------------------------------------------

/*
1. Choose partition columns based on query patterns
   - Date is most common (daily or monthly partitions)
   - Region/tenant for multi-tenant data
   - Avoid high-cardinality columns (user_id)

2. Keep partition sizes reasonable
   - Too small: many files, metadata overhead
   - Too large: limited pruning benefit
   - Target: 100MB-1GB per partition

3. Match storage organization to partition scheme
   - Upstream systems should write with partition structure
   - Retrofitting partitions is expensive

4. Always filter on partition columns
   - Queries without partition filters scan everything
   - Add partition column to WHERE clause first

5. Monitor partition count
   - External tables with 1M+ partitions have metadata overhead
   - Consider coarser partitioning for very granular data
*/

-- ----------------------------------------------------------------------------
-- Partition performance comparison
-- ----------------------------------------------------------------------------

-- Good: Uses partition pruning
SELECT COUNT(*) FROM lake.events_partitioned
WHERE year = 2025 AND month = 1;

-- Bad: No partition pruning (scans all files)
SELECT COUNT(*) FROM lake.events_partitioned
WHERE TO_DATE(event_timestamp) = '2025-01-15';
-- The partition columns (year, month, day) aren't in the filter

-- Better: Combine partition filter with detailed filter
SELECT COUNT(*) FROM lake.events_partitioned
WHERE year = 2025 AND month = 1 AND day = 15
  AND event_timestamp >= '2025-01-15 10:00:00'
  AND event_timestamp < '2025-01-15 11:00:00';
