# Use Case: Batch Migration

Loading historical data from another system into Snowflake. Common when onboarding a new data source, migrating from another warehouse, or backfilling.

## Scenario

You have:
- Historical data in files (Parquet, CSV, JSON) staged in cloud storage
- A one-time or periodic need to load bulk data
- Tolerance for batch processing (not real-time)

## Architecture

```
Source System → Export to Files → Cloud Storage → COPY INTO → Snowflake
                                      ↑
                                  (S3/GCS/Azure)
```

## Setup

### 1. Create storage integration

```sql
-- AWS S3 example
CREATE OR REPLACE STORAGE INTEGRATION migration_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-migration'
    STORAGE_ALLOWED_LOCATIONS = ('s3://migration-bucket/');

-- Get IAM user ARN and external ID for AWS trust policy
DESC STORAGE INTEGRATION migration_integration;
```

### 2. Create external stage

```sql
CREATE OR REPLACE STAGE migration_stage
    STORAGE_INTEGRATION = migration_integration
    URL = 's3://migration-bucket/data/'
    FILE_FORMAT = (TYPE = 'PARQUET');
```

### 3. Create target table

```sql
-- Option A: Define schema manually
CREATE OR REPLACE TABLE raw.customers (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file STRING
);

-- Option B: Infer schema from files
CREATE OR REPLACE TABLE raw.customers
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@migration_stage/customers/',
            FILE_FORMAT => 'parquet_format'
        )
    )
);

-- Add audit columns
ALTER TABLE raw.customers ADD COLUMN _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
ALTER TABLE raw.customers ADD COLUMN _source_file STRING;
```

## Implementation

### Step 1: Validate before loading

```sql
-- Check file structure
LIST @migration_stage/customers/;

-- Preview data
SELECT * FROM @migration_stage/customers/ LIMIT 10;

-- Validate schema compatibility
COPY INTO raw.customers
FROM @migration_stage/customers/
FILE_FORMAT = (TYPE = 'PARQUET')
VALIDATION_MODE = 'RETURN_ALL_ERRORS';

-- Check results
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

### Step 2: Load data

```sql
-- For clean data
COPY INTO raw.customers
FROM (
    SELECT 
        $1:customer_id::STRING,
        $1:customer_name::STRING,
        $1:email::STRING,
        $1:created_at::TIMESTAMP_NTZ,
        $1:updated_at::TIMESTAMP_NTZ,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @migration_stage/customers/
)
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR = 'ABORT_STATEMENT';

-- For data with known quality issues
COPY INTO raw.customers
FROM (
    SELECT 
        $1:customer_id::STRING,
        $1:customer_name::STRING,
        $1:email::STRING,
        $1:created_at::TIMESTAMP_NTZ,
        $1:updated_at::TIMESTAMP_NTZ,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @migration_stage/customers/
)
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR = 'CONTINUE';
```

### Step 3: Verify load

```sql
-- Check row counts
SELECT COUNT(*) as loaded_rows FROM raw.customers;

-- Compare to source count (if known)
-- Expected: 1,000,000 rows

-- Check for issues
SELECT 
    status,
    SUM(row_count) as rows_loaded,
    SUM(row_parsed) as rows_parsed,
    SUM(row_parsed - row_count) as rows_skipped
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
GROUP BY status;

-- Sample data quality
SELECT 
    COUNT(*) as total,
    COUNT_IF(customer_id IS NULL) as null_ids,
    COUNT_IF(email IS NULL) as null_emails,
    MIN(created_at) as earliest,
    MAX(created_at) as latest
FROM raw.customers;
```

## Large Migration Strategy

For very large migrations (100GB+), load in chunks:

```sql
-- Load by date partition
COPY INTO raw.events
FROM @migration_stage/events/
PATTERN = '.*year=2023/.*'
FILE_FORMAT = (TYPE = 'PARQUET');

COPY INTO raw.events
FROM @migration_stage/events/
PATTERN = '.*year=2024/.*'
FILE_FORMAT = (TYPE = 'PARQUET');

-- Or by file pattern
COPY INTO raw.events
FROM @migration_stage/events/
PATTERN = '.*batch_00[0-9].*'
FILE_FORMAT = (TYPE = 'PARQUET');
```

For massive migrations, consider warehouse sizing:

```sql
-- Use larger warehouse for backfill
ALTER WAREHOUSE migration_wh SET WAREHOUSE_SIZE = 'LARGE';

COPY INTO raw.events FROM @migration_stage/events/ ...;

-- Scale back down
ALTER WAREHOUSE migration_wh SET WAREHOUSE_SIZE = 'SMALL';
```

## Monitoring

```sql
-- Track migration progress
SELECT 
    DATE_TRUNC('hour', last_load_time) as hour,
    COUNT(*) as files_loaded,
    SUM(row_count) as rows_loaded,
    SUM(file_size) / 1e9 as gb_loaded
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.customers',
    start_time => dateadd('day', -7, current_timestamp())
))
GROUP BY hour
ORDER BY hour;

-- Check for failed files
SELECT file_name, status, first_error_message
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.customers',
    start_time => dateadd('day', -7, current_timestamp())
))
WHERE status != 'LOADED';
```

## Troubleshooting

### "Number of columns in file does not match"

Schema mismatch between file and table:

```sql
-- Check file schema
SELECT * FROM TABLE(
    INFER_SCHEMA(LOCATION => '@migration_stage/customers/', FILE_FORMAT => 'parquet_format')
);

-- Compare to table
DESC TABLE raw.customers;
```

### "Timestamp 'X' is not recognized"

Date format mismatch:

```sql
-- Use TRY_TO_TIMESTAMP with format
SELECT TRY_TO_TIMESTAMP($1:date_field::STRING, 'YYYY-MM-DD HH24:MI:SS')
FROM @migration_stage/data/ LIMIT 10;
```

### Very slow loading

Check file sizes:

```sql
LIST @migration_stage/data/;

-- If many small files, consider consolidating upstream
-- Or accept the overhead
```

### Out of memory

Reduce batch size:

```sql
COPY INTO raw.large_table
FROM @migration_stage/data/
FILE_FORMAT = (TYPE = 'PARQUET')
SIZE_LIMIT = 5000000000;  -- 5GB per COPY statement
```

## Post-Migration

### Clean up

```sql
-- After successful migration, optionally clean stage
-- (Be careful - verify data is correct first!)
-- REMOVE @migration_stage/customers/;

-- Or keep for audit/replay
```

### Set up ongoing loads

If this becomes a recurring pattern, consider:
- Scheduled COPY INTO for batch updates
- Snowpipe for continuous file ingestion
- See [Incremental Loading](incremental-loading.md)

## Related

- [COPY INTO patterns](../patterns/copy-into/)
- [File sizing](../operations/file-sizing/)
- [Error handling](../operations/error-handling/)
