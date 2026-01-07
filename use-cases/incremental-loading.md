# Use Case: Incremental Loading

Adding new or updated records to existing tables on a regular schedule. The most common ongoing data loading pattern.

## Scenario

You have:
- An existing table with historical data
- New files arriving periodically (hourly, daily)
- Files containing either new records only, or a mix of new and updated records

## Architecture Options

### Option A: Append-only (new records)

```
New Files → COPY INTO → Append to Table
```

Use when: Source only sends new records, never updates.

### Option B: Upsert (new + updated records)

```
New Files → Stage Table → MERGE INTO → Target Table
```

Use when: Files may contain updates to existing records.

## Setup

### Append-only setup

```sql
-- Target table with load tracking
CREATE OR REPLACE TABLE raw.events (
    event_id STRING PRIMARY KEY,
    event_type STRING,
    event_timestamp TIMESTAMP_NTZ,
    payload VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file STRING
);

-- Stage for incoming files
CREATE OR REPLACE STAGE incremental_stage
    STORAGE_INTEGRATION = data_integration
    URL = 's3://data-bucket/events/'
    FILE_FORMAT = (TYPE = 'PARQUET');
```

### Upsert setup

```sql
-- Target table
CREATE OR REPLACE TABLE raw.customers (
    customer_id STRING PRIMARY KEY,
    customer_name STRING,
    email STRING,
    status STRING,
    updated_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file STRING
);

-- Staging table (recreated each load)
CREATE OR REPLACE TABLE raw.customers_staging (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    status STRING,
    updated_at TIMESTAMP_NTZ,
    _source_file STRING
);
```

## Implementation: Append-Only

### Simple scheduled load

```sql
-- Run on schedule (Airflow, Snowflake Task, etc.)
COPY INTO raw.events
FROM (
    SELECT 
        $1:event_id::STRING,
        $1:event_type::STRING,
        $1:event_timestamp::TIMESTAMP_NTZ,
        $1:payload::VARIANT,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @incremental_stage/
)
FILE_FORMAT = (TYPE = 'PARQUET')
PATTERN = '.*[.]parquet'
ON_ERROR = 'CONTINUE';
```

Snowflake tracks loaded files for 64 days. Files won't reload unless you use `FORCE = TRUE`.

### With Snowpipe (continuous)

```sql
-- Create pipe for automatic loading
CREATE OR REPLACE PIPE raw.events_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO raw.events
    FROM (
        SELECT 
            $1:event_id::STRING,
            $1:event_type::STRING,
            $1:event_timestamp::TIMESTAMP_NTZ,
            $1:payload::VARIANT,
            CURRENT_TIMESTAMP(),
            METADATA$FILENAME
        FROM @incremental_stage/
    )
    FILE_FORMAT = (TYPE = 'PARQUET');
```

## Implementation: Upsert (MERGE)

### Step 1: Load to staging

```sql
-- Clear and reload staging
TRUNCATE TABLE raw.customers_staging;

COPY INTO raw.customers_staging
FROM (
    SELECT 
        $1:customer_id::STRING,
        $1:customer_name::STRING,
        $1:email::STRING,
        $1:status::STRING,
        $1:updated_at::TIMESTAMP_NTZ,
        METADATA$FILENAME
    FROM @incremental_stage/customers/
)
FILE_FORMAT = (TYPE = 'PARQUET');
```

### Step 2: Merge to target

```sql
MERGE INTO raw.customers t
USING raw.customers_staging s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
    UPDATE SET 
        customer_name = s.customer_name,
        email = s.email,
        status = s.status,
        updated_at = s.updated_at,
        _loaded_at = CURRENT_TIMESTAMP(),
        _source_file = s._source_file
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, email, status, updated_at, _loaded_at, _source_file)
    VALUES (s.customer_id, s.customer_name, s.email, s.status, s.updated_at, CURRENT_TIMESTAMP(), s._source_file);
```

### Complete upsert procedure

```sql
CREATE OR REPLACE PROCEDURE raw.load_customers_incremental()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    staged_count INTEGER;
    merged_inserts INTEGER;
    merged_updates INTEGER;
BEGIN
    -- Stage new files
    TRUNCATE TABLE raw.customers_staging;
    
    COPY INTO raw.customers_staging
    FROM @incremental_stage/customers/
    FILE_FORMAT = (TYPE = 'PARQUET');
    
    SELECT COUNT(*) INTO staged_count FROM raw.customers_staging;
    
    -- Merge
    MERGE INTO raw.customers t
    USING raw.customers_staging s
    ON t.customer_id = s.customer_id
    WHEN MATCHED AND s.updated_at > t.updated_at THEN
        UPDATE SET 
            customer_name = s.customer_name,
            email = s.email,
            status = s.status,
            updated_at = s.updated_at,
            _loaded_at = CURRENT_TIMESTAMP(),
            _source_file = s._source_file
    WHEN NOT MATCHED THEN
        INSERT (customer_id, customer_name, email, status, updated_at, _loaded_at, _source_file)
        VALUES (s.customer_id, s.customer_name, s.email, s.status, s.updated_at, CURRENT_TIMESTAMP(), s._source_file);
    
    -- Get merge stats (approximate)
    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    
    RETURN 'Staged: ' || staged_count || ' rows';
END;
$$;

-- Run the procedure
CALL raw.load_customers_incremental();
```

## Handling Late-Arriving Data

For event data that may arrive out of order:

```sql
-- Use event timestamp for deduplication, not load time
MERGE INTO raw.events t
USING (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY event_id ORDER BY event_timestamp DESC
    ) as rn
    FROM raw.events_staging
) s
ON t.event_id = s.event_id
WHEN MATCHED AND s.rn = 1 AND s.event_timestamp > t.event_timestamp THEN
    UPDATE SET ...
WHEN NOT MATCHED AND s.rn = 1 THEN
    INSERT ...;
```

## Scheduling

### Snowflake Task

```sql
-- Create task for hourly load
CREATE OR REPLACE TASK raw.hourly_customer_load
    WAREHOUSE = load_wh
    SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
    CALL raw.load_customers_incremental();

-- Enable the task
ALTER TASK raw.hourly_customer_load RESUME;
```

### External orchestration

For Airflow, Dagster, or other tools, call the procedure or run SQL directly:

```python
# Airflow example
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

load_task = SnowflakeOperator(
    task_id='load_customers',
    sql='CALL raw.load_customers_incremental()',
    snowflake_conn_id='snowflake_default'
)
```

## Monitoring

```sql
-- Track incremental load history
SELECT 
    DATE_TRUNC('day', last_load_time) as load_date,
    COUNT(*) as load_count,
    SUM(row_count) as total_rows,
    AVG(row_count) as avg_rows_per_load
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.customers_staging',
    start_time => dateadd('day', -7, current_timestamp())
))
GROUP BY load_date
ORDER BY load_date DESC;

-- Check for gaps
SELECT 
    DATE_TRUNC('hour', _loaded_at) as hour,
    COUNT(*) as rows_loaded
FROM raw.customers
WHERE _loaded_at > dateadd('day', -2, current_timestamp())
GROUP BY hour
ORDER BY hour;
```

## Troubleshooting

### Duplicate records appearing

Check if MERGE key is unique:

```sql
SELECT customer_id, COUNT(*)
FROM raw.customers_staging
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

If duplicates in source, dedupe in staging first:

```sql
CREATE OR REPLACE TEMPORARY TABLE raw.customers_staging_deduped AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) as rn
    FROM raw.customers_staging
)
WHERE rn = 1;
```

### Updates not applying

Check the MATCHED condition:

```sql
-- Are staged records actually newer?
SELECT 
    s.customer_id,
    s.updated_at as staged_updated_at,
    t.updated_at as target_updated_at,
    s.updated_at > t.updated_at as would_update
FROM raw.customers_staging s
JOIN raw.customers t ON s.customer_id = t.customer_id
WHERE s.updated_at <= t.updated_at;
```

### Performance degradation

If MERGE is slow:
- Ensure target table is clustered on merge key
- Check staging table size (should be small relative to target)
- Consider batch size limits

```sql
-- Add clustering
ALTER TABLE raw.customers CLUSTER BY (customer_id);
```

## Related

- [COPY INTO patterns](../patterns/copy-into/)
- [Snowpipe patterns](../patterns/snowpipe/)
- [Duplicate loading anti-pattern](../anti-patterns/duplicate-loading.md)
