# Anti-Pattern: Duplicate Loading

Loading the same files or records multiple times, creating duplicate data.

## The Problem

Snowflake tracks loaded files to prevent reprocessing, but this protection has limits:
- COPY INTO tracking expires after 64 days
- Snowpipe tracking expires after 14 days
- `FORCE = TRUE` bypasses tracking
- Different file names with same content bypass tracking

Duplicates corrupt analytics, inflate storage costs, and break downstream systems expecting unique records.

## Cost Impact

| Impact Type | Consequence |
|-------------|-------------|
| Compute | 2x credits for loading same data |
| Storage | 2x storage costs |
| Query performance | Larger tables, slower queries |
| Data integrity | Wrong aggregations, broken reports |
| Downstream | Failed transformations, incorrect dashboards |

## Detection

### Find duplicate files in load history

```sql
-- Files loaded multiple times
SELECT 
    file_name,
    COUNT(*) as load_count,
    MIN(last_load_time) as first_load,
    MAX(last_load_time) as last_load
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -30, current_timestamp())
))
GROUP BY file_name
HAVING COUNT(*) > 1
ORDER BY load_count DESC;
```

### Find duplicate records in table

```sql
-- Duplicate primary keys
SELECT 
    event_id,
    COUNT(*) as occurrence_count
FROM raw.events
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 100;

-- Duplicate summary
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT event_id) as unique_ids,
    COUNT(*) - COUNT(DISTINCT event_id) as duplicate_rows,
    ROUND((COUNT(*) - COUNT(DISTINCT event_id)) * 100.0 / COUNT(*), 2) as duplicate_pct
FROM raw.events;
```

### Detect duplicate content across different file names

```sql
-- If same content uploaded with different file names
WITH file_hashes AS (
    SELECT 
        METADATA$FILENAME as file_name,
        MD5(LISTAGG(MD5(TO_CHAR($1)), '') 
            WITHIN GROUP (ORDER BY METADATA$FILE_ROW_NUMBER)) as content_hash,
        COUNT(*) as row_count
    FROM @stage/data/
    GROUP BY file_name
)
SELECT 
    content_hash,
    COUNT(*) as file_count,
    LISTAGG(file_name, ', ') as files
FROM file_hashes
GROUP BY content_hash
HAVING COUNT(*) > 1;
```

## Root Causes

1. **Retry logic without idempotency** - Retrying failed jobs without checking what already loaded
2. **FORCE = TRUE in production** - Using force flag routinely instead of selectively
3. **File name changes** - Same content with different timestamps in file names
4. **Multiple pipelines loading same data** - Overlapping Snowpipe and scheduled COPY INTO
5. **Tracking expiration** - Reprocessing old files after 64-day window
6. **Manual interventions** - Ad-hoc loads without checking history

## Remediation

### Design for idempotency

```sql
-- Use MERGE instead of INSERT for replayable loads
MERGE INTO raw.events t
USING (
    SELECT * FROM @stage/events/
    (FILE_FORMAT => 'json_format')
) s
ON t.event_id = s.$1:event_id::STRING
WHEN NOT MATCHED THEN
    INSERT (event_id, event_type, event_timestamp)
    VALUES (s.$1:event_id, s.$1:event_type, s.$1:event_timestamp);
```

### Stage-then-merge pattern

```sql
-- Load to staging
CREATE OR REPLACE TEMPORARY TABLE raw.events_staging AS
SELECT 
    $1:event_id::STRING as event_id,
    $1:event_type::STRING as event_type,
    $1:event_timestamp::TIMESTAMP_NTZ as event_timestamp,
    METADATA$FILENAME as source_file
FROM @stage/events/;

-- Check for duplicates before inserting
SELECT COUNT(*) as would_be_duplicates
FROM raw.events_staging s
WHERE EXISTS (SELECT 1 FROM raw.events e WHERE e.event_id = s.event_id);

-- Insert only new records
INSERT INTO raw.events (event_id, event_type, event_timestamp, source_file)
SELECT event_id, event_type, event_timestamp, source_file
FROM raw.events_staging s
WHERE NOT EXISTS (SELECT 1 FROM raw.events e WHERE e.event_id = s.event_id);
```

### Track loads externally

```sql
-- Create load tracking table
CREATE TABLE IF NOT EXISTS ops.load_tracking (
    load_id STRING,
    file_name STRING,
    file_hash STRING,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    row_count INTEGER
);

-- Before loading, check if already loaded
SELECT COUNT(*) FROM ops.load_tracking
WHERE file_name = 'events/2025/01/15/batch_001.json';

-- After loading, record it
INSERT INTO ops.load_tracking (load_id, file_name, row_count)
SELECT 
    UUID_STRING(),
    file_name,
    row_count
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('minute', -5, current_timestamp())
));
```

### Deduplicate existing data

```sql
-- Find duplicates
CREATE OR REPLACE TEMPORARY TABLE raw.events_duplicates AS
SELECT event_id, COUNT(*) as cnt
FROM raw.events
GROUP BY event_id
HAVING COUNT(*) > 1;

-- Keep only first occurrence (by some criteria)
CREATE OR REPLACE TABLE raw.events_clean AS
SELECT * FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY loaded_at) as rn
    FROM raw.events
)
WHERE rn = 1;

-- Or delete duplicates in place
DELETE FROM raw.events
WHERE (event_id, loaded_at) NOT IN (
    SELECT event_id, MIN(loaded_at)
    FROM raw.events
    GROUP BY event_id
);
```

### Prevent at source

```sql
-- Add unique constraint (if supported by your pattern)
ALTER TABLE raw.events ADD CONSTRAINT pk_events PRIMARY KEY (event_id);
-- Note: Snowflake doesn't enforce PK, but it documents intent

-- Use DISTINCT in COPY INTO transformation
COPY INTO raw.events
FROM (
    SELECT DISTINCT 
        $1:event_id::STRING,
        $1:event_type::STRING,
        $1:event_timestamp::TIMESTAMP_NTZ
    FROM @stage/events/
)
FILE_FORMAT = (TYPE = 'JSON');
```

## Monitoring

```sql
-- Alert on duplicate rate increase
CREATE OR REPLACE ALERT ops.alert_duplicate_rate
    WAREHOUSE = ops_wh
    SCHEDULE = 'USING CRON 0 * * * * UTC'
    IF (EXISTS (
        SELECT 1
        FROM raw.events
        WHERE loaded_at > dateadd('hour', -1, current_timestamp())
        GROUP BY event_id
        HAVING COUNT(*) > 1
    ))
    THEN
        CALL system$send_email(
            'data_alerts',
            'data-team@company.com',
            'Duplicate Data Alert',
            'Duplicates detected in raw.events in the last hour.'
        );
```

## Related

- [Error handling recovery](../operations/error-handling/recovery-procedures.sql)
- [Testing patterns](../operations/testing/)
