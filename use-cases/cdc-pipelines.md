# Use Case: CDC Pipelines

Capturing changes from operational databases and replicating them to Snowflake. Change Data Capture (CDC) enables near-real-time analytics on transactional data.

## Scenario

You have:
- An operational database (PostgreSQL, MySQL, SQL Server, etc.)
- Need to replicate changes (inserts, updates, deletes) to Snowflake
- Requirements for low latency (minutes to seconds)

## Architecture Options

### Option A: Managed CDC (Fivetran, Airbyte, etc.)

```
Source DB → Managed CDC Tool → Snowflake
```

Simplest option. Tool handles extraction, transformation, delivery.

### Option B: Kafka-based CDC

```
Source DB → Debezium → Kafka → Snowflake Kafka Connector → Snowflake
```

More control, lower latency, higher complexity.

### Option C: Custom CDC with Snowpipe

```
Source DB → Custom Extractor → Cloud Storage → Snowpipe → Snowflake
```

DIY approach when managed tools don't fit.

## Option A: Managed CDC

### Fivetran example

Fivetran handles CDC extraction and delivers to Snowflake.

**Setup:** Configured in Fivetran UI/API - connect source DB, target Snowflake.

**Result:** Tables land in Snowflake with standard schema:

```sql
-- Fivetran creates tables with metadata columns
SELECT 
    id,
    customer_name,
    email,
    _fivetran_synced,     -- Sync timestamp
    _fivetran_deleted     -- Soft delete flag
FROM raw_fivetran.customers
WHERE NOT _fivetran_deleted;
```

**Handling deletes:**

```sql
-- Current state view (exclude deleted)
CREATE OR REPLACE VIEW analytics.customers AS
SELECT * FROM raw_fivetran.customers
WHERE NOT _fivetran_deleted;

-- Or use Fivetran's history mode for full audit trail
```

**Post-load trigger (dbt):**

```yaml
# dbt_project.yml - run models after Fivetran sync
on-run-start:
  - "{{ fivetran_log.fivetran_trigger() }}"
```

### Airbyte example

Similar pattern, slightly different metadata:

```sql
SELECT 
    id,
    customer_name,
    email,
    _airbyte_extracted_at,
    _airbyte_meta
FROM raw_airbyte.customers;
```

## Option B: Kafka-based CDC

For teams with Kafka infrastructure and sub-minute latency requirements.

### Architecture

```
PostgreSQL 
    → Debezium (CDC connector)
    → Kafka (change events)
    → Snowflake Kafka Connector (Streaming mode)
    → Snowflake
```

### Debezium configuration

```json
{
  "name": "postgres-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres.example.com",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "${secrets:postgres-password}",
    "database.dbname": "production",
    "table.include.list": "public.customers,public.orders",
    "topic.prefix": "cdc",
    "plugin.name": "pgoutput"
  }
}
```

### Snowflake Kafka connector

```properties
# Connector config for Snowpipe Streaming
name=snowflake-cdc-sink
connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
tasks.max=4

snowflake.url.name=account.snowflakecomputing.com
snowflake.user.name=kafka_user
snowflake.private.key=<key>
snowflake.database.name=RAW
snowflake.schema.name=CDC

# Use streaming for low latency
snowflake.ingestion.method=SNOWPIPE_STREAMING

# CDC topics
topics=cdc.public.customers,cdc.public.orders
snowflake.topic2table.map=cdc.public.customers:CUSTOMERS_CDC,cdc.public.orders:ORDERS_CDC

# Aggressive flush for low latency
buffer.count.records=100
buffer.flush.time=1
```

### Processing CDC records

Debezium sends change events with before/after state:

```sql
-- Raw CDC records land as JSON
SELECT 
    RECORD_CONTENT:payload.op::STRING as operation,  -- c=create, u=update, d=delete
    RECORD_CONTENT:payload.after as after_state,
    RECORD_CONTENT:payload.before as before_state,
    RECORD_CONTENT:payload.source.ts_ms as source_timestamp
FROM raw.customers_cdc
LIMIT 10;
```

### Materializing current state

```sql
-- Create materialized current state from CDC stream
CREATE OR REPLACE TABLE analytics.customers AS
WITH ranked_changes AS (
    SELECT 
        RECORD_CONTENT:payload.after.id::STRING as customer_id,
        RECORD_CONTENT:payload.op::STRING as operation,
        RECORD_CONTENT:payload.after as data,
        RECORD_CONTENT:payload.source.ts_ms::TIMESTAMP_NTZ as change_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY RECORD_CONTENT:payload.after.id::STRING 
            ORDER BY RECORD_CONTENT:payload.source.ts_ms DESC
        ) as rn
    FROM raw.customers_cdc
)
SELECT 
    customer_id,
    data:customer_name::STRING as customer_name,
    data:email::STRING as email,
    data:status::STRING as status,
    change_timestamp as updated_at
FROM ranked_changes
WHERE rn = 1 AND operation != 'd';  -- Exclude deletes
```

### Incremental refresh with Streams

```sql
-- Create stream on CDC table
CREATE OR REPLACE STREAM raw.customers_cdc_stream
ON TABLE raw.customers_cdc;

-- Task to process changes
CREATE OR REPLACE TASK analytics.refresh_customers
    WAREHOUSE = transform_wh
    SCHEDULE = 'USING CRON */5 * * * * UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('raw.customers_cdc_stream')
AS
MERGE INTO analytics.customers t
USING (
    SELECT 
        RECORD_CONTENT:payload.after.id::STRING as customer_id,
        RECORD_CONTENT:payload.op::STRING as operation,
        RECORD_CONTENT:payload.after:customer_name::STRING as customer_name,
        RECORD_CONTENT:payload.after:email::STRING as email,
        RECORD_CONTENT:payload.source.ts_ms::TIMESTAMP_NTZ as updated_at
    FROM raw.customers_cdc_stream
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY RECORD_CONTENT:payload.after.id 
        ORDER BY RECORD_CONTENT:payload.source.ts_ms DESC
    ) = 1
) s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.operation = 'd' THEN DELETE
WHEN MATCHED THEN UPDATE SET 
    customer_name = s.customer_name,
    email = s.email,
    updated_at = s.updated_at
WHEN NOT MATCHED AND s.operation != 'd' THEN INSERT 
    (customer_id, customer_name, email, updated_at)
    VALUES (s.customer_id, s.customer_name, s.email, s.updated_at);

ALTER TASK analytics.refresh_customers RESUME;
```

## Option C: Custom CDC with Snowpipe

For simpler needs or when managed tools aren't available.

### Extraction script

```python
# Example: Extract changes from PostgreSQL to S3
import psycopg2
import boto3
import json
from datetime import datetime, timedelta

def extract_changes(last_sync_time):
    conn = psycopg2.connect(...)
    cursor = conn.cursor()
    
    # Query for records changed since last sync
    cursor.execute("""
        SELECT id, customer_name, email, status, updated_at
        FROM customers
        WHERE updated_at > %s
    """, (last_sync_time,))
    
    records = cursor.fetchall()
    
    # Write to S3 as JSON
    s3 = boto3.client('s3')
    filename = f"customers/changes_{datetime.now().isoformat()}.json"
    
    s3.put_object(
        Bucket='cdc-bucket',
        Key=filename,
        Body=json.dumps([dict(zip(['id', 'name', 'email', 'status', 'updated_at'], r)) for r in records])
    )
    
    return len(records)
```

### Snowpipe for ingestion

```sql
CREATE OR REPLACE PIPE raw.customers_changes_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO raw.customers_changes
    FROM @cdc_stage/customers/
    FILE_FORMAT = (TYPE = 'JSON');
```

### Merge to target

```sql
-- Scheduled merge from changes table
MERGE INTO analytics.customers t
USING (
    SELECT * FROM raw.customers_changes
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
) s
ON t.customer_id = s.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

## Handling Soft Deletes

Many CDC systems use soft deletes rather than physical deletes:

```sql
-- Option 1: Filter in views
CREATE VIEW analytics.active_customers AS
SELECT * FROM raw.customers WHERE NOT is_deleted;

-- Option 2: Separate deleted records
CREATE TABLE raw.customers_deleted AS
SELECT * FROM raw.customers WHERE is_deleted;

DELETE FROM raw.customers WHERE is_deleted;
```

## Monitoring

```sql
-- CDC lag monitoring
SELECT 
    DATE_TRUNC('minute', _loaded_at) as minute,
    COUNT(*) as records,
    AVG(DATEDIFF('second', 
        RECORD_CONTENT:payload.source.ts_ms::TIMESTAMP_NTZ, 
        _loaded_at
    )) as avg_lag_seconds
FROM raw.customers_cdc
WHERE _loaded_at > dateadd('hour', -1, current_timestamp())
GROUP BY minute
ORDER BY minute DESC;

-- Stream processing status
SHOW STREAMS LIKE '%cdc%';

-- Check stream has data
SELECT SYSTEM$STREAM_HAS_DATA('raw.customers_cdc_stream');
```

## Troubleshooting

### High latency

- Check Kafka consumer lag
- Verify connector buffer settings
- Monitor Snowflake streaming metrics

### Missing changes

- Verify CDC tool is capturing all tables
- Check for filtered events in connector config
- Validate stream hasn't been consumed without processing

### Duplicate records

- Ensure MERGE uses proper deduplication
- Check for multiple CDC sources writing to same table
- Validate ROW_NUMBER partitioning

## Related

- [Snowpipe Streaming](../patterns/streaming/)
- [Kafka connector](../patterns/streaming/kafka-connector.md)
- [Incremental loading](incremental-loading.md)
