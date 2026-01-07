# Kafka Connector for Snowpipe Streaming

The Snowflake Connector for Kafka can use Snowpipe Streaming for sub-second ingestion. This is the most common path for teams with existing Kafka infrastructure.

## Configuration

Set the ingestion method to Snowpipe Streaming in your connector properties:

```properties
# Kafka Connect configuration
name=snowflake-streaming-sink
connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
tasks.max=4

# Snowflake connection
snowflake.url.name=<account>.snowflakecomputing.com
snowflake.user.name=kafka_user
snowflake.private.key=<private_key>
snowflake.database.name=RAW
snowflake.schema.name=KAFKA

# Use Snowpipe Streaming instead of file-based Snowpipe
snowflake.ingestion.method=SNOWPIPE_STREAMING

# Topic to table mapping
topics=events,orders,users
snowflake.topic2table.map=events:EVENTS,orders:ORDERS,users:USERS

# Buffer settings (tune for latency vs throughput)
buffer.count.records=10000
buffer.flush.time=10
buffer.size.bytes=5000000

# Schema handling
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

## Key Properties

### Ingestion Method

```properties
snowflake.ingestion.method=SNOWPIPE_STREAMING
```

Options:
- `SNOWPIPE` - File-based ingestion (default)
- `SNOWPIPE_STREAMING` - Row-level streaming

### Buffer Tuning

Buffer settings control the trade-off between latency and efficiency:

```properties
# Flush when any condition is met
buffer.count.records=10000    # Max records before flush
buffer.flush.time=10          # Max seconds before flush
buffer.size.bytes=5000000     # Max bytes before flush
```

Lower values = lower latency, higher overhead.
Higher values = higher latency, better efficiency.

For sub-second requirements, aggressive settings:

```properties
buffer.count.records=100
buffer.flush.time=1
buffer.size.bytes=100000
```

### Channel Management

Each task opens channels to target tables. More tasks = more parallelism but more channels to manage.

```properties
tasks.max=4  # Parallelism level
```

Monitor channel health in Snowflake:

```sql
SELECT * FROM TABLE(information_schema.snowpipe_streaming_file_migration_history(
    date_range_start => dateadd('hour', -1, current_timestamp()),
    date_range_end => current_timestamp()
));
```

## Authentication

Key pair authentication is required for Snowpipe Streaming:

```properties
snowflake.user.name=kafka_user
snowflake.private.key=<base64_encoded_private_key>
# Or reference a file
snowflake.private.key.path=/path/to/rsa_key.p8
snowflake.private.key.passphrase=<passphrase_if_encrypted>
```

Generate key pair:

```bash
# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Assign to Snowflake user
ALTER USER kafka_user SET RSA_PUBLIC_KEY='<public_key_contents>';
```

## Schema Handling

### JSON (most common)

```properties
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

Data lands in a `RECORD_CONTENT` VARIANT column by default.

### Avro with Schema Registry

```properties
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry:8081
```

### Schematization

Enable automatic schema detection to create typed columns:

```properties
snowflake.enable.schematization=true
```

Snowflake will infer column types from the data. Requires tables to not pre-exist or use schema evolution.

## High-Performance Connector

A high-performance Kafka connector entered preview in December 2025, targeting multi-GB/s throughput. Configuration is similar but optimized for high-volume workloads.

Check Snowflake documentation for current status and configuration details.

## Monitoring

### Connector-side

Monitor Kafka Connect metrics:

- `sink-record-send-rate` - Records per second
- `sink-record-lag-max` - Consumer lag
- `offset-commit-completion-rate` - Commit success rate

### Snowflake-side

```sql
-- Streaming file migration history
SELECT 
    table_name,
    start_time,
    end_time,
    credits_used,
    num_rows_migrated
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('hour', -24, current_timestamp())
ORDER BY start_time DESC;
```

## Troubleshooting

### "Channel not found" errors

- Verify Snowflake user has INSERT permissions on target tables
- Check that tables exist before connector starts
- Ensure database/schema names are correct (case-sensitive)

### High latency

- Reduce buffer settings
- Increase `tasks.max` for more parallelism
- Check Kafka consumer lag

### Data not appearing

- Verify connector is running: `GET /connectors/snowflake-streaming-sink/status`
- Check for errors in connector logs
- Confirm messages are being produced to topics

### Schema evolution failures

- Snowpipe Streaming doesn't auto-evolve schema by default
- Either pre-create tables with expected columns
- Or enable schematization and handle type conflicts

## Example: Complete Configuration

```properties
# Connector basics
name=production-streaming-sink
connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
tasks.max=8

# Snowflake connection
snowflake.url.name=myaccount.us-east-1.snowflakecomputing.com
snowflake.user.name=KAFKA_STREAMING_USER
snowflake.private.key.path=/secrets/snowflake/rsa_key.p8
snowflake.database.name=RAW
snowflake.schema.name=STREAMING

# Streaming ingestion
snowflake.ingestion.method=SNOWPIPE_STREAMING

# Topics
topics=clickstream,transactions,user_events
snowflake.topic2table.map=clickstream:CLICKSTREAM,transactions:TRANSACTIONS,user_events:USER_EVENTS

# Aggressive buffer for low latency
buffer.count.records=500
buffer.flush.time=2
buffer.size.bytes=1000000

# JSON handling
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false

# Error handling
errors.tolerance=all
errors.deadletterqueue.topic.name=snowflake-dlq
errors.deadletterqueue.topic.replication.factor=3
```
