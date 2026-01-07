# Loading Patterns

Five native methods for getting data into Snowflake, each with different latency, cost and complexity characteristics.

## Comparison

| Method | Latency | Cost Model | Complexity | Best For |
|--------|---------|------------|------------|----------|
| [COPY INTO](copy-into/) | Minutes-hours | Warehouse time | Low | Batch loads, backfills |
| [Snowpipe](snowpipe/) | 1-2 minutes | Per-file serverless | Medium | Continuous file ingestion |
| [Streaming](streaming/) | Seconds | Per-row serverless | High | Real-time, Kafka |
| [Iceberg](iceberg/) | Varies | Warehouse time | Medium | Multi-engine, open format |
| [External Tables](external-tables/) | Query-time | Warehouse time | Low | Archive, query-in-place |

## Choosing a Pattern

Start with two questions:

**1. How fresh does the data need to be?**

- **Daily/hourly batches** → COPY INTO on a schedule
- **Within minutes of file arrival** → Snowpipe
- **Within seconds of event occurrence** → Snowpipe Streaming

**2. What constraints exist?**

- **Multiple query engines need access** → Iceberg tables
- **Data must stay in existing storage** → External tables
- **Predictable costs matter more than latency** → COPY INTO
- **Arrival patterns are unpredictable** → Snowpipe

## Pattern Index

### COPY INTO

Bulk loading on a warehouse you control. Transactional, predictable, handles transformations during load.

- [README](copy-into/README.md) - Overview and when to use
- [basic-usage.sql](copy-into/basic-usage.sql) - Standard patterns
- [transformations.sql](copy-into/transformations.sql) - Transform during load
- [error-handling.sql](copy-into/error-handling.sql) - Validation and recovery
- [schema-detection.sql](copy-into/schema-detection.sql) - INFER_SCHEMA patterns
- [options-reference.md](copy-into/options-reference.md) - All COPY INTO options

### Snowpipe

Event-driven serverless loading. Cloud storage events trigger automatic ingestion.

- [README](snowpipe/README.md) - Overview and architecture
- [setup-s3.sql](snowpipe/setup-s3.sql) - AWS S3 configuration
- [setup-gcs.sql](snowpipe/setup-gcs.sql) - Google Cloud Storage configuration
- [setup-azure.sql](snowpipe/setup-azure.sql) - Azure Blob configuration
- [monitoring.sql](snowpipe/monitoring.sql) - Pipe status queries
- [troubleshooting.md](snowpipe/troubleshooting.md) - Common issues and fixes

### Snowpipe Streaming

Sub-second row-level ingestion via SDK. No file staging required.

- [README](streaming/README.md) - Overview and architecture
- [kafka-connector.md](streaming/kafka-connector.md) - Kafka integration
- [python-sdk-example.py](streaming/python-sdk-example.py) - Python SDK usage
- [rest-api-example.py](streaming/rest-api-example.py) - REST API usage
- [monitoring.sql](streaming/monitoring.sql) - Streaming metrics

### Iceberg Tables

Open Parquet format with standardized metadata for multi-engine access.

- [README](iceberg/README.md) - Overview and use cases
- [load-modes.sql](iceberg/load-modes.sql) - FULL_INGEST vs ADD_FILES_COPY
- [external-catalog.sql](iceberg/external-catalog.sql) - External catalog integration
- [multi-engine-access.md](iceberg/multi-engine-access.md) - Cross-engine patterns

### External Tables

Query data in cloud storage without loading it into Snowflake.

- [README](external-tables/README.md) - Overview and trade-offs
- [setup.sql](external-tables/setup.sql) - Basic configuration
- [partitioning.sql](external-tables/partitioning.sql) - Partition pruning
- [materialized-views.sql](external-tables/materialized-views.sql) - Caching hot data
