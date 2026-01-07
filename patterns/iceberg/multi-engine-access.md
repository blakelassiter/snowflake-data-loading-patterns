# Multi-Engine Access with Iceberg Tables

Iceberg's primary value is interoperability. Data written by one engine can be read by others without export/import cycles.

## Architecture Patterns

### Snowflake as Primary Writer

Snowflake manages the Iceberg table. Other engines read from the same storage.

```
Snowflake (write) → S3/GCS/Azure (Parquet + metadata) → Spark, Trino, Presto (read)
```

Setup in Snowflake:

```sql
CREATE ICEBERG TABLE analytics.shared_metrics (
    metric_id STRING,
    metric_value DOUBLE,
    recorded_at TIMESTAMP_NTZ
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'shared_volume'
    BASE_LOCATION = 'metrics/';
```

External engines access via:
- Storage path: `s3://bucket/metrics/data/`
- Metadata path: `s3://bucket/metrics/metadata/`

### Spark as Primary Writer

Spark manages the Iceberg table (often via AWS Glue or Polaris). Snowflake reads via catalog integration.

```
Spark (write) → S3 + Glue Catalog → Snowflake (read)
```

Spark writes:

```python
df.writeTo("glue_catalog.analytics.events") \
    .using("iceberg") \
    .createOrReplace()
```

Snowflake reads:

```sql
CREATE ICEBERG TABLE lake.events
    EXTERNAL_VOLUME = 'data_volume'
    CATALOG = 'glue_catalog'
    CATALOG_TABLE_NAME = 'events';

SELECT * FROM lake.events WHERE event_date = CURRENT_DATE();
```

### Hybrid: Write Segmentation

Different engines write different data, unified via Iceberg.

```
Snowflake → analytics tables (aggregations, reports)
Spark → raw event processing (high volume ETL)
Both → same storage layer, queryable by either
```

This works when:
- Workloads have clear ownership
- Each engine writes to distinct tables
- Cross-engine reads are the common case

## Spark Integration

### Reading Snowflake-Managed Iceberg

PySpark code to read from Snowflake-managed Iceberg table:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("iceberg-reader") \
    .config("spark.sql.catalog.snowflake_iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.snowflake_iceberg.type", "hadoop") \
    .config("spark.sql.catalog.snowflake_iceberg.warehouse", "s3://bucket/iceberg/") \
    .getOrCreate()

# Read the table
df = spark.read.format("iceberg").load("s3://bucket/metrics/")
df.show()
```

### Writing to Glue-Cataloged Iceberg

```python
spark = SparkSession.builder \
    .appName("iceberg-writer") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://bucket/iceberg/") \
    .getOrCreate()

# Write to Iceberg table
df.writeTo("glue_catalog.analytics.events").append()
```

## Trino/Presto Integration

### Catalog Configuration

trino/etc/catalog/iceberg.properties:

```properties
connector.name=iceberg
iceberg.catalog.type=glue
hive.metastore.glue.region=us-east-1
iceberg.file-format=PARQUET
```

### Querying Iceberg Tables

```sql
-- Trino SQL
SELECT * FROM iceberg.analytics.events
WHERE event_date = CURRENT_DATE;

-- Time travel
SELECT * FROM iceberg.analytics.events FOR VERSION AS OF 123456789;
```

## Considerations

### Schema Evolution

Iceberg supports schema evolution (add columns, rename, etc.). Changes made in one engine are visible to others after metadata refresh.

Snowflake limitations:
- External catalog tables require manual `REFRESH` after schema changes
- Snowflake-managed tables handle evolution automatically

### Data Types

Not all type mappings are perfect across engines. Test edge cases:

| Iceberg Type | Snowflake | Spark | Trino |
|--------------|-----------|-------|-------|
| `timestamp` | `TIMESTAMP_NTZ` | `TimestampType` | `timestamp(6)` |
| `decimal(38,9)` | `NUMBER(38,9)` | `DecimalType(38,9)` | `decimal(38,9)` |
| `binary` | `BINARY` | `BinaryType` | `varbinary` |

### Partitioning

Iceberg partitioning is defined at table creation. All engines see the same partitions.

```sql
-- Snowflake: create partitioned Iceberg table
CREATE ICEBERG TABLE events (
    event_id STRING,
    event_type STRING,
    event_date DATE,
    payload VARIANT
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'vol'
    BASE_LOCATION = 'events/'
    -- Partition by event_date
    CLUSTER BY (event_date);
```

Spark and Trino will use the same partition structure for efficient queries.

### Performance Considerations

Reading across engines adds latency compared to native tables:
- Metadata lookups to external storage
- No Snowflake-specific optimizations (clustering, search optimization)
- Network round trips for S3/GCS/Azure access

Iceberg is for interoperability, not maximum single-engine performance.

## When Multi-Engine Makes Sense

**Good fit:**
- Data lake architecture with multiple consumers
- Spark for heavy ETL, Snowflake for analytics/BI
- Avoiding data duplication across platforms
- Gradual migration from Spark to Snowflake (or vice versa)

**Probably overkill:**
- Single-engine architecture
- All workloads fit in Snowflake
- Interoperability isn't a current or planned requirement

## Troubleshooting

### Stale data in Snowflake

After external writes, refresh metadata:

```sql
ALTER ICEBERG TABLE lake.events REFRESH;
```

### Schema mismatch

Verify types align:

```sql
-- Snowflake
DESC TABLE lake.events;
```

```python
# Spark
spark.table("glue_catalog.analytics.events").printSchema()
```

### Permission errors

Each engine needs appropriate IAM/credentials for:
- Storage access (S3/GCS/Azure read/write)
- Catalog access (Glue, Polaris, etc.)
- Cross-account access if resources are in different AWS accounts
