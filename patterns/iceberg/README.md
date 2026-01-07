# Iceberg Tables

[Iceberg tables](https://docs.snowflake.com/en/user-guide/tables-iceberg) store data in open Parquet format with standardized metadata. Multiple query engines (Snowflake, Spark, Trino, Presto) can read the same underlying files.

## When to Use

Iceberg makes sense when:

- Multiple query engines need to access the same data
- An open-format data lake strategy is a priority
- You want to avoid vendor lock-in on the data layer
- Interoperability with Spark, Trino or other engines is required

If Snowflake is your only query engine, standard tables are simpler to operate and maintain. Don't add Iceberg complexity without a clear need for cross-engine compatibility.

## Architecture

Snowflake supports two Iceberg configurations:

**Snowflake-managed Iceberg tables:** Snowflake manages both data and metadata. Simplest setup, full Snowflake DML support. Other engines can read the data.

**Externally-managed Iceberg tables:** An external catalog (AWS Glue, Polaris, etc.) manages metadata. Snowflake reads from the catalog. Useful when another engine is the primary writer.

## Loading Methods

All three native loading methods work with Snowflake-managed Iceberg tables:

- **COPY INTO** with `LOAD_MODE` options
- **Snowpipe** for continuous file-based ingestion
- **Snowpipe Streaming** (requires Ingest SDK 3.0.0+)

The `LOAD_MODE` parameter controls how data lands:

| Mode | Behavior | Best For |
|------|----------|----------|
| `FULL_INGEST` | Read source, write Iceberg Parquet | Any format (CSV, JSON, Avro, Parquet) |
| `ADD_FILES_COPY` | Copy Parquet files directly | Pre-formatted Iceberg-compatible Parquet |

`ADD_FILES_COPY` is faster when you already have properly formatted Parquet because Snowflake skips scanning and rewriting.

## Cost Model

Iceberg tables use warehouse compute for loading - same as standard tables. Storage costs are for the underlying Parquet files in your cloud storage.

One consideration: Iceberg's metadata overhead can increase storage slightly compared to Snowflake's native format. For most workloads this is negligible.

## Files in This Directory

- [load-modes.sql](load-modes.sql) - FULL_INGEST vs ADD_FILES_COPY examples
- [external-catalog.sql](external-catalog.sql) - External catalog integration
- [multi-engine-access.md](multi-engine-access.md) - Cross-engine patterns

## Related

- [Snowflake docs: Iceberg Tables](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- [Decision framework](../../decision-framework/) - Choosing between Iceberg and standard tables
