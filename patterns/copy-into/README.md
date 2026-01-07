# COPY INTO

The bulk loading command that reads files from a stage and loads them into a table. Runs on a warehouse you control, transactional, predictable behavior.

## When to Use

COPY INTO fits scheduled batch loads - daily, hourly, every 15 minutes. It works well for:

- Historical backfills and one-time migrations
- Scheduled ingestion where latency requirements are minutes to hours
- Loads where you want to transform data during ingestion
- Situations where cost predictability matters more than real-time freshness

The 60-second minimum billing per warehouse resume means batching enough work to justify spinning up. Loading a single 5MB file on a dedicated warehouse wastes compute.

## Stages

COPY INTO reads from stages. Two types:

**External stages** point to cloud storage you manage (S3, GCS, Azure Blob). Your pipelines write files there, Snowflake reads from that location. More common in production.

**Internal stages** are storage within Snowflake. Upload files with the `PUT` command. Every user has a user stage, every table has a table stage, and you can create named stages for shared access. Useful for ad-hoc loads or when you don't want to manage external storage.

## Cost Model

You pay for warehouse time. The cost is predictable and controllable:

- Schedule loads during off-peak hours
- Right-size the warehouse for the data volume
- Monitor credit consumption directly via `query_history`

A Medium warehouse loading 100GB of well-sized Parquet files typically takes 8-12 minutes depending on file count and complexity.

## File Tracking

Files are tracked by default. Re-running COPY INTO on the same files does nothing unless you set `FORCE = TRUE`. This prevents duplicates but can be confusing during testing.

The load history is retained for 64 days. After that, Snowflake may reload files if you run COPY INTO again.

## Files in This Directory

- [basic-usage.sql](basic-usage.sql) - Standard COPY INTO patterns
- [transformations.sql](transformations.sql) - Transform data during load
- [error-handling.sql](error-handling.sql) - Validation and recovery patterns
- [schema-detection.sql](schema-detection.sql) - INFER_SCHEMA for semi-structured data
- [options-reference.md](options-reference.md) - Complete options documentation

## Related

- [File sizing guidance](../../operations/file-sizing/) - Optimal file sizes for COPY INTO
- [COPY history monitoring](../../operations/monitoring/copy-history-queries.sql) - Track load status
- [Snowflake docs: COPY INTO](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
