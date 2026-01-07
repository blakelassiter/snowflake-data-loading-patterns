# Snowpipe

Serverless, event-driven loading. Instead of scheduling COPY INTO commands, configure a pipe that watches a stage location and automatically loads new files as they arrive.

## When to Use

Snowpipe makes sense when:

- Data arrives continuously throughout the day
- You want data available within 1-2 minutes of file landing
- Arrival patterns are unpredictable (can't easily schedule COPY INTO)
- You prefer serverless compute over managing warehouse scheduling

The typical latency is 1-2 minutes from file arrival to data availability. This assumes properly configured event notifications - without auto-ingest, you'd need to call the REST API manually.

## Architecture

1. Files land in cloud storage (S3, Azure Blob, GCS)
2. Cloud event notification triggers Snowpipe
3. Snowpipe queues the file for processing
4. Snowflake-managed compute loads the data
5. Data available for query

The compute is serverless - you don't manage warehouses for Snowpipe loads. Billing is per-file based on compute time required.

## Cost Model

Snowpipe billing has two components:

**Compute:** Charged per-second of serverless compute used to load files. Roughly 1.25x the cost of equivalent warehouse compute.

**File notification overhead:** Small charge per file notification received.

The serverless premium is offset when:
- You'd otherwise need a warehouse running idle waiting for files
- Arrival patterns are unpredictable
- You want to avoid warehouse management overhead

For predictable, scheduled loads with known arrival times, COPY INTO on a warehouse is often cheaper.

## File Tracking

Like COPY INTO, Snowpipe tracks loaded files. Re-delivering the same file does nothing by default. The load history is retained for 14 days for Snowpipe (vs 64 days for COPY INTO).

## Files in This Directory

- [setup-s3.sql](setup-s3.sql) - AWS S3 event notification setup
- [setup-gcs.sql](setup-gcs.sql) - Google Cloud Storage setup
- [setup-azure.sql](setup-azure.sql) - Azure Blob Storage setup
- [monitoring.sql](monitoring.sql) - Pipe status and history queries
- [troubleshooting.md](troubleshooting.md) - Common issues and fixes

## Related

- [Pipe status monitoring](../../operations/monitoring/pipe-status-queries.sql)
- [Cost tracking](../../operations/monitoring/cost-tracking.sql)
- [Snowflake docs: Snowpipe](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro)
