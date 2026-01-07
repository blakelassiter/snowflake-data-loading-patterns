# Snowpipe Streaming

Row-level ingestion with sub-second latency. Data flows directly into Snowflake without staging files first. Requires SDK integration but eliminates the file-based bottleneck entirely.

## Two Architectures (as of 2025)

**High-Performance Architecture (Recommended for new projects)**
- GA September 2025, available on AWS, Azure, and GCP
- Up to 10 GB/s throughput per table
- 5-10 second end-to-end latency
- Uses new `snowpipe-streaming` SDK (Python 3.9+, Java 11+)
- PIPE object for data flow management
- Server-side schema validation
- Throughput-based pricing (credits per uncompressed GB)

**Classic Architecture**
- Original streaming implementation
- Uses `snowflake-ingest-sdk` (Java 8+)
- Channels point directly to tables
- Client-side schema validation
- Pricing based on compute + active connections

For new streaming projects, start with the high-performance architecture.

## When to Use

Snowpipe Streaming makes sense when:

- Sub-second latency is a genuine business requirement
- You have streaming infrastructure (Kafka, Kinesis, custom producers)
- The operational complexity is justified by the latency gains
- File-based approaches can't meet freshness requirements

## When to Skip

Skip Snowpipe Streaming if:

- Minute-level latency is acceptable (use regular Snowpipe)
- Batch loads work fine for your use case (use COPY INTO)
- You don't have streaming infrastructure already
- The learning curve and operational overhead aren't justified

Many teams adopt streaming before they need it. The complexity cost is real.

## Architecture

Snowpipe Streaming uses channels to manage data flow:

1. Application opens a channel to a target table (via PIPE object in high-perf)
2. Rows are written to the channel via SDK
3. Snowflake buffers and commits in micro-batches (~1 second default)
4. Data becomes queryable within seconds

No files are staged. The SDK handles buffering, batching and error handling.

## Integration Options

**Kafka Connector:** Most common path. The [Snowflake Connector for Kafka](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-kafka) uses Streaming under the hood when configured with `snowflake.ingestion.method = SNOWPIPE_STREAMING`.

**Ingest SDK:** Direct integration for custom producers. Available in Java and Python. Use when Kafka isn't in the picture or you need fine-grained control.

**REST API:** HTTP-based ingestion for simpler integrations and edge/IoT deployments. Available with high-performance architecture.

## Cost Model

As of June 2025, Snowpipe pricing was simplified to a per-GB ingested model across both Snowpipe (file-based) and Snowpipe Streaming, offering approximately 50% better economics than the previous model.

The high-performance architecture uses transparent, throughput-based pricing (credits per uncompressed GB). Compare against:
- Snowpipe file-based costs for same volume
- Warehouse costs for equivalent COPY INTO frequency
- Operational costs of managing streaming infrastructure

## Files in This Directory

- [kafka-connector.md](kafka-connector.md) - Kafka integration setup
- [python-sdk-example.py](python-sdk-example.py) - Python SDK usage (high-performance)
- [rest-api-example.py](rest-api-example.py) - REST API integration
- [monitoring.sql](monitoring.sql) - Streaming metrics queries

## Related

- [Streaming metrics](../../operations/monitoring/streaming-metrics.sql)
- [Cost comparison](../../decision-framework/cost-comparison.md)
- [Snowflake docs: Snowpipe Streaming High-Performance](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview)
- [Snowflake docs: Snowpipe Streaming Classic](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-classic-overview)
