# Snowflake Data Loading Guide

Production patterns, monitoring queries and decision frameworks for loading data into Snowflake.

> 📥 **[Download the Quick Reference Card](https://blakelassiter.gumroad.com/l/snowflake-data-loading-patterns)** - A 4-page PDF with method comparisons, syntax examples, anti-patterns and monitoring queries.

This repository accompanies the article [Loading Data into Snowflake: Choosing the Right Strategy](https://medium.com/@blakelassiter/loading-data-into-snowflake-choosing-the-right-strategy-af2c7803fff1) which provides narrative context for these patterns. For dbt transformation optimization after data lands, see [dbt-snowflake-optimization](https://github.com/blakelassiter/dbt-snowflake-optimization).

## Quick Start

**Choosing a loading method:**

| Method | Latency | Best For | Compute |
|--------|---------|----------|---------|
| [COPY INTO](patterns/copy-into/) | Minutes-hours | Batch loads, backfills, migrations | Warehouse |
| [Snowpipe](patterns/snowpipe/) | 1-2 minutes | Continuous file ingestion | Serverless |
| [Snowpipe Streaming](patterns/streaming/) | Seconds | Real-time, Kafka integration | Serverless |
| [Iceberg Tables](patterns/iceberg/) | Varies | Multi-engine access, open format | Warehouse |
| [External Tables](patterns/external-tables/) | Query-time | Archive data, query without loading | Warehouse |

**Two questions drive the decision:**

1. How fresh does the data need to be? (seconds, minutes, hours, daily)
2. What does the data flow look like? (bulk files, continuous stream, unpredictable bursts)

See the [Decision Framework](decision-framework/) for detailed guidance.

## Repository Structure

```
snowflake-data-loading-patterns/
├── patterns/                    # Method-specific reference
│   ├── copy-into/              # Bulk loading workhorse
│   ├── snowpipe/               # Event-driven file loading
│   ├── streaming/              # Sub-second row-level ingestion
│   ├── iceberg/                # Open format tables
│   └── external-tables/        # Query without loading
│
├── operations/                  # Cross-cutting concerns
│   ├── monitoring/             # Status queries, cost tracking, alerting
│   ├── file-sizing/            # Optimization for load performance
│   ├── error-handling/         # Validation, error recovery, rejected records
│   └── testing/                # Reconciliation, checksums, schema drift
│
├── anti-patterns/              # Common mistakes and how to avoid them
│   ├── tiny-files.md           # Per-file overhead costs
│   ├── warehouse-oversizing.md # Right-sizing for loads
│   ├── duplicate-loading.md    # Idempotency patterns
│   ├── missing-partitions.md   # External table performance
│   └── ignoring-errors.md      # Silent data loss
│
├── use-cases/                   # End-to-end recipes
│   ├── batch-migration.md      # One-time bulk loads
│   ├── incremental-loading.md  # Ongoing updates
│   └── cdc-pipelines.md        # Change data capture
│
├── infrastructure/              # Infrastructure-as-code
│   ├── terraform/              # Snowflake resources (stages, pipes, warehouses)
│   ├── airflow/                # Orchestration DAGs
│   ├── dagster/                # Asset-based pipelines with Sling
│   └── integrations/           # Fivetran/Airbyte webhooks
│
├── dbt-integration/            # Post-load transformation triggers
│   ├── post-load-triggers.md   # Trigger dbt after loads
│   ├── task-orchestration.sql  # Native Snowflake Tasks + Streams
│   └── freshness-checks.sql    # dbt source freshness patterns
│
├── decision-framework/          # Choosing the right approach
│   ├── latency-requirements.md
│   ├── cost-comparison.md
│   └── complexity-tradeoffs.md
│
└── LICENSE
```

## Patterns

Each pattern directory contains:

- **README.md** - When to use, architecture, trade-offs
- **SQL files** - Copy-paste ready examples
- **Supporting docs** - Troubleshooting, configuration options

### COPY INTO

The bulk loading command for scheduled batch loads. Runs on a warehouse you control with predictable costs.

```sql
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
PATTERN = '.*[.]parquet'
ON_ERROR = 'CONTINUE';
```

[Full COPY INTO patterns →](patterns/copy-into/)

### Snowpipe

Serverless, event-driven loading. Files trigger automatic ingestion within 1-2 minutes of arrival.

```sql
CREATE PIPE raw.events_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'JSON');
```

[Full Snowpipe patterns →](patterns/snowpipe/)

### Snowpipe Streaming

Row-level ingestion with sub-second latency. Requires SDK integration but eliminates file staging entirely.

[Full Streaming patterns →](patterns/streaming/)

### Iceberg Tables

Open Parquet format with standardized metadata. Multiple engines (Snowflake, Spark, Trino) can read the same underlying files.

[Full Iceberg patterns →](patterns/iceberg/)

### External Tables

Query data directly in cloud storage without loading. The data stays in S3/GCS/Azure - Snowflake reads on demand.

[Full External Table patterns →](patterns/external-tables/)

## Operations

### Monitoring

Track load status, costs and failures across all loading methods.

```sql
-- Recent COPY INTO history
SELECT table_name, file_count, row_count, status
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('hour', -24, current_timestamp())
));
```

[Full monitoring queries →](operations/monitoring/)

### File Sizing

Snowflake recommends 100-250MB compressed files. Smaller files add per-file overhead; larger files reduce parallelism.

```sql
-- Check file size distribution
SELECT 
    CASE 
        WHEN "size" < 10000000 THEN 'Under 10MB'
        WHEN "size" BETWEEN 10000000 AND 100000000 THEN '10-100MB'
        WHEN "size" BETWEEN 100000000 AND 250000000 THEN '100-250MB (optimal)'
        WHEN "size" BETWEEN 250000000 AND 500000000 THEN '250-500MB'
        ELSE 'Over 500MB'
    END as size_bucket,
    COUNT(*) as file_count
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
GROUP BY size_bucket;
```

[Full file sizing guidance →](operations/file-sizing/)

## Anti-Patterns

Common mistakes that hurt performance and cost:

| Anti-Pattern | Impact | Solution |
|--------------|--------|----------|
| [Tiny Files](anti-patterns/tiny-files.md) | 5-10x cost increase | Batch upstream, consolidate |
| [Warehouse Oversizing](anti-patterns/warehouse-oversizing.md) | 4x cost for same work | Right-size by workload |
| [Duplicate Loading](anti-patterns/duplicate-loading.md) | 2x compute + storage | Use MERGE, track loads |
| [Missing Partitions](anti-patterns/missing-partitions.md) | 100x scan cost | Add partition expressions |
| [Ignoring Errors](anti-patterns/ignoring-errors.md) | Silent data loss | Monitor skip rates |

[Full anti-patterns guide →](anti-patterns/)

## Use Cases

End-to-end recipes for common scenarios:

- [Batch Migration](use-cases/batch-migration.md) - One-time or periodic bulk loads
- [Incremental Loading](use-cases/incremental-loading.md) - Ongoing updates with deduplication
- [CDC Pipelines](use-cases/cdc-pipelines.md) - Change data capture from operational databases

[Full use cases →](use-cases/)

## Infrastructure

Infrastructure-as-code for deploying loading pipelines:

- [Terraform](infrastructure/terraform/) - Storage integrations, stages, pipes, warehouses
- [Airflow](infrastructure/airflow/) - Scheduled batch loading DAGs
- [Dagster](infrastructure/dagster/) - Asset-based pipelines with Sling
- [Integrations](infrastructure/integrations/) - Fivetran/Airbyte webhook handlers

[Full infrastructure examples →](infrastructure/)

## dbt Integration

Triggering transformations after data loads:

```python
# Trigger dbt when Fivetran sync completes
def handle_sync_end(event):
    if event["data"]["status"] == "SUCCESSFUL":
        trigger_dbt_job(cause=f"Fivetran: {event['connector_name']}")
```

[Full dbt integration patterns →](dbt-integration/)

## Decision Framework

Not sure which method fits your use case? The [decision framework](decision-framework/) walks through:

- [Latency requirements](decision-framework/latency-requirements.md) - Matching freshness needs to method capabilities
- [Cost comparison](decision-framework/cost-comparison.md) - Warehouse vs serverless economics
- [Complexity trade-offs](decision-framework/complexity-tradeoffs.md) - Operational overhead of each approach

## Related Resources

- **Quick Reference Card:** [Download from Gumroad](https://blakelassiter.gumroad.com/l/snowflake-data-loading-patterns) - 4-page PDF with syntax, anti-patterns, monitoring queries
- **Article:** [Loading Data into Snowflake: Choosing the Right Strategy](https://medium.com/@blakelassiter/loading-data-into-snowflake-choosing-the-right-strategy-af2c7803fff1)
- **Transformation patterns:** [dbt-snowflake-optimization](https://github.com/blakelassiter/dbt-snowflake-optimization)
- **Snowflake docs:** [Data Loading Overview](https://docs.snowflake.com/en/user-guide/data-load-overview)

## License

MIT License - see [LICENSE](LICENSE) for details.

## About

Maintained by [Blake Lassiter](https://www.linkedin.com/in/blakelassiter/) - Principal Data Architect & Engineer focused on modern data stack optimization and production AI systems.
