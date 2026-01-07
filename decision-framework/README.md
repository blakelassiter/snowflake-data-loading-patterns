# Decision Framework

Choosing the right data loading method depends on latency requirements, cost constraints and operational complexity tolerance.

## The Two Key Questions

**1. How fresh does the data need to be?**

| Freshness Need | Method |
|----------------|--------|
| Seconds | Snowpipe Streaming |
| Minutes | Snowpipe |
| Hours or scheduled | COPY INTO |

**2. What constraints exist?**

| Constraint | Method |
|------------|--------|
| Multi-engine access required | Iceberg tables |
| Data must stay in external storage | External tables |
| Cost predictability is critical | COPY INTO |
| Unpredictable file arrival | Snowpipe |

## Decision Tree

```
Is real-time (seconds) latency required?
├── Yes → Snowpipe Streaming
└── No → Do multiple query engines need access?
    ├── Yes → Iceberg Tables (with any loading method)
    └── No → Should data be loaded into Snowflake?
        ├── No (query in place) → External Tables
        └── Yes → Are file arrivals unpredictable?
            ├── Yes → Snowpipe
            └── No → COPY INTO on schedule
```

## Detailed Guides

- [Latency Requirements](latency-requirements.md) - Matching freshness needs to capabilities
- [Cost Comparison](cost-comparison.md) - Warehouse vs serverless economics
- [Complexity Trade-offs](complexity-tradeoffs.md) - Operational overhead of each approach

## Quick Reference

| Method | Latency | Cost Model | Setup Complexity | Best For |
|--------|---------|------------|------------------|----------|
| COPY INTO | Minutes-hours | Warehouse | Low | Scheduled batch |
| Snowpipe | 1-2 min | Serverless | Medium | Continuous files |
| Streaming | Seconds | Serverless | High | Real-time |
| Iceberg | Varies | Warehouse | Medium | Multi-engine |
| External | Query-time | Warehouse | Low | Query-in-place |

## Common Scenarios

### "We get daily files from a vendor"

Use **COPY INTO** on a schedule. Simple, predictable costs.

### "Events land in S3 throughout the day"

Use **Snowpipe** with auto-ingest. Files load within minutes of arrival.

### "We have Kafka and need real-time dashboards"

Use **Snowpipe Streaming** via Kafka connector. Sub-second latency.

### "Our data science team uses Spark and SQL analysts use Snowflake"

Use **Iceberg tables**. Both engines read the same data.

### "We have 5 years of logs but only query recent months"

Use **External tables** for the archive, load recent data into native tables.

## Anti-Patterns

**Streaming when batching works** - Sub-second latency has real complexity costs. If hourly freshness is acceptable, batch it.

**COPY INTO when arrivals are unpredictable** - A warehouse sitting idle waiting for files wastes money. Snowpipe's serverless model handles bursts better.

**External tables for frequently-queried data** - Query performance is significantly worse than native tables. Load hot data.

**Iceberg when Snowflake is the only engine** - Standard tables are simpler to operate when you don't need cross-engine compatibility.
