# Latency Requirements

Match your data freshness needs to the right loading method.

## Latency Spectrum

| Requirement | Typical Latency | Method | Notes |
|-------------|-----------------|--------|-------|
| Real-time | 5-10 seconds | Snowpipe Streaming | Requires SDK or Kafka |
| Near-real-time | 1-2 minutes | Snowpipe | File-based, event-driven |
| Frequent batch | 5-15 minutes | COPY INTO (scheduled) | Warehouse-based |
| Hourly | 1 hour | COPY INTO | Standard batch |
| Daily | 24 hours | COPY INTO | Simple scheduling |

## Understanding the Gaps

### Snowpipe Streaming (Seconds)

End-to-end latency of 5-10 seconds from event occurrence to query availability. This assumes:

- Application writes to SDK/Kafka immediately
- Network latency to Snowflake is reasonable
- No batching delays in producer

The High-Performance Kafka Connector (preview as of Dec 2025) achieves 10 GB/s throughput while maintaining sub-second latency.

**When seconds matter:**
- Fraud detection during transactions
- Real-time operational dashboards
- Live event scoring/personalization

### Snowpipe (Minutes)

1-2 minute latency from file landing to data availability. The breakdown:

- File lands in cloud storage
- Event notification triggers (~seconds)
- Snowpipe queues and processes (~60-90 seconds)
- Data available for query

**When minutes are fine:**
- Operational reporting
- Near-real-time analytics
- IoT sensor aggregation
- Log analysis

### COPY INTO (Flexible)

Latency depends on your schedule:

- Run every 5 minutes → 5-10 minute freshness
- Run every hour → up to 60 minute freshness
- Run daily → up to 24 hour freshness

The trade-off is warehouse utilization. Frequent runs mean more warehouse time but fresher data.

**When batch works:**
- Financial reporting (daily/hourly)
- Marketing analytics
- Data warehouse loads
- Historical backfills

## Questioning Latency Requirements

Before committing to real-time complexity, challenge the requirement:

**"We need real-time data"**
- What decisions depend on second-level freshness?
- What's the actual cost of 5-minute-old data?
- Who consumes this data and how often do they refresh?

Many teams discover that "real-time" actually means "faster than our current daily batch" - and 15-minute Snowpipe loads satisfy the need with far less complexity.

## Latency vs Cost Trade-off

Lower latency generally costs more:

| Method | Relative Cost | Why |
|--------|---------------|-----|
| COPY INTO (daily) | $ | One warehouse run per day |
| COPY INTO (hourly) | $$ | 24 warehouse runs per day |
| Snowpipe | $$$ | Serverless premium, per-file overhead |
| Streaming | $$$$ | Continuous compute, SDK complexity |

The cost difference isn't always dramatic, but it compounds with volume.

## Latency Consistency

Consider not just average latency but consistency:

**Snowpipe Streaming:** Very consistent - seconds regardless of volume (within capacity).

**Snowpipe:** Generally 1-2 minutes, but can spike during high-volume bursts as queue depth increases.

**COPY INTO:** Depends on schedule and file availability. If files arrive late, data is stale until next run.

For SLA-bound use cases, streaming's consistency may justify the complexity even when average latency requirements are relaxed.

## Hybrid Approaches

Not all data needs the same latency:

**Hot/cold split:**
- Critical events → Streaming (seconds)
- Regular events → Snowpipe (minutes)
- Bulk historical → COPY INTO (daily)

**Different tables, same source:**
- Real-time aggregates for dashboards → Streaming
- Detailed records for analysis → Batch

This adds complexity but optimizes cost for mixed requirements.

## Measurement

Once deployed, measure actual latency:

```sql
-- If you capture event_timestamp and load_timestamp
SELECT 
    DATE_TRUNC('hour', loaded_at) as hour,
    AVG(DATEDIFF('second', event_timestamp, loaded_at)) as avg_latency_sec,
    MAX(DATEDIFF('second', event_timestamp, loaded_at)) as max_latency_sec,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY 
        DATEDIFF('second', event_timestamp, loaded_at)
    ) as p95_latency_sec
FROM your_table
WHERE loaded_at > dateadd('day', -1, current_timestamp())
GROUP BY hour
ORDER BY hour DESC;
```

Compare measured latency against requirements to validate your architecture choice.
