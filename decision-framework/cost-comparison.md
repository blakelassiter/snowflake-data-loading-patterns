# Cost Comparison

Understanding the economics of each loading method.

## Cost Models Overview

| Method | Billing Unit | What You Pay For |
|--------|--------------|------------------|
| COPY INTO | Warehouse credits | Time warehouse runs |
| Snowpipe | Serverless credits | Per-file compute + overhead |
| Streaming | Serverless credits | Continuous compute |
| Iceberg | Warehouse credits | Same as COPY INTO |
| External Tables | Warehouse credits | Query-time only |

## COPY INTO Costs

**Model:** You pay for warehouse time during the load.

**Factors:**
- Warehouse size (credits/hour)
- Load duration
- Data volume and file count
- Transformation complexity

**Example calculation:**

```
Warehouse: Medium (4 credits/hour)
Load time: 15 minutes
Daily loads: 4

Daily cost: 4 credits/hour × 0.25 hours × 4 loads = 4 credits/day
Monthly cost: ~120 credits
```

**Optimization levers:**
- Right-size warehouse (don't use XL for small loads)
- Batch more data per run (amortize startup overhead)
- Optimize file sizes (100-250MB)
- Simplify transformations

**Hidden costs:**
- Warehouse idle time if waiting for files
- Failed loads still consume credits
- Resume/suspend overhead (60-second minimum billing)

## Snowpipe Costs

**Model:** As of June 2025, Snowpipe moved to a simplified per-GB ingested pricing model (approximately 50% better economics than the previous per-file model).

**New pricing (2025+):**
- Credits charged per GB ingested
- Consistent across Snowpipe (file-based) and Snowpipe Streaming
- More predictable cost estimation

**Legacy pricing factors (pre-2025):**
- Per-file processing time
- Queue management overhead
- ~1.25x premium over equivalent warehouse compute

**When Snowpipe saves money:**
- Unpredictable file arrivals (no idle warehouse)
- Low volume with many small files
- 24/7 trickle of data

**When COPY INTO is cheaper:**
- Predictable schedules
- Large batches at known times
- High volume concentrated in windows

**Query for comparison:**

```sql
-- Snowpipe credits last 30 days
SELECT 
    pipe_name,
    SUM(credits_used) as total_credits,
    SUM(files_inserted) as total_files,
    SUM(bytes_inserted) / (1024*1024*1024) as total_gb,
    ROUND(SUM(credits_used) / NULLIF(SUM(bytes_inserted), 0) * (1024*1024*1024), 4) as credits_per_gb
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY pipe_name
ORDER BY total_credits DESC;
```

## Streaming Costs

**Model (2025+):** Simplified per-GB ingested pricing, same as Snowpipe.

The high-performance architecture (GA September 2025) provides transparent, throughput-based pricing (credits per uncompressed GB), making cost estimation straightforward.

**Factors:**
- Data volume (GB ingested)
- Data type doesn't significantly impact pricing

**When streaming cost is justified:**
- Sub-second latency is a hard requirement
- Value of fresh data exceeds cost premium
- Alternative (frequent Snowpipe) would cost more

**Monitoring query:**

```sql
-- Streaming credits last 30 days
SELECT 
    DATE_TRUNC('day', start_time) as day,
    SUM(credits_used) as credits,
    SUM(bytes_inserted) / (1024*1024*1024) as gb_ingested,
    ROUND(SUM(credits_used) / NULLIF(SUM(bytes_inserted), 0) * (1024*1024*1024), 4) as credits_per_gb
FROM snowflake.account_usage.snowpipe_streaming_file_migration_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY day
ORDER BY day DESC;
```

## External Tables

**Model:** No load cost - pay only at query time.

**Storage:** You pay your cloud provider (S3/GCS/Azure), not Snowflake.

**Query cost:** Warehouse time during queries. Typically higher than native table queries because:
- Remote reads from cloud storage
- No Snowflake clustering/optimization
- Network latency

**When cost-effective:**
- Archive data rarely queried
- Exploration before loading
- Regulatory requirement for external storage

**When expensive:**
- Frequently queried data (load it instead)
- Complex aggregations (native tables are faster)

## Comparing Methods

**Scenario: 100GB daily load**

| Method | Assumptions | Est. Daily Credits |
|--------|-------------|-------------------|
| COPY INTO | Medium WH, 20 min | ~1.3 credits |
| Snowpipe | 1000 files, 100MB each | ~1.5-2 credits |
| Streaming | Continuous at 1GB/hour | ~3-4 credits |

These are rough estimates. Actual costs vary based on data complexity, file characteristics and Snowflake pricing.

**Scenario: Unpredictable small files**

| Method | Assumptions | Outcome |
|--------|-------------|---------|
| COPY INTO (always-on) | Small WH idle 80% | High idle waste |
| COPY INTO (scheduled) | Hourly runs | Stale data between runs |
| Snowpipe | Event-driven | Pay only for actual files |

Snowpipe wins when arrival patterns are unpredictable.

## Total Cost of Ownership

Beyond Snowflake credits, consider:

**Snowpipe/Streaming:**
- Cloud event notification costs (SQS, Pub/Sub, Event Grid)
- Kafka infrastructure (if using connector)
- Monitoring and alerting overhead

**COPY INTO:**
- Orchestration tool costs (Airflow, Dagster, etc.)
- Retry/failure handling complexity
- Schedule management

**External tables:**
- Cloud storage egress (data transfer out)
- Higher query costs for equivalent analysis

## Cost Tracking Queries

```sql
-- All loading costs by method
SELECT 
    service_type,
    DATE_TRUNC('month', start_time) as month,
    SUM(credits_used) as credits
FROM snowflake.account_usage.metering_history
WHERE service_type IN ('WAREHOUSE_METERING', 'PIPE', 'SNOWPIPE_STREAMING')
  AND start_time > dateadd('month', -6, current_date())
GROUP BY service_type, month
ORDER BY month DESC, credits DESC;
```

See [operations/monitoring/cost-tracking.sql](../operations/monitoring/cost-tracking.sql) for more detailed queries.
