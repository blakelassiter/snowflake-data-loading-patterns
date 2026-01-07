# Anti-Pattern: Warehouse Oversizing

Using a larger warehouse than the workload requires.

## The Problem

Bigger warehouses cost more per second but don't always load data faster. A load job on an XL warehouse costs 4x what it costs on a Medium, but loading 10GB of data might take the same wall-clock time on both.

Warehouse size matters for parallelism - more nodes can process more files simultaneously. But if you're loading 10 files, you don't need 128 nodes.

## Cost Impact

**Snowflake warehouse credits per hour:**

| Size | Credits/Hour |
|------|--------------|
| X-Small | 1 |
| Small | 2 |
| Medium | 4 |
| Large | 8 |
| X-Large | 16 |
| 2X-Large | 32 |

**Scenario: Loading 10GB from 100 files**

| Warehouse | Time | Credits Used | Cost Ratio |
|-----------|------|--------------|------------|
| Medium | 12 min | 0.8 | 1x |
| Large | 10 min | 1.3 | 1.6x |
| X-Large | 9 min | 2.4 | 3x |

The XL is faster but not 4x faster - you're paying for idle capacity.

## Detection

```sql
-- Analyze warehouse utilization during loads
SELECT 
    warehouse_name,
    warehouse_size,
    query_type,
    COUNT(*) as query_count,
    AVG(execution_time) / 1000 as avg_seconds,
    AVG(total_elapsed_time) / 1000 as avg_elapsed_seconds,
    AVG(bytes_scanned) / 1e9 as avg_gb_scanned,
    SUM(credits_used_cloud_services) as total_credits
FROM snowflake.account_usage.query_history
WHERE query_type = 'COPY'
  AND start_time > dateadd('day', -7, current_date())
GROUP BY warehouse_name, warehouse_size, query_type
ORDER BY warehouse_name;

-- Look for short-running queries on large warehouses
SELECT 
    query_id,
    warehouse_name,
    warehouse_size,
    execution_time / 1000 as seconds,
    bytes_scanned / 1e9 as gb_scanned
FROM snowflake.account_usage.query_history
WHERE query_type = 'COPY'
  AND warehouse_size IN ('X-Large', '2X-Large', '3X-Large', '4X-Large')
  AND execution_time < 60000  -- Under 1 minute
  AND start_time > dateadd('day', -7, current_date())
ORDER BY execution_time;
```

**Red flags:**
- COPY jobs completing in under 60 seconds on Large+ warehouses
- Average bytes scanned < 10GB on XL warehouses
- Small file counts (< 100) on large warehouses

## Root Causes

1. **One warehouse for everything** - Using the analytics XL for loading
2. **Defaulting to large** - "Bigger is better" mentality
3. **Not testing alternatives** - Never measured actual impact
4. **Fear of slowness** - Over-provisioning "just in case"

## Remediation

### Right-size for the workload

**General guidance:**

| Data Volume | File Count | Suggested Size |
|-------------|------------|----------------|
| < 1GB | < 50 | X-Small or Small |
| 1-10GB | 50-200 | Small or Medium |
| 10-100GB | 200-1000 | Medium or Large |
| 100GB+ | 1000+ | Large or XL |

### Create dedicated load warehouses

```sql
-- Small warehouse for small/frequent loads
CREATE WAREHOUSE load_wh_small
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Medium for larger batches
CREATE WAREHOUSE load_wh_medium
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Use appropriately
USE WAREHOUSE load_wh_small;
COPY INTO raw.small_table FROM @stage/small/;

USE WAREHOUSE load_wh_medium;
COPY INTO raw.large_table FROM @stage/large/;
```

### Test and measure

```sql
-- Test same load on different warehouse sizes
-- Run on Small
ALTER SESSION SET WAREHOUSE = load_wh_small;
COPY INTO raw.test_table FROM @stage/test/ FORCE = TRUE;

-- Record: Query ID, time, credits from query_history

-- Run on Medium
ALTER SESSION SET WAREHOUSE = load_wh_medium;
COPY INTO raw.test_table FROM @stage/test/ FORCE = TRUE;

-- Compare results
SELECT 
    warehouse_size,
    execution_time / 1000 as seconds,
    credits_used_cloud_services as credits
FROM snowflake.account_usage.query_history
WHERE query_id IN ('query_id_1', 'query_id_2');
```

### Use multi-cluster for variable loads

If load volumes vary significantly:

```sql
CREATE WAREHOUSE load_wh_elastic
    WAREHOUSE_SIZE = 'MEDIUM'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 4
    SCALING_POLICY = 'STANDARD'
    AUTO_SUSPEND = 60;
```

This starts small and scales up only when needed.

## Monitoring

```sql
-- Weekly warehouse efficiency report
SELECT 
    DATE_TRUNC('week', start_time) as week,
    warehouse_name,
    warehouse_size,
    COUNT(*) as copy_jobs,
    AVG(execution_time) / 1000 as avg_seconds,
    SUM(credits_used_cloud_services) as total_credits,
    SUM(bytes_scanned) / 1e9 as total_gb,
    ROUND(SUM(bytes_scanned) / 1e9 / NULLIF(SUM(credits_used_cloud_services), 0), 2) as gb_per_credit
FROM snowflake.account_usage.query_history
WHERE query_type = 'COPY'
  AND start_time > dateadd('week', -4, current_date())
GROUP BY week, warehouse_name, warehouse_size
ORDER BY week DESC, total_credits DESC;
```

Higher gb_per_credit = more efficient.

## Exception: Backfills

For one-time historical backfills with tight timelines, larger warehouses can be justified. The trade-off shifts when:
- Wall-clock time matters (deadline-driven)
- Larger warehouse actually processes faster (high file count)
- It's a one-time cost, not ongoing

Even then, test first before assuming XL is needed.
