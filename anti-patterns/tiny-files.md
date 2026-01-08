# Anti-Pattern: Tiny Files

Loading thousands of small files instead of fewer large files.

## The Problem

Snowflake has fixed overhead per file:
- File metadata parsing
- Stage access operations
- Load tracking records
- Micro-partition creation

With tiny files (<1MB), this overhead dominates actual data processing. Loading 10,000 x 1KB files costs far more than loading 10 x 1MB files, even though total data volume is identical.

## Cost Impact

**Scenario comparison:**

| Approach | Files | Total Size | Relative Cost |
|----------|-------|------------|---------------|
| 10,000 x 1KB files | 10,000 | 10MB | ~10x baseline |
| 100 x 100KB files | 100 | 10MB | ~2x baseline |
| 10 x 1MB files | 10 | 10MB | 1x baseline |

The overhead varies but 5-10x cost inflation for extremely small files is common.

**Snowpipe impact is worse:**
- Per-file serverless billing
- Queue management overhead per file
- Event notification costs per file

## Detection

```sql
-- Check file sizes in stage
LIST @your_stage/data/;

-- Analyze distribution
SELECT 
    CASE 
        WHEN "size" < 100000 THEN 'Under 100KB (problem)'
        WHEN "size" < 1000000 THEN '100KB-1MB (suboptimal)'
        WHEN "size" < 10000000 THEN '1-10MB (acceptable)'
        WHEN "size" < 100000000 THEN '10-100MB (good)'
        WHEN "size" < 250000000 THEN '100-250MB (optimal)'
        ELSE 'Over 250MB'
    END as size_bucket,
    COUNT(*) as file_count,
    ROUND(SUM("size") / 1e6, 2) as total_mb
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
GROUP BY size_bucket
ORDER BY MIN("size");
```

**Red flags:**
- More than 50% of files under 1MB
- Average file size under 10MB
- Thousands of files per load batch

## Root Causes

1. **Micro-batching too aggressively** - Writing files every few seconds when minutes would suffice
2. **Partitioning too granularly** - One file per minute per partition
3. **Streaming to files** - Each message becomes a file
4. **Legacy system constraints** - Source system generates small files

## Remediation

### Option 1: Batch upstream

Modify the source system to batch data before writing:

```python
# Instead of:
for record in records:
    write_file(record)  # 1 record per file

# Do:
batch = []
for record in records:
    batch.append(record)
    if len(batch) >= 10000 or time_elapsed > 60:
        write_file(batch)
        batch = []
```

### Option 2: Consolidate in stage

If you can't change the source, consolidate before loading:

```sql
-- Copy small files to consolidation stage
-- Use external process (Spark, AWS Glue) to merge files
-- Then load from consolidated stage

-- Or use Snowflake to consolidate via external table + CTAS
CREATE TABLE raw.events_consolidated AS
SELECT * FROM external_table_pointing_to_small_files;

-- Then use the consolidated table
```

### Option 3: Adjust load frequency

Instead of loading continuously, batch loads:

```sql
-- Instead of loading every file immediately with Snowpipe,
-- run COPY INTO less frequently with more files per batch

-- Every 15 minutes instead of continuous
COPY INTO raw.events
FROM @stage/events/
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = '.*[.]json';
```

### Option 4: Accept the cost (sometimes valid)

If latency requirements mandate small files and the cost is acceptable, document the trade-off and monitor.

## Monitoring

Track file sizes over time:

```sql
-- Create monitoring table
CREATE TABLE IF NOT EXISTS ops.file_size_metrics (
    check_date DATE,
    stage_name STRING,
    file_count INTEGER,
    avg_size_mb FLOAT,
    pct_under_1mb FLOAT
);

-- Daily check
INSERT INTO ops.file_size_metrics
SELECT 
    CURRENT_DATE(),
    '@your_stage/',
    COUNT(*),
    AVG("size") / 1e6,
    COUNT_IF("size" < 1000000) * 100.0 / COUNT(*)
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  -- After LIST

-- Alert if degrading
SELECT * FROM ops.file_size_metrics
WHERE pct_under_1mb > 50
ORDER BY check_date DESC;
```

## Related

- [File sizing guidance](../operations/file-sizing/)
- [Snowpipe cost model](../patterns/snowpipe/README.md)
