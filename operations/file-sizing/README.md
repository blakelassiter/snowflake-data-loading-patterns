# File Sizing

Snowflake's load performance depends significantly on file sizes. This directory contains guidance and queries for analyzing and optimizing file sizes.

## Optimal File Sizes

Snowflake recommends **100-250MB compressed** files for optimal loading performance.

| Size | Impact |
|------|--------|
| Under 10MB | High per-file overhead, inefficient |
| 10-100MB | Acceptable for Snowpipe; batch for COPY INTO |
| 100-250MB | Optimal range |
| 250-500MB | Still good, slightly reduced parallelism |
| Over 500MB | Consider splitting for better parallelism |

## Why Size Matters

**Too small:**
- Per-file processing overhead dominates
- More metadata operations
- Snowpipe charges per-file regardless of size
- Thousands of tiny files slow down stage listing

**Too large:**
- Reduced loading parallelism
- Longer recovery time if load fails mid-file
- Memory pressure during processing

## Files in This Directory

- [size-distribution-query.sql](size-distribution-query.sql) - Analyze file sizes in a stage
- [optimization-patterns.md](optimization-patterns.md) - How to fix size issues

## Quick Check

```sql
-- List files with sizes
LIST @my_stage/data/;

-- Analyze size distribution
SELECT 
    CASE 
        WHEN "size" < 10000000 THEN 'Under 10MB'
        WHEN "size" BETWEEN 10000000 AND 100000000 THEN '10-100MB'
        WHEN "size" BETWEEN 100000000 AND 250000000 THEN '100-250MB (optimal)'
        WHEN "size" BETWEEN 250000000 AND 500000000 THEN '250-500MB'
        ELSE 'Over 500MB'
    END as size_bucket,
    COUNT(*) as file_count,
    SUM("size") / 1e9 as total_gb
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
GROUP BY size_bucket
ORDER BY MIN("size");
```

## Related

- [COPY INTO patterns](../../patterns/copy-into/)
- [Snowpipe setup](../../patterns/snowpipe/)
- [Snowflake docs: File Sizing](https://docs.snowflake.com/en/user-guide/data-load-considerations-prepare#file-sizing-best-practices-and-limitations)
