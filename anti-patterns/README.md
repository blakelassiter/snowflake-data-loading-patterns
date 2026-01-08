# Anti-Patterns

Common mistakes in Snowflake data loading and their costs. Each anti-pattern includes detection queries and remediation approaches.

## Index

| Anti-Pattern | Impact | Detection Difficulty |
|--------------|--------|---------------------|
| [Tiny Files](tiny-files.md) | High compute overhead | Easy |
| [Warehouse Oversizing](warehouse-oversizing.md) | Wasted credits | Medium |
| [Duplicate Loading](duplicate-loading.md) | Data integrity issues | Medium |
| [Missing Partitions](missing-partitions.md) | Full table scans | Easy |
| [Ignoring Errors](ignoring-errors.md) | Silent data loss | Hard |

## Quick Cost Reference

These are rough estimates based on typical scenarios. Actual impact varies.

| Anti-Pattern | Typical Overhead |
|--------------|------------------|
| 10,000 x 1KB files vs 100 x 100MB files | 5-10x more credits |
| XL warehouse for 10GB load vs Medium | 4x credit cost |
| Loading same files twice | 2x credits + storage |
| External table without partitions | 10-100x query time |

## Detection Queries

### Tiny files

```sql
LIST @your_stage/;

SELECT 
    COUNT_IF("size" < 1000000) as files_under_1mb,
    COUNT_IF("size" BETWEEN 1000000 AND 10000000) as files_1_to_10mb,
    COUNT_IF("size" > 100000000) as files_over_100mb
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

### Warehouse oversizing

```sql
SELECT 
    warehouse_name,
    warehouse_size,
    AVG(execution_time) / 1000 as avg_seconds,
    AVG(credits_used_cloud_services) as avg_credits
FROM snowflake.account_usage.query_history
WHERE query_type = 'COPY'
  AND start_time > dateadd('day', -7, current_date())
GROUP BY warehouse_name, warehouse_size;
```

### Duplicate files loaded

```sql
SELECT file_name, COUNT(*) as load_count
FROM TABLE(information_schema.copy_history(
    table_name => 'your_table',
    start_time => dateadd('day', -30, current_timestamp())
))
GROUP BY file_name
HAVING COUNT(*) > 1;
```

## Related

- [File sizing guidance](../operations/file-sizing/)
- [Monitoring queries](../operations/monitoring/)
- [Decision framework](../decision-framework/)
