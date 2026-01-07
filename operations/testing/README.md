# Testing

Data quality validation during and after loading. Catch issues before they propagate to downstream systems.

## Testing Categories

1. **Reconciliation** - Row counts and checksums match source
2. **Schema validation** - Structure matches expectations
3. **Data quality** - Values within expected ranges and formats
4. **Freshness** - Data arrived on time

## Files in This Directory

- [row-count-reconciliation.sql](row-count-reconciliation.sql) - Source-to-target count validation
- [checksum-validation.sql](checksum-validation.sql) - Hash-based data verification
- [schema-drift-detection.sql](schema-drift-detection.sql) - Detect unexpected schema changes
- [data-quality-checks.sql](data-quality-checks.sql) - Value validation and profiling

## Quick Checks

### Row count reconciliation

```sql
-- Compare staged file count to loaded rows
SELECT 
    (SELECT COUNT(*) FROM @stage/data/) as staged_rows,
    (SELECT COUNT(*) FROM raw.events WHERE loaded_at > dateadd('hour', -1, current_timestamp())) as loaded_rows;
```

### Check for unexpected nulls

```sql
SELECT 
    COUNT(*) as total,
    COUNT_IF(customer_id IS NULL) as null_customer_ids,
    COUNT_IF(order_total IS NULL) as null_totals
FROM raw.orders
WHERE loaded_at > dateadd('hour', -1, current_timestamp());
```

### Freshness check

```sql
SELECT 
    MAX(event_timestamp) as latest_event,
    DATEDIFF('minute', MAX(event_timestamp), CURRENT_TIMESTAMP()) as minutes_behind
FROM raw.events;
```

## Integration with dbt

For teams using dbt, these patterns complement dbt tests:

- Run SQL checks immediately after load (pre-dbt)
- Use dbt tests for transformation validation
- See [dbt-snowflake-optimization](https://github.com/blakelassiter/dbt-snowflake-optimization) for dbt-specific patterns

## Related

- [Error handling](../error-handling/) - What to do when tests fail
- [Monitoring](../monitoring/) - Tracking test results over time
