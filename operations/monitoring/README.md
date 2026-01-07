# Monitoring

Queries for tracking load status, costs and failures across all loading methods.

## Files

- [copy-history-queries.sql](copy-history-queries.sql) - COPY INTO and Snowpipe load history
- [pipe-status-queries.sql](pipe-status-queries.sql) - Snowpipe health monitoring
- [streaming-metrics.sql](streaming-metrics.sql) - Snowpipe Streaming monitoring
- [cost-tracking.sql](cost-tracking.sql) - Credit consumption by loading method

## Quick Reference

### Check recent load status

```sql
SELECT table_name, file_name, status, row_count, first_error_message
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('hour', -24, current_timestamp())
))
ORDER BY last_load_time DESC;
```

### Check pipe health

```sql
SELECT SYSTEM$PIPE_STATUS('raw.events_pipe');
```

### Daily load summary

```sql
SELECT 
    DATE_TRUNC('day', last_load_time) as day,
    COUNT(*) as files_loaded,
    SUM(row_count) as total_rows,
    COUNT_IF(status != 'LOADED') as failed_files
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -7, current_timestamp())
))
GROUP BY day
ORDER BY day DESC;
```

## Data Sources

### Information Schema (real-time, limited history)

- `information_schema.copy_history()` - Load history for specific table
- `SYSTEM$PIPE_STATUS()` - Current pipe state

### Account Usage (45-minute latency, 365-day history)

- `snowflake.account_usage.copy_history` - All COPY INTO history
- `snowflake.account_usage.pipe_usage_history` - Snowpipe credit consumption
- `snowflake.account_usage.snowpipe_streaming_file_migration_history` - Streaming metrics

## Related

- [Snowpipe troubleshooting](../../patterns/snowpipe/troubleshooting.md)
- [Snowflake docs: Monitoring Data Loading](https://docs.snowflake.com/en/user-guide/data-load-monitor)
