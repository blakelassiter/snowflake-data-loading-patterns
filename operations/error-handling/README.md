# Error Handling

Patterns for validating data before loading, handling errors during load, and recovering from failures.

## Overview

Error handling spans three phases:

1. **Pre-load validation** - Catch problems before committing data
2. **Load-time handling** - Control behavior when errors occur
3. **Post-load recovery** - Fix and reload failed data

## Files in This Directory

- [validation-patterns.sql](validation-patterns.sql) - VALIDATION_MODE usage, pre-load checks
- [on-error-options.sql](on-error-options.sql) - ON_ERROR behavior deep dive
- [recovery-procedures.sql](recovery-procedures.sql) - Reloading failed files, backfill patterns
- [rejected-records.sql](rejected-records.sql) - Capturing and analyzing failed rows

## Quick Reference

### ON_ERROR Options

| Option | Behavior | Best For |
|--------|----------|----------|
| `ABORT_STATEMENT` | Stop on first error | Critical data, zero tolerance |
| `CONTINUE` | Skip bad rows, load good ones | Tolerant pipelines |
| `SKIP_FILE` | Skip file on any error | File-level atomicity |
| `SKIP_FILE_<n>` | Skip if errors exceed count | Threshold-based |
| `SKIP_FILE_<n>%` | Skip if errors exceed percentage | Percentage-based |

### Validation Before Loading

```sql
-- Test without loading
COPY INTO target_table
FROM @stage/data/
FILE_FORMAT = (TYPE = 'PARQUET')
VALIDATION_MODE = 'RETURN_ALL_ERRORS';
```

### Check Results After Loading

```sql
-- What happened?
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

## Related

- [COPY INTO error handling](../../patterns/copy-into/error-handling.sql)
- [Snowpipe troubleshooting](../../patterns/snowpipe/troubleshooting.md)
- [Monitoring queries](../monitoring/)
