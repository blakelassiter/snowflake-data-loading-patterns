# Snowpipe Troubleshooting

Common issues and how to diagnose them.

## Pipe Not Loading Files

### Check pipe status first

```sql
SELECT SYSTEM$PIPE_STATUS('raw.events_pipe');
```

**executionState values:**

| State | Meaning | Action |
|-------|---------|--------|
| `RUNNING` | Pipe is active | Check pending files, event notifications |
| `PAUSED` | Pipe manually paused | Resume if intentional: `ALTER PIPE ... SET PIPE_EXECUTION_PAUSED = FALSE` |
| `STALLED` | Pipe encountered errors | Check copy history for error details |

### High pending file count

If `pendingFileCount` is high and not decreasing:

1. Check for schema mismatches between files and table
2. Look for malformed files in the batch
3. Verify file format settings match actual file structure

```sql
-- Check recent load errors
SELECT file_name, status, first_error_message
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('hour', -1, current_timestamp())
))
WHERE status != 'LOADED';
```

### No events arriving

If `lastReceivedMessageTimestamp` is stale:

**AWS S3:**
- Verify S3 event notification points to correct SQS queue
- Check SQS queue permissions allow Snowflake to read
- Confirm event filter prefix matches file paths

**GCS:**
- Verify Pub/Sub subscription exists and is active
- Check GCS notification is configured for correct bucket
- Confirm service account has Pub/Sub Subscriber role

**Azure:**
- Verify Event Grid subscription is active
- Check Storage Queue has messages arriving
- Confirm service principal has queue read permissions

## Files Load But Data Is Wrong

### Schema mismatch

```sql
-- Check what Snowflake sees in the file
SELECT $1 FROM @raw.stage/sample_file.json LIMIT 5;

-- Compare to table schema
DESC TABLE raw.events;
```

### Type conversion errors

Files may load with `ON_ERROR = CONTINUE` but skip rows:

```sql
SELECT file_name, row_parsed, row_count, 
       row_parsed - row_count as skipped_rows
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -1, current_timestamp())
))
WHERE row_parsed > row_count;
```

### Duplicate data

Snowpipe tracks files for 14 days. After that, redelivered files may reload.

Check if duplicates correlate with file redelivery:

```sql
-- Find potential duplicate loads
SELECT file_name, COUNT(*) as load_count
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -30, current_timestamp())
))
GROUP BY file_name
HAVING COUNT(*) > 1;
```

## Performance Issues

### Slow load times

Snowpipe latency is typically 1-2 minutes. If consistently slower:

1. **File size:** Very small files (<1MB) add overhead. Batch upstream if possible.
2. **File count:** Thousands of tiny files in a burst overwhelm the queue.
3. **Complex transformations:** Heavy SELECT logic in COPY slows per-file processing.

### Cost higher than expected

```sql
-- Credits per file loaded
SELECT 
    pipe_name,
    SUM(credits_used) as total_credits,
    SUM(files_inserted) as total_files,
    ROUND(SUM(credits_used) / NULLIF(SUM(files_inserted), 0), 6) as credits_per_file
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY pipe_name;
```

High credits-per-file often indicates:
- Very small files (fixed overhead per file)
- Complex transformations in COPY statement
- Many partial file loads due to errors

## Cloud-Specific Issues

### AWS S3

**"Access Denied" errors:**
- Storage integration IAM role missing `s3:GetObject` permission
- Trust policy doesn't include Snowflake's external ID
- Bucket policy explicitly denies access

**Events not arriving:**
- S3 event notification not configured
- SQS queue policy doesn't allow S3 to send messages
- Event filter prefix doesn't match file paths

```sql
-- Get SQS ARN for S3 configuration
SHOW PIPES LIKE 'events_pipe';
-- notification_channel column has the ARN
```

### Google Cloud Storage

**"Permission denied" errors:**
- Service account missing Storage Object Viewer on bucket
- Wrong project or bucket in storage integration

**Pub/Sub issues:**
- Subscription doesn't exist or wrong name format
- Service account missing Pub/Sub Subscriber role
- GCS notification not configured

```sql
-- Verify notification integration
DESC NOTIFICATION INTEGRATION gcs_notification;
```

### Azure

**Consent not granted:**
- Navigate to `AZURE_CONSENT_URL` from `DESC STORAGE INTEGRATION`
- Admin must approve in Azure AD

**Queue not receiving events:**
- Event Grid subscription misconfigured
- Storage Queue doesn't exist
- Subject filter doesn't match blob paths

## Recovery Procedures

### Reload failed files

```sql
-- Identify failed files
SELECT DISTINCT file_name
FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('day', -7, current_timestamp())
))
WHERE status = 'LOAD_FAILED';

-- After fixing the issue, refresh to retry
ALTER PIPE raw.events_pipe REFRESH;
```

### Load files that arrived before pipe was created

```sql
-- Refresh loads all files in stage not yet loaded
ALTER PIPE raw.events_pipe REFRESH;

-- Or specify a prefix/date range
ALTER PIPE raw.events_pipe REFRESH PREFIX = 'data/2025-01/';
```

### Pause and resume

```sql
-- Pause for maintenance
ALTER PIPE raw.events_pipe SET PIPE_EXECUTION_PAUSED = TRUE;

-- Resume
ALTER PIPE raw.events_pipe SET PIPE_EXECUTION_PAUSED = FALSE;
```

Files that arrive while paused queue up and load when resumed.

### Recreate pipe

If pipe is corrupted or needs significant changes:

```sql
-- Capture current definition
SHOW PIPES LIKE 'events_pipe';

-- Drop and recreate
DROP PIPE raw.events_pipe;

CREATE PIPE raw.events_pipe ... ;

-- Refresh to load any files added during recreation
ALTER PIPE raw.events_pipe REFRESH;
```

Note: Recreating resets load tracking. Use `FORCE = TRUE` in manual COPY if needed to avoid duplicates, or ensure idempotent downstream handling.
