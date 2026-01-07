-- ============================================================================
-- Monitoring: Alerting Patterns
-- ============================================================================
-- Snowflake Alerts for detecting and responding to data loading issues.
-- Alerts run on a schedule and trigger actions when conditions are met.

-- ----------------------------------------------------------------------------
-- Alert: COPY INTO failures
-- ----------------------------------------------------------------------------

-- Detect failed COPY INTO loads in the last hour
CREATE OR REPLACE ALERT ops.alert_copy_failures
    WAREHOUSE = ops_wh
    SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every hour
    IF (EXISTS (
        SELECT 1 
        FROM TABLE(information_schema.copy_history(
            table_name => 'raw.events',
            start_time => dateadd('hour', -1, current_timestamp())
        ))
        WHERE status = 'LOAD_FAILED'
    ))
    THEN
        CALL system$send_email(
            'data_alerts',
            'data-team@company.com',
            'COPY INTO Failure Alert',
            'One or more COPY INTO loads failed in the last hour. Check copy_history for details.'
        );

-- Enable the alert
ALTER ALERT ops.alert_copy_failures RESUME;

-- ----------------------------------------------------------------------------
-- Alert: Snowpipe stalled
-- ----------------------------------------------------------------------------

-- Detect when Snowpipe stops processing
CREATE OR REPLACE ALERT ops.alert_pipe_stalled
    WAREHOUSE = ops_wh
    SCHEDULE = 'USING CRON */15 * * * * UTC'  -- Every 15 minutes
    IF (EXISTS (
        SELECT 1
        FROM (
            SELECT 
                PARSE_JSON(SYSTEM$PIPE_STATUS('raw.events_pipe')):executionState::STRING as state,
                PARSE_JSON(SYSTEM$PIPE_STATUS('raw.events_pipe')):pendingFileCount::INTEGER as pending
        )
        WHERE state = 'STALLED' OR pending > 1000
    ))
    THEN
        CALL system$send_email(
            'data_alerts',
            'data-team@company.com',
            'Snowpipe Alert: Pipe Stalled or Backlogged',
            'Pipe raw.events_pipe is stalled or has high pending file count.'
        );

ALTER ALERT ops.alert_pipe_stalled RESUME;

-- ----------------------------------------------------------------------------
-- Alert: No data loaded recently
-- ----------------------------------------------------------------------------

-- Detect when expected data hasn't arrived
CREATE OR REPLACE ALERT ops.alert_no_recent_data
    WAREHOUSE = ops_wh
    SCHEDULE = 'USING CRON 0 8 * * * UTC'  -- Daily at 8 AM UTC
    IF (EXISTS (
        SELECT 1
        FROM raw.events
        HAVING MAX(event_timestamp) < dateadd('hour', -4, current_timestamp())
    ))
    THEN
        CALL system$send_email(
            'data_alerts',
            'data-team@company.com',
            'Data Freshness Alert',
            'No new events loaded in the last 4 hours.'
        );

ALTER ALERT ops.alert_no_recent_data RESUME;

-- ----------------------------------------------------------------------------
-- Alert: High error rate in loads
-- ----------------------------------------------------------------------------

CREATE OR REPLACE ALERT ops.alert_high_error_rate
    WAREHOUSE = ops_wh
    SCHEDULE = 'USING CRON 0 * * * * UTC'
    IF (EXISTS (
        SELECT 1
        FROM (
            SELECT 
                SUM(row_parsed) as total_parsed,
                SUM(row_count) as total_loaded,
                (SUM(row_parsed) - SUM(row_count)) * 100.0 / NULLIF(SUM(row_parsed), 0) as error_pct
            FROM TABLE(information_schema.copy_history(
                table_name => 'raw.events',
                start_time => dateadd('hour', -1, current_timestamp())
            ))
        )
        WHERE error_pct > 5  -- More than 5% errors
    ))
    THEN
        CALL system$send_email(
            'data_alerts',
            'data-team@company.com',
            'High Load Error Rate',
            'More than 5% of rows failed to load in the last hour.'
        );

ALTER ALERT ops.alert_high_error_rate RESUME;

-- ----------------------------------------------------------------------------
-- Alert: Cost threshold exceeded
-- ----------------------------------------------------------------------------

-- Daily cost check using Account Usage
CREATE OR REPLACE ALERT ops.alert_cost_threshold
    WAREHOUSE = ops_wh
    SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- Daily at 6 AM
    IF (EXISTS (
        SELECT 1
        FROM snowflake.account_usage.metering_history
        WHERE service_type IN ('PIPE', 'SNOWPIPE_STREAMING')
          AND start_time >= dateadd('day', -1, current_date())
        HAVING SUM(credits_used) > 100  -- Threshold: 100 credits/day
    ))
    THEN
        CALL system$send_email(
            'data_alerts',
            'data-team@company.com',
            'Data Loading Cost Alert',
            'Daily Snowpipe/Streaming credits exceeded 100.'
        );

ALTER ALERT ops.alert_cost_threshold RESUME;

-- ----------------------------------------------------------------------------
-- View alert history
-- ----------------------------------------------------------------------------

-- Check when alerts fired
SELECT *
FROM TABLE(information_schema.alert_history(
    scheduled_time_range_start => dateadd('day', -7, current_timestamp()),
    scheduled_time_range_end => current_timestamp()
))
ORDER BY scheduled_time DESC;

-- ----------------------------------------------------------------------------
-- Suspend and resume alerts
-- ----------------------------------------------------------------------------

-- Suspend during maintenance
ALTER ALERT ops.alert_copy_failures SUSPEND;

-- Resume after maintenance
ALTER ALERT ops.alert_copy_failures RESUME;

-- ----------------------------------------------------------------------------
-- Alternative: Webhook notifications
-- ----------------------------------------------------------------------------

-- Instead of email, call an external API (e.g., PagerDuty, Slack)
CREATE OR REPLACE ALERT ops.alert_pipe_stalled_webhook
    WAREHOUSE = ops_wh
    SCHEDULE = 'USING CRON */15 * * * * UTC'
    IF (EXISTS (
        SELECT 1
        FROM (
            SELECT PARSE_JSON(SYSTEM$PIPE_STATUS('raw.events_pipe')):executionState::STRING as state
        )
        WHERE state = 'STALLED'
    ))
    THEN
        -- Requires external function setup for webhook
        -- CALL ops.send_pagerduty_alert('Snowpipe stalled: raw.events_pipe');
        CALL system$log('error', 'Snowpipe stalled: raw.events_pipe');

-- ----------------------------------------------------------------------------
-- Setup: Email notification integration
-- ----------------------------------------------------------------------------

/*
Before using system$send_email, create a notification integration:

CREATE NOTIFICATION INTEGRATION data_alerts
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('data-team@company.com', 'oncall@company.com');

Grant usage to roles that will create alerts:
GRANT USAGE ON INTEGRATION data_alerts TO ROLE data_engineer;
*/

-- ----------------------------------------------------------------------------
-- Cleanup: Drop alerts
-- ----------------------------------------------------------------------------

-- DROP ALERT ops.alert_copy_failures;
-- DROP ALERT ops.alert_pipe_stalled;
-- DROP ALERT ops.alert_no_recent_data;
-- DROP ALERT ops.alert_high_error_rate;
-- DROP ALERT ops.alert_cost_threshold;
