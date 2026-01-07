-- ============================================================================
-- Native Snowflake Task Orchestration
-- ============================================================================
-- Use Snowflake Tasks and Streams to trigger dbt after data loads.
-- No external orchestrator required.

-- ----------------------------------------------------------------------------
-- Pattern 1: Stream-based trigger
-- ----------------------------------------------------------------------------
-- Create a stream on raw data, use task to detect changes and call dbt

-- Create stream on raw table
CREATE OR REPLACE STREAM raw.events_stream
ON TABLE raw.events
APPEND_ONLY = TRUE;

-- Task that runs dbt when stream has data
-- Note: Requires external function or stored procedure to call dbt API
CREATE OR REPLACE TASK ops.dbt_events_trigger
    WAREHOUSE = ops_wh
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('raw.events_stream')
AS
CALL ops.trigger_dbt_run('events_pipeline');

-- Resume the task
ALTER TASK ops.dbt_events_trigger RESUME;

-- ----------------------------------------------------------------------------
-- Stored procedure to call dbt Cloud API
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE ops.trigger_dbt_run(selector STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // This requires setting up a secret and external access integration
    // See: https://docs.snowflake.com/en/developer-guide/external-network-access/external-network-access-overview
    
    var dbt_account_id = 'YOUR_ACCOUNT_ID';
    var dbt_job_id = 'YOUR_JOB_ID';
    var dbt_api_token = snowflake.execute({
        sqlText: "SELECT secret_string FROM ops.secrets WHERE name = 'dbt_cloud_token'"
    }).next().getColumnValue(1);
    
    // Build request
    var url = `https://cloud.getdbt.com/api/v2/accounts/${dbt_account_id}/jobs/${dbt_job_id}/run/`;
    var body = {
        cause: `Snowflake Task: ${SELECTOR}`,
        steps_override: SELECTOR ? [`dbt run --selector ${SELECTOR}`] : null
    };
    
    // Note: Actual HTTP call requires external access integration
    // This is a simplified example
    
    return 'Triggered dbt run for: ' + SELECTOR;
$$;

-- ----------------------------------------------------------------------------
-- Pattern 2: Task chain (load → transform)
-- ----------------------------------------------------------------------------
-- Chain tasks so dbt runs immediately after load completes

-- Parent task: load data
CREATE OR REPLACE TASK ops.load_events
    WAREHOUSE = loading_wh
    SCHEDULE = '1 HOUR'
AS
COPY INTO raw.events
FROM @events_stage/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

-- Child task: run dbt (triggered by parent success)
CREATE OR REPLACE TASK ops.transform_events
    WAREHOUSE = transform_wh
    AFTER ops.load_events
AS
CALL ops.trigger_dbt_run('events_pipeline');

-- Resume both tasks (start with leaf, then root)
ALTER TASK ops.transform_events RESUME;
ALTER TASK ops.load_events RESUME;

-- ----------------------------------------------------------------------------
-- Pattern 3: Conditional dbt trigger based on row count
-- ----------------------------------------------------------------------------
-- Only run dbt if enough data was loaded

CREATE OR REPLACE TASK ops.conditional_dbt_trigger
    WAREHOUSE = ops_wh
    SCHEDULE = '1 HOUR'
AS
DECLARE
    rows_loaded INTEGER;
BEGIN
    -- Check how many rows were loaded in last hour
    SELECT SUM(row_count) INTO :rows_loaded
    FROM TABLE(information_schema.copy_history(
        table_name => 'raw.events',
        start_time => dateadd('hour', -1, current_timestamp())
    ));
    
    -- Only trigger dbt if meaningful amount of data
    IF (rows_loaded > 100) THEN
        CALL ops.trigger_dbt_run('events_pipeline');
    END IF;
END;

-- ----------------------------------------------------------------------------
-- Pattern 4: Using Snowflake Alerts for dbt triggering
-- ----------------------------------------------------------------------------
-- Alert when data meets criteria, trigger dbt as action

CREATE OR REPLACE ALERT ops.new_data_alert
    WAREHOUSE = ops_wh
    SCHEDULE = '5 MINUTE'
IF (
    EXISTS (
        SELECT 1 
        FROM raw.events_stream
        LIMIT 1
    )
)
THEN
    CALL ops.trigger_dbt_run('events_pipeline');

-- Resume alert
ALTER ALERT ops.new_data_alert RESUME;

-- ----------------------------------------------------------------------------
-- External Access Integration (required for HTTP calls)
-- ----------------------------------------------------------------------------
-- Set up external network access to call dbt Cloud API

-- Create network rule
CREATE OR REPLACE NETWORK RULE ops.dbt_cloud_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('cloud.getdbt.com:443');

-- Create secret for API token
CREATE OR REPLACE SECRET ops.dbt_cloud_token
    TYPE = GENERIC_STRING
    SECRET_STRING = 'your-dbt-cloud-api-token';

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_cloud_integration
    ALLOWED_NETWORK_RULES = (ops.dbt_cloud_rule)
    ALLOWED_AUTHENTICATION_SECRETS = (ops.dbt_cloud_token)
    ENABLED = TRUE;

-- Now stored procedures can use this integration to make HTTP calls
-- See Snowflake docs for full implementation details

-- ----------------------------------------------------------------------------
-- Monitoring task execution
-- ----------------------------------------------------------------------------

-- View task history
SELECT *
FROM TABLE(information_schema.task_history(
    scheduled_time_range_start => dateadd('hour', -24, current_timestamp()),
    result_limit => 100
))
WHERE name = 'DBT_EVENTS_TRIGGER'
ORDER BY scheduled_time DESC;

-- View currently running tasks
SELECT *
FROM TABLE(information_schema.task_history())
WHERE state = 'EXECUTING';

-- Check stream status
SELECT 
    table_name,
    stale_after,
    stale
FROM TABLE(information_schema.applicable_roles())
WHERE TRUE;

-- Actually check stream offset
SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP('raw.events_stream');

-- ----------------------------------------------------------------------------
-- Task management
-- ----------------------------------------------------------------------------

-- Suspend all tasks (for maintenance)
ALTER TASK ops.transform_events SUSPEND;
ALTER TASK ops.load_events SUSPEND;

-- View task dependencies
SHOW TASKS IN SCHEMA ops;

-- Drop task chain
DROP TASK ops.transform_events;
DROP TASK ops.load_events;
