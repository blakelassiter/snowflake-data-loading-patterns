-- ============================================================================
-- Snowpipe: AWS S3 Setup
-- ============================================================================
-- Configure Snowpipe with S3 event notifications for automatic ingestion.
-- Requires: S3 bucket, IAM role, storage integration.

-- ----------------------------------------------------------------------------
-- Step 1: Create storage integration
-- ----------------------------------------------------------------------------
-- Grants Snowflake access to your S3 bucket via IAM role

CREATE OR REPLACE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-access'
    STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/data/');

-- Get the AWS IAM user ARN and external ID for trust policy
DESC STORAGE INTEGRATION s3_integration;

-- Use STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- to configure the trust relationship on your IAM role

-- ----------------------------------------------------------------------------
-- Step 2: Create external stage
-- ----------------------------------------------------------------------------

CREATE OR REPLACE STAGE raw.s3_stage
    STORAGE_INTEGRATION = s3_integration
    URL = 's3://my-bucket/data/'
    FILE_FORMAT = (TYPE = 'JSON');

-- Verify stage access
LIST @raw.s3_stage;

-- ----------------------------------------------------------------------------
-- Step 3: Create target table
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE raw.events (
    event_id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP_NTZ,
    payload VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file STRING
);

-- ----------------------------------------------------------------------------
-- Step 4: Create pipe
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PIPE raw.events_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO raw.events (event_id, event_type, event_timestamp, payload, _source_file)
    FROM (
        SELECT 
            $1:event_id::STRING,
            $1:event_type::STRING,
            $1:event_timestamp::TIMESTAMP_NTZ,
            $1:payload::VARIANT,
            METADATA$FILENAME
        FROM @raw.s3_stage
    )
    FILE_FORMAT = (TYPE = 'JSON');

-- Get the SQS queue ARN for S3 event configuration
SHOW PIPES LIKE 'events_pipe';
-- Note the notification_channel column - this is the SQS queue ARN

-- ----------------------------------------------------------------------------
-- Step 5: Configure S3 event notification (AWS Console or CLI)
-- ----------------------------------------------------------------------------

-- Using AWS CLI:
-- aws s3api put-bucket-notification-configuration \
--   --bucket my-bucket \
--   --notification-configuration '{
--     "QueueConfigurations": [{
--       "QueueArn": "<SQS_ARN_FROM_SHOW_PIPES>",
--       "Events": ["s3:ObjectCreated:*"],
--       "Filter": {
--         "Key": {
--           "FilterRules": [{
--             "Name": "prefix",
--             "Value": "data/"
--           }]
--         }
--       }
--     }]
--   }'

-- ----------------------------------------------------------------------------
-- Step 6: Verify setup
-- ----------------------------------------------------------------------------

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('raw.events_pipe');

-- Force a refresh to load existing files
ALTER PIPE raw.events_pipe REFRESH;

-- Check for loaded files
SELECT * FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('hour', -1, current_timestamp())
));

-- ----------------------------------------------------------------------------
-- Alternative: Manual pipe (no auto-ingest)
-- ----------------------------------------------------------------------------
-- For cases where you trigger loads via REST API instead of S3 events

CREATE OR REPLACE PIPE raw.manual_pipe
    AUTO_INGEST = FALSE
    AS
    COPY INTO raw.events
    FROM @raw.s3_stage
    FILE_FORMAT = (TYPE = 'JSON');

-- Trigger via REST API:
-- POST https://<account>.snowflakecomputing.com/v1/data/pipes/<pipe_name>/insertFiles
-- Body: {"files": ["path/to/file1.json", "path/to/file2.json"]}
