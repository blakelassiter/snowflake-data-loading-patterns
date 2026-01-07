-- ============================================================================
-- Snowpipe: Google Cloud Storage Setup
-- ============================================================================
-- Configure Snowpipe with GCS Pub/Sub notifications for automatic ingestion.
-- Requires: GCS bucket, service account, storage integration, Pub/Sub topic.

-- ----------------------------------------------------------------------------
-- Step 1: Create storage integration
-- ----------------------------------------------------------------------------

CREATE OR REPLACE STORAGE INTEGRATION gcs_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://my-bucket/data/');

-- Get the service account email for GCS bucket permissions
DESC STORAGE INTEGRATION gcs_integration;

-- Grant Storage Object Viewer role to STORAGE_GCP_SERVICE_ACCOUNT
-- on your GCS bucket (via Google Cloud Console or gcloud CLI)

-- ----------------------------------------------------------------------------
-- Step 2: Create notification integration
-- ----------------------------------------------------------------------------
-- GCS uses Pub/Sub for event notifications (different from S3's SQS)

CREATE OR REPLACE NOTIFICATION INTEGRATION gcs_notification
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    ENABLED = TRUE
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/my-project/subscriptions/snowflake-sub';

-- Get the service account for Pub/Sub permissions
DESC NOTIFICATION INTEGRATION gcs_notification;

-- Grant Pub/Sub Subscriber role to GCP_PUBSUB_SERVICE_ACCOUNT
-- on your Pub/Sub subscription

-- ----------------------------------------------------------------------------
-- Step 3: Create GCS Pub/Sub subscription (Google Cloud)
-- ----------------------------------------------------------------------------

-- 1. Create Pub/Sub topic
-- gcloud pubsub topics create snowflake-notifications

-- 2. Create subscription for Snowflake
-- gcloud pubsub subscriptions create snowflake-sub \
--   --topic=snowflake-notifications

-- 3. Configure GCS bucket notification
-- gsutil notification create \
--   -t snowflake-notifications \
--   -f json \
--   -e OBJECT_FINALIZE \
--   gs://my-bucket

-- ----------------------------------------------------------------------------
-- Step 4: Create external stage
-- ----------------------------------------------------------------------------

CREATE OR REPLACE STAGE raw.gcs_stage
    STORAGE_INTEGRATION = gcs_integration
    URL = 'gcs://my-bucket/data/'
    FILE_FORMAT = (TYPE = 'PARQUET');

-- Verify stage access
LIST @raw.gcs_stage;

-- ----------------------------------------------------------------------------
-- Step 5: Create target table
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
-- Step 6: Create pipe with notification integration
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PIPE raw.events_pipe
    AUTO_INGEST = TRUE
    INTEGRATION = 'GCS_NOTIFICATION'
    AS
    COPY INTO raw.events (event_id, event_type, event_timestamp, payload, _source_file)
    FROM (
        SELECT 
            $1:event_id::STRING,
            $1:event_type::STRING,
            $1:event_timestamp::TIMESTAMP_NTZ,
            $1:payload::VARIANT,
            METADATA$FILENAME
        FROM @raw.gcs_stage
    )
    FILE_FORMAT = (TYPE = 'PARQUET');

-- ----------------------------------------------------------------------------
-- Step 7: Verify setup
-- ----------------------------------------------------------------------------

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('raw.events_pipe');

-- Force refresh for existing files
ALTER PIPE raw.events_pipe REFRESH;

-- Monitor load history
SELECT * FROM TABLE(information_schema.copy_history(
    table_name => 'raw.events',
    start_time => dateadd('hour', -1, current_timestamp())
));

-- ----------------------------------------------------------------------------
-- Troubleshooting GCS-specific issues
-- ----------------------------------------------------------------------------

-- Check notification integration status
SHOW NOTIFICATION INTEGRATIONS;

-- Verify Pub/Sub subscription is receiving messages
-- gcloud pubsub subscriptions pull snowflake-sub --auto-ack --limit=10

-- Common issues:
-- 1. Service account missing Storage Object Viewer on bucket
-- 2. Service account missing Pub/Sub Subscriber on subscription
-- 3. GCS notification not configured for correct bucket prefix
-- 4. Subscription name format incorrect (must be full path)
