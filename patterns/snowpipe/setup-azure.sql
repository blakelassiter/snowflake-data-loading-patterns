-- ============================================================================
-- Snowpipe: Azure Blob Storage Setup
-- ============================================================================
-- Configure Snowpipe with Azure Event Grid notifications for automatic ingestion.
-- Requires: Azure storage account, container, storage integration, Event Grid.

-- ----------------------------------------------------------------------------
-- Step 1: Create storage integration
-- ----------------------------------------------------------------------------

CREATE OR REPLACE STORAGE INTEGRATION azure_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'AZURE'
    ENABLED = TRUE
    AZURE_TENANT_ID = '<tenant_id>'
    STORAGE_ALLOWED_LOCATIONS = ('azure://myaccount.blob.core.windows.net/mycontainer/data/');

-- Get consent URL and service principal info
DESC STORAGE INTEGRATION azure_integration;

-- Navigate to AZURE_CONSENT_URL and grant consent
-- Then grant Storage Blob Data Reader role to AZURE_MULTI_TENANT_APP_NAME
-- on your storage account/container

-- ----------------------------------------------------------------------------
-- Step 2: Create notification integration
-- ----------------------------------------------------------------------------
-- Azure uses Event Grid with Storage Queue for notifications

CREATE OR REPLACE NOTIFICATION INTEGRATION azure_notification
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
    ENABLED = TRUE
    AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://myaccount.queue.core.windows.net/snowflake-queue'
    AZURE_TENANT_ID = '<tenant_id>';

-- Get service principal for queue access
DESC NOTIFICATION INTEGRATION azure_notification;

-- Grant Storage Queue Data Contributor role to the service principal
-- on the storage queue

-- ----------------------------------------------------------------------------
-- Step 3: Configure Azure Event Grid (Azure Portal or CLI)
-- ----------------------------------------------------------------------------

-- 1. Create Storage Queue
-- az storage queue create --name snowflake-queue --account-name myaccount

-- 2. Create Event Grid subscription
-- az eventgrid event-subscription create \
--   --name snowflake-events \
--   --source-resource-id /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/myaccount \
--   --endpoint-type storagequeue \
--   --endpoint /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/myaccount/queueservices/default/queues/snowflake-queue \
--   --included-event-types Microsoft.Storage.BlobCreated \
--   --subject-begins-with /blobServices/default/containers/mycontainer/blobs/data/

-- ----------------------------------------------------------------------------
-- Step 4: Create external stage
-- ----------------------------------------------------------------------------

CREATE OR REPLACE STAGE raw.azure_stage
    STORAGE_INTEGRATION = azure_integration
    URL = 'azure://myaccount.blob.core.windows.net/mycontainer/data/'
    FILE_FORMAT = (TYPE = 'JSON');

-- Verify stage access
LIST @raw.azure_stage;

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
    INTEGRATION = 'AZURE_NOTIFICATION'
    AS
    COPY INTO raw.events (event_id, event_type, event_timestamp, payload, _source_file)
    FROM (
        SELECT 
            $1:event_id::STRING,
            $1:event_type::STRING,
            $1:event_timestamp::TIMESTAMP_NTZ,
            $1:payload::VARIANT,
            METADATA$FILENAME
        FROM @raw.azure_stage
    )
    FILE_FORMAT = (TYPE = 'JSON');

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
-- Troubleshooting Azure-specific issues
-- ----------------------------------------------------------------------------

-- Check notification integration status
SHOW NOTIFICATION INTEGRATIONS;

-- Common issues:
-- 1. Consent not granted - visit AZURE_CONSENT_URL
-- 2. Missing Storage Blob Data Reader on container
-- 3. Missing Storage Queue Data Contributor on queue
-- 4. Event Grid subscription filtering wrong path
-- 5. Tenant ID mismatch between integration and Azure resources

-- Verify queue is receiving messages (Azure CLI):
-- az storage message peek --queue-name snowflake-queue --account-name myaccount

-- ----------------------------------------------------------------------------
-- Alternative: SAS token authentication
-- ----------------------------------------------------------------------------
-- If service principal approach doesn't fit, use SAS tokens

CREATE OR REPLACE STAGE raw.azure_sas_stage
    URL = 'azure://myaccount.blob.core.windows.net/mycontainer/data/'
    CREDENTIALS = (AZURE_SAS_TOKEN = '<sas_token>')
    FILE_FORMAT = (TYPE = 'JSON');

-- Note: SAS tokens require manual rotation and don't support auto-ingest
-- notifications as cleanly as storage integrations
