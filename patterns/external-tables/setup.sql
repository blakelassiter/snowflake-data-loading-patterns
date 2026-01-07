-- ============================================================================
-- External Tables: Setup
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Prerequisites: Storage integration and stage
-- ----------------------------------------------------------------------------

-- Create storage integration (if not exists)
CREATE OR REPLACE STORAGE INTEGRATION ext_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-access'
    STORAGE_ALLOWED_LOCATIONS = ('s3://my-data-lake/');

-- Create external stage
CREATE OR REPLACE STAGE archive_stage
    STORAGE_INTEGRATION = ext_integration
    URL = 's3://my-data-lake/archive/'
    FILE_FORMAT = (TYPE = 'PARQUET');

-- Verify access
LIST @archive_stage;

-- ----------------------------------------------------------------------------
-- Basic external table
-- ----------------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL TABLE lake.events_archive (
    event_id STRING AS (VALUE:event_id::STRING),
    event_type STRING AS (VALUE:event_type::STRING),
    user_id STRING AS (VALUE:user_id::STRING),
    event_timestamp TIMESTAMP_NTZ AS (VALUE:event_timestamp::TIMESTAMP_NTZ),
    payload VARIANT AS (VALUE:payload::VARIANT)
)
WITH LOCATION = @archive_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = FALSE;

-- Query the external table
SELECT * FROM lake.events_archive
WHERE event_timestamp > '2024-01-01'
LIMIT 100;

-- ----------------------------------------------------------------------------
-- External table with inferred columns
-- ----------------------------------------------------------------------------

-- Let Snowflake detect columns from Parquet schema
CREATE OR REPLACE EXTERNAL TABLE lake.orders_archive
WITH LOCATION = @archive_stage/orders/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = FALSE;

-- Check detected schema
DESC EXTERNAL TABLE lake.orders_archive;

-- ----------------------------------------------------------------------------
-- External table with metadata columns
-- ----------------------------------------------------------------------------

-- Include file-level metadata for debugging and filtering
CREATE OR REPLACE EXTERNAL TABLE lake.logs_archive (
    log_level STRING AS (VALUE:level::STRING),
    message STRING AS (VALUE:message::STRING),
    timestamp TIMESTAMP_NTZ AS (VALUE:timestamp::TIMESTAMP_NTZ),
    -- Metadata columns
    file_name STRING AS METADATA$FILENAME,
    file_row_number INTEGER AS METADATA$FILE_ROW_NUMBER
)
WITH LOCATION = @archive_stage/logs/
FILE_FORMAT = (TYPE = 'JSON')
AUTO_REFRESH = FALSE;

-- Query with file context
SELECT file_name, COUNT(*) as rows
FROM lake.logs_archive
GROUP BY file_name;

-- ----------------------------------------------------------------------------
-- Auto-refresh external table
-- ----------------------------------------------------------------------------

-- Enable automatic metadata refresh when files change
-- Note: Requires event notification (similar to Snowpipe)

CREATE OR REPLACE EXTERNAL TABLE lake.events_live (
    event_id STRING AS (VALUE:event_id::STRING),
    event_type STRING AS (VALUE:event_type::STRING),
    event_timestamp TIMESTAMP_NTZ AS (VALUE:event_timestamp::TIMESTAMP_NTZ)
)
WITH LOCATION = @archive_stage/recent/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE;

-- Check refresh status
SELECT SYSTEM$EXTERNAL_TABLE_PIPE_STATUS('lake.events_live');

-- ----------------------------------------------------------------------------
-- Manual refresh
-- ----------------------------------------------------------------------------

-- Refresh metadata to see new files
ALTER EXTERNAL TABLE lake.events_archive REFRESH;

-- Refresh specific path
ALTER EXTERNAL TABLE lake.events_archive REFRESH '2025/01/';

-- ----------------------------------------------------------------------------
-- External table for CSV data
-- ----------------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL TABLE lake.legacy_csv (
    col1 STRING AS (VALUE:c1::STRING),
    col2 INTEGER AS (VALUE:c2::INTEGER),
    col3 DATE AS (VALUE:c3::DATE)
)
WITH LOCATION = @archive_stage/legacy/
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_DELIMITER = ','
)
AUTO_REFRESH = FALSE;

-- Note: CSV external tables use positional column names (c1, c2, etc.)
-- Map them explicitly in column definitions

-- ----------------------------------------------------------------------------
-- External table for JSON data
-- ----------------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL TABLE lake.api_responses (
    request_id STRING AS (VALUE:request_id::STRING),
    status_code INTEGER AS (VALUE:status_code::INTEGER),
    response_body VARIANT AS (VALUE:body::VARIANT),
    response_time_ms INTEGER AS (VALUE:response_time_ms::INTEGER),
    timestamp TIMESTAMP_NTZ AS (VALUE:timestamp::TIMESTAMP_NTZ)
)
WITH LOCATION = @archive_stage/api/
FILE_FORMAT = (TYPE = 'JSON')
AUTO_REFRESH = FALSE;

-- Query nested JSON
SELECT 
    request_id,
    response_body:user:name::STRING as user_name,
    response_body:user:email::STRING as user_email
FROM lake.api_responses
WHERE status_code = 200;

-- ----------------------------------------------------------------------------
-- Drop and recreate external table
-- ----------------------------------------------------------------------------

-- External table changes require drop and recreate
-- (can't ALTER columns like regular tables)

DROP EXTERNAL TABLE IF EXISTS lake.events_archive;

CREATE EXTERNAL TABLE lake.events_archive (
    -- Updated column definitions
    event_id STRING AS (VALUE:event_id::STRING),
    event_type STRING AS (VALUE:event_type::STRING),
    user_id STRING AS (VALUE:user_id::STRING),
    event_timestamp TIMESTAMP_NTZ AS (VALUE:event_timestamp::TIMESTAMP_NTZ),
    -- New column
    session_id STRING AS (VALUE:session_id::STRING)
)
WITH LOCATION = @archive_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = FALSE;

-- ----------------------------------------------------------------------------
-- Grant access
-- ----------------------------------------------------------------------------

-- External tables need explicit grants like regular tables
GRANT SELECT ON TABLE lake.events_archive TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA lake TO ROLE analyst_role;
