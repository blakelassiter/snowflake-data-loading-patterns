-- ============================================================================
-- Iceberg Tables: External Catalog Integration
-- ============================================================================
-- Connect Snowflake to externally-managed Iceberg tables where another
-- system (Spark, Flink, AWS Glue) manages the Iceberg metadata.

-- ----------------------------------------------------------------------------
-- AWS Glue Catalog Integration
-- ----------------------------------------------------------------------------

-- Create catalog integration for AWS Glue
CREATE OR REPLACE CATALOG INTEGRATION glue_catalog
    CATALOG_SOURCE = GLUE
    CATALOG_NAMESPACE = 'my_glue_database'
    TABLE_FORMAT = ICEBERG
    GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-glue-role'
    GLUE_CATALOG_ID = '123456789012'
    GLUE_REGION = 'us-east-1'
    ENABLED = TRUE;

-- Verify integration
DESC CATALOG INTEGRATION glue_catalog;

-- ----------------------------------------------------------------------------
-- Create external volume for data access
-- ----------------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL VOLUME glue_data_volume
    STORAGE_LOCATIONS = (
        (
            NAME = 's3_data'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://my-data-lake/iceberg/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-data-role'
        )
    );

-- ----------------------------------------------------------------------------
-- Create Iceberg table from external catalog
-- ----------------------------------------------------------------------------

-- Read-only table linked to Glue-managed Iceberg table
CREATE OR REPLACE ICEBERG TABLE lake.customers_external
    EXTERNAL_VOLUME = 'glue_data_volume'
    CATALOG = 'glue_catalog'
    CATALOG_TABLE_NAME = 'customers';

-- Query the external table
SELECT * FROM lake.customers_external LIMIT 100;

-- ----------------------------------------------------------------------------
-- Polaris Catalog Integration
-- ----------------------------------------------------------------------------

-- Polaris is Snowflake's open-source Iceberg catalog

CREATE OR REPLACE CATALOG INTEGRATION polaris_catalog
    CATALOG_SOURCE = POLARIS
    TABLE_FORMAT = ICEBERG
    CATALOG_NAMESPACE = 'production'
    REST_CONFIG = (
        CATALOG_URI = 'https://my-polaris-server.example.com/api/catalog'
        WAREHOUSE = 'my_warehouse'
    )
    REST_AUTHENTICATION = (
        TYPE = OAUTH
        OAUTH_CLIENT_ID = 'snowflake-client'
        OAUTH_CLIENT_SECRET = 'client-secret'
        OAUTH_TOKEN_URI = 'https://my-polaris-server.example.com/api/catalog/v1/oauth/tokens'
    )
    ENABLED = TRUE;

-- Create table from Polaris catalog
CREATE OR REPLACE ICEBERG TABLE lake.orders_polaris
    EXTERNAL_VOLUME = 'polaris_volume'
    CATALOG = 'polaris_catalog'
    CATALOG_TABLE_NAME = 'orders';

-- ----------------------------------------------------------------------------
-- Object Storage Catalog (no external metastore)
-- ----------------------------------------------------------------------------

-- For Iceberg tables with metadata stored alongside data (no Glue/Polaris)

CREATE OR REPLACE ICEBERG TABLE lake.events_direct
    EXTERNAL_VOLUME = 'data_volume'
    CATALOG = 'OBJECT_STORAGE'
    METADATA_FILE_PATH = 'events/metadata/00001-abc123.metadata.json';

-- Note: OBJECT_STORAGE catalog is read-only from Snowflake

-- ----------------------------------------------------------------------------
-- Refresh external table metadata
-- ----------------------------------------------------------------------------

-- When external systems update the Iceberg table, refresh Snowflake's view
ALTER ICEBERG TABLE lake.customers_external REFRESH;

-- For OBJECT_STORAGE catalog, specify new metadata file
ALTER ICEBERG TABLE lake.events_direct REFRESH
    METADATA_FILE_PATH = 'events/metadata/00002-def456.metadata.json';

-- ----------------------------------------------------------------------------
-- IAM setup for AWS Glue integration
-- ----------------------------------------------------------------------------

/*
Snowflake needs permissions to:
1. Access Glue catalog
2. Read data from S3

Trust policy for Glue role (snowflake-glue-role):
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "<STORAGE_AWS_IAM_USER_ARN from DESC CATALOG INTEGRATION>"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID from DESC CATALOG INTEGRATION>"
                }
            }
        }
    ]
}

Permissions policy for Glue access:
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetTableVersion",
                "glue:GetTableVersions"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:123456789012:catalog",
                "arn:aws:glue:us-east-1:123456789012:database/my_glue_database",
                "arn:aws:glue:us-east-1:123456789012:table/my_glue_database/*"
            ]
        }
    ]
}
*/

-- ----------------------------------------------------------------------------
-- Query external Iceberg table with time travel
-- ----------------------------------------------------------------------------

-- List available snapshots
SELECT * FROM TABLE(lake.customers_external.SNAPSHOTS());

-- Query at specific snapshot
SELECT * FROM lake.customers_external AT(SNAPSHOT => '1234567890123456789');

-- ----------------------------------------------------------------------------
-- Bi-directional access (write back)
-- ----------------------------------------------------------------------------

-- For Snowflake-managed Iceberg tables, you can write via Snowflake
-- and other engines can read the data

-- Create Snowflake-managed table that external engines can access
CREATE OR REPLACE ICEBERG TABLE lake.shared_data (
    id STRING,
    value VARIANT,
    updated_at TIMESTAMP_NTZ
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'shared_volume'
    BASE_LOCATION = 'shared/';

-- Insert data via Snowflake
INSERT INTO lake.shared_data
SELECT 'id1', PARSE_JSON('{"key": "value"}'), CURRENT_TIMESTAMP();

-- External engines (Spark, Trino) can read from:
-- s3://shared-bucket/shared/data/*.parquet
-- Using metadata at: s3://shared-bucket/shared/metadata/

-- ----------------------------------------------------------------------------
-- Limitations of external catalog tables
-- ----------------------------------------------------------------------------

/*
External catalog tables (Glue, Polaris, OBJECT_STORAGE) have limitations:

Read-only in Snowflake:
- Cannot INSERT, UPDATE, DELETE via Snowflake
- Writes must happen via the external system

Refresh required:
- Snowflake caches metadata
- Must REFRESH after external changes

Feature gaps:
- Some Iceberg features may not be fully supported
- Check documentation for current limitations

Use cases:
- Query Spark-managed data lakes from Snowflake
- Federated queries across multiple engines
- Analytics on data owned by other teams/systems
*/
