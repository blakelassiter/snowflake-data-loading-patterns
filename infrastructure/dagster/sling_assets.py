"""
Dagster Assets: Sling-based EL Pipelines

Uses Sling for simplified data movement from various sources to Snowflake.
Sling handles schema inference, incremental loading, and type mapping.
"""

from dagster import (
    AssetExecutionContext,
    EnvVar,
    Definitions,
)
from dagster_embedded_elt.sling import (
    SlingResource,
    SlingConnectionResource,
    sling_assets,
    SlingReplicationConfig,
    SlingMode,
)


# -----------------------------------------------------------------------------
# Sling Connections
# -----------------------------------------------------------------------------

# S3 source connection
s3_connection = SlingConnectionResource(
    name="S3_DATA_LAKE",
    type="s3",
    bucket=EnvVar("S3_BUCKET"),
    access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
    secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
)

# Snowflake target connection
snowflake_connection = SlingConnectionResource(
    name="SNOWFLAKE",
    type="snowflake",
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    schema=EnvVar("SNOWFLAKE_SCHEMA"),
)

# PostgreSQL source (for CDC example)
postgres_connection = SlingConnectionResource(
    name="POSTGRES_PROD",
    type="postgres",
    host=EnvVar("POSTGRES_HOST"),
    port=5432,
    user=EnvVar("POSTGRES_USER"),
    password=EnvVar("POSTGRES_PASSWORD"),
    database=EnvVar("POSTGRES_DATABASE"),
)

# Sling resource combining connections
sling_resource = SlingResource(
    connections=[s3_connection, snowflake_connection, postgres_connection]
)


# -----------------------------------------------------------------------------
# S3 to Snowflake: Full refresh
# -----------------------------------------------------------------------------

@sling_assets(
    replication_config=SlingReplicationConfig(
        source="S3_DATA_LAKE",
        target="SNOWFLAKE",
        defaults={
            "mode": "full-refresh",
            "object": "{stream_schema}.{stream_table}",
        },
        streams={
            "s3://data-lake/reference/countries.parquet": {
                "object": "raw.countries",
                "primary_key": "country_code",
            },
            "s3://data-lake/reference/currencies.parquet": {
                "object": "raw.currencies",
                "primary_key": "currency_code",
            },
        },
    ),
    name="reference_data_from_s3",
)
def reference_data_from_s3(context: AssetExecutionContext, sling: SlingResource):
    """
    Load reference data from S3 with full refresh.
    
    Small, slowly-changing tables that are replaced entirely each run.
    """
    yield from sling.replicate(context=context)


# -----------------------------------------------------------------------------
# S3 to Snowflake: Incremental append
# -----------------------------------------------------------------------------

@sling_assets(
    replication_config=SlingReplicationConfig(
        source="S3_DATA_LAKE",
        target="SNOWFLAKE",
        defaults={
            "mode": "incremental",
            "object": "{stream_schema}.{stream_table}",
        },
        streams={
            "s3://data-lake/events/": {
                "object": "raw.events",
                "primary_key": "event_id",
                "update_key": "event_timestamp",  # Watermark for incremental
            },
        },
    ),
    name="events_from_s3",
)
def events_from_s3(context: AssetExecutionContext, sling: SlingResource):
    """
    Incrementally load events from S3.
    
    Sling tracks the last loaded timestamp and only processes new files.
    """
    yield from sling.replicate(context=context)


# -----------------------------------------------------------------------------
# PostgreSQL to Snowflake: Incremental sync
# -----------------------------------------------------------------------------

@sling_assets(
    replication_config=SlingReplicationConfig(
        source="POSTGRES_PROD",
        target="SNOWFLAKE",
        defaults={
            "mode": "incremental",
            "object": "raw.{stream_table}",
        },
        streams={
            "public.customers": {
                "primary_key": "customer_id",
                "update_key": "updated_at",
            },
            "public.orders": {
                "primary_key": "order_id",
                "update_key": "updated_at",
            },
            "public.products": {
                "primary_key": "product_id",
                "update_key": "updated_at",
            },
        },
    ),
    name="postgres_sync",
)
def postgres_sync(context: AssetExecutionContext, sling: SlingResource):
    """
    Sync tables from PostgreSQL to Snowflake incrementally.
    
    Uses updated_at column to detect changed records.
    """
    yield from sling.replicate(context=context)


# -----------------------------------------------------------------------------
# PostgreSQL to Snowflake: Snapshot (full refresh)
# -----------------------------------------------------------------------------

@sling_assets(
    replication_config=SlingReplicationConfig(
        source="POSTGRES_PROD",
        target="SNOWFLAKE",
        defaults={
            "mode": "snapshot",  # Full refresh with timestamp
            "object": "raw.{stream_table}_snapshot",
        },
        streams={
            "public.inventory": {
                "primary_key": "sku",
            },
            "public.pricing": {
                "primary_key": "product_id",
            },
        },
    ),
    name="postgres_snapshots",
)
def postgres_snapshots(context: AssetExecutionContext, sling: SlingResource):
    """
    Snapshot tables from PostgreSQL.
    
    Creates timestamped snapshots for slowly-changing dimension tracking.
    """
    yield from sling.replicate(context=context)


# -----------------------------------------------------------------------------
# Custom SQL extraction
# -----------------------------------------------------------------------------

@sling_assets(
    replication_config=SlingReplicationConfig(
        source="POSTGRES_PROD",
        target="SNOWFLAKE",
        defaults={
            "mode": "full-refresh",
        },
        streams={
            # Custom SQL query as source
            "sql: SELECT customer_id, email, created_at FROM customers WHERE status = 'active'": {
                "object": "raw.active_customers",
                "primary_key": "customer_id",
            },
            # Aggregated query
            "sql: SELECT DATE(created_at) as order_date, COUNT(*) as order_count, SUM(total) as revenue FROM orders GROUP BY 1": {
                "object": "raw.daily_order_summary",
                "primary_key": "order_date",
            },
        },
    ),
    name="custom_extracts",
)
def custom_extracts(context: AssetExecutionContext, sling: SlingResource):
    """
    Custom SQL extractions to Snowflake.
    
    Useful for:
    - Filtered extracts
    - Aggregations
    - Joins across source tables
    """
    yield from sling.replicate(context=context)


# -----------------------------------------------------------------------------
# File patterns with wildcards
# -----------------------------------------------------------------------------

@sling_assets(
    replication_config=SlingReplicationConfig(
        source="S3_DATA_LAKE",
        target="SNOWFLAKE",
        defaults={
            "mode": "incremental",
        },
        streams={
            # Wildcard pattern for partitioned data
            "s3://data-lake/logs/{date}/*.json": {
                "object": "raw.application_logs",
                "primary_key": "log_id",
                "update_key": "timestamp",
            },
        },
    ),
    name="logs_from_s3",
)
def logs_from_s3(context: AssetExecutionContext, sling: SlingResource):
    """
    Load logs from date-partitioned S3 paths.
    
    Sling handles the wildcard expansion and incremental tracking.
    """
    yield from sling.replicate(context=context)


# -----------------------------------------------------------------------------
# Replication config from YAML file
# -----------------------------------------------------------------------------

# For complex configurations, you can load from a YAML file:
#
# @sling_assets(
#     replication_config=SlingReplicationConfig.from_file("sling_replication.yaml"),
#     name="yaml_configured_assets",
# )
# def yaml_configured_assets(context: AssetExecutionContext, sling: SlingResource):
#     yield from sling.replicate(context=context)
#
# Example sling_replication.yaml:
# source: POSTGRES_PROD
# target: SNOWFLAKE
# defaults:
#   mode: incremental
# streams:
#   public.customers:
#     object: raw.customers
#     primary_key: customer_id
#     update_key: updated_at
