"""
Dagster Resources: Snowflake and External Connections

Configurable resources for Snowflake access and related services.
"""

from dagster import EnvVar
from dagster_snowflake import SnowflakeResource


# -----------------------------------------------------------------------------
# Snowflake Resource
# -----------------------------------------------------------------------------

snowflake_resource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    schema=EnvVar("SNOWFLAKE_SCHEMA"),
    role=EnvVar("SNOWFLAKE_ROLE"),
)


# -----------------------------------------------------------------------------
# Environment-specific configurations
# -----------------------------------------------------------------------------

def get_snowflake_resource(environment: str = "dev") -> SnowflakeResource:
    """
    Get Snowflake resource configured for specific environment.
    
    Args:
        environment: One of 'dev', 'staging', 'prod'
    
    Returns:
        Configured SnowflakeResource
    """
    warehouse_map = {
        "dev": "DEV_LOADING_WH",
        "staging": "STAGING_LOADING_WH",
        "prod": "PROD_LOADING_WH",
    }
    
    database_map = {
        "dev": "DEV_RAW",
        "staging": "STAGING_RAW",
        "prod": "RAW",
    }
    
    return SnowflakeResource(
        account=EnvVar("SNOWFLAKE_ACCOUNT"),
        user=EnvVar("SNOWFLAKE_USER"),
        password=EnvVar("SNOWFLAKE_PASSWORD"),
        warehouse=warehouse_map.get(environment, "LOADING_WH"),
        database=database_map.get(environment, "RAW"),
        schema="LOADING",
        role=EnvVar("SNOWFLAKE_ROLE"),
    )


# -----------------------------------------------------------------------------
# Resource with key-pair authentication
# -----------------------------------------------------------------------------

def get_snowflake_keypair_resource() -> SnowflakeResource:
    """
    Snowflake resource using key-pair authentication.
    
    More secure than password for production and automation.
    """
    return SnowflakeResource(
        account=EnvVar("SNOWFLAKE_ACCOUNT"),
        user=EnvVar("SNOWFLAKE_USER"),
        private_key=EnvVar("SNOWFLAKE_PRIVATE_KEY"),
        private_key_password=EnvVar("SNOWFLAKE_PRIVATE_KEY_PASSWORD"),
        warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
        database=EnvVar("SNOWFLAKE_DATABASE"),
        schema=EnvVar("SNOWFLAKE_SCHEMA"),
        role=EnvVar("SNOWFLAKE_ROLE"),
    )


# -----------------------------------------------------------------------------
# Multiple warehouses for different workloads
# -----------------------------------------------------------------------------

# Small warehouse for metadata operations
snowflake_ops = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    warehouse="OPS_WH",
    database=EnvVar("SNOWFLAKE_DATABASE"),
    schema=EnvVar("SNOWFLAKE_SCHEMA"),
    role=EnvVar("SNOWFLAKE_ROLE"),
)

# Large warehouse for heavy loads
snowflake_loading = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    warehouse="LOADING_WH",
    database=EnvVar("SNOWFLAKE_DATABASE"),
    schema=EnvVar("SNOWFLAKE_SCHEMA"),
    role=EnvVar("SNOWFLAKE_ROLE"),
)
