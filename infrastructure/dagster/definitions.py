"""
Dagster Definitions: Entry Point

Combines all assets, resources, schedules, and sensors into a single Definitions object.
This is the main entry point for Dagster.
"""

from dagster import (
    Definitions,
    ScheduleDefinition,
    DefaultScheduleStatus,
    AssetSelection,
    define_asset_job,
    sensor,
    RunRequest,
    SensorEvaluationContext,
)
from dagster_snowflake import SnowflakeResource

# Import assets
from assets import (
    raw_events,
    raw_events_daily,
    raw_customers,
    raw_orders,
    stg_events,
)

from sling_assets import (
    reference_data_from_s3,
    events_from_s3,
    postgres_sync,
    postgres_snapshots,
    custom_extracts,
    logs_from_s3,
    sling_resource,
)

from resources import snowflake_resource


# -----------------------------------------------------------------------------
# Jobs
# -----------------------------------------------------------------------------

# Job for all raw layer assets
raw_layer_job = define_asset_job(
    name="raw_layer_job",
    selection=AssetSelection.groups("raw"),
    description="Load all raw layer assets",
)

# Job for specific assets
events_job = define_asset_job(
    name="events_job",
    selection=AssetSelection.assets(raw_events),
    description="Load events only",
)

# Job for incremental loads
incremental_job = define_asset_job(
    name="incremental_job",
    selection=AssetSelection.assets(raw_customers, postgres_sync),
    description="Run incremental loads",
)


# -----------------------------------------------------------------------------
# Schedules
# -----------------------------------------------------------------------------

# Hourly events load
hourly_events_schedule = ScheduleDefinition(
    job=events_job,
    cron_schedule="0 * * * *",  # Every hour
    default_status=DefaultScheduleStatus.RUNNING,
)

# Daily reference data refresh
daily_reference_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="reference_data_job",
        selection=AssetSelection.assets(reference_data_from_s3),
    ),
    cron_schedule="0 6 * * *",  # 6 AM daily
    default_status=DefaultScheduleStatus.RUNNING,
)

# Frequent incremental sync
incremental_schedule = ScheduleDefinition(
    job=incremental_job,
    cron_schedule="*/15 * * * *",  # Every 15 minutes
    default_status=DefaultScheduleStatus.RUNNING,
)


# -----------------------------------------------------------------------------
# Sensors
# -----------------------------------------------------------------------------

@sensor(
    job=define_asset_job(
        name="orders_job",
        selection=AssetSelection.assets(raw_orders),
    ),
    minimum_interval_seconds=60,
)
def orders_file_sensor(context: SensorEvaluationContext, snowflake: SnowflakeResource):
    """
    Sensor that triggers orders load when new files appear.
    
    Checks the stage for unprocessed files.
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        
        # Check for files in stage not yet loaded
        cursor.execute("""
            SELECT COUNT(*) as new_files
            FROM (
                SELECT "name" FROM TABLE(RESULT_SCAN(
                    (SELECT LAST_QUERY_ID() FROM TABLE(RESULT_SCAN(SYSTEM$LAST_QUERY_ID())) WHERE 1=0)
                ))
            )
            -- This is simplified; in practice you'd track loaded files
        """)
        
        # For demo purposes, use a simpler check
        cursor.execute("LIST @orders_stage/")
        files = cursor.fetchall()
        
        if files and len(files) > 0:
            # Could add more sophisticated logic here to check
            # if files are actually new (not yet loaded)
            yield RunRequest(
                run_key=f"orders_{context.cursor}",
                run_config={},
            )


@sensor(
    job=raw_layer_job,
    minimum_interval_seconds=300,  # 5 minutes
)
def snowpipe_backfill_sensor(context: SensorEvaluationContext, snowflake: SnowflakeResource):
    """
    Sensor that triggers backfill if Snowpipe falls behind.
    
    Monitors pipe status and triggers COPY INTO if needed.
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT PARSE_JSON(SYSTEM$PIPE_STATUS('raw.events_pipe')):pendingFileCount::INTEGER
        """)
        result = cursor.fetchone()
        pending = result[0] if result else 0
        
        # If too many files pending, trigger direct load
        if pending > 500:
            context.log.info(f"Snowpipe backlogged with {pending} files, triggering direct load")
            yield RunRequest(
                run_key=f"backfill_{context.cursor}",
                run_config={},
            )


# -----------------------------------------------------------------------------
# Definitions
# -----------------------------------------------------------------------------

defs = Definitions(
    assets=[
        # Direct Snowflake assets
        raw_events,
        raw_events_daily,
        raw_customers,
        raw_orders,
        stg_events,
        # Sling-based assets
        reference_data_from_s3,
        events_from_s3,
        postgres_sync,
        postgres_snapshots,
        custom_extracts,
        logs_from_s3,
    ],
    resources={
        "snowflake": snowflake_resource,
        "sling": sling_resource,
    },
    schedules=[
        hourly_events_schedule,
        daily_reference_schedule,
        incremental_schedule,
    ],
    sensors=[
        orders_file_sensor,
        snowpipe_backfill_sensor,
    ],
    jobs=[
        raw_layer_job,
        events_job,
        incremental_job,
    ],
)
