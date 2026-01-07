"""
Dagster Assets: Snowflake Data Loading

Software-defined assets for loading data into Snowflake.
"""

from dagster import (
    asset,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MetadataValue,
    Output,
    AssetIn,
)
from dagster_snowflake import SnowflakeResource
from datetime import datetime


# -----------------------------------------------------------------------------
# Simple COPY INTO asset
# -----------------------------------------------------------------------------

@asset(
    group_name="raw",
    description="Events loaded from S3 via COPY INTO",
    compute_kind="snowflake",
)
def raw_events(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """
    Load events from cloud storage stage.
    
    Snowflake tracks loaded files for 64 days, making this idempotent.
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        
        # Execute COPY INTO
        cursor.execute("""
            COPY INTO raw.events
            FROM (
                SELECT 
                    $1:event_id::STRING,
                    $1:event_type::STRING,
                    $1:event_timestamp::TIMESTAMP_NTZ,
                    $1:user_id::STRING,
                    $1:payload::VARIANT,
                    CURRENT_TIMESTAMP() as _loaded_at,
                    METADATA$FILENAME as _source_file
                FROM @events_stage/
            )
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE'
        """)
        
        # Get load statistics
        cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
        results = cursor.fetchall()
        
        total_rows = sum(r[3] for r in results) if results else 0  # row_count column
        total_files = len(results)
        
        context.log.info(f"Loaded {total_rows} rows from {total_files} files")
        
        return Output(
            value={"rows_loaded": total_rows, "files_processed": total_files},
            metadata={
                "rows_loaded": MetadataValue.int(total_rows),
                "files_processed": MetadataValue.int(total_files),
            }
        )


# -----------------------------------------------------------------------------
# Partitioned daily asset
# -----------------------------------------------------------------------------

@asset(
    group_name="raw",
    description="Daily events partitioned by date",
    partitions_def=DailyPartitionsDefinition(start_date="2025-01-01"),
    compute_kind="snowflake",
)
def raw_events_daily(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """
    Load events for a specific date partition.
    
    Files expected at: @events_stage/YYYY/MM/DD/
    """
    partition_date = context.partition_key
    date_parts = partition_date.split("-")
    year, month, day = date_parts[0], date_parts[1], date_parts[2]
    
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        
        # Load specific partition
        cursor.execute(f"""
            COPY INTO raw.events
            FROM @events_stage/{year}/{month}/{day}/
            FILE_FORMAT = (TYPE = 'PARQUET')
            ON_ERROR = 'SKIP_FILE_5%'
        """)
        
        cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
        results = cursor.fetchall()
        
        total_rows = sum(r[3] for r in results) if results else 0
        
        context.log.info(f"Loaded {total_rows} rows for {partition_date}")
        
        return Output(
            value=total_rows,
            metadata={
                "partition_date": MetadataValue.text(partition_date),
                "rows_loaded": MetadataValue.int(total_rows),
            }
        )


# -----------------------------------------------------------------------------
# Incremental load with MERGE
# -----------------------------------------------------------------------------

@asset(
    group_name="raw",
    description="Customers with incremental MERGE pattern",
    compute_kind="snowflake",
)
def raw_customers(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """
    Incremental customer load using staging table and MERGE.
    
    Handles inserts and updates safely.
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        
        # Step 1: Load to staging
        cursor.execute("TRUNCATE TABLE raw.customers_staging")
        
        cursor.execute("""
            COPY INTO raw.customers_staging
            FROM @customers_stage/
            FILE_FORMAT = (TYPE = 'PARQUET')
        """)
        
        cursor.execute("SELECT COUNT(*) FROM raw.customers_staging")
        staged_count = cursor.fetchone()[0]
        
        if staged_count == 0:
            context.log.info("No new data to load")
            return Output(
                value={"staged": 0, "inserted": 0, "updated": 0},
                metadata={"staged_rows": MetadataValue.int(0)}
            )
        
        # Step 2: MERGE to target
        cursor.execute("""
            MERGE INTO raw.customers t
            USING (
                SELECT * FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) as rn
                    FROM raw.customers_staging
                )
                WHERE rn = 1
            ) s
            ON t.customer_id = s.customer_id
            WHEN MATCHED AND s.updated_at > t.updated_at THEN
                UPDATE SET 
                    customer_name = s.customer_name,
                    email = s.email,
                    status = s.status,
                    updated_at = s.updated_at,
                    _loaded_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (customer_id, customer_name, email, status, updated_at, _loaded_at)
                VALUES (s.customer_id, s.customer_name, s.email, s.status, s.updated_at, CURRENT_TIMESTAMP())
        """)
        
        # Get merge statistics
        cursor.execute("""
            SELECT "number of rows inserted", "number of rows updated"
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        """)
        result = cursor.fetchone()
        inserted, updated = result if result else (0, 0)
        
        context.log.info(f"MERGE complete: {inserted} inserted, {updated} updated")
        
        return Output(
            value={"staged": staged_count, "inserted": inserted, "updated": updated},
            metadata={
                "staged_rows": MetadataValue.int(staged_count),
                "rows_inserted": MetadataValue.int(inserted),
                "rows_updated": MetadataValue.int(updated),
            }
        )


# -----------------------------------------------------------------------------
# Dependent asset (downstream of raw)
# -----------------------------------------------------------------------------

@asset(
    group_name="staging",
    description="Cleaned events for analytics",
    ins={"raw_events": AssetIn(key="raw_events")},
    compute_kind="snowflake",
)
def stg_events(context: AssetExecutionContext, snowflake: SnowflakeResource, raw_events):
    """
    Transform raw events into staging layer.
    
    This creates a dependency on raw_events, ensuring load before transform.
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE OR REPLACE TABLE staging.events AS
            SELECT 
                event_id,
                event_type,
                event_timestamp,
                user_id,
                payload:page_url::STRING as page_url,
                payload:referrer::STRING as referrer,
                _loaded_at,
                _source_file
            FROM raw.events
            WHERE event_timestamp >= DATEADD('day', -90, CURRENT_DATE())
        """)
        
        cursor.execute("SELECT COUNT(*) FROM staging.events")
        row_count = cursor.fetchone()[0]
        
        return Output(
            value=row_count,
            metadata={"row_count": MetadataValue.int(row_count)}
        )


# -----------------------------------------------------------------------------
# Sensor-triggered asset
# -----------------------------------------------------------------------------

@asset(
    group_name="raw",
    description="Orders loaded when files arrive",
    compute_kind="snowflake",
)
def raw_orders(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """
    Load orders from stage.
    
    Typically triggered by a sensor watching for new files.
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            COPY INTO raw.orders
            FROM @orders_stage/
            FILE_FORMAT = (TYPE = 'PARQUET')
            ON_ERROR = 'ABORT_STATEMENT'
        """)
        
        cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
        results = cursor.fetchall()
        
        total_rows = sum(r[3] for r in results) if results else 0
        
        return Output(
            value=total_rows,
            metadata={"rows_loaded": MetadataValue.int(total_rows)}
        )
