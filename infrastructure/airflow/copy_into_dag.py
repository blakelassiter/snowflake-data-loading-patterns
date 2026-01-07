"""
Airflow DAG: Scheduled COPY INTO Loading

Loads data from cloud storage to Snowflake on a schedule.
Suitable for batch loading where files arrive predictably.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

SNOWFLAKE_CONN_ID = "snowflake_default"
WAREHOUSE = "LOADING_WH"
DATABASE = "RAW"
SCHEMA = "LOADING"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# -----------------------------------------------------------------------------
# DAG: Simple hourly load
# -----------------------------------------------------------------------------

@dag(
    dag_id="copy_into_events_hourly",
    default_args=DEFAULT_ARGS,
    description="Load events from S3 to Snowflake hourly",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake", "loading", "events"],
)
def copy_into_events_hourly():
    """
    Simple COPY INTO pattern for hourly batch loading.
    
    Snowflake tracks loaded files for 64 days, so re-runs are safe.
    """

    copy_events = SnowflakeOperator(
        task_id="copy_events",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        sql="""
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
            PATTERN = '.*[.]json'
            ON_ERROR = 'CONTINUE';
        """,
    )

    check_results = SnowflakeOperator(
        task_id="check_results",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        sql="""
            SELECT 
                status,
                SUM(row_count) as rows_loaded,
                SUM(row_parsed - row_count) as rows_skipped
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1)))
            GROUP BY status;
        """,
    )

    copy_events >> check_results


# -----------------------------------------------------------------------------
# DAG: Multiple tables
# -----------------------------------------------------------------------------

@dag(
    dag_id="copy_into_daily_batch",
    default_args=DEFAULT_ARGS,
    description="Load multiple tables from daily batch files",
    schedule="0 6 * * *",  # 6 AM UTC daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake", "loading", "batch"],
)
def copy_into_daily_batch():
    """
    Load multiple tables from daily batch files.
    
    Uses execution_date to target specific partitions.
    """

    # Load events
    load_events = SnowflakeOperator(
        task_id="load_events",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        sql="""
            COPY INTO raw.events
            FROM @events_stage/{{ ds_nodash }}/
            FILE_FORMAT = (TYPE = 'PARQUET')
            ON_ERROR = 'SKIP_FILE_5%';
        """,
    )

    # Load customers
    load_customers = SnowflakeOperator(
        task_id="load_customers",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        sql="""
            COPY INTO raw.customers
            FROM @customers_stage/{{ ds_nodash }}/
            FILE_FORMAT = (TYPE = 'PARQUET')
            ON_ERROR = 'ABORT_STATEMENT';
        """,
    )

    # Load orders
    load_orders = SnowflakeOperator(
        task_id="load_orders",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        sql="""
            COPY INTO raw.orders
            FROM @orders_stage/{{ ds_nodash }}/
            FILE_FORMAT = (TYPE = 'PARQUET')
            ON_ERROR = 'ABORT_STATEMENT';
        """,
    )

    # Validate load
    validate_counts = SnowflakeOperator(
        task_id="validate_counts",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        sql="""
            SELECT 
                'events' as table_name, COUNT(*) as row_count
            FROM raw.events WHERE DATE(_loaded_at) = '{{ ds }}'
            UNION ALL
            SELECT 'customers', COUNT(*)
            FROM raw.customers WHERE DATE(_loaded_at) = '{{ ds }}'
            UNION ALL
            SELECT 'orders', COUNT(*)
            FROM raw.orders WHERE DATE(_loaded_at) = '{{ ds }}';
        """,
    )

    # Tables can load in parallel
    [load_events, load_customers, load_orders] >> validate_counts


# -----------------------------------------------------------------------------
# DAG: With validation and alerting
# -----------------------------------------------------------------------------

@dag(
    dag_id="copy_into_with_validation",
    default_args=DEFAULT_ARGS,
    description="Load with pre-validation and alerting",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake", "loading", "validation"],
)
def copy_into_with_validation():
    """
    Production pattern with validation before loading.
    """

    @task
    def validate_files():
        """Check if files exist and are valid before loading."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # Check for files
        result = hook.get_first("LIST @events_stage/")
        if not result:
            raise ValueError("No files found in stage")
        
        return True

    @task
    def pre_load_validation():
        """Validate data before committing."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # Run validation mode
        errors = hook.get_records("""
            COPY INTO raw.events
            FROM @events_stage/
            FILE_FORMAT = (TYPE = 'JSON')
            VALIDATION_MODE = 'RETURN_ERRORS'
        """)
        
        if errors and len(errors) > 0:
            # Log errors but don't fail - let ON_ERROR handle it
            print(f"Validation found {len(errors)} errors")
        
        return True

    load_data = SnowflakeOperator(
        task_id="load_data",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        sql="""
            COPY INTO raw.events
            FROM @events_stage/
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE';
        """,
    )

    @task
    def post_load_validation(**context):
        """Validate load results and alert on issues."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        result = hook.get_first("""
            SELECT 
                SUM(row_parsed) as parsed,
                SUM(row_count) as loaded,
                SUM(row_parsed - row_count) as skipped
            FROM TABLE(information_schema.copy_history(
                table_name => 'raw.events',
                start_time => dateadd('hour', -1, current_timestamp())
            ))
        """)
        
        parsed, loaded, skipped = result
        skip_rate = (skipped / parsed * 100) if parsed > 0 else 0
        
        if skip_rate > 5:
            # Could trigger alert here
            raise ValueError(f"High skip rate: {skip_rate:.1f}%")
        
        return {"parsed": parsed, "loaded": loaded, "skipped": skipped}

    validate_files() >> pre_load_validation() >> load_data >> post_load_validation()


# Instantiate DAGs
copy_into_events_hourly()
copy_into_daily_batch()
copy_into_with_validation()
