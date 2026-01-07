"""
Airflow DAG: Incremental Loading with MERGE

Loads new/updated records using staging table and MERGE pattern.
Handles duplicates and updates safely.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

SNOWFLAKE_CONN_ID = "snowflake_default"
WAREHOUSE = "LOADING_WH"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# -----------------------------------------------------------------------------
# DAG: Incremental customer load
# -----------------------------------------------------------------------------

@dag(
    dag_id="incremental_customers_load",
    default_args=DEFAULT_ARGS,
    description="Load customer updates with MERGE pattern",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake", "loading", "incremental", "customers"],
)
def incremental_customers_load():
    """
    Incremental load pattern:
    1. Load new files to staging table
    2. Deduplicate within staging
    3. MERGE to target table
    4. Log results
    """

    # Step 1: Clear and load staging
    load_staging = SnowflakeOperator(
        task_id="load_staging",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        sql="""
            -- Clear staging table
            TRUNCATE TABLE raw.customers_staging;
            
            -- Load new files
            COPY INTO raw.customers_staging
            FROM (
                SELECT 
                    $1:customer_id::STRING as customer_id,
                    $1:customer_name::STRING as customer_name,
                    $1:email::STRING as email,
                    $1:status::STRING as status,
                    $1:updated_at::TIMESTAMP_NTZ as updated_at,
                    METADATA$FILENAME as _source_file
                FROM @customers_stage/
            )
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE';
        """,
    )

    # Step 2: Check staging results
    @task
    def check_staging():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        result = hook.get_first("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM raw.customers_staging
        """)
        
        total, unique = result
        print(f"Staged {total} rows, {unique} unique customers")
        
        if total == 0:
            return {"rows": 0, "skip_merge": True}
        
        return {"rows": total, "unique": unique, "skip_merge": False}

    # Step 3: Merge to target
    merge_to_target = SnowflakeOperator(
        task_id="merge_to_target",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        sql="""
            MERGE INTO raw.customers t
            USING (
                -- Deduplicate staging: keep latest update per customer
                SELECT * FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY customer_id 
                            ORDER BY updated_at DESC
                        ) as rn
                    FROM raw.customers_staging
                )
                WHERE rn = 1
            ) s
            ON t.customer_id = s.customer_id
            
            -- Update if staged record is newer
            WHEN MATCHED AND s.updated_at > t.updated_at THEN
                UPDATE SET 
                    customer_name = s.customer_name,
                    email = s.email,
                    status = s.status,
                    updated_at = s.updated_at,
                    _loaded_at = CURRENT_TIMESTAMP(),
                    _source_file = s._source_file
            
            -- Insert if new customer
            WHEN NOT MATCHED THEN
                INSERT (customer_id, customer_name, email, status, updated_at, _loaded_at, _source_file)
                VALUES (s.customer_id, s.customer_name, s.email, s.status, s.updated_at, CURRENT_TIMESTAMP(), s._source_file);
        """,
    )

    # Step 4: Log results
    @task
    def log_merge_results(**context):
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # Get merge statistics from result scan
        result = hook.get_first("""
            SELECT 
                "number of rows inserted",
                "number of rows updated"
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        """)
        
        inserted, updated = result if result else (0, 0)
        
        # Log to audit table
        hook.run(f"""
            INSERT INTO ops.load_audit (
                dag_id, execution_date, table_name, 
                rows_inserted, rows_updated, completed_at
            )
            VALUES (
                'incremental_customers_load',
                '{{{{ ds }}}}',
                'raw.customers',
                {inserted},
                {updated},
                CURRENT_TIMESTAMP()
            )
        """)
        
        print(f"MERGE complete: {inserted} inserted, {updated} updated")
        return {"inserted": inserted, "updated": updated}

    # DAG flow
    staging_result = check_staging()
    load_staging >> staging_result >> merge_to_target >> log_merge_results()


# -----------------------------------------------------------------------------
# DAG: Incremental orders with soft deletes
# -----------------------------------------------------------------------------

@dag(
    dag_id="incremental_orders_with_deletes",
    default_args=DEFAULT_ARGS,
    description="Load orders including soft deletes",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake", "loading", "incremental", "orders"],
)
def incremental_orders_with_deletes():
    """
    Incremental load handling inserts, updates, and deletes.
    
    Source sends:
    - operation: 'I' (insert), 'U' (update), 'D' (delete)
    """

    load_staging = SnowflakeOperator(
        task_id="load_staging",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        sql="""
            TRUNCATE TABLE raw.orders_staging;
            
            COPY INTO raw.orders_staging
            FROM @orders_stage/
            FILE_FORMAT = (TYPE = 'JSON');
        """,
    )

    process_changes = SnowflakeOperator(
        task_id="process_changes",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        sql="""
            -- Process changes in order
            MERGE INTO raw.orders t
            USING (
                SELECT * FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY order_id 
                            ORDER BY change_timestamp DESC
                        ) as rn
                    FROM raw.orders_staging
                )
                WHERE rn = 1
            ) s
            ON t.order_id = s.order_id
            
            -- Delete
            WHEN MATCHED AND s.operation = 'D' THEN
                DELETE
            
            -- Update
            WHEN MATCHED AND s.operation IN ('I', 'U') 
                 AND s.change_timestamp > t.updated_at THEN
                UPDATE SET 
                    customer_id = s.customer_id,
                    order_total = s.order_total,
                    status = s.status,
                    updated_at = s.change_timestamp,
                    _loaded_at = CURRENT_TIMESTAMP()
            
            -- Insert
            WHEN NOT MATCHED AND s.operation != 'D' THEN
                INSERT (order_id, customer_id, order_total, status, updated_at, _loaded_at)
                VALUES (s.order_id, s.customer_id, s.order_total, s.status, s.change_timestamp, CURRENT_TIMESTAMP());
        """,
    )

    validate_referential_integrity = SnowflakeOperator(
        task_id="validate_referential_integrity",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        sql="""
            -- Check for orphan orders (customer doesn't exist)
            SELECT COUNT(*) as orphan_orders
            FROM raw.orders o
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.customers c 
                WHERE c.customer_id = o.customer_id
            )
            AND o._loaded_at > DATEADD('hour', -1, CURRENT_TIMESTAMP());
        """,
    )

    load_staging >> process_changes >> validate_referential_integrity


# Instantiate DAGs
incremental_customers_load()
incremental_orders_with_deletes()
