"""
Airflow DAG: Snowpipe Monitoring

Monitors Snowpipe health and alerts on issues.
Useful when Snowpipe handles ingestion but you need observability.
"""

from datetime import datetime, timedelta
import json
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

SNOWFLAKE_CONN_ID = "snowflake_default"

# Pipes to monitor
PIPES_TO_MONITOR = [
    {"database": "RAW", "schema": "LOADING", "pipe": "EVENTS_PIPE"},
    {"database": "RAW", "schema": "LOADING", "pipe": "CUSTOMERS_PIPE"},
]

# Alerting thresholds
MAX_PENDING_FILES = 1000
MAX_LAG_MINUTES = 30
MIN_FILES_PER_HOUR = 1  # Expect at least this many files

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# -----------------------------------------------------------------------------
# DAG: Snowpipe health check
# -----------------------------------------------------------------------------

@dag(
    dag_id="snowpipe_health_monitor",
    default_args=DEFAULT_ARGS,
    description="Monitor Snowpipe status and alert on issues",
    schedule="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake", "monitoring", "snowpipe"],
)
def snowpipe_health_monitor():
    """
    Monitors Snowpipe health:
    - Pipe execution state (RUNNING, PAUSED, STALLED)
    - Pending file count
    - Load errors
    - Data freshness
    """

    @task
    def check_pipe_status():
        """Check execution state of all monitored pipes."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        issues = []
        statuses = []
        
        for pipe_config in PIPES_TO_MONITOR:
            pipe_name = f"{pipe_config['database']}.{pipe_config['schema']}.{pipe_config['pipe']}"
            
            try:
                result = hook.get_first(f"SELECT SYSTEM$PIPE_STATUS('{pipe_name}')")
                status = json.loads(result[0]) if result else {}
                
                execution_state = status.get("executionState", "UNKNOWN")
                pending_files = status.get("pendingFileCount", 0)
                
                statuses.append({
                    "pipe": pipe_name,
                    "state": execution_state,
                    "pending": pending_files
                })
                
                # Check for issues
                if execution_state == "STALLED":
                    issues.append(f"{pipe_name} is STALLED")
                elif execution_state == "PAUSED":
                    issues.append(f"{pipe_name} is PAUSED")
                
                if pending_files > MAX_PENDING_FILES:
                    issues.append(f"{pipe_name} has {pending_files} pending files (threshold: {MAX_PENDING_FILES})")
                    
            except Exception as e:
                issues.append(f"Failed to check {pipe_name}: {str(e)}")
        
        return {"statuses": statuses, "issues": issues}

    @task
    def check_load_errors():
        """Check for recent load errors."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        issues = []
        
        for pipe_config in PIPES_TO_MONITOR:
            # Get error count from copy history
            result = hook.get_first(f"""
                SELECT 
                    COUNT(*) as error_count,
                    SUM(row_parsed - row_count) as rows_skipped
                FROM TABLE(information_schema.copy_history(
                    table_name => '{pipe_config['database']}.{pipe_config['schema']}.EVENTS',
                    start_time => dateadd('hour', -1, current_timestamp())
                ))
                WHERE status = 'LOAD_FAILED' OR row_parsed > row_count
            """)
            
            error_count, rows_skipped = result if result else (0, 0)
            
            if error_count and error_count > 0:
                issues.append(f"{pipe_config['pipe']}: {error_count} failed loads in last hour")
            
            if rows_skipped and rows_skipped > 100:
                issues.append(f"{pipe_config['pipe']}: {rows_skipped} rows skipped in last hour")
        
        return {"issues": issues}

    @task
    def check_data_freshness():
        """Check if data is arriving as expected."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        issues = []
        
        # Check events table freshness
        result = hook.get_first("""
            SELECT 
                MAX(_loaded_at) as latest_load,
                DATEDIFF('minute', MAX(_loaded_at), CURRENT_TIMESTAMP()) as minutes_since_load
            FROM raw.events
        """)
        
        if result:
            latest_load, minutes_behind = result
            
            if minutes_behind and minutes_behind > MAX_LAG_MINUTES:
                issues.append(f"Events table is {minutes_behind} minutes behind (threshold: {MAX_LAG_MINUTES})")
        
        # Check file arrival rate
        result = hook.get_first("""
            SELECT COUNT(DISTINCT file_name) as files_loaded
            FROM TABLE(information_schema.copy_history(
                table_name => 'raw.events',
                start_time => dateadd('hour', -1, current_timestamp())
            ))
        """)
        
        if result:
            files_loaded = result[0] or 0
            
            if files_loaded < MIN_FILES_PER_HOUR:
                issues.append(f"Only {files_loaded} files loaded in last hour (expected: {MIN_FILES_PER_HOUR}+)")
        
        return {"issues": issues}

    @task
    def aggregate_and_alert(pipe_status, load_errors, freshness):
        """Combine all checks and alert if needed."""
        all_issues = (
            pipe_status.get("issues", []) + 
            load_errors.get("issues", []) + 
            freshness.get("issues", [])
        )
        
        if all_issues:
            # Log issues
            print("=" * 50)
            print("SNOWPIPE HEALTH ISSUES DETECTED")
            print("=" * 50)
            for issue in all_issues:
                print(f"  - {issue}")
            print("=" * 50)
            
            # Could trigger external alert here:
            # - Send to Slack
            # - Create PagerDuty incident
            # - Send email
            
            # For now, fail the task to trigger Airflow alerting
            raise ValueError(f"Snowpipe health issues: {len(all_issues)} problems detected")
        
        print("All Snowpipe health checks passed")
        return {"status": "healthy", "pipes_checked": len(PIPES_TO_MONITOR)}

    @task
    def record_metrics(pipe_status):
        """Record metrics for trending."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        for status in pipe_status.get("statuses", []):
            hook.run(f"""
                INSERT INTO ops.snowpipe_metrics (
                    check_time, pipe_name, execution_state, pending_files
                )
                VALUES (
                    CURRENT_TIMESTAMP(),
                    '{status["pipe"]}',
                    '{status["state"]}',
                    {status["pending"]}
                )
            """)

    # DAG flow
    pipe_status = check_pipe_status()
    load_errors = check_load_errors()
    freshness = check_data_freshness()
    
    aggregate_and_alert(pipe_status, load_errors, freshness)
    record_metrics(pipe_status)


# -----------------------------------------------------------------------------
# DAG: Snowpipe recovery
# -----------------------------------------------------------------------------

@dag(
    dag_id="snowpipe_recovery",
    default_args=DEFAULT_ARGS,
    description="Refresh Snowpipe to recover missed files",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake", "recovery", "snowpipe"],
)
def snowpipe_recovery():
    """
    Manual recovery DAG for refreshing Snowpipe.
    
    Use when:
    - Pipe was paused and needs to catch up
    - Event notifications were missed
    - Manual backfill needed
    """

    @task
    def refresh_pipes():
        """Refresh all monitored pipes."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        results = []
        
        for pipe_config in PIPES_TO_MONITOR:
            pipe_name = f"{pipe_config['database']}.{pipe_config['schema']}.{pipe_config['pipe']}"
            
            try:
                # Refresh pipe
                hook.run(f"ALTER PIPE {pipe_name} REFRESH")
                
                # Check status after refresh
                result = hook.get_first(f"SELECT SYSTEM$PIPE_STATUS('{pipe_name}')")
                status = json.loads(result[0]) if result else {}
                
                results.append({
                    "pipe": pipe_name,
                    "refreshed": True,
                    "pending_after": status.get("pendingFileCount", 0)
                })
                
            except Exception as e:
                results.append({
                    "pipe": pipe_name,
                    "refreshed": False,
                    "error": str(e)
                })
        
        return results

    @task
    def verify_recovery(refresh_results):
        """Wait and verify files are being processed."""
        import time
        
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # Wait for processing
        time.sleep(120)  # 2 minutes
        
        for result in refresh_results:
            if result.get("refreshed"):
                pipe_name = result["pipe"]
                
                status_result = hook.get_first(f"SELECT SYSTEM$PIPE_STATUS('{pipe_name}')")
                status = json.loads(status_result[0]) if status_result else {}
                
                pending_before = result.get("pending_after", 0)
                pending_after = status.get("pendingFileCount", 0)
                
                print(f"{pipe_name}: {pending_before} -> {pending_after} pending files")

    refresh_results = refresh_pipes()
    verify_recovery(refresh_results)


# Instantiate DAGs
snowpipe_health_monitor()
snowpipe_recovery()
