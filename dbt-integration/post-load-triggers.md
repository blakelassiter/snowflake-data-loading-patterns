# Post-Load Triggers

Patterns for triggering dbt runs after data loading completes.

## Orchestrator Pattern (Airflow Example)

The orchestrator handles both loading and transformation:

```python
from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

@dag(schedule="@hourly")
def load_and_transform():
    
    # Step 1: Load data
    load_events = SnowflakeOperator(
        task_id="load_events",
        sql="""
            COPY INTO raw.events
            FROM @events_stage/
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE';
        """,
    )
    
    # Step 2: Verify load succeeded
    @task
    def check_load_results(**context):
        ti = context["ti"]
        # Pull result from previous task
        # Fail if no rows loaded
        return True
    
    # Step 3: Trigger dbt Cloud job
    trigger_dbt = SimpleHttpOperator(
        task_id="trigger_dbt",
        http_conn_id="dbt_cloud",
        endpoint="/api/v2/accounts/{account_id}/jobs/{job_id}/run/",
        method="POST",
        headers={"Authorization": "Token {{ var.value.dbt_cloud_token }}"},
        data=json.dumps({"cause": "Airflow load complete"}),
    )
    
    # Step 4: Wait for dbt completion (optional)
    # Use DbtCloudRunJobOperator from airflow-dbt-cloud provider
    
    load_events >> check_load_results() >> trigger_dbt
```

## Webhook Pattern (Fivetran + dbt Cloud)

Configure Fivetran to call dbt Cloud API on sync completion:

### 1. Create webhook handler

See [fivetran_webhook.py](../infrastructure/integrations/fivetran_webhook.py) for full implementation.

```python
def handle_sync_end(event: dict):
    """Trigger dbt when Fivetran sync completes."""
    
    if event["data"]["status"] == "SUCCESSFUL":
        # Call dbt Cloud API
        response = requests.post(
            f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/",
            headers={"Authorization": f"Token {DBT_API_KEY}"},
            json={"cause": f"Fivetran: {event['connector_name']}"}
        )
        return response.json()
```

### 2. Configure Fivetran webhook

In Fivetran dashboard:
1. Go to connector settings
2. Add webhook URL pointing to your handler
3. Select events: `sync_end`

### 3. Map connectors to dbt jobs

Different sources may need different dbt jobs:

```python
CONNECTOR_TO_JOB = {
    "postgres_prod": "123456",      # Run full refresh
    "salesforce": "789012",         # Run incremental
    "stripe": "345678",             # Run payments models only
}
```

## Selective Model Runs

Don't run all models for every load. Target specific models:

### Using dbt tags

```yaml
# models/staging/stg_events.yml
models:
  - name: stg_events
    config:
      tags: ["events", "hourly"]
```

```bash
# Run only events-related models
dbt run --select tag:events
```

### Using dbt selectors

```yaml
# selectors.yml
selectors:
  - name: events_pipeline
    definition:
      union:
        - method: source
          value: source:raw.events
          children: true
```

```bash
dbt run --selector events_pipeline
```

### Triggering specific selectors via API

```python
def trigger_dbt_for_source(source_name: str):
    """Trigger dbt run for models downstream of a source."""
    
    # Map sources to selectors or tags
    source_to_selector = {
        "events": "events_pipeline",
        "customers": "customer_pipeline",
        "orders": "orders_pipeline",
    }
    
    selector = source_to_selector.get(source_name)
    
    if selector:
        response = requests.post(
            f"{DBT_API_BASE}/jobs/{JOB_ID}/run/",
            headers={"Authorization": f"Token {DBT_API_KEY}"},
            json={
                "cause": f"Load complete: {source_name}",
                "steps_override": [f"dbt run --selector {selector}"]
            }
        )
```

## Handling Multiple Sources

When multiple sources load at different times:

### Option 1: Per-source dbt jobs

Create separate dbt Cloud jobs for each source. Trigger the appropriate job from each webhook.

### Option 2: Single job with dynamic selectors

Override steps in API call based on which source completed:

```python
def trigger_dbt(source: str):
    steps = [f"dbt run --select source:{source}+"]
    
    requests.post(
        f"{DBT_API_BASE}/jobs/{JOB_ID}/run/",
        json={"steps_override": steps}
    )
```

### Option 3: Debounced trigger

Wait for all sources to be fresh before running dbt:

```python
def check_all_sources_fresh():
    """Check if all sources have fresh data."""
    
    # Query source freshness table
    result = snowflake_conn.execute("""
        SELECT source_name, 
               DATEDIFF('minute', max_loaded_at, CURRENT_TIMESTAMP()) as minutes_stale
        FROM analytics.source_freshness
        WHERE minutes_stale > 60
    """)
    
    stale_sources = [r[0] for r in result]
    
    if not stale_sources:
        trigger_dbt_full_run()
    else:
        print(f"Waiting for: {stale_sources}")
```

## Error Handling

### Retry failed dbt runs

```python
def trigger_dbt_with_retry(max_attempts: int = 3):
    """Trigger dbt with retry on failure."""
    
    for attempt in range(max_attempts):
        run = trigger_dbt_job()
        status = wait_for_completion(run["id"])
        
        if status == "success":
            return run
        elif status == "error":
            if attempt < max_attempts - 1:
                time.sleep(60 * (attempt + 1))  # Backoff
            else:
                raise Exception(f"dbt failed after {max_attempts} attempts")
```

### Alert on transformation failures

```python
def handle_dbt_failure(run_id: int):
    """Handle failed dbt run."""
    
    # Get failure details
    run = get_dbt_run(run_id)
    
    # Send alert
    send_slack_notification(
        f"❌ dbt run failed: {run['id']}\n"
        f"Job: {run['job']['name']}\n"
        f"Error: {run['status_message']}"
    )
    
    # Optionally create incident
    create_pagerduty_incident(run)
```

## Monitoring Integration

Track the full pipeline from load to transform:

```sql
-- Combined load + transform monitoring view
CREATE OR REPLACE VIEW ops.pipeline_status AS
WITH loads AS (
    SELECT 
        table_name,
        MAX(last_load_time) as loaded_at,
        SUM(row_count) as rows_loaded
    FROM TABLE(information_schema.copy_history(
        start_time => dateadd('hour', -24, current_timestamp())
    ))
    GROUP BY table_name
),
transforms AS (
    SELECT 
        model_name,
        MAX(completed_at) as transformed_at,
        status
    FROM analytics.dbt_run_results
    WHERE run_started_at > dateadd('hour', -24, current_timestamp())
    GROUP BY model_name, status
)
SELECT 
    l.table_name,
    l.loaded_at,
    l.rows_loaded,
    t.transformed_at,
    t.status as transform_status,
    DATEDIFF('minute', l.loaded_at, t.transformed_at) as load_to_transform_minutes
FROM loads l
LEFT JOIN transforms t ON t.model_name = 'stg_' || LOWER(l.table_name);
```
