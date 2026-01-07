# Airflow

Apache Airflow DAGs for orchestrating Snowflake data loading.

## Files

| File | Purpose |
|------|---------|
| [copy_into_dag.py](copy_into_dag.py) | Scheduled batch loading with COPY INTO |
| [incremental_dag.py](incremental_dag.py) | Incremental load with staging and merge |
| [snowpipe_monitor_dag.py](snowpipe_monitor_dag.py) | Monitor Snowpipe health |

## Prerequisites

```bash
# Install Snowflake provider
pip install apache-airflow-providers-snowflake
```

## Connection Setup

Create a Snowflake connection in Airflow:

```python
# airflow connections add
airflow connections add 'snowflake_default' \
    --conn-type 'snowflake' \
    --conn-login 'username' \
    --conn-password 'password' \
    --conn-schema 'RAW' \
    --conn-extra '{
        "account": "your_account",
        "warehouse": "LOADING_WH",
        "database": "RAW",
        "role": "DATA_ENGINEER"
    }'
```

Or via environment variable:

```bash
export AIRFLOW_CONN_SNOWFLAKE_DEFAULT='snowflake://user:password@account/RAW/LOADING?warehouse=LOADING_WH&role=DATA_ENGINEER'
```

## Usage

Copy DAGs to your Airflow DAGs folder:

```bash
cp *.py $AIRFLOW_HOME/dags/
```

## DAG Patterns

### Simple scheduled load

```python
# Run COPY INTO on a schedule
@dag(schedule='@hourly')
def simple_load():
    SnowflakeOperator(
        task_id='copy_events',
        sql="COPY INTO raw.events FROM @stage/events/"
    )
```

### Sensor-based load

```python
# Wait for files, then load
@dag(schedule='@hourly')
def sensor_load():
    wait = S3KeySensor(bucket='data-bucket', bucket_key='events/*.json')
    load = SnowflakeOperator(sql="COPY INTO ...")
    wait >> load
```

### Multi-step pipeline

```python
# Stage, validate, merge
@dag(schedule='@daily')
def incremental_load():
    stage = SnowflakeOperator(sql="COPY INTO staging...")
    validate = SnowflakeOperator(sql="SELECT COUNT(*) ...")
    merge = SnowflakeOperator(sql="MERGE INTO target ...")
    stage >> validate >> merge
```

## Best Practices

1. **Idempotency** - Design tasks to be safely re-run
2. **Atomic loads** - Use staging tables for multi-step loads
3. **Error handling** - Configure retries and alerts
4. **Monitoring** - Use Airflow's built-in metrics

## Related

- [COPY INTO patterns](../../patterns/copy-into/)
- [Incremental loading](../../use-cases/incremental-loading.md)
- [Terraform warehouses](../terraform/warehouses.tf)
