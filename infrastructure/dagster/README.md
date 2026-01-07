# Dagster

Dagster assets for Snowflake data loading, featuring Sling integration for simplified EL pipelines.

## Files

| File | Purpose |
|------|---------|
| [assets.py](assets.py) | Software-defined assets for data loading |
| [sling_assets.py](sling_assets.py) | Sling-based EL pipelines |
| [resources.py](resources.py) | Snowflake and Sling resource definitions |
| [definitions.py](definitions.py) | Dagster definitions entry point |

## Why Dagster + Sling?

[Sling](https://slingdata.io/) handles the low-level data movement:
- Schema inference and evolution
- Incremental loading with watermarks
- Multiple source/target connectors

Dagster provides:
- Software-defined assets (lineage, observability)
- Scheduling and sensors
- Partitioning
- Integration with dbt

Together they eliminate boilerplate while maintaining control.

## Prerequisites

```bash
pip install dagster dagster-snowflake dagster-embedded-elt sling
```

## Configuration

Set environment variables:

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="LOADING_WH"
export SNOWFLAKE_DATABASE="RAW"
export SNOWFLAKE_SCHEMA="LOADING"
```

Or use Dagster's configuration:

```yaml
# dagster.yaml
resources:
  snowflake:
    config:
      account: your_account
      user: your_user
      password:
        env: SNOWFLAKE_PASSWORD
      warehouse: LOADING_WH
      database: RAW
      schema: LOADING
```

## Usage

```bash
# Run Dagster UI
dagster dev -f definitions.py

# Materialize specific assets
dagster asset materialize --select raw_events

# Run all assets
dagster asset materialize --select "*"
```

## Asset Patterns

### Simple SQL asset

```python
@asset
def raw_events(snowflake: SnowflakeResource):
    snowflake.execute_query("""
        COPY INTO raw.events FROM @stage/events/
    """)
```

### Sling asset

```python
@sling_assets(
    replication_config=SlingReplicationConfig(
        source="S3",
        target="SNOWFLAKE",
        streams={"s3://bucket/events/": "raw.events"}
    )
)
def events_from_s3(): ...
```

### Partitioned asset

```python
@asset(partitions_def=DailyPartitionsDefinition(start_date="2025-01-01"))
def daily_events(context, snowflake):
    date = context.partition_key
    snowflake.execute_query(f"""
        COPY INTO raw.events FROM @stage/events/{date}/
    """)
```

## Related

- [Sling documentation](https://docs.slingdata.io/)
- [Dagster Snowflake integration](https://docs.dagster.io/integrations/snowflake)
- [COPY INTO patterns](../../patterns/copy-into/)
