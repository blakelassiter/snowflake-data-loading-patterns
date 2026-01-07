# dbt Integration

Patterns for triggering dbt transformations after data loading completes.

## The Problem

Data loading and transformation are separate concerns:

1. **Loading** moves raw data into Snowflake (COPY INTO, Snowpipe, Streaming)
2. **Transformation** cleans and models data for analytics (dbt)

Coordinating these requires answering: *"How do we run dbt only after fresh data arrives?"*

## Index

| File | Pattern |
|------|---------|
| [post-load-triggers.md](post-load-triggers.md) | Trigger dbt after COPY INTO or Snowpipe |
| [task-orchestration.sql](task-orchestration.sql) | Native Snowflake Tasks + Streams |
| [freshness-checks.sql](freshness-checks.sql) | dbt source freshness for monitoring |

## Common Patterns

### Pattern 1: Orchestrator-Driven

The orchestrator (Airflow, Dagster, Prefect) handles both loading and transformation:

```
Airflow DAG:
  load_task (COPY INTO) 
    → wait_for_success 
    → dbt_run_task
```

Pros: Single control plane, clear dependencies
Cons: Tightly coupled, orchestrator becomes bottleneck

### Pattern 2: Webhook-Triggered

Managed tools (Fivetran, Airbyte) trigger dbt via webhook:

```
Fivetran sync complete 
  → webhook 
  → dbt Cloud API 
  → dbt run
```

Pros: Decoupled, each tool does one thing
Cons: Webhook reliability, debugging across systems

### Pattern 3: Native Snowflake Tasks

Snowflake Streams detect new data, Tasks run dbt:

```
STREAM on raw.events 
  → TASK checks stream 
  → calls dbt via external function or stored procedure
```

Pros: No external dependencies, event-driven
Cons: More Snowflake-specific, external function setup

### Pattern 4: Polling/Scheduled

dbt runs on a schedule, checks freshness first:

```
dbt run (every hour)
  → source freshness check
  → skip if data isn't fresh
```

Pros: Simple, self-contained
Cons: Latency, may run unnecessarily

## Choosing a Pattern

| If you have... | Use... |
|----------------|--------|
| Airflow/Dagster already | Pattern 1 (Orchestrator) |
| Fivetran/Airbyte + dbt Cloud | Pattern 2 (Webhook) |
| Pure Snowflake, no external tools | Pattern 3 (Tasks) |
| Simple needs, latency tolerance | Pattern 4 (Polling) |

## Related

- [Webhook handlers](../infrastructure/integrations/) - Fivetran/Airbyte webhooks
- [Airflow DAGs](../infrastructure/airflow/) - Orchestrated loading
- [dbt-snowflake-optimization](https://github.com/blakelassiter/dbt-snowflake-optimization) - Transform optimization
