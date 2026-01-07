# Infrastructure

Infrastructure-as-code examples for deploying and managing Snowflake data loading pipelines.

## Index

| Directory | Purpose | Tools |
|-----------|---------|-------|
| [terraform/](terraform/) | Snowflake resources and cloud storage | Terraform |
| [airflow/](airflow/) | Orchestrated batch loading | Apache Airflow |
| [dagster/](dagster/) | Asset-based pipelines | Dagster + Sling |
| [integrations/](integrations/) | Managed tool webhooks | Fivetran, Airbyte |

## Choosing an Approach

**"We use Terraform for everything"**
→ [terraform/](terraform/) - Storage integrations, stages, pipes as code

**"We have Airflow for orchestration"**
→ [airflow/](airflow/) - DAGs for scheduled COPY INTO

**"We're adopting Dagster"**
→ [dagster/](dagster/) - Software-defined assets with Sling

**"We use Fivetran/Airbyte"**
→ [integrations/](integrations/) - Post-load triggers and webhooks

## Combining Approaches

These aren't mutually exclusive:

- Terraform for infrastructure + Airflow for orchestration
- Dagster for pipeline logic + Terraform for Snowflake resources
- Fivetran for ingestion + dbt (triggered by webhook) for transformation

## Prerequisites

All examples assume:
- Snowflake account with appropriate permissions
- Cloud storage (S3/GCS/Azure) configured
- Relevant tool installed and authenticated

## Related

- [Loading patterns](../patterns/) - What these tools orchestrate
- [Operations](../operations/) - Monitoring the pipelines
- [dbt-snowflake-optimization](https://github.com/blakelassiter/dbt-snowflake-optimization) - Post-load transformations
