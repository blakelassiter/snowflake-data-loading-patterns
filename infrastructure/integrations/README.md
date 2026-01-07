# Integrations

Webhook handlers and triggers for managed ETL tools (Fivetran, Airbyte).

## Files

| File | Purpose |
|------|---------|
| [fivetran_webhook.py](fivetran_webhook.py) | Webhook handler for Fivetran sync completion |
| [airbyte_webhook.py](airbyte_webhook.py) | Webhook handler for Airbyte sync completion |
| [dbt_trigger.py](dbt_trigger.py) | Trigger dbt runs after data loads |

## Pattern: Post-Load Triggers

Managed ETL tools handle extraction and loading. You need to trigger downstream processing (dbt, alerts, etc.) when syncs complete.

```
Fivetran Sync Complete
        ↓
    Webhook
        ↓
  Trigger dbt run
        ↓
 Transform in Snowflake
```

## Fivetran Integration

Fivetran can send webhooks on sync completion. Use these to:

1. **Trigger dbt runs** - Start transformation after fresh data arrives
2. **Send notifications** - Alert team of sync status
3. **Update dashboards** - Refresh cached reports
4. **Audit logging** - Track sync history

### Setup

1. Deploy webhook handler (AWS Lambda, Cloud Function, or server)
2. Configure Fivetran webhook in connector settings
3. Set webhook URL and secret

### Example payload

```json
{
  "event": "sync_end",
  "created": "2025-01-15T10:30:00Z",
  "connector_id": "abc123",
  "connector_name": "postgres_prod",
  "sync_id": "xyz789",
  "data": {
    "status": "SUCCESSFUL",
    "tables_synced": 15,
    "rows_updated": 50000
  }
}
```

## Airbyte Integration

Similar pattern for Airbyte:

1. Configure webhook in Airbyte connection
2. Handle sync completion events
3. Trigger downstream processing

## Deployment Options

### AWS Lambda

```python
# Deploy webhook as Lambda function
# API Gateway → Lambda → dbt Cloud API
```

### Cloud Functions (GCP)

```python
# Deploy as HTTP-triggered Cloud Function
```

### Server-based

```python
# Flask/FastAPI endpoint on existing infrastructure
```

## Security

- Verify webhook signatures
- Use secrets management
- Restrict IP ranges if possible
- Log all webhook events

## Related

- [dbt-snowflake-optimization](https://github.com/blakelassiter/dbt-snowflake-optimization)
- [CDC pipelines](../../use-cases/cdc-pipelines.md)
