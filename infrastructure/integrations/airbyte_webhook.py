"""
Airbyte Webhook Handler

Handles Airbyte sync completion webhooks to trigger downstream processing.
Deploy as AWS Lambda, Cloud Function, or server endpoint.
"""

import json
import os
from datetime import datetime
from typing import Optional
import requests


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

DBT_CLOUD_API_KEY = os.environ.get("DBT_CLOUD_API_KEY", "")
DBT_CLOUD_ACCOUNT_ID = os.environ.get("DBT_CLOUD_ACCOUNT_ID", "")
DBT_CLOUD_JOB_ID = os.environ.get("DBT_CLOUD_JOB_ID", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

# Map Airbyte connection IDs to dbt job IDs (for selective triggering)
CONNECTION_TO_JOB_MAP = {
    # "airbyte_connection_id": "dbt_job_id",
    "postgres_prod_connection": "123456",
    "salesforce_connection": "789012",
}


# -----------------------------------------------------------------------------
# Event handlers
# -----------------------------------------------------------------------------

def handle_sync_succeeded(event: dict) -> dict:
    """
    Handle successful Airbyte sync completion.
    
    Triggers dbt job and sends notification.
    """
    connection_id = event.get("connection", {}).get("connectionId", "")
    connection_name = event.get("connection", {}).get("name", "unknown")
    job_id = event.get("job", {}).get("id", "")
    
    # Get bytes and records from job info
    job_info = event.get("job", {})
    bytes_synced = job_info.get("bytesSynced", 0)
    records_synced = job_info.get("recordsSynced", 0)
    
    result = {
        "connection": connection_name,
        "connection_id": connection_id,
        "status": "succeeded",
        "actions": []
    }
    
    # Trigger dbt job (either specific or default)
    dbt_job = CONNECTION_TO_JOB_MAP.get(connection_id, DBT_CLOUD_JOB_ID)
    
    if dbt_job:
        dbt_result = trigger_dbt_job(
            job_id=dbt_job,
            cause=f"Airbyte sync: {connection_name}"
        )
        result["actions"].append({
            "action": "trigger_dbt",
            "job_id": dbt_job,
            "result": dbt_result
        })
    
    # Send success notification
    send_slack_notification(
        f"✅ Airbyte sync complete: {connection_name}\n"
        f"Records: {records_synced:,}, Bytes: {bytes_synced / 1e6:.2f} MB"
    )
    result["actions"].append({"action": "notify", "result": "sent"})
    
    return result


def handle_sync_failed(event: dict) -> dict:
    """Handle failed Airbyte sync."""
    connection_name = event.get("connection", {}).get("name", "unknown")
    failure_reason = event.get("job", {}).get("failureReason", "Unknown error")
    
    # Send failure alert
    send_slack_notification(
        f"❌ Airbyte sync FAILED: {connection_name}\n"
        f"Reason: {failure_reason}\n"
        f"Check Airbyte dashboard for details.",
        is_error=True
    )
    
    return {
        "connection": connection_name,
        "status": "failed",
        "reason": failure_reason,
        "action": "alerted"
    }


def handle_sync_started(event: dict) -> dict:
    """Handle sync started (optional logging)."""
    connection_name = event.get("connection", {}).get("name", "unknown")
    
    print(f"Airbyte sync started: {connection_name} at {datetime.utcnow().isoformat()}")
    
    return {"connection": connection_name, "status": "started"}


# -----------------------------------------------------------------------------
# dbt Cloud integration
# -----------------------------------------------------------------------------

def trigger_dbt_job(job_id: str, cause: str = "Airbyte webhook") -> dict:
    """Trigger a dbt Cloud job run."""
    if not all([DBT_CLOUD_API_KEY, DBT_CLOUD_ACCOUNT_ID]):
        return {"error": "dbt Cloud not configured"}
    
    url = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_CLOUD_ACCOUNT_ID}/jobs/{job_id}/run/"
    
    headers = {
        "Authorization": f"Token {DBT_CLOUD_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {"cause": cause}
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        run_id = data.get("data", {}).get("id")
        
        return {"success": True, "run_id": run_id}
        
    except requests.exceptions.RequestException as e:
        return {"success": False, "error": str(e)}


# -----------------------------------------------------------------------------
# Notifications
# -----------------------------------------------------------------------------

def send_slack_notification(message: str, is_error: bool = False) -> None:
    """Send notification to Slack."""
    if not SLACK_WEBHOOK_URL:
        print(f"Slack not configured. Message: {message}")
        return
    
    payload = {
        "text": message,
        "username": "Airbyte Bot",
        "icon_emoji": ":x:" if is_error else ":white_check_mark:",
    }
    
    try:
        requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Slack notification: {e}")


# -----------------------------------------------------------------------------
# Main handler (AWS Lambda format)
# -----------------------------------------------------------------------------

def lambda_handler(event: dict, context) -> dict:
    """
    AWS Lambda handler for Airbyte webhooks.
    
    Airbyte webhook payload structure varies by event type.
    """
    # Extract payload
    body = event.get("body", "{}")
    
    try:
        payload = json.loads(body) if isinstance(body, str) else body
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON"})
        }
    
    # Determine event type from payload
    # Airbyte uses different structures; adapt based on your version
    event_type = payload.get("eventType", "")
    
    # Alternative: Check job status
    job_status = payload.get("job", {}).get("status", "")
    
    if event_type == "sync.succeeded" or job_status == "succeeded":
        result = handle_sync_succeeded(payload)
    elif event_type == "sync.failed" or job_status == "failed":
        result = handle_sync_failed(payload)
    elif event_type == "sync.started" or job_status == "running":
        result = handle_sync_started(payload)
    else:
        result = {"message": f"Unhandled event: {event_type or job_status}"}
    
    return {
        "statusCode": 200,
        "body": json.dumps(result)
    }


# -----------------------------------------------------------------------------
# Flask app (alternative deployment)
# -----------------------------------------------------------------------------

def create_flask_app():
    """Create Flask app for server deployment."""
    from flask import Flask, request, jsonify
    
    app = Flask(__name__)
    
    @app.route("/webhook/airbyte", methods=["POST"])
    def airbyte_webhook():
        payload = request.json
        
        event_type = payload.get("eventType", "")
        job_status = payload.get("job", {}).get("status", "")
        
        if event_type == "sync.succeeded" or job_status == "succeeded":
            result = handle_sync_succeeded(payload)
        elif event_type == "sync.failed" or job_status == "failed":
            result = handle_sync_failed(payload)
        elif event_type == "sync.started" or job_status == "running":
            result = handle_sync_started(payload)
        else:
            result = {"message": f"Unhandled: {event_type or job_status}"}
        
        return jsonify(result)
    
    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "healthy"})
    
    return app


if __name__ == "__main__":
    app = create_flask_app()
    app.run(debug=True, port=5001)
