"""
Fivetran Webhook Handler

Handles Fivetran sync completion webhooks to trigger downstream processing.
Deploy as AWS Lambda, Cloud Function, or server endpoint.
"""

import json
import hmac
import hashlib
import os
from datetime import datetime
from typing import Optional
import requests


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

FIVETRAN_WEBHOOK_SECRET = os.environ.get("FIVETRAN_WEBHOOK_SECRET", "")
DBT_CLOUD_API_KEY = os.environ.get("DBT_CLOUD_API_KEY", "")
DBT_CLOUD_ACCOUNT_ID = os.environ.get("DBT_CLOUD_ACCOUNT_ID", "")
DBT_CLOUD_JOB_ID = os.environ.get("DBT_CLOUD_JOB_ID", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


# -----------------------------------------------------------------------------
# Signature verification
# -----------------------------------------------------------------------------

def verify_fivetran_signature(payload: bytes, signature: str, secret: str) -> bool:
    """
    Verify Fivetran webhook signature.
    
    Fivetran signs webhooks with HMAC-SHA256.
    """
    expected = hmac.new(
        secret.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(expected, signature)


# -----------------------------------------------------------------------------
# Event handlers
# -----------------------------------------------------------------------------

def handle_sync_end(event: dict) -> dict:
    """
    Handle sync_end event from Fivetran.
    
    Triggers dbt job and sends notification.
    """
    connector_name = event.get("connector_name", "unknown")
    sync_status = event.get("data", {}).get("status", "UNKNOWN")
    tables_synced = event.get("data", {}).get("tables_synced", 0)
    rows_updated = event.get("data", {}).get("rows_updated", 0)
    
    result = {
        "connector": connector_name,
        "status": sync_status,
        "actions": []
    }
    
    if sync_status == "SUCCESSFUL":
        # Trigger dbt job
        dbt_result = trigger_dbt_job(
            cause=f"Fivetran sync: {connector_name}"
        )
        result["actions"].append({
            "action": "trigger_dbt",
            "result": dbt_result
        })
        
        # Send success notification
        send_slack_notification(
            f"✅ Fivetran sync complete: {connector_name}\n"
            f"Tables: {tables_synced}, Rows: {rows_updated:,}"
        )
        result["actions"].append({"action": "notify", "result": "sent"})
        
    elif sync_status == "FAILED":
        # Send failure alert
        send_slack_notification(
            f"❌ Fivetran sync FAILED: {connector_name}\n"
            f"Check Fivetran dashboard for details.",
            is_error=True
        )
        result["actions"].append({"action": "alert", "result": "sent"})
    
    return result


def handle_sync_start(event: dict) -> dict:
    """Handle sync_start event (optional logging)."""
    connector_name = event.get("connector_name", "unknown")
    
    # Log sync start for monitoring
    print(f"Sync started: {connector_name} at {datetime.utcnow().isoformat()}")
    
    return {"connector": connector_name, "action": "logged"}


def handle_schema_change(event: dict) -> dict:
    """
    Handle schema_change event.
    
    Schema changes may require dbt model updates.
    """
    connector_name = event.get("connector_name", "unknown")
    changes = event.get("data", {}).get("changes", [])
    
    # Alert on schema changes
    send_slack_notification(
        f"⚠️ Schema change detected: {connector_name}\n"
        f"Changes: {len(changes)} table(s) affected\n"
        f"Review and update dbt models if needed.",
        is_error=False
    )
    
    return {"connector": connector_name, "changes": len(changes)}


# -----------------------------------------------------------------------------
# dbt Cloud integration
# -----------------------------------------------------------------------------

def trigger_dbt_job(cause: str = "Fivetran webhook") -> dict:
    """
    Trigger a dbt Cloud job run.
    
    Returns job run details or error.
    """
    if not all([DBT_CLOUD_API_KEY, DBT_CLOUD_ACCOUNT_ID, DBT_CLOUD_JOB_ID]):
        return {"error": "dbt Cloud not configured"}
    
    url = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_CLOUD_ACCOUNT_ID}/jobs/{DBT_CLOUD_JOB_ID}/run/"
    
    headers = {
        "Authorization": f"Token {DBT_CLOUD_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "cause": cause,
        "git_sha": None,  # Use default branch
        "schema_override": None,
        "dbt_version_override": None,
        "threads_override": None,
        "target_name_override": None,
        "generate_docs_override": None,
        "timeout_seconds_override": None,
        "steps_override": None,
    }
    
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
        "username": "Fivetran Bot",
        "icon_emoji": ":warning:" if is_error else ":white_check_mark:",
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
    AWS Lambda handler for Fivetran webhooks.
    
    Expects event from API Gateway with:
    - body: JSON payload from Fivetran
    - headers: Including X-Fivetran-Signature
    """
    # Extract payload and signature
    body = event.get("body", "{}")
    headers = event.get("headers", {})
    signature = headers.get("X-Fivetran-Signature", headers.get("x-fivetran-signature", ""))
    
    # Verify signature
    if FIVETRAN_WEBHOOK_SECRET:
        if not verify_fivetran_signature(body.encode(), signature, FIVETRAN_WEBHOOK_SECRET):
            return {
                "statusCode": 401,
                "body": json.dumps({"error": "Invalid signature"})
            }
    
    # Parse payload
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON"})
        }
    
    # Route by event type
    event_type = payload.get("event", "")
    
    handlers = {
        "sync_end": handle_sync_end,
        "sync_start": handle_sync_start,
        "schema_change": handle_schema_change,
    }
    
    handler = handlers.get(event_type)
    
    if handler:
        result = handler(payload)
        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }
    else:
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Unhandled event type: {event_type}"})
        }


# -----------------------------------------------------------------------------
# Flask app (alternative deployment)
# -----------------------------------------------------------------------------

def create_flask_app():
    """Create Flask app for server deployment."""
    from flask import Flask, request, jsonify
    
    app = Flask(__name__)
    
    @app.route("/webhook/fivetran", methods=["POST"])
    def fivetran_webhook():
        # Verify signature
        signature = request.headers.get("X-Fivetran-Signature", "")
        
        if FIVETRAN_WEBHOOK_SECRET:
            if not verify_fivetran_signature(request.data, signature, FIVETRAN_WEBHOOK_SECRET):
                return jsonify({"error": "Invalid signature"}), 401
        
        # Process event
        payload = request.json
        event_type = payload.get("event", "")
        
        handlers = {
            "sync_end": handle_sync_end,
            "sync_start": handle_sync_start,
            "schema_change": handle_schema_change,
        }
        
        handler = handlers.get(event_type)
        
        if handler:
            result = handler(payload)
            return jsonify(result)
        else:
            return jsonify({"message": f"Unhandled: {event_type}"})
    
    return app


if __name__ == "__main__":
    # Run Flask app locally for testing
    app = create_flask_app()
    app.run(debug=True, port=5000)
