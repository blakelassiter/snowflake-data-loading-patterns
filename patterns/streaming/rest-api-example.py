"""
Snowpipe Streaming: REST API Example

HTTP-based ingestion for simpler integrations. The high-performance architecture
(GA September 2025) includes a native REST API for streaming, which is recommended
for new implementations.

This example shows the SQL API approach, which works but has lower throughput
than the dedicated streaming REST API or SDK.

For new projects, consider:
1. High-performance REST API (best for IoT, edge deployments)
2. Python SDK (best for throughput)
3. SQL API (shown here - simplest, for lower volumes)

Requirements:
    pip install requests cryptography PyJWT

Documentation:
    https://docs.snowflake.com/en/developer-guide/sql-api/intro
    https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview
"""

import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


# ============================================================================
# Configuration
# ============================================================================

SNOWFLAKE_CONFIG = {
    "account": "myaccount",
    "user": "API_USER",
    "private_key_path": "/path/to/rsa_key.p8",
    "database": "RAW",
    "schema": "STREAMING",
}


# ============================================================================
# JWT Authentication
# ============================================================================

def load_private_key(key_path: str, passphrase: Optional[str] = None):
    """Load private key from file."""
    
    with open(key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )
    
    return private_key


def generate_jwt_token(account: str, user: str, private_key) -> str:
    """
    Generate JWT token for Snowflake authentication.
    
    Token is valid for 1 hour (Snowflake maximum).
    """
    
    # Account identifier format for JWT
    account_identifier = account.upper()
    qualified_user = f"{account_identifier}.{user.upper()}"
    
    # Token timestamps
    now = datetime.utcnow()
    lifetime = timedelta(minutes=59)
    
    payload = {
        "iss": f"{qualified_user}.SHA256:{get_public_key_fingerprint(private_key)}",
        "sub": qualified_user,
        "iat": now,
        "exp": now + lifetime,
    }
    
    token = jwt.encode(payload, private_key, algorithm="RS256")
    
    return token


def get_public_key_fingerprint(private_key) -> str:
    """Get SHA256 fingerprint of public key for JWT issuer."""
    
    import hashlib
    import base64
    
    public_key = private_key.public_key()
    public_key_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    
    sha256_hash = hashlib.sha256(public_key_bytes).digest()
    fingerprint = base64.b64encode(sha256_hash).decode("utf-8")
    
    return fingerprint


# ============================================================================
# SQL API Client
# ============================================================================

class SnowflakeRestClient:
    """Simple REST client for Snowflake SQL API."""
    
    def __init__(self, account: str, user: str, private_key_path: str):
        self.account = account
        self.user = user
        self.private_key = load_private_key(private_key_path)
        self.token = None
        self.token_expiry = None
        
        # API endpoint
        self.base_url = f"https://{account}.snowflakecomputing.com/api/v2"
    
    def _get_token(self) -> str:
        """Get valid JWT token, refreshing if needed."""
        
        now = datetime.utcnow()
        
        if self.token is None or self.token_expiry <= now:
            self.token = generate_jwt_token(
                self.account, self.user, self.private_key
            )
            self.token_expiry = now + timedelta(minutes=55)
        
        return self.token
    
    def _headers(self) -> dict:
        """Get request headers with authentication."""
        
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        }
    
    def execute_sql(
        self,
        statement: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        timeout: int = 60,
    ) -> dict:
        """
        Execute SQL statement via REST API.
        
        Args:
            statement: SQL to execute
            database: Database context
            schema: Schema context
            timeout: Statement timeout in seconds
        
        Returns:
            Response data including results
        """
        
        url = f"{self.base_url}/statements"
        
        payload = {
            "statement": statement,
            "timeout": timeout,
            "resultSetMetaData": {"format": "jsonv2"},
        }
        
        if database:
            payload["database"] = database
        if schema:
            payload["schema"] = schema
        
        response = requests.post(url, headers=self._headers(), json=payload)
        response.raise_for_status()
        
        return response.json()
    
    def insert_rows(
        self,
        table: str,
        rows: list[dict],
        database: str,
        schema: str,
    ) -> dict:
        """
        Insert rows using SQL INSERT statements.
        
        For high-volume streaming, use the native SDK instead.
        This approach is suitable for lower-volume REST integrations.
        """
        
        if not rows:
            return {"rows_inserted": 0}
        
        # Build multi-row INSERT statement
        columns = list(rows[0].keys())
        column_list = ", ".join(columns)
        
        values_list = []
        for row in rows:
            values = []
            for col in columns:
                val = row.get(col)
                if val is None:
                    values.append("NULL")
                elif isinstance(val, str):
                    # Escape single quotes
                    escaped = val.replace("'", "''")
                    values.append(f"'{escaped}'")
                elif isinstance(val, bool):
                    values.append("TRUE" if val else "FALSE")
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                elif isinstance(val, datetime):
                    values.append(f"'{val.isoformat()}'::TIMESTAMP_NTZ")
                else:
                    # JSON for complex types
                    escaped = json.dumps(val).replace("'", "''")
                    values.append(f"PARSE_JSON('{escaped}')")
            
            values_list.append(f"({', '.join(values)})")
        
        # Batch inserts (Snowflake has limits on statement size)
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(values_list), batch_size):
            batch_values = values_list[i:i + batch_size]
            
            sql = f"""
                INSERT INTO {database}.{schema}.{table} ({column_list})
                VALUES {', '.join(batch_values)}
            """
            
            result = self.execute_sql(sql, database=database, schema=schema)
            
            # Check for row count in result
            if "rowsAffected" in result:
                total_inserted += result["rowsAffected"]
            else:
                total_inserted += len(batch_values)
        
        return {"rows_inserted": total_inserted}


# ============================================================================
# Usage Example
# ============================================================================

def main():
    """Example: Insert events via REST API."""
    
    # Create client
    client = SnowflakeRestClient(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        private_key_path=SNOWFLAKE_CONFIG["private_key_path"],
    )
    
    # Generate sample events
    events = [
        {
            "event_id": str(uuid.uuid4()),
            "event_type": "api_call",
            "user_id": f"user_{i}",
            "endpoint": "/api/v1/data",
            "response_ms": 150 + (i % 100),
            "timestamp": datetime.utcnow(),
        }
        for i in range(50)
    ]
    
    # Insert events
    result = client.insert_rows(
        table="API_EVENTS",
        rows=events,
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
    )
    
    print(f"Inserted {result['rows_inserted']} rows")
    
    # Query to verify
    verify_result = client.execute_sql(
        "SELECT COUNT(*) as cnt FROM API_EVENTS WHERE timestamp > DATEADD('minute', -5, CURRENT_TIMESTAMP())",
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
    )
    
    print(f"Recent events in table: {verify_result}")


# ============================================================================
# Webhook Handler Pattern
# ============================================================================

"""
Example: Flask webhook handler that inserts data on HTTP POST

from flask import Flask, request, jsonify

app = Flask(__name__)
client = SnowflakeRestClient(...)

@app.route("/webhook/events", methods=["POST"])
def handle_event():
    data = request.json
    
    # Transform webhook payload to row format
    row = {
        "event_id": data.get("id"),
        "event_type": data.get("type"),
        "payload": data,
        "received_at": datetime.utcnow(),
    }
    
    result = client.insert_rows(
        table="WEBHOOK_EVENTS",
        rows=[row],
        database="RAW",
        schema="WEBHOOKS",
    )
    
    return jsonify({"status": "ok", "rows": result["rows_inserted"]})
"""


# ============================================================================
# Target Table Schema
# ============================================================================

"""
CREATE TABLE raw.streaming.api_events (
    event_id STRING,
    event_type STRING,
    user_id STRING,
    endpoint STRING,
    response_ms INTEGER,
    timestamp TIMESTAMP_NTZ,
    _inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Grant permissions
GRANT INSERT, SELECT ON TABLE raw.streaming.api_events TO ROLE api_role;
"""


if __name__ == "__main__":
    main()
