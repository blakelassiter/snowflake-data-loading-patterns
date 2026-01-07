"""
Snowpipe Streaming: Python SDK Example

Direct row-level ingestion without file staging. The high-performance architecture
(GA September 2025) provides up to 10 GB/s throughput with 5-10 second latency.

This example shows the NEW high-performance SDK (snowpipe-streaming).
For the classic architecture, see python-sdk-classic.py.

Requirements:
    Python 3.9 or later
    pip install snowpipe-streaming

Documentation:
    https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

# High-performance SDK (GA late 2025)
# Note: The SDK has a Rust core with Python bindings for performance
from snowpipe_streaming import (
    SnowpipeStreamingClient,
    SnowpipeStreamingClientConfig,
    Channel,
    InsertValidationResponse,
)


# ============================================================================
# Configuration
# ============================================================================

def load_config_from_profile(profile_path: str = "profile.json") -> dict:
    """
    Load configuration from profile.json file.
    
    Expected format:
    {
        "user": "MY_USER",
        "account": "your_account_identifier",
        "url": "https://your_account_identifier.snowflakecomputing.com:443",
        "private_key_file": "rsa_key.p8",
        "role": "MY_ROLE"
    }
    """
    with open(profile_path, "r") as f:
        return json.load(f)


# ============================================================================
# Client Setup (High-Performance Architecture)
# ============================================================================

def create_streaming_client(profile_path: str = "profile.json") -> SnowpipeStreamingClient:
    """
    Create a streaming client using the high-performance SDK.
    
    The high-performance architecture uses:
    - PIPE objects for data flow management
    - Server-side schema validation
    - Throughput-based pricing
    """
    
    config = load_config_from_profile(profile_path)
    
    # Read private key for authentication
    private_key_path = Path(config.get("private_key_file", "rsa_key.p8"))
    with open(private_key_path, "r") as key_file:
        private_key = key_file.read()
    
    # Build client configuration
    client_config = SnowpipeStreamingClientConfig(
        account=config["account"],
        user=config["user"],
        private_key=private_key,
        role=config.get("role", "PUBLIC"),
        url=config.get("url"),
    )
    
    # Create client
    client = SnowpipeStreamingClient(client_config)
    
    return client


# ============================================================================
# Channel Management
# ============================================================================

def open_channel(
    client: SnowpipeStreamingClient,
    database: str,
    schema: str,
    table: str,
    channel_name: str,
    pipe_name: str = None,
) -> Channel:
    """
    Open a streaming channel to a table.
    
    In the high-performance architecture, channels are opened against a PIPE object.
    If pipe_name is not specified, the default pipe for the table is used.
    
    Args:
        client: Streaming client
        database: Target database
        schema: Target schema  
        table: Target table
        channel_name: Unique name for this channel
        pipe_name: Optional PIPE object name (uses default if not specified)
    """
    
    channel = client.open_channel(
        channel_name=channel_name,
        database=database,
        schema=schema,
        table=table,
        pipe_name=pipe_name,  # None uses default pipe
    )
    
    return channel


def get_channel_status(channel: Channel) -> dict:
    """Get current status of a channel."""
    return {
        "name": channel.name,
        "is_valid": channel.is_valid(),
        "latest_committed_offset": channel.get_latest_committed_offset_token(),
    }


# ============================================================================
# Data Ingestion
# ============================================================================

def insert_rows(channel: Channel, rows: list[dict]) -> InsertValidationResponse:
    """
    Insert rows into a streaming channel.
    
    Args:
        channel: Open streaming channel
        rows: List of dictionaries matching table schema
    
    Returns:
        Validation response with any errors
    
    Example row:
        {"event_id": "123", "event_type": "click", "timestamp": "2025-01-01T00:00:00Z"}
    """
    
    # Insert rows - SDK handles batching internally
    response = channel.insert_rows(rows)
    
    return response


def insert_rows_with_offset(
    channel: Channel,
    rows: list[dict],
    offset_token: str,
) -> str:
    """
    Insert rows with offset tracking for exactly-once semantics.
    
    The offset token allows recovery from failures - restart from
    the last committed offset rather than reprocessing everything.
    
    Args:
        channel: Open streaming channel
        rows: Rows to insert
        offset_token: Application-provided offset for tracking
    
    Returns:
        The committed offset token
    """
    
    # Insert with offset
    channel.insert_rows(rows, offset_token=offset_token)
    
    # Wait for commit and get confirmed offset
    committed_offset = channel.get_latest_committed_offset_token()
    
    return committed_offset


# ============================================================================
# Error Handling
# ============================================================================

def insert_with_error_handling(channel: Channel, rows: list[dict]) -> dict:
    """
    Insert rows with comprehensive error handling.
    
    Returns:
        Dict with success count, failed rows, and any errors
    """
    
    result = {
        "success_count": 0,
        "failed_rows": [],
        "errors": [],
    }
    
    try:
        response = channel.insert_rows(rows)
        
        # Check for validation errors (schema mismatch, type errors, etc.)
        if response.has_errors():
            for error in response.get_insert_errors():
                result["errors"].append({
                    "row_index": error.row_index,
                    "message": error.message,
                    "exception": str(error.exception) if error.exception else None,
                })
                result["failed_rows"].append(rows[error.row_index])
        
        result["success_count"] = len(rows) - len(result["failed_rows"])
        
    except Exception as e:
        # Channel-level error (connection, authentication, etc.)
        result["errors"].append({
            "row_index": None,
            "message": f"Channel error: {str(e)}",
            "exception": type(e).__name__,
        })
        result["failed_rows"] = rows
    
    return result


# ============================================================================
# Complete Example: Event Ingestion
# ============================================================================

def main():
    """Complete example: ingest events into Snowflake via streaming."""
    
    # Create client
    client = create_streaming_client()
    
    # Open channel to events table
    # Note: Table must exist before opening channel
    channel = open_channel(
        client,
        database="RAW",
        schema="STREAMING",
        table="EVENTS",
        channel_name="events_producer_1",
    )
    
    try:
        # Generate sample events
        events = [
            {
                "event_id": f"evt_{i}",
                "event_type": "page_view",
                "user_id": f"user_{i % 100}",
                "page_url": f"/products/{i}",
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
            }
            for i in range(1000)
        ]
        
        # Ingest in batches
        batch_size = 100
        total_success = 0
        total_failed = 0
        
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            result = insert_with_error_handling(channel, batch)
            
            total_success += result["success_count"]
            total_failed += len(result["failed_rows"])
            
            if result["errors"]:
                print(f"Batch {i // batch_size + 1}: {len(result['errors'])} errors")
                for error in result["errors"]:
                    print(f"  - Row {error['row_index']}: {error['message']}")
            else:
                print(f"Batch {i // batch_size + 1}: {result['success_count']} rows ingested")
        
        print(f"\nIngestion complete: {total_success} success, {total_failed} failed")
        
        # Check final channel status
        status = get_channel_status(channel)
        print(f"Latest committed offset: {status['latest_committed_offset']}")
        
    finally:
        # Always close channel and client
        channel.close()
        client.close()


# ============================================================================
# Continuous Ingestion Pattern
# ============================================================================

def continuous_ingestion(source_generator, database: str, schema: str, table: str):
    """
    Pattern for continuous data ingestion from a generator.
    
    Handles reconnection and offset tracking for reliability.
    
    Args:
        source_generator: Iterator yielding rows to ingest
        database: Target database
        schema: Target schema
        table: Target table
    """
    
    client = create_streaming_client()
    channel = open_channel(
        client, database, schema, table,
        channel_name=f"{table}_continuous_{int(time.time())}"
    )
    
    batch = []
    batch_size = 500
    last_flush = time.time()
    flush_interval = 1.0  # seconds (data flushes automatically every ~1s anyway)
    
    try:
        for row in source_generator:
            batch.append(row)
            
            # Flush on batch size or time interval
            should_flush = (
                len(batch) >= batch_size or
                time.time() - last_flush >= flush_interval
            )
            
            if should_flush and batch:
                result = insert_with_error_handling(channel, batch)
                
                if result["failed_rows"]:
                    # Handle failed rows (dead-letter queue, retry, etc.)
                    handle_failed_rows(result["failed_rows"], result["errors"])
                
                batch = []
                last_flush = time.time()
        
        # Final flush
        if batch:
            insert_with_error_handling(channel, batch)
            
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
    finally:
        if batch:
            # Try to flush remaining data
            insert_with_error_handling(channel, batch)
        channel.close()
        client.close()


def handle_failed_rows(rows: list[dict], errors: list[dict]):
    """Handle failed rows - implement based on your requirements."""
    # Options:
    # - Write to dead-letter queue
    # - Retry with backoff
    # - Log and alert
    # - Write to error table
    for i, row in enumerate(rows):
        print(f"Failed row: {row}, Error: {errors[i] if i < len(errors) else 'Unknown'}")


# ============================================================================
# Target Table and PIPE Setup
# ============================================================================

"""
Target table should exist before streaming. The high-performance architecture
uses a PIPE object which is auto-created as a default pipe, or you can create
a custom one for transformations.

-- Create target table
CREATE TABLE raw.streaming.events (
    event_id STRING,
    event_type STRING,
    user_id STRING,
    page_url STRING,
    event_timestamp TIMESTAMP_NTZ,
    _inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Grant permissions to streaming role
GRANT INSERT ON TABLE raw.streaming.events TO ROLE streaming_role;

-- (Optional) Create custom PIPE with transformations
CREATE OR REPLACE PIPE raw.streaming.events_pipe
  COPY INTO raw.streaming.events
  FROM (
    SELECT 
      $1:event_id::STRING,
      $1:event_type::STRING,
      $1:user_id::STRING,
      $1:page_url::STRING,
      $1:event_timestamp::TIMESTAMP_NTZ,
      CURRENT_TIMESTAMP()
    FROM TABLE(FLATTEN(INPUT => PARSE_JSON($1)))
  );

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('raw.streaming.events_pipe');
"""


if __name__ == "__main__":
    main()
