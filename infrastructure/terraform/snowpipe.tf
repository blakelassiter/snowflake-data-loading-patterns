# =============================================================================
# Snowpipe Configuration
# =============================================================================
# Snowpipe for continuous, event-driven data loading.

# -----------------------------------------------------------------------------
# Target tables
# -----------------------------------------------------------------------------

resource "snowflake_table" "events" {
  database = var.database
  schema   = var.schema
  name     = "EVENTS"

  column {
    name = "EVENT_ID"
    type = "STRING"
  }

  column {
    name = "EVENT_TYPE"
    type = "STRING"
  }

  column {
    name = "EVENT_TIMESTAMP"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "USER_ID"
    type = "STRING"
  }

  column {
    name = "PAYLOAD"
    type = "VARIANT"
  }

  column {
    name     = "_LOADED_AT"
    type     = "TIMESTAMP_NTZ"
    nullable = false
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

  column {
    name = "_SOURCE_FILE"
    type = "STRING"
  }

  comment = "Events table loaded via Snowpipe"
}

# -----------------------------------------------------------------------------
# Snowpipe - AWS S3
# -----------------------------------------------------------------------------

resource "snowflake_pipe" "events_s3" {
  count = var.cloud_provider == "aws" && var.enable_snowpipe ? 1 : 0

  database = var.database
  schema   = var.schema
  name     = "${upper(var.environment)}_EVENTS_PIPE"

  auto_ingest = true

  copy_statement = <<-EOT
    COPY INTO ${var.database}.${var.schema}.${snowflake_table.events.name}
    FROM (
      SELECT 
        $1:event_id::STRING,
        $1:event_type::STRING,
        $1:event_timestamp::TIMESTAMP_NTZ,
        $1:user_id::STRING,
        $1:payload::VARIANT,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
      FROM @${var.database}.${var.schema}.${snowflake_stage.s3_events[0].name}
    )
    FILE_FORMAT = (TYPE = 'JSON')
  EOT

  comment = "Auto-ingest pipe for events from S3"

  depends_on = [
    snowflake_stage.s3_events,
    snowflake_table.events
  ]
}

# Output SQS queue ARN for S3 event notification
output "snowpipe_s3_notification_channel" {
  value       = var.cloud_provider == "aws" && var.enable_snowpipe ? snowflake_pipe.events_s3[0].notification_channel : null
  description = "SQS queue ARN for S3 event notifications"
}

# -----------------------------------------------------------------------------
# Snowpipe - GCS
# -----------------------------------------------------------------------------

resource "snowflake_pipe" "events_gcs" {
  count = var.cloud_provider == "gcp" && var.enable_snowpipe ? 1 : 0

  database = var.database
  schema   = var.schema
  name     = "${upper(var.environment)}_EVENTS_PIPE"

  auto_ingest = true

  copy_statement = <<-EOT
    COPY INTO ${var.database}.${var.schema}.${snowflake_table.events.name}
    FROM (
      SELECT 
        $1:event_id::STRING,
        $1:event_type::STRING,
        $1:event_timestamp::TIMESTAMP_NTZ,
        $1:user_id::STRING,
        $1:payload::VARIANT,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
      FROM @${var.database}.${var.schema}.${snowflake_stage.gcs_events[0].name}
    )
    FILE_FORMAT = (TYPE = 'JSON')
  EOT

  comment = "Auto-ingest pipe for events from GCS"

  depends_on = [
    snowflake_stage.gcs_events,
    snowflake_table.events
  ]
}

output "snowpipe_gcs_notification_channel" {
  value       = var.cloud_provider == "gcp" && var.enable_snowpipe ? snowflake_pipe.events_gcs[0].notification_channel : null
  description = "Pub/Sub subscription for GCS notifications"
}

# -----------------------------------------------------------------------------
# Snowpipe - Azure
# -----------------------------------------------------------------------------

resource "snowflake_pipe" "events_azure" {
  count = var.cloud_provider == "azure" && var.enable_snowpipe ? 1 : 0

  database = var.database
  schema   = var.schema
  name     = "${upper(var.environment)}_EVENTS_PIPE"

  auto_ingest = true

  copy_statement = <<-EOT
    COPY INTO ${var.database}.${var.schema}.${snowflake_table.events.name}
    FROM (
      SELECT 
        $1:event_id::STRING,
        $1:event_type::STRING,
        $1:event_timestamp::TIMESTAMP_NTZ,
        $1:user_id::STRING,
        $1:payload::VARIANT,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
      FROM @${var.database}.${var.schema}.${snowflake_stage.azure_events[0].name}
    )
    FILE_FORMAT = (TYPE = 'JSON')
  EOT

  comment = "Auto-ingest pipe for events from Azure"

  depends_on = [
    snowflake_stage.azure_events,
    snowflake_table.events
  ]
}

output "snowpipe_azure_notification_channel" {
  value       = var.cloud_provider == "azure" && var.enable_snowpipe ? snowflake_pipe.events_azure[0].notification_channel : null
  description = "Storage Queue URL for Azure Event Grid"
}

# -----------------------------------------------------------------------------
# Pipe grants
# -----------------------------------------------------------------------------

resource "snowflake_pipe_grant" "events_pipe_operate" {
  count = var.enable_snowpipe ? 1 : 0

  database_name = var.database
  schema_name   = var.schema
  pipe_name     = local.events_pipe_name

  privilege = "OPERATE"
  roles     = var.pipe_grant_roles

  depends_on = [
    snowflake_pipe.events_s3,
    snowflake_pipe.events_gcs,
    snowflake_pipe.events_azure
  ]
}

locals {
  events_pipe_name = (
    var.cloud_provider == "aws" && var.enable_snowpipe ? snowflake_pipe.events_s3[0].name :
    var.cloud_provider == "gcp" && var.enable_snowpipe ? snowflake_pipe.events_gcs[0].name :
    var.cloud_provider == "azure" && var.enable_snowpipe ? snowflake_pipe.events_azure[0].name :
    null
  )
}
