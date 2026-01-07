# =============================================================================
# Outputs
# =============================================================================
# Consolidated outputs for reference and downstream automation.

# -----------------------------------------------------------------------------
# Storage Integration
# -----------------------------------------------------------------------------

output "storage_integration_name" {
  value       = local.storage_integration_name
  description = "Name of the storage integration"
}

# -----------------------------------------------------------------------------
# Stages
# -----------------------------------------------------------------------------

output "events_stage_name" {
  value       = local.events_stage_name
  description = "Fully qualified name of events stage"
}

output "events_stage_url" {
  value = (
    var.cloud_provider == "aws" ? "s3://${var.s3_bucket_name}/${var.s3_bucket_path}events/" :
    var.cloud_provider == "gcp" ? "gcs://${var.gcs_bucket_name}/${var.gcs_bucket_path}events/" :
    var.cloud_provider == "azure" ? "azure://${var.azure_storage_account}.blob.core.windows.net/${var.azure_container_name}/${var.azure_path}events/" :
    null
  )
  description = "Cloud storage URL for events stage"
}

# -----------------------------------------------------------------------------
# Snowpipe
# -----------------------------------------------------------------------------

output "events_pipe_name" {
  value       = var.enable_snowpipe ? local.events_pipe_name : null
  description = "Name of events Snowpipe"
}

output "snowpipe_notification_channel" {
  value = (
    var.cloud_provider == "aws" && var.enable_snowpipe ? snowflake_pipe.events_s3[0].notification_channel :
    var.cloud_provider == "gcp" && var.enable_snowpipe ? snowflake_pipe.events_gcs[0].notification_channel :
    var.cloud_provider == "azure" && var.enable_snowpipe ? snowflake_pipe.events_azure[0].notification_channel :
    null
  )
  description = "Notification channel for cloud event configuration"
}

# -----------------------------------------------------------------------------
# Warehouses
# -----------------------------------------------------------------------------

output "warehouses" {
  value = {
    loading    = snowflake_warehouse.loading.name
    operations = snowflake_warehouse.operations.name
  }
  description = "Map of warehouse names"
}

# -----------------------------------------------------------------------------
# File Formats
# -----------------------------------------------------------------------------

output "file_formats" {
  value = {
    json    = "${var.database}.${var.schema}.${snowflake_file_format.json.name}"
    parquet = "${var.database}.${var.schema}.${snowflake_file_format.parquet.name}"
    csv     = "${var.database}.${var.schema}.${snowflake_file_format.csv.name}"
  }
  description = "Fully qualified file format names"
}

# -----------------------------------------------------------------------------
# Tables
# -----------------------------------------------------------------------------

output "events_table" {
  value       = "${var.database}.${var.schema}.${snowflake_table.events.name}"
  description = "Fully qualified events table name"
}

# -----------------------------------------------------------------------------
# Connection info for downstream tools
# -----------------------------------------------------------------------------

output "airflow_config" {
  value = {
    warehouse = snowflake_warehouse.loading.name
    database  = var.database
    schema    = var.schema
    stage     = local.events_stage_name
  }
  description = "Configuration values for Airflow DAGs"
}

output "dbt_config" {
  value = {
    database = var.database
    schema   = var.schema
  }
  description = "Configuration values for dbt"
}

# -----------------------------------------------------------------------------
# Cloud-specific setup instructions
# -----------------------------------------------------------------------------

output "cloud_setup_instructions" {
  value = (
    var.cloud_provider == "aws" ? <<-EOT
      AWS S3 Setup Required:
      1. Update IAM role trust policy with:
         - AWS IAM User ARN: ${try(snowflake_storage_integration.s3[0].storage_aws_iam_user_arn, "N/A")}
         - External ID: (see sensitive output s3_integration_aws_external_id)
      2. Configure S3 event notification to SQS:
         - Queue ARN: ${try(snowflake_pipe.events_s3[0].notification_channel, "N/A")}
    EOT
    :
    var.cloud_provider == "gcp" ? <<-EOT
      GCS Setup Required:
      1. Grant GCS bucket access to service account:
         - Service Account: ${try(snowflake_storage_integration.gcs[0].storage_gcp_service_account, "N/A")}
      2. Configure Pub/Sub notification:
         - Subscription: ${try(snowflake_pipe.events_gcs[0].notification_channel, "N/A")}
    EOT
    :
    var.cloud_provider == "azure" ? <<-EOT
      Azure Setup Required:
      1. Complete Azure AD consent flow:
         - Consent URL: ${try(snowflake_storage_integration.azure[0].azure_consent_url, "N/A")}
      2. Configure Event Grid to Storage Queue:
         - Queue URL: ${try(snowflake_pipe.events_azure[0].notification_channel, "N/A")}
    EOT
    :
    "No cloud-specific setup required."
  )
  description = "Instructions for completing cloud provider setup"
}
