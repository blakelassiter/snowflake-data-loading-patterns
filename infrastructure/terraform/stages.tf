# =============================================================================
# External Stages
# =============================================================================
# Stages point to cloud storage locations and define file formats.

# -----------------------------------------------------------------------------
# File Formats
# -----------------------------------------------------------------------------

resource "snowflake_file_format" "json" {
  name        = "${upper(var.environment)}_JSON_FORMAT"
  database    = var.database
  schema      = var.schema
  format_type = "JSON"

  compression              = "AUTO"
  strip_outer_array        = false
  enable_octal             = false
  allow_duplicate          = false
  strip_null_values        = false
  ignore_utf8_errors       = false
  skip_byte_order_mark     = true

  comment = "Standard JSON format for ${var.environment}"
}

resource "snowflake_file_format" "parquet" {
  name        = "${upper(var.environment)}_PARQUET_FORMAT"
  database    = var.database
  schema      = var.schema
  format_type = "PARQUET"

  compression = "AUTO"

  comment = "Standard Parquet format for ${var.environment}"
}

resource "snowflake_file_format" "csv" {
  name        = "${upper(var.environment)}_CSV_FORMAT"
  database    = var.database
  schema      = var.schema
  format_type = "CSV"

  compression         = "AUTO"
  field_delimiter     = ","
  record_delimiter    = "\n"
  skip_header         = 1
  field_optionally_enclosed_by = "\""
  trim_space          = true
  error_on_column_count_mismatch = true
  escape_unenclosed_field = "\\"
  null_if             = ["NULL", "null", ""]

  comment = "Standard CSV format for ${var.environment}"
}

# -----------------------------------------------------------------------------
# External Stages - AWS S3
# -----------------------------------------------------------------------------

resource "snowflake_stage" "s3_events" {
  count = var.cloud_provider == "aws" ? 1 : 0

  name        = "${upper(var.environment)}_EVENTS_STAGE"
  database    = var.database
  schema      = var.schema
  url         = "s3://${var.s3_bucket_name}/${var.s3_bucket_path}events/"

  storage_integration = snowflake_storage_integration.s3[0].name
  file_format         = "FORMAT_NAME = ${var.database}.${var.schema}.${snowflake_file_format.json.name}"

  comment = "Events data stage for ${var.environment}"
}

resource "snowflake_stage" "s3_customers" {
  count = var.cloud_provider == "aws" ? 1 : 0

  name        = "${upper(var.environment)}_CUSTOMERS_STAGE"
  database    = var.database
  schema      = var.schema
  url         = "s3://${var.s3_bucket_name}/${var.s3_bucket_path}customers/"

  storage_integration = snowflake_storage_integration.s3[0].name
  file_format         = "FORMAT_NAME = ${var.database}.${var.schema}.${snowflake_file_format.parquet.name}"

  comment = "Customer data stage for ${var.environment}"
}

# -----------------------------------------------------------------------------
# External Stages - GCS
# -----------------------------------------------------------------------------

resource "snowflake_stage" "gcs_events" {
  count = var.cloud_provider == "gcp" ? 1 : 0

  name        = "${upper(var.environment)}_EVENTS_STAGE"
  database    = var.database
  schema      = var.schema
  url         = "gcs://${var.gcs_bucket_name}/${var.gcs_bucket_path}events/"

  storage_integration = snowflake_storage_integration.gcs[0].name
  file_format         = "FORMAT_NAME = ${var.database}.${var.schema}.${snowflake_file_format.json.name}"

  comment = "Events data stage for ${var.environment}"
}

# -----------------------------------------------------------------------------
# External Stages - Azure
# -----------------------------------------------------------------------------

resource "snowflake_stage" "azure_events" {
  count = var.cloud_provider == "azure" ? 1 : 0

  name        = "${upper(var.environment)}_EVENTS_STAGE"
  database    = var.database
  schema      = var.schema
  url         = "azure://${var.azure_storage_account}.blob.core.windows.net/${var.azure_container_name}/${var.azure_path}events/"

  storage_integration = snowflake_storage_integration.azure[0].name
  file_format         = "FORMAT_NAME = ${var.database}.${var.schema}.${snowflake_file_format.json.name}"

  comment = "Events data stage for ${var.environment}"
}

# -----------------------------------------------------------------------------
# Stage grants
# -----------------------------------------------------------------------------

resource "snowflake_stage_grant" "events_stage_usage" {
  database_name = var.database
  schema_name   = var.schema
  stage_name    = local.events_stage_name

  privilege = "USAGE"
  roles     = var.stage_grant_roles

  depends_on = [
    snowflake_stage.s3_events,
    snowflake_stage.gcs_events,
    snowflake_stage.azure_events
  ]
}

locals {
  events_stage_name = (
    var.cloud_provider == "aws" ? snowflake_stage.s3_events[0].name :
    var.cloud_provider == "gcp" ? snowflake_stage.gcs_events[0].name :
    var.cloud_provider == "azure" ? snowflake_stage.azure_events[0].name :
    null
  )
}
