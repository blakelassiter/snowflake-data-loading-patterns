# =============================================================================
# Storage Integrations
# =============================================================================
# Storage integrations allow Snowflake to access cloud storage securely
# without embedding credentials in stage definitions.
#
# IMPORTANT: As of Snowflake Terraform provider v1.x (late 2024+), the
# snowflake_storage_integration resource is a preview feature. You must add
# "snowflake_storage_integration_resource" to preview_features_enabled in
# your provider configuration. See README.md for details.

# -----------------------------------------------------------------------------
# AWS S3 Storage Integration
# -----------------------------------------------------------------------------

resource "snowflake_storage_integration" "s3" {
  count = var.cloud_provider == "aws" ? 1 : 0

  name    = "${upper(var.environment)}_S3_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider         = "S3"
  storage_aws_role_arn     = "arn:aws:iam::${var.aws_account_id}:role/${var.s3_iam_role_name}"
  storage_allowed_locations = [
    "s3://${var.s3_bucket_name}/${var.s3_bucket_path}"
  ]

  comment = "Storage integration for ${var.environment} data loading"
}

# Output the AWS IAM user ARN and external ID for trust policy configuration
output "s3_integration_aws_iam_user_arn" {
  value       = var.cloud_provider == "aws" ? snowflake_storage_integration.s3[0].storage_aws_iam_user_arn : null
  description = "AWS IAM user ARN to add to S3 bucket policy"
}

output "s3_integration_aws_external_id" {
  value       = var.cloud_provider == "aws" ? snowflake_storage_integration.s3[0].storage_aws_external_id : null
  description = "External ID for IAM role trust policy"
  sensitive   = true
}

# -----------------------------------------------------------------------------
# GCS Storage Integration
# -----------------------------------------------------------------------------

resource "snowflake_storage_integration" "gcs" {
  count = var.cloud_provider == "gcp" ? 1 : 0

  name    = "${upper(var.environment)}_GCS_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider          = "GCS"
  storage_allowed_locations = [
    "gcs://${var.gcs_bucket_name}/${var.gcs_bucket_path}"
  ]

  comment = "Storage integration for ${var.environment} data loading"
}

output "gcs_integration_service_account" {
  value       = var.cloud_provider == "gcp" ? snowflake_storage_integration.gcs[0].storage_gcp_service_account : null
  description = "GCS service account to grant bucket access"
}

# -----------------------------------------------------------------------------
# Azure Storage Integration
# -----------------------------------------------------------------------------

resource "snowflake_storage_integration" "azure" {
  count = var.cloud_provider == "azure" ? 1 : 0

  name    = "${upper(var.environment)}_AZURE_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider            = "AZURE"
  azure_tenant_id             = var.azure_tenant_id
  storage_allowed_locations   = [
    "azure://${var.azure_storage_account}.blob.core.windows.net/${var.azure_container_name}/${var.azure_path}"
  ]

  comment = "Storage integration for ${var.environment} data loading"
}

output "azure_integration_consent_url" {
  value       = var.cloud_provider == "azure" ? snowflake_storage_integration.azure[0].azure_consent_url : null
  description = "URL for Azure AD consent flow"
}

output "azure_integration_app_id" {
  value       = var.cloud_provider == "azure" ? snowflake_storage_integration.azure[0].azure_multi_tenant_app_name : null
  description = "Azure AD app ID for consent"
}

# -----------------------------------------------------------------------------
# Grant integration usage to roles
# -----------------------------------------------------------------------------
# Note: snowflake_integration_grant is deprecated in provider v1.x
# Use snowflake_grant_privileges_to_account_role instead

resource "snowflake_grant_privileges_to_account_role" "storage_usage" {
  for_each = toset(var.integration_grant_roles)
  
  account_role_name = each.value
  privileges        = ["USAGE"]
  
  on_account_object {
    object_type = "INTEGRATION"
    object_name = local.storage_integration_name
  }

  depends_on = [
    snowflake_storage_integration.s3,
    snowflake_storage_integration.gcs,
    snowflake_storage_integration.azure
  ]
}

locals {
  storage_integration_name = (
    var.cloud_provider == "aws" ? snowflake_storage_integration.s3[0].name :
    var.cloud_provider == "gcp" ? snowflake_storage_integration.gcs[0].name :
    var.cloud_provider == "azure" ? snowflake_storage_integration.azure[0].name :
    null
  )
}
