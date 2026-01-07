# =============================================================================
# Variables
# =============================================================================

# -----------------------------------------------------------------------------
# General
# -----------------------------------------------------------------------------

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "cloud_provider" {
  description = "Cloud provider for storage (aws, gcp, azure)"
  type        = string

  validation {
    condition     = contains(["aws", "gcp", "azure"], var.cloud_provider)
    error_message = "Cloud provider must be aws, gcp, or azure."
  }
}

# -----------------------------------------------------------------------------
# Snowflake
# -----------------------------------------------------------------------------

variable "database" {
  description = "Snowflake database name"
  type        = string
  default     = "RAW"
}

variable "schema" {
  description = "Snowflake schema name"
  type        = string
  default     = "LOADING"
}

# -----------------------------------------------------------------------------
# AWS S3
# -----------------------------------------------------------------------------

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
  default     = ""
}

variable "s3_bucket_name" {
  description = "S3 bucket name"
  type        = string
  default     = ""
}

variable "s3_bucket_path" {
  description = "S3 bucket path prefix"
  type        = string
  default     = ""
}

variable "s3_iam_role_name" {
  description = "IAM role name for Snowflake access"
  type        = string
  default     = "snowflake-data-loading"
}

# -----------------------------------------------------------------------------
# GCS
# -----------------------------------------------------------------------------

variable "gcs_bucket_name" {
  description = "GCS bucket name"
  type        = string
  default     = ""
}

variable "gcs_bucket_path" {
  description = "GCS bucket path prefix"
  type        = string
  default     = ""
}

# -----------------------------------------------------------------------------
# Azure
# -----------------------------------------------------------------------------

variable "azure_tenant_id" {
  description = "Azure tenant ID"
  type        = string
  default     = ""
}

variable "azure_storage_account" {
  description = "Azure storage account name"
  type        = string
  default     = ""
}

variable "azure_container_name" {
  description = "Azure container name"
  type        = string
  default     = ""
}

variable "azure_path" {
  description = "Azure blob path prefix"
  type        = string
  default     = ""
}

# -----------------------------------------------------------------------------
# Snowpipe
# -----------------------------------------------------------------------------

variable "enable_snowpipe" {
  description = "Enable Snowpipe auto-ingest"
  type        = bool
  default     = true
}

# -----------------------------------------------------------------------------
# Warehouses
# -----------------------------------------------------------------------------

variable "loading_warehouse_size" {
  description = "Size of loading warehouse"
  type        = string
  default     = "MEDIUM"

  validation {
    condition = contains([
      "XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE", "X4LARGE"
    ], upper(var.loading_warehouse_size))
    error_message = "Invalid warehouse size."
  }
}

variable "loading_warehouse_max_clusters" {
  description = "Maximum clusters for loading warehouse"
  type        = number
  default     = 2
}

# -----------------------------------------------------------------------------
# Resource Monitor
# -----------------------------------------------------------------------------

variable "create_resource_monitor" {
  description = "Create a resource monitor for loading"
  type        = bool
  default     = false
}

variable "monthly_credit_quota" {
  description = "Monthly credit quota for resource monitor"
  type        = number
  default     = 100
}

variable "resource_monitor_name" {
  description = "Existing resource monitor name (if not creating new)"
  type        = string
  default     = null
}

variable "resource_monitor_notify_users" {
  description = "Users to notify when resource monitor triggers"
  type        = list(string)
  default     = []
}

# -----------------------------------------------------------------------------
# Grants
# -----------------------------------------------------------------------------

variable "integration_grant_roles" {
  description = "Roles to grant storage integration usage"
  type        = list(string)
  default     = ["DATA_ENGINEER"]
}

variable "stage_grant_roles" {
  description = "Roles to grant stage usage"
  type        = list(string)
  default     = ["DATA_ENGINEER"]
}

variable "pipe_grant_roles" {
  description = "Roles to grant pipe operate"
  type        = list(string)
  default     = ["DATA_ENGINEER"]
}

variable "warehouse_grant_roles" {
  description = "Roles to grant warehouse usage"
  type        = list(string)
  default     = ["DATA_ENGINEER", "ANALYST"]
}

variable "warehouse_admin_roles" {
  description = "Roles to grant warehouse operate/modify"
  type        = list(string)
  default     = ["DATA_ENGINEER"]
}
