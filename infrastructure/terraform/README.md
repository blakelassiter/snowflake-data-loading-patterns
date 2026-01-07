# Terraform

Terraform modules for Snowflake data loading infrastructure.

## Files

| File | Purpose |
|------|---------|
| [storage-integration.tf](storage-integration.tf) | S3/GCS/Azure storage integrations |
| [stages.tf](stages.tf) | External stages pointing to cloud storage |
| [snowpipe.tf](snowpipe.tf) | Snowpipe with auto-ingest |
| [warehouses.tf](warehouses.tf) | Loading warehouses with appropriate sizing |
| [variables.tf](variables.tf) | Input variables |
| [outputs.tf](outputs.tf) | Output values for downstream use |

## Prerequisites

```hcl
# Install Snowflake provider (note: provider moved to snowflakedb namespace in 2024)
terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
  }
}

# Provider configuration
provider "snowflake" {
  account  = var.snowflake_account
  user     = var.snowflake_user
  role     = var.snowflake_role
  
  # Authentication: use one of these methods
  # Option 1: Key-pair (recommended for automation)
  private_key = var.snowflake_private_key
  
  # Option 2: Password (simpler but less secure)
  # password = var.snowflake_password
  
  # Preview features: storage_integration requires this as of provider v1.x
  preview_features_enabled = ["snowflake_storage_integration_resource"]
}
```

> **Note:** The `snowflake_storage_integration` resource is currently a preview feature in the Snowflake Terraform provider v1.x. You must enable it via `preview_features_enabled` in the provider configuration. See [provider documentation](https://registry.terraform.io/providers/snowflakedb/snowflake/latest/docs) for current status.

## Usage

```bash
# Initialize
terraform init

# Plan changes
terraform plan -var-file="prod.tfvars"

# Apply
terraform apply -var-file="prod.tfvars"
```

## Example tfvars

```hcl
# prod.tfvars
environment     = "prod"
snowflake_role  = "DATA_ENGINEER"
aws_account_id  = "123456789012"
s3_bucket_name  = "company-data-lake"
s3_bucket_path  = "raw/"
```

## Module Structure

For larger deployments, consider organizing as modules:

```
terraform/
├── modules/
│   ├── storage-integration/
│   ├── snowpipe/
│   └── loading-warehouse/
├── environments/
│   ├── dev/
│   ├── staging/
│   └── prod/
└── main.tf
```

## State Management

For team use, configure remote state:

```hcl
terraform {
  backend "s3" {
    bucket = "terraform-state-bucket"
    key    = "snowflake/data-loading/terraform.tfstate"
    region = "us-east-1"
  }
}
```

## Related

- [Snowpipe patterns](../../patterns/snowpipe/)
- [Cost comparison](../../decision-framework/cost-comparison.md)
