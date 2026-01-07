# =============================================================================
# Warehouses
# =============================================================================
# Dedicated warehouses for data loading workloads.

# -----------------------------------------------------------------------------
# Loading warehouse (for COPY INTO)
# -----------------------------------------------------------------------------

resource "snowflake_warehouse" "loading" {
  name           = "${upper(var.environment)}_LOADING_WH"
  warehouse_size = var.loading_warehouse_size

  auto_suspend         = 60  # Seconds of inactivity before suspend
  auto_resume          = true
  initially_suspended  = true

  # Scaling policy for variable workloads
  min_cluster_count    = 1
  max_cluster_count    = var.loading_warehouse_max_clusters
  scaling_policy       = "STANDARD"

  # Resource monitor (optional)
  resource_monitor = var.resource_monitor_name

  comment = "Warehouse for ${var.environment} data loading operations"

  # Prevent accidental destruction
  lifecycle {
    prevent_destroy = false  # Set to true in production
  }
}

# -----------------------------------------------------------------------------
# Operations warehouse (for monitoring, maintenance)
# -----------------------------------------------------------------------------

resource "snowflake_warehouse" "operations" {
  name           = "${upper(var.environment)}_OPS_WH"
  warehouse_size = "XSMALL"

  auto_suspend         = 60
  auto_resume          = true
  initially_suspended  = true

  min_cluster_count = 1
  max_cluster_count = 1

  comment = "Warehouse for ${var.environment} operations (monitoring, alerts)"
}

# -----------------------------------------------------------------------------
# Warehouse grants
# -----------------------------------------------------------------------------

resource "snowflake_warehouse_grant" "loading_usage" {
  warehouse_name = snowflake_warehouse.loading.name
  privilege      = "USAGE"
  roles          = var.warehouse_grant_roles
}

resource "snowflake_warehouse_grant" "loading_operate" {
  warehouse_name = snowflake_warehouse.loading.name
  privilege      = "OPERATE"
  roles          = var.warehouse_admin_roles
}

resource "snowflake_warehouse_grant" "ops_usage" {
  warehouse_name = snowflake_warehouse.operations.name
  privilege      = "USAGE"
  roles          = var.warehouse_grant_roles
}

# -----------------------------------------------------------------------------
# Resource monitor (optional)
# -----------------------------------------------------------------------------

resource "snowflake_resource_monitor" "loading" {
  count = var.create_resource_monitor ? 1 : 0

  name         = "${upper(var.environment)}_LOADING_MONITOR"
  credit_quota = var.monthly_credit_quota

  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"

  # Notification thresholds
  notify_triggers = [75, 90, 100]

  # Suspend triggers
  suspend_trigger      = 100
  suspend_immediate_trigger = 110

  # Notify users
  notify_users = var.resource_monitor_notify_users
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "loading_warehouse_name" {
  value       = snowflake_warehouse.loading.name
  description = "Name of the loading warehouse"
}

output "operations_warehouse_name" {
  value       = snowflake_warehouse.operations.name
  description = "Name of the operations warehouse"
}
