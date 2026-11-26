output "api_repository_url" {
  description = "Push the api_bronze image here (see README.md)."
  value       = module.platform.api_repository_url
}

output "bronze_bucket" {
  description = "Bronze bucket."
  value       = module.platform.bronze_bucket
}

output "config_uri" {
  description = "CONFIG_URI shared by every function."
  value       = module.platform.config_uri
}

output "functions" {
  description = "Function name per source."
  value = {
    vendor_orders          = module.vendor_orders.function_name
    vendor_countries       = module.vendor_countries.function_name
    vendor_orders_backfill = module.vendor_orders_backfill.function_name
  }
}

output "grants" {
  description = "S3 prefixes and secrets each function was granted, derived from its config."
  value = {
    vendor_orders          = module.vendor_orders.grants
    vendor_countries       = module.vendor_countries.grants
    vendor_orders_backfill = module.vendor_orders_backfill.grants
  }
}

output "vendor_orders_loop_arn" {
  description = "Split-loading state machine of vendor_orders."
  value       = module.vendor_orders.loop_state_machine_arn
}
