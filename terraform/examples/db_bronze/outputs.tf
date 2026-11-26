output "db_repository_url" {
  description = "Push the db_bronze image here (see README.md)."
  value       = module.platform.db_repository_url
}

output "bronze_bucket" {
  description = "Bronze bucket."
  value       = module.platform.bronze_bucket
}

output "config_uri" {
  description = "CONFIG_URI shared by every function."
  value       = module.platform.config_uri
}

output "function_security_group_ids" {
  description = "Allow these in the database's ingress."
  value = {
    core_banking          = module.core_banking.security_group_id
    core_banking_snapshot = module.core_banking_snapshot.security_group_id
  }
}

output "grants" {
  description = "S3 prefixes and secrets each function was granted, derived from its config."
  value = {
    core_banking          = module.core_banking.grants
    core_banking_snapshot = module.core_banking_snapshot.grants
  }
}

output "snapshot_state_machine_arn" {
  description = "Start a paged snapshot here."
  value       = module.core_banking_snapshot.snapshot_state_machine_arn
}
