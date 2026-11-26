output "function_name" {
  description = "Lambda function name."
  value       = module.function.function_name
}

output "function_arn" {
  description = "Lambda function ARN."
  value       = module.function.function_arn
}

output "role_name" {
  description = "Function execution role name."
  value       = module.function.role_name
}

output "loop_state_machine_arn" {
  description = "Split-loading state machine ARN (null when no schedule loops)."
  value       = module.function.loop_state_machine_arn
}

output "config_s3_uri" {
  description = "Where the config document was uploaded."
  value       = module.function.config_s3_uri
}

output "grants" {
  description = "What the function role was granted, derived from the config: S3 prefixes (list/read/write/delete) and secret ids."
  value = {
    s3_data_urls = local.s3_data_urls
    secret_ids   = local.secret_ids
  }
}

output "quarantine_uri" {
  description = "Where this source's broken pages are written."
  value       = "${local.quarantine_url}/_quarantine/${local.pipeline_name}/"
}
