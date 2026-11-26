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

output "security_group_id" {
  description = "The function's security group: allow it in the database's ingress."
  value       = aws_security_group.function.id
}

output "loop_state_machine_arn" {
  description = "Split-loading state machine ARN (null when no schedule loops)."
  value       = module.function.loop_state_machine_arn
}

output "snapshot_state_machine_arn" {
  description = "Paged-snapshot state machine ARN (null unless snapshot_state_machine)."
  value       = try(aws_sfn_state_machine.snapshot[0].arn, null)
}

output "grants" {
  description = "What the function role was granted, derived from the config: S3 prefixes (list/read/write/delete) and secret ids."
  value = {
    s3_data_urls = local.s3_data_urls
    secret_ids   = local.secret_ids
  }
}

output "config_s3_uri" {
  description = "Where the config document was uploaded."
  value       = module.function.config_s3_uri
}
