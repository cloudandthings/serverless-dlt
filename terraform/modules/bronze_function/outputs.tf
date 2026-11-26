output "function_name" {
  description = "Lambda function name."
  value       = aws_lambda_function.this.function_name
}

output "function_arn" {
  description = "Lambda function ARN."
  value       = aws_lambda_function.this.arn
}

output "role_name" {
  description = "Function execution role name (attach extra policies here)."
  value       = aws_iam_role.function.name
}

output "role_arn" {
  description = "Function execution role ARN."
  value       = aws_iam_role.function.arn
}

output "log_group_name" {
  description = "Function log group."
  value       = aws_cloudwatch_log_group.this.name
}

output "loop_state_machine_arn" {
  description = "Split-loading state machine ARN (null when no schedule loops)."
  value       = try(aws_sfn_state_machine.loop[0].arn, null)
}

output "config_s3_uri" {
  description = "Where the config document was uploaded."
  value       = "s3://${var.config_bucket}/${local.config_key}"
}
