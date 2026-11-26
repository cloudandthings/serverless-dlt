output "api_repository_url" {
  description = "ECR repository URL for the api_bronze image (null when not created)."
  value       = try(aws_ecr_repository.this["api"].repository_url, null)
}

output "db_repository_url" {
  description = "ECR repository URL for the db_bronze image (null when not created)."
  value       = try(aws_ecr_repository.this["db"].repository_url, null)
}

output "bronze_bucket" {
  description = "Bronze bucket name. Sources' bucket_url is s3://<this>/<prefix>."
  value       = aws_s3_bucket.this["bronze"].id
}

output "bronze_bucket_arn" {
  description = "Bronze bucket ARN."
  value       = aws_s3_bucket.this["bronze"].arn
}

output "config_bucket" {
  description = "Config bucket name."
  value       = aws_s3_bucket.this["config"].id
}

output "config_bucket_arn" {
  description = "Config bucket ARN."
  value       = aws_s3_bucket.this["config"].arn
}

output "config_prefix" {
  description = "Key prefix of the source configs in the config bucket."
  value       = var.config_prefix
}

output "config_uri" {
  description = "CONFIG_URI for every function: configs are read from <config_uri>/<source_id>.json."
  value       = "s3://${aws_s3_bucket.this["config"].id}/${var.config_prefix}"
}
