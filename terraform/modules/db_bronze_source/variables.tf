variable "source_id" {
  description = "Source id: the config lands at <config_prefix>/<source_id>.json and scheduled events carry it."
  type        = string
}

variable "config" {
  description = "The db_bronze config document (decoded JSON, docs/db/configuration.md). bucket_url, dataset_name, connection secrets and tables drive IAM, networking and alarms."
  type        = any

  validation {
    condition = alltrue([
      for k in ["pipeline_name", "dataset_name", "bucket_url", "connection", "tables"] : can(var.config[k])
    ])
    error_message = "config must have pipeline_name, dataset_name, bucket_url, connection and tables."
  }

  validation {
    condition     = startswith(try(var.config.bucket_url, ""), "s3://")
    error_message = "config.bucket_url must be an s3:// URL for a Lambda deployment."
  }
}

variable "function_name" {
  description = "Lambda function name. Default: db-bronze-<source_id>, underscores as dashes."
  type        = string
  default     = null
}

variable "config_bucket" {
  description = "Config bucket (bronze_platform output config_bucket)."
  type        = string
}

variable "config_prefix" {
  description = "Config key prefix (bronze_platform output config_prefix)."
  type        = string
}

variable "image_uri" {
  description = "db_bronze image in ECR (docker/Dockerfile.db, linux/amd64 only: the function is x86_64)."
  type        = string
}

variable "memory_mb" {
  description = "Size from a measured validate-plus-run on the largest table (docs/deployment.md \"Memory sizing\"); ~1 GB covers the fixed cost."
  type        = number
  default     = 1024
}

variable "ephemeral_storage_mb" {
  description = "/tmp size: dlt buffers files there before upload."
  type        = number
  default     = 2048
}

variable "timeout_s" {
  description = "Longer than SAFETY_MARGIN_S plus the slowest page."
  type        = number
  default     = 900
}

variable "safety_margin_s" {
  description = "SAFETY_MARGIN_S."
  type        = number
  default     = 120
}

variable "reserved_concurrency" {
  description = "Keep 1 for incremental functions. Null only for a paged-snapshot or parallel-backfill function whose config has its own pipeline_name (docs/deployment.md \"Orchestration\")."
  type        = number
  default     = 1
  nullable    = true
}

variable "log_level" {
  description = "LOG_LEVEL."
  type        = string
  default     = "INFO"
}

variable "log_retention_days" {
  description = "CloudWatch log retention."
  type        = number
  default     = 30
}

# --- Network --------------------------------------------------------------------------

variable "vpc_id" {
  description = "VPC with a route to the database."
  type        = string
}

variable "subnet_ids" {
  description = "Private subnets for the function. They also need a path to S3 and Secrets Manager: VPC endpoints (gateway for S3, interface for Secrets Manager) or a NAT gateway."
  type        = list(string)
}

variable "db_port" {
  description = "Database port. Default: connection.port from the config."
  type        = number
  default     = null
}

variable "db_cidr_blocks" {
  description = "CIDR blocks of the database, for the egress rule. Give this or db_security_group_id."
  type        = list(string)
  default     = []
}

variable "db_security_group_id" {
  description = "The database's security group, for the egress rule. Its ingress must allow this function's security group (output security_group_id)."
  type        = string
  default     = null

  validation {
    condition     = var.db_security_group_id != null || length(var.db_cidr_blocks) > 0
    error_message = "Give db_cidr_blocks or db_security_group_id so the function can reach the database."
  }
}

variable "https_egress_cidr_blocks" {
  description = "HTTPS egress for S3, Secrets Manager and CloudWatch (endpoints or NAT)."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "extra_security_group_ids" {
  description = "Further security groups for the function."
  type        = list(string)
  default     = []
}

# --- Access, orchestration, alarms ---------------------------------------------------

variable "extra_secret_ids" {
  description = "Secrets beyond the {\"secret_id\": ...} references found directly under connection."
  type        = list(string)
  default     = []
}

variable "kms_key_arns" {
  description = "Customer managed keys the function needs (bronze bucket, secrets)."
  type        = list(string)
  default     = []
}

variable "schedules" {
  description = "EventBridge Scheduler schedules: { name = { expression, event (JSON string; default {\"source_id\": ...}), loop, timezone, enabled } }. loop=true re-invokes an incremental run while is_empty is false."
  type = map(object({
    expression = string
    event      = optional(string)
    loop       = optional(bool, false)
    timezone   = optional(string, "UTC")
    enabled    = optional(bool, true)
  }))
  default = {}
}

variable "max_loop_iterations" {
  description = "Split-loading loop cap."
  type        = number
  default     = 48
}

variable "snapshot_state_machine" {
  description = "Create the paged-snapshot state machine (plan -> Map over pages -> finalize; examples/db_snapshot.asl.json). Only for a function whose config has its own pipeline_name and reserved_concurrency = null."
  type        = bool
  default     = false

  validation {
    condition     = !var.snapshot_state_machine || var.reserved_concurrency == null || coalesce(var.reserved_concurrency, 0) >= var.snapshot_max_concurrency
    error_message = "Snapshot pages run in parallel: set reserved_concurrency to null (or at least snapshot_max_concurrency), with a pipeline_name used by no incremental."
  }
}

variable "snapshot_max_concurrency" {
  description = "Pages in flight at once. Bounds the load on the source database: deep OFFSETs get slower."
  type        = number
  default     = 4
}

variable "alarm_actions" {
  description = "SNS topic ARNs notified by the alarms."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags for every resource."
  type        = map(string)
  default     = {}
}
