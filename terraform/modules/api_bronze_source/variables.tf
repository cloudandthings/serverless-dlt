variable "source_id" {
  description = "Source id: the config lands at <config_prefix>/<source_id>.json and scheduled events carry it."
  type        = string
}

variable "config" {
  description = "The api_bronze config document (decoded JSON, docs/api/configuration.md). bucket_url, dataset_name, pipeline_name, quarantine_url and the client's secret references drive the IAM grants."
  type        = any

  validation {
    condition = alltrue([
      for k in ["pipeline_name", "dataset_name", "bucket_url", "resources"] : can(var.config[k])
    ])
    error_message = "config must have pipeline_name, dataset_name, bucket_url and resources."
  }

  validation {
    condition     = startswith(try(var.config.bucket_url, ""), "s3://")
    error_message = "config.bucket_url must be an s3:// URL for a Lambda deployment."
  }
}

variable "function_name" {
  description = "Lambda function name. Default: api-bronze-<source_id>, underscores as dashes."
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
  description = "api_bronze image in ECR (docker/Dockerfile.api). One image serves every API source."
  type        = string
}

variable "architecture" {
  description = "arm64 or x86_64; must match the platform the image was built for (docker:build -p)."
  type        = string
  default     = "arm64"
}

variable "memory_mb" {
  description = "A page is held several times over (bytes, text, parsed, Arrow): size to the largest page. Start at 1-2 GB and measure."
  type        = number
  default     = 1536
}

variable "ephemeral_storage_mb" {
  description = "/tmp size (2-10 GB): dlt buffers files there before upload."
  type        = number
  default     = 2048
}

variable "timeout_s" {
  description = "Split loading uses the remaining time; unsorted APIs must finish within it."
  type        = number
  default     = 900
}

variable "safety_margin_s" {
  description = "SAFETY_MARGIN_S."
  type        = number
  default     = 120
}

variable "reserved_concurrency" {
  description = "Keep 1 for any function running incremental loads. Null only for a dedicated parallel-backfill function whose config has its own pipeline_name."
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

variable "extra_secret_ids" {
  description = "Secrets beyond the {\"secret_id\": ...} references found directly under client.auth and client.headers (e.g. nested ones)."
  type        = list(string)
  default     = []
}

variable "kms_key_arns" {
  description = "Customer managed keys the function needs (bronze bucket, secrets)."
  type        = list(string)
  default     = []
}

variable "schedules" {
  description = "EventBridge Scheduler schedules: { name = { expression, event (JSON string; default {\"source_id\": ...}), loop, timezone, enabled } }. loop=true needs every resource sorted_ascending (split loading)."
  type = map(object({
    expression = string
    event      = optional(string)
    loop       = optional(bool, false)
    timezone   = optional(string, "UTC")
    enabled    = optional(bool, true)
  }))
  default = {}

  validation {
    condition = !anytrue([for s in values(var.schedules) : s.loop]) || alltrue([
      for r in try(var.config.resources, []) : try(r.incremental.sorted_ascending, false)
    ])
    error_message = "loop=true needs every resource to be incremental with sorted_ascending=true: split loading is off otherwise, and one run must finish inside the timeout."
  }
}

variable "max_loop_iterations" {
  description = "Split-loading loop cap."
  type        = number
  default     = 48
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
