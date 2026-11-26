# Internal module: what api_bronze_source and db_bronze_source share. Use those wrappers;
# they derive the S3 grants from the source config and set the package-specific defaults.

variable "function_name" {
  description = "Lambda function name; also names the role, log group, schedules and state machine."
  type        = string
}

variable "source_id" {
  description = "The config is uploaded to <config_prefix>/<source_id>.json and events carry this source_id."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9_][A-Za-z0-9_.-]*$", var.source_id))
    error_message = "source_id must be a plain name: no '/', not starting with '.'."
  }
}

variable "config" {
  description = "The source config document (decoded JSON). Uploaded as-is; the handler validates it."
  type        = any
}

variable "config_bucket" {
  description = "Bucket holding the configs."
  type        = string
}

variable "config_prefix" {
  description = "Key prefix of the configs in config_bucket (CONFIG_URI = s3://<bucket>/<prefix>)."
  type        = string
}

variable "image_uri" {
  description = "Container image URI (ECR), built from docker/Dockerfile.api or docker/Dockerfile.db."
  type        = string
}

variable "architecture" {
  description = "arm64 or x86_64. Must match the image's platform."
  type        = string

  validation {
    condition     = contains(["arm64", "x86_64"], var.architecture)
    error_message = "architecture must be arm64 or x86_64."
  }
}

variable "memory_mb" {
  description = "Function memory. Size from a measured run on the largest page/table."
  type        = number
}

variable "ephemeral_storage_mb" {
  description = "/tmp size: dlt buffers files there before upload."
  type        = number
}

variable "timeout_s" {
  description = "Function timeout."
  type        = number

  validation {
    condition     = var.timeout_s >= 60 && var.timeout_s <= 900
    error_message = "timeout_s must be between 60 and 900."
  }
}

variable "safety_margin_s" {
  description = "SAFETY_MARGIN_S: seconds kept free before the timeout."
  type        = number

  validation {
    condition     = var.timeout_s > var.safety_margin_s + 60
    error_message = "timeout_s must exceed safety_margin_s + 60, or split loading has no budget (the minimum budget is 60 s)."
  }
}

variable "reserved_concurrency" {
  description = "Reserved concurrency. 1 for any function running incremental loads (two concurrent runs of one pipeline both read state and the last writer wins). Null leaves the function unreserved."
  type        = number
  nullable    = true
}

variable "log_level" {
  description = "LOG_LEVEL."
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log retention."
  type        = number
}

variable "extra_environment" {
  description = "Additional environment variables."
  type        = map(string)
  default     = {}
}

variable "vpc_config" {
  description = "Subnets and security groups when the function runs in a VPC."
  type = object({
    subnet_ids         = list(string)
    security_group_ids = list(string)
  })
  default = null
}

variable "s3_data_urls" {
  description = "s3://bucket/prefix URLs the function lists, reads, writes and deletes under."
  type        = list(string)

  validation {
    condition     = alltrue([for u in var.s3_data_urls : can(regex("^s3://[^/]+", u))])
    error_message = "Every s3_data_urls entry must be an s3:// URL."
  }
}

variable "secret_ids" {
  description = "Secrets Manager secret names or ARNs referenced by the config ({\"secret_id\": ...})."
  type        = list(string)
}

variable "kms_key_arns" {
  description = "KMS keys the function may use (bucket CMK, secrets CMK)."
  type        = list(string)
}

variable "schedules" {
  description = "EventBridge Scheduler schedules. event is the Lambda event as a JSON string. loop=true runs the event through the split-loading state machine, which re-invokes while is_empty is false; only incremental events may loop."
  type = map(object({
    expression = string
    event      = string
    loop       = optional(bool, false)
    timezone   = optional(string, "UTC")
    enabled    = optional(bool, true)
  }))

  validation {
    condition     = alltrue([for s in values(var.schedules) : can(jsondecode(s.event))])
    error_message = "Every schedule event must be a JSON string (use jsonencode)."
  }

  validation {
    condition = alltrue([
      for s in values(var.schedules) : !s.loop || try(jsondecode(s.event).mode, "incremental") == "incremental"
    ])
    error_message = "Only incremental events may loop: a full or backfill run returns is_empty=false whenever it lands rows, so the loop would re-run it."
  }
}

variable "max_loop_iterations" {
  description = "Split-loading loop cap. Reaching it fails the execution (MaxIterationsReached), which alarms."
  type        = number
}

variable "alarm_actions" {
  description = "SNS topic ARNs (or other alarm actions) notified by every alarm."
  type        = list(string)
}

variable "tags" {
  description = "Tags for every resource."
  type        = map(string)
}
