variable "name_prefix" {
  description = "Prefix for every resource name (ECR repositories, buckets)."
  type        = string
}

variable "bronze_bucket_name" {
  description = "Bronze bucket name. Default: <name_prefix>-bronze. Part of every source's bucket_url, which is part of its dlt state identity: never rename it under running sources."
  type        = string
  default     = null
}

variable "config_bucket_name" {
  description = "Bucket holding source configs (CONFIG_URI). Default: <name_prefix>-config."
  type        = string
  default     = null
}

variable "config_prefix" {
  description = "Key prefix under the config bucket; CONFIG_URI becomes s3://<config bucket>/<config_prefix>."
  type        = string
  default     = "sources"
}

variable "kms_key_arn" {
  description = "Customer managed KMS key for both buckets. Null uses SSE-S3. With a CMK, grant every function kms:Decrypt and kms:GenerateDataKey on it."
  type        = string
  default     = null
}

variable "create_api_repository" {
  description = "Create the ECR repository for the api_bronze image (docker/Dockerfile.api)."
  type        = bool
  default     = true
}

variable "create_db_repository" {
  description = "Create the ECR repository for the db_bronze image (docker/Dockerfile.db)."
  type        = bool
  default     = true
}

variable "ecr_keep_images" {
  description = "How many images each ECR repository keeps. Keep enough to roll back."
  type        = number
  default     = 20
}

variable "force_destroy" {
  description = "Allow destroying non-empty buckets and repositories. Keep false for anything but a sandbox: the bronze bucket holds the dlt cursor state."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags for every resource."
  type        = map(string)
  default     = {}
}
