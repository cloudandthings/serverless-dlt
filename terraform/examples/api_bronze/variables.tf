variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-west-1"
}

variable "name_prefix" {
  description = "Prefix for the shared resources; bucket names must be globally unique."
  type        = string
}

variable "image_tag" {
  description = "Tag of the api_bronze image pushed to the platform's ECR repository."
  type        = string
}

variable "architecture" {
  description = "Platform the image was built for: arm64 (docker:build default) or x86_64."
  type        = string
  default     = "arm64"
}

variable "alarm_email" {
  description = "Subscribe this address to the alarm topic (confirm the email AWS sends). Null: topic without subscribers."
  type        = string
  default     = null
}

variable "tags" {
  description = "Default tags."
  type        = map(string)
  default     = { project = "bronze" }
}
