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
  description = "Tag of the db_bronze image (linux/amd64) pushed to the platform's ECR repository."
  type        = string
}

variable "vpc_id" {
  description = "VPC with a route to the database."
  type        = string
}

variable "subnet_ids" {
  description = "Private subnets for the functions, with a path to S3 and Secrets Manager (endpoints or NAT)."
  type        = list(string)
}

variable "db_cidr_blocks" {
  description = "CIDR blocks of the database server."
  type        = list(string)
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
