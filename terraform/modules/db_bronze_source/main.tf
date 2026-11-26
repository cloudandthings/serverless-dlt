# One database source = one config document + one VPC function (docs/deployment.md).
# The image is x86_64 only: ibm_db has no aarch64 wheel.

locals {
  function_name = coalesce(var.function_name, "db-bronze-${replace(var.source_id, "_", "-")}")
  pipeline_name = var.config.pipeline_name

  # Data, _dlt_loads, _dlt_pipeline_state and _manifests all live under <bucket_url>/<dataset_name>.
  s3_data_urls = ["${trimsuffix(var.config.bucket_url, "/")}/${var.config.dataset_name}"]

  # {"secret_id": ...} references directly under connection (username, password, ...).
  secret_ids = distinct(concat(
    compact([for v in values(var.config.connection) : try(v.secret_id, null)]),
    var.extra_secret_ids,
  ))

  db_port = coalesce(var.db_port, try(var.config.connection.port, null))

  tables = toset([for t in var.config.tables : t.name])

  schedules = {
    for k, s in var.schedules : k => merge(s, {
      event = coalesce(s.event, jsonencode({ source_id = var.source_id }))
    })
  }
}

# --- Network --------------------------------------------------------------------------

resource "aws_security_group" "function" {
  name_prefix = "${local.function_name}-"
  description = "db_bronze ${var.source_id}: egress to the database and HTTPS"
  vpc_id      = var.vpc_id
  tags        = merge(var.tags, { Name = local.function_name })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_egress_rule" "db_cidr" {
  for_each = toset(var.db_cidr_blocks)

  security_group_id = aws_security_group.function.id
  description       = "Database"
  ip_protocol       = "tcp"
  from_port         = local.db_port
  to_port           = local.db_port
  cidr_ipv4         = each.value
}

resource "aws_vpc_security_group_egress_rule" "db_sg" {
  count = var.db_security_group_id == null ? 0 : 1

  security_group_id            = aws_security_group.function.id
  description                  = "Database"
  ip_protocol                  = "tcp"
  from_port                    = local.db_port
  to_port                      = local.db_port
  referenced_security_group_id = var.db_security_group_id
}

resource "aws_vpc_security_group_egress_rule" "https" {
  for_each = toset(var.https_egress_cidr_blocks)

  security_group_id = aws_security_group.function.id
  description       = "S3, Secrets Manager, CloudWatch"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = each.value
}

# --- Function -------------------------------------------------------------------------

module "function" {
  source = "../bronze_function"

  function_name = local.function_name
  source_id     = var.source_id
  config        = var.config
  config_bucket = var.config_bucket
  config_prefix = var.config_prefix

  image_uri            = var.image_uri
  architecture         = "x86_64"
  memory_mb            = var.memory_mb
  ephemeral_storage_mb = var.ephemeral_storage_mb
  timeout_s            = var.timeout_s
  safety_margin_s      = var.safety_margin_s
  reserved_concurrency = var.reserved_concurrency
  log_level            = var.log_level
  log_retention_days   = var.log_retention_days

  vpc_config = {
    subnet_ids         = var.subnet_ids
    security_group_ids = concat([aws_security_group.function.id], var.extra_security_group_ids)
  }

  s3_data_urls = local.s3_data_urls
  secret_ids   = local.secret_ids
  kms_key_arns = var.kms_key_arns

  schedules           = local.schedules
  max_loop_iterations = var.max_loop_iterations
  alarm_actions       = var.alarm_actions
  tags                = var.tags
}
