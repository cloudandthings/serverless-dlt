# db_bronze on Lambda for the README sample database source (examples/core_banking.json):
#
#   core_banking           transactions (cursor, keyset) incremental every 30 min through
#                          the split-loading loop; branches (no cursor: a snapshot table)
#                          as mode=full once a day. Reserved concurrency 1.
#   core_banking_snapshot  same database under its own pipeline_name, unreserved, with the
#                          paged-snapshot state machine (plan -> Map over pages -> finalize).
#                          Start it by hand: {"source_id": "core_banking_snapshot",
#                          "table": "transactions", "page_size": 500000}
#
# A function needs its image in ECR, so the first apply is two-phase (see README.md).

locals {
  bucket_url = "s3://${module.platform.bronze_bucket}/bronze"
  image_uri  = "${module.platform.db_repository_url}:${var.image_tag}"

  core_banking = merge(jsondecode(file("${path.module}/../../../examples/core_banking.json")), { bucket_url = local.bucket_url })

  # Paged snapshots write dlt state rows and run in parallel: never the incremental's
  # pipeline_name (README "Orchestration").
  core_banking_snapshot = merge(local.core_banking, { pipeline_name = "core_banking_snapshot" })

  common = {
    config_bucket  = module.platform.config_bucket
    config_prefix  = module.platform.config_prefix
    image_uri      = local.image_uri
    vpc_id         = var.vpc_id
    subnet_ids     = var.subnet_ids
    db_cidr_blocks = var.db_cidr_blocks
    alarm_actions  = [aws_sns_topic.alarms.arn]
  }
}

module "platform" {
  source = "../../modules/bronze_platform"

  name_prefix           = var.name_prefix
  create_api_repository = false
}

resource "aws_sns_topic" "alarms" {
  name = "${var.name_prefix}-db-bronze-alarms"
}

resource "aws_sns_topic_subscription" "alarm_email" {
  count = var.alarm_email == null ? 0 : 1

  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

module "core_banking" {
  source = "../../modules/db_bronze_source"

  source_id      = "core_banking"
  config         = local.core_banking
  config_bucket  = local.common.config_bucket
  config_prefix  = local.common.config_prefix
  image_uri      = local.common.image_uri
  vpc_id         = local.common.vpc_id
  subnet_ids     = local.common.subnet_ids
  db_cidr_blocks = local.common.db_cidr_blocks
  alarm_actions  = local.common.alarm_actions

  schedules = {
    # Snapshot tables cannot run incremental: name the cursor tables with `tables`.
    incremental = {
      expression = "rate(30 minutes)"
      event      = jsonencode({ source_id = "core_banking", tables = ["transactions"] })
      loop       = true
    }
    branches_full = {
      expression = "cron(0 3 * * ? *)"
      event      = jsonencode({ source_id = "core_banking", mode = "full", tables = ["branches"] })
      timezone   = "Africa/Johannesburg"
    }
  }
}

module "core_banking_snapshot" {
  source = "../../modules/db_bronze_source"

  source_id      = "core_banking_snapshot"
  config         = local.core_banking_snapshot
  config_bucket  = local.common.config_bucket
  config_prefix  = local.common.config_prefix
  image_uri      = local.common.image_uri
  vpc_id         = local.common.vpc_id
  subnet_ids     = local.common.subnet_ids
  db_cidr_blocks = local.common.db_cidr_blocks
  alarm_actions  = local.common.alarm_actions

  reserved_concurrency     = null
  snapshot_state_machine   = true
  snapshot_max_concurrency = 4
}
