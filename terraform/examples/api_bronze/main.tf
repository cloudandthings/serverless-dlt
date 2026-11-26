# api_bronze on Lambda, end to end, for the sample sources in examples/:
#
#   vendor_orders           incremental, sorted_ascending: every 15 min through the
#                           split-loading loop (re-invoke while is_empty is false)
#   vendor_countries        snapshot endpoint: mode=full once a day
#   vendor_orders_backfill  same API, its own pipeline_name and no reserved concurrency,
#                           for parallel backfill windows; invoked by hand, never scheduled
#
# The configs are the repo's sample files with bucket_url pointed at the platform bucket.
# A function needs its image in ECR, so the first apply is two-phase (see terraform/README.md).

locals {
  bucket_url = "s3://${module.platform.bronze_bucket}/bronze"
  image_uri  = "${module.platform.api_repository_url}:${var.image_tag}"

  vendor_orders    = merge(jsondecode(file("${path.module}/../../../examples/vendor_orders.json")), { bucket_url = local.bucket_url })
  vendor_countries = merge(jsondecode(file("${path.module}/../../../examples/vendor_countries.json")), { bucket_url = local.bucket_url })

  # Parallel backfills must not share a pipeline name with the incremental: they still
  # write dlt state rows (docs/deployment.md "Orchestration").
  vendor_orders_backfill = merge(local.vendor_orders, { pipeline_name = "vendor_orders_backfill" })

  common = {
    config_bucket = module.platform.config_bucket
    config_prefix = module.platform.config_prefix
    image_uri     = local.image_uri
    architecture  = var.architecture
    alarm_actions = [aws_sns_topic.alarms.arn]
  }
}

module "platform" {
  source = "../../modules/bronze_platform"

  name_prefix          = var.name_prefix
  create_db_repository = false
}

resource "aws_sns_topic" "alarms" {
  name = "${var.name_prefix}-bronze-alarms"
}

resource "aws_sns_topic_subscription" "alarm_email" {
  count = var.alarm_email == null ? 0 : 1

  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

module "vendor_orders" {
  source = "../../modules/api_bronze_source"

  source_id     = "vendor_orders"
  config        = local.vendor_orders
  config_bucket = local.common.config_bucket
  config_prefix = local.common.config_prefix
  image_uri     = local.common.image_uri
  architecture  = local.common.architecture
  alarm_actions = local.common.alarm_actions

  schedules = {
    incremental = { expression = "rate(15 minutes)", loop = true }
  }
}

module "vendor_countries" {
  source = "../../modules/api_bronze_source"

  source_id     = "vendor_countries"
  config        = local.vendor_countries
  config_bucket = local.common.config_bucket
  config_prefix = local.common.config_prefix
  image_uri     = local.common.image_uri
  architecture  = local.common.architecture
  alarm_actions = local.common.alarm_actions

  memory_mb = 1024

  schedules = {
    full = {
      expression = "cron(0 2 * * ? *)"
      event      = jsonencode({ source_id = "vendor_countries", mode = "full" })
    }
  }
}

module "vendor_orders_backfill" {
  source = "../../modules/api_bronze_source"

  source_id     = "vendor_orders_backfill"
  config        = local.vendor_orders_backfill
  config_bucket = local.common.config_bucket
  config_prefix = local.common.config_prefix
  image_uri     = local.common.image_uri
  architecture  = local.common.architecture
  alarm_actions = local.common.alarm_actions

  # Windows run in parallel; each is stateless for the cursor (docs/api/runs.md).
  reserved_concurrency = null
}
