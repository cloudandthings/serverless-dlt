# One API source = one config document + one function (docs/deployment.md).

locals {
  bucket_url     = trimsuffix(var.config.bucket_url, "/")
  quarantine_url = trimsuffix(coalesce(try(var.config.quarantine_url, null), var.config.bucket_url), "/")
  pipeline_name  = var.config.pipeline_name

  broken_page_metric = "BrokenPage.${var.config.pipeline_name}"

  # dlt writes data, _dlt_loads and _dlt_pipeline_state under <bucket_url>/<dataset_name>;
  # broken pages go to <quarantine_url>/_quarantine/<pipeline_name>.
  s3_data_urls = [
    "${local.bucket_url}/${var.config.dataset_name}",
    "${local.quarantine_url}/_quarantine/${local.pipeline_name}",
  ]

  # {"secret_id": ...} references directly under client.auth and client.headers.
  client_refs = concat(
    values(try(var.config.client.auth, {})),
    values(try(var.config.client.headers, {})),
  )
  secret_ids = distinct(concat(
    compact([for v in local.client_refs : try(v.secret_id, null)]),
    var.extra_secret_ids,
  ))

  schedules = {
    for k, s in var.schedules : k => merge(s, {
      event = coalesce(s.event, jsonencode({ source_id = var.source_id }))
    })
  }
}

module "function" {
  source = "../bronze_function"

  function_name = coalesce(var.function_name, "api-bronze-${replace(var.source_id, "_", "-")}")
  source_id     = var.source_id
  config        = var.config
  config_bucket = var.config_bucket
  config_prefix = var.config_prefix

  image_uri            = var.image_uri
  architecture         = var.architecture
  memory_mb            = var.memory_mb
  ephemeral_storage_mb = var.ephemeral_storage_mb
  timeout_s            = var.timeout_s
  safety_margin_s      = var.safety_margin_s
  reserved_concurrency = var.reserved_concurrency
  log_level            = var.log_level
  log_retention_days   = var.log_retention_days

  s3_data_urls = local.s3_data_urls
  secret_ids   = local.secret_ids
  kms_key_arns = var.kms_key_arns

  schedules           = local.schedules
  max_loop_iterations = var.max_loop_iterations
  alarm_actions       = var.alarm_actions
  tags                = var.tags
}

# "Vendor sent garbage" apart from every other failure (docs/deployment.md "Alarms").
resource "aws_cloudwatch_log_metric_filter" "broken_page" {
  name           = "${module.function.function_name}-broken-page"
  log_group_name = module.function.log_group_name
  pattern        = "\"BrokenPageError\""

  # A text pattern cannot carry dimensions, so the pipeline is in the metric name.
  metric_transformation {
    namespace = "api_bronze"
    name      = local.broken_page_metric
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "broken_page" {
  alarm_name          = "${module.function.function_name}-broken-page"
  alarm_description   = "${local.pipeline_name}: a 2xx page was unusable. The raw body is under ${local.quarantine_url}/_quarantine/${local.pipeline_name}/ (docs/api/broken-responses.md). The cursor has not moved."
  namespace           = "api_bronze"
  metric_name         = local.broken_page_metric
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_actions
  ok_actions          = var.alarm_actions
  tags                = var.tags
}
