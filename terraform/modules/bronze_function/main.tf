data "aws_partition" "current" {}
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  partition  = data.aws_partition.current.partition
  region     = data.aws_region.current.region
  account_id = data.aws_caller_identity.current.account_id

  config_key = "${trimsuffix(var.config_prefix, "/")}/${var.source_id}.json"
  config_uri = "s3://${var.config_bucket}/${trimsuffix(var.config_prefix, "/")}"

  loop_enabled = anytrue([for s in values(var.schedules) : s.loop])
}

# --- Config document ----------------------------------------------------------------

# The handler reads ${CONFIG_URI}/${source_id}.json at every invocation, so a config change
# applies from the next run without touching the function.
resource "aws_s3_object" "config" {
  bucket       = var.config_bucket
  key          = local.config_key
  content      = jsonencode(var.config)
  content_type = "application/json"
  tags         = var.tags
}

# --- Function -----------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_lambda_function" "this" {
  function_name = var.function_name
  role          = aws_iam_role.function.arn
  package_type  = "Image"
  image_uri     = var.image_uri
  architectures = [var.architecture]

  memory_size                    = var.memory_mb
  timeout                        = var.timeout_s
  reserved_concurrent_executions = coalesce(var.reserved_concurrency, -1)

  ephemeral_storage {
    size = var.ephemeral_storage_mb
  }

  environment {
    variables = merge(var.extra_environment, {
      CONFIG_URI      = local.config_uri
      SAFETY_MARGIN_S = tostring(var.safety_margin_s)
      LOG_LEVEL       = var.log_level
    })
  }

  dynamic "vpc_config" {
    for_each = var.vpc_config == null ? [] : [var.vpc_config]
    content {
      subnet_ids         = vpc_config.value.subnet_ids
      security_group_ids = vpc_config.value.security_group_ids
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.this.name
  }

  tags = var.tags

  depends_on = [
    aws_iam_role_policy.function,
    aws_iam_role_policy_attachment.vpc,
  ]
}

# EventBridge Scheduler invokes Lambda asynchronously. Lambda would otherwise retry a
# failed run twice on its own; a failure should surface once (Errors alarm) and the next
# scheduled run retries it, since a failed run lands nothing and holds the cursor.
resource "aws_lambda_function_event_invoke_config" "this" {
  function_name          = aws_lambda_function.this.function_name
  maximum_retry_attempts = 0
}
