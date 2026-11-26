# Offline: mocked AWS provider, plan only.

mock_provider "aws" {
  override_during = plan

  mock_data "aws_region" {
    defaults = { region = "eu-west-1" }
  }
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  mock_resource "aws_lambda_function" {
    defaults = { arn = "arn:aws:lambda:eu-west-1:123456789012:function:fn" }
  }
  mock_resource "aws_sfn_state_machine" {
    defaults = { arn = "arn:aws:states:eu-west-1:123456789012:stateMachine:fn-loop" }
  }
}

variables {
  function_name        = "fn"
  source_id            = "orders"
  config               = { pipeline_name = "orders" }
  config_bucket        = "cfg"
  config_prefix        = "sources/"
  image_uri            = "123456789012.dkr.ecr.eu-west-1.amazonaws.com/api:v1"
  architecture         = "arm64"
  memory_mb            = 1024
  ephemeral_storage_mb = 2048
  timeout_s            = 900
  safety_margin_s      = 120
  reserved_concurrency = 1
  log_level            = "INFO"
  log_retention_days   = 30
  s3_data_urls         = ["s3://bronze/bronze/vendor", "s3://bronze/bronze/_quarantine/orders/"]
  secret_ids           = ["ingest/vendor", "arn:aws:secretsmanager:eu-west-1:123456789012:secret:other-AbCdEf"]
  kms_key_arns         = []
  max_loop_iterations  = 10
  alarm_actions        = []
  tags                 = {}
  schedules = {
    incremental = { expression = "rate(15 minutes)", event = "{\"source_id\":\"orders\"}", loop = true }
    full        = { expression = "cron(0 2 * * ? *)", event = "{\"source_id\":\"orders\",\"mode\":\"full\"}" }
  }
}

run "function" {
  command = plan

  assert {
    condition     = aws_s3_object.config.key == "sources/orders.json" && jsondecode(aws_s3_object.config.content) == var.config
    error_message = "The config lands verbatim at <prefix>/<source_id>.json."
  }

  assert {
    condition     = aws_lambda_function.this.environment[0].variables == tomap({ CONFIG_URI = "s3://cfg/sources", SAFETY_MARGIN_S = "120", LOG_LEVEL = "INFO" })
    error_message = "Environment must carry CONFIG_URI, SAFETY_MARGIN_S and LOG_LEVEL."
  }

  assert {
    condition     = aws_lambda_function.this.reserved_concurrent_executions == 1 && aws_lambda_function.this.package_type == "Image"
    error_message = "Image function with reserved concurrency 1."
  }

  assert {
    condition     = aws_lambda_function_event_invoke_config.this.maximum_retry_attempts == 0
    error_message = "Async invocations must not be retried by Lambda."
  }

  assert {
    condition     = [for s in data.aws_iam_policy_document.function.statement : s.resources if s.sid == "ReadWriteData1"][0] == toset(["arn:aws:s3:::bronze/bronze/_quarantine/orders/*"])
    error_message = "Data grants are scoped to the prefix (trailing slash trimmed)."
  }

  assert {
    condition = toset(one([
      for s in data.aws_iam_policy_document.function.statement : one(s.condition).values if s.sid == "ListData0"
    ])) == toset(["bronze/vendor", "bronze/vendor/*"])
    error_message = "ListBucket is conditioned on the prefix."
  }

  assert {
    condition = [for s in data.aws_iam_policy_document.function.statement : s.resources if s.sid == "ReadSecrets"][0] == toset([
      "arn:aws:secretsmanager:eu-west-1:123456789012:secret:ingest/vendor-??????",
      "arn:aws:secretsmanager:eu-west-1:123456789012:secret:other-AbCdEf",
    ])
    error_message = "Secret names become ARNs with the random suffix wildcard; ARNs pass through."
  }

  assert {
    condition     = length(aws_sfn_state_machine.loop) == 1
    error_message = "A looping schedule creates the split-loading state machine."
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.loop[0].definition).States.Drained.Choices[0] == { Variable = "$.result.is_empty", BooleanEquals = true, Next = "Done" }
    error_message = "The loop stops when a run lands nothing."
  }

  assert {
    condition     = jsondecode(aws_scheduler_schedule.this["incremental"].target[0].input) == { event = { source_id = "orders" } }
    error_message = "A looping schedule wraps the event for the state machine."
  }

  assert {
    condition     = aws_scheduler_schedule.this["full"].target[0].arn == aws_lambda_function.this.arn
    error_message = "A non-looping schedule invokes the function directly."
  }
}

run "no_vpc_no_loop" {
  command = plan

  variables {
    reserved_concurrency = null
    schedules            = {}
  }

  assert {
    condition     = aws_lambda_function.this.reserved_concurrent_executions == -1
    error_message = "Null reserved concurrency leaves the function unreserved."
  }

  assert {
    condition     = length(aws_sfn_state_machine.loop) == 0 && length(aws_iam_role.scheduler) == 0
    error_message = "No schedules, no loop: neither role nor state machine."
  }
}

run "loop_rejects_full_mode" {
  command = plan

  variables {
    schedules = {
      full = { expression = "rate(1 day)", event = "{\"source_id\":\"orders\",\"mode\":\"full\"}", loop = true }
    }
  }

  expect_failures = [var.schedules]
}

run "timeout_leaves_a_budget" {
  command = plan

  variables {
    timeout_s       = 150
    safety_margin_s = 120
  }

  expect_failures = [var.safety_margin_s]
}
