# Schedules, and the split-loading loop. An incremental run that stops at its time budget
# returns is_empty=false; the state machine re-invokes until a run lands nothing
# (docs/api/runs.md, docs/db/modes.md "Time budget and re-invocation").

locals {
  # Transient Lambda service errors only. A handler error (BrokenPageError, ConfigError,
  # PreflightError...) fails the execution: retrying it would not change the outcome, and the
  # next scheduled run retries anyway because a failed run holds the cursor.
  transient_retry = [
    {
      ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"]
      IntervalSeconds = 2
      MaxAttempts     = 3
      BackoffRate     = 2
    },
    {
      # Reserved concurrency 1: an overlapping run (another schedule, a manual backfill on
      # the same function) is throttled. Wait for it rather than fail.
      ErrorEquals     = ["Lambda.TooManyRequestsException"]
      IntervalSeconds = 30
      MaxAttempts     = 10
      BackoffRate     = 1.5
    },
  ]

  loop_definition = {
    Comment = "Split loading for ${var.function_name}: invoke while is_empty is false. Input: {\"event\": <incremental Lambda event>}."
    StartAt = "Init"
    States = {
      Init = {
        Type       = "Pass"
        Parameters = { "event.$" = "$.event", iteration = 1 }
        Next       = "Run"
      }
      Run = {
        Type     = "Task"
        Resource = "arn:${local.partition}:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.this.arn
          "Payload.$"  = "$.event"
        }
        ResultSelector = {
          "is_empty.$"     = "$.Payload.is_empty"
          "time_limited.$" = "$.Payload.time_limited"
          "load_ids.$"     = "$.Payload.load_ids"
        }
        ResultPath = "$.result"
        Retry      = local.transient_retry
        Next       = "Drained"
      }
      Drained = {
        Type = "Choice"
        Choices = [
          { Variable = "$.result.is_empty", BooleanEquals = true, Next = "Done" },
          { Variable = "$.iteration", NumericGreaterThanEquals = var.max_loop_iterations, Next = "MaxIterationsReached" },
        ]
        Default = "Again"
      }
      Again = {
        Type       = "Pass"
        Parameters = { "event.$" = "$.event", "iteration.$" = "States.MathAdd($.iteration, 1)" }
        Next       = "Run"
      }
      Done = { Type = "Succeed" }
      MaxIterationsReached = {
        Type  = "Fail"
        Error = "MaxIterationsReached"
        Cause = "Still landing rows after ${var.max_loop_iterations} runs: raise max_loop_iterations, schedule more often, or check for an API that never drains."
      }
    }
  }
}

# --- Split-loading state machine ----------------------------------------------------

data "aws_iam_policy_document" "states_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "loop" {
  count = local.loop_enabled ? 1 : 0

  name               = "${var.function_name}-loop"
  assume_role_policy = data.aws_iam_policy_document.states_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "invoke_function" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.this.arn, "${aws_lambda_function.this.arn}:*"]
  }
}

resource "aws_iam_role_policy" "loop" {
  count = local.loop_enabled ? 1 : 0

  name   = "invoke-function"
  role   = aws_iam_role.loop[0].id
  policy = data.aws_iam_policy_document.invoke_function.json
}

resource "aws_sfn_state_machine" "loop" {
  count = local.loop_enabled ? 1 : 0

  name       = "${var.function_name}-loop"
  role_arn   = aws_iam_role.loop[0].arn
  definition = jsonencode(local.loop_definition)
  tags       = var.tags
}

# --- Schedules ----------------------------------------------------------------------

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  count = length(var.schedules) > 0 ? 1 : 0

  name               = "${var.function_name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "scheduler" {
  source_policy_documents = [data.aws_iam_policy_document.invoke_function.json]

  dynamic "statement" {
    for_each = local.loop_enabled ? [1] : []
    content {
      actions   = ["states:StartExecution"]
      resources = [aws_sfn_state_machine.loop[0].arn]
    }
  }
}

resource "aws_iam_role_policy" "scheduler" {
  count = length(var.schedules) > 0 ? 1 : 0

  name   = "start-runs"
  role   = aws_iam_role.scheduler[0].id
  policy = data.aws_iam_policy_document.scheduler.json
}

resource "aws_scheduler_schedule" "this" {
  for_each = var.schedules

  name                         = "${var.function_name}-${each.key}"
  schedule_expression          = each.value.expression
  schedule_expression_timezone = each.value.timezone
  state                        = each.value.enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = each.value.loop ? aws_sfn_state_machine.loop[0].arn : aws_lambda_function.this.arn
    role_arn = aws_iam_role.scheduler[0].arn
    input    = each.value.loop ? jsonencode({ event = jsondecode(each.value.event) }) : each.value.event

    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}
