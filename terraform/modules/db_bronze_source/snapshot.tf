# Paged snapshot: plan -> Map over pages -> finalize (docs/db/design.md D8). The template is
# examples/db_snapshot.asl.json with the function ARN and MaxConcurrency as variables;
# tests/test_terraform_asl.py keeps the two identical.
#
# Start an execution with {"source_id": "<source_id>", "table": "<table>", "page_size": N}.

data "aws_iam_policy_document" "states_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "snapshot" {
  count = var.snapshot_state_machine ? 1 : 0

  name               = "${local.function_name}-snapshot"
  assume_role_policy = data.aws_iam_policy_document.states_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "snapshot" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [module.function.function_arn, "${module.function.function_arn}:*"]
  }
}

resource "aws_iam_role_policy" "snapshot" {
  count = var.snapshot_state_machine ? 1 : 0

  name   = "invoke-function"
  role   = aws_iam_role.snapshot[0].id
  policy = data.aws_iam_policy_document.snapshot.json
}

resource "aws_sfn_state_machine" "snapshot" {
  count = var.snapshot_state_machine ? 1 : 0

  name     = "${local.function_name}-snapshot"
  role_arn = aws_iam_role.snapshot[0].arn
  definition = templatefile("${path.module}/templates/db_snapshot.asl.json.tftpl", {
    function_arn    = module.function.function_arn
    max_concurrency = var.snapshot_max_concurrency
  })
  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "snapshot_failed" {
  count = var.snapshot_state_machine ? 1 : 0

  alarm_name          = "${local.function_name}-snapshot-failed"
  alarm_description   = "A paged snapshot of ${local.pipeline_name} failed. Without finalize no manifest is written, so silver ignores its pages: re-run the snapshot."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.snapshot[0].arn }
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
