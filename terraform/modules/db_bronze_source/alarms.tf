# EMF metrics the handler emits (namespace db_bronze, dimensions pipeline and table) when
# one of these errors ends a run (README "Errors"). One alarm per table and metric.

locals {
  emf_alarms = {
    PreflightFailed  = "Reflection checks failed before extraction. Fix: \"backend\": \"pyarrow\", exclude or cast the column, or upgrade the server."
    ConnectorXFailed = "ConnectorX raised. Fix: \"backend\": \"pyarrow\" on the table, under a new name (bronze types change)."
    BackendChanged   = "The resolved backend differs from the last successful run. Fix: land under a new name, or set backend back."
  }

  table_alarms = {
    for pair in setproduct(local.tables, keys(local.emf_alarms)) :
    "${pair[0]}.${pair[1]}" => { table = pair[0], metric = pair[1] }
  }
}

resource "aws_cloudwatch_metric_alarm" "emf" {
  for_each = local.table_alarms

  alarm_name          = "${local.function_name}-${replace(each.value.table, "_", "-")}-${lower(each.value.metric)}"
  alarm_description   = "${local.pipeline_name}.${each.value.table}: ${local.emf_alarms[each.value.metric]}"
  namespace           = "db_bronze"
  metric_name         = each.value.metric
  dimensions          = { pipeline = local.pipeline_name, table = each.value.table }
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
