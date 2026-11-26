# Offline: mocked AWS provider, plan only. The config is the example's sample.

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
    defaults = { arn = "arn:aws:lambda:eu-west-1:123456789012:function:db-bronze-core-banking-snapshot" }
  }
}

variables {
  source_id      = "core_banking"
  config         = jsondecode(file("../../../examples/core_banking.json"))
  config_bucket  = "cfg"
  config_prefix  = "sources"
  image_uri      = "123456789012.dkr.ecr.eu-west-1.amazonaws.com/db:v1"
  vpc_id         = "vpc-1"
  subnet_ids     = ["subnet-1"]
  db_cidr_blocks = ["10.0.0.10/32"]
}

run "incremental" {
  command = plan

  assert {
    condition = jsonencode(output.grants) == jsonencode({
      s3_data_urls = ["s3://bronze-bucket/bronze/core_banking"]
      secret_ids   = ["ingest/core_banking"]
    })
    error_message = "Grants follow bucket_url, dataset_name and the connection's secret references."
  }

  assert {
    condition     = aws_vpc_security_group_egress_rule.db_cidr["10.0.0.10/32"].from_port == 1433
    error_message = "DB egress uses connection.port."
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.emf) == 6
    error_message = "Three EMF alarms per table."
  }

  assert {
    condition     = output.snapshot_state_machine_arn == null
    error_message = "No snapshot state machine unless asked."
  }
}

run "snapshot" {
  command = plan

  variables {
    reserved_concurrency   = null
    snapshot_state_machine = true
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.snapshot[0].definition) == jsondecode(file("../../../examples/db_snapshot.asl.json"))
    error_message = "The rendered state machine must equal examples/db_snapshot.asl.json (same ARN, MaxConcurrency 4)."
  }
}

run "snapshot_needs_concurrency" {
  command = plan

  variables {
    snapshot_state_machine = true
  }

  expect_failures = [var.snapshot_state_machine]
}

run "needs_a_route_to_the_db" {
  command = plan

  variables {
    db_cidr_blocks = []
  }

  expect_failures = [var.db_security_group_id]
}
