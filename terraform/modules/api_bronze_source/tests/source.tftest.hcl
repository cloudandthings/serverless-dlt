# Offline: mocked AWS provider, plan only. The config is the repo's sample.

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
}

variables {
  source_id     = "vendor_orders"
  config        = jsondecode(file("../../../examples/vendor_orders.json"))
  config_bucket = "cfg"
  config_prefix = "sources"
  image_uri     = "123456789012.dkr.ecr.eu-west-1.amazonaws.com/api:v1"
}

run "grants_from_config" {
  command = plan

  assert {
    condition = jsonencode(output.grants) == jsonencode({
      s3_data_urls = ["s3://example-bronze-bucket/bronze/vendor", "s3://example-bronze-bucket/bronze/_quarantine/vendor_orders"]
      secret_ids   = ["ingest/vendor"]
    })
    error_message = "Grants follow bucket_url, dataset_name, pipeline_name and client.auth secret references."
  }

  assert {
    condition     = output.function_name == "api-bronze-vendor-orders"
    error_message = "Default function name."
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.broken_page.metric_name == "BrokenPage.vendor_orders"
    error_message = "BrokenPageError alarm per pipeline."
  }
}

run "quarantine_url_override" {
  command = plan

  variables {
    config = merge(jsondecode(file("../../../examples/vendor_orders.json")), { quarantine_url = "s3://q-bucket/q/" })
  }

  assert {
    condition     = output.grants.s3_data_urls[1] == "s3://q-bucket/q/_quarantine/vendor_orders"
    error_message = "quarantine_url replaces bucket_url for quarantine writes."
  }
}

run "loop_needs_sorted_resources" {
  command = plan

  variables {
    config    = jsondecode(file("../../../examples/vendor_countries.json"))
    source_id = "vendor_countries"
    schedules = { incremental = { expression = "rate(15 minutes)", loop = true } }
  }

  expect_failures = [var.schedules]
}
