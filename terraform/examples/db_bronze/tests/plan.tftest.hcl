# Offline plan of the whole example against a mocked AWS provider: no credentials needed.
# Behaviour is asserted in the modules' own tests; this proves the example wires together.

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

# Bucket names are computed; give the mocks the names the platform would create.
override_resource {
  target          = module.platform.aws_s3_bucket.this["bronze"]
  override_during = plan
  values          = { id = "test-bronze", arn = "arn:aws:s3:::test-bronze" }
}

override_resource {
  target          = module.platform.aws_s3_bucket.this["config"]
  override_during = plan
  values          = { id = "test-config", arn = "arn:aws:s3:::test-config" }
}

variables {
  name_prefix    = "test"
  image_tag      = "v1"
  vpc_id         = "vpc-1"
  subnet_ids     = ["subnet-1"]
  db_cidr_blocks = ["10.0.0.10/32"]
}

run "plan" {
  command = plan

  assert {
    condition     = output.config_uri == "s3://test-config/sources"
    error_message = "CONFIG_URI must be the platform's config bucket and prefix."
  }

  assert {
    condition     = jsonencode(output.grants.core_banking.s3_data_urls) == jsonencode(["s3://test-bronze/bronze/core_banking"])
    error_message = "core_banking must be granted its dataset under the platform bucket."
  }
}
