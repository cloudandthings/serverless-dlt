# Function role. The S3 grants are scoped to the prefixes the wrapper derives from the
# config; confirm the minimal action set on the first real-S3 run (docs/open-items.md item 1:
# s3fs and dlt's filesystem destination have only run against file:// so far).

locals {
  s3_locations = [
    for url in distinct(var.s3_data_urls) : {
      bucket = regex("^s3://([^/]+)", url)[0]
      prefix = trim(replace(url, "/^s3://[^/]+/", ""), "/")
    }
  ]

  secret_arns = [
    for id in var.secret_ids :
    startswith(id, "arn:") ? id : "arn:${local.partition}:secretsmanager:${local.region}:${local.account_id}:secret:${id}-??????"
  ]
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "function" {
  name               = "${var.function_name}-function"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "function" {
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.this.arn}:*"]
  }

  statement {
    sid       = "ReadConfig"
    actions   = ["s3:GetObject"]
    resources = ["arn:${local.partition}:s3:::${var.config_bucket}/${local.config_key}"]
  }

  dynamic "statement" {
    for_each = local.s3_locations
    content {
      sid       = "ListData${statement.key}"
      actions   = ["s3:ListBucket"]
      resources = ["arn:${local.partition}:s3:::${statement.value.bucket}"]
      condition {
        test     = "StringLike"
        variable = "s3:prefix"
        values   = statement.value.prefix == "" ? ["*"] : [statement.value.prefix, "${statement.value.prefix}/*"]
      }
    }
  }

  dynamic "statement" {
    for_each = local.s3_locations
    content {
      sid = "ReadWriteData${statement.key}"
      actions = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
      ]
      resources = [
        statement.value.prefix == ""
        ? "arn:${local.partition}:s3:::${statement.value.bucket}/*"
        : "arn:${local.partition}:s3:::${statement.value.bucket}/${statement.value.prefix}/*"
      ]
    }
  }

  dynamic "statement" {
    for_each = length(local.secret_arns) > 0 ? [1] : []
    content {
      sid       = "ReadSecrets"
      actions   = ["secretsmanager:GetSecretValue"]
      resources = local.secret_arns
    }
  }

  dynamic "statement" {
    for_each = length(var.kms_key_arns) > 0 ? [1] : []
    content {
      sid       = "UseKeys"
      actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
      resources = var.kms_key_arns
    }
  }
}

resource "aws_iam_role_policy" "function" {
  name   = "bronze"
  role   = aws_iam_role.function.id
  policy = data.aws_iam_policy_document.function.json
}

resource "aws_iam_role_policy_attachment" "vpc" {
  count = var.vpc_config == null ? 0 : 1

  role       = aws_iam_role.function.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}
