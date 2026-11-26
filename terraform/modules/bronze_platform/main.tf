# Shared platform for api_bronze and db_bronze: one ECR repository per image, the bronze
# bucket, and the config bucket (CONFIG_URI). Sources are added with api_bronze_source /
# db_bronze_source; nothing here changes per source.

locals {
  bronze_bucket = coalesce(var.bronze_bucket_name, "${var.name_prefix}-bronze")
  config_bucket = coalesce(var.config_bucket_name, "${var.name_prefix}-config")

  repositories = merge(
    var.create_api_repository ? { api = "${var.name_prefix}-api-bronze" } : {},
    var.create_db_repository ? { db = "${var.name_prefix}-db-bronze" } : {},
  )

  buckets = {
    bronze = local.bronze_bucket
    config = local.config_bucket
  }
}

# --- ECR ----------------------------------------------------------------------------

resource "aws_ecr_repository" "this" {
  for_each = local.repositories

  name                 = each.value
  image_tag_mutability = "IMMUTABLE" # a function's image_uri must keep meaning the same image
  force_delete         = var.force_destroy

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = var.kms_key_arn == null ? "AES256" : "KMS"
    kms_key         = var.kms_key_arn
  }

  tags = var.tags
}

resource "aws_ecr_lifecycle_policy" "this" {
  for_each = aws_ecr_repository.this

  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the last ${var.ecr_keep_images} images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = var.ecr_keep_images
      }
      action = { type = "expire" }
    }]
  })
}

# --- Buckets ------------------------------------------------------------------------

resource "aws_s3_bucket" "this" {
  for_each = local.buckets

  bucket        = each.value
  force_destroy = var.force_destroy
  tags          = var.tags
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = aws_s3_bucket.this

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_arn == null ? "AES256" : "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = var.kms_key_arn != null
  }
}

# Configs are small and hand-edited: keep their history.
resource "aws_s3_bucket_versioning" "config" {
  bucket = aws_s3_bucket.this["config"].id
  versioning_configuration {
    status = "Enabled"
  }
}

# Bronze lifecycle: deliberately no expiration. Silver decides completeness from
# _dlt_loads/ and dlt restores cursor state from _dlt_pipeline_state/ (README
# "Operations"), and deleting a dataset prefix makes dlt wipe its state. Retention for
# those small per-run objects is still an open item (README "Open items" 6); add
# transitions here once it is decided. Only incomplete multipart uploads are cleaned up.
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.this["bronze"].id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
