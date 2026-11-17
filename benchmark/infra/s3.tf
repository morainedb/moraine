locals {
  name = "${var.name_prefix}-${random_id.suffix.hex}"

  tags = {
    Project = "moraine"
    Purpose = "benchmark"
  }
}

resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "standard" {
  bucket = local.name
  tags   = local.tags

  # Benchmark data is reproducible, so teardown may delete it.
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "standard" {
  bucket = aws_s3_bucket.standard.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "standard" {
  bucket = aws_s3_bucket.standard.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "standard" {
  bucket = aws_s3_bucket.standard.id

  rule {
    id     = "expire-benchmark-objects"
    status = "Enabled"

    filter {}

    expiration {
      days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# Moraine selects S3 Express addressing from the bucket name, so the
# `--<zone>--x-s3` suffix is what switches the client over.
resource "aws_s3_directory_bucket" "express" {
  bucket          = "${local.name}--${var.express_availability_zone_id}--x-s3"
  data_redundancy = "SingleAvailabilityZone"
  force_destroy   = true

  location {
    name = var.express_availability_zone_id
  }
}
