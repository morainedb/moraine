# The runner is the only client of the catalogs above, which is why none of
# them is reachable from outside the VPC.
data "aws_ssm_parameter" "al2023_x86_64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# Directory buckets authorize data-plane calls through a session token, so
# CreateSession is the whole grant for S3 Express.
data "aws_iam_policy_document" "bench" {
  statement {
    sid = "StandardBucket"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.standard.arn]
  }

  statement {
    sid = "StandardObjects"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]
    resources = ["${aws_s3_bucket.standard.arn}/*"]
  }

  statement {
    sid       = "ExpressSession"
    actions   = ["s3express:CreateSession"]
    resources = [aws_s3_directory_bucket.express.arn]
  }
}

resource "aws_iam_role" "runner" {
  name = "${local.name}-runner"
  tags = local.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "runner_bench" {
  name   = "bench-buckets"
  role   = aws_iam_role.runner.id
  policy = data.aws_iam_policy_document.bench.json
}

resource "aws_iam_role_policy_attachment" "runner_ssm" {
  role       = aws_iam_role.runner.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "runner" {
  name = "${local.name}-runner"
  role = aws_iam_role.runner.name
  tags = local.tags
}

# No inbound rule at all: Session Manager reaches the instance outbound.
resource "aws_security_group" "runner" {
  name        = "${local.name}-runner"
  description = "Benchmark runner"
  vpc_id      = data.aws_vpc.default.id
  tags        = local.tags
}

resource "aws_vpc_security_group_egress_rule" "runner" {
  security_group_id = aws_security_group.runner.id
  description       = "Package, crate, and catalog access"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_instance" "runner" {
  ami           = data.aws_ssm_parameter.al2023_x86_64.value
  instance_type = var.runner_instance_type
  subnet_id     = local.instance_subnet_id

  vpc_security_group_ids      = [aws_security_group.runner.id]
  iam_instance_profile        = aws_iam_instance_profile.runner.name
  associate_public_ip_address = true

  user_data = templatefile("${path.module}/runner-user-data.sh.tftpl", {
    region           = var.region
    repository       = var.repository
    standard_bucket  = aws_s3_bucket.standard.bucket
    express_bucket   = aws_s3_directory_bucket.express.bucket
    postgres_dsn     = local.postgres_dsn
    quack_endpoint   = local.quack_endpoint
    quack_hostname   = local.quack_hostname
    quack_private_ip = aws_instance.quack.private_ip
    quack_token      = random_password.quack_token.result
    quack_ca_cert    = tls_self_signed_cert.quack.cert_pem
  })

  # Rebuilding the runner throws away a DuckDB build tree, so the script is
  # applied to a fresh instance only.
  user_data_replace_on_change = false

  root_block_device {
    volume_size = var.runner_volume_gb
    volume_type = "gp3"
    encrypted   = true
  }

  metadata_options {
    http_tokens = "required"
  }

  tags = merge(local.tags, { Name = "${local.name}-runner" })
}
