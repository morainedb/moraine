# The server must speak the DuckDB release the extension is built for, so
# the version comes from the repository's pin. The first uncommented line
# of the manifest is the primary pin.
locals {
  duckdb_version = regex("(?m)^(v[0-9]+\\.[0-9]+\\.[0-9]+)", file("${path.module}/../../.github/duckdb-versions"))[0]

  quack_hostname = "quack.${var.name_prefix}.internal"
  quack_port     = 9494
  quack_endpoint = "${local.quack_hostname}:${local.quack_port}"
}

data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

resource "random_password" "quack_token" {
  # No punctuation: the token is quoted inside SQL and a shell heredoc.
  length  = 32
  special = false
}

# `quack_serve` speaks TLS to every client but the loopback, and the client
# verifies the name it dialed. Naming the host here rather than an address
# is what lets the certificate exist before the instance does; the runner
# resolves it through a hosts entry.
resource "tls_private_key" "quack" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "tls_self_signed_cert" "quack" {
  private_key_pem       = tls_private_key.quack.private_key_pem
  dns_names             = [local.quack_hostname]
  validity_period_hours = 720
  allowed_uses          = ["digital_signature", "key_encipherment", "server_auth"]

  subject {
    common_name = local.quack_hostname
  }
}

resource "aws_security_group" "quack" {
  name        = "${local.name}-quack"
  description = "Quack server access from the moraine benchmark runner"
  vpc_id      = data.aws_vpc.default.id
  tags        = local.tags
}

resource "aws_vpc_security_group_ingress_rule" "quack" {
  security_group_id            = aws_security_group.quack.id
  description                  = "Benchmark runner"
  referenced_security_group_id = aws_security_group.runner.id
  from_port                    = local.quack_port
  to_port                      = local.quack_port
  ip_protocol                  = "tcp"
}

# Outbound reaches the DuckDB release and the extension repository; the
# instance installs both on first boot.
resource "aws_vpc_security_group_egress_rule" "quack" {
  security_group_id = aws_security_group.quack.id
  description       = "DuckDB CLI and extension downloads"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_iam_role" "quack" {
  name = "${local.name}-quack"
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

resource "aws_iam_role_policy_attachment" "quack_ssm" {
  role       = aws_iam_role.quack.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "quack" {
  name = "${local.name}-quack"
  role = aws_iam_role.quack.name
  tags = local.tags
}

resource "aws_instance" "quack" {
  ami = data.aws_ssm_parameter.al2023_arm64.value

  # A catalog server under a benchmark is memory-bound: it holds the
  # metadata every sweep writes, and two gigabytes left the box wedged
  # under five backends — a listening socket answering nothing, with the
  # agent killed along with everything else.
  instance_type = "t4g.large"
  subnet_id     = local.instance_subnet_id

  vpc_security_group_ids = [aws_security_group.quack.id]
  iam_instance_profile   = aws_iam_instance_profile.quack.name

  # No inbound rule reaches the internet; the address is how first boot
  # downloads the CLI.
  associate_public_ip_address = true

  user_data = templatefile("${path.module}/quack-user-data.sh.tftpl", {
    cli_asset      = "duckdb-cli-linux-arm64.tar.gz"
    duckdb_version = local.duckdb_version
    port           = local.quack_port
    token          = random_password.quack_token.result
    cert_pem       = tls_self_signed_cert.quack.cert_pem
    key_pem        = tls_private_key.quack.private_key_pem
  })

  # A new script means a new server, and the catalog it served is scratch.
  user_data_replace_on_change = true

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
  }

  metadata_options {
    http_tokens = "required"
  }

  tags = merge(local.tags, { Name = "${local.name}-quack" })
}
