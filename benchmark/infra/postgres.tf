resource "random_password" "postgres" {
  # No punctuation: the password travels inside a libpq DSN.
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "postgres" {
  name       = local.name
  subnet_ids = data.aws_subnets.all.ids
  tags       = local.tags
}

resource "aws_security_group" "postgres" {
  name        = local.name
  description = "Postgres access from the moraine benchmark runner"
  vpc_id      = data.aws_vpc.default.id
  tags        = local.tags
}

resource "aws_vpc_security_group_ingress_rule" "postgres" {
  security_group_id            = aws_security_group.postgres.id
  description                  = "Benchmark runner"
  referenced_security_group_id = aws_security_group.runner.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

# Engine defaults everywhere: a tuned instance would not compare with the
# loopback clusters the harness provisions itself. Same zone as the runner,
# so the measurement is the catalog rather than an AZ crossing.
resource "aws_db_instance" "postgres" {
  identifier        = local.name
  engine            = "postgres"
  engine_version    = var.postgres_version
  instance_class    = var.postgres_instance_class
  availability_zone = local.availability_zone

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  # The master user holds CREATEDB, which the harness needs to create a
  # database per run.
  username = "bench"
  password = random_password.postgres.result

  publicly_accessible    = false
  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  vpc_security_group_ids = [aws_security_group.postgres.id]

  # A benchmark instance holds nothing worth keeping.
  backup_retention_period = 0
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true

  multi_az                     = false
  auto_minor_version_upgrade   = true
  performance_insights_enabled = false
  monitoring_interval          = 0

  tags = local.tags
}
