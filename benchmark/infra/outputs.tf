locals {
  postgres_dsn = join(" ", [
    "host=${aws_db_instance.postgres.address}",
    "port=${aws_db_instance.postgres.port}",
    "user=${aws_db_instance.postgres.username}",
    "password=${random_password.postgres.result}",
    "sslmode=require",
  ])
}

output "runner_instance_id" {
  description = "The runner, reached with `aws ssm start-session --target`."
  value       = aws_instance.runner.id
}

output "availability_zone" {
  description = "Zone every resource sits in, for the machine stamp."
  value       = local.availability_zone
}

output "s3_bucket" {
  description = "Standard bucket, named by /etc/moraine-bench/s3.env on the runner."
  value       = aws_s3_bucket.standard.bucket
}

output "s3_express_bucket" {
  description = "Directory bucket, named by /etc/moraine-bench/s3express.env on the runner."
  value       = aws_s3_directory_bucket.express.bucket
}

output "postgres_host" {
  description = "RDS endpoint, resolvable inside the VPC only."
  value       = aws_db_instance.postgres.address
}

output "postgres_dsn" {
  description = "Value for MORAINE_BENCH_POSTGRES, for debugging from the runner."
  value       = local.postgres_dsn
  sensitive   = true
}

output "quack_endpoint" {
  description = "Value for MORAINE_BENCH_QUACK. The name resolves through the runner's hosts entry."
  value       = local.quack_endpoint
}

output "quack_instance_id" {
  description = "The Quack server, for `aws ssm start-session --target`."
  value       = aws_instance.quack.id
}

output "quack_token" {
  description = "Value for MORAINE_BENCH_QUACK_TOKEN, for debugging from the runner."
  value       = random_password.quack_token.result
  sensitive   = true
}
