variable "region" {
  description = "Region holding every resource."
  type        = string
  default     = "us-east-1"
}

variable "express_availability_zone_id" {
  description = "Zone for the S3 Express directory bucket, which must be in `region` and offer S3 Express. Everything that talks to it is placed in the same zone."
  type        = string
  default     = "use1-az4"
}

variable "name_prefix" {
  description = "Prefix for resource names. A random suffix is appended, because bucket names are global."
  type        = string
  default     = "moraine-bench"
}

variable "postgres_version" {
  description = "Engine version. A major version alone takes the latest minor, matching what the loopback runs measured."
  type        = string
  default     = "16"
}

variable "postgres_instance_class" {
  description = "Instance class. The catalog workload is latency-bound, so a small class is representative."
  type        = string
  default     = "db.t4g.small"
}

variable "runner_instance_type" {
  description = "Runner instance type, x86-64 to match the published stamps. Eight vCPUs and 16 GiB is the shape behind every number in docs/benchmarks.md."
  type        = string
  default     = "c6i.2xlarge"
}

variable "runner_volume_gb" {
  description = "Runner root volume. It holds a DuckDB build tree, the Rust target directory, and the benchmark's Parquet."
  type        = number
  default     = 100
}
