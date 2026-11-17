# The directory bucket lives in one zone, and a cross-AZ hop is a large
# share of what S3 Express saves, so the runner and both servers sit in
# that zone too.
data "aws_vpc" "default" {
  default = true
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_subnets" "all" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_subnets" "in_zone" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "availability-zone"
    values = [local.availability_zone]
  }
}

locals {
  availability_zone = zipmap(
    data.aws_availability_zones.available.zone_ids,
    data.aws_availability_zones.available.names
  )[var.express_availability_zone_id]

  instance_subnet_id = data.aws_subnets.in_zone.ids[0]
}
