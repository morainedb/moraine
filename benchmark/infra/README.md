# Benchmark infrastructure

OpenTofu for a networked run of the catalog comparison: a benchmark runner
and the three catalogs it measures against it — moraine on S3 and on S3
Express, DuckLake on RDS Postgres, and DuckLake on a DuckDB file served
over Quack. The plain DuckDB-file catalog stays on the runner's own disk,
since DuckLake cannot write a catalog file to an object store, and is the
control.

There is nothing to configure. `tofu apply` creates everything, and every
catalog is private to the runner's security group, so no address of yours
appears anywhere:

```sh
cd benchmark/infra
tofu init
tofu apply
```

This is separate from `.github/aws/real-s3-benchmark.yml`, the
CloudFormation stack behind the `Real S3 benchmark` workflow. That stack
runs the core's object-storage suite on a CodeBuild worker and is
unaffected by this directory.

Numbers measured here belong in
[docs/benchmarks.md](../../docs/benchmarks.md), stamped with date, commit,
and machine, as every other harness is. The machine is the runner's
instance type and the zone in the outputs.

## Why a runner instance

Running the sweep from a laptop or a sandbox VM measures three things that
are not the catalog. A directory bucket lives in one zone, so a client
elsewhere pays a cross-AZ hop — a large share of what S3 Express saves. A
shared VM's CPU is contended, and moraine's commit path still has
single-digit milliseconds of compute in it. And "my workspace VM" is not a
machine stamp anyone can reproduce; two of them already differed by 1.9×
between releases.

So the runner, both servers, and the directory bucket are placed in one
zone, and the runner's type is `c6i.2xlarge`: eight vCPUs and 16 GiB,
matching the shape behind the existing numbers. The resulting figures are
a best case — everything is one zone away — which the stamp should say.
Distance is still simulated with the delay line the catalog documents, not
bought.

## What it creates

| resource | why |
|---|---|
| EC2 runner, `c6i.2xlarge`, zone-pinned | runs `cargo xtask bench`; its instance profile carries the bucket grants |
| S3 bucket, encrypted, lifecycle expiry | `MORAINE_S3_BUCKET` for the standard-S3 leg |
| S3 Express directory bucket | the same leg on Express; moraine selects Express addressing from the bucket name |
| RDS Postgres, private, engine defaults | `MORAINE_BENCH_POSTGRES` |
| EC2 Quack server with a self-signed certificate | `MORAINE_BENCH_QUACK`, a DuckDB file served to a remote client |
| three security groups | Postgres and Quack admit the runner's group and nothing else |

Neither server has an inbound rule reaching the internet, and the RDS
endpoint resolves inside the VPC only. Session Manager, which is outbound,
is the way in.

## Running a sweep

```sh
aws ssm start-session --region "$(tofu output -raw region)" \
  --target "$(tofu output -raw runner_instance_id)"
sudo -iu ec2-user          # a session starts as ssm-user, which owns none of this
cd moraine                 # cloned at boot, submodules and all
set -a; . /etc/moraine-bench/bench.env; set +a
cargo xtask bench --backends moraine,moraine_express,postgres,quack,duckdb \
  --seconds 5 --concurrency 1,2,4,8,16
```

First boot clones the repository into `~ec2-user/moraine` with its
submodules, which the extension build needs: without `duckdb` and
`extension-ci-tools` it stops at a missing makefile rather than at
anything naming a submodule. A clone made by hand needs
`--recurse-submodules`, or `git submodule update --init` after the
fact — not `--depth`, since DuckDB is pinned to a commit that is not a
branch head.

`cargo` only works inside that clone: rustup is installed with no default
toolchain, so the version comes from the repository's
`rust-toolchain.toml` rather than from whatever the runner happened to
install.

First boot writes that environment, then installs the build toolchain
from [CONTRIBUTING.md](../../CONTRIBUTING.md) and `rustup`, and finishes
by creating `/etc/moraine-bench/ready`. If that file is missing, setup
did not finish and `/var/log/cloud-init-output.log` says why; replace the
instance with `tofu apply -replace=aws_instance.runner` rather than
repairing it, since cloud-init runs a script once per instance.

Amazon Linux has no `ccache`, so the first `bench` compiles DuckDB from
scratch, which is the slow part. Snapshot an AMI once that has happened
and later sweeps start in a minute.

`/etc/moraine-bench/bench.env` names both buckets, so `moraine` and
`moraine_express` are two columns of one run rather than two runs: they
then share a machine, a clock and a network, which is what makes the
comparison between the stores worth anything. The S3 prefix is stamped
when the file is sourced, so re-source it between runs to keep their
catalogs apart.

Record the round trip to each endpoint beside the numbers. Every catalog
here costs several sequential requests per commit, so endpoint latency is
most of what a run reports:

```sh
curl -so /dev/null -w '%{time_connect}\n' "https://s3.$AWS_REGION.amazonaws.com"
time psql "$MORAINE_BENCH_POSTGRES dbname=postgres" -qtc 'select 1'
```

## The Quack server

`quack_serve` speaks TLS to every client but the loopback, so the server
cannot be reached in the clear. Its certificate names
`quack.moraine-bench.internal` rather than an address, which is how it can
be issued before the instance exists; the runner's hosts entry resolves
that name to the server's private address. The harness turns
`MORAINE_BENCH_QUACK_CA_CERT` into a global `ca_cert_file` — a
session-local one is not enough, because DuckLake's metadata connection
does not see it.

DuckDB's own advice is to put a reverse proxy in front of the RPC. This
does not, deliberately: a proxy adds a hop to the very latency being
measured. The security group is the control instead, and the token is 32
random characters.

The server runs the DuckDB release pinned in `.github/duckdb-versions`,
read from that file so it cannot drift from the extension that talks to
it. First boot downloads the CLI and installs the `quack` extension, so
give it a minute. To look at it:

```sh
aws ssm start-session --region "$(tofu output -raw region)" \
  --target "$(tofu output -raw quack_instance_id)"
sudo journalctl -u quack -n 50
```

The service exits rather than lingering when a serve call is rejected, so
a failure shows up as a restart loop in that log rather than as a healthy
unit with no listener.

## State and secrets

State is local and gitignored. It holds the Postgres password, the Quack
token, and the server's key in clear text, so keep it off shared storage.
The token and key also travel in EC2 user data, which anyone on the
instance or holding `ec2:DescribeInstanceAttribute` can read. The runner
needs no access key: its instance profile carries the bucket grants.
Nothing here writes a secret into the repository.

## Teardown

```sh
tofu destroy
```

RDS and both instances bill per hour, so destroy them as soon as a sweep
is done, keeping the AMI snapshot if you made one. Both buckets are
created with `force_destroy`, so teardown deletes benchmark data with them;
objects in the standard bucket also expire after seven days.

## Assumptions

- Every command names the region explicitly, from the `region` output.
  These resources are in one region whatever your CLI defaults to, and an
  SSM call to the wrong one reports the instance as not connected rather
  than as absent.
- The region has a default VPC with public subnets, and a subnet in the
  zone of `express_availability_zone_id`.
- That zone offers S3 Express. The default, `use1-az4`, does; change it
  with the region.
