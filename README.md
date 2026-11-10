# moraine

[![CI](https://github.com/morainedb/moraine/actions/workflows/ci.yml/badge.svg)](https://github.com/morainedb/moraine/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![crates.io](https://img.shields.io/crates/v/moraine.svg)](https://crates.io/crates/moraine)
[![docs.rs](https://docs.rs/moraine/badge.svg)](https://docs.rs/moraine)

Moraine brings a [SlateDB](https://slatedb.io) backend to
[DuckLake](https://ducklake.select): a DuckLake catalog implemented on a
transactional KV store over object storage, instead of the usual relational
catalog database. Guides and design documents live on the project site:
<https://morainedb.github.io/>.

> **Status: pre-1.0, under active development.** The catalog core and
> DuckDB extension work end to end: DuckLake SQL, including `CREATE`,
> `INSERT`, `UPDATE`, `DELETE`, time travel, and maintenance, runs against
> moraine as its catalog and is validated against real DuckDB in CI.
> Released on crates.io; APIs may still change before 1.0.

## Why

DuckLake keeps table data in object storage but stores its catalog, the
transactional source of truth, in a SQL database: a DuckDB file locally,
Postgres or MySQL for concurrent access. That catalog is the one stateful
server left in an otherwise serverless lakehouse: something to provision,
back up, fail over, and pay for while idle.

Moraine replaces it with [SlateDB](https://slatedb.io), a transactional KV
store whose state lives in object storage, so the catalog sits in the
bucket next to the Parquet files. There is no catalog server to deploy,
back up, or pay for while idle. The catalog is as durable as the bucket,
and copying the bucket copies data and catalog together. The core is a
plain Rust library, so any tokio host can read and commit against the
catalog without DuckDB in the path.

The cost is commit latency. A commit is durable once one object-store PUT
lands: 5 to 10 ms on S3 Express One Zone, 50 to 100 ms on S3 Standard.
Workloads that write Parquet for seconds before committing will not notice,
and small inserts use DuckLake's data inlining to avoid a Parquet file per
commit. Workloads that need sub-millisecond commits want a server with
local state, and moraine does not compete there.

## One writer, many readers

Exactly one process may attach a moraine lake read-write. Every other
process must attach `READ_ONLY`. Readers are uncoordinated and unbounded in
number, and never disturb the writer. SlateDB's writer fencing gives the
writer role to the newest writer, so a second process attaching read-write
fences the first instead of failing, and two of them take turns fencing
each other's committer.

```sql
-- The one writer.
ATTACH 'ducklake:moraine:s3://bucket/lake' AS lake
       (DATA_PATH 's3://bucket/lake-data/', READ_WRITE);
-- Everyone else.
ATTACH 'ducklake:moraine:s3://bucket/lake' AS lake
       (DATA_PATH 's3://bucket/lake-data/', READ_ONLY);
```

A fenced writer writes nothing and corrupts nothing, so safety is never at
risk. Availability is, so this is a real limitation next to the
multi-client SQL catalogs DuckLake otherwise uses: a fleet of independent
DuckDB processes writing one lake is not a topology moraine supports on its
own. A deployment that needs commit concurrency funnels its commits through
one long-lived writer process. Concurrent commits inside that process share
a flush, so the funnel is not the throughput ceiling it looks like.

`READ_ONLY` is read-only at the catalog level, not the IAM level. A reader
that follows the latest state writes a checkpoint into the manifest and
refreshes it while it lives, so its credentials still need write access. A
deployment whose readers hold strictly read-only credentials pins them to a
checkpoint taken ahead of time with `CHECKPOINT`, which writes nothing and
reads the fixed cut that checkpoint named. See
[`crates/moraine-duckdb/README.md`](crates/moraine-duckdb/README.md) for
the attach surface.

## Architecture

- `crates/moraine` is the core library: DuckLake catalog semantics
  (snapshots, schemas, tables, transactional commits) mapped onto SlateDB's
  KV model. Pure Rust, embeddable in any tokio host.
- `crates/moraine-duckdb` is a DuckDB extension wrapping the core, so
  DuckLake can use moraine as its catalog backend.

How the pieces fit is in [`ARCHITECTURE.md`](ARCHITECTURE.md). Design
decisions are recorded in [`docs/rfcs/`](docs/rfcs/README.md). The
[roadmap](ROADMAP.md) tracks each feature.

## Versioning

Pre-1.0 semver: breaking changes bump the minor version.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at
your option. Contributions are accepted under the same terms; see
[CONTRIBUTING.md](CONTRIBUTING.md).
