# Moraine

[![CI](https://github.com/morainedb/moraine/actions/workflows/ci.yml/badge.svg)](https://github.com/morainedb/moraine/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![crates.io](https://img.shields.io/crates/v/moraine.svg)](https://crates.io/crates/moraine)
[![docs.rs](https://docs.rs/moraine/badge.svg)](https://docs.rs/moraine)

Run DuckLake entirely in object storage, no server required. Documentation:
<https://morainedb.com/>.

[DuckLake](https://ducklake.select) stores table data as Parquet files in object
storage and hosts its catalog in a SQL database: SQLite, Postgres, or DuckDB
(served over [Quack](https://duckdb.org/quack/)). The database layer provides
transactional consistency and ACID guarantees, but requires hosting an
additional server.

Moraine removes the need for a separate catalog database. Instead, it serves the
catalog from [SlateDB](https://slatedb.io), an embedded transactional key-value
store that also lives in object storage. That means that the catalog resides in
the same bucket as the data files. In addition, Moraine is the only DuckLake
extension that offers DuckDB native indexes for faster querying.

```sql
LOAD 'moraine.v2.0.0.linux_amd64.duckdb_extension';

CREATE SECRET aws_secret (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-west-2');
ATTACH 'ducklake:moraine:s3://bucket/lake' AS lake
  (DATA_PATH 's3://bucket/lake-data/', READ_WRITE);

CREATE TABLE lake.events (id BIGINT, payload VARCHAR);
INSERT INTO lake.events VALUES (1, 'hello, bucket'), (2, 'hello again');
SELECT * FROM lake.events;

CREATE UNIQUE INDEX events_by_id ON lake.events (id);
SELECT payload FROM lake.events WHERE id = 2;
```

## What you get

- **No catalog server:** No catalog server to deploy or monitor. The only
  permanent resource is a storage bucket.
- **Nothing to pay for while idle:** An idle lake costs only storage, no
  compute or database hosting fees.
- **Bucket durability:** The catalog is objects in the bucket, so it inherits
  the bucket's durability, replication, and versioning. A copy taken while no
  writer is attached is a complete lake; see
  [Backups and copies](https://morainedb.com/guide/operating/#backups-and-copies).
- **Indexed lookups:** Ordered indexes over table columns, created with
  `CREATE INDEX` and maintained in the same commit as the rows, so a query
  by key probes the index instead of scanning every file; see
  [Index a column](https://morainedb.com/guide/getting-started/#index-a-column).

## When not to use Moraine

- **Sub-5 ms commits.** A commit is durable once one write-ahead-log PUT
  lands: 50 to 100 ms on S3 Standard, 5 to 10 ms with the log on S3 Express
  One Zone via
  [`META_WAL_PATH`](https://morainedb.com/guide/configuration/#the-write-ahead-log-on-a-faster-and-cheaper-bucket).
- **Many independent writer processes.** One process writes; the rest
  attach `READ_ONLY`. See
  [one writer, many readers](https://morainedb.com/guide/getting-started/#one-writer-many-readers).
- **`INSTALL moraine FROM community`.** The listing is in progress. Until
  then, signed builds come from Moraine's own extension repository, which
  takes one `CREATE EXTENSION REPOSITORY` to register.

## By the numbers

Identical DuckLake workloads through the same DuckDB CLI, only the catalog
swapped. Local disk, loopback Postgres 16, medians of three runs on an
8-vCPU x86-64 VM:

| | Moraine | DuckDB file | Postgres |
|---|---:|---:|---:|
| attach | 8 ms | 19 ms | 113 ms |
| create table and insert 100,000 rows | 94 ms | 102 ms | 124 ms |
| 20 single-row commits, one connection | 254 ms | 218 ms | 135 ms |
| 128 single-row commits over 8 connections | 417 ms | 1.76 s | 1.77 s |
| 128 single-row commits over 16 connections | 461 ms | 2.85 s | 2.94 s |

Serial commits pay a write-ahead-log flush each, where a local Postgres is
faster. Overlapping commits can share one flush, giving Moraine more competitive
performance. On S3, each flush is one PUT round trip, about 30 ms same-region.
For more details, see [Benchmarks](https://morainedb.com/benchmarks/).

## Status

Moraine is under active development and in production use at
[contour.app](https://contour.app). All DuckLake SQL, including `CREATE`,
`INSERT`, `UPDATE`, `DELETE`, time travel, and maintenance, runs against
Moraine. Questions and ideas go to
[Discussions](https://github.com/morainedb/moraine/discussions).

## More details

- [Getting started](https://morainedb.com/guide/getting-started/): how to
  install the extension and attach a lake.
- [Configuration](https://morainedb.com/guide/configuration/): attach
  options, environment variables, and DuckDB functions.
- [Operating manual](https://morainedb.com/guide/operating/): retention,
  maintenance, encryption, backups, and upgrades.
- [Comparison](https://morainedb.com/guide/comparison/): Moraine
  alongside DuckDB, Postgres, Quack, and Iceberg-style catalogs.
- [Design](https://morainedb.com/rfcs/): key design decisions and
  rationale.
- [Benchmarks](https://morainedb.com/benchmarks/): performance benchmarks
  against other DuckLake catalogs.
- [Releases](https://morainedb.com/releases/): changelog, extension
  downloads, and the DuckDB compatibility table.
- [Roadmap](https://morainedb.com/roadmap/): what's next for Moraine.

## In this repository

- `crates/moraine` is the core library: DuckLake catalog semantics mapped onto
  SlateDB's key-value model. Pure Rust, embeddable in any tokio host.
- `crates/moraine-duckdb` is the DuckDB extension wrapping the core.
- [`ARCHITECTURE.md`](ARCHITECTURE.md) is the map of the code;
  [`docs/rfcs/`](docs/rfcs/README.md) holds the design documents;
  [`docs/benchmarks.md`](docs/benchmarks.md) catalogs the measurement
  harnesses and their headline numbers;
  [`CONTRIBUTING.md`](CONTRIBUTING.md) covers setup and the local gate.

## Versioning

Pre-1.0 semver: breaking changes bump the minor version.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at
your option. Contributions are accepted under the same terms; see
[CONTRIBUTING.md](CONTRIBUTING.md).
