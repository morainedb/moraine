# Moraine

[![CI](https://github.com/morainedb/moraine/actions/workflows/ci.yml/badge.svg)](https://github.com/morainedb/moraine/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![crates.io](https://img.shields.io/crates/v/moraine.svg)](https://crates.io/crates/moraine)
[![docs.rs](https://docs.rs/moraine/badge.svg)](https://docs.rs/moraine)

Run a DuckLake data warehouse entirely in object storage: many readers, one
writer, no catalog server. Documentation: <https://morainedb.github.io/>.

[DuckLake](https://ducklake.select) typically stores table data as Parquet
files in object storage and keeps its catalog in a SQL database: a DuckDB or
SQLite file for one process; Postgres, or DuckDB served over
[Quack](https://duckdb.org/quack/), its new client-server protocol, for
several.

Moraine hosts that catalog database in [SlateDB](https://slatedb.io), a
transactional key-value store that also lives in object storage. The catalog
sits in the bucket beside the data files. DuckLake's SQL runs against it
unchanged.

```sql
LOAD 'moraine.v2.0.0.linux_amd64.duckdb_extension';

CREATE SECRET s (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-west-2');
ATTACH 'ducklake:moraine:s3://bucket/lake' AS lake
  (DATA_PATH 's3://bucket/lake-data/', READ_WRITE);

CREATE TABLE lake.events (id BIGINT, payload VARCHAR);
INSERT INTO lake.events VALUES (1, 'hello, bucket'), (2, 'hello again');
SELECT * FROM lake.events;

CREATE UNIQUE INDEX events_by_id ON lake.events (id);
SELECT payload FROM lake.events WHERE id = 2;
```

## What you get

- **No catalog server.** Nothing to deploy, monitor, or back up. A deployment
  is a storage bucket.
- **Bucket durability.** The catalog is objects in the bucket, so it gets the
  bucket's durability, replication, and versioning. A copy taken while no
  writer is attached is a complete lake; see
  [Backups and copies](https://morainedb.github.io/guide/operating/#backups-and-copies).
- **Nothing to pay for while idle.** An idle lake costs only storage, no
  compute or database hosting fees.
- **Indexed lookups.** Ordered indexes over table columns, created with
  `CREATE INDEX` and maintained in the same commit as the rows, so a query by
  key probes the index instead of scanning every file; see
  [Index a column](https://morainedb.github.io/guide/getting-started/#index-a-column).

## When not to use this

- **Commits under about 5 ms.** A commit is durable once one write-ahead-log
  PUT lands: 50 to 100 ms on S3 Standard, 5 to 10 ms with the log on S3
  Express One Zone via
  [`WAL_PATH`](https://morainedb.github.io/guide/configuration/#the-write-ahead-log-on-a-faster-bucket).
- **Many independent writer processes.** One process writes; the rest attach
  `READ_ONLY`. Concurrent commits go through that one writer. See
  [One writer, many readers](https://morainedb.github.io/guide/getting-started/#one-writer-many-readers).
- **Engines other than DuckDB.** The catalog is read through the DuckDB
  extension or the Rust crate, not by other DuckLake clients.
- **A signed extension, today.** Not yet in DuckDB's community repository: it
  loads with `-unsigned`, one build per DuckDB version.

## Status

Moraine is under active development and in production use at
[contour.app](https://contour.app). DuckLake SQL, including `CREATE`,
`INSERT`, `UPDATE`, `DELETE`, time travel, and maintenance, runs against
Moraine as its catalog and is validated against real DuckDB in CI;
[How Moraine is tested](https://morainedb.github.io/guide/testing/) covers
crash recovery, writer fencing, and fuzzing. The [roadmap](ROADMAP.md) lists
what is done and what is next. Questions and ideas go to
[Discussions](https://github.com/morainedb/moraine/discussions).

## More details

- [Getting started](https://morainedb.github.io/guide/getting-started/) installs
  the extension and attaches a lake.
- [Configuration](https://morainedb.github.io/guide/configuration/) lists every
  attach option and function.
- [Operating manual](https://morainedb.github.io/guide/operating/) covers
  retention, maintenance, encryption, backups, and upgrades.
- [Compared with other catalogs](https://morainedb.github.io/guide/comparison/)
  sets Moraine beside the DuckDB file, Postgres, Quack, and Iceberg-style
  catalogs.
- [Architecture](https://morainedb.github.io/guide/architecture/) explains how
  the catalog is laid out and how commits and reads work.
- [Design documents](https://morainedb.github.io/rfcs/) specify each decision.
- [Benchmarks](https://morainedb.github.io/benchmarks/) measures the metadata
  path and compares Moraine with the other DuckLake catalogs.
- [Releases](https://morainedb.github.io/releases/) has the changelog,
  extension downloads, and the DuckDB compatibility table.

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
