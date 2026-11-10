---
title: Overview
description: What moraine is, what it replaces, and what it costs.
sidebar:
  order: 1
---

[DuckLake](https://ducklake.select) stores table data as Parquet files in
object storage and keeps its catalog, the transactional source of truth,
in a SQL database: a DuckDB file for one process, Postgres or MySQL for
several. That database is the one server left in an otherwise serverless
lakehouse.

Moraine is a DuckLake catalog backend that replaces the database with
[SlateDB](https://slatedb.io), a transactional key-value store whose state
lives in object storage. The catalog sits in the bucket beside the data
files.

## What that changes

- There is no catalog server to deploy, monitor, back up, or pay for while
  idle. A deployment is a bucket and credentials.
- The catalog is as durable as the bucket. Copying or replicating the
  bucket copies data and catalog together.
- An idle lake costs storage.
- The core is a Rust library. Any tokio host can read and commit against
  the catalog without DuckDB in the path.

## The cost

A commit is durable once one object-store PUT lands. On S3 Standard that is
50 to 100 ms; on S3 Express One Zone, 5 to 10 ms. Workloads that write
Parquet for seconds before committing will not notice. Small inserts use
DuckLake's data inlining and avoid writing a Parquet file per commit.
Workloads that need sub-millisecond commits want a server with local
state, and moraine does not compete there.

## One writer, many readers

Exactly one process may attach a lake read-write. Every other process
attaches `READ_ONLY`. SlateDB fences by newest writer, so a second
read-write attach does not fail: it takes the writer role, and the first
writer's next commit fails with a fencing error. A deployment that needs
concurrent commits funnels them through one writer process, where
concurrent commits share a flush.

## How it fits together

```
DuckDB engine
  └─ ducklake extension        planner, transactions, query execution
       └─ moraine extension    DuckDB StorageExtension, a thin C++ shim
            └─ moraine core    DuckLake catalog semantics on SlateDB (Rust)
                 └─ SlateDB → object store
```

DuckLake remains the planner and transaction layer. Moraine serves the
`ducklake_*` metadata tables row for row out of SlateDB, so DuckLake's own
SQL runs unchanged against it.

Two crates:

- `moraine` is the core: snapshots, schemas, tables, and transactional
  commits mapped onto SlateDB's key-value model. Pure Rust, async.
- `moraine-duckdb` is the DuckDB extension. It holds no domain logic, only
  the `StorageExtension` registration, the C ABI, and the sync-to-async
  bridge.

## Using the core from Rust

```toml
[dependencies]
moraine = "0.7"
```

The crate-root documentation on [docs.rs](https://docs.rs/moraine) teaches
the API by example. Pre-1.0 semver applies: breaking changes bump the minor
version.

## Status

Pre-1.0 and under active development. DuckLake SQL, including `CREATE`,
`INSERT`, `UPDATE`, `DELETE`, time travel, and maintenance, runs against
moraine as its catalog and is validated against real DuckDB in CI. The
[roadmap](https://github.com/morainedb/moraine/blob/main/ROADMAP.md) lists
what is done and what is not.
