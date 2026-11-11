---
title: Architecture
description: How Moraine fits under DuckDB and DuckLake, and how it lays a catalog out in SlateDB.
sidebar:
  order: 7
---

DuckLake keeps table data in object storage and its catalog, the transactional
source of truth, in a SQL database. Moraine replaces that database with
[SlateDB](https://slatedb.io), a transactional key-value store whose state lives
in object storage, so the catalog sits in the bucket next to the Parquet files.
It implements the same catalog interface as the Postgres, SQLite, and DuckDB
backends.

## The stack

```text
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

- `moraine` is the core: snapshots, schemas, tables, data-file metadata,
  and transactional commits mapped onto SlateDB's key-value model. Pure
  Rust, async, and usable without DuckDB: a tokio host embeds it directly
  ([docs.rs/moraine](https://docs.rs/moraine)).
- `moraine-duckdb` registers the DuckDB `StorageExtension` and calls the Rust
  core through a C ABI and sync-to-async bridge. DuckDB's stable C extension API
  cannot register a catalog, so the shim links DuckDB's internals, which pins
  each build to one DuckDB release.

## Storage model

The keyspace is partitioned by a leading tag byte into subspaces: `system`
(format version, head pointer), `snapshot`, `current` (live entities),
`history` (ended versions), `inline` (inlined rows), `index`
(equality-index entries), and a few smaller ones.

DuckLake versions
every entity with a begin and end snapshot. Moraine keeps live versions in
`current` and moves ended versions to `history` in the same commit, so
loading the live catalog scans `current` only, at a cost proportional to
the live catalog rather than its history. Time travel reads `history` too.

Keys are an enum tree with fixed-width big-endian numeric components, so
byte order equals numeric order and each table's collections are
contiguous ranges. Values are protobuf behind a versioned framing header. A
format version in `system` identifies the key layout. Stores below a
build's supported range require an explicit migration before opening.

## Commits

One DuckLake mutation is exactly one SlateDB write batch. A commit reads
the head pointer, allocates ids locally, stages the batch, and writes it
conditional on the head being unchanged. The durable floor is one
write-ahead-log flush: a commit is durable once one object-store PUT lands.

Concurrent transactions in the writer process can race. Lost races are
classified against DuckLake's own conflict rules, at table grain, with
delete-versus-delete compared per data file. Disjoint change sets are a benign
race and retry inside the catalog; overlapping sets return a typed conflict for
DuckLake to re-drive. Concurrent commits in one writer process share a flush,
each minting its own snapshot.

The write-ahead log may live on its own object store, so the one
latency-critical PUT can land on S3 Express One Zone while the rest of the
catalog stays on standard storage. A commit whose outcome could not be
observed is reported as unknown and never replayed automatically.

## Reads

Every read runs at one fixed store revision and decodes only the records
it asks for. The writer reads through a SlateDB snapshot; read-only
attaches follow the writer through checkpoints it publishes at durable
boundaries. Catalog reads are cached in one block cache per process, shared
by every attached store, and per-file row summaries sit beside the files
they describe so pruning does not open Parquet footers.

Small inserts and deletes can be inlined: stored as rows in the catalog
and flushed to Parquet later, so a stream of tiny commits does not produce
a Parquet file each.

## Writers and readers

One process attaches a lake read-write; every other process attaches
`READ_ONLY`. SlateDB fences by newest writer: a second read-write attach takes
the writer role, and the first writer's next commit fails with a fencing error.
[Getting started](../getting-started/#one-writer-many-readers) covers recovery
and what readers' credentials need.

## Further reading

The [design documents](/rfcs/) describe
the [storage format](/rfcs/0001-storage-format/), the
[commit protocol](/rfcs/0003-commit-protocol/), the
[read path](/rfcs/0007-read-path-and-caching/), and the
[extension surface](/rfcs/0006-extension-surface/) in detail.
