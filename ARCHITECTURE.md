# Architecture

This is the map of Moraine: what the pieces are, how they fit, and where each
decision is specified. Every design decision has a document in
[`docs/rfcs/`](docs/rfcs/README.md), linked inline. For what Moraine is, start
with the [README](README.md); for where it is going, the [ROADMAP](ROADMAP.md);
for how to work in it, [CONTRIBUTING.md](CONTRIBUTING.md).

Status: pre-1.0, under active development. Everything below is implemented
and exercised end to end through real DuckDB.

## What Moraine is

Moraine is a DuckLake catalog backend on SlateDB; the
[site's architecture page](https://morainedb.com/rfcs/architecture/)
explains it for users. This file maps the code.

```text
DuckDB engine
  └─ ducklake extension        planner, transactions, query execution
       └─ moraine extension    DuckDB StorageExtension, a thin C++ shim
            └─ moraine core    DuckLake catalog semantics on SlateDB (Rust)
                 └─ SlateDB → object store
```

DuckLake stays the planner and transaction layer. Moraine serves the
`ducklake_*` metadata tables row for row out of SlateDB: the tables are the
catalog state, not a projection of it.

## Crates

A Cargo workspace with a virtual root.

| Crate | Type | Role |
|---|---|---|
| [`moraine`](crates/moraine) | lib | The core. DuckLake catalog semantics on SlateDB. Pure Rust, async, embeddable in any tokio host. |
| [`moraine-duckdb`](crates/moraine-duckdb) | cdylib + C++ shim | The DuckDB extension. No domain logic: `StorageExtension` registration, the C ABI, and the sync-to-async bridge. Logic that accumulates here moves to the core. |
| [`xtask`](xtask) | bin, unpublished | Automation: build and package the extension, run the e2e suite, check pins. |

The catalog logic lives in `moraine`, free of the FFI and build tax and
testable as plain Rust against a real in-memory store. The extension is one
consumer of it.

## Inside the core

`crates/moraine/src` is a small set of modules with one-way dependencies:

| Module | Owns |
|---|---|
| `lib.rs` | Crate docs and re-exports. No logic. |
| `error.rs` | One `Error` enum, one variant per failure domain. |
| `catalog` | The DuckLake domain: snapshots, schemas, tables, data-file metadata, read scopes, maintenance verbs. Holds the store handle but knows no keys or values. |
| `store` | The SlateDB layer: key layout, value codecs, opening, caching, checkpoints. Knows nothing about DuckLake. |
| `transaction` | The commit protocol. Turns a catalog transaction into one atomic SlateDB write. Bridges `catalog` and `store`. |
| `data_file` | The only module that reads Parquet: scoped reads of registered files. |
| `commit_log` | The boundary an external commit log plugs into. |
| `memory` | Working-memory reservations and host budgets. |
| `fault` | Crash and stall injection points used by the tests. |
| `telemetry` | Counters and log records. |
| `ffi_support` | Helpers shared with the extension's C ABI. |
| `fuzz` | Entry points for the fuzz targets under `fuzz/`. |

`catalog` never touches SlateDB directly, `store` knows nothing about
DuckLake, and `transaction` bridges them. This keeps catalog logic testable
against an in-memory store and concentrates every key-encoding decision in
one place.

The [public API](docs/rfcs/0002-public-api.md) is a few types over that
error enum. SlateDB never appears in a public signature.

- `Catalog` is the handle: `Catalog::open(object_store, options)`, cheap to
  clone, drives reads and commits. It derefs to `ReadOnlyCatalog`, which
  `open_read_only` returns, so the mode is in the type.
- `ReadScope` is a read pinned to one store revision. It loads catalog
  rows on demand through bounded cursors rather than materializing the
  catalog.
- `Transaction` is the mutation handle passed to a commit closure. It adds
  DuckLake-shaped mutators such as `create_table` and `register_data_file`.

Writes go through a closure with retry. The closure may run more than once
when a benign race is retried, so it must be pure and idempotent. That is
the one contract every embedder has to respect.

## Storage model

How catalog state is laid out in SlateDB is the most expensive decision to
reverse. It is the [storage format](docs/rfcs/0001-storage-format.md).

- The keyspace is partitioned by a leading tag byte into subspaces:
  `system` (format version, head pointer), `snapshot`, `current` (live
  entities), `history` (ended versions), `inline` (inlined rows), `index`
  (equality-index entries), and a few smaller ones.
- The `current`/`history` split is the central decision. DuckLake versions
  entities with begin and end snapshots. Moraine keeps live versions in
  `current` and moves ended versions to `history` in the same commit, so
  loading the live catalog scans `current` only, at a cost proportional to
  the live catalog rather than its history. Time travel reads `history`
  too.
- Keys are an enum tree with fixed-width big-endian numeric components, so
  byte order equals numeric order and per-table collections are contiguous
  ranges. Values are protobuf behind a magic-plus-version framing header.
- A format version in `system` gates key-structure changes. Migration is an
  explicit verb, never automatic on open.

## Commit protocol

Turning a mutation into a durable, atomic commit is the
[commit protocol](docs/rfcs/0003-commit-protocol.md). One commit is exactly
one SlateDB write batch.

- Topology: one writer, many readers. The newest writer fences the
  previous one. A deployment that needs concurrent commits funnels them
  through one writer process.
- Optimistic, head compare-and-set. A commit reads the head, allocates ids
  locally, stages one batch, and writes it conditional on the head being
  unchanged. The durable floor is one write-ahead-log flush.
- Table-level conflict detection over DuckLake's own change grammar, with
  delete-versus-delete compared at data-file grain. Disjoint sets are a
  benign race and retry internally; overlapping sets return a typed
  conflict for DuckLake to re-drive.
- Group commit batches concurrent commits into one flush, each minting its
  own snapshot.
- The write-ahead log may live on its own object store, so the one
  latency-critical PUT can land on S3 Express One Zone while the rest of
  the catalog stays on standard storage.
- A commit whose outcome could not be observed returns
  `CommitOutcomeUnknown` and is never replayed automatically.

The [commit-log seam](docs/rfcs/0011-commit-log-seam.md) is the boundary a
bucket-resident commit log, maintained in a separate library, plugs into for
multi-process writers. The single-writer path is complete without it.

## Extension surface

`moraine-duckdb` makes the core reachable from DuckDB
([extension surface](docs/rfcs/0006-extension-surface.md)).

- DuckDB's stable C extension API cannot register a catalog, and a writable
  DuckLake catalog needs a `StorageExtension`. So the extension is a thin
  C++ shim linking DuckDB's internals, calling a C ABI into the Rust core.
  That pins the extension to one DuckDB release; `cargo xtask check-pins`
  keeps every place that names the version in agreement.
- Interception is at the catalog, table-scan, and DML layer, never by
  parsing SQL. DuckDB's executor plans DuckLake's statements and calls
  Moraine per table. Rows cross the ABI as row structs.
- The sync-to-async bridge lives in the Rust ABI layer. It owns a tokio
  runtime per attached catalog and blocks on core futures, so the shim only
  calls synchronous functions. Cancellation polls DuckDB's interrupt flag;
  a commit already submitted completes and reports its outcome.

## Beyond the core loop

- [Data inlining](docs/rfcs/0004-data-inlining.md): small inserts land as
  rows in the catalog and flush to Parquet later.
- [DuckLake feature mapping](docs/rfcs/0005-ducklake-feature-mapping.md):
  how schema evolution, partitioning, name mapping, macros, encryption, and
  the change feed map onto the keyspace.
- [Read path, caching, and memory](docs/rfcs/0007-read-path-and-caching.md):
  reads pinned to a revision, checkpoint readers, the block cache, row
  summaries, and working-memory budgets.
- [Equality and range indexes](docs/rfcs/0009-equality-indexes.md): index
  entries keyed by canonical value encoding, uniqueness by key collision,
  staged builds, and index-driven row lookup.
- [Maintenance](docs/rfcs/0010-maintenance.md): how DuckLake's expiry,
  cleanup, and compaction translate onto the keyspace, and the scheduled
  pass that runs them.

## Testing strategy

A catalog's worst failure is silent corruption, so testing is part of the
architecture.

| Tier | Home | What |
|---|---|---|
| Unit | colocated `#[cfg(test)]` | Key encoding, codecs, conflict resolution. Proptest roundtrips are mandatory in `store`. |
| Integration | `crates/moraine/tests/` | The public API against real SlateDB on an in-memory `object_store`. No mocks of the store layer. |
| E2E | `crates/moraine-duckdb/tests/`, `test/sql`, via `cargo xtask e2e` | Build the extension, load it into real DuckDB, run DuckLake SQL. |
| Fuzzing | `fuzz/` | Store codecs, the commit read path, and crash recovery. |
| Measurement | `#[ignore]`d harnesses beside the tests, `cargo xtask bench` and kin | Print stamped numbers rather than assert; [docs/benchmarks.md](docs/benchmarks.md) catalogs them. |

[Crash recovery](docs/rfcs/0008-crash-recovery.md) names every point where
a process can die during a state-changing operation and what must hold
after reopen. A single-batch path is safe by atomicity; a multi-step path by
ordering. The suite drives the cases as data.

Process is test first, and every bug fix lands with a regression test. The
local gate is in [CONTRIBUTING.md](CONTRIBUTING.md#the-local-gate).
