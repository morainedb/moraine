# Architecture

This is the map of moraine: what the pieces are, how they fit, and where each
decision is specified in full. It is a reading guide, not a specification —
every claim here has an authoritative RFC in [`docs/rfcs/`](docs/rfcs/), linked
inline. For *what* moraine is and *why*, start with the [README](README.md);
for *where it's going*, the [ROADMAP](ROADMAP.md); for *how to work in it*,
[CONTRIBUTING.md](CONTRIBUTING.md).

> **Status: pre-1.0, actively developed.** The architecture below is
> implemented and exercised end-to-end through real DuckDB; a few features
> remain (see the [ROADMAP](ROADMAP.md)).

## What moraine is

DuckLake keeps table data in object storage but stores its catalog — the
transactional source of truth — in a SQL database (DuckDB, Postgres, MySQL).
Moraine replaces that database with [SlateDB](https://slatedb.io), a
transactional KV store whose entire state lives in object storage. The catalog
then sits in the bucket next to the Parquet files: nothing to operate,
durability for free, the bucket is the whole lake. The trade-off and the
motivation are in the [README](README.md#why).

Architecturally, moraine is **DuckLake's catalog backend** — it occupies
exactly the slot a Postgres/SQLite/DuckDB catalog database occupies, and
implements DuckLake's contract rather than inventing its own semantics
([RFC 0006](docs/rfcs/0006-extension-surface.md)).

## The stack

```
DuckDB engine
  └─ ducklake extension        planner, transactions, query execution
       └─ moraine catalog       DuckDB StorageExtension  (the extension surface)
            └─ moraine core      DuckLake catalog semantics on SlateDB  (Rust)
                 └─ SlateDB → object store
```

DuckLake stays the query/planner/transaction layer. Moraine serves the
`ducklake_*` metadata tables **row-faithfully** — the tables *are* the catalog
state, not a re-modeled projection — out of SlateDB.

## Crates

A Cargo workspace with a virtual root (no root crate). The full rationale is
[RFC 0001](docs/rfcs/0001-repository-structure-and-conventions.md).

| Crate | Type | Role |
|---|---|---|
| [`moraine`](crates/moraine) | lib | **The core.** DuckLake catalog semantics on SlateDB. Pure Rust, async, embeddable in any tokio host. The flagship crate — all the hard problems live here. |
| [`moraine-duckdb`](crates/moraine-duckdb) | cdylib + C++ shim | The DuckDB extension wrapping the core. **Thin by policy**: no domain logic, only `StorageExtension` registration, C-ABI marshalling, and the sync↔async bridge. If logic accumulates here, it belongs in the core. |
| [`xtask`](xtask) | bin (unpublished) | Automation (`cargo xtask e2e`): build/package the extension, orchestrate the e2e suite. Rust instead of shell scripts — cross-platform and type-checked. |

**Library-first.** The catalog logic lives in `moraine`, free of the FFI/build
tax and testable as plain Rust against a real in-memory store. The extension is
one consumer of it, not the center of gravity.

## Inside the core

`crates/moraine/src` is three modules with a deliberate one-way dependency, so
each decision has one home:

```
lib.rs      # crate docs + re-exports only; no logic
error.rs    # one thiserror Error enum, one variant per failure domain
catalog.rs  # DuckLake domain: snapshots, schemas, tables, data-file metadata
store.rs    # SlateDB layer: key layout + value codec
transaction.rs      # commit protocol: catalog transaction → one atomic SlateDB write
```

The load-bearing rule: **`catalog` never touches SlateDB directly; `store`
knows nothing about DuckLake semantics; `transaction` bridges them.** This keeps
catalog logic testable against an in-memory store and concentrates every
key-encoding decision in one reviewable place.

The public surface ([RFC 0003](docs/rfcs/0003-public-api-shape.md)) is three
types over that error taxonomy — SlateDB never appears in a public signature:

- **`Catalog`** — the handle. `Catalog::open(object_store, options)`; cheap to
  clone; drives reads and commits.
- **`CatalogSnapshot`** — an immutable, materialized read view. Built by
  scanning live state once, then answered entirely in memory — a value, not a
  cursor.
- **`Transaction`** — the mutation handle passed to a commit closure. `Deref`s to
  `CatalogSnapshot` (reads available inside a commit) and adds DuckLake-shaped
  mutators (`create_table`, `register_data_file`, …).

Writes go through a **closure-with-retry** model, so the single-`WriteBatch`
atomicity invariant and the conflict-retry loop live in the core — never
duplicated in a host:

```rust
let new_snapshot = catalog.commit(|tx| {
    let s = tx.create_schema("sales")?;
    let t = tx.create_table(s, "orders", columns)?;
    tx.register_data_file(t, data_file)?;
    Ok(())
}).await?;
```

The closure may run more than once (benign-race retry), so it must be pure and
idempotent — the single most important contract of the API.

## Storage model

How DuckLake catalog state is laid out in SlateDB is the single
most expensive-to-reverse decision in the project; it is
[RFC 0002](docs/rfcs/0002-slatedb-key-encoding.md).

- **Keyspace partitioned by a leading tag byte** into subspaces: `system` (format
  version, head pointer), `snapshot` (one immutable record per snapshot), `current`
  (live catalog entities), `history` (ended entity versions), `inline` (inlined
  data, [RFC 0005](docs/rfcs/0005-data-inlining.md)).
- **The `current`/`history` split is the load-bearing decision.** DuckLake versions
  entities temporally (`begin_snapshot`/`end_snapshot`). Rather than store one
  history stream and filter it on every read, moraine keeps *live* versions in
  `current` and moves *ended* versions to `history` atomically as part of the same
  commit. Loading the current catalog — the hot path, every attach — scans
  `current` only, at a cost proportional to the live catalog, never its history.
  Time travel scans the relevant `history` ranges too.
- **Keys are `tag · kind · u64 components`**, big-endian fixed-width so byte
  order equals numeric order. No strings in keys; entities are keyed by
  DuckLake-allocated ids, names live in values. Per-table collections are
  keyed `table_id`-first so "everything about table T" is one contiguous range.
- **Values are protobuf** (`prost`) behind a 5-byte framing header (`MRNE` magic
  + encoding version), so a corrupt/wrong-kind value fails loudly and fields can
  evolve without rewriting the store.
- **Property tests are mandatory** for anything in `store` that encodes/decodes:
  key roundtrips, byte-order-equals-component-order, value roundtrips, and
  framing rejection.

## Commit protocol

Turning a catalog mutation into a durable, atomic commit is
[RFC 0004](docs/rfcs/0004-commit-protocol.md) over the topology
[RFC 0022](docs/rfcs/0022-commit-log-and-leader-role.md) fixes, built on RFC
0002's invariant that **one commit is exactly one atomic unit**: a commit-log
slot at commit time, and one SlateDB `WriteBatch` later, when the folder
applies it.

- **Topology: fleet multi-writer over a commit log.** Every commit races a
  conditional put against an object-storage commit log; any number of
  processes may commit concurrently, with nothing beyond the bucket
  coordinating them. SlateDB's one fenced writer still exists, but it belongs
  to the **folder** role — tailing the log into SlateDB as a derived index —
  not the commit path, so a dead folder cannot lose a commit, only lengthen
  the tail a reader replays.
- **Optimistic, log-arbitrated.** A commit materializes the head (store state
  plus tail replay past the fold cursor), allocates ids locally, stages one
  payload, and races it into the next log slot. A win is durable at the ack;
  nothing spans two atomic units.
- **Table-level conflict detection**, with one file-grain exception. On a lost
  race, the committer compares the `table_id`s it touched against the
  intervening commits' — except delete-versus-delete, which compares the data
  files each targeted, matching the one place DuckLake goes finer. Disjoint
  sets are a benign race, retried internally; overlapping sets are a true
  `Conflict` aborted with a typed error — the core is DuckLake-agnostic and
  cannot replay the originating SQL, so re-driving belongs to whoever authored
  the operation.
- **Group commit** is permitted — a process with several pending commits can
  chain them into one slot, and concurrent committers in one process are
  coalesced into a single slot without asking — but never required; a chain of
  one is the normal path.

## Extension surface

`moraine-duckdb` makes the core reachable from DuckDB
([RFC 0006](docs/rfcs/0006-extension-surface.md)).

- DuckDB's *stable C extension API cannot register a catalog*, and a writable
  DuckLake catalog requires a `StorageExtension`. So the extension is a **thin
  C++ shim** linking DuckDB's internals, calling a **minimal C ABI** into the
  Rust core (compiled as a staticlib): open/attach, begin/commit/rollback,
  `scan(table, projection, filters) -> Arrow`, `apply(mutations)`.
- Interception is at the **catalog / table-scan / DML layer** (the
  `postgres_scanner` pattern), never by parsing SQL — DuckDB's own executor
  plans DuckLake's statements and calls moraine per table.
- The ABI boundary is the **Arrow C Data Interface** — stable, language-neutral,
  near-zero-copy.
- The **sync↔async bridge** lives in the Rust C-ABI layer: it owns a tokio
  runtime and `block_on`s core futures, so the C++ shim only ever calls
  synchronous functions. The mechanism, runtime shape, cancellation, and
  panic-containment are [RFC 0010](docs/rfcs/0010-async-sync-bridge.md).
- Linking DuckDB's C++ internals pins the extension to a single supported DuckDB
  release — the "FFI/build/version-pinning tax" RFC 0001 deliberately deferred
  to this boundary and pays only here.

## Beyond the core loop

The RFCs also specify the operations a catalog needs past open/read/commit:

- **Data inlining** ([RFC 0005](docs/rfcs/0005-data-inlining.md)) — small
  inserts land as rows in the catalog and flush to Parquet later, skipping the
  tiny-Parquet-file tax. A launch feature: the workload an LSM is built for, and
  where a KV catalog beats a SQL one rather than merely matching it.
- **Snapshot expiry & GC** ([RFC 0007](docs/rfcs/0007-snapshot-expiry-and-gc.md))
  — reclaiming `history`/`snapshot`/flushed-inline records and cleaning up orphaned
  object-store files, as ordinary commits guarded by a retention horizon and a
  grace window.
- **Compaction** ([RFC 0008](docs/rfcs/0008-compaction.md)) — merging small data
  files and materializing merge-on-read deletes, while preserving DuckLake
  row-id lineage (compaction never allocates row ids).
- **Reader consistency & caching**
  ([RFC 0009](docs/rfcs/0009-reader-consistency-and-caching.md)) — pinning a
  `CatalogSnapshot` to one SlateDB read-snapshot, refreshing it incrementally
  from the `snapshot_changes` changelog, and observing snapshot isolation.

## Testing strategy

Testing is a first-class part of the architecture, because a catalog's worst
failure mode is silent corruption. Tiers, per
[RFC 0001](docs/rfcs/0001-repository-structure-and-conventions.md#testing-strategy):

| Tier | Home | What |
|---|---|---|
| **Unit** | colocated `#[cfg(test)]` | Tricky internals: key encoding, codecs, conflict resolution. **Proptest roundtrips mandatory** in `store`. |
| **Integration** | `crates/moraine/tests/` | Public API against **real SlateDB on in-memory `object_store` — no mocks of the store layer.** Snapshot visibility, concurrent-commit conflicts, crash-shaped sequences. |
| **E2E** | `crates/moraine-duckdb/tests/`, via `cargo xtask e2e` | Build the cdylib, load into real DuckDB, run actual DuckLake SQL — validating assumptions about what DuckLake demands, not just that the code does what we think. |
| **Fuzzing** (future) | `fuzz/` | `cargo-fuzz` on `store` codecs and the commit read-path once codecs stabilize. |

Crash coverage is not left to prose: [RFC 0011](docs/rfcs/0011-crash-recovery.md)
names every place a process can die during a state-changing operation — commit,
flush, cleanup, takeover, genesis — and pins each to what must hold after
reopen. Each case is survivable for one of two reasons, and which one follows
from the path: a single-batch path is safe by *atomicity* (one commit is one
batch, so there is no torn intermediate to find), a multi-step path by
*ordering* (bytes before the record that references them, bytes after the
record that referenced them). The suite drives the cases as data.

Process is **TDD** — test first, watch it fail, implement — and every bugfix
lands with a regression test. The full local gate (fmt, clippy, test, doc, deny,
e2e) is in [CONTRIBUTING.md](CONTRIBUTING.md#the-local-gate).

## Where decisions live

RFCs are the source of truth for anything expensive to reverse — the KV key
layout, the commit protocol, and the public API shape *require* one. The full
index is [`docs/rfcs/README.md`](docs/rfcs/README.md).
