# RFC 0003: Public API shape of the moraine core

- **Date:** 2026-07-09

## Summary

Defines the public API of the `moraine` core crate: how a host opens a
catalog, reads it, and commits changes to it. This is the third
expensive-to-reverse decision the project requires an RFC for (key layout —
RFC 0002/0005; commit protocol — RFC 0004; public API shape — here). The
surface is four types — `Catalog` (the read-write handle), `ReadOnlyCatalog`
(the read surface a read-only attach gets, which `Catalog` derefs to),
`CatalogSnapshot` (an immutable materialized read view), and `Transaction`
(the mutation handle passed to a commit closure) — over an error taxonomy with one variant per failure domain.
Writes go through a closure-with-retry model so the RFC 0002 single-`WriteBatch`
atomicity invariant and conflict-retry loop live in the core, not duplicated in
every host. SlateDB never appears in a public signature: the substrate is an
implementation detail behind `object_store` plus moraine's own options type.
The operation set is enumerated in full, grounded in DuckLake v1.0 catalog
semantics.

## Goals

- One read API, usable both standalone (`catalog.snapshot()`) and inside a
  commit (the `Transaction` handed to the closure exposes the same accessors). A host
  learns the accessors once.
- The atomicity invariant (RFC 0002: one catalog commit is exactly one SlateDB
  `WriteBatch`) and the conflict-retry loop are the core's responsibility.
- SlateDB is an implementation detail. No `slatedb::` type crosses the public
  boundary, so the substrate's version churn stays out of moraine's semver
  surface
- The `prost`-generated value types (RFC 0002) stay private to `store`.
- Every DuckLake v1.0 catalog mutation the entities in RFC 0002 imply has a
  named, DuckLake-shaped operation on `Transaction`. The version and `current`↔`history`
  bookkeeping is internal.
- Errors are matchable per failure domain, so the DuckDB bridge maps each to a
  DuckDB error code without parsing strings.

Non-goals:

- The commit protocol itself — conflict detection, snapshot allocation, group
  commit, the CAS/fencing discipline on `sys/head`. That is RFC 0004.
- The DuckDB extension entry points (RFC 0006) and the sync↔async bridge
  (RFC 0010). This RFC defines the async core surface that bridge wraps.
- Snapshot expiry / `history` garbage collection — RFC 0007. No public verb is
  reserved for it here.

## Background

Moraine's core is a plain Rust library any tokio host can embed (README).
Its first and defining consumer is `crates/moraine-duckdb`, a cdylib that is
thin by policy: if logic accumulates there it belongs in the core. That
charter is only honorable if the core surface is drawn so the bridge has
nothing left to do but translate sync↔async and map errors. In particular,
the bridge must not host a conflict-retry loop or assemble writes — those are
core concerns.

SlateDB is async (tokio). The core surface is therefore `async`; the core
spawns no runtime and no threads of its own — the caller drives it, and the
DuckDB bridge owns the sync↔async translation. It does spawn *tasks* onto the
runtime already driving it, which is how a commit's durable write survives its
caller being cancelled (RFC 0010); that needs no runtime of its own.

RFC 0002 establishes the read model this API exposes: a client builds an
in-memory catalog by scanning the `current` subspace at attach; the live catalog
is small by design, name→id resolution happens against that in-memory
snapshot, and the hot path never scans history. This RFC turns that model into
types.

## Design

### Types and layering

Three public types map onto the existing private modules (`catalog`, `store`,
`transaction`). `store` stays entirely private; `lib.rs` re-exports the public types
alongside `Error`/`Result`.

- **`ReadOnlyCatalog`** — the read surface, and the whole of it. Every read
  is defined here once; it carries no mutator at all. Lives in `catalog`.
- **`Catalog`** — the read-write handle. Owns the store handle (private
  field), constructed via `open`, cheap to clone (an `Arc` internally). It
  adds the mutators and reaches the reads through
  `Deref<Target = ReadOnlyCatalog>`, so a writer serves the whole read
  surface without restating a method of it.

  **The mode is in the type.** `Catalog::open_read_only` returns a
  `ReadOnlyCatalog`, so `commit` on a read-only handle is a compile error
  rather than a runtime `Error::Constraint` — a `compile_fail` doctest on
  `ReadOnlyCatalog` pins that, and the tests that used to assert the runtime
  refusal are gone with it. The runtime check survives in exactly one place,
  the extension shim, because a C ABI has one handle type and no types of
  its own to refuse with; that is the boundary where a type argument ends.
- **`CatalogSnapshot`** — an immutable, materialized read view built by
  scanning `current` (or `current` + the relevant `history` ranges, for time travel) per
  RFC 0002. All accessors are in-memory; after construction it never touches
  the store. Lives in `catalog`.
- **`Transaction`** — the mutation handle passed to the commit closure. It `Deref`s to
  `CatalogSnapshot`, so every read accessor is available inside a commit for
  name→id resolution and validation, and adds the mutators. The commit
  machinery (retry, `WriteBatch` assembly, `current`↔`history` bookkeeping) lives in
  `transaction`; `Transaction` is its public face.

Public catalog instants use `Timestamp`, a signed microsecond count from the
Unix epoch. `SnapshotInfo::time`, `ScheduledDeletion::schedule_start`, and
maintenance status pass start times carry that type so pre-epoch values and
the full persisted `i64` range cross the core API unchanged. Storage records
and host ABIs keep the raw microsecond count and convert only at the boundary.

### Front door

```rust
use std::sync::Arc;
use object_store::ObjectStore;

let object_store: Arc<dyn ObjectStore> = /* bucket + credentials */;
let catalog = Catalog::open(object_store, CatalogOptions::default()).await?;
```

`object_store` is the only substrate primitive in the signature — it is the
deployment unit the README already sells ("a deployment is a bucket and
credentials"). `CatalogOptions` surfaces deliberate SlateDB/WAL tuning (e.g.
WAL bucket, flush cadence) through moraine's own type, so no `slatedb::` name
appears publicly and options can be documented and evolved on moraine's terms.
It also carries the store's path within the bucket, defaulting to the bucket
root — the default deployment stays "a bucket and credentials", and a prefix
is opt-in for hosts sharing a bucket.

`Catalog::open_read_only` is the reader's door onto the same store, and takes
the same options. One of them selects *which* kind of reader:
`CatalogOptions::checkpoint` pins the open to an existing checkpoint id
instead of following the latest state, which is the difference between a
reader that writes the manifest and one that writes nothing at all (RFC
0004, Topology). Checkpoints are minted through `Catalog::create_checkpoint`
on a writer and released through `Catalog::delete_checkpoint`, which — like
`Catalog::migrate` — is free-standing because it touches only the manifest
and so runs against a live catalog without fencing it. The id crosses the
API as a `String`, matching how every other identifier of SlateDB's that
moraine hands out is spelled, so no `slatedb::` or `uuid::` name appears
publicly.

### Reads

```rust
let snaphot = catalog.snapshot().await?;                 // current catalog
let past = catalog.snapshot_at(snapshot_id).await?;   // time travel
```

Both return `Arc<CatalogSnapshot>`. `snapshot()` serves the handle's cached
view when it already stands at head (RFC 0009) and scans `current` when it
does not; `snapshot_at(S)` always scans, additionally reading the relevant
`history` ranges and filtering by begin/end per RFC 0002.

The `Arc` is what makes a warm read genuinely cheap. The view is immutable
and often large, so handing back a shared pointer lets the handle and every
caller name the same materialization; returning it by value would copy the
whole catalog on each read and give back most of what the cache saves.
Callers who want an owned copy can still clone through the `Arc`.

Accessors (all in-memory, name→id resolved internally):

| Accessor | Returns |
|---|---|
| `schemas()` | all live schemas |
| `schema_by_name(name)` / `schema_by_id(id)` | one schema |
| `tables_in(schema)` | tables of a schema |
| `table_by_name(schema, name)` / `table_by_id(id)` | one table |
| `views_in(schema)` / `view_by_name` / `view_by_id` | views |
| `columns_of(table)` | ordered columns (tags embedded) |
| `partitioning_of(table)` | partition spec, if any |
| `sorting_of(table)` | sort spec, if any |
| `data_files_of(table)` | live data files (incl. inlined chunks, RFC 0005) |
| `delete_files_of(table)` | live delete files (incl. inlined deletes) |
| `table_stats(table)` / `column_stats(table, column)` | statistics |
| `option(scope, key)` | resolved option value |
| `current_snapshot()` | snapshot id + metadata of this view |

Reads issue no store I/O after the snapshot is built — a `CatalogSnapshot` is a
value, not a cursor.

Inlined rows (RFC 0005) are the one read the snapshot does **not** serve.
`recent_rows(table)`, `recent_rows_at(table, snapshot)` and
`recent_row(table, row_id)` are `Catalog` methods, each one contiguous range
scan of the `inline` subspace — at head, or, for `recent_rows_at`, at the
snapshot named. That id resolves through the rule `snapshot_at` uses, so an
unminted id and an expired one are refused the same way whichever read is
asking. They are row
data, not catalog metadata: an inlined chunk carries the table's actual values
as an Arrow IPC body, so materializing them into every `CatalogSnapshot` would
put unbounded row bytes behind an accessor the whole catalog shares and break
the "the live catalog is small by design" premise the materialized view rests
on. Rows of one chunk share one body and rows of one schema version share one
schema, so each set of bytes is read and returned once however many rows
reference it.

`warm_tables(&[table])` is the one read that returns nothing: it pulls the
`index` and `inline` ranges a lookup on those tables probes into the block
cache (RFC 0009), for a host that knows which tables a query is about to
touch. The same pass runs on its own, in the background, the first time a
handle serves a lookup or inline read for a table.

### Observability

Two read-side counters sit beside the reads, both plain `Copy` structs
marked `#[non_exhaustive]` so fields can be added without a break:

- `cache_status()` (free function): the process-shared caches' capacity,
  occupancy, and evictions. Its `row_summaries: RowSummaryOccupancy`
  splits the auxiliary allowance's file row-id summaries out from the
  Parquet footers they share it with — `range`, `roaring`, and `sorted`
  counts of resident summaries by shape, and their `bytes` together, the
  summary share of `auxiliary_metadata_occupancy_bytes`.
- `ReadOnlyCatalog::object_store_tally()`: physical requests this handle
  has issued since it attached. `main_*` and `wal_*` count what SlateDB
  sent for the catalog store; `data_gets` and `data_bytes` count the byte
  ranges the handle read from the data store — footers, row-id columns,
  delete files, and scoped reads under `locate_row_ids`,
  `warm_row_summaries`, backfill, index build, and commit-time index
  maintenance. A footer or summary served from cache adds nothing.

### Writes: closure-with-retry

```rust
let new_snapshot: SnapshotId = catalog.commit(|tx| {
    let s = tx.create_schema("sales")?;
    let t = tx.create_table(s, "orders", columns)?;
    tx.register_data_file(t, data_file)?;
    Ok(())
}).await?;
```

`commit` reads a fresh `CatalogSnapshot`, constructs a `Tx` over it, runs the
closure to accumulate mutations, assembles exactly one `WriteBatch` (RFC 0002
atomicity invariant), and commits it under the protocol of RFC 0004. On a
transient write-write race it retries the whole cycle — including re-reading
the snapshot and re-running the closure — up to a bounded budget. On a logical
error it aborts immediately.

Because the closure may run more than once, it must be pure and idempotent:
its only effects are the `Transaction` mutators it calls and the value it returns.
This is documented on `commit` and is the single most important contract of
the API. The re-run is not merely permitted, it is load-bearing: RFC 0004's
benign-race retry re-runs the closure against the fresh snapshot precisely
so that logical premises (name uniqueness, entity existence) are
re-validated against the state that won the race — a mechanical re-stage
without the closure would commit duplicate names. Two Rust-shape
consequences follow and are part of the contract, not incidental: the
closure is **`Fn`, not `FnOnce`** (it cannot consume captured values — move
clones in, or stage owned data on the `Transaction`), and it is **synchronous** (no
I/O of its own; name→id resolution and staging run against the in-memory
snapshot, and anything slow — writing Parquet, say — happens *before*
`commit`, per the RFC 0005 data-before-metadata order). Snapshot allocation
is implicit — a successful `commit` produces one new snapshot; there is no
explicit `begin_snapshot` verb.

`commit_group` is the same surface for several closures at once:

```rust
let ids: Vec<SnapshotId> = catalog.commit_group(&[
    &|tx: &mut Transaction| tx.create_schema("sales").map(|_| ()),
    &|tx: &mut Transaction| tx.create_schema("ops").map(|_| ()),
]).await?;
```

Members are `CommitMember`: a trait object, so one call can pass closures
that do different things, and `Sync`, so a grouped commit is as spawnable
as a lone one. Every contract above holds per member, purity included — a
lost race re-runs the whole group. One snapshot id comes back per member,
in member order.

This is the explicit half of RFC 0004's group commit. The implicit half
needs no API, since concurrent `commit` callers are batched without asking;
either way the batching is invisible in what is returned here.

This closure/verb surface is the **embedding API** — one of RFC 0004's two
commit front doors. The DuckDB extension (RFC 0006) does not call it: that
path commits DuckLake-authored row mutations through RFC 0004's staged-row
protocol, which never retries internally. The retry contract described here
is a verb-path property.

### Error taxonomy

`Error` is `#[non_exhaustive]`, one variant per failure domain:

| Variant | Meaning | Retried? |
|---|---|---|
| `CommitConflict` | write-write race, retry budget exhausted | (internal retries preceded it) |
| `NotFound` | operation references a missing entity | no — abort |
| `AlreadyExists` / `Constraint` | logical conflict (duplicate name, constraint violation) | no — abort |
| `Store(#[source])` | underlying object-store / SlateDB I/O failure | no |
| `Corruption` | value decode failure or unknown `sys/format` version (RFC 0002) | no |
| `Unsupported` | a DuckLake feature moraine does not yet implement (e.g. VARIANT inlining, RFC 0005) | no |
| `SnapshotExpired` | a held/requested snapshot fell below the RFC 0007 retention horizon (RFC 0009) — re-resolve from head | no |
| `Interrupted` | operation cancelled by a host interrupt before its point of no return, or a durable write past that point whose outcome went unreported (RFC 0010) — re-resolve head | no |
| `Migration` | store requires, is undergoing, or is newer than a structural format the binary supports (RFC 0015) | no |

The conflict split follows from closure-with-retry: transient races
(another writer advanced `sys/head`) are retried internally and only surface
as `CommitConflict` when the budget is exhausted; logical conflicts
(`AlreadyExists`, `NotFound`) surface immediately because re-running the
closure against fresher state cannot resolve them. Source errors are preserved
via `thiserror` `#[source]`, so the DuckDB bridge maps each variant to a
DuckDB error code without inspecting message text.

### Domain types

The public surface is hand-written domain types, decoupled from the
`prost`-generated messages that `store` uses on disk:

- **Newtype ids** (wrapping the DuckLake-allocated `u64`s of RFC 0002):
  `SchemaId`, `TableId`, `ViewId`, `MacroId`, `MappingId`, `ColumnId`,
  `DataFileId`, `DeleteFileId`, `PartitionId`, `SortId`, `SnapshotId`.
- **Value structs:** `TableInfo`, `ViewInfo`, `MacroInfo`, `MappingInfo`,
  `ColumnDef`, `DataFile`, `DeleteFile`, `PartitionSpec`, `PartitionColumnDef`,
  `SortSpec`, `SortKeyDef`, `ColumnStats`, `TableStats`, `OptionScope`,
  `TagTarget`, `InlineChunk`, `FlushedDataFile`, `RecentRow`.
- **`DataStore`:** the `DATA_PATH` object store every data-file read takes
  (`locate_row_ids`, `warm_row_summaries`, backfill, index build, staged
  transactions), wrapping the `Arc<dyn ObjectStore>` with the identity the
  footer and row-summary caches key on. Built once per store and cloned:
  a durable store is named by its location, so cached footers outlive the
  process; an in-memory store is named at random per `DataStore::new`.

Keeping these separate from the wire types is what lets RFC 0002's protobuf
field evolution stay an internal change instead of a public breaking one.

### Operation enumeration

Every mutator is a method on `Transaction`. Operations read as DuckLake catalog verbs,
not as low-level "put entity version" primitives; the `current`↔`history` version
bookkeeping of RFC 0002 is internal to each. Grounded in DuckLake v1.0
semantics and the entities RFC 0002 maps:

| Group | Operations |
|---|---|
| **Schemas** | `create_schema`, `drop_schema` (no rename verb — DuckLake models no schema rename: `ducklake_schema` is one row per id under a primary key, and its alter path handles tables and views only) |
| **Tables** | `create_table`, `rename_table`, `set_table_schema` (move to another schema), `drop_table` |
| **Views** | `create_view`, `alter_view`, `drop_view` |
| **Macros** (RFC 0019) | `create_macro`, `drop_macro` (no alter verb — DuckLake models macro replacement as drop + create under a fresh `macro_id`; macro names collide with live macros in the schema only, not with tables/views) |
| **Columns** | `add_column`, `rename_column`, `alter_column` (type / default / nullability), `drop_column` |
| **Partitioning** | `set_partitioning`, `clear_partitioning` |
| **Sorting** (RFC 0013) | `set_sorting`, `clear_sorting` |
| **Data files** | `register_data_file` (carries its file column stats), `expire_data_file` |
| **Delete files** | `register_delete_file`, `expire_delete_file` |
| **Statistics** | `update_table_stats`, `update_column_stats` |
| **Tags** | `set_tag`, `remove_tag` (on a schema, table, or view; column tags travel on the column record and have no verb) |
| **Options** | `set_option`, `unset_option` (global / schema / table scopes) |
| **Inlined data** (RFC 0005) | `inline_insert`, `inline_delete`, `flush_inlined_data` |
| **Maintenance** (RFC 0021) | `maintain` — reclaims the entry ranges of indexes no longer live; `store_census` — what each subspace weighs, from the manifest, plus an opt-in live count; `compact_store` — merges a subspace's sorted runs into one, reclaiming superseded versions and tombstones. None is a `Transaction` mutator: all three are `Catalog` verbs, since none mints a snapshot. `store_census` is the one maintenance verb a read-only catalog serves — it reads, and the compactor that executes a merge runs inside the writer |
| **Format migration** (RFC 0015) | `migrate` — rewrites the store to the newest structural format the binary understands. Not a `Transaction` mutator and not even a `Catalog` method: an associated function taking the object store, because the stores it exists to act on are the ones an ordinary attach refuses |

Notes:

- `ducklake_files_scheduled_for_deletion` (RFC 0002 `gcfile`) has no public
  verb, and the `expire_*` operations do **not** write it: expiring a file
  only ends its live version into history — historical snapshots still
  reference the bytes, so scheduling physical deletion belongs to the
  RFC 0007 expiry commit, which writes `gcfile` records when it prunes the
  `history` records below the retention horizon.
- `alter_column` is one verb taking an optional change per attribute, rather
  than three verbs, because DuckLake models a column alteration as a single
  new column version regardless of which attributes changed.
- Sorting and partitioning are twins in shape and differ in two ways the
  verbs must carry. A sort change marks the table altered without bumping
  the schema version — a sort spec constrains writes and never invalidates
  a cross-file compaction, so DuckLake does not bump, and neither does the
  verb path. And a sort key names its column inside a verbatim SQL
  expression rather than by field id, so `set_sorting` has no column to
  resolve and validates none: what a rename or a drop does to a sorted
  column is DuckLake's binder's business, and moraine records what
  committed (RFC 0013). `clear_sorting` is a genuine clear, unlike
  `clear_partitioning`, whose DuckLake counterpart lands a live spec with
  no columns.
- Every verb that puts a column type into the catalog — `create_table`,
  `add_column`, `alter_column` — enforces the RFC 0005 type policy and raises
  `Unsupported` for a type moraine cannot store. The rule is the core's, and
  the staged path enforces the same one at the same boundary, so a `VARIANT`
  column is refused where it enters the catalog rather than at the first
  insert that would need it. The shim keeps its own refusal at the Arrow
  conversion, and it is not redundant: DuckLake builds the inlined data table
  while flushing the `CREATE TABLE`, *before* the `ducklake_column` rows the
  core validates reach a commit, so removing it leaves the user with DuckDB's
  bare "Unsupported Arrow type VARIANT". Measured by deleting it and running
  the e2e suite. This is also why `register_data_file` carries no variant
  statistics: `ducklake_file_variant_stats` describes the shredded paths of
  a `VARIANT` column, and no such column can reach the catalog to have any.
  The field stays in the stored record for row-faithfulness, and arrives on
  the verb surface with `VARIANT` support or not at all.
- `flush_inlined_data` registers the files it drains into, rather than leaving
  them to `register_data_file`. A flushed file is not an ordinary
  registration: its rows keep the ids they were inlined under and its record
  is backdated to the earliest snapshot among them, neither of which
  `register_data_file` can express. Keeping that inside the flush verb is why
  the core needs no general row-id-preserving registration: compaction is the
  only other operation of that shape, and it is DuckLake's own — it reaches
  the store through the staged-row path, never through this surface. A verb
  is added if an embedding consumer ever writes a rewritten file itself
  (additive).
- A registered file carries the partition it falls in — one value per key
  of the table's live spec, in key order, verbatim text. The spec is
  resolved rather than named by the caller: a file is written under the one
  in force, and a registration racing a repartition already conflicts as
  append-versus-alter, so passing an id could only ever name the spec the
  commit is about to be arbitrated against. A partitioned table requires
  its values and an unpartitioned one refuses any, on the same reasoning as
  the index-entry rule — a file silently landing in no partition is a file
  no pruning scan can place.
- A flushed file additionally carries `partial_max`, the newest snapshot
  among its rows, whenever a drain sweeps up rows inlined at several. Both
  bounds are needed and neither is derivable from the other: the record is
  live from the earliest so pre-flush time travel finds it, and a reader at
  a snapshot inside the span filters the file's rows per row against its
  own snapshot column rather than taking all of them. It is validated to
  sit at or above the backdated snapshot and below this commit's, the same
  window the backdating check enforces from the other side.
- One commit may both inline into a table and flush it. The drain reads the
  store as it stood before the commit, so the chunk the commit stages is not
  among the chunks it drains — those rows stay inlined for the next flush.
  What the drain reads is also what bounds the file: a flushed file's rows
  must lie below the row ids the table had allocated when the commit began,
  since the caller wrote its Parquet before calling `commit` and so cannot
  have put a row this commit mints into it. The pairing that does *not*
  compose is tombstoning a row the same commit flushes: the file carries that
  row as live and the drain removes the chunk the tombstone hangs off, so the
  delete would vanish. It is refused, and a delete file against the id
  `flush_inlined_data` returns expresses it instead.
- Column/name mappings (RFC 0018) have **no verb**, and this is settled
  rather than deferred. A mapping exists only to resolve the columns of
  *foreign* Parquet — a file written without DuckLake's field ids — and
  moraine does not expect to serve one. DuckLake creates mappings as a side
  effect of `ducklake_add_data_files`, which a user can still issue through
  the extension; the staged path carries those rows verbatim and always
  will. What is settled is the verb surface: no `create_mapping`, and
  `register_data_file` leaves `mapping_id` unset, because a file this
  surface registers is one the host wrote against the table's own
  columns.
- Snapshot expiry / `history` GC has no verb (deferred, per non-goals).
  RFC 0021's `maintain` is not one: it reclaims moraine's own orphaned
  index entries and nothing else — it computes no retention policy, and
  adds no substrate step, the SlateDB store collecting its own superseded
  objects unprompted. RFC 0021 *does* orchestrate DuckLake's expiry and
  cleanup functions, but from the DuckDB shim, which can issue SQL — never
  from this surface, which cannot.
- This table covers the entities the core models today. The extension
  contract that once gated the full set is settled: RFC 0005 maps every
  DuckLake statement the shim issues onto a store record, and none of them
  needs a verb this table lacks — the extension rides the staged-row path,
  not this surface. What is left is the DuckLake v1.0 spec's remaining
  tables; as e2e reaches them, operations are added here — this RFC is
  updated, not diverged from.

### Testing obligations

Per RFC 0001:

- Integration tests exercise the public API only, against real SlateDB on an
  in-memory `object_store` (the existing `tests/smoke.rs` pattern) — no mocks
  of the store layer.
- `Catalog::open` and `commit` carry doctests that teach by worked example.
- The `crates/moraine/examples/` program on the ROADMAP ("first runnable
  example once the API exists") becomes the crate-root worked example the RFC
  0001 docs rule asks for.

## Alternatives considered

- **Explicit `begin`/`commit` transactions** (`let mut tx =
  catalog.begin().await?; …; tx.commit().await?`): gives the host full
  control of the retry loop, but pushes retry boilerplate to every call site —
  including inside the DuckDB bridge, exactly where "thin by policy" forbids
  it. Rejected: the conflict-retry loop and the atomicity invariant belong in
  the core, and a closure is how the core keeps ownership of them.

- **Build-a-changeset, then `apply`** (host constructs an inspectable data
  value describing all mutations, then `catalog.apply(changeset).await?`):
  maximally decoupled and testable, but pushes read and name→id resolution
  onto the host and reads least like DuckLake DDL/DML. Rejected: the `Transaction`
  closure already gives an inspectable, testable unit while keeping resolution
  in the core where the in-memory snapshot lives.

- **Lazy read handle** (accessors issue store scans on demand, nothing
  materialized): contradicts RFC 0002's "scan `current` at attach" model and
  reintroduces the hot-path history-filtering cost the `current`/`history` split
  exists to avoid. Rejected: `CatalogSnapshot` is materialized because the
  live catalog is small by design.

- **Caller provides a `slatedb::Db`**: exposes the substrate in the public
  API, ties moraine's semver to SlateDB's, and contradicts keeping `store`
  internal. Rejected: `object_store` is the right primitive to expose;
  the KV engine is not.

- **Exposing the `prost` value types directly**: would save the hand-written
  domain layer, but turns every RFC 0002 protobuf field change into a public
  breaking change and leaks the codec into the API. Rejected: the domain
  layer is what preserves RFC 0002's evolution promise.

- **Coarse two-variant error type** (`Conflict` + `Backend(source)` with
  stringly-typed detail): simpler enum, but forces the DuckDB bridge (and any
  host) to parse message text to react. Rejected: one variant per failure
  domain is a small, non-exhaustive enum that maps cleanly to DuckDB error
  codes.

- **Structural-shape-only scope** (define the types and models, leave the
  operation set to fill in later): considered and rejected in favor of the
  full enumeration above, grounding the operation set in DuckLake v1.0
  semantics rather than in the extension contract, which was unresolved when
  this was written. The bet paid: the contract settled (RFC 0005) without
  moving a verb, and the surface was legible throughout.
