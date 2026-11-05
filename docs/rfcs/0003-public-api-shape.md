# RFC 0003: Public API shape of the moraine core

- **Date:** 2026-07-09

## Summary

Defines the public API of the `moraine` core crate: how a host opens a
catalog, reads it, and commits changes to it. This is the third
expensive-to-reverse decision the project requires an RFC for (key layout —
RFC 0002/0005; commit protocol — RFC 0004; public API shape — here). The
surface is four types — `Catalog` (the read-write handle), `ReadOnlyCatalog`
(the read surface a read-only attach gets, which `Catalog` derefs to),
`ReadScope` (a fixed, demand-loaded read view), and `Transaction`
(the mutation handle passed to a commit closure) — over an error taxonomy with one variant per failure domain.
Writes go through a closure-with-retry model so the RFC 0002 single-`WriteBatch`
atomicity invariant and conflict-retry loop live in the core, not duplicated in
every host. SlateDB never appears in a public signature: the substrate is an
implementation detail behind `object_store` plus moraine's own options type.
The operation set is enumerated in full, grounded in DuckLake v1.0 catalog
semantics.

## Goals

- One read API, usable both standalone (`catalog.read_scope()`) and inside a
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

Catalog metadata can be larger than memory. Opening a scope pins a store
revision without decoding the catalog. Point lookups and typed cursors load
metadata on demand; Foyer owns block reuse, and callers own decoded results.

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
- **`ReadScope`** — a fixed store revision plus a current or historical
  snapshot ID. Fallible asynchronous lookups load only their requested records.
  `CatalogCursor<T>` owns a pinned scope and streams typed metadata one record
  at a time. Neither retains a decoded catalog cache.
- **`Transaction`** — the asynchronous mutation handle passed to `commit`.
  It loads validation dependencies on demand, preserves staged reads, and has
  no `Deref` to a materialized snapshot. The commit protocol owns allocation,
  conflict detection, retries, and atomic publication.

`CatalogSnapshot` is an internal operation projection used by translation and
an unstable ABI seam. There is no public whole-catalog materialization entry
point. Unit tests may build complete reference views to check scoped behavior.

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
The WAL bucket is one of those: `CatalogOptions::wal_store` takes a
`WalStore`, which pairs a second object store with the name the catalog
records for it (RFC 0004), because a store and the name a later open is held
to must not be settable apart. `Catalog::move_wal_store` moves it, and is
free-standing for the reason `Catalog::migrate` is — it takes the writer, so
no attach may hold the catalog while it runs.
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

Ordinary reads use a stable read-point handle with fallible asynchronous
lookups and bounded cursors. The handle retains the store revision, not a
catalog-wide decoded map. Unversioned metadata and versioned entities must
observe that same revision. Decoded values belong to the consuming operation;
there is no decoded-object cache.

```rust
let current = catalog.read_scope().await?;
let past = catalog.read_scope_at(snapshot_id).await?;
let schema = current.schema_by_name("sales").await?;
let mut schemas = current.scan_schemas().await?;
while let Some(schema) = schemas.next().await? {
    // Process and release each descriptor.
}
```

Both scope constructors pin a fixed store revision and validate the requested
snapshot against retention. Historical entity reads seek the requested
identity or range and filter lifecycle visibility. Unversioned metadata uses
the same pinned store revision. Scope acquisition must fail if it cannot pin;
it never falls back to a catalog-wide materialization or a following reader.
`current_snapshot()` is synchronous because scope acquisition already read the
header. Other metadata lookups are asynchronous and fallible. Collection
accessors retain their requested result; `scan_*` cursors bound enumeration
memory to a store page and one decoded record.

The old `snapshot()` and `snapshot_at()` methods and the eager `moraine_snapshot`
ABI entry point are removed. Hosts migrate to fixed scopes; there is no eager
compatibility path.

`ffi_support::catalog_read::table_sources_for` resolves an exact schema/table
name and point-reads only the requested current data-file IDs at one read point.
At format 12 it seeks the delete-target directory and point-reads only the
matching current delete records. Legacy formats stream the table's delete
records and retain only the requested targets. Committed file-name mappings are fetched by mapping ID when a file
is read; source projections do not retain the table's mapping directory.

Staged-row translation resolves an operation-owned dependency projection:
point reads for authored entity identities; the affected tables' descriptors,
columns, index definitions and table statistics; referenced file mappings;
and a scan of each affected table's delete-file directory retaining only
records targeting referenced files. These partial projections are never
installed as complete catalog snapshots. Rebase retains the caller's fixed
authored read scope until its dependencies are resolved. Public verb callbacks
load dependencies as operations request them, extending both the original
premise and the staged view without overwriting earlier mutations. Internal
index, option, data-path, and version changes declare their required premises
and fold pending batch writes into that operation-owned projection.

Internal immediate/staged backfills and index verification stream current file
records and resolve delete files per target at the same fixed read point. Their
file-plan concurrency bounds retained descriptors. Explicit backfill `Vec`
results and verification's entry set remain proportional to the requested
result, not an additional catalog inventory.

`locate_row_ids` reads file descriptors in pages of at most 128 records, stopping
a page after its decode allowance reaches 1 MiB (one oversized record can cross
that threshold). It resolves row-ID statistics only for those files and uses
the same read point for inline membership. Retained data summaries store
process-local 128-bit source signatures instead of decoded file records;
`DataFileInfo` implements `Hash` for this validation. The file directory holds
only the last page. Large tables therefore require a streaming metadata pass
on each uncached row lookup; Foyer serves storage blocks, and the data cache
continues to serve immutable file summaries.

Accessors (asynchronous and fallible except `current_snapshot`, names resolved internally):

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

Reads perform store I/O on demand at the scope's fixed revision.

Inlined rows (RFC 0005) are the one read the snapshot does **not** serve.
`recent_rows(table)`, `recent_rows_at(table, snapshot)` and
`recent_row(table, row_id)` are `Catalog` methods, each one contiguous range
scan of the `inline` subspace — at head, or, for `recent_rows_at`, at the
snapshot named. That id resolves through the rule `read_scope_at` uses, so an
unminted id and an expired one are refused the same way whichever read is
asking. They are row
data, not catalog metadata: an inlined chunk carries the table's actual values
as an Arrow IPC body, so materializing them into every `CatalogSnapshot` would
put unbounded row bytes behind an accessor the whole catalog shares and break
the complete-result memory cost a collection accessor carries
on. Rows of one chunk share one body and rows of one schema version share one
schema, so each set of bytes is read and returned once however many rows
reference it.

`publish_tables(&[table])` is the one read that returns nothing: it pulls the
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
  `publish_row_summaries`, backfill, index build, and commit-time index
  maintenance. A footer or summary served from cache adds nothing.

### Writes: closure-with-retry

```rust
let new_snapshot: SnapshotId = catalog.commit(async |tx| {
    let schema = tx.create_schema("sales").await?;
    let table = tx.create_table(schema, "orders", columns).await?;
    tx.register_data_file(table, data_file.clone(), &[]).await?;
    Ok(())
}).await?;
```

`commit` starts with the head and allocation counters. Each read or mutation
loads its own dependencies through the batch's read session. An operation
projection holds original records and a separate staged view, so a later read
sees earlier changes without replacing them when new dependencies arrive.
Name checks, cascades, index upkeep, options, and ID allocation keep their
existing validation. The accumulated delta is published atomically.

Callbacks implement `AsyncFn` and may be retried after a benign head race.
They must not consume captured state or perform external side effects; write
Parquet before entering the callback. Asynchronous catalog methods perform
fallible metadata reads, not independent commits. A callback error aborts its
whole member, and a group error aborts every member. A following commit-log
reader moving during dependency loading produces a conflict rather than
publishing a mixed premise.

`commit_group` accepts heterogeneous callbacks returning boxed `Send` futures:

```rust
let ids = catalog.commit_group(&[
    &|tx: &mut Transaction| Box::pin(async move {
        tx.create_schema("sales").await.map(|_| ())
    }),
    &|tx: &mut Transaction| Box::pin(async move {
        tx.create_schema("ops").await.map(|_| ())
    }),
]).await?;
```

`CommitMember` callbacks are `Sync`. Members run in order, observe the preceding
members' staged writes, and receive distinct snapshot IDs. A benign race
re-runs the entire group, preserving atomicity and validation.

This is the explicit half of RFC 0004's group commit. The implicit half
needs no API, since concurrent `commit` callers are batched without asking;
either way the batching is invisible in what is returned here.

This closure/verb surface is the **embedding API** — one of RFC 0004's two
commit front doors. The DuckDB extension (RFC 0006) does not call it: that
path commits DuckLake-authored row mutations through RFC 0004's staged-row
protocol, which joins the same batch and re-drives a benign lost race by
rebasing the head-derived values DuckLake stamped — for data-only commits;
every other shape surfaces its lost race to DuckLake. The closure re-run
described here is a verb-path property; the bounded budget and the typed
outcomes are shared by both doors.

### Error taxonomy

`Error` is `#[non_exhaustive]`, one variant per failure domain:

| Variant | Meaning | Retried? |
|---|---|---|
| `CommitConflict` | write-write race, retry budget exhausted | (internal retries preceded it) |
| `NotFound` | operation references a missing entity | no — abort |
| `AlreadyExists` / `Constraint` | logical conflict (duplicate name, constraint violation) | no — abort |
| `Store(#[source])` | underlying object-store / SlateDB I/O failure | no |
| `Corruption` | value decode failure or unknown `sys/format` version (RFC 0002) | no |
| `Unsupported` | a DuckLake feature moraine does not yet implement | no |
| `SnapshotExpired` | a held/requested snapshot fell below the RFC 0007 retention horizon (RFC 0009) — re-resolve from head | no |
| `Interrupted` | operation cancelled or interrupted before it could report a result | no |
| `CommitOutcomeUnknown` | a submitted commit was not acknowledged; retain external files and reconcile the operation before resubmitting (RFC 0010) | no |
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
  (`locate_row_ids`, `publish_row_summaries`, backfill, index build, staged
  transactions), wrapping the `Arc<dyn ObjectStore>` with the identity the
  footer and row-summary caches key on. Built once per store and cloned:
  `DataStore::new` assigns an isolated random identity, regardless of the
  store's `Display`. `DataStore::with_cache_identity` accepts a caller-supplied
  `CacheIdentity` to share entries across handles and restarts.
- **`CacheIdentity`:** an opaque object-namespace identity. `Default` generates
  an isolated identity; `new(namespace)` deterministically hashes an explicit
  namespace; `local(&LocalFileSystem)` names the resolved filesystem root.
  Equal identities assert that equal object paths name the same immutable
  contents. Custom stores must include backend, endpoint, bucket/root,
  wrapper prefixes, and any other routing context in their namespace.
  Credentials and human-readable display names do not establish equivalence.
  Replacing a store's contents requires a new namespace or clearing its cache.
  `CatalogOptions::cache_identity` applies the same contract to catalog block
  caches; the catalog path is included separately. `None` isolates each open.

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
- Every DuckLake column type is storable, `VARIANT` included, so no verb
  refuses one. An equality index is the one place a type is still turned
  away: `create_index` raises `Constraint` for a 128-bit integer, which
  Parquet narrows to a lossy double, and for a `VARIANT`, which holds no
  single scalar an index key could encode. `register_data_file` still
  carries no variant statistics — `ducklake_file_variant_stats` describes
  the shredded paths a Parquet writer found, which the registering caller
  does not have; they reach the catalog through the staged-row path that
  DuckLake's own writes take.
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
