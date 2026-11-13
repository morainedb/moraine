# RFC 0010: Maintenance

## Summary

DuckLake drives reclamation. `ducklake_expire_snapshots` computes the dead
set and issues a row cascade, `ducklake_cleanup_old_files` deletes bytes,
and the compaction functions rewrite Parquet. moraine translates the row
shapes those functions issue, mints no snapshot for a maintenance-only
commit, and removes orphaned index ranges, file statistics, inlined rows of
dropped tables, and row-summary sidecars.
Set `META_MAINTENANCE_INTERVAL` at attach to schedule maintenance.
The scheduler runs the enabled steps in a fixed order.

## Expiry and cleanup translation

`ducklake_expire_snapshots` selects the expirable snapshots and, in one metadata
transaction, deletes their records and every entity version no surviving
snapshot can see. The dead-row rule is DuckLake's: a version is dead when no
surviving snapshot id falls in `[begin_snapshot, end_snapshot)`. The selected
set is arbitrary, since `versions` names interior snapshots directly.
`ducklake_cleanup_old_files` reads the deletion schedule, removes the files
through DuckDB's filesystem, then deletes the schedule rows.
`ducklake_delete_orphaned_files` lists the data prefix and deletes unreferenced
files without touching the catalog. The cascade is specified by
[DuckLake's metadata manager](https://github.com/duckdb/ducklake/blob/main/src/metadata_manager/ducklake_metadata_manager.cpp).

DuckLake determines which records and files to delete and validates the
retention parameters. Moraine receives the cascade through the staged-row path
of [the extension surface](0006-extension-surface.md) and translates into one
atomic batch by these rules.

| Staged row | Translation |
|---|---|
| `ducklake_files_scheduled_for_deletion` insert | Put `current/gcfile` keyed by the row's `data_file_id`. A delete by `data_file_id` removes the key. No moraine-allocated id exists. |
| `ducklake_snapshot` delete | Delete the `snapshot` key. Deleting the head snapshot is refused with `Error::Constraint`. The paired `ducklake_snapshot_changes` delete is a validated no-op, since the two tables are one record. |
| Versioned-entity delete | Names one version by entity id and `end_snapshot`. `NULL` deletes the `current` key, a value deletes that `history` key. This is a hard prune and nothing moves to history. |
| Inlined-version delete (`RowOperation::InlineRowsDelete`) | Groups the `(row_id, begin_snapshot)` selections per table, removes wholly selected chunks, and rewrites partially selected chunks as contiguous surviving row-id runs in the existing chunk encodings. A missing requested version fails the transaction. Prefix drains staged in the same transaction combine with the selections first. |
| Embedded-row delete (`ducklake_column_tag`, `ducklake_partition_column`, `ducklake_sort_expression`, `ducklake_file_partition_value`, `ducklake_name_mapping`) | Validated no-op. The row rides its parent record and leaves with it in the same batch. A delete whose parent is still live and not dying in the batch is refused. |
| `ducklake_column_mapping` delete | Hard delete keyed `(mapping_id, table_id)`, the one unversioned kind on this path. |
| `ducklake_schema_versions` delete | Deletes the `schema_version` key for `(table_id, begin_snapshot)`. DuckLake issues it only in dead-table cleanup, so schema-version rows outlive the snapshots that wrote them. |
| A cascade with no `ducklake_snapshot` insert | Lands head-preserving: one batch, no new snapshot record, and `sys/head` keeps its snapshot id while its batch count advances. |

A commit that inserts or updates entities without a `ducklake_snapshot`
insert is refused with `Error::Constraint`. DuckLake mints a snapshot for
every real catalog change, so the only snapshot-free commits are
maintenance.

### Read-your-writes

The cascade's `NOT EXISTS` subqueries run after the same transaction staged its
snapshot deletes, and its orphan-cascade deletes read parents deleted moments
earlier. Projections served through an open staged transaction therefore overlay
its staged rows on the committed records in stage order
(`crates/moraine/src/transaction/staged/overlay.rs`). An insert appends, a hard
delete drops the version it names, `UPDATE ... SET end_snapshot` ends the live
version, `UPDATE ... SET begin_snapshot` rebases it, and an unversioned kind
overwrites or removes by key. Validation runs at commit; the overlay includes
staged rows even if commit-time translation will reject them.

Every kind is overlaid. One materialization per DuckDB transaction serves
both the metadata scan and the staged-write sink, so a rowid the scan emits
resolves against the list the sink reads. Once a staged transaction is
open, every read of a writable kind goes to the overlaid dump and nothing
caches over it.

### Reader safety

The retention window must exceed the longest read or attach, and the
cleanup grace period (`older_than` on `cleanup_old_files`) must exceed the
longest scan. Logical expiry deletes no data-file bytes. A reader whose
snapshot has been expired receives `Error::SnapshotExpired` and re-resolves
from head. moraine keeps no registry of live reader snapshots: readers are
cross-process and uncoordinated, so a process-local registry could not
derive a window.

Every physical deletion passes through the schedule. Inline flush
hard-deletes its consumed inline records itself. Merge schedules its
superseded files directly. Rewrite ends its sources into history, where a
later expiry reclaims and schedules them.

## Compaction translation

DuckLake plans and executes compaction. `ducklake_merge_adjacent_files` and
`ducklake_rewrite_data_files` read and write the Parquet, mint an ordinary
snapshot, and author the row changes. moraine translates two shapes the
ordinary write path does not produce.

**Merge.** The merged file carries per-row `row_id` and `snapshot_id`
columns, and its `ducklake_data_file` row is inserted with a backdated
`begin_snapshot`, the first source's. The source rows are hard-deleted
across `ducklake_data_file`, `ducklake_file_column_stats`,
`ducklake_delete_file`, `ducklake_file_partition_value`, and
`ducklake_file_variant_stats`, current and history rows alike. Each source
path is inserted into `ducklake_files_scheduled_for_deletion`. Time travel
into the merged file filters rows by `snapshot_id`.

**Rewrite.** Survivors are written to a new file with their original row
ids. The source data file and its consumed delete file are ended with `SET
end_snapshot`. The replacement's `begin_snapshot` is then rebased with
`UPDATE ducklake_data_file SET begin_snapshot`
(`RowOperation::UpdateSetBegin`), which only `ducklake_data_file` accepts
and only for its live version. Nothing is scheduled.

Compaction never allocates row ids. DuckLake stages no change to
`next_row_id`, so the table statistics carry whatever was there. Index
entries name rows, and a compaction changes no row id or value, so a
compaction-only change set stages no index work and no entry is touched.

### Positional-delete validation

Before publishing staged delete files or inlined file deletions, Moraine checks
that each target data file is live in the commit's catalog view or is registered
by the same batch. A target retired by a concurrent merge or rewrite causes a
commit conflict, including when DuckLake opens its metadata transaction after
the replacement committed. The client must retry against the new file layout;
a successful commit must never leave a deletion pointing only at retired input.

### Row-id validation

Before a compaction lands, the commit reads the row ids of the files it
registers and of the files it hard-deletes or ends, and compares them
(`crates/moraine/src/transaction/staged/row_id_invariants.rs`). A file
registered below the minted snapshot or rebased onto it is an output. A
merge's outputs must carry the same multiset of row ids as its sources. A
rewrite's outputs may carry fewer, and none the sources lacked. A
difference is refused with `Error::Corruption` and the sources stay live.
A commit with no data store to read from skips the check.

The bundled DuckLake carries
`patches/ducklake/0007-fix-keep-the-row-ids-a-merged-file-carries.patch`, under
which a merge always writes the row-id column. The commit also validates row ids
before accepting the replacement files.

### Conflict grain

`merge_adjacent:<table>` and `rewrite_delete:<table>` change kinds parse
into per-table compaction markers. A compaction conflicts with a drop, a
delete-from, or another compaction of the same table, and with nothing
else, so appends never abort it. Classification stays at table grain, as
[the commit protocol](0003-commit-protocol.md) defines.

## The scheduled pass

One scheduler thread per read-write attach starts at `ATTACH` when
`MAINTENANCE_INTERVAL` is set (`crates/moraine-duckdb/cpp/maintenance.cpp`).
Each pass opens a `Connection` of its own, resolves the lake name by matching
attached DuckLake databases on store path, and runs the steps below in
order. When no DuckLake database matches, the name falls back to stripping
`__ducklake_metadata_`. The sequence is shim work because only the shim
reaches both DuckLake's SQL and the core.

| # | Step | Enabled by | Runs |
|---|---|---|---|
| 1 | `expire_snapshots` | `MAINTENANCE_EXPIRE_SNAPSHOTS[_OLDER_THAN\|_VERSIONS]` | `CALL ducklake_expire_snapshots('lake', ...)`, batched |
| 2 | `flush_inlined_data` | `MAINTENANCE_FLUSH_INLINED_DATA` | `CALL ducklake_flush_inlined_data('lake', ...)`, one call per table |
| 3 | `merge_adjacent_files` | `MAINTENANCE_MERGE_ADJACENT_FILES[_MAX_COMPACTED_FILES]` | `CALL ducklake_merge_adjacent_files('lake', ...)` |
| 4 | `rewrite_data_files` | `MAINTENANCE_REWRITE_DATA_FILES[_DELETE_THRESHOLD\|_MAX_COMPACTED_FILES]` | `CALL ducklake_rewrite_data_files('lake', ...)` |
| 5 | `cleanup_old_files` | `MAINTENANCE_CLEANUP_OLD_FILES[_OLDER_THAN\|_CLEANUP_ALL]` | `CALL ducklake_cleanup_old_files('lake', ...)` |
| 6 | `delete_orphaned_files` | `MAINTENANCE_DELETE_ORPHANED_FILES[_OLDER_THAN\|_CLEANUP_ALL]` | `CALL ducklake_delete_orphaned_files('lake', ...)` |
| 7 | `sweep_indexes` | `MAINTENANCE_SWEEP_INDEXES` (default true) | `moraine_maintain`: `repair_deferred_indexes` finishes indexes left in the `maintaining` state, then `Catalog::maintain` reclaims dead index ranges |
| 8 | `sweep_file_stats` | same switch | Reported from the same `maintain` call: `file_column_stats_reclaimed` |
| 9 | `sweep_inline_tables` | same switch | Reported from the same call: `inline_tables_swept`, `inline_records_reclaimed` |
| 10 | `sweep_row_summaries` | always | `ReadOnlyCatalog::sweep_published_summaries`: deletes row-summary sidecars whose data file is gone from `DATA_PATH`. An attach without a data path sweeps nothing. |
| 11 | `compact_store` | `MAINTENANCE_COMPACT_STORE[_SUBSPACE\|_TIMEOUT]` (default false) | `Catalog::compact_store` |

Expiry runs first because it is the only step that shrinks the catalog, and
every later step reads a smaller snapshot projection. Flush precedes merge
so flushed files are merge input. Merge and rewrite precede cleanup because
merge schedules its superseded bytes and cleanup drains that schedule in
the same pass. Cleanup precedes orphan detection. The sweeps follow, on
what the earlier steps left behind. Steps 7 to 9 are one core call reported
as three rows, because what orphans each is different: `drop_index` orphans
entries, while expiry's dead-table cleanup orphans statistics and inlined
rows. Step 10 follows cleanup because DuckLake's orphan scan matches
`.parquet` only and cannot remove a summary sidecar. The store merge runs
last so the tombstones the pass created are merged away. Every step is
idempotent.

Steps 1 to 6 mutate the lake and have no default. Step 11 rewrites the
store's bytes and defaults off for cost. A read-only attach never schedules.

## Attach options

The shim parses options named `MAINTENANCE_*` (`ParseMaintenanceOptions`).
A DuckLake attach forwards its `META_`-prefixed options to the metadata
catalog with the prefix removed, so an operator writes
`META_MAINTENANCE_INTERVAL` at the lake attach. Step options derive from
DuckLake's names by one rule: `MAINTENANCE_<function minus ducklake_>`
enables a step with DuckLake's defaults, and
`MAINTENANCE_<that>_<DuckLake parameter>` passes a parameter through as a
SQL literal. The step name is matched longest-first and the remainder is
the parameter. Supplying a parameter enables its step.

| Option | Default | Meaning |
|---|---|---|
| `MAINTENANCE_INTERVAL` | unset, no thread | Seconds or an `INTERVAL` between passes |
| `MAINTENANCE_BATCH_SIZE` | 65,536 | Deletes per sweep commit |
| `MAINTENANCE_SWEEP_INDEXES` | `true` | Runs steps 7 to 9 |
| `MAINTENANCE_TABLE_BATCHES` | `false` | Merge and rewrite issue one call per table, each committing independently |
| `MAINTENANCE_PROGRESS` | `false` | Writes step and table boundaries to stderr with resident memory |
| `MAINTENANCE_COMPACT_STORE` | `false` | Runs step 11 |
| `MAINTENANCE_COMPACT_STORE_SUBSPACE` | every subspace | Narrows the merge to one subspace name |
| `MAINTENANCE_COMPACT_STORE_TIMEOUT` | 0, submit and return | How long a pass waits for each merge |
| `MAINTENANCE_EXPIRE_SNAPSHOTS` | off | `OLDER_THAN`, `VERSIONS` |
| `MAINTENANCE_FLUSH_INLINED_DATA` | off | no parameters |
| `MAINTENANCE_MERGE_ADJACENT_FILES` | off | `MAX_COMPACTED_FILES` |
| `MAINTENANCE_REWRITE_DATA_FILES` | off | `DELETE_THRESHOLD`, `MAX_COMPACTED_FILES` |
| `MAINTENANCE_CLEANUP_OLD_FILES` | off | `OLDER_THAN`, `CLEANUP_ALL` |
| `MAINTENANCE_DELETE_ORPHANED_FILES` | off | `OLDER_THAN`, `CLEANUP_ALL` |

An `older_than` given as an `INTERVAL` renders as `now()::TIMESTAMP -
<interval>`, which DuckLake evaluates on every pass. Attach options are
evaluated once, so a timestamp written as `now()` would freeze at attach. A
timestamp literal passes through verbatim. DuckLake refuses a list-valued
`META_` option, so `versions` is spelled as a string such as `'[1,2]'`.

Validation happens at bind. An unknown option, an unknown parameter for a
known step, an unknown subspace name, a non-positive interval, batch size,
timeout, or `max_compacted_files`, and a step set `false` while one of its
parameters is supplied each raise a `BinderException`. The subspace name is
checked against the core's vocabulary through `moraine_subspace_is_known`.

## Scheduler invariants

- **Single flight.** A tick that finds a pass running skips the tick.
  The manual trigger takes the same lock and waits out a pass in
  flight.
- **Stops before the database closes.** `DETACH` stops and joins the thread
  before the handle is released. Without `DETACH`, the close of the last
  host `ClientContext` records a context state whose destructor stops and
  joins after DuckDB's connection-manager lock is released. Connections a
  pass opens are tagged and excluded from the host count. Once every host
  context has closed, scheduled passes stay stopped until a new attach.
- **Status window.** `moraine_maintenance_status` serves the last 16 passes
  (`RETAINED_PASSES`), newest first, from one unversioned
  `sys/maintenance_status` value. The write after a pass is head-preserving
  and appears in no time travel. A failed status write is logged and leaves
  the pass's effects standing.
- **Bounded retry.** Flush, merge, rewrite, and expiry rerun the whole call
  on a fresh connection after a recognized transaction conflict, at most 3
  attempts, waiting 100 ms then 200 ms. Cleanup and orphan deletion have
  external effects and are never retried. An unknown commit outcome is never
  retried.
- **Expiry in batches.** The step first calls `ducklake_expire_snapshots`
  with `dry_run => true` to fix the eligible ids, then expires at most 64
  per call, oldest first. A commit refused for exceeding
  `MAX_STAGED_COST_PER_COMMIT` halves the batch and retries. A single
  snapshot above the cap fails the step and leaves `work_pending`.
- **Table-scoped deferral.** Flush always runs one call per table, and
  merge and rewrite do when `MAINTENANCE_TABLE_BATCHES` is set or an earlier
  step deferred a table. A table that exhausts its retries is deferred and
  the others continue. Catalog-wide steps after it are skipped. The next
  pass tries again.
- **Failure handling.** Any other failed DuckLake step abandons the rest of
  the DuckLake sequence, since cleanup drains what merge scheduled.
  Abandoned steps are reported `skipped`, naming the step that failed. The
  sweeps and the store merge still run. The next tick starts from the top.
- **Explicit transactions refused.** The trigger blocks while its own
  second connection writes, so it is refused unless the caller's transaction
  is autocommit.

## Verbs and core API

| Function | Attach | Rows |
|---|---|---|
| `moraine_maintenance('lake')` | read-write, autocommit | Runs one pass immediately: `step`, `status`, `detail` |
| `moraine_maintenance_status('lake')` | any | Retained passes: `started_at`, `trigger`, `step`, `status`, `detail`, `work_pending` |
| `moraine_compact_store('lake', subspace :=, timeout :=, require_completed :=)` | read-write | One row per subspace: `subspace`, `outcome`, `detail`, `bytes_before`, `bytes_after` |
| `moraine_store_census('lake', live :=)` | any | One row per subspace: `manifest_id`, `subspace`, `bytes`, `filter_bytes`, `index_bytes`, `stats_bytes`, `l0_ssts`, `sorted_runs`, `sorted_run_ssts`, `live_keys`, `live_key_bytes`, `live_value_bytes`, `scheduled_files`, `store_total_bytes`, `store_wal_bytes`, `store_manifest_bytes`, `store_sst_bytes` |

Each accepts the lake name or the metadata catalog name through
`ResolveMoraineCatalog`. `moraine_maintenance` takes no parameters:
configuration lives at attach. `require_completed` needs a `timeout` and
turns a pending or failed merge into a query error. The census's live
columns are `NULL` when `live` was not requested. `bytes_after` is `NULL`
unless the merge committed.

The core verbs, in `crates/moraine/src/catalog/handle/maintenance.rs` and
`crates/moraine/src/catalog/census.rs`:

```rust
pub struct MaintenanceRequest {
    pub sweep_orphaned_index_entries: bool,     // default true
    pub sweep_orphaned_file_column_stats: bool, // default true
    pub sweep_orphaned_inline_tables: bool,     // default true
    pub batch_size: usize,                      // default 65_536, nonzero
    pub pace: Duration,                         // default 1 s
}

pub struct MaintenanceReport {
    pub indexes_swept: u64,
    pub index_entries_reclaimed: u64,
    pub file_column_stats_reclaimed: u64,
    pub inline_tables_swept: u64,
    pub inline_records_reclaimed: u64,
}

pub struct MaintenanceStatusPass {
    pub started_at: Timestamp,
    pub trigger: String,
    pub steps: Vec<MaintenanceStatusStep>, // step, status, detail
    pub head_snapshot_id: u64,
    pub head_batch_seq: u64,
    pub work_pending: bool,
}

pub enum CompactionTarget { WholeStore, Subspace(SubspaceName) }

pub struct CompactStoreRequest {
    pub target: CompactionTarget,
    pub wait: Option<Duration>,
    pub require_completed: bool,
}

pub struct CompactStoreReport { pub merges: Vec<SubspaceMerge> }

pub struct SubspaceMerge {
    pub subspace: SubspaceName,
    pub outcome: MergeOutcome, // Completed | Failed(String) | Pending | Skipped(&'static str)
    pub bytes_before: u64,
    pub bytes_after: Option<u64>,
}

pub struct CensusRequest { pub count_live_entries: bool }

pub struct StoreCensus {
    pub manifest_id: u64,
    pub subspaces: Vec<SubspaceCensus>,
    pub objects: Option<StoreObjects>,
}

pub struct SubspaceCensus {
    pub subspace: SubspaceName,
    pub bytes: u64,
    pub filter_bytes: u64,
    pub index_bytes: u64,
    pub stats_bytes: u64,
    pub l0_ssts: u32,
    pub sorted_runs: u32,
    pub sorted_run_ssts: u32,
    pub live: Option<LiveCount>, // keys, key_bytes, value_bytes, scheduled_files
}

impl Catalog {
    pub async fn maintain(&self, request: MaintenanceRequest) -> Result<MaintenanceReport>;
    pub async fn record_maintenance_pass(&self, pass: MaintenanceStatusPass) -> Result<()>;
    pub async fn reclaim_index_entries(&self, index: IndexId, limit: usize) -> Result<usize>;
    pub async fn compact_store(&self, request: CompactStoreRequest) -> Result<CompactStoreReport>;
}

impl ReadOnlyCatalog {
    pub async fn maintenance_status(&self) -> Result<Vec<MaintenanceStatusPass>>;
    pub async fn store_census(&self, request: CensusRequest) -> Result<StoreCensus>;
}
```

`SubspaceName` names every subspace of
[the storage format](0001-storage-format.md) plus `Unknown(Vec<u8>)` for a
segment prefix this build does not recognize. `StoreObjects` totals the object
store by kind: write-ahead log, manifest, SST, and other. It is `None` when the
listing is refused, which read-only credentials without `ListBucket` produce.
The result structs are `#[non_exhaustive]`. `maintain`, `compact_store`, and
`record_maintenance_pass` refuse a read-only catalog with `Error::Constraint`.

## Orphan sweep and store merge

### The index sweep

An `index_id` is allocated from the global `next_catalog_id`, so ids are
monotonic and never reused, and a range not live at a snapshot never
becomes live again. The sweep therefore needs no lock and no quiet period.

Discovery is a skip-scan. Entry keys place `index_id` immediately after the kind
discriminant, so for each `IndexKind` (`Unique`, `Multi`) the sweep calls
`first_index_id_from` to seek the lowest id at or after a cursor. A live id is
skipped by seeking past its whole range. A dead id is reclaimed by
`reclaim_dead_range`, which opens one bulk scan of the range and drains it into
head-preserving commits of `batch_size` deletes<!-- site:omit -->, waiting
`pace` between batches on a commit log<!-- /site:omit -->. Ids at or above the
snapshot's `next_catalog_id` are left alone, since a later commit may have
allocated them. Cost is one seek per distinct index id present.

The dead set derives from what the `index` subspace contains, checked
against the live definitions. History is never consulted: expiry may prune
a dropped definition's history record before any sweep, after which a
history-derived set would lose the id and leak the range.

The companion sweeps use the same batch shape. A `FileColumnStats` record is
reclaimed when its data file is absent from both `current` and `history`,
because statistics carry no snapshot and a time-traveling read resolves through
the one record. The `inline/*` records of a table id recorded in neither
`current` nor `history` are reclaimed table by table. Each batch is a
head-preserving commit on the writer<!-- site:omit -->, or a slot on
[the commit log](0011-commit-log-seam.md), where `pace` bounds
contention<!-- /site:omit -->.

### The store merge

`Catalog::compact_store` takes a census, then submits SlateDB's own
full-tree plan for each targeted subspace: every sorted run is a source and
the tree's lowest run is the destination, which is the shape that lets
superseded versions and tombstones drop. L0 is excluded so flushes continue.
A tree with no sorted runs is reported `Skipped("no sorted runs to merge")`.
A tree already being merged is adopted and its outcome reported as the
tree's. With `wait` set, the call polls each submission to a terminal
status and re-measures to fill `bytes_after`. A merge that outlives the wait
is reported `Pending` and keeps running. Submission needs no writer, but
execution belongs to whichever compactor serves the store, so a read-only
catalog refuses.

### The census

The default census reads the manifest and lists the object store: two
object reads plus one listing, independent of store size. Every known
subspace is reported, with zeros where the manifest carries no segment, so
two censuses compare row by row. The `count_live_entries` leg scans every
subspace under one read session, counting keys and encoded bytes and
separating `current/gcfile` schedule rows from entity records. It costs a
full read of the store. Both legs read and neither writes, so a read-only
attach may run it.

### The `DATA_PATH` overlap guard

`ducklake_delete_orphaned_files` deletes everything under `DATA_PATH` the
catalog does not reference, and the catalog's own SSTs, manifests, and
write-ahead log would qualify if they sat there. Attach refuses with
`Constraint` when the store path and the data path are on the same object
store and one contains the other (`refuse_overlapping_data_path`,
`moraine::store_paths_overlap`). Containment is compared by path component,
so siblings that share leading text attach normally. For a path the attach
names, the check runs before the catalog records anything, so a refused
attach leaves no value for the next attach to inherit. The path compared is
`META_DATA_PATH` when given, otherwise the value recorded for the lake,
checked again when a root DuckLake recorded is adopted after attach.

## Tests

- A staged expiry cascade removes the named snapshot and dead history keys,
  puts the scheduled files in `current/gcfile`, mints no snapshot, and
  leaves `sys/head`'s snapshot id unchanged with its batch count advanced.
- A dump through a staged transaction that deleted a snapshot omits it,
  while a dump outside the transaction still serves it.
- A merge whose output renumbers a sparse source is refused with
  `Error::Corruption` and keeps its sources. One carrying its sources' ids
  lands with `next_row_id` unchanged.
- The sweep reclaims a dropped index's range, spares live indexes
  interleaved by id, and a second pass reports zero.
- A failed DuckLake step reports the later DuckLake steps `skipped` naming
  it, and the sweeps still run.
- Attaching with `DATA_PATH` nested in the store path is refused, and
  sibling prefixes attach.

## Alternatives

- **A core-side scheduler.** The core is a Rust library beneath DuckDB with
  no path to SQL, so it could not issue `ducklake_expire_snapshots`. The
  shim sequences and schedules, and the core keeps the sweep and the merge.
- **A per-process scheduler.** One thread per process would have to hold
  every attached lake's handle and lake name and outlive any of them. One
  thread per read-write attach ties the pass to the handle it mutates and
  stops with it.
