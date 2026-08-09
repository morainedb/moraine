# RFC 0007: Snapshot expiry and garbage collection

- **Date:** 2026-07-09 (revised 2026-07-13 against DuckLake's implementation)

## Summary

DuckLake catalog history accumulates without bound: ended entity versions
pile up in `history`, snapshot records in `snapshot`, and superseded
data/delete files linger in object storage after no snapshot references
them. This RFC defines how that state is reclaimed when moraine is the
catalog. The load-bearing fact, source-verified against DuckLake
(`DuckLakeMetadataManager::DeleteSnapshots`, `ducklake_cleanup_files.cpp`):
**reclamation is DuckLake-driven, not moraine-driven.**
`ducklake_expire_snapshots` computes the dead set and issues a cascade of
interleaved SELECTs and DELETE/INSERT statements against the metadata
catalog inside one transaction; `ducklake_cleanup_old_files` and
`ducklake_delete_orphaned_files` physically delete bytes through DuckDB's
own filesystem. moraine's whole job is faithful **translation** (staged
DELETEs against snapshot, history, and schedule records), faithful
**projection** (a real `ducklake_files_scheduled_for_deletion` table), and
**read-your-writes** (the cascade's SELECTs observe its own uncommitted
DELETEs). moraine invents no retention policy, computes no dead set, and
deletes no data bytes.

## Goals

- Loading the current catalog stays proportional to *live* state (RFC
  0002's load-bearing guarantee). Expiry keeps `history` / `snapshot` from
  growing with total history rather than live catalog size.
- **Faithful to DuckLake.** moraine translates the exact row operations
  `ducklake_expire_snapshots` / `ducklake_cleanup_old_files` /
  `ducklake_delete_orphaned_files` issue; it invents no stricter or looser
  policy (consistent with RFC 0004: implement DuckLake's model, don't
  impose one).
- **Reclamation is atomic.** A maintenance transaction lands as one
  SlateDB `WriteBatch` (RFC 0002 atomicity invariant) — a crash leaves the
  whole cleanup or none.
- **Physical deletion is decoupled from logical expiry** by the schedule
  (`ducklake_files_scheduled_for_deletion`), so a reader holding a
  slightly stale view never dereferences bytes that were just reclaimed.
  The split is DuckLake's own design; moraine serves and mutates the
  schedule, DuckDB deletes the bytes.

Non-goals:

- **Expiry policy / scheduling** — *when* to expire, the retention window
  (`older_than` / `versions`), and the cleanup grace period are DuckLake
  parameters; moraine neither computes nor validates them.
- **Compaction / data-file rewriting** — RFC 0008. Expiry removes *dead*
  state; compaction rewrites *live* state. The two meet at the schedule:
  merge-superseded files are scheduled by compaction directly,
  rewrite-superseded files become dead history this RFC's cascade later
  reclaims.
- **Multi-writer coordination** — the RFC 0004 topology stands; expiry and
  cleanup run inside the single writer.
- **A moraine-native maintenance surface** — no public verb (RFC 0003).
  The staged-row path is the only mutation surface, as for every
  DuckLake-authored change.

## Background

RFC 0002 established the relevant shape:

- `history` (append-only) holds ended entity versions; its keys end with
  the version's `end_snapshot`. RFC 0002 delegates its garbage collection
  here.
- `snapshot` is one immutable record per snapshot; `sys/head` holds the
  latest committed `snapshot_id`.
- `current/gcfile`, keyed by `data_file_id`, maps
  `ducklake_files_scheduled_for_deletion` — a live bookkeeping list, not a
  time-versioned entity (no begin/end).
- `file` / `delfile` records reference object-store paths in their values.

RFC 0005's flush hard-deletes inlined chunks outright (DuckLake's
delete-at-flush semantics), so the `inline` subspace leaves nothing for
this RFC to reclaim.

RFC 0006 fixes the surface this RFC rides: DuckLake speaks generic SQL
against moraine's row-faithful `ducklake_*` projections, and its writes
arrive as staged row operations translated into one atomic batch.

## Design

### What DuckLake actually issues

**`ducklake_expire_snapshots(older_than / versions)`** selects the
expirable snapshots `E` (never the most recent) and runs, in one metadata
transaction (`DeleteSnapshots`):

`E` is an arbitrary set of snapshot ids, not a prefix of history:
`versions` names interior snapshots directly, leaving later ones live.
Nothing in the translation below is tail-specific — the dead-row rule asks
whether *some* surviving snapshot sees a version, never whether the
survivors form a suffix.

1. `DELETE FROM ducklake_snapshot WHERE snapshot_id IN (E)`; same for
   `ducklake_snapshot_changes`.
2. `SELECT table_id FROM ducklake_table WHERE end_snapshot IS NOT NULL AND
   NOT EXISTS (SELECT … FROM ducklake_snapshot WHERE snapshot_id >=
   begin_snapshot AND snapshot_id < end_snapshot) …` — the **dead-row
   rule**: a version no surviving snapshot can see. This SELECT must
   observe step 1's uncommitted deletes.
3. The same dead-row rule over `ducklake_data_file` (plus a dropped-table
   filter) → `DELETE FROM ducklake_data_file / ducklake_file_column_stats
   / ducklake_file_variant_stats / ducklake_file_partition_value WHERE
   data_file_id IN (…)` and `INSERT INTO
   ducklake_files_scheduled_for_deletion VALUES (data_file_id, path,
   path_is_relative, NOW())` per file.
4. The same over `ducklake_delete_file` → row deletes + schedule inserts.
5. For fully-dead tables: `DELETE … WHERE table_id IN (…)` across
   `ducklake_table`, both stats tables, partition/sort tables,
   `ducklake_column`, `ducklake_column_tag`, `ducklake_schema_versions`,
   `ducklake_inlined_data_tables`, `ducklake_column_mapping`, plus `DROP
   TABLE IF EXISTS ducklake_inlined_data_<t>_<v>`.
6. The dead-row rule over `ducklake_schema` / `ducklake_view` /
   `ducklake_tag` / `ducklake_macro`, then orphan-cascade deletes on
   `ducklake_macro_impl` / `ducklake_macro_parameters` /
   `ducklake_name_mapping` (NOT EXISTS against their parents — observing
   step 6's own deletes).

**No new snapshot row is inserted anywhere in the cascade** — expiry does
not advance head.

**`ducklake_cleanup_old_files(older_than / cleanup_all)`** reads the
schedule, calls `fs.RemoveFiles(paths)` — DuckDB's filesystem, not
moraine — then `DELETE FROM ducklake_files_scheduled_for_deletion WHERE
data_file_id IN (…)`. Bytes first, then the record: a crash between the
two leaves a schedule row whose file is already gone, and the next cleanup
re-issues a no-op delete and converges. Also mints no snapshot.

**`ducklake_delete_orphaned_files(older_than / cleanup_all)`** LISTs the
data prefix through DuckDB's filesystem, diffs against the catalog's
referenced set (read through moraine's projections), and deletes
never-referenced files older than the threshold. It mutates no catalog
record; moraine's involvement is serving correct projections.

### moraine's translation obligations

The cascade arrives on the staged-row path (RFC 0006) as row operations
moraine must translate into one atomic batch:

- **Schedule rows.** `ducklake_files_scheduled_for_deletion` insert →
  put `current/gcfile` keyed by the row's `data_file_id` (its identity in
  DuckLake's own schema — inserts carry it, cleanup deletes by it; no
  moraine-allocated id and no counter). Delete by `data_file_id` → delete
  the key.
- **Snapshot records.** `ducklake_snapshot` delete by `snapshot_id` →
  delete the `snapshot` key. Deleting the head snapshot is refused
  (DuckLake's own filter already excludes `MAX(snapshot_id)`; moraine
  enforces it as a constraint). The paired `ducklake_snapshot_changes`
  delete is a validated no-op — the two tables are one merged record.
- **Versioned-entity rows.** A DELETE against a versioned kind names the
  exact row by `(entity id …, begin_snapshot, end_snapshot)`:
  `end_snapshot IS NULL` deletes the `current` key, otherwise the
  `history` key. This is a hard prune — nothing moves to history; the
  record ceases to exist.
- **Embedded rows.** Deletes against tables whose rows embed in a parent
  record (`ducklake_column_tag`, `ducklake_partition_column`,
  `ducklake_sort_expression`, `ducklake_file_partition_value`,
  `ducklake_name_mapping`) ride their parent's deletion in the same cascade
  and translate as validated no-ops. Step 6's orphan sweep over
  `ducklake_name_mapping` is one of these: the rows it looks for were freed
  with the record step 5 deleted, so the sweep is accepted and lands as a
  no-op whether it shares that transaction or follows it.
- **The mapping record itself** takes a real hard delete in step 5, keyed
  `(mapping_id, table_id)` and carrying no `end_snapshot` — the one
  unversioned kind on that path.
- **Schema-version rows.** `ducklake_schema_versions` rows are **not**
  expired with the snapshots that wrote them. They live in their own
  subspace (RFC 0002) and are deleted only where DuckLake's own catalogs
  delete them: step 5's dead-table cleanup, by `table_id`. Expiring them
  with their snapshot would leave every data file older than the
  retention horizon unable to resolve the schema version it was written
  under, which aborts DuckLake's compaction planner (RFC 0008) at bind
  for the whole table. A store that lost its rows that way is repaired on
  read: the projection floors each affected table at its oldest data
  file, carrying the oldest schema version still known for it.
- **No snapshot minted.** A staged transaction whose operations are all
  maintenance operations commits without a `ducklake_snapshot` insert:
  one `WriteBatch`, no new snapshot record, no head update. Atomicity is
  unchanged; a racing writer is caught by the store transaction's
  write-write conflict detection and surfaces the same `conflict`-substring
  error every staged commit uses. A mixed set (entity mutations but no
  snapshot row) stays a constraint error — DuckLake always mints a
  snapshot for real catalog changes.

### Read-your-writes projections

Steps 2, 3, and 6 of the cascade compute dead sets with `NOT EXISTS`
subqueries over `ducklake_snapshot` *after* the same transaction staged
its snapshot deletes, and the orphan-cascade deletes read parents deleted
moments earlier. Projections that serve only committed state would make
the cascade see expired snapshots as still present and silently
under-schedule — expiry would delete snapshot rows but never reclaim
files.

The extension surface therefore serves projections **through the active
staged transaction** when one exists: the dump overlays the transaction's
accumulated operations on the committed state, so a statement reads what
its transaction already wrote. Outside a write transaction, dumps serve
committed state as before. This is an RFC 0006 obligation.

The overlay is **not** commit-time translation run early, and that is what
makes it cheap. A staged row is stored untranslated — DuckLake's own cells,
carrying the snapshot ids DuckLake itself authored — so what a transaction
will see is fully determined the moment it stages the row. The overlay
therefore decodes each staged row with the same decoders translation uses
and applies it to the scanned records in stage order: an insert appends, a
hard delete drops exactly the version it names, an `UPDATE ... SET
end_snapshot` ends the live version, an `UPDATE ... SET begin_snapshot`
rebases it, and the unversioned kinds overwrite or remove by key. It
validates nothing and diffs nothing; a row translation would refuse still
appears here and fails at commit, where the refusal belongs.

One overlay serves every kind, so a kind is added by naming its key, its
lifecycle accessors, and its row decoder — never by hand-writing a
projection. Committed records are scanned once per transaction and shared
across kinds, since the transaction is snapshot-isolated and a metadata
population asks for one kind after another.

Four shapes cover the whole catalog:

- **Versioned records** — the entity kinds, keyed by their `EntityKey` and
  moved by the lifecycle rules above.
- **Unversioned records** — the statistics kinds, the deletion schedule, and
  the mappings: overwritten or removed by key, no lifecycle at all.
- **Embedded rows** — a partition spec's columns, a file's partition values,
  a macro's implementations and their parameters, a mapping's name rows, a
  column's tags. These ride their parent's record, so the projection is its
  parents' rows plus the rows this transaction staged for parents it is
  inserting. A staged child always names a parent the same batch inserts —
  translation refuses one that does not — and an embedded row is only ever
  removed alongside the parent that carries it, so the parent's own overlay
  is the delete.
- **Derived projections** — `ducklake_schema_versions`, assembled from the
  `schema_version` records plus the snapshots that still fold the same rows
  in plus the data files the floor rows repair, and `ducklake_metadata`,
  whose fixed rows are store facts and whose stored half is an option
  overlay. Each is built from its overlaid inputs by the very function the
  committed dump uses, so the two cannot diverge.

**Every kind is tx-aware, and that is not merely thoroughness.** Partial
coverage is *worse* than none: a cascade joining an overlaid table against a
committed-state one sees a torn view — a data file this transaction deleted,
still carrying partition-value rows that name it. Uniformity is what makes
the view a view.

**The scan and the write path must overlay identically.** A rowid a
metadata scan emits is the row's index into whatever list that scan
materialized, and the staged-write sink resolves the index back against a
list of its own. Making a kind tx-aware for the scan alone is therefore
not a partial improvement but a corruption: an `UPDATE`/`DELETE ... WHERE
ctid`-shaped statement would resolve an index into the overlaid list
against the committed one, and name a different row or none.

Identical *content* is not enough, because two materializations taken at
two committed heads can differ in content — which is why the two are one
materialization rather than two that agree. **A table is materialized once
per DuckDB transaction and shared** (RFC 0009's read-side companion to
this): the scan takes the list, the sink takes the same list, and the
index cannot mean two things. The sink pins it *before* opening the staged
transaction, because opening one is what moves the transaction from
reading the pinned committed dump to reading through the staged
transaction's own read point — and the scan that emitted the rowids bound
in whichever of those held.

That sharing is confined to the reader side. Once a transaction has opened
a staged transaction, every read of a writable kind goes to the staged
dump and nothing caches over it: DuckLake's commit loop re-reads exactly
that surface between attempts, and a retry served the state its first
attempt saw would re-check its conflict matrix against a premise that
already lost.

### Reader safety

The safety contract is unchanged from DuckLake's own, and is documented
for operators in the "Operating a lake" guide: **the retention window must
exceed the maximum expected read/attach duration**, and the cleanup grace period
(`cleanup_old_files`' `older_than`) must exceed the maximum reader/scan
duration. Logical expiry deletes no bytes; a reader holding a view of an
expired snapshot keeps scanning bytes that survive until a cleanup whose
grace period has passed. A reader that held a view longer than the window
may find its snapshot expired and must re-resolve from head
(`SnapshotExpired`, RFC 0003/0009).

moraine exposes no registry of live reader snapshots. Readers are
cross-process and uncoordinated (RFC 0004), so an extension-local registry
would be incomplete and could not safely derive a retention window. Operators
set the window from the workload's externally observed transaction and scan
durations; a process-local diagnostic would not change that contract.

### One reclamation path for flush and compaction

Inline flush (RFC 0005) hard-deletes its consumed inline records itself.
Compaction (RFC 0008) schedules merge-superseded files directly and ends
rewrite-superseded rows into history, whence this RFC's cascade reclaims
them once no surviving snapshot references them. Every path to physical
deletion goes through `ducklake_files_scheduled_for_deletion` and
DuckDB's cleanup functions.

### Test obligations

Per RFC 0001, core tests run against real SlateDB on in-memory
`object_store`; the live path is pinned by `cargo xtask e2e`:

- **Maintenance commits preserve head.** A staged expiry lands with no new
  snapshot record and an unchanged `sys/head`.
- **Expiry prunes.** After a staged cascade mirroring DuckLake's output:
  the named snapshot records are gone; dead history keys are gone; the
  scheduled files appear in `current/gcfile` with their paths; the current
  view at head is unchanged.
- **Cleanup forgets.** A schedule-row delete removes the `gcfile` key.
- **Head-snapshot deletion is refused** with a constraint error.
- **Time travel below the prune no longer resolves; above it, resolves
  identically to pre-expiry.**
- **Read-your-writes.** A dump through a staged transaction that deleted a
  snapshot omits it; a dump outside the transaction still serves it.
- **Live e2e.** `ducklake_expire_snapshots` then `ducklake_cleanup_old_files`
  against a real DuckLake: expired snapshots unlistable, time travel to
  them errors, current results unchanged, scheduled Parquet survives until
  cleanup and is deleted by it, the schedule drains, and
  `ducklake_delete_orphaned_files` removes a planted stray while every
  catalogued file survives.
- **Racing verb commits stay sound.** A verb-path retry whose base
  predates a concurrent expiry treats a missing intervening snapshot
  record as a conflict-and-refresh, never as corruption.

## Alternatives considered

- **A moraine-side expiry engine** (the prior revision of this RFC:
  moraine computes a retention horizon, prunes history by an
  `end_snapshot <= H` rule, allocates deletion ids, advances head, and
  issues object-store DELETEs itself). Rejected on source-verification:
  DuckLake already drives all of it through the metadata catalog, and a
  parallel moraine engine would be a second policy implementation to keep
  faithful forever. The v0.1 target is DuckLake parity through DuckLake's
  own code paths; a native surface can be designed later if a non-DuckLake
  consumer appears.
- **Expiry commits that mint a snapshot** (every head-visible mutation is
  a snapshot). Rejected: DuckLake's cascade inserts no snapshot row, and
  minting one moraine-side would desynchronize `ducklake_snapshot` from
  what DuckLake wrote — the projections would serve a row DuckLake never
  authored. Head-preserving maintenance commits keep row-faithfulness;
  options-only commits set the precedent.
- **Keying the schedule by a moraine-allocated `deletion_id`** (the prior
  revision). Rejected: the schedule's identity in DuckLake's schema is
  `data_file_id` (inserts carry it first; cleanup deletes
  `WHERE data_file_id IN (…)`), a file is scheduled at most once (its
  catalog rows are removed in the transaction that schedules it), and a
  moraine counter would need somewhere durable to advance — maintenance
  commits mint no snapshot record to carry it.
- **Reference-counting files instead of scan-based scheduling.** Rejected:
  a per-file count is mutable shared state requiring read-modify-write on
  every commit that adds or ends a reference — contradicting RFC 0002's
  append-only writes and inflating RFC 0004's conflict sets. DuckLake's
  dead-row rule identifies unreferenced files without maintaining any
  count.
- **Tombstoning `snapshot` records instead of deleting them.** Rejected:
  the goal is to reclaim space; a tombstone still occupies the range a
  time-travel loader scans. `snapshot` immutability is relaxed *only* for
  expiry, and deletion is the whole point.
- **Committed-state-only projections with a re-ordered cascade.** Rejected:
  the statement order is DuckLake's, not moraine's to re-order, and its
  correctness depends on transactional read-your-writes — the property
  every SQL-backed metadata catalog provides. Serving it at the
  projection layer is the faithful move; anything else silently
  under-reclaims.
