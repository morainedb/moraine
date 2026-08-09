# RFC 0008: Maintenance operations — compaction and delete-file consolidation

- **Date:** 2026-07-09 (revised 2026-07-13 against DuckLake's implementation)

## Summary

Frequent commits and inline flushes (RFC 0005) leave a table with many small
Parquet data files; merge-on-read deletes leave data files shadowed by
delete files. Both slow scans. DuckLake's answer is **compaction** —
`ducklake_merge_adjacent_files` combines small adjacent files,
`ducklake_rewrite_data_files` rewrites a file with its deletions applied —
and, source-verified (`DuckLakeCompactor`,
`DuckLakeMetadataManager::WriteMergeAdjacent` / `WriteDeleteRewrites`),
**DuckLake does all of it itself**: it reads and rewrites the Parquet,
mints an ordinary snapshot, and authors the catalog row changes through
the metadata connection. moraine's job is translating two row-operation
shapes the ordinary write path does not produce — hard deletes of
superseded file rows (merge) and a `SET begin_snapshot` rebase of the
replacement file (rewrite) — plus serving the deletion schedule the
superseded bytes flow into (RFC 0007). The load-bearing invariant is
unchanged: compaction **never allocates row ids** — outputs carry their
input rows' ids, `next_row_id` is untouched, and row lineage holds.

## Goals

- **Row-id lineage preserved exactly.** Rewritten rows keep their row ids
  (DuckLake preserves them across compaction for UPDATE and time-travel
  row identity), so the per-table `next_row_id` in `tstat` is read-only
  for the whole operation.
- **Compaction is an ordinary snapshot-minting commit.** DuckLake writes
  the new Parquet first, then one metadata transaction that lands as one
  moraine `WriteBatch` (RFC 0002 atomicity): the new file row, the
  superseded rows' fates, the schedule inserts, the snapshot record, the
  head advance.
- **Conflict semantics faithful to DuckLake.** Compaction's
  `snapshot_changes` carry `merge_adjacent:<t>` / `rewrite_delete:<t>`;
  moraine's conflict classifier arbitrates them per DuckLake's own matrix
  (`DuckLakeTransactionState::CheckForConflicts`): conflicting with drops,
  delete-froms, and other compactions of the same table — and with
  nothing else, so appends never abort a compaction.
- **Time-travel safe**, by DuckLake's own construction (below): a snapshot
  older than the compaction still resolves the same rows.

Non-goals:

- **Physical deletion of superseded files** — RFC 0007's schedule and
  DuckDB's cleanup functions. Compaction schedules (merge) or dead-ends
  (rewrite) the old rows; it deletes no bytes.
- **Auto-compaction policy** (which files, when, target size) — DuckLake
  parameters; moraine computes nothing.
- **Inline flush** — RFC 0005, itself a compaction-shaped op that *does*
  allocate row ids (it materializes new inserts). Kept strictly separate:
  reusing flush's id allocation here would renumber rows and break
  lineage.
- **Key layout** — RFC 0002.

## Source-verified mechanics

**Merge (`merge_adjacent_files`)** — one snapshot-minting metadata
transaction per batch:

1. DuckLake writes the merged Parquet, carrying per-row `row_id` **and
   `snapshot_id`** columns (`DuckLakeCompactor` sets `write_snapshot_id`
   for merges): time travel *within* the merged file is by row filtering,
   not by file selection.
2. The new `ducklake_data_file` row is inserted through the ordinary
   path with **backdated** `begin_snapshot` — the first source file's
   `begin_snapshot` — so every retained snapshot resolves the merged file.
3. The source rows are **hard-deleted**: `DELETE FROM ducklake_data_file /
   ducklake_file_column_stats / ducklake_delete_file /
   ducklake_file_partition_value / ducklake_file_variant_stats WHERE
   data_file_id IN (…)` — current and history rows alike, no history
   mirror. Old snapshots resolve the merged file thereafter; the original
   rows cease to exist. A partitioned source's `file_partition_value`
   rows are embedded in its `file` record (RFC 0013), so one batch
   hard-deletes a parent **and** its embedded children, in no guaranteed
   order. Whether an embedded row's parent survives is therefore a
   question about the batch, not about state accumulated so far: a hard
   delete leaves the working state alone by design, so the parent still
   reads as live there.
4. Each source path is inserted into
   `ducklake_files_scheduled_for_deletion` — merge schedules its
   superseded bytes directly, without waiting for snapshot expiry.
5. A source carrying live delete files is not plainly merged; its deletes
   are materialized into the output (the rewrite shape below) — a delete
   file must never outlive its data file.
6. Merge groups by partition and never crosses a boundary. Four files
   across two partition values merge to two — one per value, as two
   independent merges — so a merged file always carries exactly one
   partition value and the governing spec stays satisfied by construction.
   The eligibility rule is DuckLake's, applied before moraine sees the
   batch; moraine records the merge it is handed and enforces nothing.

**Rewrite (`rewrite_data_files`)** — also snapshot-minting:

1. DuckLake writes the survivor rows (original row ids) to a new file,
   inserted through the ordinary path.
2. `UPDATE ducklake_data_file SET end_snapshot = <rewrite snapshot> WHERE
   data_file_id = <source>` and the same for the consumed
   `ducklake_delete_file` row — the standard lifecycle convention; the
   old rows move to history and stay resolvable for time travel.
3. `UPDATE ducklake_data_file SET begin_snapshot = <rewrite snapshot>
   WHERE data_file_id = <new file>` — the one statement outside the
   ordinary write vocabulary: the replacement file's visibility window is
   rebased after its insert, in the same transaction.
4. Nothing is scheduled here: the ended rows become dead history that
   RFC 0007's expiry cascade later reclaims (and schedules) once no
   surviving snapshot references them.

## moraine's translation obligations

Both shapes ride the staged-row path (RFC 0006) inside an ordinary
snapshot-minting commit:

- **Hard deletes of file rows** (merge step 3): a DELETE against a
  versioned kind names the exact row by `(entity id …, end_snapshot)`;
  `NULL` deletes the `current` key, a value deletes that `history` key.
  Shared with RFC 0007's cascade translation — one mechanism, two
  callers.
- **Schedule inserts** (merge step 4): `current/gcfile` puts keyed by the
  scheduled file's id (RFC 0007).
- **`SET begin_snapshot`** (rewrite step 3): rebases the current data-file
  record's `begin_snapshot` in place. Legal only against a live
  `ducklake_data_file` row; DuckLake only ever issues it against the file
  the same transaction just inserted.
- **Backdated inserts** (merge step 2) already translate: the staged path
  carries `begin_snapshot` verbatim (inline flush backdates the same
  way).
- **`next_row_id` untouched:** compaction stages no `ducklake_table_stats`
  change that advances it; the translation carries whatever DuckLake
  authors, and DuckLake authors no allocation.
- **Equality-index entries untouched:** entries name rows, not files, and
  compaction changes neither a row's id nor its values. A commit whose
  change set is compaction alone stages no index work and does not read
  the files it registers (RFC 0016).

### Conflict classification

Already in place: `merge_adjacent` / `rewrite_delete` change kinds parse
into per-table compaction markers, and the conflict matrix makes
compaction × {delete-from, drop, compaction} of one table a true
conflict while appends and disjoint commits stay benign. A compaction
that loses a race surfaces the `conflict`-substring error DuckLake's own
retry loop scans for.

Conflict classification remains at table grain. DuckLake's change records
name the table, and no measured workload shows same-table compactions losing
enough races to justify carrying and comparing file sets in the commit
protocol. File-set-grain classification would be a replacement design backed
by such a measurement, not latent behavior this protocol promises.

### Test obligations

Core, against real SlateDB on in-memory `object_store`:

- **Merge-shaped commit.** New file inserted backdated; source rows (and
  their stats) hard-deleted including history rows; sources scheduled;
  `next_row_id` unchanged; head advanced by exactly one; the changes
  parse as a compaction of the table.
- **Rewrite-shaped commit.** Source file and delete file end into
  history; the replacement's `begin_snapshot` rebases in place; nothing
  scheduled; `next_row_id` unchanged.
- **`SET begin_snapshot` misuse is rejected** (a non-current target is a
  shape error).

Live, via `cargo xtask e2e`:

- **`merge_adjacent_files`**: several small files merge to fewer; rows and
  row ids identical before and after; time travel to a pre-merge snapshot
  returns the pre-merge answer; source paths scheduled and reclaimed by
  `cleanup_old_files`; an UPDATE after the merge still hits the right row.
- **`rewrite_data_files`**: after a DELETE, the rewrite leaves no live
  delete file; survivors keep their row ids; time travel to the
  pre-rewrite snapshot still shows the deleted rows.

## Alternatives considered

- **A moraine-side compaction engine** (the prior revision: moraine plans
  the rewrite, carries ids through, and stages an end-to-history commit
  itself). Rejected on source-verification, for RFC 0007's reason:
  DuckLake already does the planning, the Parquet I/O, and the row
  authoring; moraine re-implementing it would be a second copy of
  DuckLake's semantics to keep faithful forever.
- **Translating merge's source-row deletes as end-to-history
  transitions** ("time-travel safe because old files survive in
  history"). Rejected — it contradicts what DuckLake writes. Merge
  hard-deletes the source rows and keeps time travel correct by
  backdating the merged file and filtering rows by the per-row
  `snapshot_id` column; mirroring rows into history that DuckLake deleted
  would desynchronize the projections from the catalog DuckLake believes
  it wrote.
- **Allocating fresh row ids for compacted rows** (reuse the RFC 0005
  flush id path). Rejected: breaks DuckLake row lineage — UPDATE and
  time-travel row identity depend on stable ids — and bumps
  `next_row_id`, turning a no-op into row-id churn.
- **Deleting superseded bytes inside the compaction commit.** Rejected:
  couples object-store deletes to the catalog commit and destroys the
  grace the schedule provides; merge schedules, cleanup deletes
  (RFC 0007), exactly DuckLake's own split.
