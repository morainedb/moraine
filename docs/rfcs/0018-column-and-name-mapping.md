# RFC 0018: Column and name mapping for externally written Parquet

- **Date:** 2026-07-13

## Summary

DuckLake registers externally written Parquet — files without DuckLake's
field ids — through `ducklake_add_data_files(...)`, which records how each
foreign file's physical columns resolve to the table's logical fields in
two catalog tables: `ducklake_column_mapping` (one row per distinct
physical layout) and `ducklake_name_mapping` (that layout's column tree).
moraine models both as **one unversioned `mapping` kind** keyed
`(table_id, mapping_id)`, with the `ducklake_name_mapping` rows embedded in
the value — a third unversioned record class beside stats
(overwrite-in-place) and options (last-write-wins): **immutable
create-only**. The staged write path gains a child-row fold that groups
`ducklake_name_mapping` inserts under their parent `ducklake_column_mapping`
insert by `mapping_id`. There is no verb-path surface: mappings exist only
as a side effect of `ducklake_add_data_files`, so only the staged path
writes them.

## Goals

- **Row-faithful.** Every column of both tables maps to exactly one stored
  field; the served projections reproduce DuckLake's rows verbatim, and
  DuckLake's own SQL (the join, the ordering, the `mapping_id >= cursor`
  filter) does the rest.
- **Honor the upstream read contract** (Background): ids in the file-id
  space, inner-join visibility, the parent-before-child tree invariant.
- **Table locality.** A table's mappings live in its contiguous key range,
  so "everything about table T" stays one scan per subspace and the eventual
  GC delete (`WHERE table_id IN (...)`) is a range operation.

Non-goals:

- **A verb-path API.** DuckLake has no mapping DDL — mappings are a side
  effect of `ducklake_add_data_files` — and moraine does not expect to
  serve foreign Parquet, so the embedding surface will not grow a
  `create_mapping` verb or a way to point a registered file at a mapping.
  This is a settled position, not a deferral: a file the verb surface
  registers is one the host wrote against the table's own columns, which
  needs no mapping to resolve. The staged path serves the one writer that
  does create them, for a user who issues `ducklake_add_data_files` through
  the extension, and that is what the rest of this RFC specifies.
- **Using mappings.** moraine never reads Parquet; resolving a file's
  columns through its mapping is DuckLake's scanner's job. moraine stores
  and serves.
- **Physical deletion.** DuckLake deletes mapping rows only during snapshot
  expiry, by `table_id` for dropped tables and then a referential orphan
  sweep over `ducklake_name_mapping`. moraine honors both. The record's
  delete carries its key columns — `(mapping_id, table_id)` — and no
  `end_snapshot`, mappings being unversioned, which makes it the one
  unversioned kind on the hard-delete path.

  The orphan sweep has nothing left to remove here. The name-mapping rows
  are embedded in the record the first DELETE just reclaimed (RFC 0002), so
  where a SQL catalog needs a second pass to collect rows its foreign key
  never cascaded, this keyspace frees them with their parent. The sweep is
  still *accepted* — DuckLake issues it either way — and lands as a no-op,
  whether it arrives in the same transaction as the parent's delete or
  after the parent is already gone.

## Background: the DuckLake contract

Verified against the tracked DuckLake sources (`target/ducklake-src`);
these are the constraints the design must not violate.

- **One writer.** Only `ducklake_add_data_files` (and the server-side
  commit replay of the same data) writes these tables. Files DuckLake
  writes itself carry field ids and get `data_file.mapping_id = NULL`.
- **`mapping_id` draws from `next_file_id`**, the same snapshot counter as
  `data_file_id` — *not* `next_catalog_id`
  (`ducklake_transaction_state.cpp:528`). The reader depends on it: mappings
  load lazily (not at attach) via
  `SELECT ... FROM ducklake_column_mapping JOIN ducklake_name_mapping
  USING (mapping_id) [WHERE mapping_id >= <cursor>] ORDER BY mapping_id,
  parent_column NULLS FIRST`, where the cursor is the previous load's
  `snapshot.next_file_id`. Ids outside that monotone space would be skipped
  or never loaded.
- **Immutable and append-only.** No UPDATE is ever issued against either
  table. DuckLake deduplicates structurally before inserting (an identical
  layout reuses the existing `mapping_id`), so duplicates never reach the
  catalog. Deletes happen only in snapshot expiry: `ducklake_column_mapping`
  by `table_id` of dropped tables, then `ducklake_name_mapping` rows whose
  `mapping_id` no longer exists.
- **Unversioned.** Neither table has begin/end snapshot columns. Lifecycle
  rides on the `ducklake_data_file` rows that reference the mapping.
- **Row semantics.** `ducklake_column_mapping(mapping_id, table_id, type)`:
  `type` is always the literal `'map_by_name'` (the reader throws on
  anything else). `ducklake_name_mapping(mapping_id, column_id,
  source_name, target_field_id, parent_column, is_partition)`: `column_id`
  is a 0-based ordinal local to the mapping, assigned by preorder DFS over
  the column tree; `parent_column` is NULL for roots, otherwise the parent's
  ordinal — preorder assignment guarantees **parent < child**, which is what
  makes DuckLake's one-pass tree rebuild (roots first, `NULLS FIRST`) sound;
  `target_field_id` is the table's field id; `is_partition = true` marks a
  virtual column whose value comes from the file's hive path, not the
  Parquet body.
- **Inner-join visibility.** A `column_mapping` row with no `name_mapping`
  rows is invisible to the reader; orphan `name_mapping` rows likewise.
  DuckLake never writes either shape.
- **Both tables must always exist** (the lazy read and the expiry sweep
  reference them unconditionally); empty is the normal state for a lake with
  no foreign files.

## Design

### Keyspace and value

One new kind in the `current` subspace (RFC 0002's map gains its row):

| Kind | Key components | DuckLake table(s) |
|---|---|---|
| `mapping` | `table_id, mapping_id` | `ducklake_column_mapping` (+ `ducklake_name_mapping` embedded) |

`mapping` joins the table-scoped kinds (`table_id`-first, typed scan-bound
constructor). The value embeds the child rows verbatim:

```proto
MappingValue { mapping_id, table_id, map_type, repeated NameMapping }
NameMapping  { column_id, source_name, target_field_id,
               optional parent_column, is_partition }
```

The kind is **unversioned and immutable**: written once at creation, never
overwritten, never mirrored to `history` (a data file's historical read
finds its mapping in `current` — the mapping outlives every file version
that references it, until RFC 0007 GC removes both). Snapshot
materialization therefore includes mappings regardless of the time-travel
target, exactly as it does stats.

### Staged write: the child-row fold

DuckLake's commit lands both inserts in one staged batch. Translation runs
a pre-pass that drains `ducklake_name_mapping` insert rows into groups
keyed by `mapping_id`; each `ducklake_column_mapping` insert then consumes
its group into one `MappingValue`. Validated, not trusted — each violation
is a typed constraint error naming the mapping:

- a `column_mapping` insert whose group is empty (invisible to the reader);
- `name_mapping` groups left unconsumed after the pass (orphans);
- duplicate `column_id` within a mapping;
- `parent_column >= column_id` (breaks the reader's one-pass rebuild);
- a `(table_id, mapping_id)` that already exists in the base snapshot or
  the batch.

`column_id` contiguity (0..N-1) is observed upstream but not load-bearing
for the reader, so it is not enforced — the fold must not reject a future
DuckLake that assigns ordinals differently while preserving the parent <
child invariant. `map_type` is stored verbatim, not validated against
`'map_by_name'`, for the same reason.

Updates against both tables are rejected at the shim: the diff stage only
ever creates mapping records, so a changed record is unreachable by
construction. Deletes are accepted, because the RFC 0007 expiry cleanup is
issued as ordinary row deletes: a `ducklake_column_mapping` delete drops
the record outright — unversioned and create-only, so cleanup is a direct
`current` key delete rather than an end transition — and the embedded
`ducklake_name_mapping` rows ride with their parent.

### Reads

Two dump projections flat-map the stored records back into rows:
`ducklake_column_mapping` (one row per record) and `ducklake_name_mapping`
(the embedded rows). The shim's always-empty stand-ins for both tables are
replaced by these providers. Ordering needs no special handling here:
DuckLake sends real SQL (`ORDER BY mapping_id, parent_column NULLS FIRST`)
and DuckDB executes it over the projection — unlike the macro tables
(RFC 0019), nothing relies on served row order.

`ducklake_data_file.mapping_id` is already carried end to end by the staged
path and the file dump; this RFC pins it with tests rather than changing
it. The verb path's `register_data_file` continues to write
`mapping_id = None` (no verb-path mappings exist to reference).

### Test obligations

- **Store:** proptest roundtrip for `MappingValue`; golden key vector for
  the `mapping` kind (current subspace, table-scoped bounds).
- **Staged:** happy-path fold (batch of `column_mapping` +
  `name_mapping` + `data_file` rows lands one record and the file's
  `mapping_id`); one test per constraint above.
- **Dumps:** row-faithful projection of a nested (struct/list) mapping with
  an `is_partition` column.
- **e2e (`ducklake_load.rs`):** write Parquet with plain DuckDB `COPY`
  (no field ids, including one hive-partitioned path), register through
  `ducklake_add_data_files`, `SELECT` back through the mapping, and verify
  the `ducklake_column_mapping`/`ducklake_name_mapping`/`ducklake_data_file`
  projections through a standalone `moraine:` attach; time travel across a
  subsequent write still reads the registered file.

## Alternatives considered

- **A separate `name_mapping` kind** keyed `(mapping_id, column_id)`.
  Rejected: the rows have no independent lifecycle (never updated, only
  swept when the parent goes), which is exactly RFC 0002's embed criterion;
  separate kinds add keys, codecs, and goldens to reassemble a join the
  reader immediately re-flattens.
- **Keying by `mapping_id` alone.** It is globally unique (file-id space),
  but a global key loses table locality, makes the RFC 0007 per-table
  delete a full-subspace scan, and no read path looks up a mapping without
  already knowing its table.
- **Embedding mappings in the table record.** Rejected: mappings are
  referenced per data file and loaded lazily by id cursor upstream; a table
  with many foreign-file layouts would turn every catalog load of that
  table into a mapping load, and table-version transitions would rewrite
  immutable mapping data.
- **Versioning the kind** (begin/end + history mirror). Rejected: the
  upstream tables carry no snapshot columns and the rows never transition;
  inventing a lifecycle would be re-modeling, not row-faithfulness.
- **A `create_mapping` verb now.** Deferred, not refused: the verb API
  gains it if an embedding consumer materializes (additive change); today
  it would be invented API with no caller and an untestable contract.
