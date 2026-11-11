# RFC 0004: Data inlining

## Summary

DuckLake inlining keeps small inserts and small deletes in the catalog
instead of writing a Parquet file per tiny commit. Moraine stores them in
the `inline` subspace as Arrow-encoded chunks, append-only tombstones, and
inlined file deletions, with a chunk directory for row lookups. A flush
writes the rows to Parquet, registers the file backdated, and hard-deletes
the consumed records in the same commit. The DuckDB shim routes DuckLake's
dynamically named inlined tables into this keyspace.

## Key layout

Every key below lives under the `inline` subspace of
[the storage format](0001-storage-format.md). Keys are defined by
`InlineKey` in `crates/moraine/src/store/key.rs`.

| Kind | Key components | Value |
|---|---|---|
| `schema` | `table_id, schema_version` | `InlineSchemaValue`: an Arrow IPC schema-only stream, or a `same_as_version` reference to another version of the same table with identical bytes |
| `insert` | `table_id, schema_version, begin_snapshot, chunk_seq` | `InlineChunkValue`: an Arrow IPC record-batch body over the user columns, plus `row_id_start` and `row_count` |
| `row_tombstone` | `table_id, row_id, end_snapshot` | `InlineInlineDeleteValue` repeating `end_snapshot`. One immutable deletion event per row and snapshot |
| `file_delete` | `table_id, data_file_id, row_id` | `InlineFileDeleteValue` holding `begin_snapshot`. An inlined delete against a Parquet row |
| `file_delete_table` | `table_id` | Empty. Marks that `ducklake_inlined_delete_<table_id>` exists |
| `schema_dropped` | `table_id, schema_version` | Empty. Marks that `ducklake_inlined_data_<t>_<v>` has been deregistered by a flush |
| `chunk_locator` | `table_id, row_id_end, schema_version, begin_snapshot, chunk_seq` | `InlineChunkRangeValue`: `row_id_start` and the chunk identity. One per live insert chunk, `row_id_end` inclusive |
| `chunk_range` | `table_id, row_id_end` | Superseded locator keyed by range end alone. Nothing writes it. The directory repair sweeps what an older store left |
| `inline_delete` | `table_id, row_id` | Superseded row-level tombstone. Read and removed only by the format migration |

`chunk_locator` is the directory both paths use. The write path emits one
per staged chunk through `inline_chunk_range_write`, which despite its name
encodes an `InlineKey::ChunkLocator`. The read path walks `chunk_locator`
keys through `find_inline_chunk_locators_for_rows`. `chunk_range` keys are
read only by `scan_legacy_inline_chunk_ranges`, for the flush repair to
delete.

A locator is keyed by its range end and then by the chunk's own identity.
An `UPDATE` re-inlines a row whose id another live chunk's range still ends
on, so two live chunks can share an end. The end leads the key so a scan
still seeks by row id.

Ranges may overlap. A point lookup collects every chunk whose range holds
the target and does not stop at the first match. Coverage is judged by
chunk identity, never by the set of range ends. The walk is bounded by the
widest chunk the table is known to hold: a chunk holding row `t` ends at or
before `t + W`. `W` is memoized per table in the projection cache
(`inline_chunk_width`), raised when a chunk is staged and again by any walk
that verifies the directory, and never lowered. An unknown `W` bounds
nothing and the walk runs to the end of the directory.

Each inline row version ends at the earliest tombstone snapshot strictly
greater than its `begin_snapshot`. An update's deletion ends the old version
while its same-snapshot insertion stays live. Later updates append events.
Readers resolve events across schema versions, so a row that moved through
Parquet between two inline versions keeps its gap.

## Value format

A column is inlinable when DuckDB's Arrow encoding carries it. Scalars,
`BLOB`, `UUID`, `DECIMAL`, and nested `LIST`, `STRUCT`, and `MAP` inline.
`VARIANT` inlines through DuckDB's own Arrow extension type. `GEOMETRY`
inlines when `spatial` is loaded, since that extension registers the Arrow
type. Dictionary-encoded columns are refused by the encoder: a record-batch
body carries no dictionary messages.

Each `insert` value carries the record-batch body only: the batch message
and buffers, with no schema message. Decode supplies the schema from the
version's `schema` record through arrow-rs `read_record_batch`. The schema
is stored once per `(table_id, schema_version)`, in the same batch as the
first chunk for that version. It is stored, never derived from the
catalog's column metadata, so a chunk decodes identically however the
DuckLake-type to Arrow-type mapping changes later.

Buffer compression stays off. SlateDB's block compression reclaims
cross-chunk redundancy at rest.

The shim and the Rust bridge split encoding along the Arrow C Data
Interface. C++ exports a `DataChunk`'s user columns with DuckDB's
`ArrowConverter` and `crates/moraine-duckdb/src/arrow_ipc.rs` serializes
them. Decode reverses it through `ArrowTableFunction::ArrowToDuckDB`.

## Write path

An insert below the row limit becomes one `insert` chunk per commit and
table, with row ids allocated from the table's row-id counter the way a
Parquet registration allocates them. `chunk_seq` disambiguates several
chunks for one `(table, schema_version, snapshot)`. The same batch writes
the chunk's `chunk_locator`. Chunks are immutable after commit.

A delete against an inlined row writes a `row_tombstone`. A delete against
a Parquet row that is small enough to inline writes a `file_delete` and
the table's `file_delete_table` marker. The marker is written idempotently
by every path that stages a deletion into the table or removes one from
it, so a store written before the marker heals on its first flush.

Expiry prunes exact `(schema_version, row_id, begin_snapshot)` identities
through `RowOperation::InlineRowsDelete`, rewriting a partial chunk into
surviving dense runs without changing their versions. No flush is required.
A pruned data file takes the `file_delete` records targeting it with it.

## Read path

Live inlined rows of table `T` at snapshot `S` are the rows of chunks with
`begin_snapshot <= S` whose resolved end is absent or greater than `S`.
`InlineScanKind` in `crates/moraine/src/catalog/inline.rs` names the four
predicates DuckLake issues: `Table`, `Insertions`, `Deletions`, and
`ForFlush`, the last including tombstoned rows. The tombstone set for a
table is scanned once per read and held in memory.

A scan selects its rows once under one read session and serves them in
windows. Each window point-reads only the chunk bodies its rows reference
and releases them before the next window. A scan that projects no user
column reads no body (`InlineBodies::Skip`).

The directory serves a read once it is known complete, meaning it names
every live chunk. Completeness is judged by comparing the directory against
a full chunk walk and remembered per table, only when the store format locks
out writers that predate the directory (`FORMAT_WITH_INLINE_CHUNK_DIRECTORY`).
A flush that finds the directory incomplete heals the gap onto its own
batch: locators for uncovered chunks and deletions for locators naming no
chunk. The read path never writes, so it judges without healing, and only
on an isolated session. A directory short of complete costs the full chunk
walk, which keeps each chunk's row span and drops its body as it passes.

Each chunk decodes against its own version's `schema` record. DuckLake reads
each `ducklake_inlined_data_<t>_<v>` separately and the shim serves it only
the chunks of version `v`.

## Flush

Flush reproduces `ducklake_flush_inlined_data` as one catalog commit. The
Parquet writes precede the batch.

1. Write every inlined row version through the flush snapshot, live and
   tombstoned, to one data file carrying per-row row-id and snapshot
   columns. Write a delete file naming the tombstoned rows' positions in
   that file.
2. Register the data file as a `FlushedDataFile`: the record is backdated
   to the earliest `begin_snapshot` among its rows, keeps the inlined row
   ids, and adds only its bytes to the table statistics. Register the delete
   file against it in the same batch.
3. Hard-delete the consumed `insert` chunks, their `chunk_locator`
   entries, and every `row_tombstone` no surviving version needs.
   Deregister the superseded schema version as described below.
4. Verify row ids: the multiset of row ids in the data files the commit
   registers for a table must equal the multiset drained from its chunks
   (`verify_flushed_row_ids`). A mismatch is refused as corruption, unless
   the head has moved past the snapshot the flush read its rows at: a
   version that landed after the read is then a race the flush lost, and
   the mismatch is reported as `CommitConflict` for DuckLake to re-drive.
   Files the commit registers at its own snapshot are left out of the
   comparison. A `(table, schema_version)` drain named twice by DuckLake
   counts once.

Pre-flush time travel is served from the flushed Parquet by its per-row
snapshot column. The catalog retains nothing, since retained rows would be
double-counted against the backdated file.

The rows reach DuckLake's writer column-wise. A chunk body decodes once
through DuckDB's Arrow importer into a `DataChunk`, and the shim's scan
emits output by copying contiguous runs of that chunk across with
`VectorOperations::Copy`, one vector at a time. The three metadata columns
are written into flat vectors. No cell passes through `duckdb::Value`.
Windows bound residency: the rows resident at any point are one window's.

A physical rowid is the row's position in the scan's emission order, so
versions sharing a logical row id stay distinct.

## Extension surface

With `data_inlining_row_limit` nonzero, DuckLake creates and drives
per-table physical tables in the metadata catalog. The shim
(`crates/moraine-duckdb/cpp/inline_tables.cpp`) recognizes two name
families and routes every operation into the `inline` keyspace over the
same staged-row commit path as the fixed `ducklake_*` tables, described in
[the commit protocol](0003-commit-protocol.md).

- **`ducklake_inlined_data_<table_id>_<schema_version>`**: columns
  `row_id`, `begin_snapshot`, `end_snapshot`, then the table's user columns
  at that schema version. The three metadata columns are derived on read
  and never stored in the body.
- **`ducklake_inlined_delete_<table_id>`**: columns `file_id`, `row_id`,
  `begin_snapshot`.

| DuckLake SQL | Moraine record |
|---|---|
| `CREATE TABLE ducklake_inlined_data_<t>_<v>(...)` with its `ducklake_inlined_data_tables` registration | `schema` at `(t, v)` holding the user columns as an Arrow schema-only stream. Clears any `schema_dropped` marker for the version in the same batch |
| `INSERT INTO ducklake_inlined_data_<t>_<v> VALUES ...`, one multi-row `VALUES` per commit | One `insert` chunk at `(t, v, snapshot, chunk_seq)` and one `chunk_locator` |
| `UPDATE ducklake_inlined_data_<t>_<v> SET end_snapshot = s WHERE row_id = r` | `row_tombstone` at `(t, r, s)` |
| `SELECT ... FROM ducklake_inlined_data_<t>_<v>` with the snapshot predicate, and the insertions, deletions, and flush variants | Directory-served or chunk-walk scan for `t` at `v`, Arrow decode, the three derived columns, the scan-kind predicate, ordered by `row_id` |
| `INSERT INTO ducklake_inlined_delete_<t> VALUES (file_id, row_id, s)` | `file_delete` at `(t, file_id, row_id)` holding `s`, plus the `file_delete_table` marker |
| `DELETE FROM ducklake_inlined_delete_<t>`, the flush's cleanup | Remove each matched `file_delete`, keeping the marker. Translated per matched row. Naming a record the table does not carry is an error |
| Selective `DELETE FROM ducklake_inlined_data_<t>_<v>`, snapshot expiry | Remove the selected versions and rewrite partial chunks, retaining the deletion events a surviving version needs |
| `DELETE FROM ducklake_inlined_data_<t>_<v> WHERE begin_snapshot <= flush_snapshot`, then `DROP TABLE` and the registry delete | Remove the flushed chunks, their locators, and consumed tombstones. Write `schema_dropped` for `(t, v)`, retaining `schema` |
| Hard `DELETE FROM ducklake_data_file` naming `(t, file_id)` | Remove every `file_delete` targeting that file, keeping the marker. Silent on a miss |
| `UPDATE ducklake_data_file SET end_snapshot` | Nothing |
| `DROP TABLE ducklake_inlined_delete_<t>` | Remove every `file_delete` for `t` and the `file_delete_table` marker |
| `DROP TABLE lake.<schema>.<t>` cascade | Remove every `inline` record for `t` |

## Schema deregistration

DuckLake caches a table's inlined-table list under the schema version and a
flush does not move that version. A reader in another process keeps naming
a table the flush dropped. The `schema_dropped` marker splits registration
from existence: the version leaves `ducklake_inlined_data_tables`, so a
reader that loads the list afterwards never learns it, while the `schema`
record stays, so the name still binds and scans empty. Registering a
version clears its marker in the same batch, so a version is never both
listed and marked. The drop cascade removes the markers.

Deregistered `schema` records live as long as their table. DuckLake mints a
fresh inlined table whenever its cached list is empty, with columns
unchanged, so the bytes repeat. A deregistration collapses its record to a
`same_as_version` reference when another version of the same table carries
identical bytes (`inline_schema_collapse_target`). Resolution is one hop
onto a version carrying bytes. A version that is already a reference, and a
version another reference resolves through, are left whole. A reference
naming a version with no bytes is corruption. The registration read walks
keys only and decodes no schema.

The `file_delete_table` marker answers the same caching for the delete
table, which a flush empties without dropping.

## Tombstone history and format migration

`row_tombstone` keeps one event per deletion snapshot, so an inline row's
history survives repeated updates. The migration unit
`version-inline-tombstones` rewrites every `inline_delete` record into its
`row_tombstone` key, preserving the value and removing the source key in
the same batch, under the bounded, cursor-driven protocol in
[the storage format](0001-storage-format.md). It moves a store from format
8 to format 9, `FORMAT_WITH_INLINE_TOMBSTONE_HISTORY`, which is also
`MIN_FORMAT_VERSION`. An older store is refused at open until
`Catalog::migrate` or the SQL function `moraine_migrate` runs. Deletion
snapshots an older writer overwrote are not recoverable, and the migration
does not repair Parquet files flushed with those lifetimes.

## Equality-index upkeep

A flush registers a data file and a delete file against that same file in
one commit. Index upkeep resolves such a delete at the add: the killed
positions are excluded as the file's entries are derived, so those rows are
never indexed. `stream_file_delete_index_entries` handles only targets the
committed head already holds, and a target the commit itself registers is
skipped. Staging a removal beside the add would be indistinguishable from
an `UPDATE`, which adds and removes one `(key, row_id)` in a single batch
and must keep the entry. Upkeep returns early for a table with no live
index. Details are in [equality and range indexes](0009-equality-indexes.md).

Upkeep for a `row_tombstone` needs the deleted row's indexed values. It
seeks the chunk directory from that row id, reads the owning chunks, and
decodes only those bodies. An incomplete directory falls back to the full
chunk scan.

An inlined deletion has no `end_snapshot`, so removal is the only way it
goes away. The cascade keys on how its target file leaves, following
[maintenance](0010-maintenance.md):

- **Pruned**, as a merge's source or by cleanup: the file row is
  hard-deleted from current state and history, so the deletion is
  unreachable and is removed with it.
- **Ended**, as a rewrite's source: the row moves to history because a
  reader below the rewrite still sees it. Its inlined deletions stay until
  expiry prunes the ended row.

## Row limit

The catalog serves no `ducklake_metadata` row for `data_inlining_row_limit`,
so DuckLake's compiled default of 10 rows applies. A stored option row would
outrank both the attach option and the session setting. All three ways of
raising the limit work: `ATTACH ... (DATA_INLINING_ROW_LIMIT n)`,
`SET ducklake_default_data_inlining_row_limit`, and
`CALL lake.set_option('data_inlining_row_limit', n)`.

## Tests

- Nested `LIST`, `STRUCT`, and `MAP` values round-trip through inline storage
  and flush.
- The locator width bound stops the directory walk without skipping an
  overlapping chunk.
- A flush served from a complete directory removes the same chunks and
  tombstones as a chunk walk, and heals an incomplete directory.
- A flushed inlined table deregisters but still binds, and a duplicate schema
  collapses to a reference.
- Hard-deleting a data file drops the inlined deletions against it, and ending
  one keeps them.
- The inlined deletion table exists from its first deletion until the drop.

## Alternatives

- **Storing inlined rows outside SlateDB as small Parquet in the bucket.**
  Rejected: it recreates the tiny-file problem inlining exists to remove.
