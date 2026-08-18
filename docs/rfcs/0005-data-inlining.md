# RFC 0005: Data inlining on SlateDB

- **Date:** 2026-07-08

## Summary

DuckLake data inlining stores small inserts as rows in the catalog database
instead of writing a Parquet file per tiny commit, flushing them to Parquet
later. This RFC defines how moraine implements inlining in the `inline`
subspace reserved by RFC 0002: chunked Arrow-encoded inserts, append-only
tombstones for deletes, and a flush operation that mirrors
`ducklake_flush_inlined_data`. Inlining is strategically important for
moraine — small frequent writes are the workload an LSM is built for, and
this is where a KV catalog can beat a SQL catalog rather than merely match
it — and it is a launch feature: moraine ships with it from the first
release.

## Goals

- An inlined insert is part of the same single-`WriteBatch` commit as its
  catalog metadata (RFC 0002 atomicity invariant) — inlining must not add
  round trips to the commit path.
- Reading a table's live inlined rows is one contiguous range scan.
- Time travel over inlined data works exactly as DuckLake specifies:
  live inlined rows are visible for `begin_snapshot <= S < end_snapshot`;
  after flush, pre-flush snapshots are served from the **flushed Parquet**
  (backdated file record + per-row snapshot columns — Background), never
  from retained catalog rows.
- Storage is append-only on the write path — no read-modify-write of
  existing records inside a commit.

Non-goals:

- Auto-flush policy. moraine exposes the flush mechanism; the caller owns when
  to invoke it as part of its write and maintenance policy. No moraine-chosen
  threshold or scheduler is part of this design.
- Inlining `VARIANT` columns. Arrow cannot represent the type, so a
  `VARIANT` column is refused rather than inlined; carrying it would mean a
  second, non-Arrow value format.

## Background

DuckLake v1.0 models inlined data as catalog tables:

- **Inlined insert tables**, one per `(table, schema_version)`: columns
  `row_id`, `begin_snapshot`, `end_snapshot`, plus the user table's
  columns. A new inlined table per schema version keeps the row layout
  matched to the schema.
- **Inlined deletion tables**, one per table: `(file_id, row_id,
  begin_snapshot)` — deletes against rows in *existing Parquet files*,
  inlined to avoid writing a tiny deletion file.
- Deletes that target inlined insert rows set that row's `end_snapshot`.
- `ducklake_flush_inlined_data()` materializes inlined inserts to Parquet
  and then **hard-deletes** the inlined rows from the catalog
  (source-verified: `DELETE FROM <inlined table> WHERE begin_snapshot <=
  flush_snapshot`; empty superseded inlined tables are then dropped).
  Time travel survives because the flushed file carries hidden per-row
  `_ducklake_internal_row_id` / `_ducklake_internal_snapshot_id` columns
  and its `ducklake_data_file` record is **backdated** to the minimum
  per-row snapshot — a pre-flush time-travel scan reads the Parquet with
  a per-row snapshot filter. Accumulated deletions consolidate into a
  partial deletion file. The catalog retains nothing; retaining and
  serving flushed rows on any catalog path would **double-count** them
  against the backdated file.
- The row threshold comes from `data_inlining_row_limit`, settable
  globally, per attach, or persistently per table/schema (RFC 0002
  `option` records).

SQL catalogs store nested types as `VARCHAR` because they are limited to
their host's type system. Moraine controls its value format and does not
inherit that limitation.

## Design

### Keyspace (fills in the RFC 0002 `inline` reservation)

| Kind | Key components | Value |
|---|---|---|
| `inline/schema` | `table_id, schema_version` | Arrow IPC schema-only stream (written once per schema version) |
| `inline/insert` | `table_id, schema_version, begin_snapshot, chunk_seq` | Arrow IPC record-batch **body** (the batch message + buffers, no schema) over the user columns + `row_id_start`, `row_count`. Decoded against the version's `inline/schema` stream, so the schema is not re-serialized per chunk |
| `inline/inline_delete` | `table_id, row_id` | `end_snapshot` (tombstone for an inlined insert row) |
| `inline/file_delete` | `table_id, data_file_id, row_id` | `begin_snapshot` (inlined delete against a Parquet file) |
| `inline/file_delete_table` | `table_id` | Empty — the key is the fact. Marks that `ducklake_inlined_delete_<table_id>` exists |
| `inline/chunk_range` | `table_id, row_id_end` | `row_id_start, schema_version, begin_snapshot, chunk_seq` — one locator per live insert chunk; `row_id_end` is inclusive |

The insert and tombstone records are append-only on the commit path. The
file-delete-table record is a marker, and exists because existence cannot be
derived from the file-delete records: a flush
materializes a table's inlined deletions into a real delete file and
clears them, and an emptied SQL table still exists. DuckLake caches that
table's existence for the life of the catalog and never re-probes, so a
content-derived existence disappears under it mid-session and every later
bind fails. Written idempotently by both paths that prove the table
exists — staging a deletion into it, and removing one from it, the latter
so a store written before the marker heals on its first flush — and
removed by the `DROP TABLE` cascade. The chunk-range record is an immutable
secondary directory written and removed atomically with its owning chunk.
Its inclusive end is the ordered key, so a forward seek beginning at a
deleted row id lands on the first range that could own the row; the value's
start rejects gaps. This is one entry per chunk, not one reverse entry per
row.

### Write path

An insert below the row limit becomes one `inline/insert` **chunk record**:
the commit's rows for that table, Arrow-IPC-encoded, with row ids
allocated from the table's row-id counter exactly as a Parquet write would
allocate them — the per-table row-id high-water mark in `tstat`, as
DuckLake allocates it. Chunk-per-commit (not row-per-key) because the read
unit is "all live inlined rows of table T", and because one key per commit
rides the `WriteBatch` with negligible overhead. `chunk_seq` disambiguates
multiple chunks in one commit (how rows are batched within a commit is an
implementation detail). The same batch writes the chunk's
`inline/chunk_range` locator.

Type eligibility here is **moraine's**, not DuckLake's, and it is set by the
value format below: an inline chunk is Arrow IPC, so a column is inlinable
exactly when DuckDB's Arrow encoding can carry it. That is a different and
narrower rule than DuckLake's own — stock DuckLake stores inline data as SQL
values and inlines types moraine cannot.

`GEOMETRY` is inlinable when `spatial` is loaded, because spatial registers
the Arrow extension type; values survive both the inline keyspace and the
flush. `VARIANT` has no Arrow representation at all, so moraine refuses the
column at `CREATE TABLE` with an error naming moraine, the type, and Arrow
as the cause — a refusal, not a silent Parquet fallback, because the table
would otherwise appear to work until its first insert. Scalars, `BLOB`,
`UUID`, `DECIMAL` and nested `LIST`/`STRUCT`/`MAP` all inline.

Arrow IPC is the value format because inlined data is *row data*, not
metadata: it carries the table's actual types — including nested
STRUCT/MAP/LIST natively, where SQL catalogs degrade to `VARCHAR` — and
the flush path can feed record batches to a Parquet writer without a
transcoding step. The chunk's schema is pinned by `schema_version` in the
key, mirroring DuckLake's inlined-table-per-schema-version design: schema
evolution never rewrites existing chunks.

Deletes:

- Against an inlined insert row → `inline/inline_delete` tombstone carrying
  `end_snapshot`. DuckLake's SQL form updates the row in place; a
  tombstone is the append-only equivalent and keeps chunks immutable.
- Against a Parquet-file row, when small enough to inline →
  `inline/file_delete`, matching the spec's inlined deletion table shape.

### Encoding overhead

Arrow IPC is chosen for the flush path and type fidelity, not for
compactness at a few rows per chunk — and inlining is nothing but the
small-chunk regime. Two costs are inherent to the format there: buffer
alignment (each column buffer is padded to an 8/64-byte boundary, which
at three rows can exceed the row bytes themselves) and the per-message
flatbuffer header. Both are fixed per chunk, so they are worst when
chunks are smallest — exactly the workload this feature targets. This is
a deliberate trade for a transcode-free flush, and it is bounded: chunk
sizes are capped by the row limit and drained by flush cadence.

The schema is *not* one of these costs, and is not paid per chunk.
`schema_version` is a key component (`inline/insert`), so the reader schema
is recoverable without re-embedding it. Each `inline/insert` value carries
the record-batch **body** only — the batch message (buffer layout, null
counts) plus the buffers, with no schema message — and decode supplies the
schema back from the version's `inline/schema` record (arrow-rs's low-level
`encode`/`read_record_batch`, either side of the FFI bridge). So the WAL
append for a tiny commit never re-serializes the schema.

An `inline/schema` record is still stored once per `(table_id,
schema_version)` — the Arrow IPC schema-only stream — written in the same
`WriteBatch` as the first inlined insert for that schema version. It is
what reconstructs the synthesized table entry's columns (names and types)
when the table is looked up, including for an empty scan. Storing it
rather than deriving the columns from the catalog's per-version metadata
is deliberate: the stored message is self-describing, so the column layout
resolves identically no matter how moraine's DuckLake-type → Arrow-type
mapping evolves after it was written — correct under the append-only
immutability invariant, for one small record per schema version.

Buffer compression stays off. Arrow's LZ4/ZSTD codecs are framed per
buffer and lose to their own overhead at these sizes; whatever cross-
chunk redundancy remains is reclaimed by SlateDB's SST block compression
at rest, where it costs nothing on the write path. (That reclamation
assumes plaintext values, which holds: RFC 0014 delegates catalog-at-rest
encryption to the object store, which encrypts post-compression.)

### Read path

Live inlined rows of table T at snapshot S: range-scan
`inline/insert/{table_id}` (all schema versions), keep chunks with
`begin_snapshot <= S`, subtract row ids from `inline/inline_delete` tombstones with
`end_snapshot <= S`. Inlined file-deletes overlay Parquet scans the same
way delete files do. The tombstone set for a table is scanned once and
held in memory — inlined data is bounded by the row limit and flush
cadence, so these sets are small by construction.

Equality-index maintenance for an `inline/inline_delete` needs the deleted
row's indexed values. It seeks the `inline/chunk_range` directory from that
row id, reads only the owning immutable chunk, and decodes that body. A
partially populated directory means an older chunk is still live, so the
lookup falls back to the full chunk scan rather than risking an uncovered
index removal.

The directory serves every other inline read the same way once it is known
**complete** — naming every live chunk. Completeness is judged once, by
comparing the directory against a full chunk walk, and remembered per
table (RFC 0009, store facts) — but only when the store format locks out
writers that predate the directory, because from that format on every
chunk arrives with its locator in the same batch and completeness is
monotone. From there:

- the flush walk and the row-location probe run on locators alone — row
  spans without the Arrow bodies, which were the bulk of a flush's block
  misses and were paid again per DELETE statement;
- a table scan materializes its row set from the locators, selects the
  scan's rows, and point-reads only the chunk bodies the selected rows
  reference — a chunk whose rows are all tombstoned, or outside an
  incremental scan's window, is never hauled.

Who judges and who repairs is split by capability. A flush that finds the
directory incomplete heals the gap onto its own batch — locators for
surviving uncovered chunks, deletions for locators naming no chunk — and
remembers nothing off a repair still riding the batch: completeness is
trusted only once it has landed. The read path never writes, so it judges
without healing, and only on an isolated session — a manifest-following
pass can straddle a commit and must not judge. A directory found short of
complete costs what it always did: the full chunk scan.

Each chunk carries its `schema_version` (its key component), so a
body-only chunk is decoded against *its own* version's `inline/schema`,
never a neighbor's. This matters once a table is schema-evolved and holds
inline chunks under several versions: DuckLake reads each
`ducklake_inlined_data_<t>_<v>` separately, and the shim serves it only the
chunks of version `v`.

### Flush

Flush reproduces `ducklake_flush_inlined_data` semantics as one catalog
commit (still one `WriteBatch`, plus the Parquet PUTs that precede it, in
that order — data before metadata, like any DuckLake write):

1. Write live inlined rows to Parquet data file(s); write a partial
   deletion file consolidating tombstones if any, preserving per-row
   snapshot metadata.

   The rows reach DuckLake's writer **column-wise**. A chunk decodes once,
   through DuckDB's own Arrow importer, and stays as the `DataChunk` that
   importer produced; the scan then emits its output by copying whole runs
   of it across, vector to vector. Runs are long in practice: the scan
   orders by `row_id`, and row ids within a chunk follow insertion order.
   The three metadata columns are written straight into flat vectors, and a
   column no one projected is never copied at all. Nothing is transcoded
   through `duckdb::Value` in either direction, which is the point — a
   row-by-row materialization would undo the transcode-free property the
   format was chosen for, one `Value` per cell, twice.

   A rowid the scan emits stays its index into that scan's row list, since
   the UPDATE and DELETE paths resolve one back by re-materializing the
   list. They need only `row_id` and `begin_snapshot`, so that
   re-materialization decodes no Arrow body at all.
2. In the commit batch: create the `file` (and `delfile`) records — the
   file record backdated to the minimum per-row snapshot, row-faithfully,
   as DuckLake writes it — and **delete** the flushed `inline/insert` chunks,
   their `inline/chunk_range` locators, and consumed
   `inline/inline_delete`/`inline/file_delete` records, matching DuckLake's
   delete-at-flush semantics. Pre-flush time travel is served by the
   flushed Parquet (per-row snapshot columns), not by retained chunks —
   retained chunks visible to any catalog scan would double-count rows.

### Why this is a fit for the substrate

Every inlined commit is a small append into SlateDB's WAL — the access
pattern LSMs exist for. Sustained small-insert throughput is then governed
by WAL group commit (many catalog commits per PUT), not by
one-Parquet-file-per-commit overhead. And because the `inline` subspace is
its own SlateDB segment (RFC 0002: format v1 stores are created with a
tag-byte segment extractor), inline churn compacts independently — the
heaviest write traffic in the store never drags the small metadata
subspaces into shared rewrites. Latency per commit remains
PUT-bound as documented in the README; what inlining changes is that tiny
commits stop costing a Parquet file, a data-file record, and eventual
compaction debt each. Flush is the explicit analogue of LSM compaction,
converting accumulated small writes into scan-optimized storage.

### Extension surface (as implemented)

DuckLake does not write inlined rows as fixed `ducklake_*` rows. When
`data_inlining_row_limit != 0` (its compiled default is 10, which moraine
leaves standing — it serves no `ducklake_metadata` row for the key,
because one would outrank the ATTACH option and the DuckDB setting alike
and shadow every way of raising the limit), DuckLake
**dynamically creates and drives per-table physical tables** in the
metadata catalog and issues ordinary SQL against them. moraine's
`StorageExtension` recognizes these table-name patterns and routes every
operation into the `inline/*` keyspace instead of materializing real
tables — the same staged-commit substrate the fixed `ducklake_*` tables
ride, extended to two dynamic name families:

- `ducklake_inlined_data_<table_id>_<schema_version>` — inlined inserts.
  Columns `(row_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT,
  <the table's user columns at that schema version>)`.
- `ducklake_inlined_delete_<table_id>` — inlined deletes against Parquet
  rows. Columns `(file_id BIGINT, row_id BIGINT, begin_snapshot BIGINT)`.

The operation → keyspace mapping (source-verified against DuckLake
`ducklake_metadata_manager.cpp` / `ducklake_inline_data.cpp` /
`ducklake_flush_inlined_data.cpp`):

| DuckLake SQL | moraine record |
|---|---|
| `CREATE TABLE ducklake_inlined_data_<t>_<v>(...)` (batched with the `INSERT INTO ducklake_inlined_data_tables` registration) | `inline/schema` at `(t, v)` holding the user columns as an Arrow IPC schema-only stream (DuckDB's `ArrowConverter::ToArrowSchema` transcodes the column list; the Rust bridge serializes it); the table appears in the now-live `ducklake_inlined_data_tables` projection |
| `INSERT INTO ducklake_inlined_data_<t>_<v> VALUES (row_id, {snap}, NULL, <cols>), …` (one multi-row `VALUES` per commit) | one `inline/insert` chunk at `(t, v, begin_snapshot={snap}, chunk_seq)`: the user-column cells as one Arrow IPC record-batch body (no schema message; decoded against the version's `inline/schema`), plus `row_id_start` (first row's `row_id`) and `row_count`; and one `inline/chunk_range` locator keyed by its inclusive row-id end. The `row_id`/`begin_snapshot`/`end_snapshot` columns are moraine-derived on read (`row_id = row_id_start + offset`, `begin_snapshot` from the key, `end_snapshot` from `inline/inline_delete`), never stored in the body |
| `UPDATE ducklake_inlined_data_<t>_<v> SET end_snapshot={snap} WHERE row_id=r …` | `inline/inline_delete` at `(t, r)` holding `end_snapshot={snap}` |
| `SELECT <cols> FROM ducklake_inlined_data_<t>_<v> WHERE {snap} >= begin_snapshot AND ({snap} < end_snapshot OR end_snapshot IS NULL) ORDER BY row_id` (and the `SCAN_INSERTIONS`/`SCAN_DELETIONS`/`SCAN_FOR_FLUSH` filter variants) | range-scan `inline/insert` for `t` at `v`, decode Arrow, reconstruct the three virtual columns, apply the snapshot predicate, subtract `inline/inline_delete` tombstones, project and order by `row_id` |
| `INSERT INTO ducklake_inlined_delete_<t> VALUES (file_id, row_id, {snap}), …` | `inline/file_delete` at `(t, file_id, row_id)` holding `begin_snapshot={snap}`, plus the `inline/file_delete_table` marker for `t` |
| `DELETE FROM ducklake_inlined_delete_<t>` (the flush's clean-up, once those deletions are written out as a real delete file) | remove the `inline/file_delete` record behind each matched row, keeping the `inline/file_delete_table` marker. Translated per row rather than as a table-wide clear: DuckLake's flush happens to delete every row, but what it issues is an ordinary SQL `DELETE`, so a filtered one removes exactly what it matched. Naming a record the table does not carry is a typed error, not a silent no-op |
| `DELETE FROM ducklake_inlined_data_<t>_<v> WHERE begin_snapshot <= {flush_snap}` then `DROP TABLE …` + `DELETE FROM ducklake_inlined_data_tables …` (flush / superseded-table cleanup) | remove the flushed `inline/insert` chunks, their `inline/chunk_range` locators, and consumed `inline/inline_delete`; drop the `inline/schema` and deregister. The flushed data lives on as the backdated `ducklake_data_file` DuckLake registers through the ordinary file path |
| hard `DELETE FROM ducklake_data_file` naming `(t, file_id)` (a merge pruning its sources, cleanup draining the schedule) | remove every `inline/file_delete` record targeting that file, keeping the marker. Silent on a miss, unlike the removal above: this cascade names a file rather than records, and a pruned file carrying no inlined deletion is the ordinary case |
| `UPDATE ducklake_data_file SET end_snapshot` (a rewrite ending its source) | nothing — see below |
| `DROP TABLE lake.<schema>.<t>` cascade | drop every `inline/*` record for `t` |

This is served through the same staged-row commit path (RFC 0004): the
inline operations arrive as staged INSERT/UPDATE/DELETE inside DuckLake's
metadata-commit transaction and translate at commit into one atomic batch
of `inline/*` records — same one-batch atomicity, same no-internal-retry,
same `conflict` wire contract. Values DuckLake authors (`row_id`,
`begin_snapshot`, `end_snapshot`, user cells) are stored verbatim per the
keyspace; nothing is re-derived on write.

A nested column reaches the catalog as DuckLake represents it: a top-level
marker row (`list`/`struct`/`map`) plus child `ducklake_column` rows linked
by `parent_column`. moraine stores those rows verbatim, passes the marker
through its `ducklake_column` projection, and reconstructs the nested
`LogicalType` from the child hierarchy for its own catalog entries
(`catalog.cpp`'s `BuildColumnType`); the Arrow IPC inline path carries the
values themselves natively. `LIST`, `STRUCT`, and `MAP` are verified live
through inline and flush by the e2e test
`ducklake_inline_nested_types_round_trip_through_flush`.

Three reconciliations with the surrounding RFCs, recorded here because they
governed the implementation:

- **Flush removes inline records; it does not move them to `history`.**
  RFC 0004's commit-participants note phrases flush as "moves consumed
  inline records to `history`"; the accurate behavior — matching DuckLake's
  hard `DELETE` and RFC 0005's flush section — is that `inline/*` records
  are *deleted* at flush (the data survives as the backdated Parquet
  file). The `inline` subspace is append-then-delete, not begin/end
  versioned like the entity subspaces, so this slice hard-deletes the
  consumed records at flush.
- **Schema stored, not derived.** `inline/schema` holds the Arrow schema
  written at `CREATE` time (transcoded from DuckLake's column list), so an
  `inline/insert` chunk decodes self-describingly without coupling to the
  mutable `ducklake_column` type mapping — as the Alternatives section
  requires.
- **Arrow IPC via the C Data Interface, transcode split across the FFI.**
  DuckDB's C++ has no Arrow IPC serializer, so the shim (`inline_tables.cpp`)
  and the Rust bridge (`arrow_ipc.rs`) split the work along the C Data
  Interface: C++ converts a `DataChunk`'s user columns to `ArrowArray`/
  `ArrowSchema` with DuckDB's own `ArrowConverter`, and Rust serializes
  those to IPC bytes with `arrow-rs`. Decode reverses it — Rust turns IPC
  bytes back into C Data Interface structs, and the shim feeds them to
  DuckDB's own record-batch importer (`ArrowTableFunction::ArrowToDuckDB`).
  Because DuckDB owns both the export and the import, the encoding is
  exactly as type-faithful as DuckDB's Arrow support (nested `LIST`/`STRUCT`
  round-trip; proven at the bridge level in `arrow_ipc.rs`), not the
  lossier `Value::ToString()` text a shim-local codec would force. The one
  cost the RFC's "encoding overhead" note flags remains: flush is **not
  transcode-free** — `ducklake_flush_inlined_data` reads inlined rows back
  through the importer into a `DataChunk` (each cell then boxed into a
  `duckdb::Value` for the shim's row-oriented scan) and DuckLake's writer
  re-encodes to Parquet. A future column-oriented decode path could hand
  the imported `DataChunk` to the flush writer directly, but the row
  materialization is not on the tiny-commit hot path inlining optimizes.

### Flush registers a data file and a delete file against it in one commit

A flush does not filter tombstoned rows out of the Parquet it writes. It
materializes **every** inlined row of the table — live and tombstoned
alike — into one data file, then writes a delete file naming the
tombstoned rows' *positions in that file*
(`ducklake_flush_inlined_data.cpp`'s `AttachDeleteFilesToWrittenFiles`).
One commit therefore carries a `ducklake_data_file` insert and a
`ducklake_delete_file` insert whose `data_file_id` is that same
brand-new file.

Equality-index upkeep therefore resolves such a delete **at the add**: the
rows it kills are left out of the file's entries as the file is read, so
they are never indexed at all. The commit first groups delete sources by
target without opening their objects. A newly registered target resolves its
own delete files before deriving that target's additions, while unrelated
additions and removals run beside that discovery. Nothing relies on the two
catalog rows' order in the batch.

Staging a removal beside the add instead would be wrong, not merely
wasteful. An index entry's key carries no file, so an add and a removal of
one `(key, row_id)` in a single batch is *exactly* the shape an `UPDATE`
produces — DuckLake rewrites the updated row into a new file under its
preserved row id, ending the old file's copy — and there the entry must
survive. The two are indistinguishable at the batch layer and can only be
told apart by whether the removal targets the very file the add came from.
`stage_file_delete_entries` accordingly handles only targets the committed
head already holds; a target the commit itself registers is skipped there
because the add already accounted for it.

Only an indexed table reaches any of this: upkeep returns early when the
table carries no live index.

### An inlined deletion dies with its target's row, not with its target

`ducklake_inlined_delete_<t>` has no `end_snapshot` column: a deletion
exists or it does not, and there is no way to express one that applied over
a window. Removal is therefore the only way one goes away, and every
removal has to be paired with something that preserves what it meant — the
flush pairs its clear with a backdated delete file.

That forces the cascade to key on how a data file leaves, which RFC 0021
already distinguishes for its own reasons:

- **Pruned** (a merge's sources, cleanup's schedule) — the row is
  hard-deleted, current *and* history, because the backdated replacement
  subsumed its whole visibility history. Nothing can read that file at any
  snapshot again, so an inlined deletion against it is unreachable. It is
  removed with the file's row. Left behind, it would still be served, and
  the next flush would materialize it into a delete file naming a data file
  that no longer exists — which the commit refuses.
- **Ended** (a rewrite's source) — the row moves to `history` precisely
  because a reader below the rewrite must still see the rows it
  materialized deletes for. The deletions that make those rows dead have to
  stay readable alongside it, so ending a file removes nothing. They go
  later, when expiry and cleanup prune the ended row.

Both are covered by
`hard_deleting_a_data_file_drops_the_inlined_deletions_against_it` and
`ending_a_data_file_keeps_the_inlined_deletions_against_it`.

## Alternatives considered

- **Row-per-key inserts (`inline/insert/<table_id>/<row_id>`):** enables
  point deletes by key but makes every read a per-row decode and bloats
  key overhead for the dominant scan workload. Rejected; chunks match the
  read unit.
- **In-place `end_snapshot` updates on delete (mirror the SQL design):**
  requires read-modify-write of a chunk inside the commit path, breaking
  append-only writes and inflating write amplification for one deleted
  row. Rejected in favor of tombstones.
- **Encoding rows as protobuf like metadata records:** loses native
  columnar types, adds a transcode on flush, and reimplements what Arrow
  already defines. Rejected; the metadata codec argument (RFC 0002) does
  not transfer to row data.
- **Self-contained IPC stream per chunk (schema embedded in every
  value):** re-serializes the schema into the WAL on every tiny commit for
  bytes the key already determines via `schema_version` — the encoding to
  avoid, and avoided. Each `inline/insert` value stores the record-batch
  **body** alone (via arrow-rs's low-level `encode`), decoded against the
  version's `inline/schema` stream (`read_record_batch`). The
  round-trip-robustness a self-contained stream would have bought is instead
  guaranteed by DuckDB owning both the export and import — the body carries
  the batch message (buffer layout, null counts), which is all `read_record_
  batch` needs alongside the stored schema. Dictionary-encoded columns are
  the one shape the body cannot carry (no dictionary messages); inlined user
  columns are not dictionary-encoded, and the encoder rejects them if they
  ever were.
- **Deriving the reader schema from catalog column metadata** (instead of
  the `inline/schema` record): saves that record, but couples chunk
  decode to a frozen DuckLake-type → Arrow-type mapping — a later mapping
  change would silently misread existing chunks. Rejected; a self-
  describing stored schema is worth one small record per schema version
  under the append-only immutability invariant.
- **Storing inlined rows outside SlateDB (small Parquet in the bucket):**
  that is just… not inlining; it recreates the tiny-file problem inlining
  exists to solve.
