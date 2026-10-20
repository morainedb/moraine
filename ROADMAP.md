# Roadmap

Moraine is in the **v0.7 release series**. This checklist records implemented
DuckLake catalog coverage and remaining work. Catalog features are validated
against real DuckLake SQL in the e2e suite. Unchecked items are
implementation gaps, not supported attach modes.

## Next capabilities

- [ ] `VARIANT` inlining and variant statistics.
- [ ] Signed community-extension distribution.

## Foundations

### Catalog core on SlateDB
- [x] SlateDB key encoding for DuckLake catalog state (RFC 0002)
- [x] Commit/transaction protocol (RFC 0004)
- [x] Key layout and codecs with proptest roundtrips
- [x] Snapshots, schemas, tables, and data-file metadata
- [x] Atomic commit with conflict detection
- [x] First runnable example

### DuckDB extension
- [x] Extension surface: moraine as a DuckLake catalog via a DuckDB `StorageExtension` (RFC 0006)
- [x] C++ shim registering the catalog and transaction manager over a C ABI to the Rust core
- [x] Extension entry points and loading into a real DuckDB
- [x] Read-write and read-only attach; read-only never fences the live writer (RFC 0017)
- [x] Full DuckLake SQL against moraine as the catalog: `ATTACH`, `CREATE`/`INSERT`/`UPDATE`/`DELETE`/rename/`DROP`, `SELECT`/`COUNT`/time travel
- [x] Query interruption: Ctrl-C aborts a blocked read; submitted commits survive cancellation, which reports an unknown outcome (RFC 0006)

### Multi-process writers
Moraine's own topology is one fenced SlateDB writer with many readers. A
bucket-resident commit log — slot objects written with create-if-absent
conditional puts, folded into the store — replaces it. The log lives in a
separate library under its own license and is never a dependency of this
repository; what moraine owns is the boundary it plugs into, and the open
build stays complete on the single-writer path without it.

- [x] The provider boundary: SlateDB write-ahead-log plugs, the committer,
  the log's garbage-collection plug, a process registry plus a per-open
  override, and the format stamp a provider-less binary refuses (RFC 0024)
- [x] The folder path: adoption at open, staged writes offered to the log
  before they commit locally, a lost race folded before the retry lands
- [x] Committers over a tailing reader: every read-write open past the one
  holding the store commits through the log, taking the store when the tail
  grows long
- [x] Folding on a cadence, a maintenance pass from any committer, and
  `demote` to take a lake back off its log
- [x] Attach options reaching all of it from the extension
  (`COMMIT_LOG_TAIL`, `COMMIT_LOG_POLL_INTERVAL_MS`), ignored with no
  provider registered

## Catalog & schema
- [x] Schemas, tables, and views
- [x] Schema evolution: `ADD`/`RENAME`/`DROP COLUMN` and `ALTER COLUMN … TYPE` promotion, over inlined and flushed data (RFC 0012)
- [x] All DuckLake scalar and nested `LIST`/`STRUCT`/`MAP` types, inline and post-flush; `VARIANT` pending (RFC 0005)
- [x] Column and name mapping for externally written Parquet, including hive-partitioned foreign files (RFC 0018)
- [x] Scalar and table macros with parameters, overloads, defaults, and `CREATE OR REPLACE`/`DROP` (RFC 0019)

## Data, deletes & layout
- [x] Parquet data files on object storage (`data_file`)
- [x] Row-level deletes via delete files / merge-on-read (`delete_file`)
- [x] Data inlining: inlined inserts/deletes plus flush, on by default (RFC 0005)
- [x] Partitioning: definitions, per-file values, and partition-pruned reads (RFC 0013)
- [x] Sort orders, set/changed/reset, with history (RFC 0013)
- [x] Statistics for pruning: table, column, and per-file (`table_stats`, `table_column_stats`, `file_column_stats`); variant stats pending

## Transactions & time travel
- [x] Multi-statement, cross-table ACID transactions with conflict detection; lost races surface as conflicts for DuckLake's retry (RFC 0004)
- [x] Snapshots and time travel to any snapshot, reading past data and schema across inline, evolution, and flush (`snapshot`)
- [x] Change data feed between snapshots, verified differentially against a stock DuckLake catalog (`snapshot_changes`, RFC 0020)

## Maintenance & operations
- [x] Compaction and data-file rewriting: `ducklake_merge_adjacent_files` and `ducklake_rewrite_data_files`, preserving row ids and pre-op time travel (RFC 0008)
- [x] Snapshot expiry and orphaned-file cleanup / deletion scheduling; expired snapshots resolve cleanly (RFC 0007, `files_scheduled_for_deletion`)
- [x] Data-file encryption: `ENCRYPTED` attaches, encrypted Parquet at rest, per-file keys round-tripped; moraine holds no crypto (RFC 0014)
- [x] Table/column tags and catalog options via `COMMENT ON` and options (`tag`, `column_tag`, `metadata`)
- [x] Maintenance orchestration: a scheduled in-writer pass runs DuckLake's own maintenance functions in a fixed order, then reclaims orphaned equality-index entries; `moraine_maintenance` triggers one on demand and `moraine_maintenance_status` reports the retained passes (RFC 0021)

## Performance
- [x] Commit-served projections: `snapshot`, `table_stats`, and `table_column_stats` are folded forward from each commit and served from an in-memory cache when current, removing per-commit latency growth with snapshot history. Attach-tunable WAL flush cadence bounds the per-commit durable wait.
- [x] Catalog commit preparation shares unchanged maps and derives writes from
  changed entities; allocation and CPU benchmarks separate preparation from WAL latency.
- [x] The write-ahead log on an object store of its own, so a commit's one PUT
  can land on an S3 Express One Zone bucket while the rest of the catalog
  stays on standard storage (`META_WAL_PATH`); `moraine_move_wal` moves an
  existing lake's log either way (RFC 0004).
- [x] Staged index derivation streams inline sources with resumable cursors and
  whole-operation peak-memory measurement.
- [x] Small row lookups select requested inline ranges and use cached file
  intervals, retaining conservative handling for arbitrary row-ID files.
- [ ] Reduce metadata-query overhead in DuckDB/DuckLake. Measured: DuckLake's
  metadata SQL is 14.5 ms of a 23-25 ms one-row commit, against 3.3 ms of
  moraine's own commit work ([measurement](metadata-query-overhead.md)). It is
  SQL front-end cost, not storage: parse, bind and optimize are 2.5 ms of a
  2.7 ms statement, execution 0.2 ms, and a prepared statement saves only the
  parse because DuckDB re-binds per execution. Native metadata tables would
  move the 0.2 ms; the leverage is in not re-planning generated SQL every
  commit, on DuckLake's side of the boundary.
- [ ] Take the metadata-overhead measurement further before any design rests
  on it. Where does execution overtake planning as a lake grows — this one
  is 16 files and 1,600 rows, and 16 → 128 files already moves the total
  13.4 → 14.1 ms. And can DuckLake be made to re-plan less, given a prepared
  statement still re-binds on every execution. Profiling has already closed
  the third question: moraine's own code is 1.5% of a commit's CPU and its
  SlateDB reads 2.4%, so there is no material win on this path here.


## Hardening & release
- [x] Real object storage tests (RustFS)
- [x] Arbitrary-bytes decode proptests for store codecs (never panic on garbage)
- [x] Release pipeline: versioned tags drive crate publishing and extension artifacts
- [x] Extension distribution: per-DuckDB-version, per-platform builds attached to GitHub releases (unsigned)
- [ ] Signed distribution via a duckdb/community-extensions submission
- [ ] Resume crates.io publishing, held while the workspace patches slatedb:
  a registry build resolves the published slatedb, which has no
  `DbReader::refresh()`. Tags, the changelog and the extension binaries are
  unaffected, and 0.7.0 stays the newest version on the registry until the
  patch is released upstream.
