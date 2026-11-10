# Roadmap

Moraine is at 0.8, built for DuckDB 2.0.0. Everything listed as done runs
against real DuckLake SQL in the end-to-end suite. Unchecked items are gaps,
not configuration.

## Next

- [ ] Signed distribution through DuckDB's community extension repository,
  so `INSTALL moraine FROM community` works without `-unsigned`.
- [ ] Publishing to crates.io again. Held while the workspace patches
  SlateDB; a registry build would resolve the published SlateDB, which lacks
  the reader refresh Moraine needs. Tags, the changelog, and the extension
  binaries are unaffected, and 0.7.0 stays the newest version on the registry
  until the patch ships upstream.
- [ ] Online index rebuild. Today a rebuild is `DROP INDEX` then
  `CREATE INDEX`, and the index serves neither lookups nor uniqueness in
  between.
- [ ] Windows builds. The extension is built and proven for Linux and macOS
  on x86-64 and arm64; Windows is unverified and WebAssembly is out of scope,
  since the Rust core's dependencies do not target it.
- [ ] Lower per-commit overhead on DuckLake's side. Of a 23 to 25 ms one-row
  commit, DuckLake's metadata SQL is 14.5 ms against 3.3 ms of Moraine's own
  work, and nearly all of it is parsing, binding, and optimizing the
  generated SQL, which DuckDB re-plans on every execution
  ([measurement](docs/benchmarks.md#where-the-time-goes)). Moraine
  reports a catalog version so that plan reuse is possible once DuckDB
  stops re-planning; the rest of the gain is upstream.

## Done

### Catalog

- [x] DuckLake catalog state on SlateDB: snapshots, schemas, tables, views,
  data-file metadata ([storage format](docs/rfcs/0001-storage-format.md)).
- [x] Atomic, multi-statement, cross-table transactions with DuckLake's
  conflict rules; lost races surface as conflicts for DuckLake's retry
  ([commit protocol](docs/rfcs/0003-commit-protocol.md)).
- [x] Time travel to any snapshot and the change data feed between
  snapshots, verified differentially against a stock DuckLake catalog.
- [x] Schema evolution (`ADD`, `RENAME`, `DROP COLUMN`, `ALTER COLUMN … TYPE`)
  over inlined and flushed data; every DuckLake scalar and nested type,
  `VARIANT` included; column and name mapping for externally written
  Parquet; macros; tags and comments; catalog options
  ([DuckLake feature mapping](docs/rfcs/0005-ducklake-feature-mapping.md)).
- [x] Partitioning with pruned reads, sort orders, and table, column, and
  per-file statistics.
- [x] Data inlining, on by default, with flush to Parquet
  ([data inlining](docs/rfcs/0004-data-inlining.md)).
- [x] Data-file encryption: `ENCRYPTED` lakes with per-file keys held in the
  catalog.

### Indexes

- [x] Ordered, optionally unique indexes over table columns, maintained in
  the same commit as the rows; `CREATE INDEX` and `DROP INDEX` from DuckDB;
  equality, `IN`, range, and `IS NULL` lookups chosen by the planner; staged
  builds for large tables; verification and scan-free `DELETE` on a
  verified index
  ([equality and range indexes](docs/rfcs/0009-equality-indexes.md)).

### DuckDB extension

- [x] Moraine as DuckLake's catalog through a DuckDB `StorageExtension`, with
      the full DuckLake SQL surface
      ([extension surface](docs/rfcs/0006-extension-surface.md)).
- [x] Read-write and read-only attaches; a reader never fences the writer;
  checkpoint-pinned readers for strictly read-only credentials.
- [x] Query interruption: Ctrl-C aborts a blocked read, and a submitted
  commit completes and reports its outcome.
- [x] Bundled, patched DuckLake, so one `LOAD` is the whole setup.

### Operations

- [x] DuckLake's compaction, rewrite, snapshot expiry, and file cleanup, with
  row ids and pre-operation time travel preserved; a scheduled in-writer
  maintenance pass that runs them in order and then reclaims what only
  Moraine can see ([maintenance](docs/rfcs/0010-maintenance.md)).
- [x] Explicit, resumable, one-way on-disk format migration; the DuckLake
  catalog version raised on request.
- [x] Crash recovery: every state-changing path is one atomic batch or a
  resumable sequence ([crash recovery](docs/rfcs/0008-crash-recovery.md)).

### Performance

- [x] The write-ahead log on an object store of its own, so a commit's one
  PUT can land on S3 Express One Zone while the catalog stays on standard
  storage; `moraine_move_wal` moves an existing lake's log either way.
- [x] Attach-tunable flush cadence; group commit, so concurrent commits
  share one flush.
- [x] One block cache per process with a disk tier, metadata protected in
  memory, and a preload at attach; row summaries published beside data
  files so pruning skips Parquet footers
  ([read path](docs/rfcs/0007-read-path-and-caching.md)).
- [x] Commit-served projections and shared commit preparation, so commit
  latency does not grow with snapshot history.
- [x] Working-memory budgets per process, transaction, commit, and summary
  build, refusing instead of growing.

<!-- site:omit -->
### Multi-process writers

Moraine's own topology is one fenced SlateDB writer with many readers. A
bucket-resident commit log, slot objects written with create-if-absent
conditional puts and folded into the store, replaces it. The log lives in a
separate library under its own license and is never a dependency of this
repository; what Moraine owns is the boundary it plugs into
([commit-log seam](docs/rfcs/0011-commit-log-seam.md)), and the open build
stays complete on the single-writer path without it.

- [x] The provider boundary, the committer, the garbage-collection plug, a
  process registry with a per-open override, and the format stamp a
  provider-less binary refuses.
- [x] Adoption at open, staged writes offered to the log before they commit
  locally, a lost race folded before the retry lands.
- [x] Committers over a tailing reader, folding on a cadence, a maintenance
  pass from any committer, and `demote` to take a lake back off its log.
- [x] `COMMIT_LOG_TAIL` and `COMMIT_LOG_POLL_INTERVAL_MS` reaching all of it
  from the extension, ignored with no provider registered.
<!-- /site:omit -->

### Hardening and release

- [x] Tests against real object storage; arbitrary-bytes decode proptests
  for every store codec; fuzzing of codecs, the commit read path, and crash
  recovery.
- [x] Versioned tags drive the changelog and per-DuckDB-version,
  per-platform extension builds attached to GitHub releases.
