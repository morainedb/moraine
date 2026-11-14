# RFC 0006: Extension surface

## Summary

`moraine-duckdb` exposes the moraine core to DuckDB as a DuckLake metadata
catalog. A thin C++ shim registers a `StorageExtension` and calls a C ABI
exported by the Rust core as a static library. DuckLake attaches moraine as its
catalog and drives it with ordinary SQL against the `ducklake_*` tables, which
moraine serves row for row from the store.

## Composition

DuckDB's stable C extension API registers scalar and table functions and cannot
register a catalog. A writable DuckLake catalog needs a `StorageExtension`, a
`Catalog`, and a `TransactionManager`, and registering those means linking
DuckDB's internal C++ headers. The extension is therefore two layers in one
loadable:

1. **The C++ shim** in `crates/moraine-duckdb/cpp/` links DuckDB's internals,
   registers the storage extension under the attach type `moraine`, and
   translates DuckDB catalog callbacks into C calls. It holds no DuckLake domain
   logic. Its one carve-out is sequencing calls to DuckLake's own maintenance
   SQL, which the core cannot issue.
2. **The Rust C ABI** in `crates/moraine-duckdb/src/` is the moraine core
   compiled as a staticlib with an `extern "C"` surface. `build.rs` generates
   `cpp/moraine_abi.h` from it with cbindgen, so the header cannot drift from
   the Rust declarations.

Because the shim links DuckDB internals, a build is bound to one DuckDB release
string, patch level included, and the loadable carries a statically linked
DuckDB. Metadata scan results cross the ABI as owned arrays of `#[repr(C)]` row
structs with one `_free` function per array, while inline chunk bodies and
indexed row scans use the Arrow C Data Interface (`src/arrow_ipc.rs`,
`moraine_row_scan_next_arrow`). The bundled, patched DuckLake is linked into the
same loadable and registered from the same entry point, so `LOAD moraine` loads
both extensions. Loading stock DuckLake first is refused.

## Attach forms

The primary form is DuckLake's:

```sql
ATTACH 'ducklake:moraine:<store-uri>' AS lake (DATA_PATH '<object-store-uri>');
```

DuckLake connects to its metadata catalog by executing a nested
`ATTACH 'moraine:<store-uri>'` with the alias `__ducklake_metadata_<alias>` and
`HIDDEN true`. DuckDB's prefix dispatch resolves `moraine:` to the registered
storage extension, so the nested attach and the standalone
`ATTACH '<store-uri>' AS m (TYPE moraine)` reach the same
`MoraineCatalog::Attach`. The standalone attach serves listings, `DESCRIBE`, and
every `ducklake_*` projection. A `SELECT` on a user table through it binds and
then raises `InvalidInputException` at execution, naming the `ducklake:moraine:`
attach. User data is served only through DuckLake.

The store URI selects the object store:

| URI | Store |
|---|---|
| `/path/to/dir` | local filesystem, created if absent |
| `memory://` | ephemeral in-process store |
| `s3://<bucket>[/<prefix>]` | S3 or S3-compatible, bucket must exist |

Credentials are never attach options. For an `s3://` store the shim resolves the
DuckDB `s3`-type secret whose scope matches the path and passes it across the
ABI as `MoraineS3Config`. A secret with `REFRESH 'auto'` supplies a refresh
recipe, which the core uses as its signing provider. The `WAL_PATH` store
resolves its own secret.

DuckDB opens any attach whose path starts with a remote prefix read-only unless
`READ_WRITE` is given, so creating or writing an S3-backed lake requires an
explicit `READ_WRITE`. A read-only attach of an uninitialized store fails with a
message that names this fix.

## Attach options

Every option reaches the shim through DuckLake's `META_` passthrough on a
`ducklake:moraine:` attach, or bare on a standalone attach. Names are matched
case-insensitively. The shim parses the following in `cpp/catalog.cpp` and
forwards them through <!-- site:omit -->
`moraine_attach_with_commit_log`<!-- /site:omit the attach call -->. Semantics
live with the owning document.

| Option | Core binding | Default |
|---|---|---|
| `ENCRYPTED` | `CatalogOptions::encrypted`, creation-time only | `false` |
| `DATA_PATH` | `CatalogOptions::data_path`; normally omitted, since the root the bundled DuckLake records on the first attach is adopted on first use | the recorded root |
| `WAL_PATH` | `CatalogOptions::wal_store`, see [the commit protocol](0003-commit-protocol.md) | catalog store |
| `FLUSH_INTERVAL_MS` | `CatalogOptions::flush_interval`, zero maps to continuous flush | 100 |
| `FLUSH_ON_COMMIT` | `CatalogOptions::flush_on_commit` | `false` |
| `READER_POLL_INTERVAL_MS` | `CatalogOptions::reader_poll_interval` | 10000 |
| `CHECKPOINT` | `CatalogOptions::checkpoint`, read-only attach only | unset |
| `CACHE_DIR`, `CACHE_SIZE`, `CACHE_MEMORY`, `CACHE_AUXILIARY_PERCENT` | the cache settings below, which these stand in for on the first attach; accepted for existing attaches | the settings |
| `CACHE_PUTS` | `CatalogOptions::cache_puts` | `true` |
| `CACHE_COMPACTION_PUTS` | `CatalogOptions::cache_compaction_puts` | `true` |
| `CACHE_PRELOAD` | `CatalogOptions::cache_preload`, one of `none`, `off`, `l0`, `all` | `l0` |
| `MAINTENANCE_INTERVAL`, `MAINTENANCE_BATCH_SIZE`, `MAINTENANCE_TABLE_BATCHES`, `MAINTENANCE_PROGRESS`, `MAINTENANCE_SWEEP_INDEXES` | shim `MaintenanceConfig`, see [maintenance](0010-maintenance.md) | pass disabled |
| `MAINTENANCE_COMPACT_STORE`, `MAINTENANCE_COMPACT_STORE_SUBSPACE`, `MAINTENANCE_COMPACT_STORE_TIMEOUT` | shim `MaintenanceConfig`, subspace validated at attach | off |
| `MAINTENANCE_<step>`, `MAINTENANCE_<step>_<parameter>` | shim `MaintenanceConfig`, steps `expire_snapshots`, `flush_inlined_data`, `merge_adjacent_files`, `rewrite_data_files`, `cleanup_old_files`, `delete_orphaned_files` | off |

A misspelled `CACHE_PRELOAD` level is refused. An unknown
`MAINTENANCE_COMPACT_STORE_SUBSPACE` is refused at attach by asking the core
through `moraine_subspace_is_known`. The shim also reads DuckDB's `threads`
setting and the buffer manager's memory limit at attach, passing the first as
`host_threads` and checking the second through `moraine_validate_host_memory`.

## Attach modes

Use DuckDB's `READ_ONLY` attach flag to open a reader. DuckDB
resolves the flag into `AttachOptions::access_mode`, and the shim reads that
field in `MoraineCatalog::Attach` and passes a `read_only` bit across the ABI.
DuckLake forwards its outer `READ_ONLY` into the nested metadata attach, so the
flag the user types on the `ducklake:moraine:` attach is the one the shim sees.

The bit selects the core open. Read-write calls `Catalog::open`, which takes the
one SlateDB writer. Read-only calls `Catalog::open_read_only`, which returns a
`ReadOnlyCatalog` over a `DbReader`. The ABI holds both behind one
`MoraineCatalogHandle`, so a write on a read-only handle is refused at the ABI
with `CONSTRAINT`. A `DbReader` never fences the writer, and any number may run
beside it.

A deployment designates one read-write process. SlateDB fencing is
newest-writer-wins, so a second read-write attach does not fail itself. The
incumbent's next store operation fails with `Error::Fenced`, mapped to `FENCED`
and raised as `IOException`, with a message telling the operator to re-attach.
The topology is specified in [the commit protocol](0003-commit-protocol.md).

A follow-latest reader pins the checkpoint the writer publishes and issues no
writes, so read-only credentials suffice. `READER_POLL_INTERVAL_MS` bounds how
long its transactions keep one publication before looking for a newer one. The
checkpoint reader is a sub-mode of read-only: `CHECKPOINT '<id>'` opens the
`DbReader` against a checkpoint minted ahead of time. It reads a fixed cut and
never observes later commits. A checkpoint id on a read-write attach, an id the
manifest no longer carries, and a malformed id are each refused.

## Settings

Process-wide settings are registered at `LOAD` by `cpp/settings.cpp` and
read by the first attach. Changes after that require restarting the process
and setting the new values before attaching. Size parsing follows DuckDB's
`memory_limit`. An attach option listed above stands in for its setting
when given.

| Setting | Reaches | Default |
|---|---|---|
| `moraine_cache_dir` | `CatalogOptions::cache_dir`, see [the read path](0007-read-path-and-caching.md) | unset |
| `moraine_cache_size` | `CatalogOptions::cache_size` | 16 GiB |
| `moraine_cache_memory` | `CatalogOptions::cache_memory` | 640 MiB |
| `moraine_cache_auxiliary_percent` | `CatalogOptions::cache_auxiliary_percent`, capped at 50 | one eightieth of the budget |
| `moraine_working_memory`, `moraine_staging_memory`, `moraine_commit_memory`, `moraine_store_memory`, `moraine_summary_memory`, `moraine_host_memory`, `moraine_host_headroom` | `moraine_configure_memory` → `configure_memory(MemoryBudgets)`, once per process, before the host check | the read path's defaults |

`moraine_summary_scan_threads` is the one setting read per statement rather
than at attach.

## Catalog tables

moraine intercepts at DuckDB's catalog, table-scan, and DML layer. It never
parses SQL. The catalog exposes every `ducklake_*` table DuckLake queries as an
entry with DuckLake's own column list, transcribed in
`cpp/metadata_table_specs.cpp`, and serves each from a `moraine_dump_*`
provider. Tables with no modeled records are registered and empty, because
DuckLake's commit batches join them unconditionally. `ducklake_metadata` is
synthesized from store facts, so a fresh moraine store is a valid DuckLake
catalog and DuckLake's bootstrap DDL never runs against one.

Both `current` and `history` rows of a versioned kind
are emitted, since DuckLake filters lifecycles in SQL. Time travel and schema
reconstruction therefore need no logic in moraine. Stored column types are
translated at one point, `DuckLakeColumnType`, from DuckDB SQL names to
DuckLake's lowercase vocabulary.

DuckLake's `INSERT`, `UPDATE`, and `DELETE` against these tables reach the core
through four entry points: `moraine_tx_begin`, `moraine_tx_stage`,
`moraine_tx_commit`, and `moraine_tx_rollback`. Every id, counter,
`schema_version`, and `begin_snapshot` value DuckLake authors crosses the ABI
verbatim. One convention is interpreted: an `UPDATE ... SET end_snapshot`
becomes a `current`-key delete plus a `history`-key write, because
[the storage format](0001-storage-format.md) encodes the lifecycle columns in
the key. DuckLake's delete-rewrite also issues an
`UPDATE ... SET begin_snapshot` against the replacement file it inserted in the
same transaction, staged as `RowOperation::UpdateSetBegin` on
`ducklake_data_file` only.

The shim enforces DuckLake's primary keys. An insert whose id
already names a live row under one of them is refused with `CONSTRAINT`, and the
message carries no retry substring. No name uniqueness is enforced on any name
column, because DuckLake declares none and polices naming in its binder.

With inlining on, DuckLake creates and drives per-table physical tables named
`ducklake_inlined_data_<table_id>_<schema_version>` and
`ducklake_inlined_delete_<table_id>`. The shim recognizes both families by
pattern in `cpp/inline_tables.cpp` and routes them through the same staged path
into the `inline/*` keyspace, as specified in
[data inlining](0004-data-inlining.md).

## Commit routing

`MoraineMetadataManager` is registered under the `moraine:` metadata prefix and
opts every DuckLake commit into the server-side route by calling
`SetRetrialsServerSide(true)`. Inside `FlushChangesServerSide` the shim runs
DuckLake's own `RunCommitLoop`, which generates the commit's rows and drives
them through the ordinary DML hooks under DuckLake's retry budget. A data-only
commit never spends that budget: the core's batch rebases the head-derived
values DuckLake stamped and re-drives a benign lost race itself, as
[the commit protocol](0003-commit-protocol.md) specifies. A commit that is not
data-only cannot be rebased, so a lost race returns `COMMIT_CONFLICT` and
DuckLake's loop re-drives it.

The metadata manager reads the latest snapshot through a metadata cursor at the
transaction's pinned head, avoiding SQL planning and a scan of retained history.
It checks the served catalog version using the same metadata rows as the SQL
path, including scoped version options and the attach-pinned table shape. A
transaction with staged writes, or without a pinned head, retains DuckLake's
SQL path. Time-travel snapshot lookup continues through DuckLake. Inline writes
resolve the highest registered schema version through the same pinned registry
provider as the metadata table, without executing the aggregate lookup SQL.

On a table-statistics cache miss, the manager reads the table and column rows
through table-scoped metadata cursors at the same pinned read point, including
staged writes. DuckLake still converts the rows into optimizer statistics and
keys its cache by the file counter and schema version. A scoped response
caches only the requested table, including an absent result; other tables remain
uncached. Conflict-resolution statistics retain DuckLake's existing query.

Append-only commits without schema, deletion, or maintenance changes queue
DuckLake's calculated statistics for direct staging after the SQL batch. The
shim uses the existing metadata cell conversion and `moraine_tx_stage`,
preserving UPDATE's treatment of absent rows, nullable fields, exactness, and
serialized extra statistics. Schema changes, deletes, and maintenance retain
their SQL ordering. Rollback clears the queue, and retries calculate fresh
statistics. Snapshot change records use the same direct row contract while
DuckLake retains ownership of their grammar and commit information. An empty SQL
batch still flushes pending rows and commits the metadata transaction.

The metadata manager stages the finalized snapshot through `moraine_tx_stage`
instead of binding an INSERT. DuckLake calls `TryWriteSnapshot` after generating
changes, when the schema version and next-id counters are final. The timestamp
is the metadata transaction's start time, matching SQL `NOW()`. Other metadata
backends retain the snapshot INSERT through the default hook.

Inline writes queue references to the transaction-owned data and their resolved
table names. `FlushInlinedAppends` runs after the commit SQL successfully
creates the inline tables, but before committing the metadata connection. Every
chunk then goes through the Appender, including tables created by this commit.
The queue is consumed before appending and cleared on rollback; a retry
constructs it afresh. An append failure rolls back the snapshot, table
definitions, and all preceding appends together. The shim only translates rows;
validation, conflict detection, and publication remain in the core.

`moraine_tx_commit` returns the snapshot id the commit landed. After the loop
the shim reads the catalog's last committed snapshot and, if it exceeds the id
DuckLake recorded, calls `SetCommittedSnapshotId` so DuckLake adopts the
catalog's id.

DuckLake decides retryability by substring match on the lowercased error text.
Its retry substrings are `conflict`, `concurrent`, `unique`, and `primary key`.
The `COMMIT_CONFLICT` message always contains `conflict`.
Every terminal error, including `RETRY_EXHAUSTED`, `FENCED`, `CONSTRAINT`,
`COMMIT_OUTCOME_UNKNOWN`, the equality-index uniqueness rejection, and the
standalone data-scan redirect, contains none of the four. The
`COMMIT_OUTCOME_UNKNOWN` message is a fixed string for this reason, and the shim
adds the structured DuckDB error field `commit_outcome=unknown`, which the
bundled DuckLake patch reads to keep the commit's data and delete files through
rollback.

## Table functions

The shim registers these functions at load. Each takes a lake name or a store
path as its first argument, as noted.

| Function | Argument | Purpose | Owner |
|---|---|---|---|
| `moraine_create_checkpoint` | lake, optional lifetime | mints a SlateDB checkpoint through the attached writer | [commit protocol](0003-commit-protocol.md) |
| `moraine_checkpoints` | store path | lists checkpoints with `manifest_id`, `created_at`, `expires_at` | [commit protocol](0003-commit-protocol.md) |
| `moraine_delete_checkpoint` | store path, id | releases one checkpoint | [commit protocol](0003-commit-protocol.md) |
| `moraine_move_wal` | store path, `wal_path` | moves a lake's write-ahead log store, lake attached nowhere | [commit protocol](0003-commit-protocol.md) |
| `moraine_rewrite_data_path` | store path, old root, new root, optional `wal_path` | points a copied lake at its new data root, lake attached nowhere | this document |
| `moraine_migrate` | store path | runs a structural format migration, lake attached nowhere | [storage format](0001-storage-format.md) |
| `moraine_raise_format` | lake | raises the store's structural format version | [storage format](0001-storage-format.md) |
| `moraine_raise_catalog_version` | lake, `dry_run`, `confirm` | raises the DuckLake catalog format from 1.0 to 1.1 | [feature mapping](0005-ducklake-feature-mapping.md) |
| `moraine_maintenance` | lake | runs one maintenance pass | [maintenance](0010-maintenance.md) |
| `moraine_maintenance_status` | lake | lists persisted maintenance pass records and their steps | [maintenance](0010-maintenance.md) |
| `moraine_compact_store` | lake, options | merges a store subspace | [maintenance](0010-maintenance.md) |
| `moraine_store_census` | lake | per-subspace manifest bytes and object counts | [read path](0007-read-path-and-caching.md) |
| `moraine_cache_tally` | none or lake | cache hit and miss counters | [read path](0007-read-path-and-caching.md) |
| `moraine_cache_status` | none | process-wide cache capacity and occupancy | [read path](0007-read-path-and-caching.md) |
| `moraine_sidecar_tally` | none | row-summary counters | [read path](0007-read-path-and-caching.md) |
| `moraine_memory_tally` | lake | attributable live memory | [read path](0007-read-path-and-caching.md) |
| `moraine_object_store_tally` | lake | object-store request counts and durations | [read path](0007-read-path-and-caching.md) |
| `moraine_indexes` | catalog, schema, table | lists indexes | [indexes](0009-equality-indexes.md) |
| `moraine_index_lookup` | catalog, schema, table, index, keys | equality lookup | [indexes](0009-equality-indexes.md) |
| `moraine_index_in` | catalog, schema, table, index, list | multi-key lookup | [indexes](0009-equality-indexes.md) |
| `moraine_index_range` | catalog, schema, table, index, bounds | range lookup | [indexes](0009-equality-indexes.md) |
| `moraine_index_nulls` | catalog, schema, table, index, prefix | null-aware prefix lookup | [indexes](0009-equality-indexes.md) |
| `moraine_index_verify` | catalog, schema, table, index | verifies and certifies an index | [indexes](0009-equality-indexes.md) |
| `moraine_fault_stall_next_read` | milliseconds | arms a stall in the next cancellable read, for cancellation tests | this document |

`CREATE INDEX` and `DROP INDEX` on a moraine-backed table are intercepted by
`RegisterMoraineIndexDdl`. The scan operators `moraine_metadata_scan`,
`moraine_inline_data_scan`, `moraine_summary_scan`, and `moraine_scan` appear in
query plans and are bound to table entries. They are not callable by name.

## Relocating a copied lake

A copied lake retains its original global `data_path`, so the destination
refuses an attach that names the destination root. The rewrite changes that
root explicitly, after the operator has copied the data and stopped all access
to the destination catalog:

```sql
CALL moraine_rewrite_data_path(
    's3://destination/catalog',
    's3://source/data/',
    's3://destination/data/'
);
```

The function opens the catalog independently and closes it on either success
or failure. It requires autocommit and a detached catalog; opening the writer
fences any other writer, as the other offline commands do. An optional
`wal_path` names the catalog's current separate WAL store and resolves its own
credentials. Catalog credentials come from DuckDB secrets or the environment
fallback. Data credentials are unnecessary, because the command touches
metadata only.

A read-only open first rejects a missing catalog. Before opening a writer, the
command rejects a destination data root that overlaps the catalog or WAL
store, and rejects empty paths. The attach path continues to reject mismatched
roots. The core method is `Catalog::rewrite_data_path` in the
[public API](0002-public-api.md#catalog-level-operations).

The copy procedure:

1. Stop writers, compactors, garbage collectors, and maintenance before
   copying catalog objects. Close the source catalog cleanly and keep it
   quiescent throughout; copying a live bucket is not a backup.
2. For an independent copy, first use `moraine_move_wal` to bring any
   separate WAL into the source catalog store, then copy the catalog and data
   prefixes preserving relative object keys. Two independent copies must never
   share a separate WAL prefix.
3. Run the rewrite against the destination catalog, naming its old and new
   data roots, with destination readers and writers detached.
4. Attach the destination with the new `DATA_PATH` and `META_DATA_PATH`.

The command does not copy objects, rewrite absolute paths, change WAL
identity, or turn an arbitrary partial copy into a consistent catalog.

## Error codes

Every fallible entry point returns an `i32` and, on failure, fills a
caller-owned `MoraineError` with the code and a UTF-8 message that is freed only
through `moraine_error_free`. The codes are defined in
`crates/moraine-duckdb/src/error.rs`, and `ThrowMoraineError` in
`cpp/catalog.cpp` maps them to DuckDB exceptions.

| Code | Constant | Source | Shim raises |
|---|---|---|---|
| 0 | `OK` | success | none |
| 1 | `NOT_FOUND` | `Error::NotFound` | `CatalogException` |
| 2 | `ALREADY_EXISTS` | `Error::AlreadyExists` | `CatalogException` |
| 3 | `CONSTRAINT` | `Error::Constraint`, and a write on a read-only handle | `CatalogException` |
| 4 | `COMMIT_CONFLICT` | `Error::CommitConflict`, message contains `conflict` | `TransactionException` |
| 5 | `CORRUPTION` | `Error::Corruption`, or a catalog string with an embedded NUL | `IOException` |
| 6 | `STORE` | `Error::Store`, `Error::Configuration`, `Error::IndexBuilding`, and any other variant | `IOException` |
| 7 | `INVALID_ARGUMENT` | ABI validation: null pointer, invalid UTF-8, unsupported scheme | `InvalidInputException` |
| 8 | `INTERNAL` | a panic caught at the FFI boundary, fixed message | `InternalException` |
| 9 | `INTERRUPTED` | the call's probe cancelled it, or it outran the statement deadline | `InterruptException` |
| 10 | `RETRY_EXHAUSTED` | `Error::RetryBudgetExhausted`, terminal | `TransactionException` |
| 11 | `FENCED` | `Error::Fenced`, terminal | `IOException` |
| 12 | `MIGRATION` | `Error::Migration`, at attach the message names `moraine_migrate` | `IOException` |
| 13 | `SNAPSHOT_EXPIRED` | `Error::SnapshotExpired` | `CatalogException` |
| 14 | `UNSUPPORTED` | `Error::Unsupported` | `NotImplementedException` |
| 15 | `OPEN_RACED` | `Error::OpenRaced`, attaching again adopts the store | `IOException` |
| 16 | `COMMIT_OUTCOME_UNKNOWN` | `Error::CommitOutcomeUnknown`, or a commit cancelled after its first poll | `IOException` with `commit_outcome=unknown` |

Codes 4 and 10 both raise `TransactionException` because DuckLake's retry
decision reads the text, and the two texts differ in whether they carry a retry
substring.

## Runtime and cancellation

The core is async and owns no runtime. The ABI layer creates one multi-threaded
tokio runtime per attached catalog inside <!-- site:omit -->
`moraine_attach_with_commit_log`<!-- /site:omit the attach call -->, holds it in
`MoraineCatalogHandle`, and drops it in `moraine_detach` after closing the
catalog. The runtime is multi-threaded so SlateDB's flush and compaction tasks
progress while DuckDB threads are parked in catalog calls. Its worker count is
DuckDB's `threads` setting, passed as `host_threads` and clamped by
`worker_threads` to `MIN_WORKER_THREADS` of 2 and `MAX_WORKER_THREADS` of 8
(`crates/moraine-duckdb/src/runtime.rs`). The count is fixed for the attach's
life.

Each entry point runs on one of DuckDB's own threads, which is outside the
runtime, and calls `block_on` on the handle's runtime. Core code only awaits and
never blocks on a runtime, so a pure-Rust host drives the same core on its own
runtime. A catalog operation never calls back into DuckDB, so there is no
re-entrant FFI chain and no nested `block_on`.

DuckDB offers no interrupt callback to a storage
extension, only `ClientContext::IsInterrupted()`, an atomic load any thread may
perform. Every cancellable entry point therefore takes a trailing
`MoraineInterruptProbe probe, void *probe_ctx` pair, and the shim passes
`moraine_shim_is_interrupted` with the `ClientContext`. A null probe makes the
call non-cancellable. `block_on_cancellable_within` races the core future in a
`biased` `tokio::select!` against a loop that ticks the probe every
`INTERRUPT_POLL_INTERVAL` of 100 ms, with the probe branch first so a standing
interrupt wins a tie. A third branch, `STATEMENT_DEADLINE` of one hour, reports
`INTERRUPTED` for a call that outruns it. The probe is per call, so one
connection's interrupt cannot cancel another's query on the same handle. The
signal is level-triggered and nothing consumes it.

DuckLake runs its metadata SQL on a connection it creates and never hands out,
so that context's flag cannot be set by a user. `MoraineMetadataManager::Query`
and `AttachMetadata` record the driving context on that connection as
`ParentContextState`, and `moraine_shim_is_interrupted` reads the driver's flag
when the connection's own is clear.

A cancelled read returns `INTERRUPTED` and the handle stays usable. An attach is
cancellable as well, in <!-- site:omit -->
`moraine_attach_with_commit_log`<!-- /site:omit the ABI -->: a cancelled attach
leaves nothing attached but may have taken the writer epoch. Its abandoned
runtime is wound down with a `CANCELLED_ATTACH_SHUTDOWN` deadline of 5 seconds.
`moraine_detach` takes no probe. A commit cancelled before its future is first
polled returns `INTERRUPTED` and submitted nothing. A commit cancelled after
that returns `COMMIT_OUTCOME_UNKNOWN` while any submitted write continues on its
own task. The cancel-safety contract, including the shielded batch write and
what a caller does with an unknown outcome, is specified in
[the commit protocol](0003-commit-protocol.md).

Every entry point wraps its body, `block_on` included, in `catch_unwind` through
`guard` in `crates/moraine-duckdb/src/abi.rs`. A caught panic becomes `INTERNAL`
with a fixed message that never derives from the panic payload, so no unwind
crosses the C boundary.

The extension ships one fault seam for proving cancellation:
`moraine_fault_stall_next_read(milliseconds)` arms a sleep that the next
cancellable call awaits inside its `select!`, in place of a slow store. One
arming stalls one call, and only the probe hop transfers it.

## Logging

The core emits `tracing` events, and the extension is a separately loaded
library with its own `tracing`, so a host subscriber never sees them. The first
attach installs a process-wide buffering subscriber through `try_init`, and a
host that already installed one keeps it. `MORAINE_LOG` sets the captured level
for moraine's own targets, default `info`, and `MORAINE_LOG_DEPENDENCIES` does
the same for dependencies. The buffer holds `LOG_BUFFER_CAPACITY` of 512
records, evicts oldest-first, and reports the drop count on the next drain.
Delivery is per handle: each attach's runtime tags its worker threads and each
`block_on` tags the calling thread, and an event is attributed to the handle
whose thread it fired on. The catalog registers a database-scoped writer as its
handle's push sink through `moraine_register_log_sink` and unregisters it in its
destructor through `moraine_unregister_log_sink`, so records appear under the
`moraine` type in `duckdb_logs` as they fire. Events with no routable sink
buffer, and the shim drains them through `moraine_drain_logs` at attach exit,
transaction start and commit, the end of each maintenance pass, and after each
table function that opens a store. Events are emitted per operation, never per
row or per file.

## Pins

| What | Pinned at |
|---|---|
| DuckDB | `v2.0.0`, the version string the loadable is bound to |
| DuckLake extension | commit `417a9117`, hard-coded by DuckDB in `.github/config/extensions/ducklake.cmake` |
| DuckLake branch | `main` |
| DuckLake catalog format | 1.0 by default, 1.1 on a raised store |

## Tests

- Two threads blocked in catalog calls on one handle both complete while
  SlateDB's background flush advances.
- An interrupt before a commit future is polled returns `INTERRUPTED` and leaves
  the head unchanged.
- A forced panic in a core call surfaces as `InternalException` and the handle
  serves the next call.
- A read-only attach opened beside a live writer does not fence it.
- The `COMMIT_CONFLICT` message contains `conflict` and every terminal code's
  message contains no retry substring.
- The nested attach text, DuckLake's `ducklake_*` access set, and the bundled
  DuckLake commit match the pins.

## Alternatives

- **Intercepting DuckLake's SQL text.** Parsing and answering the SQL DuckLake
  emits would re-implement a query engine. DuckDB's executor already plans those
  statements and calls moraine per table.
- **One runtime per process.** A shared runtime would couple every attached
  catalog's teardown and background tasks and would need a worker count serving
  an unknown number of catalogs. One runtime per attach keeps `DETACH` local.

## Background maintenance ownership

The core owns bounded warming, committed-file publication, and full-backfill
jobs. Each has its own progress state and independent admission lane. Only
unstarted warming jobs coalesce; publication retains every registered-file
batch. Failed attempts rotate behind pending work with bounded backoff, keeping
their acknowledged frontiers. Cancellation drops active attempts and releases
admission; dropping the queue releases pending reservations and read scopes.

The extension owns the runtime task: create `BackgroundWork`, forward
`BackgroundRequest::attach` and `BackgroundRequest::commit`, then abort and
join the task before closing the catalog. It contains no job selection,
coalescing, retry, or publication policy.
