//! DuckDB extension for [moraine]: attaches a moraine store as a DuckDB
//! catalog. Three layers, thin by policy — no DuckLake domain logic lives
//! outside the core crate:
//!
//! 1. a **C++ shim** (`cpp/*.cpp`, compiled by the DuckDB extension toolchain)
//!    links DuckDB's internal C++ API and registers a `StorageExtension`;
//! 2. a **C ABI** (the [`abi`] module, mirrored by hand in `cpp/moraine_abi.h`)
//!    marshals calls across the language boundary and owns the sync↔async
//!    bridge — one tokio runtime per attached catalog, `block_on` at every
//!    entry point, `catch_unwind` so a core panic surfaces as an error code,
//!    never an unwind into C++;
//! 3. the async [moraine] core, unaware any of this exists.
//!
//! **Primary path:** `ATTACH 'ducklake:moraine:<store>' AS lake (DATA_PATH
//! '<data-path>')` — DuckLake drives moraine as its own metadata catalog.
//! `CREATE`/`INSERT`/`UPDATE`/`DELETE`/`DROP`/rename against `lake.*`
//! translate to staged row mutations (the [`staged`] module) committed as
//! one atomic batch; reads (`SELECT`, time travel, `ducklake_snapshots()`)
//! go through DuckLake's own reader over the `ducklake_*` rows this crate
//! projects (the [`dumps`] module). See `README.md`'s "Serving as
//! DuckLake's metadata catalog" section.
//!
//! **Secondary path — metadata-only inspection:** `ATTACH '<path>' AS m
//! (TYPE moraine)`, or the bare `moraine:<path>` prefix (the same form
//! DuckLake's nested attach uses internally). Schema/table/view listing
//! (`duckdb_databases()`, `duckdb_tables()`, `duckdb_views()`,
//! `duckdb_columns()`), `DESCRIBE`, and every `ducklake_*` metadata table
//! work through this attach. User-table *data* does not: a `SELECT`
//! against a real user table binds normally (so `DESCRIBE`/`EXPLAIN` still
//! work) but raises `InvalidInputException` at execution time, naming the
//! `ducklake:moraine:` attach to use instead. See `README.md`'s
//! "User-table data" section.
//!
//! **Not implemented, throws `NotImplementedException`:** DDL issued
//! directly against a user schema/table (`CREATE`/`DROP`/`ALTER` outside
//! DuckLake's own `ducklake_*` writes) and querying a view's definition
//! (no SQL parser vendored).
//!
//! **Single writer.** A read-write attach opens a [`moraine::Catalog`]
//! over the writer, so only one process may hold a given store attached
//! that way; a second read-write attach fences the first's writer rather
//! than failing itself. `READ_ONLY` opens a reader instead, which never
//! fences the live writer, so any number may attach alongside it. See
//! `README.md` for the pinned build shape.
//!
//! # Maintenance
//!
//! `CALL moraine_maintenance('lake')` runs one pass of the configured
//! maintenance sequence and returns `(step, status, detail)`;
//! `moraine_maintenance_status('lake')` reports the last 16 durable passes
//! without running anything, adding `started_at` and whether each was
//! `scheduled` or `manual` (including after a process restart and from a
//! read-only attach). Both accept either the lake name or the name of its
//! metadata catalog. The trigger refuses inside an explicit transaction and
//! on a read-only attach.
//!
//! When `META_MAINTENANCE_INTERVAL` is given, a read-write attach runs a
//! thread that performs the same pass on that cadence. Without an interval
//! no thread starts, and a read-only attach never schedules.
//!
//! ```sql
//! ATTACH 'ducklake:moraine:/lake/catalog' AS lake (
//!     DATA_PATH '/lake/data', META_DATA_PATH '/lake/data',
//!     META_MAINTENANCE_INTERVAL INTERVAL '1 hour',
//!     META_MAINTENANCE_EXPIRE_SNAPSHOTS_OLDER_THAN INTERVAL '7 days',
//!     META_MAINTENANCE_MERGE_ADJACENT_FILES true,
//!     META_MAINTENANCE_CLEANUP_OLD_FILES_OLDER_THAN INTERVAL '1 hour');
//! ```
//!
//! Give a scheduled `older_than` an interval, not a timestamp: attach
//! options are evaluated once, so a timestamp freezes at attach time. An
//! interval is a rolling window evaluated on each pass. A timestamp is
//! what a one-off `moraine_maintenance` call wants.
//!
//! Every step that mutates the lake is opt-in. Step options derive from
//! DuckLake's own names: `META_MAINTENANCE_<function minus its `ducklake_`
//! prefix>` enables a step with DuckLake's defaults, and appending
//! `_<parameter>` passes one through unaltered. The steps run in this
//! order: `expire_snapshots`, `flush_inlined_data`, `merge_adjacent_files`,
//! `rewrite_data_files`, `cleanup_old_files`, `delete_orphaned_files`,
//! then moraine's orphaned-index sweep, then the store merge
//! (`META_MAINTENANCE_COMPACT_STORE`). A failed step abandons the rest of
//! the DuckLake sequence but never the sweep.
//! `META_MAINTENANCE_SWEEP_INDEXES false` disables the sweep and
//! `META_MAINTENANCE_BATCH_SIZE` bounds its deletes per commit. DuckLake
//! rejects a list-valued `META_` option, so `expire_snapshots`' `versions`
//! is spelled as a string: `META_MAINTENANCE_EXPIRE_SNAPSHOTS_VERSIONS
//! '[1,2]'`.
//!
//! # Measurement
//!
//! `CALL moraine_store_census('lake')` measures the store itself: one row
//! per keyspace subspace, carrying physical bytes, SST counts, and
//! sorted-run counts read from the store's manifest. It costs two object
//! reads, mutates nothing, and works on a `READ_ONLY` attach. `live :=
//! true` adds a scan that counts live keys, which costs a full read of the
//! store; without it the live columns are NULL. `moraine_compact_store`
//! runs the store merge alone.
//!
//! ```sql
//! -- If store_sst_bytes is far below store_total_bytes the weight is in
//! -- the write-ahead log, which no merge reclaims.
//! SELECT any_value(store_total_bytes), any_value(store_sst_bytes),
//!        any_value(store_wal_bytes)
//! FROM moraine_store_census('lake');
//!
//! -- Where is the weight, and how much of it is dead?
//! SELECT subspace, bytes, sorted_runs FROM moraine_store_census('lake')
//! ORDER BY bytes DESC;
//!
//! -- Reclaim it, once, without re-attaching.
//! SELECT * FROM moraine_compact_store('lake', timeout := INTERVAL 10 MINUTES);
//!
//! -- ...or on a cadence, as the last step of the maintenance pass.
//! ATTACH 'ducklake:moraine:s3://bucket/catalog' AS lake (
//!   DATA_PATH 's3://bucket/data',
//!   META_MAINTENANCE_INTERVAL INTERVAL 1 HOUR,
//!   META_MAINTENANCE_COMPACT_STORE true
//! );
//! ```
//!
//! # Store format
//!
//! A store's format rises on its own, stamped by the commit that first
//! writes a record shape needing it — a live index, an inline chunk
//! locator, a collapsed inline schema. Each step is one-way: a store that
//! has taken a format can no longer be opened by a binary that predates
//! it, and a reader already attached on such a binary is not refused, so
//! it keeps running and may misread later commits. Upgrade every reader
//! of a store before its writers start producing shapes the readers do
//! not know.
//!
//! `moraine_raise_format` takes the newest purely additive format
//! deliberately, for an operator who would rather not wait for the first
//! commit that needs it, and reports the move.
//!
//! ```sql
//! -- What format is this store, and where would a raise take it?
//! SELECT from_format, to_format
//! FROM moraine_raise_format('lake', dry_run := true);
//!
//! -- Take it, once every reader is on a binary that understands it.
//! SELECT from_format, to_format FROM moraine_raise_format('lake');
//! ```
//!
//! `dry_run := true` reports the move without making it — the only way
//! to read a store's format without moving it, and the way to check
//! where a store sits before an upgrade.
//!
//! It is distinct from `moraine_migrate`, which rewrites a keyspace to
//! make a store readable by this binary at all, and it stops below any
//! format such a rewrite targets.
//!
//! # Diagnostics
//!
//! The core emits `tracing` events; this crate forwards them to DuckDB's
//! logger, so they appear in `duckdb_logs` under the `moraine` type.
//! `MORAINE_LOG` sets the captured level (default `info`). See [`logging`]
//! for the buffering and drain mechanics.
//!
//! `enable_logging` is a table function, and its default storage writes to
//! stdout; ask for `memory` to query the records back.
//!
//! ```sql
//! CALL enable_logging(level => 'info', storage => 'memory');
//! -- run the workload, then:
//! SELECT log_level, message FROM duckdb_logs WHERE type = 'moraine';
//! ```

#![deny(unsafe_op_in_unsafe_fn)]

pub mod abi;
pub mod arrow_ipc;
pub mod dumps;
pub mod error;
pub mod inline;
pub mod logging;
pub mod runtime;
pub mod staged;
#[cfg(test)]
mod test_support;
