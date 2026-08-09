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
//! # Maintenance and measurement
//!
//! `CALL moraine_maintenance('lake')` runs one pass of the configured
//! maintenance sequence — DuckLake's own expiry, flush, compaction, and
//! cleanup functions, then moraine's orphaned-index sweep, then the store
//! merge — and returns a row per step. Every step but the sweep is opt-in
//! at `ATTACH`, through `META_MAINTENANCE_*` options; an interval starts a
//! timer thread that runs the same pass unattended, and
//! `moraine_maintenance_status` serves the last 16 passes from the catalog,
//! including after a process restart and from a read-only attach, so an
//! unattended failure stays visible. `moraine_compact_store` runs the merge
//! alone, for a store that needs it once rather than on a cadence.
//!
//! `CALL moraine_store_census('lake')` measures the store itself rather
//! than the lake: one row per keyspace subspace, carrying physical bytes,
//! SST counts, and sorted-run counts read from the store's manifest. It
//! costs two object reads however large the store is, mutates nothing, and
//! works on a `READ_ONLY` attach — the shape an operator investigating a
//! production store has. `live := true` adds a scan that counts live keys,
//! which costs a full read of the store; without it the live columns are
//! NULL rather than zero.
//!
//! ```sql
//! -- Is a merge even the right lever? If store_sst_bytes is far below
//! -- store_total_bytes the weight is in the write-ahead log, which an
//! -- unpinned read attach replays and no merge reclaims.
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
//! -- ...or on a cadence, as step 8 of the maintenance pass.
//! ATTACH 'ducklake:moraine:s3://bucket/catalog' AS lake (
//!   DATA_PATH 's3://bucket/data',
//!   META_MAINTENANCE_INTERVAL INTERVAL 1 HOUR,
//!   META_MAINTENANCE_COMPACT_STORE true
//! );
//! ```
//!
//! # Diagnostics
//!
//! The core emits `tracing` events; this crate consumes them and forwards
//! them to DuckDB's logger, so they appear in `duckdb_logs` under the
//! `moraine` type. It cannot rely on the host for this — the extension is a
//! separate dynamically-loaded library with its own statically-linked
//! `tracing`, so a subscriber installed by an embedding process never sees
//! them. `MORAINE_LOG` sets the captured level (default `info`). See
//! [`logging`] for the buffering and drain mechanics.
//!
//! `enable_logging` is a table function, and its default storage writes to
//! stdout; ask for `memory` to query the records back.
//!
//! ```sql
//! CALL enable_logging(level => 'info', storage => 'memory');
//! -- run the workload, then:
//! SELECT log_level, message FROM duckdb_logs WHERE type = 'moraine';
//! ```
//!
//! # Maintenance
//!
//! A read-write attach can carry a maintenance schedule. When
//! `META_MAINTENANCE_INTERVAL` is given, the shim runs a thread that
//! performs a pass on that cadence: DuckLake's own maintenance functions
//! in a fixed order, then moraine's orphaned-index-entry sweep. Without an
//! interval no thread starts, and a read-only attach never schedules.
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
//! **Give `older_than` an interval, not a timestamp.** Attach options are
//! evaluated once, so `now()` freezes into a literal and a schedule would
//! keep expiring against its attach-time instant forever — retention
//! quietly stopping as the lake moves on. An interval is rendered as a
//! rolling window that DuckLake evaluates on each pass. A timestamp is
//! still accepted, and is what you want for a one-off
//! `moraine_maintenance` call.
//!
//! Every step that mutates the lake is opt-in, so an attach naming none of
//! them reclaims only orphaned index entries — which no query can observe.
//! Step options derive from DuckLake's own names:
//! `META_MAINTENANCE_<function minus its `ducklake_` prefix>` enables a
//! step with DuckLake's defaults, and appending `_<parameter>` passes one
//! through unaltered. The steps run in this order: `expire_snapshots`,
//! `flush_inlined_data`, `merge_adjacent_files`, `rewrite_data_files`,
//! `cleanup_old_files`, `delete_orphaned_files`, then the sweep. A failed
//! step abandons the rest of the DuckLake sequence — those steps depend on
//! each other — but never the sweep, which depends on none of them.
//! `META_MAINTENANCE_SWEEP_INDEXES false` disables the sweep and
//! `META_MAINTENANCE_BATCH_SIZE` bounds its deletes per commit.
//!
//! Two table functions serve the same pass. `moraine_maintenance('lake')`
//! runs one immediately and returns `(step, status, detail)`;
//! `moraine_maintenance_status('lake')` reports the last 16 durable passes
//! without running anything, adding `started_at` and whether each was
//! `scheduled` or `manual`. Both accept either the lake name or the name of its
//! metadata catalog. The trigger refuses inside an explicit transaction —
//! it blocks while a second connection writes the catalog — and refuses on
//! a read-only attach.
//!
//! One passthrough limit is worth knowing: DuckLake rejects an attach
//! carrying a list-valued `META_` option, so `expire_snapshots`' `versions`
//! is spelled as a string, `META_MAINTENANCE_EXPIRE_SNAPSHOTS_VERSIONS
//! '[1,2]'`.

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
