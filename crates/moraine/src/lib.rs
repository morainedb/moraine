//! Moraine brings a [SlateDB](https://slatedb.io) backend to
//! [DuckLake](https://ducklake.select): a DuckLake catalog implemented on a
//! transactional KV store over object storage, instead of the usual
//! relational catalog database.
//!
//! Every commit races a conditional put against an object-storage commit
//! log, so any number of processes may open a [`Catalog`] read-write and
//! commit concurrently — the bucket is the only thing coordinating them.
//! The one fenced SlateDB writer belongs to the **folder** role, not the
//! commit path: it tails the log and applies each won commit into SlateDB
//! as a derived index, so a dead folder cannot lose a commit, only lengthen
//! the tail a reader replays past. Folding is host-driven — this crate
//! spawns no threads — through [`Catalog::fold_sprint`] and
//! [`Catalog::fold_if_stalled`]. A large fleet of readers should open
//! against one shared, existing checkpoint id rather than each minting its
//! own — traffic hygiene, not correctness. Schemas, tables, views, data
//! files, and statistics all commit through the same transaction; catalog
//! options live outside the snapshot protocol (last-write-wins, no
//! snapshot minted).
//!
//! # A worked example
//!
//! Open a catalog on a store, evolve its schema through commits, and read
//! both the current state and any past snapshot:
//!
//! ```
//! # use std::sync::Arc;
//! # use moraine::{Catalog, CatalogOptions, ColumnDef};
//! # use object_store::memory::InMemory;
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
//!
//! let v1 = catalog
//!     .commit(|tx| {
//!         let sales = tx.create_schema("sales")?;
//!         tx.create_table(
//!             sales,
//!             "orders",
//!             &[ColumnDef {
//!                 name: "id".into(),
//!                 column_type: "BIGINT".into(),
//!                 nulls_allowed: false,
//!                 default_value: None,
//!                 children: Vec::new(),
//!             }],
//!         )?;
//!         Ok(())
//!     })
//!     .await?;
//!
//! // The head sees the latest shape (plus the bootstrap-minted `main`)...
//! let head = catalog.snapshot().await?;
//! assert_eq!(head.schemas().len(), 2);
//!
//! // ...while `v1` remains queryable by time travel, forever.
//! let past = catalog.snapshot_at(v1).await?;
//! assert_eq!(past.schemas().len(), 2);
//! # Ok::<(), moraine::Error>(()) }).unwrap();
//! ```
//!
//! See `examples/walkthrough.rs` for a longer tour that also alters and
//! renames a table across a second commit. Data files register through the
//! same [`Catalog::commit`] path, minting a snapshot without bumping the
//! schema version.
//!
//! # Inlined data
//!
//! A small insert need not cost a Parquet file. [`Transaction::inline_insert`]
//! stores a chunk of rows in the catalog itself — Arrow IPC bytes the caller
//! encodes, carried by the same single write the commit already performs —
//! and the rows draw ids from the table's row-id counter exactly as a
//! registered file's do. [`ReadOnlyCatalog::recent_rows`] reads them back — or
//! [`ReadOnlyCatalog::recent_rows_at`], for the rows a past snapshot saw —
//! [`Transaction::inline_delete`] tombstones one, and
//! [`Transaction::flush_inlined_data`] drains them into a data file the
//! caller wrote, preserving their ids and backdating the file's record so
//! time travel across the flush still finds them exactly once.
//!
//! # Readers
//!
//! [`Catalog::open_read_only`] opens the same store without becoming its
//! writer, so it never fences one. What it hands back is a
//! [`ReadOnlyCatalog`], which carries the reads and no mutator at all — so
//! committing through a read-only handle is a compile error rather than a
//! runtime one, and a [`Catalog`] serves the same reads by dereferencing to
//! it. Read-only comes in two forms, and the difference is what the
//! reader's credentials must be allowed to do.
//!
//! The default follows the latest state: it sees every commit as it lands,
//! and pays for that by writing a checkpoint into the manifest on open and
//! refreshing it while it lives. The other form pins the reader to a
//! checkpoint minted in advance, and writes nothing whatsoever — for a
//! deployment whose readers hold strictly read-only credentials:
//!
//! ```
//! # use std::sync::Arc;
//! # use moraine::{Catalog, CatalogOptions};
//! # use object_store::memory::InMemory;
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! # let object_store = Arc::new(InMemory::new());
//! # let catalog = Catalog::open(object_store.clone(), CatalogOptions::default()).await?;
//! catalog.commit(|tx| tx.create_schema("sales").map(|_| ())).await?;
//! let checkpoint = catalog.create_checkpoint(None).await?;
//!
//! let mut options = CatalogOptions::default();
//! options.checkpoint = Some(checkpoint);
//! let reader = Catalog::open_read_only(object_store, options).await?;
//! assert!(reader.snapshot().await?.schema_by_name("sales").is_some());
//! # Ok::<(), moraine::Error>(()) }).unwrap();
//! ```
//!
//! A pinned reader reads that cut and no other, however long it stays open
//! — a commit made after the checkpoint is invisible to it. The checkpoint
//! also pins the objects it references against SlateDB's own collection
//! until it expires or [`Catalog::delete_checkpoint`] removes it, so give
//! one a lifetime unless something else will.
//!
//! # Maintenance
//!
//! Dropping an equality index (or a table that has one) makes its entries
//! invisible immediately but does not delete them, so the range would leak
//! without a sweep. [`Catalog::maintain`] reclaims those ranges:
//!
//! ```
//! # use std::sync::Arc;
//! # use moraine::{Catalog, CatalogOptions, MaintenanceRequest};
//! # use object_store::memory::InMemory;
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
//!
//! let report = catalog.maintain(MaintenanceRequest::default()).await?;
//! // Nothing has been dropped, so there is nothing to reclaim.
//! assert_eq!(report.indexes_swept, 0);
//! # Ok::<(), moraine::Error>(()) }).unwrap();
//! ```
//!
//! A pass mints no snapshot and leaves head unchanged, and deletes in
//! bounded batches (`MaintenanceRequest::batch_size`) so a large
//! reclamation never holds the writer. It is safe to interrupt: an
//! abandoned pass leaves a smaller one for next time, never a torn one.
//! Deciding what is dead rests on catalog ids never being reused, so a
//! sweep can run alongside a live writer without coordinating with it.
//!
//! Beneath the catalog, the store itself accumulates. Every key lives in
//! a subspace, each subspace is its own tree, and overwriting a key leaves
//! the old version readable-through until a merge rewrites the SSTs
//! holding it — which the substrate's own scheduler only does under write
//! pressure, so a store that goes quiet keeps its dead weight
//! indefinitely. [`ReadOnlyCatalog::store_census`] says where the weight is,
//! from the store's manifest alone:
//!
//! ```
//! # use std::sync::Arc;
//! # use moraine::{Catalog, CatalogOptions, CensusRequest, CompactStoreRequest, SubspaceName};
//! # use object_store::memory::InMemory;
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
//! catalog.commit(|tx| tx.create_schema("sales").map(|_| ())).await?;
//!
//! let census = catalog.store_census(CensusRequest::default()).await?;
//! // Every subspace is reported, so two censuses compare row by row.
//! assert!(census.subspaces.iter().any(|s| s.subspace == SubspaceName::Current));
//!
//! // ...and the merge collapses a subspace's sorted runs into one.
//! let report = catalog.compact_store(CompactStoreRequest::default()).await?;
//! assert!(!report.merges.is_empty());
//! # Ok::<(), moraine::Error>(()) }).unwrap();
//! ```
//!
//! The census costs a manifest read plus one listing and is available
//! read-only; its per-subspace figures count SSTs that have been written
//! out, so a commit still in the write-ahead log is in none of them. The
//! listing is what catches the rest: [`StoreCensus::unaccounted_bytes`]
//! reports what the object store holds outside those SSTs, and a large
//! figure there means a slow attach no merge will fix — an unpinned reader
//! replays the log before it materializes anything. Ask for
//! `CensusRequest::count_live_entries` and it also scans, which costs a
//! full read of the store and is the only way to learn what fraction of a
//! subspace is live. [`Catalog::compact_store`] then merges: moraine picks
//! no sources and no destination, and the plan the substrate makes for a
//! whole tree is what permits dropping a tombstone rather than carrying it
//! forward. It needs the writer, since the compactor that executes a merge
//! runs inside it.
//!
//! Snapshot expiry, file cleanup, and data-file compaction all belong to
//! DuckLake and run through its own functions; the SlateDB store collects
//! its superseded objects itself, unprompted. A host embedding this crate
//! calls these verbs on whatever cadence it likes — the crate spawns no
//! threads and schedules nothing. Under the DuckDB extension, all of it is
//! sequenced for you; see that crate's docs for `moraine_maintenance`,
//! `moraine_store_census`, and `moraine_compact_store`.
//!
//! Folding the commit log into SlateDB is the same kind of host-driven
//! work: nothing here opens the fenced writer unasked, so an embedder
//! chooses when to fold and how — one bounded pass
//! ([`Catalog::fold_sprint`]) or a standing appointment
//! ([`Catalog::fold_if_stalled`]) that stands down the moment a peer is
//! already folding.
//!
//! # Format migration
//!
//! A store records the structural layout version it was written at. A binary
//! opens any version it understands and refuses one it does not, rather than
//! misreading it. Most version bumps are *additive* — a new subspace, no
//! existing key moved — and need no rewrite: the store is stamped forward
//! the first time it uses the newer feature, and older stores stay readable.
//!
//! A bump that *moves* existing keys does need a rewrite, and
//! [`Catalog::migrate`] performs it: a one-way, crash-resumable pass that
//! takes the single writer, walks the keyspace under a durable cursor, and
//! flips the version stamp in one final atomic batch. It is a separate verb
//! from opening a catalog, so the operator chooses when to pay for it. While
//! it runs, every reader refuses the store.
//!
//! No such rewrite exists yet, so `migrate` reports a no-op against every
//! store this release can open.
//!
//! # Features
//!
//! - `fault-injection` — compiles the crash-injection seams the migration
//!   driver consults between its durable batches, and exposes `CrashPoint`,
//!   `inject_crash`, `SyntheticMigration`, `install_migration`, and `CrashCase`
//!   so a test can install a migration unit and stop it at a named durable
//!   boundary. Off by default; a build without it carries no fault surface.
//! - `fuzzing` — exposes the codec and read-path decode entry points the
//!   `fuzz/` targets drive. They live in their own crate and so cannot reach
//!   the crate-private codecs; nothing else needs this. Off by default.
//! - `leader` (off by default) — the advisory leader role: a long-lived folder
//!   opens a network port and becomes a group-commit funnel for forwarded
//!   sessions, announcing itself through the commit log. Additive: nothing in
//!   the direct or folder commit path depends on it, and with the feature off
//!   the whole module is absent and the fleet is purely direct.
//!
//! # Diagnostics
//!
//! The crate emits [`tracing`](https://docs.rs/tracing) events and installs
//! no subscriber. Rare, consequential moments log at `info` (an open, the
//! once-ever bootstrap, a format-stamp upgrade) or `warn` (a spent retry
//! budget, an oversized commit refused); per-commit summaries and retry
//! traces log at `debug`. Every event is per-operation — never per row,
//! entry, or file. An embedding host sees them by installing any
//! subscriber. The DuckDB extension consumes its own — it cannot share the
//! host's — and forwards them to `duckdb_logs`; see that crate's `logging`
//! module.
//!
//! # Layering
//!
//! - `catalog` — the DuckLake domain model. Never touches SlateDB directly.
//! - `store` — the SlateDB layer: key layout and value codecs. Knows nothing
//!   about DuckLake semantics.
//! - `transaction` — the commit protocol turning a catalog transaction into an
//!   atomic store write.

#![forbid(unsafe_code)]

mod catalog;
mod data_file;
mod error;
mod fault;
#[doc(hidden)]
pub mod ffi_support;
#[cfg(feature = "leader")]
mod leader;
mod store;
mod telemetry;
mod transaction;

pub use catalog::{
    BuildStep, CachePreload, Catalog, CatalogOptions, CatalogSnapshot, CensusRequest,
    ColumnAlteration, ColumnDef, ColumnId, ColumnInfo, ColumnOrder, ColumnStats,
    CompactStoreReport, CompactStoreRequest, CompactionTarget, Contention, DataFile, DataFileId,
    DataFileInfo, DeleteFile, DeleteFileId, DeleteFileInfo, FileColumnStats, FileIndexEntry,
    FileIndexRemoval, FileRowCandidate, FlushedDataFile, IndexDef, IndexEntry, IndexId, IndexInfo,
    IndexMaintenance, IndexState, InlineChunk, LiveCount, MacroId, MacroImplementationDef,
    MacroInfo, MacroParameterDef, MaintenanceReport, MaintenanceRequest, MaintenanceStatusPass,
    MaintenanceStatusStep, MappingId, MappingInfo, MergeOutcome, MigrationRequest, NameMappingDef,
    OptionScope, PartitionColumnDef, PartitionId, PartitionSpec, ReadOnlyCatalog, RecentRow,
    RowSummaryWarmth, ScheduledDeletion, SchemaId, SchemaInfo, SnapshotId, SnapshotInfo, SortId,
    SortKeyDef, SortSpec, StoreCensus, StoreObjects, SubspaceCensus, SubspaceMerge, SubspaceName,
    TableId, TableInfo, TableStats, TagEntry, TagTarget, Timestamp, ViewId, ViewInfo,
};
pub use data_file::DataStore;
pub use error::{Error, Result};
/// Fault injection for the migration driver. Unstable and not part of the
/// semver contract.
#[cfg(feature = "fault-injection")]
#[doc(hidden)]
pub use fault::{
    CrashCase, CrashPoint, SyntheticMigration, inject_crash, install_migration, stamp_base_format,
};
/// Decode entry points for the out-of-crate fuzz targets. Unstable and not
/// part of the semver contract.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzz;
#[cfg(feature = "leader")]
pub use leader::{Leader, LeaderConfig, LeaderStats};
pub use moraine_wal::FoldReport;
pub use store::{
    cache::{
        CacheStatus, CacheTally, ObjectStoreTally, RowSummaryOccupancy, cache_status, cache_tally,
    },
    index_encoding::{Direction, IndexKeyValue, IntWidth, NullOrder},
};
pub use transaction::{MigrationReport, Transaction};

/// The newest structural store format this binary reads and writes. A store
/// bootstrapped here carries it, so `migrate` finds nothing to rewrite.
pub const MAX_FORMAT_VERSION: u64 = transaction::commit::MAX_FORMAT_VERSION;
