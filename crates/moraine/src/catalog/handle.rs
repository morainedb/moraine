//! The catalog handle: the entry point a host opens, reads, and commits
//! through.

mod backfill;
mod index_build;
mod index_lookup;
mod inline_scan;
mod maintenance;
mod row_location;
mod table_warm;
#[cfg(test)]
mod tests;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub use maintenance::{
    MaintenanceReport, MaintenanceRequest, MaintenanceStatusPass, MaintenanceStatusStep,
};
use object_store::ObjectStore;
pub use row_location::RowSummaryWarmth;
use slatedb::{CloseReason, Db, DbReader, DbStatus, DbTransaction, IsolationLevel};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::{
    catalog::{
        CatalogSnapshot, RecentRow, SnapshotId, TableId,
        projection::{
            ProjectionCache, cache_epoch, cached_head_view, held_head_view, install_head_view_at,
            install_shared_current_entities, shared_current_entities,
        },
    },
    data_file,
    error::{Error, Result},
    store::{
        cache::{CacheCounters, CacheTally, ObjectStoreTally, cache_status, metadata_shortfall},
        census,
        handle::{ReadHandle, ReadSession},
        open::{self, StoreBuilder},
        proto::HeadValue,
    },
    transaction::{MigrationReport, Transaction, commit, migration},
};

/// Tables warmed concurrently, sized for a remote object store.
pub(crate) const WARM_TABLE_CONCURRENCY: usize = 32;

/// Files whose row-id summaries are read concurrently, sized for a remote
/// object store.
pub(crate) const SUMMARY_READ_CONCURRENCY: usize = 64;

/// Immutable Parquet files decoded concurrently by one scoped operation.
pub(crate) const BACKFILL_FILE_READ_CONCURRENCY: usize = 8;

/// How a [`Catalog::migrate`] call should run.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct MigrationRequest {
    /// Take a whole-store checkpoint before the first rewrite and release it
    /// once the last finish batch is durable, leaving a manual recovery point
    /// if the migration fails partway. Off by default.
    pub checkpoint: bool,
}

/// How a handle has served its reads, for the diagnostics a slow attach
/// needs.
#[derive(Debug, Default)]
struct ReadTally {
    materializations: AtomicU64,
    refreshes: AtomicU64,
    cache_hits: AtomicU64,
    head_reads: AtomicU64,
    materialize_micros: AtomicU64,
    current_scans: AtomicU64,
    history_scans: AtomicU64,
}

impl ReadTally {
    /// Records a full rebuild and reports it, with the running totals that
    /// make one attach's behaviour legible in a log.
    fn materialized(&self, elapsed: Duration) {
        let count = self.materializations.fetch_add(1, Ordering::Relaxed) + 1;
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let total = self.materialize_micros.fetch_add(micros, Ordering::Relaxed) + micros;
        info!(
            elapsed_ms = crate::telemetry::milliseconds(elapsed),
            materializations = count,
            total_ms = crate::telemetry::milliseconds(Duration::from_micros(total)),
            cache_hits = self.cache_hits.load(Ordering::Relaxed),
            refreshes = self.refreshes.load(Ordering::Relaxed),
            head_reads = self.head_reads.load(Ordering::Relaxed),
            "materialized the catalog from `current`"
        );
    }
}

/// Where a store lives, for the admin-surface verbs that address it by
/// path rather than through the open handle.
struct StoreLocation {
    path: String,
    object_store: Arc<dyn ObjectStore>,
}

/// The open store behind a catalog: the read-write `Db` writer, or a
/// read-only `DbReader`. A read-only catalog never opens a `Db`, so it never
/// fences a live writer.
pub(crate) enum Store {
    /// The single read-write writer, and whether its commits force the
    /// write-ahead log out rather than waiting for the flush timer.
    Writer { db: Db, flush_on_commit: bool },
    /// A read-only reader following the manifest, shared into read sessions.
    Reader(Arc<DbReader>),
}

impl Store {
    /// The writer behind this store, for a caller that needs to read after
    /// its own transaction is spent — a lost staged commit, deciding what
    /// it lost to.
    pub(crate) fn writer_db(&self) -> Option<&Db> {
        match self {
            Self::Writer { db, .. } => Some(db),
            Self::Reader(_) => None,
        }
    }

    /// How a commit through this store reaches object storage. A reader
    /// commits nothing, so it names the waiting route.
    pub(crate) fn commit_durability(&self) -> commit::CommitDurability {
        match self {
            Self::Writer {
                db,
                flush_on_commit,
            } => commit::writer_durability(db, *flush_on_commit),
            Self::Reader(_) => commit::CommitDurability::OnFlushInterval,
        }
    }
}

/// Options for opening a catalog.
///
/// # Examples
///
/// ```
/// let options = moraine::CatalogOptions::default();
/// assert_eq!(
///     options.flush_interval,
///     std::time::Duration::from_millis(100)
/// );
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CatalogOptions {
    /// Path prefix of the catalog within the bucket. Empty (the default)
    /// places the catalog at the bucket root; set it when several stores
    /// share a bucket.
    pub path: String,
    /// Whether DuckLake encrypts this catalog's data files. Creation-time
    /// only: recorded as the stored global `encrypted` option when a fresh
    /// store bootstraps, and ignored on an already-initialized store,
    /// where the stored value is authoritative.
    pub encrypted: bool,
    /// How often the store's write-ahead log is flushed to object
    /// storage. Durable commits wait for the next flush, so this bounds
    /// per-commit latency; smaller values mean more frequent (on S3,
    /// costlier) object-store PUTs. Zero flushes continuously (no timer),
    /// so a durable commit waits only on the object-store PUT — the lowest
    /// latency, at the cost of a busy flush loop. Defaults to 100ms.
    pub flush_interval: Duration,
    /// Whether a commit forces the write-ahead log out to object storage
    /// rather than waiting for the next flush. It trades one object-store
    /// PUT per commit for a commit latency that no longer includes up to a
    /// whole [`flush_interval`](Self::flush_interval) of waiting — worth it
    /// for a low commit rate, wasteful for a high one, where the interval
    /// is what batches many commits into one PUT. Defaults to `false`,
    /// leaving the interval in charge.
    pub flush_on_commit: bool,
    /// Local directory under which each store's block cache and the parsed
    /// Parquet metadata cache keep their disk tiers, recovered by the next
    /// process to open them. When set, warm queries skip repeat object-store
    /// GETs and a re-attach starts warm — worthwhile for remote (`s3://`)
    /// stores, redundant for local ones. Process-wide, like
    /// [`cache_size`](Self::cache_size): the first catalog to open decides
    /// whether there is a disk tier at all, and where. `None` (the
    /// default) keeps the caches in memory.
    pub cache_dir: Option<std::path::PathBuf>,
    /// How many bytes of disk each store's cache device may hold. The
    /// first catalog to open settles it for the process. `None` (the
    /// default) leaves a cap of 16 GiB in force, and without a
    /// [`cache_dir`](Self::cache_dir) there is no device to bound.
    pub cache_size: Option<u64>,
    /// How much memory the process-wide caches may hold — SST metadata,
    /// parsed Parquet metadata used by equality-index upkeep, and data
    /// blocks, which tier to the device when one is configured.
    /// Process-wide, like [`cache_size`](Self::cache_size).
    /// `None` (the default) takes what SlateDB gives a single store, now
    /// for the whole process. Never inert: the memory slots exist with or
    /// without a `cache_dir`, and this is the number to weigh against
    /// DuckDB's own `memory_limit` when sizing a host.
    pub cache_memory: Option<u64>,
    /// What to warm into the cache while the catalog opens, so the first
    /// query pays no first touch. Warming is reading, so it is bounded by
    /// the same caps and best-effort throughout — a subspace that cannot
    /// be read is skipped, never fatal — but it is part of the open, so an
    /// open that preloads returns only once it has. `None` (the default)
    /// warms nothing, leaving the cache to fill as reads ask for blocks.
    pub cache_preload: Option<CachePreload>,
    /// Whether objects this catalog writes are cached as they are written,
    /// rather than only when something reads them back. A flushed or
    /// compacted store object then costs one local write and no later
    /// fetch, and — since store objects are immutable and land atomically
    /// — a reader sharing the [`cache_dir`](Self::cache_dir) reads what
    /// the writer cached. Compaction output is cached too, so a merge can
    /// evict what reads had warmed; `false` (the default) leaves the cache
    /// filled by reads alone. Inert without a `cache_dir`.
    pub cache_puts: bool,
    /// The lake's data root (DuckLake's `DATA_PATH`). Creation-time only:
    /// recorded as the stored global `data_path` option when a fresh store
    /// bootstraps, so a later open can read it back
    /// ([`CatalogSnapshot::data_path`](crate::CatalogSnapshot::data_path)).
    /// `None` records nothing.
    pub data_path: Option<String>,
    /// How often a **read-only** catalog polls object storage for state a
    /// writer has committed since it last looked.
    ///
    /// This is a reader's freshness bound: nothing pushes a commit to it,
    /// so a read-only catalog can be one poll interval behind the writer,
    /// and its cached view is served for exactly that long. Shorter means
    /// fresher reads and more (on S3, billed) manifest and WAL listings.
    /// Defaults to 10 seconds. Ignored by [`Catalog::open`], which reads
    /// its own writes, and by a catalog pinned to a
    /// [`checkpoint`](Self::checkpoint), which polls for nothing.
    pub reader_poll_interval: Duration,
    /// An existing checkpoint id (a UUID) to pin a **read-only** catalog to,
    /// as reported by [`Catalog::create_checkpoint`].
    ///
    /// The default (`None`) follows the latest state, which writes a
    /// checkpoint of its own into the manifest on open and refreshes it for
    /// the catalog's lifetime. Set it for a reader whose credentials cannot
    /// write at all: the open then reads a fixed cut and writes nothing —
    /// at the cost of never seeing a later commit. Refused by
    /// [`Catalog::open`], which is a writer.
    pub checkpoint: Option<String>,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            path: String::new(),
            encrypted: false,
            flush_interval: Duration::from_millis(100),
            flush_on_commit: false,
            cache_dir: None,
            cache_size: None,
            cache_memory: None,
            cache_preload: None,
            cache_puts: false,
            data_path: None,
            reader_poll_interval: Duration::from_secs(10),
            checkpoint: None,
        }
    }
}

/// How much of a store to load into the on-disk object cache as it opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachePreload {
    /// The newest objects only — those a compaction has not yet merged
    /// down. Bounded by how far the store has run since its last merge,
    /// so an open stays quick.
    L0,
    /// Every object the store's manifest references, in full. The open
    /// waits for a copy of the whole store, so this suits a store that
    /// fits the cache with room to spare.
    All,
}

/// Warns when an `All` preload cannot hold the store it is about to load.
/// Diagnostics only: a manifest that could not be read is left to the open
/// itself to report.
fn warn_if_preload_cannot_fit(options: &CatalogOptions, manifest: Option<census::ManifestBytes>) {
    if options.cache_preload != Some(CachePreload::All) || options.cache_dir.is_none() {
        return;
    }

    let Some(store_bytes) = manifest.map(|manifest| manifest.store_bytes) else {
        return;
    };
    if let Some(shortfall) = open::preload_shortfall(store_bytes, options.cache_size) {
        warn!(
            path = options.path,
            store_bytes,
            cache_size = options.cache_size,
            shortfall,
            "preload cannot hold this store: the load stops once the cache is full, leaving \
             the rest to be fetched on demand"
        );
    }
}

/// Warns when the process's metadata cache is smaller than the store's SST
/// filters, indexes, and statistics.
///
/// Diagnostics only; the process-wide capacity is known only after the
/// open.
fn warn_if_metadata_cache_cannot_hold(path: &str, metadata_bytes: Option<u64>) {
    let Some(metadata_bytes) = metadata_bytes else {
        return;
    };
    let capacity = cache_status().metadata_capacity_bytes;
    if let Some(shortfall) = metadata_shortfall(metadata_bytes, capacity) {
        warn!(
            path,
            metadata_bytes,
            metadata_capacity_bytes = capacity,
            shortfall,
            "the metadata cache cannot hold this store's SST filters and indexes, so probes \
             will fetch them from object storage. Raise the cache memory budget on the first \
             attach in the process."
        );
    }
}

/// Parses a configured checkpoint id, naming the option in the error.
fn parse_checkpoint(checkpoint: Option<&str>) -> Result<Option<uuid::Uuid>> {
    checkpoint
        .map(|id| {
            uuid::Uuid::parse_str(id).map_err(|err| {
                Error::Configuration(format!("checkpoint `{id}` is not a valid id: {err}"))
            })
        })
        .transpose()
}

/// One member of a [`Catalog::commit_group`] batch: a closure authoring
/// one logical commit.
pub type CommitMember<'a> = &'a (dyn Fn(&mut Transaction) -> Result<()> + Sync);

/// The read surface of a moraine catalog: cheap to clone, drives every
/// read. This is what a read-only attach hands back, and what a
/// read-write [`Catalog`] derefs to.
///
/// It carries no mutator: a `commit` against a catalog opened read-only is
/// a compile error rather than a runtime [`Error::Constraint`].
///
/// ```compile_fail
/// # use std::sync::Arc;
/// # use moraine::{Catalog, CatalogOptions};
/// # use object_store::memory::InMemory;
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// # let object_store = Arc::new(InMemory::new());
/// # let writer = Catalog::open(object_store.clone(), CatalogOptions::default()).await?;
/// # writer.close().await?;
/// let reader = Catalog::open_read_only(object_store, CatalogOptions::default()).await?;
/// // There is no `commit` on a read-only handle, so this does not build.
/// reader.commit(|tx| tx.create_schema("nope").map(|_| ())).await?;
/// # Ok::<(), moraine::Error>(()) }).unwrap();
/// ```
///
/// The reads it does carry work exactly as they do on a writer:
///
/// ```
/// # use std::sync::Arc;
/// # use moraine::{Catalog, CatalogOptions};
/// # use object_store::memory::InMemory;
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// # let object_store = Arc::new(InMemory::new());
/// # let writer = Catalog::open(object_store.clone(), CatalogOptions::default()).await?;
/// # writer.commit(|tx| tx.create_schema("sales").map(|_| ())).await?;
/// # writer.close().await?;
/// let reader = Catalog::open_read_only(object_store, CatalogOptions::default()).await?;
/// assert!(reader.snapshot().await?.schema_by_name("sales").is_some());
/// # Ok::<(), moraine::Error>(()) }).unwrap();
/// ```
#[derive(Clone)]
pub struct ReadOnlyCatalog {
    store: Arc<Store>,
    reads: Arc<ReadTally>,
    cache: Arc<CacheCounters>,
    data_reads: Arc<data_file::DataStoreCounters>,
    location: Arc<StoreLocation>,
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
    commits: Arc<commit::Coalescer>,
    // `None` on a read-only handle.
    writer_status: Option<watch::Receiver<DbStatus>>,
}

impl std::fmt::Debug for ReadOnlyCatalog {
    // `slatedb::Db` carries no `Debug` impl.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadOnlyCatalog").finish_non_exhaustive()
    }
}

/// A read-write handle to a moraine catalog: cheap to clone, drives reads
/// and commits.
///
/// Every read lives on [`ReadOnlyCatalog`] and reaches this type through
/// `Deref`; this type adds the mutators. Exactly one process may hold one
/// per store.
#[derive(Clone)]
pub struct Catalog {
    inner: ReadOnlyCatalog,
}

impl std::fmt::Debug for Catalog {
    // `slatedb::Db` carries no `Debug` impl.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog").finish_non_exhaustive()
    }
}

impl std::ops::Deref for Catalog {
    type Target = ReadOnlyCatalog;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ReadOnlyCatalog {
    /// The maintained-projection state shared by this handle's clones.
    pub(crate) fn projections(&self) -> &Arc<std::sync::RwLock<ProjectionCache>> {
        &self.projections
    }

    /// What the process-wide block cache has served for **this catalog**
    /// since it attached, metadata and data blocks counted apart;
    /// [`cache_tally`](crate::cache_tally) is the same counts for the
    /// process.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// let tally = catalog.cache_tally();
    /// // A fresh attach of a fresh store has read nothing back through
    /// // the cache, so there is no rate to report yet.
    /// assert_eq!(
    ///     tally.block_hit_rate().is_none(),
    ///     tally.block_hits == 0 && tally.block_misses == 0
    /// );
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    #[must_use]
    pub fn cache_tally(&self) -> CacheTally {
        self.cache.tally()
    }

    /// Roughly what this handle's decoded catalog holds, in bytes.
    ///
    /// The other caches are the process's and are budgeted by
    /// [`CatalogOptions::cache_memory`]; this one is the handle's, is
    /// bounded by the catalog's own size rather than by a cap, and is
    /// replaced when the head stamp moves rather than evicted under
    /// pressure. It is reported so a host sizing a process can see it at
    /// all.
    ///
    /// An estimate over the decoded record sets, the maintained
    /// projections, and the head view derived from them. Encoded record
    /// lengths stand in for what those records occupy in memory, so treat
    /// this as a floor.
    #[must_use]
    pub fn projection_bytes(&self) -> u64 {
        self.projections
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .estimated_bytes()
    }

    /// Physical object-store requests this catalog has issued since it
    /// attached.
    ///
    /// Counts include retries. Moraine's WAL objects use its main physical
    /// store today, so their requests appear under `main_*`; `wal_*` is for
    /// a separately configured WAL store. Durations are summed request
    /// latency and can exceed wall-clock time when requests overlap.
    #[must_use]
    pub fn object_store_tally(&self) -> ObjectStoreTally {
        let mut tally = self.cache.object_store_tally();
        tally.data_gets = self.data_reads.gets();
        tally.data_bytes = self.data_reads.bytes();
        tally
    }

    pub(crate) fn data_reads(&self) -> Arc<data_file::DataStoreCounters> {
        Arc::clone(&self.data_reads)
    }

    /// A fresh scoped-read tally whose data-store reads also count towards
    /// this handle's [`object_store_tally`](Self::object_store_tally).
    pub(crate) fn data_read_metrics(&self) -> Arc<data_file::ScopedReadMetrics> {
        Arc::new(data_file::ScopedReadMetrics::reporting_to(Arc::clone(
            &self.data_reads,
        )))
    }

    /// Records that the `current` half of the shared record set was
    /// scanned from the store.
    pub(crate) fn tally_current_scan(&self) {
        self.reads.current_scans.fetch_add(1, Ordering::Relaxed);
    }

    /// Records that the `history` half was scanned from the store.
    pub(crate) fn tally_history_scan(&self) {
        self.reads.history_scans.fetch_add(1, Ordering::Relaxed);
    }

    /// How many times each half of the shared record set has been scanned:
    /// `(current, history)`. At one head each moves at most once.
    #[cfg(test)]
    pub(crate) fn entity_scan_tallies(&self) -> (u64, u64) {
        (
            self.reads.current_scans.load(Ordering::Relaxed),
            self.reads.history_scans.load(Ordering::Relaxed),
        )
    }

    /// Records that a read resolved the head from the store.
    pub(crate) fn note_head_read(&self) {
        self.reads.head_reads.fetch_add(1, Ordering::Relaxed);
    }

    /// How many reads have resolved the head from the store.
    #[cfg(test)]
    pub(crate) fn head_reads(&self) -> u64 {
        self.reads.head_reads.load(Ordering::Relaxed)
    }

    /// Refuses if this handle has lost the writer epoch, or its `Db` has
    /// closed for any other reason. Reads the status channel only: no
    /// store read and no session.
    pub(crate) fn refuse_if_closed(&self) -> Result<()> {
        let Some(status) = &self.writer_status else {
            return Ok(());
        };
        match status.borrow().close_reason {
            None => Ok(()),
            Some(CloseReason::Fenced) => Err(crate::error::fenced()),
            Some(reason) => Err(Error::Interrupted(format!(
                "this catalog's store handle has closed ({reason:?}); re-attach to use it"
            ))),
        }
    }

    /// The held view a read-write handle may serve without resolving the
    /// head from the store; `None` on a read-only handle, which always
    /// reads. Callers must pair it with
    /// [`refuse_if_closed`](Self::refuse_if_closed).
    pub(crate) fn writer_head_view(&self) -> Option<Arc<CatalogSnapshot>> {
        if !self.holds_the_writer() {
            return None;
        }
        held_head_view(&self.projections)
    }

    /// Whether this handle is the store's writer, and so the only thing
    /// that can move `sys/head`.
    pub(crate) fn holds_the_writer(&self) -> bool {
        matches!(self.store.as_ref(), Store::Writer { .. })
    }

    /// An immutable view of the catalog at the latest committed snapshot.
    ///
    /// Shared rather than owned: a warm handle hands back the view it
    /// already holds, so repeated reads cost neither a store scan nor a
    /// copy of the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be read.
    pub async fn snapshot(&self) -> Result<Arc<CatalogSnapshot>> {
        self.view(None).await
    }

    /// An immutable view of the catalog as of `snapshot` (time travel).
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if `snapshot` is beyond the head, or
    /// another error if the store cannot be read.
    pub async fn snapshot_at(&self, snapshot: SnapshotId) -> Result<Arc<CatalogSnapshot>> {
        self.view(Some(snapshot.get())).await
    }

    /// A table's inlined rows live at the latest committed snapshot, in
    /// row-id order, each carrying the Arrow IPC bytes it decodes from.
    ///
    /// These rows are not part of a [`CatalogSnapshot`]: they are served
    /// from the `inline` subspace on demand, one range scan per call. Rows
    /// of one chunk share one body and rows of one schema version share one
    /// schema.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corruption`] if a chunk names a schema version with
    /// no recorded schema, or another error if the store cannot be read.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, ColumnDef, InlineChunk};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// # let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// # let created = std::cell::Cell::new(None);
    /// catalog
    ///     .commit(|tx| {
    ///         let main = tx.schema_by_name("main").expect("bootstrap schema").id;
    ///         let orders = tx.create_table(
    ///             main,
    ///             "orders",
    ///             &[ColumnDef {
    ///                 name: "id".into(),
    ///                 column_type: "BIGINT".into(),
    ///                 nulls_allowed: false,
    ///                 default_value: None,
    ///                 children: Vec::new(),
    ///             }],
    ///         )?;
    ///         // The Arrow bytes are the caller's to produce; moraine stores
    ///         // and returns them verbatim.
    ///         tx.inline_insert(
    ///             orders,
    ///             &InlineChunk {
    ///                 schema_version: 0,
    ///                 arrow_schema: arrow_schema_bytes(),
    ///                 arrow_body: record_batch_bytes(),
    ///                 row_count: 2,
    ///             },
    ///             &[],
    ///         )?;
    /// #       created.set(Some(orders));
    ///         Ok(())
    ///     })
    ///     .await?;
    /// # let orders = created.get().unwrap();
    ///
    /// let rows = catalog.recent_rows(orders).await?;
    /// assert_eq!(rows.len(), 2);
    /// assert_eq!(rows[0].row_id, 0);
    /// assert_eq!(rows[1].offset_in_chunk, 1);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// # fn arrow_schema_bytes() -> Vec<u8> { b"schema".to_vec() }
    /// # fn record_batch_bytes() -> Vec<u8> { b"body".to_vec() }
    /// ```
    pub async fn recent_rows(&self, table: TableId) -> Result<Vec<RecentRow>> {
        let session = self.begin_read().await?;
        let outcome = self.scan_recent_rows(&session, table, None).await;
        session.finish();

        outcome
    }

    /// A table's inlined rows as of `snapshot` (time travel): the rows whose
    /// insert had landed by then and whose tombstone had not.
    ///
    /// Only rows still inlined are found: past snapshots of flushed rows
    /// read from the backdated data file
    /// [`CatalogSnapshot::data_files_of`] serves, never from here.
    ///
    /// # Errors
    ///
    /// As [`Self::recent_rows`], plus [`Error::NotFound`] if `snapshot` is
    /// beyond the head and [`Error::SnapshotExpired`] if it has fallen below
    /// the retention horizon.
    pub async fn recent_rows_at(
        &self,
        table: TableId,
        snapshot: SnapshotId,
    ) -> Result<Vec<RecentRow>> {
        let session = self.begin_read().await?;
        let outcome = self
            .scan_recent_rows(&session, table, Some(snapshot.get()))
            .await;
        session.finish();

        outcome
    }

    /// One inlined row of a table by id, or `None` when no live inlined row
    /// carries it — including when the row lives in a data file instead.
    ///
    /// # Errors
    ///
    /// As [`Self::recent_rows`].
    pub async fn recent_row(&self, table: TableId, row_id: u64) -> Result<Option<RecentRow>> {
        Ok(self
            .recent_rows(table)
            .await?
            .into_iter()
            .find(|row| row.row_id == row_id))
    }

    /// The row ids of `table`'s live inlined rows at head, without their
    /// bodies — what a row-location probe needs. Served from the
    /// chunk-range directory once it is known complete; the first walk
    /// verifies it against the chunk scan and remembers the answer when
    /// the store format shuts out writers that predate the directory.
    pub(crate) async fn live_inline_row_ids(&self, table: TableId) -> Result<Vec<u64>> {
        let session = self.begin_read().await?;
        let outcome = self.scan_live_inline_row_ids(&session, table).await;
        session.finish();

        outcome
    }

    /// The view at `at`, or at head when `None`. Time travel always
    /// materializes: the cache holds head views only.
    async fn view(&self, at: Option<u64>) -> Result<Arc<CatalogSnapshot>> {
        if at.is_some() {
            let session = self.begin_read().await?;
            let started = Instant::now();
            let view = commit::materialize(session.handle(), at).await;
            session.finish();
            if view.is_ok() {
                self.reads.materialized(started.elapsed());
            }

            return view.map(Arc::new);
        }

        // Captured before the first store read, so an invalidation racing
        // this read cannot be overwritten by what it invalidated.
        if let Some(view) = self.writer_head_view() {
            self.refuse_if_closed()?;
            self.reads.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(view);
        }

        let epoch = cache_epoch(&self.projections);
        let session = self.begin_read().await?;
        let view = self.head_view(session.handle()).await;
        session.finish();

        let view = view?;
        install_head_view_at(&self.projections, epoch, Arc::clone(&view));

        Ok(view)
    }

    /// The cached view when it already stands at head, a view refreshed
    /// across the gap when it has fallen behind and the gap is replayable,
    /// else a fresh materialization.
    async fn head_view(&self, handle: ReadHandle<'_>) -> Result<Arc<CatalogSnapshot>> {
        self.note_head_read();
        let head = commit::read_head_value(handle).await?;
        if let Some(cached) = cached_head_view(&self.projections, &head) {
            self.reads.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached);
        }

        if let Some(behind) = held_head_view(&self.projections)
            && let Some(refreshed) = commit::refresh(handle, &behind).await?
        {
            self.reads.refreshes.fetch_add(1, Ordering::Relaxed);
            return Ok(Arc::new(refreshed));
        }

        // Derive from the shared `current` half before paying a scan. The
        // stamp is re-verified under the same consistent cut the build
        // reads through, so a derived view equals a scanned one; a store
        // that moved in between reports `None` and falls through.
        if let Some(current) = shared_current_entities(&self.projections, &head)
            && let Some(view) = commit::materialize_from(handle, &head, &current).await?
        {
            self.reads.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Arc::new(view));
        }

        let started = Instant::now();
        let (view, records) = commit::materialize_capturing(handle).await?;
        self.reads.materialized(started.elapsed());
        self.tally_current_scan();
        // Stamped with the state the *view* settled at — the consistent
        // cut may be newer than the head read above, never mismatched
        // with the records.
        install_shared_current_entities(
            &self.projections,
            HeadValue {
                snapshot_id: view.snapshot.snapshot_id,
                batch_seq: view.batch_seq,
            },
            records,
        );

        Ok(Arc::new(view))
    }

    /// Opens a read session at the current head — a read-write transaction or
    /// the read-only reader — the same isolation
    /// [`snapshot`](Self::snapshot)/[`snapshot_at`](Self::snapshot_at) use.
    /// Every read of the store opens its session here, so this is where a
    /// store mid-structural-migration is refused. The check is keyed on
    /// the head: a migration start moves it, so a head already observed
    /// clear opens without re-reading the marker — the head is rewritten
    /// every batch and stays hot, where the absent marker is the store's
    /// most expensive possible read.
    pub(crate) async fn begin_read(&self) -> Result<ReadSession> {
        let session = match self.store.as_ref() {
            Store::Writer { db, .. } => ReadSession::Tx(
                db.begin(IsolationLevel::Snapshot)
                    .await
                    .map_err(Error::from)?,
            ),
            Store::Reader(reader) => ReadSession::Reader(reader.clone()),
        };

        let refused = async {
            let head = commit::read_head_value(session.handle()).await?;
            commit::refuse_mid_migration_at(session.handle(), &self.projections, &head).await
        };
        if let Err(error) = refused.await {
            session.finish();
            return Err(error);
        }

        Ok(session)
    }

    /// Closes the catalog, flushing background work.
    ///
    /// A [`Catalog`] is cheaply cloneable, and all clones share one
    /// underlying store handle: closing through any clone shuts that
    /// store down for every clone, so subsequent operations on any of
    /// them — this one included — fail.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store fails to close cleanly.
    pub async fn close(&self) -> Result<()> {
        match self.store.as_ref() {
            Store::Writer { db, .. } => db.close().await.map_err(Error::from),
            Store::Reader(reader) => reader.close().await.map_err(Error::from),
        }
    }
}

impl Catalog {
    /// The underlying writer `Db`; a `Catalog` is only ever built around
    /// one, so the reader arm is unreachable by construction.
    fn writer(&self) -> Result<&Db> {
        match self.store.as_ref() {
            Store::Writer { db, .. } => Ok(db),
            Store::Reader(_) => Err(Error::Constraint(
                "catalog opened read-only; writes are unavailable".to_string(),
            )),
        }
    }

    /// Raises the store's format to the newest purely additive one this
    /// binary writes, ahead of the commit that would otherwise take it.
    ///
    /// One-way: a store that has taken the format can no longer be opened
    /// by a binary that predates it, and a reader already attached on one
    /// is not refused — it keeps running and may misread later commits.
    /// Raise it only once every reader is on a binary that understands
    /// it.
    ///
    /// `dry_run` reports the move it would make and stamps nothing — the
    /// only way to read a store's format without moving it.
    ///
    /// # Errors
    ///
    /// Returns an error if the store is mid-migration, or if the stamp
    /// cannot be committed durably.
    pub async fn raise_format(
        &self,
        dry_run: bool,
    ) -> Result<crate::transaction::migration::FormatRaise> {
        let raised = crate::transaction::migration::raise_format(self.writer()?, dry_run).await?;
        if !dry_run {
            crate::catalog::projection::raise_format_floor(self.projections(), raised.to_format);
        }
        Ok(raised)
    }

    /// Opens (creating and initializing if empty) the catalog in
    /// `object_store` at `options.path`.
    ///
    /// Exactly one process may hold a read-write catalog per store —
    /// opening a second fences the first. An open that is fenced while
    /// *creating* the catalog re-attempts a few times before reporting it:
    /// nothing of the fenced attempt reached the store, so the re-attempt
    /// either adopts the catalog the other process created or creates it.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be opened, is mid-migration,
    /// is stamped with a structural format this binary does not
    /// understand, or is still being created out from under this open once
    /// the re-attempts are spent ([`Error::Fenced`]).
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// // Bootstrap mints the default `main` schema.
    /// assert_eq!(catalog.snapshot().await?.schemas().len(), 1);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn open(object_store: Arc<dyn ObjectStore>, options: CatalogOptions) -> Result<Self> {
        if options.checkpoint.is_some() {
            return Err(Error::Configuration(
                "a checkpoint pins a read-only catalog to a fixed cut; a writer commits new \
                 state and cannot be opened against one"
                    .to_string(),
            ));
        }
        let located = Arc::clone(&object_store);
        // One manifest read serves both diagnostics, before the open.
        let manifest = census::manifest_bytes(&options.path, Arc::clone(&located))
            .await
            .ok();
        warn_if_preload_cannot_fit(&options, manifest);
        let store = StoreBuilder::new(&options.path, object_store)
            .flush_interval(options.flush_interval)
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_memory(options.cache_memory)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts);
        let (db, cache, format) = commit::open_initialized(
            store,
            options.encrypted,
            options.data_path.as_deref(),
            options.flush_on_commit,
        )
        .await?;
        warn_if_metadata_cache_cannot_hold(
            &options.path,
            manifest.map(|manifest| manifest.metadata_bytes),
        );
        info!(
            path = options.path,
            flush_interval_ms = options.flush_interval.as_millis(),
            flush_on_commit = options.flush_on_commit,
            "opened catalog read-write"
        );
        let projections = Arc::new(std::sync::RwLock::new(ProjectionCache::empty()));
        // The open already validated the stamp; commits below this floor
        // owe no format read.
        crate::catalog::projection::raise_format_floor(&projections, format);
        let durability = commit::writer_durability(&db, options.flush_on_commit);
        Ok(Self {
            inner: ReadOnlyCatalog {
                writer_status: Some(db.subscribe()),
                store: Arc::new(Store::Writer {
                    db,
                    flush_on_commit: options.flush_on_commit,
                }),
                location: Arc::new(StoreLocation {
                    path: options.path,
                    object_store: located,
                }),
                reads: Arc::new(ReadTally::default()),
                cache,
                data_reads: Arc::default(),
                commits: Arc::new(commit::Coalescer::new(Arc::clone(&projections), durability)),
                projections,
            },
        })
    }

    /// Opens the catalog **read-only** in `object_store` at `options.path`,
    /// as a `DbReader` following the latest manifest — or, when
    /// [`CatalogOptions::checkpoint`] is set, pinned to that checkpoint.
    ///
    /// A read-only catalog never opens the writer `Db`, so it never fences a
    /// live read-write process. It never bootstraps: opening a store no
    /// writer has initialized is refused.
    ///
    /// Following the latest state writes a checkpoint into the manifest on
    /// open and refreshes it while the catalog lives, so those credentials
    /// still need manifest write access. A catalog opened against a
    /// checkpoint writes nothing and never sees a later commit.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be opened, is not an initialized
    /// moraine catalog, is stamped with an unknown structural format, or
    /// names a checkpoint that is not a valid id or no longer exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let object_store = Arc::new(InMemory::new());
    /// let catalog = Catalog::open(object_store.clone(), CatalogOptions::default()).await?;
    /// catalog.commit(|tx| tx.create_schema("sales").map(|_| ())).await?;
    /// let checkpoint = catalog.create_checkpoint(None).await?;
    ///
    /// // A commit after the checkpoint is not in it.
    /// catalog.commit(|tx| tx.create_schema("ops").map(|_| ())).await?;
    ///
    /// let mut options = CatalogOptions::default();
    /// options.checkpoint = Some(checkpoint);
    /// let reader = Catalog::open_read_only(object_store, options).await?;
    /// let view = reader.snapshot().await?;
    /// assert!(view.schema_by_name("sales").is_some());
    /// assert!(view.schema_by_name("ops").is_none());
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn open_read_only(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
    ) -> Result<ReadOnlyCatalog> {
        let checkpoint = parse_checkpoint(options.checkpoint.as_deref())?;
        let located = Arc::clone(&object_store);
        // One manifest read serves both diagnostics, before the open.
        let manifest = census::manifest_bytes(&options.path, Arc::clone(&located))
            .await
            .ok();
        warn_if_preload_cannot_fit(&options, manifest);
        let store = StoreBuilder::new(&options.path, object_store)
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_memory(options.cache_memory)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts)
            .poll_interval(options.reader_poll_interval)
            .checkpoint(checkpoint);

        let (reader, cache, format) = commit::open_reader_initialized(store).await?;
        warn_if_metadata_cache_cannot_hold(
            &options.path,
            manifest.map(|manifest| manifest.metadata_bytes),
        );
        info!(
            path = options.path,
            checkpoint = options.checkpoint,
            "opened catalog read-only"
        );
        let projections = Arc::new(std::sync::RwLock::new(ProjectionCache::empty()));
        crate::catalog::projection::raise_format_floor(&projections, format);
        Ok(ReadOnlyCatalog {
            writer_status: None,
            store: Arc::new(Store::Reader(Arc::new(reader))),
            location: Arc::new(StoreLocation {
                path: options.path,
                object_store: located,
            }),
            reads: Arc::new(ReadTally::default()),
            cache,
            data_reads: Arc::default(),
            commits: Arc::new(commit::Coalescer::new(
                Arc::clone(&projections),
                commit::CommitDurability::OnFlushInterval,
            )),
            projections,
        })
    }

    /// Rewrites the store in place to the newest structural format this
    /// binary understands, resuming an interrupted migration if one is in
    /// flight, and reports what it did.
    ///
    /// Not part of opening a catalog. It takes the writer epoch exactly as
    /// [`open`](Self::open) does, so it fences a running catalog and is
    /// itself fenced by one.
    ///
    /// Running it against a store already at the newest format is a no-op:
    /// the returned report names the same format twice and no units.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corruption`] if the store is not an initialized
    /// moraine catalog, or carries a marker its format stamp contradicts;
    /// [`Error::Migration`] if a migration is in flight that this binary does
    /// not carry; or a store error if a batch fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, MigrationRequest};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let object_store = Arc::new(InMemory::new());
    /// let catalog = Catalog::open(object_store.clone(), CatalogOptions::default()).await?;
    /// catalog.close().await?;
    ///
    /// let report = Catalog::migrate(
    ///     object_store,
    ///     CatalogOptions::default(),
    ///     MigrationRequest::default(),
    /// )
    /// .await?;
    /// // A fresh store is already current, so nothing runs.
    /// assert_eq!(report.from_format, report.to_format);
    /// assert!(report.units_run.is_empty());
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn migrate(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
        request: MigrationRequest,
    ) -> Result<MigrationReport> {
        let (db, _cache) = StoreBuilder::new(&options.path, object_store.clone())
            .flush_interval(options.flush_interval)
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_memory(options.cache_memory)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts)
            .open_writer()
            .await?;

        let checkpoint = if request.checkpoint {
            let taken = open::create_checkpoint(&db, None).await?;
            info!(checkpoint = %taken, "took a pre-migration checkpoint");
            Some(taken)
        } else {
            None
        };

        let report = migration::run(&db).await;
        let closed = db.close().await.map_err(Error::from);

        // A failed migration keeps its checkpoint as the recovery point.
        let report = report.and_then(|report| closed.map(|()| report))?;

        if let Some(checkpoint) = checkpoint {
            StoreBuilder::new(&options.path, object_store)
                .delete_checkpoint(checkpoint)
                .await?;
        }

        Ok(report)
    }

    /// Pins everything committed so far as a checkpoint, and reports its id.
    ///
    /// A checkpoint is an immutable cut of the store that
    /// [`CatalogOptions::checkpoint`] opens a reader against. It pins every
    /// object it references against garbage collection, so a checkpoint
    /// with no `lifetime` holds storage until
    /// [`delete_checkpoint`](Self::delete_checkpoint) removes it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] if the catalog was opened read-only —
    /// creating a checkpoint is a manifest write — or a store error if the
    /// write fails.
    pub async fn create_checkpoint(&self, lifetime: Option<Duration>) -> Result<String> {
        let id = open::create_checkpoint(self.writer()?, lifetime).await?;
        info!(checkpoint = %id, "created a checkpoint");
        Ok(id.to_string())
    }

    /// Deletes the checkpoint `checkpoint`, releasing the objects it pinned.
    ///
    /// Never opens the writer `Db`, so it runs against a live catalog
    /// without fencing it. Readers already open against the deleted
    /// checkpoint keep serving; a reader that opens against it afterwards
    /// is refused.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Configuration`] if `checkpoint` is not a valid id,
    /// or a store error if the manifest update fails.
    pub async fn delete_checkpoint(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
        checkpoint: &str,
    ) -> Result<()> {
        let id = parse_checkpoint(Some(checkpoint))?
            .ok_or_else(|| Error::Configuration("no checkpoint given".to_string()))?;
        StoreBuilder::new(&options.path, object_store)
            .delete_checkpoint(id)
            .await
    }

    /// Every checkpoint the store's manifest carries, as the ids
    /// [`create_checkpoint`](Self::create_checkpoint) hands out.
    ///
    /// Reads the manifest and never opens the writer `Db`, so it runs
    /// against a live catalog without fencing it. Reader-established
    /// checkpoints show up here too.
    ///
    /// # Errors
    ///
    /// Returns a store error if the manifest cannot be read.
    pub async fn checkpoints(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
    ) -> Result<Vec<String>> {
        let ids = StoreBuilder::new(&options.path, object_store)
            .list_checkpoints()
            .await?;
        Ok(ids.iter().map(uuid::Uuid::to_string).collect())
    }

    /// Opens a read-write transaction for the staged-row commit path. Fails
    /// with [`Error::Constraint`] on a read-only catalog.
    /// The open store, shared so a spent transaction can still read.
    pub(crate) fn store(&self) -> Arc<Store> {
        Arc::clone(&self.store)
    }

    pub(crate) async fn begin_write_tx(&self) -> Result<DbTransaction> {
        self.writer()?
            .begin(IsolationLevel::Snapshot)
            .await
            .map_err(Error::from)
    }

    /// Commits catalog mutations atomically, producing one new snapshot.
    ///
    /// The closure stages mutations on the [`Transaction`]; reads on the
    /// `Transaction` observe its own staged state. It may be re-run against
    /// fresh state after a lost race with a concurrent commit, so it must
    /// be pure: no I/O, no effects other than the `Transaction` calls. A
    /// closure that stages nothing commits nothing and returns the
    /// unchanged head snapshot id.
    ///
    /// # Errors
    ///
    /// Returns whatever error the closure returns (the commit is
    /// aborted), or an error from the underlying store. Returns
    /// [`Error::CommitConflict`] when a concurrent commit truly conflicts
    /// — it touched the same tables or the schema list. Returns
    /// [`Error::RetryBudgetExhausted`] when the bounded internal retry
    /// budget runs out before a benign race resolves; unlike a conflict,
    /// that is terminal, and the caller re-drives the work itself —
    /// usually as smaller commits.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, ColumnDef};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// # let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// let snapshot = catalog
    ///     .commit(|tx| {
    ///         let sales = tx.create_schema("sales")?;
    ///         tx.create_table(
    ///             sales,
    ///             "orders",
    ///             &[ColumnDef {
    ///                 name: "id".into(),
    ///                 column_type: "BIGINT".into(),
    ///                 nulls_allowed: false,
    ///                 default_value: None,
    ///                 children: Vec::new(),
    ///             }],
    ///         )?;
    ///         Ok(())
    ///     })
    ///     .await?;
    /// // `main` plus the newly created `sales` schema.
    /// assert_eq!(catalog.snapshot_at(snapshot).await?.schemas().len(), 2);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn commit<F>(&self, f: F) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        let ids =
            commit::commit_cycle(self.writer()?, std::slice::from_ref(&f), &self.commits).await?;
        ids.first().copied().ok_or_else(|| {
            Error::Corruption("a commit of one member reported no snapshot".to_string())
        })
    }

    /// Commits several mutations as one batch, made durable by one flush.
    ///
    /// Each closure is its own logical commit with its own snapshot — a
    /// group batches commits, it does not merge them — so the returned ids
    /// are one per member, in member order, and time travel resolves each
    /// separately. Members run in the order given, and each stages against
    /// the state the members before it left, so a group never conflicts
    /// with itself. Where [`Catalog::commit`] costs one durable flush per
    /// mutation, a group costs one for all of them.
    ///
    /// The batch is the unit of durability: a crash leaves every member
    /// committed or none of them, and a member that fails aborts the whole
    /// group, including members that already staged. Closures may be re-run
    /// as a group after a lost race with a concurrent commit, so the purity
    /// requirement of [`Catalog::commit`] applies to every member.
    ///
    /// # Errors
    ///
    /// Returns whatever error any member returns (the whole group is
    /// aborted), or the errors [`Catalog::commit`] documents — a conflict
    /// or an exhausted retry budget applies to the group as a whole.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, Transaction};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// # let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// let ids = catalog
    ///     .commit_group(&[
    ///         &|tx: &mut Transaction| tx.create_schema("sales").map(|_| ()),
    ///         &|tx: &mut Transaction| tx.create_schema("ops").map(|_| ()),
    ///     ])
    ///     .await?;
    ///
    /// // Two snapshots, one flush.
    /// assert_eq!(ids.len(), 2);
    /// assert_eq!(ids[1].get(), ids[0].get() + 1);
    /// assert!(catalog.snapshot_at(ids[0]).await?.schema_by_name("ops").is_none());
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn commit_group(&self, members: &[CommitMember<'_>]) -> Result<Vec<SnapshotId>> {
        commit::commit_cycle(self.writer()?, members, &self.commits).await
    }
}
