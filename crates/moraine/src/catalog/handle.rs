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

use futures::StreamExt;
pub use maintenance::{
    MaintenanceReport, MaintenanceRequest, MaintenanceStatusPass, MaintenanceStatusStep,
};
use moraine_wal::FoldReport;
use object_store::{ObjectStore, path::Path};
pub use row_location::RowSummaryWarmth;
use slatedb::{Db, DbReader};
use tracing::{info, warn};

use crate::{
    catalog::{CatalogSnapshot, RecentRow, SnapshotId, TableId, projection::ProjectionCache},
    data_file,
    error::{Error, Result},
    store::{
        cache::{CacheCounters, CacheTally, ObjectStoreTally, cache_status, metadata_shortfall},
        census,
        handle::{ReadHandle, ReadSession},
        open::{self, StoreBuilder},
    },
    transaction::{MigrationReport, Transaction, commit, folder, migration, slot_commit},
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

/// The open store behind a catalog.
pub(crate) enum Store {
    /// The slot-log-backed store: a reader following the folded store plus the
    /// slot log its commits ride. Boxed to keep the enum's footprint down.
    Slots(Box<SlotStore>),
}

/// A slot-backed store: the folded state a reader follows, plus the slot log
/// commits ride and the per-attach state shared by every clone of the handle.
pub(crate) struct SlotStore {
    pub(crate) reader: Arc<DbReader>,
    pub(crate) slots: moraine_wal::SlotLog,
    /// Retained for reopening the fenced writer (folder role) and fresh
    /// readers (past a truncation).
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) options: CatalogOptions,
    /// Whether this attach may write: `false` refuses commits and folder-role
    /// work, so a read-only attach never opens the writer.
    pub(crate) read_only: bool,
    /// Whether this attach is pinned to a checkpoint: a fixed cut that reads
    /// the folded store the checkpoint captured and never replays the tail
    /// committed after it.
    pub(crate) pinned: bool,
    /// Serializes and coalesces this process's slot commits. Shared by every
    /// clone of the handle.
    pub(crate) coalescer: slot_commit::CommitCoalescer,
    /// The materialized head cached across reads, so a repeated read need not
    /// re-materialize from the folded store. Shared by every clone of the
    /// handle; a successful commit updates it in place.
    pub(crate) head_cache: slot_commit::HeadCache,
    /// The handle's projection cache, shared so the commit path can consult
    /// the format floor without a handle to hand.
    pub(crate) projections: Arc<std::sync::RwLock<ProjectionCache>>,
    /// Slot-race and retry counters accumulated across commits, shared by every
    /// clone of the handle through the store's `Arc` and by the staged
    /// transactions it opens, which arm forwarding by counting their lost
    /// races.
    pub(crate) contention: Arc<slot_commit::ContentionCounters>,
    /// The endpoints this process has given up forwarding to, shared by every
    /// clone of the handle. A lost race arms forwarding; an unreachable
    /// endpoint ages here so the client stops retrying it and commits
    /// directly.
    #[cfg(feature = "leader")]
    pub(crate) forwarding: Arc<slot_commit::Forwarding>,
}

/// Options for opening a catalog.
///
/// # Examples
///
/// ```
/// let options = moraine::CatalogOptions::default();
/// assert_eq!(
///     options.reader_poll_interval,
///     std::time::Duration::from_secs(10)
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
    /// How long a commit may wait to be batched with others of this process
    /// before racing the log. Zero (the default) still coalesces whatever is
    /// already queued into one envelope — it only declines to *wait* — so an
    /// uncontended commit adds no latency. A non-zero window trades latency
    /// for fewer object-store PUTs under load, the write-side cost axis.
    pub commit_batch_window: Duration,
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
    /// How often a reader polls the object store for a fresher folded view.
    /// A reader lags the head by up to this interval, replaying that many
    /// more slots per materialization; a shorter interval trades more
    /// manifest reads for less fold lag, the read-side cost axis. Defaults
    /// to 10 seconds. Ignored by a catalog pinned to a
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
            commit_batch_window: Duration::ZERO,
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

/// How many times a read-write open re-probes after a racing migration fences
/// its own migration write. Each retry re-reads a store one competitor has by
/// then converted, so a small bound suffices.
const MIGRATION_ATTEMPTS: usize = 8;

/// A probe reader's stored structural format, as a read-write open classifies
/// it.
enum FormatClass {
    /// No format stamp: an empty or unfinished store, to bootstrap.
    Empty,
    /// The slot-log format this binary attaches directly.
    SlotLog,
    /// A legacy format 1–3 store, to migrate to the slot-log format.
    Legacy,
    /// A format newer than this binary understands, to refuse.
    TooNew(u64),
}

/// What `path` holds when an open could not read a store there: nothing to
/// destroy (a fresh prefix to bootstrap), a SlateDB manifest (a store a peer
/// created or is still creating — a benign race), or other objects (a damaged
/// or foreign prefix a bootstrap must refuse to stamp over). A listing that
/// fails answers `Foreign`: anything short of proof the prefix is empty denies
/// creation.
enum PrefixState {
    Empty,
    HasManifest,
    Foreign,
}

async fn prefix_state(object_store: &Arc<dyn ObjectStore>, path: &str) -> PrefixState {
    let prefix: Path = path.split('/').filter(|part| !part.is_empty()).collect();
    let mut listing = object_store.list(Some(&prefix));
    let mut foreign = false;
    while let Some(entry) = listing.next().await {
        match entry {
            Ok(object) if object.location.as_ref().contains("manifest") => {
                return PrefixState::HasManifest;
            }
            Ok(object) => {
                warn!(
                    path,
                    found = %object.location,
                    "refusing to create a store: the prefix already holds objects"
                );
                foreign = true;
            }
            Err(err) => {
                warn!(
                    path,
                    error = %err,
                    "refusing to create a store: the prefix could not be listed"
                );
                foreign = true;
            }
        }
    }

    if foreign {
        PrefixState::Foreign
    } else {
        PrefixState::Empty
    }
}

/// The outcome of probing a store before opening it read-write: a reader over
/// an existing store, an empty prefix to bootstrap, or a peer mid-creation
/// whose half-written manifest reads no consistent version yet — a race the
/// caller waits out rather than a store to bootstrap or refuse.
enum ProbeOutcome {
    Reader(Box<(DbReader, Arc<CacheCounters>)>),
    Empty,
    Racing,
}

/// One materialized head, with what a slot-backed attach adds.
struct HeadRead {
    view: CatalogSnapshot,
    /// The unfolded tail's writes; `None` on a single-topology store.
    tail: Option<moraine_wal::Overlay>,
    /// Set when the view came from a reader the materialization opened for
    /// itself rather than the session's.
    reader: Option<DbReader>,
}

/// A probe's read: the session it scans through and the head it resolves
/// against.
struct ProbeRead {
    session: ReadSession,
    head: HeadRead,
}

impl ProbeRead {
    /// The handle to scan entries through: the reader the head's view came
    /// from, which after a hole retry is not the session's.
    fn handle(&self) -> ReadHandle<'_> {
        match &self.head.reader {
            Some(reader) => ReadHandle::Reader(reader),
            None => self.session.handle(),
        }
    }

    fn view(&self) -> &CatalogSnapshot {
        &self.head.view
    }

    /// The stamp this probe reads at, both halves.
    fn head_value(&self) -> crate::store::proto::HeadValue {
        crate::store::proto::HeadValue {
            snapshot_id: self.head.view.snapshot.snapshot_id,
            batch_seq: self.head.view.batch_seq,
        }
    }

    fn tail(&self) -> Option<&moraine_wal::Overlay> {
        self.head.tail.as_ref()
    }

    /// Releases both the session and any reader the head opened.
    async fn finish(self) {
        slot_commit::release_reader(self.head.reader.as_ref()).await;
        self.session.finish();
    }
}

/// A raw dump's read: the scan handle it reads through and, on a slot-backed
/// attach, the unfolded tail to overlay so a winner no folder has applied yet
/// is not missed. Released by [`finish`](DumpRead::finish).
pub(crate) enum DumpRead {
    /// A pinned slot-log head: scans read through `reader` (or the one the
    /// head opened past a truncation) overlaid with the tail.
    Slots {
        reader: Arc<DbReader>,
        head: Box<crate::transaction::slot_commit::SlotHead>,
    },
}

impl DumpRead {
    /// The handle raw scans read through.
    pub(crate) fn handle(&self) -> ReadHandle<'_> {
        let Self::Slots { reader, head } = self;
        head.handle(ReadHandle::Reader(reader))
    }

    /// The unfolded tail to overlay, `Some` for the overlay-accepting scan
    /// functions this feeds.
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn overlay(&self) -> Option<&moraine_wal::Overlay> {
        let Self::Slots { head, .. } = self;
        Some(&head.overlay)
    }

    /// The stamp this dump reads at, both halves: a maintenance batch reuses
    /// the snapshot id, so the batch count is what says the store moved.
    pub(crate) fn head_value(&self) -> crate::store::proto::HeadValue {
        let Self::Slots { head, .. } = self;
        crate::store::proto::HeadValue {
            snapshot_id: head.view.snapshot.snapshot_id,
            batch_seq: head.view.batch_seq,
        }
    }

    /// Releases the reader a hole retry opened.
    pub(crate) async fn finish(self) {
        let Self::Slots { head, .. } = self;
        slot_commit::release_reader(head.reader.as_ref()).await;
    }
}

/// A point-in-time read of a slot-backed store's contention counters,
/// accumulated across every commit through a handle and its clones. They
/// measure slot-race pressure; a nonzero `races_lost` is the signal
/// contention-triggered forwarding reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Contention {
    /// Commits that won a slot.
    pub commits: u64,
    /// Slot races lost on the way to those wins.
    pub races_lost: u64,
    /// Commits that spent their retry budget without winning.
    pub exhaustions: u64,
}

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
        let Store::Slots(store) = self.store.as_ref();
        // The decoded head lives in the head cache, the served projections
        // beside it; both are this handle's decoded catalog.
        self.projections
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .estimated_bytes()
            .saturating_add(store.head_cache.estimated_bytes())
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
        let read = self.begin_probe().await?;
        let outcome = self.scan_recent_rows(&read, table, None).await;
        read.finish().await;

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
        let read = self.begin_probe().await?;
        let outcome = self
            .scan_recent_rows(&read, table, Some(snapshot.get()))
            .await;
        read.finish().await;

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
        let read = self.begin_probe().await?;
        let outcome = self.scan_live_inline_row_ids(&read, table).await;
        read.finish().await;

        outcome
    }

    /// The view at `at`, or at head when `None`. Time travel always
    /// materializes: the cache holds head views only.
    ///
    /// A migration rewrites the keyspace under any reader already attached, so
    /// this refuses one in progress the same way opening a session does — a
    /// cached head is no reason to serve a view of a store being rewritten.
    async fn view(&self, at: Option<u64>) -> Result<Arc<CatalogSnapshot>> {
        let Store::Slots(store) = self.store.as_ref();
        commit::refuse_mid_migration(ReadHandle::Reader(&store.reader)).await?;
        match at {
            None => {
                self.note_head_read();
                let head = slot_commit::cached_slot_head(store).await?;
                slot_commit::release_reader(head.reader.as_ref()).await;
                Ok(Arc::new(head.view))
            }
            Some(snapshot) => {
                let started = Instant::now();
                let view = slot_commit::materialize_slot_view_at(store, snapshot).await?;
                self.reads.materialized(started.elapsed());
                Ok(Arc::new(view))
            }
        }
    }

    /// One head read: the view, and the byte-level overlay of the slots no
    /// folder has applied — what a probe the projection does not model must
    /// read over the store.
    async fn head_view(&self) -> Result<HeadRead> {
        let Store::Slots(store) = self.store.as_ref();
        self.note_head_read();
        // A fresh reader: entry scans read folder-written entries — index
        // backfills above all — straight from the store, not the tail.
        let head = slot_commit::materialize_slot_head_fresh(store).await?;
        Ok(HeadRead {
            view: head.view,
            tail: Some(head.overlay),
            reader: head.reader,
        })
    }

    /// Opens a read session at the current head — the read-only reader shared
    /// with the catalog — the same isolation
    /// [`snapshot`](Self::snapshot)/[`snapshot_at`](Self::snapshot_at) use.
    ///
    /// Every read in the crate opens its session here, so this is where a
    /// store mid-structural-migration is refused. The check costs one point
    /// read per session and belongs here rather than at each call site: a
    /// reader that skips it scans a keyspace being rewritten under it and
    /// returns a catalog with a hole in it, and an open-time check cannot
    /// catch a migration that starts after the handle attached.
    pub(crate) async fn begin_read(&self) -> Result<ReadSession> {
        let Store::Slots(store) = self.store.as_ref();
        let session = ReadSession::Reader(store.reader.clone());

        if let Err(error) = commit::refuse_mid_migration(session.handle()).await {
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
        let Store::Slots(store) = self.store.as_ref();
        store.reader.close().await.map_err(Error::from)
    }

    /// The slot-log-backed store behind this handle — the leader role
    /// materializes heads, mints the secret, and races slots through it.
    #[cfg(feature = "leader")]
    pub(crate) fn slot_store(&self) -> &SlotStore {
        let Store::Slots(store) = self.store.as_ref();
        store
    }

    /// Opens a read session and materializes the head through it, so a probe's
    /// entry scans and the catalog they resolve against are one cut. Released
    /// by [`ProbeRead::finish`].
    async fn begin_probe(&self) -> Result<ProbeRead> {
        let session = self.begin_read().await?;
        match self.head_view().await {
            Ok(head) => Ok(ProbeRead { session, head }),
            Err(err) => {
                session.finish();
                Err(err)
            }
        }
    }

    /// Opens a read for the raw current+history dumps: a single read session
    /// on a single-topology store, or the pinned slot-log head (reader plus
    /// unfolded tail) on a slot-backed attach, so a dump taken mid-transaction
    /// reflects a winner no folder has applied yet.
    pub(crate) async fn begin_dump(&self) -> Result<DumpRead> {
        let Store::Slots(store) = self.store.as_ref();
        // A read-write attach reads through a reader opened for this dump, so a
        // folder-role write — a mid-migration marker above all — is seen and a
        // dump scanning a keyspace being rewritten refuses rather than serve a
        // catalog with a hole. A read-only attach cannot be mid-migration (its
        // open refuses one) and reads through the shared reader, whose warm
        // block cache keeps a run of dumps from rescanning the store.
        let head = if store.read_only {
            slot_commit::cached_slot_head(store).await?
        } else {
            slot_commit::materialize_slot_head_fresh(store).await?
        };
        if let Err(err) =
            commit::refuse_mid_migration(head.handle(ReadHandle::Reader(&store.reader))).await
        {
            slot_commit::release_reader(head.reader.as_ref()).await;
            return Err(err);
        }
        Ok(DumpRead::Slots {
            reader: store.reader.clone(),
            head: Box::new(head),
        })
    }

    /// Opens a read of commit-written catalog state: the cached slot head with
    /// its unfolded tail.
    ///
    /// A slot-committed record reaches a reader through the overlay, so
    /// serving one needs no reader of its own — where
    /// [`begin_dump`](Self::begin_dump) opens one per call because a
    /// folder-role write appears in neither the cached head nor the tail. A
    /// caller reading records this handle's own commits produce belongs here;
    /// one reading what a folder wrote does not.
    pub(crate) async fn begin_catalog_read(&self) -> Result<DumpRead> {
        let Store::Slots(store) = self.store.as_ref();
        let head = slot_commit::cached_slot_head(store).await?;
        if let Err(err) =
            commit::refuse_mid_migration(head.handle(ReadHandle::Reader(&store.reader))).await
        {
            slot_commit::release_reader(head.reader.as_ref()).await;
            return Err(err);
        }
        Ok(DumpRead::Slots {
            reader: store.reader.clone(),
            head: Box::new(head),
        })
    }

    /// Test-only: every stored `(key, value)` under `prefix`, read through a
    /// fresh slot head — the folded store overlaid with the unfolded tail — so
    /// both folder-written entries and slot-committed ones are seen.
    #[cfg(test)]
    pub(crate) async fn scan_prefix_overlaid(&self, prefix: Vec<u8>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let Store::Slots(store) = self.store.as_ref();
        let head = slot_commit::materialize_slot_head_fresh(store)
            .await
            .unwrap();
        let handle = head.handle(ReadHandle::Reader(&store.reader));
        let mut merged: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
            std::collections::BTreeMap::new();
        let mut iter = handle
            .scan_prefix(prefix.clone(), .., crate::store::handle::ScanShape::Bulk)
            .await
            .unwrap();
        while let Some(entry) = iter.next().await.unwrap() {
            merged.insert(entry.key.to_vec(), entry.value.to_vec());
        }
        for (key, value) in head.overlay.prefixed(&prefix) {
            match value {
                Some(bytes) => {
                    merged.insert(key.to_vec(), bytes.to_vec());
                }
                None => {
                    merged.remove(key);
                }
            }
        }
        slot_commit::release_reader(head.reader.as_ref()).await;
        merged.into_iter().collect()
    }

    /// A snapshot of this store's contention counters — commits, slot races
    /// lost, and budget exhaustions — accumulated across every commit through
    /// this handle and its clones. A nonzero `races_lost` is the signal
    /// contention-triggered forwarding reads; the counts measure slot-race
    /// pressure over the handle's life. Reads atomics only, never the store.
    #[must_use]
    pub fn contention(&self) -> Contention {
        let Store::Slots(store) = self.store.as_ref();
        store.contention.snapshot()
    }

    /// Resolves whether `transaction_id` committed, and at which snapshot: the
    /// exactly-once recovery surface for a committer that crashed mid-commit
    /// and must not double-apply. Scans the unfolded slot tail and the folded
    /// snapshot records above `floor`.
    ///
    /// `Some(snapshot)` is the snapshot the transaction committed at. `None`
    /// means the commit never landed and a retry is safe — but only when
    /// `floor` is at or below the head the original attempt validated against
    /// and no truncation has passed it; a `floor` above the truncation horizon
    /// could yield a false `None`. Both scans are bounded by truncation
    /// retention and slot expiry.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corruption`] if the scanned tail has a hole below the
    /// transaction — a possibly-committed transaction cannot be ruled out past
    /// a destroyed slot — or a store error if a scan fails.
    pub async fn transaction_outcome(
        &self,
        transaction_id: uuid::Uuid,
        floor: SnapshotId,
    ) -> Result<Option<SnapshotId>> {
        let Store::Slots(store) = self.store.as_ref();
        Ok(
            slot_commit::transaction_outcome(store, transaction_id.into_bytes(), floor.get())
                .await?
                .map(SnapshotId::new),
        )
    }
}

impl Catalog {
    /// Opens (creating and initializing if empty) the catalog in
    /// `object_store` at `options.path`.
    ///
    /// Several processes may hold a read-write catalog per store: commits
    /// arbitrate through the slot log rather than a single writer epoch. A
    /// legacy store is migrated to the slot-log format on the way in.
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
        let (reader, cache) =
            Box::pin(Self::open_read_write_reader(&object_store, &options)).await?;
        warn_if_metadata_cache_cannot_hold(
            &options.path,
            manifest.map(|manifest| manifest.metadata_bytes),
        );
        info!(
            path = options.path,
            poll_interval_ms = options.reader_poll_interval.as_millis(),
            "opened catalog read-write"
        );
        let projections = Arc::new(std::sync::RwLock::new(ProjectionCache::empty()));
        // A read-write attach always lands at the slot-log format: the open
        // bootstraps or migrates until it does.
        crate::catalog::projection::raise_format_floor(&projections, commit::FORMAT_MULTI_WRITER);
        let location = Arc::new(StoreLocation {
            path: options.path.clone(),
            object_store: located,
        });
        let slots = moraine_wal::SlotLog::new(Arc::clone(&object_store), &options.path);
        let coalescer = slot_commit::CommitCoalescer::new(options.commit_batch_window);
        Ok(Self {
            inner: ReadOnlyCatalog {
                store: Arc::new(Store::Slots(Box::new(SlotStore {
                    reader: Arc::new(reader),
                    slots,
                    object_store,
                    options,
                    read_only: false,
                    pinned: false,
                    coalescer,
                    projections: Arc::clone(&projections),
                    head_cache: slot_commit::HeadCache::default(),
                    contention: Arc::new(slot_commit::ContentionCounters::default()),
                    #[cfg(feature = "leader")]
                    forwarding: Arc::new(slot_commit::Forwarding::default()),
                }))),
                location,
                reads: Arc::new(ReadTally::default()),
                cache,
                data_reads: Arc::default(),
                projections,
            },
        })
    }

    /// Resolves a read-write attach to the reader it follows: bootstraps an
    /// empty store at [`commit::FORMAT_MULTI_WRITER`], attaches a store already
    /// there, migrates a legacy format 1–3 store in one atomic batch, and
    /// refuses a format newer than this binary understands. A migration whose
    /// write a racing migration fenced re-probes and finds the store converted.
    async fn open_read_write_reader(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
    ) -> Result<(DbReader, Arc<CacheCounters>)> {
        for attempt in 0..MIGRATION_ATTEMPTS {
            let (reader, cache) = match Self::open_probe_reader(object_store, options).await? {
                ProbeOutcome::Reader(reader) => *reader,
                // Empty, or a peer mid-creation: route to the writer path, which
                // SlateDB serializes by fencing. A concurrent creator can still
                // leave the store half-formed, so a failed bootstrap backs off
                // and re-probes — one racer wins and the rest adopt it.
                ProbeOutcome::Empty | ProbeOutcome::Racing => {
                    match Self::bootstrap_slot_reader(object_store, options).await {
                        Ok(opened) => return Ok(opened),
                        // A definitively lost race — a peer already created the
                        // store — names itself; surface it rather than retry.
                        Err(err @ Error::OpenRaced(_)) => return Err(err),
                        Err(_) if attempt + 1 < MIGRATION_ATTEMPTS => {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                1u64 << attempt.min(6),
                            ))
                            .await;
                            continue;
                        }
                        Err(err) => return Err(err),
                    }
                }
            };
            match Self::classify_format(&reader).await?.0 {
                FormatClass::SlotLog => return Ok((reader, cache)),
                FormatClass::Empty => {
                    reader.close().await.map_err(Error::from)?;
                    match Self::bootstrap_slot_reader(object_store, options).await {
                        Ok(opened) => return Ok(opened),
                        // A definitively lost race — a peer already created the
                        // store — names itself; surface it rather than retry.
                        Err(err @ Error::OpenRaced(_)) => return Err(err),
                        Err(_) if attempt + 1 < MIGRATION_ATTEMPTS => {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                1u64 << attempt.min(6),
                            ))
                            .await;
                        }
                        Err(err) => return Err(err),
                    }
                }
                FormatClass::TooNew(version) => {
                    reader.close().await.map_err(Error::from)?;
                    return Err(Error::Configuration(format!(
                        "store format {version} is newer than this binary understands \
                         (up to {}); upgrade moraine to attach it",
                        commit::MAX_FORMAT_VERSION
                    )));
                }
                FormatClass::Legacy => {
                    reader.close().await.map_err(Error::from)?;
                    let writer_store = StoreBuilder::new(&options.path, object_store.clone())
                        .cache_dir(options.cache_dir.clone())
                        .cache_size(options.cache_size)
                        .cache_memory(options.cache_memory)
                        .cache_preload(options.cache_preload)
                        .cache_puts(options.cache_puts);
                    match commit::migrate_to_slot_log(writer_store).await {
                        // Converted, or a racing migration converted it first:
                        // fall through to re-probe, which finds the slot format.
                        Ok(()) | Err(Error::Fenced(_)) => {}
                        Err(err) => return Err(err),
                    }
                }
            }
        }
        Err(Error::Fenced(
            "a legacy store's migration was fenced repeatedly and never converged; \
             retry the attach"
                .to_string(),
        ))
    }

    /// Opens a probe reader over the store: a reader when the prefix is
    /// readable, otherwise what the prefix holds — a known-empty prefix that
    /// licenses a bootstrap, or a peer mid-creation to wait out. A prefix
    /// holding objects whose manifest will not open is a damaged store, and
    /// the error propagates rather than stamping over it.
    async fn open_probe_reader(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
    ) -> Result<ProbeOutcome> {
        let reader_store = StoreBuilder::new(&options.path, object_store.clone())
            .poll_interval(options.reader_poll_interval)
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_memory(options.cache_memory)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts);
        match reader_store.open_reader().await {
            Ok(opened) => Ok(ProbeOutcome::Reader(Box::new(opened))),
            Err(err) => match prefix_state(object_store, &options.path).await {
                PrefixState::Empty => Ok(ProbeOutcome::Empty),
                PrefixState::HasManifest => Ok(ProbeOutcome::Racing),
                PrefixState::Foreign => Err(err),
            },
        }
    }

    /// Classifies a probe reader's stored structural format, with the version
    /// it read (zero when the store carries no stamp).
    async fn classify_format(reader: &DbReader) -> Result<(FormatClass, u64)> {
        let handle = ReadHandle::Reader(reader);
        if crate::store::read::read_migration(handle).await?.is_some() {
            return Err(Error::Migration(
                "store is mid-migration; refusing to open — Catalog::migrate resumes it from \
                 the durable cursor, and takes a store path rather than an open catalog, so it \
                 runs against a store no attach will touch"
                    .to_string(),
            ));
        }
        match crate::store::read::read_format(handle).await? {
            None => Ok((FormatClass::Empty, 0)),
            Some(format) if format.format_version == commit::FORMAT_MULTI_WRITER => {
                Ok((FormatClass::SlotLog, format.format_version))
            }
            Some(format) if format.format_version > commit::MAX_FORMAT_VERSION => Ok((
                FormatClass::TooNew(format.format_version),
                format.format_version,
            )),
            Some(format) if format.format_version < commit::MIN_FORMAT_VERSION => {
                Err(Error::Configuration(format!(
                    "store format {} predates this binary's minimum ({}); it cannot be attached",
                    format.format_version,
                    commit::MIN_FORMAT_VERSION
                )))
            }
            Some(format) => Ok((FormatClass::Legacy, format.format_version)),
        }
    }

    /// Bootstraps an empty store at the slot-log format through the writer,
    /// fencing any incumbent old-binary writer, then closes it and reopens
    /// read-only.
    async fn bootstrap_slot_reader(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
    ) -> Result<(DbReader, Arc<CacheCounters>)> {
        let writer_store = StoreBuilder::new(&options.path, object_store.clone())
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_memory(options.cache_memory)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts);
        let (db, _, _) = commit::open_initialized(
            writer_store,
            options.encrypted,
            options.data_path.as_deref(),
        )
        .await?;
        db.close().await.map_err(Error::from)?;

        let reader_store = StoreBuilder::new(&options.path, object_store.clone())
            .poll_interval(options.reader_poll_interval)
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_memory(options.cache_memory)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts);
        let (reader, cache, _) = commit::open_reader_initialized(reader_store).await?;

        Ok((reader, cache))
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
        let store = StoreBuilder::new(&options.path, Arc::clone(&object_store))
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_memory(options.cache_memory)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts)
            .poll_interval(options.reader_poll_interval)
            .checkpoint(checkpoint);

        // A store with no manifest fails to open at all; the failure propagates
        // (a read-only attach never bootstraps).
        let (reader, cache) = store.open_reader().await?;
        let format = match Self::classify_format(&reader).await {
            // Format 1–7 all serve read-only.
            Ok((FormatClass::SlotLog | FormatClass::Legacy, version)) => version,
            Ok((FormatClass::Empty, _)) => {
                slot_commit::release_reader(Some(&reader)).await;
                return Err(Error::Corruption(
                    "store is not an initialized moraine catalog; a read-only attach \
                     needs a writer to have created it first"
                        .to_string(),
                ));
            }
            // A too-new format is a compatibility problem, not corruption — the
            // same kind the read-write path refuses it with.
            Ok((FormatClass::TooNew(version), _)) => {
                slot_commit::release_reader(Some(&reader)).await;
                return Err(Error::Configuration(format!(
                    "store format {version} is newer than this binary understands \
                     (up to {}); upgrade moraine to attach it",
                    commit::MAX_FORMAT_VERSION
                )));
            }
            Err(err) => {
                slot_commit::release_reader(Some(&reader)).await;
                return Err(err);
            }
        };
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

        // Every readable store serves read-only through the slot topology: a
        // legacy format 1–3 store never migrates (a read-only attach writes
        // nothing), and its absent fold cursor reads as 0 with an empty tail,
        // so it is a slot store with no slots.
        let location = Arc::new(StoreLocation {
            path: options.path.clone(),
            object_store: located,
        });
        let slots = moraine_wal::SlotLog::new(Arc::clone(&object_store), &options.path);
        let coalescer = slot_commit::CommitCoalescer::new(options.commit_batch_window);
        let pinned = options.checkpoint.is_some();
        Ok(ReadOnlyCatalog {
            store: Arc::new(Store::Slots(Box::new(SlotStore {
                reader: Arc::new(reader),
                slots,
                object_store,
                options,
                read_only: true,
                pinned,
                coalescer,
                projections: Arc::clone(&projections),
                head_cache: slot_commit::HeadCache::default(),
                contention: Arc::new(slot_commit::ContentionCounters::default()),
                #[cfg(feature = "leader")]
                forwarding: Arc::new(slot_commit::Forwarding::default()),
            }))),
            location,
            reads: Arc::new(ReadTally::default()),
            cache,
            data_reads: Arc::default(),
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
        // A checkpoint captures the folded store and a pinned reader never
        // replays the tail, so every committed slot is folded in before the cut
        // is taken.
        let Store::Slots(store) = self.store.as_ref();
        if !store.read_only {
            folder::fold_sprint(store, u64::MAX).await?;
        }
        let id = self
            .with_writer(async |db| open::create_checkpoint(db, lifetime).await)
            .await?;
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
        let Store::Slots(store) = self.store.as_ref();
        store.coalescer.commit(store, &f).await
    }

    /// Whether direct-store writes are available: a read-write slot-backed
    /// attach (through the folder role). A read-only attach refuses.
    fn ensure_writable(&self) -> Result<()> {
        match self.store.as_ref() {
            Store::Slots(store) if !store.read_only => Ok(()),
            Store::Slots(_) => Err(Error::Constraint(
                "catalog opened read-only; writes are unavailable".to_string(),
            )),
        }
    }

    /// Runs `body` against a writable `Db` for a direct-store maintenance
    /// write: a read-write attach opens a fenced folder session for it — the
    /// one process allowed to write the store directly — and a read-only attach
    /// refuses.
    pub(crate) async fn with_writer<T, F>(&self, body: F) -> Result<T>
    where
        F: AsyncFnOnce(&Db) -> Result<T>,
    {
        match self.store.as_ref() {
            Store::Slots(store) if !store.read_only => folder::with_folder(store, body).await,
            Store::Slots(_) => Err(Error::Constraint(
                "catalog opened read-only; writes are unavailable".to_string(),
            )),
        }
    }

    /// Runs `body` against the writable folder `Db`, for tests that plant
    /// direct-store keys under a live handle.
    #[cfg(test)]
    pub(crate) async fn with_folder_writer<T, F>(&self, body: F) -> Result<T>
    where
        F: AsyncFnOnce(&Db) -> Result<T>,
    {
        self.with_writer(body).await
    }

    /// Test-only: writes `entries` straight into the folded store under the
    /// folder role — the state a completed fold would leave, so the folder-role
    /// sweep can be exercised before a fold implementation lands.
    #[cfg(test)]
    pub(crate) async fn seed_folded_writes(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) {
        self.with_writer(async |db| {
            let tx = db
                .begin(slatedb::IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            for (key, value) in &entries {
                tx.put(key.clone(), value.clone()).map_err(Error::from)?;
            }
            commit::commit_durably(db, tx).await.map_err(Error::from)?;
            Ok(())
        })
        .await
        .unwrap();
    }

    /// Opens a staged-row transaction: materializes the head and pins it, so
    /// the staged batch races one slot at commit. A read-only attach returns
    /// [`Error::Constraint`].
    pub(crate) async fn begin_staged(
        &self,
        data_store: Option<data_file::DataStore>,
        data_prefix: String,
    ) -> Result<crate::transaction::staged::StagedTransaction> {
        use crate::transaction::staged::StagedTransaction;

        let Store::Slots(store) = self.store.as_ref();
        if store.read_only {
            return Err(Error::Constraint(
                "catalog attached read-only; writes are unavailable".to_string(),
            ));
        }
        let head = slot_commit::materialize_slot_head(store).await?;
        let transaction = StagedTransaction::begin_slots(
            head,
            store.reader.clone(),
            store.slots.clone(),
            data_store,
            data_prefix,
            store.head_cache.clone(),
            Arc::clone(&store.contention),
            self.data_reads(),
            Arc::clone(self.projections()),
        );

        // A lost race arms forwarding: this fresh re-drive opens forwarded when a
        // leader is reachable, direct otherwise. An uncontended transaction never
        // reaches this and never connects.
        #[cfg(feature = "leader")]
        let transaction = match slot_commit::forward_target(store).await? {
            Some(target) => transaction.forwarded(slot_commit::forward_context(store, target)),
            None => transaction,
        };

        Ok(transaction)
    }

    /// One bounded fold pass: opens the fenced writer, applies up to `limit`
    /// unfolded slots — each as one atomic batch advancing the fold cursor —
    /// and closes, leaving the store an accurate index of the log that far.
    /// Folding is invisible to readers: the served state is byte-identical
    /// before and after. Resuming after a crash re-reads the durable cursor and
    /// never re-applies a folded slot.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] on a read-only attach, [`Error::Fenced`]
    /// if a concurrent folder fenced this session, [`Error::Corruption`] if the
    /// tail has a hole or a slot does not chain, or a store error.
    pub async fn fold_sprint(&self, limit: u64) -> Result<FoldReport> {
        let store = self.folder_store()?;
        folder::fold_sprint(store, limit).await
    }

    /// Slots committed but not yet folded — the clock-free staleness signal.
    /// One tail-length probe from the fold cursor: existence checks, no slot
    /// bodies fetched.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] on a read-only attach, or a store error.
    pub async fn unfolded_tail(&self) -> Result<u64> {
        let store = self.folder_store()?;
        folder::unfolded_tail(store).await
    }

    /// The self-appointment rule: if the unfolded tail exceeds `threshold`,
    /// wait `delay` plus jitter, re-check fold progress, and sprint only if no
    /// other folder advanced the cursor. `Ok(None)` means this handle stood
    /// down — the tail was short, or a peer folded during the wait.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] on a read-only attach, [`Error::Fenced`]
    /// if a concurrent folder fenced this session, or a store error.
    pub async fn fold_if_stalled(
        &self,
        threshold: u64,
        delay: Duration,
        limit: u64,
    ) -> Result<Option<FoldReport>> {
        let store = self.folder_store()?;
        folder::fold_if_stalled(store, threshold, delay, limit).await
    }

    /// Deletes slots durably folded into the store, oldest first, and returns
    /// how many were removed. The horizon is the lower of two bounds: the fold
    /// cursor as seen by the attach's reader (durable by construction — a
    /// reader cannot see a memtable), and the fold cursor the oldest live
    /// reader still sits at, held back by a retention margin. Truncation is
    /// conservative garbage collection: it never deletes a slot a reader
    /// still needs to resolve state, so it may remove nothing when readers
    /// lag.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] on a read-only attach, or a store error.
    pub async fn truncate_folded_slots(&self) -> Result<u64> {
        let store = self.folder_store()?;
        folder::truncate_folded_slots(store).await
    }

    /// The slot store a folder surface runs against, refusing a read-only
    /// attach: folding and the staleness signal are the writer's monopoly.
    fn folder_store(&self) -> Result<&SlotStore> {
        let Store::Slots(store) = self.store.as_ref();
        if store.read_only {
            return Err(Error::Constraint(
                "catalog attached read-only; folding is unavailable".to_string(),
            ));
        }
        Ok(store)
    }
}
