//! The catalog handle: the entry point a host opens, reads, and commits
//! through.

use std::{
    collections::{HashMap, HashSet},
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectStore, path::Path};
use slatedb::{CloseReason, Db, DbReader, DbStatus, DbTransaction, IsolationLevel};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::{
    catalog::{
        BuildStep, CatalogSnapshot, ColumnId, ColumnOrder, DataFileId, FileIndexEntry, IndexDef,
        IndexEntry, IndexId, IndexInfo, IndexMaintenance, IndexState, RecentRow, SnapshotId,
        TableId, Timestamp,
        census::{
            CensusRequest, CompactStoreReport, CompactStoreRequest, CompactionTarget, LiveCount,
            MergeOutcome, StoreCensus, StoreObjects, SubspaceCensus, SubspaceMerge, SubspaceName,
        },
        inline::{InlineScanKind, materialize_inline_rows},
        projection::{
            ProjectionCache, cache_epoch, cached_head_view, held_head_view, install_head_view_at,
            install_shared_current_entities, shared_current_entities,
        },
        scoped_read,
    },
    error::{Error, Result},
    store::{
        StagedBytes,
        cache::{CacheCounters, CacheTally, ObjectStoreTally},
        census::{self as store_census, SegmentSize},
        compaction::{self as store_compaction, MergeEnd},
        handle::{ReadHandle, ReadSession, ScanShape},
        index_encoding::{
            CanonicalKey, Direction, IndexKeyValue, NullOrder, encode_ordered_values,
        },
        inline as store_inline,
        key::{
            IndexKey, IndexKind, InlineOperation, Key, Subspace, index_index_prefix,
            index_kind_prefix, subspace_prefix,
        },
        open::{self, StoreBuilder},
        proto::HeadValue,
    },
    transaction::{MigrationReport, Transaction, commit, index_maintenance, migration},
};

/// Exact probes kept in flight by one batched index lookup.
const INDEX_LOOKUP_CONCURRENCY: usize = 32;

/// One subspace's row, zeroed when the manifest carries no segment for it.
fn measure(subspace: SubspaceName, segment: Option<&SegmentSize>) -> SubspaceCensus {
    SubspaceCensus {
        subspace,
        bytes: segment.map_or(0, |segment| segment.bytes),
        l0_ssts: segment.map_or(0, |segment| segment.l0_ssts),
        sorted_runs: segment.map_or(0, |segment| segment.sorted_runs),
        sorted_run_ssts: segment.map_or(0, |segment| segment.sorted_run_ssts),
        live: None,
    }
}

/// Counts the live entries of each measured subspace under one read
/// session.
async fn count_live_entries(
    handle: ReadHandle<'_>,
    subspaces: &mut [SubspaceCensus],
) -> Result<()> {
    for measured in subspaces {
        // An unknown segment addresses no keys this build can decode, so
        // there is nothing to scan and no count to report.
        let Some(subspace) = measured.subspace.subspace() else {
            continue;
        };
        let tally = store_census::scan_live(handle, subspace).await?;
        measured.live = Some(LiveCount {
            keys: tally.keys,
            key_bytes: tally.key_bytes,
            value_bytes: tally.value_bytes,
            scheduled_files: tally.scheduled_files,
        });
    }

    Ok(())
}

/// The physical bytes `census` recorded for `subspace`, or zero if it
/// carries no such subspace.
fn bytes_of(census: &StoreCensus, subspace: &SubspaceName) -> u64 {
    census
        .subspaces
        .iter()
        .find(|measured| &measured.subspace == subspace)
        .map_or(0, |measured| measured.bytes)
}

/// How many times a staged build re-derives after losing a race before
/// giving up.
const BUILD_DERIVATION_ATTEMPTS: usize = 8;

#[expect(
    clippy::cast_precision_loss,
    reason = "the percentage is diagnostic; f64 is exact for every practical row count"
)]
fn build_progress_percent(completed: usize, total: usize) -> f64 {
    if total == 0 {
        100.0
    } else {
        completed as f64 * 100.0 / total as f64
    }
}

/// The per-column orders `orders` asks for, as a definition records them.
/// An empty list means ascending / NULLS LAST throughout.
fn requested_orders(orders: &[ColumnOrder], columns: usize) -> (Vec<Direction>, Vec<NullOrder>) {
    (0..columns)
        .map(|position| {
            orders
                .get(position)
                .map_or((Direction::Ascending, NullOrder::Last), |order| {
                    (order.direction, order.nulls)
                })
        })
        .unzip()
}

/// One streaming derivation pass's bounded entry buffer.
struct BuildStepBuffer<'a> {
    catalog: &'a Catalog,
    table: TableId,
    index: IndexId,
    index_name: &'a str,
    bound: BuildStep,
    derivation_attempt: usize,
    total_entries: usize,
    entries: Vec<IndexEntry>,
    nominal_bytes: u64,
    pending_source: Option<(u64, u64)>,
    completed_entries: usize,
    peak_buffered_entries: usize,
}

impl BuildStepBuffer<'_> {
    fn cover_source(&mut self, file_id: u64, position: u64) {
        self.pending_source = Some((file_id, position));
    }

    async fn push(&mut self, entry: IndexEntry, source: Option<(u64, u64)>) -> Result<()> {
        let entry_bytes = entry.nominal_bytes();
        let full = !self.entries.is_empty()
            && (self.entries.len() >= self.bound.entries
                || self.nominal_bytes.saturating_add(entry_bytes) > self.bound.bytes);
        if full {
            self.flush(false).await?;
        }

        self.nominal_bytes = self.nominal_bytes.saturating_add(entry_bytes);
        self.entries.push(entry);
        if let Some((file_id, position)) = source {
            self.cover_source(file_id, position);
        }
        self.peak_buffered_entries = self.peak_buffered_entries.max(self.entries.len());
        Ok(())
    }

    async fn flush(&mut self, is_final: bool) -> Result<()> {
        let entries = std::mem::take(&mut self.entries);
        self.nominal_bytes = 0;
        let step_entries = entries.len();
        let build_cursor = entries.last().map(|entry| entry.row_id);
        let source = self.pending_source;
        let commit_started = Instant::now();

        self.catalog
            .commit(|tx| {
                tx.build_index_source_step(self.index, &entries, is_final, source)
                    .map(|_| ())
            })
            .await?;

        let state = self
            .catalog
            .snapshot()
            .await?
            .indexes_of(self.table)
            .into_iter()
            .find(|index| index.id == self.index)
            .ok_or_else(|| Error::NotFound(format!("index {}", self.index)))?
            .state;
        if state == IndexState::Poisoned {
            return Err(Error::Constraint(format!(
                "index {} was poisoned by a duplicate value",
                self.index
            )));
        }

        self.completed_entries = self.completed_entries.saturating_add(step_entries);
        info!(
            table = self.table.get(),
            index = self.index.get(),
            index_name = %self.index_name,
            derivation_attempt = self.derivation_attempt,
            step_entries,
            completed_entries = self.completed_entries,
            total_entries = self.total_entries,
            progress_percent = build_progress_percent(
                self.completed_entries,
                self.total_entries,
            ),
            build_cursor = ?build_cursor,
            source_file = ?source.map(|cursor| cursor.0),
            source_position = ?source.map(|cursor| cursor.1),
            is_final,
            commit_ms = commit_started.elapsed().as_secs_f64() * 1_000.0,
            "staged index build step committed"
        );
        Ok(())
    }
}

/// Filters one file's dead or legacy-covered rows before buffering its live
/// entries. The physical cursor advances across filtered rows too.
struct BuildFileConsumer<'a, 'b> {
    buffer: &'a mut BuildStepBuffer<'b>,
    file_id: u64,
    dead_positions: Option<&'a HashSet<u64>>,
    dead_row_ids: Option<&'a HashSet<u64>>,
    legacy_row_cursor: Option<u64>,
}

impl scoped_read::ScopedEntryBatchConsumer for BuildFileConsumer<'_, '_> {
    async fn consume(
        &mut self,
        entries: Vec<scoped_read::ScopedReadEntry>,
        first_ordinal: u64,
    ) -> Result<()> {
        for (offset, entry) in entries.into_iter().enumerate() {
            let offset = u64::try_from(offset).map_err(|_| {
                Error::Corruption("scoped-read batch position exceeds u64".to_owned())
            })?;
            let ordinal = first_ordinal.saturating_add(offset);
            let dead = self
                .dead_positions
                .is_some_and(|positions| positions.contains(&ordinal))
                || self
                    .dead_row_ids
                    .is_some_and(|rows| rows.contains(&entry.row_id));
            let covered = self
                .legacy_row_cursor
                .is_some_and(|cursor| entry.row_id <= cursor);
            if !dead && !covered {
                self.buffer
                    .push(
                        IndexEntry {
                            row_id: entry.row_id,
                            values: entry.values,
                        },
                        Some((self.file_id, ordinal)),
                    )
                    .await?;
            } else {
                self.buffer.cover_source(self.file_id, ordinal);
            }
        }
        Ok(())
    }
}

/// What a maintenance pass should reclaim.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaintenanceRequest {
    /// Reclaim the entry ranges of indexes that are no longer live —
    /// orphaned by `drop_index`, or by a `drop_table` that ended the
    /// table's indexes with it.
    pub sweep_orphaned_index_entries: bool,
    /// Maximum entries deleted per commit. Each batch is one atomic
    /// write; the pass yields between them so a large reclamation never
    /// holds the writer. Must be nonzero.
    pub batch_size: usize,
}

impl Default for MaintenanceRequest {
    fn default() -> Self {
        Self {
            sweep_orphaned_index_entries: true,
            batch_size: 1024,
        }
    }
}

/// How a [`Catalog::migrate`] call should run.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct MigrationRequest {
    /// Take a whole-store checkpoint before the first rewrite and release it
    /// once the last finish batch is durable, leaving a manual recovery point
    /// if the migration fails partway. Off by default: migrations are
    /// one-way and there is no automatic rollback, so the checkpoint is the
    /// sanctioned recovery path when an operator wants one.
    pub checkpoint: bool,
}

/// What a maintenance pass reclaimed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaintenanceReport {
    /// Dead indexes whose entry ranges were reclaimed.
    pub indexes_swept: u64,
    /// Entry keys deleted across those ranges.
    pub index_entries_reclaimed: u64,
}

/// One step reported by a completed maintenance pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStatusStep {
    /// Maintenance operation name.
    pub step: String,
    /// Outcome name, such as `ran`, `skipped`, or `failed`.
    pub status: String,
    /// Human-readable outcome detail.
    pub detail: String,
}

impl MaintenanceStatusStep {
    /// Builds one reported maintenance step.
    #[must_use]
    pub fn new(
        step: impl Into<String>,
        status: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            step: step.into(),
            status: status.into(),
            detail: detail.into(),
        }
    }
}

/// One completed maintenance pass in the catalog's durable status history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStatusPass {
    /// When the pass began.
    pub started_at: Timestamp,
    /// What triggered the pass, such as `scheduled` or `manual`.
    pub trigger: String,
    /// Steps in execution order.
    pub steps: Vec<MaintenanceStatusStep>,
}

impl MaintenanceStatusPass {
    /// Builds one completed maintenance pass.
    #[must_use]
    pub fn new(
        started_at: Timestamp,
        trigger: impl Into<String>,
        steps: Vec<MaintenanceStatusStep>,
    ) -> Self {
        Self {
            started_at,
            trigger: trigger.into(),
            steps,
        }
    }
}

/// How a handle has served its reads, for the diagnostics a slow attach
/// needs.
///
/// A materialization is meant to be rare — one per handle, then the cache
/// serves — so the count is the diagnostic: a handle reporting hundreds is
/// rebuilding the catalog per read, which no amount of reclaiming the store
/// would fix.
#[derive(Debug, Default)]
struct ReadTally {
    materializations: AtomicU64,
    refreshes: AtomicU64,
    cache_hits: AtomicU64,
    head_reads: AtomicU64,
    materialize_micros: AtomicU64,
    // How many times each half of the shared record set was actually
    // scanned from the store. At one head each should move at most once,
    // however many consumers read at it — the tally is what tests pin
    // that with.
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
            elapsed_ms = elapsed.as_secs_f64() * 1_000.0,
            materializations = count,
            total_micros = total,
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
enum Store {
    /// The single read-write writer.
    Writer(Db),
    /// A read-only reader following the manifest, shared into read sessions.
    Reader(Arc<DbReader>),
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
    /// Local directory backing the block cache's disk tier, which holds
    /// data blocks evicted from memory. When set, warm queries skip repeat
    /// object-store GETs — worthwhile for remote (`s3://`) stores,
    /// redundant for local ones. Process-wide, like
    /// [`cache_size`](Self::cache_size): the first catalog to open decides
    /// whether there is a disk tier at all, and where. `None` (the
    /// default) keeps the cache in memory.
    pub cache_dir: Option<std::path::PathBuf>,
    /// How many bytes of disk the block cache's device may hold, for the
    /// whole process rather than per catalog: one cache is shared by every
    /// store a process opens, and the first to open sizes it. `None` (the
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
///
/// The choice is between paying for freshness and paying for everything:
/// the newest objects are what a writer's own next reads want, while a
/// whole store is what a query session wants and is only affordable when
/// the store is small enough to sit on the local disk.
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
///
/// The load stops at the first object that would exceed the cap and says
/// nothing about having stopped, so an attach that silently warms half a
/// store looks exactly like one that warmed all of it. Diagnostics only:
/// a manifest that cannot be read here is left to the open itself to
/// report, and nothing about the open changes either way.
async fn warn_if_preload_cannot_fit(options: &CatalogOptions, object_store: Arc<dyn ObjectStore>) {
    if options.cache_preload != Some(CachePreload::All) || options.cache_dir.is_none() {
        return;
    }
    let Ok(store_bytes) = store_census::manifest_bytes(&options.path, object_store).await else {
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
///
/// `Sync` so a grouped commit is as spawnable as a lone one; an ordinary
/// closure satisfies that on its own.
pub type CommitMember<'a> = &'a (dyn Fn(&mut Transaction) -> Result<()> + Sync);

/// The read surface of a moraine catalog: cheap to clone, drives every
/// read. This is what a read-only attach hands back, and what a
/// read-write [`Catalog`] derefs to, so the reads are written once and
/// both modes serve them.
///
/// It carries no mutator at all. That is the point: a `commit` against a
/// catalog opened read-only is a compile error rather than a runtime
/// [`Error::Constraint`], so the mode a handle was opened in is visible in
/// its type. The storage substrate never appears in this API — a catalog
/// lives in a bucket reachable through any [`ObjectStore`].
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
    // Shared across handle clones: how this attach has served its reads.
    reads: Arc<ReadTally>,
    // Shared across handle clones: what the process's block cache has
    // served for this attach in particular.
    cache: Arc<CacheCounters>,
    // Where the store lives. Retained because the census and the store
    // merge reach SlateDB's admin surface, which addresses a store by path
    // rather than through an open handle.
    location: Arc<StoreLocation>,
    // Shared across handle clones: decoded projections folded forward on
    // commit, served without rescanning when their head matches.
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
    // Shared across handle clones: where concurrent commits meet so
    // several of them become one batch and one flush.
    commits: Arc<commit::Coalescer>,
    // The writer `Db`'s status channel, so a read served from the held view
    // can check the fence without opening a transaction to do it. `None` on
    // a read-only handle, which holds no writer to lose.
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
/// `Deref`, so a writer serves the whole read surface without restating
/// it; what this type adds is the mutators. Exactly one process may hold
/// one per store.
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

/// The whole read surface, without restating a method of it. Field access
/// through the deref is what lets the mutators below read `self.store` and
/// `self.commits` unchanged.
impl std::ops::Deref for Catalog {
    type Target = ReadOnlyCatalog;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ReadOnlyCatalog {
    /// Returns the last 16 completed maintenance passes, newest first.
    ///
    /// Status is stored in the catalog, so a read-only attach and a process
    /// that reopens the store see passes completed by the previous writer.
    /// Recording status does not create a DuckLake snapshot.
    ///
    /// # Errors
    ///
    /// Returns a store or decoding error if the durable status cannot be
    /// read.
    pub async fn maintenance_status(&self) -> Result<Vec<MaintenanceStatusPass>> {
        crate::transaction::maintenance_status::read(self).await
    }

    /// The maintained-projection state shared by this handle's clones.
    pub(crate) fn projections(&self) -> &Arc<std::sync::RwLock<ProjectionCache>> {
        &self.projections
    }

    /// What the block cache has served for **this catalog** since it
    /// attached — metadata and data blocks counted apart, because they are
    /// sized apart.
    ///
    /// The cache itself is the process's: every catalog a process attaches
    /// reads through one instance under one budget. These are that
    /// instance's numbers for this attach alone, which is what tells a
    /// busy catalog from an idle one sharing it;
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

    /// Physical object-store requests this catalog has issued since it
    /// attached.
    ///
    /// Counts include retries. Moraine's WAL objects use its main physical
    /// store today, so their requests appear under `main_*`; `wal_*` is for
    /// a separately configured WAL store. Durations are summed request
    /// latency and can exceed wall-clock time when requests overlap.
    #[must_use]
    pub fn object_store_tally(&self) -> ObjectStoreTally {
        self.cache.object_store_tally()
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
    /// closed for any other reason.
    ///
    /// The fence check a read served from the held view performs in place
    /// of opening a session. SlateDB reports a close by setting it on the
    /// status channel this handle subscribed to at open, so reading it is a
    /// borrow of a watch value — no store read, no manifest copy, and no
    /// entry in the transaction manager, which is what opening a session
    /// takes a global write lock to make.
    ///
    /// Same reach as a session's: both fail exactly once the `Db` is
    /// closed, because closing it is what the fence does. A handle that
    /// skipped this would serve its cache past its own displacement, and
    /// quietly.
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

    /// The held view a read may serve without resolving the head from the
    /// store, or `None` when it must resolve it there.
    ///
    /// A read-write handle holds the writer epoch, so this process is the
    /// store's only writer: nothing can move `sys/head` under it, and every
    /// batch that lands either folds the held view forward or drops it — a
    /// durable write whose fate is unknown drops it too. The held view is
    /// therefore never behind the store, and its own stamp is the head.
    ///
    /// Paired with [`refuse_if_closed`](Self::refuse_if_closed), never
    /// served alone: that this handle still holds the writer is the premise
    /// of the paragraph above, not a cost it can skip.
    ///
    /// A read-only handle follows another process's commits and has no
    /// such premise, so it always reads.
    pub(crate) fn writer_head_view(&self) -> Option<Arc<CatalogSnapshot>> {
        if !self.holds_the_writer() {
            return None;
        }
        held_head_view(&self.projections)
    }

    /// Whether this handle is the store's writer, and so the only thing
    /// that can move `sys/head`.
    pub(crate) fn holds_the_writer(&self) -> bool {
        matches!(self.store.as_ref(), Store::Writer(_))
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
    /// These rows are not part of a [`CatalogSnapshot`]: they are row data,
    /// not catalog metadata, so they are served from the `inline` subspace
    /// on demand — one contiguous range scan per call — rather than
    /// materialized into a view every reader of the catalog shares. Rows of
    /// one chunk share one body and rows of one schema version share one
    /// schema, so each set of bytes is read and carried once.
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
    /// Only rows still inlined are found. A flush drains the chunks it
    /// consumes, so past snapshots of flushed rows read from the backdated
    /// data file [`CatalogSnapshot::data_files_of`] serves, never from here
    /// — which is what keeps the rows in exactly one place at every
    /// snapshot.
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

    /// The inline rows of `table` live at `at` (head, when `None`), read
    /// through an open session.
    async fn scan_recent_rows(
        &self,
        session: &ReadSession,
        table: TableId,
        at: Option<u64>,
    ) -> Result<Vec<RecentRow>> {
        let handle = session.handle();
        commit::refuse_mid_migration(handle).await?;
        // Head takes no id and so needs no resolution; a requested snapshot
        // is resolved exactly as `snapshot_at` resolves one.
        let read_at = match at {
            Some(_) => commit::resolve_read_snapshot(handle, at).await?.0,
            None => commit::read_head_id(handle).await?,
        };
        let chunks = store_inline::scan_inline_chunks(handle, table.get()).await?;
        let tombstones = store_inline::scan_inline_deletes(handle, table.get()).await?;

        let live = InlineScanKind::Table.select(
            &materialize_inline_rows(&chunks, &tombstones),
            read_at,
            0,
        );
        // One body per referenced chunk and one schema per referenced
        // version, however many rows point at them.
        let mut bodies: HashMap<usize, Arc<Vec<u8>>> = HashMap::new();
        let mut schemas: HashMap<u64, Arc<Vec<u8>>> = HashMap::new();
        let mut rows = Vec::with_capacity(live.len());
        for row in live {
            let (operation, chunk) = &chunks[row.chunk];
            // Every chunk a row was materialized from is an insert.
            let InlineOperation::Insert { schema_version, .. } = operation else {
                return Err(Error::Corruption(format!(
                    "inline row {} of table {table} references a non-insert chunk",
                    row.row_id
                )));
            };
            let arrow_schema = if let Some(schema) = schemas.get(schema_version) {
                Arc::clone(schema)
            } else {
                let schema = store_inline::read_inline_schema(handle, table.get(), *schema_version)
                    .await?
                    .ok_or_else(|| {
                        Error::Corruption(format!(
                            "no inline schema for table {table} version {schema_version}"
                        ))
                    })?;
                let schema = Arc::new(schema.arrow_schema.to_vec());
                schemas.insert(*schema_version, Arc::clone(&schema));
                schema
            };
            let chunk_body = Arc::clone(
                bodies
                    .entry(row.chunk)
                    .or_insert_with(|| Arc::new(chunk.body.to_vec())),
            );

            rows.push(RecentRow {
                row_id: row.row_id,
                begin_snapshot: SnapshotId::new(row.begin_snapshot),
                schema_version: *schema_version,
                offset_in_chunk: row.offset_in_chunk,
                chunk_body,
                arrow_schema,
            });
        }

        Ok(rows)
    }

    /// Time travel always materializes: the cache holds head views only, and
    /// a past snapshot is reconstructed from `history` rather than advanced
    /// from a newer state.
    ///
    /// A read-only catalog caches too. It folds no batch of its own — it has
    /// none — so it advances by replaying the changelog of the commits it
    /// missed, and the head stamp its cached view carries tells it exactly
    /// which store state it stands at.
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
    /// else a fresh materialization. A read-only handle polling a quiet
    /// catalog pays one point read and no copy: the committer folds each
    /// batch forward, so the cache is normally current. A read-write
    /// handle does not reach here at all while its view is held —
    /// [`writer_head_view`](Self::writer_head_view) serves it first.
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

    /// Resolves an equality lookup to the rows currently holding `values`.
    ///
    /// Head-only: the lookup materializes the current head and scans the
    /// `index` subspace under one read session, so the entries and the catalog
    /// they resolve against are one consistent cut. Entries are live-only,
    /// so there is no time-travel variant. Returns stable row ids; the caller
    /// applies delete files as any DuckLake scan does.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the index does not exist,
    /// [`Error::IndexBuilding`] if its staged backfill has not completed,
    /// [`Error::Constraint`] if a value exceeds the size cap, or a store
    /// error if the scan fails.
    pub async fn index_lookup(
        &self,
        table: TableId,
        index: IndexId,
        values: &[IndexKeyValue],
    ) -> Result<Vec<u64>> {
        self.index_lookup_many(table, index, &[values.to_vec()])
            .await
    }

    /// Resolves an `IN` lookup to the union of rows currently holding any
    /// complete equality key in `keys`.
    ///
    /// Duplicate keys are probed once and duplicate row ids are returned
    /// once. An empty key set returns no rows after validating that the index
    /// exists and is ready. The whole batch uses one read session and one head
    /// view, so every result belongs to the same consistent cut.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the index does not exist,
    /// [`Error::IndexBuilding`] if its staged backfill has not completed,
    /// [`Error::Constraint`] if a key is partial or exceeds the size cap, or
    /// a store error if a probe fails.
    pub async fn index_lookup_many(
        &self,
        table: TableId,
        index: IndexId,
        keys: &[Vec<IndexKeyValue>],
    ) -> Result<Vec<u64>> {
        let session = self.begin_read().await?;
        let handle = session.handle();

        let outcome = crate::store::read::consistent(handle, || async {
            // The head view, not a fresh materialization: a probe that
            // rematerializes re-scans `current` under a bulk shape, which
            // admits no blocks, so every lookup batch pays a store read for a
            // view the handle already holds. The scan the probe actually
            // needs is the `index` one below, and that one is warm.
            let view = self.head_view(handle).await?;
            let info = view
                .index_by_id(table, index)
                .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

            match info.state {
                IndexState::Ready => {}
                IndexState::Building | IndexState::Maintaining => {
                    return Err(Error::IndexBuilding(format!(
                        "index {index} is still building"
                    )));
                }
                IndexState::Poisoned => {
                    return Err(Error::NotFound(format!("index {index} was poisoned")));
                }
            }

            let mut encoded = Vec::with_capacity(keys.len());
            for key in keys {
                if key.len() != info.columns.len() {
                    return Err(Error::Constraint(format!(
                        "index lookup: {} values do not address the {}-column index {index}; an \
                         equality lookup names every column",
                        key.len(),
                        info.columns.len()
                    )));
                }
                encoded.push(encode_ordered_values(
                    &key.iter().cloned().map(Some).collect::<Vec<_>>(),
                    &info.directions,
                    &info.nulls,
                )?);
            }
            encoded.sort_unstable();
            encoded.dedup();

            // Exact probes are independent. Keep enough in flight to hide an
            // object-store round trip while bounding iterator and result
            // memory for a large key list. `buffered` preserves canonical-key
            // order, though callers must not depend on result order.
            let row_id_groups = stream::iter(encoded.into_iter().map(|key| async move {
                index_maintenance::lookup_row_ids(handle, index.get(), info.unique, &key).await
            }))
            .buffered(INDEX_LOOKUP_CONCURRENCY);
            let mut row_id_groups = std::pin::pin!(row_id_groups);

            let mut seen = HashSet::new();
            let mut row_ids = Vec::new();
            while let Some(group) = row_id_groups.try_next().await? {
                row_ids.extend(group.into_iter().filter(|row_id| seen.insert(*row_id)));
            }
            Ok(row_ids)
        })
        .await;
        session.finish();

        outcome
    }

    /// Resolves a comparison query to the rows whose indexed value falls
    /// between `lower` and `upper` (`<`, `<=`, `>`, `>=`, `BETWEEN`, and their
    /// half-open forms via [`Bound::Unbounded`]). Each bound names the leading
    /// columns' values; equality is the degenerate closed `[v, v]` range.
    ///
    /// Head-only and candidate-returning, exactly like
    /// [`index_lookup`](Self::index_lookup): the scan and the catalog it
    /// resolves against are one consistent cut, and the caller applies delete
    /// files. Results are in the index's stored order, or its exact opposite
    /// when `reverse` is set. Both directions stream from the store in the
    /// requested order.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the index does not exist,
    /// [`Error::IndexBuilding`] if its staged backfill has not completed,
    /// [`Error::Constraint`] if a bound value exceeds the size cap, or a
    /// store error if the scan fails.
    pub async fn index_range(
        &self,
        table: TableId,
        index: IndexId,
        lower: Bound<Vec<IndexKeyValue>>,
        upper: Bound<Vec<IndexKeyValue>>,
        reverse: bool,
    ) -> Result<Vec<u64>> {
        let session = self.begin_read().await?;
        let handle = session.handle();

        let outcome = async {
            // The head view, not a fresh materialization: a probe that
            // rematerializes re-scans `current` under a bulk shape, which
            // admits no blocks, so every lookup pays a store read for a
            // view the handle already holds. The scan the probe actually
            // needs is the `index` one below, and that one is warm.
            let view = self.head_view(handle).await?;
            let info = view
                .index_by_id(table, index)
                .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

            match info.state {
                IndexState::Ready => {}
                IndexState::Building | IndexState::Maintaining => {
                    return Err(Error::IndexBuilding(format!(
                        "index {index} is still building"
                    )));
                }
                IndexState::Poisoned => {
                    return Err(Error::NotFound(format!("index {index} was poisoned")));
                }
            }

            let (byte_lower, byte_upper) = encode_range_bounds(&info, index, lower, upper)?;

            // NULL placement is per column, and only the leading column's
            // flag byte bounds the scan's open sides.
            let leading_nulls = info.nulls.first().copied().unwrap_or(NullOrder::Last);

            let row_ids = index_maintenance::range_row_ids(
                handle,
                index.get(),
                info.unique,
                leading_nulls,
                byte_lower,
                byte_upper,
                crate::store::handle::ScanOrder::from_reverse(reverse),
            )
            .await?;
            Ok(row_ids)
        }
        .await;
        session.finish();

        outcome
    }

    /// Resolves an `IS NULL` query to the rows whose leading indexed columns
    /// match `prefix` — a leading run of `Some(value)` (equality) and `None`
    /// (`IS NULL`) predicates, e.g. `[None]` for `a IS NULL` or
    /// `[Some(5), None]` for `a = 5 AND b IS NULL`. The prefix must cover the
    /// leading columns contiguously and name at least one `IS NULL`; a gap
    /// (an unconstrained leading column) is not expressible, so a bare
    /// non-leading `IS NULL` is not served — use a scan filter for that.
    ///
    /// Head-only and candidate-returning like
    /// [`index_lookup`](Self::index_lookup).
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the index does not exist,
    /// [`Error::IndexBuilding`] while its staged backfill runs, or
    /// [`Error::Constraint`] if the prefix is empty, longer than the index,
    /// or names no `IS NULL`.
    pub async fn index_nulls(
        &self,
        table: TableId,
        index: IndexId,
        prefix: Vec<Option<IndexKeyValue>>,
        reverse: bool,
    ) -> Result<Vec<u64>> {
        let session = self.begin_read().await?;
        let handle = session.handle();

        let outcome = async {
            // The head view, not a fresh materialization: a probe that
            // rematerializes re-scans `current` under a bulk shape, which
            // admits no blocks, so every lookup pays a store read for a
            // view the handle already holds. The scan the probe actually
            // needs is the `index` one below, and that one is warm.
            let view = self.head_view(handle).await?;
            let info = view
                .index_by_id(table, index)
                .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

            match info.state {
                IndexState::Ready => {}
                IndexState::Building | IndexState::Maintaining => {
                    return Err(Error::IndexBuilding(format!(
                        "index {index} is still building"
                    )));
                }
                IndexState::Poisoned => {
                    return Err(Error::NotFound(format!("index {index} was poisoned")));
                }
            }

            if prefix.is_empty() || prefix.len() > info.columns.len() {
                return Err(Error::Constraint(format!(
                    "index_nulls: a prefix of {} predicates does not fit the {}-column index \
                     {index}",
                    prefix.len(),
                    info.columns.len()
                )));
            }
            if prefix.iter().all(Option::is_some) {
                return Err(Error::Constraint(
                    "index_nulls: the prefix names no IS NULL; use index_lookup for pure equality"
                        .to_owned(),
                ));
            }

            let key = encode_ordered_values(&prefix, &info.directions, &info.nulls)?;
            let row_ids = index_maintenance::null_prefix_row_ids(
                handle,
                index.get(),
                &key,
                crate::store::handle::ScanOrder::from_reverse(reverse),
            )
            .await?;
            Ok(row_ids)
        }
        .await;
        session.finish();

        outcome
    }

    /// Opens a read session at the current head — a read-write transaction or
    /// the read-only reader — the same isolation
    /// [`snapshot`](Self::snapshot)/[`snapshot_at`](Self::snapshot_at) use.
    /// Used by [`crate::ffi_support`]'s raw current+history dumps and inline
    /// scans; every other reader goes through `snapshot`/`snapshot_at`.
    ///
    /// Every read of the store opens its session here, so this is where a
    /// store mid-structural-migration is refused. The check costs one point
    /// read per session and belongs here rather than at each call site: a
    /// reader that skips it scans a keyspace being rewritten under it and
    /// returns a catalog with a hole in it, and an open-time check cannot
    /// catch a migration that starts after the handle attached.
    pub(crate) async fn begin_read(&self) -> Result<ReadSession> {
        let session = match self.store.as_ref() {
            Store::Writer(db) => ReadSession::Tx(
                db.begin(IsolationLevel::Snapshot)
                    .await
                    .map_err(Error::from)?,
            ),
            Store::Reader(reader) => ReadSession::Reader(reader.clone()),
        };

        if let Err(error) = commit::refuse_mid_migration(session.handle()).await {
            session.finish();
            return Err(error);
        }

        Ok(session)
    }

    /// Derives the index entries for a file the extension path registers, by
    /// scoped-reading it — DuckLake supplies none, so moraine reads them.
    /// The caller resolves each of the index's columns to its physical
    /// position in the file (through the column-mapping rules) and passes
    /// them in the index's column order. The returned entries feed
    /// [`Transaction::register_data_file`] so registration stays covered.
    ///
    /// The file must not carry an embedded row-id column — its rows already
    /// have ids, and re-registering them under a fresh dense range would
    /// fork their identity — so such a file is refused.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corruption`] if the file cannot be read or a column
    /// type does not match its Parquet type, or [`Error::Constraint`] for a
    /// non-indexable column type or a file carrying an embedded row-id
    /// column.
    pub async fn scoped_file_index_entries(
        &self,
        object_store: Arc<dyn ObjectStore>,
        path: &Path,
        index: IndexId,
        indexed_positions: &[usize],
    ) -> Result<Vec<FileIndexEntry>> {
        let entries = scoped_read::scoped_read_entries(
            object_store,
            path,
            indexed_positions,
            scoped_read::ScopedRows::All,
            scoped_read::RowIdSource::Ordinal,
            None,
        )
        .await?;
        Ok(entries
            .into_iter()
            .map(|entry| FileIndexEntry {
                index,
                // Ordinal-sourced ids are positions the registration
                // re-maps onto its freshly allocated dense range.
                ordinal: entry.row_id,
                values: entry.values,
            })
            .collect())
    }

    /// Backfills an index over a table's live data by scoped-reading every
    /// live file from `object_store` (the `DATA_PATH` store) and deriving one
    /// entry per row — the extension-path build for a table that already
    /// holds data. The returned entries feed `create_index`'s backfill.
    /// Indexed columns are located by resolving each field id to its physical
    /// position (the file's columns follow the table's column order).
    ///
    /// Row ids resolve per file: the embedded row-id column when the file
    /// carries one (rewrite and flush output), else `row_id_start +
    /// ordinal`. Rows already dead — named by a delete file's positions or an
    /// inline file-delete's row ids — are excluded, so entries stay live-only.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or a column is not live,
    /// [`Error::Constraint`] for a non-indexable type, or
    /// [`Error::Corruption`] if a file cannot be read or names no row-id
    /// source.
    pub async fn scoped_backfill_entries(
        &self,
        object_store: Arc<dyn ObjectStore>,
        data_prefix: &str,
        table: TableId,
        columns: &[ColumnId],
    ) -> Result<Vec<IndexEntry>> {
        let session = self.begin_read().await?;

        let outcome = async {
            let snapshot = commit::materialize(session.handle(), None).await?;
            // `columns_of` is ordered by the column's ordinal, so a column's
            // 0-based index here is its physical position in a file written
            // under this schema — the mapping the scoped read needs. (Ordinals
            // are 1-based in the stored value, so the stored order can't be
            // used directly.)
            let live_columns = snapshot.columns_of(table);
            let positions = columns
                .iter()
                .map(|column| {
                    live_columns
                        .iter()
                        .position(|c| c.id == *column)
                        .ok_or_else(|| Error::NotFound(format!("column {column} of table {table}")))
                })
                .collect::<Result<Vec<_>>>()?;

            let table_prefix = snapshot.table_data_prefix(table)?;
            let resolve = |path: &str, is_relative: bool| {
                let relative = match (is_relative, data_prefix.is_empty()) {
                    (false, _) => path.to_owned(),
                    (true, true) => format!("{table_prefix}{path}"),
                    (true, false) => format!("{data_prefix}/{table_prefix}{path}"),
                };
                object_store::path::Path::from(relative.as_str())
            };

            // Rows already dead when the index is built must not be backfilled
            // (entries are live-only): delete files name positions within their
            // target, inline file-deletes name row ids.
            let mut killed_positions: HashMap<u64, HashSet<u64>> = HashMap::new();
            let mut killed_row_ids: HashMap<u64, HashSet<u64>> = HashMap::new();
            for (data_file_id, row_id, _) in
                store_inline::scan_inline_file_deletes(session.handle(), table.get()).await?
            {
                killed_row_ids
                    .entry(data_file_id)
                    .or_default()
                    .insert(row_id);
            }
            for delete in snapshot.delete_files_of(table) {
                let path = resolve(&delete.path, delete.path_is_relative);
                let positions =
                    scoped_read::delete_file_positions(object_store.as_ref(), &path).await?;
                killed_positions
                    .entry(delete.data_file_id.get())
                    .or_default()
                    .extend(positions);
            }

            let mut entries = Vec::new();
            for file in snapshot.data_files_of(table) {
                let path = resolve(&file.path, file.path_is_relative);
                let scoped = scoped_read::scoped_read_entries_with_footer(
                    Arc::clone(&object_store),
                    &path,
                    &positions,
                    scoped_read::ScopedRows::All,
                    scoped_read::RowIdSource::Resolve {
                        row_id_start: file.row_id_start,
                    },
                    Some(file.file_size_bytes),
                    Some(file.footer_size),
                )
                .await?;
                let dead_positions = killed_positions.get(&file.id.get());
                let dead_row_ids = killed_row_ids.get(&file.id.get());
                entries.extend(
                    scoped
                        .into_iter()
                        .enumerate()
                        .filter_map(|(ordinal, entry)| {
                            let ordinal = u64::try_from(ordinal).unwrap_or(u64::MAX);
                            let dead = dead_positions.is_some_and(|dead| dead.contains(&ordinal))
                                || dead_row_ids.is_some_and(|dead| dead.contains(&entry.row_id));
                            (!dead).then_some(IndexEntry {
                                row_id: entry.row_id,
                                values: entry.values,
                            })
                        }),
                );
            }
            Ok(entries)
        }
        .await;
        session.finish();

        outcome
    }

    /// Backfill entries for a table's live **inline** rows, by scanning its
    /// inline chunks — the counterpart to [`Self::scoped_backfill_entries`]
    /// for rows moraine holds in the store rather than external files.
    /// Tombstoned (inline-deleted) rows are excluded; a NULL indexed value
    /// yields a `None`, so `IS NULL` finds the row. Reads the catalog store,
    /// so it needs no data object store.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if a column is not live, or [`Error::Corruption`]
    /// if a chunk names no recorded schema or cannot be decoded.
    pub async fn inline_backfill_entries(
        &self,
        table: TableId,
        columns: &[ColumnId],
    ) -> Result<Vec<IndexEntry>> {
        let session = self.begin_read().await?;

        let outcome = async {
            let snapshot = commit::materialize(session.handle(), None).await?;
            let live_columns = snapshot.columns_of(table);
            let positions = columns
                .iter()
                .map(|column| {
                    live_columns
                        .iter()
                        .position(|c| c.id == *column)
                        .ok_or_else(|| Error::NotFound(format!("column {column} of table {table}")))
                })
                .collect::<Result<Vec<_>>>()?;

            // A tombstone ends only versions begun before it. UPDATE can
            // reinsert the same row id in the tombstone's snapshot, and that
            // newer value is live and must be indexed.
            let dead: HashMap<u64, u64> =
                store_inline::scan_inline_deletes(session.handle(), table.get())
                    .await?
                    .into_iter()
                    .map(|(row_id, deletion)| (row_id, deletion.end_snapshot))
                    .collect();

            let mut entries = Vec::new();
            let mut schemas: HashMap<u64, arrow::datatypes::SchemaRef> = HashMap::new();
            for (op, chunk) in
                store_inline::scan_inline_chunks(session.handle(), table.get()).await?
            {
                let InlineOperation::Insert {
                    schema_version,
                    begin_snapshot,
                    ..
                } = op
                else {
                    continue;
                };
                let schema = if let Some(schema) = schemas.get(&schema_version) {
                    Arc::clone(schema)
                } else {
                    let record = store_inline::read_inline_schema(
                        session.handle(),
                        table.get(),
                        schema_version,
                    )
                    .await?
                    .ok_or_else(|| {
                        Error::Corruption(format!(
                            "no inline schema for table {table} version {schema_version}"
                        ))
                    })?;
                    let schema = scoped_read::decode_inline_schema(record.arrow_schema)?;
                    schemas.insert(schema_version, Arc::clone(&schema));
                    schema
                };
                let scoped = scoped_read::inline_batch_entries(
                    schema,
                    &chunk.body,
                    &positions,
                    chunk.row_id_start,
                )?;
                entries.extend(
                    scoped
                        .into_iter()
                        .filter(|entry| {
                            dead.get(&entry.row_id)
                                .is_none_or(|end_snapshot| begin_snapshot >= *end_snapshot)
                        })
                        .map(|entry| IndexEntry {
                            row_id: entry.row_id,
                            values: entry.values,
                        }),
                );
            }
            Ok(entries)
        }
        .await;
        session.finish();

        outcome
    }

    /// What the store weighs, subspace by subspace.
    ///
    /// The default request reads the store's manifest and nothing else —
    /// two object reads, a cost independent of how large the store is — and
    /// reports physical bytes, SST counts, and sorted-run counts per
    /// subspace. Those figures include superseded versions and tombstones,
    /// which is the point: the gap between them and the live count is what
    /// [`compact_store`](Catalog::compact_store) reclaims.
    ///
    /// Setting [`CensusRequest::count_live_entries`] adds a scan of every
    /// subspace, which costs a full read of the store.
    ///
    /// Available on a read-only catalog: both legs read, neither writes.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest or, for the scanning leg, the store
    /// cannot be read.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, CensusRequest, SubspaceName};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// catalog.commit(|tx| tx.create_schema("sales").map(|_| ())).await?;
    ///
    /// let census = catalog.store_census(CensusRequest::default()).await?;
    /// // Physical figures count what has been written out, so a store whose
    /// // commits are still in the write-ahead log reports nothing yet.
    /// assert!(census.total_bytes() >= 0);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn store_census(&self, request: CensusRequest) -> Result<StoreCensus> {
        let physical = store_census::read_manifest_census(
            &self.location.path,
            Arc::clone(&self.location.object_store),
        )
        .await?;

        // Every subspace is reported, whether or not the manifest carries a
        // segment for it: a subspace absent from the manifest is one whose
        // writes have not been written out, which is a measurement rather
        // than a reason to omit the row. Two censuses of one store are then
        // always comparable row by row.
        let mut subspaces: Vec<SubspaceCensus> = Subspace::ALL
            .into_iter()
            .map(|subspace| {
                let prefix = subspace_prefix(subspace);
                measure(SubspaceName::from(subspace), physical.segment(&prefix))
            })
            .collect();
        subspaces.extend(
            physical
                .segments
                .iter()
                .filter(|segment| {
                    matches!(
                        SubspaceName::of_prefix(&segment.prefix),
                        SubspaceName::Unknown(_)
                    )
                })
                .map(|segment| measure(SubspaceName::of_prefix(&segment.prefix), Some(segment))),
        );

        if request.count_live_entries {
            // One session for every subspace, so the counts are one
            // consistent cut rather than a sequence of unrelated ones.
            let session = self.begin_read().await?;
            let counted = count_live_entries(session.handle(), &mut subspaces).await;
            session.finish();
            counted?;
        }

        Ok(StoreCensus {
            manifest_id: physical.manifest_id,
            subspaces,
            objects: physical.objects.map(|totals| StoreObjects {
                total_objects: totals.total_objects,
                total_bytes: totals.total_bytes,
                wal_objects: totals.wal_objects,
                wal_bytes: totals.wal_bytes,
                manifest_objects: totals.manifest_objects,
                manifest_bytes: totals.manifest_bytes,
                sst_objects: totals.sst_objects,
                sst_bytes: totals.sst_bytes,
                other_objects: totals.other_objects,
                other_bytes: totals.other_bytes,
            }),
        })
    }

    /// The lowest index id at or after `from` holding an entry of `kind`,
    /// or `None` past the last one. One seek per distinct index present —
    /// the scan stops at the first key rather than walking the range.
    pub(crate) async fn first_index_id_from(
        &self,
        kind: IndexKind,
        from: u64,
    ) -> Result<Option<u64>> {
        let kind_prefix = index_kind_prefix(kind);
        let start = index_index_prefix(kind, from);
        // `scan_prefix` takes its bounds as a suffix of the prefix.
        let suffix = start[kind_prefix.len()..].to_vec();

        let session = self.begin_read().await?;
        let first = session
            .handle()
            .scan_prefix(kind_prefix, suffix.., ScanShape::Probe)
            .await
            .map_err(Error::from)?
            .next()
            .await
            .map_err(Error::from)?;
        session.finish();

        let Some(entry) = first else {
            return Ok(None);
        };
        match Key::decode(&entry.key)? {
            Key::Index(IndexKey::Unique { index_id, .. } | IndexKey::Multi { index_id, .. }) => {
                Ok(Some(index_id))
            }
            other => Err(Error::Corruption(format!(
                "key in the index subspace decoded as {other:?}"
            ))),
        }
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
            Store::Writer(db) => db.close().await.map_err(Error::from),
            Store::Reader(reader) => reader.close().await.map_err(Error::from),
        }
    }
}

/// Maps a value window onto the byte bounds one index scan answers, refusing
/// the shapes no single scan can.
///
/// Each bound names a leading run of the index's columns. The last named is
/// the range column; its direction decides whether value order runs with or
/// against byte order.
fn encode_range_bounds(
    info: &IndexInfo,
    index: IndexId,
    lower: Bound<Vec<IndexKeyValue>>,
    upper: Bound<Vec<IndexKeyValue>>,
) -> Result<(Bound<CanonicalKey>, Bound<CanonicalKey>)> {
    // How many columns a bound names; `None` when it names none at all.
    let named_len = |bound: &Bound<Vec<IndexKeyValue>>| match bound {
        Bound::Included(values) | Bound::Excluded(values) => Some(values.len()),
        Bound::Unbounded => None,
    };
    let (lower_len, upper_len) = (named_len(&lower), named_len(&upper));

    // A bound naming no column describes no window: its encoding is the empty
    // key, whose extension bound spans the whole index.
    if lower_len == Some(0) || upper_len == Some(0) {
        return Err(Error::Constraint(format!(
            "index_range: a bound of index {index} must name at least one column"
        )));
    }

    // A bound naming more columns than the index has would encode components
    // no stored key carries, silently returning the wrong rows.
    let widest = lower_len.unwrap_or(0).max(upper_len.unwrap_or(0));
    if widest > info.columns.len() {
        return Err(Error::Constraint(format!(
            "index_range: a bound of {widest} values does not fit the {}-column index {index}",
            info.columns.len()
        )));
    }

    // A tuple window needs one byte range. Columns sharing a direction give
    // one; mixed directions do not, so both bounds must name the same leading
    // values and differ only in the last.
    let range_column = widest.saturating_sub(1);
    let mixed_directions = info
        .directions
        .iter()
        .take(widest)
        .any(|direction| Some(direction) != info.directions.first());
    let pinned_prefix = match (&lower, &upper) {
        (
            Bound::Included(low) | Bound::Excluded(low),
            Bound::Included(high) | Bound::Excluded(high),
        ) => low.len() == high.len() && low[..range_column] == high[..range_column],
        _ => false,
    };
    if mixed_directions && !pinned_prefix {
        return Err(Error::Constraint(format!(
            "index_range: index {index} names columns of differing sort directions, so both \
             bounds must pin the same leading values and compare on the last column"
        )));
    }

    let encode_bound = |bound: Bound<Vec<IndexKeyValue>>| -> Result<Bound<_>> {
        let encode = |values: Vec<IndexKeyValue>| {
            encode_ordered_values(
                &values.into_iter().map(Some).collect::<Vec<_>>(),
                &info.directions,
                &info.nulls,
            )
        };
        Ok(match bound {
            Bound::Included(values) => Bound::Included(encode(values)?),
            Bound::Excluded(values) => Bound::Excluded(encode(values)?),
            Bound::Unbounded => Bound::Unbounded,
        })
    };

    // A descending range column reverses value order, so the value-lower
    // bound is the byte-upper bound and vice versa.
    let descending = info.directions.get(range_column).copied() == Some(Direction::Descending);
    let (byte_lower, byte_upper) = if descending {
        (upper, lower)
    } else {
        (lower, upper)
    };
    Ok((encode_bound(byte_lower)?, encode_bound(byte_upper)?))
}

impl Catalog {
    /// Durably records a completed maintenance pass.
    ///
    /// The history retains the newest 16 passes. This is an unversioned
    /// catalog update: it does not advance the DuckLake snapshot head.
    ///
    /// # Errors
    ///
    /// Returns an error if the status cannot be encoded or durably written.
    pub async fn record_maintenance_pass(&self, pass: MaintenanceStatusPass) -> Result<()> {
        crate::transaction::maintenance_status::record(self, pass).await
    }

    /// The underlying store handle.
    ///
    /// A `Catalog` is only ever built around a `Store::Writer` — that is
    /// what makes it a `Catalog` rather than a [`ReadOnlyCatalog`] — so the
    /// reader arm is unreachable by construction. It still returns a
    /// `Result` rather than asserting, because the invariant lives in the
    /// two constructors above rather than in the type of the field, and a
    /// wrong answer here would be a silent write to the wrong handle.
    fn writer(&self) -> Result<&Db> {
        match self.store.as_ref() {
            Store::Writer(db) => Ok(db),
            Store::Reader(_) => Err(Error::Constraint(
                "catalog opened read-only; writes are unavailable".to_string(),
            )),
        }
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
        warn_if_preload_cannot_fit(&options, Arc::clone(&object_store)).await;
        let located = Arc::clone(&object_store);
        let store = StoreBuilder::new(&options.path, object_store)
            .flush_interval(options.flush_interval)
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_memory(options.cache_memory)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts);
        let (db, cache) =
            commit::open_initialized(store, options.encrypted, options.data_path.as_deref())
                .await?;
        info!(
            path = options.path,
            flush_interval_ms = options.flush_interval.as_millis(),
            "opened catalog read-write"
        );
        let projections = Arc::new(std::sync::RwLock::new(ProjectionCache::empty()));
        Ok(Self {
            inner: ReadOnlyCatalog {
                writer_status: Some(db.subscribe()),
                store: Arc::new(Store::Writer(db)),
                location: Arc::new(StoreLocation {
                    path: options.path,
                    object_store: located,
                }),
                reads: Arc::new(ReadTally::default()),
                cache,
                commits: Arc::new(commit::Coalescer::new(Arc::clone(&projections))),
                projections,
            },
        })
    }

    /// Opens the catalog **read-only** in `object_store` at `options.path`,
    /// as a `DbReader` following the latest manifest — or, when
    /// [`CatalogOptions::checkpoint`] is set, pinned to that checkpoint.
    ///
    /// A read-only catalog never opens the writer `Db`, so it never fences a
    /// live read-write process — any number of read-only catalogs may attach
    /// alongside the one writer. It never bootstraps: opening a
    /// store no writer has initialized is refused. [`commit`](Self::commit)
    /// returns [`Error::Constraint`].
    ///
    /// "Read-only" is a catalog property, not an IAM one: following the
    /// latest state means writing a checkpoint into the manifest on open and
    /// refreshing it while the catalog lives, so those credentials still
    /// need manifest write access. A catalog opened against a checkpoint
    /// writes nothing whatsoever, and in exchange reads the fixed cut that
    /// checkpoint names — later commits never appear, however long it stays
    /// open.
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
        warn_if_preload_cannot_fit(&options, Arc::clone(&object_store)).await;
        let located = Arc::clone(&object_store);
        let store = StoreBuilder::new(&options.path, object_store)
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_memory(options.cache_memory)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts)
            .poll_interval(options.reader_poll_interval)
            .checkpoint(checkpoint);

        let (reader, cache) = commit::open_reader_initialized(store).await?;
        info!(
            path = options.path,
            checkpoint = options.checkpoint,
            "opened catalog read-only"
        );
        let projections = Arc::new(std::sync::RwLock::new(ProjectionCache::empty()));
        Ok(ReadOnlyCatalog {
            writer_status: None,
            store: Arc::new(Store::Reader(Arc::new(reader))),
            location: Arc::new(StoreLocation {
                path: options.path,
                object_store: located,
            }),
            reads: Arc::new(ReadTally::default()),
            cache,
            commits: Arc::new(commit::Coalescer::new(Arc::clone(&projections))),
            projections,
        })
    }

    /// Rewrites the store in place to the newest structural format this
    /// binary understands, resuming an interrupted migration if one is in
    /// flight, and reports what it did.
    ///
    /// Deliberately **not** part of opening a catalog. A structural rewrite
    /// walks the keyspace and holds the single writer for its duration, so
    /// it is the operator's explicit choice, never a side effect of someone
    /// attaching with a newer binary. It takes the writer epoch exactly as
    /// [`open`](Self::open) does, so it fences a running catalog and is
    /// itself fenced by one — exactly one migrator runs.
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

        // A failed migration keeps its checkpoint: it is the recovery point
        // the operator asked for, and releasing it here would discard the one
        // thing that makes the failure recoverable.
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
    /// [`CatalogOptions::checkpoint`] opens a reader against — the one way to
    /// read a moraine catalog with credentials that cannot write at all,
    /// since a reader that follows the latest state maintains a checkpoint of
    /// its own and so writes the manifest.
    ///
    /// It also pins every object it references against SlateDB's garbage
    /// collection, so a checkpoint with no `lifetime` holds storage until
    /// [`delete_checkpoint`](Self::delete_checkpoint) removes it. Give one a
    /// lifetime unless something will.
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
    /// Free-standing rather than a method, exactly as
    /// [`migrate`](Self::migrate) is: it CASes the manifest and never opens
    /// the writer `Db`, so it runs against a live catalog without fencing
    /// it. Readers already open against the deleted checkpoint keep
    /// serving; a reader that opens against it afterwards is refused.
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
    /// Free-standing for the same reason
    /// [`delete_checkpoint`](Self::delete_checkpoint) is: it reads the
    /// manifest and never opens the writer `Db`, so it runs against a live
    /// catalog without fencing it. A checkpoint given no lifetime pins
    /// what it references until it is deleted, so this is how an operator
    /// finds one whose id was lost — reader-established checkpoints show
    /// up here too, and are not theirs to delete.
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
    pub(crate) async fn begin_write_tx(&self) -> Result<DbTransaction> {
        self.writer()?
            .begin(IsolationLevel::Snapshot)
            .await
            .map_err(Error::from)
    }

    /// Creates an index by a staged (multi-commit) build, driving it to
    /// `ready` before returning — for a table whose backfill exceeds what
    /// one commit may stage.
    ///
    /// The definition lands `building` in its own commit; each pass then
    /// derives the table's live entries (external files through
    /// `data_store`, inline rows from the catalog store) in durable source
    /// order and commits them in steps bounded by `step`. Writers maintain
    /// entries from the first commit forward.
    ///
    /// Interrupting the call leaves the definition `building`: calling again
    /// with the same `def` resumes from the persisted cursor, and
    /// [`Transaction::drop_index`](crate::Transaction::drop_index) abandons
    /// the build. A concurrent write to the table conflicts with a step,
    /// which re-derives at a fresh snapshot rather than staging entries for
    /// rows the winner deleted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::AlreadyExists`] if the table already holds a ready
    /// index of this name, or [`Error::Constraint`] if either `step` bound
    /// is zero, the resumed definition differs from `def`, or the rows
    /// duplicate a unique value. A failed build drops its definition.
    pub async fn create_index_staged(
        &self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: &str,
        step: Option<BuildStep>,
    ) -> Result<IndexId> {
        self.create_index_staged_with_maintenance(
            table,
            def,
            orders,
            IndexMaintenance::Synchronous,
            data_store,
            data_prefix,
            step,
        )
        .await
    }

    /// Creates an index by a staged build with the requested upkeep mode.
    /// Deferred upkeep is available only to non-unique indexes.
    ///
    /// # Errors
    ///
    /// As [`Self::create_index_staged`], plus [`Error::Constraint`] when
    /// deferred upkeep is requested for a unique index.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_index_staged_with_maintenance(
        &self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        maintenance: IndexMaintenance,
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: &str,
        step: Option<BuildStep>,
    ) -> Result<IndexId> {
        let step = step.unwrap_or_default();
        if step.entries == 0 || step.bytes == 0 {
            return Err(Error::Constraint(
                "a staged build's step must admit at least one entry and one byte".to_owned(),
            ));
        }

        let index = self
            .begin_staged_index(table, def, orders, maintenance)
            .await?;
        let outcome = self
            .drive_staged_build(table, def, index, data_store, data_prefix, step)
            .await;

        // A build that cannot finish leaves no half-covered index behind.
        // A cleanup that itself fails is logged, never substituted for the
        // failure that caused it.
        if outcome.is_err()
            && let Err(cleanup) = self.commit(|tx| tx.drop_index(index)).await
        {
            warn!(
                index = index.get(),
                error = %cleanup,
                "could not drop the definition of a failed staged build"
            );
        }
        outcome.map(|()| index)
    }

    /// Repairs every deferred non-unique index awaiting upkeep, using the
    /// same bounded streaming driver as an initial staged build.
    ///
    /// Returns the number of definitions flipped back to ready. A failed
    /// repair remains in `maintaining` state and serves no lookups, so a
    /// later call safely resumes it.
    ///
    /// # Errors
    ///
    /// Returns a store or derivation error, or [`Error::Constraint`] when a
    /// step bound is zero.
    pub async fn repair_deferred_indexes(
        &self,
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: &str,
        step: Option<BuildStep>,
    ) -> Result<u64> {
        let step = step.unwrap_or_default();
        if step.entries == 0 || step.bytes == 0 {
            return Err(Error::Constraint(
                "a deferred repair step must admit at least one entry and one byte".to_owned(),
            ));
        }

        let snapshot = self.snapshot().await?;
        let pending: Vec<_> = snapshot
            .indexes
            .values()
            .flat_map(|per_table| per_table.values())
            .map(super::snapshot::index_info)
            .filter(|index| index.state == IndexState::Maintaining)
            .collect();
        let mut repaired = 0u64;
        for index in pending {
            if data_store.is_none() && !snapshot.data_files_of(index.table_id).is_empty() {
                return Err(Error::Constraint(format!(
                    "deferred index {} cannot be repaired without a data-path store",
                    index.id
                )));
            }
            let def = IndexDef {
                name: index.name.clone(),
                columns: index.columns.clone(),
                unique: index.unique,
            };
            self.drive_staged_build(
                index.table_id,
                &def,
                index.id,
                data_store.clone(),
                data_prefix,
                step,
            )
            .await?;
            repaired = repaired.saturating_add(1);
        }
        Ok(repaired)
    }

    /// Deletes up to `limit` orphaned entries of a dropped index, in one
    /// bounded batch outside the commit protocol (entries are not catalog
    /// entities, and the dropping commit's batch must stay bounded). Returns
    /// the number deleted; a host loops until it returns 0. Index ids are
    /// never reused, so a concurrent create cannot collide with a sweep.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] if the index is still live (reclaiming
    /// a live index's entries would corrupt it), or a store error.
    pub async fn reclaim_index_entries(&self, index: IndexId, limit: usize) -> Result<usize> {
        let head = self.snapshot().await?;
        if head
            .indexes
            .values()
            .any(|per_table| per_table.contains_key(&index.get()))
        {
            return Err(Error::Constraint(format!(
                "index {index} is still live; drop it before reclaiming its entries"
            )));
        }

        let tx = self.begin_write_tx().await?;
        let mut staged = StagedBytes::default();
        let deleted =
            index_maintenance::reclaim_entries(&tx, index.get(), limit, &mut staged).await?;
        commit::commit_durable(tx, "entry reclamation", staged)
            .await
            .map_err(Error::from)?;

        Ok(deleted)
    }

    /// Runs one maintenance pass, reclaiming what only moraine knows is
    /// dead, and reports what it did.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] on a read-only catalog,
    /// [`Error::Configuration`] for a zero `batch_size`, or a store error.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, MaintenanceRequest};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// // A fresh catalog has nothing to reclaim.
    /// let report = catalog.maintain(MaintenanceRequest::default()).await?;
    /// assert_eq!(report.index_entries_reclaimed, 0);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn maintain(&self, request: MaintenanceRequest) -> Result<MaintenanceReport> {
        // Refuse before doing anything, including before the
        // nothing-to-do shortcut: a pass that reclaims nothing is still a
        // pass, and answering it differently on a read-only catalog would
        // make the outcome depend on the request rather than the handle.
        self.writer()?;
        if request.batch_size == 0 {
            return Err(Error::Configuration(
                "batch_size must be nonzero; zero would reclaim nothing and never terminate"
                    .to_string(),
            ));
        }

        let mut report = MaintenanceReport::default();
        if !request.sweep_orphaned_index_entries {
            return Ok(report);
        }

        // Index ids come from the monotonic catalog-id counter and are
        // never reused, so an id absent from this view can never become
        // live again: deciding liveness once, here, is sound for the
        // whole pass however long it runs.
        let live: HashSet<u64> = self
            .snapshot()
            .await?
            .indexes
            .values()
            .flat_map(|per_table| per_table.keys().copied())
            .collect();

        for kind in [IndexKind::Unique, IndexKind::Multi] {
            let mut from = 0u64;
            while let Some(index_id) = self.first_index_id_from(kind, from).await? {
                if !live.contains(&index_id) {
                    let reclaimed = self
                        .reclaim_dead_range(kind, index_id, request.batch_size)
                        .await?;
                    if reclaimed > 0 {
                        report.indexes_swept += 1;
                        report.index_entries_reclaimed += reclaimed;
                    }
                }
                // Seek past this index rather than walking its entries.
                match index_id.checked_add(1) {
                    Some(next) => from = next,
                    None => break,
                }
            }
        }

        Ok(report)
    }

    /// Merges each targeted subspace's sorted runs into one, reclaiming the
    /// superseded versions and tombstones they hold.
    ///
    /// moraine plans nothing: SlateDB decides which runs merge into which,
    /// and the plan it makes for a whole tree destines that tree's bottom
    /// run — which is what permits dropping a tombstone rather than carrying
    /// it forward. A subspace with no sorted runs is skipped, as is one
    /// already being merged.
    ///
    /// With [`CompactStoreRequest::wait`] set, the call returns once every
    /// submitted merge has committed or failed. A merge that outlives the
    /// wait is **not** cancelled: it keeps running, is reported
    /// [`MergeOutcome::Pending`], and a later census shows the result.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] if the catalog was opened read-only —
    /// the compactor that executes a submitted merge runs inside the writer,
    /// so a reader would queue work nothing would run — or a store error if
    /// the merge cannot be submitted.
    pub async fn compact_store(&self, request: CompactStoreRequest) -> Result<CompactStoreReport> {
        self.writer()?;

        let target = match &request.target {
            CompactionTarget::WholeStore => None,
            CompactionTarget::Subspace(name) => match name.subspace() {
                Some(subspace) => Some(subspace_prefix(subspace)),
                // An unknown subspace addresses no keys, so there is no
                // tree to name in a request.
                None => {
                    return Err(Error::Configuration(format!(
                        "{name} is not a subspace this build can merge"
                    )));
                }
            },
        };

        let before = self.store_census(CensusRequest::default()).await?;
        let submitted = store_compaction::submit_full_merge(
            &self.location.path,
            Arc::clone(&self.location.object_store),
            target.as_deref(),
        )
        .await?;

        let mut merges = Vec::new();
        for merge in &submitted {
            let subspace = SubspaceName::of_prefix(&merge.segment);
            let outcome = match request.wait {
                None => MergeOutcome::Pending,
                Some(budget) => match store_compaction::await_merge(
                    &self.location.path,
                    Arc::clone(&self.location.object_store),
                    &merge.compaction,
                    budget,
                )
                .await?
                {
                    MergeEnd::Completed => MergeOutcome::Completed,
                    MergeEnd::Failed => {
                        MergeOutcome::Failed("the merge ended without committing".to_string())
                    }
                    MergeEnd::Pending => MergeOutcome::Pending,
                },
            };

            merges.push(SubspaceMerge {
                subspace: subspace.clone(),
                outcome,
                bytes_before: bytes_of(&before, &subspace),
                bytes_after: None,
            });
        }

        // Every subspace the request covered but nothing was submitted for
        // is reported rather than dropped, so two calls stay comparable.
        for measured in &before.subspaces {
            let covered = target
                .as_ref()
                .is_none_or(|prefix| SubspaceName::of_prefix(prefix) == measured.subspace);
            if !covered || merges.iter().any(|m| m.subspace == measured.subspace) {
                continue;
            }
            // The only reason a plan omits a tree: L0 SSTs are not
            // eligible sources, so a tree without sorted runs has nothing
            // to merge. A tree already being merged is adopted rather than
            // omitted, so it never reaches here.
            merges.push(SubspaceMerge {
                subspace: measured.subspace.clone(),
                outcome: MergeOutcome::Skipped("no sorted runs to merge"),
                bytes_before: measured.bytes,
                bytes_after: None,
            });
        }

        if merges
            .iter()
            .any(|merge| merge.outcome == MergeOutcome::Completed)
        {
            let after = self.store_census(CensusRequest::default()).await?;
            for merge in &mut merges {
                if merge.outcome == MergeOutcome::Completed {
                    merge.bytes_after = Some(bytes_of(&after, &merge.subspace));
                }
            }
        }

        Ok(CompactStoreReport { merges })
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
    /// Commits the `building` definition, or adopts the one already there.
    /// A ready definition of the same name belongs to a finished index.
    async fn begin_staged_index(
        &self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        maintenance: IndexMaintenance,
    ) -> Result<IndexId> {
        if let Some(existing) = self.snapshot().await?.index_by_name(table, &def.name) {
            return match existing.state {
                IndexState::Ready => Err(Error::AlreadyExists(format!(
                    "index {} on table {table}",
                    def.name
                ))),
                IndexState::Building | IndexState::Poisoned => {
                    // Resuming adopts the stored definition, whose entries
                    // are encoded under its own orders.
                    let (directions, nulls) = requested_orders(orders, def.columns.len());
                    if existing.columns != def.columns
                        || existing.unique != def.unique
                        || existing.maintenance != maintenance
                        || existing.directions != directions
                        || existing.nulls != nulls
                    {
                        return Err(Error::Constraint(format!(
                            "index {} on table {table} is already building over a different \
                             definition; drop it to rebuild",
                            def.name
                        )));
                    }
                    Ok(existing.id)
                }
                IndexState::Maintaining => Err(Error::Constraint(format!(
                    "index {} on table {table} is awaiting deferred maintenance",
                    def.name
                ))),
            };
        }

        let index = std::cell::Cell::new(None);
        self.commit(|tx| {
            let id =
                tx.create_index_staged_ordered_with_maintenance(table, def, orders, maintenance)?;
            index.set(Some(id));
            Ok(())
        })
        .await?;

        index
            .get()
            .ok_or_else(|| Error::Corruption("staged create returned no index id".to_owned()))
    }

    /// Derives the live backfill and commits it in bounded steps until the
    /// index is ready, re-deriving at a fresh snapshot after a lost race.
    #[allow(clippy::too_many_lines)]
    async fn drive_staged_build(
        &self,
        table: TableId,
        def: &IndexDef,
        index: IndexId,
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: &str,
        step: BuildStep,
    ) -> Result<()> {
        for attempt in 1..=BUILD_DERIVATION_ATTEMPTS {
            info!(
                table = table.get(),
                index = index.get(),
                index_name = %def.name,
                derivation_attempt = attempt,
                "staged index backfill derivation started"
            );
            let derivation_started = Instant::now();
            let snapshot = self.snapshot().await?;
            let info = snapshot
                .indexes_of(table)
                .into_iter()
                .find(|info| info.id == index)
                .ok_or_else(|| Error::NotFound(format!("index {index}")))?;
            let total_entries = snapshot
                .table_stats(table)
                .and_then(|stats| usize::try_from(stats.record_count).ok())
                .unwrap_or(usize::MAX);
            let initial_file_cursor = info.build_file_cursor.map(DataFileId::get);
            let initial_position_cursor = info.build_position_cursor;
            let legacy_row_cursor = initial_file_cursor
                .is_none()
                .then_some(info.build_cursor)
                .flatten();
            // Deferred UPDATE may reinsert an inline row under its preserved
            // id, so row-id watermarks cannot distinguish its new value.
            // Inline data is policy-bounded; re-derive its live set during
            // repair. Initial builds still resume by their durable row id.
            let inline_row_cursor = (info.state != IndexState::Maintaining)
                .then_some(info.build_cursor)
                .flatten();
            let mut buffer = BuildStepBuffer {
                catalog: self,
                table,
                index,
                index_name: &def.name,
                bound: step,
                derivation_attempt: attempt,
                total_entries,
                entries: Vec::new(),
                nominal_bytes: 0,
                pending_source: initial_file_cursor.zip(initial_position_cursor),
                completed_entries: 0,
                peak_buffered_entries: 0,
            };

            let pass = async {
                // Inline rows precede file sources. Older builds that carry
                // only a row-id cursor resume this leg by that watermark.
                let mut inline = self.inline_backfill_entries(table, &def.columns).await?;
                inline.sort_unstable_by_key(|entry| entry.row_id);
                for entry in inline
                    .into_iter()
                    .filter(|entry| inline_row_cursor.is_none_or(|row| entry.row_id > row))
                {
                    buffer.push(entry, None).await?;
                }

                if let Some(store) = &data_store {
                    self.stream_backfill_files(
                        Arc::clone(store),
                        data_prefix,
                        table,
                        &def.columns,
                        initial_file_cursor,
                        initial_position_cursor,
                        legacy_row_cursor,
                        &mut buffer,
                    )
                    .await?;
                }
                buffer.flush(true).await
            }
            .await;

            match pass {
                Ok(()) => {
                    info!(
                        table = table.get(),
                        index = index.get(),
                        index_name = %def.name,
                        derivation_attempt = attempt,
                        total_entries = buffer.completed_entries,
                        peak_buffered_entries = buffer.peak_buffered_entries,
                        derive_ms = derivation_started.elapsed().as_secs_f64() * 1_000.0,
                        sort_ms = 0.0,
                        "staged index backfill derived"
                    );
                    return Ok(());
                }
                Err(Error::CommitConflict(_)) => {
                    warn!(
                        table = table.get(),
                        index = index.get(),
                        index_name = %def.name,
                        derivation_attempt = attempt,
                        completed_entries = buffer.completed_entries,
                        total_entries,
                        progress_percent = build_progress_percent(
                            buffer.completed_entries,
                            total_entries,
                        ),
                        "staged index build step conflicted; re-deriving"
                    );
                }
                Err(other) => return Err(other),
            }
        }
        Err(Error::CommitConflict(format!(
            "staged build of index {index} lost its race {BUILD_DERIVATION_ATTEMPTS} times; \
             the table is under concurrent write"
        )))
    }

    /// Streams the external-file leg of a build into `buffer`, excluding
    /// rows already dead at this pass's pinned snapshot.
    #[allow(clippy::too_many_arguments)]
    async fn stream_backfill_files(
        &self,
        object_store: Arc<dyn ObjectStore>,
        data_prefix: &str,
        table: TableId,
        columns: &[ColumnId],
        file_cursor: Option<u64>,
        position_cursor: Option<u64>,
        legacy_row_cursor: Option<u64>,
        buffer: &mut BuildStepBuffer<'_>,
    ) -> Result<()> {
        let session = self.begin_read().await?;
        let outcome = async {
            let snapshot = commit::materialize(session.handle(), None).await?;
            let live_columns = snapshot.columns_of(table);
            let positions = columns
                .iter()
                .map(|column| {
                    live_columns
                        .iter()
                        .position(|candidate| candidate.id == *column)
                        .ok_or_else(|| Error::NotFound(format!("column {column} of table {table}")))
                })
                .collect::<Result<Vec<_>>>()?;
            let table_prefix = snapshot.table_data_prefix(table)?;
            let resolve = |path: &str, is_relative: bool| {
                let relative = match (is_relative, data_prefix.is_empty()) {
                    (false, _) => path.to_owned(),
                    (true, true) => format!("{table_prefix}{path}"),
                    (true, false) => format!("{data_prefix}/{table_prefix}{path}"),
                };
                Path::from(relative.as_str())
            };

            let mut killed_positions: HashMap<u64, HashSet<u64>> = HashMap::new();
            let mut killed_row_ids: HashMap<u64, HashSet<u64>> = HashMap::new();
            for (data_file_id, row_id, _) in
                store_inline::scan_inline_file_deletes(session.handle(), table.get()).await?
            {
                killed_row_ids
                    .entry(data_file_id)
                    .or_default()
                    .insert(row_id);
            }

            for delete in snapshot.delete_files_of(table) {
                let path = resolve(&delete.path, delete.path_is_relative);
                let positions =
                    scoped_read::delete_file_positions(object_store.as_ref(), &path).await?;
                killed_positions
                    .entry(delete.data_file_id.get())
                    .or_default()
                    .extend(positions);
            }

            for file in snapshot.data_files_of(table) {
                if file_cursor.is_some_and(|cursor| file.id.get() < cursor) {
                    continue;
                }
                let start = if file_cursor == Some(file.id.get()) {
                    position_cursor.map_or(0, |position| position.saturating_add(1))
                } else {
                    0
                };
                if start >= file.record_count {
                    continue;
                }
                let path = resolve(&file.path, file.path_is_relative);
                let mut consumer = BuildFileConsumer {
                    buffer,
                    file_id: file.id.get(),
                    dead_positions: killed_positions.get(&file.id.get()),
                    dead_row_ids: killed_row_ids.get(&file.id.get()),
                    legacy_row_cursor,
                };
                scoped_read::scoped_read_entry_batches(
                    Arc::clone(&object_store),
                    &path,
                    &positions,
                    scoped_read::RowIdSource::Resolve {
                        row_id_start: file.row_id_start,
                    },
                    Some(file.file_size_bytes),
                    Some(file.footer_size),
                    start,
                    &mut consumer,
                )
                .await?;
            }
            Ok(())
        }
        .await;
        session.finish();
        outcome
    }

    /// Deletes every entry of one dead index, `batch_size` per commit,
    /// returning the total. The caller has already established that the
    /// index is not live.
    async fn reclaim_dead_range(
        &self,
        kind: IndexKind,
        index_id: u64,
        batch_size: usize,
    ) -> Result<u64> {
        let mut total = 0u64;
        // Each batch resumes where the last one stopped. Restarting at
        // the range's beginning would make every batch step over the
        // tombstones its predecessors left, which is quadratic in the
        // size of the range.
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let tx = self.begin_write_tx().await?;
            // Non-durable below, so nothing reads the staged size.
            let mut staged = StagedBytes::default();
            let (deleted, last) = index_maintenance::reclaim_entries_from(
                &tx,
                kind,
                index_id,
                batch_size,
                cursor.as_deref(),
                &mut staged,
            )
            .await?;
            if deleted == 0 {
                tx.rollback();
                return Ok(total);
            }
            // Batches commit non-durably: awaiting a flush tick per batch
            // makes the whole sweep flush-bound, and durability buys nothing
            // here. A dead index id is never reused and the deletes are
            // idempotent, so a batch lost to a crash simply leaves entries a
            // later pass rediscovers.
            tx.commit_with_options(&commit::non_durable())
                .await
                .map_err(Error::from)?;
            total += deleted as u64;
            cursor = last;
        }
    }
}
