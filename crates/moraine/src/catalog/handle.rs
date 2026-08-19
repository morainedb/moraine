//! The catalog handle: the entry point a host opens, reads, and commits
//! through.

#[cfg(test)]
mod tests;

use std::{
    collections::{HashMap, HashSet},
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures::StreamExt;
use moraine_wal::FoldReport;
use object_store::{ObjectStore, path::Path};
use slatedb::{Db, DbReader, IsolationLevel};
use tracing::{info, warn};

use crate::{
    catalog::{
        CatalogSnapshot, ColumnId, ColumnOrder, DataFileId, DataFileInfo, FileIndexEntry, IndexDef,
        IndexEntry, IndexId, IndexInfo, IndexState, RecentRow, RowHolder, RowLocation, SnapshotId,
        TableId,
        census::{
            CensusRequest, CompactStoreReport, CompactStoreRequest, CompactionTarget, LiveCount,
            MergeOutcome, StoreCensus, StoreObjects, SubspaceCensus, SubspaceMerge, SubspaceName,
        },
        inline::{InlineScanKind, materialize_inline_rows},
        projection::ProjectionCache,
        scoped_read,
    },
    error::{Error, Result},
    store::{
        census::{self as store_census, SegmentSize},
        compaction::{self as store_compaction, MergeEnd},
        handle::{ReadHandle, ReadSession},
        index_encoding::{
            CanonicalKey, Direction, IndexKeyValue, NullOrder, encode_ordered_values,
        },
        inline as store_inline,
        key::{
            IndexKey, IndexKind, InlineOperation, Key, Subspace, index_index_prefix,
            index_kind_prefix, subspace_prefix,
        },
        open::{self, StoreBuilder},
    },
    transaction::{
        MigrationReport, Transaction, commit, folder, index_maintenance, migration, slot_commit,
    },
};

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

/// How many entries one staged build step commits. At roughly a kilobyte
/// of write-path memory apiece, a step peaks near a gigabyte.
const BUILD_STEP_ENTRIES: usize = 1_000_000;

/// How many times a staged build re-derives after losing a race before
/// giving up.
const BUILD_DERIVATION_ATTEMPTS: usize = 8;

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

/// The outcome of probing a store before opening it read-write: a reader over
/// an existing store, an empty prefix to bootstrap, or a peer mid-creation
/// whose half-written manifest reads no consistent version yet — a race the
/// caller waits out rather than a store to bootstrap or refuse.
enum ProbeOutcome {
    Reader(Box<DbReader>),
    Empty,
    Racing,
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

/// Encodes a staged build's entry batch into store-ready staged entries under
/// the index's declared orders, marked building so a duplicate poisons the
/// definition rather than failing the commit.
fn build_staged_entries(
    info: &IndexInfo,
    index: IndexId,
    batch: &[IndexEntry],
) -> Result<Vec<crate::transaction::index_maintenance::StagedIndexEntry>> {
    batch
        .iter()
        .map(|entry| {
            let has_null = entry.values.iter().any(Option::is_none);
            let key = encode_ordered_values(&entry.values, &info.directions, &info.nulls)?;
            Ok(crate::transaction::index_maintenance::StagedIndexEntry {
                index_id: index.get(),
                unique: info.unique && !has_null,
                key,
                row_id: entry.row_id,
                delete: false,
                building: true,
            })
        })
        .collect()
}

/// Derives index-backfill entries for a table's live inline rows, scanning the
/// inline chunk bodies through `handle` overlaid with `overlay` — the unfolded
/// tail on a slot-backed store — so rows in slots no folder has applied are
/// covered. Inline-tombstoned rows are excluded.
async fn inline_backfill_from(
    handle: ReadHandle<'_>,
    overlay: Option<&moraine_wal::Overlay>,
    snapshot: &CatalogSnapshot,
    table: TableId,
    columns: &[ColumnId],
) -> Result<Vec<IndexEntry>> {
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

    // Rows tombstoned out of their chunk by an inline delete are dead and must
    // not be indexed.
    let dead: HashSet<u64> = store_inline::scan_inline_inline_deletes(handle, overlay, table.get())
        .await?
        .into_iter()
        .map(|(row_id, _)| row_id)
        .collect();

    let mut entries = Vec::new();
    for (op, chunk) in store_inline::scan_inline_chunks(handle, overlay, table.get()).await? {
        let InlineOperation::Insert { schema_version, .. } = op else {
            continue;
        };
        let schema = store_inline::read_inline_schema(handle, overlay, table.get(), schema_version)
            .await?
            .ok_or_else(|| {
                Error::Corruption(format!(
                    "no inline schema for table {table} version {schema_version}"
                ))
            })?;
        let scoped = scoped_read::inline_batch_entries(
            &schema.arrow_schema,
            &chunk.body,
            &positions,
            chunk.row_id_start,
        )?;
        entries.extend(
            scoped
                .into_iter()
                .filter(|entry| !dead.contains(&entry.row_id))
                .map(|entry| IndexEntry {
                    row_id: entry.row_id,
                    values: entry.values,
                }),
        );
    }
    Ok(entries)
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

    /// The head snapshot id, `Some` for the projection callers that gate on a
    /// known head.
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn head_id(&self) -> Option<u64> {
        let Self::Slots { head, .. } = self;
        Some(head.view.snapshot.snapshot_id)
    }

    /// Releases the reader a hole retry opened.
    pub(crate) async fn finish(self) {
        let Self::Slots { head, .. } = self;
        slot_commit::release_reader(head.reader.as_ref()).await;
    }
}

/// Whether a run of build steps finished the index or lost its race.
enum BuildProgress {
    /// A final step flipped the index ready.
    Ready,
    /// A step lost its race; the backfill must be re-derived.
    Conflicted,
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
    materialize_micros: AtomicU64,
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

/// The open store behind a catalog. Every attach — read-write or read-only —
/// builds [`Store::Slots`].
enum Store {
    /// The slot-log-backed store: a reader following the folded store plus the
    /// slot log its commits ride. Boxed to keep the enum's footprint down.
    Slots(Box<SlotStore>),
}

/// The slot-log-backed store: a reader following the folded store plus the slot
/// log its commits ride. A read-only attach never opens the fenced writer, so
/// it never fences a live writer; a read-write attach opens it only for
/// folder-role work.
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
/// assert_eq!(options.refresh_interval, std::time::Duration::from_secs(10));
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
    /// Local directory backing SlateDB's on-disk object cache, which holds
    /// fetched object parts. When set, reads are served from a disk-backed
    /// cache that survives process restarts, so warm queries skip repeat
    /// object-store GETs — worthwhile for remote (`s3://`) stores,
    /// redundant for local ones. `None` (the default) leaves only the
    /// in-memory caches: a block cache and a metadata cache, both at
    /// SlateDB's own sizes and not configurable here.
    pub cache_dir: Option<std::path::PathBuf>,
    /// How many bytes of disk the on-disk object cache may hold. The cap is
    /// per open catalog, not per directory, so catalogs sharing a
    /// [`cache_dir`](Self::cache_dir) each spend up to it — size the volume
    /// for the number of catalogs a process attaches. `None` (the default)
    /// leaves SlateDB's own cap of 16 GiB in force, and without a
    /// `cache_dir` there is no object cache to bound. The in-memory caches
    /// are a separate mechanism and are unaffected.
    pub cache_size: Option<u64>,
    /// What to load into the on-disk object cache while the catalog opens,
    /// so the first query pays no first touch. The load is bounded by
    /// [`cache_size`](Self::cache_size) and best-effort — a fetch that
    /// fails is skipped, never fatal — but it is part of the open, so an
    /// open that preloads returns only once it has. `None` (the default)
    /// loads nothing, leaving the cache to fill as reads ask for objects.
    /// Inert without a [`cache_dir`](Self::cache_dir).
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
    /// How long a commit may wait to be batched with others of this process
    /// before racing the log. Zero (the default) still coalesces whatever is
    /// already queued into one envelope — it only declines to *wait* — so an
    /// uncontended commit adds no latency. A non-zero window trades latency
    /// for fewer object-store PUTs under load, the write-side cost axis.
    pub commit_batch_window: Duration,
    /// How often a reader polls the object store for a fresher folded view.
    /// A reader lags the head by up to this interval, replaying that many more
    /// slots per materialization; a shorter interval trades more manifest
    /// reads for less fold lag, the read-side cost axis. Defaults to 10s.
    /// Ignored by a catalog pinned to a [`checkpoint`](Self::checkpoint),
    /// which polls for nothing.
    pub refresh_interval: Duration,
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
            cache_dir: None,
            cache_size: None,
            cache_preload: None,
            cache_puts: false,
            data_path: None,
            commit_batch_window: Duration::ZERO,
            refresh_interval: Duration::from_secs(10),
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

/// A handle to a moraine catalog: cheap to clone, drives reads and
/// commits. The storage substrate never appears in this API — a catalog
/// lives in a bucket reachable through any [`ObjectStore`].
#[derive(Clone)]
pub struct Catalog {
    store: Arc<Store>,
    // Shared across handle clones: how this attach has served its reads.
    reads: Arc<ReadTally>,
    // Where the store lives. Retained because the census and the store
    // merge reach SlateDB's admin surface, which addresses a store by path
    // rather than through an open handle.
    location: Arc<StoreLocation>,
    // Shared across handle clones: decoded projections folded forward on
    // commit, served without rescanning when their head matches.
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
}

impl std::fmt::Debug for Catalog {
    // `slatedb::Db` carries no `Debug` impl.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog").finish_non_exhaustive()
    }
}

impl Catalog {
    /// Opens (creating and initializing if empty) the catalog in
    /// `object_store` at `options.path`.
    ///
    /// Exactly one process may hold a read-write catalog per store —
    /// opening a second fences the first.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be opened, is mid-migration,
    /// or is stamped with a structural format this binary does not
    /// understand.
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
        let reader = Box::pin(Self::open_read_write_reader(&object_store, &options)).await?;
        info!(
            path = options.path,
            refresh_interval_ms = options.refresh_interval.as_millis(),
            "opened catalog read-write"
        );
        let location = Arc::new(StoreLocation {
            path: options.path.clone(),
            object_store: Arc::clone(&object_store),
        });
        let slots = moraine_wal::SlotLog::new(object_store.clone(), &options.path);
        let coalescer = slot_commit::CommitCoalescer::new(options.commit_batch_window);
        Ok(Self {
            store: Arc::new(Store::Slots(Box::new(SlotStore {
                reader: Arc::new(reader),
                slots,
                object_store,
                options,
                read_only: false,
                pinned: false,
                coalescer,
                head_cache: slot_commit::HeadCache::default(),
                contention: Arc::new(slot_commit::ContentionCounters::default()),
                #[cfg(feature = "leader")]
                forwarding: Arc::new(slot_commit::Forwarding::default()),
            }))),
            reads: Arc::new(ReadTally::default()),
            location,
            projections: Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
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
    ) -> Result<DbReader> {
        for attempt in 0..MIGRATION_ATTEMPTS {
            let reader = match Self::open_probe_reader(object_store, options).await? {
                ProbeOutcome::Reader(reader) => *reader,
                // Empty, or a peer mid-creation: route to the writer path, which
                // SlateDB serializes by fencing. A concurrent creator can still
                // leave the store half-formed, so a failed bootstrap backs off
                // and re-probes — one racer wins and the rest adopt it.
                ProbeOutcome::Empty | ProbeOutcome::Racing => {
                    match Self::bootstrap_slot_reader(object_store, options).await {
                        Ok(reader) => return Ok(reader),
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
            match Self::classify_format(&reader).await? {
                FormatClass::SlotLog => return Ok(reader),
                FormatClass::Empty => {
                    reader.close().await.map_err(Error::from)?;
                    match Self::bootstrap_slot_reader(object_store, options).await {
                        Ok(reader) => return Ok(reader),
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

    /// Opens a probe reader over the store: `Some(reader)` when the prefix is
    /// readable, `None` when it is a known-empty prefix that licenses a
    /// bootstrap. A prefix holding objects whose manifest will not open is a
    /// damaged store, and the error propagates rather than stamping over it.
    async fn open_probe_reader(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
    ) -> Result<ProbeOutcome> {
        let reader_store = StoreBuilder::new(&options.path, object_store.clone())
            .refresh_interval(options.refresh_interval)
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts);
        match reader_store.open_reader().await {
            Ok(reader) => Ok(ProbeOutcome::Reader(Box::new(reader))),
            Err(err) => match prefix_state(object_store, &options.path).await {
                PrefixState::Empty => Ok(ProbeOutcome::Empty),
                PrefixState::HasManifest => Ok(ProbeOutcome::Racing),
                PrefixState::Foreign => Err(err),
            },
        }
    }

    /// Classifies a probe reader's stored structural format.
    async fn classify_format(reader: &DbReader) -> Result<FormatClass> {
        let handle = ReadHandle::Reader(reader);
        if crate::store::read::read_migration(handle).await?.is_some() {
            return Err(Error::Corruption(
                "store is mid-migration; refusing to open".to_string(),
            ));
        }
        match crate::store::read::read_format(handle).await? {
            None => Ok(FormatClass::Empty),
            Some(format) if format.format_version == commit::FORMAT_MULTI_WRITER => {
                Ok(FormatClass::SlotLog)
            }
            Some(format) if format.format_version > commit::MAX_FORMAT_VERSION => {
                Ok(FormatClass::TooNew(format.format_version))
            }
            Some(format) if format.format_version < commit::MIN_FORMAT_VERSION => {
                Err(Error::Configuration(format!(
                    "store format {} predates this binary's minimum ({}); it cannot be attached",
                    format.format_version,
                    commit::MIN_FORMAT_VERSION
                )))
            }
            Some(_) => Ok(FormatClass::Legacy),
        }
    }

    /// Bootstraps an empty store at the slot-log format through the writer,
    /// fencing any incumbent old-binary writer, then closes it and reopens
    /// read-only.
    async fn bootstrap_slot_reader(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
    ) -> Result<DbReader> {
        let writer_store = StoreBuilder::new(&options.path, object_store.clone())
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts);
        let db = commit::open_initialized(
            writer_store,
            options.encrypted,
            options.data_path.as_deref(),
        )
        .await?;
        db.close().await.map_err(Error::from)?;

        let reader_store = StoreBuilder::new(&options.path, object_store.clone())
            .refresh_interval(options.refresh_interval)
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts);
        let (reader, _) = commit::open_reader_initialized(reader_store)
            .await?
            .ok_or_else(|| {
                Error::Corruption(
                    "store still uninitialized immediately after bootstrap".to_string(),
                )
            })?;

        Ok(reader)
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
    ) -> Result<Self> {
        let checkpoint = parse_checkpoint(options.checkpoint.as_deref())?;
        warn_if_preload_cannot_fit(&options, Arc::clone(&object_store)).await;
        let store = StoreBuilder::new(&options.path, object_store.clone())
            .refresh_interval(options.refresh_interval)
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
            .cache_preload(options.cache_preload)
            .cache_puts(options.cache_puts)
            .checkpoint(checkpoint);
        // A store with no manifest fails to open at all; the failure propagates
        // (a read-only attach never bootstraps).
        let reader = store.open_reader().await?;
        match Self::classify_format(&reader).await {
            // Format 1–4 all serve read-only.
            Ok(FormatClass::SlotLog | FormatClass::Legacy) => {}
            Ok(FormatClass::Empty) => {
                slot_commit::release_reader(Some(&reader)).await;
                return Err(Error::Corruption(
                    "store is not an initialized moraine catalog; a read-only attach \
                     needs a writer to have created it first"
                        .to_string(),
                ));
            }
            // A too-new format is a compatibility problem, not corruption — the
            // same kind the read-write path refuses it with.
            Ok(FormatClass::TooNew(version)) => {
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
        }
        info!(
            path = options.path,
            checkpoint = options.checkpoint,
            "opened catalog read-only"
        );

        // Every readable store serves read-only through the slot topology: a
        // legacy format 1–3 store never migrates (a read-only attach writes
        // nothing), and its absent fold cursor reads as 0 with an empty tail,
        // so it is a slot store with no slots.
        let location = Arc::new(StoreLocation {
            path: options.path.clone(),
            object_store: Arc::clone(&object_store),
        });
        let slots = moraine_wal::SlotLog::new(object_store.clone(), &options.path);
        let coalescer = slot_commit::CommitCoalescer::new(options.commit_batch_window);
        let pinned = options.checkpoint.is_some();
        Ok(Self {
            store: Arc::new(Store::Slots(Box::new(SlotStore {
                reader: Arc::new(reader),
                slots,
                object_store,
                options,
                read_only: true,
                pinned,
                coalescer,
                head_cache: slot_commit::HeadCache::default(),
                contention: Arc::new(slot_commit::ContentionCounters::default()),
                #[cfg(feature = "leader")]
                forwarding: Arc::new(slot_commit::Forwarding::default()),
            }))),
            reads: Arc::new(ReadTally::default()),
            location,
            projections: Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
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
        let db = StoreBuilder::new(&options.path, object_store.clone())
            .cache_dir(options.cache_dir.clone())
            .cache_size(options.cache_size)
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

    /// The maintained-projection state shared by this handle's clones.
    pub(crate) fn projections(&self) -> &Arc<std::sync::RwLock<ProjectionCache>> {
        &self.projections
    }

    /// The slot-log-backed store behind this handle — the leader role
    /// materializes heads, mints the secret, and races slots through it.
    #[cfg(feature = "leader")]
    pub(crate) fn slot_store(&self) -> &SlotStore {
        let Store::Slots(store) = self.store.as_ref();
        store
    }

    /// Whether this catalog maintains served projections. The slot topology
    /// folds no local commits into a served head view, so dumps always scan.
    #[allow(clippy::unused_self)]
    pub(crate) fn maintains_projections(&self) -> bool {
        false
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
    async fn with_writer<T, F>(&self, body: F) -> Result<T>
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

    async fn view(&self, at: Option<u64>) -> Result<Arc<CatalogSnapshot>> {
        let Store::Slots(store) = self.store.as_ref();
        match at {
            None => {
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

    /// One head read: the view, and on a slot-backed attach the byte-level
    /// overlay of the slots no folder has applied — what a probe the projection
    /// does not model must read over the store.
    async fn head_view(&self) -> Result<HeadRead> {
        let Store::Slots(store) = self.store.as_ref();
        // A fresh reader: entry scans read folder-written entries — index
        // backfills above all — straight from the store, not the tail.
        let head = slot_commit::materialize_slot_head_fresh(store).await?;
        Ok(HeadRead {
            view: head.view,
            tail: Some(head.overlay),
            reader: head.reader,
        })
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
        let probe = self.begin_probe().await?;
        let read_at = probe.view().snapshot.snapshot_id;
        let outcome = self.scan_recent_rows(&probe, table, read_at).await;
        probe.finish().await;

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
        let probe = self.begin_probe().await?;
        let head = probe.view().snapshot.snapshot_id;
        let read_at = snapshot.get();
        let outcome = if read_at > head {
            Err(Error::NotFound(format!(
                "snapshot {read_at} (head is {head})"
            )))
        } else {
            self.scan_recent_rows(&probe, table, read_at).await
        };
        probe.finish().await;

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

    /// The inline rows of `table` live at `read_at`, scanned through the
    /// probe's head reader and its unfolded slot overlay so a row a winner
    /// no folder has applied yet is not missed.
    async fn scan_recent_rows(
        &self,
        probe: &ProbeRead,
        table: TableId,
        read_at: u64,
    ) -> Result<Vec<RecentRow>> {
        let handle = probe.handle();
        let overlay = probe.tail();
        let chunks = store_inline::scan_inline_chunks(handle, overlay, table.get()).await?;
        let tombstones =
            store_inline::scan_inline_inline_deletes(handle, overlay, table.get()).await?;

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
                let schema =
                    store_inline::read_inline_schema(handle, overlay, table.get(), *schema_version)
                        .await?
                        .ok_or_else(|| {
                            Error::Corruption(format!(
                                "no inline schema for table {table} version {schema_version}"
                            ))
                        })?;
                let schema = Arc::new(schema.arrow_schema);
                schemas.insert(*schema_version, Arc::clone(&schema));
                schema
            };
            let chunk_body = Arc::clone(
                bodies
                    .entry(row.chunk)
                    .or_insert_with(|| Arc::new(chunk.body.clone())),
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

    /// Resolves an equality lookup to the rows currently holding `values`.
    ///
    /// Head-only: the lookup materializes the current head and scans the
    /// `index` subspace under one read session, so the entries and the catalog
    /// they resolve against are one consistent cut. Entries are live-only,
    /// so there is no time-travel variant. Returns candidate
    /// [`RowLocation`]s; the caller applies delete files as any DuckLake
    /// scan does.
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
    ) -> Result<Vec<RowLocation>> {
        let read = self.begin_probe().await?;
        let handle = read.handle();

        let outcome = async {
            let info = read
                .view()
                .index_by_id(table, index)
                .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

            match info.state {
                IndexState::Ready => {}
                IndexState::Building => {
                    return Err(Error::IndexBuilding(format!(
                        "index {index} is still building"
                    )));
                }
                IndexState::Poisoned => {
                    return Err(Error::NotFound(format!("index {index} was poisoned")));
                }
            }
            let key = encode_ordered_values(
                &values.iter().cloned().map(Some).collect::<Vec<_>>(),
                &info.directions,
                &info.nulls,
            )?;
            let row_ids = index_maintenance::lookup_row_ids(
                handle,
                read.tail(),
                index.get(),
                info.unique,
                &key,
            )
            .await?;
            let holders = RowHolders::of(&read.view().data_files_of(table));
            Ok(row_ids
                .into_iter()
                .map(|row_id| RowLocation {
                    row_id,
                    holder: holders.holder(row_id),
                })
                .collect())
        }
        .await;
        read.finish().await;

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
    /// when `reverse` is set — the reverse of the materialized result, which
    /// needs no reverse iterator.
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
    ) -> Result<Vec<RowLocation>> {
        let read = self.begin_probe().await?;
        let handle = read.handle();

        let outcome = async {
            let info = read
                .view()
                .index_by_id(table, index)
                .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

            match info.state {
                IndexState::Ready => {}
                IndexState::Building => {
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

            let mut row_ids = index_maintenance::range_row_ids(
                handle,
                read.tail(),
                index.get(),
                info.unique,
                leading_nulls,
                byte_lower,
                byte_upper,
            )
            .await?;
            // The scan yields the index's declared order; reversing the
            // materialized result serves the exact opposite order.
            if reverse {
                row_ids.reverse();
            }
            let holders = RowHolders::of(&read.view().data_files_of(table));
            Ok(row_ids
                .into_iter()
                .map(|row_id| RowLocation {
                    row_id,
                    holder: holders.holder(row_id),
                })
                .collect())
        }
        .await;
        read.finish().await;

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
    ) -> Result<Vec<RowLocation>> {
        let read = self.begin_probe().await?;
        let handle = read.handle();

        let outcome = async {
            let info = read
                .view()
                .index_by_id(table, index)
                .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

            match info.state {
                IndexState::Ready => {}
                IndexState::Building => {
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
            let mut row_ids =
                index_maintenance::null_prefix_row_ids(handle, read.tail(), index.get(), &key)
                    .await?;
            if reverse {
                row_ids.reverse();
            }
            let holders = RowHolders::of(&read.view().data_files_of(table));

            Ok(row_ids
                .into_iter()
                .map(|row_id| RowLocation {
                    row_id,
                    holder: holders.holder(row_id),
                })
                .collect())
        }
        .await;
        read.finish().await;

        outcome
    }

    /// Opens a read session at the current head — the read-only reader shared
    /// with the catalog — the same isolation
    /// [`snapshot`](Self::snapshot)/[`snapshot_at`](Self::snapshot_at) use.
    /// Used by [`crate::ffi_support`]'s raw current+history dumps and inline
    /// scans; every other reader goes through `snapshot`/`snapshot_at`.
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
        let mut iter = handle.scan_prefix(prefix.clone(), ..).await.unwrap();
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

    /// Test-only: writes `entries` straight into the folded store under the
    /// folder role — the state a completed fold would leave, so the folder-role
    /// sweep can be exercised before a fold implementation lands.
    #[cfg(test)]
    pub(crate) async fn seed_folded_writes(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) {
        self.with_writer(async |db| {
            let tx = db
                .begin(IsolationLevel::Snapshot)
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
        data_store: Option<Arc<dyn ObjectStore>>,
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
            let head = self.head_view().await?;
            slot_commit::release_reader(head.reader.as_ref()).await;
            let snapshot = head.view;
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
                store_inline::scan_inline_file_deletes(session.handle(), None, table.get()).await?
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
                let scoped = scoped_read::scoped_read_entries(
                    Arc::clone(&object_store),
                    &path,
                    &positions,
                    scoped_read::RowIdSource::Resolve {
                        row_id_start: file.row_id_start,
                    },
                    Some(file.file_size_bytes),
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

    /// Creates an index by a staged (multi-commit) build, driving it to
    /// `ready` before returning — for a table whose backfill exceeds what
    /// one commit may stage.
    ///
    /// The definition lands `building` in its own commit; each pass then
    /// derives the table's live entries (external files through
    /// `data_store`, inline rows from the catalog store), orders them by row
    /// id, and commits them in steps of `step_entries`, defaulting to a
    /// million. Writers maintain entries from the first commit forward.
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
    /// index of this name, or [`Error::Constraint`] if `step_entries` is
    /// zero, the resumed definition differs from `def`, or the rows
    /// duplicate a unique value. A failed build drops its definition.
    pub async fn create_index_staged(
        &self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: &str,
        step_entries: Option<usize>,
    ) -> Result<IndexId> {
        if let Store::Slots(store) = self.store.as_ref()
            && store.read_only
        {
            return Err(Error::Constraint(
                "catalog attached read-only; writes are unavailable".to_string(),
            ));
        }

        let step_entries = step_entries.unwrap_or(BUILD_STEP_ENTRIES);
        if step_entries == 0 {
            return Err(Error::Constraint(
                "a staged build's step size must be at least one entry".to_owned(),
            ));
        }

        let index = self.begin_staged_index(table, def, orders).await?;
        let outcome = self
            .drive_staged_build(table, def, index, data_store, data_prefix, step_entries)
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

    /// Commits the `building` definition, or adopts the one already there.
    /// A ready definition of the same name belongs to a finished index.
    async fn begin_staged_index(
        &self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
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
            };
        }

        let index = std::cell::Cell::new(None);
        self.commit(|tx| {
            let id = tx.create_index_staged_ordered(table, def, orders)?;
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
    async fn drive_staged_build(
        &self,
        table: TableId,
        def: &IndexDef,
        index: IndexId,
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: &str,
        step_entries: usize,
    ) -> Result<()> {
        for _ in 0..BUILD_DERIVATION_ATTEMPTS {
            let mut entries = match &data_store {
                Some(store) => {
                    self.scoped_backfill_entries(
                        Arc::clone(store),
                        data_prefix,
                        table,
                        &def.columns,
                    )
                    .await?
                }
                None => Vec::new(),
            };
            entries.extend(self.inline_backfill_entries(table, &def.columns).await?);
            // One watermark can describe the covered set only in row-id
            // order, which per-row-id rewrite files would otherwise break.
            entries.sort_unstable_by_key(|entry| entry.row_id);

            if let BuildProgress::Ready = self
                .commit_build_steps(table, index, &entries, step_entries)
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::CommitConflict(format!(
            "staged build of index {index} lost its race {BUILD_DERIVATION_ATTEMPTS} times; \
             the table is under concurrent write"
        )))
    }

    /// Commits `entries` above the persisted cursor in steps, the last one
    /// flipping the index ready. A single-writer store stages each step's
    /// entries in the commit itself; a slot-backed store writes them straight
    /// into the store under the folder role and rides only the cursor advance
    /// and the ready/poison flip through the log, so a million-row backfill
    /// never rides one envelope.
    async fn commit_build_steps(
        &self,
        table: TableId,
        index: IndexId,
        entries: &[IndexEntry],
        step_entries: usize,
    ) -> Result<BuildProgress> {
        loop {
            let cursor = self.staged_build_cursor(table, index).await?;
            // The cursor is the highest row id covered; absent means none
            // is, so row id 0 is still pending.
            let pending = match cursor {
                Some(covered) => entries.partition_point(|entry| entry.row_id <= covered),
                None => 0,
            };
            let remaining = &entries[pending..];
            let step = &remaining[..remaining.len().min(step_entries)];
            let is_final = step.len() == remaining.len();

            let Store::Slots(store) = self.store.as_ref();
            let committed = self
                .commit_build_step_slots(store, table, index, step, is_final)
                .await;
            match committed {
                Ok(()) => {
                    if is_final {
                        return Ok(BuildProgress::Ready);
                    }
                }
                Err(Error::CommitConflict(_)) => return Ok(BuildProgress::Conflicted),
                Err(other) => return Err(other),
            }
        }
    }

    /// Commits one staged build step over the slot log: the entry batch lands
    /// in the store directly under the folder role, then a slot commit advances
    /// the build cursor and, on the final step, flips the index ready. A
    /// duplicate the batch discovers poisons the definition through a slot and
    /// fails the build.
    async fn commit_build_step_slots(
        &self,
        store: &SlotStore,
        table: TableId,
        index: IndexId,
        step: &[IndexEntry],
        is_final: bool,
    ) -> Result<()> {
        let info = self
            .snapshot()
            .await?
            .indexes_of(table)
            .into_iter()
            .find(|info| info.id == index)
            .ok_or_else(|| Error::NotFound(format!("index {index}")))?;

        if self.write_build_entries(store, &info, index, step).await? {
            self.commit(|tx| tx.poison_index_build(index)).await?;
            return Err(Error::Constraint(format!(
                "index {index} was poisoned by a duplicate value during its staged build"
            )));
        }

        // The highest row id this step covered advances the cursor; an empty
        // final step (no rows) still needs its flip, so the cursor holds.
        let covered = step.last().map(|entry| entry.row_id);
        self.commit(|tx| tx.advance_index_build(index, covered, is_final).map(|_| ()))
            .await
            .map(|_| ())
    }

    /// Writes a staged build's entry batch straight into the store under the
    /// folder role — the single direct writer — enforcing uniqueness against
    /// the folded store overlaid with the unfolded tail. Returns whether a
    /// duplicate poisoned the build.
    async fn write_build_entries(
        &self,
        store: &SlotStore,
        info: &IndexInfo,
        index: IndexId,
        batch: &[IndexEntry],
    ) -> Result<bool> {
        if batch.is_empty() {
            return Ok(false);
        }
        let staged = build_staged_entries(info, index, batch)?;
        let head = slot_commit::materialize_slot_head(store).await?;
        let overlay = head.overlay;
        let outcome = folder::with_folder(store, async |db| {
            let tx = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            let probe = index_maintenance::ProbeHandle::Overlaid {
                store: ReadHandle::Tx(&tx),
                overlay: &overlay,
            };
            let (poisoned, writes) = index_maintenance::plan_index_entries(probe, &staged).await?;
            commit::stage_writes(&tx, &writes)?;
            commit::commit_durably(db, tx).await.map_err(Error::from)?;
            Ok(!poisoned.is_empty())
        })
        .await;
        slot_commit::release_reader(head.reader.as_ref()).await;
        outcome
    }

    /// The staged build's persisted watermark. An index that is no longer
    /// building is refused.
    async fn staged_build_cursor(&self, table: TableId, index: IndexId) -> Result<Option<u64>> {
        let info = self
            .snapshot()
            .await?
            .indexes_of(table)
            .into_iter()
            .find(|info| info.id == index)
            .ok_or_else(|| Error::NotFound(format!("index {index}")))?;

        match info.state {
            IndexState::Building => Ok(info.build_cursor),
            IndexState::Ready => Err(Error::Constraint(format!(
                "index {index} finished building under this build"
            ))),
            IndexState::Poisoned => Err(Error::Constraint(format!(
                "index {index} was poisoned by a duplicate value"
            ))),
        }
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
        let head = match self.head_view().await {
            Ok(head) => head,
            Err(err) => {
                session.finish();
                return Err(err);
            }
        };

        // Scan the inline chunk bodies through the head's reader (fresh on a
        // slot-backed store) overlaid with the unfolded tail, so inline rows
        // committed to slots no folder has applied are backfilled rather than
        // silently skipped — an index built over them would otherwise flip
        // ready covering nothing.
        let scan_handle = head
            .reader
            .as_ref()
            .map_or(session.handle(), ReadHandle::Reader);
        let outcome =
            inline_backfill_from(scan_handle, head.tail.as_ref(), &head.view, table, columns).await;

        slot_commit::release_reader(head.reader.as_ref()).await;
        session.finish();
        outcome
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

        // The dead entries live in the unfolded tail until a fold applies them;
        // a reclaim reads and deletes them through the folded store, so it folds
        // the tail in first.
        let Store::Slots(store) = self.store.as_ref();
        if !store.read_only {
            folder::fold_sprint(store, u64::MAX).await?;
        }

        self.with_writer(async |db| {
            let tx = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            let deleted = index_maintenance::reclaim_entries(&tx, index.get(), limit).await?;
            commit::commit_durably(db, tx).await.map_err(Error::from)?;
            Ok(deleted)
        })
        .await
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
        self.ensure_writable()?;
        if request.batch_size == 0 {
            return Err(Error::Configuration(
                "batch_size must be nonzero; zero would reclaim nothing and never terminate"
                    .to_string(),
            ));
        }

        if !request.sweep_orphaned_index_entries {
            return Ok(MaintenanceReport::default());
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

        // Dead entries live in the unfolded tail until a fold applies them, and
        // the sweep reads and deletes them through the folded store, so it folds
        // the tail in first.
        let Store::Slots(store) = self.store.as_ref();
        folder::fold_sprint(store, u64::MAX).await?;

        // The entry deletions are derived-state upkeep in the index subspace,
        // never replayed into a view, so they run under the folder role: the
        // single direct writer of a slot-backed store.
        self.with_writer(async |db| {
            let mut report = MaintenanceReport::default();
            for kind in [IndexKind::Unique, IndexKind::Multi] {
                let mut from = 0u64;
                while let Some(index_id) = self.first_index_id_from(kind, from).await? {
                    if !live.contains(&index_id) {
                        let reclaimed = self
                            .reclaim_dead_range(db, kind, index_id, request.batch_size)
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
        })
        .await
    }

    /// What the store weighs, subspace by subspace.
    ///
    /// The default request reads the store's manifest and nothing else —
    /// two object reads, a cost independent of how large the store is — and
    /// reports physical bytes, SST counts, and sorted-run counts per
    /// subspace. Those figures include superseded versions and tombstones,
    /// which is the point: the gap between them and the live count is what
    /// [`compact_store`](Self::compact_store) reclaims.
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
        self.ensure_writable()?;

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

    /// The lowest index id at or after `from` holding an entry of `kind`,
    /// or `None` past the last one. One seek per distinct index present —
    /// the scan stops at the first key rather than walking the range.
    ///
    /// Scanned through a reader opened for this call, not the handle's
    /// follower reader: a fold earlier in the same maintenance pass may have
    /// just landed a dropped index's entry range in the store, and the
    /// follower lags the store by its poll interval — reading it would miss
    /// the range and reclaim nothing.
    pub(crate) async fn first_index_id_from(
        &self,
        kind: IndexKind,
        from: u64,
    ) -> Result<Option<u64>> {
        let Store::Slots(store) = self.store.as_ref();
        let kind_prefix = index_kind_prefix(kind);
        let start = index_index_prefix(kind, from);
        // `scan_prefix` takes its bounds as a suffix of the prefix.
        let suffix = start[kind_prefix.len()..].to_vec();

        let head = slot_commit::materialize_slot_head_fresh(store).await?;
        let first = head
            .handle(ReadHandle::Reader(&store.reader))
            .scan_prefix(kind_prefix, suffix..)
            .await
            .map_err(Error::from)?
            .next()
            .await
            .map_err(Error::from)?;
        slot_commit::release_reader(head.reader.as_ref()).await;

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

    /// Deletes every entry of one dead index, `batch_size` per commit,
    /// returning the total. The caller has already established that the
    /// index is not live, and holds the writer the batches commit through.
    async fn reclaim_dead_range(
        &self,
        db: &Db,
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
            let tx = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            let (deleted, last) = index_maintenance::reclaim_entries_from(
                &tx,
                kind,
                index_id,
                batch_size,
                cursor.as_deref(),
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
            tx.commit().await.map_err(Error::from)?;
            total += deleted as u64;
            cursor = last;
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
        let Store::Slots(store) = self.store.as_ref();
        store.reader.close().await.map_err(Error::from)
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

/// A table's dense row-id ranges, ordered for lookup. A query resolves every
/// hit against the same table, so the ranges are built once and searched per
/// row rather than rescanned; an index range can return far more rows than a
/// table has files.
struct RowHolders {
    /// `(start, end, file)` per file carrying a dense range, sorted by start
    /// and disjoint — each row id belongs to at most one file.
    ranges: Vec<(u64, u64, DataFileId)>,
}

impl RowHolders {
    fn of(files: &[DataFileInfo]) -> Self {
        let mut ranges: Vec<_> = files
            .iter()
            .filter_map(|file| {
                let start = file.row_id_start?;
                Some((start, start.saturating_add(file.record_count), file.id))
            })
            .collect();
        ranges.sort_unstable();
        Self { ranges }
    }

    /// The file whose range holds `row_id`, else `Inline` — an inlined row,
    /// or one in a file carrying explicit per-row ids rather than a range.
    fn holder(&self, row_id: u64) -> RowHolder {
        // Only the last range starting at or below `row_id` can contain it.
        let above = self.ranges.partition_point(|(start, ..)| *start <= row_id);
        match self.ranges[..above].last() {
            Some(&(_, end, file)) if row_id < end => RowHolder::DataFile(file),
            _ => RowHolder::Inline,
        }
    }
}
