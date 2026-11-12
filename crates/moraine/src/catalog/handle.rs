//! The catalog handle: the entry point a host opens, reads, and commits
//! through.

mod backfill;
mod index_build;
mod index_lookup;
mod index_predicate;
mod index_probe_cache;
pub use index_predicate::{IndexComparison, IndexPredicate, IndexPredicateRows};
mod index_read_scope;
mod index_verify;
mod reader_scopes;
pub use index_verify::IndexVerification;
mod inline_scan;
mod maintenance;
#[cfg(test)]
mod memory_fixture;
mod migration_store;
mod mutations;
mod revision_memo;
mod row_location;
mod row_lookup;
use crate::transaction::table_read;
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

pub use index_read_scope::{IndexReadIdentity, IndexReadScope};
pub(crate) use inline_scan::InlineScan;
pub use maintenance::{
    MaintenanceReport, MaintenanceRequest, MaintenanceStatusPass, MaintenanceStatusStep,
};
use object_store::ObjectStore;
pub use row_location::{
    ExcludedPositions, LocatedDeletion, LocatedRowScan, RowSummaryProgress, RowSummaryPublish,
};
use slatedb::{CloseReason, Db, DbReader, DbStatus, DbTransaction, IsolationLevel};
use tokio::sync::watch;
use tracing::{info, warn};

#[cfg(test)]
use crate::CatalogSnapshot;
use crate::{
    catalog::{
        RecentRow, SnapshotId, StoreCheckpoint, TableId, Timestamp, projection::ProjectionCache,
    },
    commit_log::LogCommitter,
    data_file,
    error::{Error, Result},
    store::{
        cache::{
            CacheCounters, CacheTally, MemoryTally, ObjectStoreTally, cache_status,
            metadata_shortfall,
        },
        census,
        handle::{ReadHandle, ReadSession},
        open::{self, StoreBuilder},
    },
    transaction::{
        MigrationReport, Transaction, commit, commit::MIN_FORMAT_VERSION, migration,
        session::CommitSession,
    },
};

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
    #[cfg(test)]
    materializations: AtomicU64,
    #[cfg(test)]
    refreshes: AtomicU64,
    #[cfg(test)]
    cache_hits: AtomicU64,
    head_reads: AtomicU64,
    /// Read transactions opened on the writer.
    read_transactions: AtomicU64,
    #[cfg(test)]
    materialize_micros: AtomicU64,
    #[cfg(test)]
    current_scans: AtomicU64,
}

#[cfg(test)]
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
    /// The store the write-ahead log lives on, when it is not the one
    /// beside it.
    wal_object_store: Option<Arc<dyn ObjectStore>>,
}

/// The open store behind a catalog: the read-write `Db` writer, or a
/// read-only `DbReader`. A read-only catalog never opens a `Db`, so it never
/// fences a live writer.
pub(crate) enum Store {
    /// The single read-write writer and the pacer its commits flush through.
    Writer {
        db: Db,
        pacer: Arc<commit::FlushPacer>,
        counters: Arc<crate::store::cache::CacheCounters>,
    },
    /// A read-only reader following the manifest, shared into read sessions.
    Reader(Arc<DbReader>),
    /// A committer over a tailing reader: reads what the log has served it
    /// and commits through the log alone.
    Committer {
        reader: Arc<DbReader>,
        commit_log: LogCommitter,
    },
}

impl Store {
    /// A read side over the store itself, each read atomic on its own.
    pub(crate) fn read_handle(&self) -> ReadHandle<'_> {
        match self {
            Self::Writer { db, .. } => ReadHandle::Writer(db),
            Self::Reader(reader) | Self::Committer { reader, .. } => ReadHandle::Reader(reader),
        }
    }

    /// Opens the session one batch stages against and commits through.
    ///
    /// # Errors
    ///
    /// [`Error::Constraint`] for a read-only store; the store's error if its
    /// transaction cannot begin.
    pub(crate) fn begin_commit_session(&self) -> Result<CommitSession> {
        match self {
            Self::Writer { db, counters, .. } => {
                if counters.slatedb_unflushed_bytes() >= crate::memory::store_memory_budget()? {
                    return Err(Error::Constraint("storage memory admission full; wait for the store to flush before retrying".into()));
                }
                Ok(CommitSession::folder(
                    db.begin(IsolationLevel::Snapshot).map_err(Error::from)?,
                ))
            }
            Self::Committer { reader, .. } => Ok(CommitSession::committer(Arc::clone(reader))),
            Self::Reader(_) => Err(Error::Constraint(
                "catalog opened read-only; writes are unavailable".to_string(),
            )),
        }
    }

    /// The commit log this store commits through, when its commits are
    /// slots.
    pub(crate) fn commit_log(&self) -> Option<&LogCommitter> {
        match self {
            Self::Committer { commit_log, .. } => Some(commit_log),
            Self::Writer { .. } | Self::Reader(_) => None,
        }
    }

    /// How a commit through this store reaches object storage. A reader
    /// commits nothing, so it names the waiting route.
    pub(crate) fn commit_durability(&self) -> commit::CommitDurability {
        match self {
            Self::Writer { pacer, .. } => commit::CommitDurability::Paced(Arc::clone(pacer)),
            // A committer's batch is durable at its slot; it awaits no flush.
            Self::Reader(_) | Self::Committer { .. } => commit::CommitDurability::OnFlushInterval,
        }
    }
}

/// What [`Catalog::demote`] left: the slots it folded on the way, and
/// the format the store now carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Demoted {
    /// Slots folded into the store before it was stamped.
    pub folded: u64,
    /// The format the store was stamped at: the one below the log's.
    pub format: u64,
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
// These are independent attach options, not states of one operation.
#[allow(clippy::struct_excessive_bools)]
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
    /// The minimum spacing between two flushes of the store's write-ahead
    /// log to object storage, each one PUT. A commit that finds the spacing
    /// elapsed flushes at once and waits only on its own PUT; one that
    /// finds it running joins a single flush deferred to when it elapses,
    /// so under load PUTs never exceed one per spacing and commits batch
    /// into them, and no commit waits longer than the spacing plus a PUT.
    /// Zero flushes every commit that finds no flush in the air. Defaults
    /// to 100ms.
    pub flush_interval: Duration,
    /// Whether every commit flushes on its own: the same as a zero
    /// [`flush_interval`](Self::flush_interval), kept for callers that set
    /// it. Defaults to `false`.
    pub flush_on_commit: bool,
    /// The store the write-ahead log is written to, when it is not the
    /// catalog's own ([`WalStore`](crate::WalStore) carries the name the
    /// catalog records for it). The log keeps the catalog's own
    /// [`path`](Self::path) within that store.
    ///
    /// Settled when the catalog is created: the name is recorded then, and
    /// every later open must name the same store — an open that names a
    /// different one, or none, is refused rather than replaying an empty
    /// log over live state. `None` (the default) keeps the log in the
    /// catalog store.
    pub wal_store: Option<crate::WalStore>,
    /// The commit-log provider this open builds its store over, outranking
    /// the one [`register_commit_log`](crate::register_commit_log) holds for
    /// the process. `None` (the default) falls back to the registered
    /// provider, and to the store's own log when none is registered.
    pub commit_log: Option<Arc<dyn crate::CommitLogProvider>>,
    /// Whether a read-only open over a commit log tails the log, seeing a
    /// commit once its slot lands rather than once a fold has carried it
    /// into the store, at a listing per
    /// [`reader_poll_interval`](Self::reader_poll_interval). A committer
    /// always tails, and refreshes rather than polls. Defaults to
    /// `false`.
    pub commit_log_tail: bool,
    /// How long a commit waits between refreshes while its slot is not
    /// yet served. A commit's wait is round trips, not ticks, so this
    /// only paces them. Defaults to 50ms.
    pub commit_log_poll_interval: Duration,
    /// How many slots a process holding the store folds before it writes
    /// its memtable out and truncates the log below the replay point. A
    /// cold open replays at most this many slots. Defaults to 256.
    pub commit_log_flush_slots: u64,
    /// How often a process holding the store looks for slots other
    /// committers landed, when it is not losing races to them. One listing
    /// per poll. Defaults to two seconds.
    pub commit_log_fold_poll: Duration,
    /// How old a slot must be before truncation may delete it, so a reader
    /// mid-replay is not pulled out from under. Defaults to five minutes.
    pub commit_log_retention: Duration,
    /// Every this many slots, the committer that lands one folds the log
    /// into the store: it opens the store, applies every slot the store
    /// lacks, writes it out, truncates the log below it, and closes. The
    /// sequence is the whole trigger, so no process is appointed and
    /// none measures anything to find out — whichever committer lands
    /// slot `n * commit_log_fold_slots` does that fold.
    ///
    /// Smaller folds more often, for less each time and a folded store
    /// that trails less; larger is rarer and lumpier, and the process
    /// that lands the slot pays. Zero folds after every commit.
    /// Defaults to 1024.
    pub commit_log_fold_slots: u64,
    /// Local directory under which each store's block cache and the parsed
    /// Parquet metadata cache keep their disk tiers, recovered by the next
    /// process to open them. When set, warm queries skip repeat object-store
    /// GETs and a re-attach starts warm — worthwhile for remote (`s3://`)
    /// stores, redundant for local ones. Process-wide, like
    /// [`cache_size`](Self::cache_size): the first catalog to open decides
    /// whether there is a disk tier at all, and where. `None` (the
    /// default) keeps the caches in memory.
    pub cache_dir: Option<std::path::PathBuf>,
    /// The object store's cache namespace; the catalog path is added
    /// separately. `None` isolates each open. Supply the same identity only
    /// for equivalent object stores to share cached blocks across opens and
    /// process restarts.
    pub cache_identity: Option<crate::CacheIdentity>,
    /// How many bytes of disk each store's cache device may hold. The
    /// first catalog to open settles it for the process. `None` (the
    /// default) leaves a cap of 16 GiB in force, and without a
    /// [`cache_dir`](Self::cache_dir) there is no device to bound.
    pub cache_size: Option<u64>,
    /// Process-wide allowance for SlateDB blocks/metadata, parsed Parquet
    /// metadata, and lookup retention; the first attach fixes it (default 640
    /// MiB). This is not an RSS limit: uncached query results, snapshots,
    /// write buffers, staging, DuckDB, and allocator overhead require
    /// additional memory.
    pub cache_memory: Option<u64>,
    /// Percent of each cache budget the auxiliary slot takes, for parsed
    /// Parquet metadata and file row summaries. Process-wide and settled by
    /// the first attach; unset takes one eightieth of each configured budget.
    pub cache_auxiliary_percent: Option<u32>,
    /// What to warm into the cache while the catalog opens, so the first
    /// query pays no first touch. Warming is reading, so it is bounded by
    /// the same caps and best-effort throughout — a subspace that cannot
    /// be read is skipped, never fatal — but it is part of the open, so an
    /// open that preloads returns only once it has. `None` (the default)
    /// warms nothing, leaving the cache to fill as reads ask for blocks.
    pub cache_preload: Option<CachePreload>,
    /// Whether flushed SST metadata and data blocks enter
    /// the process-shared decoded cache on write. Works with or without
    /// [`cache_dir`](Self::cache_dir); admitted blocks remain subject to
    /// eviction. Defaults to `true`; `false` disables flush admission.
    pub cache_puts: bool,
    /// Whether compaction-output SST metadata and data blocks enter the
    /// decoded cache on write. Defaults to `true`, independently of
    /// [`cache_puts`](Self::cache_puts), so a rewritten index is resident
    /// as soon as the merge lands; `false` keeps a large merge from
    /// displacing read-hot blocks.
    pub cache_compaction_puts: bool,
    /// Whether another process compacts and collects this store. When set,
    /// a writer opens with SlateDB's in-process compactor and garbage
    /// collector off, and a standalone compactor and collector run against
    /// the store instead. Every writer of such a store must set it: a
    /// writer's own compactor and a standalone one fence each other through
    /// the compactor epoch, and the fenced side closes its store. Defaults
    /// to `false`.
    pub external_compaction: bool,
    /// The lake's data root (DuckLake's `DATA_PATH`). Creation-time only:
    /// recorded as the stored global `data_path` option when a fresh store
    /// bootstraps, so a later open can read it back
    /// ([`CatalogSnapshot::data_path`](crate::CatalogSnapshot::data_path)).
    /// `None` records nothing.
    pub data_path: Option<String>,
    /// How stale a **read-only** catalog may be: a transaction keeps the
    /// writer's publication its predecessor pinned for up to this long
    /// before listing for a newer one, and the catalog's own reader polls
    /// the manifest at this interval.
    ///
    /// Nothing pushes a commit to a reader, so a read-only catalog can be
    /// one interval behind the writer. Shorter means fresher reads and more
    /// (on S3, billed) manifest reads; zero lists on every transaction.
    /// Defaults to 10 seconds.
    ///
    /// A committer's reads do not wait on it at all: each refreshes the
    /// reader and is served what the refresh found, and each transaction
    /// lists for the newest publication, so the log is read when a read
    /// asks and never on a clock. Its reader still polls at
    /// this interval, to keep the checkpoint it holds from expiring —
    /// longer is cheaper, and must stay well inside SlateDB's checkpoint
    /// lifetime.
    ///
    /// Ignored by a [`Catalog::open`] without a commit log, which reads
    /// its own writes, and by a catalog pinned to a
    /// [`checkpoint`](Self::checkpoint), which polls for nothing.
    pub reader_poll_interval: Duration,
    /// An existing checkpoint id (a UUID) to pin a **read-only** catalog to,
    /// as reported by [`Catalog::create_checkpoint`].
    ///
    /// The default (`None`) follows the writer by pinning the checkpoints
    /// it publishes, and writes nothing. Set it for a fixed cut that never
    /// sees a later commit, however long the catalog stays open. Refused by
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
            wal_store: None,
            commit_log: None,
            commit_log_tail: false,
            commit_log_poll_interval: Duration::from_millis(50),
            commit_log_flush_slots: 256,
            commit_log_fold_poll: Duration::from_secs(2),
            commit_log_retention: Duration::from_mins(5),
            commit_log_fold_slots: 1024,
            cache_dir: None,
            cache_identity: None,
            cache_size: None,
            cache_memory: None,
            cache_auxiliary_percent: None,
            cache_preload: None,
            cache_puts: true,
            cache_compaction_puts: true,
            external_compaction: false,
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

/// The object store `options` names for the write-ahead log, if any.
/// What a read-write open brings up: the store, its cache counters, its
/// format after any adoption, its pacer, and the log it commits through.
struct OpenedWriter {
    db: Db,
    cache: Arc<crate::store::cache::CacheCounters>,
    format: u64,
    pacer: Arc<commit::FlushPacer>,
}

/// The builder every writer open here shares. The writer paces its own
/// flushes, so the store's timer stays off and the pacing is exact.
fn writer_store_builder<'a>(
    object_store: &Arc<dyn ObjectStore>,
    options: &'a CatalogOptions,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    warm_segments: Vec<census::SegmentSize>,
) -> StoreBuilder<'a> {
    StoreBuilder::new(&options.path, Arc::clone(object_store))
        .wal_object_store(wal_object_store)
        .without_flush_timer()
        .cache_dir(options.cache_dir.clone())
        .cache_identity(options.cache_identity)
        .cache_size(options.cache_size)
        .cache_memory(options.cache_memory)
        .cache_auxiliary_percent(options.cache_auxiliary_percent)
        .cache_preload(options.cache_preload)
        .warm_segments(warm_segments)
        .cache_puts(options.cache_puts)
        .cache_compaction_puts(options.cache_compaction_puts)
        .external_compaction(external_compaction(options))
}

/// Opens the writer's store. A store stamped for a commit log is refused
/// here: with a provider present the open is a committer's, which never
/// reaches this.
async fn open_writer_store(
    object_store: Arc<dyn ObjectStore>,
    options: &CatalogOptions,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    warm_segments: Vec<census::SegmentSize>,
) -> Result<OpenedWriter> {
    let flush_spacing = if options.flush_on_commit {
        Duration::ZERO
    } else {
        options.flush_interval
    };
    let (db, cache, format, pacer) = commit::open_initialized(
        writer_store_builder(&object_store, options, wal_object_store, warm_segments),
        options.encrypted,
        options.data_path.as_deref(),
        options.wal_store.as_ref().map(crate::WalStore::name),
        flush_spacing,
        false,
    )
    .await?;

    Ok(OpenedWriter {
        db,
        cache,
        format,
        pacer,
    })
}

/// Opens the store at `options.path` as its writer over a fresh session,
/// bootstrapping an empty one, for the library's take and for demoting: the
/// open's replay is the fold. The session is returned with the store, for
/// the commit store built over both, and the format the store stands at.
async fn open_writer_db(
    object_store: Arc<dyn ObjectStore>,
    options: &CatalogOptions,
) -> Result<(Db, Arc<dyn crate::WriterSession>, u64)> {
    let provider = commit_log_provider(options).ok_or_else(|| {
        Error::Configuration("taking a log store requires a commit-log provider".into())
    })?;
    let builder = writer_store_builder(
        &object_store,
        options,
        wal_object_store(options),
        Vec::new(),
    );
    let session = provider.writer_session(builder.log_store(), &options.path);
    if !session.adopted().await? {
        commit::drain_own_log(writer_store_builder(
            &object_store,
            options,
            wal_object_store(options),
            Vec::new(),
        ))
        .await?;
    }
    let (db, _counters, format, _pacer) = commit::open_initialized(
        builder.writer_session(Some(Arc::clone(&session))),
        options.encrypted,
        options.data_path.as_deref(),
        options.wal_store.as_ref().map(crate::WalStore::name),
        options.flush_interval,
        true,
    )
    .await?;

    Ok((db, session, format))
}

/// How the library opens the store at `options.path` as its writer: the
/// catalog's own writer open, declined when a fold would stall.
struct Folder {
    object_store: Arc<dyn ObjectStore>,
    options: CatalogOptions,
}

#[async_trait::async_trait]
impl crate::TakeStore for Folder {
    async fn take_for_publication(&self) -> Result<(Db, Arc<dyn crate::WriterSession>)> {
        // Publication replays the WAL and closes without writing an L0 file.
        Box::pin(open_writer_db(
            Arc::clone(&self.object_store),
            &self.options,
        ))
        .await
        .map(|(db, session, _format)| (db, session))
    }

    async fn take(&self) -> Result<Option<(Db, Arc<dyn crate::WriterSession>)>> {
        // A fold writes an L0 file, and where the provider leaves
        // compaction to another process nothing merges those away. A
        // store already at its limit would stall on that write and never
        // come back, taking the commit that triggered the fold with it.
        // Fold nothing instead: the slots are durable, the log grows
        // rather than the writer stopping, and the first fold after a
        // compactor runs picks up everything this one left.
        let ceiling = slatedb::config::Settings::default().l0_max_ssts;
        let l0_files = census::l0_files(&self.options.path, Arc::clone(&self.object_store)).await?;
        if usize::try_from(l0_files).unwrap_or(usize::MAX) + 1 >= ceiling {
            warn!(
                l0_files,
                ceiling,
                "not folding: the store is at its limit of unmerged files and nothing has \
                 compacted it. Commits carry on and the log grows until a compactor serves \
                 this lake."
            );

            return Ok(None);
        }

        // Boxed: the open is a catalog open of its own.
        Box::pin(open_writer_db(
            Arc::clone(&self.object_store),
            &self.options,
        ))
        .await
        .map(|(db, session, _format)| Some((db, session)))
    }
}

/// What the catalog hands the library with the store or reader it opened.
fn catalog_hooks(
    object_store: &Arc<dyn ObjectStore>,
    options: &CatalogOptions,
) -> crate::CatalogHooks {
    crate::CatalogHooks {
        take: Arc::new(Folder {
            object_store: Arc::clone(object_store),
            options: options.clone(),
        }),
        inspect: Arc::new(inspect_head_stamp),
        path: options.path.clone(),
        object_store: Arc::clone(object_store),
        settings: crate::CommitLogSettings {
            fold_every: options.commit_log_fold_slots,
            flush_every: options.commit_log_flush_slots.max(1),
            retention: options.commit_log_retention,
            poll: options.commit_log_fold_poll,
            catch_up: crate::commit_log::READER_CATCHUP_TIMEOUT,
            refresh_backoff: options.commit_log_poll_interval,
        },
    }
}

/// Every slot past the first carries exactly one head stamp, which is what
/// lets a stamp name its slot; one that does not is corruption.
fn inspect_head_stamp(sequence: u64, envelope: &crate::SlotEnvelope) -> Result<()> {
    let head_key = crate::store::key::Key::Sys(crate::store::key::SysKey::Head).encode();
    let stamps = envelope
        .writes()
        .filter(|write| write.key == head_key)
        .count();
    if sequence > 1 && stamps != 1 {
        return Err(Error::Corruption(format!(
            "slot {sequence} carries {stamps} head stamps; every slot past the first carries one"
        )));
    }

    Ok(())
}

/// The commit-log provider an open builds its store over: the options' own,
/// else the process's registered one.
fn commit_log_provider(options: &CatalogOptions) -> Option<Arc<dyn crate::CommitLogProvider>> {
    options
        .commit_log
        .clone()
        .or_else(crate::commit_log::registered_commit_log)
}

/// How long a scope may keep the publication its predecessor listed. A
/// committer lists on every transaction, so each reads what the one before
/// it committed; a read-only catalog keeps it for the poll interval.
fn publication_reuse(options: &CatalogOptions, refresh_on_read: bool) -> Duration {
    if refresh_on_read {
        Duration::ZERO
    } else {
        options.reader_poll_interval
    }
}

/// Mints the log session used by an ordinary or fixed checkpoint reader.
fn reader_session(
    object_store: Arc<dyn ObjectStore>,
    options: &CatalogOptions,
    tail: bool,
) -> Option<Arc<dyn crate::ReaderSession>> {
    commit_log_provider(options).map(|provider| {
        let log_store = StoreBuilder::new(&options.path, object_store)
            .wal_object_store(wal_object_store(options))
            .log_store();
        provider.reader_session(log_store, &options.path, tail)
    })
}

/// Whether a writer leaves compaction and collection to another process:
/// asked for on the options, or declared by the provider its log rides.
fn external_compaction(options: &CatalogOptions) -> bool {
    options.external_compaction
        || commit_log_provider(options).is_some_and(|provider| provider.external_compaction())
}

fn wal_object_store(options: &CatalogOptions) -> Option<Arc<dyn ObjectStore>> {
    options
        .wal_store
        .as_ref()
        .map(|wal| Arc::clone(wal.object_store()))
}

/// Warns when an `All` preload cannot hold the store it is about to load.
/// Diagnostics only: a manifest that could not be read is left to the open
/// itself to report.
fn warn_if_preload_cannot_fit(options: &CatalogOptions, manifest: Option<&census::ManifestBytes>) {
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

/// Warns when this store's share of the process's block cache is smaller
/// than its index subspace: probes then read index blocks back from the
/// disk tier or object storage on every statement, at a hit rate that
/// looks healthy.
///
/// Diagnostics only. The share is the block capacity over the stores
/// attached, which is what the cache borrows toward when they are all busy.
fn warn_if_block_cache_cannot_hold_index(path: &str, index_bytes: Option<u64>) {
    let Some(index_bytes) = index_bytes else {
        return;
    };
    let status = cache_status();
    let attached_stores = status.attached_stores.max(1);
    let share_bytes = status.block_capacity_bytes / attached_stores;
    if let Some(shortfall) = crate::store::cache::index_share_shortfall(
        index_bytes,
        status.block_capacity_bytes,
        attached_stores,
    ) {
        warn!(
            path,
            index_bytes,
            block_capacity_bytes = status.block_capacity_bytes,
            attached_stores,
            share_bytes,
            shortfall,
            "this store's share of the block cache cannot hold its index subspace, so probes \
             will read index blocks from the disk tier or object storage on every statement. \
             Raise the cache memory budget on the first attach in the process."
        );
    }
}

/// Warns about what this store's own figures say the process's caches
/// cannot hold. Diagnostics only, from the manifest read the open already
/// made.
fn warn_if_caches_cannot_hold(path: &str, manifest: Option<&census::ManifestBytes>) {
    warn_if_metadata_cache_cannot_hold(path, manifest.map(|manifest| manifest.metadata_bytes));
    warn_if_block_cache_cannot_hold_index(
        path,
        manifest.and_then(|manifest| manifest.subspace_bytes(crate::store::key::Subspace::Index)),
    );
}

/// How long an open may take before a missing cache directory is worth
/// reporting. Below it the open is a handful of round trips whatever the
/// store holds; above it, it is re-reading bytes a local cache would have
/// kept.
const SLOW_OPEN: Duration = Duration::from_millis(250);

/// Whether an open was slow enough, with no cache directory configured, to
/// be worth telling the operator about.
pub(super) fn slow_open_without_cache(
    cache_dir: Option<&std::path::Path>,
    elapsed: Duration,
) -> bool {
    cache_dir.is_none() && elapsed >= SLOW_OPEN
}

/// Whether this is the first slow open to go unreported, latching
/// `warned`. The advice is to set `CACHE_DIR`, which an operator does
/// once, so repeating it on every open is noise rather than signal.
pub(super) fn first_slow_open(warned: &std::sync::atomic::AtomicBool) -> bool {
    !warned.swap(true, std::sync::atomic::Ordering::Relaxed)
}

/// Warns when a slow open had no cache directory to read from. The bytes
/// it fetched are fetched again by every process that opens this store,
/// where a cache directory keeps them on local disk: over 20 000 files
/// behind an endpoint costing about 20 ms a request, that was 1378 ms
/// against 451 ms. Reported once per process.
fn warn_if_slow_open_without_cache(options: &CatalogOptions, elapsed: Duration) {
    static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

    if !slow_open_without_cache(options.cache_dir.as_deref(), elapsed) {
        return;
    }
    if !first_slow_open(&WARNED) {
        return;
    }
    warn!(
        path = options.path,
        elapsed_ms = crate::telemetry::milliseconds(elapsed),
        "opening this store read what no cache directory kept, and every process that \
         opens it reads the same bytes again. Set `CACHE_DIR` to a local directory to \
         keep them on disk across restarts."
    );
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
pub type CommitMember<'a> = &'a (
        dyn for<'tx> Fn(
    &'tx mut Transaction,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<()>> + Send + 'tx>,
> + Sync
    );

/// Ceilings for the per-revision memos beside the probe cache: located
/// candidates are a few tens of bytes per row; a deletion ledger can run to
/// hundreds of thousands of positions.
const LOCATED_MEMO_BYTES: usize = 8 * 1024 * 1024;
/// Positions are eight bytes a row, held for both halves of one located
/// change rather than for a table's history.
const POSITIONED_MEMO_BYTES: usize = 8 * 1024 * 1024;
const LEDGER_MEMO_BYTES: usize = 32 * 1024 * 1024;
const MEMO_ENTRIES: usize = 4096;

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
/// reader.commit(async |tx| tx.create_schema("nope").await.map(|_| ())).await?;
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
/// writer.commit(async |tx| tx.create_schema("sales").await.map(|_| ())).await?;
/// # writer.close().await?;
/// let reader = Catalog::open_read_only(object_store, CatalogOptions::default()).await?;
/// assert!(reader.read_scope().await?.schema_by_name("sales").await?.is_some());
/// # Ok::<(), moraine::Error>(()) }).unwrap();
/// ```
#[derive(Clone)]
pub struct ReadOnlyCatalog {
    pinned: Option<Arc<index_read_scope::PinnedRead>>,
    reader_scopes: Option<Arc<reader_scopes::ReaderScopes>>,
    store: Arc<Store>,
    reads: Arc<ReadTally>,
    cache: Arc<CacheCounters>,
    data_reads: Arc<data_file::DataStoreCounters>,
    row_lookups: Arc<row_lookup::RowLookupCache>,
    index_probes: Arc<index_probe_cache::IndexProbeCache>,
    /// The exact positions a request's rows sit at, keyed by the rows asked
    /// for; the scan and the deletion of one located change position once.
    positioned_files: Arc<
        revision_memo::RevisionMemo<
            row_location::PositioningKey,
            Vec<row_location::PositionedFile>,
        >,
    >,
    /// The file candidates a resolved probe's row ids located, keyed by
    /// those ids; a rebind at the same revision locates nothing twice.
    located_rows: Arc<revision_memo::RevisionMemo<Vec<u64>, Vec<crate::catalog::FileRowCandidate>>>,
    /// A table's committed inlined deletions by data file, read once per
    /// revision however many scans a statement opens.
    inlined_file_deletes: Arc<
        revision_memo::RevisionMemo<
            u64,
            std::collections::HashMap<u64, std::collections::HashSet<u64>>,
        >,
    >,
    location: Arc<StoreLocation>,
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
    commits: Arc<commit::Coalescer>,
    // `None` on a read-only handle.
    writer_status: Option<watch::Receiver<DbStatus>>,
    /// The session a read brings the reader up to the log through before
    /// it runs. A committer carries one, so a peer's slot is never a tick
    /// away; every other store leaves the reader to its own poll.
    refresh: Option<Arc<dyn crate::ReaderSession>>,
    /// What a read-only reader over a commit log lists it for: a read
    /// asks, and the next poll lists. `None` where there is no log to
    /// list, or where reads refresh instead.
    asked: Option<Arc<crate::commit_log::asked::Asked>>,
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
    /// Invalidates revision-derived lookup bounds after a host-side conflict.
    /// Kept for callers using the former decoded-view invalidation API.
    pub fn forget_head_view(&self) {
        crate::catalog::projection::invalidate_head_view(self.projections());
    }

    pub(crate) fn projections(&self) -> &Arc<std::sync::RwLock<ProjectionCache>> {
        &self.projections
    }

    /// Where this catalog's concurrent commits meet.
    pub(crate) fn commits(&self) -> &Arc<commit::Coalescer> {
        &self.commits
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

    /// Estimated bytes in bounded row directories and scoped probe results,
    /// outside the process-shared block-cache allowance. Catalog descriptors
    /// are operation-owned. Shared allocations can be counted twice.
    #[must_use]
    pub fn projection_bytes(&self) -> u64 {
        self.row_lookups
            .estimated_bytes()
            .saturating_add(self.index_probes.estimated_bytes())
            .saturating_add(self.located_rows.estimated_bytes())
            .saturating_add(self.positioned_files.estimated_bytes())
            .saturating_add(self.inlined_file_deletes.estimated_bytes())
    }

    /// Logical memory attributed to this catalog and the process-shared caches.
    #[must_use]
    pub fn memory_tally(&self) -> MemoryTally {
        let status = cache_status();
        let (last_commit_index_entries, last_commit_staged_bytes) = self.cache.last_commit_memory();
        MemoryTally {
            slatedb_unflushed_bytes: self.cache.slatedb_unflushed_bytes(),
            projection_bytes: self.projection_bytes(),
            cache_metadata_bytes: status.metadata_occupancy_bytes,
            cache_block_bytes: status.block_occupancy_bytes,
            auxiliary_metadata_bytes: status.auxiliary_metadata_occupancy_bytes,
            last_commit_index_entries,
            last_commit_staged_bytes,
        }
    }

    pub(crate) fn cache_counters(&self) -> Arc<CacheCounters> {
        Arc::clone(&self.cache)
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
    #[cfg(test)]
    pub(crate) fn tally_current_scan(&self) {
        self.reads.current_scans.fetch_add(1, Ordering::Relaxed);
    }

    /// The number of full current-subspace materializations.
    #[cfg(test)]
    pub(crate) fn full_entity_scans(&self) -> u64 {
        self.reads.current_scans.load(Ordering::Relaxed)
    }

    /// Records that a read resolved the head from the store.
    pub(crate) fn note_head_read(&self) {
        self.reads.head_reads.fetch_add(1, Ordering::Relaxed);
    }

    /// How many read transactions this handle has opened on the writer.
    #[cfg(test)]
    pub(crate) fn read_transactions(&self) -> u64 {
        self.reads.read_transactions.load(Ordering::Relaxed)
    }

    /// Refuses if this handle has lost the writer epoch, or its `Db` has
    /// closed for any other reason. Reads the status channel only: no
    /// store read and no session.
    pub(crate) fn refuse_if_closed(&self) -> Result<()> {
        let reason = if let Some(status) = &self.writer_status {
            status.borrow().close_reason
        } else {
            match self.store.as_ref() {
                Store::Reader(reader) | Store::Committer { reader, .. } => {
                    reader.status().close_reason
                }
                Store::Writer { db, .. } => db.status().close_reason,
            }
        };
        match reason {
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
    /// The store revision a pinned read scope holds, under which a read's
    /// answer is fixed and may be memoized; `None` outside one.
    pub(crate) fn pinned_revision(&self) -> Option<u64> {
        self.pinned.as_ref().map(|pinned| pinned.revision)
    }

    /// The held view and a transaction-free read over the writer, on a
    /// warm read-write handle; `None` on a read-only handle or a cold
    /// writer. The view stands at head: this handle is the store's only
    /// writer. Pair a pass over it with [`still_holds`](Self::still_holds).
    /// A handle owning what it reads through, so work on it can run on a
    /// task: the pinned scope's transaction when one is held, else the
    /// store's own writer or reader.
    pub(crate) fn pinned_read_handle(&self) -> Option<ReadHandle<'_>> {
        self.pinned.as_ref().map(|pinned| pinned.handle.borrow())
    }

    pub(crate) fn owned_read_handle(&self) -> crate::store::handle::OwnedReadHandle {
        use crate::store::handle::OwnedReadHandle;

        if let Some(pinned) = &self.pinned {
            return pinned.handle.clone();
        }
        match self.store.as_ref() {
            Store::Writer { db, .. } => OwnedReadHandle::Writer(db.clone()),
            Store::Reader(reader) | Store::Committer { reader, .. } => {
                OwnedReadHandle::Reader(reader.clone())
            }
        }
    }

    /// An immutable view of the catalog at the latest committed snapshot.
    ///
    /// Explicitly materializes all catalog entities. The caller owns this
    /// allocation; the handle does not retain a decoded copy.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be read.
    #[cfg(test)]
    pub(crate) async fn materialized(&self) -> Result<Arc<CatalogSnapshot>> {
        self.view(None).await
    }

    /// An immutable view of the catalog as of `snapshot` (time travel).
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if `snapshot` is beyond the head, or
    /// another error if the store cannot be read.
    #[cfg(test)]
    pub(crate) async fn materialized_at(
        &self,
        snapshot: SnapshotId,
    ) -> Result<Arc<CatalogSnapshot>> {
        self.view(Some(snapshot.get())).await
    }

    /// A table's inlined rows live at the latest committed snapshot, in
    /// row-id order, each carrying the Arrow IPC bytes it decodes from.
    ///
    /// These rows are not part of a [`crate::ReadScope`]: they are served
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
    ///     .commit(async |tx| {
    ///         let main = tx.schema_by_name("main").await?.expect("bootstrap schema").id;
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
    ///         ).await?;
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
    ///         ).await?;
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
    /// [`crate::ReadScope::data_files_of`] serves, never from here.
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
        let session = self.begin_read().await?;
        let outcome = self.scan_recent_row(&session, table, row_id).await;
        session.finish();
        outcome
    }

    /// Materializes a caller-owned view at one store read point.
    #[cfg(test)]
    async fn view(&self, at: Option<u64>) -> Result<Arc<CatalogSnapshot>> {
        let session = self.begin_read().await?;
        let started = Instant::now();
        let view = commit::materialize(session.handle(), at).await;
        session.finish();
        if view.is_ok() {
            self.reads.materialized(started.elapsed());
        }
        view.map(Arc::new)
    }

    #[cfg(test)]
    async fn head_view(&self, handle: ReadHandle<'_>, _epoch: u64) -> Result<Arc<CatalogSnapshot>> {
        self.load_head_view(handle).await
    }

    #[cfg(test)]
    async fn load_head_view(&self, handle: ReadHandle<'_>) -> Result<Arc<CatalogSnapshot>> {
        self.note_head_read();
        let started = Instant::now();
        let view = commit::materialize(handle, None).await?;
        self.reads.materialized(started.elapsed());
        self.tally_current_scan();
        Ok(Arc::new(view))
    }

    /// Opens a read session at the current head — a read-write transaction or
    /// the read-only reader — the same isolation
    /// [`snapshot`](Self::read_scope)/[`snapshot_at`](Self::read_scope_at) use.
    /// Every read of the store opens its session here, so this is where a
    /// store mid-structural-migration is refused. The check is keyed on
    /// the head: a migration start moves it, so a head already observed
    /// clear opens without re-reading the marker — the head is rewritten
    /// every batch and stays hot, where the absent marker is the store's
    /// most expensive possible read.
    pub(crate) async fn begin_read(&self) -> Result<ReadSession> {
        if let Some(pinned) = &self.pinned {
            self.refuse_if_closed()?;
            return Ok(ReadSession::Pinned(pinned.handle.clone()));
        }
        let session = match self.store.as_ref() {
            Store::Writer { db, .. } => {
                self.reads.read_transactions.fetch_add(1, Ordering::Relaxed);
                ReadSession::Tx(db.begin(IsolationLevel::Snapshot).map_err(Error::from)?)
            }
            Store::Reader(reader) | Store::Committer { reader, .. } => {
                if let Some(asked) = &self.asked {
                    asked.ask();
                }
                if let Some(session) = &self.refresh {
                    // Refreshed rather than polled for, so the read is
                    // served the log as it stands.
                    session.refresh(reader).await?;
                }
                ReadSession::Reader(reader.clone())
            }
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
        // A commit store closes the handle it was built over: a held store
        // is written out and let go of first.
        match self.store.as_ref() {
            Store::Writer { db, pacer, .. } => {
                let publication = if db.status().close_reason.is_none() {
                    pacer.publish_flushed().await.map_err(Error::from)
                } else {
                    Ok(())
                };
                let closed = db.close().await.map_err(Error::from);
                publication.and(closed)
            }
            Store::Reader(reader) => {
                if let Some(scopes) = &self.reader_scopes {
                    scopes.close().await;
                }
                reader.close().await.map_err(Error::from)
            }
            Store::Committer { commit_log, .. } => {
                if let Some(scopes) = &self.reader_scopes {
                    scopes.close().await;
                }
                commit_log.store.close().await
            }
        }
    }
}

impl Catalog {
    /// Refuses a read-only handle; a writer and a committer both pass.
    fn require_writes(&self) -> Result<()> {
        match self.store.as_ref() {
            Store::Reader(_) => Err(Error::Constraint(
                "catalog opened read-only; writes are unavailable".to_string(),
            )),
            Store::Writer { .. } | Store::Committer { .. } => Ok(()),
        }
    }

    /// The store this handle writes into directly: the writer's own, or the
    /// one a committer holds.
    fn writer(&self) -> Result<Db> {
        match self.store.as_ref() {
            Store::Writer { db, .. } => Ok(db.clone()),
            Store::Reader(_) => Err(Error::Constraint(
                "catalog opened read-only; writes are unavailable".to_string(),
            )),
            Store::Committer { .. } => Err(Error::Constraint(
                "this process commits through the log and does not hold the store".to_string(),
            )),
        }
    }

    /// Reports the store's format and raises it to the supported additive
    /// ceiling when needed. This build requires format 9 at open, so an
    /// attached catalog already meets that ceiling and this call is a no-op.
    ///
    /// `dry_run` reports the result without writing. Structural upgrades
    /// require [`Self::migrate`] instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the store is mid-migration, or if the stamp
    /// cannot be committed durably.
    pub async fn raise_format(
        &self,
        dry_run: bool,
    ) -> Result<crate::transaction::migration::FormatRaise> {
        let raised = crate::transaction::migration::raise_format(
            &self.writer()?,
            &self.store.commit_durability(),
            dry_run,
        )
        .await?;
        if !dry_run {
            crate::catalog::projection::raise_format_floor(self.projections(), raised.to_format);
        }
        Ok(raised)
    }

    /// Raises the catalog version this store records to the newest shape
    /// this build serves, and reports what moved.
    ///
    /// The shape moraine synthesizes follows that version, so this is
    /// **one-way in practice**: a DuckLake that requires the older version
    /// refuses the store outright at its own catalog-version check, before
    /// it reads a table. Raise only alongside the DuckLake that asks for
    /// the wider shape.
    ///
    /// `dry_run` reports the move it would make and records nothing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Migration`] if the store records a version this
    /// build does not serve — a newer build wrote it, and lowering it
    /// would move a shape this one cannot read — or an error if the move
    /// cannot be committed.
    pub async fn raise_catalog_version(
        &self,
        dry_run: bool,
    ) -> Result<crate::transaction::migration::CatalogVersionRaise> {
        let from = crate::ffi_support::catalog_read::global_option(self, "version")
            .await?
            .unwrap_or_else(|| crate::catalog::CATALOG_VERSION.to_owned());
        let to = crate::catalog::MAX_CATALOG_VERSION.to_owned();

        // Never lower a store: a version this build cannot order is one a
        // newer build recorded, and that shape is not ours to move.
        if !crate::catalog::serves_catalog_version(&from) {
            return Err(Error::Migration(format!(
                "store records catalog version {from}, which this build does not serve"
            )));
        }

        if dry_run || from == to {
            return Ok(crate::transaction::migration::CatalogVersionRaise { from, to });
        }

        let target = to.clone();
        self.commit_scoped(
            crate::transaction::scoped::CommitRead::Options(crate::OptionScope::Global),
            move |tx| {
                // Re-read inside the commit. Recording the version is
                // last-write-wins, so a check made against the snapshot above
                // could be overtaken between that read and this write; this is
                // the one that holds the never-lower invariant.
                let recorded = tx
                    .catalog_version()
                    .unwrap_or_else(|| crate::catalog::CATALOG_VERSION.to_owned());
                if !crate::catalog::serves_catalog_version(&recorded) {
                    return Err(Error::Migration(format!(
                        "store records catalog version {recorded}, which this build does not serve"
                    )));
                }

                tx.set_catalog_version(&target);
                Ok(())
            },
        )
        .await?;
        Ok(crate::transaction::migration::CatalogVersionRaise { from, to })
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
    /// assert_eq!(catalog.read_scope().await?.schemas().await?.len(), 1);
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
        // On a commit log every read-write open is a committer, which takes
        // the store itself when the tail is long or the log unadopted.
        if let Some(provider) = commit_log_provider(&options) {
            return Self::open_as_committer(object_store, options, provider).await;
        }
        let started = Instant::now();
        let located = Arc::clone(&object_store);
        // One manifest read serves both diagnostics, before the open.
        let manifest = census::manifest_bytes(&options.path, Arc::clone(&located))
            .await
            .ok();
        warn_if_preload_cannot_fit(&options, manifest.as_ref());
        let wal_object_store = options
            .wal_store
            .as_ref()
            .map(|wal| Arc::clone(wal.object_store()));
        let OpenedWriter {
            db,
            cache,
            format,
            pacer,
        } = open_writer_store(
            Arc::clone(&object_store),
            &options,
            wal_object_store.clone(),
            manifest
                .as_ref()
                .map(|manifest| manifest.segments.clone())
                .unwrap_or_default(),
        )
        .await?;
        warn_if_caches_cannot_hold(&options.path, manifest.as_ref());
        let elapsed = started.elapsed();
        info!(
            path = options.path,
            flush_interval_ms = options.flush_interval.as_millis(),
            flush_on_commit = options.flush_on_commit,
            elapsed_ms = crate::telemetry::milliseconds(elapsed),
            "opened catalog read-write"
        );
        warn_if_slow_open_without_cache(&options, elapsed);
        let projections = Arc::new(std::sync::RwLock::new(ProjectionCache::empty()));
        // The open already validated the stamp; commits below this floor
        // owe no format read.
        crate::catalog::projection::raise_format_floor(&projections, format);
        let durability = commit::CommitDurability::Paced(Arc::clone(&pacer));
        Ok(Self {
            inner: ReadOnlyCatalog {
                pinned: None,
                reader_scopes: None,
                writer_status: Some(db.subscribe()),
                store: Arc::new(Store::Writer {
                    db,
                    pacer,
                    counters: Arc::clone(&cache),
                }),
                location: Arc::new(StoreLocation {
                    path: options.path,
                    object_store: located,
                    wal_object_store,
                }),
                reads: Arc::new(ReadTally::default()),
                cache,
                data_reads: Arc::default(),
                row_lookups: Arc::new(row_lookup::RowLookupCache::shared()),
                index_probes: Arc::new(index_probe_cache::IndexProbeCache::shared()),
                located_rows: Arc::new(revision_memo::RevisionMemo::shared(
                    LOCATED_MEMO_BYTES,
                    MEMO_ENTRIES,
                )),
                positioned_files: Arc::new(revision_memo::RevisionMemo::shared(
                    POSITIONED_MEMO_BYTES,
                    MEMO_ENTRIES,
                )),
                inlined_file_deletes: Arc::new(revision_memo::RevisionMemo::shared(
                    LEDGER_MEMO_BYTES,
                    MEMO_ENTRIES,
                )),
                commits: Arc::new(commit::Coalescer::new(
                    Arc::clone(&projections),
                    durability,
                    None,
                )),
                projections,
                refresh: None,
                asked: None,
            },
        })
    }

    /// Opens the catalog as a commit-log committer: a tailing reader that
    /// polls at the commit-log poll interval, with every commit a slot. A
    /// log nothing has adopted is adopted first, by a store taken and let
    /// go.
    async fn open_as_committer(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
        provider: Arc<dyn crate::CommitLogProvider>,
    ) -> Result<Self> {
        let options = CatalogOptions {
            commit_log: Some(Arc::clone(&provider)),
            ..options
        };
        let log_store = StoreBuilder::new(&options.path, Arc::clone(&object_store))
            .wal_object_store(wal_object_store(&options))
            .log_store();
        // An unadopted log is adopted by whoever arrives first: open the
        // store as its writer, which adopts, and let it go, since no
        // committer keeps it.
        if !provider
            .writer_session(Arc::clone(&log_store), &options.path)
            .adopted()
            .await?
        {
            let (db, session, format) =
                Box::pin(open_writer_db(Arc::clone(&object_store), &options)).await?;
            let store =
                provider.holding(db.clone(), session, catalog_hooks(&object_store, &options));
            let adopted = commit::adopt_commit_log(store.as_ref(), &db, format).await;
            store.close().await?;
            adopted?;
        }

        // Every read refreshes, so the reader's poll is no part of
        // freshness: it runs only to keep its checkpoint from expiring,
        // at whatever interval the caller set.
        let session = provider.reader_session(Arc::clone(&log_store), &options.path, true);
        let mut inner = Self::open_reader_catalog(
            Arc::clone(&object_store),
            CatalogOptions {
                commit_log_tail: true,
                ..options.clone()
            },
            true,
            Some(Arc::clone(&session)),
        )
        .await?;
        let Some(Store::Reader(reader)) = Arc::into_inner(inner.store) else {
            return Err(Error::Corruption(
                "a fresh read-only open must hold its reader alone".to_string(),
            ));
        };
        let batch_origin = commit::batch_origin(ReadHandle::Reader(&reader)).await?;
        let store = provider.following(
            Arc::clone(&reader),
            session,
            catalog_hooks(&object_store, &options),
        );
        let log = LogCommitter {
            store,
            batch_origin,
        };
        inner.store = Arc::new(Store::Committer {
            reader,
            commit_log: log.clone(),
        });
        inner.commits = Arc::new(commit::Coalescer::new(
            Arc::clone(&inner.projections),
            commit::CommitDurability::OnFlushInterval,
            Some(log),
        ));
        info!(
            path = inner.location.path,
            "opened catalog as a commit-log committer"
        );

        Ok(Self { inner })
    }

    /// What the catalog hands the library for the store at `options.path`:
    /// how to open it as its writer, what every folded slot must carry,
    /// where it lives, and the cadence. For a maintenance process that
    /// folds a lake once, measures its unfolded tail, or holds its store
    /// with the library and no live catalog.
    #[must_use]
    pub fn commit_log_hooks(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
    ) -> crate::CatalogHooks {
        catalog_hooks(object_store, options)
    }

    /// How the library opens the store at `options.path` as its writer:
    /// the catalog's own writer open, declined when a fold would stall.
    /// The `take` of [`commit_log_hooks`](Self::commit_log_hooks).
    #[must_use]
    pub fn store_taker(
        object_store: Arc<dyn ObjectStore>,
        options: &CatalogOptions,
    ) -> Arc<dyn crate::TakeStore> {
        Arc::new(Folder {
            object_store,
            options: options.clone(),
        })
    }

    /// The commit store behind this catalog, when its commits are slots:
    /// the library's, for everything the seam does not carry.
    #[must_use]
    pub fn commit_store(&self) -> Option<Arc<dyn crate::CommitStore>> {
        self.store.commit_log().map(|log| Arc::clone(&log.store))
    }

    /// Commits `writes` as one head-preserving batch: the head keeps its
    /// snapshot and its batch counter moves by one, so a reader learns state
    /// moved and, on a commit log, the batch lands as a slot of its own.
    /// Retries a lost race against a fresh head. Returns once the store has
    /// the batch, not once it is durable: on a log the slot already is, and
    /// on a store of its own the batch sits in the memtable until the next
    /// flush. For rows idempotent to rewrite, the sweeps, where a batch
    /// lost to a crash is rediscovered.
    ///
    /// # Errors
    ///
    /// [`Error::RetryBudgetExhausted`] after
    /// [`MAX_COMMIT_ATTEMPTS`](commit::MAX_COMMIT_ATTEMPTS) lost races, or
    /// the store's error.
    pub(crate) async fn commit_own_rows(&self, writes: Vec<commit::StagedWrite>) -> Result<()> {
        self.commit_own_rows_with(writes, false).await
    }

    /// As [`commit_own_rows`](Self::commit_own_rows), returning once the
    /// store has made the batch durable, for a batch a crash must not take
    /// back: the per-index reclamation a host resumes from where it
    /// stopped.
    pub(crate) async fn commit_own_rows_durably(
        &self,
        writes: Vec<commit::StagedWrite>,
    ) -> Result<()> {
        self.commit_own_rows_with(writes, true).await
    }

    async fn commit_own_rows_with(
        &self,
        writes: Vec<commit::StagedWrite>,
        await_durable: bool,
    ) -> Result<()> {
        for _ in 0..commit::MAX_COMMIT_ATTEMPTS {
            let session = self.store.begin_commit_session()?;
            let base = commit::read_head_value(session.handle()).await?;
            commit::refuse_mid_migration_at(session.handle(), &self.projections, &base).await?;
            let head = base.snapshot_id;
            let mut batch = writes.clone();
            batch.push(commit::head_stamp(head, base.batch_seq));
            let staged = commit::stage_writes_on(&session, &batch).await?;
            let heads = commit::HeadTransition {
                before: head,
                after: head,
            };
            let landing = commit::Landing {
                projections: &self.projections,
                log: self.store.commit_log(),
            };
            let submitted =
                commit::submit_batch(Arc::new(session), heads, &batch, staged, landing).await?;
            match submitted {
                commit::Submission::LostRace => {}
                commit::Submission::Submitted(submitted) if await_durable => {
                    commit::await_submitted(
                        submitted,
                        &self.projections,
                        &self.store.commit_durability(),
                        1,
                    )
                    .await?;
                    return Ok(());
                }
                // The batch is in the store; nothing waits on its flush.
                commit::Submission::Submitted(_) => return Ok(()),
            }
        }

        Err(Error::RetryBudgetExhausted(
            "a head-preserving batch lost every race it ran".to_string(),
        ))
    }

    /// Whether this handle commits through a commit log, where a batch of
    /// its own contends with every other committer's.
    pub(crate) fn commits_through_log(&self) -> bool {
        self.store.commit_log().is_some()
    }

    /// Takes a lake off its commit log, so a binary without the log's
    /// provider can open it again.
    ///
    /// Adoption stamped the store at the log-resident format and put its
    /// commits in the log; a binary without the provider refuses such a
    /// store rather than commit past a log it cannot read. This is the
    /// way back: take the store, fold every slot, stamp it at the format
    /// below the log's, write that out, and let go. The provider's own
    /// log is then dead weight the provider removes — the base object
    /// and any slot — since this crate cannot reach into a log it only
    /// knows through plugs.
    ///
    /// Stop every committer first. Taking the store fences them off the
    /// store, but not off the log: a slot landing after the fold is a
    /// commit this would leave behind, so a tail that is not empty after
    /// the fold is refused rather than folded again and raced.
    ///
    /// A store already below the log's format is left as it is, with
    /// nothing folded and without taking it, so an interrupted demote
    /// can be run again. Demoting is one way where the provider numbers
    /// its slots above a store's own rows: a demoted store has rows up
    /// there, and such a provider refuses to adopt it again.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] without a commit-log provider — the lake
    /// was adopted with one and must be left with it; [`Error::Constraint`]
    /// if the store is at its limit of unmerged files, since the stamp is
    /// one more write and would stall, or if a committer landed a slot
    /// after the fold; otherwise the store's error.
    pub async fn demote(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
    ) -> Result<Demoted> {
        if commit_log_provider(&options).is_none() {
            return Err(Error::Configuration(
                "demoting needs the commit-log provider the lake was adopted with".to_string(),
            ));
        }

        // Read before taking: an open with the provider adopts a store
        // below the log's format, which a demoted store must not be again.
        let (reader, _counters) = StoreBuilder::new(&options.path, Arc::clone(&object_store))
            .wal_object_store(
                options
                    .wal_store
                    .as_ref()
                    .map(|wal| Arc::clone(wal.object_store())),
            )
            .open_reader()
            .await?;
        let format = crate::store::read::read_format(ReadHandle::Reader(&reader)).await?;
        reader.close().await.map_err(Error::from)?;
        match format {
            None => {
                return Err(Error::NotFound(format!(
                    "no catalog at {}: nothing to demote",
                    options.path
                )));
            }
            Some(format) if !commit::uses_commit_log(&format) => {
                return Ok(Demoted {
                    folded: 0,
                    format: format.format_version,
                });
            }
            Some(_) => {}
        }

        // The stamp is one more write and a flush. A store at its limit
        // would stall on it and never come back.
        let ceiling = slatedb::config::Settings::default().l0_max_ssts;
        let l0_files = census::l0_files(&options.path, Arc::clone(&object_store)).await?;
        if usize::try_from(l0_files).unwrap_or(usize::MAX) + 1 >= ceiling {
            return Err(Error::Constraint(format!(
                "the store holds {l0_files} unmerged files against a limit of {ceiling}; \
                 compact it before demoting, or the stamp cannot be written"
            )));
        }

        let Some(provider) = commit_log_provider(&options) else {
            return Err(Error::Configuration(
                "the store opened without a commit log to leave".to_string(),
            ));
        };
        let (db, session, format) = open_writer_db(Arc::clone(&object_store), &options).await?;
        let demoted_format = if format == commit::FORMAT_WITH_COMMIT_LOG {
            MIN_FORMAT_VERSION
        } else {
            format
        };
        let store = provider.holding(db.clone(), session, catalog_hooks(&object_store, &options));

        // The open replayed every slot; anything that landed since is a
        // committer still writing, and a second look that finds more says so.
        let folded = store.fold_tail().await?;
        if store.fold_tail().await? > 0 {
            let _ = store.close().await;
            return Err(Error::Constraint(
                "a slot landed after the fold: a committer is still writing this lake; stop \
                 every committer before demoting"
                    .to_string(),
            ));
        }

        // The format below the log's, with no batch origin: what the
        // bootstrap writes for a store that was never adopted.
        let mut batch = slatedb::WriteBatch::new();
        batch.put(
            crate::store::key::Key::Sys(crate::store::key::SysKey::Format).encode(),
            crate::store::value::encode_value(&crate::store::proto::FormatValue {
                format_version: demoted_format,
                writer_version: env!("CARGO_PKG_VERSION").to_string(),
                commit_log_batch_origin: None,
            }),
        );
        drop(
            db.write_with_options(batch, &slatedb::config::WriteOptions::default())
                .await
                .map_err(Error::from)?,
        );
        commit::flush_memtable(&db).await?;
        store.close().await?;
        info!(
            folded,
            format = demoted_format,
            "demoted the lake off its commit log"
        );

        Ok(Demoted {
            folded,
            format: demoted_format,
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
    /// catalog.commit(async |tx| tx.create_schema("sales").await.map(|_| ())).await?;
    /// let checkpoint = catalog.create_checkpoint(None).await?;
    ///
    /// // A commit after the checkpoint is not in it.
    /// catalog.commit(async |tx| tx.create_schema("ops").await.map(|_| ())).await?;
    ///
    /// let mut options = CatalogOptions::default();
    /// options.checkpoint = Some(checkpoint);
    /// let reader = Catalog::open_read_only(object_store, options).await?;
    /// let view = reader.read_scope().await?;
    /// assert!(view.schema_by_name("sales").await?.is_some());
    /// assert!(view.schema_by_name("ops").await?.is_none());
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn open_read_only(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
    ) -> Result<ReadOnlyCatalog> {
        Self::open_reader_catalog(object_store, options, false, None).await
    }

    /// [`open_read_only`](Self::open_read_only), refreshing the reader
    /// before every read when `refresh_on_read` — what a committer
    /// wants, where a peer's slot is worth a round trip. A reader
    /// without it follows its own poll.
    async fn open_reader_catalog(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
        refresh_on_read: bool,
        session: Option<Arc<dyn crate::ReaderSession>>,
    ) -> Result<ReadOnlyCatalog> {
        // A read-only tail is listed for a read; a committer refreshes
        // instead, and a store with no log has nothing to list.
        let asked = (!refresh_on_read && options.commit_log_tail && options.commit_log.is_some())
            .then(|| Arc::new(crate::commit_log::asked::Asked::default()));
        let checkpoint = parse_checkpoint(options.checkpoint.as_deref())?;
        let started = Instant::now();
        let located = Arc::clone(&object_store);
        // One manifest read serves both diagnostics, before the open.
        let manifest = census::manifest_bytes(&options.path, Arc::clone(&located))
            .await
            .ok();
        warn_if_preload_cannot_fit(&options, manifest.as_ref());
        let wal_object_store = options
            .wal_store
            .as_ref()
            .map(|wal| Arc::clone(wal.object_store()));
        // A reader over a log is built over a session: the committer's own,
        // or one minted here for a read-only open.
        let session = session.or_else(|| {
            reader_session(Arc::clone(&object_store), &options, options.commit_log_tail)
        });
        let store = StoreBuilder::new(&options.path, object_store)
            .wal_object_store(wal_object_store.clone())
            .reader_session(session.clone())
            .cache_dir(options.cache_dir.clone())
            .cache_identity(options.cache_identity)
            .cache_size(options.cache_size)
            .cache_memory(options.cache_memory)
            .cache_auxiliary_percent(options.cache_auxiliary_percent)
            .cache_preload(options.cache_preload)
            .warm_segments(
                manifest
                    .as_ref()
                    .map(|manifest| manifest.segments.clone())
                    .unwrap_or_default(),
            )
            .cache_puts(options.cache_puts)
            .cache_compaction_puts(options.cache_compaction_puts)
            .external_compaction(external_compaction(&options))
            .poll_interval(options.reader_poll_interval)
            .read_asked(asked.clone())
            .checkpoint(checkpoint);

        let (reader, cache, format) = commit::open_reader_initialized(
            store,
            options.wal_store.as_ref().map(crate::WalStore::name),
            commit_log_provider(&options).is_some(),
        )
        .await?;
        warn_if_caches_cannot_hold(&options.path, manifest.as_ref());
        let elapsed = started.elapsed();
        info!(
            path = options.path,
            checkpoint = options.checkpoint,
            elapsed_ms = crate::telemetry::milliseconds(elapsed),
            "opened catalog read-only"
        );
        warn_if_slow_open_without_cache(&options, elapsed);
        let projections = Arc::new(std::sync::RwLock::new(ProjectionCache::empty()));
        crate::catalog::projection::raise_format_floor(&projections, format);
        let reader = Arc::new(reader);
        let reader_scopes = Some(Arc::new(reader_scopes::ReaderScopes::new(
            located.clone(),
            options.clone(),
            reader.clone(),
            publication_reuse(&options, refresh_on_read),
        )));
        Ok(ReadOnlyCatalog {
            pinned: None,
            reader_scopes,
            writer_status: None,
            store: Arc::new(Store::Reader(reader)),
            location: Arc::new(StoreLocation {
                path: options.path,
                object_store: located,
                wal_object_store,
            }),
            reads: Arc::new(ReadTally::default()),
            cache,
            data_reads: Arc::default(),
            row_lookups: Arc::new(row_lookup::RowLookupCache::shared()),
            index_probes: Arc::new(index_probe_cache::IndexProbeCache::shared()),
            located_rows: Arc::new(revision_memo::RevisionMemo::shared(
                LOCATED_MEMO_BYTES,
                MEMO_ENTRIES,
            )),
            positioned_files: Arc::new(revision_memo::RevisionMemo::shared(
                POSITIONED_MEMO_BYTES,
                MEMO_ENTRIES,
            )),
            inlined_file_deletes: Arc::new(revision_memo::RevisionMemo::shared(
                LEDGER_MEMO_BYTES,
                MEMO_ENTRIES,
            )),
            commits: Arc::new(commit::Coalescer::new(
                Arc::clone(&projections),
                commit::CommitDurability::OnFlushInterval,
                None,
            )),
            projections,
            refresh: session.filter(|_| refresh_on_read),
            asked,
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
        let (db, log) = migration_store::open(object_store.clone(), &options).await?;

        let checkpoint = if request.checkpoint {
            let taken = open::create_checkpoint(&db, None).await?;
            info!(checkpoint = %taken, "took a pre-migration checkpoint");
            Some(taken)
        } else {
            None
        };

        let report = migration::run(&db, log.as_ref()).await;
        let closed = if let Some(log) = log {
            log.store.close().await
        } else {
            let publication = if report.is_ok() {
                crate::store::checkpoint::Publisher::new(
                    db.clone(),
                    StoreBuilder::new(&options.path, object_store.clone())
                        .wal_object_store(wal_object_store(&options))
                        .admin(),
                )
                .flushed()
                .await
                .map_err(Error::from)
            } else {
                Ok(())
            };
            let closed = db.close().await.map_err(Error::from);
            publication.and(closed)
        };

        // A failed migration keeps its checkpoint as the recovery point.
        let report = report.and_then(|report| closed.map(|()| report))?;

        if let Some(checkpoint) = checkpoint {
            StoreBuilder::new(&options.path, object_store)
                .wal_object_store(wal_object_store(&options))
                .delete_checkpoint(checkpoint)
                .await?;
        }

        Ok(report)
    }

    /// Moves the catalog's write-ahead log to `to` (into the catalog store
    /// itself, when `None`), and reports where it was and where it is now.
    ///
    /// `options.wal_store` names the log store as it stands — the one the
    /// catalog records — so a log already in the catalog store is moved out
    /// with `None` there and the new store here. A log already on `to` is
    /// left alone and reported as unmoved.
    ///
    /// Free-standing, and takes the writer twice: nothing may hold the
    /// catalog while this runs, and a live attach is fenced by it. The
    /// first open drains the log it finds into a sorted-string table, so a
    /// crash anywhere in the move leaves the catalog openable against one
    /// store or the other with nothing lost — at worst the move has to be
    /// run again. Every attach after it names `to`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Configuration`] if `options.wal_store` is not the
    /// log store the catalog records (the message names the one it does),
    /// [`Error::Corruption`] if there is no catalog at `options.path`, or a
    /// store error if a drain or the recording write fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, WalStore};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let object_store = Arc::new(InMemory::new());
    /// let catalog = Catalog::open(object_store.clone(), CatalogOptions::default()).await?;
    /// catalog.close().await?;
    ///
    /// let log = WalStore::new("memory://log", Arc::new(InMemory::new()));
    /// let moved = Catalog::move_wal_store(
    ///     object_store.clone(),
    ///     CatalogOptions::default(),
    ///     Some(log.clone()),
    /// )
    /// .await?;
    /// assert_eq!(moved.from, None);
    /// assert_eq!(moved.to.as_deref(), Some("memory://log"));
    ///
    /// // Every open from now on names it.
    /// let mut options = CatalogOptions::default();
    /// options.wal_store = Some(log);
    /// let reopened = Catalog::open(object_store, options).await?;
    /// reopened.close().await?;
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn move_wal_store(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
        to: Option<crate::WalStore>,
    ) -> Result<crate::WalStoreMove> {
        let builder = |wal: Option<&crate::WalStore>| {
            StoreBuilder::new(&options.path, Arc::clone(&object_store))
                .wal_object_store(wal.map(|wal| Arc::clone(wal.object_store())))
                .flush_interval(options.flush_interval)
                .cache_dir(options.cache_dir.clone())
                .cache_identity(options.cache_identity)
                .cache_size(options.cache_size)
                .cache_memory(options.cache_memory)
                .cache_auxiliary_percent(options.cache_auxiliary_percent)
                .external_compaction(external_compaction(&options))
        };

        crate::transaction::wal_move::run(
            builder(options.wal_store.as_ref()),
            builder(to.as_ref()),
            options.wal_store.as_ref().map(crate::WalStore::name),
            to.as_ref().map(crate::WalStore::name),
        )
        .await
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
        let id = open::create_checkpoint(&self.writer()?, lifetime).await?;
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
            .wal_object_store(wal_object_store(&options))
            .delete_checkpoint(id)
            .await
    }

    /// Every checkpoint the store's manifest carries, with the manifest it
    /// pins and when it lapses.
    ///
    /// Reads the manifest and never opens the writer `Db`, so it runs
    /// against a live catalog without fencing it. Reader-established
    /// checkpoints show up here too, which is what distinguishes a lease a
    /// live reader keeps refreshing from one nothing will ever release.
    ///
    /// # Errors
    ///
    /// Returns a store error if the manifest cannot be read.
    pub async fn checkpoints(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
    ) -> Result<Vec<StoreCheckpoint>> {
        let records = StoreBuilder::new(&options.path, object_store)
            .wal_object_store(wal_object_store(&options))
            .list_checkpoints()
            .await?;
        Ok(records
            .into_iter()
            .map(|record| StoreCheckpoint {
                id: record.id.to_string(),
                manifest_id: record.manifest_id,
                created_at: Timestamp::from_micros(record.created_micros),
                expires_at: record.expires_micros.map(Timestamp::from_micros),
            })
            .collect())
    }

    /// Opens a read-write transaction for the staged-row commit path. Fails
    /// with [`Error::Constraint`] on a read-only catalog.
    /// The open store, shared so a spent transaction can still read.
    pub(crate) fn store(&self) -> Arc<Store> {
        Arc::clone(&self.store)
    }

    pub(crate) fn begin_write_tx(&self) -> Result<DbTransaction> {
        self.writer()?
            .begin(IsolationLevel::Snapshot)
            .map_err(Error::from)
    }

    /// Opens the session one staged commit stages against and commits
    /// through.
    ///
    /// # Errors
    ///
    /// As [`Store::begin_commit_session`].
    pub(crate) fn begin_commit_session(&self) -> Result<CommitSession> {
        self.store.begin_commit_session()
    }

    /// Writes the memtable out as a sorted-string table, so a test can put
    /// what it committed behind block reads.
    #[cfg(test)]
    pub(crate) async fn flush_memtable(&self) -> Result<()> {
        use slatedb::config::{FlushOptions, FlushType};
        self.writer()?
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .map_err(Error::from)
    }

    /// Commits a raw store transaction through the writer's paced route,
    /// for tests that stage bytes the verbs do not; the writer runs no
    /// flush timer, so a bare durability wait would never resolve.
    #[cfg(test)]
    pub(crate) async fn commit_write_tx(&self, tx: DbTransaction) -> Result<()> {
        commit::commit_durable(
            tx,
            "test",
            crate::store::StagedBytes::default(),
            &self.store.commit_durability(),
        )
        .await
        .map(|_| ())
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
    /// usually as smaller commits. An unacknowledged submission returns
    /// [`Error::CommitOutcomeUnknown`]; retain external files and reconcile
    /// the operation before resubmitting.
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
    ///     .commit(async |tx| {
    ///         let sales = tx.create_schema("sales").await?;
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
    ///         ).await?;
    ///         Ok(())
    ///     })
    ///     .await?;
    /// // `main` plus the newly created `sales` schema.
    /// assert_eq!(catalog.read_scope_at(snapshot).await?.schemas().await?.len(), 2);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    #[cfg(test)]
    pub(crate) async fn commit_materialized<F>(&self, f: F) -> Result<SnapshotId>
    where
        F: Fn(&mut crate::transaction::verbs::Transaction) -> Result<()>,
    {
        let ids =
            commit::commit_cycle(&self.store, std::slice::from_ref(&f), &self.commits).await?;
        ids.first().copied().ok_or_else(|| {
            Error::Corruption("a commit of one member reported no snapshot".to_string())
        })
    }
}

impl Catalog {
    /// Commits an atomic mutation, loading metadata on demand.
    /// The callback may run again after a concurrent commit and must not
    /// perform external side effects.
    ///
    /// # Errors
    /// Returns a callback error, commit conflict, or store error.
    pub async fn commit<F>(&self, callback: F) -> Result<SnapshotId>
    where
        F: AsyncFn(&mut Transaction) -> Result<()>,
    {
        let ids = commit::commit_cycle(&self.store, &commit::DemandMember(callback), &self.commits)
            .await?;
        ids.first()
            .copied()
            .ok_or_else(|| Error::Corruption("commit returned no snapshot".into()))
    }

    /// Commits callbacks as one atomic group with one snapshot per member.
    /// Callbacks may be retried together and must not perform external side
    /// effects.
    ///
    /// # Errors
    /// Any callback error aborts the group. Store and conflict errors
    /// propagate.
    pub async fn commit_group(&self, members: &[CommitMember<'_>]) -> Result<Vec<SnapshotId>> {
        commit::commit_cycle(&self.store, &commit::DemandGroup(members), &self.commits).await
    }
}
