//! Opening a moraine store.
//!
//! [`StoreBuilder`] opens a store on an object store as a read-write [`Db`]
//! or a read-only [`DbReader`], carrying the shared open configuration.
//! Every store is created — and must thereafter be opened — with the
//! tag-byte segment extractor; SlateDB persists the extractor identity and
//! refuses a mismatched open.

use std::{path::PathBuf, sync::Arc, time::Duration};

use futures::{StreamExt, stream};
use object_store::ObjectStore;
use slatedb::{
    BlockCachePolicy, CacheTarget, Db, DbReader, DbReaderMode,
    admin::AdminBuilder,
    config::{CheckpointOptions, CheckpointScope, DbReaderOptions, Settings, SstBlockSize},
};
use uuid::Uuid;

use crate::{
    error::{Error, Result},
    store::{
        cache, census, counted,
        handle::{ReadHandle, SCAN_READ_AHEAD_BYTES, ScanShape},
        key, retry,
        segment::TagSegmentExtractor,
    },
};

/// The default WAL flush cadence when none is configured.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// How often a read-only handle polls for new state when none is
/// configured (SlateDB's own default).
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// How long a reader's view of the store outlives the moment it opened.
/// A reader keeps no checkpoint, so the collector's retention is what lets a
/// transaction finish reading the state it started on; it must therefore
/// exceed the longest transaction.
pub(crate) const READ_WINDOW: Duration = Duration::from_mins(15);
/// How often a writer polls the manifest for fencing and compacted state;
/// each poll is one object-store read.
const WRITER_MANIFEST_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// The key count from which an SST carries a bloom filter: every SST, so an
/// absent-key probe clears a filter instead of reading data blocks.
const MIN_FILTER_KEYS: u32 = 0;

/// The stored block grain for every writer and reader. Swept against 16
/// and 64 KiB over a cold attach and a wide scan of 20 000 files: all
/// three within noise, and the request count identical, since the bulk
/// read-ahead coalesces blocks into far larger fetches either way.
const SST_BLOCK_SIZE: SstBlockSize = SstBlockSize::Block4Kib;

#[cfg(test)]
mod probe_tests;

#[cfg(test)]
mod probe_bench;

#[cfg(test)]
mod compaction_cache_tests;

#[cfg(test)]
mod disk_tier_tests;

/// One checkpoint the manifest carries, with what it pins against garbage
/// collection and when it lapses. Times are microseconds from the Unix
/// epoch; `expires_micros` is `None` for a checkpoint minted without a
/// lifetime, which holds until it is deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CheckpointRecord {
    pub(crate) id: Uuid,
    pub(crate) manifest_id: u64,
    pub(crate) created_micros: i64,
    pub(crate) expires_micros: Option<i64>,
}

/// Creates a checkpoint of every write `db` has taken (not only the
/// already-durable ones), expiring after `lifetime` (never, if `None`),
/// and reports its id.
pub(crate) async fn create_checkpoint(db: &Db, lifetime: Option<Duration>) -> Result<Uuid> {
    let created = db
        .create_checkpoint(
            CheckpointScope::All,
            &CheckpointOptions {
                lifetime,
                ..CheckpointOptions::default()
            },
        )
        .await
        .map_err(Error::from)?;
    Ok(created.id)
}

/// Opens a moraine store on `object_store` — a read-write [`Db`] via
/// [`open_writer`](Self::open_writer) or a read-only [`DbReader`] via
/// [`open_reader`](Self::open_reader) — carrying the shared open
/// configuration: the WAL flush cadence (writer only) and the on-disk object
/// cache.
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct StoreBuilder<'a> {
    path: &'a str,
    object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    /// The commit-log session a writer is built over, when its commits are
    /// slots; the plug replays the log in place of the store's own.
    writer_session: Option<Arc<dyn crate::WriterSession>>,
    /// The commit-log session supplying durable WAL for a reader.
    reader_session: Option<Arc<dyn crate::ReaderSession>>,
    counters: Option<Arc<cache::CacheCounters>>,
    flush_interval: Option<Duration>,
    poll_interval: Duration,
    cache_dir: Option<PathBuf>,
    cache_identity: crate::CacheIdentity,
    cache_size: Option<u64>,
    cache_memory: Option<u64>,
    cache_auxiliary_percent: Option<u32>,
    cache_puts: bool,
    cache_compaction_puts: bool,
    external_compaction: bool,
    checkpoint: Option<Uuid>,
    /// A reader mode chosen by the caller, over the one the open derives.
    reader_mode: Option<DbReaderMode>,
}

impl<'a> StoreBuilder<'a> {
    /// A builder for the store at `path` on `object_store`, with the default
    /// flush cadence and no on-disk object cache. Every store SlateDB is
    /// opened on reads through retries, since SlateDB drains some of its
    /// reads outside its own.
    pub(crate) fn new(path: &'a str, object_store: Arc<dyn ObjectStore>) -> Self {
        Self::from_retrying_stores(path, retry::Store::wrap(object_store), None)
    }

    /// Uses stores already wrapped for read retries, preserving their identity
    /// across views.
    pub(crate) fn from_retrying_stores(
        path: &'a str,
        object_store: Arc<dyn ObjectStore>,
        wal_object_store: Option<Arc<dyn ObjectStore>>,
    ) -> Self {
        Self {
            path,
            object_store,
            wal_object_store,
            writer_session: None,
            reader_session: None,
            counters: None,
            flush_interval: Some(DEFAULT_FLUSH_INTERVAL),
            poll_interval: DEFAULT_POLL_INTERVAL,
            cache_dir: None,
            cache_identity: crate::CacheIdentity::default(),
            cache_size: None,
            cache_memory: None,
            cache_auxiliary_percent: None,
            cache_puts: true,
            cache_compaction_puts: true,
            external_compaction: false,
            checkpoint: None,
            reader_mode: None,
        }
    }

    /// Writes the write-ahead log to `wal_object_store` instead of the
    /// store's own, under the same keys. Nothing in the store itself
    /// records that: the catalog records the store's name and holds every
    /// later open to it.
    pub(crate) fn wal_object_store(
        mut self,
        wal_object_store: Option<Arc<dyn ObjectStore>>,
    ) -> Self {
        self.wal_object_store = wal_object_store.map(retry::Store::wrap);
        self
    }

    /// Builds the writer over `session`'s write-ahead-log plug, so a
    /// bucket-resident commit log is the store's log: the open replays it
    /// in place of the store's own, and the session is what the commit
    /// store is then built over. Writer only.
    pub(crate) fn writer_session(mut self, session: Option<Arc<dyn crate::WriterSession>>) -> Self {
        self.writer_session = session;
        self
    }

    /// Builds the reader over `session`'s write-ahead-log plug. Reader only.
    pub(crate) fn reader_session(mut self, session: Option<Arc<dyn crate::ReaderSession>>) -> Self {
        self.reader_session = session;
        self
    }

    /// The counters this store's requests and cache lookups land in, so
    /// every store one attach opens reports to one tally.
    pub(crate) fn counters(mut self, counters: Arc<cache::CacheCounters>) -> Self {
        self.counters = Some(counters);
        self
    }

    fn take_counters(&self) -> Arc<cache::CacheCounters> {
        self.counters.clone().unwrap_or_else(cache::store_counters)
    }

    /// The store the log lives on: the separate log store when one is
    /// configured, else the store itself.
    pub(crate) fn log_store(&self) -> Arc<dyn ObjectStore> {
        self.wal_object_store
            .as_ref()
            .map_or_else(|| Arc::clone(&self.object_store), Arc::clone)
    }

    /// Sets how often a read-only handle polls for new state. Reader only —
    /// a writer reads its own writes.
    pub(crate) fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Sets the WAL flush cadence, which bounds durable-commit latency;
    /// zero flushes continuously with no timer. Writer only — a reader
    /// never flushes.
    pub(crate) fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = Some(flush_interval);
        self
    }

    /// Opens the writer with no flush timer: nothing flushes the
    /// write-ahead log but an explicit `Db::flush`, so a durability wait
    /// must be paired with one.
    pub(crate) fn without_flush_timer(mut self) -> Self {
        self.flush_interval = None;
        self
    }

    /// Sets the local directory under which each store's block cache and
    /// the parsed-footer cache keep, and recover, their disk tiers.
    /// Process-wide: the first store to open decides whether there is a
    /// disk tier, and where. `None` (the default) keeps the caches in memory.
    pub(crate) fn cache_dir(mut self, cache_dir: Option<PathBuf>) -> Self {
        self.cache_dir = cache_dir;
        self
    }

    /// Shares a cache only with opens declaring the same object namespace.
    pub(crate) fn cache_identity(mut self, identity: Option<crate::CacheIdentity>) -> Self {
        if let Some(identity) = identity {
            self.cache_identity = identity;
        }
        self
    }

    /// Sets how many bytes of disk each store's cache device may hold.
    /// Process-wide: the first store to open settles it. `None` (the
    /// default) takes SlateDB's default; without a
    /// [`cache_dir`](Self::cache_dir) there is no device to bound.
    pub(crate) fn cache_size(mut self, cache_size: Option<u64>) -> Self {
        self.cache_size = cache_size;
        self
    }

    /// Sets how much memory every store's cache and the parsed-footer cache
    /// may hold together. Process-wide: the first store to open sizes it.
    /// `None` (the default) takes SlateDB's single-store default for the
    /// whole process.
    pub(crate) fn cache_memory(mut self, cache_memory: Option<u64>) -> Self {
        self.cache_memory = cache_memory;
        self
    }

    /// Sets the percent of each cache budget the auxiliary slot takes, for
    /// parsed Parquet metadata and file row summaries. Process-wide and
    /// settled by the first attach, like the budgets it divides. Unset
    /// keeps the derived share.
    pub(crate) fn cache_auxiliary_percent(mut self, percent: Option<u32>) -> Self {
        self.cache_auxiliary_percent = percent;
        self
    }

    /// Sets whether flushed SST metadata and data blocks enter the decoded
    /// cache as they are written (default: on).
    pub(crate) fn cache_puts(mut self, cache_puts: bool) -> Self {
        self.cache_puts = cache_puts;
        self
    }

    /// Sets compaction-output admission independently of flush admission.
    pub(crate) fn cache_compaction_puts(mut self, enabled: bool) -> Self {
        self.cache_compaction_puts = enabled;
        self
    }

    /// Leaves compaction and collection to another process: the writer
    /// starts neither SlateDB's compactor nor its collector.
    pub(crate) fn external_compaction(mut self, external: bool) -> Self {
        self.external_compaction = external;
        self
    }

    /// Pins the reader to an existing checkpoint instead of the latest
    /// manifest. Reader only, and the only zero-write open.
    pub(crate) fn checkpoint(mut self, checkpoint: Option<Uuid>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    /// Opens the reader in `mode` instead of the mode the open derives from
    /// its checkpoint and session, with bounded store retries so a refused
    /// write fails instead of stalling. Reader only.
    pub(crate) fn reader_mode(mut self, mode: DbReaderMode) -> Self {
        self.reader_mode = Some(mode);
        self
    }

    /// Opens (or creates) the store as a read-write [`Db`], with the
    /// per-store counters its reads tally into.
    pub(crate) async fn open_writer(&self) -> Result<(Db, Arc<cache::CacheCounters>)> {
        let mut settings = self.settings();
        let limit = crate::memory::store_memory_budget()?;
        settings.max_unflushed_bytes = usize::try_from(limit).map_err(|_| {
            Error::Configuration("the store memory budget exceeds platform capacity".into())
        })?;
        settings.l0_sst_size_bytes = settings
            .l0_sst_size_bytes
            .min(settings.max_unflushed_bytes / 2)
            .max(1);
        let counters = self.take_counters();
        let mut builder = Db::builder(self.path, Arc::clone(&self.object_store))
            .with_settings(settings)
            .with_sst_block_size(SST_BLOCK_SIZE)
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .with_filter_policies(super::index_filter::policies())
            .with_block_cache_policy(self.block_cache_policy())
            .with_metrics_recorder(cache::recorder(Arc::clone(&counters)));
        if let Some(wal) = &self.wal_object_store {
            builder = builder.with_wal_object_store(Arc::clone(wal));
        }
        if let Some(session) = &self.writer_session {
            builder = builder.with_wal_writer(session.writer_init());
        }

        let location = self.location();
        let cache_id = location.cache_id();
        if let Some(cache) =
            cache::shared(&self.cache_config(), location, Arc::clone(&counters)).await
        {
            builder = builder.with_db_cache(cache, cache_id);
        }

        let db = builder.build().await.map_err(Error::from)?;

        Ok((db, counters))
    }

    /// Opens the store read-only as a [`DbReader`], which never fences a
    /// live writer; the flush cadence is ignored. Without a
    /// [`checkpoint`](Self::checkpoint) the reader follows the latest
    /// manifest, writing a checkpoint of its own only when it follows a
    /// commit log.
    pub(crate) async fn open_reader(&self) -> Result<(DbReader, Arc<cache::CacheCounters>)> {
        self.open_reader_mode(false).await
    }

    /// Opens a reader fixed at the log boundary it finds, through the
    /// commit-log session when there is one.
    pub(crate) async fn open_fixed_reader(&self) -> Result<(DbReader, Arc<cache::CacheCounters>)> {
        self.open_reader_mode(true).await
    }

    async fn open_reader_mode(&self, fixed: bool) -> Result<(DbReader, Arc<cache::CacheCounters>)> {
        let (builder, options, counters) = self.reader_builder(fixed).await?;
        let reader = if fixed && let Some(session) = &self.reader_session {
            session
                .open_fixed_reader(builder, options, self.admin())
                .await?
        } else {
            builder.build().await.map_err(Error::from)?
        };
        Ok((reader, counters))
    }

    /// Opens a successor with independent protection when the provider supports
    /// it.
    pub(crate) async fn open_incremental_reader(
        &self,
        previous: &DbReader,
    ) -> Result<Option<(DbReader, Arc<cache::CacheCounters>)>> {
        let Some(session) = &self.reader_session else {
            return Ok(None);
        };
        let (builder, options, counters) = self.reader_builder(true).await?;
        let reader = session
            .open_incremental_reader(previous, builder, options, self.admin())
            .await?;
        Ok(reader.map(|reader| (reader, counters)))
    }

    async fn reader_builder(
        &self,
        fixed: bool,
    ) -> Result<(
        slatedb::DbReaderBuilder<String>,
        DbReaderOptions,
        Arc<cache::CacheCounters>,
    )> {
        let defaults = DbReaderOptions::default();
        let options = DbReaderOptions {
            manifest_poll_interval: self.poll_interval,
            // A managed checkpoint must outlive two polls.
            checkpoint_lifetime: defaults.checkpoint_lifetime.max(self.poll_interval * 3),
            // SlateDB retries a refused write without bound; a mode the caller
            // chose gives up after one retry, so the refusal reaches them.
            object_store_max_retries: self.reader_mode.map(|_| 1),
            ..defaults
        };
        let counters = self.take_counters();
        let mut builder = DbReader::builder(self.path.to_owned(), Arc::clone(&self.object_store))
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .with_filter_policies(super::index_filter::policies())
            .with_metrics_recorder(cache::recorder(Arc::clone(&counters)))
            .with_options(options.clone());

        if let Some(wal) = &self.wal_object_store {
            builder = builder.with_wal_object_store(Arc::clone(wal));
        }

        let location = self.location();
        let cache_id = location.cache_id();
        if let Some(cache) =
            cache::shared(&self.cache_config(), location, Arc::clone(&counters)).await
        {
            builder = builder.with_db_cache(cache, cache_id);
        }
        // A reader with no log to follow writes no checkpoint of its own:
        // its transactions pin the writer's publications, and this reader
        // serves head reads from the latest manifest between polls. A
        // reader over a log keeps SlateDB's managed checkpoint.
        let mode = match (self.reader_mode, self.checkpoint) {
            (Some(mode), _) => mode,
            (None, Some(checkpoint)) => DbReaderMode::Checkpoint(checkpoint),
            (None, None) if self.reader_session.is_none() => DbReaderMode::FollowLatest,
            (None, None) => DbReaderMode::ManagedCheckpoint,
        };
        builder = builder.with_reader_mode(mode);
        let wal: Arc<dyn slatedb::wal::WalReader> = if let Some(session) = &self.reader_session {
            session.reader()
        } else {
            // SlateDB instruments only the stores it wraps itself; this
            // reader's requests are tallied here so the tally sees them.
            let mut wal = slatedb::wal::SlateDbWalReaderBuilder::new()
                .with_path(object_store::path::Path::from(self.path))
                .with_object_store(counted::CountedStore::wrap(
                    Arc::clone(&self.object_store),
                    Arc::clone(&counters),
                    cache::RequestStore::Main,
                ))
                .with_options(slatedb::wal::SlateDbWalReaderOptions {
                    max_buffered_bytes: 512 * 1024,
                    max_fetch_tasks: 2,
                    read_ahead_bytes: 256 * 1024,
                });
            if let Some(store) = &self.wal_object_store {
                wal = wal.with_wal_object_store(counted::CountedStore::wrap(
                    Arc::clone(store),
                    Arc::clone(&counters),
                    cache::RequestStore::Wal,
                ));
            }
            Arc::new(wal.build()?)
        };
        let reservation = crate::MemoryReservation::reserve(0)?;
        let admitted = if fixed {
            super::reader_memory::AdmittedReader::fixed(wal, reservation)
        } else {
            super::reader_memory::AdmittedReader::new(wal, reservation)
        };
        builder = builder.with_wal_reader(Arc::new(admitted));

        Ok((builder, options, counters))
    }

    /// Deletes the checkpoint `checkpoint`, unpinning whatever it held
    /// against garbage collection.
    pub(crate) async fn delete_checkpoint(&self, checkpoint: Uuid) -> Result<()> {
        self.admin()
            .delete_checkpoint(checkpoint)
            .await
            .map_err(Error::from)
    }

    /// Every checkpoint the store's manifest currently carries, oldest
    /// first — reader-established ones included.
    pub(crate) async fn list_checkpoints(&self) -> Result<Vec<CheckpointRecord>> {
        let checkpoints = self
            .admin()
            .list_checkpoints(None)
            .await
            .map_err(Error::from)?;

        Ok(checkpoints
            .into_iter()
            .map(|c| CheckpointRecord {
                id: c.id,
                manifest_id: c.manifest_id,
                created_micros: c.create_time.timestamp_micros(),
                expires_micros: c.expire_time.map(|at| at.timestamp_micros()),
            })
            .collect())
    }

    /// The admin surface over this store, which reaches the write-ahead
    /// log wherever it lives.
    pub(crate) fn admin(&self) -> slatedb::admin::Admin {
        let mut builder = AdminBuilder::new(self.path, Arc::clone(&self.object_store));
        if let Some(wal) = &self.wal_object_store {
            builder = builder.with_wal_object_store(Arc::clone(wal));
        }
        builder.build()
    }

    /// SlateDB settings for a writer. The in-process compactor writes its
    /// SSTs with these too, so the filter threshold holds across a merge.
    fn settings(&self) -> Settings {
        let mut settings = Settings {
            flush_interval: self.flush_interval,
            manifest_poll_interval: WRITER_MANIFEST_POLL_INTERVAL,
            min_filter_keys: MIN_FILTER_KEYS,
            ..Default::default()
        };

        // Readers write nothing, so retention is the only thing keeping
        // what they read: the collector must not take an object a live
        // transaction still needs.
        let retained = slatedb::config::GarbageCollectorDirectoryOptions {
            min_age: READ_WINDOW,
            ..Default::default()
        };
        settings.garbage_collector_options = Some(slatedb::config::GarbageCollectorOptions {
            wal_options: Some(retained),
            compacted_options: Some(retained),
            ..Default::default()
        });

        if self.external_compaction {
            settings.compactor_options = None;
            settings.garbage_collector_options = None;
        }

        settings
    }

    fn location(&self) -> cache::StoreLocation {
        cache::StoreLocation {
            identity: self.cache_identity,
            path: self.path.to_owned(),
        }
    }

    /// How this store asks for the process-shared cache.
    fn cache_config(&self) -> cache::CacheConfig {
        cache::CacheConfig {
            auxiliary_percent: self.cache_auxiliary_percent,
            memory: self.cache_memory,
            dir: self.cache_dir.clone(),
            disk_size: self.cache_size,
        }
    }

    /// Which blocks of flushed and compacted SSTs enter the cache as they
    /// are written.
    fn block_cache_policy(&self) -> BlockCachePolicy {
        let targets: &[CacheTarget] = &[
            CacheTarget::Index,
            CacheTarget::Filters,
            CacheTarget::Stats,
            CacheTarget::data::<&[u8], _>(..),
        ];
        BlockCachePolicy::default()
            .with_flush_targets(if self.cache_puts { targets } else { &[] })
            .with_compaction_output_targets(if self.cache_compaction_puts {
                targets
            } else {
                &[]
            })
    }
}

/// The shape that reaches one entry of a subspace whose segment holds
/// `bytes`: probe when the whole segment fits one read-ahead, so the one
/// request each SST costs anyway brings the segment in and admits it; seek
/// otherwise, and when the size is unknown.
pub(crate) fn warm_shape(bytes: Option<u64>) -> ScanShape {
    let read_ahead = u64::try_from(SCAN_READ_AHEAD_BYTES).unwrap_or(u64::MAX);
    match bytes {
        Some(bytes) if bytes <= read_ahead => ScanShape::wide().retain(),
        _ => ScanShape::seek().retain(),
    }
}

/// Reads one entry of each subspace every statement reads, the system keys,
/// the live catalog, and the snapshot records, so their SST metadata is
/// resident. Best-effort; returns how many subspaces warmed and how many
/// failed.
pub(crate) async fn warm_every_statement(
    handle: ReadHandle<'_>,
    segments: &[census::SegmentSize],
) -> (usize, usize) {
    let subspaces = [
        key::Subspace::System,
        key::Subspace::Current,
        key::Subspace::Snapshot,
    ];

    let (warmed, failed) = stream::iter(subspaces.into_iter().map(|subspace| {
        let prefix = key::subspace_prefix(subspace);
        let shape = warm_shape(
            segments
                .iter()
                .find(|segment| segment.prefix == prefix)
                .map(|segment| segment.bytes),
        );
        async move {
            let mut iterator = handle.scan_prefix(&prefix, .., shape).await?;
            iterator.next().await?;
            Ok::<(), Error>(())
        }
    }))
    .buffer_unordered(subspaces.len())
    .fold((0_usize, 0_usize), |(warmed, failed), result| async move {
        match result {
            Ok(()) => (warmed + 1, failed),
            Err(_) => (warmed, failed + 1),
        }
    })
    .await;
    tracing::info!(warmed, failed, "warmed the subspaces every statement reads");
    (warmed, failed)
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::store::key::{self, Key, SysKey};

    fn memory_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    /// The warm touches the three subspaces every statement reads and
    /// nothing else.
    #[tokio::test]
    async fn the_warm_touches_what_every_statement_reads() {
        let (db, _counters) = StoreBuilder::new("warm", memory_store())
            .open_writer()
            .await
            .unwrap();
        let tx = db.begin(IsolationLevel::Snapshot).unwrap();
        assert_eq!(warm_every_statement(ReadHandle::Tx(&tx), &[]).await, (3, 0));
        tx.rollback();
        db.close().await.unwrap();
    }

    /// A subspace whose segment fits one read-ahead warms in probe shape,
    /// which brings the segment in for one request; a larger or unknown
    /// segment warms in seek shape.
    #[test]
    fn the_warm_shape_follows_the_segments_size() {
        assert_eq!(warm_shape(Some(4 * 1024)), ScanShape::wide().retain());
        assert_eq!(
            warm_shape(Some(u64::try_from(SCAN_READ_AHEAD_BYTES).unwrap())),
            ScanShape::wide().retain()
        );
        assert_eq!(
            warm_shape(Some(u64::try_from(SCAN_READ_AHEAD_BYTES).unwrap() + 1)),
            ScanShape::seek().retain()
        );
        assert_eq!(warm_shape(None), ScanShape::seek().retain());
    }

    #[test]
    fn identical_display_names_do_not_share_catalog_cache_locations() {
        let first = StoreBuilder::new("catalog", memory_store());
        let second = StoreBuilder::new("catalog", memory_store());
        assert_ne!(first.location(), second.location());
    }

    #[test]
    fn explicit_catalog_cache_identities_share_only_the_same_namespace_and_path() {
        let identity = crate::CacheIdentity::new("https://endpoint/bucket");
        let location = |path, identity| {
            StoreBuilder::new(path, memory_store())
                .cache_identity(Some(identity))
                .location()
        };
        assert_eq!(location("catalog", identity), location("catalog", identity));
        assert_ne!(location("catalog", identity), location("other", identity));
        assert_ne!(
            location("catalog", identity),
            location("catalog", crate::CacheIdentity::new("https://other/bucket"))
        );
    }

    /// The format grain is a moraine choice rather than an upstream default
    /// that can move without the matching scan/probe evidence.
    #[test]
    fn the_sst_block_size_is_fixed_at_four_kibibytes() {
        assert_eq!(SST_BLOCK_SIZE.as_bytes(), 4 * 1024);
    }

    /// Every SST a writer flushes or compacts carries a bloom filter, however
    /// few keys it holds.
    #[test]
    fn every_sst_carries_a_bloom_filter() {
        let settings = StoreBuilder::new("s", memory_store()).settings();
        assert_eq!(settings.min_filter_keys, 0);
    }

    /// A writer polls the manifest every five seconds, not SlateDB's default
    /// one.
    #[test]
    fn writers_poll_the_manifest_every_five_seconds() {
        let settings = StoreBuilder::new("s", memory_store()).settings();
        assert_eq!(settings.manifest_poll_interval, Duration::from_secs(5));
    }

    /// The writer settings are ones SlateDB accepts at open.
    #[test]
    fn writer_settings_pass_slatedb_validation() {
        let settings = StoreBuilder::new("s", memory_store()).settings();
        settings.validate().unwrap();
    }

    /// A writer under external compaction starts neither SlateDB's
    /// compactor nor its collector, and the settings still validate.
    #[test]
    fn external_compaction_turns_off_the_compactor_and_the_collector() {
        let settings = StoreBuilder::new("s", memory_store())
            .external_compaction(true)
            .settings();
        assert!(settings.compactor_options.is_none());
        assert!(settings.garbage_collector_options.is_none());
        settings.validate().unwrap();

        let settings = StoreBuilder::new("s", memory_store()).settings();
        assert!(settings.compactor_options.is_some());
        assert!(settings.garbage_collector_options.is_some());
    }

    /// A commit-shaped transaction spanning several subspaces lands
    /// atomically, and per-subspace prefix scans see exactly their own
    /// segment's keys (multi-segment batches satisfy the antichain rule).
    #[tokio::test]
    async fn multi_subspace_transaction_and_prefix_scans() {
        let (db, _) = StoreBuilder::new("test/store", memory_store())
            .open_writer()
            .await
            .unwrap();

        let head = Key::Sys(SysKey::Head).encode();
        let snapshot = Key::Snapshot { snapshot_id: 1 }.encode();
        let table = Key::current(key::EntityKey::Table { table_id: 7 }).encode();

        let tx = db.begin(IsolationLevel::Snapshot).unwrap();
        tx.put(&head, b"head").unwrap();
        tx.put(&snapshot, b"snap").unwrap();
        tx.put(&table, b"table").unwrap();
        tx.commit()
            .await
            .unwrap()
            .unwrap()
            .await_durable()
            .await
            .unwrap();

        assert_eq!(db.get(&head).await.unwrap().unwrap().as_ref(), b"head");

        // Each subspace scan returns exactly its own keys.
        let mut iter = db
            .scan_prefix(key::subspace_prefix(key::Subspace::Current), ..)
            .await
            .unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.key.as_ref(), table.as_slice());
        assert!(iter.next().await.unwrap().is_none());

        let mut iter = db
            .scan_prefix(key::subspace_prefix(key::Subspace::Snapshot), ..)
            .await
            .unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.key.as_ref(), snapshot.as_slice());
        assert!(iter.next().await.unwrap().is_none());

        db.close().await.unwrap();
    }

    /// A zero flush interval is allowed: SlateDB flushes continuously
    /// rather than on a timer, so the store opens and a durable write lands.
    #[tokio::test]
    async fn zero_flush_interval_opens_a_working_store() {
        let (db, _) = StoreBuilder::new("test/store", memory_store())
            .flush_interval(Duration::ZERO)
            .open_writer()
            .await
            .unwrap();

        let head = Key::Sys(SysKey::Head).encode();
        db.put(&head, b"head")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        assert_eq!(db.get(&head).await.unwrap().unwrap().as_ref(), b"head");

        db.close().await.unwrap();
    }

    /// An explicit flush interval reaches the SlateDB builder: the store
    /// opens, and a durable commit still lands.
    #[tokio::test]
    async fn explicit_flush_interval_opens_a_working_store() {
        let (db, _) = StoreBuilder::new("test/store", memory_store())
            .flush_interval(Duration::from_millis(1))
            .open_writer()
            .await
            .unwrap();

        let head = Key::Sys(SysKey::Head).encode();
        db.put(&head, b"head")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        assert_eq!(db.get(&head).await.unwrap().unwrap().as_ref(), b"head");

        db.close().await.unwrap();
    }

    /// SlateDB persists the extractor identity: reopening the store
    /// without it is refused rather than silently mis-segmented.
    #[tokio::test]
    async fn reopen_without_extractor_is_refused() {
        let object_store = memory_store();
        let (db, _) = StoreBuilder::new("test/store", object_store.clone())
            .open_writer()
            .await
            .unwrap();
        db.put(&Key::Sys(SysKey::Head).encode(), b"head")
            .await
            .unwrap();
        db.close().await.unwrap();

        let bare = Db::builder("test/store", object_store).build().await;
        assert!(bare.is_err(), "unsegmented reopen must be refused");
    }

    /// A configured cache directory leaves the store readable. Whether a
    /// device is created is the shared cache's business and is tested
    /// there — the process builds one cache, so the first store to open
    /// decides its shape and a later one cannot assert its own.
    #[tokio::test]
    async fn cache_dir_backs_the_block_slot_with_a_device() {
        let object_store = memory_store();
        let cache = std::env::temp_dir().join(format!("moraine-cache-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cache);

        let head = Key::Sys(SysKey::Head).encode();
        let (db, _) = StoreBuilder::new("s", object_store.clone())
            .flush_interval(Duration::from_millis(1))
            .cache_dir(Some(cache.clone()))
            .open_writer()
            .await
            .unwrap();
        db.put(&head, b"head").await.unwrap();
        db.close().await.unwrap();

        let (reader, _) = StoreBuilder::new("s", object_store)
            .cache_dir(Some(cache.clone()))
            .open_reader()
            .await
            .unwrap();
        assert_eq!(reader.get(head).await.unwrap().unwrap().as_ref(), b"head");
        reader.close().await.unwrap();

        let _ = std::fs::remove_dir_all(&cache);
    }

    /// The cache options a store asks for reach the shared cache's config
    /// verbatim: memory across both slots, the device and its cap.
    #[test]
    fn the_cache_config_carries_what_was_asked_for() {
        let object_store = memory_store();
        let unset = StoreBuilder::new("s", Arc::clone(&object_store));
        assert_eq!(unset.cache_config(), cache::CacheConfig::default());

        let dir = std::path::PathBuf::from("/tmp/moraine-config-test");
        let configured = StoreBuilder::new("s", object_store)
            .cache_memory(Some(1 << 30))
            .cache_dir(Some(dir.clone()))
            .cache_size(Some(64 * 1024 * 1024));
        assert_eq!(
            configured.cache_config(),
            cache::CacheConfig {
                auxiliary_percent: None,
                memory: Some(1 << 30),
                dir: Some(dir),
                disk_size: Some(64 * 1024 * 1024),
            }
        );
    }

    /// Flush and compaction-output admission both default on and move
    /// independently.
    #[test]
    fn both_admissions_default_on_and_stay_independent() {
        let object_store = memory_store();
        let admitted = [
            CacheTarget::Index,
            CacheTarget::Filters,
            CacheTarget::Stats,
            CacheTarget::data::<&[u8], _>(..),
        ];
        let unset = StoreBuilder::new("s", Arc::clone(&object_store));
        assert_eq!(
            unset.block_cache_policy(),
            BlockCachePolicy::default()
                .with_flush_targets(&admitted)
                .with_compaction_output_targets(&admitted)
        );
        let no_flush = StoreBuilder::new("s", Arc::clone(&object_store)).cache_puts(false);
        assert_eq!(
            no_flush.block_cache_policy(),
            BlockCachePolicy::default()
                .with_flush_targets(&[])
                .with_compaction_output_targets(&admitted)
        );
        let no_compaction =
            StoreBuilder::new("s", Arc::clone(&object_store)).cache_compaction_puts(false);
        assert_eq!(
            no_compaction.block_cache_policy(),
            BlockCachePolicy::default()
                .with_flush_targets(&admitted)
                .with_compaction_output_targets(&[])
        );
        let neither = StoreBuilder::new("s", object_store)
            .cache_puts(false)
            .cache_compaction_puts(false);
        assert_eq!(
            neither.block_cache_policy(),
            BlockCachePolicy::default()
                .with_flush_targets(&[])
                .with_compaction_output_targets(&[])
        );
    }
}
