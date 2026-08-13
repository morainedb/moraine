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
use tracing::info;
use uuid::Uuid;

use crate::{
    catalog::CachePreload,
    error::{Error, Result},
    store::{cache, handle::ScanShape, key, segment::TagSegmentExtractor},
};

/// The default WAL flush cadence when none is configured.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// How often a read-only handle polls for new state when none is
/// configured — SlateDB's own default, kept so an unconfigured reader
/// costs exactly what it always has.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// The stored block grain for every writer and reader.
const SST_BLOCK_SIZE: SstBlockSize = SstBlockSize::Block4Kib;

/// Creates a checkpoint of everything `db` has committed, expiring after
/// `lifetime` (never, if `None`), and reports its id. The scope is every
/// write the handle has taken, not only the already-durable ones, so a
/// commit that has returned is inside the checkpoint it precedes.
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

/// How many bytes of a store a preload bounded by `cache_size` will not
/// hold, or `None` when all of it fits. An unset `cache_size` is not an
/// unbounded one — the store's own cap still governs — so the comparison
/// is against whichever cap will actually apply.
///
/// The load stops at the first object that would exceed the cap rather
/// than skipping it, so a shortfall means the tail of the store goes
/// unloaded, not that the largest objects do.
pub(crate) fn preload_shortfall(store_bytes: u64, cache_size: Option<u64>) -> Option<u64> {
    let cap = cache_size.unwrap_or(cache::DEFAULT_CACHE_DISK);
    store_bytes
        .checked_sub(cap)
        .filter(|shortfall| *shortfall > 0)
}

/// Opens a moraine store on `object_store` — a read-write [`Db`] via
/// [`open_writer`](Self::open_writer) or a read-only [`DbReader`] via
/// [`open_reader`](Self::open_reader) — carrying the shared open
/// configuration: the WAL flush cadence (writer only) and the on-disk object
/// cache.
pub(crate) struct StoreBuilder<'a> {
    path: &'a str,
    object_store: Arc<dyn ObjectStore>,
    flush_interval: Duration,
    poll_interval: Duration,
    cache_dir: Option<PathBuf>,
    cache_size: Option<u64>,
    cache_memory: Option<u64>,
    cache_preload: Option<CachePreload>,
    cache_puts: bool,
    checkpoint: Option<Uuid>,
}

impl<'a> StoreBuilder<'a> {
    /// A builder for the store at `path` on `object_store`, with the default
    /// flush cadence and no on-disk object cache.
    pub(crate) fn new(path: &'a str, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            path,
            object_store,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            poll_interval: DEFAULT_POLL_INTERVAL,
            cache_dir: None,
            cache_size: None,
            cache_memory: None,
            cache_preload: None,
            cache_puts: false,
            checkpoint: None,
        }
    }

    /// Sets how often a read-only handle polls for new state. Reader only —
    /// a writer reads its own writes.
    pub(crate) fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Sets the WAL flush cadence. Durable commits wait for the next flush,
    /// so this bounds per-commit latency; smaller values mean more frequent
    /// (on S3, costlier) object-store PUTs. Zero flushes continuously (no
    /// timer), so a durable commit waits only on the object-store PUT — the
    /// lowest latency, at the cost of a busy flush loop. Writer only — a
    /// reader never flushes.
    pub(crate) fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    /// Sets the local directory backing the block cache's disk tier, which
    /// holds data blocks evicted from memory. When set, warm reads skip
    /// repeat object-store GETs — worthwhile for remote (`s3://`) stores,
    /// redundant for local ones. Process-wide, not per store: the first
    /// store to open decides whether there is a disk tier at all, and
    /// where. `None` (the default) keeps the cache in memory.
    pub(crate) fn cache_dir(mut self, cache_dir: Option<PathBuf>) -> Self {
        self.cache_dir = cache_dir;
        self
    }

    /// Sets how many bytes of disk the block cache's device may hold.
    /// Process-wide, not per store: the first store to open sizes the
    /// device and later ones share it. `None` (the default) takes what
    /// SlateDB gives one store's cache, and without a
    /// [`cache_dir`](Self::cache_dir) there is no device to bound. The
    /// memory budget is [`cache_memory`](Self::cache_memory).
    pub(crate) fn cache_size(mut self, cache_size: Option<u64>) -> Self {
        self.cache_size = cache_size;
        self
    }

    /// Sets how much memory the process-shared cache may hold across both
    /// slots. Process-wide, not per store: the first store to open sizes
    /// it and later ones share what it built. `None` (the default) takes
    /// what SlateDB gives a single store, now for the whole process.
    /// Never inert — the memory slots exist with or without a
    /// [`cache_dir`](Self::cache_dir).
    pub(crate) fn cache_memory(mut self, cache_memory: Option<u64>) -> Self {
        self.cache_memory = cache_memory;
        self
    }

    /// Sets what to load into the cache while the store opens. Per store,
    /// unlike the budget: each store warms its own bytes into the shared
    /// cache. The load skips what it cannot fetch, but it runs as part of
    /// the open, so an open that preloads returns only once it has. `None`
    /// (the default) loads nothing. Never inert — the memory slots exist
    /// with or without a [`cache_dir`](Self::cache_dir).
    pub(crate) fn cache_preload(mut self, cache_preload: Option<CachePreload>) -> Self {
        self.cache_preload = cache_preload;
        self
    }

    /// Sets whether the SST metadata this store writes enters the cache as
    /// it is written, rather than only when something reads it back. A
    /// flushed or compacted SST's index and filters are then resident
    /// without a later fetch. Compaction output is admitted too, so a merge
    /// can evict what reads had warmed — which is why this is off by
    /// default. Data blocks are never admitted on the write path, at either
    /// setting. Per store, and never inert: the metadata it admits lands in
    /// the memory slot, with or without a [`cache_dir`](Self::cache_dir).
    pub(crate) fn cache_puts(mut self, cache_puts: bool) -> Self {
        self.cache_puts = cache_puts;
        self
    }

    /// Pins the reader to an existing checkpoint instead of the latest
    /// manifest. Reader only, and the only truly zero-write open: a
    /// latest-mode reader creates and refreshes a checkpoint of its own,
    /// which is a manifest write.
    pub(crate) fn checkpoint(mut self, checkpoint: Option<Uuid>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    /// Opens (or creates) the store as a read-write [`Db`], with the
    /// counters its reads tally into. The cache is the process's; the
    /// counters are this store's, so a host attaching several can tell
    /// which catalog's reads the one cache is serving.
    pub(crate) async fn open_writer(&self) -> Result<(Db, Arc<cache::CacheCounters>)> {
        let settings = self.settings();
        let counters = cache::store_counters();
        let mut builder = Db::builder(self.path, Arc::clone(&self.object_store))
            .with_settings(settings)
            .with_sst_block_size(SST_BLOCK_SIZE)
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .with_block_cache_policy(self.block_cache_policy())
            .with_metrics_recorder(cache::recorder(Arc::clone(&counters)));
        if let Some(cache) = cache::shared(&self.cache_config()).await {
            builder = builder.with_db_cache(cache);
        }
        let db = builder.build().await.map_err(Error::from)?;
        if self.cache_preload.is_some() {
            let tx = db
                .begin(slatedb::IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            self.warm(crate::store::handle::ReadHandle::Tx(&tx)).await;
            tx.rollback();
        }
        Ok((db, counters))
    }

    /// Opens the store read-only as a [`DbReader`]. A `DbReader` never opens
    /// the writer `Db`, so it never fences a live writer. The flush cadence,
    /// if set, is ignored.
    ///
    /// Without a checkpoint the reader follows the latest manifest, which
    /// costs a manifest write on open and a refresh for the reader's
    /// lifetime. Pinned to a [`checkpoint`](Self::checkpoint) it writes
    /// nothing at all, and reads the fixed cut that checkpoint names.
    pub(crate) async fn open_reader(&self) -> Result<(DbReader, Arc<cache::CacheCounters>)> {
        let options = DbReaderOptions {
            manifest_poll_interval: self.poll_interval,
            ..Default::default()
        };
        let counters = cache::store_counters();
        let mut builder = DbReader::builder(self.path, Arc::clone(&self.object_store))
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .with_metrics_recorder(cache::recorder(Arc::clone(&counters)))
            .with_options(options);
        if let Some(cache) = cache::shared(&self.cache_config()).await {
            builder = builder.with_db_cache(cache);
        }
        if let Some(checkpoint) = self.checkpoint {
            builder = builder.with_reader_mode(DbReaderMode::Checkpoint(checkpoint));
        }
        let reader = builder.build().await.map_err(Error::from)?;
        if self.cache_preload.is_some() {
            self.warm(crate::store::handle::ReadHandle::Reader(&reader))
                .await;
        }
        Ok((reader, counters))
    }

    /// Deletes the checkpoint `checkpoint`, unpinning whatever it held
    /// against SlateDB's garbage collection. Readers still open against it
    /// keep serving from the objects they have already resolved.
    pub(crate) async fn delete_checkpoint(&self, checkpoint: Uuid) -> Result<()> {
        AdminBuilder::new(self.path, Arc::clone(&self.object_store))
            .build()
            .delete_checkpoint(checkpoint)
            .await
            .map_err(Error::from)
    }

    /// Every checkpoint the store's manifest currently carries, oldest
    /// first — reader-established ones included.
    pub(crate) async fn list_checkpoints(&self) -> Result<Vec<Uuid>> {
        let checkpoints = AdminBuilder::new(self.path, Arc::clone(&self.object_store))
            .build()
            .list_checkpoints(None)
            .await
            .map_err(Error::from)?;

        Ok(checkpoints.into_iter().map(|c| c.id).collect())
    }

    /// SlateDB settings for a writer. A zero flush interval flushes
    /// continuously rather than on a timer: durable commits then wait only
    /// on the object-store PUT, at the cost of a busy flush loop.
    fn settings(&self) -> Settings {
        Settings {
            flush_interval: Some(self.flush_interval),
            ..Default::default()
        }
    }

    /// How this store asks for the process-shared cache. The first store
    /// to open builds it; a later one with different numbers shares what
    /// was built, since the budget is the process's and not the store's.
    fn cache_config(&self) -> cache::CacheConfig {
        cache::CacheConfig {
            memory: self.cache_memory,
            dir: self.cache_dir.clone(),
            disk_size: self.cache_size,
        }
    }

    /// Whether the blocks of flushed and compacted SSTs enter the cache
    /// as they are written. Off by default: compaction output goes through
    /// the same policy, and a merge admitting everything evicts what reads
    /// had warmed.
    fn block_cache_policy(&self) -> BlockCachePolicy {
        let targets: &[CacheTarget] = if self.cache_puts {
            &[CacheTarget::Index, CacheTarget::Filters, CacheTarget::Stats]
        } else {
            &[]
        };
        BlockCachePolicy::default()
            .with_flush_targets(targets)
            .with_compaction_output_targets(targets)
    }

    /// Warms the cache before the first query, per `cache_preload`.
    ///
    /// Warming is reading: a scan admits the blocks it touches (probe
    /// shape) and SlateDB caches every SST index and filter it walks
    /// regardless, so a bounded read over the right subspaces populates
    /// both slots without naming a single SST. That matters beyond
    /// convenience — SlateDB's per-SST warm call takes an id type its
    /// crate does not export, so no caller outside it can name one.
    ///
    /// Which subspaces is the whole difference between the levels.
    /// `'l0'` touches each one just far enough to pull its SST metadata,
    /// which is what makes a cold probe tolerable; `'all'` additionally
    /// walks the scan-shaped subspaces whole, so an attach's first
    /// materialization reads no object storage at all. Neither walks the
    /// `index` subspace's data blocks: that is the multi-GiB bulk a
    /// preload must not pull, and it stays reachable at one fetch per
    /// probed block behind the filters just warmed.
    ///
    /// Best-effort throughout: a preload is an optimization, and no open
    /// should fail because one subspace could not be read.
    async fn warm(&self, handle: crate::store::handle::ReadHandle<'_>) {
        let Some(preload) = self.cache_preload else {
            return;
        };

        // Every subspace, so each one's SST metadata is resident.
        let metadata_only = [
            key::Subspace::System,
            key::Subspace::Current,
            key::Subspace::History,
            key::Subspace::Snapshot,
            key::Subspace::Changelog,
            key::Subspace::Index,
            key::Subspace::Inline,
        ];
        // The scan-shaped ones, whose data a materialization walks whole.
        let whole = [
            key::Subspace::System,
            key::Subspace::Current,
            key::Subspace::History,
            key::Subspace::Snapshot,
            key::Subspace::Changelog,
        ];

        let (warmed, failed) = stream::iter(metadata_only.into_iter().map(|subspace| {
            let deep = matches!(preload, CachePreload::All) && whole.contains(&subspace);
            warm_subspace(handle, subspace, deep)
        }))
        .buffer_unordered(metadata_only.len())
        .fold((0_usize, 0_usize), |(warmed, failed), result| async move {
            match result {
                Ok(()) => (warmed + 1, failed),
                Err(_) => (warmed, failed + 1),
            }
        })
        .await;
        info!(
            warmed,
            failed,
            level = match preload {
                CachePreload::L0 => "l0",
                CachePreload::All => "all",
            },
            "warmed the cache"
        );
    }
}

/// Reads `subspace` far enough to warm it: one entry for its SST
/// metadata, or the whole range when `deep`. Blocks are admitted (probe
/// shape) so what this touches stays resident.
async fn warm_subspace(
    handle: crate::store::handle::ReadHandle<'_>,
    subspace: key::Subspace,
    deep: bool,
) -> Result<()> {
    let mut iterator = handle
        .scan_prefix(key::subspace_prefix(subspace), .., ScanShape::Probe)
        .await?;
    while iterator.next().await?.is_some() {
        if !deep {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use slatedb::{IsolationLevel, config::WriteOptions};

    use super::*;
    use crate::store::key::{self, Key, SysKey};

    fn memory_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    /// The format grain is a moraine choice rather than an upstream default
    /// that can move without the matching scan/probe evidence.
    #[test]
    fn the_sst_block_size_is_fixed_at_four_kibibytes() {
        assert_eq!(SST_BLOCK_SIZE.as_bytes(), 4 * 1024);
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

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(&head, b"head").unwrap();
        tx.put(&snapshot, b"snap").unwrap();
        tx.put(&table, b"table").unwrap();
        tx.commit_with_options(&WriteOptions {
            await_durable: true,
            ..Default::default()
        })
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
        db.put(&head, b"head").await.unwrap();
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
        db.put(&head, b"head").await.unwrap();
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
                memory: Some(1 << 30),
                dir: Some(dir),
                disk_size: Some(64 * 1024 * 1024),
            }
        );
    }

    /// A preload that cannot fit reports what it will not hold: the cap
    /// stands where it is, so the shortfall is the operator's to act on.
    #[test]
    fn a_preload_larger_than_the_cap_reports_its_shortfall() {
        // Room to spare, and exactly enough, both fit.
        assert_eq!(preload_shortfall(3_400_000_000, Some(8_000_000_000)), None);
        assert_eq!(preload_shortfall(64, Some(64)), None);

        assert_eq!(preload_shortfall(100, Some(64)), Some(36));

        // No configured cap is not an unbounded one: the device's own cap
        // still governs what a preload may hold.
        assert_eq!(preload_shortfall(cache::DEFAULT_CACHE_DISK, None), None);
        assert_eq!(
            preload_shortfall(cache::DEFAULT_CACHE_DISK + 1, None),
            Some(1)
        );
    }

    /// Writes enter the cache on flush only when asked: compaction output
    /// goes through the same policy, so admitting everything by default
    /// would let a merge evict what reads had warmed.
    #[test]
    fn cache_puts_is_off_until_requested() {
        let object_store = memory_store();
        let unset = StoreBuilder::new("s", Arc::clone(&object_store));
        assert_eq!(
            unset.block_cache_policy(),
            BlockCachePolicy::default()
                .with_flush_targets(&[])
                .with_compaction_output_targets(&[]),
            "writes must not be admitted unless asked for"
        );

        let admitted = [CacheTarget::Index, CacheTarget::Filters, CacheTarget::Stats];
        let caching = StoreBuilder::new("s", object_store).cache_puts(true);
        assert_eq!(
            caching.block_cache_policy(),
            BlockCachePolicy::default()
                .with_flush_targets(&admitted)
                .with_compaction_output_targets(&admitted)
        );
    }

    /// A preload leaves the store readable and costs nothing that a read
    /// would not: it is a read, so the levels differ in reach, not in
    /// what they may observe.
    #[tokio::test]
    async fn a_preload_opens_a_readable_store_at_every_level() {
        let object_store = memory_store();
        let head = Key::Sys(SysKey::Head).encode();

        let (db, _) = StoreBuilder::new("s", Arc::clone(&object_store))
            .flush_interval(Duration::from_millis(1))
            .open_writer()
            .await
            .unwrap();
        db.put(&head, b"head").await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        for preload in [None, Some(CachePreload::L0), Some(CachePreload::All)] {
            let (reader, _) = StoreBuilder::new("s", Arc::clone(&object_store))
                .cache_preload(preload)
                .open_reader()
                .await
                .unwrap();
            assert_eq!(
                reader.get(head.clone()).await.unwrap().unwrap().as_ref(),
                b"head",
                "preload {preload:?} left the store unreadable"
            );
            reader.close().await.unwrap();
        }
    }
}
