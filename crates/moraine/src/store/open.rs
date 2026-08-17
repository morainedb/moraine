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
    store::{
        cache,
        handle::{ReadHandle, ScanShape},
        key,
        segment::TagSegmentExtractor,
    },
};

/// The default WAL flush cadence when none is configured.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// How often a read-only handle polls for new state when none is
/// configured (SlateDB's own default).
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// The stored block grain for every writer and reader.
const SST_BLOCK_SIZE: SstBlockSize = SstBlockSize::Block4Kib;

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

/// How many bytes of a store a preload bounded by `cache_size` (or the
/// default disk cap when unset) will not hold, or `None` when all of it
/// fits.
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

    /// Sets the WAL flush cadence, which bounds durable-commit latency;
    /// zero flushes continuously with no timer. Writer only — a reader
    /// never flushes.
    pub(crate) fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
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

    /// Sets what to load into the cache while the store opens. Per store;
    /// the load is best-effort and completes before the open returns.
    /// `None` (the default) loads nothing.
    pub(crate) fn cache_preload(mut self, cache_preload: Option<CachePreload>) -> Self {
        self.cache_preload = cache_preload;
        self
    }

    /// Sets whether the SST index and filters this store flushes or
    /// compacts enter the cache as they are written (default: off). Data
    /// blocks are never admitted on the write path.
    pub(crate) fn cache_puts(mut self, cache_puts: bool) -> Self {
        self.cache_puts = cache_puts;
        self
    }

    /// Pins the reader to an existing checkpoint instead of the latest
    /// manifest. Reader only, and the only zero-write open.
    pub(crate) fn checkpoint(mut self, checkpoint: Option<Uuid>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    /// Opens (or creates) the store as a read-write [`Db`], with the
    /// per-store counters its reads tally into.
    pub(crate) async fn open_writer(&self) -> Result<(Db, Arc<cache::CacheCounters>)> {
        let settings = self.settings();
        let counters = cache::store_counters();
        let mut builder = Db::builder(self.path, Arc::clone(&self.object_store))
            .with_settings(settings)
            .with_sst_block_size(SST_BLOCK_SIZE)
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .with_block_cache_policy(self.block_cache_policy())
            .with_metrics_recorder(cache::recorder(Arc::clone(&counters)));

        if let Some(cache) = cache::shared(&self.cache_config(), self.location()).await {
            builder = builder.with_db_cache(cache);
        }

        let db = builder.build().await.map_err(Error::from)?;
        if let Some(preload) = self.cache_preload {
            let tx = db
                .begin(slatedb::IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            warm(preload, ReadHandle::Tx(&tx), &counters).await;
            tx.rollback();
        }

        Ok((db, counters))
    }

    /// Opens the store read-only as a [`DbReader`], which never fences a
    /// live writer; the flush cadence is ignored. Without a
    /// [`checkpoint`](Self::checkpoint) the reader follows the latest
    /// manifest, which costs a manifest write on open.
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

        if let Some(cache) = cache::shared(&self.cache_config(), self.location()).await {
            builder = builder.with_db_cache(cache);
        }
        if let Some(checkpoint) = self.checkpoint {
            builder = builder.with_reader_mode(DbReaderMode::Checkpoint(checkpoint));
        }

        let reader = builder.build().await.map_err(Error::from)?;
        if let Some(preload) = self.cache_preload {
            warm(preload, ReadHandle::Reader(&reader), &counters).await;
        }

        Ok((reader, counters))
    }

    /// Deletes the checkpoint `checkpoint`, unpinning whatever it held
    /// against garbage collection.
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

    /// SlateDB settings for a writer.
    fn settings(&self) -> Settings {
        Settings {
            flush_interval: Some(self.flush_interval),
            ..Default::default()
        }
    }

    fn location(&self) -> cache::StoreLocation {
        cache::StoreLocation {
            object_store: self.object_store.to_string(),
            path: self.path.to_owned(),
        }
    }

    /// How this store asks for the process-shared cache.
    fn cache_config(&self) -> cache::CacheConfig {
        cache::CacheConfig {
            memory: self.cache_memory,
            dir: self.cache_dir.clone(),
            disk_size: self.cache_size,
        }
    }

    /// Which blocks of flushed and compacted SSTs enter the cache as they
    /// are written.
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
}

/// Warms the cache by reading, best-effort. `L0` reads one entry of every
/// subspace so its SST metadata is resident; `All` additionally walks the
/// scan-shaped subspaces whole. Neither reads the `index` subspace's data
/// blocks.
async fn warm(preload: CachePreload, handle: ReadHandle<'_>, counters: &cache::CacheCounters) {
    let metadata_only = [
        key::Subspace::System,
        key::Subspace::Current,
        key::Subspace::History,
        key::Subspace::Snapshot,
        key::Subspace::Changelog,
        key::Subspace::Index,
        key::Subspace::Inline,
    ];
    let whole = [
        key::Subspace::System,
        key::Subspace::Current,
        key::Subspace::History,
        key::Subspace::Snapshot,
        key::Subspace::Changelog,
    ];

    let before = counters.tally();
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
    counters.record_preload(before, counters.tally(), failed);
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

/// Reads one entry of `subspace`, or the whole range when `deep`, in probe
/// shape so the touched blocks are admitted. A deep walk of `current` or
/// `history` runs its data-scaled kinds as concurrent sub-ranges.
async fn warm_subspace(handle: ReadHandle<'_>, subspace: key::Subspace, deep: bool) -> Result<()> {
    let prefix = key::subspace_prefix(subspace);
    if !deep {
        let mut iterator = handle.scan_prefix(&prefix, .., ScanShape::Probe).await?;
        iterator.next().await?;
        return Ok(());
    }

    let split_kinds = match subspace {
        key::Subspace::Current => key::SPLIT_SCAN_KINDS
            .map(key::current_entity_kind_prefix)
            .to_vec(),
        key::Subspace::History => key::SPLIT_SCAN_KINDS
            .map(key::history_entity_kind_prefix)
            .to_vec(),
        _ => Vec::new(),
    };
    handle
        .scan_prefix_split(&prefix, &split_kinds, ScanShape::Probe)
        .await?;

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
