//! Opening a moraine store.
//!
//! [`StoreBuilder`] opens a store on an object store as a read-write [`Db`]
//! or a read-only [`DbReader`], carrying the shared open configuration.
//! Every store is created — and must thereafter be opened — with the
//! tag-byte segment extractor; SlateDB persists the extractor identity and
//! refuses a mismatched open.

use std::{path::PathBuf, sync::Arc, time::Duration};

use moraine_wal::{SlotLog, SlotWal};
use object_store::ObjectStore;
use slatedb::{
    Db, DbReader, DbReaderMode,
    admin::AdminBuilder,
    config::{
        CheckpointOptions, CheckpointScope, DbReaderOptions, ObjectStoreCacheOptions, PreloadLevel,
        Settings,
    },
};
use uuid::Uuid;

use crate::{
    catalog::CachePreload,
    error::{Error, Result},
    store::{
        key::{Key, SysKey},
        proto::LeaderValue,
        segment::TagSegmentExtractor,
        value,
    },
};

/// The default reader manifest-poll cadence when none is configured. A reader
/// following the log lags the head by up to this interval.
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

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
    let cap = cache_size.or_else(|| {
        ObjectStoreCacheOptions::default()
            .max_cache_size_bytes
            .and_then(|bytes| u64::try_from(bytes).ok())
    })?;
    store_bytes
        .checked_sub(cap)
        .filter(|shortfall| *shortfall > 0)
}

/// Opens a moraine store on `object_store` — a read-write [`Db`] via
/// [`open_writer`](Self::open_writer) or a read-only [`DbReader`] via
/// [`open_reader`](Self::open_reader) — carrying the shared open
/// configuration: the reader manifest-poll cadence (reader only), the
/// on-disk object cache, and the commit-slot log every store plugs in as
/// its write-ahead log.
pub(crate) struct StoreBuilder<'a> {
    path: &'a str,
    object_store: Arc<dyn ObjectStore>,
    refresh_interval: Duration,
    replay_limit: Option<u64>,
    cache_dir: Option<PathBuf>,
    cache_size: Option<u64>,
    cache_preload: Option<CachePreload>,
    cache_puts: bool,
    checkpoint: Option<Uuid>,
}

impl<'a> StoreBuilder<'a> {
    /// A builder for the store at `path` on `object_store`, with the default
    /// refresh cadence and no on-disk object cache.
    pub(crate) fn new(path: &'a str, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            path,
            object_store,
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
            replay_limit: None,
            cache_dir: None,
            cache_size: None,
            cache_preload: None,
            cache_puts: false,
            checkpoint: None,
        }
    }

    /// The slot log this store's writes ride, plugged in as its write-ahead
    /// log: opening the writer replays the slots past the store's fold cursor,
    /// and a reader replays them for itself. A slot's leader advert folds into
    /// `sys/leader`, so an announcement outlives the slot that carried it.
    fn wal(&self) -> SlotWal {
        SlotWal::new(SlotLog::new(Arc::clone(&self.object_store), self.path))
            .with_replay_limit(self.replay_limit)
            .with_advert_projection(Arc::new(|advert: &moraine_wal::LeaderAdvert| {
                Some((
                    Key::Sys(SysKey::Leader).encode(),
                    value::encode_value(&LeaderValue {
                        instance: advert.instance.to_vec(),
                        endpoint: advert.endpoint.clone(),
                    }),
                ))
            }))
    }

    /// Bounds how many slots this open's replay folds, so a bounded fold
    /// sprint advances the store's cursor by at most `limit` and the next open
    /// resumes where it stopped. `None` (the default) replays the whole tail.
    /// Writer only — a reader always follows the log to its end.
    pub(crate) fn replay_limit(mut self, limit: Option<u64>) -> Self {
        self.replay_limit = limit;
        self
    }

    /// Sets the reader manifest-poll cadence. A reader following the slot log
    /// refreshes its view of the folded store on this interval, so it lags the
    /// head by up to this long; a shorter interval trades more manifest reads
    /// for less fold lag. Reader only — a writer follows its own cadence.
    pub(crate) fn refresh_interval(mut self, refresh_interval: Duration) -> Self {
        self.refresh_interval = refresh_interval;
        self
    }

    /// Sets the local directory backing SlateDB's on-disk object cache,
    /// which holds fetched object parts. When set, warm reads skip repeat
    /// object-store GETs and survive process restarts — worthwhile for
    /// remote (`s3://`) stores, redundant for local ones. `None` (the
    /// default) leaves only the in-memory caches, which are SlateDB's
    /// defaults and are configured nowhere here.
    pub(crate) fn cache_dir(mut self, cache_dir: Option<PathBuf>) -> Self {
        self.cache_dir = cache_dir;
        self
    }

    /// Sets how many bytes of disk the object cache may hold. The cap is
    /// per open store, so stores sharing one
    /// [`cache_dir`](Self::cache_dir) each spend up to it. `None` (the
    /// default) leaves SlateDB's own cap in force, and without a cache
    /// directory there is no object cache to bound. The in-memory caches
    /// are untouched by this.
    pub(crate) fn cache_size(mut self, cache_size: Option<u64>) -> Self {
        self.cache_size = cache_size;
        self
    }

    /// Sets what to load into the on-disk object cache while the store
    /// opens. The load is bounded by [`cache_size`](Self::cache_size) and
    /// skips what it cannot fetch, but it runs as part of the open, so an
    /// open that preloads returns only once it has. `None` (the default)
    /// loads nothing. Inert without a [`cache_dir`](Self::cache_dir).
    pub(crate) fn cache_preload(mut self, cache_preload: Option<CachePreload>) -> Self {
        self.cache_preload = cache_preload;
        self
    }

    /// Sets whether objects this store writes are cached as they are
    /// written, rather than only when something reads them back. A flushed
    /// or compacted SST then costs one local write and no later fetch.
    /// Compaction output is cached too, so a merge can evict parts that
    /// reads had warmed — which is why this is off by default. Inert
    /// without a [`cache_dir`](Self::cache_dir).
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

    /// Opens (or creates) the store as a read-write [`Db`], folding the slot
    /// log as it opens: the replay past the store's cursor *is* the fold, and
    /// [`Db::flush`] is what makes it durable. The store journals nothing of
    /// its own — the log is the journal — so every write a session takes is
    /// durable only once it flushes.
    pub(crate) async fn open_writer(&self) -> Result<Db> {
        let settings = self.settings();
        Db::builder(self.path, Arc::clone(&self.object_store))
            .with_settings(settings)
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .with_wal_writer(self.wal().writer_init())
            .build()
            .await
            .map_err(Error::from)
    }

    /// Opens the store read-only as a [`DbReader`]. A `DbReader` never opens
    /// the writer `Db`, so it never fences a live writer. The flush cadence,
    /// if set, is ignored.
    ///
    /// Without a checkpoint the reader follows the latest manifest, which
    /// costs a manifest write on open and a refresh for the reader's
    /// lifetime. Pinned to a [`checkpoint`](Self::checkpoint) it writes
    /// nothing at all, and reads the fixed cut that checkpoint names.
    pub(crate) async fn open_reader(&self) -> Result<DbReader> {
        let options = self.reader_options();
        let mut builder = DbReader::builder(self.path, Arc::clone(&self.object_store))
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .with_wal_reader(self.wal().reader())
            .with_options(options);
        if let Some(checkpoint) = self.checkpoint {
            builder = builder.with_reader_mode(DbReaderMode::Checkpoint(checkpoint));
        }
        builder.build().await.map_err(Error::from)
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

    /// Opens the store read-only pinned to `checkpoint_id`: the reader reads
    /// the manifest that checkpoint references and neither creates nor
    /// refreshes a checkpoint of its own. Reading a slot log's truncation
    /// horizon uses this to see the fold cursor exactly as a lagging peer's
    /// checkpoint does.
    pub(crate) async fn open_reader_at(self, checkpoint_id: Uuid) -> Result<DbReader> {
        let options = self.reader_options();
        let wal = self.wal();
        DbReader::builder(self.path, self.object_store)
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .with_wal_reader(wal.reader())
            .with_options(options)
            .with_reader_mode(DbReaderMode::Checkpoint(checkpoint_id))
            .build()
            .await
            .map_err(Error::from)
    }

    fn reader_options(&self) -> DbReaderOptions {
        // The checkpoint lifetime must exceed twice the poll interval; keep
        // SlateDB's default headroom, widening it only for a long interval.
        let checkpoint_lifetime = self
            .refresh_interval
            .saturating_mul(3)
            .max(Duration::from_secs(10 * 60));
        DbReaderOptions {
            manifest_poll_interval: self.refresh_interval,
            checkpoint_lifetime,
            object_store_cache_options: self.cache_options(),
            ..Default::default()
        }
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

    /// SlateDB settings for a writer. The store's own journaling stays off:
    /// the slot log is the journal, so journaling a fold again would double
    /// every logged byte, and a write the store takes directly is durable at
    /// its flush.
    fn settings(&self) -> Settings {
        Settings {
            wal_enabled: false,
            object_store_cache_options: self.cache_options(),
            ..Default::default()
        }
    }

    /// SlateDB's on-disk object cache: fetched object parts under
    /// `cache_dir` when set, otherwise none, holding at most `cache_size`
    /// bytes. Every other field stays at SlateDB's defaults (16 GiB,
    /// 4 MiB parts). A cap wider than the platform can address saturates.
    /// The in-memory block and metadata caches are a separate mechanism and
    /// keep their own defaults.
    fn cache_options(&self) -> ObjectStoreCacheOptions {
        let defaults = ObjectStoreCacheOptions::default();

        ObjectStoreCacheOptions {
            root_folder: self.cache_dir.clone(),
            max_cache_size_bytes: match self.cache_size {
                Some(bytes) => Some(usize::try_from(bytes).unwrap_or(usize::MAX)),
                None => defaults.max_cache_size_bytes,
            },
            cache_on_flush: self.cache_puts,
            cache_on_compaction: self.cache_puts,
            preload_disk_cache_on_startup: self.cache_preload.map(|preload| match preload {
                CachePreload::L0 => PreloadLevel::L0Sst,
                CachePreload::All => PreloadLevel::AllSst,
            }),
            ..defaults
        }
    }
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

    /// A commit-shaped transaction spanning several subspaces lands
    /// atomically, and per-subspace prefix scans see exactly their own
    /// segment's keys (multi-segment batches satisfy the antichain rule).
    #[tokio::test]
    async fn multi_subspace_transaction_and_prefix_scans() {
        let db = StoreBuilder::new("test/store", memory_store())
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
        crate::transaction::commit::commit_durably(&db, tx)
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

    /// SlateDB persists the extractor identity: reopening the store
    /// without it is refused rather than silently mis-segmented.
    #[tokio::test]
    async fn reopen_without_extractor_is_refused() {
        let object_store = memory_store();
        let db = StoreBuilder::new("test/store", object_store.clone())
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

    /// A configured `cache_dir` reaches SlateDB: a fresh `DbReader`, whose
    /// in-memory caches have never seen the store's objects, serves committed
    /// data and in doing so populates the object cache directory.
    #[tokio::test]
    async fn cache_dir_backs_the_reader_with_an_on_disk_cache() {
        let object_store = memory_store();
        let cache = std::env::temp_dir().join(format!("moraine-cache-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cache);

        let head = Key::Sys(SysKey::Head).encode();
        let db = StoreBuilder::new("s", object_store.clone())
            .cache_dir(Some(cache.clone()))
            .open_writer()
            .await
            .unwrap();
        db.put(&head, b"head").await.unwrap();
        db.close().await.unwrap();

        // The reader is cold: it must GET the store's blocks, which the disk
        // cache records under `cache`.
        let reader = StoreBuilder::new("s", object_store)
            .cache_dir(Some(cache.clone()))
            .open_reader()
            .await
            .unwrap();
        assert_eq!(reader.get(head).await.unwrap().unwrap().as_ref(), b"head");
        reader.close().await.unwrap();

        let populated = std::fs::read_dir(&cache).is_ok_and(|mut entries| entries.next().is_some());
        assert!(populated, "expected an object cache under {cache:?}");
        let _ = std::fs::remove_dir_all(&cache);
    }

    /// A configured cache size caps the object cache for the writer and the
    /// reader alike; unset, SlateDB's own default cap stands.
    #[test]
    fn cache_size_caps_the_on_disk_cache() {
        let object_store = memory_store();
        let unset = StoreBuilder::new("s", Arc::clone(&object_store));
        assert_eq!(
            unset.cache_options().max_cache_size_bytes,
            ObjectStoreCacheOptions::default().max_cache_size_bytes
        );

        let capped = StoreBuilder::new("s", object_store).cache_size(Some(64 * 1024 * 1024));
        assert_eq!(
            capped.cache_options().max_cache_size_bytes,
            Some(64 * 1024 * 1024)
        );
        assert_eq!(
            capped
                .settings()
                .object_store_cache_options
                .max_cache_size_bytes,
            Some(64 * 1024 * 1024)
        );
    }

    /// A cap wider than the platform can address saturates rather than
    /// wrapping to something small.
    #[test]
    fn cache_size_saturates_at_the_addressable_maximum() {
        let capped = StoreBuilder::new("s", memory_store()).cache_size(Some(u64::MAX));
        assert_eq!(
            capped.cache_options().max_cache_size_bytes,
            Some(usize::MAX)
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

        // No configured cap is not an unbounded one: the store's own cap
        // still governs what a preload may hold.
        let default_cap = ObjectStoreCacheOptions::default()
            .max_cache_size_bytes
            .and_then(|bytes| u64::try_from(bytes).ok())
            .expect("a default cap");
        assert_eq!(preload_shortfall(default_cap, None), None);
        assert_eq!(preload_shortfall(default_cap + 1, None), Some(1));
    }

    /// Preloading is off unless asked for, and each level reaches the
    /// writer and the reader alike when it is.
    #[test]
    fn cache_preload_is_off_until_requested() {
        let object_store = memory_store();
        let unset = StoreBuilder::new("s", Arc::clone(&object_store));
        assert_eq!(unset.cache_options().preload_disk_cache_on_startup, None);

        let l0 =
            StoreBuilder::new("s", Arc::clone(&object_store)).cache_preload(Some(CachePreload::L0));
        assert_eq!(
            l0.cache_options().preload_disk_cache_on_startup,
            Some(PreloadLevel::L0Sst)
        );

        let all = StoreBuilder::new("s", object_store).cache_preload(Some(CachePreload::All));
        assert_eq!(
            all.settings()
                .object_store_cache_options
                .preload_disk_cache_on_startup,
            Some(PreloadLevel::AllSst)
        );
    }

    /// A reader asked to preload fills its cache directory as it opens —
    /// before anything has read a key through it.
    #[tokio::test]
    async fn cache_preload_fills_the_cache_as_the_store_opens() {
        let object_store = memory_store();
        let root = std::env::temp_dir().join(format!("moraine-preload-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);

        let db = StoreBuilder::new("s", Arc::clone(&object_store))
            .open_writer()
            .await
            .unwrap();
        for index in 0..64u64 {
            let key = Key::Snapshot { snapshot_id: index }.encode();
            db.put(&key, &vec![b'v'; 16 * 1024]).await.unwrap();
        }
        db.flush().await.unwrap();
        db.close().await.unwrap();

        // Two cold readers over the same store, differing only in whether
        // they preload. Neither is read from, so only the preloading one
        // has any reason to have fetched anything.
        let mut cached = Vec::new();
        for (tag, preload) in [("off", None), ("on", Some(CachePreload::All))] {
            let cache = root.join(tag);
            let reader = StoreBuilder::new("s", Arc::clone(&object_store))
                .cache_dir(Some(cache.clone()))
                .cache_preload(preload)
                .open_reader()
                .await
                .unwrap();
            reader.close().await.unwrap();
            cached.push(cached_bytes(&cache));
        }

        assert!(
            cached[1] > cached[0],
            "preloading cached {} bytes against {} without it: the open is not filling the cache",
            cached[1],
            cached[0]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Caching writes is off unless asked for, and reaches the writer and
    /// the reader alike when it is.
    #[test]
    fn cache_puts_is_off_until_requested() {
        let object_store = memory_store();
        let unset = StoreBuilder::new("s", Arc::clone(&object_store));
        assert!(!unset.cache_options().cache_on_flush);
        assert!(!unset.cache_options().cache_on_compaction);

        let caching = StoreBuilder::new("s", object_store).cache_puts(true);
        assert!(caching.cache_options().cache_on_flush);
        assert!(caching.cache_options().cache_on_compaction);
        assert!(caching.settings().object_store_cache_options.cache_on_flush);
    }

    /// A writer that caches its writes fills the cache directory with what
    /// it flushed, without anything having read the store back.
    #[tokio::test]
    async fn cache_puts_fills_the_cache_from_the_write_path() {
        let object_store = memory_store();
        let root = std::env::temp_dir().join(format!("moraine-put-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);

        // The same workload twice, differing only in whether writes are
        // cached: what the flush wrote must show up on disk only in the
        // second, since nothing here reads the store back.
        let mut sizes = Vec::new();
        for (tag, cache_puts) in [("off", false), ("on", true)] {
            let cache = root.join(tag);
            let db = StoreBuilder::new(tag, Arc::clone(&object_store))
                .cache_dir(Some(cache.clone()))
                .cache_puts(cache_puts)
                .open_writer()
                .await
                .unwrap();
            for index in 0..64u64 {
                let key = Key::Snapshot { snapshot_id: index }.encode();
                db.put(&key, &vec![b'v'; 16 * 1024]).await.unwrap();
            }
            db.flush().await.unwrap();
            db.close().await.unwrap();
            sizes.push(cached_bytes(&cache));
        }

        assert!(
            sizes[1] > sizes[0],
            "caching writes left {} bytes cached against {} without it: the flush is not \
             reaching the cache",
            sizes[1],
            sizes[0]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Total size of every file under `dir`, zero if it does not exist.
    fn cached_bytes(dir: &std::path::Path) -> u64 {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries.flatten().fold(0, |total, entry| {
            let Ok(kind) = entry.file_type() else {
                return total;
            };
            if kind.is_dir() {
                return total + cached_bytes(&entry.path());
            }
            total + entry.metadata().map_or(0, |meta| meta.len())
        })
    }
}
