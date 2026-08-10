//! The block cache every store in the process shares: one instance, two
//! slots, one budget.
//!
//! The meta slot holds SST indexes, filters, and stats in memory and
//! nothing evicts them but their own size — every point probe walks a
//! filter and an index before it can reach a data block, and letting data
//! blocks compete for that space is how a scan makes every later probe
//! pay a fetch to learn "not here". The block slot holds data blocks,
//! tiered to disk when a cache directory is configured.
//!
//! Sharing is what makes the budget mean anything: a per-store cache is
//! bounded per store, so a host attaching several catalogs is
//! over-committed by however many it attached.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
    RecoverMode, Spawner,
};
use slatedb::db_cache::{
    CachedEntry, CachedKey, DbCache, SplitCache,
    foyer::{FoyerCache, FoyerCacheOptions},
    foyer_hybrid::FoyerHybridCache,
    stats,
};
use slatedb_common::metrics::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};
use tokio::sync::OnceCell;
use tracing::{info, warn};

/// Memory the cache takes when no budget is configured: what SlateDB
/// gives a *single* store by default (512 MiB of blocks, 128 MiB of
/// metadata), now for the whole process. A single-store host is therefore
/// unchanged and a multi-store one is strictly smaller than before.
const DEFAULT_CACHE_MEMORY: u64 = 640 * 1024 * 1024;

/// The share of the memory budget the meta slot takes, as a divisor —
/// one fifth, which is SlateDB's own 128:512 split between metadata and
/// blocks. Metadata is a small fraction of any real store, so this is
/// meant to hold all of it; `moraine_store_census` reports the store's
/// index and filter bytes for sizing the budget against a store that
/// disagrees.
const META_SHARE_DIVISOR: u64 = 5;

/// Bytes of disk the block slot's device takes when no cap is
/// configured — SlateDB's own default for a store's cache.
pub(crate) const DEFAULT_CACHE_DISK: u64 = 16 * 1024 * 1024 * 1024;

/// How the process's cache is built. The first store to open decides it;
/// later stores share what it built, whatever they ask for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CacheConfig {
    /// Memory budget across both slots.
    pub(crate) memory: Option<u64>,
    /// The block slot's disk device, when one is configured.
    pub(crate) dir: Option<PathBuf>,
    /// That device's byte cap.
    pub(crate) disk_size: Option<u64>,
}

impl CacheConfig {
    /// Bytes for the meta slot and the block slot's memory tier.
    fn slots(&self) -> (u64, u64) {
        let budget = self.memory.unwrap_or(DEFAULT_CACHE_MEMORY).max(2);
        let meta = (budget / META_SHARE_DIVISOR).max(1);
        (meta, budget.saturating_sub(meta).max(1))
    }
}

/// The process's cache, built on first use. A `OnceCell` rather than a
/// per-store build: two catalogs attached in one process share one
/// budget, which is the whole point, and a cache that failed to build
/// once is not retried — the store simply runs uncached rather than
/// failing an attach over a cache.
static SHARED: OnceCell<Option<Arc<dyn DbCache>>> = OnceCell::const_new();

/// What the cache has served, by tier. Every sizing claim about the
/// budget is checkable only if the tiers report, so they do.
///
/// Read at two scopes over the one cache: [`cache_tally`] is the
/// process's, which is what the budget is set from, and
/// [`ReadOnlyCatalog::cache_tally`](crate::ReadOnlyCatalog::cache_tally)
/// is one attach's,
/// which is what says whose reads the budget is spent on.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheTally {
    /// Filter, index, and stats lookups the meta slot served.
    pub metadata_hits: u64,
    /// Those it did not, each one a fetch and a decode.
    pub metadata_misses: u64,
    /// Data-block lookups the block slot served, from either of its
    /// tiers — foyer reports a hybrid hit without saying which.
    pub block_hits: u64,
    /// Those it did not, each one an object-store read.
    pub block_misses: u64,
    /// Lookups the cache itself failed, which read through rather than
    /// failing the caller.
    pub errors: u64,
}

/// Physical object-store requests SlateDB has issued for one catalog.
///
/// Counts include retries and every SlateDB component using the catalog.
/// Moraine currently gives SlateDB one physical store, so its WAL objects
/// are included in the `main_*` fields; `wal_*` remains available for a
/// future separately configured WAL store. Durations are the sum of
/// completed request latency, so concurrent requests can add up to more
/// than wall-clock time.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ObjectStoreTally {
    /// Reads from the main store.
    pub main_gets: u64,
    /// Time spent in completed main-store reads.
    pub main_get_duration: Duration,
    /// Writes to the main store.
    pub main_puts: u64,
    /// Time spent in completed main-store writes.
    pub main_put_duration: Duration,
    /// Deletes from the main store.
    pub main_deletes: u64,
    /// Time spent in completed main-store deletes.
    pub main_delete_duration: Duration,
    /// Reads from the WAL store.
    pub wal_gets: u64,
    /// Time spent in completed WAL-store reads.
    pub wal_get_duration: Duration,
    /// Writes to the WAL store.
    pub wal_puts: u64,
    /// Time spent in completed WAL-store writes.
    pub wal_put_duration: Duration,
    /// Deletes from the WAL store.
    pub wal_deletes: u64,
    /// Time spent in completed WAL-store deletes.
    pub wal_delete_duration: Duration,
    /// Failed request attempts across both stores, including errors SlateDB
    /// handles internally such as missing-object probes.
    pub errors: u64,
}

impl CacheTally {
    /// The share of lookups the cache served, `None` before it has served
    /// any. Metadata and blocks are counted apart because they are sized
    /// apart: a healthy stack has metadata near 1.0 and blocks wherever
    /// the working set puts them.
    #[must_use]
    pub fn metadata_hit_rate(&self) -> Option<f64> {
        rate(self.metadata_hits, self.metadata_misses)
    }

    /// As [`metadata_hit_rate`](Self::metadata_hit_rate), for data blocks.
    #[must_use]
    pub fn block_hit_rate(&self) -> Option<f64> {
        rate(self.block_hits, self.block_misses)
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "a hit rate is a ratio; f64 holds counts far past any real one exactly"
)]
fn rate(hits: u64, misses: u64) -> Option<f64> {
    let total = hits.checked_add(misses).filter(|total| *total > 0)?;
    Some(hits as f64 / total as f64)
}

/// The counters SlateDB increments as the cache serves. One set per store
/// and one for the process: the cache is shared, so the process's numbers
/// are what size it, but only a per-store set can say whose reads it is
/// serving.
#[derive(Debug, Default)]
pub(crate) struct CacheCounters {
    metadata_hits: AtomicU64,
    metadata_misses: AtomicU64,
    block_hits: AtomicU64,
    block_misses: AtomicU64,
    errors: AtomicU64,
    main_gets: AtomicU64,
    main_get_nanoseconds: AtomicU64,
    main_puts: AtomicU64,
    main_put_nanoseconds: AtomicU64,
    main_deletes: AtomicU64,
    main_delete_nanoseconds: AtomicU64,
    wal_gets: AtomicU64,
    wal_get_nanoseconds: AtomicU64,
    wal_puts: AtomicU64,
    wal_put_nanoseconds: AtomicU64,
    wal_deletes: AtomicU64,
    wal_delete_nanoseconds: AtomicU64,
    object_store_errors: AtomicU64,
}

impl CacheCounters {
    pub(crate) fn tally(&self) -> CacheTally {
        CacheTally {
            metadata_hits: self.metadata_hits.load(Ordering::Relaxed),
            metadata_misses: self.metadata_misses.load(Ordering::Relaxed),
            block_hits: self.block_hits.load(Ordering::Relaxed),
            block_misses: self.block_misses.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn object_store_tally(&self) -> ObjectStoreTally {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let duration = |counter: &AtomicU64| Duration::from_nanos(load(counter));
        ObjectStoreTally {
            main_gets: load(&self.main_gets),
            main_get_duration: duration(&self.main_get_nanoseconds),
            main_puts: load(&self.main_puts),
            main_put_duration: duration(&self.main_put_nanoseconds),
            main_deletes: load(&self.main_deletes),
            main_delete_duration: duration(&self.main_delete_nanoseconds),
            wal_gets: load(&self.wal_gets),
            wal_get_duration: duration(&self.wal_get_nanoseconds),
            wal_puts: load(&self.wal_puts),
            wal_put_duration: duration(&self.wal_put_nanoseconds),
            wal_deletes: load(&self.wal_deletes),
            wal_delete_duration: duration(&self.wal_delete_nanoseconds),
            errors: load(&self.object_store_errors),
        }
    }

    fn of(&self, which: Which) -> &AtomicU64 {
        match which {
            Which::MetadataHit => &self.metadata_hits,
            Which::MetadataMiss => &self.metadata_misses,
            Which::BlockHit => &self.block_hits,
            Which::BlockMiss => &self.block_misses,
            Which::Error => &self.errors,
            Which::MainGet => &self.main_gets,
            Which::MainPut => &self.main_puts,
            Which::MainDelete => &self.main_deletes,
            Which::WalGet => &self.wal_gets,
            Which::WalPut => &self.wal_puts,
            Which::WalDelete => &self.wal_deletes,
            Which::ObjectStoreError => &self.object_store_errors,
            Which::MainGetDuration => &self.main_get_nanoseconds,
            Which::MainPutDuration => &self.main_put_nanoseconds,
            Which::MainDeleteDuration => &self.main_delete_nanoseconds,
            Which::WalGetDuration => &self.wal_get_nanoseconds,
            Which::WalPutDuration => &self.wal_put_nanoseconds,
            Which::WalDeleteDuration => &self.wal_delete_nanoseconds,
        }
    }
}

/// One counter's home in the opening store's [`CacheCounters`] and in the
/// process's, or nowhere: SlateDB registers far more than the cache's, and
/// everything else increments a sink.
struct Slot {
    store: Arc<CacheCounters>,
    process: Arc<CacheCounters>,
    which: Option<Which>,
}

#[derive(Clone, Copy)]
enum Which {
    MetadataHit,
    MetadataMiss,
    BlockHit,
    BlockMiss,
    Error,
    MainGet,
    MainPut,
    MainDelete,
    WalGet,
    WalPut,
    WalDelete,
    ObjectStoreError,
    MainGetDuration,
    MainPutDuration,
    MainDeleteDuration,
    WalGetDuration,
    WalPutDuration,
    WalDeleteDuration,
}

impl CounterFn for Slot {
    fn increment(&self, value: u64) {
        let Some(which) = self.which else {
            return;
        };
        for counters in [&self.store, &self.process] {
            counters.of(which).fetch_add(value, Ordering::Relaxed);
        }
    }
}

impl HistogramFn for Slot {
    fn record(&self, value: f64) {
        let Some(which) = self.which else {
            return;
        };
        let Ok(duration) = Duration::try_from_secs_f64(value) else {
            return;
        };
        let nanoseconds = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        for counters in [&self.store, &self.process] {
            let counter = counters.of(which);
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(nanoseconds))
            });
        }
    }
}

const OBJECT_STORE_REQUEST_COUNT: &str = "slatedb.object_store.request_count";
const OBJECT_STORE_ERROR_COUNT: &str = "slatedb.object_store.error_count";
const OBJECT_STORE_REQUEST_DURATION: &str = "slatedb.object_store.request_duration_seconds";

fn object_store_metric(labels: &[(&str, &str)], duration: bool) -> Option<Which> {
    let label = |key: &str| {
        labels
            .iter()
            .find_map(|(name, value)| (*name == key).then_some(*value))
    };
    match (label("store_type"), label("op"), duration) {
        (Some("main"), Some("get"), false) => Some(Which::MainGet),
        (Some("main"), Some("put"), false) => Some(Which::MainPut),
        (Some("main"), Some("delete"), false) => Some(Which::MainDelete),
        (Some("wal"), Some("get"), false) => Some(Which::WalGet),
        (Some("wal"), Some("put"), false) => Some(Which::WalPut),
        (Some("wal"), Some("delete"), false) => Some(Which::WalDelete),
        (Some("main"), Some("get"), true) => Some(Which::MainGetDuration),
        (Some("main"), Some("put"), true) => Some(Which::MainPutDuration),
        (Some("main"), Some("delete"), true) => Some(Which::MainDeleteDuration),
        (Some("wal"), Some("get"), true) => Some(Which::WalGetDuration),
        (Some("wal"), Some("put"), true) => Some(Which::WalPutDuration),
        (Some("wal"), Some("delete"), true) => Some(Which::WalDeleteDuration),
        _ => None,
    }
}

/// Routes SlateDB's cache and object-store counters into
/// [`CacheCounters`] and drops everything else on the floor.
///
/// The cache reports one counter per `(entry_kind, result)` pair —
/// `filter`, `index`, and `stats` are the meta slot's, `data_block` the
/// block slot's — so the routing is by label, and a kind SlateDB adds
/// later lands in neither rather than being miscounted as one.
#[derive(Debug)]
struct CacheRecorder {
    store: Arc<CacheCounters>,
}

impl MetricsRecorder for CacheRecorder {
    fn register_counter(
        &self,
        name: &str,
        _description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let label = |key: &str| {
            labels
                .iter()
                .find_map(|(name, value)| (*name == key).then_some(*value))
        };
        let which = match name {
            OBJECT_STORE_REQUEST_COUNT => object_store_metric(labels, false),
            OBJECT_STORE_ERROR_COUNT => Some(Which::ObjectStoreError),
            stats::ERROR_COUNT => Some(Which::Error),
            stats::ACCESS_COUNT => match (label("entry_kind"), label("result")) {
                (Some("filter" | "index" | "stats"), Some("hit")) => Some(Which::MetadataHit),
                (Some("filter" | "index" | "stats"), Some("miss")) => Some(Which::MetadataMiss),
                (Some("data_block"), Some("hit")) => Some(Which::BlockHit),
                (Some("data_block"), Some("miss")) => Some(Which::BlockMiss),
                _ => None,
            },
            _ => None,
        };
        Arc::new(Slot {
            store: Arc::clone(&self.store),
            process: Arc::clone(&COUNTERS),
            which,
        })
    }

    fn register_gauge(&self, _: &str, _: &str, _: &[(&str, &str)]) -> Arc<dyn GaugeFn> {
        Arc::new(Sink)
    }

    fn register_up_down_counter(
        &self,
        _: &str,
        _: &str,
        _: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        Arc::new(Sink)
    }

    fn register_histogram(
        &self,
        name: &str,
        _: &str,
        labels: &[(&str, &str)],
        _: &[f64],
    ) -> Arc<dyn HistogramFn> {
        Arc::new(Slot {
            store: Arc::clone(&self.store),
            process: Arc::clone(&COUNTERS),
            which: (name == OBJECT_STORE_REQUEST_DURATION)
                .then(|| object_store_metric(labels, true))
                .flatten(),
        })
    }
}

/// Every instrument this crate does not read.
struct Sink;

impl GaugeFn for Sink {
    fn set(&self, _: i64) {}
}

impl UpDownCounterFn for Sink {
    fn increment(&self, _: i64) {}
}

impl HistogramFn for Sink {
    fn record(&self, _: f64) {}
}

/// The counters behind the process's cache, alive from the first
/// registration so a store opened before anything reads them still counts.
static COUNTERS: std::sync::LazyLock<Arc<CacheCounters>> =
    std::sync::LazyLock::new(|| Arc::new(CacheCounters::default()));

/// A fresh set of counters for one store, which the catalog holds for as
/// long as it is open.
pub(crate) fn store_counters() -> Arc<CacheCounters> {
    Arc::new(CacheCounters::default())
}

/// The recorder an opening store's builder is given: what it counts lands
/// in `store` and in the process's set together.
pub(crate) fn recorder(store: Arc<CacheCounters>) -> Arc<dyn MetricsRecorder> {
    Arc::new(CacheRecorder { store })
}

/// What the process's block cache has served since it was built —
/// metadata and data blocks counted apart, because they are sized apart.
///
/// Process-wide, like the cache itself: every catalog a process attaches
/// reads through the one instance, so these are the host's numbers, and
/// they outlive the catalogs that produced them. Use them to size
/// [`CatalogOptions::cache_memory`](crate::CatalogOptions::cache_memory)
/// and [`cache_size`](crate::CatalogOptions::cache_size) from measured
/// curves rather than from the defaults, and
/// [`ReadOnlyCatalog::cache_tally`](crate::ReadOnlyCatalog::cache_tally)
/// to see which attach is spending them.
///
/// ```
/// let tally = moraine::cache_tally();
/// // Nothing has read yet, so there is no rate to report.
/// assert_eq!(
///     tally.metadata_hit_rate().is_none(),
///     tally.metadata_hits == 0 && tally.metadata_misses == 0
/// );
/// ```
#[must_use]
pub fn cache_tally() -> CacheTally {
    COUNTERS.tally()
}

/// The cache every store in this process shares, built to `config` if
/// nothing has built it yet. `None` when the disk tier could not be
/// opened *and* nothing else was configured — never an error: a cache is
/// an optimization, and no attach should fail because a device would not
/// open.
pub(crate) async fn shared(config: &CacheConfig) -> Option<Arc<dyn DbCache>> {
    let cache = SHARED
        .get_or_init(|| async {
            let built = build(config).await;
            let mut settled = BUILT_WITH
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *settled = Some(config.clone());
            built
        })
        .await
        .clone();

    // A later store asking for something else gets what was built. That
    // is the budget being the process's rather than the store's, and
    // refusing would fail an attach over a cache — but a mismatch nobody
    // can see is a host sized from options that never took effect, so it
    // is said out loud once per attach that disagrees.
    let settled = BUILT_WITH
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(settled) = settled.as_ref()
        && settled != config
    {
        warn!(
            requested_memory = config.memory,
            requested_disk_size = config.disk_size,
            requested_dir = ?config.dir,
            in_force_memory = settled.memory,
            in_force_disk_size = settled.disk_size,
            in_force_dir = ?settled.dir,
            "the block cache is process-wide and already built; this attach's cache options              are ignored. Set them on the first attach in the process."
        );
    }

    cache
}

/// What the process's cache was built with, for telling a later attach
/// that its own numbers did not take effect.
static BUILT_WITH: std::sync::Mutex<Option<CacheConfig>> = std::sync::Mutex::new(None);

async fn build(config: &CacheConfig) -> Option<Arc<dyn DbCache>> {
    let (meta_bytes, block_bytes) = config.slots();

    let meta: Arc<dyn DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: meta_bytes,
        ..FoyerCacheOptions::default()
    }));

    let block: Arc<dyn DbCache> = match &config.dir {
        Some(dir) => match hybrid(dir, block_bytes, config.disk_size).await {
            Some(cache) => cache,
            // The device is the only part that can fail, and a memory
            // tier alone is strictly better than no cache at all.
            None => Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: block_bytes,
                ..FoyerCacheOptions::default()
            })),
        },
        None => Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: block_bytes,
            ..FoyerCacheOptions::default()
        })),
    };

    info!(
        meta_bytes,
        block_bytes,
        disk = config.dir.is_some(),
        "built the shared block cache"
    );

    Some(Arc::new(
        SplitCache::new()
            .with_meta_cache(Some(meta))
            .with_block_cache(Some(block))
            .build(),
    ) as Arc<dyn DbCache>)
}

/// The runtime the hybrid cache spawns its fetch and flush tasks on.
///
/// It must not be an attach's runtime. The cache outlives every attach,
/// but foyer fixes its spawner when the cache is *built* — so the first
/// store to open would lend the process-wide cache a runtime that dies at
/// its detach. Tokio then cancels the tasks that runtime owned, and
/// dropping an in-flight fetch takes foyer's inflight lock with nothing
/// left running to release it: the next attach to touch the cache blocks
/// forever. Two workers is plenty; these tasks fetch and flush, they do
/// not compute.
fn cache_runtime() -> Option<tokio::runtime::Runtime> {
    match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("moraine-cache")
        .enable_all()
        .build()
    {
        Ok(runtime) => Some(runtime),
        Err(error) => {
            warn!(
                %error,
                "could not start the cache runtime; the block cache stays in memory"
            );
            None
        }
    }
}

/// The block slot backed by memory over a disk device at `dir`. `None`
/// if the device or its runtime will not open, which the caller degrades
/// from rather than failing.
async fn hybrid(dir: &PathBuf, memory: u64, disk: Option<u64>) -> Option<Arc<dyn DbCache>> {
    if let Err(error) = std::fs::create_dir_all(dir) {
        warn!(
            directory = %dir.display(),
            %error,
            "could not create the cache directory; the block cache stays in memory"
        );
        return None;
    }

    let capacity = usize::try_from(disk.unwrap_or(DEFAULT_CACHE_DISK)).unwrap_or(usize::MAX);
    let device = match FsDeviceBuilder::new(dir).with_capacity(capacity).build() {
        Ok(device) => device,
        Err(error) => {
            warn!(
                directory = %dir.display(),
                %error,
                "could not open the cache device; the block cache stays in memory"
            );
            return None;
        }
    };

    let built = HybridCacheBuilder::new()
        .memory(usize::try_from(memory).unwrap_or(usize::MAX))
        .with_weighter(|_: &CachedKey, value: &CachedEntry| value.size())
        .storage()
        // SlateDB scopes shared-cache keys with an attach-order counter.
        // Recovering them in a new process can bind one store's WAL blocks
        // to another store when that order changes.
        .with_recover_mode(RecoverMode::None)
        // Keeps the cache's tasks off the runtime of whichever attach
        // happened to build it.
        .with_spawner(Spawner::from(cache_runtime()?))
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(BlockEngineConfig::new(device))
        .build()
        .await;

    match built {
        Ok(cache) => Some(Arc::new(FoyerHybridCache::new_with_cache(cache))),
        Err(error) => {
            warn!(
                directory = %dir.display(),
                %error,
                "could not build the hybrid block cache; it stays in memory"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESTART_PHASE: &str = "MORAINE_CACHE_RESTART_PHASE";
    const RESTART_ROOT: &str = "MORAINE_CACHE_RESTART_ROOT";

    async fn run_restart_phase(phase: &str, root: &std::path::Path) {
        use object_store::local::LocalFileSystem;
        use slatedb::{
            Db, DbReader,
            config::{FlushOptions, FlushType, PutOptions, Settings, WriteOptions},
        };

        let store_root = root.join("store");
        let cache_root = root.join("cache");
        std::fs::create_dir_all(&store_root).unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(&store_root).unwrap());

        if phase == "seed" {
            let settings = Settings {
                flush_interval: None,
                ..Settings::default()
            };
            for (path, value) in [("a", b"catalog-a"), ("b", b"catalog-b")] {
                let db = Db::builder(path, object_store.clone())
                    .with_settings(settings.clone())
                    .build()
                    .await
                    .unwrap();
                db.put_with_options(
                    b"key",
                    value,
                    &PutOptions::default(),
                    &WriteOptions {
                        await_durable: false,
                        ..WriteOptions::default()
                    },
                )
                .await
                .unwrap();
                db.flush_with_options(FlushOptions {
                    flush_type: FlushType::Wal,
                })
                .await
                .unwrap();
                drop(db);
            }
            std::process::exit(0);
        }

        let cache = hybrid(&cache_root, 4 * 1024 * 1024, Some(64 * 1024 * 1024))
            .await
            .unwrap();
        let order = if phase == "warm" {
            [("a", b"catalog-a"), ("b", b"catalog-b")]
        } else {
            [("b", b"catalog-b"), ("a", b"catalog-a")]
        };

        for (path, expected) in order {
            let reader = DbReader::builder(path, object_store.clone())
                .with_db_cache(cache.clone())
                .build()
                .await
                .unwrap();
            let found = reader.get(b"key").await.unwrap().unwrap();
            assert_eq!(found.as_ref(), expected, "cache scope crossed into {path}");
            reader.close().await.unwrap();
        }
        cache.close().await.unwrap();
    }

    /// Recovered keys must not follow attach-order scope ids into another
    /// catalog. WAL SST ids are per store, so two stores deliberately use
    /// the same id here; the second process attaches them in reverse order.
    #[tokio::test(flavor = "multi_thread")]
    async fn recovered_disk_entries_do_not_cross_catalog_scopes() {
        if let Ok(phase) = std::env::var(RESTART_PHASE) {
            let root = PathBuf::from(std::env::var_os(RESTART_ROOT).unwrap());
            run_restart_phase(&phase, &root).await;
            return;
        }

        let root = std::env::temp_dir().join(format!(
            "moraine-cache-restart-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let test_name = "store::cache::tests::recovered_disk_entries_do_not_cross_catalog_scopes";

        for phase in ["seed", "warm", "probe"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", test_name, "--nocapture"])
                .env(RESTART_PHASE, phase)
                .env(RESTART_ROOT, &root)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{phase} subprocess failed:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        std::fs::remove_dir_all(root).unwrap();
    }

    /// The budget splits into two slots that add up to it, and the meta
    /// slot is the smaller one — data blocks are what needs the room.
    #[test]
    fn the_budget_splits_across_both_slots() {
        let config = CacheConfig {
            memory: Some(1_000),
            ..CacheConfig::default()
        };
        let (meta, block) = config.slots();
        assert_eq!(meta + block, 1_000);
        assert!(meta < block, "meta {meta} should be the smaller slot");
    }

    /// An unset budget is what SlateDB gives one store, so a single-store
    /// host is unchanged by the move to a shared cache.
    #[test]
    fn an_unset_budget_matches_one_stores_defaults() {
        let (meta, block) = CacheConfig::default().slots();
        assert_eq!(meta + block, DEFAULT_CACHE_MEMORY);
        assert_eq!(meta, 128 * 1024 * 1024);
        assert_eq!(block, 512 * 1024 * 1024);
    }

    /// A later store's cache options do not take effect, and the process
    /// says so rather than leaving a host sized from numbers that never
    /// applied. The first config to arrive is what stands.
    #[tokio::test]
    async fn a_later_attachs_cache_options_are_reported_as_ignored() {
        let first = CacheConfig {
            memory: Some(4 * 1024 * 1024),
            ..CacheConfig::default()
        };
        let built = shared(&first).await;

        // Whatever the second asks for, it is served the first's cache.
        let second = CacheConfig {
            memory: Some(64 * 1024 * 1024),
            dir: Some(std::path::PathBuf::from("/tmp/moraine-ignored")),
            disk_size: Some(1),
        };
        let served = shared(&second).await;
        assert_eq!(built.is_some(), served.is_some());
        assert!(
            BUILT_WITH
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_some_and(|settled| settled.memory != second.memory),
            "the first config must be the one in force"
        );
    }

    /// The recorder routes SlateDB's cache counters by label: the meta
    /// slot's three entry kinds tally together, data blocks apart, and a
    /// kind nobody here models lands in neither rather than being
    /// miscounted as one.
    #[test]
    fn the_recorder_routes_counters_by_entry_kind() {
        let counters = Arc::new(CacheCounters::default());
        let recorder = CacheRecorder {
            store: Arc::clone(&counters),
        };
        let counter = |kind: &str, result: &str| {
            recorder.register_counter(
                stats::ACCESS_COUNT,
                "",
                &[("entry_kind", kind), ("result", result)],
            )
        };

        counter("filter", "hit").increment(1);
        counter("index", "hit").increment(2);
        counter("stats", "miss").increment(3);
        counter("data_block", "hit").increment(4);
        counter("data_block", "miss").increment(5);
        counter("something_new", "hit").increment(99);
        recorder
            .register_counter(stats::ERROR_COUNT, "", &[])
            .increment(6);

        let tally = counters.tally();
        assert_eq!(tally.metadata_hits, 3);
        assert_eq!(tally.metadata_misses, 3);
        assert_eq!(tally.block_hits, 4);
        assert_eq!(tally.block_misses, 5);
        assert_eq!(tally.errors, 6);
    }

    /// The same recorder retains SlateDB's physical object-store metrics,
    /// split between its main and WAL stores so a durable write is visible
    /// apart from metadata reads.
    #[test]
    fn the_recorder_routes_object_store_requests_and_time() {
        let counters = Arc::new(CacheCounters::default());
        let recorder = CacheRecorder {
            store: Arc::clone(&counters),
        };
        let labels = [
            ("component", "db"),
            ("store_type", "main"),
            ("op", "get"),
            ("api", "get_range"),
        ];

        recorder
            .register_counter("slatedb.object_store.request_count", "", &labels)
            .increment(3);
        recorder
            .register_histogram(
                "slatedb.object_store.request_duration_seconds",
                "",
                &labels,
                &[],
            )
            .record(0.025);

        let tally = counters.object_store_tally();
        assert_eq!(tally.main_gets, 3);
        assert_eq!(tally.main_get_duration, Duration::from_millis(25));
        assert_eq!(tally.wal_puts, 0);
    }

    /// Two stores share the cache but not the counters: what one store's
    /// reads tally stays out of the other's, while the process's set takes
    /// both. Without this the per-attach tally is the process's under
    /// another name.
    #[test]
    fn a_stores_counters_are_its_own() {
        let first = store_counters();
        let second = store_counters();
        let before = cache_tally().block_hits;

        recorder(Arc::clone(&first))
            .register_counter(
                stats::ACCESS_COUNT,
                "",
                &[("entry_kind", "data_block"), ("result", "hit")],
            )
            .increment(4);

        assert_eq!(first.tally().block_hits, 4);
        assert_eq!(second.tally().block_hits, 0, "one store's hits are its own");
        assert_eq!(
            cache_tally().block_hits - before,
            4,
            "and the process still counts them"
        );
    }

    /// A rate is reported only once something has been looked up, and is
    /// the served share of those lookups.
    #[test]
    fn hit_rates_need_a_lookup_to_report() {
        assert_eq!(CacheTally::default().metadata_hit_rate(), None);
        assert_eq!(CacheTally::default().block_hit_rate(), None);

        let tally = CacheTally {
            metadata_hits: 3,
            metadata_misses: 1,
            block_hits: 1,
            block_misses: 3,
            errors: 0,
        };
        assert!((tally.metadata_hit_rate().unwrap() - 0.75).abs() < f64::EPSILON);
        assert!((tally.block_hit_rate().unwrap() - 0.25).abs() < f64::EPSILON);
    }

    /// A budget too small to split still yields usable slots rather than
    /// a zero-capacity cache.
    #[test]
    fn a_tiny_budget_still_splits() {
        let config = CacheConfig {
            memory: Some(1),
            ..CacheConfig::default()
        };
        let (meta, block) = config.slots();
        assert!(meta >= 1 && block >= 1);
    }
}
