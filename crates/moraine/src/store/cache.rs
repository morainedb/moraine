//! The caches every store in the process shares: one budget across SlateDB
//! metadata, SlateDB blocks, and auxiliary parsed metadata.
//!
//! Metadata and blocks share one cache and are separated by eviction
//! priority rather than by capacity. A scan cannot push out the filters and
//! indexes every probe walks, and metadata a store does not need is left to
//! blocks — so neither is bounded at a size the operator has to predict.
//! Blocks tier to disk when a cache directory is configured.
//!
//! Sharing is what makes the budget mean anything: a per-store cache is
//! bounded per store, so a host attaching several catalogs is
//! over-committed by however many it attached.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use foyer::{
    BlockEngineConfig, CacheProperties, DeviceBuilder, Event, EventListener, FsDeviceBuilder, Hint,
    HybridCacheBuilder, HybridCacheProperties, LruConfig, PsyncIoEngineConfig, RecoverMode,
    Spawner,
};
use slatedb::db_cache::{CacheLoader, CachedEntry, CachedKey, DbCache, stats};
use slatedb_common::metrics::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};
use tokio::sync::OnceCell;
use tracing::{info, warn};

/// Memory the cache takes when no budget is configured: what SlateDB
/// gives a *single* store by default (512 MiB of blocks, 128 MiB of
/// metadata), now for the whole process. A single-store host is therefore
/// unchanged and a multi-store one is strictly smaller than before.
const DEFAULT_CACHE_MEMORY: u64 = 640 * 1024 * 1024;

/// The most of the cache SST metadata may hold under eviction protection.
const METADATA_PRIORITY_POOL_RATIO: f64 = 0.9;

/// At most this much of the budget is reserved for parsed metadata outside
/// SlateDB.
const MAX_AUXILIARY_METADATA_MEMORY: u64 = 8 * 1024 * 1024;

/// The auxiliary allowance's divisor against the whole budget, up to its
/// ceiling.
const AUXILIARY_METADATA_SHARE_DIVISOR: u64 = 80;

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
    /// Bytes for scoped Parquet metadata and for the one cache SlateDB
    /// metadata and data blocks share.
    fn slots(&self) -> (u64, u64) {
        let budget = self.memory.unwrap_or(DEFAULT_CACHE_MEMORY).max(2);
        let auxiliary_metadata =
            (budget / AUXILIARY_METADATA_SHARE_DIVISOR).min(MAX_AUXILIARY_METADATA_MEMORY);
        let shared = budget.saturating_sub(auxiliary_metadata).max(1);

        (auxiliary_metadata, shared)
    }
}

/// Bytes of `capacity` metadata holds before it is demoted to compete with
/// data blocks.
#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a share of a byte count; the ratio is a fixed fraction under 1"
)]
fn metadata_ceiling(capacity: u64) -> u64 {
    (capacity as f64 * METADATA_PRIORITY_POOL_RATIO) as u64
}

/// Bytes of a store's SST metadata a meta slot of `capacity` cannot hold,
/// or `None` when it holds all of it.
pub(crate) fn metadata_shortfall(metadata_bytes: u64, capacity: u64) -> Option<u64> {
    metadata_bytes
        .checked_sub(capacity)
        .filter(|shortfall| *shortfall > 0)
}

static AUXILIARY_METADATA_MEMORY: AtomicU64 = AtomicU64::new(MAX_AUXILIARY_METADATA_MEMORY);

static AUXILIARY_METADATA_USAGE: AtomicU64 = AtomicU64::new(0);
static AUXILIARY_METADATA_EVICTIONS: AtomicU64 = AtomicU64::new(0);
static METADATA_EVICTIONS: AtomicU64 = AtomicU64::new(0);
static BLOCK_EVICTIONS: AtomicU64 = AtomicU64::new(0);

/// Which cached keys hold SST metadata, and what each weighs.
///
/// SlateDB names an entry's kind on the call that admits it and nowhere
/// else — neither `CachedKey` nor `CachedEntry` exposes it — so one shared
/// cache can only tell metadata from a data block by what was recorded at
/// admission. Without this the two could not be reported apart.
static METADATA_ENTRIES: std::sync::LazyLock<std::sync::RwLock<HashMap<CachedKey, u64>>> =
    std::sync::LazyLock::new(|| std::sync::RwLock::new(HashMap::new()));

static METADATA_OCCUPANCY: AtomicU64 = AtomicU64::new(0);

/// Records an admitted metadata entry. A hit on one already recorded at the
/// same weight takes a read lock and returns, so metadata reads do not
/// serialize on the write lock.
fn metadata_admitted(key: &CachedKey, bytes: u64) {
    if METADATA_ENTRIES
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(key)
        == Some(&bytes)
    {
        return;
    }

    let mut entries = METADATA_ENTRIES
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(previous) = entries.insert(key.clone(), bytes) {
        subtract_metadata_occupancy(previous);
    }

    METADATA_OCCUPANCY.fetch_add(bytes, Ordering::Relaxed);
}

/// Drops a leaving entry's accounting, reporting whether it was metadata.
/// A data block is the common case and costs one read lock.
fn metadata_left(key: &CachedKey) -> bool {
    if !METADATA_ENTRIES
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains_key(key)
    {
        return false;
    }

    let removed = METADATA_ENTRIES
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(key);
    match removed {
        Some(bytes) => {
            subtract_metadata_occupancy(bytes);
            true
        }
        None => false,
    }
}

fn subtract_metadata_occupancy(bytes: u64) {
    let _ = METADATA_OCCUPANCY.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(bytes))
    });
}

/// The process-wide allowance for auxiliary parsed metadata. It is reserved
/// from the first attach's `CACHE_MEMORY` budget.
pub(crate) fn auxiliary_metadata_memory() -> usize {
    usize::try_from(AUXILIARY_METADATA_MEMORY.load(Ordering::Relaxed)).unwrap_or(usize::MAX)
}

/// Records the parsed Parquet metadata cache's current heap footprint.
pub(crate) fn set_auxiliary_metadata_usage(bytes: usize) {
    AUXILIARY_METADATA_USAGE.store(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
}

/// Records one parsed Parquet metadata eviction.
pub(crate) fn auxiliary_metadata_evicted() {
    AUXILIARY_METADATA_EVICTIONS.fetch_add(1, Ordering::Relaxed);
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
    /// Metadata-cache hits performed by attach-time preloads.
    pub preload_metadata_hits: u64,
    /// Metadata-cache misses performed by attach-time preloads.
    pub preload_metadata_misses: u64,
    /// Data-block cache hits performed by attach-time preloads.
    pub preload_block_hits: u64,
    /// Data-block cache misses performed by attach-time preloads.
    pub preload_block_misses: u64,
    /// Subspaces an attach-time preload could not warm.
    pub preload_failures: u64,
}

/// Current size and pressure of the process-shared caches.
///
/// SlateDB metadata, SlateDB data blocks, and parsed Parquet metadata have
/// separate admission policies, so each reports its own memory figures.
/// The disk figure is a configured capacity: Foyer does not expose reliable
/// live disk occupancy through its typed cache API.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStatus {
    /// Memory reserved for decoded SlateDB filters, indexes, and stats.
    pub metadata_capacity_bytes: u64,
    /// Memory currently occupied by those entries.
    pub metadata_occupancy_bytes: u64,
    /// Metadata entries evicted from memory since the cache was built.
    pub metadata_evictions: u64,
    /// Memory reserved for SlateDB data blocks.
    pub block_capacity_bytes: u64,
    /// Memory currently occupied by SlateDB data blocks.
    pub block_occupancy_bytes: u64,
    /// Data-block entries evicted from memory since the cache was built.
    pub block_evictions: u64,
    /// Disk capacity configured for data blocks, or `None` without a disk tier.
    pub block_disk_capacity_bytes: Option<u64>,
    /// Memory reserved for parsed Parquet metadata.
    pub auxiliary_metadata_capacity_bytes: u64,
    /// Memory currently occupied by parsed Parquet metadata.
    pub auxiliary_metadata_occupancy_bytes: u64,
    /// Parsed Parquet metadata entries evicted since the cache was built.
    pub auxiliary_metadata_evictions: u64,
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
    pub(crate) fn since(self, before: Self) -> Self {
        Self {
            metadata_hits: self.metadata_hits.saturating_sub(before.metadata_hits),
            metadata_misses: self.metadata_misses.saturating_sub(before.metadata_misses),
            block_hits: self.block_hits.saturating_sub(before.block_hits),
            block_misses: self.block_misses.saturating_sub(before.block_misses),
            errors: self.errors.saturating_sub(before.errors),
            preload_metadata_hits: self
                .preload_metadata_hits
                .saturating_sub(before.preload_metadata_hits),
            preload_metadata_misses: self
                .preload_metadata_misses
                .saturating_sub(before.preload_metadata_misses),
            preload_block_hits: self
                .preload_block_hits
                .saturating_sub(before.preload_block_hits),
            preload_block_misses: self
                .preload_block_misses
                .saturating_sub(before.preload_block_misses),
            preload_failures: self
                .preload_failures
                .saturating_sub(before.preload_failures),
        }
    }

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

impl ObjectStoreTally {
    pub(crate) fn since(self, before: Self) -> Self {
        Self {
            main_gets: self.main_gets.saturating_sub(before.main_gets),
            main_get_duration: self
                .main_get_duration
                .saturating_sub(before.main_get_duration),
            main_puts: self.main_puts.saturating_sub(before.main_puts),
            main_put_duration: self
                .main_put_duration
                .saturating_sub(before.main_put_duration),
            main_deletes: self.main_deletes.saturating_sub(before.main_deletes),
            main_delete_duration: self
                .main_delete_duration
                .saturating_sub(before.main_delete_duration),
            wal_gets: self.wal_gets.saturating_sub(before.wal_gets),
            wal_get_duration: self
                .wal_get_duration
                .saturating_sub(before.wal_get_duration),
            wal_puts: self.wal_puts.saturating_sub(before.wal_puts),
            wal_put_duration: self
                .wal_put_duration
                .saturating_sub(before.wal_put_duration),
            wal_deletes: self.wal_deletes.saturating_sub(before.wal_deletes),
            wal_delete_duration: self
                .wal_delete_duration
                .saturating_sub(before.wal_delete_duration),
            errors: self.errors.saturating_sub(before.errors),
        }
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
    preload_metadata_hits: AtomicU64,
    preload_metadata_misses: AtomicU64,
    preload_block_hits: AtomicU64,
    preload_block_misses: AtomicU64,
    preload_failures: AtomicU64,
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
            preload_metadata_hits: self.preload_metadata_hits.load(Ordering::Relaxed),
            preload_metadata_misses: self.preload_metadata_misses.load(Ordering::Relaxed),
            preload_block_hits: self.preload_block_hits.load(Ordering::Relaxed),
            preload_block_misses: self.preload_block_misses.load(Ordering::Relaxed),
            preload_failures: self.preload_failures.load(Ordering::Relaxed),
        }
    }

    /// Attributes cache traffic and failures from one completed preload to
    /// this attach and to the process-wide totals.
    pub(crate) fn record_preload(&self, before: CacheTally, after: CacheTally, failures: usize) {
        let failures = u64::try_from(failures).unwrap_or(u64::MAX);
        for counters in [self, COUNTERS.as_ref()] {
            counters.preload_metadata_hits.fetch_add(
                after.metadata_hits.saturating_sub(before.metadata_hits),
                Ordering::Relaxed,
            );
            counters.preload_metadata_misses.fetch_add(
                after.metadata_misses.saturating_sub(before.metadata_misses),
                Ordering::Relaxed,
            );
            counters.preload_block_hits.fetch_add(
                after.block_hits.saturating_sub(before.block_hits),
                Ordering::Relaxed,
            );
            counters.preload_block_misses.fetch_add(
                after.block_misses.saturating_sub(before.block_misses),
                Ordering::Relaxed,
            );
            counters
                .preload_failures
                .fetch_add(failures, Ordering::Relaxed);
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

type MemoryCache = foyer::Cache<CachedKey, CachedEntry>;
type HybridCache = foyer::HybridCache<CachedKey, CachedEntry>;

/// The one cache SlateDB metadata and data blocks share, in whichever
/// tiering the first attach configured.
#[derive(Clone)]
enum Tier {
    Memory(MemoryCache),
    Hybrid(HybridCache),
}

impl Tier {
    fn capacity(&self) -> usize {
        match self {
            Self::Memory(cache) => cache.capacity(),
            Self::Hybrid(cache) => cache.memory().capacity(),
        }
    }

    fn usage(&self) -> usize {
        match self {
            Self::Memory(cache) => cache.usage(),
            Self::Hybrid(cache) => cache.memory().usage(),
        }
    }

    fn entries(&self) -> usize {
        match self {
            Self::Memory(cache) => cache.entries(),
            Self::Hybrid(cache) => cache.memory().entries(),
        }
    }
}

struct CacheObservation {
    tier: Tier,
    metadata_ceiling: u64,
    disk_capacity: Option<u64>,
}

static OBSERVATION: OnceCell<CacheObservation> = OnceCell::const_new();

/// Current process-wide cache sizing and occupancy.
///
/// Both kinds share one capacity, so the metadata figure is the ceiling past
/// which metadata stops being protected rather than a partition, and the
/// block figure is what the whole cache holds.
#[must_use]
pub fn cache_status() -> CacheStatus {
    let Some(observation) = OBSERVATION.get() else {
        return CacheStatus::default();
    };
    let capacity = u64::try_from(observation.tier.capacity()).unwrap_or(u64::MAX);
    let usage = u64::try_from(observation.tier.usage()).unwrap_or(u64::MAX);
    let metadata_occupancy = METADATA_OCCUPANCY.load(Ordering::Relaxed);

    CacheStatus {
        metadata_capacity_bytes: observation.metadata_ceiling,
        metadata_occupancy_bytes: metadata_occupancy,
        metadata_evictions: METADATA_EVICTIONS.load(Ordering::Relaxed),
        block_capacity_bytes: capacity,
        block_occupancy_bytes: usage.saturating_sub(metadata_occupancy),
        block_evictions: BLOCK_EVICTIONS.load(Ordering::Relaxed),
        block_disk_capacity_bytes: observation.disk_capacity,
        auxiliary_metadata_capacity_bytes: AUXILIARY_METADATA_MEMORY.load(Ordering::Relaxed),
        auxiliary_metadata_occupancy_bytes: AUXILIARY_METADATA_USAGE.load(Ordering::Relaxed),
        auxiliary_metadata_evictions: AUXILIARY_METADATA_EVICTIONS.load(Ordering::Relaxed),
    }
}

struct EvictionCounter;

impl EventListener for EvictionCounter {
    type Key = CachedKey;
    type Value = CachedEntry;

    fn on_leave(&self, reason: Event, key: &CachedKey, _: &CachedEntry) {
        let metadata = metadata_left(key);
        if reason != Event::Evict {
            return;
        }
        if metadata {
            &METADATA_EVICTIONS
        } else {
            &BLOCK_EVICTIONS
        }
        .fetch_add(1, Ordering::Relaxed);
    }
}

/// One cache for both kinds, with SST metadata evicted only once every data
/// block is gone.
///
/// Metadata enters hinted `Normal` and data blocks `Low`; foyer's LRU drains
/// the low-priority list first and only falls back to the high-priority one
/// when nothing else is left. That replaces a fixed metadata slot with a
/// ceiling: metadata takes what the store needs up to
/// [`METADATA_PRIORITY_POOL_RATIO`], blocks take the rest, and neither
/// starves the other at a size the operator has to predict.
struct PriorityDbCache {
    tier: Tier,
}

fn as_bytes(size: usize) -> u64 {
    u64::try_from(size).unwrap_or(u64::MAX)
}

fn fill_failed(error: impl std::error::Error + Send + Sync + 'static) -> slatedb::Error {
    slatedb::Error::unavailable("cache fill failed".to_owned()).with_source(Box::new(error))
}

impl PriorityDbCache {
    async fn fetch(
        &self,
        key: CachedKey,
        loader: CacheLoader,
        metadata: bool,
    ) -> Result<CachedEntry, slatedb::Error> {
        let hint = if metadata { Hint::Normal } else { Hint::Low };
        let entry = match &self.tier {
            Tier::Memory(cache) => cache
                .get_or_fetch(&key, move || async move {
                    loader()
                        .await
                        .map(|entry| (entry, CacheProperties::default().with_hint(hint)))
                })
                .await
                .map(|entry| entry.value().clone())
                .map_err(fill_failed)?,
            Tier::Hybrid(cache) => cache
                .get_or_fetch(&key, move || async move {
                    loader()
                        .await
                        .map(|entry| (entry, HybridCacheProperties::default().with_hint(hint)))
                })
                .await
                .map(|entry| entry.value().clone())
                .map_err(fill_failed)?,
        };

        if metadata {
            metadata_admitted(&key, as_bytes(entry.size()));
        }
        Ok(entry)
    }

    async fn read(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        match &self.tier {
            Tier::Memory(cache) => Ok(cache.get(key).map(|entry| entry.value().clone())),
            Tier::Hybrid(cache) => cache
                .get(key)
                .await
                .map(|entry| entry.map(|entry| entry.value().clone()))
                .map_err(fill_failed),
        }
    }
}

#[async_trait]
impl DbCache for PriorityDbCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.read(key).await
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.read(key).await
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.read(key).await
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.read(key).await
    }

    /// The one admission that does not say what it is caching: SlateDB
    /// calls it both for blocks a scan just read and, under write-through
    /// admission, for a fresh SST's metadata. Everything arriving here is
    /// treated as a block — guessing "metadata" would let a scan fill the
    /// protected segment, which is what this whole design prevents.
    /// Metadata guessed wrong is protected again the next time a
    /// `fetch_index`, `fetch_filter`, or `fetch_stats` reads it.
    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        match &self.tier {
            Tier::Memory(cache) => {
                cache.insert_with_properties(
                    key,
                    value,
                    CacheProperties::default().with_hint(Hint::Low),
                );
            }
            Tier::Hybrid(cache) => {
                cache.insert_with_properties(
                    key,
                    value,
                    HybridCacheProperties::default().with_hint(Hint::Low),
                );
            }
        }
    }

    async fn remove(&self, key: &CachedKey) {
        match &self.tier {
            Tier::Memory(cache) => {
                cache.remove(key);
            }
            Tier::Hybrid(cache) => cache.remove(key),
        }
    }

    fn entry_count(&self) -> u64 {
        as_bytes(self.tier.entries())
    }

    async fn fetch_block(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader, false).await
    }

    async fn fetch_index(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader, true).await
    }

    async fn fetch_filter(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader, true).await
    }

    async fn fetch_stats(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader, true).await
    }
}

/// The eviction config both tiers take. LRU is not a preference: `Hint` is
/// the only thing that separates metadata from blocks here, and LFU, S3-FIFO,
/// and Sieve all ignore it.
fn eviction_config() -> LruConfig {
    LruConfig {
        high_priority_pool_ratio: METADATA_PRIORITY_POOL_RATIO,
    }
}

fn memory_cache(capacity: u64) -> MemoryCache {
    foyer::CacheBuilder::new(usize::try_from(capacity).unwrap_or(usize::MAX))
        .with_eviction_config(eviction_config())
        .with_weighter(|_: &CachedKey, value: &CachedEntry| value.size())
        .with_event_listener(Arc::new(EvictionCounter))
        .build()
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
    let (auxiliary_metadata_bytes, shared_bytes) = config.slots();
    AUXILIARY_METADATA_MEMORY.store(auxiliary_metadata_bytes, Ordering::Relaxed);

    // The device is the only part that can fail, and a memory tier alone is
    // strictly better than no cache at all.
    let tier = match &config.dir {
        Some(dir) => match hybrid(dir, shared_bytes, config.disk_size).await {
            Some(cache) => Tier::Hybrid(cache),
            None => Tier::Memory(memory_cache(shared_bytes)),
        },
        None => Tier::Memory(memory_cache(shared_bytes)),
    };

    let disk_capacity =
        matches!(tier, Tier::Hybrid(_)).then(|| config.disk_size.unwrap_or(DEFAULT_CACHE_DISK));
    let ceiling = metadata_ceiling(shared_bytes);
    let _ = OBSERVATION.set(CacheObservation {
        tier: tier.clone(),
        metadata_ceiling: ceiling,
        disk_capacity,
    });

    info!(
        shared_bytes,
        metadata_ceiling = ceiling,
        auxiliary_metadata_bytes,
        disk = config.dir.is_some(),
        "built the shared block cache"
    );

    Some(Arc::new(PriorityDbCache { tier }) as Arc<dyn DbCache>)
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
async fn hybrid(dir: &PathBuf, memory: u64, disk: Option<u64>) -> Option<HybridCache> {
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
        .with_event_listener(Arc::new(EvictionCounter))
        .memory(usize::try_from(memory).unwrap_or(usize::MAX))
        .with_eviction_config(eviction_config())
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
        Ok(cache) => Some(cache),
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

        let tier = Tier::Hybrid(
            hybrid(&cache_root, 4 * 1024 * 1024, Some(64 * 1024 * 1024))
                .await
                .unwrap(),
        );
        let cache: Arc<dyn DbCache> = Arc::new(PriorityDbCache { tier: tier.clone() });
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
        let Tier::Hybrid(hybrid) = tier else {
            unreachable!("built as a hybrid tier")
        };
        hybrid.close().await.unwrap();
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

    /// The budget splits across both SlateDB slots and parsed Parquet
    /// metadata, with data blocks retaining the largest share.
    #[test]
    fn the_budget_includes_auxiliary_metadata() {
        let config = CacheConfig {
            memory: Some(1_000),
            ..CacheConfig::default()
        };
        let (auxiliary, shared) = config.slots();
        assert_eq!(auxiliary + shared, 1_000);
        assert!(auxiliary < shared);
    }

    /// An unset budget still totals what SlateDB gives one store, but
    /// metadata may now grow into most of it rather than a fixed fifth.
    #[test]
    fn an_unset_budget_matches_one_stores_defaults() {
        let (auxiliary, shared) = CacheConfig::default().slots();
        assert_eq!(auxiliary + shared, DEFAULT_CACHE_MEMORY);
        assert_eq!(auxiliary, 8 * 1024 * 1024);
        assert_eq!(shared, 632 * 1024 * 1024);
        assert!(metadata_ceiling(shared) > 4 * 120 * 1024 * 1024);
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
            ..CacheTally::default()
        };
        assert!((tally.metadata_hit_rate().unwrap() - 0.75).abs() < f64::EPSILON);
        assert!((tally.block_hit_rate().unwrap() - 0.25).abs() < f64::EPSILON);
    }

    /// A meta slot smaller than the store's SST metadata reports what it
    /// cannot hold; equal is not a shortfall.
    #[test]
    fn a_meta_slot_smaller_than_the_store_reports_its_shortfall() {
        assert_eq!(metadata_shortfall(0, 0), None);
        assert_eq!(metadata_shortfall(625_189_215, 625_189_215), None);
        assert_eq!(metadata_shortfall(625_189_215, 850_604_851), None);

        // Priority lifts the default ceiling from 120 MiB to 568 MiB, which
        // still leaves the store that prompted the check short.
        let (_, shared) = CacheConfig::default().slots();
        assert_eq!(
            metadata_shortfall(625_189_215, metadata_ceiling(shared)),
            Some(28_759_187)
        );
    }

    /// Metadata outlives every data block, which is the whole of the
    /// protection: foyer drains the low-priority list first and reaches the
    /// high-priority one only once nothing else is left. The hint is honoured
    /// by LRU alone, so this also pins the eviction algorithm.
    #[test]
    fn blocks_are_evicted_before_metadata() {
        // One shard: foyer divides the capacity across eight by default, and
        // a protected pool is a share of a *shard*, so a cache this small
        // would otherwise be measuring the sharding rather than the policy.
        let cache: foyer::Cache<u64, u64> = foyer::CacheBuilder::new(64)
            .with_shards(1)
            .with_eviction_config(eviction_config())
            .with_weighter(|_: &u64, _: &u64| 1)
            .build();

        cache.insert_with_properties(0, 0, CacheProperties::default().with_hint(Hint::Normal));
        for block in 1..512 {
            cache.insert_with_properties(
                block,
                block,
                CacheProperties::default().with_hint(Hint::Low),
            );
        }

        assert!(
            cache.get(&0).is_some(),
            "metadata evicted before the blocks"
        );
    }

    /// A budget too small to split still yields usable slots rather than
    /// a zero-capacity cache.
    #[test]
    fn a_tiny_budget_still_splits() {
        let config = CacheConfig {
            memory: Some(1),
            ..CacheConfig::default()
        };
        let (auxiliary, shared) = config.slots();
        assert_eq!(auxiliary, 0);
        assert!(shared >= 1);
    }

    /// The public status reports the actual live memory-cache dimensions,
    /// and occupancy never exceeds the capacity enforcing it.
    #[tokio::test]
    async fn cache_status_reports_live_memory_dimensions() {
        let _ = shared(&CacheConfig::default()).await;
        let status = cache_status();
        assert!(status.metadata_capacity_bytes > 0);
        assert!(status.block_capacity_bytes > 0);
        // Metadata past the ceiling is demoted rather than refused, so it is
        // bounded by the whole cache and not by its own protected share.
        assert!(status.metadata_capacity_bytes < status.block_capacity_bytes);
        assert!(status.metadata_occupancy_bytes <= status.block_capacity_bytes);
        assert!(status.block_occupancy_bytes <= status.block_capacity_bytes);
        assert!(
            status.auxiliary_metadata_occupancy_bytes <= status.auxiliary_metadata_capacity_bytes
        );
    }
}
