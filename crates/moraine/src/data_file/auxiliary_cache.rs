//! The process-wide cache of parsed Parquet footers and decoded file row-id
//! summaries, sharing one byte allowance reserved from the store cache's
//! budget, and the store cache's directory when it has one. Concurrent
//! misses on one footer share a single fill.

use std::{
    future::Future,
    io::{Read, Write},
    path::{Path as FilePath, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use foyer::{Cache, HybridCache};
use object_store::path::Path;
use parquet::{
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{
        PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter,
    },
};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    data_file::{
        DataStore,
        data_store::StoreIdentity,
        reader::ObjectStoreReader,
        row_set::{FileRowSet, FileRowSetKind},
        usize_as_u64,
    },
    error::{Error, Result},
    store::cache::{self, RowSummaryOccupancy},
};

/// Whether a cached footer carries the page index. Mirrors
/// [`PageIndexPolicy`], which is not hashable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(super) enum PageIndex {
    Skip,
    Optional,
    Required,
}

impl From<PageIndexPolicy> for PageIndex {
    fn from(policy: PageIndexPolicy) -> Self {
        match policy {
            PageIndexPolicy::Skip => Self::Skip,
            PageIndexPolicy::Optional => Self::Optional,
            PageIndexPolicy::Required => Self::Required,
        }
    }
}

/// One file's identity within its store. The path and recorded size guard
/// against a reused catalog file id: a mismatch misses.
pub(super) struct FileSummaryKey<'a> {
    pub(super) table_id: u64,
    pub(super) data_file_id: u64,
    pub(super) path: &'a Path,
    pub(super) file_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum AuxiliaryKey {
    Metadata {
        store: StoreIdentity,
        path: String,
        file_size: u64,
        page_index: PageIndex,
    },
    Summary {
        store: StoreIdentity,
        table_id: u64,
        data_file_id: u64,
        path: String,
        file_size: u64,
    },
}

#[derive(Debug, Clone)]
pub(super) enum AuxiliaryValue {
    Metadata(Arc<ParquetMetaData>),
    Summary(Arc<FileRowSet>),
}

/// A value with its charge measured once, at insertion.
#[derive(Debug, Clone)]
pub(super) struct Weighed {
    pub(super) value: AuxiliaryValue,
    bytes: usize,
}

impl From<AuxiliaryValue> for Weighed {
    fn from(value: AuxiliaryValue) -> Self {
        let bytes = match &value {
            AuxiliaryValue::Metadata(metadata) => metadata.memory_size(),
            AuxiliaryValue::Summary(rows) => {
                usize::try_from(rows.estimated_bytes()).unwrap_or(usize::MAX)
            }
        };

        Self { value, bytes }
    }
}

/// The disk form of a value: a tag byte, then a footer as the Parquet
/// metadata writer lays it out (page index included) or a summary in its
/// own shape.
mod tag {
    pub(super) const METADATA: u8 = 0;
    pub(super) const RANGE: u8 = 1;
    pub(super) const ROARING: u8 = 2;
    pub(super) const SORTED: u8 = 3;
}

fn parse_failed(error: impl std::error::Error + Send + Sync + 'static) -> foyer::Error {
    foyer::Error::new(foyer::ErrorKind::Parse, "auxiliary cache value").with_source(error)
}

fn read_u64(reader: &mut impl Read) -> foyer::Result<u64> {
    let mut buffer = [0; 8];
    reader
        .read_exact(&mut buffer)
        .map_err(foyer::Error::io_error)?;
    Ok(u64::from_le_bytes(buffer))
}

impl foyer::Code for Weighed {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        let io = foyer::Error::io_error;
        match &self.value {
            AuxiliaryValue::Metadata(metadata) => {
                writer.write_all(&[tag::METADATA]).map_err(io)?;
                let mut buffer = Vec::new();
                ParquetMetaDataWriter::new(&mut buffer, metadata)
                    .finish()
                    .map_err(parse_failed)?;
                writer.write_all(&buffer).map_err(io)
            }
            AuxiliaryValue::Summary(rows) => match rows.as_ref() {
                FileRowSet::Range { start, end } => {
                    writer.write_all(&[tag::RANGE]).map_err(io)?;
                    writer.write_all(&start.to_le_bytes()).map_err(io)?;
                    writer.write_all(&end.to_le_bytes()).map_err(io)
                }
                FileRowSet::Roaring(bitmap) => {
                    writer.write_all(&[tag::ROARING]).map_err(io)?;
                    bitmap.serialize_into(writer).map_err(io)
                }
                FileRowSet::Sorted(row_ids) => {
                    writer.write_all(&[tag::SORTED]).map_err(io)?;
                    writer
                        .write_all(&usize_as_u64(row_ids.len()).to_le_bytes())
                        .map_err(io)?;
                    row_ids
                        .iter()
                        .try_for_each(|row_id| writer.write_all(&row_id.to_le_bytes()))
                        .map_err(io)
                }
            },
        }
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let io = foyer::Error::io_error;
        let mut tag = [0];
        reader.read_exact(&mut tag).map_err(io)?;

        let value = match tag[0] {
            tag::METADATA => {
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).map_err(io)?;
                let metadata = ParquetMetaDataReader::new()
                    .with_page_index_policy(PageIndexPolicy::Optional)
                    .parse_and_finish(&Bytes::from(buffer))
                    .map_err(parse_failed)?;
                AuxiliaryValue::Metadata(Arc::new(metadata))
            }
            tag::RANGE => {
                let start = read_u64(reader)?;
                let end = read_u64(reader)?;
                AuxiliaryValue::Summary(Arc::new(FileRowSet::Range { start, end }))
            }
            tag::ROARING => {
                let bitmap = RoaringTreemap::deserialize_from(reader).map_err(io)?;
                AuxiliaryValue::Summary(Arc::new(FileRowSet::Roaring(bitmap)))
            }
            tag::SORTED => {
                let count = usize::try_from(read_u64(reader)?).unwrap_or(usize::MAX);
                let row_ids = (0..count)
                    .map(|_| read_u64(reader))
                    .collect::<foyer::Result<Vec<u64>>>()?;
                AuxiliaryValue::Summary(Arc::new(FileRowSet::Sorted(row_ids)))
            }
            other => {
                return Err(foyer::Error::new(
                    foyer::ErrorKind::Parse,
                    format!("unknown auxiliary cache tag {other}"),
                ));
            }
        };

        Ok(Self::from(value))
    }

    fn estimated_size(&self) -> usize {
        self.bytes
    }
}

/// Resident row summaries by shape, and their bytes together. Counted up
/// on insertion and down as entries leave, whatever the reason.
#[derive(Default)]
struct RowSummaryCounters {
    range: AtomicU64,
    roaring: AtomicU64,
    sorted: AtomicU64,
    bytes: AtomicU64,
}

impl RowSummaryCounters {
    fn of(&self, kind: FileRowSetKind) -> &AtomicU64 {
        match kind {
            FileRowSetKind::Range => &self.range,
            FileRowSetKind::Roaring => &self.roaring,
            FileRowSetKind::Sorted => &self.sorted,
        }
    }

    fn entered(&self, weighed: &Weighed) {
        if let AuxiliaryValue::Summary(rows) = &weighed.value {
            self.of(rows.kind()).fetch_add(1, Ordering::Relaxed);
            self.bytes
                .fetch_add(usize_as_u64(weighed.bytes), Ordering::Relaxed);
        }
    }

    fn left(&self, weighed: &Weighed) {
        if let AuxiliaryValue::Summary(rows) = &weighed.value {
            saturating_decrement(self.of(rows.kind()), 1);
            saturating_decrement(&self.bytes, usize_as_u64(weighed.bytes));
        }
    }

    fn occupancy(&self) -> RowSummaryOccupancy {
        RowSummaryOccupancy {
            range: self.range.load(Ordering::Relaxed),
            roaring: self.roaring.load(Ordering::Relaxed),
            sorted: self.sorted.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

fn saturating_decrement(counter: &AtomicU64, by: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(by))
    });
}

type MemoryTier = Cache<AuxiliaryKey, Weighed>;
type HybridTier = HybridCache<AuxiliaryKey, Weighed>;

/// The cache in whichever tiering the process configured. Only the memory
/// tier is resized; a disk device keeps the capacity it opened with.
enum Tier {
    Memory(MemoryTier),
    Hybrid(HybridTier),
}

impl Tier {
    fn usage(&self) -> usize {
        match self {
            Self::Memory(cache) => cache.usage(),
            Self::Hybrid(cache) => cache.memory().usage(),
        }
    }

    fn resize(&self, capacity: usize) -> std::result::Result<(), foyer::Error> {
        match self {
            Self::Memory(cache) => cache.resize(capacity),
            Self::Hybrid(cache) => cache.memory().resize(capacity),
        }
    }

    async fn get(&self, key: &AuxiliaryKey) -> Option<Weighed> {
        match self {
            Self::Memory(cache) => cache.get(key).map(|entry| entry.value().clone()),
            Self::Hybrid(cache) => match cache.get(key).await {
                Ok(entry) => entry.map(|entry| entry.value().clone()),
                Err(error) => {
                    warn!(%error, "an auxiliary cache read failed; treating it as a miss");
                    None
                }
            },
        }
    }

    async fn get_or_fetch<F, Fut, E>(
        &self,
        key: &AuxiliaryKey,
        fill: F,
    ) -> std::result::Result<Weighed, foyer::Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = std::result::Result<Weighed, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Self::Memory(cache) => cache
                .get_or_fetch(key, fill)
                .await
                .map(|entry| entry.value().clone()),
            Self::Hybrid(cache) => cache
                .get_or_fetch(key, fill)
                .await
                .map(|entry| entry.value().clone()),
        }
    }

    #[cfg(test)]
    fn insert(&self, key: AuxiliaryKey, value: Weighed) {
        match self {
            Self::Memory(cache) => {
                cache.insert(key, value);
            }
            Self::Hybrid(cache) => {
                cache.insert(key, value);
            }
        }
    }
}

/// One weighted cache over both footers and summaries.
pub(super) struct AuxiliaryCache {
    tier: Tier,
    capacity: Arc<AtomicUsize>,
    summaries: Arc<RowSummaryCounters>,
}

/// The parts of a build both tiers share.
struct Parts {
    admitted: Arc<AtomicUsize>,
    summaries: Arc<RowSummaryCounters>,
    listener: Arc<dyn foyer::EventListener<Key = AuxiliaryKey, Value = Weighed>>,
}

impl Parts {
    fn new(capacity: usize) -> Self {
        let summaries = Arc::new(RowSummaryCounters::default());
        Self {
            admitted: Arc::new(AtomicUsize::new(capacity)),
            listener: Arc::new(EvictionCounter {
                summaries: Arc::clone(&summaries),
            }),
            summaries,
        }
    }

    /// Refuses a value the allowance could never hold.
    fn filter(&self) -> impl Fn(&AuxiliaryKey, &Weighed) -> bool + Send + Sync + 'static {
        let limit = Arc::clone(&self.admitted);
        move |_: &AuxiliaryKey, value: &Weighed| {
            let limit = limit.load(Ordering::Relaxed);
            limit > 0 && value.bytes <= limit
        }
    }
}

impl AuxiliaryCache {
    pub(super) fn new(capacity: usize) -> Self {
        let parts = Parts::new(capacity);

        // One shard: foyer splits capacity across shards, and a footer must
        // fit within one.
        let cache = foyer::CacheBuilder::new(capacity)
            .with_shards(1)
            .with_weighter(|_: &AuxiliaryKey, value: &Weighed| value.bytes)
            .with_filter(parts.filter())
            .with_event_listener(Arc::clone(&parts.listener))
            .build();

        Self {
            tier: Tier::Memory(cache),
            capacity: parts.admitted,
            summaries: parts.summaries,
        }
    }

    /// A cache of `capacity` bytes over a disk device of `disk` bytes at
    /// `dir`; `None` if the device will not open.
    pub(super) async fn hybrid(capacity: usize, dir: &FilePath, disk: u64) -> Option<Self> {
        let parts = Parts::new(capacity);
        let memory = foyer::HybridCacheBuilder::new()
            .with_event_listener(Arc::clone(&parts.listener))
            .with_policy(foyer::HybridCachePolicy::WriteOnInsertion)
            .memory(capacity)
            .with_shards(1)
            .with_weighter(|_: &AuxiliaryKey, value: &Weighed| value.bytes)
            .with_filter(parts.filter());

        Some(Self {
            tier: Tier::Hybrid(cache::disk_tier(memory, dir, disk).await?),
            capacity: parts.admitted,
            summaries: parts.summaries,
        })
    }

    /// This file's parsed footer, loading it through `reader` on a miss.
    /// Concurrent misses on one key share the single fill.
    pub(super) async fn metadata(
        &self,
        reader: &ObjectStoreReader,
        prefetch: Option<usize>,
    ) -> ParquetResult<Arc<ParquetMetaData>> {
        let key = AuxiliaryKey::Metadata {
            store: reader.file.store.identity,
            path: reader.file.path.to_string(),
            file_size: reader.file.file_size,
            page_index: reader.page_index.into(),
        };

        if let Some(entry) = self.tier.get(&key).await {
            reader.file.metrics.metadata_hit();
            return metadata_of(&entry.value);
        }
        reader.file.metrics.metadata_miss();

        let mut loader = reader.clone();
        let file_size = reader.file.file_size;
        let page_index = reader.page_index;
        let fill = move || async move {
            ParquetMetaDataReader::new()
                .with_page_index_policy(page_index)
                .with_prefetch_hint(prefetch)
                .load_and_finish(&mut loader, file_size)
                .await
                .map(|metadata| Weighed::from(AuxiliaryValue::Metadata(Arc::new(metadata))))
        };

        let entry = self
            .tier
            .get_or_fetch(&key, fill)
            .await
            .map_err(|error| ParquetError::General(error.to_string()))?;

        metadata_of(&entry.value)
    }

    pub(super) async fn summary(
        &self,
        store: &DataStore,
        key: &FileSummaryKey<'_>,
    ) -> Option<Arc<FileRowSet>> {
        summary_of(
            &self
                .tier
                .get(&Self::file_summary_key(store, key))
                .await?
                .value,
        )
    }

    /// This file's row-id summary, building it through `fill` on a miss.
    /// Concurrent misses on one key share the single fill.
    pub(super) async fn fetch_summary<F, Fut>(
        &self,
        store: &DataStore,
        key: &FileSummaryKey<'_>,
        fill: F,
    ) -> Result<Arc<FileRowSet>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Arc<FileRowSet>>> + Send + 'static,
    {
        let fill = fill();
        let summaries = Arc::clone(&self.summaries);
        let fill = || async move {
            fill.await.map(|rows| {
                let weighed = Weighed::from(AuxiliaryValue::Summary(rows));
                summaries.entered(&weighed);
                weighed
            })
        };

        let entry = self
            .tier
            .get_or_fetch(&Self::file_summary_key(store, key), fill)
            .await
            .map_err(|error| {
                let cause = std::error::Error::source(&error)
                    .map_or_else(|| error.to_string(), ToString::to_string);
                Error::Corruption(format!("row summary: {cause}"))
            })?;

        summary_of(&entry.value).ok_or_else(|| {
            Error::Corruption("a footer was cached under a row summary key".to_owned())
        })
    }

    #[cfg(test)]
    pub(super) fn insert_summary(
        &self,
        store: &DataStore,
        key: &FileSummaryKey<'_>,
        rows: &Arc<FileRowSet>,
    ) {
        let weighed = Weighed::from(AuxiliaryValue::Summary(Arc::clone(rows)));
        self.summaries.entered(&weighed);
        self.tier
            .insert(Self::file_summary_key(store, key), weighed);
    }

    fn file_summary_key(store: &DataStore, key: &FileSummaryKey<'_>) -> AuxiliaryKey {
        AuxiliaryKey::Summary {
            store: store.identity,
            table_id: key.table_id,
            data_file_id: key.data_file_id,
            path: key.path.to_string(),
            file_size: key.file_size,
        }
    }

    /// Settles the allowance, evicting whatever no longer fits. A refusal
    /// leaves the previous capacity in force.
    pub(super) fn resize(&self, capacity: usize) {
        match self.tier.resize(capacity) {
            Ok(()) => {
                self.capacity.store(capacity, Ordering::Relaxed);
            }
            Err(error) => warn!(
                capacity,
                %error,
                "could not resize the auxiliary cache; its previous capacity stands"
            ),
        }
    }

    /// Tracked here because foyer's `Cache::capacity` does not follow
    /// `resize`.
    pub(super) fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    pub(super) fn usage(&self) -> usize {
        self.tier.usage()
    }

    pub(super) fn row_summaries(&self) -> RowSummaryOccupancy {
        self.summaries.occupancy()
    }
}

fn summary_of(value: &AuxiliaryValue) -> Option<Arc<FileRowSet>> {
    match value {
        AuxiliaryValue::Summary(rows) => Some(Arc::clone(rows)),
        AuxiliaryValue::Metadata(_) => None,
    }
}

fn metadata_of(value: &AuxiliaryValue) -> ParquetResult<Arc<ParquetMetaData>> {
    match value {
        AuxiliaryValue::Metadata(metadata) => Ok(Arc::clone(metadata)),
        AuxiliaryValue::Summary(_) => Err(ParquetError::General(
            "a row summary was cached under a metadata key".to_owned(),
        )),
    }
}

struct EvictionCounter {
    summaries: Arc<RowSummaryCounters>,
}

impl foyer::EventListener for EvictionCounter {
    type Key = AuxiliaryKey;
    type Value = Weighed;

    fn on_leave(&self, reason: foyer::Event, _: &AuxiliaryKey, value: &Weighed) {
        self.summaries.left(value);
        if reason == foyer::Event::Evict {
            cache::auxiliary_evicted();
        }
    }
}

/// The process's cache: the one the first attach installs, or a memory
/// cache at the default allowance if a read comes first.
static SHARED: OnceLock<AuxiliaryCache> = OnceLock::new();

pub(super) fn shared() -> &'static AuxiliaryCache {
    SHARED.get_or_init(|| AuxiliaryCache::new(cache::auxiliary_metadata_memory()))
}

/// The allowance's current size and occupancy, for the process's cache
/// report.
pub(crate) fn occupancy() -> (u64, u64) {
    let cache = shared();
    (usize_as_u64(cache.capacity()), usize_as_u64(cache.usage()))
}

/// The resident row summaries by shape, for the process's cache report.
pub(crate) fn row_summary_occupancy() -> RowSummaryOccupancy {
    shared().row_summaries()
}

/// Installs the cache the first attach sized: over a disk device of `disk`
/// bytes at `dir` when given, else in memory. A cache a read already built
/// is resized instead and stays in memory.
pub(crate) async fn install(capacity: usize, dir: Option<PathBuf>, disk: u64) {
    if let Some(existing) = SHARED.get() {
        existing.resize(capacity);
        return;
    }

    let built = match dir {
        Some(dir) => AuxiliaryCache::hybrid(capacity, &dir, disk).await,
        None => None,
    };
    let built = built.unwrap_or_else(|| AuxiliaryCache::new(capacity));

    if let Err(built) = SHARED.set(built) {
        drop(built);
        shared().resize(capacity);
    }
}
