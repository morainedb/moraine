//! The process-wide cache of parsed Parquet footers and decoded file row-id
//! summaries, sharing one byte allowance reserved from the store cache's
//! budget. Concurrent misses on one footer share a single fill.

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock, Mutex, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use foyer::Cache;
use object_store::{ObjectStore, path::Path};
use parquet::{
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
};
use tracing::warn;

use crate::{
    data_file::{reader::ObjectStoreReader, row_set::FileRowSet, usize_as_u64},
    store::cache,
};

/// Whether a cached footer carries the page index. Mirrors
/// [`PageIndexPolicy`], which is not hashable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// One file's identity within its store.
///
/// Catalog file ids are never reused, so the path and recorded size only
/// guard an imported catalog that violates that: a mismatch misses rather
/// than serving another file's rows.
pub(super) struct FileSummaryKey<'a> {
    pub(super) table_id: u64,
    pub(super) data_file_id: u64,
    pub(super) path: &'a Path,
    pub(super) file_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum AuxiliaryKey {
    Metadata {
        store: u64,
        path: Path,
        file_size: u64,
        page_index: PageIndex,
    },
    Summary {
        store: u64,
        table_id: u64,
        data_file_id: u64,
        path: Path,
        file_size: u64,
    },
}

#[derive(Debug, Clone)]
enum AuxiliaryValue {
    Metadata(Arc<ParquetMetaData>),
    Summary(Arc<FileRowSet>),
}

impl AuxiliaryValue {
    fn bytes(&self) -> usize {
        match self {
            Self::Metadata(metadata) => metadata.memory_size(),
            Self::Summary(rows) => usize::try_from(rows.estimated_bytes()).unwrap_or(usize::MAX),
        }
    }
}

type StoreIdentities = HashMap<usize, (Weak<dyn ObjectStore>, u64)>;

/// Identities handed to stores, keyed by the address each occupies.
///
/// The weak reference is what makes an address trustworthy: it keeps the
/// allocation alive, so no later store can take the address of one still
/// registered here. An entry whose store is gone is replaced rather than
/// reused, which is where the address becomes available again.
static STORE_IDENTITIES: LazyLock<Mutex<StoreIdentities>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static NEXT_STORE_IDENTITY: AtomicU64 = AtomicU64::new(0);

pub(super) fn store_id(store: &Arc<dyn ObjectStore>) -> u64 {
    let address = Arc::as_ptr(store).cast::<()>().addr();
    let mut identities = STORE_IDENTITIES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    if let Some((weak, id)) = identities.get(&address)
        && weak.strong_count() > 0
    {
        return *id;
    }

    identities.retain(|_, (weak, _)| weak.strong_count() > 0);
    let id = NEXT_STORE_IDENTITY.fetch_add(1, Ordering::Relaxed);
    identities.insert(address, (Arc::downgrade(store), id));

    id
}

/// One weighted cache over both kinds, so footers and summaries take the
/// allowance in proportion to what is actually asked for.
pub(super) struct AuxiliaryCache {
    cache: Cache<AuxiliaryKey, AuxiliaryValue>,
    /// What the cache was last built or resized to. Read by the admission
    /// filter, which cannot borrow the cache it belongs to.
    capacity: Arc<AtomicUsize>,
}

impl AuxiliaryCache {
    pub(super) fn new(capacity: usize) -> Self {
        let admitted = Arc::new(AtomicUsize::new(capacity));
        let limit = Arc::clone(&admitted);

        // One shard: the allowance is small next to a single parsed footer,
        // and foyer divides capacity evenly across eight by default — which
        // would reject any footer larger than an eighth of it.
        let cache = foyer::CacheBuilder::new(capacity)
            .with_shards(1)
            .with_weighter(|_: &AuxiliaryKey, value: &AuxiliaryValue| value.bytes())
            .with_filter(move |_: &AuxiliaryKey, value: &AuxiliaryValue| {
                let limit = limit.load(Ordering::Relaxed);
                limit > 0 && value.bytes() <= limit
            })
            .with_event_listener(Arc::new(EvictionCounter))
            .build();

        Self {
            cache,
            capacity: admitted,
        }
    }

    /// This file's parsed footer, loading it through `reader` on a miss.
    /// Concurrent misses on one key share the single fill.
    pub(super) async fn metadata(
        &self,
        reader: &ObjectStoreReader,
        prefetch: Option<usize>,
    ) -> ParquetResult<Arc<ParquetMetaData>> {
        let key = AuxiliaryKey::Metadata {
            store: store_id(&reader.store),
            path: reader.path.clone(),
            file_size: reader.file_size,
            page_index: reader.page_index.into(),
        };

        if let Some(entry) = self.cache.get(&key) {
            reader.metrics.metadata_hit();
            return metadata_of(entry.value());
        }
        reader.metrics.metadata_miss();

        let mut loader = reader.clone();
        let file_size = reader.file_size;
        let page_index = reader.page_index;
        let fill = move || async move {
            ParquetMetaDataReader::new()
                .with_page_index_policy(page_index)
                .with_prefetch_hint(prefetch)
                .load_and_finish(&mut loader, file_size)
                .await
                .map(|metadata| AuxiliaryValue::Metadata(Arc::new(metadata)))
        };

        // Deliberately the calling runtime: a fill is keyed to one store,
        // so the only readers a cancelled fill can strand belong to the
        // attach that is tearing that store down anyway. Moving fills to a
        // long-lived runtime instead costs the lockstep start that lets a
        // commit's file reads overlap.
        let fetch = self.cache.get_or_fetch(&key, fill);

        let entry = fetch
            .await
            .map_err(|error| ParquetError::General(error.to_string()))?;

        metadata_of(entry.value())
    }

    pub(super) fn summary(
        &self,
        store: &Arc<dyn ObjectStore>,
        key: &FileSummaryKey<'_>,
    ) -> Option<Arc<FileRowSet>> {
        match self.cache.get(&Self::file_summary_key(store, key))?.value() {
            AuxiliaryValue::Summary(rows) => Some(Arc::clone(rows)),
            AuxiliaryValue::Metadata(_) => None,
        }
    }

    pub(super) fn insert_summary(
        &self,
        store: &Arc<dyn ObjectStore>,
        key: &FileSummaryKey<'_>,
        rows: &Arc<FileRowSet>,
    ) {
        self.cache.insert(
            Self::file_summary_key(store, key),
            AuxiliaryValue::Summary(Arc::clone(rows)),
        );
    }

    fn file_summary_key(store: &Arc<dyn ObjectStore>, key: &FileSummaryKey<'_>) -> AuxiliaryKey {
        AuxiliaryKey::Summary {
            store: store_id(store),
            table_id: key.table_id,
            data_file_id: key.data_file_id,
            path: key.path.clone(),
            file_size: key.file_size,
        }
    }

    /// Settles the allowance, evicting whatever no longer fits. A refusal
    /// leaves the previous capacity in force: a cache that would not resize
    /// is still a working cache.
    pub(super) fn resize(&self, capacity: usize) {
        match self.cache.resize(capacity) {
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

    /// Tracked rather than read back: foyer's `resize` moves each shard's
    /// capacity and evicts against it, but leaves `Cache::capacity` at
    /// whatever the cache was built with.
    pub(super) fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    pub(super) fn usage(&self) -> usize {
        self.cache.usage()
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

struct EvictionCounter;

impl foyer::EventListener for EvictionCounter {
    type Key = AuxiliaryKey;
    type Value = AuxiliaryValue;

    fn on_leave(&self, reason: foyer::Event, _: &AuxiliaryKey, _: &AuxiliaryValue) {
        if reason == foyer::Event::Evict {
            cache::auxiliary_evicted();
        }
    }
}

static SHARED: LazyLock<AuxiliaryCache> =
    LazyLock::new(|| AuxiliaryCache::new(cache::auxiliary_metadata_memory()));

pub(super) fn shared() -> &'static AuxiliaryCache {
    &SHARED
}

/// The allowance's current size and occupancy, for the process's cache
/// report.
pub(crate) fn occupancy() -> (u64, u64) {
    (
        usize_as_u64(SHARED.capacity()),
        usize_as_u64(SHARED.usage()),
    )
}

/// Sizes the cache to the allowance the first attach reserved. The cache
/// is built on first use, which can precede that attach.
pub(crate) fn resize(capacity: usize) {
    SHARED.resize(capacity);
}
