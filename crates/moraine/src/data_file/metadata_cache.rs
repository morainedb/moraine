//! Byte-range access to one immutable Parquet object, and the process-wide
//! cache of parsed footers behind it. Concurrent misses on one path share a
//! single fill instead of each fetching the footer.

use std::{
    collections::VecDeque,
    sync::{Arc, LazyLock, Mutex, Weak},
    time::Instant,
};

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
};

use crate::{
    data_file::{metrics::ScopedReadMetrics, usize_as_u64},
    store::cache,
};

/// An [`AsyncFileReader`] over moraine's own object store: the Parquet
/// footer and the projected column chunks arrive as byte-range reads, so a
/// scoped read never downloads the whole data file. Deliberately not
/// `parquet`'s built-in `object_store` integration, which pins a different
/// `object_store` major than the workspace.
pub(super) struct ObjectStoreReader {
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) path: Path,
    /// The object's total length, required to locate the footer.
    pub(super) file_size: u64,
    /// The serialized Parquet metadata length recorded by DuckLake. The
    /// trailing length and magic occupy another eight bytes.
    pub(super) footer_size: u64,
    /// Whether to load the page index with the footer. A row selection
    /// needs it to skip pages; without one it is footer bytes for nothing.
    pub(super) page_index: PageIndexPolicy,
    pub(super) metrics: Arc<ScopedReadMetrics>,
}

/// Parsed metadata from immutable Parquet objects, bounded by its reported
/// heap footprint. A weak store reference both prevents this process-wide
/// cache from keeping catalogs alive and distinguishes equal paths belonging
/// to different stores.
struct MetadataCacheEntry {
    store: Weak<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    page_index: PageIndexPolicy,
    metadata: Arc<ParquetMetaData>,
    bytes: usize,
}

type MetadataInFlightResult = std::result::Result<Arc<ParquetMetaData>, Arc<str>>;

pub(super) enum MetadataLookup {
    Cached(Arc<ParquetMetaData>),
    InFlight(MetadataInFlightLease),
}

/// One shared fill for a cache key. The cell owns only the parsed result;
/// readers keep their own file reader and projected-column path.
pub(super) struct MetadataInFlight {
    pub(super) store: Weak<dyn ObjectStore>,
    pub(super) path: Path,
    pub(super) file_size: u64,
    pub(super) page_index: PageIndexPolicy,
    pub(super) result: Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
}

pub(super) struct MetadataInFlightLease {
    store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    page_index: PageIndexPolicy,
    pub(super) result: Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
}

impl Drop for MetadataInFlightLease {
    fn drop(&mut self) {
        release_metadata_in_flight(
            &self.store,
            &self.path,
            self.file_size,
            self.page_index,
            &self.result,
        );
    }
}

#[derive(Default)]
pub(super) struct MetadataCache {
    entries: VecDeque<MetadataCacheEntry>,
    bytes: usize,
    pub(super) in_flight: Vec<MetadataInFlight>,
}

impl MetadataCache {
    fn lookup(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        file_size: u64,
        page_index: PageIndexPolicy,
    ) -> MetadataLookup {
        match self.get(store, path, file_size, page_index) {
            Some(metadata) => MetadataLookup::Cached(metadata),
            None => MetadataLookup::InFlight(self.in_flight(store, path, file_size, page_index)),
        }
    }

    fn get(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        file_size: u64,
        page_index: PageIndexPolicy,
    ) -> Option<Arc<ParquetMetaData>> {
        self.entries.retain(|entry| entry.store.strong_count() > 0);
        self.bytes = self.entries.iter().map(|entry| entry.bytes).sum();
        let capacity = cache::parsed_metadata_memory();
        while self.bytes > capacity {
            let evicted = self.entries.pop_front()?;
            self.bytes = self.bytes.saturating_sub(evicted.bytes);
            cache::auxiliary_metadata_evicted();
        }
        cache::set_auxiliary_metadata_usage(self.bytes);

        let weak = Arc::downgrade(store);
        let position = self.entries.iter().position(|entry| {
            Weak::ptr_eq(&entry.store, &weak)
                && entry.path == *path
                && entry.file_size == file_size
                && entry.page_index == page_index
        })?;
        let entry = self.entries.remove(position)?;
        let metadata = Arc::clone(&entry.metadata);
        self.entries.push_back(entry);
        Some(metadata)
    }

    fn insert(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: Path,
        file_size: u64,
        page_index: PageIndexPolicy,
        metadata: Arc<ParquetMetaData>,
    ) {
        let bytes = metadata.memory_size();
        let capacity = cache::parsed_metadata_memory();
        if capacity == 0 || bytes > capacity {
            return;
        }

        let weak = Arc::downgrade(store);
        if let Some(position) = self.entries.iter().position(|entry| {
            Weak::ptr_eq(&entry.store, &weak)
                && entry.path == path
                && entry.file_size == file_size
                && entry.page_index == page_index
        }) && let Some(replaced) = self.entries.remove(position)
        {
            self.bytes = self.bytes.saturating_sub(replaced.bytes);
        }

        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.push_back(MetadataCacheEntry {
            store: weak,
            path,
            file_size,
            page_index,
            metadata,
            bytes,
        });
        while self.bytes > capacity {
            let Some(evicted) = self.entries.pop_front() else {
                self.bytes = 0;
                break;
            };
            self.bytes = self.bytes.saturating_sub(evicted.bytes);
            cache::auxiliary_metadata_evicted();
        }
        cache::set_auxiliary_metadata_usage(self.bytes);
    }

    fn in_flight(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        file_size: u64,
        page_index: PageIndexPolicy,
    ) -> MetadataInFlightLease {
        self.in_flight
            .retain(|in_flight| in_flight.store.strong_count() > 0);
        let weak = Arc::downgrade(store);
        let result = if let Some(in_flight) = self.in_flight.iter().find(|in_flight| {
            Weak::ptr_eq(&in_flight.store, &weak)
                && in_flight.path == *path
                && in_flight.file_size == file_size
                && in_flight.page_index == page_index
        }) {
            Arc::clone(&in_flight.result)
        } else {
            let result = Arc::new(tokio::sync::OnceCell::new());
            self.in_flight.push(MetadataInFlight {
                store: weak,
                path: path.clone(),
                file_size,
                page_index,
                result: Arc::clone(&result),
            });
            result
        };
        MetadataInFlightLease {
            store: Arc::clone(store),
            path: path.clone(),
            file_size,
            page_index,
            result,
        }
    }

    fn finish_in_flight(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        file_size: u64,
        page_index: PageIndexPolicy,
        result: &Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
    ) {
        let weak = Arc::downgrade(store);
        self.in_flight.retain(|in_flight| {
            !(Weak::ptr_eq(&in_flight.store, &weak)
                && in_flight.path == *path
                && in_flight.file_size == file_size
                && in_flight.page_index == page_index
                && Arc::ptr_eq(&in_flight.result, result))
        });
    }

    fn release_in_flight(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        file_size: u64,
        page_index: PageIndexPolicy,
        result: &Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
    ) {
        if result.get().is_some() || Arc::strong_count(result) == 2 {
            self.finish_in_flight(store, path, file_size, page_index, result);
        }
    }
}

pub(super) static METADATA_CACHE: LazyLock<Mutex<MetadataCache>> =
    LazyLock::new(|| Mutex::new(MetadataCache::default()));

pub(super) fn metadata_lookup(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    file_size: u64,
    page_index: PageIndexPolicy,
) -> MetadataLookup {
    METADATA_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .lookup(store, path, file_size, page_index)
}

fn cache_metadata(
    store: &Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    page_index: PageIndexPolicy,
    metadata: Arc<ParquetMetaData>,
) {
    METADATA_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(store, path, file_size, page_index, metadata);
}

fn finish_metadata_in_flight(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    file_size: u64,
    page_index: PageIndexPolicy,
    result: &Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
) {
    METADATA_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .finish_in_flight(store, path, file_size, page_index, result);
}

fn release_metadata_in_flight(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    file_size: u64,
    page_index: PageIndexPolicy,
    result: &Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
) {
    METADATA_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .release_in_flight(store, path, file_size, page_index, result);
}

fn footer_prefetch_size(footer_size: u64, file_size: u64) -> Option<usize> {
    (footer_size > 0)
        .then_some(footer_size)
        .and_then(|size| size.checked_add(8))
        .filter(|size| *size <= file_size)
        .and_then(|size| usize::try_from(size).ok())
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let metrics = Arc::clone(&self.metrics);
        async move {
            let started = Instant::now();
            let bytes = store
                .get_range(&path, range)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))?;
            metrics.range_read(1, usize_as_u64(bytes.len()), started.elapsed());
            Ok(bytes)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let metrics = Arc::clone(&self.metrics);
        // One `get_ranges` call, so the store can coalesce adjacent chunks.
        async move {
            let started = Instant::now();
            let bytes = store
                .get_ranges(&path, &ranges)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))?;
            let total = bytes.iter().fold(0_u64, |sum, bytes| {
                sum.saturating_add(usize_as_u64(bytes.len()))
            });
            metrics.range_read(ranges.len(), total, started.elapsed());
            Ok(bytes)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let in_flight =
            match metadata_lookup(&self.store, &self.path, self.file_size, self.page_index) {
                MetadataLookup::Cached(metadata) => {
                    self.metrics.metadata_hit();
                    return async move { Ok(metadata) }.boxed();
                }
                MetadataLookup::InFlight(in_flight) => in_flight,
            };
        self.metrics.metadata_miss();

        let file_size = self.file_size;
        let prefetch_size = footer_prefetch_size(self.footer_size, file_size);
        let page_index = self.page_index;
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        async move {
            let fill_store = Arc::clone(&store);
            let fill_path = path.clone();
            let result = in_flight
                .result
                .get_or_init(|| async move {
                    let metadata = ParquetMetaDataReader::new()
                        .with_page_index_policy(page_index)
                        .with_prefetch_hint(prefetch_size)
                        .load_and_finish(&mut *self, file_size)
                        .await
                        .map(Arc::new)
                        .map_err(|error| Arc::<str>::from(error.to_string()))?;
                    cache_metadata(
                        &fill_store,
                        fill_path,
                        file_size,
                        page_index,
                        Arc::clone(&metadata),
                    );
                    Ok(metadata)
                })
                .await
                .clone();
            finish_metadata_in_flight(&store, &path, file_size, page_index, &in_flight.result);
            result.map_err(|error| ParquetError::General(error.to_string()))
        }
        .boxed()
    }
}
