//! Byte-range access to one immutable Parquet object.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{PageIndexPolicy, ParquetMetaData},
};

use crate::data_file::{auxiliary_cache, metrics::ScopedReadMetrics, usize_as_u64};

/// An [`AsyncFileReader`] over moraine's own object store: the footer and
/// the projected column chunks arrive as byte-range reads. Not `parquet`'s
/// built-in integration, which pins a different `object_store` major.
#[derive(Clone)]
pub(super) struct ObjectStoreReader {
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) path: Path,
    /// The object's total length, required to locate the footer.
    pub(super) file_size: u64,
    /// The serialized Parquet metadata length recorded by DuckLake,
    /// excluding the trailing eight bytes of length and magic.
    pub(super) footer_size: u64,
    pub(super) page_index: PageIndexPolicy,
    pub(super) metrics: Arc<ScopedReadMetrics>,
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
        let prefetch = footer_prefetch_size(self.footer_size, self.file_size);
        async move { auxiliary_cache::shared().metadata(self, prefetch).await }.boxed()
    }
}
