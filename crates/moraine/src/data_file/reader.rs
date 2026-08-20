//! Byte-range access to one immutable Parquet object.

use std::sync::Arc;

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::Result as ParquetResult,
    file::metadata::{PageIndexPolicy, ParquetMetaData},
};

use crate::data_file::{ParquetFile, auxiliary_cache};

/// An [`AsyncFileReader`] over moraine's own object store: the footer and
/// the projected column chunks arrive as byte-range reads. Not `parquet`'s
/// built-in integration, which pins a different `object_store` major.
#[derive(Clone)]
pub(super) struct ObjectStoreReader {
    pub(super) file: ParquetFile,
    pub(super) page_index: PageIndexPolicy,
}

impl ObjectStoreReader {
    pub(super) fn new(file: &ParquetFile, page_index: PageIndexPolicy) -> Self {
        Self {
            file: file.clone(),
            page_index,
        }
    }
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
        let file = self.file.clone();
        async move { auxiliary_cache::shared().range(&file, range).await }.boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let file = self.file.clone();
        // Ranges already resident are served from the cache; the rest go
        // out as one request, so the store can coalesce adjacent chunks.
        async move { auxiliary_cache::shared().ranges(&file, ranges).await }.boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let prefetch = footer_prefetch_size(self.file.footer_size, self.file.file_size);
        async move { auxiliary_cache::shared().metadata(self, prefetch).await }.boxed()
    }
}
