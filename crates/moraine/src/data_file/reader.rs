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
    memory: Option<Arc<crate::MemoryReservation>>,
}

impl ObjectStoreReader {
    pub(super) fn new(file: &ParquetFile, page_index: PageIndexPolicy) -> Self {
        Self {
            file: file.clone(),
            page_index,
            memory: None,
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
        async move {
            let mut memory =
                crate::MemoryReservation::reserve(self.file.footer_size.saturating_mul(8))
                    .map_err(|error| parquet::errors::ParquetError::General(error.to_string()))?;
            let metadata = auxiliary_cache::shared().metadata(self, prefetch).await?;
            memory
                .resize(memory.bytes().max(metadata.memory_size() as u64))
                .map_err(|error| parquet::errors::ParquetError::General(error.to_string()))?;
            self.memory = Some(Arc::new(memory));
            Ok(metadata)
        }
        .boxed()
    }
}

/// Reserves the largest projected row group before page decoding. Compressed
/// input ranges have separate reservations; output batches retain their own.
pub(super) fn admit_decode(
    metadata: &ParquetMetaData,
    projection: &parquet::arrow::ProjectionMask,
    groups: Option<&[usize]>,
) -> crate::Result<crate::MemoryReservation> {
    let mut largest = 0_u64;
    for (index, group) in metadata.row_groups().iter().enumerate() {
        if groups.is_some_and(|groups| !groups.contains(&index)) {
            continue;
        }
        let mut bytes = 0_u64;
        for (column, chunk) in group.columns().iter().enumerate() {
            if projection.leaf_included(column) {
                let size = u64::try_from(chunk.uncompressed_size())
                    .map_err(|_| crate::Error::Corruption("negative Parquet column size".into()))?;
                bytes = bytes.saturating_add(size);
            }
        }
        // Decoded values, validity/offset buffers, and normalization scratch.
        largest = largest.max(bytes.saturating_mul(4));
    }
    crate::MemoryReservation::reserve(largest)
}
