//! Reading DuckLake's data files.
//!
//! A scoped read derives index entries from a registered Parquet file by
//! fetching only the indexed columns, the row positions, and — when the file
//! carries one — the row-id column. A bounded, merge-free projection, not
//! the scan path the no-Parquet-read rule guards.
//!
//! This is the only layer that touches Parquet. [`crate::store`] reads the
//! catalog's own keyspace out of SlateDB, and [`crate::catalog`] interprets
//! what either returns.

mod auxiliary_cache;
mod columns;
mod delete_file;
mod entries;
mod inline_batch;
mod metrics;
mod reader;
mod row_location;
mod row_set;
mod selection;
mod values;

#[cfg(test)]
mod auxiliary_cache_tests;
#[cfg(test)]
mod row_set_tests;
#[cfg(test)]
mod tests;

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use object_store::{ObjectStore, path::Path};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::ParquetRecordBatchStreamBuilder},
    file::metadata::PageIndexPolicy,
};

#[cfg(test)]
pub(crate) use crate::data_file::inline_batch::{
    inline_batch_decode_count, inline_schema_decode_count,
};
pub(crate) use crate::data_file::{
    auxiliary_cache::{
        occupancy as auxiliary_occupancy, resize as resize_auxiliary, row_summary_occupancy,
    },
    delete_file::delete_file_positions,
    inline_batch::{decode_inline_schema, inline_batch_entries, inline_batch_index_entries},
    metrics::{DataStoreCounters, ScopedReadMetrics, ScopedReadTally, run_bounded_index_encoding},
    row_location::{FileSummary, file_summary},
    selection::{RowPositions, ScopedRows},
};
use crate::{
    data_file::{
        columns::{
            embedded_row_id_position, index_positions, projection, remap_index_projections,
            resolve_row_id_source,
        },
        entries::{record_batch_entries, record_batch_index_entries},
        metrics::INDEX_ENCODING_CONCURRENCY,
        reader::ObjectStoreReader,
        selection::{scoped_selection, total_rows},
    },
    error::{Error, Result},
    store::index_encoding::{Direction, IndexKeyValue, NullOrder},
};

/// DuckDB's reserved Parquet field id for `_ducklake_internal_row_id`.
pub(crate) const ROW_ID_FIELD_ID: u64 = 2_147_483_540;

/// One entry derived from a registered file: the row id and the canonical
/// values of the indexed columns, in the index's column order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ScopedReadEntry {
    /// Physical position of this row in its Parquet file.
    pub(crate) ordinal: u64,
    /// The row this entry points at.
    pub(crate) row_id: u64,
    /// The indexed column values; a `None` is SQL NULL (stored multi-shaped).
    pub(crate) values: Vec<Option<IndexKeyValue>>,
}

/// One index's projection over an Arrow batch. Positions name Arrow columns
/// in index-column order; ordering vectors run in parallel to them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexProjection {
    /// Persistent id used in the physical entry prefix.
    pub(crate) index_id: u64,
    /// Whether non-NULL values use the unique entry shape.
    pub(crate) unique: bool,
    /// Arrow column positions in index-column order.
    pub(crate) positions: Vec<usize>,
    /// Per-column sort directions.
    pub(crate) directions: Vec<Direction>,
    /// Per-column NULL placement.
    pub(crate) nulls: Vec<NullOrder>,
}

/// A final physical index key derived directly from an Arrow row.
pub(crate) struct ScopedIndexEntry {
    /// Position of the owning index in the supplied projection plans.
    pub(crate) index: usize,
    /// Stable row id carried by the entry.
    pub(crate) row_id: u64,
    /// Fully encoded SlateDB entry key.
    pub(crate) key: Bytes,
    /// Whether the key uses the unique physical shape.
    pub(crate) unique: bool,
}

/// How a scoped read resolves each row's id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowIdSource {
    /// The embedded row-id column when the file carries one (even if a dense
    /// start is recorded), else `start + ordinal`, else refusal.
    Resolve {
        /// The catalog row's dense start, if it records one.
        row_id_start: Option<u64>,
    },
    /// Row ids are file ordinals from 0; a file carrying the embedded
    /// column is refused.
    Ordinal,
}

/// A `map_err` builder turning any displayable reader error into
/// `Error::Corruption("{what}: {err}")`.
fn corrupt<E: std::fmt::Display>(what: &'static str) -> impl Fn(E) -> Error {
    move |err| Error::Corruption(format!("{what}: {err}"))
}

/// A `usize` row count or offset as a `u64`.
fn usize_as_u64(value: usize) -> u64 {
    // `usize` is at most 64 bits on every supported target.
    #[allow(clippy::expect_used)]
    u64::try_from(value).expect("usize fits in u64 on supported targets")
}

/// Reads entries from one recorded Parquet object.
pub(crate) async fn scoped_read_recorded_entries(
    file: ParquetFile,
    indexed_positions: &[usize],
    rows: ScopedRows<'_>,
    row_id_source: RowIdSource,
) -> Result<Vec<ScopedReadEntry>> {
    scoped_read_entry_batches(file, indexed_positions, rows, row_id_source)
        .await?
        .try_fold(Vec::new(), |mut entries, batch| async move {
            entries.extend(batch);
            Ok(entries)
        })
        .await
}

/// One immutable Parquet object's location and recorded sizes.
#[derive(Clone)]
pub(crate) struct ParquetFile {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    footer_size: u64,
    metrics: Arc<ScopedReadMetrics>,
}

impl ParquetFile {
    /// Describes one object using DuckLake's recorded sizes.
    pub(crate) fn new(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        file_size: u64,
        footer_size: u64,
    ) -> Self {
        Self {
            object_store,
            path,
            file_size,
            footer_size,
            metrics: Arc::new(ScopedReadMetrics::default()),
        }
    }

    /// Records this file's reads in the supplied commit-wide tally.
    pub(crate) fn with_metrics(mut self, metrics: Arc<ScopedReadMetrics>) -> Self {
        self.metrics = metrics;
        self
    }
}

/// Whether `file` carries the reserved embedded row-id column.
pub(crate) async fn carries_embedded_row_ids(file: ParquetFile) -> Result<bool> {
    let reader = ObjectStoreReader {
        store: file.object_store,
        path: file.path,
        file_size: file.file_size,
        footer_size: file.footer_size,
        page_index: PageIndexPolicy::Skip,
        metrics: file.metrics,
    };
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .map_err(corrupt("row-id probe"))?;

    Ok(embedded_row_id_position(builder.parquet_schema()).is_some())
}

/// Rows decoded at once by a streamed scoped read. A staged build step may
/// be smaller; its caller splits this group before staging.
const BUILD_READ_BATCH_ROWS: usize = 8_192;

/// Streams a file's fused index entries in bounded Arrow batches. Shared
/// columns are projected once and borrowed directly into their final keys.
pub(crate) async fn scoped_read_index_entry_batches(
    file: ParquetFile,
    projections: Vec<IndexProjection>,
    rows: ScopedRows<'_>,
    row_id_source: RowIdSource,
    excluded_ordinals: Option<&RowPositions>,
) -> Result<BoxStream<'static, Result<Vec<ScopedIndexEntry>>>> {
    if rows.is_empty() || projections.is_empty() {
        return Ok(stream::empty().boxed());
    }

    let file_size = file.file_size;
    file.metrics.parquet_file();
    let source_positions = index_positions(&projections);
    let excluded_ordinals = excluded_ordinals.cloned();

    let reader = ObjectStoreReader {
        store: file.object_store,
        path: file.path.clone(),
        file_size,
        footer_size: file.footer_size,
        page_index: rows.page_index_policy(),
        metrics: Arc::clone(&file.metrics),
    };
    let options = ArrowReaderOptions::new().with_page_index_policy(rows.page_index_policy());
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .map_err(corrupt("scoped read"))?;
    let (row_id_position, row_id_start) =
        resolve_row_id_source(builder.parquet_schema(), row_id_source, &file.path)?;
    let (mask, indexed_output, row_id_output) =
        projection(builder.parquet_schema(), &source_positions, row_id_position)?;
    let projections = Arc::new(remap_index_projections(
        projections,
        &source_positions,
        &indexed_output,
    )?);
    let total = total_rows(builder.metadata(), &file.path)?;
    let (selection, ordinals) = scoped_selection(rows, total)?;
    let ordinals = Arc::new(ordinals);
    let mut builder = builder
        .with_projection(mask)
        .with_batch_size(BUILD_READ_BATCH_ROWS);
    if let Some(selection) = selection {
        builder = builder.with_row_selection(selection);
    }
    let arrow_reader = builder.build().map_err(corrupt("scoped read"))?;
    let mut emitted = 0usize;

    let metrics = Arc::clone(&file.metrics);
    Ok(arrow_reader
        .map(move |batch| {
            let batch = batch.map_err(corrupt("scoped read"))?;
            file.metrics.arrow_batch();
            let batch_start = emitted;
            emitted = emitted.saturating_add(batch.num_rows());
            Result::Ok((batch, batch_start))
        })
        .map(move |prepared| {
            let projections = Arc::clone(&projections);
            let ordinals = Arc::clone(&ordinals);
            let excluded_ordinals = excluded_ordinals.clone();
            let metrics = Arc::clone(&metrics);
            async move {
                let (batch, batch_start) = prepared?;
                run_bounded_index_encoding(move || {
                    let started = Instant::now();
                    let result = record_batch_index_entries(
                        &batch,
                        &projections,
                        row_id_output,
                        row_id_start,
                        ordinals.borrowed(),
                        batch_start,
                        excluded_ordinals.as_ref(),
                        None,
                    );
                    metrics.encoded(started.elapsed());
                    result
                })
                .await
            }
        })
        .buffered(INDEX_ENCODING_CONCURRENCY)
        .boxed())
}

/// Streams a file's projected index entries in bounded Arrow batches.
pub(crate) async fn scoped_read_entry_batches(
    file: ParquetFile,
    indexed_positions: &[usize],
    rows: ScopedRows<'_>,
    row_id_source: RowIdSource,
) -> Result<BoxStream<'static, Result<Vec<ScopedReadEntry>>>> {
    if rows.is_empty() {
        return Ok(stream::empty().boxed());
    }

    let ParquetFile {
        object_store,
        path,
        file_size,
        footer_size,
        metrics,
    } = file;

    let reader = ObjectStoreReader {
        store: object_store,
        path: path.clone(),
        file_size,
        footer_size,
        page_index: rows.page_index_policy(),
        metrics,
    };
    let options = ArrowReaderOptions::new().with_page_index_policy(rows.page_index_policy());
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .map_err(corrupt("scoped read"))?;
    let total = total_rows(builder.metadata(), &path)?;
    let (selection, ordinals) = scoped_selection(rows, total)?;

    let (row_id_position, row_id_start) =
        resolve_row_id_source(builder.parquet_schema(), row_id_source, &path)?;
    let (mask, indexed_output, row_id_output) =
        projection(builder.parquet_schema(), indexed_positions, row_id_position)?;
    let mut builder = builder
        .with_projection(mask)
        .with_batch_size(BUILD_READ_BATCH_ROWS);
    if let Some(selection) = selection {
        builder = builder.with_row_selection(selection);
    }
    let arrow_reader = builder.build().map_err(corrupt("scoped read"))?;

    let mut emitted = 0usize;
    let selection_stream = arrow_reader.map(move |batch| {
        let batch = batch.map_err(corrupt("scoped read"))?;
        let batch_start = emitted;
        emitted = emitted.saturating_add(batch.num_rows());

        record_batch_entries(
            &batch,
            &indexed_output,
            row_id_output,
            row_id_start,
            ordinals.borrowed(),
            batch_start,
        )
    });

    Ok(selection_stream.boxed())
}
