//! The row positions a DuckLake delete file marks dead, read from its
//! `pos` column alone.

use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::Schema};
use futures::{StreamExt, TryStreamExt};
use parquet::{
    arrow::{
        ProjectionMask, arrow_reader::ArrowReaderOptions,
        async_reader::ParquetRecordBatchStreamBuilder,
    },
    file::metadata::PageIndexPolicy,
};

use crate::{
    data_file::{
        ParquetFile, auxiliary_cache, corrupt, reader::ObjectStoreReader, row_set::FileRowSet,
        values::row_id_value,
    },
    error::{Error, Result},
};

/// The position of a delete file's `pos` column.
fn delete_position(schema: &Schema) -> Result<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == "pos")
        .ok_or_else(|| Error::Corruption("delete file has no `pos` column".to_owned()))
}

/// The non-NULL positions in `batch`'s first column; a NULL is corruption.
fn delete_positions(batch: &RecordBatch) -> Result<Vec<u64>> {
    delete_positions_in(batch, 0)
}

/// The non-NULL positions in `batch`'s column `index`; a NULL is corruption.
fn delete_positions_in(batch: &RecordBatch, index: usize) -> Result<Vec<u64>> {
    let column = batch.column(index).as_ref();
    (0..batch.num_rows())
        .map(|row| {
            if column.is_null(row) {
                return Err(Error::Corruption(
                    "delete file has a NULL position".to_owned(),
                ));
            }

            row_id_value(column, row)
        })
        .collect::<Result<Vec<_>>>()
}

/// The position of a delete file's embedded per-position snapshot column,
/// which a delete file replacing an earlier one carries.
fn delete_snapshot_position(schema: &Schema) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == "_ducklake_internal_snapshot_id")
}

/// The row positions `file` marks dead as of `visible_at`, ascending and
/// duplicate-free: every position of a plain delete file, and of one
/// embedding per-position snapshots only those deleted at or before
/// `visible_at`.
pub(crate) async fn delete_file_positions_at(
    file: ParquetFile,
    visible_at: u64,
) -> Result<Vec<u64>> {
    file.metrics.parquet_file();
    let reader = ObjectStoreReader::new(&file, PageIndexPolicy::Skip);
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .map_err(corrupt("delete-file read"))?;
    let position = delete_position(builder.schema().as_ref())?;
    let snapshot = delete_snapshot_position(builder.schema().as_ref());
    let mut projected: Vec<usize> = std::iter::once(position).chain(snapshot).collect();
    projected.sort_unstable();
    let position_index = projected.binary_search(&position).unwrap_or_default();
    let snapshot_index = snapshot.and_then(|column| projected.binary_search(&column).ok());
    let mask = ProjectionMask::roots(builder.parquet_schema(), projected);
    let delete_file_stream = builder
        .with_projection(mask)
        .build()
        .map_err(corrupt("delete-file read"))?;

    let mut positions = delete_file_stream
        .map(|batch| {
            let batch = batch.map_err(corrupt("delete-file read"))?;
            let positions = delete_positions_in(&batch, position_index)?;
            let Some(snapshot_index) = snapshot_index else {
                return Ok(positions);
            };
            let snapshots = batch.column(snapshot_index).as_ref();
            positions
                .into_iter()
                .enumerate()
                .filter_map(|(row, position)| {
                    if snapshots.is_null(row) {
                        return Some(Ok(position));
                    }
                    match row_id_value(snapshots, row) {
                        Ok(deleted_at) if deleted_at <= visible_at => Some(Ok(position)),
                        Ok(_) => None,
                        Err(error) => Some(Err(error)),
                    }
                })
                .collect::<Result<Vec<_>>>()
        })
        .try_concat()
        .await?;
    positions.sort_unstable();
    positions.dedup();

    Ok(positions)
}

/// The row positions a DuckLake delete file marks dead, ascending and
/// duplicate-free.
///
/// Decoded once per object: a delete file is immutable, and every later
/// delete against its target reads it again to subtract what was already
/// dead.
pub(crate) async fn delete_file_positions(file: ParquetFile) -> Result<Vec<u64>> {
    let positions = auxiliary_cache::shared()
        .delete_positions(&file, || decode_delete_file_positions(file.clone()))
        .await?;
    Ok(positions.to_sorted_vec())
}

/// Reads and sorts one delete file's `pos` column.
async fn decode_delete_file_positions(file: ParquetFile) -> Result<Arc<FileRowSet>> {
    file.metrics.parquet_file();
    let mut positions = read_delete_file_positions(file).await?;
    positions.sort_unstable();
    positions.dedup();
    FileRowSet::from_sorted(positions).map(Arc::new)
}

async fn read_delete_file_positions(file: ParquetFile) -> Result<Vec<u64>> {
    let reader = ObjectStoreReader::new(&file, PageIndexPolicy::Skip);
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .map_err(corrupt("delete-file read"))?;
    let position = delete_position(builder.schema().as_ref())?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), [position]);
    let delete_file_stream = builder
        .with_projection(mask)
        .build()
        .map_err(corrupt("delete-file read"))?;

    let positions = delete_file_stream
        .map(|batch| {
            let batch = batch.map_err(corrupt("delete-file read"))?;
            delete_positions(&batch)
        })
        .try_concat()
        .await?;

    Ok(positions)
}
