//! The row positions a DuckLake delete file marks dead, read from its
//! `pos` column alone.

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
    data_file::{ParquetFile, corrupt, reader::ObjectStoreReader, values::row_id_value},
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

/// The non-NULL positions in `batch`; a NULL is corruption.
fn delete_positions(batch: &RecordBatch) -> Result<Vec<u64>> {
    let column = batch.column(0).as_ref();
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

/// The row positions a DuckLake delete file marks dead, read from its
/// `pos` column alone.
pub(crate) async fn delete_file_positions(file: ParquetFile) -> Result<Vec<u64>> {
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
