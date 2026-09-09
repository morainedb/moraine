//! Whole rows read back at located positions: file rows at exact
//! ordinals, inline rows at chunk offsets, each batch projected onto the
//! table's logical columns and ending with its row ids.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array, new_null_array},
    compute::{cast, take},
    datatypes::{DataType, Field, Schema, SchemaRef},
    ipc::writer::StreamWriter,
};
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::arrow::{
    arrow_reader::ArrowReaderOptions, async_reader::ParquetRecordBatchStreamBuilder,
};

use super::{
    ParquetFile, ReadColumn, RowIdSource,
    columns::resolve_row_id_source,
    corrupt,
    inline_batch::decode_inline_batch,
    normalize_batch, read_projection,
    reader::ObjectStoreReader,
    schema::BatchProjection,
    selection::{ScopedRows, scoped_selection, total_rows},
    usize_as_u64,
};
use crate::error::{Error, Result};

/// The row-id column every located batch carries after its logical columns.
pub(crate) const ROW_ID_COLUMN: &str = "row_id";

/// The data-file column every located batch ends with; NULL for an inlined
/// row.
pub(crate) const DATA_FILE_ID_COLUMN: &str = "data_file_id";

/// Reads the logical columns at `requested` for the rows `rows` selects.
/// Each batch holds those columns in `requested` order, then a `UInt64`
/// row-id column resolved per `row_id_source`.
pub(crate) async fn scoped_read_row_batches(
    file: ParquetFile,
    requested: &[usize],
    rows: ScopedRows<'_>,
    row_id_source: RowIdSource,
) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let reader = ObjectStoreReader::new(&file, rows.page_index_policy());
    let options = ArrowReaderOptions::new().with_page_index_policy(rows.page_index_policy());
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .map_err(corrupt("located read"))?;
    let total = total_rows(builder.metadata(), &file.path)?;
    let (selection, ordinals) = scoped_selection(rows, total)?;
    let (row_id_position, row_id_start) =
        resolve_row_id_source(builder.parquet_schema(), row_id_source, &file.path)?;
    let (mask, output, row_id_output, normalization) = read_projection(
        &builder,
        file.columns.as_deref(),
        requested,
        row_id_position,
    )?;
    let mut builder = builder
        .with_projection(mask)
        .with_batch_size(file.entry_batch_rows);
    if let Some(selection) = selection {
        builder = builder.with_row_selection(selection);
    }
    let mut stream = builder.build().map_err(corrupt("located read"))?;

    let mut batches = Vec::new();
    let mut emitted = 0usize;
    while let Some(batch) = stream.try_next().await.map_err(corrupt("located read"))? {
        let batch = normalize_batch(batch, normalization.as_ref())?;
        let row_ids: ArrayRef = if let Some(position) = row_id_output {
            let column = batch.columns().get(position).ok_or_else(|| {
                Error::Corruption("located read: row-id column is out of bounds".to_owned())
            })?;
            cast(column.as_ref(), &DataType::UInt64).map_err(corrupt("located read"))?
        } else {
            let ids = (0..batch.num_rows())
                .map(|row| {
                    ordinals
                        .borrowed()
                        .at(emitted.saturating_add(row))
                        .map(|ordinal| row_id_start.saturating_add(ordinal))
                })
                .collect::<Result<Vec<u64>>>()?;
            Arc::new(UInt64Array::from(ids))
        };
        emitted = emitted.saturating_add(batch.num_rows());

        let mut arrays = output
            .iter()
            .map(|&position| {
                batch.columns().get(position).cloned().ok_or_else(|| {
                    Error::Corruption("located read: projected column is out of bounds".to_owned())
                })
            })
            .collect::<Result<Vec<_>>>()?;
        arrays.push(row_ids);
        batches.push(unnamed_batch(arrays, batch.num_rows())?);
    }

    Ok(batches)
}

/// The rows at `offsets` of one inline chunk, projected onto the logical
/// columns at `requested` and ending with `row_ids` (parallel to
/// `offsets`).
pub(crate) fn inline_rows_batch(
    schema: SchemaRef,
    body: &Bytes,
    offsets: &[u64],
    row_ids: &[u64],
    columns: &[ReadColumn],
    requested: &[usize],
) -> Result<RecordBatch> {
    let batch = decode_inline_batch(schema, body)?;
    let (projection, _) =
        BatchProjection::resolve(batch.schema().as_ref(), columns, requested, None)?;
    let projected = projection.apply(&batch)?;

    let rows = usize_as_u64(projected.num_rows());
    if offsets.iter().any(|&offset| offset >= rows) {
        return Err(Error::Corruption(
            "inline row offset is beyond its chunk".to_owned(),
        ));
    }
    let indices = UInt64Array::from(offsets.to_vec());
    let mut arrays = projected
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(corrupt("inline take")))
        .collect::<Result<Vec<_>>>()?;
    arrays.push(Arc::new(UInt64Array::from(row_ids.to_vec())));

    unnamed_batch(arrays, offsets.len())
}

/// Encodes one located batch as a self-describing Arrow IPC stream: its
/// logical columns under `names`, the row-id column, then the data-file
/// column (`data_file_id` for every row, NULL when `None`).
pub(crate) fn encode_located_batch(
    batch: &RecordBatch,
    names: &[String],
    data_file_id: Option<u64>,
) -> Result<Vec<u8>> {
    if batch.num_columns() != names.len().saturating_add(1) {
        return Err(Error::Corruption(
            "located batch does not match the table's columns".to_owned(),
        ));
    }
    let rows = batch.num_rows();
    let mut fields: Vec<Field> = names
        .iter()
        .zip(batch.columns())
        .map(|(name, array)| Field::new(name, array.data_type().clone(), true))
        .collect();
    fields.push(Field::new(ROW_ID_COLUMN, DataType::UInt64, false));
    fields.push(Field::new(DATA_FILE_ID_COLUMN, DataType::UInt64, true));

    let mut arrays = batch.columns().to_vec();
    arrays.push(match data_file_id {
        Some(id) => Arc::new(UInt64Array::from(vec![id; rows])) as ArrayRef,
        None => new_null_array(&DataType::UInt64, rows),
    });

    let schema = Arc::new(Schema::new(fields));
    let output = RecordBatch::try_new_with_options(
        Arc::clone(&schema),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(corrupt("located batch"))?;

    let mut buffer = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut buffer, &schema).map_err(corrupt("located batch"))?;
    writer.write(&output).map_err(corrupt("located batch"))?;
    writer.finish().map_err(corrupt("located batch"))?;
    Ok(buffer)
}

/// A batch over `arrays` with positional field names, for a caller that
/// names the columns once it knows the table's.
fn unnamed_batch(arrays: Vec<ArrayRef>, rows: usize) -> Result<RecordBatch> {
    let schema = Schema::new(
        arrays
            .iter()
            .enumerate()
            .map(|(index, array)| Field::new(index.to_string(), array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new_with_options(
        Arc::new(schema),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(corrupt("located batch"))
}
