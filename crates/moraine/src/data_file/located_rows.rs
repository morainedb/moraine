//! Whole rows read back at located positions: file rows at exact
//! ordinals, inline rows at chunk offsets, each batch projected onto the
//! table's logical columns and ending with its row ids.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use arrow::{
    array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array, new_null_array},
    compute::{cast, take},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use bytes::Bytes;
use futures::{
    Stream, StreamExt,
    stream::{self, BoxStream},
};
use parquet::arrow::{
    arrow_reader::ArrowReaderOptions, async_reader::ParquetRecordBatchStreamBuilder,
};

use super::{
    BUILD_READ_BATCH_ROWS, ParquetFile, ReadColumn, RowIdSource,
    columns::resolve_row_id_source,
    corrupt,
    inline_batch::decode_inline_batch,
    normalize_batch, read_projection,
    reader::ObjectStoreReader,
    schema::BatchProjection,
    selection::{ScopedRows, scoped_selection},
    usize_as_u64,
};
use crate::error::{Error, Result};

struct TimedBatches {
    stream: BoxStream<'static, Result<RecordBatch>>,
    metrics: Arc<super::ScopedReadMetrics>,
}

impl Stream for TimedBatches {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let started = Instant::now();
        let result = self.stream.as_mut().poll_next(context);
        self.metrics.decoded(started.elapsed());
        result
    }
}

/// The row-id column every located batch carries after its logical columns.
pub(crate) const ROW_ID_COLUMN: &str = "row_id";

/// The data-file column that follows the row ids; NULL for an inlined row.
pub(crate) const DATA_FILE_ID_COLUMN: &str = "data_file_id";

/// The column every located batch ends with: each row's position within
/// its data file, NULL for an inlined row.
pub(crate) const POSITION_COLUMN: &str = "position";

/// Opens a projected, position-selected reader without buffering its result.
pub(crate) async fn scoped_read_row_stream(
    file: ParquetFile,
    requested: &[usize],
    rows: ScopedRows<'_>,
    row_id_source: RowIdSource,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    if rows.is_empty() {
        return Ok(stream::empty().boxed());
    }

    let reader = ObjectStoreReader::new(&file, rows.page_index_policy());
    let options = ArrowReaderOptions::new().with_page_index_policy(rows.page_index_policy());
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .map_err(corrupt("located read"))?;
    let (scope, ordinals) = scoped_selection(rows, builder.metadata(), &file.path)?;
    let (row_id_position, row_id_start) =
        resolve_row_id_source(builder.parquet_schema(), row_id_source, &file.path)?;
    let (mask, output, row_id_output, normalization) = read_projection(
        &builder,
        file.columns.as_deref(),
        requested,
        row_id_position,
    )?;
    let decode_memory =
        crate::data_file::reader::admit_decode(builder.metadata(), &mask, scope.groups.as_deref())?;
    let builder = scope.narrow(
        builder
            .with_projection(mask)
            .with_batch_size(BUILD_READ_BATCH_ROWS),
    );
    let stream = builder.build().map_err(corrupt("located read"))?;

    let mut emitted = 0usize;
    let metrics = file.metrics.clone();
    let stream = stream
        .map(move |batch| {
            let _decode_memory = &decode_memory;
            let batch = batch.map_err(corrupt("located read"))?;
            let batch = normalize_batch(batch, normalization.as_ref())?;
            // Each emitted row's position in the file, which is also its id
            // offset when the file carries no row-id column.
            let positions = (0..batch.num_rows())
                .map(|row| ordinals.borrowed().at(emitted.saturating_add(row)))
                .collect::<Result<Vec<u64>>>()?;
            let row_ids: ArrayRef = if let Some(position) = row_id_output {
                let column = batch.columns().get(position).ok_or_else(|| {
                    Error::Corruption("located read: row-id column is out of bounds".to_owned())
                })?;
                cast(column.as_ref(), &DataType::UInt64).map_err(corrupt("located read"))?
            } else {
                Arc::new(UInt64Array::from(
                    positions
                        .iter()
                        .map(|ordinal| row_id_start.saturating_add(*ordinal))
                        .collect::<Vec<u64>>(),
                ))
            };
            emitted = emitted.saturating_add(batch.num_rows());

            let mut arrays = output
                .iter()
                .map(|&position| {
                    batch.columns().get(position).cloned().ok_or_else(|| {
                        Error::Corruption(
                            "located read: projected column is out of bounds".to_owned(),
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            arrays.push(row_ids);
            arrays.push(Arc::new(UInt64Array::from(positions)));
            let batch = unnamed_batch(arrays, batch.num_rows())?;
            crate::MemoryReservation::reserve(batch.get_array_memory_size() as u64)?
                .into_record_batch(batch)
        })
        .boxed();
    Ok(TimedBatches { stream, metrics }.boxed())
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
    arrays.push(new_null_array(&DataType::UInt64, offsets.len()));

    unnamed_batch(arrays, offsets.len())
}

/// Names a projected batch, whose last two columns are its rows' ids and
/// positions, and inserts its file identity between them without copying
/// columns.
pub(crate) fn located_batch(
    batch: &RecordBatch,
    names: &[String],
    data_file_id: Option<u64>,
) -> Result<RecordBatch> {
    if batch.num_columns() != names.len().saturating_add(2) {
        return Err(Error::Corruption(
            "located batch does not match the table's columns".to_owned(),
        ));
    }
    let rows = batch.num_rows();
    let (columns, trailing) = batch.columns().split_at(names.len());
    let mut fields: Vec<Field> = names
        .iter()
        .zip(columns)
        .map(|(name, array)| Field::new(name, array.data_type().clone(), true))
        .collect();
    fields.push(Field::new(ROW_ID_COLUMN, DataType::UInt64, false));
    fields.push(Field::new(DATA_FILE_ID_COLUMN, DataType::UInt64, true));
    fields.push(Field::new(POSITION_COLUMN, DataType::UInt64, true));

    let mut arrays = columns.to_vec();
    arrays.push(trailing[0].clone());
    arrays.push(match data_file_id {
        Some(id) => Arc::new(UInt64Array::from(vec![id; rows])) as ArrayRef,
        None => new_null_array(&DataType::UInt64, rows),
    });
    arrays.push(trailing[1].clone());

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new_with_options(
        Arc::clone(&schema),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(corrupt("located batch"))
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
