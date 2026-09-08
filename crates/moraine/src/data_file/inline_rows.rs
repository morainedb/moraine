//! One decoded inline chunk with entries derived on demand.

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use bytes::Bytes;

use super::{ReadColumn, ScopedReadEntry, inline_batch::decode_inline_batch, values::array_value};
use crate::error::{Error, Result};

pub(crate) struct InlineRows {
    batch: RecordBatch,
    position: usize,
    row_id_start: u64,
}

impl InlineRows {
    pub(crate) fn new(
        schema: SchemaRef,
        body: &Bytes,
        positions: &[usize],
        row_id_start: u64,
        start: u64,
        columns: &[ReadColumn],
    ) -> Result<Self> {
        let batch = decode_inline_batch(schema, body)?;
        let (projection, _) = super::schema::BatchProjection::resolve(
            batch.schema().as_ref(),
            columns,
            positions,
            None,
        )?;
        let position = usize::try_from(start)
            .map_err(|_| Error::Corruption("inline cursor exceeds addressable rows".into()))?;
        if position > batch.num_rows() {
            return Err(Error::Corruption("inline cursor exceeds its chunk".into()));
        }
        Ok(Self {
            batch: projection.apply(&batch)?,
            position,
            row_id_start,
        })
    }

    pub(crate) fn row_count(&self) -> usize {
        self.batch.num_rows()
    }

    pub(crate) fn decoded_bytes(&self) -> usize {
        self.batch.get_array_memory_size()
    }

    pub(crate) fn next(&mut self) -> Result<Option<ScopedReadEntry>> {
        if self.position == self.batch.num_rows() {
            return Ok(None);
        }
        let row = self.position;
        let ordinal = u64::try_from(row)
            .map_err(|_| Error::Corruption("inline row position overflow".into()))?;
        let row_id = self
            .row_id_start
            .checked_add(ordinal)
            .ok_or_else(|| Error::Corruption("inline row id overflow".into()))?;
        let values = self
            .batch
            .columns()
            .iter()
            .map(|column| array_value(column.as_ref(), row))
            .collect::<Result<_>>()?;
        self.position += 1;
        Ok(Some(ScopedReadEntry {
            ordinal,
            row_id,
            values,
        }))
    }
}
