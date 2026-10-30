//! Re-encodes surviving contiguous runs of an inline Arrow batch.

use std::ops::Range;

use arrow::{
    datatypes::SchemaRef,
    ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions},
};
use bytes::Bytes;

use super::decode_inline_batch;
use crate::{Error, Result};

/// Slices one decoded body, preserving its schema and user values.
pub(crate) fn slice_inline_batch(
    schema: SchemaRef,
    body: &Bytes,
    row_count: usize,
    runs: &[Range<usize>],
) -> Result<Vec<Bytes>> {
    let batch = decode_inline_batch(schema, body)?;
    if batch.num_rows() != row_count {
        return Err(Error::Corruption(
            "inline body row count disagrees with its chunk".into(),
        ));
    }
    let _memory = crate::MemoryReservation::reserve(
        (batch.get_array_memory_size() as u64)
            .saturating_mul(3)
            .saturating_add(65536),
    )?;
    let generator = IpcDataGenerator::default();
    let options = IpcWriteOptions::default();
    runs.iter()
        .map(|run| {
            if run.start >= run.end || run.end > row_count {
                return Err(Error::Corruption("invalid surviving inline run".into()));
            }
            let mut tracker = DictionaryTracker::new(false);
            let mut context = IpcWriteContext::default();
            let (dictionaries, encoded) = generator
                .encode(
                    &batch.slice(run.start, run.len()),
                    &mut tracker,
                    &options,
                    &mut context,
                )
                .map_err(|error| Error::Corruption(format!("encoding inline run: {error}")))?;
            if !dictionaries.is_empty() {
                return Err(Error::Corruption(
                    "inline bodies cannot carry dictionaries".into(),
                ));
            }
            let message_len = u32::try_from(encoded.ipc_message.len())
                .map_err(|_| Error::Corruption("inline run message too large".into()))?;
            let mut bytes =
                Vec::with_capacity(4 + encoded.ipc_message.len() + encoded.arrow_data.len());
            bytes.extend_from_slice(&message_len.to_le_bytes());
            bytes.extend_from_slice(&encoded.ipc_message);
            bytes.extend_from_slice(&encoded.arrow_data);
            Ok(bytes.into())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn surviving_runs_roundtrip_nullable_and_variable_width_values(
            values in prop::collection::vec((any::<Option<i64>>(), prop::option::of(".{0,80}")), 1..40),
            cut in any::<usize>(),
        ) {
            let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true), Field::new("s", DataType::Utf8, true)]));
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(values.iter().map(|v| v.0).collect::<Vec<_>>())),
                Arc::new(StringArray::from(values.iter().map(|v| v.1.as_deref()).collect::<Vec<_>>())),
            ];
            let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
            let mut tracker = DictionaryTracker::new(false);
            let (_, encoded) = IpcDataGenerator::default().encode(&batch, &mut tracker, &IpcWriteOptions::default(), &mut IpcWriteContext::default()).unwrap();
            let mut body = u32::try_from(encoded.ipc_message.len()).unwrap().to_le_bytes().to_vec();
            body.extend(encoded.ipc_message);
            body.extend(encoded.arrow_data);
            let cut = cut % values.len();
            let runs: Vec<_> = [0..cut, cut+1..values.len()].into_iter().filter(|r| !r.is_empty()).collect();
            let bodies = slice_inline_batch(schema.clone(), &body.into(), values.len(), &runs).unwrap();
            for (run, body) in runs.into_iter().zip(bodies) {
                let actual = decode_inline_batch(schema.clone(), &body).unwrap();
                prop_assert_eq!(actual, batch.slice(run.start, run.len()));
            }
        }
    }
}
