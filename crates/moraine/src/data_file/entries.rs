//! One decoded Arrow batch to entries. Row ids resolve here and excluded
//! ordinals drop here; only the finished canonical key survives the row.

use std::collections::HashSet;

use arrow::array::RecordBatch;
use bytes::Bytes;

use crate::{
    data_file::{
        IndexProjection, ScopedIndexEntry, ScopedReadEntry,
        selection::{Ordinals, RowPositions},
        values::{array_value, borrowed_array_value, row_id_value},
    },
    error::Result,
    store::index_encoding::{CanonicalKeyBuilder, Direction, NullOrder},
};

/// Derives one entry per row of `batch`: the values of the columns at
/// `positions` (direct indices into `batch`, in the index's column order),
/// and a row id read from `row_id_position` when present, else
/// `row_id_start` plus the row's file ordinal. `emitted` is how many rows
/// the read produced before this batch, which `ordinals` resolves against —
/// a selected read's rows are not at the positions it emits them in.
pub(super) fn record_batch_entries(
    batch: &RecordBatch,
    positions: &[usize],
    row_id_position: Option<usize>,
    row_id_start: u64,
    ordinals: Ordinals<'_>,
    emitted: usize,
) -> Result<Vec<ScopedReadEntry>> {
    (0..batch.num_rows())
        .map(|row| {
            let ordinal = ordinals.at(emitted.saturating_add(row))?;
            let values = positions
                .iter()
                .map(|&position| array_value(batch.column(position).as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            let row_id = match row_id_position {
                Some(position) => row_id_value(batch.column(position).as_ref(), row)?,
                None => row_id_start.saturating_add(ordinal),
            };

            Ok(ScopedReadEntry {
                ordinal,
                row_id,
                values,
            })
        })
        .collect()
}

/// Builds a canonical key from the given index key values.
fn build_canonical_keys(
    batch: &RecordBatch,
    projection: &IndexProjection,
    row: usize,
    row_id: u64,
) -> Result<(Bytes, bool)> {
    let mut builder = CanonicalKeyBuilder::new();
    projection
        .positions
        .iter()
        .enumerate()
        .try_for_each(|(column, &position)| {
            let values = borrowed_array_value(batch.column(position).as_ref(), row)?;
            let direction = projection
                .directions
                .get(column)
                .copied()
                .unwrap_or(Direction::Ascending);
            let nulls = projection
                .nulls
                .get(column)
                .copied()
                .unwrap_or(NullOrder::Last);
            builder.append(values, direction, nulls)?;

            Result::Ok(())
        })?;

    let (key, unique) = builder.finish_index_entry(projection.index_id, projection.unique, row_id);
    Ok((key, unique))
}

/// Encodes every requested index key straight from one Arrow batch. Only the
/// final canonical key survives the row: strings and blobs remain borrowed
/// while their escaped bytes are appended.
#[allow(clippy::too_many_arguments)]
pub(super) fn record_batch_index_entries(
    batch: &RecordBatch,
    projections: &[IndexProjection],
    row_id_position: Option<usize>,
    row_id_start: u64,
    ordinals: Ordinals<'_>,
    emitted: usize,
    excluded_ordinals: Option<&RowPositions>,
    included_row_ids: Option<&HashSet<u64>>,
) -> Result<Vec<ScopedIndexEntry>> {
    let entries = (0..batch.num_rows())
        .map(|row| {
            let ordinal = ordinals.at(emitted.saturating_add(row))?;
            let row_id = match row_id_position {
                Some(position) => row_id_value(batch.column(position).as_ref(), row)?,
                None => row_id_start.saturating_add(ordinal),
            };

            if excluded_ordinals.is_some_and(|excluded| excluded.contains(ordinal))
                || included_row_ids.is_some_and(|included| !included.contains(&row_id))
            {
                Ok(None)
            } else {
                let scoped_index_entries = projections
                    .iter()
                    .enumerate()
                    .map(|(index, projection)| {
                        let (key, unique) = build_canonical_keys(batch, projection, row, row_id)?;
                        Ok(ScopedIndexEntry {
                            index,
                            row_id,
                            key,
                            unique,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(Some(scoped_index_entries))
            }
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .flatten()
        .collect();

    Ok(entries)
}
