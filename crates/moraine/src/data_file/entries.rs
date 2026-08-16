//! One decoded Arrow batch to entries.

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
    store::index_encoding::CanonicalKeyBuilder,
};

/// Derives one entry per row of `batch`: the values of the columns at
/// `positions` (indices into `batch`, in index-column order), and a row id
/// read from `row_id_position` when present, else `row_id_start` plus the
/// row's file ordinal. `emitted` is how many rows the read produced before
/// this batch.
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

/// Builds one row's canonical key for `projection`, whose directions and
/// null orders must pair one-to-one with its positions.
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
        .zip(&projection.directions)
        .zip(&projection.nulls)
        .try_for_each(|((&position, &direction), &nulls)| {
            let values = borrowed_array_value(batch.column(position).as_ref(), row)?;
            builder.append(values, direction, nulls)?;

            Result::Ok(())
        })?;

    let (key, unique) = builder.finish_index_entry(projection.index_id, projection.unique, row_id);
    Ok((key, unique))
}

/// Encodes every requested index key straight from one Arrow batch.
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
    let mut entries = Vec::with_capacity(batch.num_rows().saturating_mul(projections.len()));
    for row in 0..batch.num_rows() {
        let ordinal = ordinals.at(emitted.saturating_add(row))?;
        let row_id = match row_id_position {
            Some(position) => row_id_value(batch.column(position).as_ref(), row)?,
            None => row_id_start.saturating_add(ordinal),
        };

        if excluded_ordinals.is_some_and(|excluded| excluded.contains(ordinal))
            || included_row_ids.is_some_and(|included| !included.contains(&row_id))
        {
            continue;
        }

        for (index, projection) in projections.iter().enumerate() {
            let (key, unique) = build_canonical_keys(batch, projection, row, row_id)?;
            entries.push(ScopedIndexEntry {
                index,
                row_id,
                key,
                unique,
            });
        }
    }

    Ok(entries)
}
