//! What a read fetches: the projection over a file's schema covering the
//! indexed and row-id columns, and how each row's id resolves.

use object_store::path::Path;
use parquet::arrow::ProjectionMask;

use crate::{
    data_file::{IndexProjection, ROW_ID_FIELD_ID, RowIdSource},
    error::{Error, Result},
};

/// The root position of the embedded row-id column, if the file has one.
pub(super) fn embedded_row_id_position(
    schema: &parquet::schema::types::SchemaDescriptor,
) -> Option<usize> {
    schema.root_schema().get_fields().iter().position(|field| {
        let info = field.get_basic_info();
        info.has_id() && u64::try_from(info.id()).ok() == Some(ROW_ID_FIELD_ID)
    })
}

/// Resolves `source` against the file's schema into the projection
/// inputs: the row-id column's position (when it is to be read) and the
/// dense start.
pub(super) fn resolve_row_id_source(
    schema: &parquet::schema::types::SchemaDescriptor,
    source: RowIdSource,
    path: &Path,
) -> Result<(Option<usize>, u64)> {
    match (source, embedded_row_id_position(schema)) {
        (RowIdSource::Ordinal, Some(_)) => Err(Error::Constraint(format!(
            "scoped read: {path} carries an embedded row-id column and cannot be read by ordinal"
        ))),
        (RowIdSource::Ordinal, None) => Ok((None, 0)),
        (RowIdSource::Resolve { .. }, Some(position)) => Ok((Some(position), 0)),
        (
            RowIdSource::Resolve {
                row_id_start: Some(start),
            },
            None,
        ) => Ok((None, start)),
        (RowIdSource::Resolve { row_id_start: None }, None) => Err(Error::Corruption(format!(
            "scoped read: {path} is recorded as carrying per-row ids but has no embedded \
             row-id column"
        ))),
    }
}

/// The projection over `schema` covering only the indexed and row-id
/// columns, with each requested position mapped to its index in the
/// projected output batch.
pub(super) fn projection(
    schema: &parquet::schema::types::SchemaDescriptor,
    indexed_positions: &[usize],
    row_id_position: Option<usize>,
) -> Result<(ProjectionMask, Vec<usize>, Option<usize>)> {
    let mut projected: Vec<usize> = indexed_positions.to_vec();
    if let Some(position) = row_id_position {
        projected.push(position);
    }
    projected.sort_unstable();
    projected.dedup();
    let mask = ProjectionMask::roots(schema, projected.iter().copied());
    // Output-batch column index for an original file position.
    let output_index = |position: usize| {
        projected
            .iter()
            .position(|&candidate| candidate == position)
            .ok_or_else(|| Error::Corruption("scoped read: projected column vanished".to_owned()))
    };
    let indexed_output = indexed_positions
        .iter()
        .map(|&position| output_index(position))
        .collect::<Result<Vec<_>>>()?;
    let row_id_output = row_id_position.map(output_index).transpose()?;

    Ok((mask, indexed_output, row_id_output))
}

/// Unique Arrow positions needed by every index plan, sorted for stable
/// Parquet projection and cheap remapping.
pub(super) fn index_positions(projections: &[IndexProjection]) -> Vec<usize> {
    let mut positions: Vec<_> = projections
        .iter()
        .flat_map(|projection| projection.positions.iter().copied())
        .collect();
    positions.sort_unstable();
    positions.dedup();
    positions
}

/// Remaps plans from source-schema positions to columns in a projected
/// `RecordBatch`. This runs once per reader, never once per row.
pub(super) fn remap_index_projections(
    projections: Vec<IndexProjection>,
    source_positions: &[usize],
    output_positions: &[usize],
) -> Result<Vec<IndexProjection>> {
    projections
        .into_iter()
        .map(|projection| {
            let IndexProjection {
                index_id,
                unique,
                positions,
                directions,
                nulls,
                ..
            } = projection;
            let positions = positions
                .iter()
                .map(|position| {
                    let source = source_positions.binary_search(position).map_err(|_| {
                        Error::Corruption(
                            "scoped read: index column vanished from projection".to_owned(),
                        )
                    })?;
                    output_positions.get(source).copied().ok_or_else(|| {
                        Error::Corruption(
                            "scoped read: projected index column has no output".to_owned(),
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(IndexProjection {
                index_id,
                unique,
                positions,
                directions,
                nulls,
            })
        })
        .collect()
}
