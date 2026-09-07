//! Logical columns resolved against an immutable file or chunk schema.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, RecordBatchOptions, StringArray, new_null_array},
    compute::{CastOptions, cast_with_options},
    datatypes::{DataType, Field, Schema},
};

use crate::error::{Error, Result};

/// A column's physical identity and the value to use when it is absent.
#[derive(Debug, Clone)]
pub(crate) struct ReadColumn {
    pub(crate) field_id: u64,
    pub(crate) source_name: Option<String>,
    pub(crate) data_type: Option<DataType>,
    pub(crate) default: Option<String>,
    pub(crate) by_name: bool,
    pub(crate) name_is_known: bool,
}

/// A projected column, resolved to a position or a constant.
#[derive(Clone)]
struct ResolvedColumn {
    position: Option<usize>,
    column: ReadColumn,
}

/// Converts projected source arrays into the logical columns requested.
#[derive(Clone)]
pub(super) struct BatchProjection {
    columns: Vec<ResolvedColumn>,
    row_id: Option<usize>,
}

impl BatchProjection {
    pub(super) fn resolve(
        schema: &Schema,
        columns: &[ReadColumn],
        requested: &[usize],
        row_id: Option<usize>,
    ) -> Result<(Self, Vec<usize>)> {
        for field in schema.fields() {
            if let Some(id) = field.metadata().get("PARQUET:field_id") {
                id.parse::<u64>().map_err(|_| {
                    Error::Corruption(format!("scoped read: invalid field id {id}"))
                })?;
            }
        }
        let identified = schema
            .fields()
            .iter()
            .all(|field| field.metadata().contains_key("PARQUET:field_id"));
        let mut resolved = Vec::with_capacity(requested.len());
        for &index in requested {
            let column = columns.get(index).ok_or_else(|| {
                Error::Corruption(format!(
                    "scoped read: logical column {index} is out of bounds"
                ))
            })?;
            let by_id = (!column.by_name)
                .then(|| {
                    schema.fields().iter().position(|field| {
                        field
                            .metadata()
                            .get("PARQUET:field_id")
                            .and_then(|id| id.parse::<u64>().ok())
                            == Some(column.field_id)
                    })
                })
                .flatten();
            let position = by_id.or_else(|| {
                if !column.name_is_known {
                    return None;
                }
                column.source_name.as_ref().and_then(|name| {
                    schema.fields().iter().position(|field| {
                        field.name().eq_ignore_ascii_case(name)
                            && (column.by_name
                                || !field.metadata().contains_key("PARQUET:field_id"))
                    })
                })
            });
            if position.is_none() && !column.name_is_known && (column.by_name || !identified) {
                return Err(Error::Constraint(format!(
                    "scoped read: schema history for field {} is unavailable",
                    column.field_id
                )));
            }
            resolved.push(ResolvedColumn {
                position,
                column: column.clone(),
            });
        }
        let mut positions: Vec<usize> = resolved
            .iter()
            .filter_map(|column| column.position)
            .chain(row_id)
            .collect();
        positions.sort_unstable();
        positions.dedup();
        Ok((
            Self {
                columns: resolved,
                row_id,
            },
            positions,
        ))
    }

    pub(super) fn remap(&mut self, positions: &[usize]) -> Result<()> {
        let remap = |source| {
            positions
                .binary_search(&source)
                .map_err(|_| Error::Corruption("scoped read: source column vanished".to_owned()))
        };
        for column in &mut self.columns {
            column.position = column.position.map(remap).transpose()?;
        }
        self.row_id = self.row_id.map(remap).transpose()?;
        Ok(())
    }

    pub(super) fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut arrays =
            Vec::with_capacity(self.columns.len() + usize::from(self.row_id.is_some()));
        for column in &self.columns {
            let array = if let Some(position) = column.position {
                batch.columns().get(position).cloned().ok_or_else(|| {
                    Error::Corruption("scoped read: source column is out of bounds".to_owned())
                })?
            } else {
                match (&column.column.default, &column.column.data_type) {
                    (Some(value), Some(_)) => {
                        Arc::new(StringArray::from(vec![value.as_str(); batch.num_rows()]))
                            as ArrayRef
                    }
                    (None, data_type) => new_null_array(
                        data_type.as_ref().unwrap_or(&DataType::Null),
                        batch.num_rows(),
                    ),
                    (Some(_), None) => {
                        return Err(Error::Constraint(
                            "scoped read: unsupported default type".to_owned(),
                        ));
                    }
                }
            };
            let array = match &column.column.data_type {
                Some(target) if array.data_type() != target => cast_with_options(
                    array.as_ref(),
                    target,
                    &CastOptions {
                        safe: false,
                        ..CastOptions::default()
                    },
                )
                .map_err(|error| {
                    Error::Constraint(format!(
                        "scoped read: cannot convert field {} to {target:?}: {error}",
                        column.column.field_id
                    ))
                })?,
                _ => array,
            };
            arrays.push(array);
        }
        if let Some(position) = self.row_id {
            arrays.push(batch.columns().get(position).cloned().ok_or_else(|| {
                Error::Corruption("scoped read: row-id column is out of bounds".to_owned())
            })?);
        }
        let schema = Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(index, array)| {
                    Field::new(index.to_string(), array.data_type().clone(), true)
                })
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new_with_options(
            Arc::new(schema),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|error| Error::Corruption(format!("scoped read: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int32Array, Int64Array};

    use super::*;

    fn column(id: u64, name: &str, default: Option<&str>) -> ReadColumn {
        ReadColumn {
            field_id: id,
            source_name: Some(name.to_owned()),
            data_type: Some(DataType::Int64),
            default: default.map(str::to_owned),
            by_name: false,
            name_is_known: true,
        }
    }

    #[test]
    fn field_ids_override_names_and_absent_fields_use_defaults() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("reused", DataType::Int32, false)
                .with_metadata([("PARQUET:field_id".to_owned(), "1".to_owned())].into()),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![10]))])
                .unwrap();
        let columns = [
            column(2, "reused", Some("20")),
            column(1, "renamed", None),
            column(3, "absent", None),
        ];
        let (projection, positions) =
            BatchProjection::resolve(&schema, &columns, &[0, 1, 2], None).unwrap();
        assert_eq!(positions, [0]);
        let actual = projection.apply(&batch).unwrap();
        assert_eq!(
            actual
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );
        assert_eq!(
            actual
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
        assert!(actual.column(2).is_null(0));
    }

    #[test]
    fn unavailable_history_requires_field_identity() {
        let mut column = column(1, "reused", None);
        column.name_is_known = false;
        let schema = Schema::new(vec![Field::new("reused", DataType::Int64, false)]);
        assert!(matches!(
            BatchProjection::resolve(&schema, &[column.clone()], &[0], None),
            Err(Error::Constraint(_))
        ));
        let schema = Schema::new(vec![
            Field::new("original", DataType::Int64, false)
                .with_metadata([("PARQUET:field_id".to_owned(), "1".to_owned())].into()),
        ]);
        let (_, positions) = BatchProjection::resolve(&schema, &[column], &[0], None).unwrap();
        assert_eq!(positions, [0]);
        assert!(BatchProjection::resolve(&schema, &[], &[0], None).is_err());
    }

    #[test]
    fn invalid_defaults_and_overflowing_casts_are_errors() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        for default in ["not-an-integer", "9223372036854775808"] {
            let (projection, _) =
                BatchProjection::resolve(&schema, &[column(1, "a", Some(default))], &[0], None)
                    .unwrap();
            assert!(matches!(
                projection.apply(&batch),
                Err(Error::Constraint(_))
            ));
        }
    }
}
