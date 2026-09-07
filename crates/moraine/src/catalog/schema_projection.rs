//! Resolves indexed fields into each stored schema.

use arrow::datatypes::{DataType, TimeUnit};

use crate::{
    catalog::{CatalogSnapshot, TableId},
    data_file::ReadColumn,
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        proto::{DataFileValue, MappingValue},
        read::{self, EntityRecord, EntityRecordKind, Versions},
    },
};

impl CatalogSnapshot {
    /// Logical columns in catalog order, with immutable file-name mappings.
    pub(crate) fn file_read_columns(
        &self,
        table: TableId,
        file: Option<&DataFileValue>,
    ) -> Result<Vec<ReadColumn>> {
        let mapping = file
            .and_then(|file| file.mapping_id)
            .map(|id| {
                self.mappings
                    .get(&table.get())
                    .and_then(|mappings| mappings.get(&id))
                    .ok_or_else(|| Error::Corruption(format!("no mapping {id} for table {table}")))
            })
            .transpose()?;
        self.mapped_read_columns(table, file, mapping)
    }

    /// Resolves a mapping that may still be staged by the current commit.
    pub(crate) fn mapped_read_columns(
        &self,
        table: TableId,
        file: Option<&DataFileValue>,
        mapping: Option<&MappingValue>,
    ) -> Result<Vec<ReadColumn>> {
        if let Some(mapping) = mapping
            && mapping.map_type != "map_by_name"
        {
            return Err(Error::Constraint(format!(
                "unsupported column mapping {}",
                mapping.map_type
            )));
        }
        self.columns_of(table)
            .iter()
            .map(|column| {
                let record = self
                    .columns
                    .get(&table.get())
                    .and_then(|columns| columns.get(&column.id.get()))
                    .ok_or_else(|| Error::NotFound(format!("column {}", column.id)))?;
                let mut projected = ReadColumn {
                    field_id: column.id.get(),
                    source_name: Some(column.name.clone()),
                    data_type: arrow_type(&column.column_type),
                    default: record.initial_default.clone(),
                    by_name: mapping.is_some(),
                    name_is_known: mapping.is_some()
                        || file.is_none_or(|file| record.begin_snapshot <= file.begin_snapshot),
                };
                if let Some(mapping) = mapping {
                    let source = mapping.name_mappings.iter().find(|entry| {
                        entry.target_field_id == column.id.get() && entry.parent_column.is_none()
                    });
                    projected.source_name = source.map(|source| source.source_name.clone());
                    if let Some(source) = source
                        && source.is_partition
                    {
                        let file = file.ok_or_else(|| {
                            Error::Corruption("partition mapping has no file".to_owned())
                        })?;
                        projected.default = hive_value(
                            &file.path,
                            &source.source_name,
                            projected.data_type.as_ref(),
                        )?;
                        projected.source_name = None;
                    }
                }
                Ok(projected)
            })
            .collect()
    }

    /// Resolves historical names for files without native field ids.
    pub(crate) async fn file_read_columns_at(
        &self,
        handle: ReadHandle<'_>,
        table: TableId,
        file: &DataFileValue,
    ) -> Result<Vec<ReadColumn>> {
        let columns = self.file_read_columns(table, Some(file))?;
        if file.mapping_id.is_some() {
            return Ok(columns);
        }
        self.read_column_names_at(handle, table, file.begin_snapshot, columns, false)
            .await
    }

    /// Inline schemas retain the names used when the chunk was written.
    pub(crate) async fn inline_read_columns(
        &self,
        handle: ReadHandle<'_>,
        table: TableId,
        snapshot: u64,
    ) -> Result<Vec<ReadColumn>> {
        let columns = self.file_read_columns(table, None)?;
        self.read_column_names_at(handle, table, snapshot, columns, true)
            .await
    }

    async fn read_column_names_at(
        &self,
        handle: ReadHandle<'_>,
        table: TableId,
        snapshot: u64,
        mut columns: Vec<ReadColumn>,
        by_name: bool,
    ) -> Result<Vec<ReadColumn>> {
        let changed = self.columns.get(&table.get()).is_some_and(|columns| {
            columns
                .values()
                .any(|column| column.begin_snapshot > snapshot)
        });
        if !changed {
            return Ok(columns);
        }
        let history =
            read::scan_entity_kind(handle, EntityRecordKind::Column, Versions::LiveAndEnded)
                .await?;
        let source_retained = read::read_snapshot(handle, snapshot).await?.is_some();
        for column in &mut columns {
            let historical = history.iter().find_map(|record| match record {
                EntityRecord::Column(record)
                    if record.table_id == table.get()
                        && record.column_id == column.field_id
                        && record.begin_snapshot <= snapshot
                        && record.end_snapshot.is_none_or(|end| snapshot < end) =>
                {
                    Some(record)
                }
                _ => None,
            });
            column.source_name = historical.map(|record| record.column_name.clone());
            column.name_is_known = historical.is_some() || source_retained;
            column.by_name = by_name;
        }
        Ok(columns)
    }
}

fn arrow_type(column_type: &str) -> Option<DataType> {
    Some(match column_type.to_ascii_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "INT8" | "TINYINT" => DataType::Int8,
        "INT16" | "SMALLINT" => DataType::Int16,
        "INT32" | "INTEGER" | "INT" => DataType::Int32,
        "INT64" | "BIGINT" => DataType::Int64,
        "UINT8" | "UTINYINT" => DataType::UInt8,
        "UINT16" | "USMALLINT" => DataType::UInt16,
        "UINT32" | "UINTEGER" => DataType::UInt32,
        "UINT64" | "UBIGINT" => DataType::UInt64,
        "FLOAT32" | "FLOAT" | "REAL" => DataType::Float32,
        "FLOAT64" | "DOUBLE" => DataType::Float64,
        "VARCHAR" | "STRING" | "TEXT" => DataType::Utf8,
        "DATE" => DataType::Date32,
        "TIMESTAMP" | "TIMESTAMP_US" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "TIMESTAMP_S" => DataType::Timestamp(TimeUnit::Second, None),
        "TIMESTAMP_MS" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "TIMESTAMP_NS" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        _ => return None,
    })
}

/// The virtual field in a Hive-style directory, decoded as DuckLake reads it.
fn hive_value(path: &str, name: &str, data_type: Option<&DataType>) -> Result<Option<String>> {
    let parent = path
        .rsplit_once(['/', '\\'])
        .map_or("", |(parent, _)| parent);
    let value = parent
        .split(['/', '\\'])
        .find_map(|part| {
            let (key, value) = part.split_once('=')?;
            (key.eq_ignore_ascii_case(name) && !value.contains(['=', '?', '\n'])).then_some(value)
        })
        .ok_or_else(|| {
            Error::Corruption(format!("mapped partition {name} is absent from {path}"))
        })?;
    if value.eq_ignore_ascii_case("NULL")
        || value == "__HIVE_DEFAULT_PARTITION__"
        || (value.is_empty() && data_type != Some(&DataType::Utf8))
    {
        return Ok(None);
    }
    let mut decoded = Vec::with_capacity(value.len());
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let high = bytes.next().and_then(|byte| char::from(byte).to_digit(16));
            let low = bytes.next().and_then(|byte| char::from(byte).to_digit(16));
            let (Some(high), Some(low)) = (high, low) else {
                return Err(Error::Corruption(
                    "invalid Hive partition escape".to_owned(),
                ));
            };
            decoded.push(
                u8::try_from(high * 16 + low)
                    .map_err(|_| Error::Corruption("invalid Hive partition byte".to_owned()))?,
            );
        } else {
            decoded.push(byte);
        }
    }
    String::from_utf8(decoded)
        .map(Some)
        .map_err(|error| Error::Corruption(format!("invalid Hive partition text: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hive_values_decode_escapes_and_preserve_string_null_rules() {
        assert_eq!(
            hive_value(
                "/region=east%20west+south/file.parquet",
                "region",
                Some(&DataType::Utf8)
            )
            .unwrap(),
            Some("east west+south".to_owned())
        );
        for value in ["NULL", "null", "__HIVE_DEFAULT_PARTITION__"] {
            assert_eq!(
                hive_value(
                    &format!("/region={value}/file.parquet"),
                    "region",
                    Some(&DataType::Utf8)
                )
                .unwrap(),
                None
            );
        }
        assert_eq!(
            hive_value("/region=/file.parquet", "region", Some(&DataType::Utf8)).unwrap(),
            Some(String::new())
        );
        assert_eq!(
            hive_value("/region=/file.parquet", "region", Some(&DataType::Int64)).unwrap(),
            None
        );
        for path in [
            "/region=%GG/file.parquet",
            "/region=%FF/file.parquet",
            "/region=x.parquet",
        ] {
            assert!(hive_value(path, "region", Some(&DataType::Utf8)).is_err());
        }
    }
}
