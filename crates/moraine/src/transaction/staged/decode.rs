//! Row decoding: `Cursor` over a staged row's cells, and the per-kind
//! decoders turning DuckLake's rows into wire values and keys.

use super::{CatalogSnapshot, Cell, EntityKey, Error, Result, TableKind, corrupt_row, proto};

/// A positional reader over one staged row's cells, typed cell-by-cell
/// against the column each `ducklake_*` table declares.
pub(super) struct Cursor<'a> {
    table: TableKind,
    items: std::slice::Iter<'a, Cell>,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(table: TableKind, cells: &'a [Cell]) -> Self {
        Self {
            table,
            items: cells.iter(),
        }
    }

    pub(super) fn next(&mut self) -> Result<&'a Cell> {
        self.items
            .next()
            .ok_or_else(|| corrupt_row(self.table, "too few cells"))
    }

    pub(super) fn u64(&mut self) -> Result<u64> {
        match self.next()? {
            Cell::U64(v) => Ok(*v),
            other => Err(corrupt_row(
                self.table,
                format!("expected u64, got {other:?}"),
            )),
        }
    }

    pub(super) fn opt_u64(&mut self) -> Result<Option<u64>> {
        match self.next()? {
            Cell::Null => Ok(None),
            Cell::U64(v) => Ok(Some(*v)),
            other => Err(corrupt_row(
                self.table,
                format!("expected optional u64, got {other:?}"),
            )),
        }
    }

    pub(super) fn i64(&mut self) -> Result<i64> {
        match self.next()? {
            Cell::I64(v) => Ok(*v),
            other => Err(corrupt_row(
                self.table,
                format!("expected i64, got {other:?}"),
            )),
        }
    }

    pub(super) fn bool(&mut self) -> Result<bool> {
        match self.next()? {
            Cell::Bool(v) => Ok(*v),
            other => Err(corrupt_row(
                self.table,
                format!("expected bool, got {other:?}"),
            )),
        }
    }

    pub(super) fn opt_bool(&mut self) -> Result<Option<bool>> {
        match self.next()? {
            Cell::Null => Ok(None),
            Cell::Bool(v) => Ok(Some(*v)),
            other => Err(corrupt_row(
                self.table,
                format!("expected optional bool, got {other:?}"),
            )),
        }
    }

    pub(super) fn string(&mut self) -> Result<String> {
        match self.next()? {
            Cell::Str(v) => Ok(v.clone()),
            other => Err(corrupt_row(
                self.table,
                format!("expected string, got {other:?}"),
            )),
        }
    }

    pub(super) fn opt_string(&mut self) -> Result<Option<String>> {
        match self.next()? {
            Cell::Null => Ok(None),
            Cell::Str(v) => Ok(Some(v.clone())),
            other => Err(corrupt_row(
                self.table,
                format!("expected optional string, got {other:?}"),
            )),
        }
    }

    /// Confirms every cell was consumed.
    pub(super) fn finish(mut self) -> Result<()> {
        if self.items.next().is_some() {
            Err(corrupt_row(self.table, "too many cells"))
        } else {
            Ok(())
        }
    }
}

pub(super) fn decode_schema(cells: &[Cell]) -> Result<proto::SchemaValue> {
    let mut c = Cursor::new(TableKind::Schema, cells);
    let value = proto::SchemaValue {
        schema_id: c.u64()?,
        schema_uuid: c.string()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        schema_name: c.string()?,
        path: c.string()?,
        path_is_relative: c.bool()?,
    };
    c.finish()?;
    Ok(value)
}

/// `ducklake_table`'s row shape, minus `next_column_id` — moraine-internal
/// bookkeeping DuckLake never authors (see [`table_value`]).
pub(super) struct TableCells {
    pub(super) table_id: u64,
    pub(super) table_uuid: String,
    pub(super) begin_snapshot: u64,
    pub(super) end_snapshot: Option<u64>,
    pub(super) schema_id: u64,
    pub(super) table_name: String,
    pub(super) path: String,
    pub(super) path_is_relative: bool,
}

pub(super) fn decode_table(cells: &[Cell]) -> Result<TableCells> {
    let mut c = Cursor::new(TableKind::Table, cells);
    let value = TableCells {
        table_id: c.u64()?,
        table_uuid: c.string()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        schema_id: c.u64()?,
        table_name: c.string()?,
        path: c.string()?,
        path_is_relative: c.bool()?,
    };
    c.finish()?;
    Ok(value)
}

/// Builds the full `TableValue`, carrying `next_column_id` forward from
/// the table's prior version in `base` (floor of 1 for a new id).
pub(super) fn table_value(base: &CatalogSnapshot, cells: TableCells) -> proto::TableValue {
    let next_column_id = base
        .tables
        .get(&cells.table_id)
        .map_or(1, |t| t.next_column_id);
    proto::TableValue {
        table_id: cells.table_id,
        table_uuid: cells.table_uuid,
        begin_snapshot: cells.begin_snapshot,
        end_snapshot: cells.end_snapshot,
        schema_id: cells.schema_id,
        table_name: cells.table_name,
        path: cells.path,
        path_is_relative: cells.path_is_relative,
        next_column_id,
    }
}

pub(super) fn decode_view(cells: &[Cell]) -> Result<proto::ViewValue> {
    let mut c = Cursor::new(TableKind::View, cells);
    let value = proto::ViewValue {
        view_id: c.u64()?,
        view_uuid: c.string()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        schema_id: c.u64()?,
        view_name: c.string()?,
        dialect: c.string()?,
        sql: c.string()?,
        column_aliases: c.opt_string()?,
    };
    c.finish()?;
    Ok(value)
}

pub(super) fn decode_column(cells: &[Cell]) -> Result<proto::ColumnValue> {
    let mut c = Cursor::new(TableKind::Column, cells);
    let value = proto::ColumnValue {
        column_id: c.u64()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        table_id: c.u64()?,
        column_order: c.u64()?,
        column_name: c.string()?,
        column_type: c.string()?,
        initial_default: c.opt_string()?,
        default_value: c.opt_string()?,
        nulls_allowed: c.bool()?,
        parent_column: c.opt_u64()?,
        default_value_type: c.opt_string()?,
        default_value_dialect: c.opt_string()?,
        tags: Vec::new(),
    };
    c.finish()?;
    Ok(value)
}

pub(super) fn decode_data_file(cells: &[Cell]) -> Result<proto::DataFileValue> {
    let mut c = Cursor::new(TableKind::DataFile, cells);
    let value = proto::DataFileValue {
        data_file_id: c.u64()?,
        table_id: c.u64()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        file_order: c.opt_u64()?,
        path: c.string()?,
        path_is_relative: c.bool()?,
        file_format: c.string()?,
        record_count: c.u64()?,
        file_size_bytes: c.u64()?,
        footer_size: c.u64()?,
        row_id_start: c.opt_u64()?,
        partition_id: c.opt_u64()?,
        encryption_key: c.opt_string()?,
        mapping_id: c.opt_u64()?,
        partial_max: c.opt_u64()?,
        partition_values: Vec::new(),
    };
    c.finish()?;
    Ok(value)
}

pub(super) fn decode_delete_file(cells: &[Cell]) -> Result<proto::DeleteFileValue> {
    let mut c = Cursor::new(TableKind::DeleteFile, cells);
    let value = proto::DeleteFileValue {
        delete_file_id: c.u64()?,
        table_id: c.u64()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        data_file_id: c.u64()?,
        path: c.string()?,
        path_is_relative: c.bool()?,
        format: c.string()?,
        delete_count: c.u64()?,
        file_size_bytes: c.u64()?,
        footer_size: c.u64()?,
        encryption_key: c.opt_string()?,
        partial_max: c.opt_u64()?,
    };
    c.finish()?;
    Ok(value)
}

pub(super) fn decode_table_stats(cells: &[Cell]) -> Result<proto::TableStatsValue> {
    let mut c = Cursor::new(TableKind::TableStats, cells);
    let value = proto::TableStatsValue {
        table_id: c.u64()?,
        record_count: c.u64()?,
        next_row_id: c.u64()?,
        file_size_bytes: c.u64()?,
    };
    c.finish()?;
    Ok(value)
}

pub(super) fn decode_table_column_stats(cells: &[Cell]) -> Result<proto::TableColumnStatsValue> {
    let mut c = Cursor::new(TableKind::TableColumnStats, cells);
    let value = proto::TableColumnStatsValue {
        table_id: c.u64()?,
        column_id: c.u64()?,
        contains_null: c.opt_bool()?,
        contains_nan: c.opt_bool()?,
        min_value: c.opt_string()?,
        max_value: c.opt_string()?,
        extra_stats: c.opt_string()?,
    };
    c.finish()?;
    Ok(value)
}

pub(super) fn decode_file_column_stats(cells: &[Cell]) -> Result<proto::FileColumnStatsValue> {
    let mut c = Cursor::new(TableKind::FileColumnStats, cells);
    let value = proto::FileColumnStatsValue {
        data_file_id: c.u64()?,
        table_id: c.u64()?,
        column_id: c.u64()?,
        column_size_bytes: c.u64()?,
        value_count: c.u64()?,
        null_count: c.u64()?,
        min_value: c.opt_string()?,
        max_value: c.opt_string()?,
        contains_nan: c.opt_bool()?,
        extra_stats: c.opt_string()?,
        variant_stats: Vec::new(),
    };
    c.finish()?;
    Ok(value)
}

pub(super) fn decode_partition_info(cells: &[Cell]) -> Result<proto::PartitionValue> {
    let mut c = Cursor::new(TableKind::PartitionInfo, cells);
    let value = proto::PartitionValue {
        partition_id: c.u64()?,
        table_id: c.u64()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        columns: Vec::new(),
    };
    c.finish()?;
    Ok(value)
}

pub(super) fn decode_partition_column(cells: &[Cell]) -> Result<(u64, proto::PartitionColumn)> {
    let mut c = Cursor::new(TableKind::PartitionColumn, cells);
    let partition_id = c.u64()?;
    let _table_id = c.u64()?;
    let column = proto::PartitionColumn {
        partition_key_index: c.u64()?,
        column_id: c.u64()?,
        transform: c.string()?,
    };
    c.finish()?;
    Ok((partition_id, column))
}

pub(super) fn decode_file_partition_value(
    cells: &[Cell],
) -> Result<((u64, u64), proto::FilePartitionValue)> {
    let mut c = Cursor::new(TableKind::FilePartitionValue, cells);
    let data_file_id = c.u64()?;
    let table_id = c.u64()?;
    let value = proto::FilePartitionValue {
        partition_key_index: c.u64()?,
        partition_value: c.string()?,
    };
    c.finish()?;
    Ok(((table_id, data_file_id), value))
}

pub(super) fn decode_sort_info(cells: &[Cell]) -> Result<proto::SortValue> {
    let mut c = Cursor::new(TableKind::SortInfo, cells);
    let value = proto::SortValue {
        sort_id: c.u64()?,
        table_id: c.u64()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        expressions: Vec::new(),
    };
    c.finish()?;
    Ok(value)
}

pub(super) fn decode_tag_row(cells: &[Cell]) -> Result<(u64, proto::TagEntry)> {
    let mut c = Cursor::new(TableKind::Tag, cells);
    let object_id = c.u64()?;
    let entry = proto::TagEntry {
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        key: c.string()?,
        value: c.string()?,
    };
    c.finish()?;

    Ok((object_id, entry))
}

pub(super) fn decode_column_tag_row(cells: &[Cell]) -> Result<((u64, u64), proto::ColumnTag)> {
    let mut c = Cursor::new(TableKind::ColumnTag, cells);
    let table_id = c.u64()?;
    let column_id = c.u64()?;
    let tag = proto::ColumnTag {
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        key: c.string()?,
        value: c.string()?,
    };
    c.finish()?;

    Ok(((table_id, column_id), tag))
}

pub(super) fn decode_gc_file_row(cells: &[Cell]) -> Result<proto::GcFileValue> {
    let mut c = Cursor::new(TableKind::FilesScheduledForDeletion, cells);
    let value = proto::GcFileValue {
        data_file_id: c.u64()?,
        path: c.string()?,
        path_is_relative: c.bool()?,
        schedule_start_micros: c.i64()?,
    };
    c.finish()?;

    Ok(value)
}

pub(super) fn decode_sort_expression(cells: &[Cell]) -> Result<(u64, proto::SortExpression)> {
    let mut c = Cursor::new(TableKind::SortExpression, cells);
    let sort_id = c.u64()?;
    let _table_id = c.u64()?;
    let expression = proto::SortExpression {
        sort_key_index: c.u64()?,
        expression: c.string()?,
        dialect: c.string()?,
        sort_direction: c.string()?,
        null_order: c.string()?,
    };
    c.finish()?;
    Ok((sort_id, expression))
}

pub(super) fn decode_macro(cells: &[Cell]) -> Result<proto::MacroValue> {
    let mut c = Cursor::new(TableKind::Macro, cells);
    let schema_id = c.u64()?;
    let macro_id = c.u64()?;
    let macro_name = c.string()?;
    let begin_snapshot = c.u64()?;
    let end_snapshot = c.opt_u64()?;
    c.finish()?;

    Ok(proto::MacroValue {
        macro_id,
        begin_snapshot,
        end_snapshot,
        schema_id,
        macro_name,
        implementations: Vec::new(),
    })
}

pub(super) fn decode_macro_impl(cells: &[Cell]) -> Result<(u64, proto::MacroImplementation)> {
    let mut c = Cursor::new(TableKind::MacroImpl, cells);
    let macro_id = c.u64()?;
    let implementation = proto::MacroImplementation {
        impl_id: c.u64()?,
        dialect: c.string()?,
        sql: c.string()?,
        macro_type: c.string()?,
        parameters: Vec::new(),
    };
    c.finish()?;

    Ok((macro_id, implementation))
}

pub(super) fn decode_macro_parameter(
    cells: &[Cell],
) -> Result<((u64, u64), proto::MacroParameter)> {
    let mut c = Cursor::new(TableKind::MacroParameters, cells);
    let macro_id = c.u64()?;
    let impl_id = c.u64()?;
    let parameter = proto::MacroParameter {
        column_id: c.u64()?,
        parameter_name: c.string()?,
        parameter_type: c.string()?,
        default_value: c.opt_string()?,
        default_value_type: c.string()?,
    };
    c.finish()?;

    Ok(((macro_id, impl_id), parameter))
}

pub(super) fn decode_column_mapping(cells: &[Cell]) -> Result<proto::MappingValue> {
    let mut c = Cursor::new(TableKind::ColumnMapping, cells);
    let mapping_id = c.u64()?;
    let table_id = c.u64()?;
    let map_type = c.string()?;
    c.finish()?;

    Ok(proto::MappingValue {
        mapping_id,
        table_id,
        map_type,
        name_mappings: Vec::new(),
    })
}

pub(super) fn decode_name_mapping(cells: &[Cell]) -> Result<(u64, proto::NameMapping)> {
    let mut c = Cursor::new(TableKind::NameMapping, cells);
    let mapping_id = c.u64()?;
    let row = proto::NameMapping {
        column_id: c.u64()?,
        source_name: c.string()?,
        target_field_id: c.u64()?,
        parent_column: c.opt_u64()?,
        is_partition: c.bool()?,
    };
    c.finish()?;

    Ok((mapping_id, row))
}

/// Decodes an `UPDATE ... SET end_snapshot` row into the ended entity's
/// key and the new `end_snapshot` value. Defined only for the versioned
/// kinds.
pub(super) fn decode_end(table: TableKind, cells: &[Cell]) -> Result<(EntityKey, u64)> {
    let mut c = Cursor::new(table, cells);
    let key = match table {
        TableKind::Schema => EntityKey::Schema {
            schema_id: c.u64()?,
        },
        TableKind::Table => EntityKey::Table { table_id: c.u64()? },
        TableKind::View => EntityKey::View { view_id: c.u64()? },
        TableKind::Column => EntityKey::Column {
            table_id: c.u64()?,
            column_id: c.u64()?,
        },
        TableKind::DataFile => EntityKey::File {
            table_id: c.u64()?,
            data_file_id: c.u64()?,
        },
        TableKind::DeleteFile => EntityKey::DeleteFile {
            table_id: c.u64()?,
            delete_file_id: c.u64()?,
        },
        TableKind::PartitionInfo => EntityKey::Partition {
            table_id: c.u64()?,
            partition_id: c.u64()?,
        },
        TableKind::SortInfo => EntityKey::Sort {
            table_id: c.u64()?,
            sort_id: c.u64()?,
        },
        TableKind::Macro => EntityKey::Macro { macro_id: c.u64()? },
        // Tag entries end via `apply_update_set_end`'s own path.
        TableKind::Snapshot
        | TableKind::SnapshotChanges
        | TableKind::SchemaVersions
        | TableKind::TableStats
        | TableKind::TableColumnStats
        | TableKind::FileColumnStats
        | TableKind::PartitionColumn
        | TableKind::FilePartitionValue
        | TableKind::SortExpression
        | TableKind::Tag
        | TableKind::ColumnTag
        | TableKind::FilesScheduledForDeletion
        | TableKind::MacroImpl
        | TableKind::MacroParameters
        | TableKind::ColumnMapping
        | TableKind::NameMapping
        | TableKind::Metadata => {
            return Err(Error::Constraint(format!(
                "update_set_end is not defined for {table:?}"
            )));
        }
    };
    let end_snapshot = c.u64()?;
    c.finish()?;
    Ok((key, end_snapshot))
}

/// The key of one unversioned statistics row.
pub(super) enum StatsKey {
    Table(u64),
    Column(u64, u64),
    FileColumn(u64, u64, u64),
}

/// Decodes a raw-delete row's key. Defined only for the three unversioned
/// statistics kinds.
pub(super) fn decode_delete_key(table: TableKind, cells: &[Cell]) -> Result<StatsKey> {
    let mut c = Cursor::new(table, cells);
    let key = match table {
        TableKind::TableStats => StatsKey::Table(c.u64()?),
        TableKind::TableColumnStats => StatsKey::Column(c.u64()?, c.u64()?),
        TableKind::FileColumnStats => {
            let data_file_id = c.u64()?;
            let table_id = c.u64()?;
            let column_id = c.u64()?;
            StatsKey::FileColumn(table_id, data_file_id, column_id)
        }
        _ => {
            return Err(Error::Constraint(format!(
                "delete is not defined for {table:?}"
            )));
        }
    };
    c.finish()?;
    Ok(key)
}

/// Decodes a hard-delete row: the entity's key columns (decoder order)
/// followed by the row's `end_snapshot` — `NULL` names the live `current`
/// record, a value names that `history` record. `ducklake_column_mapping`
/// has no `end_snapshot` column and returns `None`.
pub(super) fn decode_hard_delete(
    table: TableKind,
    cells: &[Cell],
) -> Result<(EntityKey, Option<u64>)> {
    let mut c = Cursor::new(table, cells);
    if table == TableKind::ColumnMapping {
        let mapping_id = c.u64()?;
        let table_id = c.u64()?;
        c.finish()?;
        return Ok((
            EntityKey::Mapping {
                table_id,
                mapping_id,
            },
            None,
        ));
    }
    let key = match table {
        TableKind::Schema => EntityKey::Schema {
            schema_id: c.u64()?,
        },
        TableKind::Table => EntityKey::Table { table_id: c.u64()? },
        TableKind::View => EntityKey::View { view_id: c.u64()? },
        TableKind::Column => EntityKey::Column {
            table_id: c.u64()?,
            column_id: c.u64()?,
        },
        TableKind::DataFile => EntityKey::File {
            table_id: c.u64()?,
            data_file_id: c.u64()?,
        },
        TableKind::DeleteFile => EntityKey::DeleteFile {
            table_id: c.u64()?,
            delete_file_id: c.u64()?,
        },
        TableKind::PartitionInfo => EntityKey::Partition {
            table_id: c.u64()?,
            partition_id: c.u64()?,
        },
        TableKind::SortInfo => EntityKey::Sort {
            table_id: c.u64()?,
            sort_id: c.u64()?,
        },
        TableKind::Macro => EntityKey::Macro { macro_id: c.u64()? },
        // Callers dispatch every other kind before reaching here.
        _ => return Err(corrupt_row(table, "not a versioned kind")),
    };
    let end_snapshot = c.opt_u64()?;
    c.finish()?;
    Ok((key, end_snapshot))
}

/// Decodes a `ducklake_metadata` row into the option it names: the scope's
/// key components and the key/value pair. A `NULL` value is stored as the
/// empty string.
pub(super) fn decode_metadata(cells: &[Cell]) -> Result<((u64, u64), String, String)> {
    let mut c = Cursor::new(TableKind::Metadata, cells);
    let key = c.string()?;
    let value = c.opt_string()?.unwrap_or_default();
    let scope = c.opt_string()?;
    let scope_id = c.opt_u64()?.unwrap_or_default();
    c.finish()?;
    Ok((option_scope(scope.as_deref(), scope_id)?, key, value))
}

/// Decodes a `ducklake_metadata` delete row: the key and the scope,
/// without the value.
pub(super) fn decode_metadata_key(cells: &[Cell]) -> Result<((u64, u64), String)> {
    let mut c = Cursor::new(TableKind::Metadata, cells);
    let key = c.string()?;
    let scope = c.opt_string()?;
    let scope_id = c.opt_u64()?.unwrap_or_default();
    c.finish()?;
    Ok((option_scope(scope.as_deref(), scope_id)?, key))
}

/// The scope components of an option row, from DuckLake's scope name and
/// id.
fn option_scope(scope: Option<&str>, scope_id: u64) -> Result<(u64, u64)> {
    let scope_kind = match scope {
        None => 0,
        Some("schema") => 1,
        Some("table") => 2,
        Some(other) => {
            return Err(corrupt_row(
                TableKind::Metadata,
                format!("unknown option scope `{other}`"),
            ));
        }
    };
    // A global option carries no id, whatever the row says.
    Ok((scope_kind, if scope_kind == 0 { 0 } else { scope_id }))
}

/// A `ducklake_schema_versions` row: `(table_id, begin_snapshot,
/// schema_version)`. The cells arrive in DuckLake's declared order
/// (`begin_snapshot`, `schema_version`, `table_id`); the tuple is keyed the
/// way the projection reports it.
pub(super) fn decode_schema_version_row(cells: &[Cell]) -> Result<(u64, u64, u64)> {
    let mut c = Cursor::new(TableKind::SchemaVersions, cells);
    let begin_snapshot = c.u64()?;
    let schema_version = c.u64()?;
    let table_id = c.u64()?;
    c.finish()?;

    Ok((table_id, begin_snapshot, schema_version))
}
