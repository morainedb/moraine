//! Row fixtures shared by the staged transaction tests.

use super::{Cell, RowOperation, TableKind};

pub(super) fn insert_row(table: TableKind, cells: Vec<Cell>) -> RowOperation {
    RowOperation::Insert { table, cells }
}

pub(super) fn schema_row(id: u64, name: &str, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::Str(format!("uuid-{id}")),
        Cell::U64(begin),
        Cell::Null,
        Cell::Str(name.to_string()),
        Cell::Str(format!("{name}/")),
        Cell::Bool(true),
    ]
}

pub(super) fn table_row(
    id: u64,
    schema_id: u64,
    name: &str,
    begin: u64,
    end: Option<u64>,
) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::Str(format!("uuid-t{id}")),
        Cell::U64(begin),
        end.map_or(Cell::Null, Cell::U64),
        Cell::U64(schema_id),
        Cell::Str(name.to_string()),
        Cell::Str(format!("{name}/")),
        Cell::Bool(true),
    ]
}

pub(super) fn column_row(table_id: u64, column_id: u64, name: &str, order: u64) -> Vec<Cell> {
    vec![
        Cell::U64(column_id),
        Cell::U64(0),
        Cell::Null,
        Cell::U64(table_id),
        Cell::U64(order),
        Cell::Str(name.to_string()),
        Cell::Str("BIGINT".to_string()),
        Cell::Null,
        Cell::Null,
        Cell::Bool(true),
        Cell::Null,
        Cell::Null,
        Cell::Null,
    ]
}

pub(super) fn snapshot_row(id: u64, schema_version: u64, next_catalog_id: u64) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::I64(1),
        Cell::U64(schema_version),
        Cell::U64(next_catalog_id),
        Cell::U64(0),
    ]
}

pub(super) fn snapshot_changes_row(id: u64, changes_made: &str) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::Str(changes_made.to_string()),
        Cell::Null,
        Cell::Null,
        Cell::Null,
    ]
}

pub(super) fn file_column_stats_row(
    data_file_id: u64,
    table_id: u64,
    column_id: u64,
    min_value: &str,
    max_value: &str,
) -> Vec<Cell> {
    vec![
        Cell::U64(data_file_id),
        Cell::U64(table_id),
        Cell::U64(column_id),
        Cell::U64(0),
        Cell::U64(10),
        Cell::U64(0),
        Cell::Str(min_value.to_string()),
        Cell::Str(max_value.to_string()),
        Cell::Bool(false),
        Cell::Null,
    ]
}

pub(super) fn partition_info_row(partition_id: u64, table_id: u64, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(partition_id),
        Cell::U64(table_id),
        Cell::U64(begin),
        Cell::Null,
    ]
}

pub(super) fn partition_column_row(
    partition_id: u64,
    table_id: u64,
    index: u64,
    column_id: u64,
) -> Vec<Cell> {
    vec![
        Cell::U64(partition_id),
        Cell::U64(table_id),
        Cell::U64(index),
        Cell::U64(column_id),
        Cell::Str("identity".to_string()),
    ]
}

pub(super) fn file_partition_value_row(
    data_file_id: u64,
    table_id: u64,
    index: u64,
    value: &str,
) -> Vec<Cell> {
    vec![
        Cell::U64(data_file_id),
        Cell::U64(table_id),
        Cell::U64(index),
        Cell::Str(value.to_string()),
    ]
}

pub(super) fn data_file_row(data_file_id: u64, table_id: u64, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(data_file_id),
        Cell::U64(table_id),
        Cell::U64(begin),
        Cell::Null,                       // end_snapshot
        Cell::Null,                       // file_order
        Cell::Str("data.parquet".into()), // path
        Cell::Bool(true),                 // path_is_relative
        Cell::Str("parquet".into()),      // file_format
        Cell::U64(10),                    // record_count
        Cell::U64(1024),                  // file_size_bytes
        Cell::U64(64),                    // footer_size
        Cell::U64(0),                     // row_id_start
        Cell::Null,                       // partition_id
        Cell::Null,                       // encryption_key
        Cell::Null,                       // mapping_id
        Cell::Null,                       // partial_max
    ]
}

pub(super) fn sort_info_row(sort_id: u64, table_id: u64, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(sort_id),
        Cell::U64(table_id),
        Cell::U64(begin),
        Cell::Null,
    ]
}

pub(super) fn sort_expression_row(
    sort_id: u64,
    table_id: u64,
    index: u64,
    expression: &str,
) -> Vec<Cell> {
    vec![
        Cell::U64(sort_id),
        Cell::U64(table_id),
        Cell::U64(index),
        Cell::Str(expression.to_string()),
        Cell::Str("duckdb".to_string()),
        Cell::Str("DESC".to_string()),
        Cell::Str("NULLS_FIRST".to_string()),
    ]
}

pub(super) fn tag_row(object_id: u64, begin: u64, key: &str, value: &str) -> Vec<Cell> {
    vec![
        Cell::U64(object_id),
        Cell::U64(begin),
        Cell::Null,
        Cell::Str(key.to_string()),
        Cell::Str(value.to_string()),
    ]
}

pub(super) fn column_tag_row(
    table_id: u64,
    column_id: u64,
    begin: u64,
    key: &str,
    value: &str,
) -> Vec<Cell> {
    vec![
        Cell::U64(table_id),
        Cell::U64(column_id),
        Cell::U64(begin),
        Cell::Null,
        Cell::Str(key.to_string()),
        Cell::Str(value.to_string()),
    ]
}

pub(super) fn macro_row(schema_id: u64, macro_id: u64, name: &str, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(schema_id),
        Cell::U64(macro_id),
        Cell::Str(name.to_string()),
        Cell::U64(begin),
        Cell::Null, // end_snapshot
    ]
}

pub(super) fn macro_impl_row(
    macro_id: u64,
    impl_id: u64,
    sql: &str,
    macro_type: &str,
) -> Vec<Cell> {
    vec![
        Cell::U64(macro_id),
        Cell::U64(impl_id),
        Cell::Str("duckdb".into()),
        Cell::Str(sql.to_string()),
        Cell::Str(macro_type.to_string()),
    ]
}

pub(super) fn macro_parameter_row(
    macro_id: u64,
    impl_id: u64,
    column_id: u64,
    name: &str,
    default: Option<&str>,
) -> Vec<Cell> {
    vec![
        Cell::U64(macro_id),
        Cell::U64(impl_id),
        Cell::U64(column_id),
        Cell::Str(name.to_string()),
        Cell::Str("unknown".into()),
        default.map_or(Cell::Null, |d| Cell::Str(d.to_string())),
        Cell::Str(default.map_or("unknown", |_| "int32").to_string()),
    ]
}

pub(super) fn column_mapping_row(mapping_id: u64, table_id: u64) -> Vec<Cell> {
    vec![
        Cell::U64(mapping_id),
        Cell::U64(table_id),
        Cell::Str("map_by_name".into()),
    ]
}

pub(super) fn name_mapping_row(
    mapping_id: u64,
    column_id: u64,
    source_name: &str,
    target_field_id: u64,
    parent_column: Option<u64>,
    is_partition: bool,
) -> Vec<Cell> {
    vec![
        Cell::U64(mapping_id),
        Cell::U64(column_id),
        Cell::Str(source_name.to_string()),
        Cell::U64(target_field_id),
        parent_column.map_or(Cell::Null, Cell::U64),
        Cell::Bool(is_partition),
    ]
}

pub(super) fn delete_file_row(
    delete_file_id: u64,
    table_id: u64,
    data_file_id: u64,
    begin: u64,
) -> Vec<Cell> {
    vec![
        Cell::U64(delete_file_id),
        Cell::U64(table_id),
        Cell::U64(begin),
        Cell::Null, // end_snapshot
        Cell::U64(data_file_id),
        Cell::Str("deletes.parquet".into()), // path
        Cell::Bool(true),                    // path_is_relative
        Cell::Str("parquet".into()),         // format
        Cell::U64(1),                        // delete_count
        Cell::U64(128),                      // file_size_bytes
        Cell::U64(32),                       // footer_size
        Cell::Null,                          // encryption_key
        Cell::Null,                          // partial_max
    ]
}
