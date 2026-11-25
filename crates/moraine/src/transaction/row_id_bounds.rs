//! The row-id bounds moraine records for a data file: the statistics row
//! under the reserved row-id column that the file directory reads to pick
//! the files that can hold a row id without opening any, and that a delete
//! file's registration is checked against.

use crate::{
    data_file::ROW_ID_FIELD_ID,
    store::{
        key::{EntityKey, Key},
        proto::FileColumnStatsValue,
        value,
    },
    transaction::commit::StagedWrite,
};

/// The statistics row recording that data file `data_file_id` of table
/// `table_id` holds row ids `low` through `high`.
pub(crate) fn value(
    table_id: u64,
    data_file_id: u64,
    (low, high): (u64, u64),
) -> FileColumnStatsValue {
    FileColumnStatsValue {
        data_file_id,
        table_id,
        column_id: ROW_ID_FIELD_ID,
        column_size_bytes: 0,
        value_count: None,
        null_count: None,
        min_value: Some(low.to_string()),
        max_value: Some(high.to_string()),
        contains_nan: None,
        extra_stats: None,
        variant_stats: vec![],
        min_is_exact: Some(true),
        max_is_exact: Some(true),
    }
}

/// [`value`] as the staged write that puts it.
pub(crate) fn staged_write(table_id: u64, data_file_id: u64, bounds: (u64, u64)) -> StagedWrite {
    (
        Key::current(EntityKey::FileColumnStats {
            table_id,
            data_file_id,
            column_id: ROW_ID_FIELD_ID,
        })
        .encode(),
        Some(value::encode_value(&value(table_id, data_file_id, bounds))),
    )
}
