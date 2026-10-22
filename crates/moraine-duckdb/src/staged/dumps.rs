//! Transaction-visible metadata dumps through the C ABI.

use moraine::ffi_support::staged::{self, StagedTransaction};

use super::MoraineTxHandle;
#[cfg(doc)]
use super::moraine_tx_begin;
use crate::{
    abi::{guard, write_reserved_array},
    dumps::{
        MoraineColumnMappingRow, MoraineColumnRow, MoraineColumnTagRow, MoraineDataFileRow,
        MoraineDeleteFileRow, MoraineFileColumnStatsRow, MoraineFilePartitionValueRow,
        MoraineFileVariantStatsRow, MoraineMacroImplRow, MoraineMacroParameterRow, MoraineMacroRow,
        MoraineNameMappingRow, MoraineOptionRow, MorainePartitionColumnRow,
        MorainePartitionInfoRow, MoraineScheduledDeletionRow, MoraineSchemaRow,
        MoraineSchemaVersionRow, MoraineSnapshotRow, MoraineSortExpressionRow, MoraineSortInfoRow,
        MoraineTableColumnStatsRow, MoraineTableRow, MoraineTableStatsRow, MoraineTagRow,
        MoraineViewColumnTagRow, MoraineViewRow, convert_rows,
    },
    error::{AbiError, MoraineError, codes},
};

/// Runs a visible-row dump under the transaction lock and ABI error guard.
/// Entry-point signatures remain explicit for cbindgen.
macro_rules! tx_dump_body {
    ($tx:ident, $out_items:ident, $out_len:ident, $err:ident, $visible:path
     $(, $arg:expr)* $(,)?) => {{
        let attempt = || {
            if $tx.is_null() {
                return Err(AbiError::invalid_argument("`tx` is null"));
            }
            if $out_items.is_null() || $out_len.is_null() {
                return Err(AbiError::invalid_argument("output pointer is null"));
            }
            // SAFETY: caller contract above.
            let tx_ref = unsafe { &*$tx };
            // SAFETY: `catalog` outlives `tx` per `moraine_tx_begin`'s contract.
            let catalog_ref = unsafe { &*tx_ref.catalog };
            // Held across the dump, so a concurrent stage waits instead of
            // moving the overlay under it.
            let guard = tx_ref.lock()?;
            let rows = catalog_ref
                .block_on($visible(&guard $(, $arg)*))
                .map_err(AbiError::from)?;
            convert_rows(rows)
        };

        // SAFETY: `err` validity is this function's own safety contract.
        match unsafe { guard($err, attempt) } {
            Ok((items, memory)) => {
                // SAFETY: checked non-null above; caller contract.
                unsafe { write_reserved_array(items, memory, $out_items, $out_len) };
                codes::OK
            }
            Err(code) => code,
        }
    }};
}

/// Dumps every `ducklake_snapshot` row as this transaction sees it:
/// committed rows at the transaction's read point minus the snapshot
/// deletes staged so far. Freed with `moraine_dump_snapshots_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`]
/// and not yet committed or rolled back; its catalog must still be
/// attached. `out_items`/`out_len` must be valid, writable pointers.
/// `err`, if non-null, must be a valid, writable [`MoraineError`]. All
/// for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_snapshots(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineSnapshotRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_snapshots,
    )
}

/// Dumps transaction-visible `ducklake_data_file` rows.
/// Free with `moraine_dump_data_files_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_data_files(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineDataFileRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_data_files,
    )
}

/// As [`moraine_tx_dump_data_files`], for a caller that keeps a row only
/// while `filter_snapshot < end_snapshot` (or it is null) — the shape
/// every DuckLake read of this table carries.
///
/// Once `filter_snapshot` reaches the transaction's read point, no ended
/// version can satisfy that, so the ended half is not read; the rows are
/// the same ones either way. Freed with `moraine_dump_data_files_free`.
///
/// # Safety
///
/// As [`moraine_tx_dump_data_files`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_data_files_live_at(
    tx: *mut MoraineTxHandle,
    filter_snapshot: u64,
    out_items: *mut *mut MoraineDataFileRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_data_files_live_at,
        Some(filter_snapshot)
    )
}

/// The `filter_snapshot` that asks for every version rather than the ones
/// live at a snapshot. Zero is a real snapshot id, so the sentinel is the
/// one value no snapshot can carry.
pub const MORAINE_EVERY_VERSION: u64 = u64::MAX;

/// As [`moraine_tx_dump_data_files`], narrowed to one table and, unless
/// `filter_snapshot` is [`MORAINE_EVERY_VERSION`], to the versions live at
/// it: the rows the whole dump would show for `table_id`, staged rows
/// included. Freed with `moraine_dump_data_files_free`.
///
/// # Safety
///
/// As [`moraine_tx_dump_data_files`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_data_files_of(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    filter_snapshot: u64,
    out_items: *mut *mut MoraineDataFileRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    let bound = (filter_snapshot != MORAINE_EVERY_VERSION).then_some(filter_snapshot);
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_data_files_of,
        table_id,
        bound
    )
}

/// Dumps transaction-visible `ducklake_delete_file` rows.
/// Free with `moraine_dump_delete_files_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_delete_files(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineDeleteFileRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_delete_files,
    )
}

/// As [`moraine_tx_dump_delete_files`], narrowed as
/// [`moraine_tx_dump_data_files_of`] is. Freed with
/// `moraine_dump_delete_files_free`.
///
/// # Safety
///
/// As [`moraine_tx_dump_delete_files`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_delete_files_of(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    filter_snapshot: u64,
    out_items: *mut *mut MoraineDeleteFileRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    let bound = (filter_snapshot != MORAINE_EVERY_VERSION).then_some(filter_snapshot);
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_delete_files_of,
        table_id,
        bound
    )
}

/// As [`moraine_tx_dump_delete_files`], for a caller that keeps a row only
/// while `filter_snapshot < end_snapshot` (or it is null). Once
/// `filter_snapshot` reaches the transaction's read point the ended half is not
/// read; the rows are the same ones either way. Freed with
/// `moraine_dump_delete_files_free`.
///
/// # Safety
///
/// As [`moraine_tx_dump_delete_files`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_delete_files_live_at(
    tx: *mut MoraineTxHandle,
    filter_snapshot: u64,
    out_items: *mut *mut MoraineDeleteFileRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_delete_files_live_at,
        Some(filter_snapshot)
    )
}

/// Dumps transaction-visible `ducklake_file_column_stats` rows.
/// Free with `moraine_dump_file_column_stats_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_file_column_stats(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineFileColumnStatsRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_file_column_stats,
    )
}

/// As [`moraine_tx_dump_file_column_stats`], narrowed to one table: the
/// rows the whole dump would show for `table_id`, staged rows included.
/// Freed with `moraine_dump_file_column_stats_free`.
///
/// # Safety
///
/// As [`moraine_tx_dump_file_column_stats`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_file_column_stats_of(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    out_items: *mut *mut MoraineFileColumnStatsRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_file_column_stats_of,
        table_id
    )
}

/// Dumps transaction-visible `ducklake_files_scheduled_for_deletion` rows.
/// Free with `moraine_dump_scheduled_deletions_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_scheduled_deletions(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineScheduledDeletionRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_scheduled_deletions,
    )
}

/// Dumps transaction-visible `ducklake_column` rows.
/// Free with `moraine_dump_columns_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_columns(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineColumnRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_columns,
    )
}

/// As [`moraine_tx_dump_columns`], for a caller that keeps a row only while
/// `filter_snapshot < end_snapshot` (or it is null). Once `filter_snapshot`
/// reaches the transaction's read point the ended half is not read; the
/// rows are the same ones either way. Freed with `moraine_dump_columns_free`.
///
/// # Safety
///
/// As [`moraine_tx_dump_columns`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_columns_live_at(
    tx: *mut MoraineTxHandle,
    filter_snapshot: u64,
    out_items: *mut *mut MoraineColumnRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_columns_live_at,
        Some(filter_snapshot)
    )
}

/// Dumps transaction-visible `ducklake_table` rows.
/// Free with `moraine_dump_tables_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_tables(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineTableRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_tables,
    )
}

/// As [`moraine_tx_dump_tables`], for a caller that keeps a row only while
/// `filter_snapshot < end_snapshot` (or it is null). Once `filter_snapshot`
/// reaches the transaction's read point the ended half is not read; the
/// rows are the same ones either way. Freed with `moraine_dump_tables_free`.
///
/// # Safety
///
/// As [`moraine_tx_dump_tables`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_tables_live_at(
    tx: *mut MoraineTxHandle,
    filter_snapshot: u64,
    out_items: *mut *mut MoraineTableRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_tables_live_at,
        Some(filter_snapshot)
    )
}

/// Dumps transaction-visible `ducklake_schema` rows.
/// Free with `moraine_dump_schemas_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_schemas(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineSchemaRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_schemas,
    )
}

/// Dumps transaction-visible `ducklake_view` rows.
/// Free with `moraine_dump_views_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_views(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineViewRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_views,
    )
}

/// As [`moraine_tx_dump_views`], for a caller that keeps a row only while
/// `filter_snapshot < end_snapshot` (or it is null). Once `filter_snapshot`
/// reaches the transaction's read point the ended half is not read; the
/// rows are the same ones either way. Freed with `moraine_dump_views_free`.
///
/// # Safety
///
/// As [`moraine_tx_dump_views`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_views_live_at(
    tx: *mut MoraineTxHandle,
    filter_snapshot: u64,
    out_items: *mut *mut MoraineViewRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_views_live_at,
        Some(filter_snapshot)
    )
}

/// Dumps transaction-visible `ducklake_table_stats` rows.
/// Free with `moraine_dump_table_stats_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_table_stats(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineTableStatsRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_table_stats,
    )
}

/// Dumps transaction-visible `ducklake_table_column_stats` rows.
/// Free with `moraine_dump_table_column_stats_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_table_column_stats(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineTableColumnStatsRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_table_column_stats,
    )
}

/// Dumps transaction-visible `ducklake_partition_info` rows.
/// Free with `moraine_dump_partition_info_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_partition_info(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MorainePartitionInfoRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_partition_info,
    )
}

/// As [`moraine_tx_dump_partition_info`], for a caller that keeps a row only
/// while `filter_snapshot < end_snapshot` (or it is null). Once
/// `filter_snapshot` reaches the transaction's read point the ended half is not
/// read; the rows are the same ones either way. Freed with
/// `moraine_dump_partition_info_free`.
///
/// # Safety
///
/// As [`moraine_tx_dump_partition_info`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_partition_info_live_at(
    tx: *mut MoraineTxHandle,
    filter_snapshot: u64,
    out_items: *mut *mut MorainePartitionInfoRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_partition_info_live_at,
        Some(filter_snapshot)
    )
}

/// Dumps transaction-visible `ducklake_sort_info` rows.
/// Free with `moraine_dump_sort_info_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_sort_info(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineSortInfoRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_sort_info,
    )
}

/// As [`moraine_tx_dump_sort_info`], for a caller that keeps a row only while
/// `filter_snapshot < end_snapshot` (or it is null). Once `filter_snapshot`
/// reaches the transaction's read point the ended half is not read; the
/// rows are the same ones either way. Freed with `moraine_dump_sort_info_free`.
///
/// # Safety
///
/// As [`moraine_tx_dump_sort_info`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_sort_info_live_at(
    tx: *mut MoraineTxHandle,
    filter_snapshot: u64,
    out_items: *mut *mut MoraineSortInfoRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_sort_info_live_at,
        Some(filter_snapshot)
    )
}

/// Dumps transaction-visible `ducklake_macro` rows.
/// Free with `moraine_dump_macros_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_macros(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineMacroRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_macros,
    )
}

/// Dumps transaction-visible `ducklake_column_mapping` rows.
/// Free with `moraine_dump_column_mappings_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_column_mappings(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineColumnMappingRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        StagedTransaction::visible_mappings,
    )
}

/// Dumps transaction-visible `ducklake_partition_column` rows.
/// Free with `moraine_dump_partition_columns_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_partition_columns(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MorainePartitionColumnRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_partition_column_rows,
    )
}

/// Dumps transaction-visible `ducklake_file_partition_value` rows.
/// Free with `moraine_dump_file_partition_values_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_file_partition_values(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineFilePartitionValueRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_file_partition_value_rows,
    )
}

/// Dumps transaction-visible `ducklake_file_variant_stats` rows.
/// Free with `moraine_dump_file_variant_stats_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_file_variant_stats(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineFileVariantStatsRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_file_variant_stats_rows,
    )
}

/// As [`moraine_tx_dump_file_variant_stats`], narrowed to one table: the
/// rows the whole dump would show for `table_id`, staged rows included.
/// Freed with `moraine_dump_file_variant_stats_free`.
///
/// # Safety
///
/// As [`moraine_tx_dump_file_variant_stats`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_file_variant_stats_of(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    out_items: *mut *mut MoraineFileVariantStatsRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_file_variant_stats_rows_of,
        table_id
    )
}

/// Dumps transaction-visible `ducklake_sort_expression` rows.
/// Free with `moraine_dump_sort_expressions_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_sort_expressions(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineSortExpressionRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_sort_expression_rows,
    )
}

/// Dumps transaction-visible `ducklake_column_tag` rows.
/// Free with `moraine_dump_column_tags_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_column_tags(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineColumnTagRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(tx, out_items, out_len, err, staged::visible_column_tag_rows,)
}

/// Dumps transaction-visible `ducklake_view_column_tag` rows.
/// Free with `moraine_dump_view_column_tags_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_view_column_tags(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineViewColumnTagRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_view_column_tag_rows,
    )
}

/// Dumps transaction-visible `ducklake_macro_impl` rows.
/// Free with `moraine_dump_macro_impls_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_macro_impls(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineMacroImplRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(tx, out_items, out_len, err, staged::visible_macro_impl_rows,)
}

/// Dumps transaction-visible `ducklake_macro_parameters` rows.
/// Free with `moraine_dump_macro_parameters_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_macro_parameters(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineMacroParameterRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_macro_parameter_rows,
    )
}

/// Dumps transaction-visible `ducklake_name_mapping` rows.
/// Free with `moraine_dump_name_mappings_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_name_mappings(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineNameMappingRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_name_mapping_rows,
    )
}

/// Dumps transaction-visible `ducklake_tag` rows.
/// Free with `moraine_dump_tags_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_tags(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineTagRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(tx, out_items, out_len, err, staged::visible_tag_rows,)
}

/// Dumps transaction-visible `ducklake_metadata` rows.
/// Free with `moraine_dump_options_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_options(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineOptionRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(tx, out_items, out_len, err, staged::visible_option_rows,)
}

/// Dumps transaction-visible `ducklake_schema_versions` rows.
/// Free with `moraine_dump_schema_versions_free`.
///
/// # Safety
///
/// Same pointer and lifetime contract as [`moraine_tx_dump_snapshots`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_schema_versions(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineSchemaVersionRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_schema_version_rows,
    )
}
