//! The staged-row transaction ABI: the shim translates each row DuckLake
//! writes into a [`MoraineCell`] array and calls into this module. Rows
//! accumulate in memory via
//! [`moraine::ffi_support::staged::StagedTransaction`] until
//! [`moraine_tx_commit`] lands them all in one atomic store batch, or
//! [`moraine_tx_rollback`] discards them.
//!
//! Same conventions as [`crate::abi`]: `catch_unwind`/null discipline via
//! `crate::abi::guard`, no internal retry (a lost race at commit surfaces
//! [`codes::COMMIT_CONFLICT`] with the literal substring `conflict` in the
//! message). This module is translate-only: it decodes [`MoraineCell`]s
//! into [`Cell`]s and forwards them.
//!
//! [`MoraineTxHandle`] borrows the owning [`MoraineCatalogHandle`]'s tokio
//! runtime; the catalog must outlive every open transaction on it.

use std::{
    ffi::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
};

use moraine::ffi_support::staged::{
    self, Cell, RowOperation, StagedTransaction, TableKind, staged_begin,
};
use tracing::warn;

use crate::{
    abi::{borrow_bytes, borrow_str, guard, write_array},
    arrow_ipc::MoraineArrowBytes,
    dumps::{
        MoraineColumnMappingRow, MoraineColumnRow, MoraineColumnTagRow, MoraineDataFileRow,
        MoraineDeleteFileRow, MoraineFileColumnStatsRow, MoraineFilePartitionValueRow,
        MoraineMacroImplRow, MoraineMacroParameterRow, MoraineMacroRow, MoraineNameMappingRow,
        MoraineOptionRow, MorainePartitionColumnRow, MorainePartitionInfoRow,
        MoraineScheduledDeletionRow, MoraineSchemaRow, MoraineSchemaVersionRow, MoraineSnapshotRow,
        MoraineSortExpressionRow, MoraineSortInfoRow, MoraineTableColumnStatsRow, MoraineTableRow,
        MoraineTableStatsRow, MoraineTagRow, MoraineViewRow, column_rows, column_tag_rows,
        data_file_rows, delete_file_rows, file_column_stats_rows, file_partition_value_rows,
        macro_impl_rows, macro_parameter_rows, macro_rows, mapping_rows, name_mapping_rows,
        option_rows, partition_column_rows, partition_info_rows, scheduled_deletion_rows,
        schema_rows, schema_version_rows, snapshot_rows, sort_expression_rows, sort_info_rows,
        table_column_stats_rows, table_rows, table_stats_rows, tag_rows, view_rows,
    },
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One value in a staged row. Mirrors [`Cell`] as a tagged struct across
/// the C boundary; `str_value` is borrowed, valid only for the duration of
/// the [`moraine_tx_stage`] call that reads it.
#[repr(C)]
pub struct MoraineCell {
    /// `0` = NULL, `1` = u64, `2` = i64, `3` = bool, `4` = string.
    pub kind: i32,
    /// Valid iff `kind == 1`.
    pub u64_value: u64,
    /// Valid iff `kind == 2`.
    pub i64_value: i64,
    /// Valid iff `kind == 3`.
    pub bool_value: bool,
    /// Valid iff `kind == 4`: a borrowed, NUL-terminated UTF-8 string.
    pub str_value: *const c_char,
}

/// A staged-row transaction, opaque to C. Owns one [`StagedTransaction`]
/// plus a borrowed pointer to the catalog handle it was opened on.
pub struct MoraineTxHandle {
    catalog: *const MoraineCatalogHandle,
    tx: StagedTransaction,
}

/// Decodes the ABI's `table_kind` through the core's wire table
/// (`TableKind::ALL`).
fn decode_table_kind(v: i32) -> Result<TableKind, AbiError> {
    TableKind::try_from(v).map_err(|other| {
        AbiError::invalid_argument(format!("moraine_tx_stage: unknown table_kind {other}"))
    })
}

/// The four [`RowOperation`] shapes, decoded from `operation_kind`.
enum OperationKind {
    Insert,
    Delete,
    UpdateSetEnd,
    UpdateSetBegin,
}

fn decode_operation_kind(v: i32) -> Result<OperationKind, AbiError> {
    match v {
        0 => Ok(OperationKind::Insert),
        1 => Ok(OperationKind::Delete),
        2 => Ok(OperationKind::UpdateSetEnd),
        3 => Ok(OperationKind::UpdateSetBegin),
        other => Err(AbiError::invalid_argument(format!(
            "moraine_tx_stage: unknown operation_kind {other}"
        ))),
    }
}

/// Decodes a borrowed `MoraineCell` array into owned [`Cell`]s. A null
/// `cells` pointer is valid only when `len` is `0`.
///
/// # Safety
///
/// `cells`, if non-null, must point to `len` valid, readable
/// [`MoraineCell`]s; every non-null `str_value` inside must be a valid
/// NUL-terminated UTF-8 C string, valid for reads for the duration of
/// this call.
unsafe fn decode_cells(cells: *const MoraineCell, len: usize) -> Result<Vec<Cell>, AbiError> {
    if cells.is_null() && len == 0 {
        Ok(Vec::new())
    } else if cells.is_null() {
        Err(AbiError::invalid_argument(
            "moraine_tx_stage: `cells` is null but `cells_len` is nonzero",
        ))
    } else {
        // SAFETY: caller contract above.
        let slice = unsafe { std::slice::from_raw_parts(cells, len) };
        slice
            .iter()
            .map(|c| match c.kind {
                0 => Ok(Cell::Null),
                1 => Ok(Cell::U64(c.u64_value)),
                2 => Ok(Cell::I64(c.i64_value)),
                3 => Ok(Cell::Bool(c.bool_value)),
                4 => {
                    // SAFETY: caller contract above.
                    let s = unsafe { borrow_str(c.str_value, "cell.str_value") }?;
                    Ok(Cell::Str(s.to_string()))
                }
                other => Err(AbiError::invalid_argument(format!(
                    "moraine_tx_stage: unknown cell kind {other}"
                ))),
            })
            .collect()
    }
}

/// Opens a staged-row transaction at the current head and writes the
/// resulting handle to `*out`.
///
/// Cancellable: races the head read against `probe` (polled
/// immediately, then ~100 ms; a null `probe` disables polling). If a
/// cancellation wins, returns [`codes::INTERRUPTED`], `*out` is left
/// unwritten, and nothing is staged.
///
/// # Safety
///
/// `handle` must be a pointer previously returned by
/// [`moraine_attach`](crate::abi::moraine_attach) and not yet detached,
/// and must outlive every operation on the returned transaction handle
/// (through [`moraine_tx_commit`] or [`moraine_tx_rollback`]). `out`
/// must be a valid, writable `*mut *mut MoraineTxHandle`. `probe`, if
/// non-null, must be safe to call with `probe_ctx` from any thread.
/// `err`, if non-null, must be a valid, writable [`MoraineError`]. All
/// for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_begin(
    handle: *mut MoraineCatalogHandle,
    out: *mut *mut MoraineTxHandle,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Box<MoraineTxHandle>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out.is_null() {
            return Err(AbiError::invalid_argument("`out` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let tx = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                staged_begin(
                    handle_ref.catalog.writer()?,
                    handle_ref.data_store.clone(),
                    handle_ref.data_prefix.clone(),
                ),
            )
        }?;
        Ok(Box::new(MoraineTxHandle {
            catalog: handle,
            tx,
        }))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(handle) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe {
                *out = Box::into_raw(handle);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Accumulates one staged row mutation. Nothing touches the store until
/// [`moraine_tx_commit`]. `table_kind` is [`TableKind`]'s discriminant
/// order (`0` = `Snapshot`, `1` = `SnapshotChanges`, `2` = `Schema`, `3` =
/// `Table`, `4` = `View`, `5` = `Column`, `6` = `DataFile`, `7` =
/// `DeleteFile`, `8` = `TableStats`, `9` = `TableColumnStats`, `10` =
/// `FileColumnStats`, `11` = `SchemaVersions`, `12` = `PartitionInfo`,
/// `13` = `PartitionColumn`, `14` = `FilePartitionValue`, `15` =
/// `SortInfo`, `16` = `SortExpression`, `17` = `Tag`, `18` =
/// `ColumnTag`, `19` = `FilesScheduledForDeletion`); `operation_kind` is
/// `0` = insert, `1` = delete, `2` = update-sets-`end_snapshot`, `3` =
/// update-sets-`begin_snapshot`. `cells` are positional
/// in the column order the shim declares for `table_kind`'s table (a delete
/// or update-set-end row carries only the key columns, per [`RowOperation`]'s
/// variants).
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`]
/// and not yet committed or rolled back. `cells`, if `cells_len` is
/// nonzero, must point to `cells_len` valid [`MoraineCell`]s (every
/// non-null `str_value` inside a valid NUL-terminated UTF-8 C string).
/// `err`, if non-null, must be a valid, writable [`MoraineError`]. All
/// for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage(
    tx: *mut MoraineTxHandle,
    table_kind: i32,
    operation_kind: i32,
    cells: *const MoraineCell,
    cells_len: usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        let table = decode_table_kind(table_kind)?;
        let kind = decode_operation_kind(operation_kind)?;
        // SAFETY: caller contract above.
        let row_cells = unsafe { decode_cells(cells, cells_len) }?;
        let op = match kind {
            OperationKind::Insert => RowOperation::Insert {
                table,
                cells: row_cells,
            },
            OperationKind::Delete => RowOperation::Delete {
                table,
                cells: row_cells,
            },
            OperationKind::UpdateSetEnd => RowOperation::UpdateSetEnd {
                table,
                cells: row_cells,
            },
            OperationKind::UpdateSetBegin => RowOperation::UpdateSetBegin {
                table,
                cells: row_cells,
            },
        };
        // SAFETY: caller contract for `tx`.
        let tx_ref = unsafe { &mut *tx };
        tx_ref.tx.stage(op);
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// The shared body of every `moraine_tx_dump_*` below: check the pointers,
/// run the named projection against the open transaction, and convert its
/// records with the same function the committed dump uses. Each entry
/// point's signature stays written out: the C header is generated by
/// parsing them.
macro_rules! tx_dump_body {
    ($tx:ident, $out_items:ident, $out_len:ident, $err:ident, $visible:path, $convert:path) => {{
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
            let rows = catalog_ref
                .block_on($visible(&tx_ref.tx))
                .map_err(AbiError::from)?;
            $convert(rows)
        };

        // SAFETY: `err` validity is this function's own safety contract.
        match unsafe { guard($err, attempt) } {
            Ok(items) => {
                // SAFETY: checked non-null above; caller contract.
                unsafe { write_array(items, $out_items, $out_len) };
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
        snapshot_rows
    )
}

/// Dumps every `ducklake_data_file` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_data_files_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        data_file_rows
    )
}

/// Dumps every `ducklake_delete_file` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_delete_files_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        delete_file_rows
    )
}

/// Dumps every `ducklake_file_column_stats` row as this transaction sees
/// it: committed rows at the transaction's read point with its own staged
/// rows over them. Freed with `moraine_dump_file_column_stats_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        file_column_stats_rows
    )
}

/// Dumps every `ducklake_files_scheduled_for_deletion` row as this
/// transaction sees it: committed rows at the transaction's read point with
/// its own staged rows over them. Freed with
/// `moraine_dump_scheduled_deletions_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        scheduled_deletion_rows
    )
}

/// Dumps every `ducklake_column` row as this transaction sees it: committed
/// rows at the transaction's read point with its own staged rows over them.
/// Freed with `moraine_dump_columns_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        column_rows
    )
}

/// Dumps every `ducklake_table` row as this transaction sees it: committed
/// rows at the transaction's read point with its own staged rows over them.
/// Freed with `moraine_dump_tables_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        table_rows
    )
}

/// Dumps every `ducklake_schema` row as this transaction sees it: committed
/// rows at the transaction's read point with its own staged rows over them.
/// Freed with `moraine_dump_schemas_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        schema_rows
    )
}

/// Dumps every `ducklake_view` row as this transaction sees it: committed
/// rows at the transaction's read point with its own staged rows over them.
/// Freed with `moraine_dump_views_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        view_rows
    )
}

/// Dumps every `ducklake_table_stats` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_table_stats_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        table_stats_rows
    )
}

/// Dumps every `ducklake_table_column_stats` row as this transaction sees
/// it: committed rows at the transaction's read point with its own staged
/// rows over them. Freed with `moraine_dump_table_column_stats_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        table_column_stats_rows
    )
}

/// Dumps every `ducklake_partition_info` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_partition_info_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        partition_info_rows
    )
}

/// Dumps every `ducklake_sort_info` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_sort_info_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        sort_info_rows
    )
}

/// Dumps every `ducklake_macro` row as this transaction sees it: committed
/// rows at the transaction's read point with its own staged rows over them.
/// Freed with `moraine_dump_macros_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        macro_rows
    )
}

/// Dumps every `ducklake_column_mapping` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_column_mappings_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        mapping_rows
    )
}

/// Dumps every `ducklake_partition_column` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_partition_columns_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        partition_column_rows
    )
}

/// Dumps every `ducklake_file_partition_value` row as this transaction sees
/// it: committed rows at the transaction's read point with its own staged
/// rows over them. Freed with `moraine_dump_file_partition_values_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        file_partition_value_rows
    )
}

/// Dumps every `ducklake_sort_expression` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_sort_expressions_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        sort_expression_rows
    )
}

/// Dumps every `ducklake_column_tag` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_column_tags_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_column_tags(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineColumnTagRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_column_tag_rows,
        column_tag_rows
    )
}

/// Dumps every `ducklake_macro_impl` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_macro_impls_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_macro_impls(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineMacroImplRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_macro_impl_rows,
        macro_impl_rows
    )
}

/// Dumps every `ducklake_macro_parameters` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_macro_parameters_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        macro_parameter_rows
    )
}

/// Dumps every `ducklake_name_mapping` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_name_mappings_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        name_mapping_rows
    )
}

/// Dumps every `ducklake_tag` row as this transaction sees it: committed
/// rows at the transaction's read point with its own staged rows over them.
/// Freed with `moraine_dump_tags_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_tags(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineTagRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_tag_rows,
        tag_rows
    )
}

/// Dumps every `ducklake_metadata` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_options_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_dump_options(
    tx: *mut MoraineTxHandle,
    out_items: *mut *mut MoraineOptionRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    tx_dump_body!(
        tx,
        out_items,
        out_len,
        err,
        staged::visible_option_rows,
        option_rows
    )
}

/// Dumps every `ducklake_schema_versions` row as this transaction sees it:
/// committed rows at the transaction's read point with its own staged rows over
/// them. Freed with `moraine_dump_schema_versions_free`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back; its catalog must still be attached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
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
        schema_version_rows
    )
}

/// Translates every staged row and lands them in one atomic batch,
/// consuming `tx`. On success, writes the new snapshot id to
/// `*out_snapshot_id`.
///
/// A lost race against a concurrent commit is never retried internally: it
/// returns [`codes::COMMIT_CONFLICT`] with the literal substring `conflict`
/// in the message, and the loser leaves the store unchanged. `tx` is freed
/// either way; it must not be passed to [`moraine_tx_rollback`]
/// afterward.
///
/// Cancellable: races the commit against `probe` (polled immediately, then
/// ~100 ms; a null `probe` disables polling). Where the cancellation lands
/// decides what it means:
///
/// - Before the durable write is issued: [`codes::INTERRUPTED`], catalog
///   unchanged, head where it was.
/// - While the durable write is in flight: the write runs to completion in the
///   background and this call returns [`codes::INTERRUPTED`] promptly, so the
///   commit may still land. Head ends at either the old id or the new one; a
///   caller that needs to know re-resolves head, and must not treat
///   [`codes::INTERRUPTED`] as "nothing landed".
/// - After the write completed: the committed snapshot id is reported normally.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`]
/// and not yet committed or rolled back. `out_snapshot_id` must be a
/// valid, writable `*mut u64`. `probe`, if non-null, must be safe to call
/// with `probe_ctx` from any thread. `err`, if non-null, must be a valid,
/// writable [`MoraineError`]. All for the duration of this call. The
/// catalog handle `tx` was opened on ([`moraine_tx_begin`]'s contract)
/// must still be attached.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_commit(
    tx: *mut MoraineTxHandle,
    out_snapshot_id: *mut u64,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<u64, AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller contract above; `tx` consumed exactly once.
        let boxed = unsafe { Box::from_raw(tx) };
        let MoraineTxHandle { catalog, tx } = *boxed;
        // Checked after `tx` is reclaimed: the contract frees it on every
        // path, argument errors included.
        if out_snapshot_id.is_null() {
            tx.rollback();
            return Err(AbiError::invalid_argument("`out_snapshot_id` is null"));
        }
        // SAFETY: `catalog` outlives `tx` per `moraine_tx_begin`'s contract.
        let catalog_ref = unsafe { &*catalog };
        // Read before the commit consumes `tx`, spawned after it lands.
        let warm_tables = tx.tables_with_staged_data_files();
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let id = unsafe { catalog_ref.block_on_cancellable(probe, probe_ctx, tx.commit()) }?;
        catalog_ref.spawn_warm_tables(warm_tables);

        // Repair runs after the data snapshot is durable; its failure,
        // including cancellation, is not a commit failure.
        match catalog_ref.catalog.writer() {
            Ok(writer) => {
                let repair = writer.repair_deferred_indexes(
                    catalog_ref.data_store.clone(),
                    &catalog_ref.data_prefix,
                    None,
                );
                // SAFETY: `probe`/`probe_ctx` validity is this function's
                // own safety contract.
                let repaired =
                    unsafe { catalog_ref.block_on_cancellable(probe, probe_ctx, repair) };
                if let Err(error) = repaired {
                    warn!(error = ?error, "deferred index repair will resume during maintenance");
                }
            }
            Err(error) => {
                warn!(error = %error, "deferred index repair will resume during maintenance");
            }
        }

        Ok(id.get())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(id) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe {
                *out_snapshot_id = id;
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Discards every staged row without writing anything, consuming `tx`.
/// Best-effort: has no error channel. A null `tx` is a no-op.
///
/// # Safety
///
/// `tx`, if non-null, must be a pointer previously returned by
/// [`moraine_tx_begin`] and not yet committed or rolled back.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_rollback(tx: *mut MoraineTxHandle) {
    if tx.is_null() {
        return;
    }
    let attempt = || {
        // SAFETY: caller contract above.
        let boxed = unsafe { Box::from_raw(tx) };
        boxed.tx.rollback();
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Stages `inline/schema`: the Arrow IPC schema for one `(table_id,
/// schema_version)`, written once at inline-table creation.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`]
/// and not yet committed or rolled back. `arrow_schema`, if
/// `arrow_schema_len` is nonzero, must point to `arrow_schema_len` valid
/// bytes. `err`, if non-null, must be a valid, writable [`MoraineError`].
/// All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage_inline_schema(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    schema_version: u64,
    arrow_schema: *const u8,
    arrow_schema_len: usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller contract above.
        let bytes = unsafe { borrow_bytes(arrow_schema, arrow_schema_len, "arrow_schema") }?;
        // SAFETY: caller contract for `tx`.
        let tx_ref = unsafe { &mut *tx };
        tx_ref.tx.stage(RowOperation::InlineSchema {
            table_id,
            schema_version,
            arrow_schema: bytes.to_vec(),
        });

        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Consumes an encoded Arrow schema buffer directly into the staged row.
///
/// # Safety
///
/// Same `tx`/`err` contract as [`moraine_tx_stage_inline_schema`].
/// `arrow_schema` must be an unconsumed value returned by
/// [`crate::arrow_ipc::moraine_arrow_encode_schema`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage_inline_schema_owned(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    schema_version: u64,
    arrow_schema: MoraineArrowBytes,
    err: *mut MoraineError,
) -> i32 {
    // Reclaim first so every return path consumes the caller's buffer.
    // SAFETY: caller contract guarantees the buffer came from the encoder.
    let arrow_schema = unsafe { arrow_schema.into_vec() };
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller contract for `tx`.
        let tx_ref = unsafe { &mut *tx };
        tx_ref.tx.stage(RowOperation::InlineSchema {
            table_id,
            schema_version,
            arrow_schema,
        });
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages `inline/insert`: one Arrow record-batch chunk of inlined rows.
///
/// # Safety
///
/// Same `tx` contract as [`moraine_tx_stage_inline_schema`].
/// `arrow_body`, if `arrow_body_len` is nonzero, must point to
/// `arrow_body_len` valid bytes. `err`, if non-null, must be a valid,
/// writable [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_tx_stage_inline_insert(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    schema_version: u64,
    begin_snapshot: u64,
    row_id_start: u64,
    row_count: u64,
    arrow_body: *const u8,
    arrow_body_len: usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller contract above.
        let bytes = unsafe { borrow_bytes(arrow_body, arrow_body_len, "arrow_body") }?;
        // SAFETY: caller contract for `tx`.
        let tx_ref = unsafe { &mut *tx };
        tx_ref.tx.stage(RowOperation::InlineInsert {
            table_id,
            schema_version,
            begin_snapshot,
            row_id_start,
            row_count,
            arrow_body: bytes.to_vec(),
        });

        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Consumes an encoded Arrow chunk body directly into the staged row.
///
/// # Safety
///
/// Same metadata and `tx`/`err` contract as
/// [`moraine_tx_stage_inline_insert`]. `arrow_body` must be an unconsumed
/// value returned by [`crate::arrow_ipc::moraine_arrow_encode_chunk`].
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_tx_stage_inline_insert_owned(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    schema_version: u64,
    begin_snapshot: u64,
    row_id_start: u64,
    row_count: u64,
    arrow_body: MoraineArrowBytes,
    err: *mut MoraineError,
) -> i32 {
    // Reclaim first so every return path consumes the caller's buffer.
    // SAFETY: caller contract guarantees the buffer came from the encoder.
    let arrow_body = unsafe { arrow_body.into_vec() };
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller contract for `tx`.
        let tx_ref = unsafe { &mut *tx };
        tx_ref.tx.stage(RowOperation::InlineInsert {
            table_id,
            schema_version,
            begin_snapshot,
            row_id_start,
            row_count,
            arrow_body,
        });
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages `inline/inline_delete`: tombstones one inlined-insert row.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`]
/// and not yet committed or rolled back. `err`, if non-null, must be a
/// valid, writable [`MoraineError`]. Both for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage_inline_inline_delete(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    row_id: u64,
    end_snapshot: u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller contract for `tx`.
        let tx_ref = unsafe { &mut *tx };
        tx_ref.tx.stage(RowOperation::InlineInlineDelete {
            table_id,
            row_id,
            end_snapshot,
        });

        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages `inline/file_delete`: an inlined delete against a Parquet-file row.
///
/// # Safety
///
/// Same contract as [`moraine_tx_stage_inline_inline_delete`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage_inline_file_delete(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    data_file_id: u64,
    row_id: u64,
    begin_snapshot: u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller contract for `tx`.
        let tx_ref = unsafe { &mut *tx };
        tx_ref.tx.stage(RowOperation::InlineFileDelete {
            table_id,
            data_file_id,
            row_id,
            begin_snapshot,
        });

        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages the removal of one live `inline/file_delete` record, which a
/// `DELETE` against `ducklake_inlined_delete_<table_id>` translates to.
/// Naming a record the table does not carry is [`codes::CORRUPTION`], not
/// a no-op.
///
/// # Safety
///
/// Same contract as [`moraine_tx_stage_inline_inline_delete`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage_inline_file_delete_remove(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    data_file_id: u64,
    row_id: u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller contract for `tx`.
        let tx_ref = unsafe { &mut *tx };
        tx_ref.tx.stage(RowOperation::InlineFileDeleteRemove {
            table_id,
            data_file_id,
            row_id,
        });

        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages a flush-delete: removes every `inline/insert` chunk begun at or
/// before `flush_snapshot` for `(table_id, schema_version)`, plus the
/// `inline/inline_delete` tombstones those chunks' rows consumed.
///
/// # Safety
///
/// Same contract as [`moraine_tx_stage_inline_inline_delete`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage_inline_flush_delete(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    schema_version: u64,
    flush_snapshot: u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller contract for `tx`.
        let tx_ref = unsafe { &mut *tx };
        tx_ref.tx.stage(RowOperation::InlineFlushDelete {
            table_id,
            schema_version,
            flush_snapshot,
        });
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages a table drop: removes every `inline/*` record for `table_id`.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`]
/// and not yet committed or rolled back. `err`, if non-null, must be a
/// valid, writable [`MoraineError`]. Both for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage_inline_drop(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller contract for `tx`.
        let tx_ref = unsafe { &mut *tx };
        tx_ref.tx.stage(RowOperation::InlineDrop { table_id });
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages a schema-version-scoped deregistration: removes only the
/// `inline/schema` record for `(table_id, schema_version)`, leaving any
/// other schema version's `inline/*` records untouched (unlike
/// [`moraine_tx_stage_inline_drop`], which is table-wide).
///
/// # Safety
///
/// Same contract as [`moraine_tx_stage_inline_drop`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage_inline_schema_drop(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    schema_version: u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller contract for `tx`.
        let tx_ref = unsafe { &mut *tx };
        tx_ref.tx.stage(RowOperation::InlineSchemaDrop {
            table_id,
            schema_version,
        });

        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::{
        abi::moraine_detach,
        test_support::{
            StrArena, TempDir, attach_ok, attach_with_data_path, begin, bool_cell, commit,
            i64_cell, null_cell, stage, u64_cell,
        },
    };

    /// Stages a full `ducklake_data_file` row (`table_kind` 6,
    /// `operation_kind` 0) against `table_id`.
    fn stage_data_file_row(
        tx: *mut MoraineTxHandle,
        arena: &mut StrArena,
        data_file_id: u64,
        table_id: u64,
        begin_snapshot: u64,
    ) {
        stage(
            tx,
            6,
            0,
            &[
                u64_cell(data_file_id),
                u64_cell(table_id),
                u64_cell(begin_snapshot),
                null_cell(),
                null_cell(),
                arena.cell("data-1.parquet"),
                bool_cell(true),
                arena.cell("parquet"),
                u64_cell(10),
                u64_cell(1024),
                u64_cell(64),
                u64_cell(0),
                null_cell(),
                null_cell(),
                null_cell(),
                null_cell(),
            ],
        );
    }

    /// A commit that registers data files warms the tables it touched.
    #[test]
    fn a_commit_registering_data_files_warms_those_tables() {
        let lake = TempDir::new("commit-warm");
        let data = TempDir::new("commit-warm-data");
        let handle = attach_with_data_path(lake.path(), data.path());
        // Drained first, so what remains at the end is the commit's own.
        // SAFETY: attached above and not yet detached.
        unsafe { &*handle }.finish_warming();

        let tx = begin(handle);
        let mut arena = StrArena::new();
        stage_table_row(tx, &mut arena, 1, (1, None), 0, "t", "t/");
        stage_data_file_row(tx, &mut arena, 1, 1, 1);
        stage_snapshot_and_changes(tx, &mut arena, 1, 1, 2, "inserted_into_table:1");
        stage(tx, 11, 0, &[u64_cell(1), u64_cell(1), u64_cell(1)]);
        commit(tx);

        // SAFETY: still attached.
        let warming = unsafe { &*handle }.finish_warming();

        assert_eq!(warming, 1, "the commit spawned no warming pass");
        // SAFETY: attached above, detached exactly once.
        unsafe { moraine_detach(handle) };
    }

    /// A commit that registers no data files spawns no warming.
    #[test]
    fn a_commit_registering_no_data_files_warms_nothing() {
        let lake = TempDir::new("commit-warm-none");
        let data = TempDir::new("commit-warm-none-data");
        let handle = attach_with_data_path(lake.path(), data.path());
        // SAFETY: attached above and not yet detached.
        unsafe { &*handle }.finish_warming();

        let tx = begin(handle);
        let mut arena = StrArena::new();
        stage_table_row(tx, &mut arena, 1, (1, None), 0, "t", "t/");
        stage_snapshot_and_changes(tx, &mut arena, 1, 1, 2, r#"created_table:"main"."t""#);
        stage(tx, 11, 0, &[u64_cell(1), u64_cell(1), u64_cell(1)]);
        commit(tx);

        // SAFETY: still attached.
        let warming = unsafe { &*handle }.finish_warming();

        assert_eq!(warming, 0);
        // SAFETY: attached above, detached exactly once.
        unsafe { moraine_detach(handle) };
    }

    /// Stages a full `ducklake_table` row (`table_kind` 3, `operation_kind` 0):
    /// id, a synthetic uuid, begin/end snapshot (`lifecycle`), schema id,
    /// name, path (always relative) — the shape every test that creates or
    /// renames a table needs.
    fn stage_table_row(
        tx: *mut MoraineTxHandle,
        arena: &mut StrArena,
        table_id: u64,
        lifecycle: (u64, Option<u64>),
        schema_id: u64,
        name: &str,
        path: &str,
    ) {
        let (begin_snapshot, end_snapshot) = lifecycle;
        stage(
            tx,
            3,
            0,
            &[
                u64_cell(table_id),
                arena.cell(&format!("uuid-t{table_id}")),
                u64_cell(begin_snapshot),
                end_snapshot.map_or_else(null_cell, u64_cell),
                u64_cell(schema_id),
                arena.cell(name),
                arena.cell(path),
                bool_cell(true),
            ],
        );
    }

    /// Stages the `ducklake_snapshot` + `ducklake_snapshot_changes` pair
    /// every commit in this module bumps: id, time (same value as `id` in
    /// every fixture here), schema version, and `next_catalog_id`, plus the
    /// changes-made text DuckLake records for the same snapshot.
    fn stage_snapshot_and_changes(
        tx: *mut MoraineTxHandle,
        arena: &mut StrArena,
        snapshot_id: u64,
        schema_version: u64,
        next_catalog_id: u64,
        changes_made: &str,
    ) {
        stage(
            tx,
            0,
            0,
            &[
                u64_cell(snapshot_id),
                i64_cell(i64::try_from(snapshot_id).expect("test snapshot id fits i64")),
                u64_cell(schema_version),
                u64_cell(next_catalog_id),
                u64_cell(0),
            ],
        );
        stage(
            tx,
            1,
            0,
            &[
                u64_cell(snapshot_id),
                arena.cell(changes_made),
                null_cell(),
                null_cell(),
                null_cell(),
            ],
        );
    }

    /// A DuckLake-shaped snapshot bump plus table create: table `t` (id
    /// 1, schema 0 = bootstrap's `main`) with one column, staged and
    /// committed over the ABI as one batch, then verified through the
    /// dump ABI (the same view the metadata-table scan serves).
    #[test]
    fn stages_table_create_and_snapshot_bump_over_the_abi() {
        let dir = TempDir::new("create");
        let handle = attach_ok(dir.path());
        let tx = begin(handle);

        let mut arena = StrArena::new();
        stage_table_row(tx, &mut arena, 1, (1, None), 0, "t", "t/");
        // ducklake_column: column_id, begin_snapshot, end_snapshot,
        // table_id, column_order, column_name, column_type,
        // initial_default, default_value, nulls_allowed, parent_column,
        // default_value_type, default_value_dialect.
        stage(
            tx,
            5,
            0,
            &[
                u64_cell(1),
                u64_cell(0),
                null_cell(),
                u64_cell(1),
                u64_cell(0),
                arena.cell("a"),
                arena.cell("BIGINT"),
                null_cell(),
                null_cell(),
                bool_cell(true),
                null_cell(),
                null_cell(),
                null_cell(),
            ],
        );
        stage_snapshot_and_changes(tx, &mut arena, 1, 1, 2, r#"created_table:"main"."t""#);
        // ducklake_schema_versions: begin_snapshot, schema_version,
        // table_id — DuckLake writes one per created table, carrying this
        // commit's own snapshot values.
        stage(tx, 11, 0, &[u64_cell(1), u64_cell(1), u64_cell(1)]);

        let mut snapshot_id: u64 = 0;
        let mut err = MoraineError::default();
        // SAFETY: `tx` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_tx_commit(
                tx,
                &raw mut snapshot_id,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        // SAFETY: `err.message` is null or was just written by `moraine_tx_commit`.
        assert_eq!(code, codes::OK, "commit failed: {:?}", unsafe {
            err.message.as_ref()
        });
        assert_eq!(snapshot_id, 1);

        let mut rows: *mut crate::dumps::MoraineTableRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut dump_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            crate::dumps::moraine_dump_tables(
                handle,
                &raw mut rows,
                &raw mut len,
                None,
                ptr::null_mut(),
                &raw mut dump_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(len, 1);
        // SAFETY: just populated above.
        let name = unsafe { std::ffi::CStr::from_ptr((*rows).table_name) }
            .to_str()
            .unwrap();
        assert_eq!(name, "t");

        // The schema-version row folded into the snapshot record and
        // flattens back out of the dump verbatim.
        let mut versions: *mut crate::dumps::MoraineSchemaVersionRow = ptr::null_mut();
        let mut versions_len: usize = 0;
        let mut versions_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            crate::dumps::moraine_dump_schema_versions(
                handle,
                &raw mut versions,
                &raw mut versions_len,
                None,
                ptr::null_mut(),
                &raw mut versions_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(versions_len, 1);
        // SAFETY: just populated above.
        unsafe {
            assert_eq!((*versions).begin_snapshot, 1);
            assert_eq!((*versions).schema_version, 1);
            assert_eq!((*versions).table_id, 1);
        }

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            crate::dumps::moraine_dump_tables_free(rows, len);
            crate::dumps::moraine_dump_schema_versions_free(versions, versions_len);
            moraine_detach(handle);
        }
    }

    /// The other two op kinds over the wire: an `update_set_end` row
    /// (`operation_kind` 2 — the C++ UPDATE operator's staging for a
    /// rename/drop) moves the old table version to history, and a raw
    /// `delete` row (`operation_kind` 1) removes an unversioned statistics
    /// row. Cell layouts here are exactly what `cpp/staged_write.cpp`'s
    /// Sinks emit: key cells in decoder order, plus (for `update_set_end`)
    /// the new `end_snapshot`.
    #[test]
    fn update_set_end_and_stats_delete_over_the_abi() {
        let dir = TempDir::new("end-delete");
        let handle = attach_ok(dir.path());

        // Snapshot 1: table `t` (id 1, in bootstrap's `main`) plus its
        // stats row.
        let setup = begin(handle);
        let mut arena = StrArena::new();
        stage_table_row(setup, &mut arena, 1, (1, None), 0, "t", "t/");
        // ducklake_table_stats: table_id, record_count, next_row_id,
        // file_size_bytes.
        stage(
            setup,
            8,
            0,
            &[u64_cell(1), u64_cell(0), u64_cell(0), u64_cell(0)],
        );
        stage_snapshot_and_changes(setup, &mut arena, 1, 1, 2, r#"created_table:"main"."t""#);
        let mut id: u64 = 0;
        let mut err = MoraineError::default();
        // SAFETY: `setup` is live; outputs are valid local slots.
        let code =
            unsafe { moraine_tx_commit(setup, &raw mut id, None, ptr::null_mut(), &raw mut err) };
        assert_eq!(code, codes::OK);

        // Snapshot 2: end `t`'s live version (rename shape: end + new
        // version) and delete its stats row.
        let tx = begin(handle);
        // update_set_end on ducklake_table: [table_id, new end_snapshot].
        stage(tx, 3, 2, &[u64_cell(1), u64_cell(2)]);
        stage_table_row(tx, &mut arena, 1, (2, None), 0, "t2", "t/");
        // delete on ducklake_table_stats: [table_id].
        stage(tx, 8, 1, &[u64_cell(1)]);
        stage_snapshot_and_changes(tx, &mut arena, 2, 2, 2, "altered_table:1");
        let mut id2: u64 = 0;
        let mut err2 = MoraineError::default();
        // SAFETY: `tx` is live; outputs are valid local slots.
        let code =
            unsafe { moraine_tx_commit(tx, &raw mut id2, None, ptr::null_mut(), &raw mut err2) };
        // SAFETY: `err2.message` is null or was just written.
        assert_eq!(code, codes::OK, "commit failed: {:?}", unsafe {
            err2.message.as_ref()
        });
        assert_eq!(id2, 2);

        // The dump serves both versions: history `t` ended at 2, current `t2`.
        let mut rows: *mut crate::dumps::MoraineTableRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut dump_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            crate::dumps::moraine_dump_tables(
                handle,
                &raw mut rows,
                &raw mut len,
                None,
                ptr::null_mut(),
                &raw mut dump_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(len, 2);
        // SAFETY: just populated above with `len` live elements.
        let table_rows = unsafe { std::slice::from_raw_parts(rows, len) };
        let ended = table_rows.iter().find(|r| r.has_end_snapshot).unwrap();
        let live = table_rows.iter().find(|r| !r.has_end_snapshot).unwrap();
        assert_eq!(ended.end_snapshot, 2);
        // SAFETY: owned C strings written above, not yet freed.
        let live_name = unsafe { std::ffi::CStr::from_ptr(live.table_name) }
            .to_str()
            .unwrap();
        assert_eq!(live_name, "t2");

        // The stats row is gone (unversioned raw delete, no history mirror).
        let mut stats: *mut crate::dumps::MoraineTableStatsRow = ptr::null_mut();
        let mut stats_len: usize = 0;
        let mut stats_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            crate::dumps::moraine_dump_table_stats(
                handle,
                &raw mut stats,
                &raw mut stats_len,
                None,
                ptr::null_mut(),
                &raw mut stats_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(stats_len, 0);

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            crate::dumps::moraine_dump_tables_free(rows, len);
            crate::dumps::moraine_dump_table_stats_free(stats, stats_len);
            moraine_detach(handle);
        }
    }

    /// A lost race at commit is never retried: the loser's error carries
    /// the literal substring `conflict`, and the store reflects only the
    /// winner.
    #[test]
    fn lost_race_at_commit_is_not_retried_and_carries_conflict_text() {
        let dir = TempDir::new("race");
        let handle = attach_ok(dir.path());

        let tx_a = begin(handle);
        let tx_b = begin(handle);

        for (tx, name) in [(tx_a, "a"), (tx_b, "b")] {
            let mut arena = StrArena::new();
            // ducklake_schema: schema_id, schema_uuid, begin_snapshot,
            // end_snapshot, schema_name, path, path_is_relative.
            stage(
                tx,
                2,
                0,
                &[
                    u64_cell(1),
                    arena.cell(&format!("uuid-{name}")),
                    u64_cell(1),
                    null_cell(),
                    arena.cell(name),
                    arena.cell(&format!("{name}/")),
                    bool_cell(true),
                ],
            );
            stage(
                tx,
                0,
                0,
                &[
                    u64_cell(1),
                    i64_cell(1),
                    u64_cell(1),
                    u64_cell(2),
                    u64_cell(0),
                ],
            );
            stage(
                tx,
                1,
                0,
                &[
                    u64_cell(1),
                    arena.cell(&format!(r#"created_schema:"{name}""#)),
                    null_cell(),
                    null_cell(),
                    null_cell(),
                ],
            );
            // Leak `arena` to keep its `CString`s alive past every `stage`
            // call's borrowed pointers.
            std::mem::forget(arena);
        }

        let mut id_a: u64 = 0;
        let mut err_a = MoraineError::default();
        // SAFETY: `tx_a` is live; outputs are valid local slots.
        let code_a = unsafe {
            moraine_tx_commit(tx_a, &raw mut id_a, None, ptr::null_mut(), &raw mut err_a)
        };
        assert_eq!(code_a, codes::OK);

        let mut id_b: u64 = 0;
        let mut err_b = MoraineError::default();
        // SAFETY: `tx_b` is live; outputs are valid local slots.
        let code_b = unsafe {
            moraine_tx_commit(tx_b, &raw mut id_b, None, ptr::null_mut(), &raw mut err_b)
        };
        assert_eq!(code_b, codes::COMMIT_CONFLICT);
        assert_eq!(err_b.code, codes::COMMIT_CONFLICT);
        assert!(!err_b.message.is_null());
        // SAFETY: just populated above.
        let msg = unsafe { std::ffi::CStr::from_ptr(err_b.message) }
            .to_str()
            .unwrap();
        assert!(msg.contains("conflict"), "message: {msg}");

        // SAFETY: `err_b.message`/`handle` came from the calls above and
        // are each freed exactly once.
        unsafe {
            crate::abi::moraine_error_free(err_b.message);
            moraine_detach(handle);
        }
    }

    /// `moraine_tx_rollback` discards every staged row: nothing lands.
    #[test]
    fn rollback_discards_staged_rows() {
        let dir = TempDir::new("rollback");
        let handle = attach_ok(dir.path());
        let tx = begin(handle);

        let mut arena = StrArena::new();
        stage(
            tx,
            2,
            0,
            &[
                u64_cell(1),
                arena.cell("uuid-r"),
                u64_cell(1),
                null_cell(),
                arena.cell("rolled_back"),
                arena.cell("rolled_back/"),
                bool_cell(true),
            ],
        );

        // SAFETY: `tx` is live, not yet committed or rolled back.
        unsafe { moraine_tx_rollback(tx) };

        let mut rows: *mut crate::dumps::MoraineSchemaRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            crate::dumps::moraine_dump_schemas(
                handle,
                &raw mut rows,
                &raw mut len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        // Only bootstrap's `main` schema — the rolled-back row never
        // landed.
        assert_eq!(len, 1);

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            crate::dumps::moraine_dump_schemas_free(rows, len);
            moraine_detach(handle);
        }
    }

    /// A malformed staged row (wrong cell count) fails loudly as a
    /// corruption error at commit, not a panic.
    #[test]
    fn malformed_row_reports_corruption_not_a_panic() {
        let dir = TempDir::new("malformed");
        let handle = attach_ok(dir.path());
        let tx = begin(handle);

        // Far too few cells for `ducklake_schema`.
        stage(tx, 2, 0, &[u64_cell(1)]);
        stage(
            tx,
            0,
            0,
            &[
                u64_cell(1),
                i64_cell(1),
                u64_cell(1),
                u64_cell(2),
                u64_cell(0),
            ],
        );
        let mut arena = StrArena::new();
        stage(
            tx,
            1,
            0,
            &[
                u64_cell(1),
                arena.cell(""),
                null_cell(),
                null_cell(),
                null_cell(),
            ],
        );

        let mut snapshot_id: u64 = 0;
        let mut err = MoraineError::default();
        // SAFETY: `tx` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_tx_commit(
                tx,
                &raw mut snapshot_id,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::CORRUPTION);

        // SAFETY: `err.message`/`handle` came from the calls above and are
        // each freed exactly once.
        unsafe {
            crate::abi::moraine_error_free(err.message);
            moraine_detach(handle);
        }
    }

    /// A probe reporting an interrupt cancels BEGIN before anything is
    /// staged: out-param unwritten, and the same handle immediately opens
    /// a transaction once the probe is quiet.
    #[test]
    fn probe_cancels_tx_begin_then_quiet_probe_succeeds() {
        unsafe extern "C" fn probe_always(_probe_ctx: *mut c_void) -> bool {
            true
        }
        unsafe extern "C" fn probe_never(_probe_ctx: *mut c_void) -> bool {
            false
        }

        let dir = TempDir::new("probe-tx-begin");
        let handle = attach_ok(dir.path());

        let mut tx: *mut MoraineTxHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; `tx`/`err` are valid local slots;
        // the probes accept a null context.
        let code = unsafe {
            moraine_tx_begin(
                handle,
                &raw mut tx,
                Some(probe_always),
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::INTERRUPTED);
        assert_eq!(err.code, codes::INTERRUPTED);
        assert!(tx.is_null());
        // SAFETY: populated by the failed call above, freed exactly once.
        unsafe { crate::abi::moraine_error_free(err.message) };

        let mut tx2: *mut MoraineTxHandle = ptr::null_mut();
        let mut err2 = MoraineError::default();
        // SAFETY: same contracts as above.
        let code2 = unsafe {
            moraine_tx_begin(
                handle,
                &raw mut tx2,
                Some(probe_never),
                ptr::null_mut(),
                &raw mut err2,
            )
        };
        assert_eq!(code2, codes::OK);
        assert!(!tx2.is_null());

        // SAFETY: `tx2` consumed exactly once; `handle` freed exactly once.
        unsafe {
            moraine_tx_rollback(tx2);
            moraine_detach(handle);
        }
    }

    #[test]
    fn begin_on_null_handle_reports_invalid_argument() {
        let mut tx: *mut MoraineTxHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: a null `handle` is exactly the input this test exercises;
        // outputs are valid local slots.
        let code = unsafe {
            moraine_tx_begin(
                ptr::null_mut(),
                &raw mut tx,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::INVALID_ARGUMENT);
        assert!(tx.is_null());
        // SAFETY: `err.message` was just populated above and not yet freed.
        unsafe { crate::abi::moraine_error_free(err.message) };
    }

    #[test]
    fn rollback_on_null_tx_is_a_no_op() {
        // SAFETY: a null `tx` is exactly the input this test exercises,
        // documented as a no-op.
        unsafe { moraine_tx_rollback(ptr::null_mut()) };
    }

    #[test]
    fn stage_rejects_unknown_table_kind() {
        let dir = TempDir::new("bad-kind");
        let handle = attach_ok(dir.path());
        let tx = begin(handle);

        let mut err = MoraineError::default();
        // SAFETY: `tx` is live; empty cells slice; outputs are valid.
        let code = unsafe { moraine_tx_stage(tx, 99, 0, ptr::null(), 0, &raw mut err) };
        assert_eq!(code, codes::INVALID_ARGUMENT);

        // SAFETY: `err.message`/`tx`/`handle` came from the calls above.
        unsafe {
            crate::abi::moraine_error_free(err.message);
            moraine_tx_rollback(tx);
            moraine_detach(handle);
        }
    }

    /// The tx-aware snapshot dump observes the transaction's own staged
    /// snapshot deletes; the plain dump keeps serving committed state.
    #[test]
    fn tx_dump_snapshots_observes_staged_deletes() {
        let dir = TempDir::new("rywr");
        let handle = attach_ok(dir.path());

        // Seed snapshot 1 so the store holds {0, 1}.
        let tx = begin(handle);
        let mut arena = StrArena::new();
        stage_table_row(tx, &mut arena, 1, (1, None), 0, "t", "t/");
        stage_snapshot_and_changes(tx, &mut arena, 1, 1, 2, "created_table:\"main\".\"t\"");
        let mut snapshot_id: u64 = 0;
        let mut err = MoraineError::default();
        // SAFETY: `tx` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_tx_commit(
                tx,
                &raw mut snapshot_id,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);

        // Stage (but do not commit) an expiry of snapshot 0.
        let tx = begin(handle);
        stage(tx, 0, 1, &[u64_cell(0)]);

        let mut rows: *mut crate::dumps::MoraineSnapshotRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut dump_err = MoraineError::default();
        // SAFETY: `tx` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_tx_dump_snapshots(tx, &raw mut rows, &raw mut len, &raw mut dump_err)
        };
        assert_eq!(code, codes::OK);
        assert_eq!(len, 1, "the staged delete must hide snapshot 0");
        // SAFETY: just populated above.
        assert_eq!(unsafe { (*rows).snapshot_id }, 1);
        // SAFETY: freed exactly once.
        unsafe { crate::dumps::moraine_dump_snapshots_free(rows, len) };

        // Outside the transaction, committed state is unchanged.
        let mut committed: *mut crate::dumps::MoraineSnapshotRow = ptr::null_mut();
        let mut committed_len: usize = 0;
        let mut committed_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            crate::dumps::moraine_dump_snapshots(
                handle,
                &raw mut committed,
                &raw mut committed_len,
                None,
                ptr::null_mut(),
                &raw mut committed_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(committed_len, 2);
        // SAFETY: freed exactly once; `tx` rolled back exactly once.
        unsafe {
            crate::dumps::moraine_dump_snapshots_free(committed, committed_len);
            moraine_tx_rollback(tx);
            moraine_detach(handle);
        }
    }
}
