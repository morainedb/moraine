//! Inline data and deletion operations through the C ABI.

use moraine::ffi_support::staged::RowOperation;

use super::MoraineTxHandle;
#[cfg(doc)]
use super::moraine_tx_begin;
use crate::{
    abi::guard,
    arrow_ipc::MoraineArrowBytes,
    error::{AbiError, MoraineError, codes},
};

/// Stages an operation under the transaction lock and ABI error guard.
///
/// # Safety
///
/// A non-null `tx` must be live; a non-null `err` must be writable for this
/// call.
unsafe fn stage_operation(
    tx: *mut MoraineTxHandle,
    err: *mut MoraineError,
    operation: impl FnOnce() -> RowOperation,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() {
            return Err(AbiError::invalid_argument("`tx` is null"));
        }
        // SAFETY: caller guarantees a live transaction.
        let tx_ref = unsafe { &*tx };
        tx_ref.lock()?.try_stage(operation())?;
        Ok(())
    };

    // SAFETY: caller guarantees `err` is null or writable.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages `inline/schema`: the Arrow IPC schema for one `(table_id,
/// schema_version)`, written once at inline-table creation, consuming the
/// encoded buffer.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`] and
/// not yet committed or rolled back. `arrow_schema` must be an unconsumed
/// value returned by [`crate::arrow_ipc::moraine_arrow_encode_schema`].
/// `err`, if non-null, must be a valid, writable [`MoraineError`]. All for
/// the duration of this call.
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
    let (arrow_schema, _memory) = unsafe { arrow_schema.into_parts() };
    // SAFETY: `tx` and `err` follow this entry point's caller contract.
    unsafe {
        stage_operation(tx, err, || RowOperation::InlineSchema {
            table_id,
            schema_version,
            arrow_schema,
        })
    }
}

/// Consumes an encoded Arrow chunk body directly into the staged row.
///
/// # Safety
///
/// Same `tx`/`err` contract as [`moraine_tx_stage_inline_schema_owned`].
/// `arrow_body` must be an unconsumed value returned by
/// [`crate::arrow_ipc::moraine_arrow_encode_chunk`].
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
    let (arrow_body, _memory) = unsafe { arrow_body.into_parts() };
    // SAFETY: `tx` and `err` follow this entry point's caller contract.
    unsafe {
        stage_operation(tx, err, || RowOperation::InlineInsert {
            table_id,
            schema_version,
            begin_snapshot,
            row_id_start,
            row_count,
            arrow_body: arrow_body.into(),
        })
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
    // SAFETY: `tx` and `err` follow this entry point's caller contract.
    unsafe {
        stage_operation(tx, err, || RowOperation::InlineInlineDelete {
            table_id,
            row_id,
            end_snapshot,
        })
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
    // SAFETY: `tx` and `err` follow this entry point's caller contract.
    unsafe {
        stage_operation(tx, err, || RowOperation::InlineFileDelete {
            table_id,
            data_file_id,
            row_id,
            begin_snapshot,
        })
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
    // SAFETY: `tx` and `err` follow this entry point's caller contract.
    unsafe {
        stage_operation(tx, err, || RowOperation::InlineFileDeleteRemove {
            table_id,
            data_file_id,
            row_id,
        })
    }
}

/// One physical inline version selected for deletion.
#[repr(C)]
pub struct MoraineInlineRowVersion {
    /// Logical row identifier.
    pub row_id: u64,
    /// Snapshot that inserted this version.
    pub begin_snapshot: u64,
}

/// Stages exact inline-version deletion, preserving unselected rows.
///
/// # Safety
///
/// Same `tx`/`err` contract as [`moraine_tx_stage_inline_inline_delete`].
/// `rows` must address `len` readable elements for this call, or may be
/// null when `len` is zero. The elements are copied before returning.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage_inline_rows_delete(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    schema_version: u64,
    rows: *const MoraineInlineRowVersion,
    len: usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if tx.is_null() || (len != 0 && rows.is_null()) {
            return Err(AbiError::invalid_argument("null inline deletion input"));
        }
        let _memory = moraine::MemoryReservation::reserve(
            (len as u64).saturating_mul(std::mem::size_of::<(u64, u64)>() as u64),
        )?;
        let rows = if len == 0 {
            &[]
        } else {
            // SAFETY: the caller provides `len` readable elements.
            unsafe { std::slice::from_raw_parts(rows, len) }
        };
        let selected = rows
            .iter()
            .map(|row| (row.row_id, row.begin_snapshot))
            .collect();
        // SAFETY: the caller provides a live transaction, checked non-null.
        unsafe { &*tx }
            .lock()?
            .try_stage(RowOperation::InlineRowsDelete {
                table_id,
                schema_version,
                rows: selected,
            })?;
        Ok(())
    };
    // SAFETY: the caller provides a null or writable error pointer.
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
    // SAFETY: `tx` and `err` follow this entry point's caller contract.
    unsafe {
        stage_operation(tx, err, || RowOperation::InlineFlushDelete {
            table_id,
            schema_version,
            flush_snapshot,
        })
    }
}

/// Stages the table-wide form of
/// [`moraine_tx_stage_inline_file_delete_remove`]: removes every
/// `inline/file_delete` record for `table_id` in one operation.
///
/// Staged in place of the per-record calls when a clear matched every
/// record the table carries, which is what a flush's unqualified `DELETE`
/// against `ducklake_inlined_delete_<table_id>` always does.
///
/// # Safety
///
/// Same contract as [`moraine_tx_stage_inline_inline_delete`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage_inline_file_delete_clear(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: `tx` and `err` follow this entry point's caller contract.
    unsafe { stage_operation(tx, err, || RowOperation::InlineFileDeleteClear { table_id }) }
}

/// Stages a schema-version-scoped deregistration: removes only the
/// `inline/schema` record for `(table_id, schema_version)`, leaving any
/// other schema version's `inline/*` records untouched.
///
/// # Safety
///
/// `tx` must be a pointer previously returned by [`moraine_tx_begin`]
/// and not yet committed or rolled back. `err`, if non-null, must be a
/// valid, writable [`MoraineError`]. Both for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_stage_inline_schema_drop(
    tx: *mut MoraineTxHandle,
    table_id: u64,
    schema_version: u64,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: `tx` and `err` follow this entry point's caller contract.
    unsafe {
        stage_operation(tx, err, || RowOperation::InlineSchemaDrop {
            table_id,
            schema_version,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_null_transaction_does_not_construct_an_inline_operation() {
        let mut error = MoraineError::default();
        // SAFETY: null is a supported error input, and `error` is writable.
        let code = unsafe {
            stage_operation(std::ptr::null_mut(), &raw mut error, || {
                panic!("an invalid transaction must not construct an operation")
            })
        };
        assert_eq!(code, codes::INVALID_ARGUMENT);
        // SAFETY: the error message was just allocated and has not been freed.
        unsafe { crate::abi::moraine_error_free(error.message) };
    }
}
