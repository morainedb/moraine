//! Staged DuckLake writes over the C ABI.
//!
//! Calls decode [`MoraineCell`]s into core [`Cell`]s and accumulate operations
//! until [`moraine_tx_commit`] or [`moraine_tx_rollback`] consumes the
//! transaction. Commit conflicts surface without internal retry. The owning
//! catalog and its runtime must outlive every [`MoraineTxHandle`].

mod dumps;
mod inline;

use std::{
    ffi::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Mutex, MutexGuard},
};

pub use dumps::*;
pub use inline::*;
use moraine::ffi_support::staged::{
    Cell, RowOperation, StagedTransaction, TableKind, staged_begin,
};
use tracing::warn;

use crate::{
    abi::{borrow_str, guard},
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
///
/// The transaction sits behind a mutex: DuckDB drives concurrent pipelines
/// through one handle — a Sink staging while another scan's init dumps —
/// and `stage` mutates the overlay the dumps read.
pub struct MoraineTxHandle {
    catalog: *const MoraineCatalogHandle,
    tx: Mutex<StagedTransaction>,
}

impl MoraineTxHandle {
    /// The staged transaction, exclusively. A poisoned lock (a panic in an
    /// earlier call, already contained) makes the staged state suspect, so
    /// it is refused rather than served.
    fn lock(&self) -> Result<MutexGuard<'_, StagedTransaction>, AbiError> {
        self.tx.lock().map_err(|_| {
            AbiError::new(
                codes::INTERNAL,
                "staged transaction unusable after an earlier panic",
            )
        })
    }
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
/// Refuses a caller that has already cancelled with
/// [`codes::INTERRUPTED`], leaving `*out` unwritten and nothing staged.
/// Opening the transaction reaches no object store, so `probe` is read
/// once rather than raced.
///
/// # Safety
///
/// `handle` must be a pointer previously returned by
/// [`moraine_attach_with_wal_store`](crate::abi::moraine_attach_with_wal_store)
/// and not yet detached, and must outlive every operation on the returned
/// transaction handle (through [`moraine_tx_commit`] or
/// [`moraine_tx_rollback`]). `out` must be a valid, writable `*mut *mut
/// MoraineTxHandle`. `probe`, if non-null, must be safe to call with
/// `probe_ctx` from any thread. `err`, if non-null, must be a valid, writable
/// [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_tx_begin(
    handle: *mut MoraineCatalogHandle,
    snapshot: *const crate::runtime::MoraineSnapshotHandle,
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
        // SAFETY: caller contract for `snapshot`: null, or a live handle.
        let read_view =
            unsafe { snapshot.as_ref() }.map(|snapshot| std::sync::Arc::clone(&snapshot.snapshot));
        // Opening the transaction reaches no object store, so there is
        // nothing to race the probe against — only a caller who has
        // already cancelled to refuse.
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        if unsafe { crate::runtime::already_cancelled(probe, probe_ctx) } {
            return Err(AbiError::interrupted());
        }
        let tx = staged_begin(
            handle_ref.catalog.writer()?,
            handle_ref.data_store.clone(),
            handle_ref.data_prefix.clone(),
            read_view,
        )?;
        Ok(Box::new(MoraineTxHandle {
            catalog: handle,
            tx: Mutex::new(tx),
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
        let tx_ref = unsafe { &*tx };
        tx_ref.lock()?.stage(op);
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
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
/// - Before the core future is polled: [`codes::INTERRUPTED`], catalog
///   unchanged.
/// - After polling starts: cancellation returns
///   [`codes::COMMIT_OUTCOME_UNKNOWN`]. A submitted write keeps running; retain
///   its files and reconcile catalog state before resubmitting. Cancellation
///   during preparation is conservatively reported the same way.
/// - A completed write reports its snapshot id normally.
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
        let tx = match tx.into_inner() {
            Ok(tx) => tx,
            // A panic in an earlier call (already contained) left the
            // staged state suspect: discard it rather than commit it.
            Err(poisoned) => {
                poisoned.into_inner().rollback();
                return Err(AbiError::new(
                    codes::INTERNAL,
                    "staged transaction unusable after an earlier panic; rolled back",
                ));
            }
        };
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
        let report =
            unsafe { catalog_ref.block_on_commit(probe, probe_ctx, tx.commit_reporting()) }?;
        catalog_ref.spawn_commit_passes(warm_tables, report.snapshot_id);

        // Repair runs only when this commit deferred entries, after the data
        // snapshot is durable; its failure, including cancellation, is not
        // a commit failure. A repair that fails resumes on the next
        // deferring commit or during maintenance.
        if !report.deferred_indexes.is_empty() {
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
                        warn!(error = ?error, "deferred index repair will resume later");
                    }
                }
                Err(error) => {
                    warn!(error = %error, "deferred index repair will resume later");
                }
            }
        }

        Ok(report.snapshot_id.get())
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
        let catalog = boxed.catalog;
        // A rollback discards either way, so a poisoned lock is drained
        // rather than refused.
        match boxed.tx.into_inner() {
            Ok(tx) => tx.rollback(),
            Err(poisoned) => poisoned.into_inner().rollback(),
        }
        // The host resolves its own conflicts against the head it read from
        // this catalog, and refuses a commit before it ever reaches one. A
        // refusal decided against a stale head would otherwise be decided
        // the same way forever, because only a commit refreshes the head and
        // the refusal is what stops one happening. Forgetting it here costs
        // a rescan on a path that already failed.
        if !catalog.is_null() {
            // SAFETY: the handle's catalog outlives every transaction opened
            // against it, which is this pointer's own contract.
            unsafe { (*catalog).catalog.reads().forget_head_view() };
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

#[cfg(test)]
mod tests;
