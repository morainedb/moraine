//! Owned cursors for projected, summary-driven scans.

use std::{
    ffi::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::{MorainePositionPair, MoraineRowBatch, borrow_str, guard, resolve_table, write_array};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe, MoraineSnapshotHandle},
};

/// An execution-owned cursor, including its pinned read surface and runtime.
pub struct MoraineRowScan {
    handle: MoraineCatalogHandle,
    scan: moraine::LocatedRowScan,
}

/// Opens a projected selective scan; data batches are read by
/// `moraine_row_scan_next`.
///
/// # Safety
/// `handle` and `snapshot` must be live and refer to the same pinned read
/// scope. Strings and arrays must be valid for their lengths; `out` must be
/// writable. Cancellation and error pointers follow the catalog ABI contract.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_row_scan_open(
    handle: *mut MoraineCatalogHandle,
    snapshot: *mut MoraineSnapshotHandle,
    schema: *const c_char,
    table: *const c_char,
    pairs: *const MorainePositionPair,
    pairs_len: usize,
    columns: *const *const c_char,
    columns_len: usize,
    out: *mut *mut MoraineRowScan,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || {
        if handle.is_null()
            || snapshot.is_null()
            || out.is_null()
            || (pairs_len > 0 && pairs.is_null())
            || (columns_len > 0 && columns.is_null())
        {
            return Err(AbiError::invalid_argument("null selective scan argument"));
        }
        // SAFETY: caller supplies live handles and valid strings and arrays.
        let (handle, schema, table, pairs, columns) = unsafe {
            (
                &*handle,
                borrow_str(schema, "schema")?,
                borrow_str(table, "table")?,
                if pairs_len == 0 {
                    &[]
                } else {
                    std::slice::from_raw_parts(pairs, pairs_len)
                },
                if columns_len == 0 {
                    &[]
                } else {
                    std::slice::from_raw_parts(columns, columns_len)
                },
            )
        };
        let columns = columns
            .iter()
            .map(|&name| {
                // SAFETY: caller supplies valid column strings.
                unsafe { borrow_str(name, "column") }.map(str::to_owned)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let pairs: Vec<_> = pairs
            .iter()
            .map(|pair| {
                (
                    pair.row_id,
                    pair.has_data_file_id
                        .then(|| moraine::DataFileId::new(pair.data_file_id)),
                )
            })
            .collect();
        // SAFETY: caller supplies the pinned snapshot and cancellation pointers.
        let view = unsafe { super::pinned_or_head(handle, snapshot, probe, probe_ctx) }?;
        let table = resolve_table(&view, schema, table)?;
        let reads = handle.catalog.reads();
        let open = reads.scan_rows_at(
            &view,
            handle.data_store.clone(),
            &handle.data_prefix,
            table,
            &pairs,
            &columns,
        );
        // SAFETY: caller supplies the cancellation callback's lifetime and thread
        // safety.
        let scan = unsafe { handle.block_on_cancellable(probe, probe_ctx, open) }?;
        let cursor = Box::new(MoraineRowScan {
            handle: handle.read_alias(reads.clone()),
            scan,
        });
        // SAFETY: checked non-null; caller supplies writable output storage.
        unsafe {
            *out = Box::into_raw(cursor);
        }
        Ok(())
    };
    // SAFETY: caller supplies writable error storage when non-null.
    unsafe { guard(err, attempt) }.map_or_else(|code| code, |()| codes::OK)
}

/// Returns zero or one IPC batch; free the returned array with
/// `moraine_rows_at_free`.
///
/// # Safety
/// `scan` must be live and exclusively accessed, and outputs writable.
/// Cancellation and error pointers follow the catalog ABI contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_row_scan_next(
    scan: *mut MoraineRowScan,
    out: *mut *mut MoraineRowBatch,
    len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || {
        if scan.is_null() || out.is_null() || len.is_null() {
            return Err(AbiError::invalid_argument("null selective scan argument"));
        }
        // SAFETY: caller supplies exclusive access to a live cursor.
        let scan = unsafe { &mut *scan };
        // SAFETY: caller supplies valid cancellation pointers.
        let batch = unsafe {
            scan.handle
                .block_on_cancellable(probe, probe_ctx, scan.scan.next_batch())
        }?;
        let batches = batch.into_iter().map(MoraineRowBatch::from_vec).collect();
        // SAFETY: caller supplies writable outputs.
        unsafe {
            write_array(batches, out, len);
        }
        Ok(())
    };
    // SAFETY: caller supplies writable error storage when non-null.
    unsafe { guard(err, attempt) }.map_or_else(|code| code, |()| codes::OK)
}

/// Returns the number of data files opened by the selective scan.
///
/// # Safety
/// `scan` must be null or live, with no concurrent `moraine_row_scan_next`
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_row_scan_files_read(scan: *const MoraineRowScan) -> u64 {
    // SAFETY: caller supplies a live cursor when non-null.
    unsafe { scan.as_ref() }.map_or(0, |scan| scan.scan.files_read())
}

/// Closes a selective scan and releases its pinned read scope.
///
/// # Safety
/// `scan` must be null or an unfreed pointer returned by
/// `moraine_row_scan_open`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_row_scan_free(scan: *mut MoraineRowScan) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !scan.is_null() {
            // SAFETY: caller transfers ownership of the original allocation.
            drop(unsafe { Box::from_raw(scan) });
        }
    }));
}
