//! Owned cursors for projected, summary-driven scans.

use std::{
    ffi::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
};

use arrow::{
    array::{Array, RecordBatch, StructArray},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, to_ffi},
};

use super::{MorainePositionPair, borrow_str, guard, resolve_table};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe, MoraineSnapshotHandle},
};

/// An execution-owned cursor, including its pinned read surface and runtime.
pub struct MoraineRowScan {
    // Abort readers before releasing the runtime that owns their tasks.
    scan: moraine::LocatedRowScan,
    handle: MoraineCatalogHandle,
}

/// Opens a projected selective scan; data batches are read by
/// `moraine_row_scan_next_arrow`.
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
    // SAFETY: forwards the caller's handles, buffers and cancellation contract.
    unsafe {
        open_scan(
            handle,
            snapshot,
            schema,
            table,
            pairs,
            pairs_len,
            columns,
            columns_len,
            false,
            out,
            probe,
            probe_ctx,
            err,
        )
    }
}

/// Opens a whole-row cursor that rejects every unpositionable pair.
/// Payload batches are decoded by `moraine_row_scan_next_arrow`.
///
/// # Safety
/// Handles must be live and share a pinned scope. Strings and pairs must be
/// valid for their lengths, and `out` writable. Cancellation and error
/// pointers follow the catalog ABI contract.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_rows_at_open(
    handle: *mut MoraineCatalogHandle,
    snapshot: *mut MoraineSnapshotHandle,
    schema: *const c_char,
    table: *const c_char,
    pairs: *const MorainePositionPair,
    pairs_len: usize,
    out: *mut *mut MoraineRowScan,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwards the caller's contract; strict cursors resolve all columns.
    unsafe {
        open_scan(
            handle,
            snapshot,
            schema,
            table,
            pairs,
            pairs_len,
            std::ptr::null(),
            0,
            true,
            out,
            probe,
            probe_ctx,
            err,
        )
    }
}

#[allow(clippy::too_many_arguments)]
unsafe fn open_scan(
    handle: *mut MoraineCatalogHandle,
    snapshot: *mut MoraineSnapshotHandle,
    schema: *const c_char,
    table: *const c_char,
    pairs: *const MorainePositionPair,
    pairs_len: usize,
    columns: *const *const c_char,
    columns_len: usize,
    strict: bool,
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
        let open = async {
            if strict {
                reads
                    .scan_rows_at_strict(
                        &view,
                        handle.data_store.clone(),
                        &handle.data_prefix,
                        table,
                        &pairs,
                    )
                    .await
            } else {
                reads
                    .scan_rows_at(
                        &view,
                        handle.data_store.clone(),
                        &handle.data_prefix,
                        table,
                        &pairs,
                        &columns,
                    )
                    .await
            }
        };
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

fn export_batch(batch: RecordBatch) -> Result<(FFI_ArrowArray, FFI_ArrowSchema), AbiError> {
    to_ffi(&StructArray::from(batch).to_data())
        .map_err(|error| AbiError::new(codes::INTERNAL, format!("selective Arrow export: {error}")))
}

/// Estimates projected page coverage without reading payload columns.
///
/// # Safety
/// `scan` is live and exclusive, `out` writable; cancellation/error pointers
/// follow the catalog ABI contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_row_scan_is_selective(
    scan: *mut MoraineRowScan,
    out: *mut bool,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || {
        if scan.is_null() || out.is_null() {
            return Err(AbiError::invalid_argument("null scan estimate argument"));
        }
        // SAFETY: caller supplies a live exclusive cursor.
        let scan = unsafe { &mut *scan };
        // SAFETY: caller supplies valid cancellation pointers.
        let selective = unsafe {
            scan.handle
                .block_on_cancellable(probe, probe_ctx, scan.scan.prefers_selective_reads())
        }?;
        // SAFETY: caller supplies writable output.
        unsafe {
            *out = selective;
        }
        Ok(())
    };
    // SAFETY: caller supplies writable error storage when non-null.
    unsafe { guard(err, attempt) }.map_or_else(|code| code, |()| codes::OK)
}

/// Data-store bytes and ranges read by a cursor, excluding cache hits.
#[repr(C)]
#[derive(Default)]
pub struct MoraineRowScanMetrics {
    /// Payload/metadata bytes fetched.
    pub bytes_read: u64,
    /// Requested ranges before store-side coalescing.
    pub ranges_read: u64,
    /// Peak simultaneous read-unit workers (zero for serial scans).
    pub peak_workers: usize,
    /// Active batch polling time, excluding asynchronous waits.
    pub decode_seconds: f64,
    /// Range-fetch elapsed time, summed across workers.
    pub fetch_seconds: f64,
}

/// Samples cursor-local data-store reads.
///
/// # Safety
/// `scan` must be null or live, with no concurrent cursor call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_row_scan_metrics(
    scan: *const MoraineRowScan,
) -> MoraineRowScanMetrics {
    // SAFETY: caller supplies a live cursor when non-null.
    unsafe { scan.as_ref() }.map_or_else(MoraineRowScanMetrics::default, |scan| {
        MoraineRowScanMetrics {
            bytes_read: scan.scan.bytes_read(),
            ranges_read: scan.scan.ranges_read(),
            peak_workers: scan.scan.peak_workers(),
            decode_seconds: scan.scan.decode_seconds(),
            fetch_seconds: scan.scan.fetch_seconds(),
        }
    })
}

/// Sets the per-scan prefetch ceiling before its first batch, additionally
/// bounded by the core process-wide worker limit.
///
/// # Safety
/// `scan` is live and exclusively accessed; `err` is writable when non-null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_row_scan_parallelism(
    scan: *mut MoraineRowScan,
    maximum: usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || {
        // SAFETY: caller supplies a live cursor when non-null.
        let scan =
            unsafe { scan.as_mut() }.ok_or_else(|| AbiError::invalid_argument("null scan"))?;
        scan.scan.set_parallelism(maximum)?;
        Ok(())
    };
    // SAFETY: caller supplies writable error storage when non-null.
    unsafe { guard(err, attempt) }.map_or_else(|code| code, |()| codes::OK)
}

/// Exports the next batch without IPC; `has_batch=false` means end of stream.
/// Returned buffers outlive the cursor and are owned by Arrow release
/// callbacks.
///
/// # Safety
/// `scan` is live and exclusive; outputs are writable, empty Arrow slots.
/// The caller releases each exported array and schema exactly once.
/// Cancellation and error pointers follow the catalog ABI contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_row_scan_next_arrow(
    scan: *mut MoraineRowScan,
    out_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    has_batch: *mut bool,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || {
        if scan.is_null() || out_schema.is_null() || out_array.is_null() || has_batch.is_null() {
            return Err(AbiError::invalid_argument("null selective Arrow argument"));
        }
        // SAFETY: caller supplies exclusive cursor access and writable outputs.
        unsafe {
            *has_batch = false;
        }
        // SAFETY: the cursor and cancellation pointers are live for this call.
        let scan = unsafe { &mut *scan };
        // SAFETY: caller supplies valid cancellation pointers.
        let batch = unsafe {
            scan.handle
                .block_on_cancellable(probe, probe_ctx, scan.scan.next_record_batch())
        }?;
        if let Some(batch) = batch {
            let (array, schema) = export_batch(batch)?;
            // SAFETY: empty output slots receive ownership of the exported pair.
            unsafe {
                std::ptr::write(out_schema, schema);
                std::ptr::write(out_array, array);
                *has_batch = true;
            }
        }
        Ok(())
    };
    // SAFETY: caller supplies writable error storage when non-null.
    unsafe { guard(err, attempt) }.map_or_else(|code| code, |()| codes::OK)
}

#[cfg(test)]
mod tests;

/// Returns the number of data files opened by the selective scan.
///
/// # Safety
/// `scan` must be null or live, with no concurrent
/// `moraine_row_scan_next_arrow` call.
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
