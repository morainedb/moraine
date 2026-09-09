//! Located rows read back whole, at a pinned snapshot.

use std::{
    ffi::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
};

use super::{borrow_str, free_array, guard, resolve_table, write_array};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe, MoraineSnapshotHandle},
};

/// One batch of located rows, as [`moraine_rows_at`] returns them: a
/// self-describing Arrow IPC stream holding one record batch. Free the
/// array with [`moraine_rows_at_free`].
#[repr(C)]
pub struct MoraineRowBatch {
    /// The IPC bytes, owned.
    pub data: *mut u8,
    /// Length of `data` in bytes.
    pub len: usize,
    /// Capacity of the allocation behind `data`, retained for freeing.
    pub cap: usize,
}

impl MoraineRowBatch {
    fn from_vec(mut bytes: Vec<u8>) -> Self {
        bytes.shrink_to_fit();
        let batch = Self {
            data: bytes.as_mut_ptr(),
            len: bytes.len(),
            cap: bytes.capacity(),
        };
        std::mem::forget(bytes);
        batch
    }

    /// Reclaims the allocation, leaving the struct empty.
    ///
    /// # Safety
    ///
    /// `self` must have been minted by [`Self::from_vec`] and not reclaimed.
    unsafe fn reclaim(&mut self) {
        if self.data.is_null() {
            return;
        }
        // SAFETY: caller contract preserves the original allocation fields.
        drop(unsafe { Vec::from_raw_parts(self.data, self.len, self.cap) });
        self.data = ptr::null_mut();
        self.len = 0;
        self.cap = 0;
    }
}

/// Reads located rows back whole at `snapshot` (the catalog head when
/// null), without a scan. `pairs` are `(row_id, data_file_id)` as a lookup
/// reports them, `has_data_file_id` false naming a live inlined row.
///
/// Writes `out_items`/`out_len`: one [`MoraineRowBatch`] per batch, each an
/// Arrow IPC stream whose columns are the table's top-level columns at the
/// snapshot under their current names, then `row_id` (`UInt64`) and
/// `data_file_id` (`UInt64`, NULL for an inlined row). A row deleted at the
/// snapshot is omitted; a pair that cannot be positioned exactly fails the
/// call. Written even when empty; free exactly once with
/// [`moraine_rows_at_free`].
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `snapshot`, if
/// non-null, must be a live snapshot of `handle`'s catalog; `pairs` points
/// to `pairs_len` pairs; `out_items`/`out_len` must be non-null and
/// writable; `probe`/`probe_ctx` must satisfy the interrupt-probe contract;
/// `err`, if non-null, must be writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_rows_at(
    handle: *mut MoraineCatalogHandle,
    snapshot: *mut MoraineSnapshotHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    pairs: *const super::MorainePositionPair,
    pairs_len: usize,
    out_items: *mut *mut MoraineRowBatch,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<Vec<u8>>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        if pairs_len > 0 && pairs.is_null() {
            return Err(AbiError::invalid_argument("`pairs` is null"));
        }

        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        // SAFETY: caller contract — `pairs` points to `pairs_len` valid pairs.
        let raw_pairs = unsafe {
            if pairs_len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(pairs, pairs_len)
            }
        };
        let pairs: Vec<(u64, Option<moraine::DataFileId>)> = raw_pairs
            .iter()
            .map(|pair| {
                (
                    pair.row_id,
                    pair.has_data_file_id
                        .then(|| moraine::DataFileId::new(pair.data_file_id)),
                )
            })
            .collect();

        // SAFETY: caller contract for `snapshot`, `probe`, and `probe_ctx`.
        let view = unsafe { super::pinned_or_head(handle_ref, snapshot, probe, probe_ctx) }?;
        let table_id = resolve_table(&view, schema, table)?;

        let reads = handle_ref.catalog.reads();
        let read = reads.rows_at(
            &view,
            handle_ref.data_store.clone(),
            &handle_ref.data_prefix,
            table_id,
            &pairs,
        );
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let located = unsafe { handle_ref.block_on_cancellable(probe, probe_ctx, read) }?;
        Ok(located.batches)
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(batches) => {
            let batches: Vec<MoraineRowBatch> =
                batches.into_iter().map(MoraineRowBatch::from_vec).collect();
            // SAFETY: caller contract for the output pointers.
            unsafe { write_array(batches, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
    }
}

/// Frees the array [`moraine_rows_at`] wrote, including each batch's bytes.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written there by a
/// matching call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_rows_at_free(items: *mut MoraineRowBatch, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above; each batch is what `from_vec` minted.
        unsafe {
            free_array(items, len, |batch| batch.reclaim());
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}
