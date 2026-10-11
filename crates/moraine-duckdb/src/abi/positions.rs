//! Exact row positioning: turning `(row_id, data_file_id)` pairs into
//! the file offsets a positional delete names. Internal plumbing for the
//! automatic rewrites; no SQL surface names it.

use std::{
    ffi::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
};

use super::{MoraineRowId, borrow_str, free_array, guard, resolve_table, write_array};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One data file carrying located positions, as
/// [`moraine_locate_row_positions`] groups its answer. Free with
/// [`moraine_locate_row_positions_free_files`].
#[repr(C)]
pub struct MoraineLocatedFile {
    /// The data file these positions are within.
    pub data_file_id: u64,
    /// Newly requested positions within this file, ascending and
    /// duplicate-free, owned.
    pub positions: *mut u64,
    /// Length of `positions`.
    pub positions_len: usize,
    /// Whether a delete file is currently registered against this data
    /// file; `existing_positions` is
    /// meaningful only when set.
    pub has_existing_delete: bool,
    /// Positions the registered delete file already carries, ascending
    /// and duplicate-free, owned.
    pub existing_positions: *mut u64,
    /// Length of `existing_positions`.
    pub existing_positions_len: usize,
}

impl From<moraine::LocatedDeletion> for MoraineLocatedFile {
    fn from(deletion: moraine::LocatedDeletion) -> Self {
        let (positions, positions_len) = into_c_array(deletion.positions);
        let has_existing_delete = deletion.existing_positions.is_some();
        let (existing_positions, existing_positions_len) = deletion
            .existing_positions
            .map_or((ptr::null_mut(), 0), into_c_array);
        Self {
            data_file_id: deletion.data_file_id.get(),
            positions,
            positions_len,
            has_existing_delete,
            existing_positions,
            existing_positions_len,
        }
    }
}

/// Hands a `Vec<T>` to C as an owned (pointer, length) pair, without
/// writing through an out-parameter — for an array embedded in a struct
/// field rather than a top-level ABI output. Reclaimed the same way
/// [`write_array`]'s output is, via [`free_array`].
fn into_c_array<T>(items: Vec<T>) -> (*mut T, usize) {
    let boxed = items.into_boxed_slice();
    let len = boxed.len();
    (Box::into_raw(boxed).cast::<T>(), len)
}

/// Resolves index hits in committed files to exact positions for deletion.
/// The handle must pin the scan's catalog revision. Free the returned files
/// with [`moraine_locate_row_positions_free_files`].
///
/// # Safety
/// Every pointer must satisfy the ABI contract; `pairs` points to `pairs_len`
/// hits, output pointers are non-null and writable, and `probe`/`probe_ctx`
/// satisfy the interrupt-probe contract.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_locate_row_positions(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    pairs: *const MoraineRowId,
    pairs_len: usize,
    out_files: *mut *mut MoraineLocatedFile,
    out_files_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<moraine::LocatedDeletion>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_files.is_null() || out_files_len.is_null() {
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
        if raw_pairs.iter().any(|pair| !pair.has_data_file_id) {
            return Err(AbiError::invalid_argument(
                "positions require committed files",
            ));
        }
        let pairs: Vec<_> = raw_pairs
            .iter()
            .map(|pair| (pair.value, moraine::DataFileId::new(pair.data_file_id)))
            .collect();

        let reads = handle_ref.catalog.reads();
        // SAFETY: caller contract for `probe` and `probe_ctx`.
        let view = unsafe { handle_ref.block_on_cancellable(probe, probe_ctx, reads.snapshot()) }?;
        let table_id = resolve_table(&view, schema, table)?;

        let data_store = handle_ref.data_store.as_ref().ok_or_else(|| {
            AbiError::invalid_argument("position resolution requires a data store")
        })?;
        let locate = reads.locate_row_positions_at(
            &view,
            data_store,
            &handle_ref.data_prefix,
            table_id,
            &pairs,
        );
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        unsafe { handle_ref.block_on_cancellable(probe, probe_ctx, locate) }
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(files) => {
            // Every fallible step above already succeeded; only raw
            // pointers are minted from here on, so nothing below can leak.
            let files: Vec<MoraineLocatedFile> =
                files.into_iter().map(MoraineLocatedFile::from).collect();
            // SAFETY: caller contract for the output pointers.
            unsafe {
                write_array(files, out_files, out_files_len);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Frees the array [`moraine_locate_row_positions`] wrote to
/// `out_files`/`out_files_len`, including each file's nested positions
/// arrays.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written there by a
/// matching call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_locate_row_positions_free_files(
    items: *mut MoraineLocatedFile,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above; each nested array is exactly what
        // this call allocated via `into_c_array`.
        unsafe {
            free_array(items, len, |item| {
                free_array(item.positions, item.positions_len, |_| {});
                free_array(item.existing_positions, item.existing_positions_len, |_| {});
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}
