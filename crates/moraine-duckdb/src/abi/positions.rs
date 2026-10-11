//! Exact row positioning: turning `(row_id, data_file_id)` pairs into
//! the file offsets a positional delete names. Internal plumbing for the
//! automatic rewrites; no SQL surface names it.

use std::{
    ffi::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
};

use super::{MorainePositionPair, borrow_str, free_array, guard, resolve_table, write_array};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe, MoraineSnapshotHandle},
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

/// One [`MoraineLocatedFile`] with every owned piece still a Rust value,
/// converted to raw pointers only in [`moraine_locate_row_positions`]'s
/// final, infallible pass — so a fallible step failing after this is built
/// leaks nothing.
struct LocatedFileOwned {
    data_file_id: u64,
    positions: Vec<u64>,
    existing: Option<Vec<u64>>,
}

impl LocatedFileOwned {
    /// Converts to the C representation, minting every raw pointer this
    /// call owns. Infallible: called only after every fallible step of the
    /// same request has already succeeded.
    fn into_raw(self) -> MoraineLocatedFile {
        let (positions, positions_len) = into_c_array(self.positions);
        let (has_existing_delete, existing_positions, existing_len) = match self.existing {
            Some(positions) => {
                let (ptr, len) = into_c_array(positions);
                (true, ptr, len)
            }
            None => (false, ptr::null_mut(), 0),
        };
        MoraineLocatedFile {
            data_file_id: self.data_file_id,
            positions,
            positions_len,
            has_existing_delete,
            existing_positions,
            existing_positions_len: existing_len,
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

/// Resolves located rows to exact file positions for deletion without a
/// scan. `pairs` are `(row_id, data_file_id)` as a lookup reports them,
/// `has_data_file_id` false naming a live inlined row. Resolution is
/// against `snapshot` when non-null, else the catalog head.
///
/// Writes `out_files` (one entry per data file carrying a requested
/// position, each with its own positions and whatever delete file is
/// already registered against it) and `out_inlined` (row ids resolved as
/// live inlined rows, from `pairs` entries naming no file). Both arrays are
/// written even when empty, and each must be freed with its own matching
/// `_free` function exactly once.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `snapshot`, if
/// non-null, must be a live snapshot of `handle`'s catalog; `pairs` points
/// to `pairs_len` pairs; every `out_*` pointer must be non-null and
/// writable; `probe`/`probe_ctx` must satisfy the interrupt-probe contract;
/// `err`, if non-null, must be writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_locate_row_positions(
    handle: *mut MoraineCatalogHandle,
    snapshot: *mut MoraineSnapshotHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    pairs: *const MorainePositionPair,
    pairs_len: usize,
    out_files: *mut *mut MoraineLocatedFile,
    out_files_len: *mut usize,
    out_inlined: *mut *mut u64,
    out_inlined_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    type Output = (Vec<LocatedFileOwned>, Vec<u64>);

    let attempt = || -> Result<Output, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_files.is_null()
            || out_files_len.is_null()
            || out_inlined.is_null()
            || out_inlined_len.is_null()
        {
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

        let reads = handle_ref.catalog.reads();
        // SAFETY: caller contract for `snapshot`, `probe`, and `probe_ctx`.
        let view = unsafe { super::pinned_or_head(handle_ref, snapshot, probe, probe_ctx) }?;
        let table_id = resolve_table(&view, schema, table)?;

        let data_store = handle_ref.data_store.clone();
        let locate = reads.locate_row_positions_at(
            &view,
            data_store,
            &handle_ref.data_prefix,
            table_id,
            &pairs,
        );
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let located = unsafe { handle_ref.block_on_cancellable(probe, probe_ctx, locate) }?;

        let files = located
            .deletions
            .into_iter()
            .map(|deletion| LocatedFileOwned {
                data_file_id: deletion.data_file_id.get(),
                positions: deletion.positions,
                existing: deletion.existing_delete.map(|existing| existing.positions),
            })
            .collect();

        Ok((files, located.inlined_rows))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok((files, inlined)) => {
            // Every fallible step above already succeeded; only raw
            // pointers are minted from here on, so nothing below can leak.
            let files: Vec<MoraineLocatedFile> =
                files.into_iter().map(LocatedFileOwned::into_raw).collect();
            // SAFETY: caller contract for the output pointers.
            unsafe {
                write_array(files, out_files, out_files_len);
                write_array(inlined, out_inlined, out_inlined_len);
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
        // this same call's `into_raw` minted via `into_c_array`.
        unsafe {
            free_array(items, len, |item| {
                free_array(item.positions, item.positions_len, |_| {});
                free_array(item.existing_positions, item.existing_positions_len, |_| {});
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Frees the array [`moraine_locate_row_positions`] wrote to
/// `out_inlined`/`out_inlined_len`.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written there by a
/// matching call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_locate_row_positions_free_inlined(items: *mut u64, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above. Plain values own no heap.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}
