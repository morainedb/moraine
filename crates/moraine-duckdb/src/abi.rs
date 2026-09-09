//! The C ABI: `extern "C"` entry points the C++ shim calls into. Every
//! function here does the same four things, in order: null
//! checks, UTF-8 validation, a `catch_unwind`-guarded body that
//! `block_on`s into [`moraine`], and translation of the outcome into a
//! `(code, message)` pair (see [`crate::error`]).
//!
//! Two owned, opaque handle types cross the boundary as raw pointers:
//! [`MoraineCatalogHandle`] (one tokio runtime plus one open [`Catalog`]
//! per `ATTACH`) and [`MoraineSnapshotHandle`] (one materialized
//! [`CatalogSnapshot`] per `moraine_snapshot` call). Listing calls return
//! heap-allocated arrays of C descriptor structs; each has a paired
//! `_free` function that must be called exactly once.
//!
//! [`Catalog`]: moraine::Catalog
//! [`CatalogSnapshot`]: moraine::CatalogSnapshot

mod attach;
mod deletion;
mod indexes;
mod located_rows;
mod lookup;
mod maintenance;
mod snapshot;

#[cfg(test)]
mod tests;

mod checkpoints;

use std::{
    ffi::{CString, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
};

pub use attach::*;
pub use checkpoints::*;
pub use deletion::*;
pub use indexes::*;
pub use located_rows::*;
pub use lookup::*;
pub use maintenance::*;
pub use snapshot::*;

use crate::{
    error::{AbiError, INTERNAL_PANIC_MESSAGE, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineSnapshotHandle},
};

/// The view a read resolves against: `snapshot`'s when non-null, else the
/// catalog head read through `handle`.
///
/// # Safety
///
/// `snapshot`, if non-null, must point to a live [`MoraineSnapshotHandle`];
/// `probe`/`probe_ctx` must satisfy the interrupt-probe contract.
pub(crate) unsafe fn pinned_or_head(
    handle: &MoraineCatalogHandle,
    snapshot: *mut MoraineSnapshotHandle,
    probe: crate::runtime::MoraineInterruptProbe,
    probe_ctx: *mut std::ffi::c_void,
) -> Result<std::sync::Arc<moraine::CatalogSnapshot>, AbiError> {
    if snapshot.is_null() {
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        return unsafe {
            handle.block_on_cancellable(probe, probe_ctx, handle.catalog.reads().snapshot())
        };
    }
    // SAFETY: caller contract for `snapshot`.
    let snapshot = unsafe { &*snapshot };
    Ok(std::sync::Arc::clone(&snapshot.snapshot))
}

/// Runs `body`, containing any panic and turning both panics and `Err`
/// results into a `(code, message)` pair written to `err`.
///
/// # Safety
///
/// `err`, if non-null, must point to a valid, writable [`MoraineError`]
/// for the duration of this call.
pub(crate) unsafe fn guard<T>(
    err: *mut MoraineError,
    body: impl FnOnce() -> Result<T, AbiError>,
) -> Result<T, i32> {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(abi_err)) => {
            let code = abi_err.code;
            // SAFETY: `err` forwarded unchanged under this function's contract.
            unsafe {
                abi_err.write_into(err);
            }
            Err(code)
        }
        Err(_panic) => {
            // SAFETY: same as above.
            unsafe {
                AbiError::new(codes::INTERNAL, INTERNAL_PANIC_MESSAGE).write_into(err);
            }
            Err(codes::INTERNAL)
        }
    }
}

/// Converts a Rust string to an owned [`CString`].
///
/// An embedded NUL byte is reported as [`codes::CORRUPTION`] rather than
/// panicking.
pub(crate) fn to_c_string(s: impl Into<Vec<u8>>) -> Result<CString, AbiError> {
    CString::new(s).map_err(|error| {
        AbiError::new(
            codes::CORRUPTION,
            format!(
                "catalog string contains an embedded NUL byte: {:?}",
                String::from_utf8_lossy(&error.into_vec())
            ),
        )
    })
}

/// Frees a C string previously minted via `CString::into_raw`, if
/// non-null.
///
/// # Safety
///
/// `ptr`, if non-null, must be a pointer previously returned by
/// `CString::into_raw` and not yet freed.
pub(crate) unsafe fn free_c_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: caller contract above.
    drop(unsafe { CString::from_raw(ptr) });
}

/// Hands a `Vec<T>` to C as a heap array: writes the (pointer, length)
/// pair through `out_items`/`out_len`. Callers build `items` owned-first:
/// every `CString` in the batch converts before any raw pointer is minted,
/// so a partial failure leaks nothing.
///
/// # Safety
///
/// `out_items` and `out_len` must be valid, writable pointers for the
/// duration of this call.
pub(crate) unsafe fn write_array<T>(items: Vec<T>, out_items: *mut *mut T, out_len: *mut usize) {
    let boxed = items.into_boxed_slice();
    let len = boxed.len();
    let ptr = Box::into_raw(boxed).cast::<T>();
    // SAFETY: caller contract above.
    unsafe {
        *out_len = len;
        *out_items = ptr;
    }
}

/// Reclaims an array written by [`write_array`], running `drop_elem` on
/// every element first (to release any owned C strings inside) before
/// dropping the backing allocation.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`write_array`] call, not yet freed.
pub(crate) unsafe fn free_array<T>(items: *mut T, len: usize, mut drop_elem: impl FnMut(&mut T)) {
    if items.is_null() {
        return;
    }
    // SAFETY: caller contract above.
    let slice = unsafe { std::slice::from_raw_parts_mut(items, len) };
    for elem in &mut *slice {
        drop_elem(elem);
    }
    let raw_slice = ptr::slice_from_raw_parts_mut(items, len);
    // SAFETY: reconstructs the exact `Box<[T]>` `write_array` produced.
    drop(unsafe { Box::from_raw(raw_slice) });
}

/// The shared shell of a snapshot list export: null-check the outputs,
/// borrow the snapshot, run `produce` under the panic/error guard, and write
/// the array (see [`write_array`] for the owned-first rule on `produce`).
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `err`, if non-null, must
/// be writable.
unsafe fn snapshot_list<Row>(
    snapshot: *mut MoraineSnapshotHandle,
    out_items: *mut *mut Row,
    out_len: *mut usize,
    err: *mut MoraineError,
    produce: impl FnOnce(&moraine::CatalogSnapshot) -> Result<Vec<Row>, AbiError>,
) -> i32 {
    let attempt = || -> Result<Vec<Row>, AbiError> {
        if snapshot.is_null() {
            return Err(AbiError::invalid_argument("`snapshot` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `snapshot`.
        produce(unsafe { &(*snapshot).snapshot })
    };

    // SAFETY: `err` validity is the caller's contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
    }
}

/// The shared shell of a handle list export: null-check the handle and
/// outputs, borrow the handle, run `produce` (which drives its own
/// `block_on_cancellable`) under the guard, and write the array.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `err`, if non-null, must
/// be writable.
unsafe fn handle_list<Row>(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut Row,
    out_len: *mut usize,
    err: *mut MoraineError,
    produce: impl FnOnce(&MoraineCatalogHandle) -> Result<Vec<Row>, AbiError>,
) -> i32 {
    let attempt = || -> Result<Vec<Row>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        produce(unsafe { &*handle })
    };

    // SAFETY: `err` validity is the caller's contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
    }
}

/// Frees the message of an error previously populated by a `moraine_*`
/// call. A null `message` is a no-op.
///
/// # Safety
///
/// `message`, if non-null, must be the exact pointer a `moraine_*` call
/// wrote into [`MoraineError::message`], not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_error_free(message: *mut c_char) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe { free_c_string(message) };
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}
