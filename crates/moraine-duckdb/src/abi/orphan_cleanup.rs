//! Durable authorization for DuckLake orphan deletion.

use std::{
    collections::BTreeSet,
    ffi::{c_char, c_void},
};

use super::{borrow_str, guard};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// Reserves unreferenced paths durably and writes one approval byte per input.
/// Physical deletion is permitted only for nonzero approvals after success.
///
/// # Safety
/// `handle` must be a live writable catalog. `paths` must contain `len` valid
/// UTF-8 C strings and `approved` must hold `len` writable bytes. Both arrays
/// may be null only when `len` is zero. Probe and error pointers follow the
/// standard ABI contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_plan_orphan_deletion(
    handle: *mut MoraineCatalogHandle,
    paths: *const *const c_char,
    len: usize,
    approved: *mut u8,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() || (len != 0 && (paths.is_null() || approved.is_null())) {
            return Err(AbiError::invalid_argument("null orphan-deletion argument"));
        }
        if len == 0 {
            return Ok(());
        }
        // SAFETY: the caller provides arrays of `len` elements.
        let (paths, approved) = unsafe {
            (
                std::slice::from_raw_parts(paths, len),
                std::slice::from_raw_parts_mut(approved, len),
            )
        };
        approved.fill(0);
        let paths: Vec<String> = paths
            .iter()
            .map(|path| {
                // SAFETY: each path is a valid C string by the caller contract.
                unsafe { borrow_str(*path, "path") }.map(str::to_owned)
            })
            .collect::<Result<_, _>>()?;
        // SAFETY: the handle is live by the caller contract.
        let handle = unsafe { &*handle };
        // SAFETY: probe pointers follow the caller's ABI contract.
        let planned = unsafe {
            handle.block_on_commit(
                probe,
                probe_ctx,
                handle.catalog.writer()?.plan_orphan_deletion(&paths),
            )
        }?;
        let mut planned: BTreeSet<_> = planned.into_iter().collect();
        for (path, approval) in paths.iter().zip(approved) {
            *approval = u8::from(planned.remove(path));
        }
        Ok(())
    };
    // SAFETY: `err` follows this function's caller contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}
