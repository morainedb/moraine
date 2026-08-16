//! The checkpoint lifecycle as C-ABI entry points: mint, list, and
//! release the checkpoints a zero-write read-only attach
//! (`moraine_attach`'s `checkpoint`) pins itself to.
//!
//! Minting takes an attached handle (the core mints through the writer
//! that attach opened). Listing and releasing take a store path: they
//! touch only the manifest.

use std::{ffi::c_char, ptr};

use moraine::CatalogOptions;

use crate::{
    abi::{
        MoraineS3Config, StoreKind, borrow_s3_creds, borrow_str, free_array, free_c_string, guard,
        to_c_string, write_array,
    },
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, new_runtime},
};

/// One checkpoint the store's manifest carries.
#[repr(C)]
pub struct MoraineCheckpoint {
    /// The checkpoint's id, in the form `moraine_attach`'s `checkpoint`
    /// takes. Owned by the array.
    pub id: *mut c_char,
}

/// The object store and catalog options a path resolves to, for the
/// manifest-only entry points.
fn resolve(
    path: &str,
    s3: *const MoraineS3Config,
) -> Result<
    (
        std::sync::Arc<dyn object_store::ObjectStore>,
        CatalogOptions,
    ),
    AbiError,
> {
    let (store_kind, prefix) = StoreKind::from_path(path)?;
    // SAFETY: `s3` validity is the caller's contract, forwarded from the
    // entry point that took it.
    let s3_creds = unsafe { borrow_s3_creds(s3) };
    let object_store = store_kind.open(path, s3_creds.as_ref())?;
    let mut options = CatalogOptions::default();
    options.path = prefix;
    Ok((object_store, options))
}

/// Mints a checkpoint over `handle`'s current durable state and writes its
/// id to `*out_id` (free with `moraine_string_free`).
///
/// The handle must be a read-write attach.
///
/// `lifetime_ms` bounds how long the checkpoint holds its objects against
/// garbage collection; `0` means no expiry, which pins them until
/// [`moraine_delete_checkpoint`] releases it.
///
/// Returns [`codes::OK`] on success. On failure `*out_id` is left
/// unwritten and, if `err` is non-null, `*err` carries the code and a
/// message.
///
/// # Safety
///
/// `handle` must be a live handle from
/// [`moraine_attach`](super::moraine_attach). `out_id` must be a valid,
/// writable `*mut *mut c_char`. `err`, if non-null, must be a valid, writable
/// [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_create_checkpoint(
    handle: *mut MoraineCatalogHandle,
    lifetime_ms: u64,
    out_id: *mut *mut c_char,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_id.is_null() {
            return Err(AbiError::invalid_argument("`out_id` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let lifetime = (lifetime_ms > 0).then(|| std::time::Duration::from_millis(lifetime_ms));
        let checkpoint = handle_ref
            .block_on(handle_ref.catalog.writer()?.create_checkpoint(lifetime))
            .map_err(AbiError::from)?;

        let id = to_c_string(checkpoint)?.into_raw();
        // SAFETY: `out_id` is non-null and writable per the caller
        // contract, checked above.
        unsafe { *out_id = id };
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Lists every checkpoint the store at `path` carries — a
/// reader-established one included — into `*out_items` / `*out_len`.
/// Free with [`moraine_checkpoints_free`].
///
/// Returns [`codes::OK`] on success. On failure the out-parameters are
/// left unwritten and, if `err` is non-null, `*err` carries the code and a
/// message.
///
/// # Safety
///
/// `path` must be a valid NUL-terminated C string. `s3`, if non-null, must
/// point to a valid [`MoraineS3Config`] whose non-null fields are valid
/// NUL-terminated C strings. `out_items` must be a valid, writable
/// `*mut *mut MoraineCheckpoint` and `out_len` a valid, writable
/// `*mut usize`. `err`, if non-null, must be a valid, writable
/// [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_checkpoints(
    path: *const c_char,
    s3: *const MoraineS3Config,
    out_items: *mut *mut MoraineCheckpoint,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        // Before anything that could emit an event.
        crate::logging::install();
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("an out-parameter is null"));
        }
        // SAFETY: `path` and `s3` validity are this function's own contract.
        let path_str = unsafe { borrow_str(path, "path") }?;
        let (object_store, options) = resolve(path_str, s3)?;

        let log_id = crate::logging::allocate_handle_id();
        let _log_guard = crate::logging::enter_handle(log_id);
        let runtime = new_runtime(log_id, 0).map_err(|e| {
            AbiError::new(
                codes::INTERNAL,
                format!("failed to start tokio runtime: {e}"),
            )
        })?;

        let checkpoints = runtime
            .block_on(moraine::Catalog::checkpoints(object_store, options))
            .map_err(AbiError::from)?;

        let ids = checkpoints
            .into_iter()
            .map(to_c_string)
            .collect::<Result<Vec<_>, AbiError>>()?;
        let items = ids
            .into_iter()
            .map(|id| MoraineCheckpoint { id: id.into_raw() })
            .collect::<Vec<_>>();

        // SAFETY: both out-parameters are non-null and writable per the
        // caller contract, checked above.
        unsafe { write_array(items, out_items, out_len) };
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Frees an array written by [`moraine_checkpoints`]. A null pointer is
/// ignored.
///
/// # Safety
///
/// `items`/`len` must be a pair written by [`moraine_checkpoints`] and not
/// yet freed, or `items` null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_checkpoints_free(items: *mut MoraineCheckpoint, len: usize) {
    // SAFETY: caller contract — an array this module wrote, or null.
    unsafe {
        free_array(items, len, |item| {
            free_c_string(std::mem::replace(&mut item.id, ptr::null_mut()));
        });
    }
}

/// Releases the checkpoint named by `id`, unpinning whatever it held
/// against garbage collection. Runs against a live catalog without
/// fencing it.
///
/// Returns [`codes::OK`] on success. On failure, if `err` is non-null,
/// `*err` carries the code and a message.
///
/// # Safety
///
/// `path` and `id` must be valid NUL-terminated C strings. `s3`, if
/// non-null, must point to a valid [`MoraineS3Config`] whose non-null
/// fields are valid NUL-terminated C strings. `err`, if non-null, must be
/// a valid, writable [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_delete_checkpoint(
    path: *const c_char,
    s3: *const MoraineS3Config,
    id: *const c_char,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        crate::logging::install();
        // SAFETY: `path`, `id`, and `s3` validity are this function's own
        // contract.
        let path_str = unsafe { borrow_str(path, "path") }?;
        // SAFETY: as above — `id` validity is this function's own contract.
        let id_str = unsafe { borrow_str(id, "id") }?;
        let (object_store, options) = resolve(path_str, s3)?;

        let log_id = crate::logging::allocate_handle_id();
        let _log_guard = crate::logging::enter_handle(log_id);
        let runtime = new_runtime(log_id, 0).map_err(|e| {
            AbiError::new(
                codes::INTERNAL,
                format!("failed to start tokio runtime: {e}"),
            )
        })?;

        runtime
            .block_on(moraine::Catalog::delete_checkpoint(
                object_store,
                options,
                id_str,
            ))
            .map_err(AbiError::from)
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}
