//! Pinned read surfaces owned by transaction snapshots.

use std::{ffi::c_void, ptr};

use super::guard;
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe, MoraineSnapshotHandle},
};

/// Opens a transaction snapshot with a fixed catalog, index, and inline read
/// surface.
///
/// # Safety
/// `handle` must be attached and `out` writable. A non-null `probe` must be
/// safe to call with `probe_ctx` from any thread during this call. A non-null
/// `err` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_scoped(
    handle: *mut MoraineCatalogHandle,
    out: *mut *mut MoraineSnapshotHandle,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || {
        if handle.is_null() || out.is_null() {
            return Err(AbiError::invalid_argument("null read scope argument"));
        }
        // SAFETY: caller guarantees a live catalog handle.
        let handle = unsafe { &*handle };
        let open = async {
            let scope = handle.catalog.reads().index_read_scope().await?;
            let snapshot = {
                let head = moraine::ffi_support::head_stamp(scope.reads())
                    .await?
                    .ok_or_else(|| moraine::Error::Corruption("catalog has no head".to_owned()))?;
                MoraineSnapshotHandle {
                    stamp: (head.snapshot_id, head.batch_seq),
                    read_identity: scope.identity().clone(),
                    read_alias: Box::new(handle.read_alias(scope.reads().clone())),
                }
            };
            // The boxed alias address stays stable for the snapshot's lifetime.
            Ok::<_, moraine::Error>(Box::new(snapshot))
        };
        // SAFETY: caller supplies the cancellation callback's lifetime and thread
        // safety.
        let snapshot = unsafe { handle.block_on_cancellable(probe, probe_ctx, open) }?;
        // SAFETY: checked non-null; caller supplies writable output storage.
        unsafe {
            *out = Box::into_raw(snapshot);
        }
        Ok(())
    };
    // SAFETY: caller supplies writable error storage when non-null.
    unsafe { guard(err, attempt) }.map_or_else(|code| code, |()| codes::OK)
}

/// Borrows the snapshot's pinned read surface, or returns null when
/// unavailable. Never detach the returned alias; freeing the snapshot releases
/// it.
///
/// # Safety
/// `snapshot` must be null or a live snapshot. The borrowed alias must not
/// outlive it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_read_handle(
    snapshot: *mut MoraineSnapshotHandle,
) -> *mut MoraineCatalogHandle {
    // SAFETY: caller guarantees a live snapshot when non-null.
    unsafe { snapshot.as_ref() }
        .map(|snapshot| snapshot.read_alias.as_ref())
        .map_or(ptr::null_mut(), |handle| ptr::from_ref(handle).cast_mut())
}

/// Returns the pinned revision, to be paired with DuckDB's attachment OID.
/// False means this snapshot cannot validate statement-cache dependencies.
///
/// # Safety
/// `snapshot` must be null or live; `out`, when non-null, must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_read_revision(
    snapshot: *mut MoraineSnapshotHandle,
    out: *mut u64,
) -> bool {
    // SAFETY: caller supplies live input and writable output storage.
    let (Some(snapshot), Some(out)) = (unsafe { snapshot.as_ref() }, unsafe { out.as_mut() })
    else {
        return false;
    };
    let identity = &snapshot.read_identity;
    *out = moraine::ffi_support::index_read_revision(identity);
    true
}
