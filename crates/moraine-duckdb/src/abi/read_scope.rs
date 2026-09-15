//! Pinned read surfaces owned by transaction snapshots.

use std::{ffi::c_void, ptr};

use super::guard;
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe, MoraineSnapshotHandle},
};

/// Opens a transaction snapshot with a pinned index/inline read surface when
/// supported. Readers without pinned revisions receive an ordinary snapshot
/// instead.
///
/// # Safety
/// Same pointer and cancellation contract as [`super::moraine_snapshot`].
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
            let snapshot = if let Some(scope) = scope {
                let mut snapshot = MoraineSnapshotHandle::new(scope.reads().snapshot().await?);
                snapshot.read_identity = Some(scope.identity().clone());
                snapshot.read_alias = Some(Box::new(handle.read_alias(scope.reads().clone())));
                snapshot
            } else {
                MoraineSnapshotHandle::new(handle.catalog.reads().snapshot().await?)
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
        .and_then(|snapshot| snapshot.read_alias.as_deref())
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
    let Some(identity) = &snapshot.read_identity else {
        return false;
    };
    *out = moraine::ffi_support::index_read_revision(identity);
    true
}
