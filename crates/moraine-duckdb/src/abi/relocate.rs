//! Offline relocation of a catalog's data root.

use std::ffi::c_char;

use super::{
    MoraineS3Config, MoraineWalStore,
    attach::{StoreKind, borrow_s3_creds, borrow_str, refuse_overlapping_data_path},
    guard,
    wal_store::{borrow_wal_store, refuse_wal_store_in_data_path},
};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::new_runtime,
};

/// Rewrites a detached catalog's recorded data root after its data was copied.
///
/// # Safety
/// The three path arguments must be valid NUL-terminated strings. `s3` and
/// `wal`, if non-null, must satisfy their types' borrowing contracts. `err`,
/// if non-null, must be writable for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_rewrite_data_path_store(
    path: *const c_char,
    old_root: *const c_char,
    new_root: *const c_char,
    s3: *const MoraineS3Config,
    wal: *const MoraineWalStore,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        crate::logging::install();
        crate::memory::install();
        // SAFETY: the caller guarantees valid strings for this call.
        let (path, old_root, new_root) = unsafe {
            (
                borrow_str(path, "path")?,
                borrow_str(old_root, "old_root")?,
                borrow_str(new_root, "new_root")?,
            )
        };
        if path.is_empty() || old_root.trim().is_empty() || new_root.trim().is_empty() {
            return Err(AbiError::invalid_argument("paths must not be empty"));
        }
        refuse_overlapping_data_path(path, new_root)?;
        // SAFETY: the caller guarantees the credentials' borrowing contract.
        let credentials = unsafe { borrow_s3_creds(s3) };
        // SAFETY: the caller guarantees the WAL store's borrowing contract.
        let wal_store = unsafe { borrow_wal_store(wal) }?;
        refuse_wal_store_in_data_path(wal_store.as_ref(), new_root)?;
        let (kind, prefix) = StoreKind::from_path(path)?;
        let (store, identity) = kind.open(path, credentials.as_ref())?;
        let log_id = crate::logging::allocate_handle_id();
        let _guard = crate::logging::enter_handle(log_id);
        let runtime = new_runtime(log_id, 0)
            .map_err(|error| AbiError::new(codes::INTERNAL, error.to_string()))?;
        let mut options = moraine::CatalogOptions::default();
        options.path = prefix;
        options.cache_identity = Some(identity);
        options.wal_store = wal_store;
        runtime
            .block_on(async {
                // A read-only open refuses a missing catalog without bootstrapping it.
                let reader =
                    moraine::Catalog::open_read_only(store.clone(), options.clone()).await?;
                reader.close().await?;
                let catalog = moraine::Catalog::open(store, options).await?;
                let result = catalog.rewrite_data_path(old_root, new_root).await;
                let closed = catalog.close().await;
                result?;
                closed
            })
            .map_err(AbiError::from)
    };
    // SAFETY: `err` is writable when non-null under the caller contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}
