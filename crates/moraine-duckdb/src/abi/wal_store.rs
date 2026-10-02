//! The store a catalog's write-ahead log is written to, and moving it.

use std::{ffi::c_char, sync::Arc};

use object_store::ObjectStore;
use tracing::info;

use super::{
    attach::{
        MoraineS3Config, S3Creds, StoreKind, borrow_s3_creds, borrow_str, nested_stores,
        opt_borrow_str,
    },
    guard,
};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::new_runtime,
};

/// Mirrors the C `MoraineWalStore`: where a catalog's write-ahead log is
/// written, when it is not the catalog store itself. `path` is a store URI
/// of the same forms an attach path takes; `s3` supplies credentials for
/// an `s3://` one and may be null to use the AWS_* environment.
#[repr(C)]
pub struct MoraineWalStore {
    /// The store URI the log is written to. Null or empty keeps the log in
    /// the catalog store.
    pub path: *const c_char,
    /// S3 credentials for an `s3://` log store.
    pub s3: *const MoraineS3Config,
}

/// Resolves `wal`, if it names a path, into the store the write-ahead log
/// is written to.
///
/// # Safety
///
/// `wal`, if non-null, must point to a valid [`MoraineWalStore`] whose
/// `path` is null or a NUL-terminated C string and whose `s3` is null or a
/// valid [`MoraineS3Config`], all valid for reads for this call.
pub(crate) unsafe fn borrow_wal_store(
    wal: *const MoraineWalStore,
) -> Result<Option<moraine::WalStore>, AbiError> {
    // SAFETY: caller contract above.
    let Some(wal) = (unsafe { wal.as_ref() }) else {
        return Ok(None);
    };
    // SAFETY: the same contract covers `path`.
    let Some(path) = unsafe { opt_borrow_str(wal.path, "wal_path") }? else {
        return Ok(None);
    };
    // SAFETY: and `s3`.
    let creds = unsafe { borrow_s3_creds(wal.s3) };
    Ok(Some(open_wal_store(path, creds.as_ref())?))
}

/// The store a log-store URI names, resolved by scheme as an attach path
/// is, keeping the URI's own key prefix: SlateDB writes the log under the
/// catalog's path within whatever store it is handed, so the prefix is
/// this layer's to apply. Carries no cache identity — the log is written
/// and replayed, never cached.
pub(crate) fn open_wal_store(
    path: &str,
    s3: Option<&S3Creds>,
) -> Result<moraine::WalStore, AbiError> {
    let (kind, prefix) = StoreKind::from_path(path)?;
    let (store, _) = kind.open(path, s3)?;
    let store: Arc<dyn ObjectStore> = if prefix.is_empty() {
        store
    } else {
        Arc::new(object_store::prefix::PrefixStore::new(store, prefix))
    };
    Ok(moraine::WalStore::new(wal_store_name(path), store))
}

/// The name a catalog records for the log store a URI names, which every
/// later open is held to. A trailing separator is not part of it.
fn wal_store_name(path: &str) -> &str {
    path.trim_end_matches('/')
}

/// Refuses a write-ahead log store inside the data root, for the same
/// reason: the orphan sweep would delete commits that no sorted-string
/// table holds yet.
pub(super) fn refuse_wal_store_in_data_path(
    wal_store: Option<&moraine::WalStore>,
    data_path: &str,
) -> Result<(), AbiError> {
    let Some(wal) = wal_store else {
        return Ok(());
    };
    if nested_stores(wal.name(), data_path)? {
        return Err(AbiError::new(
            codes::CONSTRAINT,
            format!(
                "the write-ahead log store `{}` and DATA_PATH `{data_path}` are nested on the \
                 same object store; DuckLake's orphaned-file cleanup lists DATA_PATH and would \
                 delete log objects holding commits no sorted-string table carries yet. Put \
                 them in sibling locations.",
                wal.name()
            ),
        ));
    }
    Ok(())
}

/// Moves the write-ahead log of the catalog at `path` from the store it
/// records (`from`, null when its log is in the catalog store) to `to`
/// (null to bring it back there), and writes to `*out_moved` whether the
/// log changed stores — `false` means it was already on `to`.
///
/// Takes the store's writer twice, so no attach may hold it: an attach
/// that does is fenced, exactly as by [`super::moraine_migrate`]. The
/// first open drains the log it finds into a sorted-string table, so an
/// interrupted move leaves the catalog openable — against one store or the
/// other, with nothing lost — and can be run again. Every attach after a
/// move names `to`.
///
/// `from` naming a store other than the recorded one is refused, and the
/// message names the recorded one, which is how an operator who has lost
/// track finds it.
///
/// Returns [`codes::OK`] on success, having written `*out_moved`.
///
/// # Safety
///
/// `path` must be a valid NUL-terminated C string. `s3`, if non-null, must
/// point to a valid [`MoraineS3Config`] whose non-null fields are valid
/// NUL-terminated C strings. `from` and `to`, if non-null, must each point
/// to a valid [`MoraineWalStore`] under the contract
/// [`super::moraine_attach_with_wal_store`] states. `out_moved` must be a
/// valid, writable `*mut bool`, and `err`, if non-null, a valid, writable
/// [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_move_wal_store(
    path: *const c_char,
    s3: *const MoraineS3Config,
    from: *const MoraineWalStore,
    to: *const MoraineWalStore,
    out_moved: *mut bool,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        // Before anything that could emit an event.
        crate::logging::install();
        if out_moved.is_null() {
            return Err(AbiError::invalid_argument("`out_moved` is null"));
        }
        // SAFETY: `path` validity is this function's own safety contract.
        let path_str = unsafe { borrow_str(path, "path") }?;
        let (store_kind, prefix) = StoreKind::from_path(path_str)?;

        // SAFETY: `s3` validity is this function's own safety contract.
        let s3_creds = unsafe { borrow_s3_creds(s3) };
        // SAFETY: `from` validity is this function's own safety contract.
        let from_store = unsafe { borrow_wal_store(from) }?;
        // SAFETY: `to` validity is this function's own safety contract.
        let to_store = unsafe { borrow_wal_store(to) }?;

        let (object_store, cache_identity) = store_kind.open(path_str, s3_creds.as_ref())?;
        let log_id = crate::logging::allocate_handle_id();
        let _log_guard = crate::logging::enter_handle(log_id);
        let runtime = new_runtime(log_id, 0).map_err(|error| {
            AbiError::new(
                codes::INTERNAL,
                format!("failed to start tokio runtime: {error}"),
            )
        })?;

        let mut options = moraine::CatalogOptions::default();
        options.path = prefix;
        options.cache_identity = Some(cache_identity);
        options.wal_store = from_store;

        let moved = runtime
            .block_on(moraine::Catalog::move_wal_store(
                object_store,
                options,
                to_store,
            ))
            .map_err(AbiError::from)?;
        info!(
            from = moved.from,
            to = moved.to,
            moved = moved.moved(),
            "moved the catalog's write-ahead log store"
        );
        // SAFETY: `out_moved` is non-null and writable per the contract.
        unsafe { *out_moved = moved.moved() };
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}
