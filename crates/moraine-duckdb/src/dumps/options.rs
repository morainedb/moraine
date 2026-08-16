//! Dump for `ducklake_metadata`: the catalog options a store has stored.
//!
//! Options live outside the snapshot protocol — DuckLake writes them
//! within its metadata connection, minting no snapshot — so these rows
//! carry no lifecycle. They are simply what is set now.

use std::ffi::{c_char, c_void};

use super::{dump_rows, free_rows, opt_c_string, opt_into_raw, opt_u64};
use crate::{
    abi::{free_c_string, to_c_string},
    error::{AbiError, MoraineError},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One `ducklake_metadata` row, as returned by [`moraine_dump_options`].
#[repr(C)]
pub struct MoraineOptionRow {
    /// `key`, owned.
    pub key: *mut c_char,
    /// `value`, owned.
    pub value: *mut c_char,
    /// `scope`, owned, null for a global option.
    pub scope: *mut c_char,
    /// Whether `scope_id` is present.
    pub has_scope_id: bool,
    /// `scope_id`, valid iff `has_scope_id`.
    pub scope_id: u64,
}

/// Converts core `ducklake_metadata` records into the C row shape. Shared by
/// the committed dump and the transaction-aware one, so the two can never
/// drift in what they report.
pub(crate) fn option_rows(
    rows: Vec<moraine::ffi_support::OptionRow>,
) -> Result<Vec<MoraineOptionRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut row| {
            let key = to_c_string(std::mem::take(&mut row.key))?;
            let value = to_c_string(std::mem::take(&mut row.value))?;
            let scope = opt_c_string(row.scope.as_deref())?;
            Ok((row, key, value, scope))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;

    Ok(owned
        .into_iter()
        .map(|(row, key, value, scope)| {
            let (has_scope_id, scope_id) = opt_u64(row.scope_id);
            MoraineOptionRow {
                key: key.into_raw(),
                value: value.into_raw(),
                scope: opt_into_raw(scope),
                has_scope_id,
                scope_id,
            }
        })
        .collect())
}

/// Dumps every stored `ducklake_metadata` row into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach`](crate::abi::moraine_attach), valid writable
/// `out_items`/`out_len`, a `probe` callable with `probe_ctx` from any
/// thread, and a null-or-writable `err`, all for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_options(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineOptionRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: forwarded caller contract.
    unsafe {
        dump_rows(
            handle,
            out_items,
            out_len,
            probe,
            probe_ctx,
            err,
            moraine::ffi_support::dump_options,
            option_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_options`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_options`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_options_free(items: *mut MoraineOptionRow, len: usize) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len, |row| {
            free_c_string(row.key);
            free_c_string(row.value);
            free_c_string(row.scope);
        });
    }
}
