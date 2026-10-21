//! Dump for `ducklake_metadata`: the catalog options a store has stored.
//!
//! Options live outside the snapshot protocol — DuckLake writes them
//! within its metadata connection, minting no snapshot — so these rows
//! carry no lifecycle. They are simply what is set now.

use std::ffi::{c_char, c_void};

use super::{conversion::dump_row, dump_rows, free_rows};
use crate::{
    error::MoraineError,
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

dump_row! {
    option_rows(moraine::ffi_support::OptionRow) -> MoraineOptionRow {
        values {}
        optional { scope_id => has_scope_id }
        strings { key, value }
        optional_strings { scope }
    }
}

/// Dumps every stored `ducklake_metadata` row into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach_with_wal_store`](crate::abi::moraine_attach_with_wal_store), valid writable
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
        free_rows(items, len);
    }
}
