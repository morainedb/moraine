//! Dumps for the macro tables: `ducklake_macro`, `ducklake_macro_impl`,
//! and `ducklake_macro_parameters`.

use std::ffi::{c_char, c_void};

use super::{conversion::dump_row, dump_rows, free_rows};
use crate::{
    error::MoraineError,
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One `ducklake_macro` row, as returned by [`moraine_dump_macros`].
#[repr(C)]
pub struct MoraineMacroRow {
    /// `schema_id`.
    pub schema_id: u64,
    /// `macro_id`.
    pub macro_id: u64,
    /// `macro_name`, owned.
    pub macro_name: *mut c_char,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
}

dump_row! {
    moraine::ffi_support::MacroRecord => MoraineMacroRow {
        values { schema_id, macro_id, begin_snapshot }
        optional { end_snapshot => has_end_snapshot }
        strings { macro_name }
        optional_strings {}
    }
}

/// Dumps every `ducklake_macro` row — current and history — into
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
pub unsafe extern "C" fn moraine_dump_macros(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineMacroRow,
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
            moraine::ffi_support::dump_macros,
        )
    }
}

/// Frees the array returned by [`moraine_dump_macros`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching [`moraine_dump_macros`]
/// call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_macros_free(items: *mut MoraineMacroRow, len: usize) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

/// One `ducklake_macro_impl` row, as returned by
/// [`moraine_dump_macro_impls`].
#[repr(C)]
pub struct MoraineMacroImplRow {
    /// `macro_id`.
    pub macro_id: u64,
    /// `impl_id`.
    pub impl_id: u64,
    /// `dialect`, owned.
    pub dialect: *mut c_char,
    /// `sql`, owned.
    pub sql: *mut c_char,
    /// `type`, owned.
    pub macro_type: *mut c_char,
}

dump_row! {
    moraine::ffi_support::MacroImplRow => MoraineMacroImplRow {
        values { macro_id, impl_id }
        optional {}
        strings { dialect, sql, macro_type }
        optional_strings {}
    }
}

/// Dumps every `ducklake_macro_impl` row — flattened from the embedded
/// implementations of every macro record, history included, ordered by
/// `(macro_id, impl_id)`. DuckLake's macro reconstruction consumes rows
/// in served order.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach_with_wal_store`](crate::abi::moraine_attach_with_wal_store), valid writable
/// `out_items`/`out_len`, a `probe` callable with `probe_ctx` from any
/// thread, and a null-or-writable `err`, all for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_macro_impls(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineMacroImplRow,
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
            moraine::ffi_support::dump_macro_impl_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_macro_impls`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_macro_impls`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_macro_impls_free(
    items: *mut MoraineMacroImplRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

/// One `ducklake_macro_parameters` row, as returned by
/// [`moraine_dump_macro_parameters`].
#[repr(C)]
pub struct MoraineMacroParameterRow {
    /// `macro_id`.
    pub macro_id: u64,
    /// `impl_id`.
    pub impl_id: u64,
    /// `column_id`: the parameter's 0-based position within its impl.
    pub column_id: u64,
    /// `parameter_name`, owned.
    pub parameter_name: *mut c_char,
    /// `parameter_type`, owned.
    pub parameter_type: *mut c_char,
    /// `default_value`, owned, null if absent.
    pub default_value: *mut c_char,
    /// `default_value_type`, owned.
    pub default_value_type: *mut c_char,
}

dump_row! {
    moraine::ffi_support::MacroParameterRow => MoraineMacroParameterRow {
        values { macro_id, impl_id, column_id }
        optional {}
        strings { parameter_name, parameter_type, default_value_type }
        optional_strings { default_value }
    }
}

/// Dumps every `ducklake_macro_parameters` row — flattened from the
/// embedded parameters of every implementation, history included, ordered
/// by `(macro_id, impl_id, column_id)`. DuckLake's macro reconstruction
/// consumes rows in served order.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach_with_wal_store`](crate::abi::moraine_attach_with_wal_store), valid writable
/// `out_items`/`out_len`, a `probe` callable with `probe_ctx` from any
/// thread, and a null-or-writable `err`, all for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_macro_parameters(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineMacroParameterRow,
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
            moraine::ffi_support::dump_macro_parameter_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_macro_parameters`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_macro_parameters`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_macro_parameters_free(
    items: *mut MoraineMacroParameterRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}
