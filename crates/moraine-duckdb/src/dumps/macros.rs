//! Dumps for the macro tables: `ducklake_macro`, `ducklake_macro_impl`,
//! and `ducklake_macro_parameters`.

use std::ffi::{c_char, c_void};

use super::{dump_rows, free_rows, opt_c_string, opt_into_raw, opt_u64};
use crate::{
    abi::{free_c_string, to_c_string},
    error::{AbiError, MoraineError},
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

/// Converts core `ducklake_macro` records into the C row shape. Shared by the
/// committed dump and the transaction-aware one, so the two can never
/// drift in what they report.
pub(crate) fn macro_rows(
    rows: Vec<moraine::ffi_support::MacroRecord>,
) -> Result<Vec<MoraineMacroRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut m| {
            let macro_name = to_c_string(std::mem::take(&mut m.macro_name))?;
            Ok((m, macro_name))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;

    Ok(owned
        .into_iter()
        .map(|(m, macro_name)| {
            let (has_end, end) = opt_u64(m.end_snapshot);
            MoraineMacroRow {
                schema_id: m.schema_id,
                macro_id: m.macro_id,
                macro_name: macro_name.into_raw(),
                begin_snapshot: m.begin_snapshot,
                has_end_snapshot: has_end,
                end_snapshot: end,
            }
        })
        .collect())
}

/// Dumps every `ducklake_macro` row — current and history — into
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
            macro_rows,
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
        free_rows(items, len, |d| {
            free_c_string(d.macro_name);
        });
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

/// Converts core `ducklake_macro_impl` records into the C row shape. Shared by
/// the committed dump and the transaction-aware one, so the two can never
/// drift in what they report.
pub(crate) fn macro_impl_rows(
    rows: Vec<moraine::ffi_support::MacroImplRow>,
) -> Result<Vec<MoraineMacroImplRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut row| {
            let dialect = to_c_string(std::mem::take(&mut row.dialect))?;
            let sql = to_c_string(std::mem::take(&mut row.sql))?;
            let macro_type = to_c_string(std::mem::take(&mut row.macro_type))?;
            Ok((row, dialect, sql, macro_type))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;

    Ok(owned
        .into_iter()
        .map(|(row, dialect, sql, macro_type)| MoraineMacroImplRow {
            macro_id: row.macro_id,
            impl_id: row.impl_id,
            dialect: dialect.into_raw(),
            sql: sql.into_raw(),
            macro_type: macro_type.into_raw(),
        })
        .collect())
}

/// Dumps every `ducklake_macro_impl` row — flattened from the embedded
/// implementations of every macro record, history included, ordered by
/// `(macro_id, impl_id)`. DuckLake's macro reconstruction consumes rows
/// in served order.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach`](crate::abi::moraine_attach), valid writable
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
            macro_impl_rows,
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
        free_rows(items, len, |d| {
            free_c_string(d.dialect);
            free_c_string(d.sql);
            free_c_string(d.macro_type);
        });
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

/// Converts core `ducklake_macro_parameters` records into the C row shape.
/// Shared by the committed dump and the transaction-aware one, so the two can
/// never drift in what they report.
pub(crate) fn macro_parameter_rows(
    rows: Vec<moraine::ffi_support::MacroParameterRow>,
) -> Result<Vec<MoraineMacroParameterRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut row| {
            let parameter_name = to_c_string(std::mem::take(&mut row.parameter_name))?;
            let parameter_type = to_c_string(std::mem::take(&mut row.parameter_type))?;
            let default_value = opt_c_string(row.default_value.as_deref())?;
            let default_value_type = to_c_string(std::mem::take(&mut row.default_value_type))?;
            Ok((
                row,
                parameter_name,
                parameter_type,
                default_value,
                default_value_type,
            ))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;

    Ok(owned
        .into_iter()
        .map(
            |(row, parameter_name, parameter_type, default_value, default_value_type)| {
                MoraineMacroParameterRow {
                    macro_id: row.macro_id,
                    impl_id: row.impl_id,
                    column_id: row.column_id,
                    parameter_name: parameter_name.into_raw(),
                    parameter_type: parameter_type.into_raw(),
                    default_value: opt_into_raw(default_value),
                    default_value_type: default_value_type.into_raw(),
                }
            },
        )
        .collect())
}

/// Dumps every `ducklake_macro_parameters` row — flattened from the
/// embedded parameters of every implementation, history included, ordered
/// by `(macro_id, impl_id, column_id)`. DuckLake's macro reconstruction
/// consumes rows in served order.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach`](crate::abi::moraine_attach), valid writable
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
            macro_parameter_rows,
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
        free_rows(items, len, |d| {
            free_c_string(d.parameter_name);
            free_c_string(d.parameter_type);
            free_c_string(d.default_value);
            free_c_string(d.default_value_type);
        });
    }
}
