//! Dumps for the statistics tables: `ducklake_table_stats`,
//! `ducklake_table_column_stats`, and `ducklake_file_column_stats`.

use std::ffi::{c_char, c_void};

use super::{dump_rows, free_rows, opt_bool, opt_c_string, opt_into_raw};
use crate::{
    abi::free_c_string,
    error::{AbiError, MoraineError},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One `ducklake_table_stats` row, as returned by
/// [`moraine_dump_table_stats`]. Unversioned — no lifecycle columns.
#[repr(C)]
pub struct MoraineTableStatsRow {
    /// `table_id`.
    pub table_id: u64,
    /// `record_count`.
    pub record_count: u64,
    /// `next_row_id`.
    pub next_row_id: u64,
    /// `file_size_bytes`.
    pub file_size_bytes: u64,
}

/// Converts core `ducklake_table_stats` records into the C row shape. Shared by
/// the committed dump and the transaction-aware one, so the two can never
/// drift in what they report.
// Infallible — this kind's row carries no strings — but it keeps the
// fallible signature every converter shares, so `dump_rows` and the
// transaction-aware dumps take them all the same way.
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn table_stats_rows(
    rows: Vec<moraine::ffi_support::TableStatsRecord>,
) -> Result<Vec<MoraineTableStatsRow>, AbiError> {
    Ok(rows
        .into_iter()
        .map(|v| MoraineTableStatsRow {
            table_id: v.table_id,
            record_count: v.record_count,
            next_row_id: v.next_row_id,
            file_size_bytes: v.file_size_bytes,
        })
        .collect())
}

/// Dumps every `ducklake_table_stats` row into
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
pub unsafe extern "C" fn moraine_dump_table_stats(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineTableStatsRow,
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
            moraine::ffi_support::dump_table_stats,
            table_stats_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_table_stats`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_table_stats`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_table_stats_free(
    items: *mut MoraineTableStatsRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len, |_row| {});
    }
}

/// One `ducklake_table_column_stats` row, as returned by
/// [`moraine_dump_table_column_stats`]. Unversioned.
#[repr(C)]
pub struct MoraineTableColumnStatsRow {
    /// `table_id`.
    pub table_id: u64,
    /// `column_id`.
    pub column_id: u64,
    /// Whether `contains_null` is present.
    pub has_contains_null: bool,
    /// `contains_null`, valid iff `has_contains_null`.
    pub contains_null: bool,
    /// Whether `contains_nan` is present.
    pub has_contains_nan: bool,
    /// `contains_nan`, valid iff `has_contains_nan`.
    pub contains_nan: bool,
    /// `min_value`, owned, null if absent.
    pub min_value: *mut c_char,
    /// `max_value`, owned, null if absent.
    pub max_value: *mut c_char,
    /// `extra_stats`, owned, null if absent.
    pub extra_stats: *mut c_char,
}

/// Converts core `ducklake_table_column_stats` records into the C row shape.
/// Shared by the committed dump and the transaction-aware one, so the two can
/// never drift in what they report.
pub(crate) fn table_column_stats_rows(
    rows: Vec<moraine::ffi_support::TableColumnStatsRecord>,
) -> Result<Vec<MoraineTableColumnStatsRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|v| {
            let min_value = opt_c_string(v.min_value.as_deref())?;
            let max_value = opt_c_string(v.max_value.as_deref())?;
            let extra_stats = opt_c_string(v.extra_stats.as_deref())?;
            Ok((v, min_value, max_value, extra_stats))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;
    Ok(owned
        .into_iter()
        .map(|(v, min_value, max_value, extra_stats)| {
            let (has_null, contains_null) = opt_bool(v.contains_null);
            let (has_nan, contains_nan) = opt_bool(v.contains_nan);
            MoraineTableColumnStatsRow {
                table_id: v.table_id,
                column_id: v.column_id,
                has_contains_null: has_null,
                contains_null,
                has_contains_nan: has_nan,
                contains_nan,
                min_value: opt_into_raw(min_value),
                max_value: opt_into_raw(max_value),
                extra_stats: opt_into_raw(extra_stats),
            }
        })
        .collect())
}

/// Dumps every `ducklake_table_column_stats` row into
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
pub unsafe extern "C" fn moraine_dump_table_column_stats(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineTableColumnStatsRow,
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
            moraine::ffi_support::dump_table_column_stats,
            table_column_stats_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_table_column_stats`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_table_column_stats`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_table_column_stats_free(
    items: *mut MoraineTableColumnStatsRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len, |d| {
            free_c_string(d.min_value);
            free_c_string(d.max_value);
            free_c_string(d.extra_stats);
        });
    }
}

/// One `ducklake_file_column_stats` row, as returned by
/// [`moraine_dump_file_column_stats`]. Unversioned. Variant stats
/// (`ducklake_file_variant_stats`) are a separate table and not carried
/// here.
#[repr(C)]
pub struct MoraineFileColumnStatsRow {
    /// `data_file_id`.
    pub data_file_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `column_id`.
    pub column_id: u64,
    /// `column_size_bytes`.
    pub column_size_bytes: u64,
    /// `value_count`.
    pub value_count: u64,
    /// `null_count`.
    pub null_count: u64,
    /// `min_value`, owned, null if absent.
    pub min_value: *mut c_char,
    /// `max_value`, owned, null if absent.
    pub max_value: *mut c_char,
    /// Whether `contains_nan` is present.
    pub has_contains_nan: bool,
    /// `contains_nan`, valid iff `has_contains_nan`.
    pub contains_nan: bool,
    /// `extra_stats`, owned, null if absent.
    pub extra_stats: *mut c_char,
}

/// Converts core `ducklake_file_column_stats` records into the C row shape.
/// Shared by the committed dump and the transaction-aware one, so the two can
/// never drift in what they report.
pub(crate) fn file_column_stats_rows(
    rows: Vec<moraine::ffi_support::FileColumnStatsRecord>,
) -> Result<Vec<MoraineFileColumnStatsRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|v| {
            let min_value = opt_c_string(v.min_value.as_deref())?;
            let max_value = opt_c_string(v.max_value.as_deref())?;
            let extra_stats = opt_c_string(v.extra_stats.as_deref())?;
            Ok((v, min_value, max_value, extra_stats))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;
    Ok(owned
        .into_iter()
        .map(|(v, min_value, max_value, extra_stats)| {
            let (has_nan, contains_nan) = opt_bool(v.contains_nan);
            MoraineFileColumnStatsRow {
                data_file_id: v.data_file_id,
                table_id: v.table_id,
                column_id: v.column_id,
                column_size_bytes: v.column_size_bytes,
                value_count: v.value_count,
                null_count: v.null_count,
                min_value: opt_into_raw(min_value),
                max_value: opt_into_raw(max_value),
                has_contains_nan: has_nan,
                contains_nan,
                extra_stats: opt_into_raw(extra_stats),
            }
        })
        .collect())
}

/// Dumps every `ducklake_file_column_stats` row into
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
pub unsafe extern "C" fn moraine_dump_file_column_stats(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineFileColumnStatsRow,
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
            moraine::ffi_support::dump_file_column_stats,
            file_column_stats_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_file_column_stats`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_file_column_stats`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_file_column_stats_free(
    items: *mut MoraineFileColumnStatsRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len, |d| {
            free_c_string(d.min_value);
            free_c_string(d.max_value);
            free_c_string(d.extra_stats);
        });
    }
}
