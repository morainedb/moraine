//! Dumps for the statistics tables: `ducklake_table_stats`,
//! `ducklake_table_column_stats`, and `ducklake_file_column_stats`.

use std::ffi::{c_char, c_void};

use super::{conversion::dump_row, dump_rows, free_rows};
use crate::{
    error::MoraineError,
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

dump_row! {
    moraine::ffi_support::TableStatsRecord => MoraineTableStatsRow {
        values { table_id, record_count, next_row_id, file_size_bytes }
        optional {}
        strings {}
        optional_strings {}
    }
}

/// Dumps every `ducklake_table_stats` row into
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
        free_rows(items, len);
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
    /// Whether `min_is_exact` is present.
    pub has_min_is_exact: bool,
    /// `min_is_exact`, valid iff `has_min_is_exact`.
    pub min_is_exact: bool,
    /// Whether `max_is_exact` is present.
    pub has_max_is_exact: bool,
    /// `max_is_exact`, valid iff `has_max_is_exact`.
    pub max_is_exact: bool,
}

dump_row! {
    moraine::ffi_support::TableColumnStatsRecord => MoraineTableColumnStatsRow {
        values { table_id, column_id }
        optional {
            contains_null => has_contains_null,
            contains_nan => has_contains_nan,
            min_is_exact => has_min_is_exact,
            max_is_exact => has_max_is_exact,
        }
        strings {}
        optional_strings { min_value, max_value, extra_stats }
    }
}

/// Dumps every `ducklake_table_column_stats` row into
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
        free_rows(items, len);
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
    /// Whether `value_count` is present.
    pub has_value_count: bool,
    /// `value_count`, valid iff `has_value_count`.
    pub value_count: u64,
    /// Whether `null_count` is present.
    pub has_null_count: bool,
    /// `null_count`, valid iff `has_null_count`.
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
    /// Whether `min_is_exact` is present.
    pub has_min_is_exact: bool,
    /// `min_is_exact`, valid iff `has_min_is_exact`.
    pub min_is_exact: bool,
    /// Whether `max_is_exact` is present.
    pub has_max_is_exact: bool,
    /// `max_is_exact`, valid iff `has_max_is_exact`.
    pub max_is_exact: bool,
}

dump_row! {
    moraine::ffi_support::FileColumnStatsRecord => MoraineFileColumnStatsRow {
        values { data_file_id, table_id, column_id, column_size_bytes }
        optional {
            value_count => has_value_count,
            null_count => has_null_count,
            contains_nan => has_contains_nan,
            min_is_exact => has_min_is_exact,
            max_is_exact => has_max_is_exact,
        }
        strings {}
        optional_strings { min_value, max_value, extra_stats }
    }
}

/// Dumps every `ducklake_file_column_stats` row into
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
        )
    }
}

/// Dumps one table's `ducklake_file_column_stats` rows into
/// `*out_items`/`*out_len`, in the order
/// [`moraine_dump_file_column_stats`] would emit them. Free with
/// [`moraine_dump_file_column_stats_free`].
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach_with_wal_store`](crate::abi::moraine_attach_with_wal_store), valid writable
/// `out_items`/`out_len`, a `probe` callable with `probe_ctx` from any
/// thread, and a null-or-writable `err`, all for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_file_column_stats_of(
    handle: *mut MoraineCatalogHandle,
    table_id: u64,
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
            async |catalog| {
                moraine::ffi_support::dump_file_column_stats_of(catalog, table_id).await
            },
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
        free_rows(items, len);
    }
}

/// One `ducklake_file_variant_stats` row, as returned by
/// [`moraine_dump_file_variant_stats`] — flattened from the embedded
/// entries of the column's statistics record.
#[repr(C)]
pub struct MoraineFileVariantStatsRow {
    /// `data_file_id`.
    pub data_file_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `column_id`.
    pub column_id: u64,
    /// `variant_path`, owned.
    pub variant_path: *mut c_char,
    /// `shredded_type`, owned.
    pub shredded_type: *mut c_char,
    /// `column_size_bytes`.
    pub column_size_bytes: u64,
    /// Whether `value_count` is present.
    pub has_value_count: bool,
    /// `value_count`, valid iff `has_value_count`.
    pub value_count: u64,
    /// Whether `null_count` is present.
    pub has_null_count: bool,
    /// `null_count`, valid iff `has_null_count`.
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

dump_row! {
    moraine::ffi_support::FileVariantStatsRow => MoraineFileVariantStatsRow {
        values { data_file_id, table_id, column_id, column_size_bytes }
        optional {
            value_count => has_value_count,
            null_count => has_null_count,
            contains_nan => has_contains_nan,
        }
        strings { variant_path, shredded_type }
        optional_strings { min_value, max_value, extra_stats }
    }
}

/// Dumps every `ducklake_file_variant_stats` row into
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
pub unsafe extern "C" fn moraine_dump_file_variant_stats(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineFileVariantStatsRow,
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
            moraine::ffi_support::dump_file_variant_stats_rows,
        )
    }
}

/// Dumps one table's `ducklake_file_variant_stats` rows into
/// `*out_items`/`*out_len`, in the order
/// [`moraine_dump_file_variant_stats`] would emit them. Free with
/// [`moraine_dump_file_variant_stats_free`].
///
/// # Safety
///
/// As [`moraine_dump_file_variant_stats`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_file_variant_stats_of(
    handle: *mut MoraineCatalogHandle,
    table_id: u64,
    out_items: *mut *mut MoraineFileVariantStatsRow,
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
            async |catalog| {
                moraine::ffi_support::dump_file_variant_stats_rows_of(catalog, table_id).await
            },
        )
    }
}

/// Frees the array returned by [`moraine_dump_file_variant_stats`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_file_variant_stats`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_file_variant_stats_free(
    items: *mut MoraineFileVariantStatsRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}
