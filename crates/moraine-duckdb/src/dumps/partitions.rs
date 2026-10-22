//! Dumps for the partition and sort tables: `ducklake_partition_info`,
//! `ducklake_partition_column`, `ducklake_sort_info`, and
//! `ducklake_sort_expression`.

use std::ffi::{c_char, c_void};

use super::{conversion::dump_row, dump_rows, free_rows};
use crate::{
    error::MoraineError,
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One `ducklake_partition_info` row, as returned by
/// [`moraine_dump_partition_info`]. Partition columns
/// (`ducklake_partition_column`) are a separate table served by
/// [`moraine_dump_partition_columns`].
#[repr(C)]
pub struct MorainePartitionInfoRow {
    /// `partition_id`.
    pub partition_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
}

dump_row! {
    moraine::ffi_support::PartitionRecord => MorainePartitionInfoRow {
        values { partition_id, table_id, begin_snapshot }
        optional { end_snapshot => has_end_snapshot }
        strings {}
        optional_strings {}
    }
}

/// Dumps every `ducklake_partition_info` row — current and history — into
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
pub unsafe extern "C" fn moraine_dump_partition_info(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MorainePartitionInfoRow,
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
            moraine::ffi_support::dump_partition_info,
        )
    }
}

/// Frees the array returned by [`moraine_dump_partition_info`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_partition_info`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_partition_info_free(
    items: *mut MorainePartitionInfoRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

/// One `ducklake_partition_column` row, as returned by
/// [`moraine_dump_partition_columns`] — flattened from the partition
/// record's embedded columns.
#[repr(C)]
pub struct MorainePartitionColumnRow {
    /// `partition_id`.
    pub partition_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `partition_key_index`.
    pub partition_key_index: u64,
    /// `column_id`.
    pub column_id: u64,
    /// `transform`, owned.
    pub transform: *mut c_char,
}

dump_row! {
    moraine::ffi_support::PartitionColumnRow => MorainePartitionColumnRow {
        values { partition_id, table_id, partition_key_index, column_id }
        optional {}
        strings { transform }
        optional_strings {}
    }
}

/// Dumps every `ducklake_partition_column` row — one per embedded column
/// of every partition record, current and history — into
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
pub unsafe extern "C" fn moraine_dump_partition_columns(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MorainePartitionColumnRow,
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
            moraine::ffi_support::dump_partition_column_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_partition_columns`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_partition_columns`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_partition_columns_free(
    items: *mut MorainePartitionColumnRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

/// One `ducklake_sort_info` row, as returned by
/// [`moraine_dump_sort_info`]. Sort expressions
/// (`ducklake_sort_expression`) are a separate table served by
/// [`moraine_dump_sort_expressions`].
#[repr(C)]
pub struct MoraineSortInfoRow {
    /// `sort_id`.
    pub sort_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
}

dump_row! {
    moraine::ffi_support::SortRecord => MoraineSortInfoRow {
        values { sort_id, table_id, begin_snapshot }
        optional { end_snapshot => has_end_snapshot }
        strings {}
        optional_strings {}
    }
}

/// Dumps every `ducklake_sort_info` row — current and history — into
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
pub unsafe extern "C" fn moraine_dump_sort_info(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSortInfoRow,
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
            moraine::ffi_support::dump_sort_info,
        )
    }
}

/// Frees the array returned by [`moraine_dump_sort_info`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching [`moraine_dump_sort_info`]
/// call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_sort_info_free(items: *mut MoraineSortInfoRow, len: usize) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

/// One `ducklake_sort_expression` row, as returned by
/// [`moraine_dump_sort_expressions`] — flattened from the sort record's
/// embedded expressions.
#[repr(C)]
pub struct MoraineSortExpressionRow {
    /// `sort_id`.
    pub sort_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `sort_key_index`.
    pub sort_key_index: u64,
    /// `expression`, owned.
    pub expression: *mut c_char,
    /// `dialect`, owned.
    pub dialect: *mut c_char,
    /// `sort_direction`, owned.
    pub sort_direction: *mut c_char,
    /// `null_order`, owned.
    pub null_order: *mut c_char,
}

dump_row! {
    moraine::ffi_support::SortExpressionRow => MoraineSortExpressionRow {
        values { sort_id, table_id, sort_key_index }
        optional {}
        strings { expression, dialect, sort_direction, null_order }
        optional_strings {}
    }
}

/// Dumps every `ducklake_sort_expression` row — one per embedded
/// expression of every sort record, current and history — into
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
pub unsafe extern "C" fn moraine_dump_sort_expressions(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSortExpressionRow,
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
            moraine::ffi_support::dump_sort_expression_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_sort_expressions`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_sort_expressions`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_sort_expressions_free(
    items: *mut MoraineSortExpressionRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}
