//! Dumps for the file tables: `ducklake_data_file`,
//! `ducklake_delete_file`, `ducklake_file_partition_value`, and
//! `ducklake_files_scheduled_for_deletion`.

use std::ffi::{c_char, c_void};

use super::{conversion::dump_row, dump_rows, free_rows};
use crate::{
    error::MoraineError,
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One `ducklake_data_file` row, as returned by
/// [`moraine_dump_data_files`]. Per-file partition values
/// (`ducklake_file_partition_value`) are a separate table and not
/// carried here.
#[repr(C)]
pub struct MoraineDataFileRow {
    /// `data_file_id`.
    pub data_file_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// Whether `file_order` is present.
    pub has_file_order: bool,
    /// `file_order`, valid iff `has_file_order`.
    pub file_order: u64,
    /// `path`, owned.
    pub path: *mut c_char,
    /// `path_is_relative`.
    pub path_is_relative: bool,
    /// `file_format`, owned.
    pub file_format: *mut c_char,
    /// `record_count`.
    pub record_count: u64,
    /// `file_size_bytes`.
    pub file_size_bytes: u64,
    /// `footer_size`.
    pub footer_size: u64,
    /// Whether `row_id_start` is present (absent when the file's rows
    /// carry explicit per-row ids, e.g. compaction outputs).
    pub has_row_id_start: bool,
    /// `row_id_start`, valid iff `has_row_id_start`.
    pub row_id_start: u64,
    /// Whether `partition_id` is present.
    pub has_partition_id: bool,
    /// `partition_id`, valid iff `has_partition_id`.
    pub partition_id: u64,
    /// `encryption_key`, owned, null if absent.
    pub encryption_key: *mut c_char,
    /// Whether `mapping_id` is present.
    pub has_mapping_id: bool,
    /// `mapping_id`, valid iff `has_mapping_id`.
    pub mapping_id: u64,
    /// Whether `partial_max` is present.
    pub has_partial_max: bool,
    /// `partial_max`, valid iff `has_partial_max`.
    pub partial_max: u64,
    /// Whether `row_group_count` is present.
    pub has_row_group_count: bool,
    /// `row_group_count`, valid iff `has_row_group_count`.
    pub row_group_count: u64,
}

dump_row! {
    data_file_rows(moraine::ffi_support::DataFileRecord) -> MoraineDataFileRow {
        values {
            data_file_id,
            table_id,
            begin_snapshot,
            path_is_relative,
            record_count,
            file_size_bytes,
            footer_size,
        }
        optional {
            end_snapshot => has_end_snapshot,
            file_order => has_file_order,
            row_id_start => has_row_id_start,
            partition_id => has_partition_id,
            mapping_id => has_mapping_id,
            partial_max => has_partial_max,
            row_group_count => has_row_group_count,
        }
        strings { path, file_format }
        optional_strings { encryption_key }
    }
}

/// Dumps every `ducklake_data_file` row — current and history — into
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
pub unsafe extern "C" fn moraine_dump_data_files(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineDataFileRow,
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
            moraine::ffi_support::dump_data_files,
            data_file_rows,
        )
    }
}

/// Dumps one table's `ducklake_data_file` rows into
/// `*out_items`/`*out_len`, in the order [`moraine_dump_data_files`] would
/// emit them. Freed with [`moraine_dump_data_files_free`].
///
/// # Safety
///
/// As [`moraine_dump_data_files`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_data_files_of(
    handle: *mut MoraineCatalogHandle,
    table_id: u64,
    out_items: *mut *mut MoraineDataFileRow,
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
            async |catalog| moraine::ffi_support::dump_data_files_of(catalog, table_id).await,
            data_file_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_data_files`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_data_files`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_data_files_free(items: *mut MoraineDataFileRow, len: usize) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

/// One `ducklake_delete_file` row, as returned by
/// [`moraine_dump_delete_files`].
#[repr(C)]
pub struct MoraineDeleteFileRow {
    /// `delete_file_id`.
    pub delete_file_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `data_file_id`.
    pub data_file_id: u64,
    /// `path`, owned.
    pub path: *mut c_char,
    /// `path_is_relative`.
    pub path_is_relative: bool,
    /// `format`, owned.
    pub format: *mut c_char,
    /// `delete_count`.
    pub delete_count: u64,
    /// `file_size_bytes`.
    pub file_size_bytes: u64,
    /// `footer_size`.
    pub footer_size: u64,
    /// `encryption_key`, owned, null if absent.
    pub encryption_key: *mut c_char,
    /// Whether `partial_max` is present.
    pub has_partial_max: bool,
    /// `partial_max`, valid iff `has_partial_max`.
    pub partial_max: u64,
    /// Whether `row_group_count` is present.
    pub has_row_group_count: bool,
    /// `row_group_count`, valid iff `has_row_group_count`.
    pub row_group_count: u64,
}

dump_row! {
    delete_file_rows(moraine::ffi_support::DeleteFileRecord) -> MoraineDeleteFileRow {
        values {
            delete_file_id,
            table_id,
            begin_snapshot,
            data_file_id,
            path_is_relative,
            delete_count,
            file_size_bytes,
            footer_size,
        }
        optional {
            end_snapshot => has_end_snapshot,
            partial_max => has_partial_max,
            row_group_count => has_row_group_count,
        }
        strings { path, format }
        optional_strings { encryption_key }
    }
}

/// Dumps every `ducklake_delete_file` row — current and history — into
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
pub unsafe extern "C" fn moraine_dump_delete_files(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineDeleteFileRow,
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
            moraine::ffi_support::dump_delete_files,
            delete_file_rows,
        )
    }
}

/// Dumps one table's `ducklake_delete_file` rows into
/// `*out_items`/`*out_len`, in the order [`moraine_dump_delete_files`]
/// would emit them. Freed with [`moraine_dump_delete_files_free`].
///
/// # Safety
///
/// As [`moraine_dump_delete_files`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_delete_files_of(
    handle: *mut MoraineCatalogHandle,
    table_id: u64,
    out_items: *mut *mut MoraineDeleteFileRow,
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
            async |catalog| moraine::ffi_support::dump_delete_files_of(catalog, table_id).await,
            delete_file_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_delete_files`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_delete_files`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_delete_files_free(
    items: *mut MoraineDeleteFileRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

/// One `ducklake_file_partition_value` row, as returned by
/// [`moraine_dump_file_partition_values`] — flattened from the data-file
/// record's embedded partition values.
#[repr(C)]
pub struct MoraineFilePartitionValueRow {
    /// `data_file_id`.
    pub data_file_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `partition_key_index`.
    pub partition_key_index: u64,
    /// `partition_value`, owned.
    pub partition_value: *mut c_char,
}

dump_row! {
    file_partition_value_rows(moraine::ffi_support::FilePartitionValueRow) -> MoraineFilePartitionValueRow {
        values { data_file_id, table_id, partition_key_index }
        optional {}
        strings { partition_value }
        optional_strings {}
    }
}

/// Dumps every `ducklake_file_partition_value` row — one per embedded
/// partition value of every data-file record, current and history — into
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
pub unsafe extern "C" fn moraine_dump_file_partition_values(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineFilePartitionValueRow,
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
            moraine::ffi_support::dump_file_partition_value_rows,
            file_partition_value_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_file_partition_values`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_file_partition_values`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_file_partition_values_free(
    items: *mut MoraineFilePartitionValueRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}

/// One `ducklake_files_scheduled_for_deletion` row, as returned by
/// [`moraine_dump_scheduled_deletions`].
#[repr(C)]
pub struct MoraineScheduledDeletionRow {
    /// `data_file_id`.
    pub data_file_id: u64,
    /// `path`, owned.
    pub path: *mut c_char,
    /// `path_is_relative`.
    pub path_is_relative: bool,
    /// `schedule_start`, microseconds since epoch (UTC).
    pub schedule_start_micros: i64,
}

dump_row! {
    scheduled_deletion_rows(moraine::ffi_support::GcFileRecord) -> MoraineScheduledDeletionRow {
        values { data_file_id, path_is_relative, schedule_start_micros }
        optional {}
        strings { path }
        optional_strings {}
    }
}

/// Dumps every `ducklake_files_scheduled_for_deletion` row into
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
pub unsafe extern "C" fn moraine_dump_scheduled_deletions(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineScheduledDeletionRow,
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
            moraine::ffi_support::dump_scheduled_deletions,
            scheduled_deletion_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_scheduled_deletions`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_scheduled_deletions`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_scheduled_deletions_free(
    items: *mut MoraineScheduledDeletionRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len);
    }
}
