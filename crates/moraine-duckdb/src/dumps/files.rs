//! Dumps for the file tables: `ducklake_data_file`,
//! `ducklake_delete_file`, `ducklake_file_partition_value`, and
//! `ducklake_files_scheduled_for_deletion`.

use std::ffi::{c_char, c_void};

use super::{dump_rows, free_rows, opt_c_string, opt_into_raw, opt_u64};
use crate::{
    abi::{free_c_string, to_c_string},
    error::{AbiError, MoraineError},
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
}

/// Converts core `ducklake_data_file` records into the C row shape.
/// Shared by the committed dump and the transaction-aware one, so the
/// two can never drift in what they report.
pub(crate) fn data_file_rows(
    rows: Vec<moraine::ffi_support::DataFileRecord>,
) -> Result<Vec<MoraineDataFileRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut v| {
            let path = to_c_string(std::mem::take(&mut v.path))?;
            let file_format = to_c_string(std::mem::take(&mut v.file_format))?;
            let encryption_key = opt_c_string(v.encryption_key.as_deref())?;
            Ok((v, path, file_format, encryption_key))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;

    Ok(owned
        .into_iter()
        .map(|(v, path, file_format, encryption_key)| {
            let (has_end, end) = opt_u64(v.end_snapshot);
            let (has_order, order) = opt_u64(v.file_order);
            let (has_partition, partition) = opt_u64(v.partition_id);
            let (has_mapping, mapping) = opt_u64(v.mapping_id);
            let (has_partial_max, partial_max) = opt_u64(v.partial_max);
            let (has_row_id_start, row_id_start) = opt_u64(v.row_id_start);

            MoraineDataFileRow {
                data_file_id: v.data_file_id,
                table_id: v.table_id,
                begin_snapshot: v.begin_snapshot,
                has_end_snapshot: has_end,
                end_snapshot: end,
                has_file_order: has_order,
                file_order: order,
                path: path.into_raw(),
                path_is_relative: v.path_is_relative,
                file_format: file_format.into_raw(),
                record_count: v.record_count,
                file_size_bytes: v.file_size_bytes,
                footer_size: v.footer_size,
                has_row_id_start,
                row_id_start,
                has_partition_id: has_partition,
                partition_id: partition,
                encryption_key: opt_into_raw(encryption_key),
                has_mapping_id: has_mapping,
                mapping_id: mapping,
                has_partial_max,
                partial_max,
            }
        })
        .collect())
}

/// Dumps every `ducklake_data_file` row — current and history — into
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

/// As [`moraine_dump_data_files`], for a caller that keeps a row only
/// while `filter_snapshot < end_snapshot` (or it is null) — the shape
/// every DuckLake read of this table carries.
///
/// Once `filter_snapshot` reaches the head this call observes, no ended
/// version can satisfy that, so the ended half is not read; the rows are
/// the same ones either way. A bound behind the head is a time-travel
/// read and gets the full set. Freed with
/// [`moraine_dump_data_files_free`].
///
/// # Safety
///
/// As [`moraine_dump_data_files`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_data_files_live_at(
    handle: *mut MoraineCatalogHandle,
    filter_snapshot: u64,
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
            async |catalog| {
                moraine::ffi_support::dump_data_files_live_at(catalog, filter_snapshot).await
            },
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
        free_rows(items, len, |d| {
            free_c_string(d.path);
            free_c_string(d.file_format);
            free_c_string(d.encryption_key);
        });
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
}

/// Converts core `ducklake_delete_file` records into the C row shape. Shared by
/// the committed dump and the transaction-aware one, so the two can never
/// drift in what they report.
pub(crate) fn delete_file_rows(
    rows: Vec<moraine::ffi_support::DeleteFileRecord>,
) -> Result<Vec<MoraineDeleteFileRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut v| {
            let path = to_c_string(std::mem::take(&mut v.path))?;
            let format = to_c_string(std::mem::take(&mut v.format))?;
            let encryption_key = opt_c_string(v.encryption_key.as_deref())?;
            Ok((v, path, format, encryption_key))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;
    Ok(owned
        .into_iter()
        .map(|(v, path, format, encryption_key)| {
            let (has_end, end) = opt_u64(v.end_snapshot);
            let (has_partial_max, partial_max) = opt_u64(v.partial_max);
            MoraineDeleteFileRow {
                delete_file_id: v.delete_file_id,
                table_id: v.table_id,
                begin_snapshot: v.begin_snapshot,
                has_end_snapshot: has_end,
                end_snapshot: end,
                data_file_id: v.data_file_id,
                path: path.into_raw(),
                path_is_relative: v.path_is_relative,
                format: format.into_raw(),
                delete_count: v.delete_count,
                file_size_bytes: v.file_size_bytes,
                footer_size: v.footer_size,
                encryption_key: opt_into_raw(encryption_key),
                has_partial_max,
                partial_max,
            }
        })
        .collect())
}

/// Dumps every `ducklake_delete_file` row — current and history — into
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
        free_rows(items, len, |d| {
            free_c_string(d.path);
            free_c_string(d.format);
            free_c_string(d.encryption_key);
        });
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

/// Converts core `ducklake_file_partition_value` records into the C row
/// shape. Shared by the committed dump and the transaction-aware one, so
/// the two can never drift in what they report.
pub(crate) fn file_partition_value_rows(
    rows: Vec<moraine::ffi_support::FilePartitionValueRow>,
) -> Result<Vec<MoraineFilePartitionValueRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut row| {
            let partition_value = to_c_string(std::mem::take(&mut row.partition_value))?;
            Ok((row, partition_value))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;

    Ok(owned
        .into_iter()
        .map(|(row, partition_value)| MoraineFilePartitionValueRow {
            data_file_id: row.data_file_id,
            table_id: row.table_id,
            partition_key_index: row.partition_key_index,
            partition_value: partition_value.into_raw(),
        })
        .collect())
}

/// Dumps every `ducklake_file_partition_value` row — one per embedded
/// partition value of every data-file record, current and history — into
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
        free_rows(items, len, |v| {
            free_c_string(v.partition_value);
        });
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

/// Converts core `ducklake_files_scheduled_for_deletion` records into the C row
/// shape. Shared by the committed dump and the transaction-aware one, so the
/// two can never drift in what they report.
pub(crate) fn scheduled_deletion_rows(
    rows: Vec<moraine::ffi_support::GcFileRecord>,
) -> Result<Vec<MoraineScheduledDeletionRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut row| {
            let path = to_c_string(std::mem::take(&mut row.path))?;
            Ok((row, path))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;

    Ok(owned
        .into_iter()
        .map(|(row, path)| MoraineScheduledDeletionRow {
            data_file_id: row.data_file_id,
            path: path.into_raw(),
            path_is_relative: row.path_is_relative,
            schedule_start_micros: row.schedule_start_micros,
        })
        .collect())
}

/// Dumps every `ducklake_files_scheduled_for_deletion` row into
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
        free_rows(items, len, |r| {
            free_c_string(r.path);
        });
    }
}
