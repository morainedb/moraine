//! Dumps for the column-mapping tables: `ducklake_column_mapping` and
//! `ducklake_name_mapping`.

use std::ffi::{c_char, c_void};

use super::{dump_rows, free_rows, opt_u64};
use crate::{
    abi::{free_c_string, to_c_string},
    error::{AbiError, MoraineError},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One `ducklake_column_mapping` row, as returned by
/// [`moraine_dump_column_mappings`].
#[repr(C)]
pub struct MoraineColumnMappingRow {
    /// `mapping_id`.
    pub mapping_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `type`, owned.
    pub map_type: *mut c_char,
}

/// Converts core `ducklake_column_mapping` records into the C row shape. Shared
/// by the committed dump and the transaction-aware one, so the two can never
/// drift in what they report.
pub(crate) fn mapping_rows(
    rows: Vec<moraine::ffi_support::MappingRecord>,
) -> Result<Vec<MoraineColumnMappingRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut m| {
            let map_type = to_c_string(std::mem::take(&mut m.map_type))?;
            Ok((m, map_type))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;

    Ok(owned
        .into_iter()
        .map(|(m, map_type)| MoraineColumnMappingRow {
            mapping_id: m.mapping_id,
            table_id: m.table_id,
            map_type: map_type.into_raw(),
        })
        .collect())
}

/// Dumps every `ducklake_column_mapping` row into
/// `*out_items`/`*out_len`. Mappings are unversioned create-only records,
/// so this is exactly the live rows.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach`](crate::abi::moraine_attach), valid writable
/// `out_items`/`out_len`, a `probe` callable with `probe_ctx` from any
/// thread, and a null-or-writable `err`, all for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_column_mappings(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineColumnMappingRow,
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
            moraine::ffi_support::dump_mappings,
            mapping_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_column_mappings`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_column_mappings`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_column_mappings_free(
    items: *mut MoraineColumnMappingRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len, |d| {
            free_c_string(d.map_type);
        });
    }
}

/// One `ducklake_name_mapping` row, as returned by
/// [`moraine_dump_name_mappings`].
#[repr(C)]
pub struct MoraineNameMappingRow {
    /// `mapping_id`.
    pub mapping_id: u64,
    /// `column_id`: the row's 0-based ordinal within its mapping.
    pub column_id: u64,
    /// `source_name`, owned.
    pub source_name: *mut c_char,
    /// `target_field_id`.
    pub target_field_id: u64,
    /// Whether `parent_column` is present.
    pub has_parent_column: bool,
    /// `parent_column`, valid iff `has_parent_column`.
    pub parent_column: u64,
    /// `is_partition`.
    pub is_partition: bool,
}

/// Converts core `ducklake_name_mapping` records into the C row shape. Shared
/// by the committed dump and the transaction-aware one, so the two can never
/// drift in what they report.
pub(crate) fn name_mapping_rows(
    rows: Vec<moraine::ffi_support::NameMappingRow>,
) -> Result<Vec<MoraineNameMappingRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut row| {
            let source_name = to_c_string(std::mem::take(&mut row.source_name))?;
            Ok((row, source_name))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;

    Ok(owned
        .into_iter()
        .map(|(row, source_name)| {
            let (has_parent, parent) = opt_u64(row.parent_column);
            MoraineNameMappingRow {
                mapping_id: row.mapping_id,
                column_id: row.column_id,
                source_name: source_name.into_raw(),
                target_field_id: row.target_field_id,
                has_parent_column: has_parent,
                parent_column: parent,
                is_partition: row.is_partition,
            }
        })
        .collect())
}

/// Dumps every `ducklake_name_mapping` row — flattened from the embedded
/// rows of every mapping record, ordered by `(mapping_id, column_id)` —
/// into `*out_items`/`*out_len`.
///
/// # Safety
///
/// The shared dump-entry contract (`dump_rows`): a live `handle` from
/// [`moraine_attach`](crate::abi::moraine_attach), valid writable
/// `out_items`/`out_len`, a `probe` callable with `probe_ctx` from any
/// thread, and a null-or-writable `err`, all for the duration of the
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_name_mappings(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineNameMappingRow,
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
            moraine::ffi_support::dump_name_mapping_rows,
            name_mapping_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_name_mappings`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching
/// [`moraine_dump_name_mappings`] call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_name_mappings_free(
    items: *mut MoraineNameMappingRow,
    len: usize,
) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len, |d| {
            free_c_string(d.source_name);
        });
    }
}
