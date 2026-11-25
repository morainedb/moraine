//! Data file paths by id, for the located scan: the path and relativity
//! flag the catalog records for each file it names, read without
//! materializing a file list.

use std::{
    ffi::{CString, c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::{borrow_str, free_array, free_c_string, guard, resolve_table, write_reserved_array};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One data file's recorded path, as [`moraine_data_file_paths`] returns
/// it. Free with [`moraine_data_file_paths_free`].
#[repr(C)]
pub struct MoraineDataFilePath {
    /// The data file.
    pub data_file_id: u64,
    /// The path the catalog records, owned.
    pub path: *mut c_char,
    /// Whether `path` is relative to the table's data path.
    pub path_is_relative: bool,
}

/// Resolves data file ids of one table to the paths the catalog records at
/// the handle's pinned revision. Every id must name a current data file of
/// the table. Free the result with [`moraine_data_file_paths_free`].
///
/// # Safety
/// Every pointer must satisfy the ABI contract; `file_ids` points to
/// `file_ids_len` ids, output pointers are non-null and writable, and
/// `probe`/`probe_ctx` satisfy the interrupt-probe contract.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_data_file_paths(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    file_ids: *const u64,
    file_ids_len: usize,
    out_items: *mut *mut MoraineDataFilePath,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(Vec<MoraineDataFilePath>, moraine::MemoryReservation), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        if file_ids_len > 0 && file_ids.is_null() {
            return Err(AbiError::invalid_argument("`file_ids` is null"));
        }

        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        // SAFETY: caller contract — `file_ids` points to `file_ids_len` ids.
        let ids = unsafe {
            if file_ids_len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(file_ids, file_ids_len)
            }
        };
        let files: Vec<moraine::DataFileId> =
            ids.iter().map(|&id| moraine::DataFileId::new(id)).collect();

        let reads = handle_ref.catalog.reads();
        // SAFETY: caller contract for `probe` and `probe_ctx`.
        let view = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::catalog_read::table_sources_for(reads, schema, table, &files),
            )
        }?;
        let table_id = resolve_table(&view, schema, table)?;

        let mut records = Vec::with_capacity(files.len());
        let mut payload = 0_u64;
        for file in files {
            let Some(info) = view.data_file(table_id, file) else {
                return Err(AbiError::from(moraine::Error::NotFound(format!(
                    "data file {} of table {table}",
                    file.get()
                ))));
            };
            payload = payload
                .saturating_add(info.path.len() as u64)
                .saturating_add(1);
            records.push(info);
        }
        let memory = crate::abi::reserve_array::<MoraineDataFilePath>(records.len(), payload)?;

        // Owned first: every string is built before any raw pointer is
        // minted, so a failure here leaks nothing.
        let paths = records
            .iter()
            .map(|info| {
                CString::new(info.path.as_str())
                    .map_err(|_| AbiError::invalid_argument("a data file path holds a NUL byte"))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;
        let rows = records
            .iter()
            .zip(paths)
            .map(|(info, path)| MoraineDataFilePath {
                data_file_id: info.id.get(),
                path: path.into_raw(),
                path_is_relative: info.path_is_relative,
            })
            .collect();
        Ok((rows, memory))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok((rows, memory)) => {
            // SAFETY: caller contract for the output pointers.
            unsafe {
                write_reserved_array(rows, memory, out_items, out_len);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Frees the array [`moraine_data_file_paths`] wrote to
/// `out_items`/`out_len`, including each row's path.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written there by a
/// matching call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_data_file_paths_free(items: *mut MoraineDataFilePath, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above; each path is the `CString` this
        // call handed out.
        unsafe {
            free_array(items, len, |item| free_c_string(item.path));
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}
