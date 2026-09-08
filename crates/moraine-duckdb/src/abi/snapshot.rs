//! Snapshot handles and catalog descriptor arrays.

use std::{
    ffi::{CString, c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::{free_array, free_c_string, guard, snapshot_list, to_c_string};
use crate::{
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe, MoraineSnapshotHandle},
};

/// Materializes the catalog's current snapshot and writes the resulting
/// handle to `*out`.
///
/// Cancellable: races the core read against `probe` (polled
/// immediately, then ~100 ms; a null `probe` disables polling). If a
/// cancellation wins, returns [`codes::INTERRUPTED`] and `*out` is left
/// unwritten.
///
/// # Safety
///
/// `handle` must be a pointer previously returned by [`super::moraine_attach`]
/// and not yet detached. `out` must be a valid, writable
/// `*mut *mut MoraineSnapshotHandle`. `probe`, if non-null, must be safe
/// to call with `probe_ctx` from any thread. `err`, if non-null, must be
/// a valid, writable [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot(
    handle: *mut MoraineCatalogHandle,
    out: *mut *mut MoraineSnapshotHandle,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Box<MoraineSnapshotHandle>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out.is_null() {
            return Err(AbiError::invalid_argument("`out` is null"));
        }
        // SAFETY: `handle` validity is this function's own safety contract.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.reads().snapshot())
        }?;
        Ok(Box::new(MoraineSnapshotHandle::new(snapshot)))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(handle) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe {
                *out = Box::into_raw(handle);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Frees a snapshot handle previously returned by [`moraine_snapshot`].
/// A null `snapshot` is a no-op.
///
/// # Safety
///
/// `snapshot`, if non-null, must be a pointer previously returned by
/// [`moraine_snapshot`] and not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_free(snapshot: *mut MoraineSnapshotHandle) {
    if snapshot.is_null() {
        return;
    }
    let attempt = || {
        // SAFETY: caller contract above.
        drop(unsafe { Box::from_raw(snapshot) });
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One schema, as returned by [`moraine_snapshot_schemas`].
#[repr(C)]
pub struct MoraineSchemaDesc {
    /// The schema's id.
    pub id: u64,
    /// The schema's name, owned — free via
    /// [`moraine_snapshot_schemas_free`].
    pub name: *mut c_char,
}

/// Lists the snapshot's live schemas into `*out_items`/`*out_len`.
///
/// # Safety
///
/// `snapshot` must be a pointer previously returned by
/// [`moraine_snapshot`]. `out_items`/`out_len` must be valid, writable
/// pointers. `err`, if non-null, must be a valid, writable
/// [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_schemas(
    snapshot: *mut MoraineSnapshotHandle,
    out_items: *mut *mut MoraineSchemaDesc,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: caller contract for the pointers.
    unsafe {
        snapshot_list(snapshot, out_items, out_len, err, |snapshot| {
            let owned: Vec<(u64, CString)> = snapshot
                .schemas()
                .into_iter()
                .map(|s| Ok((s.id.get(), to_c_string(s.name)?)))
                .collect::<Result<_, AbiError>>()?;

            Ok(owned
                .into_iter()
                .map(|(id, name)| MoraineSchemaDesc {
                    id,
                    name: name.into_raw(),
                })
                .collect())
        })
    }
}

/// Frees an array returned by [`moraine_snapshot_schemas`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_snapshot_schemas`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_schemas_free(items: *mut MoraineSchemaDesc, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| free_c_string(d.name));
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One table, as returned by [`moraine_snapshot_tables_in`].
#[repr(C)]
pub struct MoraineTableDesc {
    /// The table's id.
    pub id: u64,
    /// The schema the table belongs to.
    pub schema_id: u64,
    /// The table's name, owned — free via
    /// [`moraine_snapshot_tables_in_free`].
    pub name: *mut c_char,
}

/// Lists the live tables of schema `schema_id` into
/// `*out_items`/`*out_len`. A schema with no live tables (or an unknown
/// `schema_id`) yields an empty array, not an error.
///
/// # Safety
///
/// Same pointer contract as [`moraine_snapshot_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_tables_in(
    snapshot: *mut MoraineSnapshotHandle,
    schema_id: u64,
    out_items: *mut *mut MoraineTableDesc,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: caller contract for the pointers.
    unsafe {
        snapshot_list(snapshot, out_items, out_len, err, |snapshot| {
            let owned: Vec<(u64, u64, CString)> = snapshot
                .tables_in(moraine::SchemaId::new(schema_id))
                .into_iter()
                .map(|t| Ok((t.id.get(), t.schema_id.get(), to_c_string(t.name)?)))
                .collect::<Result<_, AbiError>>()?;

            Ok(owned
                .into_iter()
                .map(|(id, schema_id, name)| MoraineTableDesc {
                    id,
                    schema_id,
                    name: name.into_raw(),
                })
                .collect())
        })
    }
}

/// Frees an array returned by [`moraine_snapshot_tables_in`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_snapshot_tables_in`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_tables_in_free(items: *mut MoraineTableDesc, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| free_c_string(d.name));
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One column, as returned by [`moraine_snapshot_columns_of`].
#[repr(C)]
pub struct MoraineColumnDesc {
    /// The column's field id.
    pub id: u64,
    /// The column's name, owned — free via
    /// [`moraine_snapshot_columns_of_free`].
    pub name: *mut c_char,
    /// The column's DuckLake type string, owned — free via
    /// [`moraine_snapshot_columns_of_free`].
    pub sql_type: *mut c_char,
    /// Whether NULL values are allowed.
    pub nulls_allowed: bool,
    /// Whether this is a nested child column (a `STRUCT` field, `LIST`
    /// element, or `MAP` key/value); `parent_column` is meaningful iff set.
    pub has_parent_column: bool,
    /// The parent column's field id when `has_parent_column`.
    pub parent_column: u64,
}

/// Lists the live columns of table `table_id`, ordered by position, into
/// `*out_items`/`*out_len`. An unknown `table_id` yields an empty array,
/// not an error.
///
/// # Safety
///
/// Same pointer contract as [`moraine_snapshot_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_columns_of(
    snapshot: *mut MoraineSnapshotHandle,
    table_id: u64,
    out_items: *mut *mut MoraineColumnDesc,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: caller contract for the pointers.
    unsafe {
        snapshot_list(snapshot, out_items, out_len, err, |snapshot| {
            let owned: Vec<(u64, CString, CString, bool, Option<u64>)> = snapshot
                .columns_of(moraine::TableId::new(table_id))
                .into_iter()
                .map(|c| {
                    Ok((
                        c.id.get(),
                        to_c_string(c.name)?,
                        to_c_string(c.column_type)?,
                        c.nulls_allowed,
                        c.parent_column.map(moraine::ColumnId::get),
                    ))
                })
                .collect::<Result<_, AbiError>>()?;
            Ok(owned
                .into_iter()
                .map(
                    |(id, name, sql_type, nulls_allowed, parent)| MoraineColumnDesc {
                        id,
                        name: name.into_raw(),
                        sql_type: sql_type.into_raw(),
                        nulls_allowed,
                        has_parent_column: parent.is_some(),
                        parent_column: parent.unwrap_or(0),
                    },
                )
                .collect())
        })
    }
}

/// Frees an array returned by [`moraine_snapshot_columns_of`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_snapshot_columns_of`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_columns_of_free(
    items: *mut MoraineColumnDesc,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.name);
                free_c_string(d.sql_type);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One view, as returned by [`moraine_snapshot_views_in`].
#[repr(C)]
pub struct MoraineViewDesc {
    /// The view's id.
    pub id: u64,
    /// The schema the view belongs to.
    pub schema_id: u64,
    /// The view's name, owned — free via
    /// [`moraine_snapshot_views_in_free`].
    pub name: *mut c_char,
    /// SQL dialect of the definition, owned — free via
    /// [`moraine_snapshot_views_in_free`].
    pub dialect: *mut c_char,
    /// The view's defining SQL, owned — free via
    /// [`moraine_snapshot_views_in_free`].
    pub sql: *mut c_char,
}

/// Lists the live views of schema `schema_id` into
/// `*out_items`/`*out_len`. A schema with no live views (or an unknown
/// `schema_id`) yields an empty array, not an error.
///
/// # Safety
///
/// Same pointer contract as [`moraine_snapshot_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_views_in(
    snapshot: *mut MoraineSnapshotHandle,
    schema_id: u64,
    out_items: *mut *mut MoraineViewDesc,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: caller contract for the pointers.
    unsafe {
        snapshot_list(snapshot, out_items, out_len, err, |snapshot| {
            let owned: Vec<(u64, u64, CString, CString, CString)> = snapshot
                .views_in(moraine::SchemaId::new(schema_id))
                .into_iter()
                .map(|v| {
                    Ok((
                        v.id.get(),
                        v.schema_id.get(),
                        to_c_string(v.name)?,
                        to_c_string(v.dialect)?,
                        to_c_string(v.sql)?,
                    ))
                })
                .collect::<Result<_, AbiError>>()?;
            Ok(owned
                .into_iter()
                .map(|(id, schema_id, name, dialect, sql)| MoraineViewDesc {
                    id,
                    schema_id,
                    name: name.into_raw(),
                    dialect: dialect.into_raw(),
                    sql: sql.into_raw(),
                })
                .collect())
        })
    }
}

/// Frees an array returned by [`moraine_snapshot_views_in`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_snapshot_views_in`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_views_in_free(items: *mut MoraineViewDesc, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.name);
                free_c_string(d.dialect);
                free_c_string(d.sql);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One live data file, as returned by [`moraine_snapshot_data_files_of`].
#[repr(C)]
pub struct MoraineDataFileDesc {
    /// The file's id.
    pub id: u64,
    /// Object-store path, owned — free via
    /// [`moraine_snapshot_data_files_of_free`].
    pub path: *mut c_char,
    /// Whether `path` is relative to the table's location.
    pub path_is_relative: bool,
    /// Number of rows in the file.
    pub record_count: u64,
    /// Whether `row_id_start` is present (absent when the file's rows
    /// carry explicit per-row ids, e.g. compaction outputs).
    pub has_row_id_start: bool,
    /// First row id of the file's dense per-table row-id range, valid
    /// iff `has_row_id_start`.
    pub row_id_start: u64,
    /// Total file size in bytes.
    pub file_size_bytes: u64,
    /// Footer size in bytes.
    pub footer_size: u64,
}

/// Lists the live data files of table `table_id` into
/// `*out_items`/`*out_len`. An unknown `table_id` yields an empty array,
/// not an error.
///
/// # Safety
///
/// Same pointer contract as [`moraine_snapshot_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_data_files_of(
    snapshot: *mut MoraineSnapshotHandle,
    table_id: u64,
    out_items: *mut *mut MoraineDataFileDesc,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: caller contract for the pointers.
    unsafe {
        snapshot_list(snapshot, out_items, out_len, err, |snapshot| {
            let owned: Vec<(CString, moraine::DataFileInfo)> = snapshot
                .data_files_of(moraine::TableId::new(table_id))
                .into_iter()
                .map(|mut f| Ok((to_c_string(std::mem::take(&mut f.path))?, f)))
                .collect::<Result<_, AbiError>>()?;
            Ok(owned
                .into_iter()
                .map(|(path, f)| MoraineDataFileDesc {
                    id: f.id.get(),
                    path: path.into_raw(),
                    path_is_relative: f.path_is_relative,
                    record_count: f.record_count,
                    has_row_id_start: f.row_id_start.is_some(),
                    row_id_start: f.row_id_start.unwrap_or_default(),
                    file_size_bytes: f.file_size_bytes,
                    footer_size: f.footer_size,
                })
                .collect())
        })
    }
}

/// Frees an array returned by [`moraine_snapshot_data_files_of`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_snapshot_data_files_of`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_data_files_of_free(
    items: *mut MoraineDataFileDesc,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| free_c_string(d.path));
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}
