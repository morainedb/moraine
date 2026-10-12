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
/// `handle` must be a pointer previously returned by
/// [`super::moraine_attach_with_wal_store`] and not yet detached. `out` must be
/// a valid, writable `*mut *mut MoraineSnapshotHandle`. `probe`, if non-null,
/// must be safe to call with `probe_ctx` from any thread. `err`, if non-null,
/// must be a valid, writable [`MoraineError`]. All for the duration of this
/// call.
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

/// Frees a snapshot handle returned by [`moraine_snapshot`] or
/// [`super::moraine_snapshot_scoped`].
/// A null `snapshot` is a no-op.
///
/// # Safety
///
/// `snapshot`, if non-null, must be a pointer previously returned by
/// either snapshot-opening entry point and not yet freed.
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

/// Writes the store stamp `snapshot` stands at: the head snapshot id and
/// batch count, as `moraine_head_stamp` would report them at the moment
/// the snapshot was taken.
///
/// # Safety
///
/// `snapshot` must point to a live [`MoraineSnapshotHandle`]; both output
/// pointers must be non-null and writable; `err`, if non-null, must be
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_stamp(
    snapshot: *mut MoraineSnapshotHandle,
    out_snapshot_id: *mut u64,
    out_batch_seq: *mut u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(u64, u64), AbiError> {
        if snapshot.is_null() {
            return Err(AbiError::invalid_argument("`snapshot` is null"));
        }
        if out_snapshot_id.is_null() || out_batch_seq.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `snapshot`.
        let snapshot = unsafe { &*snapshot };
        let stamp = moraine::ffi_support::snapshot_stamp(&snapshot.snapshot);
        Ok((stamp.snapshot_id, stamp.batch_seq))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok((snapshot_id, batch_seq)) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe {
                *out_snapshot_id = snapshot_id;
                *out_batch_seq = batch_seq;
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Writes the id of the snapshot `snapshot` views to `*out_snapshot_id`.
///
/// # Safety
///
/// `snapshot` must point to a live [`MoraineSnapshotHandle`];
/// `out_snapshot_id` must be non-null and writable; `err`, if non-null,
/// must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_id(
    snapshot: *mut MoraineSnapshotHandle,
    out_snapshot_id: *mut u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<u64, AbiError> {
        if snapshot.is_null() {
            return Err(AbiError::invalid_argument("`snapshot` is null"));
        }
        if out_snapshot_id.is_null() {
            return Err(AbiError::invalid_argument("`out_snapshot_id` is null"));
        }
        // SAFETY: caller contract for `snapshot`.
        let snapshot = unsafe { &*snapshot };
        Ok(snapshot.snapshot.current_snapshot().id.get())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(snapshot_id) => {
            // SAFETY: caller contract for `out_snapshot_id`.
            unsafe { *out_snapshot_id = snapshot_id };
            codes::OK
        }
        Err(code) => code,
    }
}
