//! Dumps for the catalog entity tables: `ducklake_schema`,
//! `ducklake_table`, `ducklake_view`, and `ducklake_column`.

use std::ffi::{c_char, c_void};

use super::{dump_rows, free_rows, opt_c_string, opt_into_raw, opt_u64};
use crate::{
    abi::{free_c_string, to_c_string},
    error::{AbiError, MoraineError},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// One `ducklake_schema` row, as returned by [`moraine_dump_schemas`].
#[repr(C)]
pub struct MoraineSchemaRow {
    /// `schema_id`.
    pub schema_id: u64,
    /// `schema_uuid`, owned.
    pub schema_uuid: *mut c_char,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present (`false` for a live/current row).
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `schema_name`, owned.
    pub schema_name: *mut c_char,
    /// `path`, owned.
    pub path: *mut c_char,
    /// `path_is_relative`.
    pub path_is_relative: bool,
}

/// Converts core `ducklake_schema` records into the C row shape. Shared by the
/// committed dump and the transaction-aware one, so the two can never
/// drift in what they report.
pub(crate) fn schema_rows(
    rows: Vec<moraine::ffi_support::SchemaRecord>,
) -> Result<Vec<MoraineSchemaRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut v| {
            let schema_uuid = to_c_string(std::mem::take(&mut v.schema_uuid))?;
            let schema_name = to_c_string(std::mem::take(&mut v.schema_name))?;
            let path = to_c_string(std::mem::take(&mut v.path))?;
            Ok((v, schema_uuid, schema_name, path))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;

    Ok(owned
        .into_iter()
        .map(|(v, schema_uuid, schema_name, path)| {
            let (has_end, end) = opt_u64(v.end_snapshot);
            MoraineSchemaRow {
                schema_id: v.schema_id,
                schema_uuid: schema_uuid.into_raw(),
                begin_snapshot: v.begin_snapshot,
                has_end_snapshot: has_end,
                end_snapshot: end,
                schema_name: schema_name.into_raw(),
                path: path.into_raw(),
                path_is_relative: v.path_is_relative,
            }
        })
        .collect())
}

/// Dumps every `ducklake_schema` row — current and history — into
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
pub unsafe extern "C" fn moraine_dump_schemas(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSchemaRow,
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
            moraine::ffi_support::dump_schemas,
            schema_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_schemas`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching [`moraine_dump_schemas`]
/// call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_schemas_free(items: *mut MoraineSchemaRow, len: usize) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len, |d| {
            free_c_string(d.schema_uuid);
            free_c_string(d.schema_name);
            free_c_string(d.path);
        });
    }
}

/// One `ducklake_table` row, as returned by [`moraine_dump_tables`].
/// `next_column_id` is moraine-internal field-id bookkeeping, not a
/// DuckLake column, and is not carried here.
#[repr(C)]
pub struct MoraineTableRow {
    /// `table_id`.
    pub table_id: u64,
    /// `table_uuid`, owned.
    pub table_uuid: *mut c_char,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `schema_id`.
    pub schema_id: u64,
    /// `table_name`, owned.
    pub table_name: *mut c_char,
    /// `path`, owned.
    pub path: *mut c_char,
    /// `path_is_relative`.
    pub path_is_relative: bool,
}

/// Converts core `ducklake_table` records into the C row shape. Shared by the
/// committed dump and the transaction-aware one, so the two can never
/// drift in what they report.
pub(crate) fn table_rows(
    rows: Vec<moraine::ffi_support::TableRecord>,
) -> Result<Vec<MoraineTableRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut v| {
            let table_uuid = to_c_string(std::mem::take(&mut v.table_uuid))?;
            let table_name = to_c_string(std::mem::take(&mut v.table_name))?;
            let path = to_c_string(std::mem::take(&mut v.path))?;
            Ok((v, table_uuid, table_name, path))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;
    Ok(owned
        .into_iter()
        .map(|(v, table_uuid, table_name, path)| {
            let (has_end, end) = opt_u64(v.end_snapshot);
            MoraineTableRow {
                table_id: v.table_id,
                table_uuid: table_uuid.into_raw(),
                begin_snapshot: v.begin_snapshot,
                has_end_snapshot: has_end,
                end_snapshot: end,
                schema_id: v.schema_id,
                table_name: table_name.into_raw(),
                path: path.into_raw(),
                path_is_relative: v.path_is_relative,
            }
        })
        .collect())
}

/// Dumps every `ducklake_table` row — current and history — into
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
pub unsafe extern "C" fn moraine_dump_tables(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineTableRow,
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
            moraine::ffi_support::dump_tables,
            table_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_tables`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching [`moraine_dump_tables`]
/// call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_tables_free(items: *mut MoraineTableRow, len: usize) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len, |d| {
            free_c_string(d.table_uuid);
            free_c_string(d.table_name);
            free_c_string(d.path);
        });
    }
}

/// One `ducklake_view` row, as returned by [`moraine_dump_views`].
#[repr(C)]
pub struct MoraineViewRow {
    /// `view_id`.
    pub view_id: u64,
    /// `view_uuid`, owned.
    pub view_uuid: *mut c_char,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `schema_id`.
    pub schema_id: u64,
    /// `view_name`, owned.
    pub view_name: *mut c_char,
    /// `dialect`, owned.
    pub dialect: *mut c_char,
    /// `sql`, owned.
    pub sql: *mut c_char,
    /// `column_aliases`, owned, null if absent.
    pub column_aliases: *mut c_char,
}

/// Converts core `ducklake_view` records into the C row shape. Shared by the
/// committed dump and the transaction-aware one, so the two can never
/// drift in what they report.
pub(crate) fn view_rows(
    rows: Vec<moraine::ffi_support::ViewRecord>,
) -> Result<Vec<MoraineViewRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut v| {
            let view_uuid = to_c_string(std::mem::take(&mut v.view_uuid))?;
            let view_name = to_c_string(std::mem::take(&mut v.view_name))?;
            let dialect = to_c_string(std::mem::take(&mut v.dialect))?;
            let sql = to_c_string(std::mem::take(&mut v.sql))?;
            let column_aliases = opt_c_string(v.column_aliases.as_deref())?;
            Ok((v, view_uuid, view_name, dialect, sql, column_aliases))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;

    Ok(owned
        .into_iter()
        .map(|(v, view_uuid, view_name, dialect, sql, column_aliases)| {
            let (has_end, end) = opt_u64(v.end_snapshot);
            MoraineViewRow {
                view_id: v.view_id,
                view_uuid: view_uuid.into_raw(),
                begin_snapshot: v.begin_snapshot,
                has_end_snapshot: has_end,
                end_snapshot: end,
                schema_id: v.schema_id,
                view_name: view_name.into_raw(),
                dialect: dialect.into_raw(),
                sql: sql.into_raw(),
                column_aliases: opt_into_raw(column_aliases),
            }
        })
        .collect())
}

/// Dumps every `ducklake_view` row — current and history — into
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
pub unsafe extern "C" fn moraine_dump_views(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineViewRow,
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
            moraine::ffi_support::dump_views,
            view_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_views`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching [`moraine_dump_views`]
/// call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_views_free(items: *mut MoraineViewRow, len: usize) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len, |d| {
            free_c_string(d.view_uuid);
            free_c_string(d.view_name);
            free_c_string(d.dialect);
            free_c_string(d.sql);
            free_c_string(d.column_aliases);
        });
    }
}

/// One `ducklake_column` row, as returned by [`moraine_dump_columns`].
/// Column tags (`ducklake_column_tag`) are a separate table, not a
/// column, and are not carried here.
#[repr(C)]
pub struct MoraineColumnRow {
    /// `column_id`.
    pub column_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `column_order`.
    pub column_order: u64,
    /// `column_name`, owned.
    pub column_name: *mut c_char,
    /// `column_type`, owned.
    pub column_type: *mut c_char,
    /// `initial_default`, owned, null if absent.
    pub initial_default: *mut c_char,
    /// `default_value`, owned, null if absent.
    pub default_value: *mut c_char,
    /// `nulls_allowed`.
    pub nulls_allowed: bool,
    /// Whether `parent_column` is present.
    pub has_parent_column: bool,
    /// `parent_column`, valid iff `has_parent_column`.
    pub parent_column: u64,
    /// `default_value_type`, owned, null if absent.
    pub default_value_type: *mut c_char,
    /// `default_value_dialect`, owned, null if absent.
    pub default_value_dialect: *mut c_char,
}

/// Converts core `ducklake_column` records into the C row shape. Shared by the
/// committed dump and the transaction-aware one, so the two can never
/// drift in what they report.
pub(crate) fn column_rows(
    rows: Vec<moraine::ffi_support::ColumnRecord>,
) -> Result<Vec<MoraineColumnRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|mut v| {
            let column_name = to_c_string(std::mem::take(&mut v.column_name))?;
            let column_type = to_c_string(std::mem::take(&mut v.column_type))?;
            let initial_default = opt_c_string(v.initial_default.as_deref())?;
            let default_value = opt_c_string(v.default_value.as_deref())?;
            let default_value_type = opt_c_string(v.default_value_type.as_deref())?;
            let default_value_dialect = opt_c_string(v.default_value_dialect.as_deref())?;
            Ok((
                v,
                column_name,
                column_type,
                initial_default,
                default_value,
                default_value_type,
                default_value_dialect,
            ))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;
    Ok(owned
        .into_iter()
        .map(
            |(
                v,
                column_name,
                column_type,
                initial_default,
                default_value,
                default_value_type,
                default_value_dialect,
            )| {
                let (has_end, end) = opt_u64(v.end_snapshot);
                let (has_parent, parent) = opt_u64(v.parent_column);

                MoraineColumnRow {
                    column_id: v.column_id,
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    table_id: v.table_id,
                    column_order: v.column_order,
                    column_name: column_name.into_raw(),
                    column_type: column_type.into_raw(),
                    initial_default: opt_into_raw(initial_default),
                    default_value: opt_into_raw(default_value),
                    nulls_allowed: v.nulls_allowed,
                    has_parent_column: has_parent,
                    parent_column: parent,
                    default_value_type: opt_into_raw(default_value_type),
                    default_value_dialect: opt_into_raw(default_value_dialect),
                }
            },
        )
        .collect())
}

/// Dumps every `ducklake_column` row — current and history — into
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
pub unsafe extern "C" fn moraine_dump_columns(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineColumnRow,
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
            moraine::ffi_support::dump_columns,
            column_rows,
        )
    }
}

/// Frees the array returned by [`moraine_dump_columns`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pair a matching [`moraine_dump_columns`]
/// call wrote, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_columns_free(items: *mut MoraineColumnRow, len: usize) {
    // SAFETY: forwarded caller contract.
    unsafe {
        free_rows(items, len, |d| {
            free_c_string(d.column_name);
            free_c_string(d.column_type);
            free_c_string(d.initial_default);
            free_c_string(d.default_value);
            free_c_string(d.default_value_type);
            free_c_string(d.default_value_dialect);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::CStr, ptr, sync::Arc};

    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::{
        abi::{moraine_detach, moraine_error_free},
        dumps::test_support::{TempDir, attach_ok, seed},
        error::codes,
    };

    #[test]
    fn dump_schemas_and_tables_return_current_and_history_rows() {
        let dir = TempDir::new("schemas-tables");
        seed(dir.path());
        let handle = attach_ok(dir.path());

        let mut schemas: *mut MoraineSchemaRow = ptr::null_mut();
        let mut schemas_len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_schemas(
                handle,
                &raw mut schemas,
                &raw mut schemas_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        // `main` (bootstrap) + `sales`; the rename never touched a
        // schema, so neither carries a history row.
        assert_eq!(schemas_len, 2);
        // SAFETY: just populated above with `schemas_len` live elements.
        let schema_slice = unsafe { std::slice::from_raw_parts(schemas, schemas_len) };
        assert!(schema_slice.iter().all(|s| !s.has_end_snapshot));

        let mut tables: *mut MoraineTableRow = ptr::null_mut();
        let mut tables_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_tables(
                handle,
                &raw mut tables,
                &raw mut tables_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(
            tables_len, 2,
            "rename must yield one current row + one history row"
        );
        // SAFETY: just populated above with `tables_len` live elements.
        let table_slice = unsafe { std::slice::from_raw_parts(tables, tables_len) };
        let ended = table_slice
            .iter()
            .find(|t| t.has_end_snapshot)
            .expect("one row must be ended");
        let live = table_slice
            .iter()
            .find(|t| !t.has_end_snapshot)
            .expect("one row must be live");
        // SAFETY: owned C strings written above, not yet freed.
        let ended_name = unsafe { CStr::from_ptr(ended.table_name) }
            .to_str()
            .unwrap();
        // SAFETY: same as above.
        let live_name = unsafe { CStr::from_ptr(live.table_name) }.to_str().unwrap();
        assert_eq!(ended_name, "orders");
        assert_eq!(live_name, "orders2");
        assert_eq!(ended.table_id, live.table_id);
        // Exact lifecycle stitching: the ended row's end is the live
        // row's begin.
        assert_eq!(ended.end_snapshot, live.begin_snapshot);
        assert!(live.begin_snapshot > ended.begin_snapshot);

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            moraine_dump_schemas_free(schemas, schemas_len);
            moraine_dump_tables_free(tables, tables_len);
            moraine_detach(handle);
        }
    }

    /// A catalog string with an embedded NUL fails `moraine_dump_views`
    /// with `CORRUPTION`, leaves the outputs untouched, and leaks nothing
    /// from the rows that converted before it.
    #[test]
    fn embedded_nul_in_view_sql_reports_corruption_and_leaks_nothing() {
        let dir = TempDir::new("embedded-nul");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test setup: build tokio runtime");
        rt.block_on(async {
            let store = Arc::new(
                LocalFileSystem::new_with_prefix(dir.path()).expect("test setup: open local store"),
            );
            let catalog = moraine::Catalog::open(store, moraine::CatalogOptions::default())
                .await
                .expect("test setup: open catalog");
            catalog
                .commit(|tx| {
                    let schema = tx.create_schema("s")?;
                    tx.create_view(schema, "clean", "duckdb", "select 1")?;
                    tx.create_view(schema, "poisoned", "duckdb", "select 1 as a\0b")?;
                    Ok(())
                })
                .await
                .expect("test setup: commit fixtures");
            catalog.close().await.expect("test setup: close catalog");
        });

        let handle = attach_ok(dir.path());
        let mut views: *mut MoraineViewRow = ptr::null_mut();
        let mut views_len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_views(
                handle,
                &raw mut views,
                &raw mut views_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::CORRUPTION);
        assert_eq!(err.code, codes::CORRUPTION);
        // Nothing was handed to the caller, so there is nothing to free.
        assert!(views.is_null());
        assert_eq!(views_len, 0);
        // SAFETY: just populated above.
        let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
        assert!(msg.contains("NUL"), "message: {msg}");

        // SAFETY: `err.message` was just populated and not yet freed;
        // `handle` came from `attach_ok` and is freed exactly once.
        unsafe {
            moraine_error_free(err.message);
            moraine_detach(handle);
        }
    }
}
