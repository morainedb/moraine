//! Row-faithful `ducklake_*` dumps: one C array per table kind the store
//! models, carrying every current and history row with every lifecycle column
//! verbatim and unfiltered (DuckLake filters `begin_snapshot`/
//! `end_snapshot` itself, in SQL).
//!
//! Shares [`crate::abi`]'s conventions: `catch_unwind`/null/UTF-8
//! discipline via [`guard`](crate::abi), owned-first `CString`
//! construction, one `_free` per dump. Every function opens its own fresh
//! transaction against [`moraine::ffi_support`] — no snapshot handle is
//! involved, and no two dump calls are guaranteed to observe the same
//! head.
//!
//! Two nullability conventions cross the C boundary:
//! - an optional **string** is a null pointer for `None`;
//! - an optional **scalar** (`u64`/`bool`) is carried as a `has_<field>`
//!   companion flag next to the raw field, meaningless when the flag is
//!   `false`.

mod catalog;
mod files;
mod macros;
mod mappings;
mod options;
mod partitions;
mod snapshots;
mod statistics;
mod tags;

use std::{
    ffi::{CString, c_char, c_void},
    future::Future,
    ptr,
};

pub use catalog::*;
pub use files::*;
pub use macros::*;
pub use mappings::*;
pub use options::*;
pub use partitions::*;
pub use snapshots::*;
pub use statistics::*;
pub use tags::*;

use crate::{
    abi::{free_array, guard, to_c_string, write_array},
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// Splits an optional `u64` into the `(has, value)` pair the dump row
/// structs carry.
pub(crate) fn opt_u64(v: Option<u64>) -> (bool, u64) {
    v.map_or((false, 0), |x| (true, x))
}

/// Splits an optional `bool` into the `(has, value)` pair the dump row
/// structs carry.
pub(crate) fn opt_bool(v: Option<bool>) -> (bool, bool) {
    v.map_or((false, false), |x| (true, x))
}

/// Converts an optional string to an owned, possibly-null `CString`.
pub(crate) fn opt_c_string(s: Option<&str>) -> Result<Option<CString>, AbiError> {
    s.map(to_c_string).transpose()
}

/// The raw pointer for an optional owned `CString`: null for `None`.
pub(crate) fn opt_into_raw(s: Option<CString>) -> *mut c_char {
    s.map_or(ptr::null_mut(), CString::into_raw)
}

/// The shared shell of every dump entry point: null checks, the
/// cancellable bridge into the core `fetch`, `convert` to C rows, and
/// `write_array`, all under [`guard`](crate::abi). Every `moraine_dump_*`
/// export is a thin wrapper over this, so each concrete signature stays
/// visible to cbindgen (which generates `cpp/moraine_abi.h`).
///
/// Cancellable via `probe`/`probe_ctx` (polled immediately, then ~100 ms;
/// a null `probe` disables polling): a cancellation returns
/// [`codes::INTERRUPTED`] and the out-params are left unwritten.
///
/// # Safety
///
/// `handle` must be a pointer previously returned by
/// [`moraine_attach`](crate::abi::moraine_attach) and not yet detached.
/// `out_items`/`out_len` must be valid, writable pointers. `probe`, if
/// non-null, must be safe to call with `probe_ctx` from any thread.
/// `err`, if non-null, must be a valid, writable [`MoraineError`]. All
/// for the duration of the call.
#[allow(clippy::too_many_arguments)]
pub(crate) unsafe fn dump_rows<Row, Rows>(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut Row,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
    fetch: impl for<'c> FnOnce(
        &'c moraine::ReadOnlyCatalog,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<Rows, moraine::Error>> + 'c>,
    >,
    convert: impl FnOnce(Rows) -> Result<Vec<Row>, AbiError>,
) -> i32 {
    let attempt = || -> Result<Vec<Row>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is the caller's contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, fetch(handle_ref.catalog.reads()))
        }?;
        convert(rows)
    };

    // SAFETY: `err` validity is the caller's contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
    }
}

/// The shared shell of every dump `_free`: `release` each element's owned
/// strings and reclaim the array, under `catch_unwind` so a teardown can
/// never unwind into C++. Null `items` is a no-op.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by the
/// matching dump call, not yet freed.
pub(crate) unsafe fn free_rows<Row>(items: *mut Row, len: usize, release: impl FnMut(&mut Row)) {
    let attempt = std::panic::AssertUnwindSafe(|| {
        // SAFETY: forwarded caller contract.
        unsafe {
            free_array(items, len, release);
        }
    });
    let _ = std::panic::catch_unwind(attempt);
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Shared fixtures for the dump tests, reused by the per-family
    //! submodule tests.

    use std::{path::Path, sync::Arc};

    use moraine::{ColumnDef, ColumnStats, DataFile, DeleteFile, FileColumnStats};
    use object_store::local::LocalFileSystem;

    pub(crate) use crate::test_support::{TempDir, attach_ok};

    /// Seeds a catalog whose second commit renames the table `orders`,
    /// so `ducklake_table` carries one history row (the old name, ended)
    /// alongside its new current row — the fixture every dump test below
    /// exercises. Also carries a schema, two columns, a data file, a
    /// delete file, a view, and every statistics kind.
    pub(crate) fn seed(dir: &Path) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test setup: build tokio runtime");

        rt.block_on(async {
            let store = Arc::new(
                LocalFileSystem::new_with_prefix(dir).expect("test setup: open local store"),
            );
            let catalog = moraine::Catalog::open(store, moraine::CatalogOptions::default())
                .await
                .expect("test setup: open catalog");
            catalog
                .commit(|tx| {
                    let schema = tx.create_schema("sales")?;
                    let table = tx.create_table(
                        schema,
                        "orders",
                        &[
                            ColumnDef {
                                name: "id".into(),
                                column_type: "BIGINT".into(),
                                nulls_allowed: false,
                                default_value: None,
                                children: Vec::new(),
                            },
                            ColumnDef {
                                name: "amount".into(),
                                column_type: "DOUBLE".into(),
                                nulls_allowed: true,
                                default_value: None,
                                children: Vec::new(),
                            },
                        ],
                    )?;
                    let column = tx.columns_of(table)[0].id;
                    let file = tx.register_data_file(
                        table,
                        DataFile {
                            path: "orders/data-1.parquet".into(),
                            path_is_relative: true,
                            file_format: "parquet".into(),
                            record_count: 10,
                            file_size_bytes: 1024,
                            footer_size: 64,
                            encryption_key: Some("a2V5LWRhdGE=".into()),
                            partition_values: vec![],
                            column_stats: vec![FileColumnStats {
                                column_id: column,
                                column_size_bytes: 100,
                                value_count: 10,
                                null_count: 0,
                                min_value: Some("1".into()),
                                max_value: Some("10".into()),
                                contains_nan: None,
                                extra_stats: None,
                            }],
                        },
                        &[],
                    )?;
                    tx.register_delete_file(
                        table,
                        DeleteFile {
                            data_file_id: file,
                            path: "orders/delete-1.parquet".into(),
                            path_is_relative: true,
                            format: "parquet".into(),
                            delete_count: 2,
                            file_size_bytes: 128,
                            footer_size: 32,
                            encryption_key: Some("a2V5LWRlbA==".into()),
                        },
                        &[],
                    )?;
                    tx.update_column_stats(
                        table,
                        column,
                        ColumnStats {
                            contains_null: Some(false),
                            contains_nan: None,
                            min_value: Some("1".into()),
                            max_value: Some("10".into()),
                            extra_stats: None,
                        },
                    )?;
                    tx.create_view(schema, "orders_v", "duckdb", "select * from orders")?;
                    Ok(())
                })
                .await
                .expect("test setup: commit fixtures");

            catalog
                .commit(|tx| {
                    let table = tx.tables_in(tx.schemas()[1].id)[0].id;
                    tx.rename_table(table, "orders2")
                })
                .await
                .expect("test setup: rename table");

            catalog.close().await.expect("test setup: close catalog");
        });
    }

    /// Seeds a schema + table, then tags both the table and its first
    /// column over the staged-row path — the only writer for tags, as in
    /// production (DuckLake's `COMMENT ON` batch).
    pub(crate) fn seed_with_tags(dir: &Path) {
        use moraine::ffi_support::staged::{Cell, RowOperation, TableKind, staged_begin};

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test setup: build tokio runtime");

        rt.block_on(async {
            let store = Arc::new(
                LocalFileSystem::new_with_prefix(dir).expect("test setup: open local store"),
            );
            let catalog = moraine::Catalog::open(store, moraine::CatalogOptions::default())
                .await
                .expect("test setup: open catalog");
            catalog
                .commit(|tx| {
                    let schema = tx.create_schema("sales")?;
                    tx.create_table(
                        schema,
                        "orders",
                        &[ColumnDef {
                            name: "id".into(),
                            column_type: "BIGINT".into(),
                            nulls_allowed: false,
                            default_value: None,
                            children: Vec::new(),
                        }],
                    )?;
                    Ok(())
                })
                .await
                .expect("test setup: commit fixtures");

            // Schema `sales` took global id 1, table `orders` id 2; its
            // first column has per-table field id 1.
            let mut tx = staged_begin(&catalog, None, String::new())
                .await
                .expect("test setup: begin staged tx");
            tx.stage(RowOperation::Insert {
                table: TableKind::Tag,
                cells: vec![
                    Cell::U64(2),
                    Cell::U64(2),
                    Cell::Null,
                    Cell::Str("comment".into()),
                    Cell::Str("our table".into()),
                ],
            });
            tx.stage(RowOperation::Insert {
                table: TableKind::ColumnTag,
                cells: vec![
                    Cell::U64(2),
                    Cell::U64(1),
                    Cell::U64(2),
                    Cell::Null,
                    Cell::Str("comment".into()),
                    Cell::Str("our column".into()),
                ],
            });
            tx.stage(RowOperation::Insert {
                table: TableKind::Snapshot,
                cells: vec![
                    Cell::U64(2),
                    Cell::I64(1),
                    Cell::U64(1),
                    Cell::U64(3),
                    Cell::U64(0),
                ],
            });
            tx.stage(RowOperation::Insert {
                table: TableKind::SnapshotChanges,
                cells: vec![
                    Cell::U64(2),
                    Cell::Str("altered_table:2".into()),
                    Cell::Null,
                    Cell::Null,
                    Cell::Null,
                ],
            });
            tx.commit().await.expect("test setup: commit tags");

            catalog.close().await.expect("test setup: close catalog");
        });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, c_void};

    use super::{
        test_support::{TempDir, attach_ok, seed},
        *,
    };
    use crate::{
        abi::{free_c_string, moraine_detach, moraine_error_free},
        error::{MoraineError, codes},
    };

    /// One representative dump pins the pull channel for the whole family —
    /// every dump entry point routes through the same cancellable bridge.
    #[test]
    fn probe_cancels_dump_schemas_then_quiet_probe_succeeds() {
        unsafe extern "C" fn probe_always(_probe_ctx: *mut c_void) -> bool {
            true
        }
        unsafe extern "C" fn probe_never(_probe_ctx: *mut c_void) -> bool {
            false
        }

        let dir = TempDir::new("probe-dump");
        seed(dir.path());
        let handle = attach_ok(dir.path());

        let mut items: *mut MoraineSchemaRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; out/err slots are valid; the
        // probes accept a null context.
        let code = unsafe {
            moraine_dump_schemas(
                handle,
                &raw mut items,
                &raw mut len,
                Some(probe_always),
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::INTERRUPTED);
        assert_eq!(err.code, codes::INTERRUPTED);
        assert!(items.is_null());
        // SAFETY: populated by the failed call above, freed exactly once.
        unsafe { moraine_error_free(err.message) };

        let mut items2: *mut MoraineSchemaRow = ptr::null_mut();
        let mut len2: usize = 0;
        let mut err2 = MoraineError::default();
        // SAFETY: same contracts as above.
        let code2 = unsafe {
            moraine_dump_schemas(
                handle,
                &raw mut items2,
                &raw mut len2,
                Some(probe_never),
                ptr::null_mut(),
                &raw mut err2,
            )
        };
        assert_eq!(code2, codes::OK);

        // SAFETY: freed exactly once each.
        unsafe {
            moraine_dump_schemas_free(items2, len2);
            moraine_detach(handle);
        }
    }

    #[test]
    fn dump_columns_views_and_files_carry_exact_values() {
        let dir = TempDir::new("columns-views-files");
        seed(dir.path());
        let handle = attach_ok(dir.path());
        let mut err = MoraineError::default();

        let mut columns: *mut MoraineColumnRow = ptr::null_mut();
        let mut columns_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_columns(
                handle,
                &raw mut columns,
                &raw mut columns_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(columns_len, 2);
        // SAFETY: just populated above.
        let col_slice = unsafe { std::slice::from_raw_parts(columns, columns_len) };
        assert!(col_slice.iter().all(|c| !c.has_end_snapshot));
        // Pin `nulls_allowed` per column by name: `id` was created NOT
        // NULL, `amount` nullable.
        for column in col_slice {
            // SAFETY: populated by the dump above.
            let name = unsafe { CStr::from_ptr(column.column_name) }
                .to_str()
                .unwrap();
            match name {
                "id" => assert!(!column.nulls_allowed),
                "amount" => assert!(column.nulls_allowed),
                other => panic!("unexpected column {other}"),
            }
        }

        let mut views: *mut MoraineViewRow = ptr::null_mut();
        let mut views_len: usize = 0;
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
        assert_eq!(code, codes::OK);
        assert_eq!(views_len, 1);
        // SAFETY: just populated above.
        let view_sql = unsafe { CStr::from_ptr((*views).sql) }.to_str().unwrap();
        assert_eq!(view_sql, "select * from orders");
        // SAFETY: same as above.
        assert!(unsafe { (*views).column_aliases }.is_null());

        let mut files: *mut MoraineDataFileRow = ptr::null_mut();
        let mut files_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_data_files(
                handle,
                &raw mut files,
                &raw mut files_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(files_len, 1);
        // SAFETY: just populated above.
        let file_path = unsafe { CStr::from_ptr((*files).path) }.to_str().unwrap();
        assert_eq!(file_path, "orders/data-1.parquet");
        // SAFETY: same as above.
        assert_eq!(unsafe { (*files).record_count }, 10);
        // SAFETY: same as above.
        let file_key = unsafe { CStr::from_ptr((*files).encryption_key) }
            .to_str()
            .unwrap();
        assert_eq!(file_key, "a2V5LWRhdGE=");
        // SAFETY: same as above.
        assert!(!unsafe { (*files).has_partition_id });
        // SAFETY: same as above.
        let data_file_id = unsafe { (*files).data_file_id };

        let mut deletes: *mut MoraineDeleteFileRow = ptr::null_mut();
        let mut deletes_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_delete_files(
                handle,
                &raw mut deletes,
                &raw mut deletes_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(deletes_len, 1);
        // SAFETY: just populated above.
        assert_eq!(unsafe { (*deletes).data_file_id }, data_file_id);
        // SAFETY: same as above.
        assert_eq!(unsafe { (*deletes).delete_count }, 2);
        // SAFETY: same as above.
        let delete_key = unsafe { CStr::from_ptr((*deletes).encryption_key) }
            .to_str()
            .unwrap();
        assert_eq!(delete_key, "a2V5LWRlbA==");

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            moraine_dump_columns_free(columns, columns_len);
            moraine_dump_views_free(views, views_len);
            moraine_dump_data_files_free(files, files_len);
            moraine_dump_delete_files_free(deletes, deletes_len);
            moraine_detach(handle);
        }
    }

    #[test]
    fn dump_statistics_and_snapshots_carry_exact_values() {
        let dir = TempDir::new("stats-snapshots");
        seed(dir.path());
        let handle = attach_ok(dir.path());
        let mut err = MoraineError::default();

        let mut tstat_rows: *mut MoraineTableStatsRow = ptr::null_mut();
        let mut tstat_rows_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_table_stats(
                handle,
                &raw mut tstat_rows,
                &raw mut tstat_rows_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(tstat_rows_len, 1);
        // SAFETY: just populated above.
        assert_eq!(unsafe { (*tstat_rows).record_count }, 10);

        let mut col_stat_rows: *mut MoraineTableColumnStatsRow = ptr::null_mut();
        let mut col_stat_rows_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_table_column_stats(
                handle,
                &raw mut col_stat_rows,
                &raw mut col_stat_rows_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(col_stat_rows_len, 1);
        // SAFETY: just populated above.
        assert!(unsafe { (*col_stat_rows).has_contains_null });
        // SAFETY: same as above.
        assert!(!unsafe { (*col_stat_rows).contains_null });

        let mut file_stat_rows: *mut MoraineFileColumnStatsRow = ptr::null_mut();
        let mut file_stat_rows_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_file_column_stats(
                handle,
                &raw mut file_stat_rows,
                &raw mut file_stat_rows_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(file_stat_rows_len, 1);
        // SAFETY: just populated above.
        let min_value = unsafe { CStr::from_ptr((*file_stat_rows).min_value) }
            .to_str()
            .unwrap();
        assert_eq!(min_value, "1");

        let mut snapshots: *mut MoraineSnapshotRow = ptr::null_mut();
        let mut snapshots_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_snapshots(
                handle,
                &raw mut snapshots,
                &raw mut snapshots_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        // Bootstrap (0) + the two `seed` commits.
        assert_eq!(snapshots_len, 3);
        // SAFETY: just populated above with `snapshots_len` live elements.
        let snap_slice = unsafe { std::slice::from_raw_parts(snapshots, snapshots_len) };
        let ids: Vec<u64> = snap_slice.iter().map(|s| s.snapshot_id).collect();
        assert_eq!(ids, vec![0, 1, 2]);
        // SAFETY: bootstrap's `changes_made` is a non-null string.
        let bootstrap_changes = unsafe { CStr::from_ptr(snap_slice[0].changes_made) }
            .to_str()
            .unwrap();
        assert_eq!(bootstrap_changes, "created_schema:\"main\"");
        assert!(snap_slice[0].author.is_null());

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            moraine_dump_table_stats_free(tstat_rows, tstat_rows_len);
            moraine_dump_table_column_stats_free(col_stat_rows, col_stat_rows_len);
            moraine_dump_file_column_stats_free(file_stat_rows, file_stat_rows_len);
            moraine_dump_snapshots_free(snapshots, snapshots_len);
            moraine_detach(handle);
        }
    }

    #[test]
    fn dump_on_null_handle_reports_invalid_argument() {
        let mut err = MoraineError::default();
        let mut out: *mut MoraineSchemaRow = ptr::null_mut();
        let mut len: usize = 0;
        // SAFETY: a null `handle` is exactly the input this test exercises.
        let code = unsafe {
            moraine_dump_schemas(
                ptr::null_mut(),
                &raw mut out,
                &raw mut len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::INVALID_ARGUMENT);
        assert!(out.is_null());
        // SAFETY: `err.message` was just populated above and not yet freed.
        unsafe { free_c_string(err.message) };
    }

    /// Every `_free` routes through [`free_rows`], whose null-items input
    /// is a no-op — pinned here through one representative symbol.
    #[test]
    fn dump_frees_tolerate_null() {
        // SAFETY: a null pointer with any length is documented as a no-op.
        unsafe { moraine_dump_schemas_free(ptr::null_mut(), 0) };
    }
}
