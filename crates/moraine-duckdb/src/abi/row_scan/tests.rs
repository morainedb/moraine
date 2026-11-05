use std::sync::Arc;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    ffi::from_ffi,
};

use super::*;

#[test]
fn scoped_scan_opens_only_requested_metadata_at_its_pinned_revision() {
    use std::ptr;

    use crate::abi::{
        moraine_snapshot_free, moraine_snapshot_read_handle, moraine_snapshot_scoped,
    };

    const CHILD: &str = "MORAINE_TEST_SCOPED_ROW_SCAN";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "abi::row_scan::tests::scoped_scan_opens_only_requested_metadata_at_its_pinned_revision",
            ])
            .env(CHILD, "1")
            .env("MORAINE_WORKING_MEMORY_BYTES", "67108864")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let (mut handle, table, file) = row_scan_fixture();
    let mut snapshot = ptr::null_mut();
    let mut error = MoraineError::default();
    assert_eq!(
        // SAFETY: the handle and output slots remain live through both scans.
        unsafe {
            moraine_snapshot_scoped(
                &raw mut handle,
                &raw mut snapshot,
                None,
                ptr::null_mut(),
                &raw mut error,
            )
        },
        codes::OK
    );
    handle
        .block_on(
            handle
                .catalog
                .writer()
                .unwrap()
                .commit(async |transaction| transaction.rename_table(table, "renamed").await),
        )
        .unwrap();

    // SAFETY: the snapshot remains live through both scans.
    let pinned = unsafe { moraine_snapshot_read_handle(snapshot) };
    assert!(!pinned.is_null());

    let (used, capacity) = moraine::working_memory_usage().unwrap();
    let pressure = moraine::MemoryReservation::reserve(capacity - used - 256 * 1024).unwrap();
    let pairs: Vec<_> = (0..17)
        .map(|value| MoraineRowId {
            value,
            data_file_id: file.get(),
            has_data_file_id: true,
        })
        .collect();
    let columns = [c"a".as_ptr()];
    for estimating in [true, false] {
        let mut scan = ptr::null_mut();
        // SAFETY: all inputs and output slots remain live for the call.
        let code = unsafe {
            moraine_row_scan_open(
                pinned,
                snapshot,
                c"main".as_ptr(),
                c"t".as_ptr(),
                pairs.as_ptr(),
                pairs.len(),
                columns.as_ptr(),
                columns.len(),
                ptr::null(),
                0,
                &raw mut scan,
                None,
                ptr::null_mut(),
                &raw mut error,
            )
        };
        // SAFETY: an error message, when present, is live until freed.
        let message = unsafe {
            error
                .message
                .as_ref()
                .map(|message| std::ffi::CStr::from_ptr(message))
        };
        assert_eq!(code, codes::OK, "scan open: {message:?}");
        if estimating {
            // SAFETY: the open succeeded and the cursor is exclusively owned.
            estimate_scan(unsafe { &mut *scan });
        } else {
            // SAFETY: the open succeeded and this test owns the cursor exclusively.
            let values = scan_values(unsafe { &mut *scan });
            assert_eq!(values, (0..17).collect::<Vec<i64>>());
        }
        // SAFETY: each scan is freed once, after its last use.
        unsafe { moraine_row_scan_free(scan) };
    }
    drop(pressure);
    // SAFETY: the snapshot is freed once, after both cursors finish.
    unsafe { moraine_snapshot_free(snapshot) };
    handle.block_on(handle.catalog.reads().close()).unwrap();
}

fn row_scan_fixture() -> (MoraineCatalogHandle, moraine::TableId, moraine::DataFileId) {
    use moraine::{Catalog, CatalogOptions, ColumnDef, DataFile, DataStore};
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use parquet::arrow::ArrowWriter;

    use crate::{
        logging::allocate_handle_id,
        runtime::{AttachedCatalog, new_runtime},
    };

    let log_id = allocate_handle_id();
    let runtime = new_runtime(log_id, 2).unwrap();
    let data = Arc::new(InMemory::new());
    let batch =
        RecordBatch::try_from_iter([("a", Arc::new(Int64Array::from_iter_values(0..64)) as _)])
            .unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let footer = u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
    let size = bytes.len() as u64;
    let (catalog, table, file) = runtime.block_on(async {
        data.put(&Path::from("data.parquet"), bytes.into())
            .await
            .unwrap();
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        let sql = format!("SELECT 1 /*{}*/", "x".repeat(8192));
        catalog
            .commit(async |transaction| {
                let schema = transaction
                    .schema_by_name("main")
                    .await
                    .unwrap()
                    .unwrap()
                    .id;
                let table = transaction
                    .create_table(
                        schema,
                        "t",
                        &[ColumnDef {
                            name: "a".into(),
                            column_type: "BIGINT".into(),
                            ..Default::default()
                        }],
                    )
                    .await?;
                transaction
                    .register_data_file(
                        table,
                        DataFile {
                            path: "data.parquet".into(),
                            path_is_relative: false,
                            file_format: "parquet".into(),
                            record_count: 64,
                            file_size_bytes: size,
                            footer_size: u64::from(footer),
                            encryption_key: None,
                            partition_values: vec![],
                            column_stats: vec![],
                        },
                        &[],
                    )
                    .await?;
                for index in 0..128 {
                    transaction
                        .create_view(schema, &format!("unrelated_{index}"), "duckdb", &sql)
                        .await?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let view = catalog.read_scope().await.unwrap();
        let schema = view.schema_by_name("main").await.unwrap().unwrap().id;
        let table = view.table_by_name(schema, "t").await.unwrap().unwrap().id;
        let file = view.data_files_of(table).await.unwrap()[0].id;
        (catalog, table, file)
    });
    let mut handle = MoraineCatalogHandle::new(runtime, AttachedCatalog::Writer(catalog), log_id);
    handle.data_store = Some(DataStore::new(data));
    (handle, table, file)
}

fn estimate_scan(scan: &mut MoraineRowScan) {
    let mut selective = false;
    let mut error = MoraineError::default();
    assert_eq!(
        // SAFETY: the scan is live and the output slots are writable.
        unsafe {
            moraine_row_scan_is_selective(
                scan,
                &raw mut selective,
                None,
                std::ptr::null_mut(),
                &raw mut error,
            )
        },
        codes::OK
    );
}

fn scan_values(scan: &mut MoraineRowScan) -> Vec<i64> {
    use std::ptr;
    let mut error = MoraineError::default();
    let mut values = Vec::new();
    loop {
        let mut schema = FFI_ArrowSchema::empty();
        let mut array = FFI_ArrowArray::empty();
        let mut has_batch = false;
        assert_eq!(
            // SAFETY: the scan is live and Arrow output slots are empty.
            unsafe {
                moraine_row_scan_next_arrow(
                    scan,
                    &raw mut schema,
                    &raw mut array,
                    &raw mut has_batch,
                    None,
                    ptr::null_mut(),
                    &raw mut error,
                )
            },
            codes::OK
        );
        if !has_batch {
            break;
        }
        // SAFETY: the exported array and schema belong together.
        let batch = RecordBatch::from(StructArray::from(
            unsafe { from_ffi(array, &schema) }.unwrap(),
        ));
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        values.extend(column.values().iter().copied());
    }
    values
}

/// The row-scan ABI is one Arrow cursor, with file positioning for DELETE.
#[test]
fn the_row_scan_abi_is_one_arrow_cursor() {
    let header = include_str!("../../../cpp/moraine_abi.h");
    for removed in [
        "MoraineRowBatch",
        "MorainePositionPair",
        "moraine_locate_row_positions_free_inlined(",
        "moraine_rows_at(",
        "moraine_rows_at_free(",
        "moraine_row_scan_next(",
        "moraine_rows_at_open(",
        "moraine_commit_located_deletion(",
    ] {
        assert!(!header.contains(removed), "withdrawn export: {removed}");
    }
    for retained in [
        "moraine_row_scan_open(",
        "moraine_row_scan_next_arrow(",
        "moraine_row_scan_free(",
        "moraine_locate_row_positions(",
        "MoraineLocatedFile",
    ] {
        assert!(
            header.contains(retained),
            "missing Arrow cursor export: {retained}"
        );
    }
}

#[test]
fn exported_batch_retains_original_buffers() {
    let values = Arc::new(Int64Array::from(vec![Some(7), None, Some(9)]));
    let pointer = values.values().as_ptr();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
        vec![values],
    )
    .unwrap();
    let (array, schema) = export_batch(batch).unwrap();
    // SAFETY: array and schema were exported together and ownership is transferred
    // once.
    let data = unsafe { from_ffi(array, &schema) }.unwrap();
    let batch = RecordBatch::from(StructArray::from(data));
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ptr(), pointer);
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 9);
}
