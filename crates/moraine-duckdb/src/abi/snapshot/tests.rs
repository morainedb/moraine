use std::{ffi::CStr, ptr, sync::Arc};

use super::*;

#[test]
fn schema_export_admits_only_its_result_and_releases_the_reservation() {
    const CHILD: &str = "MORAINE_TEST_SCHEMA_EXPORT_MEMORY";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "abi::snapshot::tests::schema_export_admits_only_its_result_and_releases_the_reservation",
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

    let snapshot = large_snapshot();
    let resident = moraine::working_memory_usage().unwrap().0;
    assert!(resident > 0, "the resident snapshot remains charged");
    let capacity = moraine::working_memory_usage().unwrap().1;
    let pressure = moraine::MemoryReservation::reserve(capacity - resident - 16 * 1024).unwrap();
    assert!(snapshot.reserve_copy().is_err());
    let before = moraine::working_memory_usage().unwrap().0;
    let mut handle = MoraineSnapshotHandle::new(snapshot);
    let mut schemas = ptr::null_mut();
    let mut length = 0;
    let mut error = MoraineError::default();
    // SAFETY: the handle and output slots remain live until the result is freed.
    let code = unsafe {
        moraine_snapshot_schemas(
            &raw mut handle,
            &raw mut schemas,
            &raw mut length,
            &raw mut error,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(length, 1);
    // SAFETY: the successful export returned one live descriptor.
    assert_eq!(unsafe { CStr::from_ptr((*schemas).name) }, c"main");
    let held = moraine::working_memory_usage().unwrap().0;
    assert!(held > before, "the exported result retains its reservation");
    assert!(held - before <= 16 * 1024);
    // SAFETY: the array comes from the matching export and is freed once.
    unsafe { moraine_snapshot_schemas_free(schemas, length) };
    assert_eq!(moraine::working_memory_usage().unwrap().0, before);
    let exhausted = moraine::MemoryReservation::reserve(16 * 1024).unwrap();
    schemas = ptr::null_mut();
    length = usize::MAX;
    // SAFETY: live handle and writable output slots; admission must refuse.
    let code = unsafe {
        moraine_snapshot_schemas(
            &raw mut handle,
            &raw mut schemas,
            &raw mut length,
            &raw mut error,
        )
    };
    assert_eq!(code, codes::CONSTRAINT);
    assert!(schemas.is_null());
    assert_eq!(length, usize::MAX);
    // SAFETY: the refused export produced this error message, freed once.
    unsafe { super::super::moraine_error_free(error.message) };
    error = MoraineError::default();
    assert_eq!(moraine::working_memory_usage().unwrap().0, capacity);

    let mut views = ptr::null_mut();
    // SAFETY: live handle and writable slots; the absent schema selects no rows.
    let code = unsafe {
        moraine_snapshot_views_in(
            &raw mut handle,
            u64::MAX,
            &raw mut views,
            &raw mut length,
            &raw mut error,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(length, 0);
    // SAFETY: the empty array is released with its matching free function.
    unsafe { moraine_snapshot_views_in_free(views, length) };
    assert_eq!(moraine::working_memory_usage().unwrap().0, capacity);
    drop(exhausted);
    drop(pressure);
    assert_eq!(moraine::working_memory_usage().unwrap().0, resident);
    drop(handle);
    assert!(moraine::working_memory_usage().unwrap().0 < resident);
}

fn large_snapshot() -> Arc<moraine::CatalogSnapshot> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let catalog = moraine::Catalog::open(
            Arc::new(object_store::memory::InMemory::new()),
            moraine::CatalogOptions::default(),
        )
        .await
        .unwrap();
        let sql = format!("SELECT 1 /*{}*/", "x".repeat(8192));
        catalog
            .commit(|transaction| {
                let schema = transaction.schema_by_name("main").unwrap().id;
                for index in 0..128 {
                    transaction.create_view(schema, &format!("view_{index}"), "duckdb", &sql)?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let snapshot = catalog.snapshot().await.unwrap();
        catalog.close().await.unwrap();
        snapshot
    })
}
