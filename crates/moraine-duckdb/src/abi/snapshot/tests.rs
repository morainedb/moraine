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

    let mut catalog = large_catalog();
    let mut snapshot = ptr::null_mut();
    let mut open_error = MoraineError::default();
    assert_eq!(
        // SAFETY: live catalog and writable output storage.
        unsafe {
            crate::abi::moraine_snapshot_scoped(
                &raw mut catalog,
                &raw mut snapshot,
                None,
                ptr::null_mut(),
                &raw mut open_error,
            )
        },
        codes::OK
    );
    // SAFETY: opening returned an owned handle, consumed once.
    let mut handle = unsafe { Box::from_raw(snapshot) };
    let resident = moraine::working_memory_usage().unwrap().0;
    let capacity = moraine::working_memory_usage().unwrap().1;
    let pressure = moraine::MemoryReservation::reserve(capacity - resident - 256 * 1024).unwrap();
    let before = moraine::working_memory_usage().unwrap().0;
    let mut schemas = ptr::null_mut();
    let mut length = 0;
    let mut error = MoraineError::default();
    // SAFETY: the handle and output slots remain live until the result is freed.
    let code = unsafe {
        moraine_snapshot_schemas(
            &raw mut *handle,
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
    assert!(held - before <= 256 * 1024);
    // SAFETY: the array comes from the matching export and is freed once.
    unsafe { moraine_snapshot_schemas_free(schemas, length) };
    assert_eq!(moraine::working_memory_usage().unwrap().0, before);
    let exhausted = moraine::MemoryReservation::reserve(256 * 1024).unwrap();
    schemas = ptr::null_mut();
    length = usize::MAX;
    // SAFETY: live handle and writable output slots; admission must refuse.
    let code = unsafe {
        moraine_snapshot_schemas(
            &raw mut *handle,
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
    assert_eq!(moraine::working_memory_usage().unwrap().0, capacity);

    drop(exhausted);
    drop(pressure);
    assert_eq!(moraine::working_memory_usage().unwrap().0, resident);
    drop(handle);
    drop(catalog);
}

fn large_catalog() -> crate::runtime::MoraineCatalogHandle {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let catalog = runtime.block_on(async {
        let catalog = moraine::Catalog::open(
            Arc::new(object_store::memory::InMemory::new()),
            moraine::CatalogOptions::default(),
        )
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
                for index in 0..128 {
                    transaction
                        .create_view(schema, &format!("view_{index}"), "duckdb", &sql)
                        .await?;
                }
                Ok(())
            })
            .await
            .unwrap();
        catalog
    });
    crate::runtime::MoraineCatalogHandle::new(
        runtime,
        crate::runtime::AttachedCatalog::Writer(catalog),
        crate::logging::allocate_handle_id(),
    )
}
