use std::{
    ffi::{CStr, CString, c_char, c_void},
    path::Path,
    ptr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use moraine::{ColumnDef, DataFile};
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem};

use super::*;
use crate::{
    staged::moraine_tx_commit,
    test_support::{TempDir, attach_ok, begin},
};

#[test]
fn s3_cache_identities_include_endpoint_and_bucket_but_not_credentials() {
    let builder = AmazonS3Builder::new()
        .with_bucket_name("shared-bucket")
        .with_region("us-east-1")
        .with_access_key_id("test")
        .with_secret_access_key("test");
    let first = builder.clone().with_endpoint("https://one.example");
    let second = builder.clone().with_endpoint("https://two.example");
    assert_eq!(
        first.clone().build().unwrap().to_string(),
        second.clone().build().unwrap().to_string()
    );
    assert_ne!(s3_cache_identity(&first), s3_cache_identity(&second));
    assert_ne!(
        s3_cache_identity(&first),
        s3_cache_identity(&first.clone().with_bucket_name("other"))
    );
    assert_eq!(
        s3_cache_identity(&first),
        s3_cache_identity(
            &first
                .clone()
                .with_access_key_id("rotated")
                .with_secret_access_key("rotated")
        )
    );
    assert_ne!(
        s3_cache_identity(&builder),
        s3_cache_identity(&builder.clone().with_config(
            object_store::aws::AmazonS3ConfigKey::S3Endpoint,
            "https://override.example"
        ))
    );
}

#[test]
fn local_attaches_share_cache_identity_and_memory_attaches_do_not() {
    let root = TempDir::new("cache-identity");
    let path = root.path().to_str().unwrap();
    let (_, first) = StoreKind::LocalFile.open(path, None).unwrap();
    let (_, second) = StoreKind::LocalFile.open(path, None).unwrap();
    assert_eq!(first, second);
    let (_, first) = StoreKind::Memory.open("memory://", None).unwrap();
    let (_, second) = StoreKind::Memory.open("memory://", None).unwrap();
    assert_ne!(first, second);
}

/// Every state maps to its own wire value, and only `Ready` clears
/// `is_building` — the distinction a caller gating on `is_building`
/// alone cannot make.
#[test]
fn every_index_state_maps_to_its_own_wire_value() {
    for (state, expected) in [
        (moraine::IndexState::Ready, MoraineIndexState::Ready),
        (moraine::IndexState::Building, MoraineIndexState::Building),
        (
            moraine::IndexState::Maintaining,
            MoraineIndexState::Maintaining,
        ),
        (moraine::IndexState::Poisoned, MoraineIndexState::Poisoned),
    ] {
        let mapped = MoraineIndexState::from(state);
        assert_eq!(mapped as u8, expected as u8, "{state:?} maps to itself");
        assert_eq!(
            state == moraine::IndexState::Ready,
            mapped as u8 == MoraineIndexState::Ready as u8,
            "{state:?} agrees with the ready test `is_building` inverts"
        );
    }
}

/// Seeds a catalog directly through the `moraine` API with one
/// schema, one table with two columns and one data file, and one
/// view.
fn seed(dir: &Path) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test setup: build tokio runtime");

    rt.block_on(async {
        let store =
            Arc::new(LocalFileSystem::new_with_prefix(dir).expect("test setup: open local store"));
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
                tx.register_data_file(
                    table,
                    DataFile {
                        path: "orders/data-1.parquet".into(),
                        path_is_relative: true,
                        file_format: "parquet".into(),
                        record_count: 10,
                        file_size_bytes: 1024,
                        footer_size: 64,
                        encryption_key: None,
                        partition_values: vec![],
                        column_stats: vec![],
                    },
                    &[],
                )?;
                tx.create_view(schema, "orders_v", "duckdb", "select * from orders")?;
                Ok(())
            })
            .await
            .expect("test setup: commit fixtures");

        catalog.close().await.expect("test setup: close catalog");
    });
}

/// Seeds a catalog with a two-column table (`a BIGINT`, `b VARCHAR`), a
/// three-row data file, and a composite unique index over `(a, b)` with
/// one entry per row: `(5, "x")`, `(5, "y")`, `(7, "x")`.
fn seed_composite(dir: &Path) {
    use moraine::{ColumnId, IndexDef, IndexEntry, IndexKeyValue, IntWidth};

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test setup: build tokio runtime");
    rt.block_on(async {
        let store =
            Arc::new(LocalFileSystem::new_with_prefix(dir).expect("test setup: open local store"));
        let catalog = moraine::Catalog::open(store, moraine::CatalogOptions::default())
            .await
            .expect("test setup: open catalog");
        catalog
            .commit(|tx| {
                let schema = tx.create_schema("sales")?;
                let table = tx.create_table(
                    schema,
                    "t",
                    &[
                        ColumnDef {
                            name: "a".into(),
                            column_type: "BIGINT".into(),
                            nulls_allowed: false,
                            default_value: None,
                            children: Vec::new(),
                        },
                        ColumnDef {
                            name: "b".into(),
                            column_type: "VARCHAR".into(),
                            nulls_allowed: false,
                            default_value: None,
                            children: Vec::new(),
                        },
                    ],
                )?;
                tx.register_data_file(
                    table,
                    DataFile {
                        path: "t/data-1.parquet".into(),
                        path_is_relative: true,
                        file_format: "parquet".into(),
                        record_count: 3,
                        file_size_bytes: 1024,
                        footer_size: 64,
                        encryption_key: None,
                        partition_values: vec![],
                        column_stats: vec![],
                    },
                    &[],
                )?;
                let a = |v: i128| {
                    Some(IndexKeyValue::Int {
                        value: v,
                        width: IntWidth::I64,
                    })
                };
                let b = |s: &str| Some(IndexKeyValue::Str(s.to_owned()));
                tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_ab".into(),
                        columns: vec![ColumnId::new(1), ColumnId::new(2)],
                        unique: true,
                    },
                    &[
                        IndexEntry {
                            row_id: 0,
                            values: vec![a(5), b("x")],
                        },
                        IndexEntry {
                            row_id: 1,
                            values: vec![a(5), b("y")],
                        },
                        IndexEntry {
                            row_id: 2,
                            values: vec![a(7), b("x")],
                        },
                    ],
                )?;
                Ok(())
            })
            .await
            .expect("test setup: commit composite fixtures");
        catalog.close().await.expect("test setup: close catalog");
    });
}

/// Builds an integer lookup value.
fn i64_lookup(v: i64) -> MoraineLookupValue {
    MoraineLookupValue {
        kind: 1,
        i64_value: v,
        u64_value: 0,
        f64_value: 0.0,
        bool_value: false,
        str_value: ptr::null(),
        bytes_value: ptr::null(),
        bytes_len: 0,
    }
}

/// Builds a string lookup value borrowing `text` for the call.
fn str_lookup(text: &CStr) -> MoraineLookupValue {
    MoraineLookupValue {
        kind: 5,
        i64_value: 0,
        u64_value: 0,
        f64_value: 0.0,
        bool_value: false,
        str_value: text.as_ptr(),
        bytes_value: ptr::null(),
        bytes_len: 0,
    }
}

/// Drives `moraine_index_lookup`, returning the resolved row ids on success
/// or the error message on failure.
fn composite_lookup(
    handle: *mut MoraineCatalogHandle,
    schema: &str,
    table: &str,
    index: &str,
    values: &[MoraineLookupValue],
) -> Result<Vec<u64>, String> {
    let c_schema = CString::new(schema).expect("no NUL");
    let c_table = CString::new(table).expect("no NUL");
    let c_index = CString::new(index).expect("no NUL");
    let mut items: *mut MoraineRowId = ptr::null_mut();
    let mut len: usize = 0;
    let mut err = MoraineError::default();
    // SAFETY: `handle` is attached; the C strings and `values` slice are
    // valid for the call; the out-slots are writable locals.
    let code = unsafe {
        moraine_index_lookup(
            handle,
            c_schema.as_ptr(),
            c_table.as_ptr(),
            c_index.as_ptr(),
            values.as_ptr(),
            values.len(),
            &raw mut items,
            &raw mut len,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    if code != codes::OK {
        // SAFETY: a failed call wrote a non-null message.
        let message = unsafe { CStr::from_ptr(err.message) }
            .to_str()
            .expect("utf-8")
            .to_owned();
        // SAFETY: the message was minted by the failed call, freed once.
        unsafe { moraine_error_free(err.message) };
        return Err(message);
    }
    // SAFETY: on success `items`/`len` describe a valid slice.
    let rows = unsafe { std::slice::from_raw_parts(items, len) }
        .iter()
        .map(|row_id| row_id.value)
        .collect();
    // SAFETY: `items`/`len` are exactly what the call above wrote.
    unsafe { moraine_index_lookup_free(items, len) };
    Ok(rows)
}

/// A composite index resolves a full multi-column equality key: the two
/// values, in the index's column order, pin the one matching row, and a
/// value count that does not match the index's column count is refused.
#[test]
fn index_lookup_resolves_a_composite_key() {
    let dir = TempDir::new("composite-lookup");
    seed_composite(dir.path());
    let handle = attach_ok(dir.path());

    let y = CString::new("y").expect("no NUL");
    let hit = composite_lookup(
        handle,
        "sales",
        "t",
        "by_ab",
        &[i64_lookup(5), str_lookup(&y)],
    )
    .expect("the composite key resolves");
    assert_eq!(hit, vec![1], "(5, \"y\") pins exactly row 1");

    let x = CString::new("x").expect("no NUL");
    let miss = composite_lookup(
        handle,
        "sales",
        "t",
        "by_ab",
        &[i64_lookup(9), str_lookup(&x)],
    )
    .expect("an absent composite key resolves to no rows");
    assert!(miss.is_empty(), "(9, \"x\") matches no row");

    let short = composite_lookup(handle, "sales", "t", "by_ab", &[i64_lookup(5)])
        .expect_err("one value cannot address a two-column index");
    assert!(
        short.contains("2-column"),
        "the arity error names the index width, got: {short}"
    );

    // SAFETY: `handle` was minted by `attach_ok`, detached once.
    unsafe { moraine_detach(handle) };
}

/// A batched `IN` lookup accepts full composite keys, deduplicates them,
/// and returns the union of rows for present keys.
#[test]
fn index_in_resolves_distinct_composite_keys() {
    let dir = TempDir::new("composite-in");
    seed_composite(dir.path());
    let handle = attach_ok(dir.path());

    let x = CString::new("x").expect("no NUL");
    let first = [i64_lookup(5), str_lookup(&x)];
    let second = [i64_lookup(7), str_lookup(&x)];
    let absent = [i64_lookup(9), str_lookup(&x)];
    let keys = [
        MoraineLookupKey {
            values: first.as_ptr(),
            values_len: first.len(),
        },
        MoraineLookupKey {
            values: second.as_ptr(),
            values_len: second.len(),
        },
        MoraineLookupKey {
            values: first.as_ptr(),
            values_len: first.len(),
        },
        MoraineLookupKey {
            values: absent.as_ptr(),
            values_len: absent.len(),
        },
    ];
    let schema = CString::new("sales").expect("no NUL");
    let table = CString::new("t").expect("no NUL");
    let index = CString::new("by_ab").expect("no NUL");
    let mut items: *mut MoraineRowId = ptr::null_mut();
    let mut len = 0;
    let mut err = MoraineError::default();

    // SAFETY: the handle, strings, nested key slices, and output slots
    // remain valid for the call.
    let code = unsafe {
        moraine_index_in(
            handle,
            schema.as_ptr(),
            table.as_ptr(),
            index.as_ptr(),
            keys.as_ptr(),
            keys.len(),
            &raw mut items,
            &raw mut len,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK);
    // SAFETY: the successful call wrote `items`/`len` as one valid array.
    let rows = unsafe { std::slice::from_raw_parts(items, len) }
        .iter()
        .map(|row_id| row_id.value)
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![0, 2]);
    // SAFETY: the array is freed exactly once by its matching function.
    unsafe { moraine_index_in_free(items, len) };

    // SAFETY: `handle` was minted by `attach_ok`, detached once.
    unsafe { moraine_detach(handle) };
}

/// Drives `moraine_index_range`, returning the resolved row ids (sorted)
/// on success or the error message on failure. An empty bound slice is an
/// open side.
#[allow(clippy::too_many_arguments)]
fn composite_range(
    handle: *mut MoraineCatalogHandle,
    schema: &str,
    table: &str,
    index: &str,
    lower: &[MoraineLookupValue],
    lower_inclusive: bool,
    upper: &[MoraineLookupValue],
    upper_inclusive: bool,
) -> Result<Vec<u64>, String> {
    let c_schema = CString::new(schema).expect("no NUL");
    let c_table = CString::new(table).expect("no NUL");
    let c_index = CString::new(index).expect("no NUL");
    let mut items: *mut MoraineRowId = ptr::null_mut();
    let mut len: usize = 0;
    let mut err = MoraineError::default();
    // SAFETY: `handle` is attached; the C strings and bound slices are
    // valid for the call; the out-slots are writable locals.
    let code = unsafe {
        moraine_index_range(
            handle,
            c_schema.as_ptr(),
            c_table.as_ptr(),
            c_index.as_ptr(),
            lower.as_ptr(),
            lower.len(),
            lower_inclusive,
            upper.as_ptr(),
            upper.len(),
            upper_inclusive,
            false,
            &raw mut items,
            &raw mut len,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    if code != codes::OK {
        // SAFETY: a failed call wrote a non-null message.
        let message = unsafe { CStr::from_ptr(err.message) }
            .to_str()
            .expect("utf-8")
            .to_owned();
        // SAFETY: the message was minted by the failed call, freed once.
        unsafe { moraine_error_free(err.message) };
        return Err(message);
    }
    // SAFETY: on success `items`/`len` describe a valid slice.
    let mut rows: Vec<u64> = unsafe { std::slice::from_raw_parts(items, len) }
        .iter()
        .map(|row_id| row_id.value)
        .collect();
    // SAFETY: `items`/`len` are exactly what the call above wrote.
    unsafe { moraine_index_range_free(items, len) };
    rows.sort_unstable();
    Ok(rows)
}

/// A composite index answers a range whose bounds run over its leading
/// columns: a leading-column equality window, a full-tuple window, and a
/// half-open window with one open side.
#[test]
fn index_range_spans_a_composite_window() {
    let dir = TempDir::new("composite-range");
    seed_composite(dir.path());
    let handle = attach_ok(dir.path());

    // a = 5 (a one-column prefix bound on the two-column index): the two
    // rows sharing that leading value, whatever their second column.
    let equal_a = composite_range(
        handle,
        "sales",
        "t",
        "by_ab",
        &[i64_lookup(5)],
        true,
        &[i64_lookup(5)],
        true,
    )
    .expect("a leading-column equality window resolves");
    assert_eq!(equal_a, vec![0, 1], "a = 5 spans rows 0 and 1");

    // (5, "y") ..= (7, "x") over the full tuple: excludes (5, "x") below
    // the lower bound, includes (5, "y") and (7, "x").
    let y = CString::new("y").expect("no NUL");
    let x = CString::new("x").expect("no NUL");
    let window = composite_range(
        handle,
        "sales",
        "t",
        "by_ab",
        &[i64_lookup(5), str_lookup(&y)],
        true,
        &[i64_lookup(7), str_lookup(&x)],
        true,
    )
    .expect("a full-tuple window resolves");
    assert_eq!(
        window,
        vec![1, 2],
        "(5, \"y\")..=(7, \"x\") spans rows 1 and 2"
    );

    // a >= 7 with an open upper side: only the (7, _) row.
    let high = composite_range(
        handle,
        "sales",
        "t",
        "by_ab",
        &[i64_lookup(7)],
        true,
        &[],
        true,
    )
    .expect("a half-open window resolves");
    assert_eq!(high, vec![2], "a >= 7 spans only row 2");

    // A bound wider than the index is refused, naming the index width.
    let z = CString::new("z").expect("no NUL");
    let too_wide = composite_range(
        handle,
        "sales",
        "t",
        "by_ab",
        &[i64_lookup(5), str_lookup(&z), i64_lookup(1)],
        true,
        &[],
        true,
    )
    .expect_err("a three-value bound cannot fit a two-column index");
    assert!(
        too_wide.contains("2-column"),
        "the width error names the index, got: {too_wide}"
    );

    // SAFETY: `handle` was minted by `attach_ok`, detached once.
    unsafe { moraine_detach(handle) };
}

/// Reads the stored `encrypted` flag over the ABI.
fn catalog_encrypted(handle: *mut MoraineCatalogHandle) -> bool {
    let mut encrypted = false;
    let mut err = MoraineError::default();
    // SAFETY: `handle` is attached; outputs are valid local slots; a
    // null probe disables polling.
    let code = unsafe {
        moraine_catalog_encrypted(
            handle,
            &raw mut encrypted,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    // SAFETY: `err.message` is null or just written; `as_ref` allows null.
    assert_eq!(code, codes::OK, "getter failed: {:?}", unsafe {
        err.message.as_ref()
    });
    encrypted
}

/// Bootstraps a fresh store at `dir` recording `data_path`, the way an
/// attach with `META_DATA_PATH` does.
fn seed_with_data_path(dir: &Path, data_path: &str) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test setup: build tokio runtime");
    rt.block_on(async {
        let store =
            Arc::new(LocalFileSystem::new_with_prefix(dir).expect("test setup: open local store"));
        let mut options = moraine::CatalogOptions::default();
        options.data_path = Some(data_path.to_owned());
        let catalog = moraine::Catalog::open(store, options)
            .await
            .expect("test setup: open catalog");
        catalog.close().await.expect("test setup: close catalog");
    });
}

/// A lake's data path is fixed at creation: re-attaching with a
/// conflicting `META_DATA_PATH` is refused, while the recorded value
/// (trailing separator and all) attaches cleanly.
#[test]
fn attach_refuses_a_conflicting_data_path() {
    let dir = TempDir::new("data-path-fixed");
    let data = TempDir::new("data-path-fixed-root");
    let recorded = data.path().to_str().expect("utf-8").to_owned();
    seed_with_data_path(dir.path(), &recorded);
    let c_path = dir.c_path();

    // A different data path is refused with a clear message.
    let c_bad = CString::new("/lake/other").expect("no NUL");
    let mut bad_handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut bad_err = MoraineError::default();
    // SAFETY: all pointers are valid C strings / local slots.
    let bad_code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            c_bad.as_ptr(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut bad_handle,
            &raw mut bad_err,
        )
    };
    assert_ne!(
        bad_code,
        codes::OK,
        "a conflicting data path must be refused"
    );
    // SAFETY: on failure `guard` wrote a non-null message.
    let message = unsafe { CStr::from_ptr(bad_err.message) }
        .to_str()
        .unwrap()
        .to_owned();
    assert!(message.contains("does not match"), "got: {message}");
    // SAFETY: `bad_err.message` was minted by the failed call, freed once.
    unsafe { moraine_error_free(bad_err.message) };

    // The recorded path, with a trailing separator, still attaches.
    let c_good = CString::new(format!("{recorded}/")).expect("no NUL");
    let mut good_handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut good_err = MoraineError::default();
    // SAFETY: all pointers are valid C strings / local slots.
    let good_code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            c_good.as_ptr(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut good_handle,
            &raw mut good_err,
        )
    };
    // SAFETY: `good_err.message` is null or just written; `as_ref` allows null.
    let good_message = unsafe { good_err.message.as_ref() };
    assert_eq!(
        good_code,
        codes::OK,
        "matching path failed: {good_message:?}"
    );
    // SAFETY: freed exactly once.
    unsafe { moraine_detach(good_handle) };
}

/// The maintenance ABI runs a pass and writes its counts through the
/// out-parameters, tolerating null slots for either.
#[test]
fn maintain_reports_through_the_out_parameters() {
    let dir = TempDir::new("maintain-abi");
    seed(dir.path());
    let c_path = dir.c_path();

    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: all pointers are valid C strings / local slots.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            ptr::null(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK, "attach failed");

    // A seeded store has no dropped indexes, so a pass reclaims
    // nothing and says so rather than failing.
    let mut indexes = u64::MAX;
    let mut entries = u64::MAX;
    let mut file_stats = u64::MAX;
    // SAFETY: `handle` is live; every slot is a writable local.
    let code = unsafe {
        moraine_maintain(
            handle,
            0,
            &raw mut indexes,
            &raw mut entries,
            &raw mut file_stats,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK, "maintain failed");
    assert_eq!(indexes, 0);
    assert_eq!(entries, 0);
    assert_eq!(file_stats, 0);

    // Null out-parameters are accepted: a caller that wants only the
    // status code passes neither slot.
    // SAFETY: `handle` is live; null slots are explicitly allowed.
    let code = unsafe {
        moraine_maintain(
            handle,
            64,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK, "maintain with null out-params failed");

    // SAFETY: freed exactly once.
    unsafe { moraine_detach(handle) };
}

/// Completed passes cross the ABI as borrowed inputs and owned flattened
/// rows, preserving their timestamp and strings.
#[test]
fn maintenance_status_roundtrips_through_the_abi() {
    let dir = TempDir::new("maintenance-status-abi");
    seed(dir.path());
    let handle = attach_ok(dir.path());
    let trigger = CString::new("scheduled").expect("no NUL");
    let step = CString::new("sweep_indexes").expect("no NUL");
    let status = CString::new("ran").expect("no NUL");
    let detail = CString::new("reclaimed 3 entries").expect("no NUL");
    let inputs = [MoraineMaintenanceStatusStepInput {
        step: step.as_ptr(),
        status: status.as_ptr(),
        detail: detail.as_ptr(),
    }];
    let mut err = MoraineError::default();

    // SAFETY: the live handle, strings, input slice, and error slot remain
    // valid for the call.
    let code = unsafe {
        moraine_maintenance_status_record(
            handle,
            123,
            trigger.as_ptr(),
            inputs.as_ptr(),
            inputs.len(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK, "record failed");

    let mut rows: *mut MoraineMaintenanceStatusRow = ptr::null_mut();
    let mut rows_len = 0;
    // SAFETY: the handle is live and output/error slots are writable.
    let code = unsafe {
        moraine_maintenance_status_rows(handle, &raw mut rows, &raw mut rows_len, &raw mut err)
    };
    assert_eq!(code, codes::OK, "list failed");
    assert_eq!(rows_len, 1);
    // SAFETY: the successful list call returned one valid row.
    let row = unsafe { &*rows };
    assert_eq!(row.started_at_micros, 123);
    for (actual, expected) in [
        (row.trigger, "scheduled"),
        (row.step, "sweep_indexes"),
        (row.status, "ran"),
        (row.detail, "reclaimed 3 entries"),
    ] {
        // SAFETY: every successful output string is live until the paired
        // array free below.
        let actual = unsafe { CStr::from_ptr(actual) }.to_str().unwrap();
        assert_eq!(actual, expected);
    }

    // SAFETY: both allocations are released exactly once.
    unsafe {
        moraine_maintenance_status_free(rows, rows_len);
        moraine_detach(handle);
    }
}

/// The census ABI names every subspace, writes the manifest version
/// through its slot, and carries the live counts only when asked.
#[test]
fn census_reports_every_subspace_through_the_abi() {
    let dir = TempDir::new("census-abi");
    seed(dir.path());
    let c_path = dir.c_path();

    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: all pointers are valid C strings / local slots.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            ptr::null(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK, "attach failed");

    for count_live in [false, true] {
        let mut items: *mut MoraineSubspaceCensus = ptr::null_mut();
        let mut len = 0usize;
        let mut manifest_id = u64::MAX;
        let mut objects = MoraineStoreObjects {
            listed: false,
            total_objects: 0,
            total_bytes: 0,
            wal_objects: 0,
            wal_bytes: 0,
            manifest_objects: 0,
            manifest_bytes: 0,
            sst_objects: 0,
            sst_bytes: 0,
            other_objects: 0,
            other_bytes: 0,
        };
        // SAFETY: `handle` is live; every slot is a writable local.
        let code = unsafe {
            moraine_store_census(
                handle,
                count_live,
                &raw mut items,
                &raw mut len,
                &raw mut manifest_id,
                &raw mut objects,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK, "census failed");
        assert!(len >= KNOWN_SUBSPACES.len(), "only {len} subspaces");
        assert_ne!(manifest_id, u64::MAX, "manifest version not written");
        // A local store lists fine, and a store that has been written
        // holds at least a manifest.
        assert!(objects.listed, "store not listed");
        assert!(objects.total_objects > 0, "no objects counted");
        assert!(objects.manifest_objects > 0, "no manifest counted");

        // SAFETY: `items`/`len` are what the call just wrote.
        let rows = unsafe { std::slice::from_raw_parts(items, len) };
        for row in rows {
            // SAFETY: every row owns a valid C string.
            let name = unsafe { CStr::from_ptr(row.subspace) };
            assert!(!name.to_bytes().is_empty());
            assert_eq!(row.has_live, count_live, "{name:?}");
        }
        assert!(
            rows.iter().any(|row| {
                // SAFETY: as above.
                unsafe { CStr::from_ptr(row.subspace) }.to_bytes() == b"current"
            }),
            "no `current` row"
        );

        // SAFETY: freed exactly once, with the matching length.
        unsafe { moraine_store_census_free(items, len) };
    }

    // SAFETY: freed exactly once.
    unsafe { moraine_detach(handle) };
}

/// The merge ABI reports one row per subspace, and refuses a subspace
/// name it does not know rather than merging the wrong tree.
#[test]
fn compact_store_reports_rows_and_refuses_unknown_subspaces() {
    let dir = TempDir::new("compact-abi");
    seed(dir.path());
    let c_path = dir.c_path();

    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: all pointers are valid C strings / local slots.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            ptr::null(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK, "attach failed");

    // A seeded store has no sorted runs, so every subspace is skipped
    // and none reports bytes after.
    let mut items: *mut MoraineSubspaceMerge = ptr::null_mut();
    let mut len = 0usize;
    // SAFETY: `handle` is live; every slot is a writable local.
    let code = unsafe {
        moraine_compact_store(
            handle,
            ptr::null(),
            1_000,
            false,
            &raw mut items,
            &raw mut len,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK, "compact_store failed");
    assert_eq!(len, KNOWN_SUBSPACES.len());

    // SAFETY: `items`/`len` are what the call just wrote.
    let rows = unsafe { std::slice::from_raw_parts(items, len) };
    for row in rows {
        // SAFETY: every row owns valid C strings.
        let outcome = unsafe { CStr::from_ptr(row.outcome) };
        assert_eq!(outcome.to_bytes(), b"skipped");
        assert!(!row.has_bytes_after);
        // SAFETY: as above.
        let detail = unsafe { CStr::from_ptr(row.detail) };
        assert!(!detail.to_bytes().is_empty(), "a skip states its reason");
    }
    // SAFETY: freed exactly once, with the matching length.
    unsafe { moraine_compact_store_free(items, len) };

    let unknown = CString::new("gcfile").expect("no interior nul");
    // SAFETY: `handle` is live; the name is a valid C string.
    let code = unsafe {
        moraine_compact_store(
            handle,
            unknown.as_ptr(),
            0,
            false,
            &raw mut items,
            &raw mut len,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::INVALID_ARGUMENT);
    if !err.message.is_null() {
        // SAFETY: the guard wrote an owned message.
        unsafe { moraine_error_free(err.message) };
    }

    // SAFETY: freed exactly once.
    unsafe { moraine_detach(handle) };
}

/// Nesting the catalog store inside `DATA_PATH` (or the reverse) on one
/// object store is refused; sibling locations, separate buckets, and
/// differing store kinds are not.
#[test]
fn overlapping_store_and_data_paths_are_refused() {
    let nested = [
        // The catalog sits under the swept data prefix.
        ("s3://bucket/lake/catalog", "s3://bucket/lake"),
        ("s3://bucket/lake/catalog", "s3://bucket/lake/"),
        // ...and the reverse nesting is equally unsafe.
        ("s3://bucket/lake", "s3://bucket/lake/data"),
        // Identical locations.
        ("s3://bucket/lake", "s3://bucket/lake"),
        // An empty prefix is the bucket root, containing everything.
        ("s3://bucket", "s3://bucket/data"),
        ("/tmp/lake/catalog", "/tmp/lake"),
        ("/tmp/lake", "/tmp/lake/data"),
    ];
    for (store, data) in nested {
        let error = refuse_overlapping_data_path(store, data)
            .expect_err("nested `{store}` / `{data}` must be refused");
        assert_eq!(error.code, codes::CONSTRAINT, "for {store} / {data}");
    }

    let separate = [
        // Sibling prefixes that merely share leading text.
        ("s3://bucket/lakehouse", "s3://bucket/lake"),
        ("s3://bucket/lake-catalog", "s3://bucket/lake"),
        ("/tmp/lakehouse", "/tmp/lake"),
        // True siblings.
        ("s3://bucket/catalog", "s3://bucket/data"),
        ("/tmp/catalog", "/tmp/data"),
        // Different buckets, and different store kinds.
        ("s3://catalogs/lake", "s3://data/lake"),
        ("/tmp/catalog", "s3://bucket/data"),
        ("memory://", "/tmp/data"),
    ];
    for (store, data) in separate {
        assert!(
            refuse_overlapping_data_path(store, data).is_ok(),
            "`{store}` / `{data}` are separate and must attach"
        );
    }
}

/// The overlap guard refuses before an adopted data path is recorded.
#[test]
fn attach_refuses_a_data_path_containing_the_store() {
    // A *fresh* store, deliberately not seeded: bootstrapping records
    // `data_path`, so this is the case where a late check would
    // persist the dangerous value before refusing.
    let dir = TempDir::new("overlap-guard");
    let c_path = dir.c_path();

    // DATA_PATH is the store's own parent, so orphan cleanup would
    // sweep the catalog's objects.
    let parent = dir
        .path()
        .parent()
        .expect("temp dir has a parent")
        .to_str()
        .expect("utf-8")
        .to_owned();
    let c_data = CString::new(parent).expect("no NUL");
    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: all pointers are valid C strings / local slots.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            c_data.as_ptr(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };
    assert_eq!(
        code,
        codes::CONSTRAINT,
        "a nested DATA_PATH must be refused"
    );
    // SAFETY: on failure `guard` wrote a non-null message.
    let message = unsafe { CStr::from_ptr(err.message) }
        .to_str()
        .unwrap()
        .to_owned();
    assert!(message.contains("nested"), "got: {message}");
    // SAFETY: minted by the failed call, freed once.
    unsafe { moraine_error_free(err.message) };

    // Nothing was recorded, so a later attach with a safe path still
    // adopts it.
    let safe = TempDir::new("overlap-guard-data");
    let c_safe = CString::new(safe.path().to_str().expect("utf-8")).expect("no NUL");
    let mut ok_handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut ok_err = MoraineError::default();
    // SAFETY: all pointers are valid C strings / local slots.
    let ok_code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            c_safe.as_ptr(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut ok_handle,
            &raw mut ok_err,
        )
    };
    // SAFETY: null or just written; `as_ref` allows null.
    let ok_message = unsafe { ok_err.message.as_ref() };
    assert_eq!(ok_code, codes::OK, "safe path failed: {ok_message:?}");
    // SAFETY: freed exactly once.
    unsafe { moraine_detach(ok_handle) };
}

/// A lake with no data path recorded yet (created before the option
/// existed) adopts the one given at its next attach, and enforces it
/// thereafter.
#[test]
fn attach_records_a_missing_data_path_then_fixes_it() {
    let dir = TempDir::new("legacy-data-path");
    seed(dir.path()); // a store with no data_path recorded
    let data = TempDir::new("legacy-data-path-root");
    let recorded = data.path().to_str().expect("utf-8").to_owned();
    let c_path = dir.c_path();

    // The first attach records the data path.
    let c_first = CString::new(recorded.clone()).expect("no NUL");
    let mut first_handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut first_err = MoraineError::default();
    // SAFETY: all pointers are valid C strings / local slots.
    let first_code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            c_first.as_ptr(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut first_handle,
            &raw mut first_err,
        )
    };
    // SAFETY: `first_err.message` is null or just written; `as_ref` allows null.
    let first_message = unsafe { first_err.message.as_ref() };
    assert_eq!(
        first_code,
        codes::OK,
        "recording attach failed: {first_message:?}"
    );
    // SAFETY: freed exactly once.
    unsafe { moraine_detach(first_handle) };

    // A later attach with a different data path is now refused.
    let c_other = CString::new("/lake/elsewhere").expect("no NUL");
    let mut other_handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut other_err = MoraineError::default();
    // SAFETY: all pointers are valid C strings / local slots.
    let other_code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            c_other.as_ptr(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut other_handle,
            &raw mut other_err,
        )
    };
    assert_ne!(other_code, codes::OK, "the recorded path is now enforced");
    // SAFETY: on failure `guard` wrote a non-null message.
    let other_message = unsafe { CStr::from_ptr(other_err.message) }
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        other_message.contains("does not match"),
        "got: {other_message}"
    );
    // SAFETY: minted by the failed call, freed once.
    unsafe { moraine_error_free(other_err.message) };
}

/// A read-only attach of an uninitialized store fails with guidance to
/// add `READ_WRITE`.
#[test]
fn read_only_attach_of_fresh_store_hints_read_write() {
    let dir = TempDir::new("ro-fresh");
    let c_path =
        CString::new(dir.path().to_str().expect("test path is UTF-8")).expect("no NUL in path");
    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `c_path` is a valid C string; outputs are valid local slots.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            true,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            ptr::null(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };
    assert_ne!(
        code,
        codes::OK,
        "read-only attach of a fresh store should fail"
    );
    assert!(handle.is_null());
    // SAFETY: on failure `err.message` is a valid, just-written C string.
    let message = unsafe { CStr::from_ptr(err.message) }
        .to_str()
        .expect("message is UTF-8")
        .to_owned();
    // SAFETY: frees the message allocated by the failed attach, exactly once.
    unsafe { moraine_error_free(err.message) };
    assert!(
        message.contains("READ_WRITE"),
        "read-only attach error should point at READ_WRITE: {message}"
    );
}

/// The `encrypted` flag is fixed by the attach that bootstraps the
/// store; later attaches requesting a different value do not flip it,
/// and the getter always reports the stored flag.
#[test]
fn attach_encrypted_is_fixed_at_bootstrap_and_reported() {
    let dir = TempDir::new("encrypted");
    let c_path = dir.c_path();

    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `c_path` is a valid C string; outputs are valid local slots.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            true,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            ptr::null(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK);
    assert!(catalog_encrypted(handle));
    // SAFETY: `handle` came from the attach above, detached exactly once.
    unsafe { moraine_detach(handle) };

    // Re-attach without requesting encryption: the stored flag wins.
    let handle = attach_ok(dir.path());
    assert!(catalog_encrypted(handle));
    // SAFETY: same as above.
    unsafe { moraine_detach(handle) };

    // A default-attached fresh store reports unencrypted.
    let dir_plain = TempDir::new("unencrypted");
    let handle = attach_ok(dir_plain.path());
    assert!(!catalog_encrypted(handle));
    // SAFETY: same as above.
    unsafe { moraine_detach(handle) };
}

#[test]
#[allow(clippy::too_many_lines)] // one end-to-end attach→list assertion chain
fn attach_snapshot_and_list_round_trip() {
    let dir = TempDir::new("roundtrip");
    seed(dir.path());

    let handle = attach_ok(dir.path());

    let mut snapshot: *mut MoraineSnapshotHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `handle` is attached; `snapshot`/`err` are valid local slots.
    let code = unsafe {
        moraine_snapshot(
            handle,
            &raw mut snapshot,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK);
    assert!(!snapshot.is_null());

    let mut schemas: *mut MoraineSchemaDesc = ptr::null_mut();
    let mut schemas_len: usize = 0;
    // SAFETY: `snapshot` is live; outputs are valid local slots.
    let code = unsafe {
        moraine_snapshot_schemas(
            snapshot,
            &raw mut schemas,
            &raw mut schemas_len,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK);
    // Bootstrap mints `main` (id 0); the seeded `sales` follows at id 1.
    assert_eq!(schemas_len, 2);
    // SAFETY: just populated above with `schemas_len` live elements.
    let schema_descs = unsafe { std::slice::from_raw_parts(schemas, schemas_len) };
    let schema_pairs: Vec<(u64, &str)> = schema_descs
        .iter()
        // SAFETY: owned C strings written above, not yet freed.
        .map(|s| (s.id, unsafe { CStr::from_ptr(s.name) }.to_str().unwrap()))
        .collect();
    assert_eq!(schema_pairs, [(0, "main"), (1, "sales")]);
    let schema_id = schema_descs[1].id;

    let mut tables: *mut MoraineTableDesc = ptr::null_mut();
    let mut tables_len: usize = 0;
    // SAFETY: `snapshot` is live; outputs are valid local slots.
    let code = unsafe {
        moraine_snapshot_tables_in(
            snapshot,
            schema_id,
            &raw mut tables,
            &raw mut tables_len,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(tables_len, 1);
    // SAFETY: just populated by `moraine_snapshot_tables_in` above.
    let table_id = unsafe { (*tables).id };
    // SAFETY: same as above.
    let table_name = unsafe { CStr::from_ptr((*tables).name) }.to_str().unwrap();
    assert_eq!(table_name, "orders");

    let mut columns: *mut MoraineColumnDesc = ptr::null_mut();
    let mut columns_len: usize = 0;
    // SAFETY: `snapshot` is live; outputs are valid local slots.
    let code = unsafe {
        moraine_snapshot_columns_of(
            snapshot,
            table_id,
            &raw mut columns,
            &raw mut columns_len,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(columns_len, 2);
    // SAFETY: just populated above with `columns_len` live elements.
    let cols = unsafe { std::slice::from_raw_parts(columns, columns_len) };
    let names: Vec<&str> = cols
        .iter()
        // SAFETY: owned C strings written above, not yet freed.
        .map(|c| unsafe { CStr::from_ptr(c.name) }.to_str().unwrap())
        .collect();
    assert_eq!(names, vec!["id", "amount"]);
    assert!(!cols[0].nulls_allowed);
    assert!(cols[1].nulls_allowed);

    let mut views: *mut MoraineViewDesc = ptr::null_mut();
    let mut views_len: usize = 0;
    // SAFETY: `snapshot` is live; outputs are valid local slots.
    let code = unsafe {
        moraine_snapshot_views_in(
            snapshot,
            schema_id,
            &raw mut views,
            &raw mut views_len,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(views_len, 1);
    // SAFETY: just populated by `moraine_snapshot_views_in` above.
    let view_sql = unsafe { CStr::from_ptr((*views).sql) }.to_str().unwrap();
    assert_eq!(view_sql, "select * from orders");

    let mut files: *mut MoraineDataFileDesc = ptr::null_mut();
    let mut files_len: usize = 0;
    // SAFETY: `snapshot` is live; outputs are valid local slots.
    let code = unsafe {
        moraine_snapshot_data_files_of(
            snapshot,
            table_id,
            &raw mut files,
            &raw mut files_len,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(files_len, 1);
    // SAFETY: just populated by `moraine_snapshot_data_files_of` above.
    let file_path = unsafe { CStr::from_ptr((*files).path) }.to_str().unwrap();
    assert_eq!(file_path, "orders/data-1.parquet");
    // SAFETY: same as above.
    assert_eq!(unsafe { (*files).record_count }, 10);
    // SAFETY: same as above.
    assert_eq!(unsafe { (*files).row_id_start }, 0);

    // SAFETY: each from its matching allocator; freed exactly once.
    unsafe {
        moraine_snapshot_schemas_free(schemas, schemas_len);
        moraine_snapshot_tables_in_free(tables, tables_len);
        moraine_snapshot_columns_of_free(columns, columns_len);
        moraine_snapshot_views_in_free(views, views_len);
        moraine_snapshot_data_files_of_free(files, files_len);
        moraine_snapshot_free(snapshot);
        moraine_detach(handle);
    }
}

/// A catalog string with an embedded NUL (reachable via a view's SQL,
/// since `moraine` stores `\0` verbatim) cannot cross the C boundary:
/// the listing call must fail with `CORRUPTION`, leaving the outputs
/// untouched.
#[test]
fn embedded_nul_in_catalog_data_reports_corruption() {
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
                // Two views: the clean one converts first (ordered by
                // id) and must drop cleanly when the second fails.
                tx.create_view(schema, "clean", "duckdb", "select 1")?;
                tx.create_view(schema, "poisoned", "duckdb", "select 1 as a\0b")?;
                Ok(())
            })
            .await
            .expect("test setup: commit fixtures");
        catalog.close().await.expect("test setup: close catalog");
    });

    let handle = attach_ok(dir.path());
    let mut snap: *mut MoraineSnapshotHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `handle` is attached; `snapshot`/`err` are valid local slots.
    let code =
        unsafe { moraine_snapshot(handle, &raw mut snap, None, ptr::null_mut(), &raw mut err) };
    assert_eq!(code, codes::OK);

    let mut views: *mut MoraineViewDesc = ptr::null_mut();
    let mut views_len: usize = 0;
    // Schema `s` has id 1: bootstrap's `main` schema holds id 0.
    //
    // SAFETY: `snapshot` is live; outputs are valid local slots.
    let code = unsafe {
        moraine_snapshot_views_in(snap, 1, &raw mut views, &raw mut views_len, &raw mut err)
    };
    assert_eq!(code, codes::CORRUPTION);
    assert_eq!(err.code, codes::CORRUPTION);
    // The outputs stay untouched on failure: nothing was handed to
    // the caller, so there is nothing for the caller to free.
    assert!(views.is_null());
    assert_eq!(views_len, 0);
    assert!(!err.message.is_null());
    // SAFETY: just populated above.
    let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
    assert!(msg.contains("NUL"), "message: {msg}");

    // SAFETY: `err.message` was just populated and not yet freed;
    // `snapshot`/`handle` came from the calls above and are freed exactly
    // once.
    unsafe {
        moraine_error_free(err.message);
        moraine_snapshot_free(snap);
        moraine_detach(handle);
    }
}

#[test]
fn empty_table_lists_no_data_files() {
    let dir = TempDir::new("empty-table");
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
                tx.create_table(
                    schema,
                    "empty",
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
        catalog.close().await.expect("test setup: close catalog");
    });

    let handle = attach_ok(dir.path());
    let mut snap: *mut MoraineSnapshotHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `handle` is attached; `snapshot`/`err` are valid local slots.
    let code =
        unsafe { moraine_snapshot(handle, &raw mut snap, None, ptr::null_mut(), &raw mut err) };
    assert_eq!(code, codes::OK);

    let mut tables: *mut MoraineTableDesc = ptr::null_mut();
    let mut tables_len: usize = 0;
    // Schema `s` has id 1: bootstrap's `main` schema holds id 0.
    //
    // SAFETY: `snapshot` is live; outputs are valid local slots.
    let code = unsafe {
        moraine_snapshot_tables_in(snap, 1, &raw mut tables, &raw mut tables_len, &raw mut err)
    };
    assert_eq!(code, codes::OK);
    assert_eq!(tables_len, 1);
    // SAFETY: just populated by `moraine_snapshot_tables_in` above.
    let table_id = unsafe { (*tables).id };

    let mut files: *mut MoraineDataFileDesc = ptr::null_mut();
    let mut files_len: usize = 0;
    // SAFETY: `snapshot` is live; outputs are valid local slots.
    let code = unsafe {
        moraine_snapshot_data_files_of(
            snap,
            table_id,
            &raw mut files,
            &raw mut files_len,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK);
    assert_eq!(files_len, 0);

    // SAFETY: each from its matching allocator; freed exactly once.
    unsafe {
        moraine_snapshot_tables_in_free(tables, tables_len);
        moraine_snapshot_data_files_of_free(files, files_len);
        moraine_snapshot_free(snap);
        moraine_detach(handle);
    }
}

#[test]
fn attach_on_unwritable_path_reports_invalid_argument() {
    // A path nested under a file (not a directory) can never be
    // created: `create_dir_all` fails with `NotADirectory`/`ENOTDIR`.
    let dir = TempDir::new("bad-path");
    let file_path = dir.path().join("not-a-directory");
    std::fs::write(&file_path, b"not a directory").expect("test setup: write file");
    let bogus = file_path.join("nested");

    let c_path = CString::new(bogus.to_str().expect("UTF-8")).expect("no NUL");
    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `c_path` is a valid NUL-terminated C string; `handle`/`err`
    // are valid, writable local slots.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            ptr::null(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };

    assert_eq!(code, codes::INVALID_ARGUMENT);
    assert_eq!(err.code, codes::INVALID_ARGUMENT);
    assert!(handle.is_null());
    assert!(!err.message.is_null());
    // SAFETY: just populated above.
    let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
    assert!(msg.contains("cannot create directory"), "message: {msg}");

    // SAFETY: `err.message` was just populated above and not yet freed.
    unsafe { moraine_error_free(err.message) };
}

#[test]
fn attach_rejects_unknown_store_scheme() {
    // A remote scheme moraine doesn't back is rejected from the path
    // itself, before any store is opened.
    let c_path = CString::new("gs://some-bucket").expect("no NUL");
    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `c_path` is a valid NUL-terminated C string; `s3` is null
    // (env-only); `handle`/`err` are valid, writable local slots.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            ptr::null(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };

    assert_eq!(code, codes::INVALID_ARGUMENT);
    assert!(handle.is_null());
    // SAFETY: just populated above.
    let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
    assert!(msg.contains("unsupported store scheme"), "message: {msg}");
    // SAFETY: `err.message` was just populated above and not yet freed.
    unsafe { moraine_error_free(err.message) };
}

#[test]
fn store_kind_parses_s3_bucket_and_prefix() {
    let (kind, prefix) =
        StoreKind::from_path("s3://my-bucket/catalogs/lake").expect("s3 with prefix parses");
    assert!(matches!(kind, StoreKind::S3 { ref bucket } if bucket == "my-bucket"));
    assert_eq!(prefix, "catalogs/lake");

    let (kind, prefix) = StoreKind::from_path("s3://my-bucket").expect("bare bucket parses");
    assert!(matches!(kind, StoreKind::S3 { ref bucket } if bucket == "my-bucket"));
    assert_eq!(prefix, "");

    let (kind, prefix) = StoreKind::from_path("/tmp/lake").expect("local path parses");
    assert!(matches!(kind, StoreKind::LocalFile));
    assert_eq!(prefix, "");

    assert!(
        StoreKind::from_path("s3://").is_err(),
        "empty bucket is rejected"
    );
    assert!(
        StoreKind::from_path("gs://b").is_err(),
        "unknown scheme is rejected"
    );
}

#[test]
fn attach_null_path_reports_invalid_argument_without_crashing() {
    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: a null `path` is exactly the input this test exercises;
    // `handle`/`err` are valid, writable local slots.
    let code = unsafe {
        moraine_attach(
            ptr::null(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            ptr::null(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::INVALID_ARGUMENT);
    assert!(handle.is_null());
    // SAFETY: just populated above.
    let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
    assert!(msg.contains("path"), "message: {msg}");
    // SAFETY: `err.message` was just populated above and not yet freed.
    unsafe { moraine_error_free(err.message) };
}

#[test]
fn snapshot_on_null_handle_reports_invalid_argument() {
    let mut snapshot: *mut MoraineSnapshotHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: a null `handle` is exactly the input this test exercises;
    // `snapshot`/`err` are valid, writable local slots.
    let code = unsafe {
        moraine_snapshot(
            ptr::null_mut(),
            &raw mut snapshot,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::INVALID_ARGUMENT);
    assert!(snapshot.is_null());
    // SAFETY: `err.message` was just populated above and not yet freed.
    unsafe { moraine_error_free(err.message) };
}

#[test]
fn detach_and_frees_tolerate_null() {
    // Every teardown function must be a safe no-op on null.
    //
    // SAFETY: every argument below is null, which each function's own
    // contract documents as a no-op.
    unsafe {
        moraine_detach(ptr::null_mut());
        moraine_snapshot_free(ptr::null_mut());
        moraine_error_free(ptr::null_mut());
        moraine_snapshot_schemas_free(ptr::null_mut(), 0);
        moraine_snapshot_tables_in_free(ptr::null_mut(), 0);
        moraine_snapshot_columns_of_free(ptr::null_mut(), 0);
        moraine_snapshot_views_in_free(ptr::null_mut(), 0);
        moraine_snapshot_data_files_of_free(ptr::null_mut(), 0);
    }
}

/// A panic inside `guard` surfaces as `codes::INTERNAL` with the fixed
/// message instead of unwinding across the FFI boundary.
#[test]
fn guard_contains_a_panic_as_the_internal_error_code() {
    let mut err = MoraineError::default();
    // SAFETY: `err` is a valid, writable local slot.
    let outcome: Result<(), i32> =
        unsafe { guard(&raw mut err, || -> Result<(), AbiError> { panic!("boom") }) };
    assert_eq!(outcome, Err(codes::INTERNAL));
    assert_eq!(err.code, codes::INTERNAL);
    assert!(!err.message.is_null());
    // SAFETY: just populated above.
    let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
    assert_eq!(msg, INTERNAL_PANIC_MESSAGE);
    // SAFETY: `err.message` was just populated above and not yet freed.
    unsafe { moraine_error_free(err.message) };
}

unsafe extern "C" fn probe_never(_probe_ctx: *mut c_void) -> bool {
    false
}

unsafe extern "C" fn probe_always(_probe_ctx: *mut c_void) -> bool {
    true
}

/// A probe that stays quiet forever must leave the core future to win.
#[test]
fn cancellable_block_on_completes_when_probe_never_fires() {
    let dir = TempDir::new("probe-quiet");
    seed(dir.path());
    let handle = attach_ok(dir.path());

    // SAFETY: `handle` came from `attach_ok` and is still attached.
    let handle_ref = unsafe { &*handle };
    // SAFETY: `probe_never` is callable with a null context from any
    // thread.
    let result = unsafe {
        handle_ref.block_on_cancellable(Some(probe_never), ptr::null_mut(), async {
            Ok::<_, moraine::Error>(7u32)
        })
    };
    assert_eq!(result.unwrap(), 7);

    // SAFETY: freed exactly once.
    unsafe { moraine_detach(handle) };
}

/// A null probe is the non-cancellable configuration: the future runs.
#[test]
fn cancellable_block_on_with_null_probe_completes() {
    let dir = TempDir::new("probe-null");
    seed(dir.path());
    let handle = attach_ok(dir.path());

    // SAFETY: `handle` came from `attach_ok` and is still attached.
    let handle_ref = unsafe { &*handle };
    // SAFETY: a `None` probe never dereferences `probe_ctx`.
    let result = unsafe {
        handle_ref.block_on_cancellable(None, ptr::null_mut(), async {
            Ok::<_, moraine::Error>(7u32)
        })
    };
    assert_eq!(result.unwrap(), 7);

    // SAFETY: freed exactly once.
    unsafe { moraine_detach(handle) };
}

/// A probe firing while the future is pending cancels it: the poll
/// loop, not just the immediate first check, is live. The future never
/// resolves, so only the probe can end this call.
#[test]
fn cancellable_block_on_cancels_pending_future_when_probe_fires() {
    // First poll false (the immediate pre-flight check), every later
    // poll true.
    unsafe extern "C" fn probe_true_after_first(probe_ctx: *mut c_void) -> bool {
        // SAFETY: this test passes a valid `AtomicU64` pointer below.
        let calls = unsafe { &*probe_ctx.cast::<AtomicU64>() };
        calls.fetch_add(1, Ordering::SeqCst) >= 1
    }

    let dir = TempDir::new("probe-mid-flight");
    seed(dir.path());
    let handle = attach_ok(dir.path());

    let calls = AtomicU64::new(0);

    // SAFETY: `handle` came from `attach_ok` and is still attached.
    let handle_ref = unsafe { &*handle };
    // SAFETY: `calls` outlives the call; the probe only reads it
    // atomically.
    let result: Result<(), AbiError> = unsafe {
        handle_ref.block_on_cancellable(
            Some(probe_true_after_first),
            (&raw const calls).cast_mut().cast(),
            std::future::pending::<Result<(), moraine::Error>>(),
        )
    };
    let error = result.unwrap_err();
    assert_eq!(error.code, codes::INTERRUPTED);
    assert!(calls.load(Ordering::SeqCst) >= 2);

    // SAFETY: freed exactly once.
    unsafe { moraine_detach(handle) };
}

/// An interrupt that arrives once the operation has already produced
/// its result changes nothing: the result is reported.
#[test]
fn an_interrupt_after_the_result_is_known_still_reports_it() {
    unsafe extern "C" fn probe_flag(probe_ctx: *mut c_void) -> bool {
        // SAFETY: this test passes a valid `AtomicBool` pointer below.
        unsafe { &*probe_ctx.cast::<AtomicBool>() }.load(Ordering::SeqCst)
    }

    let dir = TempDir::new("probe-after-result");
    seed(dir.path());
    let handle = attach_ok(dir.path());

    let interrupted = AtomicBool::new(false);
    // SAFETY: `handle` came from `attach_ok` and is still attached.
    let handle_ref = unsafe { &*handle };
    // SAFETY: `interrupted` outlives the call; the probe only reads it
    // atomically.
    let result: Result<u32, AbiError> = unsafe {
        handle_ref.block_on_cancellable(
            Some(probe_flag),
            (&raw const interrupted).cast_mut().cast(),
            async {
                // The host's interrupt lands here: after the work is
                // done, before the bridge has returned.
                interrupted.store(true, Ordering::SeqCst);
                Ok::<_, moraine::Error>(9u32)
            },
        )
    };
    assert_eq!(
        result.unwrap_or(0),
        9,
        "an interrupt past the point of no return must not discard a known result"
    );

    // SAFETY: freed exactly once.
    unsafe { moraine_detach(handle) };
}

#[test]
fn cancellation_after_durability_reports_an_unknown_commit_outcome() {
    unsafe extern "C" fn probe_flag(context: *mut c_void) -> bool {
        // SAFETY: the test passes a live atomic flag.
        unsafe { &*context.cast::<AtomicBool>() }.load(Ordering::SeqCst)
    }
    let dir = TempDir::new("unknown-after-durability");
    seed(dir.path());
    let handle = attach_ok(dir.path());
    // SAFETY: the handle remains attached throughout the call.
    let catalog = unsafe { &*handle };
    let interrupted = AtomicBool::new(false);
    // SAFETY: the probe context remains live for the duration of the call.
    let result: Result<(), AbiError> = unsafe {
        catalog.block_on_commit(
            Some(probe_flag),
            (&raw const interrupted).cast_mut().cast(),
            async {
                catalog
                    .catalog
                    .writer()?
                    .commit(|tx| tx.create_schema("landed").map(|_| ()))
                    .await?;
                interrupted.store(true, Ordering::SeqCst);
                std::future::pending::<Result<(), moraine::Error>>().await
            },
        )
    };
    assert_eq!(result.unwrap_err().code, codes::COMMIT_OUTCOME_UNKNOWN);
    assert!(
        catalog
            .block_on(catalog.catalog.reads().snapshot())
            .unwrap()
            .schema_by_name("landed")
            .is_some()
    );
    // SAFETY: consumed exactly once.
    unsafe { moraine_detach(handle) };
}

/// The staged-row commit honors its probe, and a commit refused before
/// it ran leaves the catalog exactly where it was.
#[test]
fn probe_cancels_a_staged_commit_and_nothing_lands() {
    let dir = TempDir::new("probe-tx-commit");
    seed(dir.path());
    let handle = attach_ok(dir.path());
    let before = snapshot_id_of(handle);

    let tx = begin(handle);
    let mut snapshot_id = 0u64;
    let mut err = MoraineError::default();
    // SAFETY: `tx` came from `begin` and is consumed exactly once;
    // the out-params are local slots; `probe_always` accepts a null
    // context.
    let code = unsafe {
        moraine_tx_commit(
            tx,
            &raw mut snapshot_id,
            Some(probe_always),
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::INTERRUPTED);
    // SAFETY: populated by the failed call above, freed exactly once.
    unsafe { moraine_error_free(err.message) };

    assert_eq!(
        snapshot_id_of(handle),
        before,
        "an interrupted commit must not advance head"
    );

    // SAFETY: freed exactly once.
    unsafe { moraine_detach(handle) };
}

/// The head this handle reports, for the cases that assert a commit
/// left it alone.
fn snapshot_id_of(handle: *mut MoraineCatalogHandle) -> u64 {
    // SAFETY: `handle` is attached for the duration of the caller.
    let handle_ref = unsafe { &*handle };
    handle_ref
        .block_on(handle_ref.catalog.reads().snapshot())
        .expect("read head")
        .current_snapshot()
        .id
        .get()
}

/// The pull channel end to end: a probe reporting an interrupt cancels
/// the snapshot (out-param unwritten), and the same handle with a
/// quiet probe succeeds right after — the signal is level-triggered
/// and scoped to the call that observed it.
#[test]
fn probe_cancels_snapshot_then_quiet_probe_succeeds() {
    let dir = TempDir::new("probe-snapshot");
    seed(dir.path());
    let handle = attach_ok(dir.path());

    let mut snapshot: *mut MoraineSnapshotHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `handle` is attached; `snapshot`/`err` are valid local
    // slots; `probe_always` accepts a null context.
    let code = unsafe {
        moraine_snapshot(
            handle,
            &raw mut snapshot,
            Some(probe_always),
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::INTERRUPTED);
    assert_eq!(err.code, codes::INTERRUPTED);
    assert!(snapshot.is_null());
    // SAFETY: populated by the failed call above, freed exactly once.
    unsafe { moraine_error_free(err.message) };

    let mut snap2: *mut MoraineSnapshotHandle = ptr::null_mut();
    let mut err2 = MoraineError::default();
    // SAFETY: same contracts; `probe_never` accepts a null context.
    let code2 = unsafe {
        moraine_snapshot(
            handle,
            &raw mut snap2,
            Some(probe_never),
            ptr::null_mut(),
            &raw mut err2,
        )
    };
    assert_eq!(code2, codes::OK);
    assert!(!snap2.is_null());

    // SAFETY: freed exactly once each.
    unsafe {
        moraine_snapshot_free(snap2);
        moraine_detach(handle);
    }
}

/// Cancellation is per call, not per handle: two reads in flight on
/// one handle carry their own probes, and interrupting one leaves the
/// other to finish.
#[test]
fn concurrent_reads_on_one_handle_cancel_independently() {
    let dir = TempDir::new("probe-concurrent");
    seed(dir.path());
    let handle = attach_ok(dir.path());
    let handle_address = handle as usize;

    // The interrupted read never resolves on its own, so only its own
    // probe can end it; the survivor waits for that to happen before
    // resolving, so the two genuinely overlap.
    let cancelled_first = Arc::new(std::sync::Barrier::new(2));
    let waiter = Arc::clone(&cancelled_first);

    let interrupted = std::thread::spawn(move || {
        let handle = handle_address as *mut MoraineCatalogHandle;
        // SAFETY: the handle outlives both threads — it is detached
        // only after they are joined.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe_always` accepts a null context.
        let result: Result<(), AbiError> = unsafe {
            handle_ref.block_on_cancellable(
                Some(probe_always),
                ptr::null_mut(),
                std::future::pending::<Result<(), moraine::Error>>(),
            )
        };
        cancelled_first.wait();
        result
    });

    let survivor = std::thread::spawn(move || {
        let handle = handle_address as *mut MoraineCatalogHandle;
        // SAFETY: as above.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe_never` accepts a null context.
        unsafe {
            handle_ref.block_on_cancellable(Some(probe_never), ptr::null_mut(), async move {
                waiter.wait();
                Ok::<_, moraine::Error>(7u32)
            })
        }
    });

    assert_eq!(
        interrupted
            .join()
            .expect("interrupted read")
            .unwrap_err()
            .code,
        codes::INTERRUPTED
    );
    assert_eq!(
        survivor.join().expect("surviving read").unwrap(),
        7,
        "one read's interrupt must not cancel or be consumed by another's"
    );

    // SAFETY: both threads are joined; freed exactly once.
    unsafe { moraine_detach(handle) };
}

/// An attach whose probe is already firing is cancelled before the
/// store is opened: no handle, the interrupted code, and the call
/// returns.
#[test]
fn attach_is_cancelled_by_a_firing_probe() {
    let dir = TempDir::new("probe-attach");
    seed(dir.path());

    let c_path = dir.c_path();
    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `c_path` is a valid C string, the out-params are local
    // slots, and `probe_always` accepts a null context.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            ptr::null(),
            0,
            0,
            0,
            false,
            ptr::null(),
            ptr::null(),
            0,
            Some(probe_always),
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };
    assert_eq!(code, codes::INTERRUPTED);
    assert_eq!(err.code, codes::INTERRUPTED);
    assert!(handle.is_null(), "a cancelled attach writes no handle");
    // SAFETY: populated by the failed call above, freed exactly once.
    unsafe { moraine_error_free(err.message) };

    // The store is untouched by the cancellation: a plain attach still
    // works, so nothing was left half-initialized.
    let handle = attach_ok(dir.path());
    // SAFETY: freed exactly once.
    unsafe { moraine_detach(handle) };
}

/// A lookup value coerces to the same canonical `IndexKeyValue` the
/// scoped read derives for the column's type — width and all — so a
/// lookup matches a stored key.
#[test]
fn coerce_lookup_value_matches_column_types() {
    use moraine::{IndexKeyValue, IntWidth};

    let blank = MoraineLookupValue {
        kind: 0,
        i64_value: 0,
        u64_value: 0,
        f64_value: 0.0,
        bool_value: false,
        str_value: ptr::null(),
        bytes_value: ptr::null(),
        bytes_len: 0,
    };

    let int_value = MoraineLookupValue {
        kind: 1,
        i64_value: 42,
        ..blank
    };
    // The same integer takes the column's width, not the literal's — and
    // DuckLake's bit-width spelling (`INT64`) resolves like the SQL name.
    // SAFETY: an integer-kind value dereferences no pointer fields.
    let as_bigint = unsafe { coerce_lookup_value(&int_value, "INT64") }.unwrap();
    assert_eq!(
        as_bigint,
        IndexKeyValue::Int {
            value: 42,
            width: IntWidth::I64
        }
    );
    // SAFETY: as above.
    let as_integer = unsafe { coerce_lookup_value(&int_value, "INT32") }.unwrap();
    assert_eq!(
        as_integer,
        IndexKeyValue::Int {
            value: 42,
            width: IntWidth::I32
        }
    );

    // A UUID arrives as 16 bytes.
    let uuid = [0x5Au8; 16];
    let bytes_value = MoraineLookupValue {
        kind: 6,
        bytes_value: uuid.as_ptr(),
        bytes_len: uuid.len(),
        ..blank
    };
    // SAFETY: `uuid` outlives the call.
    let as_uuid = unsafe { coerce_lookup_value(&bytes_value, "UUID") }.unwrap();
    assert_eq!(as_uuid, IndexKeyValue::Bytes(uuid.to_vec()));

    let text = CString::new("hello").expect("no NUL");
    let str_value = MoraineLookupValue {
        kind: 5,
        str_value: text.as_ptr(),
        ..blank
    };
    // SAFETY: `text` outlives the call.
    let as_varchar = unsafe { coerce_lookup_value(&str_value, "VARCHAR") }.unwrap();
    assert_eq!(as_varchar, IndexKeyValue::Str("hello".to_owned()));

    // A kind that cannot represent the column, and an unsupported type,
    // are both refused rather than silently mis-encoded.
    // SAFETY: integer-kind value, no pointer fields.
    let wrong_kind = unsafe { coerce_lookup_value(&int_value, "UUID") };
    assert!(wrong_kind.is_err());
    // SAFETY: as above.
    let unsupported = unsafe { coerce_lookup_value(&int_value, "DECIMAL(18,3)") };
    assert!(unsupported.is_err());
}

/// Zero bytes on the ABI means "not given", so the store's own cap
/// stands; any other value is that many bytes of object cache.
#[test]
fn a_zero_cache_size_leaves_the_default_cap() {
    assert_eq!(cache_size_option(0), None);
    assert_eq!(cache_size_option(64 * 1024 * 1024), Some(64 * 1024 * 1024));
}

/// The preload codes the ABI takes, and the refusal of one it does
/// not: a level nobody can act on is a caller mistake, not a default
/// to fall back to.
#[test]
fn cache_preload_codes_map_to_levels_and_reject_the_rest() {
    assert_eq!(cache_preload_option(0).unwrap(), None);
    assert_eq!(
        cache_preload_option(1).unwrap(),
        Some(moraine::CachePreload::L0)
    );
    assert_eq!(
        cache_preload_option(2).unwrap(),
        Some(moraine::CachePreload::All)
    );
    let refused = cache_preload_option(7).unwrap_err();
    assert_eq!(refused.code, codes::INVALID_ARGUMENT);
    assert!(refused.message.contains('7'), "{}", refused.message);
}

/// `cache_puts` crosses the ABI and opens either way; the core tests
/// what it governs.
#[test]
fn an_attach_takes_the_write_admission_flag() {
    for cache_puts in [false, true] {
        let dir = TempDir::new("put-cache-store");
        let cache = TempDir::new("put-cache-dir");
        let c_path = dir.c_path();
        let c_cache = cache.c_path();
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: both C strings outlive the call; outputs are valid
        // local slots; null s3/data_path/checkpoint are the documented
        // "none" cases.
        let code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                false,
                c_cache.as_ptr(),
                0,
                0,
                0,
                cache_puts,
                ptr::null(),
                ptr::null(),
                0,
                None,
                ptr::null_mut(),
                &raw mut handle,
                &raw mut err,
            )
        };
        // SAFETY: `err.message` is null or was just written by the call.
        let message = unsafe { err.message.as_ref() };
        assert_eq!(code, codes::OK, "attach failed: {message:?}");
        // SAFETY: attached above and not yet detached.
        unsafe { moraine_detach(handle) };
    }
}

/// An attach given a cache directory and a cap opens against them: the
/// cap crosses the ABI as a byte count rather than failing the open.
#[test]
fn an_attach_takes_a_bounded_disk_cache() {
    let dir = TempDir::new("bounded-cache-store");
    let cache = TempDir::new("bounded-cache-dir");
    let c_path = dir.c_path();
    let c_cache = cache.c_path();
    let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: both C strings outlive the call; outputs are valid local
    // slots; null s3/data_path/checkpoint are the documented "none"
    // cases.
    let code = unsafe {
        moraine_attach(
            c_path.as_ptr(),
            ptr::null(),
            false,
            false,
            0,
            false,
            c_cache.as_ptr(),
            64 * 1024 * 1024,
            0,
            0,
            false,
            ptr::null(),
            ptr::null(),
            0,
            None,
            ptr::null_mut(),
            &raw mut handle,
            &raw mut err,
        )
    };
    // SAFETY: `err.message` is null or was just written by the call.
    let message = unsafe { err.message.as_ref() };
    assert_eq!(code, codes::OK, "attach failed: {message:?}");
    // SAFETY: attached above and not yet detached.
    unsafe { moraine_detach(handle) };
}

/// An attach that outlives the one which built the block cache still
/// reads. The cache directory is what makes the first attach build
/// the hybrid, and the detach-then-attach sequence is the property.
#[test]
fn a_second_attach_outlives_the_cache_builders_runtime() {
    let cache = TempDir::new("cache-runtime-dir");
    let c_cache = cache.c_path();

    let attach = |path: &std::ffi::CString| {
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: both C strings outlive the call; outputs are valid
        // local slots; null s3/data_path/checkpoint are the documented
        // "none" cases.
        let code = unsafe {
            moraine_attach(
                path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                false,
                c_cache.as_ptr(),
                0,
                0,
                0,
                false,
                ptr::null(),
                ptr::null(),
                0,
                None,
                ptr::null_mut(),
                &raw mut handle,
                &raw mut err,
            )
        };
        // SAFETY: `err.message` is null or was just written.
        let message = unsafe { err.message.as_ref() };
        assert_eq!(code, codes::OK, "attach failed: {message:?}");
        handle
    };

    // The first attach builds the cache, then takes its runtime away.
    let first_dir = TempDir::new("cache-runtime-first");
    let first = attach(&first_dir.c_path());
    // SAFETY: attached above and not yet detached.
    unsafe { moraine_detach(first) };

    // A different store, so this reads through the cache rather than
    // answering from anything the first attach left in memory.
    let second_dir = TempDir::new("cache-runtime-second");
    seed(second_dir.path());
    let second = attach(&second_dir.c_path());

    // Any read reaches the cache; a snapshot is the cheapest.
    let mut snapshot: *mut MoraineSnapshotHandle = ptr::null_mut();
    let mut err = MoraineError::default();
    // SAFETY: `second` is attached; the out-params are valid slots.
    let code = unsafe {
        moraine_snapshot(
            second,
            &raw mut snapshot,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    assert_eq!(code, codes::OK, "the second attach could not read");
    // SAFETY: taken from the call above, not yet freed.
    unsafe { moraine_snapshot_free(snapshot) };
    // SAFETY: attached above and not yet detached.
    unsafe { moraine_detach(second) };
}

/// Seeds a catalog with table `main.orders` (one `BIGINT` column) and
/// four inlined rows (ids `0..4`), no data files — a
/// `moraine_locate_row_positions`/`moraine_commit_located_deletion`
/// round trip needs no `DATA_PATH` store when every row is inlined.
fn seed_inline_table(dir: &Path) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test setup: build tokio runtime");
    rt.block_on(async {
        let store =
            Arc::new(LocalFileSystem::new_with_prefix(dir).expect("test setup: open local store"));
        let catalog = moraine::Catalog::open(store, moraine::CatalogOptions::default())
            .await
            .expect("test setup: open catalog");
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
                let table = tx.create_table(
                    schema,
                    "orders",
                    &[ColumnDef {
                        name: "a".into(),
                        column_type: "BIGINT".into(),
                        nulls_allowed: false,
                        default_value: None,
                        children: Vec::new(),
                    }],
                )?;
                tx.inline_insert(
                    table,
                    &moraine::InlineChunk {
                        schema_version: 0,
                        arrow_schema: b"schema-v0".to_vec(),
                        arrow_body: b"rows".to_vec(),
                        row_count: 4,
                    },
                    &[],
                )?;
                Ok(())
            })
            .await
            .expect("test setup: commit fixtures");
        catalog.close().await.expect("test setup: close catalog");
    });
}

/// One `moraine_locate_row_positions` call's raw outputs, owned until
/// [`Self::free`] runs.
struct LocateOutcome {
    code: i32,
    err: MoraineError,
    files: *mut MoraineLocatedFile,
    files_len: usize,
    inlined: *mut u64,
    inlined_len: usize,
    write_directory: *mut c_char,
}

impl LocateOutcome {
    /// SAFETY: `handle` is attached; `schema`/`table` are valid C
    /// strings; `pairs` is a valid slice for the call.
    unsafe fn call(
        handle: *mut MoraineCatalogHandle,
        schema: &CString,
        table: &CString,
        pairs: &[MorainePositionPair],
    ) -> Self {
        let mut outcome = Self {
            code: codes::OK,
            err: MoraineError::default(),
            files: ptr::null_mut(),
            files_len: 0,
            inlined: ptr::null_mut(),
            inlined_len: 0,
            write_directory: ptr::null_mut(),
        };
        // SAFETY: caller contract above; every out-slot is a valid,
        // freshly initialized local.
        outcome.code = unsafe {
            moraine_locate_row_positions(
                handle,
                ptr::null_mut(),
                schema.as_ptr(),
                table.as_ptr(),
                pairs.as_ptr(),
                pairs.len(),
                &raw mut outcome.files,
                &raw mut outcome.files_len,
                &raw mut outcome.inlined,
                &raw mut outcome.inlined_len,
                &raw mut outcome.write_directory,
                None,
                ptr::null_mut(),
                &raw mut outcome.err,
            )
        };
        outcome
    }

    /// SAFETY: every field holds exactly what [`Self::call`] wrote.
    unsafe fn free(self) {
        // SAFETY: caller contract above.
        unsafe {
            moraine_locate_row_positions_free_files(self.files, self.files_len);
            moraine_locate_row_positions_free_inlined(self.inlined, self.inlined_len);
            moraine_string_free(self.write_directory);
            moraine_error_free(self.err.message);
        }
    }
}

/// `moraine_locate_row_positions`/`moraine_commit_located_deletion`
/// round trip: a NULL-file pair resolves to an inlined row id with no
/// positioned or existing-delete rows, and committing its deletion
/// mints a snapshot and makes the row unlocatable again.
#[test]
fn locate_and_commit_located_deletion_round_trip_an_inlined_row() {
    let dir = TempDir::new("located-deletion");
    seed_inline_table(dir.path());
    let handle = attach_ok(dir.path());

    let schema = CString::new("main").expect("no NUL");
    let table = CString::new("orders").expect("no NUL");
    let pairs = [MorainePositionPair {
        row_id: 0,
        data_file_id: 0,
        has_data_file_id: false,
    }];

    // SAFETY: `handle` is attached; `schema`/`table`/`pairs` are valid.
    let outcome = unsafe { LocateOutcome::call(handle, &schema, &table, &pairs) };
    // SAFETY: `outcome.err.message` is null or was just written.
    let err_message = unsafe { outcome.err.message.as_ref() };
    assert_eq!(outcome.code, codes::OK, "locate failed: {err_message:?}");
    assert_eq!(outcome.files_len, 0, "row 0 names no file to position");
    assert!(
        outcome.write_directory.is_null(),
        "no positioned file needs a write directory"
    );
    // SAFETY: on success `inlined`/`inlined_len` describe a valid slice.
    let inlined_rows = unsafe { std::slice::from_raw_parts(outcome.inlined, outcome.inlined_len) };
    assert_eq!(inlined_rows, [0], "row 0 resolves as a live inlined row");
    // SAFETY: every field is exactly what the call wrote.
    unsafe { outcome.free() };

    let mut snapshot_id: u64 = 0;
    let mut err = MoraineError::default();
    // SAFETY: `handle` is attached; the C strings and `out_snapshot_id`
    // slot are valid for the call; no registrations to position.
    let code = unsafe {
        moraine_commit_located_deletion(
            handle,
            schema.as_ptr(),
            table.as_ptr(),
            ptr::null(),
            0,
            [0_u64].as_ptr(),
            1,
            &raw mut snapshot_id,
            None,
            ptr::null_mut(),
            &raw mut err,
        )
    };
    // SAFETY: `err.message` is null or was just written by the call above.
    let err_message = unsafe { err.message.as_ref() };
    assert_eq!(code, codes::OK, "commit failed: {err_message:?}");
    assert!(snapshot_id > 0, "the commit must mint a snapshot");

    // The row is gone: locating it again fails rather than reporting
    // it inlined.
    // SAFETY: as the first `LocateOutcome::call` above.
    let outcome = unsafe { LocateOutcome::call(handle, &schema, &table, &pairs) };
    assert_ne!(
        outcome.code,
        codes::OK,
        "the deleted row must no longer locate"
    );
    assert!(
        outcome.write_directory.is_null(),
        "a failed call writes no output"
    );
    // SAFETY: every field is exactly what the call wrote; a failed call
    // wrote a non-null message.
    unsafe { outcome.free() };

    // SAFETY: `handle` was minted by `attach_ok`, detached once.
    unsafe { moraine_detach(handle) };
}
