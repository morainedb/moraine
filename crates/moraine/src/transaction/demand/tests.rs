//! Public transactions load only their dependencies and preserve staged reads.

use std::sync::Arc;

use object_store::memory::InMemory;

use crate::{Catalog, CatalogOptions, ColumnDef, Error, OptionScope, Result};

#[tokio::test]
async fn demand_commit_preserves_staged_reads_and_ignores_unrelated_corruption() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let write = catalog.begin_write_tx().unwrap();
    write
        .put(
            crate::store::key::Key::current(crate::store::key::EntityKey::FileColumnStats {
                table_id: 999,
                data_file_id: 999,
                column_id: 1,
            })
            .encode(),
            b"not a record",
        )
        .unwrap();
    catalog.commit_write_tx(write).await.unwrap();
    let snapshot = catalog
        .commit(async |tx| {
            let schema = tx.create_schema("sales").await?;
            assert_eq!(tx.schema_by_name("sales").await?.unwrap().id, schema);
            let table = tx
                .create_table(
                    schema,
                    "orders",
                    &[ColumnDef {
                        name: "id".into(),
                        column_type: "BIGINT".into(),
                        nulls_allowed: false,
                        default_value: None,
                        children: Vec::new(),
                    }],
                )
                .await?;
            tx.rename_table(table, "purchases").await?;
            assert!(tx.table_by_name(schema, "orders").await?.is_none());
            assert_eq!(
                tx.table_by_name(schema, "purchases").await?.unwrap().id,
                table
            );
            tx.set_option(OptionScope::Table(table), "one", "1").await?;
            tx.set_option(OptionScope::Table(table), "two", "2").await?;
            assert_eq!(
                tx.option(OptionScope::Table(table), "one")
                    .await?
                    .as_deref(),
                Some("1")
            );
            Ok(())
        })
        .await
        .unwrap();
    let scope = catalog.read_scope_at(snapshot).await.unwrap();
    let schema = scope.schema_by_name("sales").await.unwrap().unwrap().id;
    let table = scope
        .table_by_name(schema, "purchases")
        .await
        .unwrap()
        .unwrap()
        .id;
    assert!(
        scope
            .table_by_name(schema, "orders")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(scope.columns_of(table).await.unwrap().len(), 1);
    assert_eq!(
        scope
            .option(OptionScope::Table(table), "two")
            .await
            .unwrap()
            .as_deref(),
        Some("2")
    );
    let error: Result<_> = catalog
        .commit(async |tx| {
            tx.create_schema("aborted").await?;
            Err(Error::Constraint("abort".into()))
        })
        .await;
    assert!(error.is_err());
    assert!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .schema_by_name("aborted")
            .await
            .unwrap()
            .is_none()
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn read_scopes_preserve_history_and_unversioned_revision() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let first = catalog
        .commit(async |tx| {
            tx.create_schema("sales").await?;
            tx.set_option(OptionScope::Global, "probe", "before").await
        })
        .await
        .unwrap();
    let pinned = catalog.read_scope().await.unwrap();
    catalog
        .commit(async |tx| {
            let id = tx.schema_by_name("sales").await?.unwrap().id;
            tx.drop_schema(id).await?;
            tx.set_option(OptionScope::Global, "probe", "after").await
        })
        .await
        .unwrap();
    assert!(pinned.schema_by_name("sales").await.unwrap().is_some());
    assert_eq!(
        pinned
            .option(OptionScope::Global, "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("before")
    );
    let historical = catalog.read_scope_at(first).await.unwrap();
    assert!(historical.schema_by_name("sales").await.unwrap().is_some());
    assert_eq!(
        historical
            .option(OptionScope::Global, "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("after")
    );
    assert!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .schema_by_name("sales")
            .await
            .unwrap()
            .is_none()
    );
    drop(pinned);
    drop(historical);
    catalog.close().await.unwrap();
}

#[test]
fn small_reads_commits_and_streaming_export_fit_beside_a_large_catalog() {
    const CHILD: &str = "MORAINE_TEST_DEMAND_SCOPE_MEMORY";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "transaction::demand::tests::small_reads_commits_and_streaming_export_fit_beside_a_large_catalog"])
            .env(CHILD, "1")
            .env("MORAINE_WORKING_MEMORY_BYTES", "67108864")
            .output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
                .await
                .unwrap();
            let sql = format!("SELECT 1 /*{}*/", "x".repeat(8192));
            let version = catalog
                .commit(async |tx| {
                    let schema = tx.schema_by_name("main").await?.unwrap().id;
                    for index in 0..128 {
                        tx.create_view(schema, &format!("view_{index}"), "duckdb", &sql)
                            .await?;
                    }
                    Ok(())
                })
                .await
                .unwrap();
            let current = catalog.read_scope().await.unwrap();
            let historical = catalog.read_scope_at(version).await.unwrap();
            let (used, capacity) = crate::working_memory_usage().unwrap();
            let pressure = crate::MemoryReservation::reserve(capacity - used - 256 * 1024).unwrap();
            for scope in [&current, &historical] {
                let schema = scope.schema_by_name("main").await.unwrap().unwrap().id;
                let mut cursor = scope.scan_views_in(schema).await.unwrap();
                let mut count = 0;
                while let Some(view) = cursor.next().await.unwrap() {
                    assert_eq!(view.sql, sql);
                    count += 1;
                }
                assert_eq!(count, 128);
            }
            catalog
                .commit(async |tx| tx.set_option(OptionScope::Global, "small", "value").await)
                .await
                .unwrap();
            assert_eq!(
                catalog
                    .read_scope()
                    .await
                    .unwrap()
                    .option(OptionScope::Global, "small")
                    .await
                    .unwrap()
                    .as_deref(),
                Some("value")
            );
            drop(pressure);
            drop(current);
            drop(historical);
            catalog.close().await.unwrap();
        });
}
