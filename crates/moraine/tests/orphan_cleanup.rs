//! Orphan deletion plans fence publication and preserve referenced paths.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, DataFile, TableId};
use object_store::memory::InMemory;

fn file(path: &str) -> DataFile {
    DataFile {
        path: path.into(),
        path_is_relative: false,
        file_format: "parquet".into(),
        record_count: 1,
        file_size_bytes: 10,
        footer_size: 4,
        encryption_key: None,
        partition_values: vec![],
        column_stats: vec![],
    }
}

#[allow(clippy::unwrap_used)]
async fn setup() -> (Arc<InMemory>, Catalog, TableId) {
    let objects = Arc::new(InMemory::new());
    let catalog = Catalog::open(objects.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(async |tx| {
            let schema = tx.schema_by_name("main").await?.unwrap().id;
            tx.create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "i".into(),
                    column_type: "BIGINT".into(),
                    ..Default::default()
                }],
            )
            .await?;
            Ok(())
        })
        .await
        .unwrap();
    let view = catalog.read_scope().await.unwrap();
    let schema = view.schema_by_name("main").await.unwrap().unwrap().id;
    let table = view.table_by_name(schema, "t").await.unwrap().unwrap().id;
    (objects, catalog, table)
}

#[tokio::test]
async fn deletion_plan_prevents_publication_and_survives_reopening() {
    let (objects, catalog, table) = setup().await;
    let path = "s3://bucket/data/abandoned.parquet".to_string();
    assert_eq!(
        catalog
            .plan_orphan_deletion(std::slice::from_ref(&path))
            .await
            .unwrap(),
        vec![path.clone()]
    );
    let error = catalog
        .commit(async |tx| {
            tx.register_data_file(table, file(&path), &[]).await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("orphan deletion"), "{error}");
    catalog.close().await.unwrap();
    let reopened = Catalog::open(objects, CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        reopened
            .commit(async |tx| {
                tx.register_data_file(table, file(&path), &[]).await?;
                Ok(())
            })
            .await
            .is_err()
    );
    assert_eq!(
        reopened
            .plan_orphan_deletion(std::slice::from_ref(&path))
            .await
            .unwrap(),
        vec![path]
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn publication_before_the_plan_preserves_the_file() {
    let (_, catalog, table) = setup().await;
    let path = "s3://bucket/data/published.parquet".to_string();
    catalog
        .commit(async |tx| {
            tx.register_data_file(table, file(&path), &[]).await?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        catalog
            .plan_orphan_deletion(&[path])
            .await
            .unwrap()
            .is_empty()
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn remote_aliases_cannot_publish_a_path_reserved_for_deletion() {
    let (_, catalog, table) = setup().await;
    catalog
        .plan_orphan_deletion(&["s3://bucket/data/abandoned.parquet".into()])
        .await
        .unwrap();
    assert!(
        catalog
            .commit(async |tx| {
                tx.register_data_file(
                    table,
                    file("s3a://bucket/data/abandoned.parquet?credential=ignored"),
                    &[],
                )
                .await?;
                Ok(())
            })
            .await
            .is_err()
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn historical_files_are_not_orphans() {
    let (_, catalog, table) = setup().await;
    let path = "s3://bucket/data/historical.parquet".to_string();
    catalog
        .commit(async |tx| {
            tx.register_data_file(table, file(&path), &[]).await?;
            Ok(())
        })
        .await
        .unwrap();
    let scope = catalog.read_scope().await.unwrap();
    let files = scope.data_files_of(table).await.unwrap();
    let id = files[0].id;
    drop(scope);
    catalog
        .commit(async |tx| tx.expire_data_file(table, id).await)
        .await
        .unwrap();
    assert!(
        catalog
            .plan_orphan_deletion(&[path])
            .await
            .unwrap()
            .is_empty()
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn relative_files_and_data_root_changes_honor_reservations() {
    let (_, catalog, table) = setup().await;
    catalog.adopt_data_path("s3://bucket/old/").await.unwrap();
    let mut relative = file("file.parquet");
    relative.path_is_relative = true;
    catalog
        .commit(async |tx| {
            tx.register_data_file(table, relative.clone(), &[]).await?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        catalog
            .plan_orphan_deletion(&["s3://bucket/old/main/t/file.parquet".into()])
            .await
            .unwrap()
            .is_empty()
    );
    catalog
        .plan_orphan_deletion(&["s3://bucket/new/main/t/file.parquet".into()])
        .await
        .unwrap();
    assert!(
        catalog
            .rewrite_data_path("s3://bucket/old/", "s3://bucket/new/")
            .await
            .is_err()
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn delete_file_publication_honors_reservations() {
    let (_, catalog, table) = setup().await;
    catalog
        .commit(async |tx| {
            tx.register_data_file(table, file("s3://bucket/data/live.parquet"), &[])
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    let scope = catalog.read_scope().await.unwrap();
    let id = scope.data_files_of(table).await.unwrap()[0].id;
    drop(scope);
    let path = "s3://bucket/data/delete.parquet".to_string();
    catalog
        .plan_orphan_deletion(std::slice::from_ref(&path))
        .await
        .unwrap();
    assert!(
        catalog
            .commit(async |tx| {
                tx.register_delete_file(
                    table,
                    moraine::DeleteFile {
                        data_file_id: id,
                        path: path.clone(),
                        path_is_relative: false,
                        format: "parquet".into(),
                        delete_count: 1,
                        file_size_bytes: 10,
                        footer_size: 4,
                        encryption_key: None,
                    },
                    &[],
                )
                .await?;
                Ok(())
            })
            .await
            .is_err()
    );
    catalog.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn deleting_a_symlink_cannot_change_the_reserved_identity() {
    let (_, catalog, table) = setup().await;
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("target.parquet");
    let alias = directory.path().join("alias.parquet");
    std::fs::write(&target, b"bytes").unwrap();
    std::os::unix::fs::symlink(&target, &alias).unwrap();
    let path = alias.to_str().unwrap().to_string();
    catalog
        .plan_orphan_deletion(std::slice::from_ref(&path))
        .await
        .unwrap();
    std::fs::remove_file(&alias).unwrap();
    assert!(
        catalog
            .commit(async |tx| {
                tx.register_data_file(table, file(&path), &[]).await?;
                Ok(())
            })
            .await
            .is_err()
    );
    catalog.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_broken_symlink_still_checks_its_deleted_target() {
    let (_, catalog, table) = setup().await;
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("target.parquet");
    let alias = directory.path().join("alias.parquet");
    std::fs::write(&target, b"bytes").unwrap();
    std::os::unix::fs::symlink(&target, &alias).unwrap();
    catalog
        .plan_orphan_deletion(&[target.to_str().unwrap().into()])
        .await
        .unwrap();
    std::fs::remove_file(&target).unwrap();
    assert!(
        catalog
            .commit(async |tx| {
                tx.register_data_file(table, file(alias.to_str().unwrap()), &[])
                    .await?;
                Ok(())
            })
            .await
            .is_err()
    );
    catalog.close().await.unwrap();
}
