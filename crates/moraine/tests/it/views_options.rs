//! Views and options through the public API: versioned view lifecycle,
//! the shared relation namespace, and options committed outside the
//! snapshot protocol — real SlateDB on in-memory object storage.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, Error, OptionScope, SnapshotId};
use object_store::memory::InMemory;

use crate::fixtures::{col, open_memory};

/// A fresh store records the `data_path` passed at open into the global
/// metadata, so a later attach can read the lake's data root back without
/// being told it again. Absent when no data path is given.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn data_path_persisted_at_bootstrap() {
    let store = Arc::new(InMemory::new());

    let mut options = CatalogOptions::default();
    options.data_path = Some("s3://bucket/data".to_string());
    let catalog = Catalog::open(store.clone(), options).await.unwrap();
    assert_eq!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .data_path()
            .await
            .unwrap()
            .as_deref(),
        Some("s3://bucket/data"),
    );

    // It is bootstrap-time state: reopening the same store reads it back,
    // even though the reopen supplies no data path.
    drop(catalog);
    let reopened = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .read_scope()
            .await
            .unwrap()
            .data_path()
            .await
            .unwrap()
            .as_deref(),
        Some("s3://bucket/data"),
    );

    // A store opened without a data path has none.
    let bare = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    assert_eq!(
        bare.read_scope().await.unwrap().data_path().await.unwrap(),
        None
    );
}

#[tokio::test]
async fn views_commit_version_and_time_travel() {
    let catalog = open_memory().await;
    let created = catalog
        .commit(async |tx| {
            let s = tx.create_schema("s").await?;
            tx.create_table(s, "t", &[col("a")]).await?;
            tx.create_view(s, "v", "duckdb", "SELECT 1").await?;
            Ok(())
        })
        .await
        .unwrap();
    let head = catalog.read_scope().await.unwrap();
    let s = head.schema_by_name("s").await.unwrap().unwrap();
    let v = head.view_by_name(s.id, "v").await.unwrap().unwrap();
    assert_eq!(v.sql, "SELECT 1");
    let version_after_create = head.current_snapshot().schema_version;

    let altered = catalog
        .commit(async move |tx| tx.alter_view(v.id, "duckdb", "SELECT 2").await)
        .await
        .unwrap();
    let head = catalog.read_scope().await.unwrap();
    assert_eq!(
        head.view_by_id(v.id).await.unwrap().unwrap().sql,
        "SELECT 2"
    );
    // The old definition is still visible at the pre-alter snapshot.
    let past = catalog.read_scope_at(created).await.unwrap();
    assert_eq!(
        past.view_by_id(v.id).await.unwrap().unwrap().sql,
        "SELECT 1"
    );
    // View DDL is schema-changing.
    assert_eq!(
        head.current_snapshot().schema_version,
        version_after_create + 1
    );

    catalog
        .commit(async move |tx| tx.drop_view(v.id).await)
        .await
        .unwrap();
    let head = catalog.read_scope().await.unwrap();
    assert!(head.view_by_id(v.id).await.unwrap().is_none());
    assert!(head.views_in(s.id).await.unwrap().is_empty());
    let past = catalog.read_scope_at(altered).await.unwrap();
    assert_eq!(
        past.view_by_id(v.id).await.unwrap().unwrap().sql,
        "SELECT 2"
    );
    assert_eq!(
        head.current_snapshot().schema_version,
        version_after_create + 2
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn options_commit_without_minting_snapshots() {
    let catalog = open_memory().await;
    catalog
        .commit(async |tx| {
            let s = tx.create_schema("s").await?;
            tx.create_table(s, "t", &[col("a")]).await?;
            Ok(())
        })
        .await
        .unwrap();

    // Options-only: no snapshot minted, head unchanged.
    let id = catalog
        .commit(async |tx| tx.set_option(OptionScope::Global, "k", "v").await)
        .await
        .unwrap();
    assert_eq!(id, SnapshotId::new(1));
    let err = catalog.read_scope_at(SnapshotId::new(2)).await.unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    let head = catalog.read_scope().await.unwrap();
    assert_eq!(
        head.option(OptionScope::Global, "k")
            .await
            .unwrap()
            .as_deref(),
        Some("v")
    );

    // Unsetting in another options-only commit: still head 1, option gone.
    let id = catalog
        .commit(async |tx| tx.unset_option(OptionScope::Global, "k").await)
        .await
        .unwrap();
    assert_eq!(id, SnapshotId::new(1));
    let head = catalog.read_scope().await.unwrap();
    assert!(
        head.option(OptionScope::Global, "k")
            .await
            .unwrap()
            .is_none()
    );

    // Mixed commit: option rides the snapshot-minting batch.
    let version_before = head.current_snapshot().schema_version;
    let id = catalog
        .commit(async |tx| {
            let s = tx
                .schema_by_name("s")
                .await
                .unwrap()
                .ok_or_else(|| Error::NotFound("schema s".to_string()))?;
            tx.create_table(s.id, "t2", &[col("a")]).await?;
            tx.set_option(OptionScope::Global, "k2", "v2").await
        })
        .await
        .unwrap();
    assert_eq!(id, SnapshotId::new(2));
    let head = catalog.read_scope().await.unwrap();
    assert_eq!(
        head.option(OptionScope::Global, "k2")
            .await
            .unwrap()
            .as_deref(),
        Some("v2")
    );
    assert_eq!(head.current_snapshot().schema_version, version_before + 1);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn option_resolution_reads_through_the_catalog() {
    let catalog = open_memory().await;
    catalog
        .commit(async |tx| {
            let s = tx.create_schema("s").await?;
            tx.create_table(s, "t", &[col("a")]).await?;
            let other = tx.create_schema("other").await?;
            tx.create_table(other, "u", &[col("a")]).await?;
            tx.set_option(OptionScope::Global, "k", "g").await?;
            tx.set_option(OptionScope::Schema(s), "k", "s").await
        })
        .await
        .unwrap();
    let head = catalog.read_scope().await.unwrap();
    let s = head.schema_by_name("s").await.unwrap().unwrap();
    let t = head.table_by_name(s.id, "t").await.unwrap().unwrap();
    assert_eq!(
        head.option(OptionScope::Table(t.id), "k")
            .await
            .unwrap()
            .as_deref(),
        Some("s")
    );
    assert_eq!(
        head.option(OptionScope::Global, "k")
            .await
            .unwrap()
            .as_deref(),
        Some("g")
    );
    // A table in a schema with no override falls through to global.
    let other = head.schema_by_name("other").await.unwrap().unwrap();
    let u = head.table_by_name(other.id, "u").await.unwrap().unwrap();
    assert_eq!(
        head.option(OptionScope::Table(u.id), "k")
            .await
            .unwrap()
            .as_deref(),
        Some("g")
    );

    // The schema's option record dies with the schema.
    catalog
        .commit(async move |tx| {
            tx.drop_table(t.id).await?;
            tx.drop_schema(s.id).await
        })
        .await
        .unwrap();
    let head = catalog.read_scope().await.unwrap();
    assert_eq!(
        head.option(OptionScope::Table(u.id), "k")
            .await
            .unwrap()
            .as_deref(),
        Some("g")
    );
    assert!(head.schema_by_name("s").await.unwrap().is_none());
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_option_writes_are_last_write_wins() {
    let catalog = open_memory().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(async |tx| tx.set_option(OptionScope::Global, "k", "a").await)
            .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(async |tx| tx.set_option(OptionScope::Global, "k", "b").await)
            .await
    });
    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();
    let head = catalog.read_scope().await.unwrap();
    let value = head.option(OptionScope::Global, "k").await.unwrap();
    assert!(matches!(value.as_deref(), Some("a" | "b")));
    // Neither write minted a snapshot.
    assert_eq!(head.current_snapshot().id, SnapshotId::new(0));
    catalog.close().await.unwrap();
}
