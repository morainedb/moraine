use object_store::memory::InMemory;

use super::*;
use crate::{
    Catalog, CatalogOptions, MigrationRequest,
    store::{key::SysKey, proto::FormatValue},
};

fn deletion(id: u64, file: u64, ended: bool) -> StagedWrite {
    (
        Key::current(EntityKey::DeleteFile {
            table_id: 1,
            delete_file_id: id,
        })
        .encode(),
        Some(value::encode_value(&DeleteFileValue {
            table_id: 1,
            delete_file_id: id,
            data_file_id: file,
            end_snapshot: ended.then_some(2),
            ..Default::default()
        })),
    )
}

async fn ids(catalog: &Catalog, file: u64) -> Vec<u64> {
    let read = catalog.begin_read().await.unwrap();
    let mut cursor = DeleteCursor::open(read.handle(), 1, file).await.unwrap();
    let mut ids = Vec::new();
    while let Some((row, _memory)) = cursor.next().await.unwrap() {
        ids.push(row.delete_file_id);
    }
    drop(cursor);
    read.finish();
    ids
}

#[tokio::test]
async fn target_entries_follow_replacement_end_and_hard_delete() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit_own_rows_durably(vec![
            deletion(1, 10, false),
            deletion(2, 10, false),
            deletion(3, 20, false),
        ])
        .await
        .unwrap();
    assert_eq!(ids(&catalog, 10).await, [1, 2]);
    let old = catalog.begin_read().await.unwrap();
    catalog
        .commit_own_rows_durably(vec![
            deletion(1, 20, false),
            deletion(1, 30, false),
            deletion(2, 10, true),
            (deletion(3, 20, false).0, None),
        ])
        .await
        .unwrap();
    assert!(ids(&catalog, 10).await.is_empty());
    assert!(ids(&catalog, 20).await.is_empty());
    assert_eq!(ids(&catalog, 30).await, [1]);
    let mut cursor = DeleteCursor::open(old.handle(), 1, 10).await.unwrap();
    assert_eq!(cursor.next().await.unwrap().unwrap().0.delete_file_id, 1);
    assert_eq!(cursor.next().await.unwrap().unwrap().0.delete_file_id, 2);
    assert!(cursor.next().await.unwrap().is_none());
    drop(cursor);
    old.finish();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn legacy_targets_scan_and_migration_resumes_between_batches() {
    let store = Arc::new(InMemory::new());
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let write = catalog.begin_write_tx().unwrap();
    write
        .put(
            Key::Sys(SysKey::Format).encode(),
            value::encode_value(&FormatValue {
                format_version: 11,
                writer_version: "legacy".into(),
                commit_log_batch_origin: None,
            }),
        )
        .unwrap();
    for id in 1..=600 {
        let (key, bytes) = deletion(id, id % 3, id == 600);
        write.put(key, bytes.unwrap()).unwrap();
    }
    catalog.commit_write_tx(write).await.unwrap();
    assert_eq!(ids(&catalog, 0).await.len(), 199);
    catalog.close().await.unwrap();
    crate::fault::inject_crash(Some(crate::fault::CrashPoint::AfterStep));
    assert!(
        Catalog::migrate(
            store.clone(),
            CatalogOptions::default(),
            MigrationRequest::default()
        )
        .await
        .is_err()
    );
    assert!(
        Catalog::open(store.clone(), CatalogOptions::default())
            .await
            .is_err()
    );
    let report = Catalog::migrate(
        store.clone(),
        CatalogOptions::default(),
        MigrationRequest::default(),
    )
    .await
    .unwrap();
    assert!(report.resumed);
    assert_eq!((report.from_format, report.to_format), (11, 12));
    let reader = Catalog::open_read_only(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let scope = reader.index_read_scope().await.unwrap();
    let read = scope.reads().begin_read().await.unwrap();
    assert_eq!(
        read::read_format(read.handle())
            .await
            .unwrap()
            .unwrap()
            .format_version,
        12,
        "read-only scopes must acquire the migrated version without reopening a writer"
    );
    read.finish();
    drop(scope);
    reader.close().await.unwrap();
    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    assert_eq!(
        ids(&catalog, 0).await,
        (3..600).step_by(3).collect::<Vec<_>>()
    );
    assert_eq!(ids(&catalog, 1).await.len(), 200);
    assert_eq!(ids(&catalog, 2).await.len(), 200);
    catalog.close().await.unwrap();
}
