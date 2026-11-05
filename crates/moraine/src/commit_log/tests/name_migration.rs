use super::*;
use crate::{
    fault::{CrashPoint, inject_crash},
    store::{
        handle::ReadHandle,
        key::{EntityKey, NameKey, NameKind},
        read,
    },
    transaction::commit,
};

async fn legacy_store(format_version: u64) -> Arc<InMemory> {
    let objects = Arc::new(InMemory::new());
    bootstrapped(objects.clone(), "name-migration").await;
    let (db, _) = StoreBuilder::new("name-migration", objects.clone())
        .open_writer()
        .await
        .unwrap();
    db.put(
        Key::Sys(SysKey::Format).encode(),
        value::encode_value(&proto::FormatValue {
            format_version,
            writer_version: "legacy".into(),
            commit_log_batch_origin: None,
        }),
    )
    .await
    .unwrap();
    db.delete(
        Key::Name(NameKey {
            kind: NameKind::Schema,
            parent: 0,
            name: "main".into(),
            entity_id: 0,
        })
        .encode(),
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    objects
}

#[tokio::test]
async fn name_migration_resumes_through_log_slots_and_preserves_adoption() {
    let objects = legacy_store(9).await;
    let provider = Arc::new(TestProvider::default());
    let options = CatalogOptions {
        commit_log: Some(provider.clone()),
        ..options_at("name-migration")
    };
    let catalog = Catalog::open(objects.clone(), options.clone())
        .await
        .unwrap();
    catalog
        .commit_materialized(|tx| {
            for id in 0..300 {
                tx.create_schema(&format!("schema_{id}"))?;
            }
            Ok(())
        })
        .await
        .unwrap();
    let session = catalog.begin_read().await.unwrap();
    let before = read::read_format(session.handle()).await.unwrap().unwrap();
    assert_eq!(before.format_version, 10);
    session.finish();
    catalog.close().await.unwrap();
    let previous_sequence = provider.folded_sequence();
    inject_crash(Some(CrashPoint::AfterStep));
    let interrupted = Catalog::migrate(
        objects.clone(),
        options.clone(),
        crate::MigrationRequest::default(),
    )
    .await
    .unwrap_err();
    assert!(
        provider.folded_sequence() >= previous_sequence + 2,
        "{interrupted:?}"
    );
    let report = Catalog::migrate(
        objects.clone(),
        options.clone(),
        crate::MigrationRequest::default(),
    )
    .await
    .unwrap();
    assert!(report.resumed);
    assert_eq!(report.to_format, commit::MAX_FORMAT_VERSION);
    let catalog = Catalog::open(objects.clone(), options).await.unwrap();
    let session = catalog.begin_read().await.unwrap();
    let after = read::read_format(session.handle()).await.unwrap().unwrap();
    assert_eq!(
        after.commit_log_batch_origin,
        before.commit_log_batch_origin
    );
    assert!(commit::uses_commit_log(&after));
    for id in [0, 255, 299] {
        assert!(
            crate::transaction::names::lookup(
                session.handle(),
                NameKind::Schema,
                0,
                &format!("schema_{id}")
            )
            .await
            .unwrap()
            .is_some()
        );
    }
    session.finish();
    catalog
        .commit_materialized(|tx| tx.create_schema("after_migration").map(|_| ()))
        .await
        .unwrap();
    let session = catalog.begin_read().await.unwrap();
    assert!(
        crate::transaction::names::lookup(session.handle(), NameKind::Schema, 0, "after_migration")
            .await
            .unwrap()
            .is_some()
    );
    session.finish();
    catalog.close().await.unwrap();
    assert!(
        Catalog::open(objects.clone(), options_at("name-migration"))
            .await
            .is_err()
    );
    let (reader, _) = StoreBuilder::new("name-migration", objects)
        .open_reader()
        .await
        .unwrap();
    assert!(commit::uses_commit_log(
        &read::read_format(ReadHandle::Reader(&reader))
            .await
            .unwrap()
            .unwrap()
    ));
    reader.close().await.unwrap();
}

#[tokio::test]
async fn delete_target_migration_resumes_through_log_slots() {
    use crate::transaction::file_targets::DeleteCursor;

    let objects = legacy_store(11).await;
    let provider = Arc::new(TestProvider::default());
    let options = CatalogOptions {
        commit_log: Some(provider.clone()),
        ..options_at("name-migration")
    };
    let catalog = Catalog::open(objects.clone(), options.clone())
        .await
        .unwrap();
    let writes = (1..=300)
        .map(|id| {
            (
                Key::current(EntityKey::DeleteFile {
                    table_id: 1,
                    delete_file_id: id,
                })
                .encode(),
                Some(value::encode_value(&proto::DeleteFileValue {
                    table_id: 1,
                    delete_file_id: id,
                    data_file_id: id % 2,
                    ..Default::default()
                })),
            )
        })
        .collect();
    catalog.commit_own_rows_durably(writes).await.unwrap();
    let read = catalog.begin_read().await.unwrap();
    let before = read::read_format(read.handle()).await.unwrap().unwrap();
    read.finish();
    catalog.close().await.unwrap();
    let sequence = provider.folded_sequence();
    inject_crash(Some(CrashPoint::AfterStep));
    assert!(
        Catalog::migrate(
            objects.clone(),
            options.clone(),
            crate::MigrationRequest::default()
        )
        .await
        .is_err()
    );
    assert!(provider.folded_sequence() >= sequence + 2);
    let report = Catalog::migrate(
        objects.clone(),
        options.clone(),
        crate::MigrationRequest::default(),
    )
    .await
    .unwrap();
    assert!(report.resumed);
    assert_eq!((report.from_format, report.to_format), (11, 12));
    let reader = Catalog::open_read_only(objects.clone(), options.clone())
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
        12
    );
    read.finish();
    drop(scope);
    reader.close().await.unwrap();
    let catalog = Catalog::open(objects, options).await.unwrap();
    let read = catalog.begin_read().await.unwrap();
    let after = read::read_format(read.handle()).await.unwrap().unwrap();
    assert_eq!(
        after.commit_log_batch_origin,
        before.commit_log_batch_origin
    );
    let mut cursor = DeleteCursor::open(read.handle(), 1, 0).await.unwrap();
    let mut ids = Vec::new();
    while let Some((row, _memory)) = cursor.next().await.unwrap() {
        ids.push(row.delete_file_id);
    }
    assert_eq!(ids, (2..=300).step_by(2).collect::<Vec<_>>());
    drop(cursor);
    read.finish();
    catalog.close().await.unwrap();
}
