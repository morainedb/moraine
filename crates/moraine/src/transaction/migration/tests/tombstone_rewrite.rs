//! Migration of row-level tombstones into immutable deletion events.

use super::*;
use crate::store::key::{InlineKey, InlineOperation};

async fn old_store(format: u64, rows: u64) -> Arc<InMemory> {
    let store = Arc::new(InMemory::new());
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog.close().await.unwrap();
    let db = open_migrator(&store).await;
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    tx.put(
        Key::Sys(SysKey::Format).encode(),
        value::encode_value(&proto::FormatValue {
            format_version: format,
            writer_version: "old-writer".to_owned(),
        }),
    )
    .unwrap();
    for row_id in 0..rows {
        tx.put(
            Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
                table_id: 7,
                row_id,
            }))
            .encode(),
            value::encode_value(&proto::InlineInlineDeleteValue {
                end_snapshot: row_id + 2,
            }),
        )
        .unwrap();
    }
    tx.put(
        Key::Inline(InlineKey::Live(InlineOperation::Insert {
            table_id: 7,
            schema_version: 0,
            begin_snapshot: 1,
            chunk_seq: 0,
        }))
        .encode(),
        value::encode_value(&proto::InlineChunkValue {
            body: b"preserved-row-bodies".to_vec().into(),
            row_id_start: 0,
            row_count: rows,
            data_file_id: None,
        }),
    )
    .unwrap();
    tx.commit()
        .await
        .unwrap()
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    db.close().await.unwrap();
    store
}

async fn assert_rewritten(store: &Arc<InMemory>, rows: u64) {
    let db = open_migrator(store).await;
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    for row_id in 0..rows {
        assert!(
            tx.get(
                Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
                    table_id: 7,
                    row_id
                }))
                .encode()
            )
            .await
            .unwrap()
            .is_none()
        );
        let bytes = tx
            .get(
                Key::Inline(InlineKey::RowTombstone {
                    table_id: 7,
                    row_id,
                    end_snapshot: row_id + 2,
                })
                .encode(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            value::decode_value::<proto::InlineInlineDeleteValue>(&bytes)
                .unwrap()
                .end_snapshot,
            row_id + 2
        );
    }
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn old_formats_migrate_to_versioned_inline_tombstones() {
    for format in [1, 8] {
        let store = old_store(format, 3).await;
        assert!(matches!(
            Catalog::open(store.clone(), CatalogOptions::default()).await,
            Err(Error::Migration(_))
        ));
        let report = Catalog::migrate(
            store.clone(),
            CatalogOptions::default(),
            MigrationRequest::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.from_format, format);
        assert_eq!(report.to_format, 9);
        assert_eq!(report.units_run, ["version-inline-tombstones"]);
        assert_rewritten(&store, 3).await;
        let catalog = Catalog::open(store.clone(), CatalogOptions::default())
            .await
            .unwrap();
        assert_eq!(
            catalog
                .snapshot()
                .await
                .unwrap()
                .current_snapshot()
                .id
                .get(),
            0
        );
        let (rows, chunks) = catalog
            .select_inline_rows(7, crate::catalog::inline::InlineScanKind::Table, 1, 0, None)
            .await
            .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.end_snapshot).collect::<Vec<_>>(),
            [Some(2), Some(3), Some(4)]
        );
        assert_eq!(chunks[0].1.body.as_ref(), b"preserved-row-bodies");
        let (rows, _) = catalog
            .select_inline_rows(7, crate::catalog::inline::InlineScanKind::Table, 2, 0, None)
            .await
            .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.row_id).collect::<Vec<_>>(),
            [1, 2]
        );
        catalog.close().await.unwrap();
        let again = Catalog::migrate(
            store,
            CatalogOptions::default(),
            MigrationRequest::default(),
        )
        .await
        .unwrap();
        assert_eq!(again.from_format, again.to_format);
        assert!(again.units_run.is_empty());
    }
}

#[tokio::test]
async fn interrupted_inline_tombstone_migration_resumes() {
    for point in [
        CrashPoint::AfterStart,
        CrashPoint::AfterStep,
        CrashPoint::BeforeFinish,
        CrashPoint::AfterFinish,
    ] {
        let store = old_store(8, 513).await;
        inject_crash(Some(point));
        let first = Catalog::migrate(
            store.clone(),
            CatalogOptions::default(),
            MigrationRequest::default(),
        )
        .await;
        assert!(first.is_err());
        let (format, marker) = durable_state(&store).await;
        if point != CrashPoint::AfterFinish {
            assert_eq!(format, 8);
            assert!(marker.is_some());
            assert!(matches!(
                Catalog::open(store.clone(), CatalogOptions::default()).await,
                Err(Error::Migration(_))
            ));
        }
        let report = Catalog::migrate(
            store.clone(),
            CatalogOptions::default(),
            MigrationRequest::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.to_format, 9);
        assert_eq!(report.resumed, point != CrashPoint::AfterFinish);
        assert_rewritten(&store, 513).await;
    }
}
