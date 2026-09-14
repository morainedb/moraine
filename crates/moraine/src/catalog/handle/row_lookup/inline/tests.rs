use std::{cell::Cell, sync::Arc};

use object_store::memory::InMemory;

use crate::{
    Catalog, CatalogOptions, ColumnDef, InlineChunk, TableId,
    store::{
        handle::ReadHandle,
        key::{InlineKey, InlineOperation, Key, SysKey},
        value,
    },
    transaction::commit,
};

async fn fixture() -> (Arc<InMemory>, TableId) {
    let store = Arc::new(InMemory::new());
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let table = Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            let id = tx.create_table(
                schema,
                "inline_lookup",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    ..Default::default()
                }],
            )?;
            table.set(Some(id));
            for body in [b"first".to_vec(), b"second".to_vec(), b"third".to_vec()] {
                tx.inline_insert(
                    id,
                    &InlineChunk {
                        schema_version: 0,
                        row_count: 2,
                        arrow_schema: b"schema".to_vec(),
                        arrow_body: body,
                    },
                    &[],
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    let table = table.get().unwrap();
    catalog
        .commit(|tx| tx.inline_delete(table, 2, &[]))
        .await
        .unwrap();
    catalog.close().await.unwrap();
    (store, table)
}

/// A commit that writes no inline key of a table leaves the writer's
/// inline directory for it in place; one that does rebuilds it.
#[tokio::test]
async fn a_writer_rebuilds_an_inline_directory_only_when_its_table_changes() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let chunk = InlineChunk {
        schema_version: 0,
        row_count: 2,
        arrow_schema: b"schema".to_vec(),
        arrow_body: b"body".to_vec(),
    };
    let tables = Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            let column = ColumnDef {
                name: "a".into(),
                column_type: "BIGINT".into(),
                ..Default::default()
            };
            let looked_up = tx.create_table(schema, "looked_up", std::slice::from_ref(&column))?;
            let unrelated = tx.create_table(schema, "unrelated", std::slice::from_ref(&column))?;
            tx.inline_insert(looked_up, &chunk, &[])?;
            tables.set(Some((looked_up, unrelated)));
            Ok(())
        })
        .await
        .unwrap();
    let (looked_up, unrelated) = tables.get().unwrap();
    let builds = || catalog.row_lookups.inline_directory_builds();

    assert!(catalog.recent_row(looked_up, 1).await.unwrap().is_some());
    assert_eq!(builds(), 1);

    catalog
        .commit(|tx| tx.inline_insert(unrelated, &chunk, &[]).map(|_| ()))
        .await
        .unwrap();
    assert!(catalog.recent_row(looked_up, 1).await.unwrap().is_some());
    assert_eq!(builds(), 1, "an unrelated commit rebuilt the directory");

    catalog
        .commit(|tx| tx.inline_delete(looked_up, 1, &[]))
        .await
        .unwrap();
    assert!(catalog.recent_row(looked_up, 1).await.unwrap().is_none());
    assert_eq!(builds(), 2, "a delete on the table kept a stale directory");
    catalog.close().await.unwrap();
}

/// Rows requested together resolve their deletions at the head and at an
/// earlier snapshot, with unrequested deleted rows between them. The
/// fixture holds rows 0 to 5 with row 2 deleted.
#[tokio::test]
async fn requested_rows_resolve_deletions_across_skipped_tombstones() {
    let (store, table) = fixture().await;
    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    let before_deletes = catalog.snapshot().await.unwrap().snapshot.snapshot_id;
    for row in [3, 4] {
        catalog
            .commit(|tx| tx.inline_delete(table, row, &[]))
            .await
            .unwrap();
    }

    let requested = [0, 1, 3, 4, 5, 2, 99];
    let live = catalog
        .requested_inline_row_ids(table, &requested, None)
        .await
        .unwrap();
    let mut live: Vec<_> = live.into_iter().collect();
    live.sort_unstable();
    assert_eq!(live, vec![0, 1, 5]);

    let live = catalog
        .requested_inline_row_ids(table, &requested, Some(before_deletes))
        .await
        .unwrap();
    let mut live: Vec<_> = live.into_iter().collect();
    live.sort_unstable();
    assert_eq!(live, vec![0, 1, 3, 4, 5]);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn manifest_reader_caches_only_a_stable_inline_directory() {
    let (store, table) = fixture().await;
    let reader = Catalog::open_read_only(store, CatalogOptions::default())
        .await
        .unwrap();
    for _ in 0..2 {
        let row = reader.recent_row(table, 3).await.unwrap().unwrap();
        assert_eq!(row.offset_in_chunk, 1);
        assert_eq!(row.chunk_body.as_slice(), b"second");
        assert!(reader.recent_row(table, 2).await.unwrap().is_none());
        assert!(reader.recent_row(table, 999).await.unwrap().is_none());
    }
    assert!(super::super::lookup(&reader.row_lookups.inline, table).is_some());
    reader.close().await.unwrap();
}
async fn remove_middle_chunk(
    writer: &Catalog,
    session: &crate::store::handle::ReadSession,
    table: TableId,
) {
    let tx = writer.begin_write_tx().await.unwrap();
    let mut head = commit::read_head_value(ReadHandle::Tx(&tx)).await.unwrap();
    head.batch_seq += 1;
    tx.delete(
        Key::Inline(InlineKey::Live(InlineOperation::Insert {
            table_id: table.get(),
            schema_version: 0,
            begin_snapshot: 1,
            chunk_seq: 1,
        }))
        .encode(),
    )
    .unwrap();
    tx.delete(
        Key::Inline(InlineKey::ChunkLocator {
            table_id: table.get(),
            row_id_end: 3,
            schema_version: 0,
            begin_snapshot: 1,
            chunk_seq: 1,
        })
        .encode(),
    )
    .unwrap();
    tx.put(Key::Sys(SysKey::Head).encode(), value::encode_value(&head))
        .unwrap();
    tx.commit().await.unwrap();
    writer.close().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while commit::read_head_value(session.handle()).await.unwrap() != head {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn manifest_lookup_retries_a_chunk_removed_by_a_maintenance_batch() {
    let (store, table) = fixture().await;
    let options = CatalogOptions {
        reader_poll_interval: std::time::Duration::from_millis(10),
        ..Default::default()
    };
    let reader = Catalog::open_read_only(store.clone(), options.clone())
        .await
        .unwrap();
    assert!(reader.recent_row(table, 3).await.unwrap().is_some());
    let writer = Catalog::open(store, options).await.unwrap();
    let session = reader.begin_read().await.unwrap();
    let attempts = Cell::new(0);
    let rows = reader
        .lookup_inline(
            session.handle(),
            None,
            table,
            &[3],
            None,
            async |source, rows| {
                attempts.set(attempts.get() + 1);
                if attempts.get() == 1 {
                    remove_middle_chunk(&writer, &session, table).await;
                }
                let (rows, _) = source.resolve_chunks(session.handle(), table, rows).await?;
                Ok(rows)
            },
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
    assert_eq!(attempts.get(), 2);
    session.finish();
    reader.close().await.unwrap();
}
#[tokio::test]
async fn consistent_read_retries_errors_from_a_changed_manifest() {
    let (store, table) = fixture().await;
    let options = CatalogOptions {
        reader_poll_interval: std::time::Duration::from_millis(10),
        ..Default::default()
    };
    let reader = Catalog::open_read_only(store.clone(), options.clone())
        .await
        .unwrap();
    assert!(reader.recent_row(table, 3).await.unwrap().is_some());
    let writer = Catalog::open(store, options).await.unwrap();
    let session = reader.begin_read().await.unwrap();
    let attempts = Cell::new(0);
    let rows = crate::store::read::consistent(session.handle(), || async {
        let head = commit::read_head_value(session.handle()).await?;
        let (source, rows, _) = reader
            .requested_inline_rows(session.handle(), table, &[3], head, None)
            .await?;
        attempts.set(attempts.get() + 1);
        if attempts.get() == 1 {
            remove_middle_chunk(&writer, &session, table).await;
        }
        let (rows, _) = source.resolve_chunks(session.handle(), table, rows).await?;
        Ok(rows)
    })
    .await
    .unwrap();
    assert!(rows.is_empty());
    assert_eq!(attempts.get(), 2);
    session.finish();
    reader.close().await.unwrap();
}
#[tokio::test]
async fn full_inline_scan_retries_after_maintenance_removes_selected_rows() {
    let (store, table) = fixture().await;
    let options = CatalogOptions {
        reader_poll_interval: std::time::Duration::from_millis(10),
        ..Default::default()
    };
    let reader = Catalog::open_read_only(store.clone(), options.clone())
        .await
        .unwrap();
    let writer = Catalog::open(store, options).await.unwrap();
    let session = reader.begin_read().await.unwrap();
    let attempts = Cell::new(0);
    let rows = reader
        .with_inline_rows(session.handle(), table, async |source, rows| {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1 {
                remove_middle_chunk(&writer, &session, table).await;
            }
            let live = crate::catalog::inline::InlineScanKind::Table.select(&rows, 2, 0);
            let (live, chunks) = source.resolve_chunks(session.handle(), table, live).await?;
            reader
                .recent_rows_from_chunks(session.handle(), table, live, chunks)
                .await
        })
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.row_id).collect::<Vec<_>>(),
        vec![0, 1, 4, 5]
    );
    assert_eq!(attempts.get(), 2);
    session.finish();
    reader.close().await.unwrap();
}

#[tokio::test]
async fn a_manifest_that_moves_on_every_pass_exhausts_the_read_budget() {
    let (store, _) = fixture().await;
    let options = CatalogOptions {
        reader_poll_interval: std::time::Duration::from_millis(10),
        ..Default::default()
    };
    let reader = Catalog::open_read_only(store.clone(), options.clone())
        .await
        .unwrap();
    let session = reader.begin_read().await.unwrap();
    let attempts = Cell::new(0);
    let result = crate::store::read::consistent(session.handle(), || async {
        attempts.set(attempts.get() + 1);
        let writer = Catalog::open(store.clone(), options.clone()).await?;
        let tx = writer.begin_write_tx().await?;
        let mut head = commit::read_head_value(ReadHandle::Tx(&tx)).await?;
        head.batch_seq += 1;
        tx.put(Key::Sys(SysKey::Head).encode(), value::encode_value(&head))?;
        tx.commit().await?;
        writer.close().await?;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while commit::read_head_value(session.handle()).await.unwrap() != head {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        Ok(())
    })
    .await;
    assert!(matches!(result, Err(crate::Error::RetryBudgetExhausted(_))));
    assert_eq!(attempts.get(), 8);

    attempts.set(0);
    let result: crate::Result<()> = crate::store::read::consistent(session.handle(), || async {
        attempts.set(attempts.get() + 1);
        Err(crate::Error::Corruption("stable failure".into()))
    })
    .await;
    assert!(
        matches!(result, Err(crate::Error::Corruption(message)) if message == "stable failure")
    );
    assert_eq!(attempts.get(), 1);
    session.finish();
    reader.close().await.unwrap();
}
