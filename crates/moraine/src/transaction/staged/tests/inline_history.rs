//! Inline version lifetimes across updates, deletion, and flush.

use super::*;

async fn commit_inline(catalog: &Catalog, snapshot: u64, operations: Vec<RowOperation>) {
    let mut tx =
        StagedTransaction::begin_detached(catalog, catalog.begin_write_tx().await.unwrap());
    for operation in operations {
        tx.stage(operation);
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(snapshot, 0, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(snapshot, "inlined_insert:1,inlined_delete:1"),
    });
    tx.commit().await.unwrap();
}

fn insertion(snapshot: u64) -> RowOperation {
    RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: snapshot,
        row_id_start: 0,
        row_count: 1,
        arrow_body: snapshot.to_be_bytes().to_vec(),
    }
}

fn deletion(snapshot: u64) -> RowOperation {
    RowOperation::InlineInlineDelete {
        table_id: 1,
        row_id: 0,
        end_snapshot: snapshot,
    }
}

#[tokio::test]
async fn repeated_inline_updates_preserve_each_versions_lifetime() {
    let catalog = open().await;
    commit_inline(&catalog, 1, vec![insertion(1)]).await;
    commit_inline(&catalog, 2, vec![deletion(2), insertion(2)]).await;
    commit_inline(&catalog, 3, vec![deletion(3), insertion(3)]).await;
    commit_inline(&catalog, 4, vec![deletion(4)]).await;

    // The first pass verifies the directory; the second reads its locators.
    for _ in 0..2 {
        for snapshot in 1..=3 {
            let (rows, chunks) = catalog
                .select_inline_rows(1, InlineScanKind::Table, snapshot, 0, None)
                .await
                .unwrap();
            assert_eq!(rows.len(), 1, "snapshot {snapshot}");
            assert_eq!(rows[0].begin_snapshot, snapshot);
            assert_eq!(rows[0].end_snapshot, Some(snapshot + 1));
            assert_eq!(
                chunks[rows[0].chunk].1.body.as_ref(),
                snapshot.to_be_bytes()
            );

            let (deleted, _) = catalog
                .select_inline_rows(
                    1,
                    InlineScanKind::Deletions,
                    snapshot + 1,
                    snapshot + 1,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(deleted.len(), 1);
            assert_eq!(deleted[0].begin_snapshot, snapshot);
        }
        let (head, _) = catalog
            .select_inline_rows(1, InlineScanKind::Table, 4, 0, None)
            .await
            .unwrap();
        assert!(head.is_empty());
    }
}

#[tokio::test]
async fn an_inline_versions_end_can_precede_its_next_inline_version() {
    let catalog = open().await;
    for (snapshot, operations) in [
        (1, vec![insertion(1)]),
        (2, vec![deletion(2)]),
        (3, vec![insertion(3)]),
        (4, vec![deletion(4), insertion(4)]),
    ] {
        commit_inline(&catalog, snapshot, operations).await;
    }

    let (rows, _) = catalog
        .select_inline_rows(1, InlineScanKind::Table, 2, 0, None)
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "a row moved out of inline storage stays absent"
    );
}

#[tokio::test]
async fn flushing_one_schema_preserves_the_other_versions_tombstone() {
    let catalog = open().await;
    commit_inline(&catalog, 1, vec![insertion(1)]).await;
    commit_inline(&catalog, 2, vec![deletion(2), insertion(2)]).await;
    let mut newer_schema = insertion(3);
    if let RowOperation::InlineInsert { schema_version, .. } = &mut newer_schema {
        *schema_version = 1;
    }
    commit_inline(&catalog, 3, vec![deletion(3), newer_schema]).await;
    commit_inline(&catalog, 4, vec![deletion(4)]).await;

    for (snapshot, schema_version, expected) in [(5, 0, vec![4]), (6, 1, vec![])] {
        commit_inline(
            &catalog,
            snapshot,
            vec![RowOperation::InlineFlushDelete {
                table_id: 1,
                schema_version,
                flush_snapshot: 4,
            }],
        )
        .await;
        let read = catalog.begin_write_tx().await.unwrap();
        let ends: Vec<u64> = store_inline::scan_inline_deletes(ReadHandle::Tx(&read), 1)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, value)| value.end_snapshot)
            .collect();
        read.rollback();
        assert_eq!(ends, expected);
        let (head, _) = catalog
            .select_inline_rows(1, InlineScanKind::Table, 4, 0, None)
            .await
            .unwrap();
        assert!(head.is_empty());
    }
}

#[tokio::test]
async fn dropping_an_inline_table_removes_its_tombstone_history() {
    let catalog = open().await;
    commit_inline(&catalog, 1, vec![insertion(1)]).await;
    commit_inline(&catalog, 2, vec![deletion(2), insertion(2)]).await;
    commit_inline(&catalog, 3, vec![deletion(3)]).await;
    commit_inline(&catalog, 4, vec![RowOperation::InlineDrop { table_id: 1 }]).await;
    let tx = catalog.begin_write_tx().await.unwrap();
    assert!(
        store_inline::scan_inline_deletes(ReadHandle::Tx(&tx), 1)
            .await
            .unwrap()
            .is_empty()
    );
    tx.rollback();
}
