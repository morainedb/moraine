//! Exact inline-version deletion preserves other rows and their lifetimes.

use super::*;

async fn insert(catalog: &Catalog, snapshot: u64, start: u64, values: &[i64]) {
    let (schema, batch) = bigint_batch(values);
    let mut tx = StagedTransaction::begin_detached(catalog, catalog.begin_write_tx().unwrap());
    tx.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 0,
        arrow_schema: inline_schema_ipc(&schema),
    });
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: snapshot,
        row_id_start: start,
        row_count: values.len() as u64,
        arrow_body: inline_body(&batch).into(),
    });
    tx.stage(insert_row(
        TableKind::Snapshot,
        snapshot_row(snapshot, 0, 1),
    ));
    tx.stage(insert_row(
        TableKind::SnapshotChanges,
        snapshot_changes_row(snapshot, "inlined_insert:1"),
    ));
    tx.commit().await.unwrap();
}

async fn versions(catalog: &Catalog) -> Vec<(u64, u64, Option<u64>, i64)> {
    let (rows, chunks) = catalog
        .select_inline_rows(1, InlineScanKind::ForFlush, u64::MAX, 0, None)
        .await
        .unwrap();
    let (schema, _) = bigint_batch(&[]);
    let mut result = Vec::new();
    for row in rows {
        let entries = crate::data_file::inline_batch_entries(
            Arc::new(schema.clone()),
            &chunks[row.chunk].1,
            &[0],
            row.row_id - row.offset_in_chunk,
            None,
        )
        .unwrap();
        let value = &entries[usize::try_from(row.offset_in_chunk).unwrap()].values[0];
        let Some(crate::store::index_encoding::IndexKeyValue::Int { value, .. }) = value else {
            panic!("expected integer: {value:?}")
        };
        result.push((
            row.row_id,
            row.begin_snapshot,
            row.end_snapshot,
            i64::try_from(*value).unwrap(),
        ));
    }
    result.sort_unstable();
    result
}

#[tokio::test]
async fn pruning_splits_a_chunk_without_removing_other_versions() {
    let catalog = open().await;
    insert(&catalog, 1, 0, &[10, 11, 12, 13, 14]).await;
    insert(&catalog, 2, 1, &[21]).await;
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap());
    tx.stage(RowOperation::InlineRowsDelete {
        table_id: 1,
        schema_version: 0,
        rows: vec![(1, 1), (3, 1)],
    });
    tx.commit().await.unwrap();
    let rows = versions(&catalog).await;
    assert_eq!(
        rows,
        vec![
            (0, 1, None, 10),
            (1, 2, None, 21),
            (2, 1, None, 12),
            (4, 1, None, 14)
        ]
    );
}

async fn prune(catalog: &Catalog, schema_version: u64, rows: Vec<(u64, u64)>) {
    let mut tx = StagedTransaction::begin_detached(catalog, catalog.begin_write_tx().unwrap());
    tx.stage(RowOperation::InlineRowsDelete {
        table_id: 1,
        schema_version,
        rows,
    });
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn repeated_pruning_preserves_locators_and_deduplicates_selections() {
    let catalog = open().await;
    insert(&catalog, 1, 0, &[10, 11, 12, 13, 14]).await;
    // Populate the directory before rewriting its chunks.
    assert_eq!(versions(&catalog).await.len(), 5);
    prune(&catalog, 0, vec![(1, 1), (3, 1), (1, 1)]).await;
    assert_eq!(versions(&catalog).await.len(), 3);
    prune(&catalog, 0, vec![(2, 1)]).await;
    assert_eq!(
        versions(&catalog).await,
        vec![(0, 1, None, 10), (4, 1, None, 14)]
    );
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap());
    tx.stage(RowOperation::InlineFlushDelete {
        table_id: 1,
        schema_version: 0,
        flush_snapshot: 1,
    });
    tx.commit().await.unwrap();
    assert!(versions(&catalog).await.is_empty());
}

#[tokio::test]
async fn missing_inline_version_rolls_back_snapshot_and_row_deletions() {
    let catalog = open().await;
    insert(&catalog, 1, 0, &[10, 11, 12]).await;
    insert(&catalog, 2, 3, &[13]).await;
    let before = versions(&catalog).await;
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap());
    tx.stage(RowOperation::Delete {
        table: TableKind::Snapshot,
        cells: vec![Cell::U64(1)],
    });
    tx.stage(RowOperation::InlineRowsDelete {
        table_id: 1,
        schema_version: 0,
        rows: vec![(1, 1), (99, 1)],
    });
    assert!(
        tx.commit()
            .await
            .unwrap_err()
            .to_string()
            .contains("no inline row version")
    );
    assert_eq!(versions(&catalog).await, before);
    let tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap());
    assert!(
        tx.visible_snapshots()
            .await
            .unwrap()
            .iter()
            .any(|snapshot| snapshot.snapshot_id == 1)
    );
}

#[tokio::test]
async fn exact_deletions_and_prefix_drains_are_combined_before_splitting() {
    let catalog = open().await;
    insert(&catalog, 1, 0, &[10, 11, 12]).await;
    insert(&catalog, 2, 3, &[13, 14, 15]).await;
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap());
    tx.stage(RowOperation::InlineRowsDelete {
        table_id: 1,
        schema_version: 0,
        rows: vec![(1, 1), (4, 2)],
    });
    tx.stage(RowOperation::InlineFlushDelete {
        table_id: 1,
        schema_version: 0,
        flush_snapshot: 1,
    });
    tx.stage(RowOperation::InlineRowsDelete {
        table_id: 1,
        schema_version: 0,
        rows: vec![(4, 2)],
    });
    tx.commit().await.unwrap();
    assert_eq!(
        versions(&catalog).await,
        vec![(3, 2, None, 13), (5, 2, None, 15)]
    );
}

#[tokio::test]
async fn pruning_preserves_tombstones_used_by_another_schema_version() {
    let catalog = open().await;
    insert(&catalog, 1, 0, &[10]).await;
    let (schema, batch) = bigint_batch(&[20]);
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap());
    tx.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 1,
        arrow_schema: inline_schema_ipc(&schema),
    });
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 1,
        begin_snapshot: 2,
        row_id_start: 0,
        row_count: 1,
        arrow_body: inline_body(&batch).into(),
    });
    tx.stage(insert_row(TableKind::Snapshot, snapshot_row(2, 1, 1)));
    tx.stage(insert_row(
        TableKind::SnapshotChanges,
        snapshot_changes_row(2, "inlined_insert:1"),
    ));
    tx.commit().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap());
    tx.stage(RowOperation::InlineInlineDelete {
        table_id: 1,
        row_id: 0,
        end_snapshot: 3,
    });
    tx.stage(insert_row(TableKind::Snapshot, snapshot_row(3, 1, 1)));
    tx.stage(insert_row(
        TableKind::SnapshotChanges,
        snapshot_changes_row(3, "inlined_delete:1"),
    ));
    tx.commit().await.unwrap();
    prune(&catalog, 0, vec![(0, 1)]).await;
    assert_eq!(versions(&catalog).await, vec![(0, 2, Some(3), 20)]);
    let read = catalog.begin_write_tx().unwrap();
    assert_eq!(
        store_inline::scan_inline_deletes(ReadHandle::Tx(&read), 1)
            .await
            .unwrap()
            .len(),
        1
    );
    read.rollback();
    prune(&catalog, 1, vec![(0, 2)]).await;
    let read = catalog.begin_write_tx().unwrap();
    assert!(
        store_inline::scan_inline_deletes(ReadHandle::Tx(&read), 1)
            .await
            .unwrap()
            .is_empty()
    );
    read.rollback();
}

#[tokio::test]
async fn splitting_a_chunk_does_not_overwrite_its_neighbors_sequence() {
    let catalog = open().await;
    let (schema, batch) = bigint_batch(&[10, 11, 12, 13, 14]);
    let (_, neighbor) = bigint_batch(&[15, 16]);
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap());
    tx.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 0,
        arrow_schema: inline_schema_ipc(&schema),
    });
    for (start, batch) in [(0, batch), (5, neighbor)] {
        tx.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: start,
            row_count: batch.num_rows() as u64,
            arrow_body: inline_body(&batch).into(),
        });
    }
    tx.stage(insert_row(TableKind::Snapshot, snapshot_row(1, 0, 1)));
    tx.stage(insert_row(
        TableKind::SnapshotChanges,
        snapshot_changes_row(1, "inlined_insert:1"),
    ));
    tx.commit().await.unwrap();
    prune(&catalog, 0, vec![(1, 1), (3, 1)]).await;
    assert_eq!(
        versions(&catalog).await,
        vec![
            (0, 1, None, 10),
            (2, 1, None, 12),
            (4, 1, None, 14),
            (5, 1, None, 15),
            (6, 1, None, 16)
        ]
    );
}
