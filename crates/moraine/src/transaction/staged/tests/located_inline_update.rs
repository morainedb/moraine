//! A located update over rows that are still inlined: one commit
//! tombstones every targeted row and re-inserts it under its preserved
//! id, the shape `moraine_update` stages when its targets have not been
//! flushed yet.

use std::time::Duration;

use super::*;

/// Four 1,000-row chunks, one per commit, so the targets span several
/// chunks the way an hour of small commits does.
async fn inline_four_chunks(catalog: &Catalog) {
    for (chunk, snapshot) in (3..=6).enumerate() {
        let start = u64::try_from(chunk * 1_000).unwrap();
        let values: Vec<i64> = (0..1_000)
            .map(|offset| i64::try_from(start).unwrap() + offset)
            .collect();
        inline_insert(catalog, snapshot, start, &values, chunk == 0).await;
    }
}

/// The re-insert runs a located update stages: consecutive ids form one
/// chunk, a skipped id starts the next.
fn preserved_runs(selected: &[u64]) -> Vec<(u64, Vec<i64>)> {
    let mut runs = Vec::new();
    let mut start = 0;
    while start < selected.len() {
        let mut end = start + 1;
        while end < selected.len() && selected[end] == selected[start] + (end - start) as u64 {
            end += 1;
        }
        let values = selected[start..end]
            .iter()
            .map(|&row_id| i64::try_from(row_id).unwrap())
            .collect();
        runs.push((selected[start], values));
        start = end;
    }
    runs
}

/// Stages the update's tombstones and re-inserts for `selected` at
/// snapshot 7 and commits, failing if the commit takes over a minute.
async fn commit_update(catalog: &Catalog, selected: &[u64], tombstones: bool, reinserts: bool) {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(catalog, db_tx);
    if tombstones {
        for &row_id in selected {
            tx.stage(RowOperation::InlineInlineDelete {
                table_id: 1,
                row_id,
                end_snapshot: 7,
            });
        }
    }
    if reinserts {
        for (row_id_start, values) in preserved_runs(selected) {
            let (_, batch) = bigint_batch(&values);
            tx.stage(RowOperation::InlineInsert {
                table_id: 1,
                schema_version: 0,
                begin_snapshot: 7,
                row_id_start,
                row_count: u64::try_from(values.len()).unwrap(),
                arrow_body: inline_body(&batch).into(),
            });
        }
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(7, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(7, "inlined_insert:1,inlined_delete:1"),
    });

    tokio::time::timeout(Duration::from_secs(60), tx.commit())
        .await
        .expect("the located update's commit stalled")
        .unwrap();
}

/// The commit lands in bounded time and leaves one live entry per row:
/// the tombstoned versions' entries go, the re-inserted versions' come
/// back under the same ids.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn located_update_over_inlined_rows_commits_in_bounded_time() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
    inline_four_chunks(&catalog).await;
    assert_eq!(index_entry_count(&catalog, true, index_id).await, 4_000);

    let selected: Vec<u64> = (0..4_000).filter(|row_id| row_id % 77 != 5).collect();
    commit_update(&catalog, &selected, true, true).await;

    assert_eq!(index_entry_count(&catalog, true, index_id).await, 4_000);
    catalog.close().await.unwrap();
}

/// The tombstone half alone: removals derive against chunks that stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstones_alone_commit() {
    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    inline_four_chunks(&catalog).await;
    let selected: Vec<u64> = (0..4_000).filter(|row_id| row_id % 77 != 5).collect();
    commit_update(&catalog, &selected, true, false).await;
    catalog.close().await.unwrap();
}

/// The re-insert half alone: additions under ids the chunks still hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reinserts_alone_commit() {
    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    inline_four_chunks(&catalog).await;
    let selected: Vec<u64> = (0..4_000).filter(|row_id| row_id % 77 != 5).collect();
    commit_update(&catalog, &selected, false, true).await;
    catalog.close().await.unwrap();
}

/// A mixed update small enough that derivation never runs ahead of the
/// deletion phase commits the same way a large one does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_small_mixed_update_commits() {
    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    inline_four_chunks(&catalog).await;
    let selected: Vec<u64> = (0..300).filter(|row_id| row_id % 77 != 5).collect();
    commit_update(&catalog, &selected, true, true).await;
    catalog.close().await.unwrap();
}
