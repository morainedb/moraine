//! Staged commits riding the batch: concurrent data-only commits land
//! together, rebased onto one another, and only a true conflict is refused.

use super::*;

/// A snapshot row carrying a real file-id counter, which the rebase moves.
fn snapshot_row_allocating(id: u64, next_catalog_id: u64, next_file_id: u64) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::I64(1),
        Cell::U64(1),
        Cell::U64(next_catalog_id),
        Cell::U64(next_file_id),
    ]
}

fn table_stats_row(table_id: u64, record_count: u64, next_row_id: u64, bytes: u64) -> Vec<Cell> {
    vec![
        Cell::U64(table_id),
        Cell::U64(record_count),
        Cell::U64(next_row_id),
        Cell::U64(bytes),
    ]
}

/// A catalog holding tables 1 and 2, empty, at snapshot 1 with file ids
/// allocated from 1.
async fn catalog_with_two_tables() -> Catalog {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_on(&catalog, db_tx);
    for (table_id, name) in [(1, "t"), (2, "u")] {
        tx.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(table_id, 0, name, 1, None),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Column,
            cells: column_row(table_id, 1, "v", 0),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::TableStats,
            cells: table_stats_row(table_id, 0, 0, 0),
        });
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row_allocating(1, 3, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t",created_table:"main"."u""#),
    });
    tx.commit().await.unwrap();
    catalog
}

/// One file of ten rows appended to `table_id`, authored against head
/// `head` with file ids allocated from `next_file_id` and rows from
/// `next_row_id` — exactly what DuckLake stamps.
fn stage_append(
    tx: &mut StagedTransaction,
    table_id: u64,
    head: u64,
    next_file_id: u64,
    next_row_id: u64,
) {
    let mut file = data_file_row(next_file_id, table_id, head + 1);
    file[11] = Cell::U64(next_row_id);
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: file,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::TableStats,
        cells: table_stats_row(
            table_id,
            next_row_id + 10,
            next_row_id + 10,
            (next_row_id + 10) * 100,
        ),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row_allocating(head + 1, 3, next_file_id + 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(head + 1, &format!("inserted_into_table:{table_id}")),
    });
}

async fn begin(catalog: &Catalog) -> StagedTransaction {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    StagedTransaction::begin_detached_on(catalog, db_tx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_appends_to_disjoint_tables_both_land() {
    let catalog = catalog_with_two_tables().await;
    let (mut a, mut b) = (begin(&catalog).await, begin(&catalog).await);
    stage_append(&mut a, 1, 1, 1, 0);
    stage_append(&mut b, 2, 1, 1, 0);

    let (a, b) = tokio::join!(a.commit(), b.commit());
    let mut ids = vec![a.unwrap().get(), b.unwrap().get()];
    ids.sort_unstable();
    assert_eq!(
        ids,
        [2, 3],
        "each mints a snapshot of its own, consecutively"
    );

    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.snapshot.snapshot_id, 3);
    assert_eq!(
        head.snapshot.next_file_id, 3,
        "two files allocated across the two"
    );
    let files_of = |table_id: u64| -> Vec<u64> {
        head.data_files
            .get(&table_id)
            .map(|files| files.keys().copied().collect())
            .unwrap_or_default()
    };
    let mut all: Vec<u64> = files_of(1).into_iter().chain(files_of(2)).collect();
    all.sort_unstable();
    assert_eq!(all, [1, 2], "the loser's file id moved above the winner's");
    assert_eq!(head.table_stats.get(&1).unwrap().record_count, 10);
    assert_eq!(head.table_stats.get(&2).unwrap().record_count, 10);
}

/// Differential: the same two appends landing concurrently leave the
/// table exactly as the second one would have if it had been authored
/// after the first — row ids dense, counters advanced, stats summed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_appends_to_one_table_rebase_row_ids_and_stats() {
    let concurrent = catalog_with_two_tables().await;
    let (mut a, mut b) = (begin(&concurrent).await, begin(&concurrent).await);
    stage_append(&mut a, 1, 1, 1, 0);
    stage_append(&mut b, 1, 1, 1, 0);
    let (a, b) = tokio::join!(a.commit(), b.commit());
    a.unwrap();
    b.unwrap();

    let sequential = catalog_with_two_tables().await;
    let mut first = begin(&sequential).await;
    stage_append(&mut first, 1, 1, 1, 0);
    first.commit().await.unwrap();
    let mut second = begin(&sequential).await;
    stage_append(&mut second, 1, 2, 2, 10);
    second.commit().await.unwrap();

    let (concurrent, sequential) = (
        concurrent.snapshot().await.unwrap(),
        sequential.snapshot().await.unwrap(),
    );
    assert_eq!(concurrent.snapshot.snapshot_id, 3);
    assert_eq!(concurrent.data_files.get(&1), sequential.data_files.get(&1));
    assert_eq!(
        concurrent.table_stats.get(&1),
        sequential.table_stats.get(&1)
    );
    assert_eq!(
        concurrent.snapshot.next_file_id,
        sequential.snapshot.next_file_id
    );
    let starts: Vec<Option<u64>> = concurrent.data_files[&1]
        .values()
        .map(|file| file.row_id_start)
        .collect();
    assert_eq!(
        starts,
        [Some(0), Some(10)],
        "row ids stay dense across the race"
    );
}

#[tokio::test]
async fn an_append_begun_before_a_landed_append_rebases_onto_it() {
    let catalog = catalog_with_two_tables().await;
    let mut early = begin(&catalog).await;
    stage_append(&mut early, 1, 1, 1, 0);

    let mut landed = begin(&catalog).await;
    stage_append(&mut landed, 1, 1, 1, 0);
    assert_eq!(landed.commit().await.unwrap().get(), 2);

    assert_eq!(early.commit().await.unwrap().get(), 3);
    let head = catalog.snapshot().await.unwrap();
    let starts: Vec<Option<u64>> = head.data_files[&1]
        .values()
        .map(|file| file.row_id_start)
        .collect();
    assert_eq!(starts, [Some(0), Some(10)]);
    assert_eq!(head.table_stats[&1].next_row_id, 20);
}

/// Two deletes against one table meet at table grain, which is a true
/// conflict: one lands, the other is refused with the text DuckLake
/// re-drives on, and the batch the winner rode is not aborted by the loser.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicting_members_decline_the_batch_rather_than_aborting_it() {
    let catalog = catalog_with_two_tables().await;
    let mut seed = begin(&catalog).await;
    stage_append(&mut seed, 1, 1, 1, 0);
    seed.commit().await.unwrap();

    let stage_delete = |tx: &mut StagedTransaction, head: u64, next_file_id: u64| {
        tx.stage(RowOperation::Insert {
            table: TableKind::DeleteFile,
            cells: vec![
                Cell::U64(next_file_id),
                Cell::U64(1),
                Cell::U64(head + 1),
                Cell::Null,
                Cell::U64(1),
                Cell::Str("delete.parquet".into()),
                Cell::Bool(true),
                Cell::Str("parquet".into()),
                Cell::U64(1),
                Cell::U64(64),
                Cell::U64(16),
                Cell::Null,
                Cell::Null,
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row_allocating(head + 1, 3, next_file_id + 1),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(head + 1, "deleted_from_table:1"),
        });
    };
    let (mut a, mut b) = (begin(&catalog).await, begin(&catalog).await);
    stage_delete(&mut a, 2, 2);
    stage_delete(&mut b, 2, 2);

    let (a, b) = tokio::join!(a.commit(), b.commit());
    let (winner, loser) = match (a, b) {
        (Ok(id), Err(err)) | (Err(err), Ok(id)) => (id, err),
        other => panic!("exactly one delete may land: {other:?}"),
    };
    assert_eq!(winner.get(), 3);
    assert!(
        matches!(loser, Error::CommitConflict(_)) && loser.to_string().contains("conflict"),
        "{loser}"
    );
    assert_eq!(catalog.snapshot().await.unwrap().snapshot.snapshot_id, 3);
}
