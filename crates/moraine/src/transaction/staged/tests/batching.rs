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
    let db_tx = catalog.begin_write_tx().unwrap();
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

fn begin(catalog: &Catalog) -> StagedTransaction {
    let db_tx = catalog.begin_write_tx().unwrap();
    StagedTransaction::begin_detached_on(catalog, db_tx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_appends_to_disjoint_tables_both_land() {
    let catalog = catalog_with_two_tables().await;
    let (mut a, mut b) = (begin(&catalog), begin(&catalog));
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
    let (mut a, mut b) = (begin(&concurrent), begin(&concurrent));
    stage_append(&mut a, 1, 1, 1, 0);
    stage_append(&mut b, 1, 1, 1, 0);
    let (a, b) = tokio::join!(a.commit(), b.commit());
    a.unwrap();
    b.unwrap();

    let sequential = catalog_with_two_tables().await;
    let mut first = begin(&sequential);
    stage_append(&mut first, 1, 1, 1, 0);
    first.commit().await.unwrap();
    let mut second = begin(&sequential);
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
    let mut early = begin(&catalog);
    stage_append(&mut early, 1, 1, 1, 0);

    let mut landed = begin(&catalog);
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
    let mut seed = begin(&catalog);
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
    let (mut a, mut b) = (begin(&catalog), begin(&catalog));
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

/// Two more inlined rows for table 1, appended after the two `head`
/// already holds: what DuckLake stamps for a second insert.
fn stage_inline_insert(tx: &mut StagedTransaction, head: u64, values: &[i64]) {
    let (_, batch) = bigint_batch(values);
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: head + 1,
        row_id_start: 2,
        row_count: 2,
        arrow_body: inline_body(&batch).into(),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::TableStats,
        cells: table_stats_row(1, 4, 4, 128),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(head + 1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(head + 1, "inlined_insert:1"),
    });
}

/// Index upkeep derives entries from the rows the rebase moved: two
/// concurrent inline inserts into an indexed table land with disjoint,
/// dense row ids, and every value is findable under the id it got.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_inline_inserts_into_an_indexed_table_rebase_their_entries() {
    use crate::{
        catalog::{IndexId, TableId},
        store::index_encoding::{IndexKeyValue, IntWidth},
    };

    let (catalog, index_id) = catalog_with_indexed_inline_table(false).await;
    let head = catalog.snapshot().await.unwrap().snapshot.snapshot_id;

    // The first insert registers the inline table's schema and commits
    // alone; the members below ride the batch.
    seed_inline_rows(&catalog, head + 1).await;
    let head = head + 1;

    // Both transactions open before either commits, so both stamp their
    // rows against `head` whichever order the commits run in: opened
    // inside the join, the second reads the first's head and stamps a
    // snapshot id at it, which is a lost race rather than a rebase.
    let (mut a, mut b) = (begin(&catalog), begin(&catalog));
    stage_inline_insert(&mut a, head, &[30, 40]);
    stage_inline_insert(&mut b, head, &[50, 60]);

    let (a, b) = tokio::join!(a.commit(), b.commit());
    let mut ids = vec![a.unwrap().get(), b.unwrap().get()];
    ids.sort_unstable();
    assert_eq!(ids, [head + 1, head + 2]);

    let lookup = |value: i128| {
        let catalog = &catalog;
        async move {
            catalog
                .index_lookup(
                    TableId::new(1),
                    IndexId::new(index_id),
                    &[IndexKeyValue::Int {
                        value,
                        width: IntWidth::I64,
                    }],
                )
                .await
                .unwrap()
        }
    };
    let mut rows: Vec<u64> = Vec::new();
    for value in [30, 40, 50, 60] {
        let found = lookup(value).await;
        assert_eq!(
            found.len(),
            1,
            "value {value} is indexed exactly once: {found:?}"
        );
        rows.push(found[0]);
    }
    rows.sort_unstable();
    assert_eq!(
        rows,
        [2, 3, 4, 5],
        "the loser's rows moved past the winner's"
    );
    assert_eq!(index_entry_count(&catalog, false, index_id).await, 6);
    let stats = catalog.snapshot().await.unwrap().table_stats[&1].clone();
    assert_eq!((stats.record_count, stats.next_row_id), (6, 6));
}

/// The rebase moves index entries off a head that landed while the commit
/// was open, not only off a member ahead of it on the same batch.
#[tokio::test]
async fn an_inline_insert_begun_before_a_landed_one_rebases_its_entries() {
    use crate::{
        catalog::{IndexId, TableId},
        store::index_encoding::{IndexKeyValue, IntWidth},
    };

    let (catalog, index_id) = catalog_with_indexed_inline_table(false).await;
    let head = catalog.snapshot().await.unwrap().snapshot.snapshot_id;
    seed_inline_rows(&catalog, head + 1).await;
    let head = head + 1;

    let (mut early, mut landed) = (begin(&catalog), begin(&catalog));
    stage_inline_insert(&mut early, head, &[50, 60]);
    stage_inline_insert(&mut landed, head, &[30, 40]);

    assert_eq!(landed.commit().await.unwrap().get(), head + 1);
    assert_eq!(early.commit().await.unwrap().get(), head + 2);

    let lookup = |value: i128| {
        let catalog = &catalog;
        async move {
            catalog
                .index_lookup(
                    TableId::new(1),
                    IndexId::new(index_id),
                    &[IndexKeyValue::Int {
                        value,
                        width: IntWidth::I64,
                    }],
                )
                .await
                .unwrap()
        }
    };
    for (value, row_id) in [(30, 2), (40, 3), (50, 4), (60, 5)] {
        assert_eq!(lookup(value).await, [row_id], "value {value}");
    }
    assert_eq!(index_entry_count(&catalog, false, index_id).await, 6);
    let stats = catalog.snapshot().await.unwrap().table_stats[&1].clone();
    assert_eq!((stats.record_count, stats.next_row_id), (6, 6));
}

/// Two inlined rows into table 1 at `snapshot_id`, with the schema and
/// the statistics row every DuckLake insert carries.
async fn seed_inline_rows(catalog: &Catalog, snapshot_id: u64) {
    let (schema, seed) = bigint_batch(&[10, 20]);
    let mut tx = begin(catalog);
    tx.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 0,
        arrow_schema: inline_schema_ipc(&schema),
    });
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: snapshot_id,
        row_id_start: 0,
        row_count: 2,
        arrow_body: inline_body(&seed).into(),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::TableStats,
        cells: table_stats_row(1, 2, 2, 64),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(snapshot_id, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(snapshot_id, "inlined_insert:1"),
    });
    tx.commit().await.unwrap();
}

/// A burst wider than the sandbox has cores, every member a stats-bearing
/// append to a table of its own, twice over: all land, none is abandoned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_burst_of_sixty_four_members_all_land() {
    const WRITERS: u64 = 64;

    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().unwrap();
    let mut tx = StagedTransaction::begin_detached_on(&catalog, db_tx);
    let mut created = Vec::new();
    for table_id in 1..=WRITERS {
        tx.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(table_id, 0, &format!("t{table_id}"), 1, None),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Column,
            cells: column_row(table_id, 1, "v", 0),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::TableStats,
            cells: table_stats_row(table_id, 0, 0, 0),
        });
        created.push(format!(r#"created_table:"main"."t{table_id}""#));
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row_allocating(1, WRITERS + 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, &created.join(",")),
    });
    tx.commit().await.unwrap();

    for round in 0..3u64 {
        let head = catalog.snapshot().await.unwrap().snapshot.snapshot_id;
        let mut members = Vec::new();
        for table_id in 1..=WRITERS {
            let mut tx = begin(&catalog);
            stage_append(&mut tx, table_id, head, head, round * 10);
            members.push(tx.commit());
        }
        // A commit the batch refuses lands alone in the middle of the
        // burst, as a writer's first insert does under DuckLake.
        let lone = async {
            let mut tx = begin(&catalog);
            tx.stage(RowOperation::InlineSchema {
                table_id: 1,
                schema_version: 0,
                arrow_schema: b"schema".to_vec(),
            });
            tx.stage(RowOperation::Insert {
                table: TableKind::Snapshot,
                cells: snapshot_row_allocating(head + 1, WRITERS + 1, head + 1),
            });
            tx.stage(RowOperation::Insert {
                table: TableKind::SnapshotChanges,
                cells: snapshot_changes_row(head + 1, "inlined_insert:1"),
            });
            tx.commit().await
        };
        let (outcomes, lone) = tokio::join!(futures::future::join_all(members), lone);
        let failed: Vec<String> = outcomes
            .iter()
            .filter_map(|outcome| outcome.as_ref().err().map(ToString::to_string))
            .collect();
        assert!(failed.is_empty(), "round {round}: {failed:?}");
        // The lone commit either landed or lost its race, never more.
        if let Err(err) = lone {
            assert!(matches!(err, Error::CommitConflict(_)), "{err}");
        }
    }
    let head = catalog.snapshot().await.unwrap();
    assert!(head.snapshot.snapshot_id > 3 * WRITERS);
}

/// DuckLake stamps its rows against the snapshot it read when its own
/// transaction began, which may predate the catalog transaction the rows
/// reach. The member rebases from the view it read; the transaction's own
/// view would call the same rows a lost race.
#[tokio::test]
async fn a_member_rebases_from_the_view_it_read_not_the_one_its_transaction_opened_at() {
    let catalog = catalog_with_two_tables().await;
    let read = catalog.snapshot().await.unwrap();
    assert_eq!(read.snapshot.snapshot_id, 1);

    let mut other = begin(&catalog);
    stage_append(&mut other, 2, 1, 1, 0);
    assert_eq!(other.commit().await.unwrap().get(), 2);

    // Rows authored against head 1 on a transaction opened at head 2.
    let mut tx =
        crate::ffi_support::staged::staged_begin(&catalog, None, String::new(), Some(read))
            .unwrap();
    stage_append(&mut tx, 1, 1, 1, 0);
    assert_eq!(tx.commit().await.unwrap().get(), 3);
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.snapshot.next_file_id, 3);
    assert!(
        head.data_files[&1].contains_key(&2),
        "the file id moved past the other's"
    );

    // Without the view it read, the same shape is a lost race for DuckLake
    // to re-drive, and nothing else's batch pays for it.
    let mut late = begin(&catalog);
    stage_append(&mut late, 2, 2, 2, 10);
    assert!(matches!(
        late.commit().await.unwrap_err(),
        Error::CommitConflict(_)
    ));
    assert_eq!(catalog.snapshot().await.unwrap().snapshot.snapshot_id, 3);
}

/// A head-preserving commit derives nothing from the head, so a race it
/// loses is moraine's own to re-drive: it lands behind the winner rather
/// than surfacing a conflict for DuckLake to retry into a session that
/// never stops committing.
#[tokio::test]
async fn a_head_preserving_commit_re_drives_its_own_lost_race() {
    let catalog = catalog_with_two_tables().await;

    // Opened before the winner lands.
    let mut maintenance = begin(&catalog);
    maintenance.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![
            Cell::U64(77),
            Cell::Str("gone.parquet".to_string()),
            Cell::Bool(true),
            Cell::I64(7),
        ],
    });

    let mut winner = begin(&catalog);
    stage_append(&mut winner, 1, 1, 1, 0);
    assert_eq!(winner.commit().await.unwrap().get(), 2);

    assert_eq!(
        maintenance.commit().await.unwrap().get(),
        2,
        "head-preserving: it lands on the head it finds"
    );
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.scheduled_deletions().len(), 1);
    assert!(
        head.data_files[&1].contains_key(&1),
        "the winner's file is untouched"
    );
}

/// A commit the batch cannot take loses its race and goes back to DuckLake
/// for a retry; the batch yields to that retry, holding its own arrivals,
/// or a session committing without pause would beat it every time.
#[tokio::test]
async fn the_batch_yields_to_a_commit_it_could_not_take_after_it_lost() {
    let catalog = catalog_with_two_tables().await;

    // DDL, authored against head 1, on a transaction opened at head 1.
    let mut ddl = begin(&catalog);
    ddl.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(3, 0, "w", 2, None),
    });
    ddl.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row_allocating(2, 4, 1),
    });
    ddl.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, r#"created_table:"main"."w""#),
    });

    let mut winner = begin(&catalog);
    stage_append(&mut winner, 1, 1, 1, 0);
    assert_eq!(winner.commit().await.unwrap().get(), 2);
    assert!(matches!(
        ddl.commit().await.unwrap_err(),
        Error::CommitConflict(_)
    ));

    // The next data commit waits out the yield before it stages.
    let started = std::time::Instant::now();
    let mut next = begin(&catalog);
    stage_append(&mut next, 2, 2, 2, 0);
    assert_eq!(next.commit().await.unwrap().get(), 3);
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(200),
        "the batch yielded only {:?}",
        started.elapsed()
    );

    // The same DDL on a transaction opened after the winner: its rows read
    // as a lost race before anything is staged, and the batch yields to
    // that too.
    let mut late = begin(&catalog);
    late.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(3, 0, "w", 3, None),
    });
    late.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row_allocating(3, 4, 2),
    });
    late.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, r#"created_table:"main"."w""#),
    });
    assert!(matches!(
        late.commit().await.unwrap_err(),
        Error::CommitConflict(_)
    ));
    let started = std::time::Instant::now();
    let mut next = begin(&catalog);
    stage_append(&mut next, 1, 3, 3, 10);
    assert_eq!(next.commit().await.unwrap().get(), 4);
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(200),
        "the batch yielded only {:?} to a loss found while staging",
        started.elapsed()
    );
}
