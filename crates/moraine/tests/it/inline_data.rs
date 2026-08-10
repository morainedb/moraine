//! Inlined data through the verb surface: inlining a chunk, tombstoning a
//! row, reading rows back, and draining them to a file.
//!
//! The chunk bodies here are opaque bytes. moraine stores and returns them
//! verbatim — decoding Arrow is the caller's half of the contract — so the
//! catalog behaviour is exercised without an Arrow encoder in the way.

use moraine::{
    Catalog, CatalogOptions, DataFile, Error, FileIndexEntry, FileIndexRemoval, FlushedDataFile,
    IndexDef, IndexKeyValue, InlineChunk, IntWidth, SnapshotId, TableId,
};

use crate::fixtures::{col, datafile, seeded};

fn chunk(body: &str, rows: u64) -> InlineChunk {
    InlineChunk {
        schema_version: 0,
        arrow_schema: b"schema-v0".to_vec(),
        arrow_body: body.as_bytes().to_vec(),
        row_count: rows,
    }
}

/// The row ids of a table's live inlined rows, in order.
#[allow(clippy::unwrap_used)]
async fn row_ids(catalog: &Catalog, table: TableId) -> Vec<u64> {
    catalog
        .recent_rows(table)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.row_id)
        .collect()
}

#[tokio::test]
async fn an_inlined_chunk_reads_back_with_dense_ids_and_its_bytes() {
    let (catalog, _, table, _) = seeded().await;
    let start = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("rows-a", 3), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let rows = catalog.recent_rows(table).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(row_ids(&catalog, table).await, vec![0, 1, 2]);
    for (offset, row) in rows.iter().enumerate() {
        assert_eq!(row.offset_in_chunk, offset as u64);
        assert_eq!(row.schema_version, 0);
        assert_eq!(row.chunk_body.as_slice(), b"rows-a");
        assert_eq!(row.arrow_schema.as_slice(), b"schema-v0");
        assert_eq!(row.begin_snapshot, start);
    }
    // Rows of one chunk share one body and one schema, read and carried
    // once however many rows point at them.
    assert!(std::sync::Arc::ptr_eq(
        &rows[0].chunk_body,
        &rows[2].chunk_body
    ));
    assert!(std::sync::Arc::ptr_eq(
        &rows[0].arrow_schema,
        &rows[2].arrow_schema
    ));
}

/// `inline_insert` returns the chunk's first row id, and ids continue from
/// the table's counter across chunks and commits — the same allocation a
/// data-file registration performs.
#[tokio::test]
async fn row_ids_continue_across_chunks_commits_and_data_files() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(|tx| {
            assert_eq!(tx.inline_insert(table, &chunk("a", 2), &[])?, 0);
            assert_eq!(tx.inline_insert(table, &chunk("b", 1), &[])?, 2);
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            tx.register_data_file(table, datafile(5), &[])?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            assert_eq!(tx.inline_insert(table, &chunk("c", 1), &[])?, 8);
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(row_ids(&catalog, table).await, vec![0, 1, 2, 8]);
    let stats = catalog
        .snapshot()
        .await
        .unwrap()
        .table_stats(table)
        .unwrap();
    assert_eq!(stats.next_row_id, 9);
    // Inlined rows count as rows: four inlined plus the file's five.
    assert_eq!(stats.record_count, 9);
}

#[tokio::test]
async fn a_tombstoned_row_disappears_from_the_live_rows() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 3), &[])?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.inline_delete(table, 1, &[]))
        .await
        .unwrap();

    assert_eq!(row_ids(&catalog, table).await, vec![0, 2]);
    assert!(catalog.recent_row(table, 1).await.unwrap().is_none());
    assert_eq!(
        catalog.recent_row(table, 2).await.unwrap().unwrap().row_id,
        2
    );
    // A tombstone is its own record, never a chunk rewrite: its neighbours
    // in the chunk keep the body and offsets they were written with.
    let survivor = catalog.recent_row(table, 2).await.unwrap().unwrap();
    assert_eq!(survivor.chunk_body.as_slice(), b"a");
    assert_eq!(survivor.offset_in_chunk, 2);

    // Deletes leave statistics alone, exactly as a delete file does.
    let stats = catalog
        .snapshot()
        .await
        .unwrap()
        .table_stats(table)
        .unwrap();
    assert_eq!(stats.record_count, 3);
}

/// Time travel over inlined rows: each snapshot sees the rows live at it,
/// with the tombstoned ones back and the later ones gone.
#[tokio::test]
async fn recent_rows_at_reads_the_rows_live_at_a_past_snapshot() {
    let (catalog, _, table, _) = seeded().await;
    let inserted = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 3), &[])?;
            Ok(())
        })
        .await
        .unwrap();
    let tombstoned = catalog
        .commit(|tx| tx.inline_delete(table, 1, &[]))
        .await
        .unwrap();
    let appended = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("b", 1), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let at = async |snapshot| {
        catalog
            .recent_rows_at(table, snapshot)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.row_id)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        at(inserted).await,
        vec![0, 1, 2],
        "row 1 not yet tombstoned"
    );
    assert_eq!(at(tombstoned).await, vec![0, 2]);
    assert_eq!(at(appended).await, vec![0, 2, 3]);
    assert_eq!(at(appended).await, row_ids(&catalog, table).await);

    // The bytes travel with the rows: the row read at a past snapshot
    // carries the chunk it was written into.
    let past = catalog.recent_rows_at(table, inserted).await.unwrap();
    assert_eq!(past[1].chunk_body.as_slice(), b"a");
    assert_eq!(past[1].begin_snapshot, inserted);
}

/// A snapshot id nobody has minted is refused the same way `snapshot_at`
/// refuses it — one resolution rule for every read that takes an id.
#[tokio::test]
async fn recent_rows_at_refuses_a_snapshot_beyond_the_head() {
    let (catalog, _, table, _) = seeded().await;
    let err = catalog
        .recent_rows_at(table, SnapshotId::new(u64::MAX))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err}");
}

/// A flush leaves nothing behind for time travel to find: pre-flush
/// snapshots are served from the backdated Parquet, not from the drained
/// chunks.
#[tokio::test]
async fn recent_rows_at_finds_nothing_once_the_rows_are_flushed() {
    let (catalog, _, table, _) = seeded().await;
    let inlined = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 3), &[])?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        catalog.recent_rows_at(table, inlined).await.unwrap().len(),
        3
    );

    catalog
        .commit(move |tx| {
            tx.flush_inlined_data(
                table,
                0,
                &[FlushedDataFile {
                    file: DataFile {
                        record_count: 3,
                        ..datafile(3)
                    },
                    row_id_start: 0,
                    begin_snapshot: inlined,
                    partial_max: None,
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    assert!(
        catalog
            .recent_rows_at(table, inlined)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        catalog
            .snapshot_at(inlined)
            .await
            .unwrap()
            .data_files_of(table)
            .len(),
        1,
        "the rows are in the backdated file instead"
    );
}

#[tokio::test]
async fn recent_rows_are_scoped_to_their_table() {
    let (catalog, _, a, b) = seeded().await;
    catalog
        .commit(|tx| {
            tx.inline_insert(a, &chunk("a", 1), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(row_ids(&catalog, a).await, vec![0]);
    assert!(catalog.recent_rows(b).await.unwrap().is_empty());
    assert!(catalog.recent_row(b, 0).await.unwrap().is_none());
}

#[tokio::test]
async fn an_empty_chunk_is_refused() {
    let (catalog, _, table, _) = seeded().await;
    let err = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("", 0), &[])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
}

#[tokio::test]
async fn inlining_into_an_absent_table_is_not_found() {
    let (catalog, _, _, _) = seeded().await;
    let err = catalog
        .commit(|tx| {
            tx.inline_insert(TableId::new(9999), &chunk("a", 1), &[])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err}");
}

/// A flush drains the chunks and registers the file the caller wrote,
/// preserving the rows' ids and backdating the record — so the rows are in
/// exactly one place before and after.
#[tokio::test]
async fn a_flush_drains_the_chunks_and_registers_the_backdated_file() {
    let (catalog, _, table, _) = seeded().await;
    let inlined = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 3), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let file = DataFile {
        record_count: 3,
        ..datafile(3)
    };
    catalog
        .commit(move |tx| {
            let flushed = FlushedDataFile {
                file: file.clone(),
                row_id_start: 0,
                begin_snapshot: inlined,
                partial_max: None,
            };
            let ids = tx.flush_inlined_data(table, 0, std::slice::from_ref(&flushed))?;
            assert_eq!(ids.len(), 1);
            Ok(())
        })
        .await
        .unwrap();

    assert!(
        catalog.recent_rows(table).await.unwrap().is_empty(),
        "the drained chunks are gone"
    );
    let head = catalog.snapshot().await.unwrap();
    let files = head.data_files_of(table);
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].row_id_start, Some(0), "row ids are preserved");
    // The rows were counted when they were inlined; the flush adds only
    // the file's bytes.
    let stats = head.table_stats(table).unwrap();
    assert_eq!(stats.record_count, 3);
    assert_eq!(stats.next_row_id, 3);
    assert_eq!(stats.file_size_bytes, 30);

    // Backdated: the file is live at the snapshot the rows were inlined at.
    let past = catalog.snapshot_at(inlined).await.unwrap();
    assert_eq!(past.data_files_of(table).len(), 1);
}

#[tokio::test]
async fn a_flush_with_no_files_drains_wholly_tombstoned_chunks() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 1), &[])?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.inline_delete(table, 0, &[]))
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            tx.flush_inlined_data(table, 0, &[])?;
            Ok(())
        })
        .await
        .unwrap();

    assert!(catalog.recent_rows(table).await.unwrap().is_empty());
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .data_files_of(table)
            .is_empty()
    );
}

/// One commit both inlines into a table and flushes it. The drain reads
/// the store as it stood before the commit, so it takes the chunks already
/// there and leaves the one this commit stages for the next flush.
#[tokio::test]
async fn one_commit_inlines_into_a_table_and_flushes_it() {
    let (catalog, _, table, _) = seeded().await;
    let inlined = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 2), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let file = DataFile {
        record_count: 2,
        ..datafile(2)
    };
    catalog
        .commit(move |tx| {
            tx.inline_insert(table, &chunk("b", 1), &[])?;
            tx.flush_inlined_data(
                table,
                0,
                &[FlushedDataFile {
                    file: file.clone(),
                    row_id_start: 0,
                    begin_snapshot: inlined,
                    partial_max: None,
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let rows = catalog.recent_rows(table).await.unwrap();
    assert_eq!(
        row_ids(&catalog, table).await,
        vec![2],
        "only the new chunk"
    );
    assert_eq!(rows[0].chunk_body.as_slice(), b"b");

    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.data_files_of(table).len(), 1);
    // Two rows in the file, one still inlined, counted once each.
    let stats = head.table_stats(table).unwrap();
    assert_eq!((stats.record_count, stats.next_row_id), (3, 3));
}

/// The other half of the composition does not hold: a row this commit
/// tombstones is a row the flush's file already carries as live, and the
/// tombstone would be dropped with the chunk that is drained out from
/// under it. Refused, pointing at the delete file that does express it.
#[tokio::test]
async fn tombstoning_a_row_the_same_commit_flushes_is_refused() {
    let (catalog, _, table, _) = seeded().await;
    let inlined = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 3), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let err = catalog
        .commit(move |tx| {
            tx.inline_delete(table, 1, &[])?;
            tx.flush_inlined_data(
                table,
                0,
                &[FlushedDataFile {
                    file: DataFile {
                        record_count: 3,
                        ..datafile(3)
                    },
                    row_id_start: 0,
                    begin_snapshot: inlined,
                    partial_max: None,
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");

    // Nothing landed: the rows are still inlined and still all live.
    assert_eq!(row_ids(&catalog, table).await, vec![0, 1, 2]);

    // A tombstone against a version the flush does not drain is untouched
    // by the rule.
    catalog
        .commit(move |tx| {
            tx.inline_insert(
                table,
                &InlineChunk {
                    schema_version: 1,
                    arrow_schema: b"schema-v1".to_vec(),
                    ..chunk("v1", 1)
                },
                &[],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(move |tx| {
            tx.inline_delete(table, 3, &[])?;
            tx.flush_inlined_data(
                table,
                0,
                &[FlushedDataFile {
                    file: DataFile {
                        record_count: 3,
                        ..datafile(3)
                    },
                    row_id_start: 0,
                    begin_snapshot: inlined,
                    partial_max: None,
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(row_ids(&catalog, table).await, Vec::<u64>::new());
}

/// The flushed file's ceiling is the ids the table had allocated when the
/// commit began, so a file cannot claim rows the same commit inlined —
/// those rows are in the surviving chunk, and counting them in the file
/// too would put them in two places at once.
#[tokio::test]
async fn a_flushed_file_may_not_claim_the_ids_its_own_commit_minted() {
    let (catalog, _, table, _) = seeded().await;
    let inlined = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 2), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let err = catalog
        .commit(move |tx| {
            tx.inline_insert(table, &chunk("b", 1), &[])?;
            tx.flush_inlined_data(
                table,
                0,
                &[FlushedDataFile {
                    file: DataFile {
                        record_count: 3,
                        ..datafile(3)
                    },
                    row_id_start: 0,
                    begin_snapshot: inlined,
                    partial_max: None,
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
}

#[tokio::test]
async fn a_flushed_file_must_be_backdated_and_stay_inside_the_allocated_ids() {
    let (catalog, _, table, _) = seeded().await;
    let inlined = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 2), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    // Not backdated: a record beginning at (or above) this commit's own
    // snapshot would hide the rows from the snapshots they were live at.
    let not_backdated = catalog
        .commit(move |tx| {
            tx.flush_inlined_data(
                table,
                0,
                &[FlushedDataFile {
                    file: datafile(2),
                    row_id_start: 0,
                    begin_snapshot: SnapshotId::new(u64::MAX),
                    partial_max: None,
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(
        matches!(not_backdated, Error::Constraint(_)),
        "{not_backdated}"
    );

    // Past the allocated ids: the file claims rows the table never minted.
    let past_end = catalog
        .commit(move |tx| {
            tx.flush_inlined_data(
                table,
                0,
                &[FlushedDataFile {
                    file: datafile(2),
                    row_id_start: 1,
                    begin_snapshot: inlined,
                    partial_max: None,
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(past_end, Error::Constraint(_)), "{past_end}");
}

/// Several chunks under one schema version land under distinct keys within
/// one commit — the sequence number that disambiguates them is allocated
/// per `(table, schema_version)`.
#[tokio::test]
async fn several_chunks_in_one_commit_all_survive() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 1), &[])?;
            tx.inline_insert(table, &chunk("b", 1), &[])?;
            tx.inline_insert(table, &chunk("c", 1), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let rows = catalog.recent_rows(table).await.unwrap();
    let mut bodies: Vec<&[u8]> = rows.iter().map(|row| row.chunk_body.as_slice()).collect();
    bodies.sort_unstable();
    assert_eq!(
        bodies,
        vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
    );
}

/// Chunks of different schema versions coexist, each decoding against its
/// own recorded schema, and a flush drains only the version it names.
#[tokio::test]
async fn schema_versions_are_independent_through_read_and_flush() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("v0", 1), &[])?;
            tx.inline_insert(
                table,
                &InlineChunk {
                    schema_version: 1,
                    arrow_schema: b"schema-v1".to_vec(),
                    ..chunk("v1", 1)
                },
                &[],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let rows = catalog.recent_rows(table).await.unwrap();
    assert_eq!(rows.len(), 2);
    let schema_of = |version: u64| {
        rows.iter()
            .find(|row| row.schema_version == version)
            .map(|row| row.arrow_schema.as_slice().to_vec())
            .unwrap()
    };
    assert_eq!(schema_of(0), b"schema-v0");
    assert_eq!(schema_of(1), b"schema-v1");

    catalog
        .commit(|tx| {
            tx.flush_inlined_data(table, 0, &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let left = catalog.recent_rows(table).await.unwrap();
    assert_eq!(left.len(), 1, "only version 0 was drained");
    assert_eq!(left[0].schema_version, 1);
}

/// A catalog whose seeded table `a` carries a non-unique index over its
/// one column.
#[allow(clippy::unwrap_used)]
async fn indexed() -> (Catalog, TableId, moraine::IndexId) {
    let (catalog, _, table, _) = seeded().await;
    let column = catalog.snapshot().await.unwrap().columns_of(table)[0].id;
    catalog
        .commit(move |tx| {
            tx.create_index(
                table,
                &IndexDef {
                    name: "by_x".into(),
                    columns: vec![column],
                    unique: false,
                },
                &[],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let index = catalog
        .snapshot()
        .await
        .unwrap()
        .index_by_name(table, "by_x")
        .unwrap()
        .id;

    (catalog, table, index)
}

fn indexed_value(v: i128) -> IndexKeyValue {
    IndexKeyValue::Int {
        value: v,
        width: IntWidth::I64,
    }
}

/// An index that silently misses a table's inlined rows is a lie, so an
/// uncovered inline insert is refused — the rule a data-file registration
/// already enforces.
#[tokio::test]
async fn an_uncovered_inline_insert_into_an_indexed_table_is_refused() {
    let (catalog, table, _) = indexed().await;
    let err = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 1), &[])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");

    let err = catalog
        .commit(|tx| tx.inline_delete(table, 0, &[]))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
}

/// A covered inlined row is findable by the index; tombstoning it strips the
/// entry, so the index never reports a row the table no longer has.
#[tokio::test]
async fn a_covered_inlined_row_is_findable_until_it_is_tombstoned() {
    let (catalog, table, index) = indexed().await;
    catalog
        .commit(move |tx| {
            tx.inline_insert(
                table,
                &chunk("a", 2),
                &[
                    FileIndexEntry {
                        index,
                        ordinal: 0,
                        values: vec![Some(indexed_value(10))],
                    },
                    FileIndexEntry {
                        index,
                        ordinal: 1,
                        values: vec![Some(indexed_value(20))],
                    },
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let twenty = [indexed_value(20)];
    let found = catalog.index_lookup(table, index, &twenty).await.unwrap();
    assert_eq!(found, vec![1]);
    catalog
        .commit(move |tx| {
            tx.inline_delete(
                table,
                1,
                &[FileIndexRemoval {
                    index,
                    row_id: 1,
                    values: vec![Some(indexed_value(20))],
                }],
            )
        })
        .await
        .unwrap();
    assert!(
        catalog
            .index_lookup(table, index, &twenty)
            .await
            .unwrap()
            .is_empty()
    );
}

/// A removal must name the row being tombstoned; naming another row would
/// strip a live row's entry.
#[tokio::test]
async fn an_inline_delete_refuses_a_removal_for_another_row() {
    let (catalog, table, index) = indexed().await;
    let err = catalog
        .commit(move |tx| {
            tx.inline_delete(
                table,
                0,
                &[FileIndexRemoval {
                    index,
                    row_id: 7,
                    values: vec![Some(indexed_value(1))],
                }],
            )
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
}

/// Inlined rows are store state, not handle state: a second handle opened
/// over the same bucket reads them back, bytes and ids intact.
#[tokio::test]
async fn inlined_rows_survive_a_reopen() {
    let store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let table = {
        let catalog = Catalog::open(std::sync::Arc::clone(&store), CatalogOptions::default())
            .await
            .unwrap();
        let created = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
                let table = tx.create_table(schema, "orders", &[col("a")])?;
                tx.inline_insert(table, &chunk("durable", 2), &[])?;
                created.set(Some(table));
                Ok(())
            })
            .await
            .unwrap();
        catalog.close().await.unwrap();
        created.get().unwrap()
    };

    let reopened = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    let rows = reopened.recent_rows(table).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(row_ids(&reopened, table).await, vec![0, 1]);
    assert_eq!(rows[0].chunk_body.as_slice(), b"durable");
    assert_eq!(rows[0].arrow_schema.as_slice(), b"schema-v0");
}

/// A flush that drains rows inlined at several snapshots writes one file
/// spanning all of them: the record is backdated to the earliest and
/// bounded by `partial_max`, so a reader knows to filter the file's rows
/// per row rather than take them all as of the backdated snapshot.
#[tokio::test]
async fn a_flushed_file_spanning_snapshots_carries_its_partial_max() {
    let (catalog, _, table, _) = seeded().await;
    let first = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 2), &[])?;
            Ok(())
        })
        .await
        .unwrap();
    let second = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("b", 1), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    catalog
        .commit(move |tx| {
            tx.flush_inlined_data(
                table,
                0,
                &[FlushedDataFile {
                    file: DataFile {
                        record_count: 3,
                        ..datafile(3)
                    },
                    row_id_start: 0,
                    begin_snapshot: first,
                    partial_max: Some(second),
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let files = catalog.snapshot().await.unwrap().data_files_of(table);
    assert_eq!(files[0].partial_max, Some(second));
    // Backdated to the earliest all the same: the file is live at the
    // snapshot its oldest rows were inlined at.
    assert_eq!(
        catalog
            .snapshot_at(first)
            .await
            .unwrap()
            .data_files_of(table)
            .len(),
        1
    );
}

/// `partial_max` bounds rows that were already inlined, so it lies at or
/// above the backdated snapshot and below this commit's.
#[tokio::test]
async fn a_partial_max_outside_the_drained_snapshots_is_refused() {
    let (catalog, _, table, _) = seeded().await;
    let inlined = catalog
        .commit(|tx| {
            tx.inline_insert(table, &chunk("a", 1), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    for partial_max in [
        SnapshotId::new(inlined.get() - 1),
        SnapshotId::new(u64::MAX),
    ] {
        let err = catalog
            .commit(move |tx| {
                tx.flush_inlined_data(
                    table,
                    0,
                    &[FlushedDataFile {
                        file: DataFile {
                            record_count: 1,
                            ..datafile(1)
                        },
                        row_id_start: 0,
                        begin_snapshot: inlined,
                        partial_max: Some(partial_max),
                    }],
                )?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Constraint(_)), "{err}");
    }
}
