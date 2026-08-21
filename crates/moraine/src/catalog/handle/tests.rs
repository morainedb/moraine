use std::sync::Arc;

use object_store::memory::InMemory;

use crate::{
    catalog::{Catalog, CatalogOptions, TableId, projection},
    store::{
        key::{InlineKey, InlineOperation, Key},
        proto, value,
    },
    transaction::staged::{Cell, RowOperation, StagedTransaction, TableKind},
};

async fn open() -> Catalog {
    Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap()
}

fn snapshot_row(id: u64) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::I64(1),
        Cell::U64(0),
        Cell::U64(1),
        Cell::U64(0),
    ]
}

fn snapshot_changes_row(id: u64, changes_made: &str) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::Str(changes_made.to_string()),
        Cell::Null,
        Cell::Null,
        Cell::Null,
    ]
}

/// Commits one staged batch minting `snapshot`, carrying `ops`.
async fn commit_staged(catalog: &Catalog, snapshot: u64, changes: &str, ops: Vec<RowOperation>) {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_on(catalog, db_tx);
    for op in ops {
        tx.stage(op);
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(snapshot),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(snapshot, changes),
    });
    tx.commit().await.unwrap();
}

/// The first `recent_rows` walk verifies the chunk directory and remembers
/// it; the next serves from it. The proof is divergence: a chunk planted
/// without a locator is visible only to a body scan, so a read that misses
/// it read the directory.
#[tokio::test]
async fn recent_rows_serves_from_a_directory_verified_complete() {
    let catalog = open().await;

    commit_staged(
        &catalog,
        1,
        "inlined_insert:1",
        vec![
            RowOperation::InlineSchema {
                table_id: 1,
                schema_version: 0,
                arrow_schema: b"schema".to_vec(),
            },
            RowOperation::InlineInsert {
                table_id: 1,
                schema_version: 0,
                begin_snapshot: 1,
                row_id_start: 0,
                row_count: 2,
                arrow_body: b"chunk".to_vec(),
            },
        ],
    )
    .await;

    let rows = catalog.recent_rows(TableId::new(1)).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        projection::inline_directory_complete(catalog.projections(), 1),
        "the first walk must verify and remember a complete directory"
    );

    // The divergence: a chunk with no locator, behind the memo's back.
    let tx = catalog.begin_write_tx().await.unwrap();
    tx.put(
        Key::Inline(InlineKey::Live(InlineOperation::Insert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            chunk_seq: 5,
        }))
        .encode(),
        value::encode_value(&proto::InlineChunkValue {
            body: b"rogue".to_vec().into(),
            row_id_start: 10,
            row_count: 2,
            data_file_id: None,
        }),
    )
    .unwrap();
    tx.commit().await.unwrap();

    let rows = catalog.recent_rows(TableId::new(1)).await.unwrap();
    let row_ids: Vec<u64> = rows.iter().map(|row| row.row_id).collect();
    assert_eq!(
        row_ids,
        [0, 1],
        "a directory-served read must not see a chunk only a body scan finds"
    );
    assert_eq!(rows[0].chunk_body.as_slice(), b"chunk");
    assert_eq!(rows[0].arrow_schema.as_slice(), b"schema");

    catalog.close().await.unwrap();
}

/// Two live chunks of one table can end at the same row id: an UPDATE
/// re-inlines a row that another chunk's range still ends on. A locator is
/// keyed by its range end alone, so the second chunk overwrites the first's
/// locator and takes the first's other rows out of every directory-served
/// read — and both completeness checks compare *sets* of ends, so the
/// truncated directory is declared complete and never healed. Regression.
#[tokio::test]
async fn two_chunks_ending_at_one_row_id_keep_both_locators() {
    let catalog = open().await;

    commit_staged(
        &catalog,
        1,
        "inlined_insert:1",
        vec![
            RowOperation::InlineSchema {
                table_id: 1,
                schema_version: 0,
                arrow_schema: b"schema".to_vec(),
            },
            RowOperation::InlineInsert {
                table_id: 1,
                schema_version: 0,
                begin_snapshot: 1,
                row_id_start: 5,
                row_count: 3,
                arrow_body: b"chunk-a".to_vec(),
            },
        ],
    )
    .await;

    // The re-inlined row 7: its own chunk, ending where chunk A ends.
    commit_staged(
        &catalog,
        2,
        "inlined_insert:1",
        vec![RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 2,
            row_id_start: 7,
            row_count: 1,
            arrow_body: b"chunk-b".to_vec(),
        }],
    )
    .await;

    // The first walk verifies the directory and remembers it; the next
    // serves from it, so the two must agree.
    let walked: Vec<u64> = catalog
        .recent_rows(TableId::new(1))
        .await
        .unwrap()
        .iter()
        .map(|row| row.row_id)
        .collect();
    let served: Vec<u64> = catalog
        .recent_rows(TableId::new(1))
        .await
        .unwrap()
        .iter()
        .map(|row| row.row_id)
        .collect();

    assert_eq!(
        walked, served,
        "a directory-served read lost rows the body scan found"
    );
    assert!(
        served.contains(&5) && served.contains(&6),
        "a colliding locator took chunk A's other rows out: {served:?}"
    );
    // The point of keying a locator by chunk identity: both chunks are
    // named, so the directory is complete rather than merely declining to
    // claim completeness.
    assert!(
        projection::inline_directory_complete(catalog.projections(), 1),
        "both chunks must keep their own locator"
    );

    catalog.close().await.unwrap();
}

/// A directory-served `recent_rows` point-reads only the chunks its live
/// rows reference: a chunk whose rows are all tombstoned is never fetched.
/// The proof deletes that chunk's body out from under its locator — a
/// fetch would report corruption, so a clean read never issued one.
#[tokio::test]
async fn recent_rows_reads_only_the_chunks_live_rows_reference() {
    let catalog = open().await;

    commit_staged(
        &catalog,
        1,
        "inlined_insert:1",
        vec![
            RowOperation::InlineSchema {
                table_id: 1,
                schema_version: 0,
                arrow_schema: b"schema".to_vec(),
            },
            RowOperation::InlineInsert {
                table_id: 1,
                schema_version: 0,
                begin_snapshot: 1,
                row_id_start: 0,
                row_count: 2,
                arrow_body: b"chunk-a".to_vec(),
            },
        ],
    )
    .await;
    commit_staged(
        &catalog,
        2,
        "inlined_insert:1",
        vec![RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 2,
            row_id_start: 2,
            row_count: 2,
            arrow_body: b"chunk-b".to_vec(),
        }],
    )
    .await;
    commit_staged(
        &catalog,
        3,
        "inlined_delete:1",
        vec![
            RowOperation::InlineInlineDelete {
                table_id: 1,
                row_id: 0,
                end_snapshot: 3,
            },
            RowOperation::InlineInlineDelete {
                table_id: 1,
                row_id: 1,
                end_snapshot: 3,
            },
        ],
    )
    .await;

    let rows = catalog.recent_rows(TableId::new(1)).await.unwrap();
    assert_eq!(rows.len(), 2, "chunk-a's rows are tombstoned");
    assert!(
        projection::inline_directory_complete(catalog.projections(), 1),
        "the first walk must verify and remember a complete directory"
    );

    // Remove the dead chunk's body, leaving its locator: only a read that
    // skips unreferenced chunks survives this.
    let tx = catalog.begin_write_tx().await.unwrap();
    tx.delete(
        Key::Inline(InlineKey::Live(InlineOperation::Insert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            chunk_seq: 0,
        }))
        .encode(),
    )
    .unwrap();
    tx.commit().await.unwrap();

    let rows = catalog.recent_rows(TableId::new(1)).await.unwrap();
    let row_ids: Vec<u64> = rows.iter().map(|row| row.row_id).collect();
    assert_eq!(row_ids, [2, 3]);
    assert_eq!(rows[0].chunk_body.as_slice(), b"chunk-b");

    catalog.close().await.unwrap();
}
