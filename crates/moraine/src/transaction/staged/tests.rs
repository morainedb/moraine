use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{layer::Context, prelude::*};

use super::*;
use crate::catalog::{Catalog, CatalogOptions};

fn schema_row(id: u64, name: &str, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::Str(format!("uuid-{id}")),
        Cell::U64(begin),
        Cell::Null,
        Cell::Str(name.to_string()),
        Cell::Str(format!("{name}/")),
        Cell::Bool(true),
    ]
}

fn table_row(id: u64, schema_id: u64, name: &str, begin: u64, end: Option<u64>) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::Str(format!("uuid-t{id}")),
        Cell::U64(begin),
        end.map_or(Cell::Null, Cell::U64),
        Cell::U64(schema_id),
        Cell::Str(name.to_string()),
        Cell::Str(format!("{name}/")),
        Cell::Bool(true),
    ]
}

fn column_row(table_id: u64, column_id: u64, name: &str, order: u64) -> Vec<Cell> {
    vec![
        Cell::U64(column_id),
        Cell::U64(0),
        Cell::Null,
        Cell::U64(table_id),
        Cell::U64(order),
        Cell::Str(name.to_string()),
        Cell::Str("BIGINT".to_string()),
        Cell::Null,
        Cell::Null,
        Cell::Bool(true),
        Cell::Null,
        Cell::Null,
        Cell::Null,
    ]
}

fn snapshot_row(id: u64, schema_version: u64, next_catalog_id: u64) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::I64(1),
        Cell::U64(schema_version),
        Cell::U64(next_catalog_id),
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

async fn open() -> Catalog {
    Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap()
}

#[derive(Clone, Default)]
struct CapturedCommitEvents(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

impl<S> tracing_subscriber::Layer<S> for CapturedCommitEvents
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        #[derive(Default)]
        struct Fields(BTreeMap<String, String>);

        impl Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name().to_owned(), format!("{value:?}"));
            }

            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                self.0.insert(field.name().to_owned(), value.to_string());
            }

            fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
                self.0.insert(field.name().to_owned(), value.to_string());
            }
        }

        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.0.get("message").is_some_and(|message| {
            message == "scanned committed entities for staged transaction"
                || message == "staged commit landed"
        }) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(fields.0);
        }
    }
}

impl CapturedCommitEvents {
    fn one(&self, message: &str, transaction_id: &str) -> BTreeMap<String, String> {
        let matching = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|fields| {
                fields.get("message").is_some_and(|value| value == message)
                    && fields
                        .get("transaction_id")
                        .is_some_and(|value| value == transaction_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "events for {message}: {matching:?}");
        matching.into_iter().next().unwrap()
    }

    fn phase_milliseconds(&self, transaction_id: u64, phase: &str) -> f64 {
        self.one("staged commit landed", &transaction_id.to_string())
            .get(phase)
            .unwrap_or_else(|| panic!("commit event has no {phase}"))
            .parse()
            .unwrap_or_else(|err| panic!("commit event has invalid {phase}: {err}"))
    }
}

fn captured_commit_events() -> &'static CapturedCommitEvents {
    static EVENTS: OnceLock<CapturedCommitEvents> = OnceLock::new();
    EVENTS.get_or_init(|| {
        let events = CapturedCommitEvents::default();
        let subscriber = tracing_subscriber::registry().with(events.clone());
        assert!(
            tracing::subscriber::set_global_default(subscriber).is_ok(),
            "the unit test process installs only this tracing subscriber"
        );
        events
    })
}

/// One identifier joins the pre-commit entity scan to the eventual commit,
/// and the two events expose every phase needed to account for wall time.
#[tokio::test]
async fn staged_commit_diagnostics_join_scan_counts_and_commit_phases() {
    let events = captured_commit_events();

    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    let transaction_id = tx.diagnostic_id.to_string();
    tx.visible_tables().await.unwrap();
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    let scan = events.one(
        "scanned committed entities for staged transaction",
        &transaction_id,
    );
    assert!(scan.contains_key("current_records"));
    assert!(scan.contains_key("history_records"));
    assert!(scan.contains_key("records"));

    let commit = events.one("staged commit landed", &transaction_id);
    assert_eq!(scan.get("transaction_id"), commit.get("transaction_id"));
    for phase in [
        "head_view_ms",
        "inline_ms",
        "index_maintenance_ms",
        "translate_ms",
        "stage_ms",
        "land_ms",
        "durable_ms",
        "projection_ms",
    ] {
        assert!(commit.contains_key(phase), "missing {phase}: {commit:?}");
    }

    catalog.close().await.unwrap();
}

/// A DuckLake-shaped snapshot bump plus table create: table `t` (id
/// 1, schema 0 = bootstrap's `main`) with one column, staged and
/// committed as one batch, then verified through the ordinary
/// snapshot read (the same view the dump ABI serves).
#[tokio::test]
async fn stages_table_create_and_snapshot_bump() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });

    let id = tx.commit().await.unwrap();
    assert_eq!(id.get(), 1);

    let snapshot = catalog.snapshot().await.unwrap();
    let tables = snapshot.tables_in(crate::catalog::SchemaId::new(0));
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "t");
    let cols = snapshot.columns_of(tables[0].id);
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].name, "a");
}

/// Staged column inserts advance the table's field-id counter: a later
/// verb `add_column` — even after the highest staged column is dropped
/// — must never re-allocate a DuckLake-authored field id.
#[tokio::test]
async fn staged_columns_advance_the_field_id_counter() {
    use crate::catalog::{ColumnDef, ColumnId, SchemaId, TableId};

    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 2, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 5, "b", 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    // Drop the max-id column, then add: the freed id must not return.
    let table = TableId::new(1);
    catalog
        .commit(|tx| tx.drop_column(table, ColumnId::new(5)))
        .await
        .unwrap();
    let added = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            added.set(Some(tx.add_column(
                table,
                &ColumnDef {
                    name: "c".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                    children: Vec::new(),
                },
            )?));
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        added.get(),
        Some(ColumnId::new(6)),
        "field id 5 must not be reused"
    );

    // The counter-only table update stayed in place: one live table
    // row for snapshot 1's create, no counter-minted history version.
    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(snapshot.tables_in(SchemaId::new(0)).len(), 1);
}

/// DuckLake's UPDATE only names rows it read: an `UpdateSetEnd` for a
/// row that is not live is drift and must fail the commit loudly, not
/// pass as a silent no-op that drops the authored end.
#[tokio::test]
async fn ending_an_absent_row_is_rejected() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    // No table 7 exists; end it at this commit's snapshot id (1).
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Table,
        cells: vec![Cell::U64(7), Cell::U64(1)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "dropped_table:7"),
    });

    let err = tx.commit().await.unwrap_err();
    assert!(matches!(err, crate::error::Error::Corruption(_)), "{err}");
}

/// DuckLake-authored data-file and delete-file rows carry
/// `encryption_key` through commit and back out of the snapshot read
/// verbatim — the faithful-conduit guarantee for key material.
#[tokio::test]
async fn encryption_keys_round_trip_through_staged_rows() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: vec![
            Cell::U64(1),                     // data_file_id
            Cell::U64(1),                     // table_id
            Cell::U64(1),                     // begin_snapshot
            Cell::Null,                       // end_snapshot
            Cell::Null,                       // file_order
            Cell::Str("data.parquet".into()), // path
            Cell::Bool(true),                 // path_is_relative
            Cell::Str("parquet".into()),      // file_format
            Cell::U64(10),                    // record_count
            Cell::U64(1024),                  // file_size_bytes
            Cell::U64(64),                    // footer_size
            Cell::U64(0),                     // row_id_start
            Cell::Null,                       // partition_id
            Cell::Str("ZGF0YS1rZXk=".into()), // encryption_key
            Cell::Null,                       // mapping_id
            Cell::Null,                       // partial_max
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: vec![
            Cell::U64(2),                         // delete_file_id
            Cell::U64(1),                         // table_id
            Cell::U64(1),                         // begin_snapshot
            Cell::Null,                           // end_snapshot
            Cell::U64(1),                         // data_file_id
            Cell::Str("delete.parquet".into()),   // path
            Cell::Bool(true),                     // path_is_relative
            Cell::Str("parquet".into()),          // format
            Cell::U64(2),                         // delete_count
            Cell::U64(128),                       // file_size_bytes
            Cell::U64(32),                        // footer_size
            Cell::Str("ZGVsZXRlLWtleQ==".into()), // encryption_key
            Cell::Null,                           // partial_max
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let table = head.tables_in(crate::catalog::SchemaId::new(0))[0].id;
    assert_eq!(
        head.data_files_of(table)[0].encryption_key.as_deref(),
        Some("ZGF0YS1rZXk=")
    );
    assert_eq!(
        head.delete_files_of(table)[0].encryption_key.as_deref(),
        Some("ZGVsZXRlLWtleQ==")
    );
}

/// An `UPDATE ... SET end_snapshot` row ends a live table version:
/// the old row moves to `history`, the new one lands in `current`, exactly
/// the lifecycle convention this path interprets.
#[tokio::test]
async fn update_set_end_moves_the_old_version_to_history() {
    let catalog = open().await;

    // Seed schema `s` (id 1) and table `t` (id 1) via a plain insert.
    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::Insert {
        table: TableKind::Schema,
        cells: schema_row(1, "s", 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 1, "t_old", 1, None),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_schema:"s""#),
    });
    setup.commit().await.unwrap();

    // Rename: end the old table version, insert the renamed one.
    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut rename = StagedTransaction::begin_detached(db_tx2);
    rename.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Table,
        cells: vec![Cell::U64(1), Cell::U64(2)],
    });
    rename.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 1, "t_new", 2, None),
    });
    rename.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    rename.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    rename.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert_eq!(
        head.tables_in(crate::catalog::SchemaId::new(1))[0].name,
        "t_new"
    );

    let past = catalog
        .snapshot_at(crate::catalog::SnapshotId::new(1))
        .await
        .unwrap();
    assert_eq!(
        past.tables_in(crate::catalog::SchemaId::new(1))[0].name,
        "t_old"
    );
}

/// A rename staged in DuckLake's live order — the new version's
/// insert *before* the old version's end — keeps the new version
/// live. Translation applies ends before inserts, so the shared `current`
/// key resolves to the insert regardless of stage order.
#[tokio::test]
async fn rename_survives_insert_before_end_order() {
    let catalog = open().await;

    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::Insert {
        table: TableKind::Schema,
        cells: schema_row(1, "s", 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 1, "t_old", 1, None),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_schema:"s""#),
    });
    setup.commit().await.unwrap();

    // Insert the renamed version first, then end the old one — the
    // reverse of the safe order, matching what DuckLake emits.
    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut rename = StagedTransaction::begin_detached(db_tx2);
    rename.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 1, "t_new", 2, None),
    });
    rename.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Table,
        cells: vec![Cell::U64(1), Cell::U64(2)],
    });
    rename.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    rename.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    rename.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let live = head.tables_in(crate::catalog::SchemaId::new(1));
    assert_eq!(live.len(), 1, "exactly one live table after rename");
    assert_eq!(live[0].name, "t_new");
}

/// A lost race at commit is never retried: the loser's error carries
/// the literal substring `conflict`, and the store is left exactly as
/// the winner left it.
#[tokio::test]
async fn lost_race_is_not_retried_and_carries_conflict_text() {
    let catalog = open().await;

    let tx_a = catalog.begin_write_tx().await.unwrap();
    let tx_b = catalog.begin_write_tx().await.unwrap();
    let mut a = StagedTransaction::begin_detached(tx_a);
    let mut b = StagedTransaction::begin_detached(tx_b);

    for (tx, name) in [(&mut a, "a"), (&mut b, "b")] {
        tx.stage(RowOperation::Insert {
            table: TableKind::Schema,
            cells: schema_row(1, name, 1),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, format!(r#"created_schema:"{name}""#).as_str()),
        });
    }

    a.commit().await.unwrap();
    let err = b.commit().await.unwrap_err();
    assert!(
        err.to_string().contains("conflict"),
        "error must carry the literal substring `conflict`: {err}"
    );

    // The store reflects only the winner: schema `a`, not `b`.
    let head = catalog.snapshot().await.unwrap();
    assert!(head.schema_by_name("a").is_some());
    assert!(head.schema_by_name("b").is_none());
}

/// A malformed staged row (wrong cell count) fails loudly as
/// `Corruption` rather than panicking or silently truncating.
#[tokio::test]
async fn malformed_row_is_corruption_not_a_panic() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Schema,
        cells: vec![Cell::U64(1)], // far too few cells
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, ""),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(matches!(err, Error::Corruption(_)));
}

/// Stages an inline schema plus two inserts against the same
/// `(table_id, schema_version, begin_snapshot)` in one commit: the
/// chunks land with sequential `chunk_seq` (stage order), and the
/// schema is readable back verbatim.
#[tokio::test]
async fn stages_inline_schema_and_sequential_inserts() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    tx.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 0,
        arrow_schema: b"schema".to_vec(),
    });
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 1,
        row_id_start: 0,
        row_count: 2,
        arrow_body: b"chunk-a".to_vec(),
    });
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 1,
        row_id_start: 2,
        row_count: 1,
        arrow_body: b"chunk-b".to_vec(),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 0, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "inlined_insert:1"),
    });
    tx.commit().await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(
        chunks[0].0,
        InlineOperation::Insert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            chunk_seq: 0,
        }
    );
    assert_eq!(chunks[0].1.body, b"chunk-a");
    assert_eq!(chunks[0].1.row_id_start, 0);
    assert_eq!(chunks[0].1.row_count, 2);
    assert_eq!(
        chunks[1].0,
        InlineOperation::Insert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            chunk_seq: 1,
        }
    );
    assert_eq!(chunks[1].1.body, b"chunk-b");

    let schemas = store_inline::scan_inline_schemas(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    assert_eq!(
        schemas,
        vec![(
            0,
            proto::InlineSchemaValue {
                arrow_schema: b"schema".to_vec(),
            }
        )]
    );
    tx.rollback();
}

/// An `InlineIdel` tombstones a row: the row is absent from a
/// `Table`-kind materialization at or after its `end_snapshot`.
#[tokio::test]
async fn stages_inline_idel_and_row_disappears_from_table_scan_after_it() {
    let catalog = open().await;

    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 1,
        row_id_start: 0,
        row_count: 2,
        arrow_body: b"chunk".to_vec(),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 0, 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "inlined_insert:1"),
    });
    setup.commit().await.unwrap();

    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut inline_delete = StagedTransaction::begin_detached(db_tx2);
    inline_delete.stage(RowOperation::InlineInlineDelete {
        table_id: 1,
        row_id: 0,
        end_snapshot: 2,
    });
    inline_delete.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 0, 1),
    });
    inline_delete.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "inlined_delete:1"),
    });
    inline_delete.commit().await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    let inline_deletes = store_inline::scan_inline_inline_deletes(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    tx.rollback();
    assert_eq!(
        inline_deletes,
        vec![(0, proto::InlineInlineDeleteValue { end_snapshot: 2 })]
    );

    let rows = materialize_inline_rows(&chunks, &inline_deletes);
    assert_eq!(
        InlineScanKind::Table
            .select(&rows, 1, 0)
            .iter()
            .map(|r| r.row_id)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        InlineScanKind::Table
            .select(&rows, 2, 0)
            .iter()
            .map(|r| r.row_id)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

/// The schema-only Arrow IPC stream stored once per inline schema
/// version, matching what the extension's encoder produces.
fn inline_schema_ipc(schema: &arrow::datatypes::Schema) -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, schema).unwrap();
        writer.finish().unwrap();
    }
    buffer
}

/// One inline chunk body: a little-endian `u32` message length, the
/// record-batch message, then the arrow data buffers.
fn inline_body(batch: &arrow::array::RecordBatch) -> Vec<u8> {
    use arrow::ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
    };

    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);
    let options = IpcWriteOptions::default();
    let mut context = IpcWriteContext::default();
    let (dictionaries, encoded) = generator
        .encode(batch, &mut tracker, &options, &mut context)
        .unwrap();
    assert!(dictionaries.is_empty(), "test bodies carry no dictionaries");

    let mut buffer = Vec::new();
    buffer.extend_from_slice(
        &u32::try_from(encoded.ipc_message.len())
            .unwrap()
            .to_le_bytes(),
    );
    buffer.extend_from_slice(&encoded.ipc_message);
    buffer.extend_from_slice(&encoded.arrow_data);
    buffer
}

/// A one-column `BIGINT` batch, the shape the inline index tests insert.
fn bigint_batch(values: &[i64]) -> (arrow::datatypes::Schema, arrow::array::RecordBatch) {
    use arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };

    let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap();
    (schema, batch)
}

/// A single-`BIGINT`-column batch whose `None`s are SQL NULL.
fn nullable_bigint_batch(
    values: &[Option<i64>],
) -> (arrow::datatypes::Schema, arrow::array::RecordBatch) {
    use arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };

    let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap();
    (schema, batch)
}

/// Creates table 1 with one `BIGINT` column and an equality index over
/// it, returning the catalog and the index id.
async fn catalog_with_indexed_inline_table(unique: bool) -> (Catalog, u64) {
    use crate::catalog::{IndexDef, TableId};

    let catalog = open().await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx);
    setup.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "created_table:1"),
    });
    setup.commit().await.unwrap();

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                TableId::new(1),
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![crate::catalog::ColumnId::new(1)],
                    unique,
                },
                &[],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();

    (catalog, index.get().unwrap().get())
}

/// Every stored entry key of one index, in scan order.
async fn index_entry_keys(catalog: &Catalog, unique: bool, index_id: u64) -> Vec<Vec<u8>> {
    use crate::store::key::{IndexKind, index_index_prefix};

    let kind = if unique {
        IndexKind::Unique
    } else {
        IndexKind::Multi
    };
    let tx = catalog.begin_write_tx().await.unwrap();
    let mut iter = ReadHandle::Tx(&tx)
        .scan_prefix(
            index_index_prefix(kind, index_id),
            ..,
            crate::store::handle::ScanShape::Bulk,
        )
        .await
        .unwrap();
    let mut keys = Vec::new();
    while let Some(entry) = iter.next().await.unwrap() {
        keys.push(entry.key.to_vec());
    }
    tx.rollback();
    keys
}

/// The stored entry count for one index.
async fn index_entry_count(catalog: &Catalog, unique: bool, index_id: u64) -> usize {
    index_entry_keys(catalog, unique, index_id).await.len()
}

/// Stages one inline chunk of `values` starting at `row_id_start`,
/// registering the schema on the first call.
async fn inline_insert(
    catalog: &Catalog,
    snapshot_id: u64,
    row_id_start: u64,
    values: &[i64],
    with_schema: bool,
) {
    let (schema, batch) = bigint_batch(values);
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    if with_schema {
        tx.stage(RowOperation::InlineSchema {
            table_id: 1,
            schema_version: 0,
            arrow_schema: inline_schema_ipc(&schema),
        });
    }
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: snapshot_id,
        row_id_start,
        row_count: u64::try_from(values.len()).unwrap(),
        arrow_body: inline_body(&batch),
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

/// Tombstones one inlined row in its own commit.
async fn inline_row_delete(catalog: &Catalog, snapshot_id: u64, row_id: u64) -> Result<()> {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::InlineInlineDelete {
        table_id: 1,
        row_id,
        end_snapshot: snapshot_id,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(snapshot_id, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(snapshot_id, "inlined_delete:1"),
    });
    tx.commit().await.map(|_| ())
}

/// Deleting an inlined row removes its unique index entry, so the value
/// is free to be inserted again — entries are live-only.
#[tokio::test]
async fn inline_row_delete_removes_its_unique_index_entry() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;

    inline_insert(&catalog, 3, 0, &[7], true).await;
    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        1,
        "the insert lands one entry"
    );

    inline_row_delete(&catalog, 4, 0).await.unwrap();
    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        0,
        "the delete removes the entry it killed"
    );
}

/// The replace pattern a writer depends on: delete a row, then insert
/// the same unique value again in a later commit.
#[tokio::test]
async fn inline_delete_then_reinsert_admits_the_same_unique_value() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;

    inline_insert(&catalog, 3, 0, &[7], true).await;
    inline_row_delete(&catalog, 4, 0).await.unwrap();
    inline_insert(&catalog, 5, 1, &[7], false).await;

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        1,
        "the value is indexed again, held by the new row"
    );
}

/// Delete and reinsert of one unique value inside a single commit: the
/// removal is staged before the put, so the value reads as absent.
#[tokio::test]
async fn inline_delete_and_reinsert_in_one_commit_admits_the_same_unique_value() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;

    inline_insert(&catalog, 3, 0, &[7], true).await;

    let (_, batch) = bigint_batch(&[7]);
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::InlineInlineDelete {
        table_id: 1,
        row_id: 0,
        end_snapshot: 4,
    });
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 4,
        row_id_start: 1,
        row_count: 1,
        arrow_body: inline_body(&batch),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "inlined_insert:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        1,
        "the reinserted value holds exactly one entry"
    );
}

/// A non-unique index leaks differently: a stale entry does not block
/// writes, it makes a lookup resolve a row id that no longer exists.
#[tokio::test]
async fn inline_row_delete_removes_its_non_unique_index_entry() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(false).await;

    inline_insert(&catalog, 3, 0, &[7, 7], true).await;
    assert_eq!(
        index_entry_count(&catalog, false, index_id).await,
        2,
        "both rows share the value under a non-unique index"
    );

    inline_row_delete(&catalog, 4, 0).await.unwrap();
    assert_eq!(
        index_entry_count(&catalog, false, index_id).await,
        1,
        "only the surviving row is still indexed"
    );
}

/// Writes `batch` to `path` on `store` as Parquet, returning the
/// written object's size — the maintenance read locates the footer by
/// the recorded `file_size_bytes`, so fixtures must record the truth,
/// exactly as DuckLake records the real written size.
async fn write_parquet(store: &InMemory, path: &str, batch: &arrow::array::RecordBatch) -> u64 {
    use object_store::ObjectStoreExt;

    let mut buffer = Vec::new();
    {
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    let object_len = u64::try_from(buffer.len()).unwrap();
    store
        .put(&object_store::path::Path::from(path), buffer.into())
        .await
        .unwrap();
    object_len
}

/// A `ducklake_data_file` row for a file of `record_count` rows and
/// `file_size_bytes` bytes on the store.
fn indexed_data_file_row(record_count: u64, file_size_bytes: u64) -> Vec<Cell> {
    indexed_data_file_row_at(1, "data.parquet", record_count, file_size_bytes, 0)
}

/// As [`indexed_data_file_row`], for one of several files a commit
/// registers: its own id, path, and dense row-id range.
fn indexed_data_file_row_at(
    data_file_id: u64,
    path: &str,
    record_count: u64,
    file_size_bytes: u64,
    row_id_start: u64,
) -> Vec<Cell> {
    vec![
        Cell::U64(data_file_id),
        Cell::U64(1),
        Cell::U64(3),
        Cell::Null,
        Cell::Null,
        Cell::Str(path.into()),
        Cell::Bool(true),
        Cell::Str("parquet".into()),
        Cell::U64(record_count),
        Cell::U64(file_size_bytes),
        Cell::U64(64),
        Cell::U64(row_id_start),
        Cell::Null,
        Cell::Null,
        Cell::Null,
        Cell::Null,
    ]
}

/// Registers a Parquet data file of `values` on the indexed table,
/// returning the store it lives on.
async fn register_indexed_data_file(catalog: &Catalog, values: &[i64]) -> Arc<InMemory> {
    let store = Arc::new(InMemory::new());
    let (_, batch) = bigint_batch(values);
    // `s/` and `t/` are the bootstrap schema and table path prefixes.
    let file_size = write_parquet(&store, "main/t/data.parquet", &batch).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: indexed_data_file_row(u64::try_from(values.len()).unwrap(), file_size),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();
    store
}

/// Wraps an [`InMemory`] store, recording the most reads it ever held in
/// flight at once. Every read suspends before it is served, so a caller
/// that issues its reads concurrently holds all of them at once, while one
/// that awaits them in turn never holds more than a single read.
#[derive(Debug)]
struct InFlightStore {
    inner: InMemory,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    reads: AtomicUsize,
    read_delay: std::time::Duration,
}

impl InFlightStore {
    fn new() -> Self {
        Self::with_read_delay(std::time::Duration::ZERO)
    }

    fn with_read_delay(read_delay: std::time::Duration) -> Self {
        Self {
            inner: InMemory::new(),
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            read_delay,
        }
    }

    fn peak_in_flight(&self) -> usize {
        self.peak_in_flight.load(Ordering::Relaxed)
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }

    /// Forgets the reads a fixture's own setup issued, so a measurement
    /// covers only the commit under test.
    fn reset(&self) {
        self.peak_in_flight.store(0, Ordering::Relaxed);
        self.reads.store(0, Ordering::Relaxed);
    }
}

impl std::fmt::Display for InFlightStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InFlightStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for InFlightStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // A `head` probe carries no payload and is not a read.
        if options.head {
            return self.inner.get_opts(location, options).await;
        }

        self.reads.fetch_add(1, Ordering::Relaxed);
        let held = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak_in_flight.fetch_max(held, Ordering::AcqRel);
        // The suspension point a real store's round trip would have: a
        // concurrent caller reaches it on every read before any completes.
        if self.read_delay.is_zero() {
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(self.read_delay).await;
        }
        let result = self.inner.get_opts(location, options).await;
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

const CONTROLLED_READ_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

fn assert_index_phase_covers_read_waves(milliseconds: f64, waves: usize) {
    let waves = u32::try_from(waves).unwrap();
    let minimum = CONTROLLED_READ_DELAY.as_secs_f64() * 1_000.0 * f64::from(waves);
    assert!(
        milliseconds >= minimum,
        "index maintenance took {milliseconds:.3} ms, below {waves} controlled read waves \
         ({minimum:.3} ms)"
    );
}

/// Registers `files` Parquet data files of `rows_per_file` rows each on
/// the indexed table, in one commit minting snapshot 3. Values are
/// distinct throughout, so a unique index admits every row, and each file
/// carries a dense row-id range of its own — file `n` is `f<n>.parquet`,
/// data file id `n + 1`.
async fn register_indexed_data_files(
    catalog: &Catalog,
    store: &Arc<InFlightStore>,
    files: usize,
    rows_per_file: usize,
) -> u64 {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    let transaction_id = tx.diagnostic_id;
    for file in 0..files {
        let first = file * rows_per_file;
        let values: Vec<i64> = (first..first + rows_per_file)
            .map(|value| i64::try_from(value).unwrap())
            .collect();
        let (_, batch) = bigint_batch(&values);
        let name = format!("f{file}.parquet");
        let size = write_parquet(&store.inner, &format!("main/t/{name}"), &batch).await;

        tx.stage(RowOperation::Insert {
            table: TableKind::DataFile,
            cells: indexed_data_file_row_at(
                u64::try_from(file).unwrap() + 1,
                &name,
                u64::try_from(rows_per_file).unwrap(),
                size,
                u64::try_from(first).unwrap(),
            ),
        });
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 20),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();
    transaction_id
}

/// A DuckLake delete file naming `positions` in `target`.
async fn write_delete_file(store: &InMemory, name: &str, target: &str, positions: &[usize]) -> u64 {
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![target; positions.len()])),
            Arc::new(Int64Array::from(
                positions
                    .iter()
                    .map(|position| i64::try_from(*position).unwrap())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    write_parquet(store, &format!("main/t/{name}"), &batch).await
}

/// A `ducklake_delete_file` row for one of several files a commit
/// registers against `data_file_id`.
fn delete_file_row_at(
    delete_file_id: u64,
    path: &str,
    data_file_id: u64,
    delete_count: u64,
    file_size_bytes: u64,
) -> Vec<Cell> {
    vec![
        Cell::U64(delete_file_id),
        Cell::U64(1),
        Cell::U64(4),
        Cell::Null,
        Cell::U64(data_file_id),
        Cell::Str(path.into()),
        Cell::Bool(true),
        Cell::Str("parquet".into()),
        Cell::U64(delete_count),
        Cell::U64(file_size_bytes),
        Cell::U64(64),
        Cell::Null,
        Cell::Null,
    ]
}

/// A commit registering several data files scoped-reads them concurrently.
/// Each read is an independent fetch of an independent file, so a commit
/// that awaited them in turn would cost one round trip of store latency
/// per file — the peak reads in flight is that difference made visible.
#[tokio::test]
async fn registering_many_data_files_reads_them_concurrently() {
    const FILES: usize = 8;
    const ROWS_PER_FILE: usize = 3;

    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
    let store = Arc::new(InFlightStore::new());
    register_indexed_data_files(&catalog, &store, FILES, ROWS_PER_FILE).await;

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        FILES * ROWS_PER_FILE,
        "every registered file's rows are indexed"
    );
    assert_eq!(
        store.peak_in_flight(),
        FILES,
        "every file's scoped read is in flight at once"
    );
}

/// A commit registering several delete files reads them concurrently too:
/// collecting the positions they kill is one independent fetch apiece.
///
/// Every delete file targets the same data file, so resolving the killed
/// positions to their values costs a single scoped read — leaving the
/// delete files' own reads as the only ones that can overlap.
#[tokio::test]
async fn registering_many_delete_files_reads_them_concurrently() {
    const DELETES: usize = 8;

    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
    let store = Arc::new(InFlightStore::new());
    register_indexed_data_files(&catalog, &store, 1, DELETES).await;
    store.reset();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    for position in 0..DELETES {
        let name = format!("d{position}.parquet");
        let size = write_delete_file(&store.inner, &name, "f0.parquet", &[position]).await;
        tx.stage(RowOperation::Insert {
            table: TableKind::DeleteFile,
            cells: delete_file_row_at(u64::try_from(position).unwrap() + 2, &name, 1, 1, size),
        });
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 20),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "deleted_from_table:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        0,
        "every position is killed, so no entry survives"
    );
    assert_eq!(
        store.peak_in_flight(),
        DELETES,
        "every delete file's read is in flight at once"
    );
}

/// Deletes landing on several already-committed data files scoped-read
/// those files concurrently: each target is read to resolve its killed
/// positions to the values their entries are keyed by, and no target
/// depends on another.
///
/// Inlined file-deletes carry their target and position outright, so
/// collecting them reads nothing — leaving the targets' own reads as the
/// only ones that can overlap.
#[tokio::test]
async fn deletes_against_many_data_files_read_them_concurrently() {
    const FILES: usize = 8;

    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
    let store = Arc::new(InFlightStore::new());
    register_indexed_data_files(&catalog, &store, FILES, 1).await;
    store.reset();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    for file in 0..FILES {
        tx.stage(RowOperation::InlineFileDelete {
            table_id: 1,
            data_file_id: u64::try_from(file).unwrap() + 1,
            row_id: 0,
            begin_snapshot: 4,
        });
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 20),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "inlined_delete:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        0,
        "each file's only row is killed, so no entry survives"
    );
    assert_eq!(
        store.peak_in_flight(),
        FILES,
        "every target file's scoped read is in flight at once"
    );
}

/// An append to an indexed table reads the newly registered data file once
/// to derive its entries. The fixed-latency store makes that one read wave
/// visible in the phase diagnostic.
#[tokio::test]
async fn append_only_index_maintenance_is_one_data_read_wave() {
    let events = captured_commit_events();
    let (catalog, index_id) = catalog_with_indexed_inline_table(false).await;
    let store = Arc::new(InFlightStore::with_read_delay(CONTROLLED_READ_DELAY));

    let transaction_id = register_indexed_data_files(&catalog, &store, 1, 3).await;
    let milliseconds = events.phase_milliseconds(transaction_id, "index_maintenance_ms");

    assert_eq!(store.reads(), 1, "the registered file is read once");
    assert_index_phase_covers_read_waves(milliseconds, 1);
    assert_eq!(index_entry_count(&catalog, false, index_id).await, 3);
    eprintln!("append-only: reads=1 index_maintenance_ms={milliseconds:.3}");
    catalog.close().await.unwrap();
}

/// A delete file on an indexed table costs two serial data-store waves: one
/// to obtain the killed positions and one to recover their old index values
/// from the committed target file.
#[tokio::test]
async fn delete_only_index_maintenance_is_two_data_read_waves() {
    let events = captured_commit_events();
    let (catalog, index_id) = catalog_with_indexed_inline_table(false).await;
    let store = Arc::new(InFlightStore::with_read_delay(CONTROLLED_READ_DELAY));
    register_indexed_data_files(&catalog, &store, 1, 3).await;
    store.reset();

    let size = write_delete_file(&store.inner, "delete.parquet", "f0.parquet", &[0]).await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    let transaction_id = tx.diagnostic_id;
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: delete_file_row_at(2, "delete.parquet", 1, 1, size),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 20),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "deleted_from_table:1"),
    });
    tx.commit().await.unwrap();
    let milliseconds = events.phase_milliseconds(transaction_id, "index_maintenance_ms");

    assert_eq!(
        store.reads(),
        2,
        "the delete file and its committed target are each read once"
    );
    assert_index_phase_covers_read_waves(milliseconds, 2);
    assert_eq!(index_entry_count(&catalog, false, index_id).await, 2);
    eprintln!("delete-only: reads=2 index_maintenance_ms={milliseconds:.3}");
    catalog.close().await.unwrap();
}

/// A file-backed replacement pays three serial data-store waves: collect the
/// delete positions, derive the new file's entries, then derive removals from
/// the old target.
#[tokio::test]
async fn replace_index_maintenance_is_three_data_read_waves() {
    let events = captured_commit_events();
    let (catalog, index_id) = catalog_with_indexed_inline_table(false).await;
    let store = Arc::new(InFlightStore::with_read_delay(CONTROLLED_READ_DELAY));
    register_indexed_data_files(&catalog, &store, 1, 3).await;
    store.reset();

    let (_, replacement) = bigint_batch(&[10]);
    let replacement_size =
        write_parquet(&store.inner, "main/t/replacement.parquet", &replacement).await;
    let delete_size = write_delete_file(&store.inner, "delete.parquet", "f0.parquet", &[0]).await;
    let mut replacement_row =
        indexed_data_file_row_at(2, "replacement.parquet", 1, replacement_size, 3);
    replacement_row[2] = Cell::U64(4);

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    let transaction_id = tx.diagnostic_id;
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: delete_file_row_at(3, "delete.parquet", 1, 1, delete_size),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: replacement_row,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 20),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "deleted_from_table:1,inserted_into_table:1"),
    });
    tx.commit().await.unwrap();
    let milliseconds = events.phase_milliseconds(transaction_id, "index_maintenance_ms");

    assert_eq!(
        store.reads(),
        3,
        "the delete file, replacement file, and old target are each read once"
    );
    assert_index_phase_covers_read_waves(milliseconds, 3);
    assert_eq!(index_entry_count(&catalog, false, index_id).await, 3);
    eprintln!("replace: reads=3 index_maintenance_ms={milliseconds:.3}");
    catalog.close().await.unwrap();
}

/// A pure compaction preserves row ids and values, so it neither reads the
/// replacement file nor stages index entries.
#[tokio::test]
async fn compaction_only_index_maintenance_reads_no_data() {
    let events = captured_commit_events();
    let (catalog, index_id) = catalog_with_indexed_inline_table(false).await;
    let store = Arc::new(InFlightStore::with_read_delay(CONTROLLED_READ_DELAY));
    register_indexed_data_files(&catalog, &store, 1, 3).await;
    let before = index_entry_keys(&catalog, false, index_id).await;
    store.reset();

    let mut merged = rewrite_data_file_row(12, 4, "merged.parquet", 3, 1024);
    merged[11] = Cell::U64(0);
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    let transaction_id = tx.diagnostic_id;
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: merged,
    });
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(1), Cell::U64(4)],
    });
    tx.stage(RowOperation::UpdateSetBegin {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(12), Cell::U64(4)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 20),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "merge_adjacent:1"),
    });
    tx.commit().await.unwrap();
    let milliseconds = events.phase_milliseconds(transaction_id, "index_maintenance_ms");

    assert_eq!(store.reads(), 0, "compaction never reads the merged file");
    assert_eq!(index_entry_keys(&catalog, false, index_id).await, before);
    eprintln!("compaction-only: reads=0 index_maintenance_ms={milliseconds:.3}");
    catalog.close().await.unwrap();
}

/// Writes a two-column Parquet of `values` beside their preserved
/// `row_ids`, the trailing row-id column tagged with DuckDB's reserved
/// field id — the shape DuckLake's rewrite and flush writers emit.
async fn write_parquet_with_row_ids(
    store: &InMemory,
    path: &str,
    values: &[i64],
    row_ids: &[i64],
) -> u64 {
    use arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };

    let row_id_field = Field::new("_ducklake_internal_row_id", DataType::Int64, false)
        .with_metadata(std::collections::HashMap::from([(
            parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
            "2147483540".to_string(),
        )]));
    let schema = Schema::new(vec![Field::new("a", DataType::Int64, true), row_id_field]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(values.to_vec())),
            Arc::new(Int64Array::from(row_ids.to_vec())),
        ],
    )
    .unwrap();
    write_parquet(store, path, &batch).await
}

/// A `ducklake_data_file` row for a rewrite file: per-row ids
/// (`row_id_start` NULL), otherwise as `indexed_data_file_row`.
fn rewrite_data_file_row(
    data_file_id: u64,
    begin: u64,
    path: &str,
    record_count: u64,
    file_size_bytes: u64,
) -> Vec<Cell> {
    vec![
        Cell::U64(data_file_id),
        Cell::U64(1),
        Cell::U64(begin),
        Cell::Null,
        Cell::Null,
        Cell::Str(path.into()),
        Cell::Bool(true),
        Cell::Str("parquet".into()),
        Cell::U64(record_count),
        Cell::U64(file_size_bytes),
        Cell::U64(64),
        Cell::Null, // row_id_start: this file carries per-row ids
        Cell::Null,
        Cell::Null,
        Cell::Null,
        Cell::Null,
    ]
}

/// A commit that re-registers rows in a per-row-id file derives entries
/// that already exist: the commit lands and the `index` range is
/// byte-identical. Marked as an append, the derivation the compaction
/// kinds skip below.
#[tokio::test]
async fn per_row_id_registration_re_derives_entries_idempotently() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
    let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;
    let before = index_entry_keys(&catalog, true, index_id).await;
    assert_eq!(before.len(), 3);

    // The rewrite: same values, preserved ids 0..=2, per-row-id file 12
    // replacing file 1.
    let size =
        write_parquet_with_row_ids(&store, "main/t/rewrite.parquet", &[10, 20, 30], &[0, 1, 2])
            .await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: rewrite_data_file_row(12, 4, "rewrite.parquet", 3, size),
    });
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(1), Cell::U64(4)],
    });
    tx.stage(RowOperation::UpdateSetBegin {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(12), Cell::U64(4)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_keys(&catalog, true, index_id).await,
        before,
        "the index range is byte-identical: re-derived entries are no-ops"
    );
}

/// Stages a compaction-shaped commit under `changes`: file 12 replaces
/// file 1, carrying per-row ids unless `row_id_start` gives it a dense
/// range. The replacement is never written to the store, so a commit
/// that reads it to derive entries fails and one that skips it lands.
async fn commit_compaction(
    catalog: &Catalog,
    store: &Arc<InMemory>,
    changes: &str,
    row_id_start: Option<u64>,
) -> Result<SnapshotId> {
    let mut cells = rewrite_data_file_row(12, 4, "merged.parquet", 3, 1024);
    if let Some(start) = row_id_start {
        cells[11] = Cell::U64(start);
    }

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells,
    });
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(1), Cell::U64(4)],
    });
    tx.stage(RowOperation::UpdateSetBegin {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(12), Cell::U64(4)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, changes),
    });
    tx.commit().await
}

/// Compaction re-homes rows without renumbering or rewriting them, so
/// every entry it would derive is already stored under the same key.
/// The commit stages no index work at all: it does not even read the
/// file it registers, which is what keeps a merge of a large indexed
/// table under the per-commit entry limit.
#[tokio::test]
async fn compaction_registration_stages_no_index_work() {
    for (changes, row_id_start) in [
        // A merge of adjacent ranges keeps the dense range it merged.
        ("merge_adjacent:1", Some(0)),
        // A rewrite carries the ids it preserved per row.
        ("rewrite_delete:1", None),
    ] {
        let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
        let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;
        let before = index_entry_keys(&catalog, true, index_id).await;
        assert_eq!(before.len(), 3);

        commit_compaction(&catalog, &store, changes, row_id_start)
            .await
            .unwrap_or_else(|err| panic!("{changes} must not read the file it registers: {err}"));

        assert_eq!(
            index_entry_keys(&catalog, true, index_id).await,
            before,
            "{changes} leaves the index range untouched"
        );
        catalog.close().await.unwrap();
    }
}

/// DuckLake commits compaction alone or not at all, so a change set that
/// mixes the two is drift: the file is read and its entries derived,
/// never skipped on the compaction marker's word.
#[tokio::test]
async fn a_commit_mixing_compaction_with_another_change_still_derives() {
    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;

    let err = commit_compaction(
        &catalog,
        &store,
        "merge_adjacent:1,inserted_into_table:1",
        Some(0),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("merged.parquet"),
        "the registered file is read, not skipped: {err}"
    );
    catalog.close().await.unwrap();
}

/// An UPDATE-shaped per-row-id file carries changed values under
/// preserved ids; its entries land as adds.
#[tokio::test]
async fn update_shaped_registration_adds_changed_value_entries() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(false).await;
    let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;

    // Row 1's value changes 20 -> 99; its id is preserved.
    let size = write_parquet_with_row_ids(&store, "main/t/update.parquet", &[99], &[1]).await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: rewrite_data_file_row(12, 4, "update.parquet", 1, size),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, false, index_id).await,
        4,
        "the changed value's entry is added under the preserved id"
    );
}

/// A file recording a dense start while carrying embedded ids with a gap
/// (the flushed shape) derives entries under the embedded ids.
#[tokio::test]
async fn embedded_ids_win_over_a_recorded_dense_start() {
    use crate::store::key::{IndexKind, index_index_prefix};

    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;

    // Ids 100 and 102 — the gap at 101 is a row deleted before the flush.
    let store = Arc::new(InMemory::new());
    let size =
        write_parquet_with_row_ids(&store, "main/t/flushed.parquet", &[10, 30], &[100, 102]).await;
    let mut cells = rewrite_data_file_row(1, 3, "flushed.parquet", 2, size);
    cells[11] = Cell::U64(100); // row_id_start recorded, as a flush does
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();
    assert_eq!(index_entry_count(&catalog, true, index_id).await, 2);

    // The unique entries hold ids 100 and 102 — not 100 and 101.
    let tx = catalog.begin_write_tx().await.unwrap();
    let mut iter = ReadHandle::Tx(&tx)
        .scan_prefix(
            index_index_prefix(IndexKind::Unique, index_id),
            ..,
            crate::store::handle::ScanShape::Bulk,
        )
        .await
        .unwrap();
    let mut row_ids = Vec::new();
    while let Some(entry) = iter.next().await.unwrap() {
        row_ids.push(u64::from_be_bytes(entry.value.as_ref().try_into().unwrap()));
    }
    tx.rollback();
    row_ids.sort_unstable();
    assert_eq!(row_ids, vec![100, 102]);
}

/// Registers values 10,20,30 in a per-row-id file (file 1, embedded ids
/// 5,9,12) on the indexed table, returning the store it lives on.
async fn register_per_row_id_file(catalog: &Catalog) -> Arc<InMemory> {
    let store = Arc::new(InMemory::new());
    let size =
        write_parquet_with_row_ids(&store, "main/t/rewrite.parquet", &[10, 20, 30], &[5, 9, 12])
            .await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: rewrite_data_file_row(1, 3, "rewrite.parquet", 3, size),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();
    store
}

/// A delete file targeting a per-row-id file removes exactly the named
/// positions' entries, resolved through the embedded ids.
#[tokio::test]
async fn delete_file_against_per_row_id_target_removes_named_positions() {
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
    let store = register_per_row_id_file(&catalog).await;
    assert_eq!(index_entry_count(&catalog, true, index_id).await, 3);

    // Kill position 1 (value 20, embedded id 9): a DuckLake delete file
    // is a `file_path`/`pos` Parquet naming positions within the target.
    let deletes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["rewrite.parquet"])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    write_parquet(&store, "main/t/deletes.parquet", &deletes).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store);
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: vec![
            Cell::U64(2),
            Cell::U64(1),
            Cell::U64(4),
            Cell::Null,
            Cell::U64(1), // data_file_id: the per-row-id target
            Cell::Str("deletes.parquet".into()),
            Cell::Bool(true),
            Cell::Str("parquet".into()),
            Cell::U64(1), // delete_count
            Cell::U64(512),
            Cell::U64(64),
            Cell::Null,
            Cell::Null,
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "deleted_from_table:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        2,
        "exactly the killed position's entry is gone"
    );
}

/// An inlined file-delete names a physical position; against a per-row-id
/// target the scoped read resolves it to the row it holds, not any dense
/// range.
#[tokio::test]
async fn inline_file_delete_against_per_row_id_target_removes_the_row() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
    let store = register_per_row_id_file(&catalog).await;
    assert_eq!(index_entry_count(&catalog, true, index_id).await, 3);

    // Position 1 holds value 20 (embedded id 9); the delete names the
    // position, and its entry resolves out of the file.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store);
    tx.stage(RowOperation::InlineFileDelete {
        table_id: 1,
        data_file_id: 1,
        row_id: 1,
        begin_snapshot: 4,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "inlined_delete:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        2,
        "the named row's entry is gone"
    );
}

/// Removing an inlined file-delete drops exactly the named records and
/// leaves the rest, so a filtered `DELETE` against
/// `ducklake_inlined_delete_<t>` removes what it matched.
///
/// This is the flush's clean-up step: once an inlined deletion has been
/// materialized into a real delete file, the inlined form has to go, or
/// the row is counted deleted twice.
#[tokio::test]
async fn removing_inlined_file_deletes_drops_only_the_named_records() {
    let catalog = open().await;
    let stage = |snapshot_id: u64, ops: Vec<RowOperation>| {
        let catalog = &catalog;
        async move {
            let db_tx = catalog.begin_write_tx().await?;
            let mut tx = StagedTransaction::begin_detached(db_tx);
            for op in ops {
                tx.stage(op);
            }
            tx.stage(RowOperation::Insert {
                table: TableKind::Snapshot,
                cells: snapshot_row(snapshot_id, 1, 2),
            });
            tx.stage(RowOperation::Insert {
                table: TableKind::SnapshotChanges,
                cells: snapshot_changes_row(snapshot_id, "inlined_delete:1"),
            });
            tx.commit().await.map(|_| ())
        }
    };

    stage(
        1,
        (0..3)
            .map(|row_id| RowOperation::InlineFileDelete {
                table_id: 1,
                data_file_id: 7,
                row_id,
                begin_snapshot: 1,
            })
            .collect(),
    )
    .await
    .unwrap();
    let live = |catalog: &Catalog| {
        let catalog = catalog.clone();
        async move {
            crate::ffi_support::inline::inline_file_deletes(&catalog, 1)
                .await
                .unwrap()
        }
    };
    assert_eq!(live(&catalog).await.len(), 3);

    stage(
        2,
        vec![RowOperation::InlineFileDeleteRemove {
            table_id: 1,
            data_file_id: 7,
            row_id: 1,
        }],
    )
    .await
    .unwrap();
    let remaining: Vec<u64> = live(&catalog)
        .await
        .into_iter()
        .map(|(_, row_id, _)| row_id)
        .collect();
    assert_eq!(remaining, vec![0, 2], "only the named record is removed");

    // Removing what is no longer there is drift, not a no-op: a raw key
    // delete would pass silently, so the miss is refused.
    let err = stage(
        3,
        vec![RowOperation::InlineFileDeleteRemove {
            table_id: 1,
            data_file_id: 7,
            row_id: 1,
        }],
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&err, Error::Corruption(detail) if detail.contains("no live inlined file-delete")),
        "{err:?}"
    );

    // The failed commit staged nothing.
    assert_eq!(live(&catalog).await.len(), 2);
    catalog.close().await.unwrap();
}

/// Hard-deleting a data file's catalog row drops the inlined deletions
/// targeting it.
///
/// A merge subsumes its sources' whole visibility history into the
/// backdated replacement and then hard-deletes their rows, current and
/// history alike — so nothing can ever read those files again, and an
/// inlined deletion against one is unreachable. Left behind, it would be
/// served by `ducklake_inlined_delete_<t>` until a flush tried to
/// materialize it into a delete file naming a data file that no longer
/// exists.
///
/// Deletions against a *different* file, and against the same file id on
/// another table, are untouched.
#[tokio::test]
async fn hard_deleting_a_data_file_drops_the_inlined_deletions_against_it() {
    let catalog = open().await;
    let stage = |snapshot_id: u64, ops: Vec<RowOperation>| {
        let catalog = &catalog;
        async move {
            let db_tx = catalog.begin_write_tx().await?;
            let mut tx = StagedTransaction::begin_detached(db_tx);
            for op in ops {
                tx.stage(op);
            }
            tx.stage(RowOperation::Insert {
                table: TableKind::Snapshot,
                cells: snapshot_row(snapshot_id, 1, 2),
            });
            tx.stage(RowOperation::Insert {
                table: TableKind::SnapshotChanges,
                cells: snapshot_changes_row(snapshot_id, "inlined_delete:1"),
            });
            tx.commit().await.map(|_| ())
        }
    };

    let mut deletions: Vec<RowOperation> = (0..2)
        .map(|row_id| RowOperation::InlineFileDelete {
            table_id: 1,
            data_file_id: 7,
            row_id,
            begin_snapshot: 1,
        })
        .collect();
    deletions.push(RowOperation::InlineFileDelete {
        table_id: 1,
        data_file_id: 9,
        row_id: 0,
        begin_snapshot: 1,
    });
    deletions.push(RowOperation::InlineFileDelete {
        table_id: 2,
        data_file_id: 7,
        row_id: 0,
        begin_snapshot: 1,
    });
    stage(1, deletions).await.unwrap();

    let live = |table_id: u64| {
        let catalog = catalog.clone();
        async move {
            crate::ffi_support::inline::inline_file_deletes(&catalog, table_id)
                .await
                .unwrap()
        }
    };
    assert_eq!(live(1).await.len(), 3);
    assert_eq!(live(2).await.len(), 1);

    // A maintenance commit hard-pruning table 1's data file 7.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut prune = StagedTransaction::begin_detached(db_tx);
    prune.stage(RowOperation::Delete {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(7), Cell::Null],
    });
    prune.commit().await.unwrap();

    let remaining: Vec<(u64, u64)> = live(1)
        .await
        .into_iter()
        .map(|(data_file_id, row_id, _)| (data_file_id, row_id))
        .collect();
    assert_eq!(
        remaining,
        vec![(9, 0)],
        "only the pruned file's deletions go; file 9's stays"
    );
    assert_eq!(
        live(2).await.len(),
        1,
        "the same file id on another table is a different file"
    );

    // The table still exists: an emptied inlined-deletion table is not a
    // missing one, and the cascade must not take the marker with it.
    assert!(
        crate::ffi_support::inline::inline_file_delete_table_exists(&catalog, 1)
            .await
            .unwrap()
    );
    catalog.close().await.unwrap();
}

/// Ending a data file into history — what a rewrite does to its source —
/// leaves the inlined deletions against it alone.
///
/// A rewrite materializes deletes, so its output holds fewer rows than the
/// source and a reader below the rewrite must still see the deleted ones.
/// The source is ended rather than pruned for exactly that reason, and the
/// deletions that make those rows dead have to stay readable with it.
#[tokio::test]
async fn ending_a_data_file_keeps_the_inlined_deletions_against_it() {
    let catalog = open().await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx);
    setup.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: indexed_data_file_row(3, 512),
    });
    setup.stage(RowOperation::InlineFileDelete {
        table_id: 1,
        data_file_id: 1,
        row_id: 0,
        begin_snapshot: 1,
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "inlined_delete:1"),
    });
    setup.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut end = StagedTransaction::begin_detached(db_tx);
    end.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(1), Cell::U64(2)],
    });
    end.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    end.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "compacted_table:1"),
    });
    end.commit().await.unwrap();

    assert_eq!(
        crate::ffi_support::inline::inline_file_deletes(&catalog, 1)
            .await
            .unwrap()
            .len(),
        1,
        "the ended file is still readable below the end, and so is its deletion"
    );
    catalog.close().await.unwrap();
}

/// `ducklake_inlined_delete_<t>` exists from its first inlined deletion
/// until the table is dropped — including after a flush has cleared every
/// deletion it held.
///
/// The "including" is the whole point. DuckLake caches the table's
/// existence for the life of the catalog and never re-probes, so an
/// existence derived from whether any deletion is currently recorded
/// vanishes under it the moment a flush empties the table, and every
/// later query in that session fails to bind. An emptied SQL table still
/// exists; so does this one.
#[tokio::test]
async fn the_inlined_deletion_table_exists_from_its_first_deletion_until_the_drop() {
    let catalog = open().await;
    let exists = |catalog: &Catalog| {
        let catalog = catalog.clone();
        async move {
            crate::ffi_support::inline::inline_file_delete_table_exists(&catalog, 1)
                .await
                .unwrap()
        }
    };
    let stage = |snapshot_id: u64, op: RowOperation| {
        let catalog = &catalog;
        async move {
            let db_tx = catalog.begin_write_tx().await?;
            let mut tx = StagedTransaction::begin_detached(db_tx);
            tx.stage(op);
            tx.stage(RowOperation::Insert {
                table: TableKind::Snapshot,
                cells: snapshot_row(snapshot_id, 1, 2),
            });
            tx.stage(RowOperation::Insert {
                table: TableKind::SnapshotChanges,
                cells: snapshot_changes_row(snapshot_id, "inlined_delete:1"),
            });
            tx.commit().await.map(|_| ())
        }
    };

    assert!(
        !exists(&catalog).await,
        "a table with no inlined deletion has no such table to bind"
    );

    stage(
        1,
        RowOperation::InlineFileDelete {
            table_id: 1,
            data_file_id: 7,
            row_id: 3,
            begin_snapshot: 1,
        },
    )
    .await
    .unwrap();
    assert!(exists(&catalog).await);

    stage(
        2,
        RowOperation::InlineFileDeleteRemove {
            table_id: 1,
            data_file_id: 7,
            row_id: 3,
        },
    )
    .await
    .unwrap();
    assert!(
        crate::ffi_support::inline::inline_file_deletes(&catalog, 1)
            .await
            .unwrap()
            .is_empty(),
        "the flush cleared every deletion"
    );
    assert!(
        exists(&catalog).await,
        "an emptied inlined-deletion table still exists"
    );

    stage(3, RowOperation::InlineDrop { table_id: 1 })
        .await
        .unwrap();
    assert!(
        !exists(&catalog).await,
        "dropping the table takes its inlined-deletion table with it"
    );
    catalog.close().await.unwrap();
}

/// Backfill over a table holding a per-row-id file derives its entries
/// under the embedded ids.
#[tokio::test]
async fn backfill_derives_per_row_id_file_entries_under_embedded_ids() {
    use crate::catalog::{ColumnId, TableId};

    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    let store = register_per_row_id_file(&catalog).await;

    let entries = catalog
        .scoped_backfill_entries(store, "", TableId::new(1), &[ColumnId::new(1)])
        .await
        .unwrap();
    let mut row_ids: Vec<u64> = entries.iter().map(|e| e.row_id).collect();
    row_ids.sort_unstable();
    assert_eq!(row_ids, vec![5, 9, 12], "ids come from the embedded column");
}

/// Creating an index over a table with pre-existing **inline** rows backfills
/// them — including NULL rows, which become collision-exempt entries `IS NULL`
/// can find. Regression: inline chunks were previously skipped at create.
#[tokio::test]
async fn create_index_backfills_inline_null_rows() {
    use crate::catalog::{ColumnId, IndexDef, TableId};

    let catalog = open().await;

    // Table 1 with one BIGINT column `a`, no index yet.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx);
    setup.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "created_table:1"),
    });
    setup.commit().await.unwrap();

    // Inline-insert three rows before any index exists: 10, NULL, 30.
    let (schema, batch) = nullable_bigint_batch(&[Some(10), None, Some(30)]);
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 0,
        arrow_schema: inline_schema_ipc(&schema),
    });
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 2,
        row_id_start: 0,
        row_count: 3,
        arrow_body: inline_body(&batch),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "inlined_insert:1"),
    });
    tx.commit().await.unwrap();

    // Backfill sees all three inline rows, with a `None` for the NULL row.
    let backfill = catalog
        .inline_backfill_entries(TableId::new(1), &[ColumnId::new(1)])
        .await
        .unwrap();
    assert_eq!(backfill.len(), 3, "every live inline row is backfilled");
    assert!(
        backfill
            .iter()
            .any(|entry| entry.row_id == 1 && entry.values == vec![None]),
        "the NULL row backfills as a None value"
    );

    // Create a unique index with that backfill; IS NULL finds the NULL row.
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                TableId::new(1),
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &backfill,
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let nulls = catalog
        .index_nulls(TableId::new(1), index, vec![None], false)
        .await
        .unwrap();
    assert_eq!(
        nulls,
        vec![1],
        "IS NULL finds the pre-existing inline NULL row"
    );
}

/// A data-file row already dead (by a delete file) when the index is built
/// must not be backfilled — otherwise a unique index keeps a zombie entry
/// that manufactures a false `Constraint` when the freed value is
/// re-inserted. Regression.
#[tokio::test]
async fn scoped_backfill_excludes_delete_file_rows() {
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    use crate::catalog::{ColumnId, TableId};

    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    // A data file with values 10, 20, 30 at positions/rows 0, 1, 2.
    let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;

    // A delete file killing position 1 (value 20) of that data file.
    let deletes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["data.parquet"])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    write_parquet(&store, "main/t/deletes.parquet", &deletes).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: vec![
            Cell::U64(2),
            Cell::U64(1),
            Cell::U64(4),
            Cell::Null,
            Cell::U64(1), // data_file_id: the target
            Cell::Str("deletes.parquet".into()),
            Cell::Bool(true),
            Cell::Str("parquet".into()),
            Cell::U64(1),
            Cell::U64(512),
            Cell::U64(64),
            Cell::Null,
            Cell::Null,
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "deleted_from_table:1"),
    });
    tx.commit().await.unwrap();

    // Backfill must exclude the dead row 1 (value 20).
    let entries = catalog
        .scoped_backfill_entries(store, "", TableId::new(1), &[ColumnId::new(1)])
        .await
        .unwrap();
    let mut row_ids: Vec<u64> = entries.iter().map(|entry| entry.row_id).collect();
    row_ids.sort_unstable();
    assert_eq!(
        row_ids,
        vec![0, 2],
        "the delete-file row is excluded from backfill"
    );
}

/// An inlined delete against a Parquet-file row removes that row's
/// entry, read back out of the target file.
#[tokio::test]
async fn inlined_file_delete_removes_the_killed_rows_index_entry() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
    let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;
    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        3,
        "the registered file lands one entry per row"
    );

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store);
    tx.stage(RowOperation::InlineFileDelete {
        table_id: 1,
        data_file_id: 1,
        row_id: 1,
        begin_snapshot: 4,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "inlined_delete:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        2,
        "the deleted row's entry is gone, the other two remain"
    );
}

/// A registered delete file names positions in its target; the commit
/// reads those positions' values and removes exactly their entries.
#[tokio::test]
async fn registered_delete_file_removes_the_killed_rows_index_entries() {
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
    let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;

    // A DuckLake delete file: `file_path` plus the killed positions.
    let deletes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["data.parquet", "data.parquet"])),
            Arc::new(Int64Array::from(vec![0, 2])),
        ],
    )
    .unwrap();
    write_parquet(&store, "main/t/deletes.parquet", &deletes).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store);
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: vec![
            Cell::U64(2),
            Cell::U64(1),
            Cell::U64(4),
            Cell::Null,
            Cell::U64(1), // data_file_id
            Cell::Str("deletes.parquet".into()),
            Cell::Bool(true),
            Cell::Str("parquet".into()),
            Cell::U64(2), // delete_count
            Cell::U64(512),
            Cell::U64(64),
            Cell::Null,
            Cell::Null,
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "deleted_from_table:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        1,
        "positions 0 and 2 are unindexed; only row 1 survives"
    );
}

/// A delete file may target a data file its own commit registers, which
/// is the shape a flush of partly-tombstoned inlined rows takes: DuckLake
/// writes every inlined row — live and tombstoned alike — into one Parquet
/// file, then writes a delete file naming the tombstoned rows' positions
/// in it. Index upkeep must resolve the target through this commit's own
/// inserts, not the committed head alone, and the killed rows must end up
/// unindexed even though the same commit indexed the whole file.
#[tokio::test]
async fn delete_file_may_target_a_data_file_its_own_commit_registers() {
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;

    let store = Arc::new(InMemory::new());
    let (_, batch) = bigint_batch(&[10, 20, 30]);
    let file_size = write_parquet(&store, "main/t/data.parquet", &batch).await;

    let deletes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["data.parquet", "data.parquet"])),
            Arc::new(Int64Array::from(vec![0, 2])),
        ],
    )
    .unwrap();
    write_parquet(&store, "main/t/deletes.parquet", &deletes).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store);
    // The delete file is staged *before* its target, so the fix cannot
    // rest on DuckLake's emit order.
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: vec![
            Cell::U64(2),
            Cell::U64(1),
            Cell::U64(3),
            Cell::Null,
            Cell::U64(1), // data_file_id, registered below
            Cell::Str("deletes.parquet".into()),
            Cell::Bool(true),
            Cell::Str("parquet".into()),
            Cell::U64(2), // delete_count
            Cell::U64(512),
            Cell::U64(64),
            Cell::Null,
            Cell::Null,
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: indexed_data_file_row(3, file_size),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        1,
        "the file's three rows are indexed and the two the delete file \
         kills are removed, in the one commit that did both"
    );
}

/// The same shape against a non-unique index, where the killed row and
/// the survivor share a value and their entries differ only by row id.
///
/// The killed row must never be indexed rather than be indexed and then
/// removed. Removals stage before adds, so a removal beside the add would
/// leave the entry standing — and it could not simply be cancelled
/// against the add either: an entry's key carries no file, so that pair is
/// indistinguishable from an UPDATE's, which rewrites a row into a new
/// file under its preserved row id and must keep its entry.
#[tokio::test]
async fn a_row_deleted_out_of_the_file_its_own_commit_registers_is_never_indexed() {
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    let (catalog, index_id) = catalog_with_indexed_inline_table(false).await;

    let store = Arc::new(InMemory::new());
    // Both rows share one value, so the killed row's entry and the
    // survivor's differ only in the row id embedded in the key.
    let (_, batch) = bigint_batch(&[7, 7]);
    let file_size = write_parquet(&store, "main/t/data.parquet", &batch).await;

    let deletes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["data.parquet"])),
            Arc::new(Int64Array::from(vec![0])),
        ],
    )
    .unwrap();
    write_parquet(&store, "main/t/deletes.parquet", &deletes).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store);
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: indexed_data_file_row(2, file_size),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: vec![
            Cell::U64(2),
            Cell::U64(1),
            Cell::U64(3),
            Cell::Null,
            Cell::U64(1),
            Cell::Str("deletes.parquet".into()),
            Cell::Bool(true),
            Cell::Str("parquet".into()),
            Cell::U64(1),
            Cell::U64(512),
            Cell::U64(64),
            Cell::Null,
            Cell::Null,
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, false, index_id).await,
        1,
        "row 0's entry is removed and row 1's survives"
    );
}

/// A delete file naming a position past the target file's row count could
/// never match a scoped entry; rather than silently orphan index rows, the
/// commit is refused.
#[tokio::test]
async fn registered_delete_file_naming_an_out_of_range_position_is_refused() {
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    let (catalog, _index_id) = catalog_with_indexed_inline_table(true).await;
    let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;

    // The target holds 3 rows (positions 0..=2); position 5 names none.
    let deletes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["data.parquet"])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    )
    .unwrap();
    write_parquet(&store, "main/t/deletes.parquet", &deletes).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store);
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: vec![
            Cell::U64(2),
            Cell::U64(1),
            Cell::U64(4),
            Cell::Null,
            Cell::U64(1), // data_file_id
            Cell::Str("deletes.parquet".into()),
            Cell::Bool(true),
            Cell::Str("parquet".into()),
            Cell::U64(1), // delete_count
            Cell::U64(512),
            Cell::U64(64),
            Cell::Null,
            Cell::Null,
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "deleted_from_table:1"),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
}

/// `InlineFlushDelete` removes chunks begun at or before the flush
/// snapshot for the named schema version, plus the `inline/inline_delete`
/// tombstones those chunks' rows consumed — a later schema version's
/// chunk (begun after the flush point) survives untouched.
#[tokio::test]
async fn stages_inline_flush_delete_removes_flushed_chunks_and_their_idels() {
    let catalog = open().await;

    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 1,
        row_id_start: 0,
        row_count: 2,
        arrow_body: b"chunk".to_vec(),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 0, 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "inlined_insert:1"),
    });
    setup.commit().await.unwrap();

    // A later commit tombstones one row (a tombstone only ever ends a
    // version begun before it — DuckLake's writer never stamps a row
    // with its own insertion snapshot).
    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut delete = StagedTransaction::begin_detached(db_tx2);
    delete.stage(RowOperation::InlineInlineDelete {
        table_id: 1,
        row_id: 0,
        end_snapshot: 2,
    });
    delete.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 0, 1),
    });
    delete.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "inlined_delete:1"),
    });
    delete.commit().await.unwrap();

    let db_tx3 = catalog.begin_write_tx().await.unwrap();
    let mut flush = StagedTransaction::begin_detached(db_tx3);
    flush.stage(RowOperation::InlineFlushDelete {
        table_id: 1,
        schema_version: 0,
        flush_snapshot: 2,
    });
    flush.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 0, 1),
    });
    flush.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "flushed_inlined_data:1"),
    });
    flush.commit().await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    let inline_deletes = store_inline::scan_inline_inline_deletes(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    tx.rollback();
    assert!(chunks.is_empty(), "flushed chunk must be gone: {chunks:?}");
    assert!(
        inline_deletes.is_empty(),
        "consumed inline_delete must be gone: {inline_deletes:?}"
    );
}

/// `InlineDrop` removes every `inline/*` record for the table:
/// schema, chunks, and tombstones.
#[tokio::test]
async fn stages_inline_drop_removes_every_record_for_the_table() {
    let catalog = open().await;

    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 0,
        arrow_schema: b"schema".to_vec(),
    });
    setup.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 1,
        row_id_start: 0,
        row_count: 1,
        arrow_body: b"chunk".to_vec(),
    });
    setup.stage(RowOperation::InlineFileDelete {
        table_id: 1,
        data_file_id: 9,
        row_id: 5,
        begin_snapshot: 1,
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 0, 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "inlined_insert:1"),
    });
    setup.commit().await.unwrap();

    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut drop_tx = StagedTransaction::begin_detached(db_tx2);
    drop_tx.stage(RowOperation::InlineDrop { table_id: 1 });
    drop_tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 0, 1),
    });
    drop_tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, r#"dropped_table:"main"."t""#),
    });
    drop_tx.commit().await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    let file_deletes = store_inline::scan_inline_file_deletes(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    let schemas = store_inline::scan_inline_schemas(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    tx.rollback();
    assert!(chunks.is_empty());
    assert!(file_deletes.is_empty());
    assert!(schemas.is_empty());
}

/// `InlineSchemaDrop` removes only the named schema version's
/// `inline/schema` record, leaving a different schema version's
/// record (and its chunks) untouched — the scoped cleanup a
/// superseded-inlined-table flush needs, as opposed to `InlineDrop`'s
/// whole-table sweep.
#[tokio::test]
async fn stages_inline_schema_drop_removes_only_the_named_schema_version() {
    let catalog = open().await;

    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 0,
        arrow_schema: b"schema-v0".to_vec(),
    });
    setup.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 1,
        arrow_schema: b"schema-v1".to_vec(),
    });
    setup.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 1,
        begin_snapshot: 1,
        row_id_start: 0,
        row_count: 1,
        arrow_body: b"chunk".to_vec(),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 0, 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "inlined_insert:1"),
    });
    setup.commit().await.unwrap();

    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut drop_tx = StagedTransaction::begin_detached(db_tx2);
    drop_tx.stage(RowOperation::InlineSchemaDrop {
        table_id: 1,
        schema_version: 0,
    });
    drop_tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 0, 1),
    });
    drop_tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "flushed_inlined_data:1"),
    });
    drop_tx.commit().await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let schemas = store_inline::scan_inline_schemas(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    tx.rollback();
    assert_eq!(
        schemas,
        vec![(
            1,
            proto::InlineSchemaValue {
                arrow_schema: b"schema-v1".to_vec()
            }
        )]
    );
    assert_eq!(chunks.len(), 1, "schema_version 1's chunk must survive");
}

fn partition_info_row(partition_id: u64, table_id: u64, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(partition_id),
        Cell::U64(table_id),
        Cell::U64(begin),
        Cell::Null,
    ]
}

fn partition_column_row(partition_id: u64, table_id: u64, index: u64, column_id: u64) -> Vec<Cell> {
    vec![
        Cell::U64(partition_id),
        Cell::U64(table_id),
        Cell::U64(index),
        Cell::U64(column_id),
        Cell::Str("identity".to_string()),
    ]
}

fn file_partition_value_row(
    data_file_id: u64,
    table_id: u64,
    index: u64,
    value: &str,
) -> Vec<Cell> {
    vec![
        Cell::U64(data_file_id),
        Cell::U64(table_id),
        Cell::U64(index),
        Cell::Str(value.to_string()),
    ]
}

fn data_file_row(data_file_id: u64, table_id: u64, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(data_file_id),
        Cell::U64(table_id),
        Cell::U64(begin),
        Cell::Null,                       // end_snapshot
        Cell::Null,                       // file_order
        Cell::Str("data.parquet".into()), // path
        Cell::Bool(true),                 // path_is_relative
        Cell::Str("parquet".into()),      // file_format
        Cell::U64(10),                    // record_count
        Cell::U64(1024),                  // file_size_bytes
        Cell::U64(64),                    // footer_size
        Cell::U64(0),                     // row_id_start
        Cell::Null,                       // partition_id
        Cell::Null,                       // encryption_key
        Cell::Null,                       // mapping_id
        Cell::Null,                       // partial_max
    ]
}

/// A partition spec, its columns, and a file's partition values land
/// verbatim; repartitioning ends the old spec; time travel
/// reconstructs the spec-in-force, and every file still reports the
/// spec it was written under.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn partition_spec_rows_land_fold_and_time_travel() {
    let catalog = open().await;

    // Commit 1: table + column + spec 10 (identity on column 1) + one
    // data file written under it, carrying one partition value.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_on(&catalog, db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "part_key", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::PartitionInfo,
        cells: partition_info_row(10, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::PartitionColumn,
        cells: partition_column_row(10, 1, 0, 1),
    });
    let mut file = data_file_row(1, 1, 1);
    file[12] = Cell::U64(10); // partition_id
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: file,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::FilePartitionValue,
        cells: file_partition_value_row(1, 1, 0, "7"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 11),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let spec = &head.partitions[&1][&10];
    assert_eq!(spec.begin_snapshot, 1);
    assert_eq!(spec.columns.len(), 1);
    assert_eq!(spec.columns[0].column_id, 1);
    assert_eq!(spec.columns[0].transform, "identity");
    let stored = &head.data_files[&1][&1];
    assert_eq!(stored.partition_id, Some(10));
    assert_eq!(stored.partition_values.len(), 1);
    assert_eq!(stored.partition_values[0].partition_value, "7");

    // Commit 2: repartition — end spec 10, insert spec 11.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_on(&catalog, db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::PartitionInfo,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::PartitionInfo,
        cells: partition_info_row(11, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::PartitionColumn,
        cells: partition_column_row(11, 1, 0, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 12),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(!head.partitions[&1].contains_key(&10));
    assert!(head.partitions[&1].contains_key(&11));
    // The file still names the spec it was written under.
    assert_eq!(head.data_files[&1][&1].partition_id, Some(10));

    // Time travel reconstructs spec 10 at snapshot 1.
    let at_one = catalog
        .snapshot_at(crate::catalog::SnapshotId::new(1))
        .await
        .unwrap();
    assert!(at_one.partitions[&1].contains_key(&10));
    assert!(!at_one.partitions[&1].contains_key(&11));

    // The dump surface serves current and history rows unfiltered.
    let specs = crate::ffi_support::dump_partition_info(&catalog)
        .await
        .unwrap();
    assert!(
        specs
            .iter()
            .any(|p| p.partition_id == 10 && p.end_snapshot == Some(2))
    );
    assert!(
        specs
            .iter()
            .any(|p| p.partition_id == 11 && p.end_snapshot.is_none())
    );

    // Commit 3: clear — end spec 11, insert nothing.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_on(&catalog, db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::PartitionInfo,
        cells: vec![Cell::U64(1), Cell::U64(11), Cell::U64(3)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 12),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(
        head.partitions
            .get(&1)
            .is_none_or(std::collections::BTreeMap::is_empty)
    );

    catalog.close().await.unwrap();
}

fn sort_info_row(sort_id: u64, table_id: u64, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(sort_id),
        Cell::U64(table_id),
        Cell::U64(begin),
        Cell::Null,
    ]
}

fn sort_expression_row(sort_id: u64, table_id: u64, index: u64, expression: &str) -> Vec<Cell> {
    vec![
        Cell::U64(sort_id),
        Cell::U64(table_id),
        Cell::U64(index),
        Cell::Str(expression.to_string()),
        Cell::Str("duckdb".to_string()),
        Cell::Str("DESC".to_string()),
        Cell::Str("NULLS_FIRST".to_string()),
    ]
}

/// A sort spec and its expressions land verbatim — direction, null
/// order, and dialect untouched; re-sorting ends the old spec; time
/// travel reconstructs the spec-in-force.
#[tokio::test]
async fn sort_spec_rows_land_fold_and_time_travel() {
    let catalog = open().await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_on(&catalog, db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "v", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SortInfo,
        cells: sort_info_row(20, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SortExpression,
        cells: sort_expression_row(20, 1, 0, "v"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 21),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let spec = &head.sorts[&1][&20];
    assert_eq!(spec.begin_snapshot, 1);
    assert_eq!(spec.expressions.len(), 1);
    assert_eq!(spec.expressions[0].expression, "v");
    assert_eq!(spec.expressions[0].dialect, "duckdb");
    assert_eq!(spec.expressions[0].sort_direction, "DESC");
    assert_eq!(spec.expressions[0].null_order, "NULLS_FIRST");

    // End spec 20, insert spec 21 — the snapshot row keeps the same
    // schema_version: DuckLake does not bump it for sort changes.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_on(&catalog, db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::SortInfo,
        cells: vec![Cell::U64(1), Cell::U64(20), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SortInfo,
        cells: sort_info_row(21, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SortExpression,
        cells: sort_expression_row(21, 1, 0, "v || 'x'"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 22),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(!head.sorts[&1].contains_key(&20));
    assert_eq!(head.sorts[&1][&21].expressions[0].expression, "v || 'x'");

    let at_one = catalog
        .snapshot_at(crate::catalog::SnapshotId::new(1))
        .await
        .unwrap();
    assert!(at_one.sorts[&1].contains_key(&20));
    assert!(!at_one.sorts[&1].contains_key(&21));

    // The dump surface serves current and history rows unfiltered.
    let specs = crate::ffi_support::dump_sort_info(&catalog).await.unwrap();
    assert!(
        specs
            .iter()
            .any(|s| s.sort_id == 20 && s.end_snapshot == Some(2))
    );
    assert!(
        specs
            .iter()
            .any(|s| s.sort_id == 21 && s.end_snapshot.is_none())
    );

    catalog.close().await.unwrap();
}

/// A `partition_column` row whose spec is not inserted in the same
/// commit is a shape error, not a silent drop.
#[tokio::test]
async fn orphaned_partition_column_row_is_rejected() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::PartitionColumn,
        cells: partition_column_row(99, 1, 0, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "none"),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(err.to_string().contains("partition"), "{err}");
    catalog.close().await.unwrap();
}

fn tag_row(object_id: u64, begin: u64, key: &str, value: &str) -> Vec<Cell> {
    vec![
        Cell::U64(object_id),
        Cell::U64(begin),
        Cell::Null,
        Cell::Str(key.to_string()),
        Cell::Str(value.to_string()),
    ]
}

fn column_tag_row(table_id: u64, column_id: u64, begin: u64, key: &str, value: &str) -> Vec<Cell> {
    vec![
        Cell::U64(table_id),
        Cell::U64(column_id),
        Cell::U64(begin),
        Cell::Null,
        Cell::Str(key.to_string()),
        Cell::Str(value.to_string()),
    ]
}

async fn seed_table(catalog: &Catalog) {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();
}

/// A re-comment (`COMMENT ON` twice) is DuckLake's set-end + insert
/// pair: the old entry ends, the new one lands live, both kept in the
/// object's container for time travel.
#[tokio::test]
async fn tag_rows_land_and_a_recomment_ends_the_old_entry() {
    let catalog = open().await;
    seed_table(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Tag,
        cells: tag_row(1, 2, "comment", "first"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Tag,
        cells: vec![Cell::U64(1), Cell::Str("comment".into()), Cell::U64(3)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Tag,
        cells: tag_row(1, 3, "comment", "second"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let tags = head.tags_of(1);
    assert_eq!(tags.len(), 2);
    assert_eq!(
        (tags[0].value.as_str(), tags[0].end_snapshot),
        ("first", Some(3))
    );
    assert_eq!(
        (tags[1].value.as_str(), tags[1].end_snapshot),
        ("second", None)
    );
    catalog.close().await.unwrap();
}

/// A column tag rides its column's record without minting a column
/// version: after tagging, the column still has exactly one row on
/// the dump surface, and the tag ends in place on a re-comment.
#[tokio::test]
async fn column_tags_ride_the_column_record_without_a_version_transition() {
    let catalog = open().await;
    seed_table(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::ColumnTag,
        cells: column_tag_row(1, 1, 2, "comment", "col comment"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let columns = crate::ffi_support::dump_columns(&catalog).await.unwrap();
    let rows: Vec<_> = columns.iter().filter(|c| c.column_id == 1).collect();
    assert_eq!(rows.len(), 1, "a tag change must not mint a column version");
    assert_eq!(rows[0].begin_snapshot, 0);
    assert!(rows[0].end_snapshot.is_none());
    assert_eq!(rows[0].tags.len(), 1);
    assert_eq!(rows[0].tags[0].value, "col comment");
    catalog.close().await.unwrap();
}

/// A column version transition (rename) carries the prior version's
/// tag entries onto the new current record — DuckLake keys column
/// tags by (table, column) with their own lifecycle, so an alter
/// never re-authors them.
#[tokio::test]
async fn column_alter_carries_tags_forward() {
    let catalog = open().await;
    seed_table(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::ColumnTag,
        cells: column_tag_row(1, 1, 2, "comment", "kept"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    // Rename the column: end the old version, insert the new one.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Column,
        cells: vec![Cell::U64(1), Cell::U64(1), Cell::U64(3)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: {
            let mut cells = column_row(1, 1, "renamed", 0);
            cells[1] = Cell::U64(3); // begin_snapshot
            cells
        },
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 2, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "altered_table:1"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SchemaVersions,
        cells: vec![Cell::U64(3), Cell::U64(2), Cell::U64(1)],
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let column = &head.columns[&1][&1];
    assert_eq!(column.column_name, "renamed");
    assert_eq!(column.tags.len(), 1);
    assert_eq!(column.tags[0].value, "kept");
    catalog.close().await.unwrap();
}

/// Seeds `t` (table 1) with data file 9 (snapshot 1), then expires the
/// file's live version at snapshot 2 — leaving one dead history row,
/// the fixture the expiry tests prune.
async fn seed_expired_file(catalog: &Catalog) {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(9, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(9), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "deleted_from_table:1"),
    });
    tx.commit().await.unwrap();
}

/// The expiry cascade's staged shape: delete a dead snapshot, prune
/// the history row it alone saw, and schedule the file's bytes — all
/// in one head-preserving maintenance commit.
#[tokio::test]
async fn expiry_prunes_history_and_schedules_files_without_advancing_head() {
    let catalog = open().await;
    seed_expired_file(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Delete {
        table: TableKind::Snapshot,
        cells: vec![Cell::U64(1)],
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::SnapshotChanges,
        cells: vec![Cell::U64(1)],
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(9), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![
            Cell::U64(9),
            Cell::Str("f9.parquet".to_string()),
            Cell::Bool(true),
            Cell::I64(1_000),
        ],
    });
    let id = tx.commit().await.unwrap();
    assert_eq!(id.get(), 2, "maintenance must not advance head");

    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.current_snapshot().id.get(), 2);
    let schedule = head.scheduled_deletions();
    assert_eq!(schedule.len(), 1);
    assert_eq!(schedule[0].data_file_id, 9);
    assert_eq!(schedule[0].path, "f9.parquet");
    assert_eq!(
        schedule[0].schedule_start,
        crate::catalog::Timestamp::from_micros(1_000),
        "the staged instant reaches the projection unchanged"
    );

    // The dead snapshot no longer resolves — below head with its record
    // reclaimed, so `SnapshotExpired`, not a plain miss; the survivor
    // resolves, and the pruned history row is gone from the dump surface.
    let expired = catalog
        .snapshot_at(crate::catalog::SnapshotId::new(1))
        .await
        .unwrap_err();
    assert!(matches!(expired, Error::SnapshotExpired(_)), "{expired}");
    let surviving = catalog
        .snapshot_at(crate::catalog::SnapshotId::new(2))
        .await
        .unwrap();
    assert!(
        surviving
            .data_files_of(crate::catalog::TableId::new(1))
            .is_empty()
    );
    let files = crate::ffi_support::dump_data_files(&catalog).await.unwrap();
    assert!(files.is_empty(), "history row must be pruned: {files:?}");

    catalog.close().await.unwrap();
}

/// Cleanup's staged shape: after DuckDB deletes the bytes, the
/// schedule row is forgotten in a head-preserving commit.
#[tokio::test]
async fn cleanup_forgets_the_schedule() {
    let catalog = open().await;
    seed_expired_file(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![
            Cell::U64(9),
            Cell::Str("f9.parquet".to_string()),
            Cell::Bool(true),
            Cell::I64(1_000),
        ],
    });
    tx.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Delete {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![Cell::U64(9)],
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(head.scheduled_deletions().is_empty());
    assert_eq!(head.current_snapshot().id.get(), 2);
    catalog.close().await.unwrap();
}

/// The head snapshot can never be expired.
#[tokio::test]
async fn deleting_the_head_snapshot_is_rejected() {
    let catalog = open().await;
    seed_expired_file(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Delete {
        table: TableKind::Snapshot,
        cells: vec![Cell::U64(2)],
    });
    let err = tx.commit().await.unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
    catalog.close().await.unwrap();
}

/// A commit that mutates entities without minting a snapshot is not a
/// maintenance commit — it is a malformed write.
#[tokio::test]
async fn maintenance_commit_rejects_entity_inserts() {
    let catalog = open().await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
    catalog.close().await.unwrap();
}

/// A merge-shaped compaction commit: the merged file lands backdated,
/// the source rows (current and history alike) are hard-deleted, the
/// source bytes are scheduled, and `next_row_id` is untouched — all
/// in one ordinary snapshot-minting commit.
#[tokio::test]
async fn merge_shaped_commit_replaces_files_and_schedules_sources() {
    let catalog = open().await;

    // Seed: table 1 with files 9 and 10, both live.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(9, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(10, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::TableStats,
        cells: vec![Cell::U64(1), Cell::U64(20), Cell::U64(20), Cell::U64(2048)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    // The merge: insert file 11 backdated to the sources' begin,
    // hard-delete both sources, schedule their bytes.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(11, 1, 1),
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(9), Cell::Null],
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::Null],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![
            Cell::U64(9),
            Cell::Str("data.parquet".to_string()),
            Cell::Bool(true),
            Cell::I64(1_000),
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![
            Cell::U64(10),
            Cell::Str("data.parquet".to_string()),
            Cell::Bool(true),
            Cell::I64(1_000),
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "merge_adjacent:1"),
    });
    let id = tx.commit().await.unwrap();
    assert_eq!(id.get(), 2, "compaction mints an ordinary snapshot");

    let head = catalog.snapshot().await.unwrap();
    let files = head.data_files_of(crate::catalog::TableId::new(1));
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].id.get(), 11);
    assert_eq!(
        head.scheduled_deletions()
            .iter()
            .map(|d| d.data_file_id)
            .collect::<Vec<_>>(),
        vec![9, 10]
    );
    assert_eq!(
        head.table_stats(crate::catalog::TableId::new(1))
            .unwrap()
            .next_row_id,
        20,
        "compaction never allocates row ids"
    );

    // The sources are gone outright — no history mirror.
    let rows = crate::ffi_support::dump_data_files(&catalog).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data_file_id, 11);
    assert_eq!(rows[0].begin_snapshot, 1, "the merged file is backdated");
    catalog.close().await.unwrap();
}

/// Merging a *partitioned* table: the sources carry
/// `ducklake_file_partition_value` rows that die with them, so the batch
/// hard-deletes a parent and its embedded child together. A hard delete
/// deliberately leaves the working state alone, so the embedded-delete
/// check cannot ask whether the parent survived by reading state alone —
/// it has to know the batch is deleting it. Staged in both delete orders,
/// since DuckLake's emission order is not part of the contract.
#[tokio::test]
async fn merge_of_a_partitioned_table_deletes_parent_and_partition_values() {
    // Seeds one partitioned table with file 9 carrying a partition value,
    // then stages a merge whose deletes are emitted in `child_first` order.
    async fn merge_with(child_first: bool) -> Result<()> {
        let catalog = open().await;

        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut tx = StagedTransaction::begin_detached(db_tx);
        tx.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(1, 0, "t", 1, None),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Column,
            cells: column_row(1, 1, "part_key", 0),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::PartitionInfo,
            cells: partition_info_row(10, 1, 1),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::PartitionColumn,
            cells: partition_column_row(10, 1, 0, 1),
        });
        let mut file = data_file_row(9, 1, 1);
        file[12] = Cell::U64(10);
        tx.stage(RowOperation::Insert {
            table: TableKind::DataFile,
            cells: file,
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::FilePartitionValue,
            cells: file_partition_value_row(9, 1, 0, "7"),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
        });
        tx.commit().await.unwrap();

        // The merge: a backdated replacement, then the source's rows
        // hard-deleted — parent and embedded child.
        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut tx = StagedTransaction::begin_detached(db_tx);
        let mut merged = data_file_row(11, 1, 1);
        merged[12] = Cell::U64(10);
        tx.stage(RowOperation::Insert {
            table: TableKind::DataFile,
            cells: merged,
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::FilePartitionValue,
            cells: file_partition_value_row(11, 1, 0, "7"),
        });

        let child = RowOperation::Delete {
            table: TableKind::FilePartitionValue,
            cells: vec![Cell::U64(9), Cell::U64(1), Cell::U64(0)],
        };
        let parent = RowOperation::Delete {
            table: TableKind::DataFile,
            cells: vec![Cell::U64(1), Cell::U64(9), Cell::Null],
        };
        if child_first {
            tx.stage(child);
            tx.stage(parent);
        } else {
            tx.stage(parent);
            tx.stage(child);
        }

        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(2, 1, 2),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(2, r#"compacted_table:"main"."t""#),
        });
        let result = tx.commit().await.map(|_| ());
        catalog.close().await.unwrap();
        result
    }

    merge_with(false)
        .await
        .expect("merge with the parent deleted before its partition value");
    merge_with(true)
        .await
        .expect("merge with the partition value deleted before its parent");
}

/// A rewrite-shaped commit ends the source file and its delete file
/// into history and rebases the replacement's `begin_snapshot` in
/// place; nothing is scheduled.
#[tokio::test]
async fn rewrite_shaped_commit_ends_rows_and_rebases_the_new_file() {
    let catalog = open().await;
    seed_expired_file(&catalog).await; // file 9 ended at snapshot 2

    // Re-seed a live file 10 with a delete file 11 over it.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(10, 1, 3),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: vec![
            Cell::U64(11),
            Cell::U64(1),
            Cell::U64(3),
            Cell::Null,
            Cell::U64(10),
            Cell::Str("delete.parquet".to_string()),
            Cell::Bool(true),
            Cell::Str("parquet".to_string()),
            Cell::U64(2),
            Cell::U64(128),
            Cell::U64(32),
            Cell::Null,
            Cell::Null,
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();

    // The rewrite: new file 12, end file 10 and delete file 11,
    // rebase 12's begin to this commit's snapshot.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(12, 1, 4),
    });
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::U64(4)],
    });
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DeleteFile,
        cells: vec![Cell::U64(1), Cell::U64(11), Cell::U64(4)],
    });
    tx.stage(RowOperation::UpdateSetBegin {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(12), Cell::U64(4)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "rewrite_delete:1"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let files = head.data_files_of(crate::catalog::TableId::new(1));
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].id.get(), 12);
    assert!(
        head.delete_files_of(crate::catalog::TableId::new(1))
            .is_empty()
    );
    assert!(head.scheduled_deletions().is_empty());

    // The ended rows survive in history; the replacement is rebased.
    let rows = crate::ffi_support::dump_data_files(&catalog).await.unwrap();
    assert!(
        rows.iter()
            .any(|f| f.data_file_id == 10 && f.end_snapshot == Some(4))
    );
    assert!(
        rows.iter()
            .any(|f| f.data_file_id == 12 && f.begin_snapshot == 4 && f.end_snapshot.is_none())
    );
    catalog.close().await.unwrap();
}

/// Rebasing a file that predates the commit is a shape error —
/// DuckLake only rebases the replacement it just inserted.
#[tokio::test]
async fn set_begin_on_a_preexisting_file_is_rejected() {
    let catalog = open().await;
    seed_expired_file(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(10, 1, 3),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetBegin {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::U64(4)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "rewrite_delete:1"),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(err.to_string().contains("predates this commit"), "{err}");
    catalog.close().await.unwrap();
}

/// Ending a tag entry that does not exist is a shape error — DuckLake
/// only updates rows it just read, so a miss means drift.
#[tokio::test]
async fn ending_an_absent_tag_entry_is_rejected() {
    let catalog = open().await;
    seed_table(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Tag,
        cells: vec![Cell::U64(1), Cell::Str("comment".into()), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(err.to_string().contains("tag"), "{err}");
    catalog.close().await.unwrap();
}

fn macro_row(schema_id: u64, macro_id: u64, name: &str, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(schema_id),
        Cell::U64(macro_id),
        Cell::Str(name.to_string()),
        Cell::U64(begin),
        Cell::Null, // end_snapshot
    ]
}

fn macro_impl_row(macro_id: u64, impl_id: u64, sql: &str, macro_type: &str) -> Vec<Cell> {
    vec![
        Cell::U64(macro_id),
        Cell::U64(impl_id),
        Cell::Str("duckdb".into()),
        Cell::Str(sql.to_string()),
        Cell::Str(macro_type.to_string()),
    ]
}

fn macro_parameter_row(
    macro_id: u64,
    impl_id: u64,
    column_id: u64,
    name: &str,
    default: Option<&str>,
) -> Vec<Cell> {
    vec![
        Cell::U64(macro_id),
        Cell::U64(impl_id),
        Cell::U64(column_id),
        Cell::Str(name.to_string()),
        Cell::Str("unknown".into()),
        default.map_or(Cell::Null, |d| Cell::Str(d.to_string())),
        Cell::Str(default.map_or("unknown", |_| "int32").to_string()),
    ]
}

/// Stages one commit's rows plus the snapshot pair and returns the
/// commit error, if any.
async fn stage_macro_batch(
    catalog: &Catalog,
    snapshot_id: u64,
    next_catalog_id: u64,
    rows: Vec<(TableKind, Vec<Cell>)>,
) -> Result<()> {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    for (table, cells) in rows {
        tx.stage(RowOperation::Insert { table, cells });
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(snapshot_id, 1, next_catalog_id),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(snapshot_id, r#"created_scalar_macro:"main"."m""#),
    });
    tx.commit().await.map(|_| ())
}

/// A macro insert with its impl and parameter rows folds into one
/// record (children ordered by their ordinals regardless of emit
/// order); a later drop ends the whole record into history, children
/// intact, and time travel still reads it.
#[tokio::test]
async fn macro_rows_land_fold_and_drop() {
    let catalog = open().await;

    // Rows deliberately emitted out of ordinal order.
    stage_macro_batch(
        &catalog,
        1,
        11,
        vec![
            (
                TableKind::MacroParameters,
                macro_parameter_row(10, 1, 1, "b", Some("5")),
            ),
            (
                TableKind::MacroImpl,
                macro_impl_row(10, 1, "(a + b)", "scalar"),
            ),
            (TableKind::Macro, macro_row(0, 10, "add", 1)),
            (
                TableKind::MacroImpl,
                macro_impl_row(10, 0, "(a + 1)", "scalar"),
            ),
            (
                TableKind::MacroParameters,
                macro_parameter_row(10, 0, 0, "a", None),
            ),
            (
                TableKind::MacroParameters,
                macro_parameter_row(10, 1, 0, "a", None),
            ),
        ],
    )
    .await
    .unwrap();

    let head = catalog.snapshot().await.unwrap();
    let stored = &head.macros[&10];
    assert_eq!(stored.begin_snapshot, 1);
    assert_eq!(stored.implementations.len(), 2);
    assert_eq!(stored.implementations[0].impl_id, 0);
    assert_eq!(stored.implementations[0].sql, "(a + 1)");
    assert_eq!(stored.implementations[1].parameters.len(), 2);
    assert_eq!(stored.implementations[1].parameters[1].parameter_name, "b");
    assert_eq!(
        stored.implementations[1].parameters[1]
            .default_value
            .as_deref(),
        Some("5")
    );

    // Drop: the one UPDATE DuckLake issues, nothing touching children.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_on(&catalog, db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Macro,
        cells: vec![Cell::U64(10), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 11),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "dropped_scalar_macro:10"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(head.macros.is_empty());
    let past = catalog.snapshot_at(SnapshotId::new(1)).await.unwrap();
    let past_macro = &past.macros[&10];
    assert_eq!(past_macro.end_snapshot, Some(2));
    assert_eq!(past_macro.implementations.len(), 2);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn macro_insert_without_impl_rows_is_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![(TableKind::Macro, macro_row(0, 10, "m", 1))],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("macro_impl"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn orphaned_macro_impl_row_is_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![(TableKind::MacroImpl, macro_impl_row(99, 0, "1", "scalar"))],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("macro_impl"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn orphaned_macro_parameter_row_is_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![
            (TableKind::Macro, macro_row(0, 10, "m", 1)),
            (TableKind::MacroImpl, macro_impl_row(10, 0, "1", "scalar")),
            (
                TableKind::MacroParameters,
                macro_parameter_row(10, 7, 0, "a", None),
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("macro_parameters"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn macro_impl_id_gap_is_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![
            (TableKind::Macro, macro_row(0, 10, "m", 1)),
            (TableKind::MacroImpl, macro_impl_row(10, 0, "1", "scalar")),
            (TableKind::MacroImpl, macro_impl_row(10, 2, "2", "scalar")),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("contiguous"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn macro_parameter_column_id_gap_is_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![
            (TableKind::Macro, macro_row(0, 10, "m", 1)),
            (TableKind::MacroImpl, macro_impl_row(10, 0, "1", "scalar")),
            (
                TableKind::MacroParameters,
                macro_parameter_row(10, 0, 1, "a", None),
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("contiguous"), "{err}");
    catalog.close().await.unwrap();
}

fn column_mapping_row(mapping_id: u64, table_id: u64) -> Vec<Cell> {
    vec![
        Cell::U64(mapping_id),
        Cell::U64(table_id),
        Cell::Str("map_by_name".into()),
    ]
}

fn name_mapping_row(
    mapping_id: u64,
    column_id: u64,
    source_name: &str,
    target_field_id: u64,
    parent_column: Option<u64>,
    is_partition: bool,
) -> Vec<Cell> {
    vec![
        Cell::U64(mapping_id),
        Cell::U64(column_id),
        Cell::Str(source_name.to_string()),
        Cell::U64(target_field_id),
        parent_column.map_or(Cell::Null, Cell::U64),
        Cell::Bool(is_partition),
    ]
}

/// Stages one commit's rows plus the snapshot pair and returns the
/// commit result.
async fn stage_mapping_batch(
    catalog: &Catalog,
    snapshot_id: u64,
    rows: Vec<(TableKind, Vec<Cell>)>,
) -> Result<()> {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    for (table, cells) in rows {
        tx.stage(RowOperation::Insert { table, cells });
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(snapshot_id, 1, 11),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(snapshot_id, "inserted_into_table:1"),
    });
    tx.commit().await.map(|_| ())
}

/// A column mapping folds its name-mapping rows (emitted out of
/// ordinal order, one nested child, one hive-partition virtual
/// column) and the added file carries its `mapping_id`; the record
/// is served at any time-travel target.
#[tokio::test]
async fn mapping_rows_land_fold_and_serve_time_travel() {
    let catalog = open().await;

    let mut file = data_file_row(7, 1, 1);
    file[14] = Cell::U64(21); // mapping_id
    stage_mapping_batch(
        &catalog,
        1,
        vec![
            (
                TableKind::NameMapping,
                name_mapping_row(21, 2, "region", 3, None, true),
            ),
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 1, "id", 2, Some(0), false),
            ),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "payload", 1, None, false),
            ),
            (TableKind::DataFile, file),
        ],
    )
    .await
    .unwrap();

    let head = catalog.snapshot().await.unwrap();
    let stored = &head.mappings[&1][&21];
    assert_eq!(stored.map_type, "map_by_name");
    assert_eq!(stored.name_mappings.len(), 3);
    assert_eq!(stored.name_mappings[0].source_name, "payload");
    assert_eq!(stored.name_mappings[1].parent_column, Some(0));
    assert!(stored.name_mappings[2].is_partition);
    assert_eq!(head.data_files[&1][&7].mapping_id, Some(21));

    // Unversioned: a time-travel view still serves the mapping.
    let past = catalog.snapshot_at(SnapshotId::new(1)).await.unwrap();
    assert_eq!(past.mappings[&1][&21].name_mappings.len(), 3);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn column_mapping_without_name_rows_is_rejected() {
    let catalog = open().await;
    let err = stage_mapping_batch(
        &catalog,
        1,
        vec![(TableKind::ColumnMapping, column_mapping_row(21, 1))],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("name_mapping"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn orphaned_name_mapping_row_is_rejected() {
    let catalog = open().await;
    let err = stage_mapping_batch(
        &catalog,
        1,
        vec![(
            TableKind::NameMapping,
            name_mapping_row(99, 0, "id", 1, None, false),
        )],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("name_mapping"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn duplicate_name_mapping_ordinals_are_rejected() {
    let catalog = open().await;
    let err = stage_mapping_batch(
        &catalog,
        1,
        vec![
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "a", 1, None, false),
            ),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "b", 2, None, false),
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("unique"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn name_mapping_parent_after_child_is_rejected() {
    let catalog = open().await;
    let err = stage_mapping_batch(
        &catalog,
        1,
        vec![
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "a", 1, Some(1), false),
            ),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 1, "b", 2, None, false),
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("earlier"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn duplicate_mapping_id_against_base_is_rejected() {
    let catalog = open().await;
    let mapping = || {
        vec![
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "id", 1, None, false),
            ),
        ]
    };
    stage_mapping_batch(&catalog, 1, mapping()).await.unwrap();
    let err = stage_mapping_batch(&catalog, 2, mapping())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn update_set_end_on_column_mapping_is_rejected() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::ColumnMapping,
        cells: vec![Cell::U64(21), Cell::U64(1)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 11),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "none"),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(err.to_string().contains("not defined"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn mixed_macro_impl_types_are_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![
            (TableKind::Macro, macro_row(0, 10, "m", 1)),
            (TableKind::MacroImpl, macro_impl_row(10, 0, "1", "scalar")),
            (
                TableKind::MacroImpl,
                macro_impl_row(10, 1, "SELECT 1", "table"),
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("share a type"), "{err}");
    catalog.close().await.unwrap();
}

/// `ALL` is the wire-decode table: each kind's discriminant is its
/// index, and out-of-range values are refused. The exhaustive match
/// makes adding a variant a compile error here until `ALL` (and the
/// ABI doc) cover it.
#[test]
fn table_kind_wire_order_is_pinned() {
    for (index, kind) in TableKind::ALL.iter().enumerate() {
        assert_eq!(*kind as usize, index, "{kind:?}");
        assert_eq!(TableKind::try_from(*kind as i32), Ok(*kind));
    }
    assert_eq!(TableKind::try_from(26), Err(26));
    assert_eq!(TableKind::try_from(-1), Err(-1));

    for kind in TableKind::ALL {
        match kind {
            TableKind::Snapshot
            | TableKind::SnapshotChanges
            | TableKind::Schema
            | TableKind::Table
            | TableKind::View
            | TableKind::Column
            | TableKind::DataFile
            | TableKind::DeleteFile
            | TableKind::TableStats
            | TableKind::TableColumnStats
            | TableKind::FileColumnStats
            | TableKind::SchemaVersions
            | TableKind::PartitionInfo
            | TableKind::PartitionColumn
            | TableKind::FilePartitionValue
            | TableKind::SortInfo
            | TableKind::SortExpression
            | TableKind::Tag
            | TableKind::ColumnTag
            | TableKind::FilesScheduledForDeletion
            | TableKind::Macro
            | TableKind::MacroImpl
            | TableKind::MacroParameters
            | TableKind::ColumnMapping
            | TableKind::NameMapping
            | TableKind::Metadata => {}
        }
    }
}

/// DuckLake mints the snapshot id from the head it read, so an id that does
/// not advance the head means a commit landed in between. Landing it would
/// overwrite a snapshot record and move the head backwards, so it is
/// refused — as the lost race it is, carrying the text DuckLake re-drives
/// on, never as corruption it would abandon the transaction over.
#[tokio::test]
async fn a_snapshot_id_that_does_not_advance_the_head_is_refused() {
    for authored in [0, 1] {
        let catalog = open().await;
        // Head stands at 1 after this commit.
        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut tx = StagedTransaction::begin_detached(db_tx);
        tx.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(1, 0, "t", 1, None),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
        });
        tx.commit().await.unwrap();

        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut tx = StagedTransaction::begin_detached(db_tx);
        tx.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(2, 0, "u", authored, None),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(authored, 2, 3),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(authored, r#"created_table:"main"."u""#),
        });
        let err = tx.commit().await.unwrap_err();
        assert!(
            matches!(&err, Error::CommitConflict(_)),
            "{authored}: {err:?}"
        );
        assert!(
            err.to_string().to_lowercase().contains("conflict"),
            "the loser must carry the text DuckLake's commit loop retries on: {err}"
        );

        let snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(snapshot.current_snapshot().id.get(), 1);
        assert!(
            snapshot
                .table_by_name(crate::catalog::SchemaId::new(0), "u")
                .is_none()
        );
    }
}

/// The column-type policy is the core's, so the staged path refuses a
/// `VARIANT` column at the commit that creates it — the same refusal the
/// verb path raises, and typed `Unsupported` rather than a constraint so
/// the bridge maps it to "not implemented".
#[tokio::test]
async fn a_variant_column_is_refused_as_unsupported() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    let mut column = column_row(1, 1, "v", 0);
    column[6] = Cell::Str("VARIANT".to_string());
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });

    let err = tx.commit().await.unwrap_err();
    assert!(matches!(err, Error::Unsupported(_)), "{err:?}");
    // Nothing landed: the whole commit aborts, table included.
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .table_by_name(crate::catalog::SchemaId::new(0), "t")
            .is_none()
    );
}

/// Options arrive as `ducklake_metadata` rows and mint no snapshot:
/// DuckLake writes them within its metadata connection, outside the
/// protocol, so the head must not move — while the option itself takes
/// effect. A later row for the same key overwrites it (last write wins),
/// a delete removes it, and a scope is carried through to the record the
/// key names.
///
/// Read back through a separate read-only handle rather than the staging
/// one: `begin_detached` stages against a throwaway projection cache, so
/// the staging handle's own cached view never learns of a commit that
/// leaves the head where it was. A reader opens on the store itself, which
/// is also the stronger claim — the option is durable, not just applied.
#[tokio::test]
async fn staged_option_rows_set_scoped_options_without_minting_a_snapshot() {
    use crate::catalog::{OptionScope, TableId};

    let store = Arc::new(InMemory::new());
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let head_before = catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .id
        .get();

    let option_row = |key: &str, value: &str, scope: Option<(&str, u64)>| {
        vec![
            Cell::Str(key.to_string()),
            Cell::Str(value.to_string()),
            scope.map_or(Cell::Null, |(name, _)| Cell::Str(name.to_string())),
            scope.map_or(Cell::Null, |(_, id)| Cell::U64(id)),
        ]
    };
    let option_key_row = |key: &str, scope: Option<(&str, u64)>| {
        vec![
            Cell::Str(key.to_string()),
            scope.map_or(Cell::Null, |(name, _)| Cell::Str(name.to_string())),
            scope.map_or(Cell::Null, |(_, id)| Cell::U64(id)),
        ]
    };
    let stored = |key: &'static str, scope: OptionScope| {
        let store = store.clone();
        async move {
            let reader = Catalog::open_read_only(store, CatalogOptions::default())
                .await
                .unwrap();
            let value = reader.snapshot().await.unwrap().option(scope, key);
            reader.close().await.unwrap();
            value
        }
    };

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Metadata,
        cells: option_row("parquet_compression", "zstd", None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Metadata,
        cells: option_row("parquet_compression", "snappy", Some(("table", 1))),
    });
    let id = tx.commit().await.unwrap();
    assert_eq!(
        id.get(),
        head_before,
        "an option write mints no snapshot, so the head stands"
    );

    assert_eq!(
        stored("parquet_compression", OptionScope::Global).await,
        Some("zstd".to_string())
    );
    assert_eq!(
        stored("parquet_compression", OptionScope::Table(TableId::new(1))).await,
        Some("snappy".to_string())
    );

    // Last write wins on the same key and scope; a delete removes it.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Metadata,
        cells: option_row("parquet_compression", "gzip", None),
    });
    // A delete names the option, not its value: the key cells alone, as
    // every other raw-delete kind carries them.
    tx.stage(RowOperation::Delete {
        table: TableKind::Metadata,
        cells: option_key_row("parquet_compression", Some(("table", 1))),
    });
    assert_eq!(tx.commit().await.unwrap().get(), head_before);

    assert_eq!(
        stored("parquet_compression", OptionScope::Global).await,
        Some("gzip".to_string())
    );
    // The table override is gone, so the table scope resolves to the
    // global value again rather than to nothing.
    assert_eq!(
        stored("parquet_compression", OptionScope::Table(TableId::new(1))).await,
        Some("gzip".to_string())
    );
    catalog.close().await.unwrap();
}

fn delete_file_row(delete_file_id: u64, table_id: u64, data_file_id: u64, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(delete_file_id),
        Cell::U64(table_id),
        Cell::U64(begin),
        Cell::Null, // end_snapshot
        Cell::U64(data_file_id),
        Cell::Str("deletes.parquet".into()), // path
        Cell::Bool(true),                    // path_is_relative
        Cell::Str("parquet".into()),         // format
        Cell::U64(1),                        // delete_count
        Cell::U64(128),                      // file_size_bytes
        Cell::U64(32),                       // footer_size
        Cell::Null,                          // encryption_key
        Cell::Null,                          // partial_max
    ]
}

/// Stages `rows` plus the snapshot pair minting `snapshot_id`, and commits.
async fn stage_batch(
    catalog: &Catalog,
    snapshot_id: u64,
    rows: Vec<(TableKind, Vec<Cell>)>,
) -> Result<()> {
    let db_tx = catalog.begin_write_tx().await?;
    let mut tx = StagedTransaction::begin_detached(db_tx);
    for (table, cells) in rows {
        tx.stage(RowOperation::Insert { table, cells });
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(snapshot_id, snapshot_id, 100),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(snapshot_id, "none"),
    });
    tx.commit().await.map(|_| ())
}

/// The id-collision backstop, one case per primary-keyed kind DuckLake's
/// own schema declares: `ducklake_schema`, `ducklake_data_file`, and
/// `ducklake_delete_file`. Inserting an id whose row is already live
/// would displace that row and mint a history version DuckLake never
/// authored, so it is refused with a typed `Constraint` — the same
/// refusal a SQL catalog's primary key gives DuckLake.
#[tokio::test]
async fn duplicate_live_ids_are_refused_on_every_primary_keyed_kind() {
    // Seed: schema 7, table 1, data file 3, delete file 4 — all live.
    let catalog = open().await;
    stage_batch(
        &catalog,
        1,
        vec![
            (TableKind::Schema, schema_row(7, "s", 1)),
            (TableKind::Table, table_row(1, 7, "t", 1, None)),
            (TableKind::DataFile, data_file_row(3, 1, 1)),
            (TableKind::DeleteFile, delete_file_row(4, 1, 3, 1)),
        ],
    )
    .await
    .unwrap();

    let collisions = [
        (TableKind::Schema, schema_row(7, "other", 2), "schema_id 7"),
        (
            TableKind::DataFile,
            data_file_row(3, 1, 2),
            "data_file_id 3",
        ),
        (
            TableKind::DeleteFile,
            delete_file_row(4, 1, 3, 2),
            "delete_file_id 4",
        ),
    ];
    for (table, cells, named) in collisions {
        let err = stage_batch(&catalog, 2, vec![(table, cells)])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Constraint(detail)
                if detail.contains(named) && detail.contains("already live")),
            "{table:?}: {err:?}"
        );
    }

    // Nothing landed: head still stands where the seed left it.
    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(snapshot.current_snapshot().id.get(), 1);
    assert_eq!(
        snapshot
            .schema_by_id(crate::catalog::SchemaId::new(7))
            .expect("schema 7 survives")
            .name,
        "s"
    );
    catalog.close().await.unwrap();
}

/// The two snapshot kinds are primary-keyed too, and their backstop is
/// the head-advance check rather than a lookup: a snapshot record exists
/// only for an id at or below head, so an id that advances the head
/// cannot collide, and one that does not is already refused
/// (`a_snapshot_id_that_does_not_advance_the_head_is_refused`). This
/// pins the reasoning as a property — re-inserting *any* already-minted
/// snapshot id fails.
///
/// It fails as a lost race rather than as corruption, which is the right
/// reading: DuckLake mints the id from the head it read, so an id at or
/// below head means a commit landed in between, and DuckLake re-drives on
/// the text of that error.
#[tokio::test]
async fn re_minting_an_existing_snapshot_id_is_refused() {
    let catalog = open().await;
    stage_batch(
        &catalog,
        1,
        vec![(TableKind::Schema, schema_row(7, "s", 1))],
    )
    .await
    .unwrap();
    stage_batch(
        &catalog,
        2,
        vec![(TableKind::Schema, schema_row(8, "u", 2))],
    )
    .await
    .unwrap();

    for existing in [0, 1, 2] {
        let err = stage_batch(&catalog, existing, vec![]).await.unwrap_err();
        assert!(
            matches!(&err, Error::CommitConflict(_)),
            "snapshot {existing}: {err:?}"
        );
    }
    catalog.close().await.unwrap();
}

/// Ending a row frees its id within the same commit: a rename ends the
/// old version and re-inserts under the same id, and the backstop must
/// not mistake that for a collision. Pinned for each kind the backstop
/// covers that DuckLake actually re-inserts this way.
#[tokio::test]
async fn re_inserting_an_id_whose_row_ended_in_the_same_commit_is_accepted() {
    let catalog = open().await;
    stage_batch(
        &catalog,
        1,
        vec![
            (TableKind::Schema, schema_row(7, "s", 1)),
            (TableKind::Table, table_row(1, 7, "t", 1, None)),
            (TableKind::DataFile, data_file_row(3, 1, 1)),
        ],
    )
    .await
    .unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Schema,
        cells: vec![Cell::U64(7), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Schema,
        cells: schema_row(7, "renamed", 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 2, 100),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "none"),
    });
    tx.commit().await.unwrap();

    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .schema_by_id(crate::catalog::SchemaId::new(7))
            .expect("schema 7 is live under its new name")
            .name,
        "renamed"
    );
    catalog.close().await.unwrap();
}

/// No name uniqueness is enforced anywhere on the staged path. DuckLake's
/// own catalog schema declares no `UNIQUE` constraint on any name column
/// — it polices naming in its binder, above the catalog — so moraine
/// must accept two live rows sharing a name rather than inventing a
/// constraint DuckLake's other backends do not have. (The verb path is a
/// different surface: it authors ids itself and does refuse a duplicate
/// name.)
#[tokio::test]
async fn staged_rows_enforce_no_name_uniqueness() {
    let catalog = open().await;
    stage_batch(
        &catalog,
        1,
        vec![
            (TableKind::Schema, schema_row(7, "dup", 1)),
            (TableKind::Schema, schema_row(8, "dup", 1)),
            (TableKind::Table, table_row(1, 7, "dup", 1, None)),
            (TableKind::Table, table_row(2, 7, "dup", 1, None)),
            (TableKind::Column, column_row(1, 1, "dup", 0)),
            (TableKind::Column, column_row(1, 2, "dup", 1)),
        ],
    )
    .await
    .unwrap();

    let snapshot = catalog.snapshot().await.unwrap();
    let schemas: Vec<_> = snapshot
        .schemas()
        .into_iter()
        .filter(|s| s.name == "dup")
        .collect();
    assert_eq!(schemas.len(), 2, "two live schemas may share a name");
    let tables = snapshot.tables_in(crate::catalog::SchemaId::new(7));
    assert_eq!(tables.len(), 2, "two live tables may share a name");
    let columns = snapshot.columns_of(crate::catalog::TableId::new(1));
    assert_eq!(
        columns.iter().filter(|c| c.name == "dup").count(),
        2,
        "two live columns may share a name"
    );
    catalog.close().await.unwrap();
}

/// Read-your-writes for the data-file projection: a transaction that
/// stages an insert, ends a committed row, and hard-deletes a history one
/// must see all three in its own scan. Without the overlay a cascade
/// SELECT reads committed state and re-plans work it has already staged.
#[tokio::test]
async fn visible_data_files_overlay_staged_inserts_ends_and_deletes() {
    let catalog = open().await;

    // Commit two files so the overlay has committed rows to move.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx);
    setup.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 1),
    });
    for id in [10, 11] {
        setup.stage(RowOperation::Insert {
            table: TableKind::DataFile,
            cells: data_file_row(id, 1, 1),
        });
    }
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    setup.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    // Committed state, before anything is staged.
    let mut ids: Vec<u64> = tx
        .visible_data_files()
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.data_file_id)
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![10, 11]);

    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(12, 1, 2),
    });
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::U64(2)],
    });

    let visible = tx.visible_data_files().await.unwrap();
    let ended: Vec<Option<u64>> = visible
        .iter()
        .filter(|f| f.data_file_id == 10)
        .map(|f| f.end_snapshot)
        .collect();
    assert_eq!(ended, vec![Some(2)], "the staged end is visible at once");
    assert!(
        visible.iter().any(|f| f.data_file_id == 12),
        "the staged insert is visible before commit"
    );

    // The hard delete a cascade issues against the row it just ended.
    tx.stage(RowOperation::Delete {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::U64(2)],
    });
    let visible = tx.visible_data_files().await.unwrap();
    assert!(
        !visible.iter().any(|f| f.data_file_id == 10),
        "the staged delete removes exactly the version it names"
    );
    assert!(
        visible.iter().any(|f| f.data_file_id == 11),
        "and leaves every other row alone"
    );

    tx.rollback();
    catalog.close().await.unwrap();
}

/// The unversioned kinds overlay by key: an insert overwrites the row it
/// keys rather than duplicating it, and a delete removes it. The deletion
/// schedule is the one a cleanup pass re-reads after staging its own
/// removals.
#[tokio::test]
async fn visible_scheduled_deletions_overlay_by_key() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    let scheduled = |id: u64, path: &str| {
        vec![
            Cell::U64(id),
            Cell::Str(path.to_string()),
            Cell::Bool(true),
            Cell::I64(7),
        ]
    };

    assert!(tx.visible_scheduled_deletions().await.unwrap().is_empty());

    tx.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: scheduled(10, "a.parquet"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: scheduled(11, "b.parquet"),
    });
    // Same key again: an overwrite, not a second row.
    tx.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: scheduled(10, "a2.parquet"),
    });

    let visible = tx.visible_scheduled_deletions().await.unwrap();
    assert_eq!(visible.len(), 2);
    assert_eq!(
        visible
            .iter()
            .find(|row| row.data_file_id == 10)
            .map(|row| row.path.as_str()),
        Some("a2.parquet")
    );

    tx.stage(RowOperation::Delete {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![Cell::U64(11)],
    });
    let visible = tx.visible_scheduled_deletions().await.unwrap();
    assert_eq!(
        visible
            .iter()
            .map(|row| row.data_file_id)
            .collect::<Vec<_>>(),
        vec![10]
    );

    tx.rollback();
    catalog.close().await.unwrap();
}

/// The overlay covers the kinds a cascade walks, not only files: a table
/// ended in this transaction, and a column inserted by it, are both
/// visible to its own next read.
#[tokio::test]
async fn visible_tables_and_columns_follow_the_staged_rows() {
    let catalog = open().await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx);
    setup.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    setup.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Table,
        cells: vec![Cell::U64(1), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 2, "b", 2),
    });

    let tables = tx.visible_tables().await.unwrap();
    assert_eq!(
        tables
            .iter()
            .filter(|t| t.table_id == 1)
            .map(|t| t.end_snapshot)
            .collect::<Vec<_>>(),
        vec![Some(2)]
    );

    let mut columns: Vec<u64> = tx
        .visible_columns()
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.column_id)
        .collect();
    columns.sort_unstable();
    assert_eq!(columns, vec![1, 2]);

    tx.rollback();
    catalog.close().await.unwrap();
}

/// The container kinds fold rather than append: a staged `ducklake_tag`
/// row becomes an entry inside its object's record, an
/// `UPDATE ... SET end_snapshot` ends that entry, and a delete removes it —
/// taking an emptied container with it, so the projection stops reporting
/// the object at all.
#[tokio::test]
async fn visible_tag_containers_fold_staged_entries() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    // `ducklake_tag`'s declared order: object_id, begin, end, key, value.
    let tag = |object_id: u64, key: &str, begin: u64| {
        vec![
            Cell::U64(object_id),
            Cell::U64(begin),
            Cell::Null,
            Cell::Str(key.to_string()),
            Cell::Str("v".to_string()),
        ]
    };

    assert!(tx.visible_tag_containers().await.unwrap().is_empty());

    tx.stage(RowOperation::Insert {
        table: TableKind::Tag,
        cells: tag(7, "owner", 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Tag,
        cells: tag(7, "team", 1),
    });
    let containers = tx.visible_tag_containers().await.unwrap();
    assert_eq!(containers.len(), 1, "both entries ride one container");
    assert_eq!(containers[0].entries.len(), 2);

    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Tag,
        cells: vec![Cell::U64(7), Cell::Str("owner".to_string()), Cell::U64(2)],
    });
    let ended = tx.visible_tag_containers().await.unwrap();
    assert_eq!(
        ended[0]
            .entries
            .iter()
            .find(|e| e.key == "owner")
            .and_then(|e| e.end_snapshot),
        Some(2)
    );

    for key in ["owner", "team"] {
        tx.stage(RowOperation::Delete {
            table: TableKind::Tag,
            cells: vec![Cell::U64(7), Cell::Str(key.to_string()), Cell::U64(1)],
        });
    }
    assert!(
        tx.visible_tag_containers().await.unwrap().is_empty(),
        "the emptied container goes with its last entry"
    );

    tx.rollback();
    catalog.close().await.unwrap();
}

/// Options live outside the snapshot protocol, so their overlay is
/// last-write-wins by `(scope, key)`: an insert sets, a repeat overwrites,
/// a delete removes, and an emptied scope disappears. A delete of a key
/// already gone is a no-op, as translation treats it.
#[tokio::test]
async fn visible_option_scopes_overlay_last_write_wins() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    let option = |scope_kind: u64, scope_id: u64, key: &str, value: &str| {
        vec![
            Cell::Str(key.to_string()),
            Cell::Str(value.to_string()),
            match scope_kind {
                1 => Cell::Str("schema".to_string()),
                2 => Cell::Str("table".to_string()),
                _ => Cell::Null,
            },
            if scope_kind == 0 {
                Cell::Null
            } else {
                Cell::U64(scope_id)
            },
        ]
    };

    // Bootstrap already set the global scope's `encrypted`.
    let committed = tx.visible_option_scopes().await.unwrap();
    assert_eq!(committed.len(), 1);

    tx.stage(RowOperation::Insert {
        table: TableKind::Metadata,
        cells: option(2, 9, "k", "first"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Metadata,
        cells: option(2, 9, "k", "second"),
    });
    let scopes = tx.visible_option_scopes().await.unwrap();
    let table_scope = scopes
        .iter()
        .find(|(kind, id, _)| (*kind, *id) == (2, 9))
        .expect("the staged table scope");
    assert_eq!(
        table_scope.2.options.get("k").map(String::as_str),
        Some("second")
    );

    tx.stage(RowOperation::Delete {
        table: TableKind::Metadata,
        cells: option(2, 9, "k", "second"),
    });
    let scopes = tx.visible_option_scopes().await.unwrap();
    assert!(
        !scopes.iter().any(|(kind, id, _)| (*kind, *id) == (2, 9)),
        "the emptied scope goes with its last key"
    );
    assert_eq!(scopes.len(), 1, "and no other scope is touched");

    tx.rollback();
    catalog.close().await.unwrap();
}

/// The `schema_version` records this transaction can see: its own inserts
/// and deletes over the committed ones. The `ducklake_schema_versions`
/// projection is assembled from these plus the snapshots and data files it
/// also overlays, so what is pinned here is the record set, not the fold.
#[tokio::test]
async fn visible_schema_version_records_follow_the_staged_rows() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    // Cells arrive in DuckLake's declared order: begin_snapshot, then
    // schema_version, then table_id.
    let row = |begin: u64, version: u64, table: u64| {
        vec![Cell::U64(begin), Cell::U64(version), Cell::U64(table)]
    };

    assert!(
        tx.visible_schema_version_records()
            .await
            .unwrap()
            .is_empty(),
        "a bootstrapped store has no schema-version records of its own"
    );

    tx.stage(RowOperation::Insert {
        table: TableKind::SchemaVersions,
        cells: row(1, 1, 5),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SchemaVersions,
        cells: row(2, 2, 6),
    });
    assert_eq!(
        tx.visible_schema_version_records().await.unwrap(),
        vec![(5, 1, 1), (6, 2, 2)]
    );

    tx.stage(RowOperation::Delete {
        table: TableKind::SchemaVersions,
        cells: row(1, 1, 5),
    });
    assert_eq!(
        tx.visible_schema_version_records().await.unwrap(),
        vec![(6, 2, 2)]
    );

    tx.rollback();
    catalog.close().await.unwrap();
}

/// The dead-table cleanup's `DELETE FROM ducklake_column_mapping WHERE
/// table_id IN (...)`: the record goes, and its embedded name-mapping rows
/// go with it. The delete carries the key columns the metadata table
/// declares — `(mapping_id, table_id)` — and no `end_snapshot`, mappings
/// being unversioned.
#[tokio::test]
async fn column_mapping_delete_reclaims_the_record_and_its_embedded_rows() {
    let catalog = open().await;
    stage_mapping_batch(
        &catalog,
        1,
        vec![
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "payload", 1, None, false),
            ),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 1, "id", 2, Some(0), false),
            ),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        catalog.snapshot().await.unwrap().mappings[&1][&21]
            .name_mappings
            .len(),
        2
    );

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_on(&catalog, db_tx);
    tx.stage(RowOperation::Delete {
        table: TableKind::ColumnMapping,
        cells: vec![Cell::U64(21), Cell::U64(1)],
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(
        head.mappings
            .get(&1)
            .is_none_or(|per| !per.contains_key(&21)),
        "the mapping record must be gone"
    );
    // Embedded rows have no keys of their own, so the parent's deletion is
    // the whole reclamation — nothing survives to sweep.
    assert!(
        crate::ffi_support::dump_mappings(&catalog)
            .await
            .unwrap()
            .is_empty(),
        "no mapping row may survive its record"
    );
    catalog.close().await.unwrap();
}

/// DuckLake follows the mapping delete with an orphan sweep over
/// `ducklake_name_mapping`. In this keyspace those rows are embedded in
/// the record just deleted, so the sweep has nothing left to remove — it
/// must be accepted and land as a no-op rather than refused.
#[tokio::test]
async fn the_name_mapping_orphan_sweep_rides_its_parents_deletion() {
    let catalog = open().await;
    stage_mapping_batch(
        &catalog,
        1,
        vec![
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "payload", 1, None, false),
            ),
        ],
    )
    .await
    .unwrap();

    // Both DELETEs in one transaction, in DuckLake's order.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Delete {
        table: TableKind::ColumnMapping,
        cells: vec![Cell::U64(21), Cell::U64(1)],
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::NameMapping,
        cells: vec![Cell::U64(21)],
    });
    tx.commit().await.unwrap();

    assert!(
        crate::ffi_support::dump_mappings(&catalog)
            .await
            .unwrap()
            .is_empty()
    );
    catalog.close().await.unwrap();
}

/// A sweep that arrives after its parent is already gone — the two DELETE
/// streams landing in separate transactions — is equally benign.
#[tokio::test]
async fn a_name_mapping_sweep_after_its_parent_is_gone_is_accepted() {
    let catalog = open().await;
    stage_mapping_batch(
        &catalog,
        1,
        vec![
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "payload", 1, None, false),
            ),
        ],
    )
    .await
    .unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Delete {
        table: TableKind::ColumnMapping,
        cells: vec![Cell::U64(21), Cell::U64(1)],
    });
    tx.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Delete {
        table: TableKind::NameMapping,
        cells: vec![Cell::U64(21)],
    });
    tx.commit().await.unwrap();

    assert!(
        crate::ffi_support::dump_mappings(&catalog)
            .await
            .unwrap()
            .is_empty()
    );
    catalog.close().await.unwrap();
}

/// Read-your-writes across the delete: the transaction that staged it no
/// longer sees the mapping, while the committed state still does until it
/// lands.
#[tokio::test]
async fn a_staged_mapping_delete_is_invisible_to_its_own_transaction() {
    let catalog = open().await;
    stage_mapping_batch(
        &catalog,
        1,
        vec![
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "payload", 1, None, false),
            ),
        ],
    )
    .await
    .unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Delete {
        table: TableKind::ColumnMapping,
        cells: vec![Cell::U64(21), Cell::U64(1)],
    });
    assert!(
        tx.visible_mappings().await.unwrap().is_empty(),
        "the staging transaction must not serve a mapping it deleted"
    );
    tx.rollback();

    assert_eq!(
        crate::ffi_support::dump_mappings(&catalog)
            .await
            .unwrap()
            .len(),
        1,
        "a rolled-back delete leaves the record"
    );
    catalog.close().await.unwrap();
}
