use std::sync::Arc;

use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

use super::*;

/// A prepared head view is installed by allocation rather than rebuilt.
#[test]
fn a_prepared_head_view_is_installed_without_rebuilding() {
    let projections = std::sync::RwLock::new(ProjectionCache::empty());
    let prepared = Arc::new(CatalogSnapshot {
        snapshot: proto::SnapshotValue {
            snapshot_id: 7,
            ..proto::SnapshotValue::default()
        },
        batch_seq: 11,
        ..CatalogSnapshot::default()
    });

    install_committed_head_view(
        &projections,
        HeadViewUpdate::Prepared(Arc::clone(&prepared)),
        &[],
    );

    let installed = held_head_view(&projections).unwrap();
    assert!(Arc::ptr_eq(&installed, &prepared));
}

/// The durable-commit wrapper is transparent: it hands back exactly what
/// the commit reported. The wait it wraps is unbounded on purpose — a
/// deadline could not cancel the staged batch, so it would report failure
/// for a commit that still lands — and all it adds is a log line once the
/// wait runs long.
#[tokio::test]
async fn a_durable_commit_returns_its_own_outcome() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let (db, _) = StoreBuilder::new("", object_store)
        .open_writer()
        .await
        .unwrap();

    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    tx.put(Key::Sys(SysKey::Head).encode(), b"head").unwrap();
    assert!(
        commit_durable(tx, "test", StagedBytes::default())
            .await
            .is_ok()
    );
    assert_eq!(
        db.get(&Key::Sys(SysKey::Head).encode())
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"head"
    );

    db.close().await.unwrap();
}

/// A store whose manifest cannot be read fails the open outright, rather
/// than being mistaken for an empty store and bootstrapped over.
#[tokio::test]
async fn a_store_with_an_unreadable_manifest_refuses_to_open() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    StoreBuilder::new("", object_store.clone())
        .open_writer()
        .await
        .unwrap()
        .0
        .close()
        .await
        .unwrap();

    // Overwrite every manifest with bytes no version of one can parse.
    let manifests: Vec<_> = object_store
        .list(None)
        .map_ok(|meta| meta.location)
        .try_filter(|location| futures::future::ready(location.to_string().contains("manifest")))
        .try_collect()
        .await
        .unwrap();
    assert!(!manifests.is_empty(), "the store must have a manifest");
    for manifest in manifests {
        object_store
            .put(&manifest, vec![0xff; 64].into())
            .await
            .unwrap();
    }

    let err = open_initialized(StoreBuilder::new("", object_store), false, None)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, Error::Store(_)), "{err:?}");
}

/// A store stamped with a newer structural format must be refused,
/// not misread.
#[tokio::test]
async fn unknown_format_is_refused() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let (db, _) = StoreBuilder::new("", object_store.clone())
        .open_writer()
        .await
        .unwrap();
    db.put(
        &Key::Sys(SysKey::Format).encode(),
        &value::encode_value(&proto::FormatValue {
            format_version: MAX_FORMAT_VERSION + 1,
            writer_version: "future".into(),
        }),
    )
    .await
    .unwrap();
    db.close().await.unwrap();

    // `Result::unwrap_err` needs `T: Debug`, and `slatedb::Db` has no
    // `Debug` impl; `err().unwrap()` only needs it on the error side.
    let err = open_initialized(StoreBuilder::new("", object_store), false, None)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, Error::Migration(_)), "{err:?}");
}

/// A mid-migration marker refuses the open outright.
#[tokio::test]
async fn migration_marker_is_refused() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let (db, _) = StoreBuilder::new("", object_store.clone())
        .open_writer()
        .await
        .unwrap();
    db.put(
        &Key::Sys(SysKey::Migration).encode(),
        &value::encode_value(&proto::MigrationValue {
            from_format: 1,
            to_format: 2,
            cursor: vec![],
        }),
    )
    .await
    .unwrap();
    db.close().await.unwrap();

    let err = open_initialized(StoreBuilder::new("", object_store), false, None)
        .await
        .err()
        .unwrap();
    match err {
        Error::Migration(msg) => assert!(
            msg.contains("Catalog::migrate"),
            "mid-migration message names no verb: {msg}"
        ),
        other => panic!("expected Migration, got {other:?}"),
    }
}

/// A format below this binary's floor refuses toward the migrate path,
/// distinct from the newer-than-binary message. The floor sits at the base
/// format while every format is additive, so only a synthetic store reaches
/// this arm; the test holds it correct for the first format that raises it.
///
/// The message must name the verb, and name it as something this binary
/// runs: `Catalog::migrate` takes a store path and never goes through the
/// format check, so the store an attach refuses is still migratable by the
/// binary that refused it. That is the non-obvious half, and the half an
/// operator gets wrong.
#[tokio::test]
async fn older_format_refuses_toward_migrate() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let (db, _) = StoreBuilder::new("", object_store.clone())
        .open_writer()
        .await
        .unwrap();
    db.put(
        &Key::Sys(SysKey::Format).encode(),
        &value::encode_value(&proto::FormatValue {
            format_version: 0,
            writer_version: "t".into(),
        }),
    )
    .await
    .unwrap();
    db.close().await.unwrap();

    let err = open_initialized(StoreBuilder::new("", object_store), false, None)
        .await
        .err()
        .unwrap();
    match err {
        Error::Migration(msg) => {
            assert!(
                msg.contains("Catalog::migrate"),
                "older-store message names no verb: {msg}"
            );
            assert!(
                msg.contains("this same binary"),
                "older-store message does not say who can run it: {msg}"
            );
        }
        other => panic!("expected Migration, got {other:?}"),
    }
}

/// A migration marker present under a live head makes every materialization
/// unavailable, not partial — the reader-side gate, not only the open gate.
#[tokio::test]
async fn materialize_gate_refuses_on_marker() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let (db, _) = StoreBuilder::new("", object_store)
        .open_writer()
        .await
        .unwrap();
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    tx.put(
        Key::Sys(SysKey::Head).encode(),
        value::encode_value(&proto::HeadValue {
            snapshot_id: 0,
            batch_seq: 0,
        }),
    )
    .unwrap();
    tx.put(
        Key::Sys(SysKey::Migration).encode(),
        value::encode_value(&proto::MigrationValue {
            from_format: 1,
            to_format: 2,
            cursor: vec![],
        }),
    )
    .unwrap();
    tx.commit_with_options(&durable()).await.unwrap();

    let read = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let err = materialize(ReadHandle::Tx(&read), None)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, Error::Migration(_)), "{err:?}");
    read.rollback();
}

/// Renaming one column touches that column and nothing else: churn is
/// proportional to the change, not to the table's width.
#[test]
fn renaming_one_column_stages_no_write_for_any_sibling() {
    use crate::{
        catalog::ColumnDef,
        store::key::{CurrentKey, HistoryKey},
    };

    let column_def = |name: &str| ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
        children: Vec::new(),
    };

    let snap0 = proto::SnapshotValue {
        snapshot_id: 0,
        snapshot_time_micros: 0,
        schema_version: 0,
        next_catalog_id: 1,
        next_file_id: 0,
        changes_made: String::new(),
        author: None,
        commit_message: None,
        commit_extra_info: None,
        schema_changed_table_ids: Vec::new(),
        deleted_data_file_ids: Vec::new(),
    };
    let mut setup = Transaction::new(CatalogSnapshot::build(snap0, &[], &[], None), 1);
    let schema = setup.create_schema("s").unwrap();
    let table = setup
        .create_table(
            schema,
            "t",
            &[
                column_def("a"),
                column_def("b"),
                column_def("c"),
                column_def("d"),
            ],
        )
        .unwrap();
    let renamed = setup.columns_of(table)[1].id;
    let base = setup.into_parts().state;

    let mut tx = Transaction::new(base.clone(), 2);
    tx.rename_column(table, renamed, "b2").unwrap();
    let state = tx.into_parts().state;

    let touched: Vec<u64> = diff_writes(&base, &state, 2)
        .iter()
        .filter_map(|(key_bytes, _)| match Key::decode(key_bytes).unwrap() {
            Key::Current(CurrentKey::Entity(EntityKey::Column { column_id, .. }))
            | Key::History(HistoryKey {
                entity: EntityKey::Column { column_id, .. },
                ..
            }) => Some(column_id),
            _ => None,
        })
        .collect();

    assert!(
        touched.iter().all(|id| *id == renamed.get()),
        "a rename of column {} staged writes for siblings: {touched:?}",
        renamed.get()
    );
    assert!(
        !touched.is_empty(),
        "the renamed column itself must be written"
    );
}

/// A file registered and expired within one commit exists in neither
/// `base` nor `state`'s `data_files`: its per-file column stats are
/// orphaned and must never be staged as a write.
#[test]
fn register_then_expire_in_one_commit_stages_no_orphaned_file_column_stats() {
    use crate::{
        catalog::{ColumnDef, DataFile, FileColumnStats},
        store::key::{CurrentKey, HistoryKey},
    };

    let snap0 = proto::SnapshotValue {
        snapshot_id: 0,
        snapshot_time_micros: 1,
        schema_version: 0,
        next_catalog_id: 0,
        next_file_id: 0,
        changes_made: String::new(),
        author: None,
        commit_message: None,
        commit_extra_info: None,
        schema_changed_table_ids: Vec::new(),
        deleted_data_file_ids: Vec::new(),
    };
    let empty = CatalogSnapshot::build(snap0, &[], &[], None);
    let mut setup = Transaction::new(empty, 1);
    let schema = setup.create_schema("s").unwrap();
    let table = setup
        .create_table(
            schema,
            "t",
            &[ColumnDef {
                name: "a".into(),
                column_type: "BIGINT".into(),
                nulls_allowed: true,
                default_value: None,
                children: Vec::new(),
            }],
        )
        .unwrap();
    let column = setup.columns_of(table)[0].id;
    let base = setup.into_parts().state;

    // Register a file with column stats, then expire it — all inside
    // this one commit's transaction.
    let mut tx = Transaction::new(base.clone(), 2);
    let file = tx
        .register_data_file(
            table,
            DataFile {
                path: "f.parquet".into(),
                path_is_relative: true,
                file_format: "parquet".into(),
                record_count: 10,
                file_size_bytes: 100,
                footer_size: 4,
                encryption_key: None,
                partition_values: vec![],
                column_stats: vec![FileColumnStats {
                    column_id: column,
                    column_size_bytes: 10,
                    value_count: 10,
                    null_count: 0,
                    min_value: Some("1".into()),
                    max_value: Some("2".into()),
                    contains_nan: None,
                    extra_stats: None,
                }],
            },
            &[],
        )
        .unwrap();
    tx.expire_data_file(table, file).unwrap();
    let state = tx.into_parts().state;

    let writes = diff_writes(&base, &state, 2);
    for (key_bytes, _) in &writes {
        let key = Key::decode(key_bytes).unwrap();
        let is_file_column_stats = matches!(
            key,
            Key::Current(CurrentKey::Entity(EntityKey::FileColumnStats { .. }))
                | Key::History(HistoryKey {
                    entity: EntityKey::FileColumnStats { .. },
                    ..
                })
        );
        assert!(
            !is_file_column_stats,
            "orphaned file_column_stats write staged: {key:?}"
        );
    }
}

/// A fresh reader opened after commit returns resolves the new head:
/// commit durability must imply visibility to subsequently opened
/// handles.
#[tokio::test]
async fn fresh_reader_sees_committed_head() {
    use slatedb::DbReader;

    use crate::catalog::{Catalog, CatalogOptions};

    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("visible").map(|_| ()))
        .await
        .unwrap();

    let reader = DbReader::builder("", object_store)
        .with_segment_extractor(Arc::new(crate::store::segment::TagSegmentExtractor))
        .build()
        .await
        .unwrap();
    let head_bytes = reader
        .get(Key::Sys(SysKey::Head).encode())
        .await
        .unwrap()
        .expect("fresh reader must see the head");
    let head: proto::HeadValue = value::decode_value(&head_bytes).unwrap();
    assert_eq!(head.snapshot_id, 1);
    reader.close().await.unwrap();
    catalog.close().await.unwrap();
}

/// Verb-path DDL records the shape-changed table ids on its snapshot,
/// one per changed table or view — the id set `ducklake_schema_versions`
/// rows are served from. Data-only commits record none.
#[tokio::test]
async fn verb_ddl_records_schema_changed_table_ids() {
    use crate::catalog::{Catalog, CatalogOptions, ColumnDef, DataFile};

    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();

    // One commit creating a table and altering it twice: the id set
    // dedups to that one table.
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            let table = tx.create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                    children: Vec::new(),
                }],
            )?;
            tx.rename_table(table, "t2")?;
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();
    let table = created.get().unwrap();

    // A data-only commit changes no table's shape.
    catalog
        .commit(|tx| {
            tx.register_data_file(
                table,
                DataFile {
                    path: "f.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 1,
                    file_size_bytes: 10,
                    footer_size: 4,
                    encryption_key: None,
                    partition_values: vec![],
                    column_stats: vec![],
                },
                &[],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let ddl = read::read_snapshot(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ddl.schema_changed_table_ids, vec![table.get()]);
    let data_only = read::read_snapshot(ReadHandle::Tx(&tx), 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data_only.schema_changed_table_ids, Vec::<u64>::new());
    tx.rollback();
    catalog.close().await.unwrap();
}

async fn catalog_with_two_column_table() -> (crate::catalog::Catalog, crate::catalog::TableId) {
    use crate::catalog::{Catalog, CatalogOptions, ColumnDef};
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let table = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            let column = |name: &str| ColumnDef {
                name: name.into(),
                column_type: "BIGINT".into(),
                nulls_allowed: true,
                default_value: None,
                children: Vec::new(),
            };
            let created = tx.create_table(schema, "t", &[column("a"), column("b")])?;
            table.set(Some(created));
            Ok(())
        })
        .await
        .unwrap();
    (catalog, table.get().unwrap())
}

/// A `BIGINT`-shaped index key value.
fn int_value(value: i128) -> crate::store::index_encoding::IndexKeyValue {
    crate::store::index_encoding::IndexKeyValue::Int {
        value,
        width: crate::store::index_encoding::IntWidth::I64,
    }
}

/// A one-column index key over a single integer value.
fn int_key(value: i128) -> Vec<crate::store::index_encoding::IndexKeyValue> {
    vec![int_value(value)]
}

fn entry(row_id: u64, value: i128) -> crate::catalog::IndexEntry {
    crate::catalog::IndexEntry {
        row_id,
        values: vec![Some(int_value(value))],
    }
}

/// An index entry over two integer columns.
fn pair_entry(row_id: u64, first: i128, second: i128) -> crate::catalog::IndexEntry {
    crate::catalog::IndexEntry {
        row_id,
        values: vec![Some(int_value(first)), Some(int_value(second))],
    }
}

/// The row ids a query resolved, sorted — hit order is asserted separately.
fn sorted_row_ids(mut ids: Vec<u64>) -> Vec<u64> {
    ids.sort_unstable();
    ids
}

/// An index entry for a row whose single indexed column is NULL.
fn null_entry(row_id: u64) -> crate::catalog::IndexEntry {
    crate::catalog::IndexEntry {
        row_id,
        values: vec![None],
    }
}

async fn read_format_version(catalog: &crate::catalog::Catalog) -> u64 {
    let tx = catalog.begin_write_tx().await.unwrap();
    let format = read::read_format(ReadHandle::Tx(&tx)).await.unwrap();
    tx.rollback();
    format.map_or(FORMAT_VERSION, |f| f.format_version)
}

#[tokio::test]
async fn create_index_persists_definition_stamps_format_and_lands_entries() {
    use crate::{
        catalog::{ColumnId, IndexDef, IndexState},
        store::key::{IndexKind, index_index_prefix},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    assert_eq!(read_format_version(&catalog).await, FORMAT_VERSION);

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 20)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index_id = index.get().unwrap();

    let snapshot = catalog.snapshot().await.unwrap();
    let infos = snapshot.indexes_of(table);
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].id, index_id);
    assert_eq!(infos[0].columns, vec![ColumnId::new(1)]);
    assert!(infos[0].unique);
    assert_eq!(infos[0].state, IndexState::Ready);
    assert_eq!(read_format_version(&catalog).await, FORMAT_WITH_INDEX);

    // Both backfill rows produced a stored entry.
    let tx = catalog.begin_write_tx().await.unwrap();
    let mut iter = ReadHandle::Tx(&tx)
        .scan_prefix(
            index_index_prefix(IndexKind::Unique, index_id.get()),
            ..,
            crate::store::handle::ScanShape::Bulk,
        )
        .await
        .unwrap();
    let mut count = 0;
    while iter.next().await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 2);
    tx.rollback();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn deferred_index_is_non_unique_and_stamps_its_format() {
    use crate::catalog::{ColumnId, IndexDef, IndexMaintenance};
    let (catalog, table) = catalog_with_two_column_table().await;

    catalog
        .commit(|tx| {
            tx.create_index_ordered_with_maintenance(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                },
                &[],
                IndexMaintenance::Deferred,
                &[],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(
        snapshot.indexes_of(table)[0].maintenance,
        IndexMaintenance::Deferred
    );
    assert_eq!(
        read_format_version(&catalog).await,
        FORMAT_WITH_DEFERRED_INDEX
    );

    let error = catalog
        .commit(|tx| {
            tx.create_index_ordered_with_maintenance(
                table,
                &IndexDef {
                    name: "unique_by_b".into(),
                    columns: vec![ColumnId::new(2)],
                    unique: true,
                },
                &[],
                IndexMaintenance::Deferred,
                &[],
            )
            .map(|_| ())
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("non-unique"), "{error}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn duplicate_unique_value_in_backfill_aborts_create() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    let err = catalog
        .commit(|tx| {
            tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 10)],
            )
            .map(|_| ())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");

    // The aborted commit left no index and did not stamp the format.
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .indexes_of(table)
            .is_empty()
    );
    assert_eq!(read_format_version(&catalog).await, FORMAT_VERSION);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn non_unique_index_accepts_duplicate_values() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    catalog
        .commit(|tx| {
            tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                },
                &[entry(0, 10), entry(1, 10)],
            )
            .map(|_| ())
        })
        .await
        .unwrap();
    assert_eq!(catalog.snapshot().await.unwrap().indexes_of(table).len(), 1);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn null_indexed_value_gets_no_entry_so_unique_admits_many() {
    use crate::catalog::{ColumnId, IndexDef, IndexEntry};
    let (catalog, table) = catalog_with_two_column_table().await;
    let null_entry = |row_id| IndexEntry {
        row_id,
        values: vec![None],
    };
    // Two NULL rows under a unique index: NULLs get no entry, so no
    // collision.
    catalog
        .commit(|tx| {
            tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[null_entry(0), null_entry(1)],
            )
            .map(|_| ())
        })
        .await
        .unwrap();
    assert_eq!(catalog.snapshot().await.unwrap().indexes_of(table).len(), 1);
    catalog.close().await.unwrap();
}

async fn register_three_row_file(
    catalog: &crate::catalog::Catalog,
    table: crate::catalog::TableId,
) -> crate::catalog::DataFileId {
    use crate::catalog::DataFile;
    let file = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.register_data_file(
                table,
                DataFile {
                    path: "f.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 3,
                    file_size_bytes: 30,
                    footer_size: 4,
                    encryption_key: None,
                    partition_values: vec![],
                    column_stats: vec![],
                },
                &[],
            )?;
            file.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    file.get().unwrap()
}

#[tokio::test]
async fn index_lookup_resolves_unique_value_to_its_data_file_row() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    // Rows 0,1,2 land in this file (row_id_start = 0).
    register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let hits = catalog
        .index_lookup(table, index, &[int_value(20)])
        .await
        .unwrap();
    assert_eq!(hits, vec![1]);
    // A value no row holds resolves to nothing.
    assert!(
        catalog
            .index_lookup(table, index, &[int_value(99)])
            .await
            .unwrap()
            .is_empty()
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_lookup_returns_all_rows_for_a_non_unique_value() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // Rows 0 and 2 share value 10; row 1 is 20.
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 10)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();

    let value = crate::store::index_encoding::IndexKeyValue::Int {
        value: 10,
        width: crate::store::index_encoding::IntWidth::I64,
    };
    let mut rows: Vec<u64> = catalog
        .index_lookup(table, index.get().unwrap(), &[value])
        .await
        .unwrap()
        .into_iter()
        .collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![0, 2]);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_range_selects_unique_values_in_a_bounded_interval() {
    use std::ops::Bound;

    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // Rows 0,1,2 hold ascending values 10,20,30.
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // BETWEEN 15 AND 25 — only value 20 (row 1).
    let between = catalog
        .index_range(
            table,
            index,
            Bound::Included(int_key(15)),
            Bound::Included(int_key(25)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(between, vec![1]);

    // > 20 (half-open) — value 30 (row 2).
    let above = catalog
        .index_range(
            table,
            index,
            Bound::Excluded(int_key(20)),
            Bound::Unbounded,
            false,
        )
        .await
        .unwrap();
    assert_eq!(sorted_row_ids(above), vec![2]);

    // <= 20 — values 10 and 20 (rows 0, 1).
    let below = catalog
        .index_range(
            table,
            index,
            Bound::Unbounded,
            Bound::Included(int_key(20)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(sorted_row_ids(below), vec![0, 1]);

    // Closed [10, 30] covers every row.
    let all = catalog
        .index_range(
            table,
            index,
            Bound::Included(int_key(10)),
            Bound::Included(int_key(30)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(sorted_row_ids(all), vec![0, 1, 2]);

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_range_reverse_serves_the_opposite_order() {
    use std::ops::Bound;

    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let order = |query: Vec<u64>| query;

    // Ascending index: default order is low-to-high, `reverse` is high-to-low.
    let ascending = catalog
        .index_range(
            table,
            index,
            Bound::Included(int_key(10)),
            Bound::Included(int_key(30)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(order(ascending), vec![0, 1, 2]);
    let reversed = catalog
        .index_range(
            table,
            index,
            Bound::Included(int_key(10)),
            Bound::Included(int_key(30)),
            true,
        )
        .await
        .unwrap();
    assert_eq!(
        order(reversed),
        vec![2, 1, 0],
        "reverse serves the exact opposite order"
    );

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_range_refuses_a_bound_wider_than_the_index() {
    use std::ops::Bound;

    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // The index has one column; a two-value bound names a column it lacks.
    let err = catalog
        .index_range(
            table,
            index,
            Bound::Included(vec![int_value(10), int_value(20)]),
            Bound::Unbounded,
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_range_over_a_non_unique_index_returns_every_matching_row() {
    use std::ops::Bound;

    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // Rows 0 and 2 share value 10; row 1 is 20.
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 10)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // < 20 — both rows holding value 10.
    let low = catalog
        .index_range(
            table,
            index,
            Bound::Unbounded,
            Bound::Excluded(int_key(20)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(sorted_row_ids(low), vec![0, 2]);

    // >= 20 — only row 1.
    let high = catalog
        .index_range(
            table,
            index,
            Bound::Included(int_key(20)),
            Bound::Unbounded,
            false,
        )
        .await
        .unwrap();
    assert_eq!(sorted_row_ids(high), vec![1]);

    catalog.close().await.unwrap();
}

/// A comparison never matches a NULL. A non-unique index holds NULL-bearing
/// and valued rows in one subrange, so an open side must stop at the NULL
/// region rather than run into it.
#[tokio::test]
async fn index_range_excludes_nulls_from_an_open_side() {
    use std::ops::Bound;

    use crate::{
        catalog::{ColumnId, ColumnOrder, IndexDef},
        store::index_encoding::{Direction, NullOrder},
    };

    // NULLS LAST puts the NULL above every value, so the unbounded upper side
    // of `a >= 10` faces it; NULLS FIRST puts it below, exposing the lower
    // side of `a <= 20` instead. Both must return only the valued rows.
    for (nulls, lower, upper) in [
        (
            NullOrder::Last,
            Bound::Included(int_key(10)),
            Bound::Unbounded,
        ),
        (
            NullOrder::First,
            Bound::Unbounded,
            Bound::Included(int_key(20)),
        ),
    ] {
        let (catalog, table) = catalog_with_two_column_table().await;
        register_three_row_file(&catalog, table).await;

        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let id = tx.create_index_ordered(
                    table,
                    &IndexDef {
                        name: "by_a".into(),
                        columns: vec![ColumnId::new(1)],
                        unique: false,
                    },
                    &[ColumnOrder {
                        direction: Direction::Ascending,
                        nulls,
                    }],
                    &[entry(0, 10), entry(1, 20), null_entry(2)],
                )?;
                index.set(Some(id));
                Ok(())
            })
            .await
            .unwrap();
        let index = index.get().unwrap();

        let hits = catalog
            .index_range(table, index, lower.clone(), upper.clone(), false)
            .await
            .unwrap();
        assert_eq!(
            sorted_row_ids(hits),
            vec![0, 1],
            "{nulls:?} leaked the NULL row"
        );

        catalog.close().await.unwrap();
    }
}

#[tokio::test]
async fn descending_index_scans_high_value_first() {
    use std::ops::Bound;

    use crate::{
        catalog::{ColumnId, ColumnOrder, IndexDef},
        store::index_encoding::{Direction, NullOrder},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index_ordered(
                table,
                &IndexDef {
                    name: "by_a_desc".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[ColumnOrder {
                    direction: Direction::Descending,
                    nulls: NullOrder::Last,
                }],
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // Results come back in the index's stored order — descending by value.
    let order = |hits: Vec<u64>| hits;

    // Closed [10, 30]: 30, 20, 10 -> rows 2, 1, 0.
    let all = catalog
        .index_range(
            table,
            index,
            Bound::Included(int_key(10)),
            Bound::Included(int_key(30)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        order(all),
        vec![2, 1, 0],
        "descending index scans high first"
    );

    // a > 15 (half-open): 30, 20 -> rows 2, 1.
    let above = catalog
        .index_range(
            table,
            index,
            Bound::Excluded(int_key(15)),
            Bound::Unbounded,
            false,
        )
        .await
        .unwrap();
    assert_eq!(order(above), vec![2, 1]);

    // a <= 20: 20, 10 -> rows 1, 0.
    let below = catalog
        .index_range(
            table,
            index,
            Bound::Unbounded,
            Bound::Included(int_key(20)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(order(below), vec![1, 0]);

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn unique_index_admits_null_rows_and_index_nulls_finds_them() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // Row 0 holds a=10; rows 1 and 2 are both a=NULL. A unique index
            // must accept two NULL rows — SQL treats NULLs as distinct.
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), null_entry(1), null_entry(2)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .expect("a unique index must admit multiple NULL rows");
    let index = index.get().unwrap();

    // `a IS NULL` resolves to exactly the two NULL rows.
    let mut nulls: Vec<u64> = catalog
        .index_nulls(table, index, vec![None], false)
        .await
        .unwrap()
        .into_iter()
        .collect();
    nulls.sort_unstable();
    assert_eq!(nulls, vec![1, 2], "IS NULL finds both NULL rows");

    let reversed_nulls = catalog
        .index_nulls(table, index, vec![None], true)
        .await
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(
        reversed_nulls,
        vec![2, 1],
        "a reverse NULL-prefix scan streams the opposite order"
    );

    // The non-null value is still uniquely resolvable, unaffected.
    let value = vec![crate::store::index_encoding::IndexKeyValue::Int {
        value: 10,
        width: crate::store::index_encoding::IntWidth::I64,
    }];
    assert_eq!(
        catalog.index_lookup(table, index, &value).await.unwrap(),
        vec![0]
    );
    // A pure-equality prefix through index_nulls is refused.
    let err = catalog
        .index_nulls(
            table,
            index,
            vec![Some(crate::store::index_encoding::IndexKeyValue::Int {
                value: 10,
                width: crate::store::index_encoding::IntWidth::I64,
            })],
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn composite_index_nulls_matches_a_leading_prefix() {
    use crate::catalog::{ColumnId, IndexDef, IndexEntry};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    // This index mixes present and NULL columns, so its entries name both.
    let entry = |row_id, a, b| IndexEntry {
        row_id,
        values: vec![a, b],
    };
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // row 0: (5, NULL); row 1: (5, 3); row 2: (7, NULL).
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_ab".into(),
                    columns: vec![ColumnId::new(1), ColumnId::new(2)],
                    unique: false,
                },
                &[
                    entry(0, Some(int_value(5)), None),
                    entry(1, Some(int_value(5)), Some(int_value(3))),
                    entry(2, Some(int_value(7)), None),
                ],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // a = 5 AND b IS NULL -> only row 0 (row 1 has b=3, row 2 has a=7).
    assert_eq!(
        sorted_row_ids(
            catalog
                .index_nulls(table, index, vec![Some(int_value(5)), None], false)
                .await
                .unwrap()
        ),
        vec![0]
    );
    // b IS NULL across all a -> rows 0 and 2 is a gap pattern (leading a
    // free) and is not expressible; a IS NULL (leading) matches no row.
    assert!(
        catalog
            .index_nulls(table, index, vec![None], false)
            .await
            .unwrap()
            .is_empty()
    );

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_range_spans_a_composite_prefix_and_window() {
    use std::ops::Bound;

    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // row 0: (5, 10); row 1: (5, 20); row 2: (7, 10).
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_ab".into(),
                    columns: vec![ColumnId::new(1), ColumnId::new(2)],
                    unique: true,
                },
                &[
                    pair_entry(0, 5, 10),
                    pair_entry(1, 5, 20),
                    pair_entry(2, 7, 10),
                ],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // A one-column prefix bound pins the leading column: a = 5 spans both
    // rows sharing it, whatever their second column. This is the case the
    // outer value-framing's terminator would strand if the extension bound
    // were computed from the terminated suffix.
    let equal_a = catalog
        .index_range(
            table,
            index,
            Bound::Included(vec![int_value(5)]),
            Bound::Included(vec![int_value(5)]),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        sorted_row_ids(equal_a),
        vec![0, 1],
        "a = 5 spans rows 0 and 1"
    );

    // A full-tuple window: (5, 20) ..= (7, 10) excludes (5, 10) below the
    // lower bound and includes (5, 20) and (7, 10).
    let window = catalog
        .index_range(
            table,
            index,
            Bound::Included(vec![int_value(5), int_value(20)]),
            Bound::Included(vec![int_value(7), int_value(10)]),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        sorted_row_ids(window),
        vec![1, 2],
        "(5,20)..=(7,10) spans rows 1 and 2"
    );

    // A one-column prefix Excluded lower skips every extension of a = 5.
    let above_a = catalog
        .index_range(
            table,
            index,
            Bound::Excluded(vec![int_value(5)]),
            Bound::Unbounded,
            false,
        )
        .await
        .unwrap();
    assert_eq!(sorted_row_ids(above_a), vec![2], "a > 5 spans only row 2");

    let forward = catalog
        .index_range(table, index, Bound::Unbounded, Bound::Unbounded, false)
        .await
        .unwrap();
    let reverse = catalog
        .index_range(table, index, Bound::Unbounded, Bound::Unbounded, true)
        .await
        .unwrap();
    assert_eq!(forward, vec![0, 1, 2]);
    assert_eq!(reverse, vec![2, 1, 0]);

    catalog.close().await.unwrap();
}

/// The bound above a prefix value increments its escaped body, so it must
/// carry over trailing `0xFF`s. A descending column complements its framed
/// bytes, making a value that ends in zero bytes end in `0xFF`; a non-unique
/// entry then appends a row id the increment also has to clear.
#[tokio::test]
async fn index_range_prefix_bound_carries_over_a_descending_value() {
    use std::ops::Bound;

    use crate::{
        catalog::{ColumnId, ColumnOrder, IndexDef},
        store::index_encoding::{Direction, NullOrder},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let descending = ColumnOrder {
        direction: Direction::Descending,
        nulls: NullOrder::Last,
    };

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // 256 and 512 both end in a zero byte, which frames to `0x01 0x00`
            // and complements to `0xFE 0xFF` under a descending column.
            let id = tx.create_index_ordered(
                table,
                &IndexDef {
                    name: "by_ab_desc".into(),
                    columns: vec![ColumnId::new(1), ColumnId::new(2)],
                    unique: false,
                },
                &[descending, descending],
                &[
                    pair_entry(0, 256, 1),
                    pair_entry(1, 256, 2),
                    pair_entry(2, 512, 1),
                ],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // a = 256 spans both rows holding it, over their differing second column
    // and their trailing row ids.
    let equal_a = catalog
        .index_range(
            table,
            index,
            Bound::Included(vec![int_value(256)]),
            Bound::Included(vec![int_value(256)]),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        sorted_row_ids(equal_a),
        vec![0, 1],
        "a = 256 spans rows 0 and 1"
    );

    // a > 256 excludes every extension of 256 and keeps the larger value.
    let above_a = catalog
        .index_range(
            table,
            index,
            Bound::Excluded(vec![int_value(256)]),
            Bound::Unbounded,
            false,
        )
        .await
        .unwrap();
    assert_eq!(sorted_row_ids(above_a), vec![2], "a > 256 spans only row 2");

    catalog.close().await.unwrap();
}

/// No single byte range spans a free tuple window over columns that sort
/// opposite ways, so such bounds are refused. Pinning the leading column
/// leaves the last to order the window, which one scan serves.
#[tokio::test]
async fn index_range_over_mixed_directions_requires_a_pinned_prefix() {
    use std::ops::Bound;

    use crate::{
        catalog::{ColumnId, ColumnOrder, IndexDef},
        error::Error,
        store::index_encoding::{Direction, NullOrder},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let ascending = ColumnOrder {
        direction: Direction::Ascending,
        nulls: NullOrder::Last,
    };
    let descending = ColumnOrder {
        direction: Direction::Descending,
        nulls: NullOrder::Last,
    };

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // row 0: (5, 10); row 1: (5, 20); row 2: (7, 10).
            let id = tx.create_index_ordered(
                table,
                &IndexDef {
                    name: "by_a_asc_b_desc".into(),
                    columns: vec![ColumnId::new(1), ColumnId::new(2)],
                    unique: true,
                },
                &[ascending, descending],
                &[
                    pair_entry(0, 5, 10),
                    pair_entry(1, 5, 20),
                    pair_entry(2, 7, 10),
                ],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // A free-floating tuple window names both columns without pinning either.
    let refused = catalog
        .index_range(
            table,
            index,
            Bound::Included(vec![int_value(5), int_value(20)]),
            Bound::Included(vec![int_value(7), int_value(10)]),
            false,
        )
        .await;
    assert!(
        matches!(refused, Err(Error::Constraint(_))),
        "a free tuple window over mixed directions is refused, got {refused:?}"
    );

    // Pinning a = 5 leaves b to order the window: 10 <= b <= 20 spans both.
    let pinned = catalog
        .index_range(
            table,
            index,
            Bound::Included(vec![int_value(5), int_value(10)]),
            Bound::Included(vec![int_value(5), int_value(20)]),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        sorted_row_ids(pinned),
        vec![0, 1],
        "a = 5 and 10 <= b <= 20"
    );

    // A bound naming only the leading column involves one direction, so it
    // stays available.
    let leading = catalog
        .index_range(
            table,
            index,
            Bound::Included(vec![int_value(7)]),
            Bound::Unbounded,
            false,
        )
        .await
        .unwrap();
    assert_eq!(sorted_row_ids(leading), vec![2], "a >= 7 spans only row 2");

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_lookup_on_missing_index_is_not_found() {
    use crate::catalog::IndexId;
    let (catalog, table) = catalog_with_two_column_table().await;
    let value = crate::store::index_encoding::IndexKeyValue::Int {
        value: 1,
        width: crate::store::index_encoding::IntWidth::I64,
    };
    let err = catalog
        .index_lookup(table, IndexId::new(999), &[value])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn register_data_file_must_supply_index_entries_and_they_are_looked_up() {
    use crate::catalog::{ColumnId, DataFile, FileIndexEntry, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let file = || DataFile {
        path: "f.parquet".into(),
        path_is_relative: true,
        file_format: "parquet".into(),
        record_count: 2,
        file_size_bytes: 20,
        footer_size: 4,
        encryption_key: None,
        partition_values: vec![],
        column_stats: vec![],
    };

    // A non-empty file on an indexed table with no entries is refused.
    let refused = catalog
        .commit(|tx| tx.register_data_file(table, file(), &[]).map(|_| ()))
        .await;
    assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");

    // With entries it lands; ordinals map to row ids 0 and 1.
    catalog
        .commit(|tx| {
            tx.register_data_file(
                table,
                file(),
                &[
                    FileIndexEntry {
                        index,
                        ordinal: 0,
                        values: vec![Some(int_value(10))],
                    },
                    FileIndexEntry {
                        index,
                        ordinal: 1,
                        values: vec![Some(int_value(20))],
                    },
                ],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let hits = catalog
        .index_lookup(table, index, &[int_value(20)])
        .await
        .unwrap();
    assert_eq!(hits, vec![1]);
    catalog.close().await.unwrap();
}

/// Entries for `count` consecutive integer values starting at `first`,
/// ordinals `0..count`, one indexed column.
fn bulk_file_entries(
    index: crate::catalog::IndexId,
    first: i128,
    count: u64,
) -> Vec<crate::catalog::FileIndexEntry> {
    (0..count)
        .map(|ordinal| crate::catalog::FileIndexEntry {
            index,
            ordinal,
            values: vec![Some(int_value(first + i128::from(ordinal)))],
        })
        .collect()
}

/// A data file shaped to carry `count` rows under `path`.
fn bulk_file(path: &str, count: u64) -> crate::catalog::DataFile {
    crate::catalog::DataFile {
        path: path.into(),
        path_is_relative: true,
        file_format: "parquet".into(),
        record_count: count,
        file_size_bytes: count * 10,
        footer_size: 4,
        encryption_key: None,
        partition_values: vec![],
        column_stats: vec![],
    }
}

/// A large bulk commit resolves its unique entries in bounded concurrent
/// groups; enforcement is unchanged: fresh values land, a value a committed
/// live row already holds aborts the whole commit, and nothing of the aborted
/// commit remains visible.
#[tokio::test]
async fn bulk_unique_commit_lands_and_committed_duplicate_aborts() {
    use crate::catalog::{ColumnId, IndexDef};
    const COUNT: u64 = 1500;
    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    catalog
        .commit(|tx| {
            tx.register_data_file(table, bulk_file("a.parquet", COUNT), &{
                bulk_file_entries(index, 0, COUNT)
            })
            .map(|_| ())
        })
        .await
        .unwrap();

    // A second bulk commit whose entries repeat one committed value (42,
    // held by a live row of the first file) must abort in full.
    let mut duplicated = bulk_file_entries(index, i128::from(COUNT), COUNT);
    duplicated[700].values = vec![Some(int_value(42))];
    let err = catalog
        .commit(|tx| {
            tx.register_data_file(table, bulk_file("b.parquet", COUNT), &duplicated)
                .map(|_| ())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");

    // Value 42 still resolves to the first file's row; none of the aborted
    // commit's fresh values are visible.
    let hits = catalog
        .index_lookup(table, index, &[int_value(42)])
        .await
        .unwrap();
    assert_eq!(sorted_row_ids(hits), vec![42]);
    assert!(
        catalog
            .index_lookup(table, index, &[int_value(i128::from(COUNT) + 10)])
            .await
            .unwrap()
            .is_empty()
    );

    // A fresh bulk commit after the abort lands, and both files resolve.
    catalog
        .commit(|tx| {
            tx.register_data_file(table, bulk_file("c.parquet", COUNT), &{
                bulk_file_entries(index, i128::from(COUNT), COUNT)
            })
            .map(|_| ())
        })
        .await
        .unwrap();
    for probe in [0, 1499, 1500, 2999] {
        assert_eq!(
            catalog
                .index_lookup(table, index, &[int_value(probe)])
                .await
                .unwrap()
                .len(),
            1,
            "value {probe} resolves after the fresh bulk commit"
        );
    }
    catalog.close().await.unwrap();
}

/// Values killed earlier in the same bulk commit are free again for the
/// commit's own inserts — the delete-then-reinsert contract holds for a
/// large bulk commit too.
#[tokio::test]
async fn bulk_unique_commit_frees_values_deleted_in_the_same_commit() {
    use crate::catalog::{ColumnId, FileIndexRemoval, IndexDef};
    const COUNT: u64 = 1500;
    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    let file = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            index.set(Some(id));
            let registered = tx.register_data_file(table, bulk_file("a.parquet", COUNT), &{
                bulk_file_entries(id, 0, COUNT)
            })?;
            file.set(Some(registered));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();
    let file = file.get().unwrap();

    // One commit: kill every first-file row (freeing its value), and
    // register a new file re-claiming every one of those values.
    catalog
        .commit(|tx| {
            let removals: Vec<FileIndexRemoval> = (0..COUNT)
                .map(|row_id| FileIndexRemoval {
                    index,
                    row_id,
                    values: vec![Some(int_value(i128::from(row_id)))],
                })
                .collect();
            let mut killer = delete_file(file);
            killer.delete_count = COUNT;
            tx.register_delete_file(table, killer, &removals)?;
            tx.register_data_file(table, bulk_file("b.parquet", COUNT), &{
                bulk_file_entries(index, 0, COUNT)
            })
            .map(|_| ())
        })
        .await
        .unwrap();

    // The values resolve to the second file's rows (ids COUNT..2*COUNT).
    let hits = catalog
        .index_lookup(table, index, &[int_value(700)])
        .await
        .unwrap();
    assert_eq!(sorted_row_ids(hits), vec![COUNT + 700]);
    catalog.close().await.unwrap();
}

/// With two unique indexes maintained in one bulk commit, a duplicate is
/// detected in whichever index holds it, and the error names that index.
#[tokio::test]
async fn bulk_commit_over_two_unique_indexes_names_the_violated_index() {
    use crate::catalog::{ColumnId, IndexDef};
    const COUNT: u64 = 1500;
    let (catalog, table) = catalog_with_two_column_table().await;
    let by_a = std::cell::Cell::new(None);
    let by_b = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let a = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            let b = tx.create_index(
                table,
                &IndexDef {
                    name: "by_b".into(),
                    columns: vec![ColumnId::new(2)],
                    unique: true,
                },
                &[],
            )?;
            by_a.set(Some(a));
            by_b.set(Some(b));
            Ok(())
        })
        .await
        .unwrap();
    let by_a = by_a.get().unwrap();
    let by_b = by_b.get().unwrap();

    let both = |first_a: i128, first_b: i128| {
        let mut entries = bulk_file_entries(by_a, first_a, COUNT);
        entries.extend(bulk_file_entries(by_b, first_b, COUNT));
        entries
    };
    catalog
        .commit(|tx| {
            tx.register_data_file(table, bulk_file("a.parquet", COUNT), &both(0, 100_000))
                .map(|_| ())
        })
        .await
        .unwrap();

    // Fresh `by_a` values, one committed duplicate among `by_b`'s.
    let mut entries = both(i128::from(COUNT), 100_000 + i128::from(COUNT));
    entries[usize::try_from(COUNT).unwrap() + 900].values = vec![Some(int_value(100_042))];
    let err = catalog
        .commit(|tx| {
            tx.register_data_file(table, bulk_file("b.parquet", COUNT), &entries)
                .map(|_| ())
        })
        .await
        .unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains(&by_b.get().to_string()) && !text.contains(&by_a.get().to_string()),
        "the violation names the violated index: {text}"
    );
    catalog.close().await.unwrap();
}

/// Sets up an indexed table holding one two-row data file (values 10 and
/// 20 at row ids 0 and 1), returning the catalog, table, index and file.
async fn catalog_with_indexed_data_file() -> (
    crate::catalog::Catalog,
    crate::catalog::TableId,
    crate::catalog::IndexId,
    crate::catalog::DataFileId,
) {
    use crate::catalog::{ColumnId, DataFile, FileIndexEntry, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    let file = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            index.set(Some(id));
            let registered = tx.register_data_file(
                table,
                DataFile {
                    path: "f.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 2,
                    file_size_bytes: 20,
                    footer_size: 4,
                    encryption_key: None,
                    partition_values: vec![],
                    column_stats: vec![],
                },
                &[
                    FileIndexEntry {
                        index: id,
                        ordinal: 0,
                        values: vec![Some(int_value(10))],
                    },
                    FileIndexEntry {
                        index: id,
                        ordinal: 1,
                        values: vec![Some(int_value(20))],
                    },
                ],
            )?;
            file.set(Some(registered));
            Ok(())
        })
        .await
        .unwrap();
    (catalog, table, index.get().unwrap(), file.get().unwrap())
}

fn delete_file(data_file: crate::catalog::DataFileId) -> crate::catalog::DeleteFile {
    crate::catalog::DeleteFile {
        data_file_id: data_file,
        path: "d.parquet".into(),
        path_is_relative: true,
        format: "parquet".into(),
        delete_count: 1,
        file_size_bytes: 10,
        footer_size: 4,
        encryption_key: None,
    }
}

/// A delete file names the rows it kills, so their entries go with them
/// and the value is free to be indexed again.
#[tokio::test]
async fn register_delete_file_removes_the_entries_it_names() {
    use crate::catalog::FileIndexRemoval;
    let (catalog, table, index, file) = catalog_with_indexed_data_file().await;

    catalog
        .commit(|tx| {
            tx.register_delete_file(
                table,
                delete_file(file),
                &[FileIndexRemoval {
                    index,
                    row_id: 1,
                    values: vec![Some(int_value(20))],
                }],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    assert!(
        catalog
            .index_lookup(table, index, &[int_value(20)])
            .await
            .unwrap()
            .is_empty(),
        "the killed row's entry is gone"
    );
    assert_eq!(
        catalog
            .index_lookup(table, index, &[int_value(10)])
            .await
            .unwrap()
            .len(),
        1,
        "the surviving row is still indexed"
    );
    catalog.close().await.unwrap();
}

/// Supplying no entries on an indexed table is refused, exactly as it is
/// on the register side — a silently under-covered index is a lie.
#[tokio::test]
async fn register_delete_file_must_supply_index_entries() {
    let (catalog, table, _, file) = catalog_with_indexed_data_file().await;
    let refused = catalog
        .commit(|tx| {
            tx.register_delete_file(table, delete_file(file), &[])
                .map(|_| ())
        })
        .await;
    assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");
    catalog.close().await.unwrap();
}

/// Entries without deletes would strip the index of rows the catalog
/// still counts as live.
#[tokio::test]
async fn register_delete_file_rejects_index_entries_without_deletes() {
    use crate::catalog::FileIndexRemoval;
    let (catalog, table, index, file) = catalog_with_indexed_data_file().await;
    let refused = catalog
        .commit(|tx| {
            tx.register_delete_file(
                table,
                crate::catalog::DeleteFile {
                    delete_count: 0,
                    ..delete_file(file)
                },
                &[FileIndexRemoval {
                    index,
                    row_id: 1,
                    values: vec![Some(int_value(20))],
                }],
            )
            .map(|_| ())
        })
        .await;
    assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");
    catalog.close().await.unwrap();
}

/// A row id past a dense target's range would name a row it does not hold.
#[tokio::test]
async fn register_delete_file_rejects_an_out_of_range_row_id() {
    use crate::catalog::FileIndexRemoval;
    let (catalog, table, index, file) = catalog_with_indexed_data_file().await;
    let refused = catalog
        .commit(|tx| {
            tx.register_delete_file(
                table,
                delete_file(file),
                &[FileIndexRemoval {
                    index,
                    row_id: 2,
                    values: vec![Some(int_value(30))],
                }],
            )
            .map(|_| ())
        })
        .await;
    assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn unique_index_rejects_a_duplicate_value_across_commits() {
    use crate::catalog::{ColumnId, DataFile, FileIndexEntry, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let one_row_with = |value: i128| {
        let file = DataFile {
            path: "f.parquet".into(),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: 1,
            file_size_bytes: 10,
            footer_size: 4,
            encryption_key: None,
            partition_values: vec![],
            column_stats: vec![],
        };
        (
            file,
            FileIndexEntry {
                index,
                ordinal: 0,
                values: vec![Some(int_value(value))],
            },
        )
    };

    // First value 10 lands.
    catalog
        .commit(|tx| {
            let (file, entry) = one_row_with(10);
            tx.register_data_file(table, file, &[entry]).map(|_| ())
        })
        .await
        .unwrap();
    // A later commit inserting the same value 10 (different row) is
    // rejected by the point-get against the winner's entry.
    let dup = catalog
        .commit(|tx| {
            let (file, entry) = one_row_with(10);
            tx.register_data_file(table, file, &[entry]).map(|_| ())
        })
        .await;
    assert!(matches!(dup, Err(Error::Constraint(_))), "{dup:?}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn scoped_read_covers_a_registration_end_to_end() {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use parquet::arrow::ArrowWriter;

    use crate::{
        catalog::{ColumnId, DataFile, IndexDef},
        store::index_encoding::{IndexKeyValue, IntWidth},
    };

    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // A DATA_PATH object store holds a Parquet file with the indexed
    // column "a" at physical position 0.
    let data = Arc::new(InMemory::new());
    let path = Path::from("t/data-1.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20, 30]))]).unwrap();
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let file_size = buffer.len() as u64;
    let footer_offset = buffer.len() - 8;
    let footer_size = u64::from(u32::from_le_bytes(
        buffer[footer_offset..footer_offset + 4].try_into().unwrap(),
    ));
    data.put(&path, buffer.into()).await.unwrap();

    // moraine derives coverage entries by the scoped read (column "a"
    // at position 0), then registration lands them — DuckLake supplied
    // none, and the read stands in for the refusal.
    let entries = catalog
        .scoped_file_index_entries(data.clone(), &path, file_size, footer_size, index, &[0])
        .await
        .unwrap();
    assert_eq!(entries.len(), 3);
    catalog
        .commit(|tx| {
            tx.register_data_file(
                table,
                DataFile {
                    path: "t/data-1.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 3,
                    file_size_bytes: file_size,
                    footer_size,
                    encryption_key: None,
                    partition_values: vec![],
                    column_stats: vec![],
                },
                &entries,
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let value = IndexKeyValue::Int {
        value: 20,
        width: IntWidth::I64,
    };
    let hits = catalog.index_lookup(table, index, &[value]).await.unwrap();
    assert_eq!(hits, vec![1]);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn ddl_on_an_indexed_column_is_guarded() {
    use crate::catalog::{ColumnAlteration, ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    catalog
        .commit(|tx| {
            tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10)],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    // Dropping or retyping the indexed column is refused.
    let dropped = catalog
        .commit(|tx| tx.drop_column(table, ColumnId::new(1)))
        .await;
    assert!(matches!(dropped, Err(Error::Constraint(_))), "{dropped:?}");
    let retyped = catalog
        .commit(|tx| {
            tx.alter_column(
                table,
                ColumnId::new(1),
                ColumnAlteration {
                    column_type: Some("INTEGER".into()),
                    ..ColumnAlteration::default()
                },
            )
        })
        .await;
    assert!(matches!(retyped, Err(Error::Constraint(_))), "{retyped:?}");

    // Renaming the indexed column, and retyping a non-indexed column,
    // are unaffected.
    catalog
        .commit(|tx| tx.rename_column(table, ColumnId::new(1), "a2"))
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            tx.alter_column(
                table,
                ColumnId::new(2),
                ColumnAlteration {
                    column_type: Some("INTEGER".into()),
                    ..ColumnAlteration::default()
                },
            )
        })
        .await
        .unwrap();
    catalog.close().await.unwrap();
}

async fn scan_index_entries(
    catalog: &crate::catalog::Catalog,
    index: crate::catalog::IndexId,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    use crate::store::key::{IndexKind, index_index_prefix};
    let tx = catalog.begin_write_tx().await.unwrap();
    let mut entries = Vec::new();
    for kind in [IndexKind::Unique, IndexKind::Multi] {
        let mut iter = ReadHandle::Tx(&tx)
            .scan_prefix(
                index_index_prefix(kind, index.get()),
                ..,
                crate::store::handle::ScanShape::Bulk,
            )
            .await
            .unwrap();
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push((entry.key.to_vec(), entry.value.to_vec()));
        }
    }
    tx.rollback();
    entries.sort();
    entries
}

#[tokio::test]
async fn staged_build_gates_lookups_flips_ready_and_matches_single_commit() {
    use crate::catalog::{ColumnId, IndexDef, IndexState};
    let def = || IndexDef {
        name: "by_a".into(),
        columns: vec![ColumnId::new(1)],
        unique: true,
    };

    // Reference: a single-commit build over rows 0,1,2.
    let (single, table_single) = catalog_with_two_column_table().await;
    register_three_row_file(&single, table_single).await;
    let single_index = std::cell::Cell::new(None);
    single
        .commit(|tx| {
            let id = tx.create_index(
                table_single,
                &def(),
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            single_index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let single_index = single_index.get().unwrap();

    // Staged: same table shape, same rows, built in two batches.
    let (staged, table_staged) = catalog_with_two_column_table().await;
    register_three_row_file(&staged, table_staged).await;
    let staged_index = std::cell::Cell::new(None);
    staged
        .commit(|tx| {
            let id = tx.create_index_staged(table_staged, &def())?;
            staged_index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let staged_index = staged_index.get().unwrap();
    // Identical allocation sequence → identical index id, so the index
    // keys can be compared directly.
    assert_eq!(single_index, staged_index);

    // While building: format 3, lookups fail typed.
    assert_eq!(read_format_version(&staged).await, FORMAT_WITH_STAGED_INDEX);
    assert!(matches!(
        staged
            .index_lookup(table_staged, staged_index, &[int_value(20)])
            .await,
        Err(Error::IndexBuilding(_))
    ));

    // Two batches, the second final.
    staged
        .commit(|tx| {
            tx.build_index_step(staged_index, &[entry(0, 10), entry(1, 20)], false)
                .map(|_| ())
        })
        .await
        .unwrap();
    let final_state = std::cell::Cell::new(None);
    staged
        .commit(|tx| {
            let state = tx.build_index_step(staged_index, &[entry(2, 30)], true)?;
            final_state.set(Some(state));
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(final_state.get().unwrap(), IndexState::Ready);

    // After the flip: lookups serve, and the index range is byte-identical
    // to the single-commit build over the same rows.
    let hits = staged
        .index_lookup(table_staged, staged_index, &[int_value(20)])
        .await
        .unwrap();
    assert_eq!(hits, vec![1]);
    assert_eq!(
        scan_index_entries(&single, single_index).await,
        scan_index_entries(&staged, staged_index).await
    );

    single.close().await.unwrap();
    staged.close().await.unwrap();
}

/// A staged create carries per-column orders exactly as the single-commit
/// ordered create does: the definition records them, the steps encode with
/// them, and the finished range is byte-identical to a single-commit
/// ordered build over the same rows.
#[tokio::test]
async fn staged_ordered_create_records_orders_and_matches_single_commit() {
    use crate::{
        catalog::{ColumnId, ColumnOrder, IndexDef},
        store::index_encoding::{Direction, NullOrder},
    };
    let def = || IndexDef {
        name: "by_a_desc".into(),
        columns: vec![ColumnId::new(1)],
        unique: true,
    };
    let orders = || {
        vec![ColumnOrder {
            direction: Direction::Descending,
            nulls: NullOrder::First,
        }]
    };

    let (single, table_single) = catalog_with_two_column_table().await;
    register_three_row_file(&single, table_single).await;
    let single_index = std::cell::Cell::new(None);
    single
        .commit(|tx| {
            let id = tx.create_index_ordered(
                table_single,
                &def(),
                &orders(),
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            single_index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();

    let (staged, table_staged) = catalog_with_two_column_table().await;
    register_three_row_file(&staged, table_staged).await;
    let staged_index = std::cell::Cell::new(None);
    staged
        .commit(|tx| {
            let id = tx.create_index_staged_ordered(table_staged, &def(), &orders())?;
            staged_index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let staged_index = staged_index.get().unwrap();

    let info = staged
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table_staged)
        .remove(0);
    assert_eq!(info.directions, vec![Direction::Descending]);
    assert_eq!(info.nulls, vec![NullOrder::First]);

    staged
        .commit(|tx| {
            tx.build_index_step(staged_index, &[entry(0, 10), entry(1, 20)], false)
                .map(|_| ())
        })
        .await
        .unwrap();
    staged
        .commit(|tx| {
            tx.build_index_step(staged_index, &[entry(2, 30)], true)
                .map(|_| ())
        })
        .await
        .unwrap();

    assert_eq!(
        scan_index_entries(&single, single_index.get().unwrap()).await,
        scan_index_entries(&staged, staged_index).await
    );
    single.close().await.unwrap();
    staged.close().await.unwrap();
}

/// A writer inserting a value a live row already holds, while a unique
/// index is still building, lands its own rows and poisons the build — the
/// duplicate fails the *index*, never the write. Enforcement during a build
/// is partial by construction, so failing the writer would make the outcome
/// depend on how far the backfill happened to have run.
#[tokio::test]
async fn a_writer_duplicating_a_value_mid_build_poisons_the_index() {
    use crate::catalog::{ColumnId, DataFile, FileIndexEntry, IndexDef, IndexState};
    let (catalog, table) = catalog_with_two_column_table().await;
    // Rows 0..2 exist before the index does, so the writer below lands on a
    // fresh row id rather than re-deriving the entry the build covered.
    register_three_row_file(&catalog, table).await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index_staged(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // The build covers row 0, which holds value 10.
    catalog
        .commit(|tx| {
            tx.build_index_step(index, &[entry(0, 10)], false)
                .map(|_| ())
        })
        .await
        .unwrap();

    // A writer now registers a file whose row claims that same value.
    catalog
        .commit(|tx| {
            tx.register_data_file(
                table,
                DataFile {
                    path: "w.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 1,
                    file_size_bytes: 10,
                    footer_size: 4,
                    encryption_key: None,
                    partition_values: vec![],
                    column_stats: vec![],
                },
                &[FileIndexEntry {
                    index,
                    ordinal: 0,
                    values: vec![Some(int_value(10))],
                }],
            )
            .map(|_| ())
        })
        .await
        .expect("the writer's commit lands rather than failing on the building index");

    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(
        snapshot.indexes_of(table).remove(0).state,
        IndexState::Poisoned,
        "the duplicate poisoned the build"
    );
    // The writer's file is really there — its rows were not rolled back.
    assert_eq!(snapshot.data_files_of(table).len(), 2);
    catalog.close().await.unwrap();
}

/// The same collision against a **ready** index still fails the writer:
/// enforcement is total once the build has flipped, so the duplicate is a
/// genuine constraint violation.
#[tokio::test]
async fn a_writer_duplicating_a_value_on_a_ready_index_still_fails() {
    use crate::catalog::FileIndexEntry;
    let (catalog, table, index, _) = catalog_with_indexed_data_file().await;
    let refused = catalog
        .commit(|tx| {
            tx.register_data_file(
                table,
                crate::catalog::DataFile {
                    path: "w.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 1,
                    file_size_bytes: 10,
                    footer_size: 4,
                    encryption_key: None,
                    partition_values: vec![],
                    column_stats: vec![],
                },
                &[FileIndexEntry {
                    index,
                    ordinal: 0,
                    values: vec![Some(int_value(20))],
                }],
            )
            .map(|_| ())
        })
        .await;
    assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn staged_build_step_rejects_a_duplicate_and_a_ready_index() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index_staged(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // A duplicate value within a batch poisons the build rather than
    // failing the step — one rule for every collision found while an index
    // is building. The build's driver is what turns the poison into the
    // caller's `Constraint`.
    catalog
        .commit(|tx| {
            tx.build_index_step(index, &[entry(0, 10), entry(1, 10)], false)
                .map(|_| ())
        })
        .await
        .expect("the step lands; the duplicate poisons the definition");
    assert_eq!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .indexes_of(table)
            .remove(0)
            .state,
        crate::catalog::IndexState::Poisoned
    );

    // Complete the build, then a further step on the ready index is
    // refused.
    catalog
        .commit(|tx| {
            tx.build_index_step(index, &[entry(0, 10)], true)
                .map(|_| ())
        })
        .await
        .unwrap();
    let after_ready = catalog
        .commit(|tx| {
            tx.build_index_step(index, &[entry(1, 20)], false)
                .map(|_| ())
        })
        .await;
    assert!(
        matches!(after_ready, Err(Error::Constraint(_))),
        "{after_ready:?}"
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn reclaiming_a_dropped_index_deletes_its_orphaned_entries() {
    use crate::{
        catalog::{ColumnId, IndexDef},
        store::key::{IndexKind, index_index_prefix},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // Reclaiming a live index is refused.
    assert!(matches!(
        catalog.reclaim_index_entries(index, 100).await,
        Err(Error::Constraint(_))
    ));

    catalog.commit(|tx| tx.drop_index(index)).await.unwrap();

    // A bounded sweep deletes the three orphaned entries, then reports
    // nothing left.
    let first = catalog.reclaim_index_entries(index, 2).await.unwrap();
    assert_eq!(first, 2);
    let second = catalog.reclaim_index_entries(index, 100).await.unwrap();
    assert_eq!(second, 1);
    assert_eq!(catalog.reclaim_index_entries(index, 100).await.unwrap(), 0);

    // The index range is empty afterward.
    let tx = catalog.begin_write_tx().await.unwrap();
    let mut iter = ReadHandle::Tx(&tx)
        .scan_prefix(
            index_index_prefix(IndexKind::Unique, index.get()),
            ..,
            crate::store::handle::ScanShape::Bulk,
        )
        .await
        .unwrap();
    assert!(iter.next().await.unwrap().is_none());
    tx.rollback();
    catalog.close().await.unwrap();
}

/// Creates an index over column `a` carrying `count` entries.
async fn indexed(
    catalog: &crate::catalog::Catalog,
    table: crate::catalog::TableId,
    name: &str,
    count: u64,
) -> crate::catalog::IndexId {
    use crate::catalog::{ColumnId, IndexDef};
    let entries: Vec<_> = (0..count).map(|i| entry(i, i128::from(i) * 10)).collect();
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: name.into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &entries,
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    index.get().unwrap()
}

/// Counts every entry left in the `index` subspace, across both kinds.
async fn index_entry_count(catalog: &crate::catalog::Catalog) -> usize {
    use crate::store::key::{IndexKind, index_kind_prefix};
    let tx = catalog.begin_write_tx().await.unwrap();
    let mut total = 0;
    for kind in [IndexKind::Unique, IndexKind::Multi] {
        let mut iter = ReadHandle::Tx(&tx)
            .scan_prefix(
                index_kind_prefix(kind),
                ..,
                crate::store::handle::ScanShape::Bulk,
            )
            .await
            .unwrap();
        while iter.next().await.unwrap().is_some() {
            total += 1;
        }
    }
    tx.rollback();
    total
}

/// A sweep reclaims a dropped index's whole range, reports what it did,
/// and finds nothing left on a second pass.
#[tokio::test]
async fn maintain_sweeps_a_dropped_index_and_is_idempotent() {
    use crate::catalog::MaintenanceRequest;
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;
    let index = indexed(&catalog, table, "by_a", 3).await;

    // A live index is untouched.
    let untouched = catalog
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(untouched.indexes_swept, 0);
    assert_eq!(untouched.index_entries_reclaimed, 0);
    assert_eq!(index_entry_count(&catalog).await, 3);

    catalog.commit(|tx| tx.drop_index(index)).await.unwrap();

    let swept = catalog
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(swept.indexes_swept, 1);
    assert_eq!(swept.index_entries_reclaimed, 3);
    assert_eq!(index_entry_count(&catalog).await, 0);

    let again = catalog
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(again.indexes_swept, 0);
    assert_eq!(again.index_entries_reclaimed, 0);

    catalog.close().await.unwrap();
}

/// With live and dead indexes interleaved by id, the sweep reclaims
/// exactly the dead ones and every live index still answers lookups.
#[tokio::test]
async fn maintain_spares_live_indexes_interleaved_by_id() {
    use crate::{
        catalog::MaintenanceRequest,
        store::index_encoding::{IndexKeyValue, IntWidth},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let first = indexed(&catalog, table, "one", 3).await;
    let live = indexed(&catalog, table, "two", 3).await;
    let last = indexed(&catalog, table, "three", 3).await;
    assert!(first.get() < live.get() && live.get() < last.get());

    // Drop the lowest and highest ids, leaving a live index between them.
    catalog.commit(|tx| tx.drop_index(first)).await.unwrap();
    catalog.commit(|tx| tx.drop_index(last)).await.unwrap();

    let report = catalog
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(report.indexes_swept, 2);
    assert_eq!(report.index_entries_reclaimed, 6);

    // The survivor kept every entry and still resolves them.
    assert_eq!(index_entry_count(&catalog).await, 3);
    for row_id in 0..3u64 {
        let found = catalog
            .index_lookup(
                table,
                live,
                &[IndexKeyValue::Int {
                    value: i128::from(row_id) * 10,
                    width: IntWidth::I64,
                }],
            )
            .await
            .unwrap();
        assert_eq!(found, vec![row_id]);
    }

    catalog.close().await.unwrap();
}

/// Dropping a table ends its indexes with it, and the sweep reclaims both
/// ranges in one pass.
#[tokio::test]
async fn maintain_reclaims_both_indexes_of_a_dropped_table() {
    use crate::catalog::MaintenanceRequest;
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;
    indexed(&catalog, table, "by_a", 3).await;
    indexed(&catalog, table, "also_a", 2).await;
    assert_eq!(index_entry_count(&catalog).await, 5);

    catalog.commit(|tx| tx.drop_table(table)).await.unwrap();

    let report = catalog
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(report.indexes_swept, 2);
    assert_eq!(report.index_entries_reclaimed, 5);
    assert_eq!(index_entry_count(&catalog).await, 0);

    catalog.close().await.unwrap();
}

/// A range larger than the batch size is reclaimed across several
/// commits, and `sys/head` is unchanged across the whole sweep.
#[tokio::test]
async fn maintain_batches_without_advancing_head() {
    use crate::catalog::MaintenanceRequest;
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;
    let index = indexed(&catalog, table, "by_a", 10).await;
    catalog.commit(|tx| tx.drop_index(index)).await.unwrap();

    let before = catalog.snapshot().await.unwrap().current_snapshot().id;

    let report = catalog
        .maintain(MaintenanceRequest {
            batch_size: 3,
            ..MaintenanceRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(report.indexes_swept, 1);
    assert_eq!(report.index_entries_reclaimed, 10);
    assert_eq!(index_entry_count(&catalog).await, 0);

    let after = catalog.snapshot().await.unwrap().current_snapshot().id;
    assert_eq!(before, after, "a maintenance pass must not advance head");

    catalog.close().await.unwrap();
}

/// Batches resume where the previous one stopped rather than restarting
/// at the range's beginning, so a range reclaims correctly however small
/// the batch. With `batch_size` 1 every entry is its own commit, which is
/// the shape that would expose a cursor that failed to advance — it would
/// re-scan the same tombstones and stall, or skip live entries.
#[tokio::test]
async fn maintain_resumes_each_batch_where_the_last_one_stopped() {
    use crate::catalog::MaintenanceRequest;
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;
    let index = indexed(&catalog, table, "by_a", 25).await;
    catalog.commit(|tx| tx.drop_index(index)).await.unwrap();

    let report = catalog
        .maintain(MaintenanceRequest {
            batch_size: 1,
            ..MaintenanceRequest::default()
        })
        .await
        .unwrap();

    assert_eq!(report.indexes_swept, 1);
    assert_eq!(
        report.index_entries_reclaimed, 25,
        "every entry must be reclaimed exactly once across 25 batches"
    );
    assert_eq!(index_entry_count(&catalog).await, 0);

    catalog.close().await.unwrap();
}

/// A zero batch size would loop forever rather than reclaim nothing, so
/// it is refused.
#[tokio::test]
async fn maintain_refuses_a_zero_batch_size() {
    use crate::catalog::MaintenanceRequest;
    let (catalog, _) = catalog_with_two_column_table().await;
    assert!(matches!(
        catalog
            .maintain(MaintenanceRequest {
                batch_size: 0,
                ..MaintenanceRequest::default()
            })
            .await,
        Err(Error::Configuration(_))
    ));
    catalog.close().await.unwrap();
}

/// Maintenance mutates, so a read-only catalog refuses it rather than
/// reporting a no-op pass.
#[tokio::test]
async fn maintain_refuses_a_read_only_catalog() {
    use crate::catalog::{Catalog, CatalogOptions};
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());

    // Bootstrap and release the writer so the reader has a store to open.
    let writer = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    writer.close().await.unwrap();

    let reader = Catalog::open_read_only(object_store, CatalogOptions::default())
        .await
        .unwrap();
    // `maintain` is a mutator and lives on the read-write handle alone, so
    // there is no runtime refusal to assert — `reader.maintain(..)` does not
    // compile. What a reader keeps is the read surface.
    assert_eq!(
        reader.snapshot().await.unwrap().current_snapshot().id.get(),
        0
    );
    reader.close().await.unwrap();
}

/// Discovery seeks by index id: from any starting id it returns the
/// lowest index at or after it, and `None` past the last — the property
/// that lets the sweep skip a live index in one seek instead of walking
/// its entries.
#[tokio::test]
async fn discovery_seeks_to_the_next_index_holding_entries() {
    use crate::store::key::IndexKind;
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let first = indexed(&catalog, table, "one", 3).await;
    let second = indexed(&catalog, table, "two", 3).await;
    let (low, high) = (first.get(), second.get());
    assert!(low < high);

    // From zero, from the id itself, and from just past the lower one.
    assert_eq!(
        catalog
            .first_index_id_from(IndexKind::Unique, 0)
            .await
            .unwrap(),
        Some(low)
    );
    assert_eq!(
        catalog
            .first_index_id_from(IndexKind::Unique, low)
            .await
            .unwrap(),
        Some(low)
    );
    assert_eq!(
        catalog
            .first_index_id_from(IndexKind::Unique, low + 1)
            .await
            .unwrap(),
        Some(high)
    );
    assert_eq!(
        catalog
            .first_index_id_from(IndexKind::Unique, high + 1)
            .await
            .unwrap(),
        None
    );

    // These are unique indexes, so the multi range is empty throughout.
    assert_eq!(
        catalog
            .first_index_id_from(IndexKind::Multi, 0)
            .await
            .unwrap(),
        None
    );

    catalog.close().await.unwrap();
}

/// A sweep interleaved with commits that insert into a *live* index
/// completes without conflict, and the live index keeps every entry: the
/// only keys the sweep writes are deletes under dead index ids, which no
/// live commit touches.
#[tokio::test]
async fn maintain_does_not_conflict_with_a_live_writer() {
    use crate::{
        catalog::{DataFile, FileIndexEntry, MaintenanceRequest},
        store::index_encoding::{IndexKeyValue, IntWidth},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let dead = indexed(&catalog, table, "dead", 6).await;
    let live = indexed(&catalog, table, "live", 3).await;
    catalog.commit(|tx| tx.drop_index(dead)).await.unwrap();

    // Interleave: sweep a batch, then land a file carrying a new entry
    // for the live index, and repeat.
    let mut reclaimed = 0;
    for round in 0..3u64 {
        let report = catalog
            .maintain(MaintenanceRequest {
                batch_size: 2,
                ..MaintenanceRequest::default()
            })
            .await
            .unwrap();
        reclaimed += report.index_entries_reclaimed;

        let path = format!("live-{round}.parquet");
        catalog
            .commit(move |tx| {
                tx.register_data_file(
                    table,
                    DataFile {
                        path: path.clone(),
                        path_is_relative: true,
                        file_format: "parquet".into(),
                        record_count: 1,
                        file_size_bytes: 10,
                        footer_size: 4,
                        encryption_key: None,
                        partition_values: vec![],
                        column_stats: vec![],
                    },
                    &[FileIndexEntry {
                        index: live,
                        ordinal: 0,
                        values: vec![Some(IndexKeyValue::Int {
                            value: i128::from(1_000 + round),
                            width: IntWidth::I64,
                        })],
                    }],
                )?;
                Ok(())
            })
            .await
            .expect("a commit into a live index must not conflict with the sweep");
    }

    assert_eq!(reclaimed, 6, "the dead range is fully reclaimed");
    // Three original entries plus the three landed mid-sweep.
    assert_eq!(index_entry_count(&catalog).await, 6);

    catalog.close().await.unwrap();
}

/// Sweeping is opt-out: a request that disables it reclaims nothing.
#[tokio::test]
async fn maintain_skips_the_sweep_when_disabled() {
    use crate::catalog::MaintenanceRequest;
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;
    let index = indexed(&catalog, table, "by_a", 3).await;
    catalog.commit(|tx| tx.drop_index(index)).await.unwrap();

    let report = catalog
        .maintain(MaintenanceRequest {
            sweep_orphaned_index_entries: false,
            ..MaintenanceRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(report.index_entries_reclaimed, 0);
    assert_eq!(index_entry_count(&catalog).await, 3);

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn drop_index_ends_definition_and_keeps_format() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();

    catalog
        .commit(|tx| tx.drop_index(index.get().unwrap()))
        .await
        .unwrap();
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .indexes_of(table)
            .is_empty()
    );
    // Dropping the last index does not downgrade the stamp.
    assert_eq!(read_format_version(&catalog).await, FORMAT_WITH_INDEX);
    catalog.close().await.unwrap();
}

/// The head view the writer folds forward on every commit stays identical
/// to a fresh scan of the store, across creates, a rename, an option, a
/// column drop, and a full table drop — whose child deletes the fold must
/// apply without over-cascading.
#[tokio::test]
async fn folded_head_view_matches_a_fresh_scan() {
    use crate::catalog::{Catalog, CatalogOptions, ColumnDef, DataFile, OptionScope};

    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let column = |name: &str| ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
        children: Vec::new(),
    };

    let keep = std::cell::Cell::new(None);
    let doomed = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            let k = tx.create_table(schema, "keep", &[column("a"), column("b")])?;
            let d = tx.create_table(schema, "doomed", &[column("x")])?;
            tx.create_view(schema, "v", "duckdb", "SELECT 1")?;
            keep.set(Some(k));
            doomed.set(Some(d));
            Ok(())
        })
        .await
        .unwrap();
    let keep = keep.get().unwrap();
    let doomed = doomed.get().unwrap();

    catalog
        .commit(|tx| {
            tx.register_data_file(
                keep,
                DataFile {
                    path: "f.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 3,
                    file_size_bytes: 100,
                    footer_size: 8,
                    encryption_key: None,
                    partition_values: vec![],
                    column_stats: vec![],
                },
                &[],
            )?;
            tx.set_option(OptionScope::Global, "answer", "42")?;
            Ok(())
        })
        .await
        .unwrap();

    catalog
        .commit(|tx| {
            tx.rename_table(keep, "keep2")?;
            let second = tx.columns_of(keep)[1].id;
            tx.drop_column(keep, second)
        })
        .await
        .unwrap();

    catalog.commit(|tx| tx.drop_table(doomed)).await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let stamp = read::read_head(ReadHandle::Tx(&tx)).await.unwrap().unwrap();
    let head = stamp.snapshot_id;
    let fresh = materialize(ReadHandle::Tx(&tx), None).await.unwrap();
    tx.rollback();

    let cached = catalog
        .projections()
        .read()
        .unwrap()
        .head_view(&stamp)
        .expect("the writer caches its head view");

    assert_eq!(cached.snapshot.snapshot_id, fresh.snapshot.snapshot_id);
    // An empty diff both directions means identical current state.
    assert!(
        diff_writes(&fresh, &cached, head + 1).is_empty(),
        "folded head view diverged from a fresh scan"
    );
    assert!(
        diff_writes(&cached, &fresh, head + 1).is_empty(),
        "folded head view diverged from a fresh scan"
    );
    catalog.close().await.unwrap();
}

/// Seeds a catalog with `tables` tables of two columns each, so a base view
/// holds enough live records that a small gap stays under the refresh's
/// size backstop.
#[allow(clippy::unwrap_used)]
async fn seeded_catalog(tables: usize) -> (crate::catalog::Catalog, Vec<crate::catalog::TableId>) {
    use crate::catalog::{Catalog, CatalogOptions, ColumnDef};

    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let column = |name: &str| ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
        children: Vec::new(),
    };

    let ids = std::cell::RefCell::new(Vec::new());
    catalog
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            for index in 0..tables {
                let id =
                    tx.create_table(schema, &format!("t{index}"), &[column("a"), column("b")])?;
                ids.borrow_mut().push(id);
            }
            Ok(())
        })
        .await
        .unwrap();

    let ids = ids.into_inner();
    (catalog, ids)
}

/// A migration may be moving keys mid-scan, so a read refuses rather than
/// return a view that is silently missing records — and a warm cache is no
/// exception. The handle below materializes first; the marker must still
/// win over that view, or a cached reader would sail through a migration.
///
/// Read-only, because that is the handle a migration can start under. A
/// migrator takes the writer epoch, which fences a read-write handle but
/// leaves a reader following the store — so the reader is the one that
/// meets a marker mid-flight, and its refusal is per read for that reason.
#[tokio::test]
async fn the_migration_marker_refuses_reads_even_with_a_warm_cache() {
    use crate::catalog::{Catalog, CatalogOptions};

    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let writer = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    writer
        .commit(|tx| tx.create_schema("before").map(|_| ()))
        .await
        .unwrap();

    // A reader never fences the writer, so the two run side by side. Its
    // poll interval is the wait below, so it is set short rather than left
    // at the ten-second default.
    let options = CatalogOptions {
        reader_poll_interval: std::time::Duration::from_millis(20),
        ..CatalogOptions::default()
    };
    let reader = Catalog::open_read_only(store.clone(), options)
        .await
        .unwrap();
    reader.snapshot().await.unwrap();

    let tx = writer.begin_write_tx().await.unwrap();
    tx.put(
        Key::Sys(SysKey::Migration).encode(),
        value::encode_value(&proto::MigrationValue {
            from_format: 1,
            to_format: 2,
            cursor: Vec::new(),
        }),
    )
    .unwrap();
    tx.commit_with_options(&durable()).await.unwrap();

    // A reader's state advances when its manifest poller runs, not on the
    // write, so the refusal arrives within a bounded wait rather than at
    // the next call.
    let mut refused = None;
    for _ in 0..100 {
        if let Err(err) = reader.snapshot().await {
            refused = Some(err);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let refused = refused.expect("a warm reader served every read through a planted marker");
    assert!(matches!(refused, Error::Migration(_)), "{refused:?}");
    writer.close().await.unwrap();
}

/// A commit resolves its base view from the same warm cache a read serves
/// from, so the gate belongs at that read too. Without it the one path
/// that writes would plan against a view of a keyspace being rewritten.
#[tokio::test]
async fn the_migration_marker_refuses_a_commit_with_a_warm_cache() {
    let (catalog, _) = seeded_catalog(3).await;
    catalog.snapshot().await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    tx.put(
        Key::Sys(SysKey::Migration).encode(),
        value::encode_value(&proto::MigrationValue {
            from_format: 1,
            to_format: 2,
            cursor: Vec::new(),
        }),
    )
    .unwrap();
    tx.commit().await.unwrap();

    let err = catalog
        .commit(|tx| {
            tx.create_schema("late")?;
            Ok(())
        })
        .await
        .err()
        .unwrap();

    assert!(matches!(err, Error::Migration(_)), "{err:?}");
    catalog.close().await.unwrap();
}

/// The first attempt never waits, later ones grow to the cap, and every
/// wait carries jitter — two writers that collided must not re-collide in
/// lockstep.
#[test]
fn retry_backoff_starts_at_zero_grows_and_caps() {
    use super::{RETRY_BACKOFF_BASE_MICROS, RETRY_BACKOFF_MAX_MICROS, retry_backoff};

    assert_eq!(retry_backoff(0), std::time::Duration::ZERO);

    let ceiling = u128::from(RETRY_BACKOFF_MAX_MICROS + RETRY_BACKOFF_BASE_MICROS);
    let mut previous = 0_u128;
    for attempt in 1..MAX_COMMIT_ATTEMPTS {
        let waited = retry_backoff(attempt).as_micros();
        assert!(
            waited >= u128::from(RETRY_BACKOFF_BASE_MICROS),
            "attempt {attempt} waited {waited}µs, below the base delay"
        );
        assert!(
            waited <= ceiling,
            "attempt {attempt} waited {waited}µs, above the cap plus jitter"
        );
        // Growth holds until the cap absorbs it; jitter never reverses it.
        if previous > 0 && previous < u128::from(RETRY_BACKOFF_MAX_MICROS) {
            assert!(
                waited > previous,
                "attempt {attempt} waited {waited}µs, not more than {previous}µs"
            );
        }
        previous = waited;
    }
    // The last attempts sit at the cap rather than growing without bound.
    assert!(
        retry_backoff(MAX_COMMIT_ATTEMPTS - 1).as_micros() >= u128::from(RETRY_BACKOFF_MAX_MICROS)
    );
}

/// The whole budget must span enough time for a competing commit to land,
/// without leaving a caller waiting on a conflict for seconds.
#[test]
fn retry_backoff_budget_stays_in_a_sane_band() {
    use super::retry_backoff;

    let total: std::time::Duration = (0..MAX_COMMIT_ATTEMPTS).map(retry_backoff).sum();
    assert!(
        total >= std::time::Duration::from_millis(100),
        "whole retry budget waits only {total:?}"
    );
    assert!(
        total <= std::time::Duration::from_millis(600),
        "whole retry budget waits {total:?}"
    );
}

/// Every batch stamps the head record and moves its batch count — including
/// a maintenance batch, which reuses the standing snapshot id. Without that
/// a reader could not tell the state a maintenance batch left behind from
/// the one it reclaimed, since the ids are the same.
#[tokio::test]
async fn every_batch_moves_the_head_stamp() {
    use crate::catalog::OptionScope;

    let (catalog, _) = seeded_catalog(2).await;

    let stamp = |catalog: &crate::catalog::Catalog| {
        let catalog = catalog.clone();
        async move {
            let tx = catalog.begin_write_tx().await.unwrap();
            let head = read_head_value(ReadHandle::Tx(&tx)).await.unwrap();
            tx.rollback();
            head
        }
    };

    let seeded = stamp(&catalog).await;

    // A snapshot-minting batch moves both halves.
    catalog
        .commit(|tx| tx.create_schema("more").map(|_| ()))
        .await
        .unwrap();
    let minted = stamp(&catalog).await;
    assert_eq!(minted.snapshot_id, seeded.snapshot_id + 1);
    assert_eq!(minted.batch_seq, seeded.batch_seq + 1);

    // An option write mints no snapshot, so only the count moves.
    catalog
        .commit(|tx| tx.set_option(OptionScope::Global, "answer", "42"))
        .await
        .unwrap();
    let maintained = stamp(&catalog).await;
    assert_eq!(maintained.snapshot_id, minted.snapshot_id);
    assert_eq!(maintained.batch_seq, minted.batch_seq + 1);

    catalog.close().await.unwrap();
}

/// Replaying a gap's changelog must land on exactly the view a full
/// rematerialization at head builds — the whole point of the cheap path is
/// that it is not a different answer.
#[tokio::test]
async fn an_incremental_refresh_matches_a_full_rematerialization() {
    let (catalog, tables) = seeded_catalog(12).await;

    let tx = catalog.begin_write_tx().await.unwrap();
    let base = materialize(ReadHandle::Tx(&tx), None).await.unwrap();
    tx.rollback();

    // A gap that exercises every shape the fold has to get right: a new
    // record, a changed one, an ended child, and a whole ended subtree.
    catalog
        .commit(|tx| {
            tx.register_data_file(tables[0], bulk_file("f0.parquet", 3), &[])
                .map(|_| ())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.rename_table(tables[1], "renamed"))
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("s").unwrap().id;
            tx.create_table(
                schema,
                "late",
                &[crate::catalog::ColumnDef {
                    name: "x".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                    children: Vec::new(),
                }],
            )
            .map(|_| ())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let second = tx.columns_of(tables[3])[1].id;
            tx.drop_column(tables[3], second)
        })
        .await
        .unwrap();
    catalog.commit(|tx| tx.drop_table(tables[2])).await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let handle = ReadHandle::Tx(&tx);
    let head = read_head_value(handle).await.unwrap();
    let refreshed = refresh(handle, &base)
        .await
        .unwrap()
        .expect("a five-commit gap over a twelve-table catalog must replay");
    let full = materialize(handle, None).await.unwrap();
    tx.rollback();

    assert_eq!(refreshed.snapshot, full.snapshot);
    assert_eq!(refreshed.batch_seq, full.batch_seq);
    let next = head.snapshot_id + 1;
    assert!(
        diff_writes(&full, &refreshed, next).is_empty(),
        "a replayed view diverged from a fresh scan"
    );
    assert!(
        diff_writes(&refreshed, &full, next).is_empty(),
        "a replayed view diverged from a fresh scan"
    );
    // The entity maps agreeing is not the whole view: a rename that left a
    // stale name entry behind would still diff clean.
    let schema = refreshed.schema_by_name("s").unwrap().id;
    assert!(refreshed.table_by_name(schema, "renamed").is_some());
    assert!(refreshed.table_by_name(schema, "t1").is_none());
    assert!(refreshed.table_by_name(schema, "late").is_some());
    assert!(refreshed.table_by_name(schema, "t2").is_none());

    catalog.close().await.unwrap();
}

/// A maintenance batch mints no snapshot, so it leaves no changelog behind.
/// The head stamp still records that it landed, and a refresh that cannot
/// see what it did must decline the whole gap rather than replay around it.
#[tokio::test]
async fn a_refresh_declines_a_gap_a_maintenance_batch_landed_in() {
    use crate::catalog::OptionScope;

    let (catalog, tables) = seeded_catalog(8).await;

    let tx = catalog.begin_write_tx().await.unwrap();
    let base = materialize(ReadHandle::Tx(&tx), None).await.unwrap();
    tx.rollback();

    catalog
        .commit(|tx| tx.set_option(OptionScope::Global, "answer", "42"))
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            tx.register_data_file(tables[0], bulk_file("f0.parquet", 3), &[])
                .map(|_| ())
        })
        .await
        .unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let handle = ReadHandle::Tx(&tx);
    let refreshed = refresh(handle, &base).await.unwrap();
    tx.rollback();

    assert!(
        refreshed.is_none(),
        "a gap holding a batch that recorded no changelog must not be replayed"
    );
    catalog.close().await.unwrap();
}

/// A base further behind than the retained changelog window has nothing
/// left to walk: later commits swept the records the gap is written in.
/// This subsumes the retention horizon — expiry reclaims snapshots far
/// further back than the window keeps changelogs — so a base below the
/// horizon reaches the same fallback by the same route.
#[tokio::test]
async fn a_refresh_declines_a_gap_swept_out_of_the_changelog_window() {
    let (catalog, tables) = seeded_wide_catalog(4, 2).await;

    let tx = catalog.begin_write_tx().await.unwrap();
    let base = materialize(ReadHandle::Tx(&tx), None).await.unwrap();
    tx.rollback();

    let commits = usize::try_from(CHANGELOG_WINDOW).unwrap() + 2;
    for round in 0..commits {
        catalog
            .commit(|tx| {
                tx.register_data_file(tables[0], bulk_file(&format!("f{round}.parquet"), 3), &[])
                    .map(|_| ())
            })
            .await
            .unwrap();
    }

    let tx = catalog.begin_write_tx().await.unwrap();
    let handle = ReadHandle::Tx(&tx);
    let head = read_head_value(handle).await.unwrap();
    // The backstop is lifted so this proves the *window* declines the gap,
    // not the churn share.
    let refreshed = replay(handle, &base, &head, usize::MAX).await.unwrap();

    // The window is a sliding one: it bounds the subspace whether or not
    // anything else ever reclaims it.
    let mut retained = 0u64;
    for snapshot_id in 0..=head.snapshot_id {
        if read::read_changelog(handle, snapshot_id)
            .await
            .unwrap()
            .is_some()
        {
            retained += 1;
        }
    }
    tx.rollback();

    assert!(
        refreshed.is_none(),
        "a gap whose changelogs were swept must not be replayed"
    );
    assert!(
        retained <= CHANGELOG_WINDOW,
        "{retained} changelogs retained, above the window of {CHANGELOG_WINDOW}"
    );
    catalog.close().await.unwrap();
}

/// Past a share of the live catalog, replaying the changelog costs more
/// than the scan it replaces, so the refresh declines on size alone. Both
/// paths build the same view; which one runs is purely a cost choice.
#[tokio::test]
async fn a_refresh_declines_churn_that_outruns_the_catalog() {
    let (catalog, tables) = seeded_catalog(1).await;

    let tx = catalog.begin_write_tx().await.unwrap();
    let base = materialize(ReadHandle::Tx(&tx), None).await.unwrap();
    tx.rollback();

    for index in 0..6u64 {
        catalog
            .commit(|tx| {
                tx.register_data_file(tables[0], bulk_file(&format!("f{index}.parquet"), 3), &[])
                    .map(|_| ())
            })
            .await
            .unwrap();
    }

    let tx = catalog.begin_write_tx().await.unwrap();
    let handle = ReadHandle::Tx(&tx);
    let refreshed = refresh(handle, &base).await.unwrap();
    tx.rollback();

    assert!(
        base.live_entity_count() / REFRESH_CHURN_SHARE < 6,
        "the fixture must churn past the backstop for this to test it"
    );
    assert!(
        refreshed.is_none(),
        "churn past the backstop must fall back to a full rescan"
    );
    catalog.close().await.unwrap();
}

/// 0009 — replaying a gap's changelog versus rematerializing at head.
///
/// The refresh declines a gap whose churn passes a share of the live
/// catalog, and the share is a cost choice: both paths build the same view.
/// This puts a curve under it by timing the two against each other at a
/// range of churn levels, with the backstop lifted so the expensive side is
/// visible. Run with:
///
/// ```text
/// cargo test -p moraine --lib --release -- --ignored --nocapture measure_refresh
/// ```
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::cast_precision_loss)]
async fn measure_refresh_versus_rematerialization() {
    const COLUMNS: usize = 8;
    const FILES_PER_COMMIT: usize = 4;
    const REPEATS: usize = 9;

    println!("\n# 0009 changelog replay vs. rematerialization (in-memory object_store)");
    println!(
        "# {COLUMNS} columns per table, {FILES_PER_COMMIT} files per gap commit, \
         median of {REPEATS}"
    );
    println!("# gaps stop at the {CHANGELOG_WINDOW}-commit changelog window\n");
    println!(
        "{:>7}  {:>8}  {:>7}  {:>7}  {:>7}  {:>10}  {:>10}  {:>7}",
        "tables", "entities", "commits", "churn", "share", "full_ms", "replay_ms", "ratio"
    );

    for tables in [50usize, 200, 800] {
        for commits in [1usize, 4, 16, 32, 64] {
            let (catalog, table_ids) = seeded_wide_catalog(tables, COLUMNS).await;

            let tx = catalog.begin_write_tx().await.unwrap();
            let base = materialize(ReadHandle::Tx(&tx), None).await.unwrap();
            tx.rollback();
            let entities = base.live_entity_count();

            for round in 0..commits {
                let target = table_ids[round % table_ids.len()];
                catalog
                    .commit(|tx| {
                        for file in 0..FILES_PER_COMMIT {
                            tx.register_data_file(
                                target,
                                bulk_file(&format!("g{round}-{file}.parquet"), 10),
                                &[],
                            )?;
                        }
                        Ok(())
                    })
                    .await
                    .unwrap();
            }

            let tx = catalog.begin_write_tx().await.unwrap();
            let handle = ReadHandle::Tx(&tx);
            let head = read_head_value(handle).await.unwrap();

            let mut full = Vec::with_capacity(REPEATS);
            let mut replayed = Vec::with_capacity(REPEATS);
            for _ in 0..REPEATS {
                let started = std::time::Instant::now();
                let view = materialize(handle, None).await.unwrap();
                full.push(started.elapsed());
                std::hint::black_box(&view);

                let started = std::time::Instant::now();
                // The backstop is lifted so the replay's own cost shows even
                // where the shipped threshold would have refused it.
                let view = replay(handle, &base, &head, usize::MAX)
                    .await
                    .unwrap()
                    .expect("an unbounded replay must reach head");
                replayed.push(started.elapsed());
                std::hint::black_box(&view);
            }
            tx.rollback();

            let churn = churn_above(&catalog, base.snapshot.snapshot_id, head.snapshot_id).await;
            let median = |mut samples: Vec<std::time::Duration>| {
                samples.sort_unstable();
                samples[samples.len() / 2].as_secs_f64() * 1_000.0
            };
            let (full_ms, replay_ms) = (median(full), median(replayed));
            println!(
                "{tables:>7}  {entities:>8}  {commits:>7}  {churn:>7}  {:>7.3}  {full_ms:>10.3}  \
                 {replay_ms:>10.3}  {:>7.2}",
                churn as f64 / entities as f64,
                full_ms / replay_ms
            );

            catalog.close().await.unwrap();
        }
    }
    println!();
}

/// 0009 — what the changelog costs the read path.
///
/// It used to ride in the snapshot record, and snapshot records are what
/// DuckLake re-reads every transaction and what moraine keeps a decoded
/// projection of; measured there it grew them ~6.8x and slowed their scan
/// ~1.45x. In its own subspace it costs that path nothing, and a sliding
/// window bounds what it costs storage. This prints both halves. Run with:
///
/// ```text
/// cargo test -p moraine --lib --release -- --ignored --nocapture measure_changelog
/// ```
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::cast_precision_loss)]
async fn measure_changelog_read_cost() {
    const COLUMNS: usize = 8;
    const REPEATS: usize = 9;

    println!("\n# 0009 changelog cost on the snapshot read path (in-memory object_store)");
    println!(
        "# one data file per commit over a {COLUMNS}-column table, stats for every \
         column, median of {REPEATS}\n"
    );
    println!(
        "{:>8}  {:>13}  {:>10}  {:>14}  {:>10}  {:>13}",
        "commits", "snapshot_bytes", "log_records", "log_bytes", "log_share", "snap_scan_ms"
    );

    for commits in [64usize, 256, 1024] {
        let (catalog, tables) = seeded_wide_catalog(1, COLUMNS).await;
        for round in 0..commits {
            catalog
                .commit(|tx| {
                    // Stats for every column, as DuckLake supplies them: a
                    // registration's changelog is mostly its stats keys.
                    let stats = tx
                        .columns_of(tables[0])
                        .iter()
                        .map(|column| crate::catalog::FileColumnStats {
                            column_id: column.id,
                            column_size_bytes: 128,
                            value_count: 10,
                            null_count: 0,
                            min_value: Some("0".into()),
                            max_value: Some("9".into()),
                            contains_nan: None,
                            extra_stats: None,
                        })
                        .collect();
                    let file = crate::catalog::DataFile {
                        column_stats: stats,
                        ..bulk_file(&format!("f{round}.parquet"), 10)
                    };
                    tx.register_data_file(tables[0], file, &[]).map(|_| ())
                })
                .await
                .unwrap();
        }

        let tx = catalog.begin_write_tx().await.unwrap();
        let handle = ReadHandle::Tx(&tx);
        let snapshots = read::scan_snapshots(handle).await.unwrap();
        let snapshot_bytes: usize = snapshots.iter().map(|r| value::encode_value(r).len()).sum();

        let mut log_records = 0usize;
        let mut log_bytes = 0usize;
        let head = read_head_value(handle).await.unwrap().snapshot_id;
        for snapshot_id in 0..=head {
            if let Some(log) = read::read_changelog(handle, snapshot_id).await.unwrap() {
                log_records += 1;
                log_bytes += value::encode_value(&log).len();
            }
        }

        let mut samples = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let started = std::time::Instant::now();
            let rows = read::scan_snapshots(handle).await.unwrap();
            samples.push(started.elapsed());
            std::hint::black_box(&rows);
        }
        tx.rollback();
        samples.sort_unstable();
        let snap_scan_ms = samples[samples.len() / 2].as_secs_f64() * 1_000.0;

        println!(
            "{commits:>8}  {snapshot_bytes:>13}  {log_records:>10}  {log_bytes:>14}  \
             {:>10.2}  {snap_scan_ms:>13.3}",
            log_bytes as f64 / snapshot_bytes as f64,
        );

        catalog.close().await.unwrap();
    }
    println!();
}

/// Seeds a catalog with `tables` tables of `columns` columns each.
#[allow(clippy::unwrap_used)]
async fn seeded_wide_catalog(
    tables: usize,
    columns: usize,
) -> (crate::catalog::Catalog, Vec<crate::catalog::TableId>) {
    use crate::catalog::{Catalog, CatalogOptions, ColumnDef};

    let options = CatalogOptions {
        // Seeding and gap commits are not what these harnesses time, and at
        // the default cadence each would wait a flush tick.
        flush_interval: std::time::Duration::from_millis(1),
        ..CatalogOptions::default()
    };
    let catalog = Catalog::open(Arc::new(InMemory::new()), options)
        .await
        .unwrap();
    let defs: Vec<ColumnDef> = (0..columns)
        .map(|index| ColumnDef {
            name: format!("c{index}"),
            column_type: "BIGINT".into(),
            nulls_allowed: true,
            default_value: None,
            children: Vec::new(),
        })
        .collect();

    let ids = std::cell::RefCell::new(Vec::new());
    catalog
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            for index in 0..tables {
                ids.borrow_mut()
                    .push(tx.create_table(schema, &format!("t{index}"), &defs)?);
            }
            Ok(())
        })
        .await
        .unwrap();

    let ids = ids.into_inner();
    (catalog, ids)
}

/// Distinct `current` keys the changelogs above `from` recorded — the
/// churn a replay across that gap would pay for.
#[allow(clippy::unwrap_used)]
async fn churn_above(catalog: &crate::catalog::Catalog, from: u64, head: u64) -> usize {
    let tx = catalog.begin_write_tx().await.unwrap();
    let handle = ReadHandle::Tx(&tx);
    let mut keys = BTreeSet::new();
    for snapshot_id in (from + 1)..=head {
        if let Some(log) = read::read_changelog(handle, snapshot_id).await.unwrap() {
            keys.extend(log.keys);
        }
    }
    tx.rollback();

    keys.len()
}

/// A commit attempt must never land on premises that omit a commit which
/// landed first. The attempt reads its base *through* the transaction it
/// will write, so the premise view and the conflict window share one start
/// sequence: either the attempt's transaction began after the other commit
/// and saw it, or it began before and loses the head race. There is no
/// interleaving in between.
///
/// This stages that interleaving exactly rather than racing for it: a
/// transaction opens and materializes, another commit lands through the
/// catalog, and only then does the first stage and write.
#[tokio::test]
async fn a_commit_landing_after_an_attempts_materialization_is_always_detected() {
    let (catalog, tables) = seeded_wide_catalog(3, 2).await;

    // The attempt opens and reads its premises.
    let attempt = catalog.begin_write_tx().await.unwrap();
    let base = head_view_for(&attempt, catalog.projections())
        .await
        .unwrap();
    let head_before = base.snapshot.snapshot_id;

    // A commit lands in the window between that read and the attempt's
    // write. It is invisible to the attempt: the transaction is already
    // open at an earlier sequence.
    catalog
        .commit(|tx| {
            tx.register_data_file(tables[0], bulk_file("winner.parquet", 3), &[])
                .map(|_| ())
        })
        .await
        .unwrap();

    let reread = head_view_for(&attempt, catalog.projections())
        .await
        .unwrap();
    assert_eq!(
        reread.snapshot.snapshot_id, head_before,
        "an open transaction must not observe a commit that landed after it began"
    );
    assert!(
        reread
            .data_files_of(crate::catalog::TableId::new(tables[0].get()))
            .is_empty(),
        "the attempt's premises must not include the winner's file"
    );

    // So the attempt's own write has to lose, or it would commit against
    // premises that omit a landed commit.
    let mut staged = diff_writes(&base, &base, head_before + 1);
    staged.push(head_stamp(head_before + 1, base.batch_seq));
    let staged_bytes = stage_writes(&attempt, &staged).unwrap();
    let landed = commit_batch(
        attempt,
        head_before,
        head_before + 1,
        &staged,
        staged_bytes,
        HeadViewUpdate::Rebuild(Arc::clone(&base)),
        catalog.projections(),
    )
    .await
    .unwrap();

    assert_eq!(
        landed,
        Landed::LostRace,
        "an attempt whose premises predate a landed commit must not commit"
    );

    // And a fresh attempt does see it, so the loss is the only outcome the
    // interleaving admits — not a permanent wedge.
    let retry = catalog.begin_write_tx().await.unwrap();
    let fresh = head_view_for(&retry, catalog.projections()).await.unwrap();
    assert_eq!(fresh.snapshot.snapshot_id, head_before + 1);
    assert_eq!(
        fresh
            .data_files_of(crate::catalog::TableId::new(tables[0].get()))
            .len(),
        1
    );
    retry.rollback();

    catalog.close().await.unwrap();
}

/// A materialization issues many reads, and a commit landing between two of
/// them must not produce a view that holds one and misses the other. On a
/// read-write handle the transaction settles it: every read is at its start
/// sequence, so a commit landing mid-pass is simply invisible to all of
/// them. This stages that interleaving rather than racing for it — the
/// commit lands between the pass's head read and its `current` scan.
#[tokio::test]
async fn a_commit_landing_mid_materialization_is_invisible_to_the_whole_pass() {
    let (catalog, tables) = seeded_wide_catalog(3, 2).await;

    let pass = catalog.begin_write_tx().await.unwrap();
    let handle = ReadHandle::Tx(&pass);

    // The first read of a materialization.
    let head = read_head_value(handle).await.unwrap();

    // A commit lands in the middle of the pass, changing both halves of
    // what it is about to read.
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("s").unwrap().id;
            tx.create_table(
                schema,
                "mid",
                &[crate::catalog::ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                    children: Vec::new(),
                }],
            )?;
            tx.register_data_file(tables[0], bulk_file("mid.parquet", 3), &[])
                .map(|_| ())
        })
        .await
        .unwrap();

    // The rest of the pass.
    let current = read::scan_current_entities(handle).await.unwrap();
    let after = read_head_value(handle).await.unwrap();
    let view = materialize(handle, None).await.unwrap();
    pass.rollback();

    assert_eq!(
        after, head,
        "the pass's second head read must agree with its first"
    );
    let schema = view.schema_by_name("s").unwrap().id;
    assert!(
        view.table_by_name(schema, "mid").is_none(),
        "the pass must be entirely pre-commit, not a mix"
    );
    assert!(
        view.data_files_of(crate::catalog::TableId::new(tables[0].get()))
            .is_empty(),
        "the pass must be entirely pre-commit, not a mix"
    );
    assert_eq!(
        current
            .iter()
            .filter(|record| matches!(record, crate::store::read::EntityRecord::File(_)))
            .count(),
        0,
        "the scan half of the pass must be pre-commit too"
    );

    // A fresh pass sees the whole commit, so what the held one showed was
    // one state and not a stale accident.
    let next = catalog.begin_write_tx().await.unwrap();
    let fresh = materialize(ReadHandle::Tx(&next), None).await.unwrap();
    next.rollback();
    assert!(fresh.table_by_name(schema, "mid").is_some());
    assert_eq!(
        fresh
            .data_files_of(crate::catalog::TableId::new(tables[0].get()))
            .len(),
        1
    );

    catalog.close().await.unwrap();
}

/// The read-only half of the same obligation. A reader has no transaction:
/// its state advances underneath it between calls, so a pass of several
/// reads really can straddle a commit. The head stamp is what catches it —
/// read before and after, and a pass that saw it move is discarded and
/// re-run. This forces exactly that by committing from inside the pass and
/// waiting for the reader to observe it.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_pass_that_straddles_a_commit_is_discarded_and_re_run() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::{
        catalog::{Catalog, CatalogOptions},
        store::{handle::ReadHandle as Handle, open::StoreBuilder},
    };

    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let writer = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            flush_interval: std::time::Duration::from_millis(1),
            ..CatalogOptions::default()
        },
    )
    .await
    .unwrap();
    writer
        .commit(|tx| tx.create_schema("s").map(|_| ()))
        .await
        .unwrap();

    let (reader, _) = StoreBuilder::new("", object_store)
        .poll_interval(std::time::Duration::from_millis(20))
        .open_reader()
        .await
        .unwrap();
    let handle = Handle::Reader(&reader);

    let passes = AtomicUsize::new(0);
    let view = read::consistent(handle, || async {
        let pass = passes.fetch_add(1, Ordering::SeqCst);
        // The first pass commits from inside itself and waits for the
        // reader to pick the commit up, so it is guaranteed to straddle.
        // Later passes run clean.
        if pass == 0 {
            writer
                .commit(|tx| tx.create_schema("late").map(|_| ()))
                .await
                .unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                if read_head_value(handle).await.unwrap().snapshot_id > 1 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
        materialize(handle, None).await
    })
    .await
    .unwrap();

    assert!(
        passes.load(Ordering::SeqCst) > 1,
        "a pass the store moved under must be discarded and re-run"
    );
    // And what came back is the post-commit state whole, not a mix.
    assert!(view.schema_by_name("s").is_some());
    assert!(view.schema_by_name("late").is_some());

    writer.close().await.unwrap();
}

/// The store state a retry meets when its base predates an expiry: three
/// commits, with the middle one's snapshot record reclaimed as expiry past
/// the retention horizon leaves it.
async fn catalog_with_a_reclaimed_snapshot()
-> (Db, Arc<std::sync::RwLock<ProjectionCache>>, Arc<Coalescer>) {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let (db, _) = open_initialized(StoreBuilder::new("", object_store), false, None)
        .await
        .unwrap();
    let projections = Arc::new(std::sync::RwLock::new(ProjectionCache::empty()));
    let coalescer = Arc::new(Coalescer::new(Arc::clone(&projections)));

    for name in ["a", "b", "c"] {
        commit_cycle(
            &db,
            &[|tx: &mut Transaction| tx.create_schema(name).map(|_| ())],
            &coalescer,
        )
        .await
        .unwrap();
    }

    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    tx.delete(Key::Snapshot { snapshot_id: 2 }.encode())
        .unwrap();
    tx.commit().await.unwrap();

    (db, projections, coalescer)
}

/// A retry whose base predates a concurrent expiry classifies against a
/// hole where an intervening snapshot record used to be. The verdict must
/// be conservative — a reclaimed commit's changes are unknowable, so it
/// conflicts — and must never read as corruption: expiry reclaiming a
/// record below the retention horizon is ordinary catalog life, not a
/// damaged store.
#[tokio::test]
async fn a_retry_across_a_reclaimed_snapshot_conflicts_rather_than_corrupting() {
    let (db, _projections, _coalescer) = catalog_with_a_reclaimed_snapshot().await;

    // What a losing attempt that read head 1 classifies: everything that
    // landed above it, reclaimed record included.
    let intervening = intervening_changes(&db, 1).await.unwrap();
    let ids: Vec<u64> = intervening.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![2, 3]);

    let reclaimed = &intervening[0].1;
    let surviving = &intervening[1].1;
    assert!(
        reclaimed.has_unknown,
        "a reclaimed record leaves its commit's changes unknowable"
    );
    assert!(!surviving.has_unknown);

    // Ours is disjoint from what actually landed, so the hole is the only
    // thing that conflicts: the conservative verdict comes from the missing
    // record, not from the commits around it.
    let ours = ChangeSet::parse("inserted_into_table:7");
    assert!(!crate::transaction::operations::conflicts(&ours, surviving));
    assert!(crate::transaction::operations::conflicts(&ours, reclaimed));

    db.close().await.unwrap();
}

/// The other half of that path. An expiry is head-preserving, so it clears
/// the cached view, and the next verb commit rematerializes over a keyspace
/// whose intervening snapshot records are gone. It must land: a
/// materialization resolves head, and head is not what expiry reclaimed.
#[tokio::test]
async fn a_verb_commit_rematerializes_over_a_reclaimed_snapshot() {
    let (db, projections, coalescer) = catalog_with_a_reclaimed_snapshot().await;
    invalidate_head_view(&projections);

    commit_cycle(
        &db,
        &[|tx: &mut Transaction| tx.create_schema("d").map(|_| ())],
        &coalescer,
    )
    .await
    .unwrap();

    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let handle = ReadHandle::Tx(&tx);
    let view = materialize(handle, None).await.unwrap();
    assert_eq!(view.snapshot.snapshot_id, 4);
    for name in ["main", "a", "b", "c", "d"] {
        assert!(
            view.schema_by_name(name).is_some(),
            "{name} must survive the reclaimed record"
        );
    }

    // And the reclaimed snapshot itself reads as expired rather than
    // missing or corrupt, since ids are sequential to head.
    let expired = materialize(handle, Some(2)).await.unwrap_err();
    assert!(matches!(expired, Error::SnapshotExpired(_)), "{expired}");
    tx.rollback();

    db.close().await.unwrap();
}

/// What a real migrator does to a live read-write handle, as against what
/// a planted marker models.
///
/// `Catalog::migrate` takes the writer epoch before it writes anything, so
/// the handle it displaces is already fenced by the time a marker exists —
/// and a fenced handle reads its own state, which that marker never
/// reached. So the displaced handle does not report `Migration` for this
/// sequence at any point: it reads no marker, serves the view it holds
/// until it notices the fence, and reports `Fenced` from then on.
///
/// Pinned because the per-read marker probe looks like the guard here and
/// is not — the fence check is. A marker planted through a handle's *own*
/// transaction is the only way to make that probe fire, and no writer
/// reaches that state.
#[tokio::test]
async fn a_marker_from_the_writer_that_fenced_us_never_reads_as_a_migration() {
    use crate::catalog::{Catalog, CatalogOptions};

    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let displaced = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    displaced
        .commit(|tx| tx.create_schema("before").map(|_| ()))
        .await
        .unwrap();
    // Warm: the handle now holds a head view and serves reads from it.
    displaced.snapshot().await.unwrap();

    // The displacing writer, taking the epoch exactly as a migrator does,
    // then writing the marker a migration's start batch writes.
    let migrator = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let tx = migrator.begin_write_tx().await.unwrap();
    tx.put(
        Key::Sys(SysKey::Migration).encode(),
        value::encode_value(&proto::MigrationValue {
            from_format: 1,
            to_format: 2,
            cursor: Vec::new(),
        }),
    )
    .unwrap();
    tx.commit_with_options(&durable()).await.unwrap();

    // Fencing is reported by a background task, so the displaced handle
    // does not learn of it in the same breath. Whatever it answers in that
    // window, it is never `Migration`.
    let mut noticed = None;
    for _ in 0..100 {
        match displaced.begin_read().await {
            Ok(session) => {
                assert_eq!(
                    crate::store::read::read_migration(session.handle())
                        .await
                        .unwrap(),
                    None,
                    "the displaced handle read the marker of the writer that fenced it"
                );
                session.finish();
            }
            Err(err) => {
                noticed = Some(err);
                break;
            }
        }
        if let Err(err) = displaced.snapshot().await {
            noticed = Some(err);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let noticed = noticed.expect("the displaced handle never noticed it had been fenced");
    assert!(matches!(noticed, Error::Fenced(_)), "{noticed:?}");
}

/// The batch's reported size is the batch: every key and value staged,
/// index entries included. It is what the stall warning names, so an
/// undercount would point an operator at the wrong cause.
#[tokio::test]
async fn a_staged_batch_reports_the_bytes_it_holds() {
    let (catalog, table) = catalog_with_two_column_table().await;

    let entries: Vec<crate::catalog::IndexEntry> = (0..4)
        .map(|row_id| crate::catalog::IndexEntry {
            row_id,
            values: vec![Some(int_value(i128::from(row_id)))],
        })
        .collect();
    let def = crate::catalog::IndexDef {
        name: "by_a".into(),
        columns: vec![crate::catalog::ColumnId::new(1)],
        unique: false,
    };

    let base = catalog.snapshot().await.unwrap();
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let prepared = prepare_and_stage(
        &db_tx,
        &|tx: &mut Transaction| tx.create_index(table, &def, &entries).map(|_| ()),
        &base,
    )
    .await
    .unwrap();

    let Prepared::Staged {
        writes,
        staged_bytes,
        ..
    } = prepared
    else {
        panic!("the closure staged writes");
    };

    let catalog_bytes: u64 = writes
        .iter()
        .map(|(key, value)| (key.len() + value.as_ref().map_or(0, Vec::len)) as u64)
        .sum();
    // The entries' own contribution is bracketed rather than exact: the
    // per-entry estimate is taken before encoding, and framing escapes the
    // zero bytes a big-endian integer key is full of.
    let nominal: u64 = entries
        .iter()
        .map(crate::catalog::IndexEntry::nominal_bytes)
        .sum();

    assert!(nominal > 0, "the entries weigh something");
    assert!(
        staged_bytes.0 >= catalog_bytes + nominal,
        "{} < {catalog_bytes} + {nominal}",
        staged_bytes.0
    );
    assert!(
        staged_bytes.0 <= catalog_bytes + 2 * nominal,
        "{} > {catalog_bytes} + 2 * {nominal}",
        staged_bytes.0
    );

    db_tx.rollback();
    catalog.close().await.unwrap();
}
