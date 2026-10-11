use std::sync::Arc;

use arrow::{array::Int64Array, record_batch::RecordBatch};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::ArrowWriter;

use crate::{
    Catalog, CatalogOptions, ColumnDef, DataFile, DataStore, DeleteFile, IndexDef, OptionScope,
    TableId, catalog::handle::index_build::tests::chunk,
};

async fn parquet(store: &InMemory, path: &str, name: &str, values: Vec<i64>) -> (u64, u64) {
    let batch =
        RecordBatch::try_from_iter([(name, Arc::new(Int64Array::from(values)) as _)]).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let footer = u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
    let size = bytes.len() as u64;
    store.put(&Path::from(path), bytes.into()).await.unwrap();
    (size, u64::from(footer))
}

/// Table `t` over `data.parquet` holding 10, 20, 30, with position 1 deleted,
/// so the live rows are 10 and 30.
async fn table_with_one_deleted_row(catalog: &Catalog, data: &InMemory) -> TableId {
    let (size, footer) = parquet(data, "data.parquet", "a", vec![10, 20, 30]).await;
    let (delete_size, delete_footer) = parquet(data, "delete.parquet", "pos", vec![1]).await;
    let table = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            tx.set_option(OptionScope::Global, "data_path", "/lake/")?;
            let schema = tx.schema_by_name("main").unwrap().id;
            let id = tx.create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    ..Default::default()
                }],
            )?;
            let file = tx.register_data_file(
                id,
                DataFile {
                    path: "data.parquet".into(),
                    path_is_relative: false,
                    file_format: "parquet".into(),
                    record_count: 3,
                    file_size_bytes: size,
                    footer_size: footer,
                    encryption_key: None,
                    partition_values: vec![],
                    column_stats: vec![],
                },
                &[],
            )?;
            tx.register_delete_file(
                id,
                DeleteFile {
                    data_file_id: file,
                    path: "delete.parquet".into(),
                    path_is_relative: false,
                    format: "parquet".into(),
                    delete_count: 1,
                    file_size_bytes: delete_size,
                    footer_size: delete_footer,
                    encryption_key: None,
                },
                &[],
            )?;
            table.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    table.get().unwrap()
}

/// A verifier pass over an index built from the same rows finds no
/// discrepancy, and counts only the rows a delete file left alive.
#[tokio::test]
async fn an_index_built_from_the_rows_verifies_clean() {
    let data = InMemory::new();
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let table = table_with_one_deleted_row(&catalog, &data).await;
    let store = DataStore::new(Arc::new(data));
    let column = catalog.snapshot().await.unwrap().columns_of(table)[0].id;
    let def = IndexDef {
        name: "by_a".into(),
        columns: vec![column],
        unique: true,
    };
    catalog
        .create_index_staged(table, &def, &[], Some(store.clone()), "/lake/", None)
        .await
        .unwrap();

    let report = catalog
        .verify_index(table, "by_a", Some(store), "/lake/")
        .await
        .unwrap();

    assert_eq!(report.expected, 2, "10 and 30 are live; 20 is deleted");
    assert_eq!(report.stored, 2);
    assert_eq!(report.missing, 0, "every live row has its entry");
    assert_eq!(report.dead, 0, "no entry outlives its row");
    catalog.close().await.unwrap();
}

/// The index, the live rows, and the verifier's view of both.
async fn indexed_table() -> (Catalog, TableId, crate::IndexInfo, DataStore) {
    let data = InMemory::new();
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let table = table_with_one_deleted_row(&catalog, &data).await;
    let store = DataStore::new(Arc::new(data));
    let column = catalog.snapshot().await.unwrap().columns_of(table)[0].id;
    let def = IndexDef {
        name: "by_a".into(),
        columns: vec![column],
        unique: true,
    };
    catalog
        .create_index_staged(table, &def, &[], Some(store.clone()), "/lake/", None)
        .await
        .unwrap();
    let info = catalog
        .snapshot()
        .await
        .unwrap()
        .index_by_name(table, "by_a")
        .unwrap();
    (catalog, table, info, store)
}

/// The encoded entry key for one integer value under `info`.
fn entry_key(info: &crate::IndexInfo, value: i64, row_id: u64) -> bytes::Bytes {
    let (key, _) = crate::store::index_encoding::encode_ordered_index_entry(
        &[Some(crate::IndexKeyValue::Int {
            value: i128::from(value),
            width: crate::IntWidth::I64,
        })],
        &info.directions,
        &info.nulls,
        info.id.get(),
        info.unique,
        row_id,
    )
    .unwrap();
    key
}

/// An entry under a value no live row holds is reported dead, and names the
/// row it claims.
#[tokio::test]
async fn an_entry_no_row_requires_is_dead() {
    let (catalog, table, info, store) = indexed_table().await;
    let transaction = catalog.begin_write_tx().await.unwrap();
    transaction
        .put(entry_key(&info, 999, 7), 7u64.to_be_bytes())
        .unwrap();
    transaction.commit().await.unwrap();

    let report = catalog
        .verify_index(table, "by_a", Some(store), "/lake/")
        .await
        .unwrap();

    assert_eq!(report.stored, 3);
    assert_eq!(report.expected, 2);
    assert_eq!(report.dead, 1);
    assert_eq!(report.missing, 0);
    assert_eq!(report.sample_dead, vec![7]);
    assert!(!report.is_clean());
    catalog.close().await.unwrap();
}

/// A live row whose entry was dropped is reported missing, and names the row.
#[tokio::test]
async fn a_row_without_its_entry_is_missing() {
    let (catalog, table, info, store) = indexed_table().await;
    let transaction = catalog.begin_write_tx().await.unwrap();
    transaction.delete(entry_key(&info, 30, 2)).unwrap();
    transaction.commit().await.unwrap();

    let report = catalog
        .verify_index(table, "by_a", Some(store), "/lake/")
        .await
        .unwrap();

    assert_eq!(report.stored, 1);
    assert_eq!(report.expected, 2);
    assert_eq!(report.missing, 1);
    assert_eq!(report.dead, 0);
    assert_eq!(report.sample_missing, vec![2], "row 2 holds the value 30");
    assert!(!report.is_clean());
    catalog.close().await.unwrap();
}

/// A unique entry under the right value naming the wrong row is neither
/// missing nor dead: the key is there, and it points somewhere else.
#[tokio::test]
async fn an_entry_naming_the_wrong_row_is_mismatched() {
    let (catalog, table, info, store) = indexed_table().await;
    let transaction = catalog.begin_write_tx().await.unwrap();
    transaction
        .put(entry_key(&info, 30, 2), 41u64.to_be_bytes())
        .unwrap();
    transaction.commit().await.unwrap();

    let report = catalog
        .verify_index(table, "by_a", Some(store), "/lake/")
        .await
        .unwrap();

    assert_eq!(report.stored, 2);
    assert_eq!(report.expected, 2);
    assert_eq!(report.mismatched, 1);
    assert_eq!(report.missing, 0);
    assert_eq!(report.dead, 0);
    assert!(!report.is_clean());
    catalog.close().await.unwrap();
}

/// A building index is refused rather than reported against, since its
/// entries are incomplete by design.
#[tokio::test]
async fn a_building_index_is_refused() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let table = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            table.set(Some(tx.create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    ..Default::default()
                }],
            )?));
            Ok(())
        })
        .await
        .unwrap();
    let table = table.get().unwrap();
    let column = catalog.snapshot().await.unwrap().columns_of(table)[0].id;
    let def = IndexDef {
        name: "by_a".into(),
        columns: vec![column],
        unique: true,
    };
    catalog
        .begin_staged_index(
            table,
            &def,
            &[],
            crate::IndexMaintenance::Synchronous,
            false,
        )
        .await
        .unwrap();

    let result = catalog.verify_index(table, "by_a", None, "").await;

    assert!(
        matches!(result, Err(crate::Error::Constraint(_))),
        "{result:?}"
    );
    catalog.close().await.unwrap();
}

/// An absent index is a lookup failure, not a clean report.
#[tokio::test]
async fn an_absent_index_is_not_found() {
    let (catalog, table, _info, store) = indexed_table().await;
    let result = catalog
        .verify_index(table, "by_b", Some(store), "/lake/")
        .await;
    assert!(
        matches!(result, Err(crate::Error::NotFound(_))),
        "{result:?}"
    );
    catalog.close().await.unwrap();
}

fn integer(value: i64) -> crate::IndexKeyValue {
    crate::IndexKeyValue::Int {
        value: i128::from(value),
        width: crate::IntWidth::I64,
    }
}

/// One step of a generated DML sequence over inlined rows.
#[derive(Debug, Clone)]
enum Step {
    Insert(Vec<i64>),
    Delete(usize),
}

/// Applies `steps` to a fresh inline-only table carrying a unique index,
/// returning the catalog and the values that should be live.
async fn apply_inline_steps(steps: Vec<Step>) -> (Catalog, TableId, Vec<i64>) {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let table = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            table.set(Some(tx.create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    ..Default::default()
                }],
            )?));
            Ok(())
        })
        .await
        .unwrap();
    let table = table.get().unwrap();
    let column = catalog.snapshot().await.unwrap().columns_of(table)[0].id;
    let index = catalog
        .create_index_staged(
            table,
            &IndexDef {
                name: "by_a".into(),
                columns: vec![column],
                unique: true,
            },
            &[],
            None,
            "",
            None,
        )
        .await
        .unwrap();

    // Row ids are dense and allocated in insert order, so the live set is
    // tracked alongside the ids moraine assigns.
    let mut live: Vec<(u64, i64)> = Vec::new();
    let mut next_row_id = 0u64;
    for step in &steps {
        match step {
            Step::Insert(values) => {
                // A unique index refuses a value twice, including twice
                // within one commit, so the oracle offers each value once.
                let mut fresh: Vec<i64> = Vec::new();
                for value in values {
                    let held = live.iter().any(|(_, held)| held == value) || fresh.contains(value);
                    if !held {
                        fresh.push(*value);
                    }
                }
                if fresh.is_empty() {
                    continue;
                }
                // The embedding API takes entries from the writer, so the
                // oracle supplies them as a writer would.
                let entries: Vec<crate::FileIndexEntry> = fresh
                    .iter()
                    .enumerate()
                    .map(|(ordinal, value)| crate::FileIndexEntry {
                        index,
                        ordinal: ordinal as u64,
                        values: vec![Some(integer(*value))],
                    })
                    .collect();
                catalog
                    .commit(|tx| {
                        tx.inline_insert(table, &chunk(fresh.clone()), &entries)
                            .map(|_| ())
                    })
                    .await
                    .unwrap();
                for value in fresh {
                    live.push((next_row_id, value));
                    next_row_id += 1;
                }
            }
            Step::Delete(pick) => {
                if live.is_empty() {
                    continue;
                }
                let position = pick % live.len();
                let (row_id, value) = live[position];
                let removals = vec![crate::FileIndexRemoval {
                    index,
                    row_id,
                    values: vec![Some(integer(value))],
                }];
                catalog
                    .commit(|tx| tx.inline_delete(table, row_id, &removals))
                    .await
                    .unwrap();
                live.remove(position);
            }
        }
    }
    let values = live.into_iter().map(|(_, value)| value).collect();
    (catalog, table, values)
}

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    /// Synchronous maintenance keeps the index exactly covering the live
    /// rows, whatever order inserts and deletes arrive in.
    #[test]
    fn maintenance_keeps_the_index_verifying_clean(
        steps in prop::collection::vec(
            prop_oneof![
                prop::collection::vec(0i64..32, 1..4).prop_map(Step::Insert),
                (0usize..32).prop_map(Step::Delete),
            ],
            1..12,
        ),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (catalog, table, live) = runtime.block_on(apply_inline_steps(steps.clone()));
        let report = runtime
            .block_on(catalog.verify_index(table, "by_a", None, ""))
            .unwrap();
        prop_assert!(
            report.is_clean(),
            "{steps:?} left {report:?}",
        );
        prop_assert_eq!(
            report.expected,
            u64::try_from(live.len()).unwrap(),
            "the index covers exactly the live rows",
        );
        runtime.block_on(catalog.close()).unwrap();
    }
}

/// A NULL is collision-exempt, so its entry takes the multi shape even on a
/// unique index. Both keyspaces have to be read, or every NULL row looks
/// like a missing entry.
#[tokio::test]
async fn null_entries_on_a_unique_index_are_not_reported_missing() {
    let data = InMemory::new();
    let batch = RecordBatch::try_from_iter([(
        "a",
        Arc::new(Int64Array::from(vec![Some(10), None, Some(30), None])) as _,
    )])
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let footer = u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
    let size = bytes.len() as u64;
    data.put(&Path::from("nulls.parquet"), bytes.into())
        .await
        .unwrap();

    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let table = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            tx.set_option(OptionScope::Global, "data_path", "/lake/")?;
            let schema = tx.schema_by_name("main").unwrap().id;
            let id = tx.create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    ..Default::default()
                }],
            )?;
            tx.register_data_file(
                id,
                DataFile {
                    path: "nulls.parquet".into(),
                    path_is_relative: false,
                    file_format: "parquet".into(),
                    record_count: 4,
                    file_size_bytes: size,
                    footer_size: u64::from(footer),
                    encryption_key: None,
                    partition_values: vec![],
                    column_stats: vec![],
                },
                &[],
            )?;
            table.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let table = table.get().unwrap();
    let store = DataStore::new(Arc::new(data));
    let column = catalog.snapshot().await.unwrap().columns_of(table)[0].id;
    catalog
        .create_index_staged(
            table,
            &IndexDef {
                name: "by_a".into(),
                columns: vec![column],
                unique: true,
            },
            &[],
            Some(store.clone()),
            "/lake/",
            None,
        )
        .await
        .unwrap();

    let report = catalog
        .verify_index(table, "by_a", Some(store), "/lake/")
        .await
        .unwrap();

    assert_eq!(report.expected, 4, "two values and two NULLs");
    assert_eq!(report.stored, 4);
    assert!(report.is_clean(), "{report:?}");
    catalog.close().await.unwrap();
}
