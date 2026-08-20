//! `ReadOnlyCatalog::locate_row_ids`: which current files hold a row id.

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use moraine::{Catalog, DataFile, DataFileId, DataStore, InlineChunk, TableId};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

use crate::fixtures::{col, datafile, open_memory};

/// DuckLake's reserved row-id column, tagged so discovery finds it.
fn row_id_field() -> Field {
    Field::new("_ducklake_internal_row_id", DataType::Int64, false).with_metadata(HashMap::from([
        (
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2147483540".to_string(),
        ),
    ]))
}

/// Writes `batch` and returns the sizes the catalog records for it.
#[allow(clippy::unwrap_used)]
async fn write(store: &InMemory, path: &str, batch: &RecordBatch) -> (u64, u64) {
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    let footer_offset = buffer.len() - 8;
    let footer_size = u64::from(u32::from_le_bytes(
        buffer[footer_offset..footer_offset + 4].try_into().unwrap(),
    ));
    let file_size = u64::try_from(buffer.len()).unwrap();
    store.put(&Path::from(path), buffer.into()).await.unwrap();
    (file_size, footer_size)
}

/// A batch of `a` values carrying explicit row ids.
#[allow(clippy::unwrap_used)]
fn batch_with_row_ids(values: &[i64], row_ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        row_id_field(),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(values.to_vec())),
            Arc::new(Int64Array::from(row_ids.to_vec())),
        ],
    )
    .unwrap()
}

/// A plain batch with no row-id column, numbered by its dense start.
#[allow(clippy::unwrap_used)]
fn dense_batch(values: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
}

/// Creates table `orders` and registers `files` against it.
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn table_with(catalog: &Catalog, files: Vec<DataFile>) -> TableId {
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "orders", &[col("a")])?;
            for file in files.clone() {
                tx.register_data_file(table, file, &[])?;
            }
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();
    created.get().unwrap()
}

/// Candidate file ids for `row_id`, sorted.
fn files_for(candidates: &[moraine::FileRowCandidate], row_id: u64) -> Vec<Option<u64>> {
    let mut found: Vec<Option<u64>> = candidates
        .iter()
        .filter(|candidate| candidate.row_id == row_id)
        .map(|candidate| candidate.data_file_id.map(DataFileId::get))
        .collect();
    found.sort_unstable();
    found
}

#[tokio::test]
async fn a_dense_file_answers_from_its_recorded_range() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let (file_size_bytes, footer_size) = write(
        &data,
        "main/orders/data-3.parquet",
        &dense_batch(&[10, 20, 30]),
    )
    .await;
    let table = table_with(
        &catalog,
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;

    let located = catalog
        .locate_row_ids(Some(store), "", table, vec![0, 2, 7])
        .await
        .unwrap();

    let file = catalog.snapshot().await.unwrap().data_files_of(table)[0]
        .id
        .get();
    assert_eq!(files_for(&located, 0), vec![Some(file)]);
    assert_eq!(files_for(&located, 2), vec![Some(file)]);
    // 7 is past the file's range, so nothing holds it.
    assert_eq!(files_for(&located, 7), vec![None]);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn a_file_whose_embedded_ids_hold_gaps_is_read_exactly() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    // A dense start of 0 would claim 0, 1 and 2; the embedded ids are the
    // truth, and a range would both invent 1 and lose 12.
    let (file_size_bytes, footer_size) = write(
        &data,
        "main/orders/data-3.parquet",
        &batch_with_row_ids(&[10, 20, 30], &[5, 9, 12]),
    )
    .await;
    let table = table_with(
        &catalog,
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;

    let located = catalog
        .locate_row_ids(Some(store), "", table, vec![1, 5, 12])
        .await
        .unwrap();

    let file = catalog.snapshot().await.unwrap().data_files_of(table)[0]
        .id
        .get();
    assert_eq!(files_for(&located, 5), vec![Some(file)]);
    assert_eq!(files_for(&located, 12), vec![Some(file)]);
    assert_eq!(files_for(&located, 1), vec![None]);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn one_row_id_in_two_current_files_returns_both() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let (size_a, footer_a) = write(
        &data,
        "main/orders/data-2.parquet",
        &batch_with_row_ids(&[10, 20], &[4, 8]),
    )
    .await;
    let (size_b, footer_b) = write(
        &data,
        "main/orders/data-3.parquet",
        &batch_with_row_ids(&[30, 40, 50], &[8, 15, 16]),
    )
    .await;
    let table = table_with(
        &catalog,
        vec![
            DataFile {
                file_size_bytes: size_a,
                footer_size: footer_a,
                ..datafile(2)
            },
            DataFile {
                file_size_bytes: size_b,
                footer_size: footer_b,
                ..datafile(3)
            },
        ],
    )
    .await;

    let located = catalog
        .locate_row_ids(Some(store), "", table, vec![8])
        .await
        .unwrap();

    let snapshot = catalog.snapshot().await.unwrap();
    let mut ids: Vec<Option<u64>> = snapshot
        .data_files_of(table)
        .iter()
        .map(|file| Some(file.id.get()))
        .collect();
    ids.sort_unstable();
    assert_eq!(files_for(&located, 8), ids);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn an_unreadable_file_leaves_every_requested_row_a_candidate() {
    let catalog = open_memory().await;
    // The catalog registers a file the data store does not hold, so its
    // summary cannot be built.
    let data: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let table = table_with(&catalog, vec![datafile(3)]).await;

    let located = catalog
        .locate_row_ids(Some(store), "", table, vec![100, 200])
        .await
        .unwrap();

    let file = catalog.snapshot().await.unwrap().data_files_of(table)[0]
        .id
        .get();
    // Degrading broadens: both rows stay candidates for the file it could
    // not summarize, rather than being reported as located nowhere.
    assert_eq!(files_for(&located, 100), vec![Some(file)]);
    assert_eq!(files_for(&located, 200), vec![Some(file)]);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn an_inlined_row_locates_with_no_file() {
    let catalog = open_memory().await;
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "orders", &[col("a")])?;
            tx.inline_insert(
                table,
                &InlineChunk {
                    schema_version: 0,
                    arrow_schema: b"schema-v0".to_vec(),
                    arrow_body: b"rows".to_vec(),
                    row_count: 2,
                },
                &[],
            )?;
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();
    let table = created.get().unwrap();

    // Row 1 is inlined and row 9 is nowhere. Both come back unlocated, and
    // neither is dropped.
    let located = catalog
        .locate_row_ids(None, "", table, vec![1, 9])
        .await
        .unwrap();

    assert_eq!(files_for(&located, 1), vec![None]);
    assert_eq!(files_for(&located, 9), vec![None]);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn no_requested_rows_locate_nothing() {
    let catalog = open_memory().await;
    let table = table_with(&catalog, vec![]).await;

    let located = catalog
        .locate_row_ids(None, "", table, vec![])
        .await
        .unwrap();

    assert!(located.is_empty());
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn warming_builds_the_summaries_a_cold_lookup_would_pay_for() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    // A compaction-output shape: embedded row ids, so a lookup must read
    // the column rather than trust a dense range.
    let (file_size_bytes, footer_size) = write(
        &data,
        "main/orders/data-3.parquet",
        &batch_with_row_ids(&[10, 20, 30], &[5, 9, 12]),
    )
    .await;
    let table = table_with(
        &catalog,
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;

    let first = catalog
        .warm_row_summaries(store.clone(), "", table)
        .await
        .unwrap();
    assert_eq!(first.files_considered, 1);
    assert_eq!(first.summaries_built, 1);
    assert_eq!(first.files_failed, 0);

    // Idempotent: the summary is now resident, so a second pass reads
    // nothing.
    let second = catalog
        .warm_row_summaries(store.clone(), "", table)
        .await
        .unwrap();
    assert_eq!(second.summaries_built, 0);

    // And the warmed summary answers the same as a cold lookup would.
    let located = catalog
        .locate_row_ids(Some(store), "", table, vec![9, 11])
        .await
        .unwrap();
    let file = catalog.snapshot().await.unwrap().data_files_of(table)[0]
        .id
        .get();
    assert_eq!(files_for(&located, 9), vec![Some(file)]);
    assert_eq!(files_for(&located, 11), vec![None]);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn a_dense_file_needs_no_warming() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let (file_size_bytes, footer_size) = write(
        &data,
        "main/orders/data-3.parquet",
        &dense_batch(&[10, 20, 30]),
    )
    .await;
    let table = table_with(
        &catalog,
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;

    let warmth = catalog.warm_row_summaries(store, "", table).await.unwrap();

    // It answers from its recorded range, so nothing is read or budgeted.
    assert_eq!(warmth.files_considered, 1);
    assert_eq!(warmth.summaries_built, 0);
    assert_eq!(warmth.files_failed, 0);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn warming_counts_a_file_it_cannot_read_rather_than_failing() {
    let catalog = open_memory().await;
    let data: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let table = table_with(&catalog, vec![datafile(3)]).await;

    let warmth = catalog.warm_row_summaries(store, "", table).await.unwrap();

    assert_eq!(warmth.files_considered, 1);
    assert_eq!(warmth.summaries_built, 0);
    assert_eq!(warmth.files_failed, 1);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn warming_every_table_reaches_tables_outside_the_first_schema() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let (orders_size, orders_footer) = write(
        &data,
        "main/orders/data-3.parquet",
        &batch_with_row_ids(&[10, 20, 30], &[5, 9, 12]),
    )
    .await;
    let (events_size, events_footer) = write(
        &data,
        "analytics/events/data-2.parquet",
        &batch_with_row_ids(&[40, 50], &[21, 22]),
    )
    .await;
    catalog
        .commit(|tx| {
            let main = tx.schema_by_name("main").expect("bootstrap schema").id;
            let orders = tx.create_table(main, "orders", &[col("a")])?;
            tx.register_data_file(
                orders,
                DataFile {
                    file_size_bytes: orders_size,
                    footer_size: orders_footer,
                    ..datafile(3)
                },
                &[],
            )?;

            let analytics = tx.create_schema("analytics")?;
            let events = tx.create_table(analytics, "events", &[col("a")])?;
            tx.register_data_file(
                events,
                DataFile {
                    file_size_bytes: events_size,
                    footer_size: events_footer,
                    ..datafile(2)
                },
                &[],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let warmth = catalog.warm_all_row_summaries(store, "").await.unwrap();

    assert_eq!(warmth.files_considered, 2);
    assert_eq!(warmth.summaries_built, 2);
    assert_eq!(warmth.files_failed, 0);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn warming_every_table_carries_on_past_a_table_it_cannot_read() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let (events_size, events_footer) = write(
        &data,
        "analytics/events/data-2.parquet",
        &batch_with_row_ids(&[40, 50], &[21, 22]),
    )
    .await;
    catalog
        .commit(|tx| {
            let main = tx.schema_by_name("main").expect("bootstrap schema").id;
            // Registered but never written: unreadable, and ordered before
            // the table that must still be warmed.
            let orders = tx.create_table(main, "orders", &[col("a")])?;
            tx.register_data_file(orders, datafile(3), &[])?;

            let analytics = tx.create_schema("analytics")?;
            let events = tx.create_table(analytics, "events", &[col("a")])?;
            tx.register_data_file(
                events,
                DataFile {
                    file_size_bytes: events_size,
                    footer_size: events_footer,
                    ..datafile(2)
                },
                &[],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let warmth = catalog.warm_all_row_summaries(store, "").await.unwrap();

    assert_eq!(warmth.files_considered, 2);
    assert_eq!(warmth.summaries_built, 1);
    assert_eq!(warmth.files_failed, 1);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn located_rows_keep_the_order_they_were_requested_in() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let (file_size_bytes, footer_size) = write(
        &data,
        "main/orders/data-3.parquet",
        &batch_with_row_ids(&[10, 20, 30], &[5, 9, 12]),
    )
    .await;
    let table = table_with(
        &catalog,
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;

    // A range or NULL scan hands its ids over already ordered; locating them
    // must not reorder them.
    let located = catalog
        .locate_row_ids(Some(store), "", table, vec![12, 5, 9])
        .await
        .unwrap();

    assert_eq!(
        located.iter().map(|row| row.row_id).collect::<Vec<_>>(),
        vec![12, 5, 9]
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn an_inlined_row_with_a_file_copy_keeps_both_candidates_in_any_request_order() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let (file_size_bytes, footer_size) = write(
        &data,
        "main/orders/data-3.parquet",
        &batch_with_row_ids(&[10, 20, 30], &[0, 1, 2]),
    )
    .await;
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "orders", &[col("a")])?;
            tx.inline_insert(
                table,
                &InlineChunk {
                    schema_version: 0,
                    arrow_schema: b"schema-v0".to_vec(),
                    arrow_body: b"rows".to_vec(),
                    row_count: 3,
                },
                &[],
            )?;
            tx.register_data_file(
                table,
                DataFile {
                    file_size_bytes,
                    footer_size,
                    ..datafile(3)
                },
                &[],
            )?;
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();
    let table = created.get().unwrap();
    let file = catalog.snapshot().await.unwrap().data_files_of(table)[0]
        .id
        .get();

    // Rows 0..3 are inlined and also held by the file; requested out of
    // order, every one keeps its inlined candidate beside the file one.
    let located = catalog
        .locate_row_ids(Some(store), "", table, vec![2, 0, 1])
        .await
        .unwrap();

    for row_id in [0, 1, 2] {
        assert_eq!(files_for(&located, row_id), vec![None, Some(file)]);
    }
    catalog.close().await.unwrap();
}

/// A cold lookup pays one footer read and one row-id column read per file
/// with embedded ids, and the handle's tally shows it; a warm repeat reads
/// nothing.
#[tokio::test]
async fn data_store_reads_land_in_the_handles_tally() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let mut files = Vec::new();
    for index in 0..3_i64 {
        let (file_size_bytes, footer_size) = write(
            &data,
            &format!("main/orders/data-{index}.parquet"),
            &batch_with_row_ids(&[1, 2, 3], &[index * 10, index * 10 + 2, index * 10 + 4]),
        )
        .await;
        files.push(DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(u64::try_from(index).unwrap())
        });
    }
    let table = table_with(&catalog, files).await;
    let before = catalog.object_store_tally();
    assert_eq!((before.data_gets, before.data_bytes), (0, 0));

    catalog
        .locate_row_ids(Some(store.clone()), "", table, vec![2, 12, 24])
        .await
        .unwrap();

    let cold = catalog.object_store_tally();
    assert_eq!(
        cold.data_gets - before.data_gets,
        6,
        "one footer and one column read per file"
    );
    assert!(cold.data_bytes > before.data_bytes);

    catalog
        .locate_row_ids(Some(store), "", table, vec![2, 12, 24])
        .await
        .unwrap();

    let warm = catalog.object_store_tally();
    assert_eq!(
        (warm.data_gets, warm.data_bytes),
        (cold.data_gets, cold.data_bytes)
    );
    catalog.close().await.unwrap();
}
