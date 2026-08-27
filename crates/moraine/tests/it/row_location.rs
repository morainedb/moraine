//! `ReadOnlyCatalog::locate_row_ids`: which current files hold a row id.

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use moraine::{
    Catalog, DataFile, DataFileId, DataStore, DeleteFile, DeleteFileRegistration, Error, IndexDef,
    IndexKeyValue, InlineChunk, IntWidth, TableId,
};
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

// `ReadOnlyCatalog::locate_row_positions`: exact-or-failed resolution of
// located rows to `(data_file_id, position)`, for deletion without a scan.
mod locate_row_positions {
    use super::*;

    /// A batch of `pos` values, as a DuckLake delete file stores them.
    #[allow(clippy::unwrap_used)]
    fn pos_batch(positions: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("pos", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(positions.to_vec()))]).unwrap()
    }

    /// A table with a dense file (row ids `0..3`) and a rewrite file whose
    /// embedded ids form two ascending runs (`[50,51,52,10,11]`), plus 8
    /// inlined rows allocated after both (ids `8..16`). Returns the two
    /// data-file ids alongside the table.
    #[allow(clippy::unwrap_used, clippy::expect_used)]
    async fn fixture(catalog: &Catalog, data: &InMemory) -> (TableId, DataFileId, DataFileId) {
        let (dense_size, dense_footer) =
            write(data, "main/orders/data-3.parquet", &dense_batch(&[1, 2, 3])).await;
        let (rewrite_size, rewrite_footer) = write(
            data,
            "main/orders/data-5.parquet",
            &batch_with_row_ids(&[1, 2, 3, 4, 5], &[50, 51, 52, 10, 11]),
        )
        .await;

        let created = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                tx.set_option(moraine::OptionScope::Global, "data_path", "")?;
                let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
                let table = tx.create_table(schema, "orders", &[col("a")])?;
                tx.register_data_file(
                    table,
                    DataFile {
                        file_size_bytes: dense_size,
                        footer_size: dense_footer,
                        ..datafile(3)
                    },
                    &[],
                )?;
                tx.register_data_file(
                    table,
                    DataFile {
                        file_size_bytes: rewrite_size,
                        footer_size: rewrite_footer,
                        ..datafile(5)
                    },
                    &[],
                )?;
                tx.inline_insert(
                    table,
                    &InlineChunk {
                        schema_version: 0,
                        arrow_schema: b"schema-v0".to_vec(),
                        arrow_body: b"rows".to_vec(),
                        row_count: 8,
                    },
                    &[],
                )?;
                created.set(Some(table));
                Ok(())
            })
            .await
            .unwrap();
        let table = created.get().unwrap();
        let snapshot = catalog.snapshot().await.unwrap();
        let files = snapshot.data_files_of(table);
        let dense_id = files
            .iter()
            .find(|file| file.record_count == 3)
            .expect("dense file registered")
            .id;
        let rewrite_id = files
            .iter()
            .find(|file| file.record_count == 5)
            .expect("rewrite file registered")
            .id;
        (table, dense_id, rewrite_id)
    }

    #[tokio::test]
    async fn a_dense_file_a_rewrite_file_and_an_inlined_row_all_position_exactly() {
        let catalog = open_memory().await;
        let data = Arc::new(InMemory::new());
        let store = DataStore::new(data.clone());
        let (table, dense_id, rewrite_id) = fixture(&catalog, &data).await;

        // Row 1 is the dense file's second row (position 1); row 11 is the
        // rewrite file's last embedded id (position 4); row 9 is inlined.
        let located = catalog
            .locate_row_positions(
                Some(store),
                "",
                table,
                &[(1, Some(dense_id)), (11, Some(rewrite_id)), (9, None)],
            )
            .await
            .unwrap();

        assert_eq!(located.inlined_rows, vec![9]);
        assert_eq!(located.deletions.len(), 2);
        assert!(located.write_directory.is_some());
        let dense = located
            .deletions
            .iter()
            .find(|deletion| deletion.data_file_id == dense_id)
            .unwrap();
        assert_eq!(dense.positions, vec![1]);
        assert!(dense.existing_delete.is_none());
        let rewrite = located
            .deletions
            .iter()
            .find(|deletion| deletion.data_file_id == rewrite_id)
            .unwrap();
        assert_eq!(rewrite.positions, vec![4]);
        assert!(rewrite.existing_delete.is_none());
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_pair_naming_the_wrong_file_is_a_typed_error() {
        let catalog = open_memory().await;
        let data = Arc::new(InMemory::new());
        let store = DataStore::new(data.clone());
        let (table, _dense_id, rewrite_id) = fixture(&catalog, &data).await;

        // Row 1 actually lives in the dense file, not the rewrite file.
        let result = catalog
            .locate_row_positions(Some(store), "", table, &[(1, Some(rewrite_id))])
            .await;

        assert!(
            matches!(result, Err(Error::RowPosition { row_id: 1, .. })),
            "expected a typed RowPosition error, got {result:?}"
        );
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_unknown_file_id_is_a_typed_error() {
        let catalog = open_memory().await;
        let data = Arc::new(InMemory::new());
        let store = DataStore::new(data.clone());
        let (table, _dense_id, _rewrite_id) = fixture(&catalog, &data).await;

        let result = catalog
            .locate_row_positions(
                Some(store),
                "",
                table,
                &[(1, Some(DataFileId::new(999_999)))],
            )
            .await;

        assert!(
            matches!(result, Err(Error::RowPosition { row_id: 1, .. })),
            "expected a typed RowPosition error, got {result:?}"
        );
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_null_pair_for_a_row_that_is_not_inlined_is_a_typed_error() {
        let catalog = open_memory().await;
        let data = Arc::new(InMemory::new());
        let store = DataStore::new(data.clone());
        let (table, _dense_id, _rewrite_id) = fixture(&catalog, &data).await;

        let result = catalog
            .locate_row_positions(Some(store), "", table, &[(9999, None)])
            .await;

        assert!(
            matches!(
                result,
                Err(Error::RowPosition {
                    row_id: 9999,
                    data_file_id: None,
                    ..
                })
            ),
            "expected a typed RowPosition error, got {result:?}"
        );
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_pairs_collapse_to_one_answer() {
        let catalog = open_memory().await;
        let data = Arc::new(InMemory::new());
        let store = DataStore::new(data.clone());
        let (table, dense_id, _rewrite_id) = fixture(&catalog, &data).await;

        let located = catalog
            .locate_row_positions(
                Some(store),
                "",
                table,
                &[(1, Some(dense_id)), (1, Some(dense_id))],
            )
            .await
            .unwrap();

        assert!(located.inlined_rows.is_empty());
        assert_eq!(located.deletions.len(), 1);
        assert_eq!(located.deletions[0].positions, vec![1]);
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_existing_delete_file_surfaces_its_id_path_and_positions() {
        let catalog = open_memory().await;
        let data = Arc::new(InMemory::new());
        let store = DataStore::new(data.clone());
        let (table, dense_id, _rewrite_id) = fixture(&catalog, &data).await;

        // Row 0 (position 0) is already marked dead by a registered delete
        // file; this call positions a different row (1) in the same file.
        let (delete_size, delete_footer) =
            write(&data, "main/orders/delete-1.parquet", &pos_batch(&[0])).await;
        catalog
            .commit(|tx| {
                tx.register_delete_file(
                    table,
                    DeleteFile {
                        data_file_id: dense_id,
                        path: "delete-1.parquet".into(),
                        path_is_relative: true,
                        format: "parquet".into(),
                        delete_count: 1,
                        file_size_bytes: delete_size,
                        footer_size: delete_footer,
                        encryption_key: None,
                    },
                    &[],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let located = catalog
            .locate_row_positions(Some(store), "", table, &[(1, Some(dense_id))])
            .await
            .unwrap();

        assert_eq!(located.deletions.len(), 1);
        let existing = located.deletions[0].existing_delete.as_ref().unwrap();
        assert_eq!(existing.path, "delete-1.parquet");
        assert_eq!(existing.positions, vec![0]);
        catalog.close().await.unwrap();
    }
}

// `Catalog::commit_located_deletion`: landing located deletions through the
// existing register/expire/inline-delete verbs in one autonomous commit.
mod commit_located_deletion {
    use super::*;

    /// A table with two data files (`a`: 3 rows, `b`: 5 rows) and no
    /// inlined rows yet.
    #[allow(clippy::unwrap_used)]
    async fn table_with_two_files(catalog: &Catalog) -> (TableId, DataFileId, DataFileId) {
        let table = table_with(catalog, vec![datafile(3), datafile(5)]).await;
        let files = catalog.snapshot().await.unwrap().data_files_of(table);
        let file_a = files.iter().find(|file| file.record_count == 3).unwrap().id;
        let file_b = files.iter().find(|file| file.record_count == 5).unwrap().id;
        (table, file_a, file_b)
    }

    /// Registers a delete file directly through the verb, for a fixture to
    /// set up the "already has a delete file" starting state.
    #[allow(clippy::unwrap_used)]
    async fn register_delete_file(catalog: &Catalog, table: TableId, data_file_id: DataFileId) {
        catalog
            .commit(move |tx| {
                tx.register_delete_file(
                    table,
                    DeleteFile {
                        data_file_id,
                        path: "old.parquet".into(),
                        path_is_relative: true,
                        format: "parquet".into(),
                        delete_count: 1,
                        file_size_bytes: 10,
                        footer_size: 4,
                        encryption_key: None,
                    },
                    &[],
                )
                .map(|_| ())
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn registers_expires_and_inline_deletes_in_one_commit() {
        let catalog = open_memory().await;
        let (table, file_a, file_b) = table_with_two_files(&catalog).await;

        // `file_b` already carries a delete file; the commit must replace
        // it, not amend it.
        register_delete_file(&catalog, table, file_b).await;
        let old_delete_id = catalog.snapshot().await.unwrap().delete_files_of(table)[0].id;

        // Four inlined rows land after the two files' 8 rows: ids 8..12.
        catalog
            .commit(move |tx| {
                tx.inline_insert(
                    table,
                    &InlineChunk {
                        schema_version: 0,
                        arrow_schema: b"schema-v0".to_vec(),
                        arrow_body: b"rows".to_vec(),
                        row_count: 4,
                    },
                    &[],
                )
                .map(|_| ())
            })
            .await
            .unwrap();

        catalog
            .commit_located_deletion(
                None,
                "",
                table,
                &[
                    DeleteFileRegistration {
                        data_file_id: file_a,
                        path: "new-a.parquet".into(),
                        file_size: 20,
                        footer_size: 4,
                        delete_count: 1,
                        expires: None,
                        new_positions: Vec::new(),
                    },
                    DeleteFileRegistration {
                        data_file_id: file_b,
                        path: "new-b.parquet".into(),
                        file_size: 30,
                        footer_size: 4,
                        delete_count: 2,
                        expires: Some(old_delete_id),
                        new_positions: Vec::new(),
                    },
                ],
                &[8],
            )
            .await
            .unwrap();

        let head = catalog.snapshot().await.unwrap();
        let deletes = head.delete_files_of(table);
        assert_eq!(deletes.len(), 2, "one live delete file per data file");
        assert!(
            deletes.iter().all(|delete| delete.id != old_delete_id),
            "the replaced delete file must be gone from the head"
        );
        assert!(
            deletes
                .iter()
                .any(|delete| delete.data_file_id == file_a && delete.path == "new-a.parquet")
        );
        assert!(
            deletes
                .iter()
                .any(|delete| delete.data_file_id == file_b && delete.path == "new-b.parquet")
        );
        assert!(
            catalog.recent_row(table, 8).await.unwrap().is_none(),
            "the inline-deleted row must be gone from the live rows"
        );
        catalog.close().await.unwrap();
    }

    /// One `commit_located_deletion` call expiring `old_delete_id` against
    /// `file_a`, writing `path` as its replacement.
    fn registration(
        file_a: DataFileId,
        old_delete_id: moraine::DeleteFileId,
        path: &str,
    ) -> DeleteFileRegistration {
        DeleteFileRegistration {
            data_file_id: file_a,
            path: path.to_owned(),
            file_size: 20,
            footer_size: 4,
            delete_count: 2,
            expires: Some(old_delete_id),
            new_positions: Vec::new(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn two_concurrent_commits_expiring_the_same_delete_file_conflict() {
        let catalog = open_memory().await;
        let (table, file_a, _file_b) = table_with_two_files(&catalog).await;
        register_delete_file(&catalog, table, file_a).await;
        let old_delete_id = catalog.snapshot().await.unwrap().delete_files_of(table)[0].id;

        let first_catalog = catalog.clone();
        let second_catalog = catalog.clone();
        let first_commit = tokio::spawn(async move {
            first_catalog
                .commit_located_deletion(
                    None,
                    "",
                    table,
                    &[registration(file_a, old_delete_id, "new-1.parquet")],
                    &[],
                )
                .await
        });
        let second_commit = tokio::spawn(async move {
            second_catalog
                .commit_located_deletion(
                    None,
                    "",
                    table,
                    &[registration(file_a, old_delete_id, "new-2.parquet")],
                    &[],
                )
                .await
        });
        let first_result = first_commit.await.unwrap();
        let second_result = second_commit.await.unwrap();

        // Serialized is fine; a genuine race surfaces the typed conflict on
        // the loser — `CommitConflict` from losing the head race, or
        // `NotFound` from a coalesced retry finding the file the winner
        // already expired — exactly as the rest of the delete-file
        // conflict surface reports it.
        for result in [&first_result, &second_result] {
            match result {
                Ok(_) | Err(Error::CommitConflict(_) | Error::NotFound(_)) => {}
                Err(other) => panic!("unexpected error: {other}"),
            }
        }
        let successes = [&first_result, &second_result]
            .iter()
            .filter(|result| result.is_ok())
            .count();
        assert_eq!(successes, 1, "exactly one racing commit lands");

        let head = catalog.snapshot().await.unwrap();
        assert_eq!(
            head.delete_files_of(table).len(),
            1,
            "only the winner's file is live"
        );
        catalog.close().await.unwrap();
    }

    /// The Arrow IPC schema-only stream for a single nullable `BIGINT`
    /// column `a`, matching what the extension's encoder writes for an
    /// inlined chunk.
    #[allow(clippy::unwrap_used)]
    fn bigint_schema_ipc() -> Vec<u8> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let mut buffer = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &schema).unwrap();
            writer.finish().unwrap();
        }
        buffer
    }

    /// One inline chunk body for a single-column `BIGINT` batch: a
    /// little-endian `u32` message length, the record-batch message, then
    /// the Arrow data buffers.
    #[allow(clippy::unwrap_used)]
    fn bigint_body(values: &[i64]) -> Vec<u8> {
        use arrow::ipc::writer::{
            DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
            .unwrap();

        let generator = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let options = IpcWriteOptions::default();
        let mut context = IpcWriteContext::default();
        let (dictionaries, encoded) = generator
            .encode(&batch, &mut tracker, &options, &mut context)
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

    /// Creates table `orders` with two dense data files (`file_a`: rows
    /// `[10, 20, 30]`; `file_b`: rows `[40, 50, 60]`) and one inlined row
    /// (id `6`, value `70`), returning the table and the two file ids.
    #[allow(clippy::unwrap_used, clippy::expect_used)]
    async fn table_with_files_and_inline_row(
        catalog: &Catalog,
        data: &Arc<InMemory>,
    ) -> (TableId, DataFileId, DataFileId) {
        let (a_size, a_footer) = write(
            data,
            "main/orders/data-a.parquet",
            &dense_batch(&[10, 20, 30]),
        )
        .await;
        let (b_size, b_footer) = write(
            data,
            "main/orders/data-b.parquet",
            &dense_batch(&[40, 50, 60]),
        )
        .await;

        let created = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
                let table = tx.create_table(schema, "orders", &[col("a")])?;
                tx.register_data_file(
                    table,
                    DataFile {
                        path: "data-a.parquet".into(),
                        path_is_relative: true,
                        file_format: "parquet".into(),
                        record_count: 3,
                        file_size_bytes: a_size,
                        footer_size: a_footer,
                        encryption_key: None,
                        partition_values: vec![],
                        column_stats: vec![],
                    },
                    &[],
                )?;
                tx.register_data_file(
                    table,
                    DataFile {
                        path: "data-b.parquet".into(),
                        path_is_relative: true,
                        file_format: "parquet".into(),
                        record_count: 3,
                        file_size_bytes: b_size,
                        footer_size: b_footer,
                        encryption_key: None,
                        partition_values: vec![],
                        column_stats: vec![],
                    },
                    &[],
                )?;
                tx.inline_insert(
                    table,
                    &InlineChunk {
                        schema_version: 0,
                        arrow_schema: bigint_schema_ipc(),
                        arrow_body: bigint_body(&[70]),
                        row_count: 1,
                    },
                    &[],
                )?;
                created.set(Some(table));
                Ok(())
            })
            .await
            .unwrap();
        let table = created.get().unwrap();

        let snapshot = catalog.snapshot().await.unwrap();
        let files = snapshot.data_files_of(table);
        let file_a = files
            .iter()
            .find(|file| file.path == "data-a.parquet")
            .unwrap()
            .id;
        let file_b = files
            .iter()
            .find(|file| file.path == "data-b.parquet")
            .unwrap()
            .id;

        (table, file_a, file_b)
    }

    /// Creates a non-unique index named `idx_a` over `table`'s column `a`,
    /// backfilled from both its data files and its inlined rows.
    #[allow(clippy::unwrap_used)]
    async fn create_index_over_column_a(
        catalog: &Catalog,
        data: &Arc<InMemory>,
        table: TableId,
    ) -> moraine::IndexId {
        let snapshot = catalog.snapshot().await.unwrap();
        let column_a = snapshot.columns_of(table)[0].id;

        let mut backfill = catalog
            .scoped_backfill_entries(DataStore::new(data.clone()), "", table, &[column_a])
            .await
            .unwrap();
        backfill.extend(
            catalog
                .inline_backfill_entries(table, &[column_a])
                .await
                .unwrap(),
        );

        let created = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let index = tx.create_index(
                    table,
                    &IndexDef {
                        name: "idx_a".to_owned(),
                        columns: vec![column_a],
                        unique: false,
                    },
                    &backfill,
                )?;
                created.set(Some(index));
                Ok(())
            })
            .await
            .unwrap();
        created.get().unwrap()
    }

    /// A table with a live non-unique index over column `a`, two dense
    /// data files (`file_a`: rows `[10, 20, 30]`; `file_b`: rows
    /// `[40, 50, 60]`), and one inlined row (id `6`, value `70`).
    async fn indexed_table(
        catalog: &Catalog,
        data: &Arc<InMemory>,
    ) -> (TableId, moraine::IndexId, DataFileId, DataFileId) {
        let (table, file_a, file_b) = table_with_files_and_inline_row(catalog, data).await;
        let index = create_index_over_column_a(catalog, data, table).await;
        (table, index, file_a, file_b)
    }

    /// The equality key `value` is stored under.
    fn int_key(value: i64) -> IndexKeyValue {
        IndexKeyValue::Int {
            value: value.into(),
            width: IntWidth::I64,
        }
    }

    #[tokio::test]
    async fn commit_located_deletion_removes_a_files_index_entries_and_reads_only_that_file() {
        let catalog = open_memory().await;
        let data = Arc::new(InMemory::new());
        let (table, index, file_a, _file_b) = indexed_table(&catalog, &data).await;

        let counting = Arc::new(crate::counting_store::CountingStore::new(data));
        let counted_store = DataStore::new(counting.clone());
        let _ = counting.take_touched_paths();

        catalog
            .commit_located_deletion(
                Some(counted_store),
                "",
                table,
                &[DeleteFileRegistration {
                    data_file_id: file_a,
                    path: "delete-a.parquet".into(),
                    file_size: 10,
                    footer_size: 4,
                    delete_count: 1,
                    expires: None,
                    // Row 20 is the dense file's second row: position 1.
                    new_positions: vec![1],
                }],
                &[],
            )
            .await
            .unwrap();

        let touched = counting.take_touched_paths();
        assert_eq!(
            touched,
            vec!["main/orders/data-a.parquet".to_owned()],
            "the derivation must read only the killed file, not the untouched sibling"
        );

        assert!(
            catalog
                .index_lookup(table, index, &[int_key(20)])
                .await
                .unwrap()
                .is_empty(),
            "the deleted row's entry must be gone"
        );
        assert_eq!(
            catalog
                .index_lookup(table, index, &[int_key(10)])
                .await
                .unwrap(),
            vec![0],
            "a surviving row in the same file keeps its entry"
        );
        assert_eq!(
            catalog
                .index_lookup(table, index, &[int_key(30)])
                .await
                .unwrap(),
            vec![2],
            "a surviving row in the same file keeps its entry"
        );
        assert_eq!(
            catalog
                .index_lookup(table, index, &[int_key(40)])
                .await
                .unwrap(),
            vec![3],
            "the untouched file's rows keep their entries"
        );
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn commit_located_deletion_removes_an_inlined_rows_index_entry() {
        let catalog = open_memory().await;
        let data = Arc::new(InMemory::new());
        let (table, index, ..) = indexed_table(&catalog, &data).await;

        catalog
            .commit_located_deletion(None, "", table, &[], &[6])
            .await
            .unwrap();

        assert!(
            catalog
                .index_lookup(table, index, &[int_key(70)])
                .await
                .unwrap()
                .is_empty(),
            "the deleted inlined row's entry must be gone"
        );
        assert_eq!(
            catalog
                .index_lookup(table, index, &[int_key(10)])
                .await
                .unwrap(),
            vec![0],
            "an unrelated file-backed row keeps its entry"
        );
        catalog.close().await.unwrap();
    }

    /// A flush that lands and settles between locate and commit turns an
    /// inline tombstone into a silent no-op on a non-indexed table: the
    /// stale pair must be rejected, not reported as a deletion.
    #[tokio::test]
    async fn stale_inlined_row_flushed_before_commit_is_rejected() {
        let catalog = open_memory().await;
        let data = Arc::new(InMemory::new());
        let (table, ..) = table_with_files_and_inline_row(&catalog, &data).await;

        let located = catalog
            .locate_row_positions(None, "", table, &[(6, None)])
            .await
            .unwrap();
        assert!(located.deletions.is_empty(), "row 6 names no file");
        assert_eq!(
            located.inlined_rows,
            vec![6],
            "row 6 is located as live inlined"
        );

        // The flush lands and settles before `commit_located_deletion` is
        // ever called — no race, so the conflict matrix never sees it.
        let begin_snapshot = catalog
            .recent_row(table, 6)
            .await
            .unwrap()
            .unwrap()
            .begin_snapshot;
        let flushed_file = moraine::DataFile {
            path: "flushed.parquet".into(),
            record_count: 1,
            ..datafile(1)
        };
        catalog
            .commit(move |tx| {
                let flushed = moraine::FlushedDataFile {
                    file: flushed_file.clone(),
                    row_id_start: 6,
                    begin_snapshot,
                    partial_max: None,
                };
                tx.flush_inlined_data(table, 0, std::slice::from_ref(&flushed))
                    .map(|_| ())
            })
            .await
            .unwrap();
        assert!(
            catalog.recent_row(table, 6).await.unwrap().is_none(),
            "row 6 is no longer a live inlined row once flushed"
        );

        let before = catalog.snapshot().await.unwrap();
        let before_delete_count = before.delete_files_of(table).len();
        let before_file_count = before.data_files_of(table).len();

        let result = catalog
            .commit_located_deletion(None, "", table, &[], &located.inlined_rows)
            .await;

        assert!(
            matches!(
                result,
                Err(Error::RowPosition {
                    row_id: 6,
                    data_file_id: None,
                    ..
                })
            ),
            "a stale inlined pair must fail typed rather than silently no-op: {result:?}"
        );

        let after = catalog.snapshot().await.unwrap();
        assert_eq!(
            after.data_files_of(table).len(),
            before_file_count,
            "the rejected commit must not have minted a new snapshot"
        );
        assert_eq!(
            after.delete_files_of(table).len(),
            before_delete_count,
            "no delete file was registered against the flushed file"
        );
        catalog.close().await.unwrap();
    }
}
