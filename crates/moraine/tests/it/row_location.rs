//! `ReadOnlyCatalog::locate_row_ids`: which current files hold a row id.

use std::sync::Arc;

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use moraine::{Catalog, DataFile, DataFileId, DataStore, DeleteFile, Error, InlineChunk, TableId};
use object_store::{ObjectStore, memory::InMemory};

use crate::fixtures::{col, datafile, open_memory, row_id_field, write_parquet as write};

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
        .publish_row_summaries(store.clone(), "", table)
        .await
        .unwrap();
    assert_eq!(first.files_considered, 1);
    assert_eq!(first.summaries_built, 1);
    assert_eq!(first.files_failed, 0);

    // Idempotent: the summary is now resident, so a second pass reads
    // nothing.
    let second = catalog
        .publish_row_summaries(store.clone(), "", table)
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

    let warmth = catalog
        .publish_row_summaries(store, "", table)
        .await
        .unwrap();

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

    let warmth = catalog
        .publish_row_summaries(store, "", table)
        .await
        .unwrap();

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

    let warmth = catalog.publish_all_row_summaries(store, "").await.unwrap();

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

    let warmth = catalog.publish_all_row_summaries(store, "").await.unwrap();

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

/// A cold lookup pays one read per file with embedded ids — a small file
/// is fetched whole, footer and row-id column together — and the handle's
/// tally shows it; a warm repeat reads nothing.
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
        3,
        "one whole-object read per small file"
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

    /// Counts the events the positioning memo emits when it is reused, so a
    /// test can tell a second positioning from a shared one.
    #[derive(Clone, Default)]
    struct ReuseEvents(Arc<std::sync::Mutex<usize>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ReuseEvents {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            #[derive(Default)]
            struct Message(Option<String>);

            impl tracing::field::Visit for Message {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = Some(format!("{value:?}"));
                    }
                }
            }

            let mut message = Message::default();
            event.record(&mut message);
            if message
                .0
                .is_some_and(|text| text.contains("positioned files reused"))
            {
                *self
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
            }
        }
    }

    /// Repeated resolution at one pinned revision reuses positions.
    #[tokio::test]
    #[allow(clippy::unwrap_used, clippy::expect_used)]
    async fn repeated_resolution_positions_the_same_rows_once() {
        use tracing_subscriber::prelude::*;

        let reuses = ReuseEvents::default();
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(reuses.clone()));

        let catalog = open_memory().await;
        let data = Arc::new(InMemory::new());
        let store = DataStore::new(data.clone());
        let (table, dense_id, _) = fixture(&catalog, &data).await;

        let scope = catalog
            .index_read_scope()
            .await
            .unwrap()
            .expect("a writer holds the store, so the scope pins a revision");
        let reads = scope.reads();
        let snapshot = reads.snapshot().await.unwrap();
        let pairs = [(1, Some(dense_id)), (2, Some(dense_id))];

        // Resolve the pairs for the first time.
        reads
            .locate_row_positions_at(&snapshot, Some(store.clone()), "", table, &pairs)
            .await
            .unwrap();
        assert_eq!(
            *reuses.0.lock().unwrap(),
            0,
            "the first resolution has nothing to reuse"
        );

        // Resolve the same pairs again.
        reads
            .locate_row_positions_at(&snapshot, Some(store), "", table, &pairs)
            .await
            .unwrap();
        assert_eq!(
            *reuses.0.lock().unwrap(),
            1,
            "the second resolution reuses the positions"
        );

        drop(scope);
        catalog.close().await.unwrap();
    }

    /// Positioning a row against its file counts against the data store,
    /// which the catalog's own request counters do not reach.
    #[tokio::test]
    async fn positioning_a_row_counts_the_data_store_it_read() {
        let catalog = open_memory().await;
        let data = Arc::new(InMemory::new());
        let store = DataStore::new(data.clone());
        let (table, dense_id, _) = fixture(&catalog, &data).await;

        let before = catalog.object_store_tally();
        assert_eq!(before.data_gets, 0, "nothing has read a data file yet");

        catalog
            .locate_row_positions(Some(store), "", table, &[(1, Some(dense_id))])
            .await
            .unwrap();

        let after = catalog.object_store_tally();
        assert!(
            after.data_gets > before.data_gets,
            "positioning read no data file ({} -> {})",
            before.data_gets,
            after.data_gets
        );
        assert!(
            after.data_bytes >= after.data_gets,
            "a read returned fewer bytes than requests"
        );
        catalog.close().await.unwrap();
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
        let dense = located
            .deletions
            .iter()
            .find(|deletion| deletion.data_file_id == dense_id)
            .unwrap();
        assert_eq!(dense.positions, vec![1]);
        assert!(dense.existing_positions.is_none());
        let rewrite = located
            .deletions
            .iter()
            .find(|deletion| deletion.data_file_id == rewrite_id)
            .unwrap();
        assert_eq!(rewrite.positions, vec![4]);
        assert!(rewrite.existing_positions.is_none());
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
    async fn an_existing_delete_file_surfaces_its_positions() {
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
        let existing = located.deletions[0].existing_positions.as_ref().unwrap();
        assert_eq!(existing, &vec![0]);
        catalog.close().await.unwrap();
    }
}
