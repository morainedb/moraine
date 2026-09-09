//! `ReadOnlyCatalog::rows_at`: located rows read back at a snapshot.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Int64Type, Schema, UInt64Type},
    ipc::reader::StreamReader,
};
use moraine::{
    Catalog, CatalogSnapshot, DataFile, DataFileId, DataStore, DeleteFile, Error, InlineChunk,
    TableId,
};
use object_store::memory::InMemory;

use crate::fixtures::{col, datafile, open_memory, row_id_field, write_parquet};

/// Decodes one returned IPC stream into its single batch.
#[allow(clippy::unwrap_used)]
fn decode(ipc: &[u8]) -> RecordBatch {
    let mut batches: Vec<RecordBatch> = StreamReader::try_new(std::io::Cursor::new(ipc), None)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(batches.len(), 1, "one batch per stream");
    batches.pop().unwrap()
}

/// Every row of every returned batch as `(a, row_id, data_file_id)`, in
/// row-id order.
#[allow(clippy::expect_used)]
fn rows(located: &moraine::LocatedRows) -> Vec<(Option<i64>, u64, Option<u64>)> {
    let mut rows = Vec::new();
    for batch in &located.batches {
        let batch = decode(batch);
        let a = batch
            .column_by_name("a")
            .expect("column a")
            .as_primitive::<Int64Type>();
        let row_id = batch
            .column_by_name("row_id")
            .expect("row_id column")
            .as_primitive::<UInt64Type>();
        let file = batch
            .column_by_name("data_file_id")
            .expect("data_file_id column")
            .as_primitive::<UInt64Type>();
        for i in 0..batch.num_rows() {
            rows.push((
                (!a.is_null(i)).then(|| a.value(i)),
                row_id.value(i),
                (!file.is_null(i)).then(|| file.value(i)),
            ));
        }
    }
    rows.sort_by_key(|row| row.1);
    rows
}

#[allow(clippy::unwrap_used)]
fn dense_batch(a: &[i64], b: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(a.to_vec())),
            Arc::new(Int64Array::from(b.to_vec())),
        ],
    )
    .unwrap()
}

#[allow(clippy::unwrap_used)]
fn batch_with_row_ids(a: &[i64], row_ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        row_id_field(),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(a.to_vec())),
            Arc::new(Int64Array::from(row_ids.to_vec())),
        ],
    )
    .unwrap()
}

/// A DuckLake-shaped delete file naming `positions`.
#[allow(clippy::unwrap_used)]
fn delete_batch(positions: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("pos", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["data.parquet"; positions.len()])) as ArrayRef,
            Arc::new(Int64Array::from(positions.to_vec())),
        ],
    )
    .unwrap()
}

/// A replacement delete file as DuckLake writes one: each position tagged
/// with the snapshot that deleted it.
#[allow(clippy::unwrap_used)]
fn delete_batch_with_snapshots(positions: &[(i64, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("pos", DataType::Int64, false),
        Field::new("_ducklake_internal_snapshot_id", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["data.parquet"; positions.len()])) as ArrayRef,
            Arc::new(Int64Array::from(
                positions
                    .iter()
                    .map(|(position, _)| *position)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                positions
                    .iter()
                    .map(|(_, snapshot)| *snapshot)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// An inline chunk of `a` values, encoded as the extension encodes one.
#[allow(clippy::unwrap_used)]
fn inline_chunk(values: &[i64]) -> InlineChunk {
    use arrow::ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, StreamWriter,
    };

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap();
    let mut arrow_schema = Vec::new();
    StreamWriter::try_new(&mut arrow_schema, &schema)
        .unwrap()
        .finish()
        .unwrap();
    let (_, encoded) = IpcDataGenerator::default()
        .encode(
            &batch,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
            &mut IpcWriteContext::default(),
        )
        .unwrap();
    let mut arrow_body = u32::try_from(encoded.ipc_message.len())
        .unwrap()
        .to_le_bytes()
        .to_vec();
    arrow_body.extend(encoded.ipc_message);
    arrow_body.extend(encoded.arrow_data);
    InlineChunk {
        schema_version: 0,
        arrow_schema,
        arrow_body,
        row_count: u64::try_from(values.len()).unwrap(),
    }
}

/// Creates table `orders` with `columns` and registers `files` against it.
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn table_with(catalog: &Catalog, columns: &[&str], files: Vec<DataFile>) -> TableId {
    let created = std::cell::Cell::new(None);
    let defs: Vec<_> = columns.iter().map(|name| col(name)).collect();
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            tx.set_option(moraine::OptionScope::Global, "data_path", "")?;
            let table = tx.create_table(schema, "orders", &defs)?;
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

#[allow(clippy::unwrap_used)]
async fn only_file(catalog: &Catalog, table: TableId) -> DataFileId {
    catalog.snapshot().await.unwrap().data_files_of(table)[0].id
}

#[allow(clippy::unwrap_used)]
async fn head(catalog: &Catalog) -> Arc<CatalogSnapshot> {
    catalog.snapshot().await.unwrap()
}

#[tokio::test]
async fn file_rows_come_back_with_the_current_columns_and_their_ids() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let (file_size_bytes, footer_size) = write_parquet(
        &data,
        "main/orders/data-3.parquet",
        &dense_batch(&[10, 20, 30], &[1, 2, 3]),
    )
    .await;
    let table = table_with(
        &catalog,
        &["a", "b"],
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;
    let file = only_file(&catalog, table).await;

    let snapshot = head(&catalog).await;
    let located = catalog
        .rows_at(
            &snapshot,
            Some(DataStore::new(data)),
            "",
            table,
            &[(2, Some(file)), (1, Some(file)), (1, Some(file))],
        )
        .await
        .unwrap();

    let batch = decode(&located.batches[0]);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>(),
        ["a", "b", "row_id", "data_file_id"]
    );
    assert_eq!(
        batch
            .column_by_name("b")
            .unwrap()
            .as_primitive::<Int64Type>()
            .values()
            .to_vec(),
        [2, 3]
    );
    assert_eq!(
        rows(&located),
        vec![
            (Some(20), 1, Some(file.get())),
            (Some(30), 2, Some(file.get()))
        ]
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn a_file_with_embedded_ids_is_read_at_its_exact_positions() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let (file_size_bytes, footer_size) = write_parquet(
        &data,
        "main/orders/data-3.parquet",
        &batch_with_row_ids(&[10, 20, 30], &[5, 9, 12]),
    )
    .await;
    let table = table_with(
        &catalog,
        &["a"],
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;
    let file = only_file(&catalog, table).await;

    let snapshot = head(&catalog).await;
    let located = catalog
        .rows_at(
            &snapshot,
            Some(DataStore::new(data)),
            "",
            table,
            &[(9, Some(file)), (12, Some(file))],
        )
        .await
        .unwrap();

    assert_eq!(
        rows(&located),
        vec![
            (Some(20), 9, Some(file.get())),
            (Some(30), 12, Some(file.get()))
        ]
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn rows_deleted_at_the_snapshot_are_omitted() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let (file_size_bytes, footer_size) = write_parquet(
        &data,
        "main/orders/data-3.parquet",
        &dense_batch(&[10, 20, 30], &[1, 2, 3]),
    )
    .await;
    let table = table_with(
        &catalog,
        &["a", "b"],
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;
    let file = only_file(&catalog, table).await;
    let (delete_size, delete_footer) =
        write_parquet(&data, "main/orders/delete.parquet", &delete_batch(&[1])).await;
    catalog
        .commit(|tx| {
            tx.register_delete_file(
                table,
                DeleteFile {
                    data_file_id: file,
                    path: "delete.parquet".into(),
                    path_is_relative: true,
                    format: "parquet".into(),
                    delete_count: 1,
                    file_size_bytes: delete_size,
                    footer_size: delete_footer,
                    encryption_key: None,
                },
                &[],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let snapshot = head(&catalog).await;
    let located = catalog
        .rows_at(
            &snapshot,
            Some(DataStore::new(data)),
            "",
            table,
            &[(0, Some(file)), (1, Some(file)), (2, Some(file))],
        )
        .await
        .unwrap();

    assert_eq!(
        rows(&located),
        vec![
            (Some(10), 0, Some(file.get())),
            (Some(30), 2, Some(file.get()))
        ]
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn inlined_rows_decode_from_their_chunk() {
    let catalog = open_memory().await;
    let table = table_with(&catalog, &["a"], vec![]).await;
    catalog
        .commit(|tx| {
            tx.inline_insert(table, &inline_chunk(&[7, 8, 9]), &[])
                .map(|_| ())
        })
        .await
        .unwrap();

    let snapshot = head(&catalog).await;
    let located = catalog
        .rows_at(&snapshot, None, "", table, &[(1, None), (2, None)])
        .await
        .unwrap();

    assert_eq!(rows(&located), vec![(Some(8), 1, None), (Some(9), 2, None)]);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn an_inlined_row_deleted_at_the_snapshot_is_refused() {
    let catalog = open_memory().await;
    let table = table_with(&catalog, &["a"], vec![]).await;
    catalog
        .commit(|tx| {
            tx.inline_insert(table, &inline_chunk(&[7, 8]), &[])
                .map(|_| ())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.inline_delete(table, 1, &[]))
        .await
        .unwrap();

    let snapshot = head(&catalog).await;
    let error = catalog
        .rows_at(&snapshot, None, "", table, &[(1, None)])
        .await
        .unwrap_err();

    assert!(
        matches!(
            error,
            Error::RowPosition {
                row_id: 1,
                data_file_id: None,
                ..
            }
        ),
        "{error}"
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn a_column_added_after_a_file_was_written_reads_as_null() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let (file_size_bytes, footer_size) = write_parquet(
        &data,
        "main/orders/data-3.parquet",
        &batch_with_row_ids(&[10, 20, 30], &[0, 1, 2]),
    )
    .await;
    let table = table_with(
        &catalog,
        &["a"],
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;
    let file = only_file(&catalog, table).await;
    catalog
        .commit(|tx| tx.add_column(table, &col("b")).map(|_| ()))
        .await
        .unwrap();

    let snapshot = head(&catalog).await;
    let located = catalog
        .rows_at(
            &snapshot,
            Some(DataStore::new(data)),
            "",
            table,
            &[(1, Some(file))],
        )
        .await
        .unwrap();

    let batch = decode(&located.batches[0]);
    let b = batch.column_by_name("b").unwrap();
    assert_eq!(b.data_type(), &DataType::Int64);
    assert!(b.is_null(0));
    assert_eq!(rows(&located), vec![(Some(20), 1, Some(file.get()))]);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn an_older_snapshot_still_serves_a_row_deleted_later() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let (file_size_bytes, footer_size) = write_parquet(
        &data,
        "main/orders/data-3.parquet",
        &dense_batch(&[10, 20, 30], &[1, 2, 3]),
    )
    .await;
    let table = table_with(
        &catalog,
        &["a", "b"],
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;
    let file = only_file(&catalog, table).await;
    let pinned = head(&catalog).await;
    let (delete_size, delete_footer) =
        write_parquet(&data, "main/orders/delete.parquet", &delete_batch(&[0])).await;
    catalog
        .commit(|tx| {
            tx.register_delete_file(
                table,
                DeleteFile {
                    data_file_id: file,
                    path: "delete.parquet".into(),
                    path_is_relative: true,
                    format: "parquet".into(),
                    delete_count: 1,
                    file_size_bytes: delete_size,
                    footer_size: delete_footer,
                    encryption_key: None,
                },
                &[],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let at_pinned = catalog
        .rows_at(&pinned, Some(store.clone()), "", table, &[(0, Some(file))])
        .await
        .unwrap();
    let current = head(&catalog).await;
    let at_head = catalog
        .rows_at(&current, Some(store.clone()), "", table, &[(0, Some(file))])
        .await
        .unwrap();
    let positions_pinned = catalog
        .locate_row_positions_at(&pinned, Some(store.clone()), "", table, &[(0, Some(file))])
        .await
        .unwrap();
    let positions_head = catalog
        .locate_row_positions(Some(store), "", table, &[(0, Some(file))])
        .await
        .unwrap();

    assert_eq!(rows(&at_pinned), vec![(Some(10), 0, Some(file.get()))]);
    assert!(rows(&at_head).is_empty());
    assert!(positions_pinned.deletions[0].existing_delete.is_none());
    assert_eq!(
        positions_head.deletions[0]
            .existing_delete
            .as_ref()
            .map(|existing| existing.positions.clone()),
        Some(vec![0])
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn a_file_the_snapshot_does_not_hold_is_refused() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let store = DataStore::new(data.clone());
    let (size_a, footer_a) = write_parquet(
        &data,
        "main/orders/data-3.parquet",
        &dense_batch(&[10, 20, 30], &[1, 2, 3]),
    )
    .await;
    let table = table_with(
        &catalog,
        &["a", "b"],
        vec![DataFile {
            file_size_bytes: size_a,
            footer_size: footer_a,
            ..datafile(3)
        }],
    )
    .await;
    let pinned = head(&catalog).await;
    let (size_b, footer_b) = write_parquet(
        &data,
        "main/orders/data-2.parquet",
        &dense_batch(&[40, 50], &[4, 5]),
    )
    .await;
    catalog
        .commit(|tx| {
            tx.register_data_file(
                table,
                DataFile {
                    file_size_bytes: size_b,
                    footer_size: footer_b,
                    ..datafile(2)
                },
                &[],
            )
            .map(|_| ())
        })
        .await
        .unwrap();
    let later = catalog
        .snapshot()
        .await
        .unwrap()
        .data_files_of(table)
        .into_iter()
        .find(|file| file.record_count == 2)
        .unwrap()
        .id;

    let error = catalog
        .rows_at(&pinned, Some(store), "", table, &[(3, Some(later))])
        .await
        .unwrap_err();

    assert!(
        matches!(error, Error::RowPosition { row_id: 3, data_file_id: Some(id), .. } if id == later),
        "{error}"
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn a_row_the_named_file_does_not_hold_is_refused() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let (file_size_bytes, footer_size) = write_parquet(
        &data,
        "main/orders/data-3.parquet",
        &dense_batch(&[10, 20, 30], &[1, 2, 3]),
    )
    .await;
    let table = table_with(
        &catalog,
        &["a", "b"],
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;
    let file = only_file(&catalog, table).await;

    let snapshot = head(&catalog).await;
    let error = catalog
        .rows_at(
            &snapshot,
            Some(DataStore::new(data)),
            "",
            table,
            &[(7, Some(file))],
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, Error::RowPosition { row_id: 7, .. }),
        "{error}"
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn a_position_a_replacement_delete_file_deletes_later_is_still_served() {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let (file_size_bytes, footer_size) = write_parquet(
        &data,
        "main/orders/data-3.parquet",
        &dense_batch(&[10, 20, 30], &[1, 2, 3]),
    )
    .await;
    let table = table_with(
        &catalog,
        &["a", "b"],
        vec![DataFile {
            file_size_bytes,
            footer_size,
            ..datafile(3)
        }],
    )
    .await;
    let file = only_file(&catalog, table).await;
    // Position 0 was deleted at snapshot 1; position 1 will be, at a
    // snapshot far beyond the head this test reads at.
    let (delete_size, delete_footer) = write_parquet(
        &data,
        "main/orders/delete.parquet",
        &delete_batch_with_snapshots(&[(0, 1), (1, 1_000_000)]),
    )
    .await;
    catalog
        .commit(|tx| {
            tx.register_delete_file(
                table,
                DeleteFile {
                    data_file_id: file,
                    path: "delete.parquet".into(),
                    path_is_relative: true,
                    format: "parquet".into(),
                    delete_count: 2,
                    file_size_bytes: delete_size,
                    footer_size: delete_footer,
                    encryption_key: None,
                },
                &[],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let snapshot = head(&catalog).await;
    let located = catalog
        .rows_at(
            &snapshot,
            Some(DataStore::new(data)),
            "",
            table,
            &[(0, Some(file)), (1, Some(file)), (2, Some(file))],
        )
        .await
        .unwrap();

    assert_eq!(
        rows(&located),
        vec![
            (Some(20), 1, Some(file.get())),
            (Some(30), 2, Some(file.get()))
        ]
    );
    catalog.close().await.unwrap();
}
