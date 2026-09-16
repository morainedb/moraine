use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array},
    record_batch::RecordBatch,
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::ArrowWriter;

use super::LocatedRowScan;
use crate::{
    Catalog, CatalogOptions, ColumnDef, DataFile, DataStore, DeleteFile, ExcludedPositions,
    OptionScope, data_file,
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

/// Registers table `t` over `data.parquet` (rows 10, 20, 30) whose position 1
/// is deleted by `delete.parquet`; returns the delete file's size and footer.
async fn table_with_one_deleted_row(catalog: &Catalog, data: &InMemory) -> (u64, u64) {
    let (size, footer) = parquet(data, "data.parquet", "a", vec![10, 20, 30]).await;
    let (delete_size, delete_footer) = parquet(data, "delete.parquet", "pos", vec![1]).await;
    catalog
        .commit(|tx| {
            tx.set_option(OptionScope::Global, "data_path", "/lake/")?;
            let schema = tx.schema_by_name("main").unwrap().id;
            let table = tx.create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    ..Default::default()
                }],
            )?;
            let file = tx.register_data_file(
                table,
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
                table,
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
            Ok(())
        })
        .await
        .unwrap();
    (delete_size, delete_footer)
}

async fn first_column_values(mut scan: LocatedRowScan) -> Vec<i64> {
    let mut values = Vec::new();
    while let Some(batch) = scan.next_record_batch().await.unwrap() {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        values.extend(column.values().iter().copied());
    }
    values
}

/// Scan setup must not fill the deletion-merging cache with unused positions.
#[tokio::test]
async fn scan_positions_do_not_materialize_unfiltered_deletes() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let data = Arc::new(InMemory::new());
    let (delete_size, delete_footer) = table_with_one_deleted_row(&catalog, &data).await;
    let snapshot = catalog.snapshot().await.unwrap();
    let schema = snapshot.schema_by_name("main").unwrap().id;
    let table = snapshot.table_by_name(schema, "t").unwrap().id;
    let file = snapshot.data_files_of(table)[0].id;
    let store = DataStore::new(data);
    let pairs = [(0, Some(file)), (1, Some(file)), (2, Some(file))];

    for strict in [false, true] {
        let scan = if strict {
            catalog
                .scan_rows_at_strict(&snapshot, Some(store.clone()), "", table, &pairs)
                .await
        } else {
            catalog
                .scan_rows_at(
                    &snapshot,
                    Some(store.clone()),
                    "",
                    table,
                    &pairs,
                    &["a".into()],
                )
                .await
        }
        .unwrap();
        assert_eq!(first_column_values(scan).await, vec![10, 30]);
    }

    let metrics = Arc::new(data_file::ScopedReadMetrics::default());
    let delete_file = data_file::ParquetFile::new(
        store.clone(),
        Path::from("delete.parquet"),
        delete_size,
        delete_footer,
    )
    .with_metrics(metrics.clone());
    let positions = data_file::delete_file_positions(delete_file).await.unwrap();
    assert_eq!(positions, vec![1]);
    assert_eq!(
        metrics.tally().parquet_files,
        1,
        "scans must leave the unfiltered deletion cache cold"
    );

    let located = catalog
        .locate_row_positions(Some(store), "", table, &pairs)
        .await
        .unwrap();
    let existing = located.deletions[0].existing_delete.as_ref().unwrap();
    assert_eq!(existing.positions, vec![1]);
    catalog.close().await.unwrap();
}

/// Excluded positions join the snapshot's deletions without touching it.
#[tokio::test]
async fn excluded_positions_are_omitted_like_deletions() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let data = Arc::new(InMemory::new());
    table_with_one_deleted_row(&catalog, &data).await;
    let snapshot = catalog.snapshot().await.unwrap();
    let schema = snapshot.schema_by_name("main").unwrap().id;
    let table = snapshot.table_by_name(schema, "t").unwrap().id;
    let file = snapshot.data_files_of(table)[0].id;
    let store = DataStore::new(data);
    let pairs = [(0, Some(file)), (1, Some(file)), (2, Some(file))];
    let excluded = [ExcludedPositions {
        data_file_id: file,
        positions: vec![2, 2],
    }];

    let scan = catalog
        .scan_rows_at_excluding(
            &snapshot,
            Some(store.clone()),
            "",
            table,
            &pairs,
            &["a".into()],
            &excluded,
        )
        .await
        .unwrap();
    assert_eq!(first_column_values(scan).await, vec![10]);

    let scan = catalog
        .scan_rows_at(&snapshot, Some(store), "", table, &pairs, &["a".into()])
        .await
        .unwrap();
    assert_eq!(first_column_values(scan).await, vec![10, 30]);
    catalog.close().await.unwrap();
}
