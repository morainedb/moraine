//! Reproducible persisted metadata fixture for fresh-process memory
//! measurements.

use std::{cell::Cell, sync::Arc};

use crate::{
    Catalog, CatalogOptions, ColumnDef,
    store::{
        key::{EntityKey, Key},
        proto::{DataFileValue, FileColumnStatsValue},
        value,
    },
};

#[tokio::test]
#[ignore = "writes the catalog named by MORAINE_MEMORY_FIXTURE"]
async fn write_memory_measurement_fixture() {
    let root = std::env::var("MORAINE_MEMORY_FIXTURE").unwrap();
    let files: u64 = std::env::var("MORAINE_MEMORY_FILES")
        .unwrap()
        .parse()
        .unwrap();
    std::fs::create_dir_all(&root).unwrap();
    let store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&root).unwrap());
    let catalog = Catalog::open(
        store,
        CatalogOptions {
            cache_memory: Some(16 * 1024 * 1024),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let table = Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            let columns: Vec<_> = (0..20)
                .map(|column| ColumnDef {
                    name: format!("column_{column}"),
                    column_type: "VARCHAR".into(),
                    nulls_allowed: true,
                    default_value: None,
                    children: Vec::new(),
                })
                .collect();
            table.set(Some(tx.create_table(schema, "measurement", &columns)?));
            Ok(())
        })
        .await
        .unwrap();
    let table_id = table.get().unwrap().get();
    let snapshot = catalog.snapshot().await.unwrap();
    let columns: Vec<_> = snapshot
        .columns_of(table.get().unwrap())
        .into_iter()
        .map(|column| column.id.get())
        .collect();
    let begin_snapshot = snapshot.current_snapshot().id.get();
    drop(snapshot);
    for first in (1..=files).step_by(128) {
        let tx = catalog.begin_write_tx().unwrap();
        for data_file_id in first..(first + 128).min(files + 1) {
            let file = DataFileValue {
                table_id,
                data_file_id,
                begin_snapshot,
                path: format!("file_{data_file_id:08}.parquet"),
                path_is_relative: true,
                file_format: "parquet".into(),
                record_count: 1000,
                file_size_bytes: 1024 * 1024,
                footer_size: 1024,
                row_id_start: Some((data_file_id - 1) * 1000),
                ..Default::default()
            };
            tx.put(
                Key::current(EntityKey::File {
                    table_id,
                    data_file_id,
                })
                .encode(),
                value::encode_value(&file),
            )
            .unwrap();
            for &column_id in &columns {
                let stats = FileColumnStatsValue {
                    table_id,
                    data_file_id,
                    column_id,
                    column_size_bytes: 4096,
                    value_count: Some(1000),
                    null_count: Some(0),
                    min_value: Some(format!("{data_file_id:096x}")),
                    max_value: Some(format!("{data_file_id:096x}z")),
                    ..Default::default()
                };
                tx.put(
                    Key::current(EntityKey::FileColumnStats {
                        table_id,
                        data_file_id,
                        column_id,
                    })
                    .encode(),
                    value::encode_value(&stats),
                )
                .unwrap();
            }
        }
        catalog.commit_write_tx(tx).await.unwrap();
    }
    catalog.close().await.unwrap();
    eprintln!("fixture={root} files={files} statistics={}", files * 20);
}

#[tokio::test]
#[ignore = "reads MORAINE_MEMORY_FIXTURE with a small working-memory allowance"]
async fn deep_preload_uses_bounded_working_memory() {
    let root = std::env::var("MORAINE_MEMORY_FIXTURE").unwrap();
    let objects = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root).unwrap());
    let reader = Catalog::open_read_only(
        objects,
        CatalogOptions {
            cache_memory: Some(16 * 1024 * 1024),
            cache_preload: Some(crate::CachePreload::All),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(reader.cache_tally().preload_failures, 0);
    reader.close().await.unwrap();
}
