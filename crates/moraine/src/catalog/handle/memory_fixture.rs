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

#[tokio::test]
#[ignore = "measures active-writer generations in MORAINE_MEMORY_FIXTURE"]
async fn measure_active_writer_reader_generations() {
    let root = std::env::var("MORAINE_MEMORY_FIXTURE").unwrap();
    std::fs::create_dir_all(&root).unwrap();
    let objects = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&root).unwrap());
    let options = CatalogOptions {
        cache_memory: Some(16 * 1024 * 1024),
        reader_poll_interval: std::time::Duration::from_mins(1),
        ..Default::default()
    };
    let writer = Catalog::open(objects.clone(), options.clone())
        .await
        .unwrap();
    let reader = Catalog::open_read_only(objects.clone(), options.clone())
        .await
        .unwrap();
    let mut scopes = Vec::new();
    memory_observation("baseline", 0, 0);
    for generation in 1..=8 {
        write_memory_generation(&writer, generation).await;
        let started = std::time::Instant::now();
        match reader.index_read_scope().await {
            Ok(Some(scope)) => scopes.push(scope),
            Ok(None) => panic!("fixed reader required"),
            Err(error) => {
                eprintln!("reader_refused={error}");
                memory_observation("refused", generation, started.elapsed().as_millis());
                break;
            }
        }
        memory_observation("wal", generation, started.elapsed().as_millis());
    }
    assert!(scopes.len() >= 2);
    drop(scopes);
    reader.close().await.unwrap();
    writer.close().await.unwrap();
    let started = std::time::Instant::now();
    let reader = Catalog::open_read_only(objects, options).await.unwrap();
    let scope = reader.index_read_scope().await.unwrap().unwrap();
    memory_observation("sst", 0, started.elapsed().as_millis());
    drop(scope);
    reader.close().await.unwrap();
}

fn memory_observation(phase: &str, generation: u64, open_ms: u128) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let rss = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .unwrap();
    let peak = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .unwrap();
    let (used, limit) = crate::working_memory_usage().unwrap();
    eprintln!(
        "phase={phase} generation={generation} open_ms={open_ms} working={used}/{limit} {rss} {peak}"
    );
}

async fn write_memory_generation(writer: &Catalog, generation: u64) {
    let batch = (0..384)
        .map(|offset| {
            let data_file_id = generation * 384 + offset;
            (
                Key::current(EntityKey::FileColumnStats {
                    table_id: 1,
                    data_file_id,
                    column_id: 1,
                })
                .encode(),
                Some(value::encode_value(&FileColumnStatsValue {
                    table_id: 1,
                    data_file_id,
                    column_id: 1,
                    min_value: Some("x".repeat(2048)),
                    ..Default::default()
                })),
            )
        })
        .collect();
    writer.commit_own_rows_durably(batch).await.unwrap();
}

#[tokio::test]
#[ignore = "checks reader rotation under a 32 MiB process working allowance"]
async fn unused_reader_generation_releases_replay_before_rotating() {
    let root = std::env::var("MORAINE_MEMORY_FIXTURE").unwrap();
    std::fs::create_dir_all(&root).unwrap();
    let objects = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root).unwrap());
    let options = CatalogOptions {
        cache_memory: Some(16 * 1024 * 1024),
        reader_poll_interval: std::time::Duration::from_mins(1),
        ..Default::default()
    };
    let writer = Catalog::open(objects.clone(), options.clone())
        .await
        .unwrap();
    let reader = Catalog::open_read_only(objects, options).await.unwrap();
    for generation in 1..=12 {
        write_memory_generation(&writer, generation).await;
        let started = std::time::Instant::now();
        let scope = reader.index_read_scope().await.unwrap().unwrap();
        memory_observation("sequential", generation, started.elapsed().as_millis());
        drop(scope);
    }
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}
