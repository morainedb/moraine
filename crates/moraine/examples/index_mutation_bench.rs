//! Verified index appends at a fixed batch size. Arguments: existing rows,
//! batch.

use std::{
    alloc::System,
    cell::Cell,
    ops::Bound,
    sync::Arc,
    time::{Duration, Instant},
};

use cpu_time::ProcessTime;
use moraine::{
    Catalog, CatalogOptions, ColumnDef, DataFile, FileIndexEntry, IndexDef, IndexId, IndexKeyValue,
    IntWidth,
};
use object_store::memory::InMemory;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn value(value: u64) -> IndexKeyValue {
    IndexKeyValue::Int {
        value: i128::from(value),
        width: IntWidth::I64,
    }
}

fn entries(index: IndexId, first: u64, count: u64) -> Vec<FileIndexEntry> {
    (0..count)
        .map(|ordinal| FileIndexEntry {
            index,
            ordinal,
            values: vec![Some(value(first + ordinal))],
        })
        .collect()
}

fn file(first: u64, count: u64) -> DataFile {
    DataFile {
        path: format!("{first}.parquet"),
        path_is_relative: true,
        file_format: "parquet".into(),
        record_count: count,
        file_size_bytes: count * 8,
        footer_size: 32,
        encryption_key: None,
        partition_values: Vec::new(),
        column_stats: Vec::new(),
    }
}

#[allow(clippy::cast_precision_loss)]
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let existing: u64 = arguments.first().map_or(Ok(1024), |value| value.parse())?;
    let batch: u64 = arguments.get(1).map_or(Ok(64), |value| value.parse())?;
    if existing == 0 || batch == 0 {
        return Err("counts must be positive".into());
    }
    let mut options = CatalogOptions::default();
    options.flush_interval = Duration::ZERO;
    let catalog = Catalog::open(Arc::new(InMemory::new()), options).await?;
    let ids = Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            let table = tx.create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    ..ColumnDef::default()
                }],
            )?;
            let index = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![tx.columns_of(table)[0].id],
                    unique: true,
                },
                &[],
            )?;
            tx.register_data_file(table, file(0, existing), &entries(index, 0, existing))?;
            ids.set(Some((table, index)));
            Ok(())
        })
        .await?;
    let (table, index) = ids.get().ok_or("missing fixture")?;
    let mut elapsed = Duration::ZERO;
    let mut cpu = Duration::ZERO;
    let mut allocations = 0;
    let mut bytes = 0;
    let mut put_time = Duration::ZERO;
    for sample in 0..23 {
        let first = existing + sample * batch;
        let entries = entries(index, first, batch);
        let file = file(first, batch);
        let tally = catalog.object_store_tally();
        let region = Region::new(ALLOCATOR);
        let started_cpu = ProcessTime::now();
        let started = Instant::now();
        catalog
            .commit(|tx| {
                tx.register_data_file(table, file.clone(), &entries)
                    .map(|_| ())
            })
            .await?;
        let wall = started.elapsed();
        let used_cpu = started_cpu.elapsed();
        let stats = region.change();
        if sample >= 3 {
            elapsed += wall;
            cpu += used_cpu;
            allocations += stats.allocations + stats.reallocations;
            bytes += stats.bytes_allocated;
            put_time += catalog
                .object_store_tally()
                .main_put_duration
                .saturating_sub(tally.main_put_duration);
        }
        let found = catalog
            .index_range(
                table,
                index,
                Bound::Included(vec![value(first)]),
                Bound::Excluded(vec![value(first + batch)]),
                false,
            )
            .await?;
        if !found.into_iter().eq(first..first + batch) {
            return Err("incorrect appended index entries".into());
        }
    }
    println!("existing,batch,samples,total_us,cpu_us,allocations,allocated_bytes,put_us");
    println!(
        "{existing},{batch},20,{:.3},{:.3},{:.1},{:.1},{:.3}",
        elapsed.as_secs_f64() * 1e6 / 20.0,
        cpu.as_secs_f64() * 1e6 / 20.0,
        allocations as f64 / 20.0,
        bytes as f64 / 20.0,
        put_time.as_secs_f64() * 1e6 / 20.0
    );
    catalog.close().await?;
    Ok(())
}
