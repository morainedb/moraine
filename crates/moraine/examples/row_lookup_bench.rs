//! Fixed-hit row lookups with cold and warm caches. Arguments:
//! files|inline|recent, sources, and optional `reader` for a read-only handle.

use std::{
    alloc::System,
    cell::Cell,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use cpu_time::ProcessTime;
use moraine::{Catalog, CatalogOptions, ColumnDef, DataFile, DataStore, InlineChunk, TableId};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::ArrowWriter;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;
type BenchResult<T> = Result<T, Box<dyn std::error::Error>>;

#[allow(clippy::cast_precision_loss)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> BenchResult<()> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let mode = arguments.first().map_or("files", String::as_str);
    let count = arguments
        .get(1)
        .map_or(Ok(128), |value| value.parse::<usize>())?;
    if !["files", "inline", "recent"].contains(&mode) || count == 0 {
        return Err("expected files|inline|recent and a positive source count".into());
    }
    let metadata = Arc::new(InMemory::new());
    let data = Arc::new(InMemory::new());
    let mut options = CatalogOptions::default();
    options.flush_interval = Duration::ZERO;
    let catalog = Catalog::open(metadata.clone(), options.clone()).await?;
    let table = seed(&catalog, &data, mode, count).await?;
    catalog.close().await?;
    drop(catalog);
    let read_only = arguments
        .get(2)
        .is_some_and(|argument| argument == "reader");
    let catalog = if read_only {
        Catalog::open_read_only(metadata, options).await?
    } else {
        let writer = Catalog::open(metadata, options).await?;
        (*writer).clone()
    };
    let store = DataStore::new(data);
    let target = u64::try_from(count / 2)? * 128 + 1;
    println!(
        "mode,sources,rows,hits,cache,samples,cpu_us,latency_us,allocations,allocated_bytes,data_gets"
    );
    for (cache, samples) in [("cold", 1), ("warm", 32)] {
        let before = catalog.object_store_tally();
        let region = Region::new(ALLOCATOR);
        let cpu = ProcessTime::now();
        let started = Instant::now();
        for _ in 0..samples {
            if mode == "recent" {
                let row = catalog
                    .recent_row(table, target)
                    .await?
                    .ok_or("missing inline hit")?;
                if row.row_id != target {
                    return Err("wrong inline hit".into());
                }
            } else {
                let result = catalog
                    .locate_row_ids(
                        (mode == "files").then(|| store.clone()),
                        "",
                        table,
                        vec![target],
                    )
                    .await?;
                if result.len() != 1
                    || result[0].row_id != target
                    || result[0].data_file_id.is_some() != (mode == "files")
                {
                    return Err("wrong located hit".into());
                }
            }
        }
        let elapsed = started.elapsed();
        let cpu = cpu.elapsed();
        let stats = region.change();
        let after = catalog.object_store_tally();
        let divisor = f64::from(samples);
        println!(
            "{mode},{count},{},1,{cache},{samples},{:.3},{:.3},{:.1},{:.1},{:.1}",
            count * 128,
            cpu.as_secs_f64() * 1e6 / divisor,
            elapsed.as_secs_f64() * 1e6 / divisor,
            stats.allocations as f64 / divisor,
            stats.bytes_allocated as f64 / divisor,
            (after.data_gets - before.data_gets) as f64 / divisor
        );
    }
    catalog.close().await?;
    Ok(())
}

async fn seed(
    catalog: &Catalog,
    data: &InMemory,
    mode: &str,
    count: usize,
) -> BenchResult<TableId> {
    let mut encoded = Vec::new();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1; 128]))])?;
    let mut writer = ArrowWriter::try_new(&mut encoded, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    let footer = u32::from_le_bytes(encoded[encoded.len() - 8..encoded.len() - 4].try_into()?);
    if mode == "files" {
        for source in 0..count {
            data.put(
                &Path::from(format!("main/lookup/file-{source}.parquet")),
                encoded.clone().into(),
            )
            .await?;
        }
    }
    let table = Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx
                .schema_by_name("main")
                .ok_or_else(|| moraine::Error::NotFound("main".into()))?
                .id;
            let id = tx.create_table(
                schema,
                "lookup",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    ..Default::default()
                }],
            )?;
            table.set(Some(id));
            for source in 0..count {
                if mode == "files" {
                    tx.register_data_file(
                        id,
                        DataFile {
                            path: format!("file-{source}.parquet"),
                            path_is_relative: true,
                            file_format: "parquet".into(),
                            record_count: 128,
                            file_size_bytes: encoded.len() as u64,
                            footer_size: u64::from(footer),
                            encryption_key: None,
                            partition_values: Vec::new(),
                            column_stats: Vec::new(),
                        },
                        &[],
                    )?;
                } else {
                    tx.inline_insert(
                        id,
                        &InlineChunk {
                            schema_version: 0,
                            row_count: 128,
                            arrow_schema: b"opaque schema".to_vec(),
                            arrow_body: vec![0; 1024],
                        },
                        &[],
                    )?;
                }
            }
            Ok(())
        })
        .await?;
    table.get().ok_or_else(|| "missing table".into())
}
