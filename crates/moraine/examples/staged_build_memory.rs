//! Whole-operation peak heap for inline staged builds over a persisted store.
//! Arguments: chunks, rows per chunk, step entries, indexed string bytes.

use std::{
    cell::Cell,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, StreamWriter,
    },
};
use moraine::{BuildStep, Catalog, CatalogOptions, ColumnDef, IndexDef, InlineChunk};
use object_store::local::LocalFileSystem;

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

type BenchResult<T> = Result<T, Box<dyn std::error::Error>>;

fn argument(arguments: &[String], position: usize, default: usize) -> BenchResult<usize> {
    arguments
        .get(position)
        .map_or(Ok(default), |value| value.parse().map_err(Into::into))
}

fn chunk(first: usize, rows: usize, width: usize) -> BenchResult<InlineChunk> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
    let values: Vec<_> = (first..first + rows)
        .map(|row| format!("{row:0width$}"))
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))])?;
    let mut arrow_schema = Vec::new();
    StreamWriter::try_new(&mut arrow_schema, &schema)?.finish()?;
    let (_, encoded) = IpcDataGenerator::default().encode(
        &batch,
        &mut DictionaryTracker::new(false),
        &IpcWriteOptions::default(),
        &mut IpcWriteContext::default(),
    )?;
    let mut arrow_body = u32::try_from(encoded.ipc_message.len())?
        .to_le_bytes()
        .to_vec();
    arrow_body.extend(encoded.ipc_message);
    arrow_body.extend(encoded.arrow_data);
    Ok(InlineChunk {
        schema_version: 0,
        row_count: u64::try_from(rows)?,
        arrow_schema,
        arrow_body,
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> BenchResult<()> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let chunks = argument(&arguments, 0, 16)?;
    let rows = argument(&arguments, 1, 128)?;
    let step = argument(&arguments, 2, 128)?;
    let width = argument(&arguments, 3, 1024)?;
    if chunks == 0 || rows == 0 || step == 0 || width == 0 {
        return Err("arguments must be positive".into());
    }
    let path = std::env::temp_dir().join(format!("moraine-inline-memory-{}", std::process::id()));
    std::fs::create_dir(&path)?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(&path)?);
    let mut options = CatalogOptions::default();
    options.flush_interval = Duration::ZERO;
    options.cache_memory = Some(8 * 1024 * 1024);
    let catalog = Catalog::open(store.clone(), options.clone()).await?;
    let (table, source_bytes) = seed(&catalog, chunks, rows, width).await?;
    catalog.close().await?;
    drop(catalog);
    let catalog = Catalog::open(store, options).await?;
    let snapshot = catalog.snapshot().await?;
    let column = snapshot
        .columns_of(table)
        .first()
        .ok_or("column missing")?
        .id;
    drop(snapshot);
    let definition = IndexDef {
        name: "by_a".into(),
        columns: vec![column],
        unique: true,
    };
    let profiler = dhat::Profiler::builder()
        .testing()
        .trim_backtraces(Some(0))
        .build();
    let started = Instant::now();
    let index = catalog
        .create_index_staged(
            table,
            &definition,
            &[],
            None,
            "",
            Some(BuildStep {
                entries: step,
                bytes: 8 * 1024 * 1024,
            }),
        )
        .await?;
    let elapsed = started.elapsed();
    let stats = dhat::HeapStats::get();
    drop(profiler);
    let found = catalog
        .index_range(
            table,
            index,
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
            false,
        )
        .await?;
    if found.len() != chunks * rows {
        return Err("incomplete index".into());
    }
    println!(
        "chunks,rows_per_chunk,step_entries,value_bytes,source_bytes,peak_heap_bytes,retained_heap_bytes,total_allocated_bytes,total_ms"
    );
    println!(
        "{chunks},{rows},{step},{width},{source_bytes},{},{},{},{:.3}",
        stats.max_bytes,
        stats.curr_bytes,
        stats.total_bytes,
        elapsed.as_secs_f64() * 1000.0
    );
    catalog.close().await?;
    std::fs::remove_dir_all(path)?;
    Ok(())
}

async fn seed(
    catalog: &Catalog,
    chunks: usize,
    rows: usize,
    width: usize,
) -> BenchResult<(moraine::TableId, usize)> {
    let table = Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx
                .schema_by_name("main")
                .ok_or("main schema missing")
                .map_err(|error| moraine::Error::NotFound(error.into()))?
                .id;
            table.set(Some(tx.create_table(
                schema,
                "inline_memory",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "VARCHAR".into(),
                    ..Default::default()
                }],
            )?));
            Ok(())
        })
        .await?;
    let table = table.get().ok_or("table missing")?;
    let mut source_bytes = 0usize;
    for position in 0..chunks {
        let chunk = chunk(position * rows, rows, width)?;
        source_bytes += chunk.arrow_body.len();
        catalog
            .commit(|tx| tx.inline_insert(table, &chunk, &[]).map(|_| ()))
            .await?;
    }
    Ok((table, source_bytes))
}
