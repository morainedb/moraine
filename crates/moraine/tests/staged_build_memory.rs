//! Whole-operation peak heap of an inline staged index build over a
//! persisted store. A measurement, not a test: it installs dhat's
//! allocator, which cannot sit beside the `it` binary's tests, so it has a
//! binary of its own.
//!
//! ```text
//! cargo test -p moraine --test staged_build_memory --release -- --ignored --nocapture
//! ```

use std::{
    cell::Cell,
    ops::Bound,
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
use moraine::{BuildStep, Catalog, CatalogOptions, ColumnDef, IndexDef, InlineChunk, TableId};
use object_store::local::LocalFileSystem;

#[path = "it/conditions.rs"]
mod conditions;

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

type Failure = Box<dyn std::error::Error>;

/// Chunks, rows per chunk, and step entries per case.
const CASES: [(usize, usize, usize); 4] = [
    (16, 128, 128),
    (128, 128, 128),
    (128, 128, 16),
    (16, 1024, 128),
];
/// Width of every indexed string value.
const VALUE_BYTES: usize = 1024;
const CACHE_BYTES: u64 = 8 * 1024 * 1024;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
async fn measure_staged_build_peak_heap() -> Result<(), Failure> {
    conditions::stamp("staged index build peak heap (local filesystem store, dhat allocator)");
    println!(
        "chunks,rows_per_chunk,step_entries,value_bytes,source_bytes,peak_heap_bytes,\
         retained_heap_bytes,total_allocated_bytes,total_ms"
    );
    for (chunks, rows, step) in CASES {
        measure(chunks, rows, step).await?;
    }
    Ok(())
}

async fn measure(chunks: usize, rows: usize, step: usize) -> Result<(), Failure> {
    let path = std::env::temp_dir().join(format!(
        "moraine-staged-build-memory-{}-{chunks}-{rows}-{step}",
        std::process::id()
    ));
    std::fs::create_dir(&path)?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(&path)?);
    let mut options = CatalogOptions::default();
    options.flush_interval = Duration::ZERO;
    options.cache_memory = Some(CACHE_BYTES);
    let catalog = Catalog::open(store.clone(), options.clone()).await?;
    let (table, source_bytes) = seed(&catalog, chunks, rows).await?;
    catalog.close().await?;
    drop(catalog);

    let catalog = Catalog::open(store, options).await?;
    let snapshot = catalog.read_scope().await?;
    let column = snapshot
        .columns_of(table)
        .await?
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
                bytes: CACHE_BYTES,
            }),
        )
        .await?;
    let elapsed = started.elapsed();
    let stats = dhat::HeapStats::get();
    drop(profiler);

    let found = catalog
        .index_range(table, index, Bound::Unbounded, Bound::Unbounded, false)
        .await?;
    if found.len() != chunks * rows {
        return Err("incomplete index".into());
    }
    println!(
        "{chunks},{rows},{step},{VALUE_BYTES},{source_bytes},{},{},{},{:.3}",
        stats.max_bytes,
        stats.curr_bytes,
        stats.total_bytes,
        elapsed.as_secs_f64() * 1000.0
    );
    catalog.close().await?;
    std::fs::remove_dir_all(path)?;
    Ok(())
}

fn chunk(first: usize, rows: usize) -> Result<InlineChunk, Failure> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
    let values: Vec<_> = (first..first + rows)
        .map(|row| format!("{row:0VALUE_BYTES$}"))
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

async fn seed(catalog: &Catalog, chunks: usize, rows: usize) -> Result<(TableId, usize), Failure> {
    let table = Cell::new(None);
    catalog
        .commit(async |tx| {
            let schema = tx
                .schema_by_name("main")
                .await?
                .ok_or_else(|| moraine::Error::NotFound("main".into()))?
                .id;
            table.set(Some(
                tx.create_table(
                    schema,
                    "inline_memory",
                    &[ColumnDef {
                        name: "a".into(),
                        column_type: "VARCHAR".into(),
                        ..Default::default()
                    }],
                )
                .await?,
            ));
            Ok(())
        })
        .await?;
    let table = table.get().ok_or("table missing")?;
    let mut source_bytes = 0usize;
    for position in 0..chunks {
        let chunk = chunk(position * rows, rows)?;
        source_bytes += chunk.arrow_body.len();
        catalog
            .commit(async |tx| tx.inline_insert(table, &chunk, &[]).await.map(|_| ()))
            .await?;
    }
    Ok((table, source_bytes))
}
