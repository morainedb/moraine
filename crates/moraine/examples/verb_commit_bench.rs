//! Fixed-size verb commits with unrelated catalog state; emits one CSV row.
//! Arguments: unrelated tables, files per table, samples, group size, flush ms.

use std::{
    alloc::System,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use cpu_time::ProcessTime;
use moraine::{Catalog, CatalogOptions, ColumnDef, DataFile, SchemaId, TableId, Transaction};
use object_store::memory::InMemory;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::SubscriberExt};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[derive(Clone, Default)]
struct Durability(Arc<Mutex<Duration>>);

#[derive(Default)]
struct Timing {
    elapsed: u64,
    commit: bool,
}

impl Visit for Timing {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "elapsed_ns" => self.elapsed = value,
            "projection_ns" => self.commit = true,
            _ => {}
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

impl<S: Subscriber> Layer<S> for Durability {
    fn on_event(&self, event: &Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut timing = Timing::default();
        event.record(&mut timing);
        if timing.commit {
            *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) +=
                Duration::from_nanos(timing.elapsed);
        }
    }
}

fn file(index: usize) -> DataFile {
    DataFile {
        path: format!("file-{index}.parquet"),
        path_is_relative: true,
        file_format: "parquet".to_owned(),
        record_count: 1,
        file_size_bytes: 128,
        footer_size: 32,
        encryption_key: None,
        partition_values: Vec::new(),
        column_stats: Vec::new(),
    }
}

fn number(
    arguments: &[String],
    position: usize,
    default: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    arguments
        .get(position)
        .map_or(Ok(default), |value| value.parse().map_err(Into::into))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let tables = number(&arguments, 0, 1024)?;
    let files = number(&arguments, 1, 8)?;
    let samples = number(&arguments, 2, 100)?;
    let group = number(&arguments, 3, 1)?;
    let flush = u64::try_from(number(&arguments, 4, 0)?)?;
    if samples == 0 || group == 0 {
        return Err("samples and group size must be positive".into());
    }
    let durability = Durability::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(durability.clone()),
    )?;
    let mut options = CatalogOptions::default();
    options.flush_interval = Duration::from_millis(flush);
    let catalog = Catalog::open(Arc::new(InMemory::new()), options).await?;
    catalog
        .commit(|tx| {
            let column = ColumnDef {
                name: "value".into(),
                column_type: "BIGINT".into(),
                nulls_allowed: true,
                default_value: None,
                children: Vec::new(),
            };
            tx.create_table(SchemaId::new(0), "target", std::slice::from_ref(&column))?;
            for index in 0..tables {
                let table = tx.create_table(
                    SchemaId::new(0),
                    &format!("unrelated-{index}"),
                    std::slice::from_ref(&column),
                )?;
                for index in 0..files {
                    tx.register_data_file(table, file(index), &[])?;
                }
            }
            Ok(())
        })
        .await?;
    let held = catalog.snapshot().await?;
    let table = held
        .table_by_name(SchemaId::new(0), "target")
        .ok_or("missing target")?
        .id;
    for sample in 0..5 {
        mutate(&catalog, table, group, sample).await?;
    }
    *durability
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Duration::ZERO;
    let requests = catalog.object_store_tally();
    let allocation = Region::new(ALLOCATOR);
    let cpu = ProcessTime::now();
    let started = Instant::now();
    for sample in 0..samples {
        mutate(&catalog, table, group, sample + 5).await?;
    }
    let elapsed = started.elapsed();
    let cpu = cpu.elapsed();
    let allocation = allocation.change();
    let object_put = catalog
        .object_store_tally()
        .main_put_duration
        .saturating_sub(requests.main_put_duration);
    let durable = *durability
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let count = f64::from(u32::try_from(samples * group)?);
    println!(
        "{tables},{files},{samples},{group},{flush},{:.3},{:.3},{:.3},{:.3},{:.3},{:.1},{:.1}",
        elapsed.as_secs_f64() * 1e6 / count,
        cpu.as_secs_f64() * 1e6 / count,
        durable.as_secs_f64() * 1e6 / count,
        elapsed.saturating_sub(durable).as_secs_f64() * 1e6 / count,
        object_put.as_secs_f64() * 1e6 / count,
        f64::from(u32::try_from(
            allocation.allocations + allocation.reallocations
        )?) / count,
        allocated_bytes(allocation.bytes_allocated) / count
    );
    std::hint::black_box(held);
    Ok(())
}

async fn mutate(
    catalog: &Catalog,
    table: TableId,
    group: usize,
    sample: usize,
) -> moraine::Result<()> {
    let changes: Vec<_> = (0..group)
        .map(|member| {
            move |tx: &mut Transaction| {
                tx.update_table_stats(table, (sample * group + member) as u64, 128)
            }
        })
        .collect();
    let members: Vec<moraine::CommitMember<'_>> = changes
        .iter()
        .map(|change| change as moraine::CommitMember<'_>)
        .collect();
    catalog.commit_group(&members).await?;
    Ok(())
}

// Profiling totals are approximate; integer precision beyond 2^53 is
// unnecessary.
#[allow(clippy::cast_precision_loss)]
fn allocated_bytes(bytes: usize) -> f64 {
    bytes as f64
}
