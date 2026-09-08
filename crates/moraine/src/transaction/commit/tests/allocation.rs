//! Isolated allocation measurements over real SlateDB stores.

use std::{
    alloc::System,
    cell::Cell,
    ops::Bound,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use cpu_time::ProcessTime;
use object_store::memory::InMemory;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::SubscriberExt};

use super::{bulk_file, bulk_file_entries, int_value, seeded_wide_catalog};
use crate::{
    Catalog, CatalogOptions, ColumnDef, IndexDef, IndexId, ObjectStoreTally, TableId,
    store::handle::ReadHandle,
    transaction::commit::{materialize, refresh},
};

// This module is compiled only into the library's unit-test executable.
#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[derive(Clone, Default)]
struct Durability(Arc<Mutex<Duration>>);

#[derive(Default)]
struct CommitEvent {
    elapsed: u64,
    committed: bool,
}

impl Visit for CommitEvent {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "elapsed_ns" => self.elapsed = value,
            "projection_ns" => self.committed = true,
            _ => {}
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

impl<S: Subscriber> Layer<S> for Durability {
    fn on_event(&self, event: &Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut timing = CommitEvent::default();
        event.record(&mut timing);
        if timing.committed {
            *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) +=
                Duration::from_nanos(timing.elapsed);
        }
    }
}

impl Durability {
    fn elapsed(&self) -> Duration {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct Measurement<'a> {
    allocations: Region<'a, System>,
    cpu: ProcessTime,
    wall: Instant,
    store: ObjectStoreTally,
    durable: Duration,
}

#[derive(Default)]
struct Totals {
    wall: Duration,
    cpu: Duration,
    durable: Duration,
    allocations: usize,
    bytes: usize,
    gets: u64,
    puts: u64,
    put_time: Duration,
}

impl Measurement<'_> {
    fn start(catalog: &Catalog, durability: &Durability) -> Self {
        let store = catalog.object_store_tally();
        let durable = durability.elapsed();
        Self {
            store,
            durable,
            allocations: Region::new(ALLOCATOR),
            cpu: ProcessTime::now(),
            wall: Instant::now(),
        }
    }

    fn finish(self, catalog: &Catalog, durability: &Durability, totals: &mut Totals) {
        let wall = self.wall.elapsed();
        let cpu = self.cpu.elapsed();
        let allocations = self.allocations.change();
        let store = catalog.object_store_tally().since(self.store);
        totals.wall += wall;
        totals.cpu += cpu;
        totals.durable += durability.elapsed().saturating_sub(self.durable);
        totals.allocations += allocations.allocations + allocations.reallocations;
        totals.bytes += allocations.bytes_allocated;
        totals.gets += store.main_gets + store.wal_gets;
        totals.puts += store.main_puts + store.wal_puts;
        totals.put_time += store.main_put_duration + store.wal_put_duration;
    }
}

// Approximate benchmark averages do not need integer precision above 2^53.
#[allow(clippy::cast_precision_loss)]
fn number(value: usize) -> f64 {
    value as f64
}

impl Totals {
    fn print(&self, operation: &str, size: usize, batch: usize, samples: usize) {
        let count = number(samples);
        let micros = |duration: Duration| duration.as_secs_f64() * 1e6 / count;
        println!(
            "ALLOC,{operation},{size},{batch},{samples},{:.3},{:.3},{:.3},{:.3},{:.1},{:.1},{},{},{:.3}",
            micros(self.wall),
            micros(self.cpu),
            micros(self.durable),
            micros(self.wall.saturating_sub(self.durable)),
            number(self.allocations) / count,
            number(self.bytes) / count,
            self.gets,
            self.puts,
            micros(self.put_time)
        );
    }
}

fn header() {
    println!(
        "ALLOC,operation,size,batch,samples,total_us,cpu_us,durable_us,non_durable_us,allocations,allocated_bytes,total_gets,total_puts,put_us"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "allocation benchmark; run alone with --exact --ignored --test-threads=1 --nocapture"]
async fn snapshot_refresh() {
    header();
    let durability = Durability::default();
    for tables in [16, 128, 1024] {
        let (catalog, ids) = seeded_wide_catalog(tables, 4).await;
        catalog
            .commit(|tx| {
                for &table in &ids {
                    for file in 0..8 {
                        tx.register_data_file(table, bulk_file(&format!("f{file}"), 1), &[])?;
                    }
                }
                Ok(())
            })
            .await
            .unwrap();
        let base = catalog.snapshot().await.unwrap();
        catalog
            .commit(|tx| tx.update_table_stats(ids[0], 9, 90))
            .await
            .unwrap();
        let tx = catalog.begin_write_tx().await.unwrap();
        let handle = ReadHandle::Tx(&tx);
        // Prime store blocks for both paths; each sample still constructs a fresh view.
        std::hint::black_box(materialize(handle, None).await.unwrap());
        std::hint::black_box(refresh(handle, &base).await.unwrap().unwrap());
        let mut full = Totals::default();
        let mut incremental = Totals::default();
        for _ in 0..9 {
            let measurement = Measurement::start(&catalog, &durability);
            let rebuilt = materialize(handle, None).await.unwrap();
            measurement.finish(&catalog, &durability, &mut full);
            let measurement = Measurement::start(&catalog, &durability);
            let refreshed = refresh(handle, &base)
                .await
                .unwrap()
                .expect("one changed record must be replayable");
            measurement.finish(&catalog, &durability, &mut incremental);
            assert_eq!(rebuilt.snapshot, refreshed.snapshot);
            assert_eq!(rebuilt.table_stats, refreshed.table_stats);
            assert_eq!(rebuilt.data_files, refreshed.data_files);
            assert_eq!(refreshed.table_stats[&ids[0].get()].record_count, 9);
        }
        full.print("materialize", tables, 1, 9);
        incremental.print("refresh", tables, 1, 9);
        tx.rollback();
        catalog.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "allocation benchmark; run alone with --exact --ignored --test-threads=1 --nocapture"]
async fn warm_reads() {
    header();
    let durability = Durability::default();
    for tables in [16, 128, 1024] {
        let (catalog, _) = seeded_wide_catalog(tables, 4).await;
        let held = catalog.snapshot().await.unwrap();
        let mut totals = Totals::default();
        let measurement = Measurement::start(&catalog, &durability);
        for _ in 0..2000 {
            let view = catalog.snapshot().await.unwrap();
            assert!(Arc::ptr_eq(&held, &view));
            std::hint::black_box(view);
        }
        measurement.finish(&catalog, &durability, &mut totals);
        totals.print("warm_snapshot", tables, 1, 2000);
        catalog.close().await.unwrap();
    }
}

#[allow(clippy::unwrap_used)]
async fn indexed_catalog(entries: u64) -> (Catalog, Arc<InMemory>, TableId, IndexId) {
    let store = Arc::new(InMemory::new());
    let catalog = Catalog::open(
        store.clone(),
        CatalogOptions {
            flush_interval: Duration::from_millis(1),
            ..CatalogOptions::default()
        },
    )
    .await
    .unwrap();
    let ids = Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            let table = tx.create_table(
                schema,
                "indexed",
                &[ColumnDef {
                    name: "value".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: false,
                    default_value: None,
                    children: Vec::new(),
                }],
            )?;
            let column = tx.columns_of(table)[0].id;
            let index = tx.create_index(
                table,
                &IndexDef {
                    name: "by_value".into(),
                    columns: vec![column],
                    unique: true,
                },
                &[],
            )?;
            tx.register_data_file(
                table,
                bulk_file("seed", entries),
                &bulk_file_entries(index, 0, entries),
            )?;
            ids.set(Some((table, index)));
            Ok(())
        })
        .await
        .unwrap();
    let (table, index) = ids.get().unwrap();
    (catalog, store, table, index)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "allocation benchmark; run alone with --exact --ignored --test-threads=1 --nocapture"]
async fn index_lookups() {
    header();
    let durability = Durability::default();
    for entries in [1024u64, 16384, 65536] {
        let (writer, store, table, index) = indexed_catalog(entries).await;
        // Reopening after a durable close puts entries in the immutable store path.
        writer.close().await.unwrap();
        let catalog = Catalog::open(store, CatalogOptions::default())
            .await
            .unwrap();
        let keys: Vec<_> = (0..32)
            .map(|n| int_value(i128::from(n * (entries / 32))))
            .collect();
        for operation in ["point_hit", "point_miss", "range_32"] {
            for warm in [false, true] {
                let mut totals = Totals::default();
                let measurement = Measurement::start(&catalog, &durability);
                for (position, key) in keys.iter().enumerate() {
                    let rows = match operation {
                        "point_hit" => catalog
                            .index_lookup(table, index, std::slice::from_ref(key))
                            .await
                            .unwrap(),
                        "point_miss" => catalog
                            .index_lookup(
                                table,
                                index,
                                &[int_value(-1 - i128::try_from(position).unwrap())],
                            )
                            .await
                            .unwrap(),
                        _ => catalog
                            .index_range(
                                table,
                                index,
                                Bound::Included(vec![key.clone()]),
                                Bound::Excluded(vec![int_value(
                                    i128::try_from(position).unwrap() * i128::from(entries / 32)
                                        + 32,
                                )]),
                                false,
                            )
                            .await
                            .unwrap(),
                    };
                    assert_eq!(
                        rows.len(),
                        match operation {
                            "point_hit" => 1,
                            "point_miss" => 0,
                            _ => 32,
                        }
                    );
                    let first = u64::try_from(position).unwrap() * (entries / 32);
                    let count = u64::try_from(rows.len()).unwrap();
                    assert!(rows.iter().copied().eq(first..first + count));
                    std::hint::black_box(rows);
                }
                measurement.finish(&catalog, &durability, &mut totals);
                totals.print(
                    &format!("{operation}_{}", if warm { "warm" } else { "first" }),
                    usize::try_from(entries).unwrap(),
                    1,
                    32,
                );
            }
        }
        catalog.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "allocation benchmark; run alone with --exact --ignored --test-threads=1 --nocapture"]
async fn index_maintenance() {
    header();
    let durability = Durability::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(durability.clone()),
    )
    .unwrap();
    for existing in [1024u64, 16384, 65536] {
        for batch in [1u64, 64, 1024] {
            let (catalog, _, table, index) = indexed_catalog(existing).await;
            let mut totals = Totals::default();
            for sample in 0..8 {
                let first = existing + sample * batch;
                let entries = bulk_file_entries(index, i128::from(first), batch);
                let file = bulk_file(&format!("append-{sample}"), batch);
                let measurement = Measurement::start(&catalog, &durability);
                catalog
                    .commit(|tx| {
                        tx.register_data_file(table, file.clone(), &entries)
                            .map(|_| ())
                    })
                    .await
                    .unwrap();
                if sample >= 3 {
                    measurement.finish(&catalog, &durability, &mut totals);
                }
                let found = catalog
                    .index_range(
                        table,
                        index,
                        Bound::Included(vec![int_value(i128::from(first))]),
                        Bound::Excluded(vec![int_value(i128::from(first + batch))]),
                        false,
                    )
                    .await
                    .unwrap();
                assert!(found.into_iter().eq(first..first + batch));
            }
            totals.print(
                "index_append",
                usize::try_from(existing).unwrap(),
                usize::try_from(batch).unwrap(),
                5,
            );
            catalog.close().await.unwrap();
        }
    }
}

#[test]
#[ignore = "allocation assertion; run alone with --exact --ignored --test-threads=1"]
fn refresh_count_does_not_allocate() {
    let mut view = crate::CatalogSnapshot::default();
    for table_id in 0..1024 {
        view.put_column(crate::store::proto::ColumnValue {
            table_id,
            column_id: table_id,
            ..Default::default()
        });
    }
    let measurement = Region::new(ALLOCATOR);
    let count = std::hint::black_box(&view).live_entity_count();
    let allocations = measurement.change();
    assert_eq!(count, 1024);
    assert_eq!(allocations.allocations + allocations.reallocations, 0);
}

// Keep one synchronous frame around commit polling so heap profiles can exclude
// fixture construction, input preparation, verification, and shutdown.
#[inline(never)]
#[allow(clippy::unwrap_used)]
fn profile_index_append(
    runtime: &tokio::runtime::Runtime,
    catalog: &Catalog,
    table: TableId,
    file: &crate::DataFile,
    entries: &[crate::FileIndexEntry],
) -> stats_alloc::Stats {
    let measurement = Region::new(ALLOCATOR);
    runtime
        .block_on(catalog.commit(|tx| {
            tx.register_data_file(table, file.clone(), entries)
                .map(|_| ())
        }))
        .unwrap();
    measurement.change()
}

#[test]
#[ignore = "heap profile; run alone with --exact --ignored --test-threads=1 --nocapture"]
fn index_maintenance_profile() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (catalog, _, table, index) = runtime.block_on(indexed_catalog(1024));
    let mut allocations = 0;
    let mut bytes = 0;
    for sample in 0..20u64 {
        let first = 1024 + sample * 1024;
        let file = bulk_file(&format!("profile-{sample}"), 1024);
        let entries = bulk_file_entries(index, i128::from(first), 1024);
        let stats = profile_index_append(&runtime, &catalog, table, &file, &entries);
        allocations += stats.allocations + stats.reallocations;
        bytes += stats.bytes_allocated;
        let rows = runtime
            .block_on(catalog.index_range(
                table,
                index,
                Bound::Included(vec![int_value(i128::from(first))]),
                Bound::Excluded(vec![int_value(i128::from(first + 1024))]),
                false,
            ))
            .unwrap();
        assert!(rows.into_iter().eq(first..first + 1024));
    }
    println!("PROFILE,entries,allocations,allocated_bytes");
    println!("PROFILE,20480,{allocations},{bytes}");
    runtime.block_on(catalog.close()).unwrap();
}

#[test]
#[ignore = "allocation assertion; run alone with --exact --ignored --test-threads=1"]
fn an_integer_entry_uses_one_key_buffer_allocation() {
    use crate::store::index_encoding::{Direction, NullOrder, encode_ordered_index_entry};
    for value in [
        None,
        Some(int_value(0)),
        Some(int_value(1024)),
        Some(int_value(i128::from(i64::MIN))),
        Some(int_value(i128::from(i64::MAX))),
    ] {
        for direction in [Direction::Ascending, Direction::Descending] {
            for unique in [false, true] {
                let values = [value.clone()];
                let measurement = Region::new(ALLOCATOR);
                let key = encode_ordered_index_entry(
                    &values,
                    &[direction],
                    &[NullOrder::Last],
                    1,
                    unique,
                    0,
                )
                .unwrap();
                let stats = measurement.change();
                std::hint::black_box(key);
                assert_eq!(stats.allocations + stats.reallocations, 1);
            }
        }
    }
}

#[tokio::test]
#[ignore = "allocation assertion; run alone with --exact --ignored --test-threads=1"]
async fn missing_batch_probes_share_read_setup() {
    let (catalog, _) = seeded_wide_catalog(1, 1).await;
    let transaction = catalog.begin_write_tx().await.unwrap();
    let keys: Vec<_> = (0..128u64)
        .map(|id| {
            let mut key = vec![255];
            key.extend_from_slice(&id.to_be_bytes());
            bytes::Bytes::from(key)
        })
        .collect();
    let measurement = Region::new(ALLOCATOR);
    let values = crate::store::handle::probes::get_many(&transaction, &keys)
        .await
        .unwrap();
    let stats = measurement.change();
    assert!(values.iter().all(Option::is_none));
    assert!(
        stats.allocations + stats.reallocations < keys.len() * 4,
        "batch allocated {} times",
        stats.allocations + stats.reallocations
    );
    transaction.rollback();
    catalog.close().await.unwrap();
}

fn raw_probe_key(id: u64) -> bytes::Bytes {
    let mut key = vec![255];
    key.extend_from_slice(&id.to_be_bytes());
    bytes::Bytes::from(key)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "allocation benchmark; run alone with --exact --ignored --test-threads=1 --nocapture"]
async fn probe_read_batches() {
    use futures::{StreamExt, TryStreamExt, stream};

    let store = Arc::new(InMemory::new());
    let writer = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let seed = writer.begin_write_tx().await.unwrap();
    for id in 0..32768u64 {
        seed.put(raw_probe_key(id * 2), id.to_be_bytes()).unwrap();
    }
    seed.commit().await.unwrap();
    writer.close().await.unwrap();
    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    let transaction = catalog.begin_write_tx().await.unwrap();
    let durability = Durability::default();
    header();
    for pattern in ["dense_hit", "dense_miss", "sparse_hit", "sparse_miss"] {
        let keys: Vec<_> = (0..128u64)
            .map(|position| {
                let id = match pattern {
                    "dense_hit" => position * 2,
                    "dense_miss" => 65536 + position,
                    "sparse_hit" => position * 512,
                    _ => position * 512 + 1,
                };
                raw_probe_key(id)
            })
            .collect();
        let mut point_totals = Totals::default();
        let mut batch_totals = Totals::default();
        for sample in 0..6 {
            let measurement = Measurement::start(&catalog, &durability);
            let point: Vec<_> = stream::iter(keys.iter().map(|key| transaction.get(key)))
                .buffered(128)
                .try_collect()
                .await
                .unwrap();
            if sample > 0 {
                measurement.finish(&catalog, &durability, &mut point_totals);
            }
            let measurement = Measurement::start(&catalog, &durability);
            let batch = crate::store::handle::probes::get_many(&transaction, &keys)
                .await
                .unwrap();
            if sample > 0 {
                measurement.finish(&catalog, &durability, &mut batch_totals);
            }
            assert_eq!(point, batch);
            assert_eq!(
                batch.iter().filter(|value| value.is_some()).count(),
                if pattern.ends_with("miss") { 0 } else { 128 }
            );
        }
        point_totals.print(&format!("point_{pattern}"), 32768, 128, 5 * 128);
        batch_totals.print(&format!("batch_{pattern}"), 32768, 128, 5 * 128);
    }
    transaction.rollback();
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "allocation benchmark; run alone with --exact --ignored --test-threads=1 --nocapture"]
async fn index_maintenance_input_order() {
    header();
    let durability = Durability::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(durability.clone()),
    )
    .unwrap();
    for order in ["ascending", "descending", "scattered"] {
        let (catalog, _, table, index) = indexed_catalog(1024).await;
        let mut totals = Totals::default();
        for sample in 0..6u64 {
            let first = 1024 + sample * 4096;
            let file = bulk_file(&format!("order-{sample}"), 4096);
            let mut entries = bulk_file_entries(index, i128::from(first), 4096);
            match order {
                "descending" => entries.reverse(),
                "scattered" => entries.sort_unstable_by_key(|entry| entry.ordinal * 2053 % 4096),
                _ => {}
            }
            let measurement = Measurement::start(&catalog, &durability);
            catalog
                .commit(|tx| {
                    tx.register_data_file(table, file.clone(), &entries)
                        .map(|_| ())
                })
                .await
                .unwrap();
            if sample > 0 {
                measurement.finish(&catalog, &durability, &mut totals);
            }
            let rows = catalog
                .index_range(
                    table,
                    index,
                    Bound::Included(vec![int_value(i128::from(first))]),
                    Bound::Excluded(vec![int_value(i128::from(first + 4096))]),
                    false,
                )
                .await
                .unwrap();
            assert!(rows.into_iter().eq(first..first + 4096));
        }
        totals.print(&format!("index_append_{order}"), 1024, 4096, 5);
        catalog.close().await.unwrap();
    }
}
