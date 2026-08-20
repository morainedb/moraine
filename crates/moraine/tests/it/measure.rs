//! Measurement harnesses for the `MEASURE` items in `docs/rfcs/tasks.md`.
//! These are not assertions — they print timings, and are `#[ignore]`d so
//! the ordinary suite stays fast. Run one explicitly and read its output:
//!
//! ```text
//! cargo test -p moraine --test it --release -- --ignored --nocapture measure_
//! ```
//!
//! Every number is machine- and backend-specific; the run prints its
//! conditions so a recorded result stays interpretable. They run on the
//! in-memory `object_store`, where a durable commit performs no network IO
//! — so the durable-commit cost measured here is moraine's flush poll plus
//! compute, and a real object store adds its WAL-object PUT round-trip on
//! top. The harness makes that structure explicit rather than hiding it.

// Benchmark code: entity/entry counts cast to f64 for rate math (precision
// loss on a count is immaterial), and `Stats`' fields share a `_ms` unit
// suffix by design.
#![allow(clippy::cast_precision_loss, clippy::struct_field_names)]

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use moraine::{
    Catalog, CatalogOptions, CensusRequest, ColumnId, DataFile, FileIndexEntry, IndexDef,
    IndexEntry, IndexKeyValue, IntWidth, MaintenanceRequest, SubspaceName,
};
use object_store::{
    memory::InMemory,
    throttle::{ThrottleConfig, ThrottledStore},
};

use crate::{
    crash_recovery::freezing_store::FreezingStore,
    fixtures::{col, datafile},
};

/// Median, min, and max of a set of samples, in milliseconds.
struct Stats {
    median_ms: f64,
    min_ms: f64,
    max_ms: f64,
}

impl Stats {
    fn of(mut samples: Vec<Duration>) -> Self {
        samples.sort();
        let ms = |d: Duration| d.as_secs_f64() * 1_000.0;
        Self {
            median_ms: ms(samples[samples.len() / 2]),
            min_ms: ms(samples[0]),
            max_ms: ms(samples[samples.len() - 1]),
        }
    }
}

/// A read-only handle: it opens no writer, so it neither fences nor runs a
/// compactor that would move the state a measurement is holding still.
#[allow(clippy::unwrap_used)]
async fn open_reader(store: Arc<InMemory>) -> moraine::ReadOnlyCatalog {
    Catalog::open_read_only(store, CatalogOptions::default())
        .await
        .unwrap()
}

#[allow(clippy::unwrap_used)]
async fn open_with(store: Arc<InMemory>, flush_ms: u64) -> Catalog {
    let mut options = CatalogOptions::default();
    options.commit_batch_window = Duration::from_millis(flush_ms);
    Catalog::open(store, options).await.unwrap()
}

/// The default handle flush interval, so a "seed fast, measure at default"
/// harness names the realistic figure rather than a magic number.
const DEFAULT_FLUSH_MS: u64 = 100;
/// A fast interval for seeding, whose durable waits are not being timed.
const SEED_FLUSH_MS: u64 = 1;

/// 0009 — catalog materialization versus live-entity count.
///
/// `Catalog::snapshot()` calls `materialize` directly and never consults
/// the projection cache (which only committers populate), so this is not a
/// cold-start cost — it is paid on *every* public read. The RFC leaves open
/// whether that full materialization stays cheap enough to make partial or
/// lazy materialization unnecessary; this puts a curve under it. Timing
/// covers only the read, so the seed's flush interval is irrelevant and set
/// fast.
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_materialization_by_catalog_size() {
    const COLS_PER_TABLE: usize = 8;
    const FILES_PER_TABLE: usize = 16;
    const REPEATS: usize = 9;
    let ladder = [10usize, 50, 200, 800];

    println!("\n# 0009 materialization per snapshot() (in-memory object_store)");
    println!(
        "# {COLS_PER_TABLE} columns and {FILES_PER_TABLE} files per table, \
         median of {REPEATS} materializations\n"
    );
    println!(
        "{:>7}  {:>10}  {:>11}  {:>9}  {:>9}  {:>12}",
        "tables", "entities", "median_ms", "min_ms", "max_ms", "us_per_1k"
    );

    for &tables in &ladder {
        let store = Arc::new(InMemory::new());
        let catalog = open_with(store.clone(), SEED_FLUSH_MS).await;

        let columns: Vec<_> = (0..COLS_PER_TABLE).map(|c| col(&format!("c{c}"))).collect();
        for t in 0..tables {
            let columns = columns.clone();
            catalog
                .commit(move |tx| {
                    let schema = tx.schema_by_name("main").expect("bootstrap").id;
                    let table = tx.create_table(schema, &format!("t{t}"), &columns)?;
                    for _ in 0..FILES_PER_TABLE {
                        tx.register_data_file(table, datafile(100), &[])?;
                    }
                    Ok(())
                })
                .await
                .unwrap();
        }
        catalog.close().await.unwrap();

        // Live entity count, measured rather than assumed.
        let probe = open_with(store.clone(), SEED_FLUSH_MS).await;
        let view = probe.snapshot().await.unwrap();
        let entities: usize = view
            .schemas()
            .iter()
            .flat_map(|s| view.tables_in(s.id))
            .map(|t| 1 + view.columns_of(t.id).len() + view.data_files_of(t.id).len())
            .sum();

        probe.close().await.unwrap();

        // A fresh handle per repeat: `snapshot()` serves from the maintained
        // cache, so reusing one would time a cache hit and report
        // materialization as free. Only the read is timed, not the open.
        let mut samples = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let probe = open_with(store.clone(), SEED_FLUSH_MS).await;
            let start = Instant::now();
            let view = probe.snapshot().await.unwrap();
            samples.push(start.elapsed());
            std::hint::black_box(&view);
            probe.close().await.unwrap();
        }

        let stats = Stats::of(samples);
        let us_per_1k = (stats.median_ms * 1_000.0) / (entities as f64 / 1_000.0);
        println!(
            "{tables:>7}  {entities:>10}  {:>11.3}  {:>9.3}  {:>9.3}  {us_per_1k:>12.1}",
            stats.median_ms, stats.min_ms, stats.max_ms
        );
    }
    println!();
}

/// 0021 — cold attach cost against the physical bytes `current` holds.
///
/// The claim the store census and the store merge rest on: a cold attach
/// scans `current`, and it reads through every superseded version those
/// SSTs still hold, so its cost tracks the subspace's *physical* size
/// rather than its live entity count. This sweep holds the live catalog
/// fixed and rewrites it, so live entities are constant across every row
/// and only the dead fraction grows.
///
/// A merge is what collapses those bytes back down (`store::compaction`
/// pins that it does); this measures what they cost while they stand.
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_attach_cost_by_dead_fraction() {
    const TABLES: usize = 40;
    const COLS_PER_TABLE: usize = 8;
    const REPEATS: usize = 7;
    // Each round rewrites every table's statistics, superseding one
    // `current` record per table without changing what is live.
    let rounds = [0usize, 10, 40, 160];

    println!("\n# 0021 cold attach vs. physical `current` bytes (in-memory object_store)");
    println!(
        "# {TABLES} tables x {COLS_PER_TABLE} columns, live entities held constant; \
         each round rewrites every table's stats\n"
    );
    println!(
        "{:>7}  {:>10}  {:>14}  {:>8}  {:>5}  {:>11}  {:>9}",
        "rounds", "live_keys", "current_bytes", "l0_ssts", "runs", "median_ms", "max_ms"
    );

    for &rounds in &rounds {
        let store = Arc::new(InMemory::new());
        let catalog = open_with(store.clone(), SEED_FLUSH_MS).await;

        let columns: Vec<_> = (0..COLS_PER_TABLE).map(|c| col(&format!("c{c}"))).collect();
        let mut tables = Vec::with_capacity(TABLES);
        for t in 0..TABLES {
            let columns = columns.clone();
            let made = std::cell::Cell::new(None);
            catalog
                .commit(|tx| {
                    let schema = tx.schema_by_name("main").expect("bootstrap").id;
                    made.set(Some(tx.create_table(schema, &format!("t{t}"), &columns)?));
                    Ok(())
                })
                .await
                .unwrap();
            tables.push(made.get().unwrap());
        }

        catalog.close().await.unwrap();

        // Churn: each round supersedes one `current` record per table, and
        // closes so the memtable is written out. Without the close the
        // superseded versions stay in memory, never reach an SST, and the
        // physical size this measures does not move — the first run of this
        // harness reported exactly that flat line.
        for round in 0..rounds {
            let catalog = open_with(store.clone(), SEED_FLUSH_MS).await;
            let batch = tables.clone();
            catalog
                .commit(move |tx| {
                    for &table in &batch {
                        tx.update_table_stats(table, 100 + round as u64, 4_096)?;
                    }
                    Ok(())
                })
                .await
                .unwrap();
            catalog.close().await.unwrap();
        }

        // What `current` weighs, live and physical, from the census itself
        // — read-only, so measuring the store does not compact it.
        let probe = open_reader(store.clone()).await;
        let mut request = CensusRequest::default();
        request.count_live_entries = true;
        let census = probe.store_census(request).await.unwrap();
        let current = census
            .subspaces
            .iter()
            .find(|s| s.subspace == SubspaceName::Current)
            .expect("current is always reported");
        let live_keys = current.live.expect("the scanning leg was requested").keys;
        let bytes = current.bytes;
        let l0_ssts = current.l0_ssts;
        let runs = current.sorted_runs;
        // Everything a materialization reads besides `current`, so a cost
        // this table cannot explain has somewhere to show up.
        let other: Vec<String> = census
            .subspaces
            .iter()
            .filter(|s| s.subspace != SubspaceName::Current && s.bytes > 0)
            .map(|s| {
                format!(
                    "{}={}B/{}k",
                    s.subspace,
                    s.bytes,
                    s.live.map_or(0, |l| l.keys)
                )
            })
            .collect();
        probe.close().await.unwrap();

        // A fresh handle per repeat: a warm one serves from the cache and
        // would report a cold attach as free. Read-only, for two reasons —
        // it is the attach shape the incident this measures came from, and
        // a writer starts a compactor that would perturb the very state
        // being measured between repeats.
        let mut samples = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let probe = open_reader(store.clone()).await;
            let start = Instant::now();
            let view = probe.snapshot().await.unwrap();
            samples.push(start.elapsed());
            std::hint::black_box(&view);
            probe.close().await.unwrap();
        }

        let stats = Stats::of(samples);
        println!(
            "{rounds:>7}  {live_keys:>10}  {bytes:>14}  {l0_ssts:>8}  {runs:>5}  {:>11.3}  {:>9.3}   {}",
            stats.median_ms,
            stats.max_ms,
            other.join(" ")
        );
    }
    println!();
}

/// Seeds `tables` and then rewrites every one of their statistics records
/// `rounds` times, closing after each round.
///
/// The close is load-bearing: without it the superseded versions stay in
/// the memtable, never reach an SST, and cost nobody a GET.
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn churn_into_ssts(store: &Arc<InMemory>, tables: usize, columns: usize, rounds: usize) {
    let catalog = open_with(store.clone(), SEED_FLUSH_MS).await;
    let columns: Vec<_> = (0..columns).map(|c| col(&format!("c{c}"))).collect();
    let mut made_tables = Vec::with_capacity(tables);
    for t in 0..tables {
        let columns = columns.clone();
        let made = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                made.set(Some(tx.create_table(schema, &format!("t{t}"), &columns)?));
                Ok(())
            })
            .await
            .unwrap();
        made_tables.push(made.get().unwrap());
    }
    catalog.close().await.unwrap();

    for round in 0..rounds {
        let catalog = open_with(store.clone(), SEED_FLUSH_MS).await;
        let batch = made_tables.clone();
        catalog
            .commit(move |tx| {
                for &table in &batch {
                    tx.update_table_stats(table, 100 + round as u64, 4_096)?;
                }
                Ok(())
            })
            .await
            .unwrap();
        catalog.close().await.unwrap();
    }
}

/// 0021 — what a store merge is worth once a GET costs what S3 charges.
///
/// The companion to the sweep above, and the one that reaches the regime
/// the production incident sat in. That incident was IO-bound — ~5.3 MB/s
/// effective, a read pulling the whole store across the network — so the
/// term that matters is not bytes decoded but **object-store GETs issued**,
/// and GETs scale with how many SSTs a scan must open rather than how much
/// they hold. An in-memory store measures the wrong term however large it
/// grows; injected per-GET latency measures the right one at any size.
///
/// Sweeping latency for a fixed store makes the GET count readable off the
/// slope: attach time is roughly `GETs x latency + decode`, so the gradient
/// is the GET count and the intercept is the compute floor. Doing it for a
/// churned store and again after `compact_store` prices the merge.
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_attach_cost_under_get_latency() {
    const TABLES: usize = 40;
    const COLS_PER_TABLE: usize = 8;
    const ROUNDS: usize = 160;
    const REPEATS: usize = 5;
    let latencies = [0u64, 2, 5, 10];

    println!("\n# 0021 cold read-only attach vs. injected per-GET latency");
    println!(
        "# {TABLES} tables x {COLS_PER_TABLE} columns, {ROUNDS} churn rounds, \
         live set constant; median of {REPEATS}\n"
    );

    let store = Arc::new(InMemory::new());
    churn_into_ssts(&store, TABLES, COLS_PER_TABLE, ROUNDS).await;

    for merged in [false, true] {
        if merged {
            let writer = open_with(store.clone(), SEED_FLUSH_MS).await;
            let mut request = moraine::CompactStoreRequest::default();
            request.wait = Some(Duration::from_secs(60));
            let report = writer.compact_store(request).await.unwrap();
            let completed = report
                .merges
                .iter()
                .filter(|m| m.outcome == moraine::MergeOutcome::Completed)
                .count();
            writer.close().await.unwrap();
            println!("\n## after compact_store — {completed} subspaces merged\n");
        } else {
            println!("## churned, unmerged\n");
        }

        // What the reader will have to open, before timing anything.
        let probe = open_reader(store.clone()).await;
        let census = probe.store_census(CensusRequest::default()).await.unwrap();
        let ssts: u32 = census
            .subspaces
            .iter()
            .map(|s| s.l0_ssts + s.sorted_run_ssts)
            .sum();
        let bytes = census.total_bytes();
        probe.close().await.unwrap();
        println!("   store: {ssts} SSTs, {bytes} bytes");
        println!(
            "{:>12}  {:>11}  {:>9}",
            "get_latency", "median_ms", "max_ms"
        );

        for &latency_ms in &latencies {
            let config = ThrottleConfig {
                wait_get_per_call: Duration::from_millis(latency_ms),
                ..ThrottleConfig::default()
            };
            let throttled = Arc::new(ThrottledStore::new((*store).clone(), config));

            let mut samples = Vec::with_capacity(REPEATS);
            for _ in 0..REPEATS {
                let probe = Catalog::open_read_only(throttled.clone(), CatalogOptions::default())
                    .await
                    .unwrap();
                let start = Instant::now();
                let view = probe.snapshot().await.unwrap();
                samples.push(start.elapsed());
                std::hint::black_box(&view);
                probe.close().await.unwrap();
            }

            let stats = Stats::of(samples);
            println!(
                "{latency_ms:>10} ms  {:>11.3}  {:>9.3}",
                stats.median_ms, stats.max_ms
            );
        }
    }
    println!();
}

/// 0021 — does a cold attach pay for the `index` subspace it never scans?
///
/// A production store measured 3.364 GB in `index` (75.6M live entries)
/// against ~13 MB across every subspace a reader touches, and still took
/// minutes to attach read-only. Materialization scans `current` and point-
/// reads `sys`/`snapshot`, so on the design nothing should read `index` at
/// all — this holds the reader-visible subspaces fixed and grows `index`
/// alone to find out whether that holds in fact.
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn measure_attach_cost_by_index_size() {
    const BATCH: u64 = 8_192;
    const REPEATS: usize = 5;
    let commit_ladder = [0usize, 8, 32, 128];

    println!("\n# 0021 cold read-only attach vs. `index` subspace size");
    println!("# reader-visible subspaces held fixed; only `index` grows\n");
    println!(
        "{:>8}  {:>11}  {:>10}  {:>9}  {:>13}  {:>9}  {:>9}",
        "entries", "index_bytes", "index_ssts", "all_ssts", "manifest_bytes", "open_ms", "view_ms"
    );

    for &commits in &commit_ladder {
        let store = Arc::new(InMemory::new());
        let catalog = open_with(store.clone(), SEED_FLUSH_MS).await;

        let created = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                let table = tx.create_table(schema, "t", &[col("a")])?;
                let def = IndexDef {
                    name: "idx".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                };
                created.set(Some((table, tx.create_index(table, &def, &[])?)));
                Ok(())
            })
            .await
            .unwrap();
        let (table, index) = created.get().expect("index created");

        // Every commit registers one data file — one `current` row — and
        // BATCH index entries. `current` therefore grows by one row per
        // commit while `index` grows by thousands, which is the production
        // store's shape in miniature.
        catalog.close().await.unwrap();
        for k in 0..commits {
            let catalog = open_with(store.clone(), SEED_FLUSH_MS).await;
            let k = k as u64;
            let entries: Vec<FileIndexEntry> = (0..BATCH)
                .map(|ordinal| FileIndexEntry {
                    index,
                    ordinal,
                    values: vec![Some(IndexKeyValue::Int {
                        value: i128::from(k * BATCH + ordinal),
                        width: IntWidth::I64,
                    })],
                })
                .collect();
            catalog
                .commit(move |tx| {
                    let file = DataFile {
                        path: format!("f{k}.parquet"),
                        ..datafile(BATCH)
                    };
                    tx.register_data_file(table, file, &entries)?;
                    Ok(())
                })
                .await
                .unwrap();
            catalog.close().await.unwrap();
        }

        let probe = open_reader(store.clone()).await;
        let census = probe.store_census(CensusRequest::default()).await.unwrap();
        let weight = |name: &SubspaceName| {
            census
                .subspaces
                .iter()
                .find(|s| &s.subspace == name)
                .map_or((0, 0), |s| (s.bytes, s.l0_ssts + s.sorted_run_ssts))
        };
        let (index_bytes, index_ssts) = weight(&SubspaceName::Index);
        let all_ssts: u32 = census
            .subspaces
            .iter()
            .map(|s| s.l0_ssts + s.sorted_run_ssts)
            .sum();
        // The manifest lists every SST in every segment and is read whole
        // on every attach, before any segment routing applies.
        let manifest_bytes = census.objects.map_or(0, |o| o.manifest_bytes);
        probe.close().await.unwrap();

        // Open and first view are timed apart: the design says neither
        // should touch `index`, and if one of them does this says which.
        let mut opens = Vec::with_capacity(REPEATS);
        let mut views = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let start = Instant::now();
            let probe = open_reader(store.clone()).await;
            opens.push(start.elapsed());

            let start = Instant::now();
            let view = probe.snapshot().await.unwrap();
            views.push(start.elapsed());
            std::hint::black_box(&view);
            probe.close().await.unwrap();
        }

        println!(
            "{:>8}  {index_bytes:>11}  {index_ssts:>10}  {all_ssts:>9}  {manifest_bytes:>13}  \
             {:>9.3}  {:>9.3}",
            commits as u64 * BATCH,
            Stats::of(opens).median_ms,
            Stats::of(views).median_ms
        );
    }
    println!();
}

/// 0004 — durable-commit latency versus flush interval.
///
/// `Catalog::commit` awaits durability, and on the in-memory store the WAL
/// "flush" is driven by the handle's flush-poll timer, not an immediate
/// write. So the durable wait is dominated by the flush interval, and this
/// sweep exposes that: the compute floor shows at a 1 ms interval, and the
/// default 100 ms interval shows the realistic per-commit latency. A real
/// object store replaces the poll wait with its WAL-object PUT round-trip;
/// this is the term the 0021 sweep multiplies by its commit count.
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_commit_latency_by_flush_interval() {
    const COMMITS: usize = 60;
    let intervals = [1u64, 10, 50, 100, 250];

    println!("\n# 0004 durable-commit latency by flush interval (in-memory)");
    println!("# {COMMITS} sequential one-table commits, each await_durable\n");
    println!(
        "{:>10}  {:>11}  {:>9}  {:>9}",
        "flush_ms", "median_ms", "min_ms", "max_ms"
    );

    for &flush_ms in &intervals {
        let store = Arc::new(InMemory::new());
        let catalog = open_with(store, flush_ms).await;
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                tx.create_table(schema, "warmup", &[col("a")])?;
                Ok(())
            })
            .await
            .unwrap();

        let mut samples = Vec::with_capacity(COMMITS);
        for i in 0..COMMITS {
            let start = Instant::now();
            catalog
                .commit(move |tx| {
                    let schema = tx.schema_by_name("main").expect("bootstrap").id;
                    tx.create_table(schema, &format!("t{i}"), &[col("a")])?;
                    Ok(())
                })
                .await
                .unwrap();
            samples.push(start.elapsed());
        }
        catalog.close().await.unwrap();

        let stats = Stats::of(samples);
        println!(
            "{flush_ms:>10}  {:>11.3}  {:>9.3}  {:>9.3}",
            stats.median_ms, stats.min_ms, stats.max_ms
        );
    }
    println!();
}

/// 0004 — commit throughput as concurrency rises, and where the batch's
/// member ceiling starts to bind.
///
/// A lone commit costs one WAL flush, so sequential throughput is
/// `1 / flush_interval` and nothing else — that is the number
/// `measure_commit_latency_by_flush_interval` already records. This asks
/// the question that one cannot: what concurrent commits cost, now that
/// they coalesce. K callers commit `COMMITS / K` times each, every caller
/// appending to a table of its own so nothing conflicts, and the harness
/// counts the WAL objects the burst wrote — SlateDB's durable-commit unit,
/// so `commits / wal_writes` is the mean batch size, measured rather than
/// inferred.
///
/// Run at the default flush interval, because the flush in flight *is* the
/// batching window: a faster interval closes the window sooner and batches
/// less. The ladder runs past `MAX_BATCH_MEMBERS` (64) so the ceiling shows
/// up as the batch size flattening while concurrency keeps climbing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_commit_throughput_by_concurrency() {
    const COMMITS: usize = 128;
    let ladder = [1usize, 2, 4, 8, 16, 32, 64, 128];

    println!("\n# 0004 commit throughput by concurrency (in-memory object_store)");
    println!(
        "# {COMMITS} appends per level at the {DEFAULT_FLUSH_MS} ms default flush interval, \
         one table per caller,"
    );
    println!("# 4-worker runtime; batch size is commits per WAL object written\n");
    println!(
        "{:>12}  {:>11}  {:>12}  {:>12}  {:>12}",
        "concurrency", "wall_ms", "commits_per_s", "wal_writes", "batch_size"
    );

    for &concurrency in &ladder {
        let store = Arc::new(FreezingStore::thawed(Arc::new(InMemory::new())));
        let mut options = CatalogOptions::default();
        options.commit_batch_window = Duration::from_millis(DEFAULT_FLUSH_MS);
        let catalog = Catalog::open(store.clone(), options).await.unwrap();

        // One table per caller, created up front so the timed burst is
        // appends only.
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                for t in 0..concurrency {
                    tx.create_table(schema, &format!("t{t}"), &[col("a")])?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let head = catalog.snapshot().await.unwrap();
        let schema = head.schema_by_name("main").unwrap().id;
        let tables: Vec<_> = (0..concurrency)
            .map(|t| head.table_by_name(schema, &format!("t{t}")).unwrap().id)
            .collect();

        let per_caller = COMMITS / concurrency;
        let wal_before = store.wal_writes();
        let start = Instant::now();
        let callers: Vec<_> = tables
            .into_iter()
            .map(|table| {
                let catalog = catalog.clone();
                tokio::spawn(async move {
                    for _ in 0..per_caller {
                        catalog
                            .commit(move |tx| {
                                tx.register_data_file(table, datafile(10), &[]).map(|_| ())
                            })
                            .await
                            .unwrap();
                    }
                })
            })
            .collect();
        for caller in callers {
            caller.await.unwrap();
        }
        let elapsed = start.elapsed();
        let wal_writes = store.wal_writes() - wal_before;
        catalog.close().await.unwrap();

        let wall_ms = elapsed.as_secs_f64() * 1_000.0;
        let landed = (per_caller * concurrency) as f64;
        let commits_per_s = landed / elapsed.as_secs_f64();
        let batch_size = landed / wal_writes.max(1) as f64;
        println!(
            "{concurrency:>12}  {wall_ms:>11.1}  {commits_per_s:>12.1}  \
             {wal_writes:>12}  {batch_size:>12.1}"
        );
    }
    println!();
}

/// 0004 — durable-commit latency against a write round-trip, which is what
/// a real object store adds and the in-memory store has none of.
///
/// `measure_commit_latency_by_flush_interval` settles the in-memory shape:
/// `flush_interval + ~2 ms`. The open question is the other term — an S3
/// PUT's round trip — and whether the two compose as `max(flush cadence,
/// write RTT) + ~2 ms` or add up. This injects the round trip directly, by
/// wrapping the in-memory store in a `ThrottledStore` that sleeps before
/// every PUT, and sweeps it against the flush interval. A `max` composition
/// prints a table whose cells track the larger of the two; an additive one
/// prints their sum.
///
/// The injected latency is the honest instrument here: a localhost MinIO
/// understates a real S3 PUT by an order of magnitude, so measuring against
/// it would answer a question nobody asked. `object_storage.rs` carries the
/// endpoint-backed run for the absolute number.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_commit_latency_by_write_rtt() {
    const COMMITS: usize = 30;
    let flush_intervals = [1u64, 25, 100];
    let round_trips = [0u64, 5, 25, 100];

    println!("\n# 0004 durable-commit latency by injected write round-trip");
    println!("# {COMMITS} sequential commits per cell, median ms; columns are flush intervals\n");
    print!("{:>10}", "put_rtt_ms");
    for flush_ms in flush_intervals {
        print!("  {:>12}", format!("flush={flush_ms}ms"));
    }
    println!();

    for rtt_ms in round_trips {
        print!("{rtt_ms:>10}");
        for flush_ms in flush_intervals {
            let config = ThrottleConfig {
                wait_put_per_call: Duration::from_millis(rtt_ms),
                ..ThrottleConfig::default()
            };
            let store = Arc::new(ThrottledStore::new(InMemory::new(), config));
            let mut options = CatalogOptions::default();
            options.commit_batch_window = Duration::from_millis(flush_ms);
            let catalog = Catalog::open(store, options).await.unwrap();

            let mut samples = Vec::with_capacity(COMMITS);
            for i in 0..COMMITS {
                let start = Instant::now();
                catalog
                    .commit(move |tx| {
                        let schema = tx.schema_by_name("main").expect("bootstrap").id;
                        tx.create_table(schema, &format!("t{i}"), &[col("a")])?;
                        Ok(())
                    })
                    .await
                    .unwrap();
                samples.push(start.elapsed());
            }
            catalog.close().await.unwrap();
            print!("  {:>12.1}", Stats::of(samples).median_ms);
        }
        println!();
    }
    println!();
}

/// 0021 — reclaim throughput versus maintenance batch size.
///
/// Seeds a dropped (dead) non-unique index carrying many entries, then
/// times a full sweep at a ladder of batch sizes. Each batch is one durable
/// commit, so at the default flush interval reclaim time is roughly
/// (commit count x per-commit durable latency) + entry compute — and
/// commit count is entries/batch. The measured curve says where enlarging
/// the batch stops buying anything, which is the defensible-default
/// question. Seeded at a fast interval (untimed), swept at the default.
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_reclaim_by_batch_size() {
    const DEAD_ENTRIES: u64 = 50_000;
    const REPEATS: usize = 3;

    async fn seed(store: Arc<InMemory>) {
        let catalog = open_with(store, SEED_FLUSH_MS).await;
        let index = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                let table = tx.create_table(schema, "t", &[col("a")])?;
                let def = IndexDef {
                    name: "idx".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                };
                let entries: Vec<_> = (0..DEAD_ENTRIES)
                    .map(|row_id| IndexEntry {
                        row_id,
                        values: vec![Some(IndexKeyValue::Int {
                            value: i128::from(row_id),
                            width: IntWidth::I64,
                        })],
                    })
                    .collect();
                index.set(Some(tx.create_index(table, &def, &entries)?));
                Ok(())
            })
            .await
            .unwrap();
        let index = index.get().expect("index created");
        catalog
            .commit(move |tx| tx.drop_index(index))
            .await
            .unwrap();
        catalog.close().await.unwrap();
    }

    let ladder = [256usize, 1024, 4096, 16384, 65536];

    println!("\n# 0021 reclaim sweep by batch size (in-memory object_store)");
    println!(
        "# {DEAD_ENTRIES} dead entries in one index, median of {REPEATS} sweeps at the \
         {DEFAULT_FLUSH_MS} ms default flush interval;"
    );
    println!("# commits is analytic ceil(entries/batch)\n");
    println!(
        "{:>10}  {:>8}  {:>11}  {:>9}  {:>9}  {:>14}",
        "batch", "commits", "median_ms", "min_ms", "max_ms", "entries_per_s"
    );

    for &batch in &ladder {
        let mut samples = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let store = Arc::new(InMemory::new());
            seed(store.clone()).await;

            let mut request = MaintenanceRequest::default();
            request.batch_size = batch;

            let catalog = open_with(store, DEFAULT_FLUSH_MS).await;
            let start = Instant::now();
            let report = catalog.maintain(request).await.unwrap();
            let elapsed = start.elapsed();
            assert_eq!(report.index_entries_reclaimed, DEAD_ENTRIES);
            samples.push(elapsed);
            catalog.close().await.unwrap();
        }

        let commits = DEAD_ENTRIES.div_ceil(batch as u64);
        let stats = Stats::of(samples);
        let entries_per_s = f64::from(u32::try_from(DEAD_ENTRIES).unwrap_or(u32::MAX))
            / (stats.median_ms / 1_000.0);
        println!(
            "{batch:>10}  {commits:>8}  {:>11.3}  {:>9.3}  {:>9.3}  {entries_per_s:>14.0}",
            stats.median_ms, stats.min_ms, stats.max_ms
        );
    }
    println!();
}

/// 0010 — does object-store read latency starve the shared worker pool?
///
/// The concern is a multi-threaded runtime whose worker threads all block
/// on slow object-store reads, so concurrent scans serialize instead of
/// overlapping their waits. This wraps the in-memory store in a
/// `ThrottledStore` that adds a fixed latency to every GET/LIST, seeds a
/// small catalog *unthrottled*, then runs K concurrent `snapshot()`
/// materializations on a fixed 4-worker runtime and times the batch.
///
/// If the IO awaits yield the worker back to the pool, K concurrent
/// materializations overlap their latency and the batch stays near the
/// time of one — flat as K grows past the worker count. If SlateDB blocked
/// a worker for the duration of each read, the batch would grow with K once
/// the workers are exhausted. This isolates the IO-latency axis only;
/// CPU-bound decode monopolizing a worker is a separate question this does
/// not probe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_read_concurrency_under_io_latency() {
    const READ_LATENCY_MS: u64 = 10;
    const TABLES: usize = 20;
    const REPEATS: usize = 5;
    let concurrency = [1usize, 2, 4, 8, 16, 32];

    // Seed unthrottled: writes are not the axis under test.
    let raw = Arc::new(InMemory::new());
    {
        let catalog = open_with(raw.clone(), SEED_FLUSH_MS).await;
        for t in 0..TABLES {
            catalog
                .commit(move |tx| {
                    let schema = tx.schema_by_name("main").expect("bootstrap").id;
                    let table = tx.create_table(schema, &format!("t{t}"), &[col("a")])?;
                    for _ in 0..8 {
                        tx.register_data_file(table, datafile(100), &[])?;
                    }
                    Ok(())
                })
                .await
                .unwrap();
        }
        catalog.close().await.unwrap();
    }

    let config = ThrottleConfig {
        wait_get_per_call: Duration::from_millis(READ_LATENCY_MS),
        wait_list_per_call: Duration::from_millis(READ_LATENCY_MS),
        ..ThrottleConfig::default()
    };
    // A derived `InMemory` clone shares the backing store (only `fork`
    // copies), so the throttled handle sees the seeded data.
    let throttled = Arc::new(ThrottledStore::new((*raw).clone(), config));
    let mut options = CatalogOptions::default();
    options.commit_batch_window = Duration::from_millis(DEFAULT_FLUSH_MS);
    let catalog = Catalog::open(throttled, options).await.unwrap();

    println!("\n# 0010 read concurrency under {READ_LATENCY_MS} ms IO latency");
    println!("# {TABLES}-table catalog, 4-worker runtime, median of {REPEATS} batches\n");
    println!(
        "{:>12}  {:>11}  {:>13}",
        "concurrency", "batch_ms", "per_op_ms"
    );

    for &k in &concurrency {
        let mut samples = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let start = Instant::now();
            let handles: Vec<_> = (0..k)
                .map(|_| {
                    let catalog = catalog.clone();
                    tokio::spawn(async move { catalog.snapshot().await.map(|_| ()) })
                })
                .collect();
            for handle in handles {
                handle.await.unwrap().unwrap();
            }
            samples.push(start.elapsed());
        }
        let stats = Stats::of(samples);
        let per_op = stats.median_ms / k as f64;
        println!("{k:>12}  {:>11.3}  {per_op:>13.3}", stats.median_ms);
    }
    catalog.close().await.unwrap();
    println!();
}

/// 0016 — unique-index maintenance cost per commit as the store grows,
/// scattered versus clustered indexed values.
///
/// Uniqueness is resolved by bounded-concurrency point reads at every batch
/// size. Each probe is bloom-filtered, so its cost does not grow with the
/// index, and the fan-out overlaps latency — so per-commit cost should stay
/// flat as the store grows, and stay the same whether a commit's values
/// scatter across the whole index or cluster in a contiguous new range. This
/// runs the same successive-commit load under both distributions over an
/// identical value set and prints per-commit time against cumulative store
/// size. A ratio near one, flat across the sweep, is the expected result; a
/// scattered curve that climbed with store size would signal a regression
/// toward store-proportional resolution.
///
/// Timed at the fast seed interval, so each commit's durable wait is ~1 ms
/// and the reported time is index-maintenance compute plus store reads, not
/// the flush poll.
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_index_maintenance_by_store_size() {
    // Well above one probe group (1024), so each commit resolves across
    // several bounded-concurrency point-read groups.
    const BATCH: u64 = 8_192;
    const COMMITS: usize = 32;

    // Both distributions index the same value set — a permutation of
    // `0..COMMITS * BATCH` — and differ only in which commit carries which
    // value. Clustered gives commit `k` the contiguous block
    // `[k * BATCH, (k + 1) * BATCH)`; scattered gives it the residue class
    // `{ k + COMMITS * j : j in 0..BATCH }`, spread evenly across the whole
    // range every commit. Same total work, opposite stored layout.
    async fn run(scattered: bool) -> Vec<Duration> {
        let store = Arc::new(InMemory::new());
        let catalog = open_with(store, SEED_FLUSH_MS).await;

        let created = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                let table = tx.create_table(schema, "t", &[col("a")])?;
                let def = IndexDef {
                    name: "idx".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                };
                created.set(Some((table, tx.create_index(table, &def, &[])?)));
                Ok(())
            })
            .await
            .unwrap();
        let (table, index) = created.get().expect("index created");

        let mut samples = Vec::with_capacity(COMMITS);
        for k in 0..COMMITS {
            let k = k as u64;
            let entries: Vec<FileIndexEntry> = (0..BATCH)
                .map(|ordinal| {
                    let value = if scattered {
                        k + COMMITS as u64 * ordinal
                    } else {
                        k * BATCH + ordinal
                    };
                    FileIndexEntry {
                        index,
                        ordinal,
                        values: vec![Some(IndexKeyValue::Int {
                            value: i128::from(value),
                            width: IntWidth::I64,
                        })],
                    }
                })
                .collect();

            let start = Instant::now();
            catalog
                .commit(move |tx| {
                    let file = DataFile {
                        path: format!("f{k}.parquet"),
                        ..datafile(BATCH)
                    };
                    tx.register_data_file(table, file, &entries)?;
                    Ok(())
                })
                .await
                .unwrap();
            samples.push(start.elapsed());
        }
        catalog.close().await.unwrap();
        samples
    }

    let clustered = run(false).await;
    let scattered = run(true).await;

    println!("\n# 0016 unique-index maintenance per commit by store size (in-memory object_store)");
    println!(
        "# {BATCH} rows per commit (> merged-probe threshold), {COMMITS} commits, fast flush\n"
    );
    println!(
        "{:>7}  {:>10}  {:>13}  {:>13}  {:>7}",
        "commit", "cum_rows", "clustered_ms", "scattered_ms", "ratio"
    );
    let ms = |d: Duration| d.as_secs_f64() * 1_000.0;
    for k in 0..COMMITS {
        let cum_rows = (k as u64 + 1) * BATCH;
        let (clustered_ms, scattered_ms) = (ms(clustered[k]), ms(scattered[k]));
        let ratio = if clustered_ms > 0.0 {
            scattered_ms / clustered_ms
        } else {
            0.0
        };
        println!(
            "{:>7}  {cum_rows:>10}  {clustered_ms:>13.3}  {scattered_ms:>13.3}  {ratio:>7.2}",
            k + 1
        );
    }
    println!();
}

/// 0010 — whether a CPU-bound decode monopolizes a worker.
///
/// `measure_read_concurrency_under_io_latency` settles the IO half: awaits
/// on object-store latency yield their worker, so slow IO does not starve
/// the pool. The other half is compute. A cold materialization decodes
/// every live record, and a decode does not yield: whichever worker polls
/// it holds that worker until the whole scan is done. If that is enough to
/// crowd the pool, a small latency-sensitive call queued alongside would
/// wait behind a whole decode — which is the one place a `spawn_blocking`
/// discipline would earn its complexity.
///
/// The harness runs on a fixed 4-worker runtime over an unthrottled
/// in-memory store, so every millisecond measured is compute. It reports,
/// per concurrency level, how long K cold materializations take together
/// and how long a *small* read issued alongside them takes — against the
/// same small read with the pool idle. A small read whose latency tracks
/// the decode's duration is a monopolized worker; one that stays flat is a
/// pool that keeps its head above the load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used)]
async fn measure_cpu_bound_decode_under_worker_pressure() {
    const HEAVY_TABLES: usize = 400;
    const COLS_PER_TABLE: usize = 8;
    const FILES_PER_TABLE: usize = 16;
    const REPEATS: usize = 5;
    let concurrency = [1usize, 2, 4, 8];

    // The decode-heavy catalog, and a tiny one beside it for the
    // latency-sensitive read. Separate stores, one runtime: the small read
    // competes for workers, never for locks.
    let heavy_store = Arc::new(InMemory::new());
    {
        let catalog = open_with(heavy_store.clone(), SEED_FLUSH_MS).await;
        let columns: Vec<_> = (0..COLS_PER_TABLE).map(|c| col(&format!("c{c}"))).collect();
        for t in 0..HEAVY_TABLES {
            let columns = columns.clone();
            catalog
                .commit(move |tx| {
                    let schema = tx.schema_by_name("main").expect("bootstrap").id;
                    let table = tx.create_table(schema, &format!("t{t}"), &columns)?;
                    for _ in 0..FILES_PER_TABLE {
                        tx.register_data_file(table, datafile(100), &[])?;
                    }
                    Ok(())
                })
                .await
                .unwrap();
        }
        catalog.close().await.unwrap();
    }
    let small_store = Arc::new(InMemory::new());
    {
        let catalog = open_with(small_store.clone(), SEED_FLUSH_MS).await;
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                tx.create_table(schema, "one", &[col("a")]).map(|_| ())
            })
            .await
            .unwrap();
        catalog.close().await.unwrap();
    }

    // Read-only handles: they maintain no projection cache, so every
    // `snapshot()` is a full cold materialization rather than a cache hit,
    // which is the decode this is measuring.
    let heavy = Catalog::open_read_only(heavy_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let small = Catalog::open_read_only(small_store.clone(), CatalogOptions::default())
        .await
        .unwrap();

    // The small read alone, for the load below to be read against.
    let mut idle = Vec::with_capacity(REPEATS);
    for _ in 0..REPEATS {
        let start = Instant::now();
        std::hint::black_box(small.snapshot().await.unwrap());
        idle.push(start.elapsed());
    }
    let idle = Stats::of(idle);

    println!("\n# 0010 CPU-bound decode under worker pressure (in-memory object_store)");
    println!(
        "# {HEAVY_TABLES} tables x ({COLS_PER_TABLE} columns + {FILES_PER_TABLE} files), \
         4-worker runtime, median of {REPEATS} batches"
    );
    println!("# small read with an idle pool: {:.3} ms\n", idle.median_ms);
    println!(
        "{:>12}  {:>11}  {:>13}  {:>13}  {:>9}",
        "concurrency", "batch_ms", "per_decode_ms", "small_read_ms", "vs_idle"
    );

    for &k in &concurrency {
        let mut batches = Vec::with_capacity(REPEATS);
        let mut smalls = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let start = Instant::now();
            let decoders: Vec<_> = (0..k)
                .map(|_| {
                    let heavy = heavy.clone();
                    tokio::spawn(async move { heavy.snapshot().await.map(|_| ()) })
                })
                .collect();
            // Queued behind K decodes already in the pool's run queue.
            let probe = {
                let small = small.clone();
                tokio::spawn(async move {
                    let start = Instant::now();
                    small.snapshot().await.map(|_| ())?;
                    Ok::<_, moraine::Error>(start.elapsed())
                })
            };
            smalls.push(probe.await.unwrap().unwrap());
            for decoder in decoders {
                decoder.await.unwrap().unwrap();
            }
            batches.push(start.elapsed());
        }
        let batch = Stats::of(batches);
        let smalls = Stats::of(smalls);
        let per_decode = batch.median_ms / k as f64;
        let vs_idle = smalls.median_ms / idle.median_ms;
        println!(
            "{k:>12}  {:>11.3}  {per_decode:>13.3}  {:>13.3}  {vs_idle:>9.1}",
            batch.median_ms, smalls.median_ms
        );
    }
    heavy.close().await.unwrap();
    small.close().await.unwrap();
    println!();
}

/// Runs `worker` on each of `threads` tasks and prints one row per rung:
/// the per-read cost one caller sees, and the rate all of them together
/// sustain. The gap between rungs is what a shared resource is doing.
#[allow(clippy::unwrap_used)]
async fn read_ladder(
    label: &str,
    concurrency: &[usize],
    reads: usize,
    worker: impl Fn() -> tokio::task::JoinHandle<()>,
) {
    println!("\n# {label}");
    println!(
        "{:>8}  {:>14}  {:>16}",
        "threads", "us_per_read", "total_reads_per_s"
    );
    for &threads in concurrency {
        let started = Instant::now();
        let handles: Vec<_> = (0..threads).map(|_| worker()).collect();
        for handle in handles {
            handle.await.unwrap();
        }

        let elapsed = started.elapsed().as_secs_f64();
        let total = (threads * reads) as f64;
        println!(
            "{threads:>8}  {:>14.2}  {:>16.0}",
            elapsed * 1_000_000.0 * threads as f64 / total,
            total / elapsed
        );
    }
}

/// 0009 — what a warm read on a read-write handle costs, and where.
///
/// A warm read no longer resolves the head from the store, probes
/// `sys/migration`, or opens a session at all: it checks the writer's
/// status channel for a fence and hands back the held view. So what is
/// left is a watch borrow, and the figure that matters is whether it
/// scales where opening a session did not.
///
/// Three ladders: the read-write handle, a read-only one (which holds no
/// writer-local premise, so it opens a session and issues both point reads
/// before it can serve its cache), and the floor of handing back the view
/// with no check at all.
///
/// In-memory `object_store`, so a remote store's per-GET latency is absent
/// by construction — which is the point: what is left here is lock and
/// compute, and anything a production trace shows above it is IO the warm
/// path no longer issues.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement harness"]
async fn measure_warm_read_attribution() {
    const TABLES: usize = 50;
    const READS: usize = 2_000;
    let concurrency = [1usize, 2, 4, 8, 16, 24];

    let store = Arc::new(InMemory::new());
    let catalog = open_with(store.clone(), SEED_FLUSH_MS).await;
    let columns: Vec<_> = (0..8).map(|c| col(&format!("c{c}"))).collect();
    for t in 0..TABLES {
        let columns = columns.clone();
        catalog
            .commit(move |tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                let table = tx.create_table(schema, &format!("t{t}"), &columns)?;
                for _ in 0..16 {
                    tx.register_data_file(table, datafile(100), &[])?;
                }
                Ok(())
            })
            .await
            .unwrap();
    }
    // Warm the handle: from here every read below is served from the view.
    catalog.snapshot().await.unwrap();

    println!("\n# 0009 warm read (in-memory object_store)");
    println!("# {TABLES} tables, {READS} reads per thread, all served from a held view");

    let catalog = Arc::new(catalog);
    read_ladder(
        "Read-write handle (fence check, then the held view):",
        &concurrency,
        READS,
        || {
            let catalog = Arc::clone(&catalog);
            tokio::spawn(async move {
                for _ in 0..READS {
                    catalog.snapshot().await.unwrap();
                }
            })
        },
    )
    .await;

    let reader = Arc::new(open_reader(store.clone()).await);
    reader.snapshot().await.unwrap();
    read_ladder(
        "Read-only handle (session, then two point reads):",
        &concurrency,
        READS,
        || {
            let reader = Arc::clone(&reader);
            tokio::spawn(async move {
                for _ in 0..READS {
                    reader.snapshot().await.unwrap();
                }
            })
        },
    )
    .await;

    // The floor: handing back the same `Arc` with no check around it. The
    // gap to the first ladder is what checking the fence costs, which is
    // all a warm read does beyond serving the view.
    let view = catalog.snapshot().await.unwrap();
    read_ladder(
        "The held view alone, no fence check:",
        &concurrency,
        READS,
        || {
            let view = Arc::clone(&view);
            tokio::spawn(async move {
                for _ in 0..READS {
                    std::hint::black_box(Arc::clone(&view));
                }
            })
        },
    )
    .await;
}

/// 0009 — how many round trips a warm read-only read costs.
///
/// A read-only handle holds no writer-local premise, so serving a cache
/// hit still means asking the store two questions: whether a structural
/// migration is in flight, and where head is. They are independent, and
/// are now issued together rather than one after the other.
///
/// The number of *round trips* is what that changes, and a get has to cost
/// something before round trips are visible — so this injects per-GET
/// latency and reads the answer off the ratio. One injected latency per
/// warm read means one round trip; two means the reads serialized.
#[tokio::test]
#[ignore = "measurement harness"]
async fn measure_reader_round_trips_under_get_latency() {
    const TABLES: usize = 20;
    const REPEATS: usize = 9;
    let latencies = [2u64, 5, 10, 20];

    let store = Arc::new(InMemory::new());
    let writer = open_with(store.clone(), SEED_FLUSH_MS).await;
    let columns: Vec<_> = (0..4).map(|c| col(&format!("c{c}"))).collect();
    for t in 0..TABLES {
        let columns = columns.clone();
        writer
            .commit(move |tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                tx.create_table(schema, &format!("t{t}"), &columns)?;
                Ok(())
            })
            .await
            .unwrap();
    }
    writer.close().await.unwrap();

    println!("\n# 0009 warm read-only read, per-GET latency injected");
    println!("# {TABLES} tables, median of {REPEATS} warm reads\n");
    println!(
        "{:>12}  {:>11}  {:>9}  {:>9}  {:>13}",
        "get_latency", "median_ms", "min_ms", "max_ms", "round_trips"
    );

    for &latency_ms in &latencies {
        let config = ThrottleConfig {
            wait_get_per_call: Duration::from_millis(latency_ms),
            ..ThrottleConfig::default()
        };
        let throttled = Arc::new(ThrottledStore::new((*store).clone(), config));
        let throttled: Arc<dyn object_store::ObjectStore> = throttled;

        // The poller is held off so its own gets stay out of the window.
        let mut options = CatalogOptions::default();
        options.reader_poll_interval = Duration::from_secs(60);
        let reader = Catalog::open_read_only(Arc::clone(&throttled), options)
            .await
            .unwrap();
        // Warm: from here a read is two point reads and a cache hit.
        reader.snapshot().await.unwrap();

        let mut samples = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let start = Instant::now();
            let view = reader.snapshot().await.unwrap();
            samples.push(start.elapsed());
            std::hint::black_box(&view);
        }
        let stats = Stats::of(samples);
        reader.close().await.unwrap();

        // The same read on a handle that has never read: nothing of the
        // store is in its block cache, so any round trip the two point
        // reads owe is owed here.
        let mut cold = Vec::with_capacity(REPEATS);
        for _ in 0..REPEATS {
            let mut options = CatalogOptions::default();
            options.reader_poll_interval = Duration::from_secs(60);
            let fresh = Catalog::open_read_only(Arc::clone(&throttled), options)
                .await
                .unwrap();
            let start = Instant::now();
            let view = fresh.snapshot().await.unwrap();
            cold.push(start.elapsed());
            std::hint::black_box(&view);
            fresh.close().await.unwrap();
        }
        let cold = Stats::of(cold);
        println!(
            "{:>10} ms  {:>11.2}  {:>9.2}  {:>9.2}  {:>13.2}   (cold handle)",
            latency_ms,
            cold.median_ms,
            cold.min_ms,
            cold.max_ms,
            cold.median_ms / latency_ms as f64
        );

        println!(
            "{:>10} ms  {:>11.2}  {:>9.2}  {:>9.2}  {:>13.2}",
            latency_ms,
            stats.median_ms,
            stats.min_ms,
            stats.max_ms,
            stats.median_ms / latency_ms as f64
        );
    }
}

/// 0009 — what an index probe fetches, cold and warm.
///
/// The design replaced a part-grained object cache with a block-grained
/// one and argued the swap from grain arithmetic: a probe into a large
/// `index` run touches a handful of blocks, where a part cache faulted
/// whole 4 MiB parts to serve the same lookup. This puts bytes under that.
///
/// Read the `bytes/probe` column, not the milliseconds: on an in-memory
/// store a fetch costs no round trip, so time here is decode and nothing
/// else — while against real storage the bytes *are* the cost. A cold
/// probe fetching block-sized bytes rather than part-sized ones is the
/// whole claim.
///
/// Warm rows are the same probes repeated. They should fetch nothing:
/// the blocks are resident, which is what the block slot is for.
#[tokio::test]
#[ignore = "measurement, not a test: run with --ignored --nocapture"]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
async fn measure_probe_cost_by_index_size() {
    const BATCH: u64 = 8_192;
    const PROBES: u64 = 32;
    let commit_ladder = [1usize, 8, 32, 128];

    println!("\n# 0009 index-probe cost against the shared block cache");
    println!("# in-memory store: bytes are the transferable number, ms is decode only\n");
    println!(
        "{:>8}  {:>11}  {:>10}  {:>12}  {:>13}  {:>10}  {:>12}  {:>9}",
        "entries",
        "index_bytes",
        "cold_gets",
        "cold_bytes/p",
        "cold_ms/probe",
        "warm_gets",
        "warm_bytes/p",
        "warm_ms/p"
    );

    for &commits in &commit_ladder {
        let inner = Arc::new(InMemory::new());
        let counting = Arc::new(crate::counting_store::CountingStore::new(Arc::clone(
            &inner,
        )));
        let catalog = Catalog::open(
            Arc::clone(&counting) as Arc<dyn object_store::ObjectStore>,
            CatalogOptions::default(),
        )
        .await
        .unwrap();

        let created = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                let table = tx.create_table(schema, "t", &[col("a")])?;
                let def = IndexDef {
                    name: "idx".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                };
                created.set(Some((table, tx.create_index(table, &def, &[])?)));
                Ok(())
            })
            .await
            .unwrap();
        let (table, index) = created.get().expect("index created");

        for k in 0..commits {
            let k = k as u64;
            let entries: Vec<FileIndexEntry> = (0..BATCH)
                .map(|ordinal| FileIndexEntry {
                    index,
                    ordinal,
                    values: vec![Some(IndexKeyValue::Int {
                        value: i128::from(k * BATCH + ordinal),
                        width: IntWidth::I64,
                    })],
                })
                .collect();
            catalog
                .commit(move |tx| {
                    let file = DataFile {
                        path: format!("f{k}.parquet"),
                        ..datafile(BATCH)
                    };
                    tx.register_data_file(table, file, &entries)?;
                    Ok(())
                })
                .await
                .unwrap();
        }
        catalog.close().await.unwrap();

        let entries = commits as u64 * BATCH;
        let probe = Catalog::open_read_only(
            Arc::clone(&counting) as Arc<dyn object_store::ObjectStore>,
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let census = probe.store_census(CensusRequest::default()).await.unwrap();
        let index_bytes = census
            .subspaces
            .iter()
            .find(|s| s.subspace == SubspaceName::Index)
            .map_or(0, |s| s.bytes);

        // Keys spread across the whole run, so the probes are not all in
        // one block and the cost is not one block's amortized over many.
        let keys: Vec<i128> = (0..PROBES)
            .map(|n| i128::from(n * (entries / PROBES).max(1)))
            .collect();

        let round = |label: &str| {
            let keys = keys.clone();
            let probe = &probe;
            let counting = &counting;
            async move {
                counting.take_reads();
                counting.take_bytes();
                let start = Instant::now();
                for key in keys {
                    let found = probe
                        .index_lookup(
                            table,
                            index,
                            &[IndexKeyValue::Int {
                                value: key,
                                width: IntWidth::I64,
                            }],
                        )
                        .await
                        .unwrap();
                    std::hint::black_box(&found);
                }
                let elapsed = start.elapsed();
                let _ = label;
                (
                    counting.take_reads(),
                    counting.take_bytes(),
                    elapsed.as_secs_f64() * 1_000.0 / PROBES as f64,
                )
            }
        };

        let (cold_gets, cold_bytes, cold_ms) = round("cold").await;
        let (warm_gets, warm_bytes, warm_ms) = round("warm").await;
        probe.close().await.unwrap();

        println!(
            "{entries:>8}  {index_bytes:>11}  {cold_gets:>10}  {:>12}  {cold_ms:>13.4}  \
             {warm_gets:>10}  {:>12}  {warm_ms:>9.4}",
            cold_bytes / PROBES,
            warm_bytes / PROBES,
        );
    }
    println!();
}
