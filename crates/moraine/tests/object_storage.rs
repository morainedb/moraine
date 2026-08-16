//! Real object storage tests: public-API round-trips against S3 or an
//! S3-compatible endpoint. Ignored by default; `cargo xtask s3` starts
//! MinIO and runs them with the endpoint environment set.
//!
//! Run manually against any S3-compatible endpoint:
//!
//! ```text
//! AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
//! AWS_REGION=us-east-1 AWS_ALLOW_HTTP=true \
//! MORAINE_S3_ENDPOINT=http://127.0.0.1:9124 MORAINE_S3_BUCKET=moraine \
//! cargo test -p moraine --test object_storage -- --ignored
//! ```
//!
//! Against AWS, omit `MORAINE_S3_ENDPOINT`, set `MORAINE_S3_BUCKET` and an
//! optional unique `MORAINE_S3_PREFIX`, and use the standard AWS environment
//! variables. The builder accepts temporary session and container-role
//! credentials, so the benchmark needs no static access key.

// The tests-exempt lints (`clippy.toml`) reach `#[test]` functions and
// `#[cfg(test)]` modules, not an integration crate's plain helper
// functions — exempted here instead, crate-wide, as tests.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::{
    ops::Bound,
    sync::Arc,
    time::{Duration, Instant},
};

use moraine::{
    Catalog, CatalogOptions, CensusRequest, ColumnDef, ColumnId, CompactStoreRequest, DataFile,
    FileIndexEntry, IndexDef, IndexId, IndexKeyValue, IntWidth, MergeOutcome, ReadOnlyCatalog,
    SubspaceName, TableId,
};
use object_store::{ObjectStore, aws::AmazonS3Builder};

struct TimingStats {
    median: Duration,
    min: Duration,
    max: Duration,
}

impl TimingStats {
    fn of(mut samples: Vec<Duration>) -> Self {
        samples.sort();
        Self {
            median: samples[samples.len() / 2],
            min: samples[0],
            max: samples[samples.len() - 1],
        }
    }
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

/// Where the suite is pointed, for the conditions line of a measurement.
fn endpoint_label() -> String {
    std::env::var("MORAINE_S3_ENDPOINT").unwrap_or_else(|_| {
        format!(
            "AWS S3 in {}",
            std::env::var("AWS_REGION")
                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                .unwrap_or_else(|_| "the configured region".into())
        )
    })
}

#[test]
fn catalog_path_is_scoped_to_the_run_prefix() {
    assert_eq!(
        catalog_path("runs/42/data/", "reopen"),
        "runs/42/data/reopen"
    );
}

#[test]
fn catalog_path_works_without_a_run_prefix() {
    assert_eq!(catalog_path("", "reopen"), "reopen");
}

#[test]
fn attach_cases_isolate_churned_and_merged_stores() {
    assert_eq!(attach_case_path("churned"), "attach-latency-churned");
    assert_eq!(attach_case_path("merged"), "attach-latency-merged");
}

#[test]
fn timing_stats_reports_middle_and_extrema() {
    let stats = TimingStats::of(vec![
        Duration::from_millis(9),
        Duration::from_millis(2),
        Duration::from_millis(5),
    ]);

    assert_eq!(stats.median, Duration::from_millis(5));
    assert_eq!(stats.min, Duration::from_millis(2));
    assert_eq!(stats.max, Duration::from_millis(9));
}

fn s3_store() -> Arc<dyn ObjectStore> {
    let bucket = std::env::var("MORAINE_S3_BUCKET")
        .expect("MORAINE_S3_BUCKET must be set (see this module's doc comment)");
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let Ok(endpoint) = std::env::var("MORAINE_S3_ENDPOINT") {
        builder = builder.with_endpoint(endpoint);
    }
    Arc::new(builder.build().expect("S3 store from test configuration"))
}

fn catalog_path(prefix: &str, path: &str) -> String {
    match prefix.trim_matches('/') {
        "" => path.to_string(),
        prefix => format!("{prefix}/{path}"),
    }
}

/// Options rooted at a per-test prefix so the suite shares one bucket.
fn options_at(path: &str) -> CatalogOptions {
    let mut options = CatalogOptions::default();
    let prefix = std::env::var("MORAINE_S3_PREFIX").unwrap_or_default();
    options.path = catalog_path(&prefix, path);
    options
}

#[tokio::test]
#[ignore = "needs a live S3 endpoint; run through `cargo xtask s3`"]
async fn bootstrap_commit_and_reopen_on_s3() {
    let store = s3_store();
    let catalog = Catalog::open(store.clone(), options_at("reopen"))
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    catalog.close().await.unwrap();

    // Reopen: state persisted through the real endpoint.
    let catalog = Catalog::open(store, options_at("reopen")).await.unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert!(head.schema_by_name("sales").is_some());
    catalog.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs a live S3 endpoint; run through `cargo xtask s3`"]
async fn read_only_catalog_reads_s3_state() {
    let store = s3_store();
    let catalog = Catalog::open(store.clone(), options_at("reader"))
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("analytics").map(|_| ()))
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let reader = Catalog::open_read_only(store, options_at("reader"))
        .await
        .unwrap();
    let head = reader.snapshot().await.unwrap();
    assert!(head.schema_by_name("analytics").is_some());
    reader.close().await.unwrap();
}

async fn seed_attach_measurement(
    store: Arc<dyn ObjectStore>,
    options: &CatalogOptions,
    tables: usize,
    columns_per_table: usize,
    rounds: usize,
) {
    let mut seed_options = options.clone();
    seed_options.flush_interval = Duration::from_millis(1);
    let catalog = Catalog::open(store.clone(), seed_options.clone())
        .await
        .unwrap();
    let columns: Vec<_> = (0..columns_per_table)
        .map(|column| ColumnDef {
            name: format!("c{column}"),
            column_type: "BIGINT".into(),
            nulls_allowed: true,
            default_value: None,
            children: Vec::new(),
        })
        .collect();
    let mut table_ids = Vec::with_capacity(tables);

    for table in 0..tables {
        let columns = columns.clone();
        let created = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").expect("bootstrap").id;
                created.set(Some(tx.create_table(
                    schema,
                    &format!("t{table}"),
                    &columns,
                )?));
                Ok(())
            })
            .await
            .unwrap();
        table_ids.push(created.get().expect("table created"));
    }
    catalog.close().await.unwrap();

    // A close after every round forces the superseded records into SSTs.
    // Keeping one writer open would leave the churn in its memtable and
    // measure a different, physically small store.
    for round in 0..rounds {
        let catalog = Catalog::open(store.clone(), seed_options.clone())
            .await
            .unwrap();
        let table_ids = table_ids.clone();
        catalog
            .commit(move |tx| {
                for &table in &table_ids {
                    tx.update_table_stats(table, 100 + round as u64, 4_096)?;
                }
                Ok(())
            })
            .await
            .unwrap();
        catalog.close().await.unwrap();
    }
}

async fn measure_fresh_attaches(
    store: Arc<dyn ObjectStore>,
    options: &CatalogOptions,
    repeats: usize,
) -> (TimingStats, TimingStats, TimingStats) {
    let mut opens = Vec::with_capacity(repeats);
    let mut views = Vec::with_capacity(repeats);
    let mut totals = Vec::with_capacity(repeats);

    for _ in 0..repeats {
        let total_start = Instant::now();
        let open_start = Instant::now();
        let reader = Catalog::open_read_only(store.clone(), options.clone())
            .await
            .unwrap();
        opens.push(open_start.elapsed());

        let view_start = Instant::now();
        let view = reader.snapshot().await.unwrap();
        views.push(view_start.elapsed());
        totals.push(total_start.elapsed());
        std::hint::black_box(&view);
        reader.close().await.unwrap();
    }

    (
        TimingStats::of(opens),
        TimingStats::of(views),
        TimingStats::of(totals),
    )
}

const ATTACH_TABLES: usize = 40;
const ATTACH_COLUMNS_PER_TABLE: usize = 8;
const ATTACH_ROUNDS: usize = 160;
const ATTACH_REPEATS: usize = 7;

#[derive(Clone, Copy)]
enum AttachMode {
    LiveFollowing,
    FixedCheckpoint,
}

impl AttachMode {
    fn label(self) -> &'static str {
        match self {
            Self::LiveFollowing => "live",
            Self::FixedCheckpoint => "fixed",
        }
    }
}

fn attach_case_path(state: &str) -> String {
    format!("attach-latency-{state}")
}

async fn compact_attach_store(store: Arc<dyn ObjectStore>, options: &CatalogOptions) -> usize {
    let mut writer_options = options.clone();
    writer_options.flush_interval = Duration::from_millis(1);
    let writer = Catalog::open(store, writer_options).await.unwrap();
    let mut request = CompactStoreRequest::default();
    request.wait = Some(Duration::from_secs(120));
    let report = writer.compact_store(request).await.unwrap();
    let completed = report
        .merges
        .iter()
        .filter(|merge| merge.outcome == MergeOutcome::Completed)
        .count();
    writer.close().await.unwrap();
    completed
}

async fn fixed_checkpoint_options(
    store: Arc<dyn ObjectStore>,
    options: &CatalogOptions,
) -> (CatalogOptions, String) {
    let mut reader_options = options.clone();
    let mut writer_options = options.clone();
    writer_options.flush_interval = Duration::from_millis(1);
    let writer = Catalog::open(store, writer_options).await.unwrap();
    let checkpoint = writer.create_checkpoint(None).await.unwrap();
    writer.close().await.unwrap();
    reader_options.checkpoint = Some(checkpoint.clone());
    (reader_options, checkpoint)
}

/// Runs one endpoint attach instrument over independently seeded churned and
/// merged stores. Separate prefixes keep one state's reader checkpoints from
/// changing the manifest history measured by the other.
async fn measure_attach_latency_against_endpoint() {
    let store = s3_store();
    let target = endpoint_label();

    println!("\n# 0021 fresh-handle attach against {target}");
    println!(
        "# {ATTACH_TABLES} tables x {ATTACH_COLUMNS_PER_TABLE} columns, \
         {ATTACH_ROUNDS} churn rounds; median of {ATTACH_REPEATS} fresh handles"
    );
    println!("# fixed opens an immutable checkpoint and writes nothing");
    println!("# live follows latest and writes a manifest checkpoint; close is not timed");
    println!("# total is open + first materialized view; states use separate prefixes\n");
    println!(
        "{:>7}  {:>10}  {:>13}  {:>12}  {:>11}  {:>11}  {:>11}  {:>9}  {:>9}",
        "reader",
        "state",
        "current_bytes",
        "current_ssts",
        "open_med_ms",
        "view_med_ms",
        "total_med_ms",
        "total_min",
        "total_max"
    );

    for merged in [false, true] {
        let state = if merged { "merged" } else { "churned" };
        let options = options_at(&attach_case_path(state));
        seed_attach_measurement(
            store.clone(),
            &options,
            ATTACH_TABLES,
            ATTACH_COLUMNS_PER_TABLE,
            ATTACH_ROUNDS,
        )
        .await;
        if merged {
            let completed = compact_attach_store(store.clone(), &options).await;
            println!("# compact_store completed {completed} subspace merges");
        }

        let (fixed_options, checkpoint) = fixed_checkpoint_options(store.clone(), &options).await;
        let census_reader = Catalog::open_read_only(store.clone(), fixed_options.clone())
            .await
            .unwrap();
        let census = census_reader
            .store_census(CensusRequest::default())
            .await
            .unwrap();
        let current = census
            .subspaces
            .iter()
            .find(|subspace| subspace.subspace == SubspaceName::Current)
            .expect("current subspace is present");
        let current_ssts = current.l0_ssts + current.sorted_run_ssts;
        let all_ssts: u32 = census
            .subspaces
            .iter()
            .map(|subspace| subspace.l0_ssts + subspace.sorted_run_ssts)
            .sum();
        let all_bytes = census.total_bytes();
        let objects = census.objects.expect("benchmark role can list its prefix");
        let current_bytes = current.bytes;
        census_reader.close().await.unwrap();

        println!(
            "# {state} store: {all_bytes} subspace bytes/{all_ssts} SSTs; \
             {} WAL objects/{} bytes; {} manifest bytes",
            objects.wal_objects, objects.wal_bytes, objects.manifest_bytes
        );

        // Fixed goes first: it writes nothing, so live begins from the same
        // manifest. Live's own repeated checkpoint writes are the behavior it
        // measures and cannot contaminate the fixed samples that precede it.
        for mode in [AttachMode::FixedCheckpoint, AttachMode::LiveFollowing] {
            let reader_options = match mode {
                AttachMode::FixedCheckpoint => &fixed_options,
                AttachMode::LiveFollowing => &options,
            };
            let (open, view, total) =
                measure_fresh_attaches(store.clone(), reader_options, ATTACH_REPEATS).await;
            let reader = mode.label();
            println!(
                "{reader:>7}  {state:>10}  {current_bytes:>13}  {current_ssts:>12}  \
                 {:>11.1}  {:>11.1}  {:>11.1}  {:>9.1}  {:>9.1}",
                milliseconds(open.median),
                milliseconds(view.median),
                milliseconds(total.median),
                milliseconds(total.min),
                milliseconds(total.max)
            );
        }

        Catalog::delete_checkpoint(store.clone(), options, &checkpoint)
            .await
            .unwrap();
    }
    println!();
}

/// 0021 — fresh fixed-checkpoint and live-following attach against the
/// endpoint.
///
/// Fixed readers go first against independently seeded churned and merged
/// stores. They open one immutable checkpoint and write nothing. Live readers
/// then measure the production path whose open and close maintain a manifest
/// checkpoint. Reporting the modes separately keeps read-only endpoint cost
/// distinct from the live checkpoint-write cost.
///
/// A measurement, not an assertion — it prints and passes.
#[tokio::test]
#[ignore = "needs a live S3 endpoint; run through `cargo xtask s3`"]
async fn measure_attach_latency_against_the_endpoint() {
    measure_attach_latency_against_endpoint().await;
}

/// 0004 — durable-commit latency where the WAL flush is a real PUT.
///
/// The in-memory harness (`tests/it/measure.rs`) settles the shape:
/// `max(flush cadence, write RTT) + ~2 ms`, with the round trip injected
/// rather than incurred. This is the same sweep against a live endpoint,
/// so the round trip is the endpoint's own. Read it accordingly: against a
/// loopback MinIO the PUT costs a millisecond or two and the flush cadence
/// dominates every row, which *tests* the composition but understates S3;
/// against a real bucket the first row is the write RTT itself.
///
/// A measurement, not an assertion — it prints and passes.
#[tokio::test]
#[ignore = "needs a live S3 endpoint; run through `cargo xtask s3`"]
async fn measure_commit_latency_against_the_endpoint() {
    const COMMITS: usize = 20;
    let intervals = [1u64, 25, 100];

    let store = s3_store();
    let target = endpoint_label();
    println!("\n# 0004 durable-commit latency against {target}");
    println!("# {COMMITS} sequential one-table commits per row, each await_durable\n");
    println!(
        "{:>10}  {:>11}  {:>9}  {:>9}",
        "flush_ms", "median_ms", "min_ms", "max_ms"
    );

    for (row, flush_ms) in intervals.iter().enumerate() {
        let mut options = options_at(&format!("latency-{flush_ms}-{row}"));
        options.flush_interval = Duration::from_millis(*flush_ms);
        let catalog = Catalog::open(store.clone(), options).await.unwrap();

        let mut samples = Vec::with_capacity(COMMITS);
        for i in 0..COMMITS {
            let start = Instant::now();
            catalog
                .commit(move |tx| {
                    let schema = tx.schema_by_name("main").expect("bootstrap").id;
                    tx.create_table(
                        schema,
                        &format!("t{i}"),
                        &[ColumnDef {
                            name: "a".into(),
                            column_type: "BIGINT".into(),
                            nulls_allowed: true,
                            default_value: None,
                            children: Vec::new(),
                        }],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
            samples.push(start.elapsed());
        }
        catalog.close().await.unwrap();

        samples.sort();
        let ms = |d: Duration| d.as_secs_f64() * 1_000.0;
        println!(
            "{flush_ms:>10}  {:>11.1}  {:>9.1}  {:>9.1}",
            ms(samples[samples.len() / 2]),
            ms(samples[0]),
            ms(samples[samples.len() - 1])
        );
    }
    println!();
}

/// Data files registered under the index-lookup measurement, and rows (so
/// entries per index) each carries; the two indexes hold `2 x` this many
/// entries in total.
const INDEX_LOOKUP_FILES: u64 = 64;
const INDEX_LOOKUP_ROWS_PER_FILE: u64 = 2_000;
/// The seed writer closes after this many files, so the entries land in
/// several SSTs rather than one memtable's flush.
const INDEX_LOOKUP_FILES_PER_HANDLE: u64 = 8;
/// Rows sharing one non-unique key.
const INDEX_LOOKUP_ROWS_PER_GROUP: u64 = 8;
const INDEX_LOOKUP_REPEATS: usize = 5;
const INDEX_LOOKUP_STEADY_KEYS: u64 = 20;
const INDEX_LOOKUP_IN_LIST_KEYS: u64 = 50;
const INDEX_LOOKUP_RANGE_ENTRIES: u64 = 100;

const INDEX_LOOKUP_ENTRIES: u64 = INDEX_LOOKUP_FILES * INDEX_LOOKUP_ROWS_PER_FILE;

struct IndexLookupFixture {
    table: TableId,
    unique: IndexId,
    non_unique: IndexId,
}

fn int_key(value: u64) -> IndexKeyValue {
    IndexKeyValue::Int {
        value: i128::from(value),
        width: IntWidth::I64,
    }
}

/// `count` distinct present unique keys spread across the whole index, so
/// no two probes of one phase share a block; `salt` shifts a phase off the
/// keys another phase touched.
fn spread_keys(count: u64, salt: u64) -> Vec<u64> {
    (0..count)
        .map(|position| (position * (INDEX_LOOKUP_ENTRIES / count) + salt) % INDEX_LOOKUP_ENTRIES)
        .collect()
}

#[test]
fn spread_keys_are_distinct_and_present() {
    let keys = spread_keys(INDEX_LOOKUP_IN_LIST_KEYS, 13);
    let mut sorted = keys.clone();
    sorted.sort_unstable();
    sorted.dedup();

    assert_eq!(sorted.len(), keys.len());
    assert!(keys.iter().all(|key| *key < INDEX_LOOKUP_ENTRIES));
}

/// Both indexes' entries for one data file: `a` is the global row number,
/// `b` groups consecutive rows.
fn index_lookup_entries(fixture: &IndexLookupFixture, file: u64) -> Vec<FileIndexEntry> {
    let first_row = file * INDEX_LOOKUP_ROWS_PER_FILE;
    (0..INDEX_LOOKUP_ROWS_PER_FILE)
        .flat_map(|ordinal| {
            let row = first_row + ordinal;
            [
                FileIndexEntry {
                    index: fixture.unique,
                    ordinal,
                    values: vec![Some(int_key(row))],
                },
                FileIndexEntry {
                    index: fixture.non_unique,
                    ordinal,
                    values: vec![Some(int_key(row / INDEX_LOOKUP_ROWS_PER_GROUP))],
                },
            ]
        })
        .collect()
}

/// One table with a unique index on `a` and a non-unique index on `b`, then
/// the data files whose entries fill both.
async fn seed_index_lookup_measurement(
    store: Arc<dyn ObjectStore>,
    options: &CatalogOptions,
) -> IndexLookupFixture {
    let mut seed_options = options.clone();
    seed_options.flush_interval = Duration::from_millis(1);
    let catalog = Catalog::open(store.clone(), seed_options.clone())
        .await
        .unwrap();

    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.create_schema("bench")?;
            let columns: Vec<ColumnDef> = ["a", "b", "c"]
                .iter()
                .map(|name| ColumnDef {
                    name: (*name).into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                    children: Vec::new(),
                })
                .collect();
            let table = tx.create_table(schema, "lookups", &columns)?;
            let unique = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            let non_unique = tx.create_index(
                table,
                &IndexDef {
                    name: "by_b".into(),
                    columns: vec![ColumnId::new(2)],
                    unique: false,
                },
                &[],
            )?;
            created.set(Some(IndexLookupFixture {
                table,
                unique,
                non_unique,
            }));
            Ok(())
        })
        .await
        .unwrap();
    let fixture = created.into_inner().expect("table and indexes created");
    catalog.close().await.unwrap();

    for handle in 0..INDEX_LOOKUP_FILES / INDEX_LOOKUP_FILES_PER_HANDLE {
        let catalog = Catalog::open(store.clone(), seed_options.clone())
            .await
            .unwrap();
        for file in
            handle * INDEX_LOOKUP_FILES_PER_HANDLE..(handle + 1) * INDEX_LOOKUP_FILES_PER_HANDLE
        {
            let entries = index_lookup_entries(&fixture, file);
            let table = fixture.table;
            catalog
                .commit(move |tx| {
                    tx.register_data_file(
                        table,
                        DataFile {
                            path: format!("f{file}.parquet"),
                            path_is_relative: true,
                            file_format: "parquet".into(),
                            record_count: INDEX_LOOKUP_ROWS_PER_FILE,
                            file_size_bytes: INDEX_LOOKUP_ROWS_PER_FILE * 24,
                            footer_size: 4,
                            encryption_key: None,
                            partition_values: Vec::new(),
                            column_stats: Vec::new(),
                        },
                        &entries,
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
        }
        catalog.close().await.unwrap();
    }

    fixture
}

/// Wall time and main-store GETs one operation cost on `reader`.
async fn timed<F, T>(reader: &ReadOnlyCatalog, operation: F) -> (Duration, u64, T)
where
    F: std::future::Future<Output = T>,
{
    let gets_before = reader.object_store_tally().main_gets;
    let start = Instant::now();
    let result = operation.await;
    let elapsed = start.elapsed();
    let gets = reader
        .object_store_tally()
        .main_gets
        .saturating_sub(gets_before);
    (elapsed, gets, result)
}

/// Samples of one measured phase across repetitions.
#[derive(Default)]
struct PhaseSamples {
    durations: Vec<Duration>,
    gets: Vec<u64>,
}

impl PhaseSamples {
    fn push(&mut self, duration: Duration, gets: u64) {
        self.durations.push(duration);
        self.gets.push(gets);
    }

    fn print(&self, phase: &str) {
        let stats = TimingStats::of(self.durations.clone());
        let mut gets = self.gets.clone();
        gets.sort_unstable();
        println!(
            "{phase:>22}  {:>9}  {:>11.2}  {:>9.2}  {:>9.2}  {:>10}",
            self.durations.len(),
            milliseconds(stats.median),
            milliseconds(stats.min),
            milliseconds(stats.max),
            gets[gets.len() / 2],
        );
    }
}

/// Index-lookup latency against the endpoint on the current read path:
/// per-table warm on first touch, `warm_tables`, batched probes, and
/// remote-sized read-ahead.
///
/// Every "cold" row opens a fresh read-only handle. `cold_first_lookup`
/// includes whatever the first touch of the table starts in the
/// background, and its GET count may absorb some of that burst; the
/// `warm_tables` row pays the burst explicitly and `lookup_after_warm` is
/// the first lookup behind it. `locate_row_ids` runs with no data store,
/// so it prices the catalog side only. Entries live in the store alone, so
/// no Parquet object is written.
///
/// A measurement, not an assertion — it prints and passes.
#[tokio::test]
#[ignore = "needs a live S3 endpoint; run through `cargo xtask s3`"]
#[allow(clippy::too_many_lines)]
async fn measure_index_lookup_latency_against_the_endpoint() {
    let store = s3_store();
    let options = options_at("index-lookup-latency");
    let target = endpoint_label();

    let fixture = seed_index_lookup_measurement(store.clone(), &options).await;

    let census_reader = Catalog::open_read_only(store.clone(), options.clone())
        .await
        .unwrap();
    let census = census_reader
        .store_census(CensusRequest::default())
        .await
        .unwrap();
    let index = census
        .subspaces
        .iter()
        .find(|subspace| subspace.subspace == SubspaceName::Index)
        .expect("index subspace is present");
    let index_ssts = index.l0_ssts + index.sorted_run_ssts;
    let index_bytes = index.bytes;
    census_reader.close().await.unwrap();

    println!("\n# index-lookup latency against {target}");
    println!("# prefix `{}`", options.path);
    println!(
        "# {INDEX_LOOKUP_FILES} files x {INDEX_LOOKUP_ROWS_PER_FILE} rows: \
         {INDEX_LOOKUP_ENTRIES} entries per index, one unique and one non-unique \
         ({INDEX_LOOKUP_ROWS_PER_GROUP} rows per key); index subspace {index_bytes} bytes \
         in {index_ssts} SSTs"
    );
    println!(
        "# {INDEX_LOOKUP_REPEATS} repetitions, a fresh read-only attach each; \
         steady_lookup pools {INDEX_LOOKUP_STEADY_KEYS} keys per attach"
    );
    println!(
        "# in_list is one index_lookup_many of {INDEX_LOOKUP_IN_LIST_KEYS} keys; \
         range is one index_range over {INDEX_LOOKUP_RANGE_ENTRIES} entries"
    );
    println!("# gets is the median main-store GET count the phase issued\n");
    println!(
        "{:>22}  {:>9}  {:>11}  {:>9}  {:>9}  {:>10}",
        "phase", "samples", "median_ms", "min_ms", "max_ms", "gets"
    );

    let mut attach = PhaseSamples::default();
    let mut cold_first = PhaseSamples::default();
    let mut warm_second = PhaseSamples::default();
    let mut steady = PhaseSamples::default();
    let mut non_unique = PhaseSamples::default();
    let mut in_list = PhaseSamples::default();
    let mut range = PhaseSamples::default();
    let mut locate = PhaseSamples::default();
    let mut warm_burst = PhaseSamples::default();
    let mut after_warm = PhaseSamples::default();

    for repeat in 0..INDEX_LOOKUP_REPEATS as u64 {
        let salt = repeat * 97;
        let attach_start = Instant::now();
        let reader = Catalog::open_read_only(store.clone(), options.clone())
            .await
            .unwrap();
        attach.push(
            attach_start.elapsed(),
            reader.object_store_tally().main_gets,
        );

        let key = spread_keys(1, salt + 1)[0];
        let (elapsed, gets, rows) = timed(
            &reader,
            reader.index_lookup(fixture.table, fixture.unique, &[int_key(key)]),
        )
        .await;
        assert_eq!(rows.unwrap(), vec![key]);
        cold_first.push(elapsed, gets);

        let key = spread_keys(1, salt + 3)[0];
        let (elapsed, gets, rows) = timed(
            &reader,
            reader.index_lookup(fixture.table, fixture.unique, &[int_key(key)]),
        )
        .await;
        assert_eq!(rows.unwrap(), vec![key]);
        warm_second.push(elapsed, gets);

        for key in spread_keys(INDEX_LOOKUP_STEADY_KEYS, salt + 5) {
            let (elapsed, gets, rows) = timed(
                &reader,
                reader.index_lookup(fixture.table, fixture.unique, &[int_key(key)]),
            )
            .await;
            assert_eq!(rows.unwrap(), vec![key]);
            steady.push(elapsed, gets);
        }

        let group = spread_keys(1, salt + 7)[0] / INDEX_LOOKUP_ROWS_PER_GROUP;
        let (elapsed, gets, rows) = timed(
            &reader,
            reader.index_lookup(fixture.table, fixture.non_unique, &[int_key(group)]),
        )
        .await;
        assert_eq!(rows.unwrap().len() as u64, INDEX_LOOKUP_ROWS_PER_GROUP);
        non_unique.push(elapsed, gets);

        let keys: Vec<Vec<IndexKeyValue>> = spread_keys(INDEX_LOOKUP_IN_LIST_KEYS, salt + 11)
            .into_iter()
            .map(|key| vec![int_key(key)])
            .collect();
        let (elapsed, gets, rows) = timed(
            &reader,
            reader.index_lookup_many(fixture.table, fixture.unique, &keys),
        )
        .await;
        assert_eq!(rows.unwrap().len() as u64, INDEX_LOOKUP_IN_LIST_KEYS);
        in_list.push(elapsed, gets);

        let lower =
            spread_keys(1, salt + 13)[0].min(INDEX_LOOKUP_ENTRIES - INDEX_LOOKUP_RANGE_ENTRIES);
        let (elapsed, gets, rows) = timed(
            &reader,
            reader.index_range(
                fixture.table,
                fixture.unique,
                Bound::Included(vec![int_key(lower)]),
                Bound::Excluded(vec![int_key(lower + INDEX_LOOKUP_RANGE_ENTRIES)]),
                false,
            ),
        )
        .await;
        assert_eq!(rows.unwrap().len() as u64, INDEX_LOOKUP_RANGE_ENTRIES);
        range.push(elapsed, gets);

        let row_ids = spread_keys(INDEX_LOOKUP_IN_LIST_KEYS, salt + 17);
        let (elapsed, gets, candidates) = timed(
            &reader,
            reader.locate_row_ids(None, "", fixture.table, row_ids.clone()),
        )
        .await;
        assert_eq!(candidates.unwrap().len(), row_ids.len());
        locate.push(elapsed, gets);

        reader.close().await.unwrap();

        let reader = Catalog::open_read_only(store.clone(), options.clone())
            .await
            .unwrap();
        let (elapsed, gets, warmed) = timed(&reader, reader.warm_tables(&[fixture.table])).await;
        warmed.unwrap();
        warm_burst.push(elapsed, gets);

        let key = spread_keys(1, salt + 19)[0];
        let (elapsed, gets, rows) = timed(
            &reader,
            reader.index_lookup(fixture.table, fixture.unique, &[int_key(key)]),
        )
        .await;
        assert_eq!(rows.unwrap(), vec![key]);
        after_warm.push(elapsed, gets);
        reader.close().await.unwrap();
    }

    attach.print("attach_read_only");
    cold_first.print("cold_first_lookup");
    warm_second.print("warm_second_lookup");
    steady.print("steady_lookup");
    non_unique.print("non_unique_lookup");
    in_list.print("in_list_lookup_many");
    range.print("range");
    locate.print("locate_row_ids");
    warm_burst.print("warm_tables");
    after_warm.print("lookup_after_warm");
    println!();
}
