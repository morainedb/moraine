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
    sync::Arc,
    time::{Duration, Instant},
};

use moraine::{Catalog, CatalogOptions, ColumnDef};
use object_store::{ObjectStore, aws::AmazonS3Builder};

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
    let target = std::env::var("MORAINE_S3_ENDPOINT").unwrap_or_else(|_| {
        format!(
            "AWS S3 in {}",
            std::env::var("AWS_REGION")
                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                .unwrap_or_else(|_| "the configured region".into())
        )
    });
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
