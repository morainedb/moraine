//! What spreading uniqueness probes over tasks is worth, against the shape
//! that runs them today.
//!
//! `resolve_probes` puts a batch in `buffer_unordered` and `ProbeWindow`
//! pushes those batches into a `FuturesUnordered`, so every probe of every
//! batch is polled on the one task driving the commit. That overlaps I/O
//! and nothing else: a probe whose cost is synchronous runs to completion
//! before the next is polled. Production's are synchronous -- a slow commit
//! reads nothing and misses no cache, and still pays about 12 ms a probe.
//!
//! The per-probe cost is simulated rather than provoked, because what is
//! being measured is the shape's scaling, not the cause of the cost.

use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{StreamExt, TryStreamExt, stream};
use object_store::memory::InMemory;
use slatedb::{Db, DbTransaction, IsolationLevel};

type Failure = Box<dyn std::error::Error>;

/// `index_maintenance::PROBE_BATCH_SIZE`.
const BATCH: usize = 128;

fn key(value: u64) -> Vec<u8> {
    let mut bytes = b"idx/".to_vec();
    bytes.extend_from_slice(&value.to_be_bytes());
    bytes
}

/// Stands in for whatever a production probe spends inside its `get`.
fn burn(micros: u64) {
    if micros == 0 {
        return;
    }
    let until = Instant::now() + Duration::from_micros(micros);
    let mut churn = 0_u64;
    while Instant::now() < until {
        churn = churn.wrapping_mul(2_654_435_761).wrapping_add(1);
    }
    black_box(churn);
}

async fn probe(transaction: &DbTransaction, key: &[u8], micros: u64) -> Result<bool, Failure> {
    let found = transaction.get(key).await?;
    burn(micros);
    Ok(found.is_some())
}

/// Today: every probe polled on the caller's task.
async fn buffered(
    transaction: &DbTransaction,
    keys: &[Vec<u8>],
    micros: u64,
) -> Result<Duration, Failure> {
    let started = Instant::now();
    stream::iter(keys.iter().map(|key| probe(transaction, key, micros)))
        .buffer_unordered(BATCH)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(started.elapsed())
}

/// One task a probe: the runtime's workers all take a share, at a spawn
/// apiece.
async fn spawned(
    transaction: &Arc<DbTransaction>,
    keys: &[Vec<u8>],
    micros: u64,
) -> Result<Duration, Failure> {
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(keys.len());
    for key in keys {
        let transaction = Arc::clone(transaction);
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            probe(&transaction, &key, micros).await.is_ok()
        }));
    }
    for task in tasks {
        task.await?;
    }
    Ok(started.elapsed())
}

/// `chunks` tasks, each walking its own slice: the spawn is paid per chunk
/// rather than per probe, which is the shape `resolve_chunked` already uses
/// on the lookup side.
async fn chunked(
    transaction: &Arc<DbTransaction>,
    keys: &[Vec<u8>],
    micros: u64,
    chunks: usize,
) -> Result<Duration, Failure> {
    let started = Instant::now();
    let size = keys.len().div_ceil(chunks.max(1));
    let mut tasks = Vec::new();
    for chunk in keys.chunks(size) {
        let transaction = Arc::clone(transaction);
        let chunk = chunk.to_vec();
        tasks.push(tokio::spawn(async move {
            for key in chunk {
                if probe(&transaction, &key, micros).await.is_err() {
                    return false;
                }
            }
            true
        }));
    }
    for task in tasks {
        task.await?;
    }
    Ok(started.elapsed())
}

/// Production stages a put onto the same transaction as each probe lands,
/// so the write batch is never empty and its lock is never uncontended.
fn staged_writes(transaction: &DbTransaction, count: usize) -> Result<(), Failure> {
    for value in 0..count as u64 {
        transaction.put(key(9_000_000 + value), b"v")?;
    }
    Ok(())
}

/// `shards` tasks, each running today's `buffer_unordered` over its share:
/// the I/O concurrency the single task already has, spread over workers.
async fn sharded(
    transaction: &Arc<DbTransaction>,
    keys: &[Vec<u8>],
    cost: impl Fn(usize) -> u64 + Copy + Send + Sync + 'static,
    shards: usize,
) -> Result<Duration, Failure> {
    let started = Instant::now();
    let depth = (BATCH / shards.max(1)).max(1);
    let mut tasks = Vec::new();
    for (shard, chunk) in keys.chunks(keys.len().div_ceil(shards.max(1))).enumerate() {
        let transaction = Arc::clone(transaction);
        let chunk = chunk.to_vec();
        let base = shard * keys.len().div_ceil(shards.max(1));
        tasks.push(tokio::spawn(async move {
            let share: Vec<(usize, Vec<u8>)> = chunk.into_iter().enumerate().collect();
            let reads: Vec<_> = share
                .iter()
                .map(|(offset, key)| probe(&transaction, key, cost(base + offset)))
                .collect();
            stream::iter(reads)
                .buffer_unordered(depth)
                .try_collect::<Vec<_>>()
                .await
                .is_ok()
        }));
    }
    for task in tasks {
        task.await?;
    }
    Ok(started.elapsed())
}

/// Same shape on one task, so the comparison isolates the spawn.
async fn buffered_skewed(
    transaction: &DbTransaction,
    keys: &[Vec<u8>],
    cost: impl Fn(usize) -> u64,
) -> Result<Duration, Failure> {
    let started = Instant::now();
    stream::iter(
        keys.iter()
            .enumerate()
            .map(|(offset, key)| probe(transaction, key, cost(offset))),
    )
    .buffer_unordered(BATCH)
    .try_collect::<Vec<_>>()
    .await?;
    Ok(started.elapsed())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Failure> {
    let db = Db::builder("probe-parallel-bench", Arc::new(InMemory::new()))
        .build()
        .await?;
    for value in 0..50_000_u64 {
        db.put(key(value), b"v").await?;
    }
    db.flush().await?;
    println!("8 workers; keys absent, as a new-value probe's are\n");

    for probes in [16usize, 256] {
        let keys: Vec<Vec<u8>> = (0..probes as u64)
            .map(|i| key(5_000_000 + i * 7919))
            .collect();
        for micros in [0_u64, 100, 1_000, 12_000] {
            let transaction = Arc::new(db.begin(IsolationLevel::Snapshot).await?);
            let one = buffered(&transaction, &keys, micros).await?;
            let per = spawned(&transaction, &keys, micros).await?;
            let eight = chunked(&transaction, &keys, micros, 8).await?;
            let sixteen = chunked(&transaction, &keys, micros, 16).await?;
            println!(
                "probes={probes:<4} cpu_per_probe={micros:>6}us  buffered={one:>10.2?}  \
                 spawned={per:>10.2?}  chunked8={eight:>10.2?}  chunked16={sixteen:>10.2?}  \
                 speedup={:>5.1}x",
                one.as_secs_f64() / eight.as_secs_f64().max(f64::EPSILON)
            );
        }
        println!();
    }

    contended(&db).await?;

    Ok(())
}

/// The sections that vary the transaction rather than the probe shape.
async fn contended(db: &Db) -> Result<(), Failure> {
    println!("the cheap case, ten rounds: is spawning a regression when a probe is free?\n");
    for probes in [16usize, 128, 256] {
        let keys: Vec<Vec<u8>> = (0..probes as u64)
            .map(|i| key(5_000_000 + i * 7919))
            .collect();
        let transaction = Arc::new(db.begin(IsolationLevel::Snapshot).await?);
        let mut one = Vec::new();
        let mut eight = Vec::new();
        for _ in 0..10 {
            one.push(buffered(&transaction, &keys, 0).await?);
            eight.push(chunked(&transaction, &keys, 0, 8).await?);
        }
        one.sort_unstable();
        eight.sort_unstable();
        println!(
            "probes={probes:<4} buffered median={:>9.2?} worst={:>9.2?}   \
             chunked8 median={:>9.2?} worst={:>9.2?}",
            one[5], one[9], eight[5], eight[9]
        );
    }
    println!();

    println!("against a transaction already holding staged writes, 12ms a probe\n");
    let keys: Vec<Vec<u8>> = (0..256_u64).map(|i| key(5_000_000 + i * 7919)).collect();
    for staged in [0usize, 10_000, 100_000] {
        let transaction = Arc::new(db.begin(IsolationLevel::Snapshot).await?);
        staged_writes(&transaction, staged)?;
        let one = buffered(&transaction, &keys, 12_000).await?;
        let eight = chunked(&transaction, &keys, 12_000, 8).await?;
        println!(
            "staged_writes={staged:<7} buffered={one:>10.2?}  chunked8={eight:>10.2?}  \
             speedup={:>5.1}x",
            one.as_secs_f64() / eight.as_secs_f64().max(f64::EPSILON)
        );
    }

    println!("\nwith the commit task staging puts while the probes run\n");
    for staged in [0usize, 100_000] {
        let transaction = Arc::new(db.begin(IsolationLevel::Snapshot).await?);
        staged_writes(&transaction, staged)?;
        let writer = Arc::clone(&transaction);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let halt = Arc::clone(&stop);
        let churn = tokio::spawn(async move {
            let mut value = 0_u64;
            while !halt.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = writer.put(key(8_000_000 + value), b"v");
                value += 1;
                tokio::task::yield_now().await;
            }
            value
        });
        let eight = chunked(&transaction, &keys, 12_000, 8).await?;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let wrote = churn.await?;
        println!("staged_writes={staged:<7} chunked8={eight:>10.2?}  (writer landed {wrote} puts)");
    }

    println!("\nsharded: each task keeps today's buffer_unordered over its share\n");
    let keys: Vec<Vec<u8>> = (0..256_u64).map(|i| key(5_000_000 + i * 7919)).collect();
    let transaction = Arc::new(db.begin(IsolationLevel::Snapshot).await?);
    for (name, cost) in [
        ("uniform 12ms", (|_| 12_000) as fn(usize) -> u64),
        (
            "skewed 16:1",
            |offset| if offset % 16 == 0 { 180_000 } else { 800 },
        ),
    ] {
        let one = buffered_skewed(&transaction, &keys, cost).await?;
        let four = sharded(&transaction, &keys, cost, 4).await?;
        let eight = sharded(&transaction, &keys, cost, 8).await?;
        let per = sharded(&transaction, &keys, cost, 256).await?;
        println!(
            "{name:<14} buffered={one:>9.2?}  shard4={four:>9.2?}  shard8={eight:>9.2?}  \
             per-probe={per:>9.2?}"
        );
    }

    Ok(())
}
