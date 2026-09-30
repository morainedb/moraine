//! What a batched index lookup's probes cost, and which shape runs them
//! fastest. Arguments: distinct indexed values (default 50000).
//!
//! `resolve_encoded` puts every probe in flight on one task. These futures
//! are synchronous between await points, so that shape is worth nothing:
//! `unordered` matches `sequential` at every batch size. Splitting a probe
//! into `build` and `drain` says why -- the cost is constructing the
//! iterator, not reading the entries it returns.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{StreamExt, stream::FuturesUnordered};
use object_store::memory::InMemory;
use slatedb::{Db, DbTransaction, IsolationLevel, config::ScanOptions};

type Failure = Box<dyn std::error::Error>;

/// Entries sharing one indexed value, as a non-unique index holds them.
const ROWS_PER_VALUE: u64 = 2;

/// Stride through the value space, so a batch is scattered rather than
/// adjacent -- the shape a located delete's parent ids have.
const STRIDE: u64 = 7919;

fn key(value: u64, row: u64) -> Vec<u8> {
    let mut bytes = prefix(value);
    bytes.extend_from_slice(&row.to_be_bytes());
    bytes
}

fn prefix(value: u64) -> Vec<u8> {
    let mut bytes = b"idx/".to_vec();
    bytes.extend_from_slice(&value.to_be_bytes());
    bytes
}

/// `ScanShape::Equality`'s options, which is what an index probe uses.
fn options() -> ScanOptions {
    ScanOptions {
        read_ahead_bytes: 4 * 1024,
        max_fetch_tasks: 2,
        cache_blocks: true,
        ..ScanOptions::default()
    }
}

#[derive(Clone, Copy)]
enum Handle<'a> {
    Tx(&'a DbTransaction),
    Db(&'a Db),
}

impl Handle<'_> {
    async fn scan(self, value: u64) -> Result<slatedb::DbIterator, Failure> {
        let iterator = match self {
            Self::Tx(tx) => {
                tx.scan_prefix_with_options(prefix(value), .., &options())
                    .await?
            }
            Self::Db(db) => {
                db.scan_prefix_with_options(prefix(value), .., &options())
                    .await?
            }
        };
        Ok(iterator)
    }
}

async fn probe(handle: Handle<'_>, value: u64) -> Result<usize, Failure> {
    let mut iterator = handle.scan(value).await?;
    let mut found = 0;
    while iterator.next().await?.is_some() {
        found += 1;
    }
    Ok(found)
}

/// Building the iterator against draining it, which says whether a probe's
/// cost is per-scan setup or per-entry work.
async fn split(handle: Handle<'_>, values: &[u64]) -> Result<(Duration, Duration), Failure> {
    let (mut build, mut drain) = (Duration::ZERO, Duration::ZERO);
    for &value in values {
        let started = Instant::now();
        let mut iterator = handle.scan(value).await?;
        build += started.elapsed();

        let started = Instant::now();
        while iterator.next().await?.is_some() {}
        drain += started.elapsed();
    }
    Ok((build, drain))
}

async fn sequential(handle: Handle<'_>, values: &[u64]) -> Result<Duration, Failure> {
    let started = Instant::now();
    for &value in values {
        probe(handle, value).await?;
    }
    Ok(started.elapsed())
}

/// `resolve_encoded`'s shape: every probe in flight, all on one task.
async fn unordered(handle: Handle<'_>, values: &[u64]) -> Result<Duration, Failure> {
    let started = Instant::now();
    let mut probes: FuturesUnordered<_> = values.iter().map(|&v| probe(handle, v)).collect();
    while let Some(found) = probes.next().await {
        found?;
    }
    Ok(started.elapsed())
}

/// One iterator seeking forward to each key. For a scattered batch the
/// seeks traverse rather than skip, so this walks the whole index.
async fn seeking(db: &Db, sorted: &[u64]) -> Result<Duration, Failure> {
    let started = Instant::now();
    let mut iterator = db
        .scan_prefix_with_options(b"idx/".to_vec(), .., &options())
        .await?;
    for &value in sorted {
        let target = prefix(value);
        if iterator.seek(&target).await.is_err() {
            continue;
        }
        while let Some(entry) = iterator.next().await? {
            if !entry.key.starts_with(&target) {
                break;
            }
        }
    }
    Ok(started.elapsed())
}

/// One task per probe: parallel, but it pays a spawn per key.
async fn spawned(db: &Arc<Db>, values: &[u64]) -> Result<Duration, Failure> {
    let started = Instant::now();
    let mut tasks = Vec::new();
    for &value in values {
        let db = Arc::clone(db);
        tasks.push(tokio::spawn(async move {
            probe(Handle::Db(&db), value).await.is_ok()
        }));
    }
    for task in tasks {
        task.await?;
    }
    Ok(started.elapsed())
}

/// `chunked`, with each task's own keys left in flight against each other:
/// a task puts a core on the batch, and the probes inside it still overlap.
async fn chunked_overlapped(
    db: &Arc<Db>,
    sorted: &[u64],
    chunks: usize,
) -> Result<Duration, Failure> {
    let started = Instant::now();
    let size = sorted.len().div_ceil(chunks.max(1));
    let mut tasks = Vec::new();
    for chunk in sorted.chunks(size) {
        let db = Arc::clone(db);
        let chunk = chunk.to_vec();
        tasks.push(tokio::spawn(async move {
            let mut probes: FuturesUnordered<_> =
                chunk.iter().map(|&v| probe(Handle::Db(&db), v)).collect();
            while let Some(found) = probes.next().await {
                if found.is_err() {
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

/// Sorted keys cut into `chunks` adjacent runs, one task each: the spawn is
/// paid per chunk, and a run's keys share the structure they walk.
async fn chunked(db: &Arc<Db>, sorted: &[u64], chunks: usize) -> Result<Duration, Failure> {
    let started = Instant::now();
    let size = sorted.len().div_ceil(chunks.max(1));
    let mut tasks = Vec::new();
    for chunk in sorted.chunks(size) {
        let db = Arc::clone(db);
        let chunk = chunk.to_vec();
        tasks.push(tokio::spawn(async move {
            for value in chunk {
                if probe(Handle::Db(&db), value).await.is_err() {
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

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Failure> {
    let values: u64 = match std::env::args().nth(1) {
        Some(argument) => argument.parse()?,
        None => 50_000,
    };

    let db = Db::builder("probe-bench", Arc::new(InMemory::new()))
        .build()
        .await?;
    for value in 0..values {
        for row in 0..ROWS_PER_VALUE {
            db.put(key(value, row), b"v").await?;
        }
    }
    db.flush().await?;
    println!("{} values, {} entries", values, values * ROWS_PER_VALUE);

    let db = Arc::new(db);
    let batch: Vec<u64> = (0..256).map(|i| (i * STRIDE) % values).collect();

    for count in [28usize, 156, 256] {
        let scattered = &batch[..count];
        let mut sorted = scattered.to_vec();
        sorted.sort_unstable();
        sorted.dedup();

        let (build, drain) = split(Handle::Db(&db), scattered).await?;

        let transaction = db.begin(IsolationLevel::Snapshot).await?;
        let tx_unordered = unordered(Handle::Tx(&transaction), scattered).await?;
        drop(transaction);

        println!(
            "keys={count:<4} build={build:>9.2?} drain={drain:>9.2?} | \
             sequential={:>9.2?} unordered={:>9.2?} tx_unordered={tx_unordered:>9.2?} \
             sorted={:>9.2?} seek={:>9.2?} spawned={:>9.2?} chunked16={:>9.2?} \
             chunked16_overlapped={:>9.2?}",
            sequential(Handle::Db(&db), scattered).await?,
            unordered(Handle::Db(&db), scattered).await?,
            unordered(Handle::Db(&db), &sorted).await?,
            seeking(&db, &sorted).await?,
            spawned(&db, scattered).await?,
            chunked(&db, &sorted, 16).await?,
            chunked_overlapped(&db, &sorted, 16).await?,
        );
    }

    Ok(())
}
