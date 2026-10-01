//! What a commit's uniqueness probes cost, and what makes them expensive.
//!
//! Production shows a 256-probe batch spanning three seconds when the
//! commit stages no deletions, and three milliseconds when it stages ~2,200
//! -- at 1.8 effective concurrency against 256 admitted in flight. Both
//! regimes are reproduced here, with the staged writes and the probed keys
//! varied separately so the two explanations can be told apart.
//!
//! Arguments: entries already committed to the index (default 200000).

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{StreamExt, stream};
use object_store::{
    memory::InMemory,
    throttle::{ThrottleConfig, ThrottledStore},
};
use slatedb::{Db, DbTransaction, IsolationLevel};

type Failure = Box<dyn std::error::Error>;

/// `index_maintenance::PROBE_BATCH_SIZE`, which is what bounds a real
/// batch's concurrency.
const PROBE_BATCH_SIZE: usize = 128;

/// Probes per batch, as the slow production commits issue.
const PROBES: usize = 256;

/// Scatters a batch across the key space rather than leaving it adjacent,
/// which is the shape a flush's indexed values have.
const STRIDE: u64 = 7919;

fn key(value: u64) -> Vec<u8> {
    let mut bytes = b"idx/".to_vec();
    bytes.extend_from_slice(&value.to_be_bytes());
    bytes
}

/// One batch resolved as `resolve_probes` resolves it: every probe a point
/// get on the one transaction, `PROBE_BATCH_SIZE` in flight.
async fn probe_batch(transaction: &DbTransaction, keys: &[Vec<u8>]) -> Result<Measured, Failure> {
    let started = Instant::now();
    let outcomes: Vec<(Duration, bool)> = stream::iter(keys.iter().map(|key| async move {
        let started = Instant::now();
        let found = transaction.get(key).await?;
        Ok::<_, slatedb::Error>((started.elapsed(), found.is_some()))
    }))
    .buffer_unordered(PROBE_BATCH_SIZE)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<_, _>>()?;

    Ok(Measured {
        window: started.elapsed(),
        service: outcomes.iter().map(|(service, _)| *service).sum(),
        hits: outcomes.iter().filter(|(_, found)| *found).count(),
        probes: keys.len(),
    })
}

/// The same batch against the database rather than a transaction, which
/// says whether the transaction is what serializes them.
async fn probe_database(db: &Db, keys: &[Vec<u8>]) -> Result<Measured, Failure> {
    let started = Instant::now();
    let outcomes: Vec<(Duration, bool)> = stream::iter(keys.iter().map(|key| async move {
        let started = Instant::now();
        let found = db.get(key).await?;
        Ok::<_, slatedb::Error>((started.elapsed(), found.is_some()))
    }))
    .buffer_unordered(PROBE_BATCH_SIZE)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<_, _>>()?;

    Ok(Measured {
        window: started.elapsed(),
        service: outcomes.iter().map(|(service, _)| *service).sum(),
        hits: outcomes.iter().filter(|(_, found)| *found).count(),
        probes: keys.len(),
    })
}

struct Measured {
    window: Duration,
    service: Duration,
    hits: usize,
    probes: usize,
}

impl Measured {
    // Reporting a millisecond average; the count never nears the mantissa.
    #[allow(clippy::cast_precision_loss)]
    fn report(&self, label: &str) {
        let per = self.service.as_secs_f64() * 1e3 / self.probes as f64;
        let effective = self.service.as_secs_f64() / self.window.as_secs_f64().max(f64::EPSILON);
        println!(
            "{label:<34} window={:>9.2?} service={:>9.2?} per_probe={per:>7.3} ms  \
             effective_concurrency={effective:>5.1}  hits={}/{}",
            self.window, self.service, self.hits, self.probes
        );
    }
}

/// One scenario: a transaction carrying `puts` additions and `deletes`
/// removals, probed with keys that are either all absent or all present.
async fn scenario(
    db: &Db,
    committed: u64,
    label: &str,
    puts: u64,
    deletes: u64,
    absent: bool,
) -> Result<(), Failure> {
    let transaction = db.begin(IsolationLevel::Snapshot).await?;
    for i in 0..puts {
        transaction.put(key(committed + 1_000_000 + i), b"v")?;
    }
    for i in 0..deletes {
        transaction.delete(key((i * 13) % committed))?;
    }

    let keys: Vec<Vec<u8>> = (0..PROBES as u64)
        .map(|i| {
            if absent {
                key(committed + 5_000_000 + i * STRIDE)
            } else {
                key((i * STRIDE) % committed)
            }
        })
        .collect();

    probe_batch(&transaction, &keys).await?.report(label);
    drop(transaction);
    Ok(())
}

/// The same batch while the runtime is busy with other work, which is the
/// condition production's slow commits sit in: they cluster into ten-second
/// windows an hour apart rather than tracking load.
async fn probe_under_load(
    db: &Arc<Db>,
    keys: &[Vec<u8>],
    hogs: usize,
) -> Result<Measured, Failure> {
    let stop = Arc::new(AtomicBool::new(false));
    let mut load = Vec::new();
    for _ in 0..hogs {
        let stop = Arc::clone(&stop);
        load.push(tokio::task::spawn_blocking(move || {
            let mut churn = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                churn = churn.wrapping_mul(2_654_435_761).wrapping_add(1);
            }
            churn
        }));
    }

    let transaction = db.begin(IsolationLevel::Snapshot).await?;
    let measured = probe_batch(&transaction, keys).await;
    stop.store(true, Ordering::Relaxed);
    for task in load {
        task.await?;
    }
    measured
}

/// The same batch while a writer keeps flushing, so compaction and L0
/// churn run against the probes rather than beside them.
async fn probe_under_writes(
    db: &Arc<Db>,
    keys: &[Vec<u8>],
    from: u64,
) -> Result<Measured, Failure> {
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = Arc::clone(db);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut value = from;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..2_000 {
                    if db.put(key(value), b"v").await.is_err() {
                        return;
                    }
                    value += 1;
                }
                let _ = db.flush().await;
            }
        })
    };

    let transaction = db.begin(IsolationLevel::Snapshot).await?;
    let measured = probe_batch(&transaction, keys).await;
    stop.store(true, Ordering::Relaxed);
    let _ = writer.await;
    measured
}

/// A store whose every GET costs a round trip, which is the one thing a
/// local bench otherwise lacks and the one that matches the 25 ms a
/// production probe takes when it is slow.
fn remote(latency: Duration) -> Arc<dyn object_store::ObjectStore> {
    Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig {
            wait_get_per_call: latency,
            ..ThrottleConfig::default()
        },
    ))
}

/// The probes spread over their own tasks rather than left on the
/// caller's: the shape `buffer_unordered` cannot deliver, because these
/// futures are synchronous between await points unless a read actually
/// suspends.
async fn probe_spawned(db: &Arc<Db>, keys: &[Vec<u8>]) -> Result<Measured, Failure> {
    let started = Instant::now();
    let mut tasks = Vec::new();
    for key in keys {
        let db = Arc::clone(db);
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            let started = Instant::now();
            let found = db.get(&key).await.map(|value| value.is_some());
            (started.elapsed(), found)
        }));
    }
    let mut service = Duration::ZERO;
    let mut hits = 0;
    for task in tasks {
        let (elapsed, found) = task.await?;
        service += elapsed;
        if found? {
            hits += 1;
        }
    }
    Ok(Measured {
        window: started.elapsed(),
        service,
        hits,
        probes: keys.len(),
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Failure> {
    let committed: u64 = match std::env::args().nth(1) {
        Some(argument) => argument.parse()?,
        None => 200_000,
    };

    let db = Db::builder("unique-probe-bench", Arc::new(InMemory::new()))
        .build()
        .await?;
    for value in 0..committed {
        db.put(key(value), b"v").await?;
    }
    db.flush().await?;
    println!("{committed} committed entries, {PROBES} probes a batch\n");

    let absent: Vec<Vec<u8>> = (0..PROBES as u64)
        .map(|i| key(committed + 5_000_000 + i * STRIDE))
        .collect();
    let present: Vec<Vec<u8>> = (0..PROBES as u64)
        .map(|i| key((i * STRIDE) % committed))
        .collect();

    probe_database(&db, &absent)
        .await?
        .report("db, absent keys");
    probe_database(&db, &present)
        .await?
        .report("db, present keys");
    println!();

    // Production's two regimes: a pure-insert flush, and an update flush
    // that also stages deletions.
    scenario(&db, committed, "tx, insert shape, absent", 2_200, 0, true).await?;
    scenario(
        &db,
        committed,
        "tx, update shape, present",
        5_500,
        2_200,
        false,
    )
    .await?;
    println!();

    // The same two variables, crossed, so the staged deletions and the
    // hit/miss split are told apart rather than confounded.
    scenario(
        &db,
        committed,
        "tx, update shape, absent",
        5_500,
        2_200,
        true,
    )
    .await?;
    scenario(&db, committed, "tx, insert shape, present", 2_200, 0, false).await?;
    scenario(&db, committed, "tx, nothing staged, absent", 0, 0, true).await?;
    scenario(&db, committed, "tx, nothing staged, present", 0, 0, false).await?;
    println!();

    // Whether anything reproduces production's 25 ms a probe. The shape
    // above does not, at any combination, so the remaining candidate is
    // what else the process is doing at the time.
    let db = Arc::new(db);
    for hogs in [8usize, 32] {
        probe_under_load(&db, &absent, hogs)
            .await?
            .report(&format!("tx, absent, {hogs} busy threads"));
    }
    probe_under_writes(&db, &absent, committed + 20_000_000)
        .await?
        .report("tx, absent, writer flushing");
    println!();

    // The one condition the bench lacked. A cold probe against a store with
    // a round trip is what production's slow commits are made of.
    let latency = Duration::from_millis(25);
    let store = remote(latency);
    let slow_db = Arc::new(
        Db::builder("unique-probe-bench-remote", Arc::clone(&store))
            .build()
            .await?,
    );
    let cold: u64 = 20_000;
    for value in 0..cold {
        slow_db.put(key(value), b"v").await?;
    }
    slow_db.flush().await?;
    let cold_absent: Vec<Vec<u8>> = (0..PROBES as u64)
        .map(|i| key(cold + 5_000_000 + i * STRIDE))
        .collect();

    println!("against a store with a {latency:?} GET:");
    let transaction = slow_db.begin(IsolationLevel::Snapshot).await?;
    probe_batch(&transaction, &cold_absent)
        .await?
        .report("  warm caches, buffer_unordered");
    drop(transaction);
    slow_db.close().await?;

    // Reopened, so the filter and index blocks those probes clear
    // themselves against are not resident. This is the state a compaction
    // leaves behind, and the only one that puts a probe on the network.
    for (label, spawned) in [("buffer_unordered(128)", false), ("one task a probe", true)] {
        let reopened = Arc::new(
            Db::builder("unique-probe-bench-remote", Arc::clone(&store))
                .build()
                .await?,
        );
        let measured = if spawned {
            probe_spawned(&reopened, &cold_absent).await?
        } else {
            let transaction = reopened.begin(IsolationLevel::Snapshot).await?;
            let measured = probe_batch(&transaction, &cold_absent).await?;
            drop(transaction);
            measured
        };
        measured.report(&format!("  cold caches, {label}"));
        reopened.close().await?;
    }

    Ok(())
}
