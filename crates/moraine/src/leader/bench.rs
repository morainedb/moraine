//! The adaptive-leader bench: the final judgment on whether the leader role
//! earns its cost. It is `#[ignore]`d — a measurement, not a gate. Run it with
//!
//! ```text
//! cargo test -p moraine --features leader --lib -- --ignored --nocapture \
//!     leader::bench
//! ```
//!
//! Three shapes of an N-committer fleet on one bucket answer three questions:
//!
//! - **no leader** (direct racing): PUTs per successful commit under contention
//!   approaches N — the write amplification the slot topology pays when every
//!   committer races. This is also the pre-leader number: with no advert on the
//!   log, `forward_target` never connects, so the path is byte-for-byte the
//!   direct one, and the role costs nothing when absent.
//! - **leader from the start**: the fleet forwards and coalesces, so PUTs per
//!   commit falls toward one and the leader's win share converges the fleet.
//! - **leader killed mid-run**: the survivors degrade to direct racing — a
//!   bounded spike in PUTs, never a stall; every commit still lands.
//!
//! The measured quantity is object-store Creates to `commits/` — one per slot
//! race, win or lose — counted per store, so a forwarded commit shows on the
//! leader's store and a direct one on the committer's own.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::{sync::Notify, task::JoinHandle};

use super::{Leader, LeaderConfig};
use crate::{
    Error, Result,
    catalog::{Catalog, CatalogOptions, SnapshotId},
    transaction::staged::{Cell, RowOperation, TableKind},
};

/// Counts a store's own `commits/` slot Creates — every race is a PUT — split
/// into the wins (the create returned `Ok`) and the total. A forwarded commit
/// rides the leader's store, so it never touches the forwarding client's
/// counter.
#[derive(Debug)]
struct SlotMeter {
    inner: Arc<dyn ObjectStore>,
    attempts: AtomicU64,
    wins: AtomicU64,
}

impl SlotMeter {
    fn wrap(inner: &Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::clone(inner),
            attempts: AtomicU64::new(0),
            wins: AtomicU64::new(0),
        })
    }

    fn attempts(&self) -> u64 {
        self.attempts.load(Ordering::Relaxed)
    }

    fn wins(&self) -> u64 {
        self.wins.load(Ordering::Relaxed)
    }
}

impl std::fmt::Display for SlotMeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlotMeter({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlotMeter {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let races_slot =
            location.as_ref().starts_with("commits/") && matches!(opts.mode, PutMode::Create);
        if races_slot {
            self.attempts.fetch_add(1, Ordering::Relaxed);
        }
        let result = self.inner.put_opts(location, payload, opts).await;
        if races_slot && result.is_ok() {
            self.wins.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A leader serving on a background task, over its own metered store.
struct RunningLeader {
    store: Arc<SlotMeter>,
    shutdown: Arc<Notify>,
    crash: Arc<Notify>,
    join: JoinHandle<Result<()>>,
}

impl RunningLeader {
    async fn spawn(inner: &Arc<dyn ObjectStore>, window: Duration) -> Self {
        let store = SlotMeter::wrap(inner);
        let catalog = Arc::new(open_over(&store, window).await);
        let leader = Leader::bind(catalog, LeaderConfig::new(free_addr(), 64))
            .await
            .unwrap();
        let shutdown = Arc::new(Notify::new());
        let crash = Arc::new(Notify::new());
        // The leader's sessions are `!Send`, so `serve` needs a runtime of its
        // own rather than a slot on the shared pool. A blocking task runs to
        // completion however early it is cancelled, so the crash is a signal
        // rather than an abort.
        let join = tokio::task::spawn_blocking({
            let shutdown = Arc::clone(&shutdown);
            let crash = Arc::clone(&crash);
            move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("leader test runtime")
                    .block_on(async move {
                        tokio::select! {
                            served = leader.serve(shutdown) => served,
                            () = crash.notified() => Ok(()),
                        }
                    })
            }
        });
        Self {
            store,
            shutdown,
            crash,
            join,
        }
    }

    async fn stop(self) {
        self.shutdown.notify_one();
        let _ = self.join.await;
    }

    /// A crash: the serve future drops with its listener. `abort` cannot do
    /// this — a blocking task runs to completion however early it is
    /// cancelled.
    fn kill(&self) {
        self.crash.notify_one();
    }
}

/// A free loopback address, released so the leader can bind and advertise it.
fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}

fn open_options(window: Duration) -> CatalogOptions {
    CatalogOptions {
        commit_batch_window: window,
        ..CatalogOptions::default()
    }
}

async fn open_over(store: &Arc<SlotMeter>, window: Duration) -> Catalog {
    Catalog::open(
        Arc::clone(store) as Arc<dyn ObjectStore>,
        open_options(window),
    )
    .await
    .unwrap()
}

/// A head-preserving maintenance commit: concurrent commits contend on slots
/// without conflicting on head, so a lost race is always benign and re-drives.
fn gc_insert(data_file_id: u64) -> RowOperation {
    RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![
            Cell::U64(data_file_id),
            Cell::Str(format!("orphan-{data_file_id}.parquet")),
            Cell::Bool(true),
            Cell::I64(0),
        ],
    }
}

/// One staged transaction, re-driving a lost race as a fresh transaction
/// exactly as DuckLake's commit loop does — the boundary forwarding acts at.
async fn drive_staged(catalog: &Catalog, op: RowOperation) -> Result<SnapshotId> {
    for _ in 0..512 {
        let mut tx = catalog.begin_staged(None, String::new()).await?;
        tx.stage(op.clone());
        match tx.commit().await {
            Ok(id) => return Ok(id),
            Err(Error::CommitConflict(_)) => {}
            Err(other) => return Err(other),
        }
    }
    Err(Error::CommitConflict("re-drive budget exhausted".into()))
}

/// Forces one lost slot race, arming the catalog's forwarding trigger.
async fn arm(catalog: &Catalog, winner: u64, loser: u64) {
    let mut win = catalog.begin_staged(None, String::new()).await.unwrap();
    let mut lose = catalog.begin_staged(None, String::new()).await.unwrap();
    win.stage(gc_insert(winner));
    lose.stage(gc_insert(loser));
    win.commit().await.unwrap();
    let _ = lose.commit().await;
}

/// Every scheduled file across a fresh read of the whole tail.
async fn scheduled_count(inner: &Arc<dyn ObjectStore>) -> usize {
    let reader = Catalog::open_read_only(Arc::clone(inner), open_options(Duration::ZERO))
        .await
        .unwrap();
    reader.snapshot().await.unwrap().scheduled_deletions().len()
}

/// One shape's tally.
struct Report {
    label: &'static str,
    committers: u64,
    commits: u64,
    client_attempts: u64,
    client_wins: u64,
    leader_attempts: u64,
    elapsed: Duration,
}

impl Report {
    fn print(&self) {
        let total_attempts = self.client_attempts + self.leader_attempts;
        let per_commit = total_attempts as f64 / self.commits.max(1) as f64;
        // A forwarded commit lands on the leader, never the committer's own
        // store, so a direct commit is one the committer won for itself.
        let direct = self.client_wins;
        let forwarded = self.commits.saturating_sub(direct);
        let win_share = forwarded as f64 / self.commits.max(1) as f64;
        println!(
            "{:<26} N={:<3} commits={:<4} PUT/commit={:>5.2}  client_PUT={:>4}  \
             leader_PUT={:>4}  forwarded={:>4} ({:>4.1}% win share)  {:>6.1}ms",
            self.label,
            self.committers,
            self.commits,
            per_commit,
            self.client_attempts,
            self.leader_attempts,
            forwarded,
            win_share * 100.0,
            self.elapsed.as_secs_f64() * 1000.0,
        );
    }
}

/// Runs `committers` tasks, each committing `per` head-preserving commits, over
/// metered client stores sharing one bucket. `leader` — when present — is
/// pre-discovered by arming each client, so the fleet forwards from its first
/// workload commit. `kill_after` fires a leader kill once the run is that far
/// in (a rough wall-clock split, since the kill races the workload).
async fn run_fleet(
    label: &'static str,
    inner: &Arc<dyn ObjectStore>,
    committers: u64,
    per: u64,
    leader: Option<&RunningLeader>,
    kill: Option<&RunningLeader>,
) -> Report {
    let mut client_stores = Vec::new();
    let mut clients = Vec::new();
    for _ in 0..committers {
        let store = SlotMeter::wrap(inner);
        let catalog = Arc::new(open_over(&store, Duration::ZERO).await);
        client_stores.push(store);
        clients.push(catalog);
    }

    // Arm each client so a live leader is discovered from the first commit.
    if leader.is_some() {
        for (offset, client) in clients.iter().enumerate() {
            let base = 500_000 + offset as u64 * 10;
            arm(client, base, base + 1).await;
        }
    }

    let base_client_attempts: u64 = client_stores.iter().map(|s| s.attempts()).sum();
    let base_client_wins: u64 = client_stores.iter().map(|s| s.wins()).sum();
    let base_leader_attempts = leader.map_or(0, |l| l.store.attempts());

    let started = Instant::now();

    if let Some(victim) = kill {
        // Kill the leader partway into the run so the fleet forwards for a while,
        // then must degrade to direct racing mid-flight.
        let victim_handle = victim.join.abort_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            victim_handle.abort();
        });
    }

    let mut tasks = Vec::new();
    for (offset, client) in clients.iter().enumerate() {
        let client = Arc::clone(client);
        let base = 1_000_000 + offset as u64 * 10_000;
        // Its own runtime: a staged commit's futures borrow the operations
        // they read, so this work is `!Send` and cannot ride the shared pool.
        tasks.push(tokio::task::spawn_blocking(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("client runtime")
                .block_on(async move {
                    for i in 0..per {
                        drive_staged(&client, gc_insert(base + i)).await.unwrap();
                    }
                });
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    let elapsed = started.elapsed();

    let client_attempts =
        client_stores.iter().map(|s| s.attempts()).sum::<u64>() - base_client_attempts;
    let client_wins = client_stores.iter().map(|s| s.wins()).sum::<u64>() - base_client_wins;
    let leader_attempts = leader.map_or(0, |l| l.store.attempts() - base_leader_attempts);

    Report {
        label,
        committers,
        commits: committers * per,
        client_attempts,
        client_wins,
        leader_attempts,
        elapsed,
    }
}

/// The three-shape bench across a fleet of `N` committers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bench: run with --ignored --nocapture"]
async fn adaptive_leader_bench() {
    const N: u64 = 8;
    const PER: u64 = 20;

    println!("\n=== adaptive leader bench: {N} committers, {PER} commits each ===");

    // The direct-racing amplification grows with the contending fleet size —
    // PUT/commit rises toward the committer count as more racers lose per win.
    println!("-- no-leader amplification vs fleet size --");
    for n in [2u64, 4, 8, 16] {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        run_fleet("no leader (direct)", &inner, n, PER, None, None)
            .await
            .print();
    }

    // Shape (a): no leader — direct racing. This is the pre-leader path: with no
    // advert on the log, nothing forwards, and PUT/commit shows the amplification.
    println!("-- three shapes at N={N} --");
    let no_leader = {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let report = run_fleet("no leader (direct)", &inner, N, PER, None, None).await;
        assert_eq!(
            report.leader_attempts, 0,
            "no leader path touches no leader"
        );
        assert_eq!(
            report.commits, report.client_wins,
            "every commit landed on its own committer's store"
        );
        assert!(scheduled_count(&inner).await as u64 >= report.commits);
        report
    };
    no_leader.print();

    // Shape (b): leader present from the start — the fleet forwards and coalesces.
    let with_leader = {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let leader = RunningLeader::spawn(&inner, Duration::ZERO).await;
        let report = run_fleet("leader (window=0)", &inner, N, PER, Some(&leader), None).await;
        leader.stop().await;
        report
    };
    with_leader.print();

    // Shape (b'): the same, with a widened coalescing window — the knob the RFC
    // reaches for before backoff asymmetry when convergence is too slow.
    let with_leader_windowed = {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let leader = RunningLeader::spawn(&inner, Duration::from_millis(2)).await;
        let report = run_fleet("leader (window=2ms)", &inner, N, PER, Some(&leader), None).await;
        leader.stop().await;
        report
    };
    with_leader_windowed.print();

    // Shape (c): leader killed mid-run — the survivors degrade to direct racing.
    let killed = {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let leader = RunningLeader::spawn(&inner, Duration::ZERO).await;
        let report = run_fleet(
            "leader killed mid-run",
            &inner,
            N,
            PER,
            Some(&leader),
            Some(&leader),
        )
        .await;
        leader.kill();
        // Every commit still landed despite the mid-run death.
        assert!(
            scheduled_count(&inner).await as u64 >= report.commits,
            "the kill degraded to direct; no commit was lost"
        );
        report
    };
    killed.print();

    println!();

    // The core claims, asserted so the bench is a real (ignored) test.
    let per = |r: &Report| (r.client_attempts + r.leader_attempts) as f64 / r.commits as f64;
    let direct_amp = per(&no_leader);
    let leader_amp = per(&with_leader).min(per(&with_leader_windowed));
    assert!(
        direct_amp > 2.0,
        "direct racing amplifies PUT/commit under contention: {direct_amp:.2}"
    );
    assert!(
        leader_amp < direct_amp,
        "a leader lowers PUT/commit versus direct racing: {leader_amp:.2} vs {direct_amp:.2}"
    );
    // A bounded spike, not a stall: the killed run finishes and stays under the
    // direct amplification (it began forwarded, then degraded).
    assert!(
        per(&killed) <= direct_amp + 1.0,
        "the kill is a bounded spike, not a runaway: {:.2}",
        per(&killed)
    );
}
