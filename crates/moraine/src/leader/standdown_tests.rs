//! Duels, supersession, and stand-down: the leader role's sloppiness budget
//! made concrete. Two leaders converge to one advertiser by freshest advert
//! (slot order the tiebreak, no clocks); a dead leader's advert is superseded
//! by a successor's announcement and clients re-adopt it; a superseded leader
//! stands down and its clients land elsewhere; a healthy leader is not
//! abandoned merely because uncontended direct commits land above its advert.
//! Every case wastes throughput; none loses a commit.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures::stream::BoxStream;
use moraine_wal::LeaderAdvert;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::{sync::Notify, task::JoinHandle};

use super::{Leader, LeaderConfig, current_advert};
use crate::{
    Error, Result,
    catalog::{Catalog, CatalogOptions, SnapshotId},
    transaction::staged::{Cell, RowOperation, TableKind},
};

/// A short poll so a duel resolves within a test's patience; the mechanism is
/// slot order, not this interval.
const FAST_POLL: Duration = Duration::from_millis(50);

/// Wraps a shared bucket, counting this catalog's own conditional-create PUTs
/// to `commits/`. A forwarded commit rides the leader's store, so it never
/// bumps the forwarding client's own counter — the observable that separates a
/// forwarded commit from a direct one.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    slot_puts: AtomicU64,
}

impl CountingStore {
    fn wrap(inner: &Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::clone(inner),
            slot_puts: AtomicU64::new(0),
        })
    }

    fn slot_puts(&self) -> u64 {
        self.slot_puts.load(Ordering::Relaxed)
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().starts_with("commits/") && matches!(opts.mode, PutMode::Create) {
            self.slot_puts.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.put_opts(location, payload, opts).await
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

/// A leader serving on a background task, capturing the endpoint and instance a
/// duel resolves on.
struct StandDownLeader {
    endpoint: String,
    instance: [u8; 16],
    shutdown: Arc<Notify>,
    crash: Arc<Notify>,
    join: JoinHandle<Result<()>>,
}

impl StandDownLeader {
    /// Binds a leader that advertises the real address it binds, polling `poll`
    /// for a fresher rival.
    async fn spawn(catalog: Arc<Catalog>, poll: Duration) -> Self {
        let endpoint = free_addr();
        let config = LeaderConfig {
            bind_address: endpoint.clone(),
            advertise_address: endpoint.clone(),
            max_sessions: 16,
            supersession_poll: poll,
        };
        let leader = Leader::bind(catalog, config).await.unwrap();
        let instance = leader.instance();
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
            endpoint,
            instance,
            shutdown,
            crash,
            join,
        }
    }

    /// A clean stand-down: withdraws the advert while still the effective
    /// leader.
    async fn stop(self) {
        self.shutdown.notify_one();
        let _ = self.join.await;
    }

    /// A crash: the serve future drops with its listener, leaving a stale
    /// advert and a dead port — no withdrawal. Awaited, so the port is shut
    /// before the caller observes the crash.
    async fn kill(self) {
        self.crash.notify_one();
        let _ = self.join.await;
    }
}

/// A concrete free loopback address: bound to resolve a port, then released so
/// the leader can bind and advertise it.
fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}

fn shared_bucket() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

fn open_options() -> CatalogOptions {
    CatalogOptions {
        commit_batch_window: Duration::ZERO,
        ..CatalogOptions::default()
    }
}

async fn open_over(store: &Arc<CountingStore>) -> Catalog {
    Catalog::open(Arc::clone(store) as Arc<dyn ObjectStore>, open_options())
        .await
        .unwrap()
}

/// The freshest advert as a fresh read-only attach materializes it — the same
/// discovery a client's forwarding trigger performs.
async fn read_advert(inner: &Arc<dyn ObjectStore>) -> Option<LeaderAdvert> {
    let reader = Catalog::open_read_only(Arc::clone(inner), open_options())
        .await
        .unwrap();
    current_advert(reader.slot_store()).await.unwrap()
}

/// A maintenance commit scheduling one file for deletion — head-preserving, so
/// concurrent commits contend on slots without conflicting on head, and each
/// unique id lands exactly once.
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

/// Drives one staged transaction, re-driving a lost race as a fresh transaction
/// exactly as DuckLake's commit loop does — the boundary the forwarding trigger
/// acts at.
async fn drive_staged(catalog: &Catalog, op: RowOperation) -> Result<SnapshotId> {
    for _ in 0..64 {
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

/// Forces one lost slot race on `catalog`, arming its forwarding trigger. Both
/// commits are the transaction's first attempt, so neither forwards (an
/// uncontended first try commits direct); the loser's work is left for a later
/// re-drive.
async fn arm(catalog: &Catalog, winner: u64, loser: u64) {
    let mut win = catalog.begin_staged(None, String::new()).await.unwrap();
    let mut lose = catalog.begin_staged(None, String::new()).await.unwrap();
    win.stage(gc_insert(winner));
    lose.stage(gc_insert(loser));
    win.commit().await.unwrap();
    let raced = lose.commit().await.unwrap_err();
    assert!(
        matches!(raced, Error::CommitConflict(_)),
        "the second transaction loses the slot: {raced:?}"
    );
    assert!(
        catalog.contention().races_lost >= 1,
        "a lost race arms forwarding"
    );
}

/// The scheduled-deletion ids as a fresh attach materializes them — reflecting
/// every committer's tail, direct and forwarded alike.
async fn scheduled_ids(inner: &Arc<dyn ObjectStore>) -> Vec<u64> {
    let reader = Catalog::open_read_only(Arc::clone(inner), open_options())
        .await
        .unwrap();
    let mut ids: Vec<u64> = reader
        .snapshot()
        .await
        .unwrap()
        .scheduled_deletions()
        .iter()
        .map(|deletion| deletion.data_file_id)
        .collect();
    ids.sort_unstable();
    ids
}

/// Waits for a leader's serve task to return — its stand-down — within a bound,
/// polling the join rather than any timing of the poll interval.
async fn await_standdown(join: &JoinHandle<Result<()>>) {
    for _ in 0..200 {
        if join.is_finished() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("leader did not stand down within the bound");
}

/// Two leaders started against one bucket converge to a single advertiser: the
/// later announcement rides the later slot and wins, the earlier one stands
/// down. A direct committer's work through the duel is never lost.
#[tokio::test]
async fn two_leaders_converge_to_one_advertiser_with_no_lost_commits() {
    let inner = shared_bucket();
    let store_a = CountingStore::wrap(&inner);
    let store_b = CountingStore::wrap(&inner);
    let cat_a = Arc::new(open_over(&store_a).await);
    let cat_b = Arc::new(open_over(&store_b).await);

    // A announces first, B second: B's advert rides the later slot and wins.
    let leader_a = StandDownLeader::spawn(Arc::clone(&cat_a), FAST_POLL).await;
    let leader_b = StandDownLeader::spawn(Arc::clone(&cat_b), FAST_POLL).await;

    // A direct committer does work while the duel resolves; none of it is lost.
    let store_c = CountingStore::wrap(&inner);
    let direct = open_over(&store_c).await;
    for id in 500..505 {
        drive_staged(&direct, gc_insert(id)).await.unwrap();
    }

    // The earlier announcer stands down; the later keeps serving.
    await_standdown(&leader_a.join).await;
    assert!(
        !leader_b.join.is_finished(),
        "the later announcer keeps serving"
    );

    // The log converges on one advertiser: B's live announcement.
    let advert = read_advert(&inner)
        .await
        .expect("a leader still advertises");
    assert_eq!(
        advert.instance, leader_b.instance,
        "the later announcer is the surviving advertiser"
    );
    assert!(
        advert.endpoint.is_some(),
        "the survivor's advert carries an endpoint"
    );

    // Every direct commit landed exactly once; the tail replays with no hole.
    assert_eq!(scheduled_ids(&inner).await, (500..505).collect::<Vec<_>>());

    leader_b.stop().await;
}

/// A crashed leader leaves a stale advert nothing decays. A client pays exactly
/// one failed connect against it, ages it locally, and commits direct; a
/// successor folder that announces supersedes the stale advert, and the client
/// adopts the successor on its next contention.
#[tokio::test]
async fn a_stale_advert_is_superseded_and_the_client_readopts_the_successor() {
    let inner = shared_bucket();

    // Leader A announces, then crashes without withdrawing: a stale advert, a
    // dead port.
    let store_a = CountingStore::wrap(&inner);
    let cat_a = Arc::new(open_over(&store_a).await);
    let leader_a = StandDownLeader::spawn(Arc::clone(&cat_a), FAST_POLL).await;
    let endpoint_a = leader_a.endpoint.clone();
    leader_a.kill().await;

    // An armed client pays one failed connect against the stale advert, ages it,
    // and commits direct.
    let client_store = CountingStore::wrap(&inner);
    let client = open_over(&client_store).await;
    arm(&client, 1, 2).await;
    drive_staged(&client, gc_insert(2)).await.unwrap();
    assert!(
        client.slot_store().forwarding.is_aged(&endpoint_a),
        "the dead endpoint is aged after exactly one probe"
    );

    // A successor takes the role and announces — a fresher advert in a later
    // slot supersedes the stale one.
    let store_b = CountingStore::wrap(&inner);
    let cat_b = Arc::new(open_over(&store_b).await);
    let leader_b = StandDownLeader::spawn(Arc::clone(&cat_b), FAST_POLL).await;
    let advert = read_advert(&inner).await.expect("the successor advertises");
    assert_eq!(
        advert.instance, leader_b.instance,
        "the successor's announcement is the freshest advert"
    );
    assert!(advert.endpoint.is_some());

    // The still-armed client adopts the successor: its next commit forwards to B,
    // never touching its own store.
    let b_before = store_b.slot_puts();
    let client_before = client_store.slot_puts();
    drive_staged(&client, gc_insert(5)).await.unwrap();
    assert!(
        store_b.slot_puts() > b_before,
        "the re-drive forwarded to the successor"
    );
    assert_eq!(
        client_store.slot_puts(),
        client_before,
        "the forwarded commit did not race the client's own store"
    );

    assert_eq!(scheduled_ids(&inner).await, vec![1, 2, 5]);

    leader_b.stop().await;
}

/// A leader superseded by a successor stands down, and a client that had
/// adopted it lands on the successor — never on the stood-down leader.
#[tokio::test]
async fn a_superseded_leader_stands_down_and_its_client_lands_on_the_successor() {
    let inner = shared_bucket();
    let store_a = CountingStore::wrap(&inner);
    let cat_a = Arc::new(open_over(&store_a).await);
    let leader_a = StandDownLeader::spawn(Arc::clone(&cat_a), FAST_POLL).await;

    // A client adopts A: armed, its commit forwards to A's store.
    let client_store = CountingStore::wrap(&inner);
    let client = open_over(&client_store).await;
    arm(&client, 1, 2).await;
    drive_staged(&client, gc_insert(2)).await.unwrap();
    assert!(store_a.slot_puts() > 0, "the client adopted the leader");

    // A successor announces; A is superseded and stands down.
    let store_b = CountingStore::wrap(&inner);
    let cat_b = Arc::new(open_over(&store_b).await);
    let leader_b = StandDownLeader::spawn(Arc::clone(&cat_b), FAST_POLL).await;
    await_standdown(&leader_a.join).await;

    // The client's next commit lands on the successor, never on the stood-down
    // leader.
    let a_before = store_a.slot_puts();
    let b_before = store_b.slot_puts();
    drive_staged(&client, gc_insert(3)).await.unwrap();
    assert_eq!(
        store_a.slot_puts(),
        a_before,
        "no commit reached the stood-down leader"
    );
    assert!(
        store_b.slot_puts() > b_before,
        "the client landed on the successor"
    );

    assert_eq!(scheduled_ids(&inner).await, vec![1, 2, 3]);

    leader_b.stop().await;
}

/// A healthy leader is not abandoned merely because uncontended direct commits
/// land above its advert: direct traffic is not a liveness signal, so the
/// leader keeps serving and keeps advertising.
#[tokio::test]
async fn a_healthy_leader_is_not_abandoned_by_direct_commits_above_its_advert() {
    let inner = shared_bucket();
    let store_a = CountingStore::wrap(&inner);
    let cat_a = Arc::new(open_over(&store_a).await);
    let leader = StandDownLeader::spawn(Arc::clone(&cat_a), FAST_POLL).await;

    // An uncontended committer lands many commit slots above the advert; being
    // uncontended it never forwards, so this is pure direct traffic.
    let store_c = CountingStore::wrap(&inner);
    let direct = open_over(&store_c).await;
    for id in 700..720 {
        drive_staged(&direct, gc_insert(id)).await.unwrap();
    }
    assert_eq!(
        direct.contention().races_lost,
        0,
        "uncontended direct traffic"
    );

    // Give the monitor several polls to observe the direct traffic.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The leader is not abandoned: still serving, still the advertiser.
    assert!(
        !leader.join.is_finished(),
        "direct commits above the advert do not stand a leader down"
    );
    let advert = read_advert(&inner)
        .await
        .expect("the healthy leader still advertises");
    assert_eq!(advert.instance, leader.instance);
    assert!(advert.endpoint.is_some());

    leader.stop().await;
}
