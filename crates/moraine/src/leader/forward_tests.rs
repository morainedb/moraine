//! Client-side contention-triggered forwarding: a lost race arms forwarding,
//! the re-drive lands through the leader, an unreachable leader retreats to a
//! direct commit with the endpoint aged, an uncontended client never connects,
//! and no transaction id ever reaches two committers.
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
use moraine_remote::Request;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
};

use super::{Leader, LeaderConfig};
use crate::{
    Error, Result,
    catalog::{Catalog, CatalogOptions, SnapshotId},
    transaction::staged::{Cell, RowOperation, TableKind},
};

/// Wraps a shared bucket, counting this catalog's own conditional-create PUTs
/// to `commits/`. A forwarded commit rides the *leader's* store, so it never
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

/// A leader serving on a background task.
struct RunningLeader {
    shutdown: Arc<Notify>,
    join: JoinHandle<Result<()>>,
}

impl RunningLeader {
    async fn spawn(catalog: Arc<Catalog>, config: LeaderConfig) -> Self {
        let leader = Leader::bind(catalog, config).await.unwrap();
        let shutdown = Arc::new(Notify::new());
        // The leader's sessions are `!Send`, so `serve` needs a runtime of its
        // own rather than a slot on the shared pool.
        let join = tokio::task::spawn_blocking({
            let shutdown = Arc::clone(&shutdown);
            move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("leader test runtime")
                    .block_on(leader.serve(shutdown))
            }
        });
        Self { shutdown, join }
    }

    /// A clean stand-down: withdraws the advert.
    async fn stop(self) {
        self.shutdown.notify_one();
        let _ = self.join.await;
    }

    /// A crash: the task drops with its listener, leaving a stale advert and a
    /// dead port — no withdrawal.
    fn kill(self) {
        self.join.abort();
    }
}

/// A concrete free loopback address: bound to resolve a port, then released so
/// the leader can bind and advertise it — a `:0` bind would advertise port 0,
/// which no client could connect to.
fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}

/// Spawns a leader that advertises the real address it binds, so clients
/// discover it through the log.
async fn spawn_reachable_leader(catalog: Arc<Catalog>) -> RunningLeader {
    RunningLeader::spawn(catalog, LeaderConfig::new(free_addr(), 16)).await
}

fn shared_bucket() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

/// When a fault proxy severs a forwarded session, relative to the leader's slot
/// PUT — the two shapes of an ambiguous forwarded commit.
#[derive(Clone, Copy)]
enum DropWhen {
    /// Forward the `Commit`, wait for the leader's `Committed` (the slot is
    /// durable), then drop the ack: the slot **landed** but the client did not
    /// hear so.
    AfterLeaderLands,
    /// Drop on the client's `Commit` before it reaches the leader: the slot
    /// **never landed**, but the client cannot tell.
    BeforeLeaderLands,
}

/// A framed-message proxy between a forwarding client and a real leader that
/// severs the session at `drop_when`, manufacturing an ambiguous outcome the
/// client must resolve by identity. The forwarded session is a strict
/// request/response alternation, so the proxy needs no duplex machinery.
fn spawn_fault_proxy(proxy: TcpListener, leader_addr: String, drop_when: DropWhen) {
    tokio::spawn(async move {
        while let Ok((client, _)) = proxy.accept().await {
            let leader_addr = leader_addr.clone();
            tokio::spawn(async move {
                let _ = proxy_session(client, &leader_addr, drop_when).await;
            });
        }
    });
}

async fn proxy_session(
    mut client: TcpStream,
    leader_addr: &str,
    drop_when: DropWhen,
) -> std::io::Result<()> {
    let mut leader = TcpStream::connect(leader_addr).await?;
    loop {
        let request = read_frame(&mut client).await?;
        let is_commit = matches!(Request::decode(&request), Ok(Request::Commit { .. }));

        if is_commit && matches!(drop_when, DropWhen::BeforeLeaderLands) {
            // The leader never sees the commit: its session rolls back, nothing
            // lands.
            return Ok(());
        }

        write_frame(&mut leader, &request).await?;
        let response = read_frame(&mut leader).await?;

        // Only a real landing manufactures the ambiguous-landed shape. A
        // forwarded commit can lose its slot to a concurrent direct committer
        // (the leader answers a conflict, nothing landed); that is relayed so the
        // client re-drives, and the proxy waits for the attempt that actually
        // commits before dropping its ack.
        if is_commit {
            let landed = matches!(
                moraine_remote::Response::decode(&response),
                Ok(moraine_remote::Response::Committed { .. })
            );
            if landed {
                return Ok(());
            }
        }
        write_frame(&mut client, &response).await?;
    }
}

async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let mut length = [0u8; 4];
    reader.read_exact(&mut length).await?;
    let mut framed = vec![0u8; u32::from_be_bytes(length) as usize];
    reader.read_exact(&mut framed).await?;
    Ok(framed)
}

async fn write_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    framed: &[u8],
) -> std::io::Result<()> {
    writer
        .write_all(
            &u32::try_from(framed.len())
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        )
        .await?;
    writer.write_all(framed).await?;
    writer.flush().await
}

/// Commit-bearing slots across the whole tail, read through a fresh attach —
/// the exactly-once counter (an advert or withdrawal slot carries no commits).
async fn commit_bearing_slots(inner: &Arc<dyn ObjectStore>) -> usize {
    let reader = Catalog::open_read_only(Arc::clone(inner), open_options(Duration::ZERO))
        .await
        .unwrap();
    let tail = reader.slot_store().slots.read_tail(1).await.unwrap();
    tail.slots
        .iter()
        .filter(|(_, envelope)| !envelope.commits.is_empty())
        .count()
}

fn open_options(window: Duration) -> CatalogOptions {
    CatalogOptions {
        commit_batch_window: window,
        ..CatalogOptions::default()
    }
}

async fn open_over(store: &Arc<CountingStore>) -> Catalog {
    Catalog::open(
        Arc::clone(store) as Arc<dyn ObjectStore>,
        open_options(Duration::ZERO),
    )
    .await
    .unwrap()
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
            // A lost race re-drives as a fresh transaction, exactly as DuckLake's
            // loop does.
            Err(Error::CommitConflict(_)) => {}
            Err(other) => return Err(other),
        }
    }
    Err(Error::CommitConflict("re-drive budget exhausted".into()))
}

/// Forces one lost slot race on `catalog`, arming its forwarding trigger: two
/// transactions pin the same slot; the first wins, the second loses.
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

/// Every committed transaction id across the whole slot tail.
async fn all_transaction_ids(catalog: &Catalog) -> Vec<[u8; 16]> {
    let tail = catalog.slot_store().slots.read_tail(1).await.unwrap();
    tail.slots
        .iter()
        .flat_map(|(_, envelope)| envelope.commits.iter().map(|commit| commit.transaction_id))
        .collect()
}

/// The scheduled-deletion ids as a fresh attach materializes them — reading
/// through a new catalog sidesteps any one handle's cached head, so it reflects
/// every committer's tail, direct and forwarded alike.
async fn scheduled_ids(inner: &Arc<dyn ObjectStore>) -> Vec<u64> {
    let reader = Catalog::open_read_only(Arc::clone(inner), open_options(Duration::ZERO))
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

/// A lost race arms forwarding, and the very next re-drive lands through the
/// leader — the ephemeral single-commit case: direct first attempt, forwarded
/// retry. The forwarded commit never touches the client's own store.
#[tokio::test]
async fn a_single_lost_race_forwards_the_re_drive_through_the_leader() {
    let inner = shared_bucket();
    let leader_store = CountingStore::wrap(&inner);
    let leader_catalog = Arc::new(open_over(&leader_store).await);
    let leader = spawn_reachable_leader(Arc::clone(&leader_catalog)).await;

    let client_store = CountingStore::wrap(&inner);
    let client = open_over(&client_store).await;

    // A forced lost race arms forwarding.
    arm(&client, 1, 2).await;
    let client_puts_after_arm = client_store.slot_puts();
    let leader_puts_after_arm = leader_store.slot_puts();

    // The re-drive of the loser's work opens forwarded: it lands through the
    // leader, so the client's own store sees no new slot PUT.
    drive_staged(&client, gc_insert(2)).await.unwrap();

    assert_eq!(
        client_store.slot_puts(),
        client_puts_after_arm,
        "the forwarded re-drive did not race a slot on the client's own store"
    );
    assert!(
        leader_store.slot_puts() > leader_puts_after_arm,
        "the forwarded commit landed through the leader's store"
    );

    assert_eq!(scheduled_ids(&inner).await, vec![1, 2]);

    leader.stop().await;
}

/// Two contending handles converge onto the leader: once armed, both forward,
/// so their own stores carry almost no slot PUTs even as they commit a full
/// workload.
#[tokio::test]
async fn two_contending_handles_converge_onto_the_leader() {
    let inner = shared_bucket();
    let leader_store = CountingStore::wrap(&inner);
    let leader_catalog = Arc::new(open_over(&leader_store).await);
    let leader = spawn_reachable_leader(Arc::clone(&leader_catalog)).await;

    let store_a = CountingStore::wrap(&inner);
    let store_b = CountingStore::wrap(&inner);
    let client_a = Arc::new(open_over(&store_a).await);
    let client_b = Arc::new(open_over(&store_b).await);

    // Pre-arm both, then measure only the workload that follows.
    arm(&client_a, 100, 101).await;
    arm(&client_b, 200, 201).await;
    let base_a = store_a.slot_puts();
    let base_b = store_b.slot_puts();
    let base_leader = leader_store.slot_puts();

    let per_client = 10u64;
    let task_a = {
        let client = Arc::clone(&client_a);
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("client runtime")
                .block_on(async move {
                    for i in 0..per_client {
                        drive_staged(&client, gc_insert(1_000 + i)).await.unwrap();
                    }
                });
        })
    };
    let task_b = {
        let client = Arc::clone(&client_b);
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("client runtime")
                .block_on(async move {
                    for i in 0..per_client {
                        drive_staged(&client, gc_insert(2_000 + i)).await.unwrap();
                    }
                });
        })
    };
    task_a.await.unwrap();
    task_b.await.unwrap();

    let client_puts = (store_a.slot_puts() - base_a) + (store_b.slot_puts() - base_b);
    assert!(
        client_puts < per_client,
        "converged onto the leader: {client_puts} client slot PUTs for {} commits",
        2 * per_client
    );
    assert!(
        leader_store.slot_puts() > base_leader,
        "the workload landed through the leader"
    );

    // Every file from both clients is scheduled exactly once.
    let ids = scheduled_ids(&inner).await;
    // The arm winners land; their losers are never re-driven here.
    let mut expected: Vec<u64> = (1_000..1_000 + per_client)
        .chain(2_000..2_000 + per_client)
        .chain([100, 200])
        .collect();
    expected.sort_unstable();
    assert_eq!(ids, expected, "exactly-once across both forwarding clients");

    leader.stop().await;
}

/// A client that cannot reach the advertised address times out once, ages the
/// hint locally, commits directly, and does not probe the endpoint again.
#[tokio::test]
async fn an_unreachable_leader_times_out_once_then_commits_direct() {
    let inner = shared_bucket();
    let leader_store = CountingStore::wrap(&inner);
    let leader_catalog = Arc::new(open_over(&leader_store).await);
    // Bind a real listener but advertise a black-hole address (RFC 5737
    // TEST-NET-1) that never answers, so a forwarding client times out.
    let config = LeaderConfig {
        bind_address: free_addr(),
        advertise_address: "192.0.2.1:9".to_string(),
        max_sessions: 16,
        supersession_poll: Duration::from_secs(1),
    };
    let leader = RunningLeader::spawn(Arc::clone(&leader_catalog), config).await;

    let client_store = CountingStore::wrap(&inner);
    let client = open_over(&client_store).await;

    arm(&client, 1, 2).await;
    let before = client_store.slot_puts();

    // The first armed re-drive pays one connect timeout, then commits directly.
    drive_staged(&client, gc_insert(2)).await.unwrap();
    assert!(
        client_store.slot_puts() > before,
        "the retreat committed on the client's own store"
    );

    // The hint is aged after that one failed probe — a non-timing invariant for
    // "stops trying": the client will not connect this endpoint again.
    assert!(
        client.slot_store().forwarding.is_aged("192.0.2.1:9"),
        "the unreachable endpoint is aged after one probe"
    );

    // A second commit lands direct with no further probe.
    drive_staged(&client, gc_insert(3)).await.unwrap();
    assert_eq!(scheduled_ids(&inner).await, vec![1, 2, 3]);

    leader.stop().await;
}

/// A solo, uncontended client never connects to a live leader: no lost race, no
/// forwarding — it commits directly past the advert.
#[tokio::test]
async fn a_solo_client_never_connects_to_a_live_leader() {
    let inner = shared_bucket();
    let leader_store = CountingStore::wrap(&inner);
    let leader_catalog = Arc::new(open_over(&leader_store).await);
    let leader = spawn_reachable_leader(Arc::clone(&leader_catalog)).await;

    let client_store = CountingStore::wrap(&inner);
    let client = open_over(&client_store).await;

    let before_client = client_store.slot_puts();
    let before_leader = leader_store.slot_puts();

    // No prior lost race: this commit stays direct even with a live leader.
    drive_staged(&client, gc_insert(7)).await.unwrap();

    assert_eq!(client.contention().races_lost, 0, "uncontended");
    assert_eq!(
        client_store.slot_puts(),
        before_client + 1,
        "the solo commit raced its own slot directly"
    );
    assert_eq!(
        leader_store.slot_puts(),
        before_leader,
        "an uncontended client never routes through the leader"
    );

    leader.stop().await;
}

/// A mid-flight leader death rolls the forwarded session back; the
/// DuckLake-style re-drive lands directly, and no transaction id reaches two
/// committers.
#[tokio::test]
async fn a_dead_leader_rolls_back_and_the_re_drive_lands_direct() {
    let inner = shared_bucket();
    let leader_store = CountingStore::wrap(&inner);
    let leader_catalog = Arc::new(open_over(&leader_store).await);
    let leader = spawn_reachable_leader(Arc::clone(&leader_catalog)).await;

    let client_store = CountingStore::wrap(&inner);
    let client = open_over(&client_store).await;

    arm(&client, 1, 2).await;

    // The leader crashes without withdrawing: its advert is stale, its port dead.
    leader.kill();

    let before = client_store.slot_puts();
    // The armed re-drive attempts the dead leader, retreats, and lands direct.
    drive_staged(&client, gc_insert(2)).await.unwrap();
    assert!(
        client_store.slot_puts() > before,
        "the re-drive committed directly after the leader died"
    );

    assert_eq!(scheduled_ids(&inner).await, vec![1, 2]);

    // No id rides two committers.
    let ids = all_transaction_ids(&client).await;
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "every committed id is distinct");
}

/// The subset guard: a commit that lands forwarded and a later direct commit
/// use distinct ids, and no id ever appears in two slots — the same id never
/// reaches two committers.
#[tokio::test]
async fn a_forwarded_then_direct_re_drive_uses_a_fresh_id() {
    let inner = shared_bucket();
    let leader_store = CountingStore::wrap(&inner);
    let leader_catalog = Arc::new(open_over(&leader_store).await);
    let leader = spawn_reachable_leader(Arc::clone(&leader_catalog)).await;

    let client_store = CountingStore::wrap(&inner);
    let client = open_over(&client_store).await;

    // Arm, then land a forwarded commit through the live leader.
    arm(&client, 1, 2).await;
    drive_staged(&client, gc_insert(2)).await.unwrap();
    let ids_after_forward: std::collections::HashSet<[u8; 16]> =
        all_transaction_ids(&client).await.into_iter().collect();

    // Kill the leader; the next armed re-drive retreats to a fresh direct
    // transaction with a fresh id.
    leader.kill();
    drive_staged(&client, gc_insert(3)).await.unwrap();

    let ids = all_transaction_ids(&client).await;
    let unique: std::collections::HashSet<[u8; 16]> = ids.iter().copied().collect();
    assert_eq!(
        unique.len(),
        ids.len(),
        "no transaction id reaches two committers"
    );
    // The direct re-drive introduced at least one id the forwarded phase did not
    // carry — a fresh id, never one already on a slot.
    assert!(
        unique.len() > ids_after_forward.len(),
        "the direct re-drive committed under a fresh id"
    );

    assert_eq!(scheduled_ids(&inner).await, vec![1, 2, 3]);
}

/// A mixed fleet — forwarding clients and a direct committer — upholds
/// one-winner and exactly-once: every file lands once, the tail has no hole,
/// and every id is distinct.
#[tokio::test]
async fn a_mixed_fleet_holds_one_winner_and_exactly_once() {
    let inner = shared_bucket();
    let leader_store = CountingStore::wrap(&inner);
    let leader_catalog = Arc::new(open_over(&leader_store).await);
    let leader = spawn_reachable_leader(Arc::clone(&leader_catalog)).await;

    // Two forwarding clients (pre-armed) and one direct committer that never
    // arms — a mixed fleet on one bucket.
    let store_a = CountingStore::wrap(&inner);
    let store_b = CountingStore::wrap(&inner);
    let store_c = CountingStore::wrap(&inner);
    let forward_a = Arc::new(open_over(&store_a).await);
    let forward_b = Arc::new(open_over(&store_b).await);
    let direct_c = Arc::new(open_over(&store_c).await);

    arm(&forward_a, 9_000, 9_001).await;
    arm(&forward_b, 9_100, 9_101).await;

    let each = 8u64;
    let mut tasks = Vec::new();
    for (catalog, base) in [
        (Arc::clone(&forward_a), 10_000u64),
        (Arc::clone(&forward_b), 20_000u64),
        (Arc::clone(&direct_c), 30_000u64),
    ] {
        tasks.push(tokio::task::spawn_blocking(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("client runtime")
                .block_on(async move {
                    for i in 0..each {
                        drive_staged(&catalog, gc_insert(base + i)).await.unwrap();
                    }
                });
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    // Exactly-once: every file lands once (the tail replays with no hole, which
    // a fresh materialization would surface as `Corruption`).
    // The arm winners land; their losers are never re-driven here.
    let mut expected: Vec<u64> = (10_000..10_000 + each)
        .chain(20_000..20_000 + each)
        .chain(30_000..30_000 + each)
        .chain([9_000, 9_100])
        .collect();
    expected.sort_unstable();
    assert_eq!(
        scheduled_ids(&inner).await,
        expected,
        "exactly-once across the mixed fleet"
    );

    // One-winner at the id level too: no id reaches two committers.
    let ids = all_transaction_ids(&leader_catalog).await;
    let unique: std::collections::HashSet<[u8; 16]> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "every committed id is distinct");

    leader.stop().await;
}

/// Sets up a leader reachable only through a fault proxy that severs the
/// session at `drop_when`, so a forwarded commit's outcome is ambiguous.
/// Returns the live leader and a client armed for forwarding.
async fn ambiguous_setup(
    leader_store: &Arc<CountingStore>,
    client_store: &Arc<CountingStore>,
    drop_when: DropWhen,
) -> (RunningLeader, Catalog) {
    let leader_catalog = Arc::new(open_over(leader_store).await);
    let leader_bind = free_addr();
    // The proxy binds its own port and the leader advertises it, so the client
    // discovers the proxy through the log and every forwarded byte flows through
    // the fault.
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy.local_addr().unwrap().to_string();
    let leader = RunningLeader::spawn(
        Arc::clone(&leader_catalog),
        LeaderConfig {
            bind_address: leader_bind.clone(),
            advertise_address: proxy_addr,
            max_sessions: 16,
            supersession_poll: Duration::from_secs(1),
        },
    )
    .await;
    spawn_fault_proxy(proxy, leader_bind, drop_when);

    let client = open_over(client_store).await;
    arm(&client, 1, 2).await;
    (leader, client)
}

/// The highest-stakes path: a forwarded commit whose slot **lands** but whose
/// ack is lost. The client must resolve by identity to `Committed` and must not
/// retreat — a retreat would double-apply. Asserts the resolution and that the
/// transaction applied **exactly once**.
///
/// Revert check: making `resolve_ambiguous` retreat instead of resolving turns
/// this into an unbounded land-then-retreat-then-re-forward loop that exhausts
/// the re-drive budget — `drive_staged` then returns `Err` and the `unwrap`
/// below panics, and the slot count blows past one. Confirmed failing on that
/// change; restored.
#[tokio::test]
async fn an_ambiguous_landed_commit_resolves_committed_exactly_once() {
    let inner = shared_bucket();
    let leader_store = CountingStore::wrap(&inner);
    let client_store = CountingStore::wrap(&inner);
    let (leader, client) =
        ambiguous_setup(&leader_store, &client_store, DropWhen::AfterLeaderLands).await;

    let slots_before = commit_bearing_slots(&inner).await;
    let client_puts_before = client_store.slot_puts();

    // The forwarded commit lands on the leader; the proxy drops the ack; the
    // client resolves the ambiguous outcome by id to Committed.
    let committed = drive_staged(&client, gc_insert(99)).await.unwrap();
    assert!(committed.get() >= 1, "resolved to a committed snapshot");

    // Exactly once: the ambiguous commit landed a single slot — no retreat, no
    // double-apply.
    assert_eq!(
        commit_bearing_slots(&inner).await,
        slots_before + 1,
        "the ambiguous-landed commit applied exactly once"
    );
    assert_eq!(scheduled_ids(&inner).await, vec![1, 99]);
    // The client raced no slot of its own across the forward: the only landing
    // was the forwarded one it resolved to, not a direct retreat.
    assert_eq!(
        client_store.slot_puts(),
        client_puts_before,
        "the client did not retreat to a direct commit"
    );

    leader.stop().await;
}

/// The reverse: a forwarded commit whose slot **never landed** (the session
/// severed before the leader saw it). The client resolves to absent and
/// retreats to a fresh direct commit that lands it once — no lost write, no
/// double-apply.
#[tokio::test]
async fn an_ambiguous_unlanded_commit_retreats_direct_exactly_once() {
    let inner = shared_bucket();
    let leader_store = CountingStore::wrap(&inner);
    let client_store = CountingStore::wrap(&inner);
    let (leader, client) =
        ambiguous_setup(&leader_store, &client_store, DropWhen::BeforeLeaderLands).await;

    let before = commit_bearing_slots(&inner).await;

    // The forward never reached the leader; the client resolves to absent and
    // retreats to a direct race that lands the work.
    drive_staged(&client, gc_insert(99)).await.unwrap();

    assert_eq!(
        commit_bearing_slots(&inner).await,
        before + 1,
        "the retreat landed the work exactly once"
    );
    assert_eq!(scheduled_ids(&inner).await, vec![1, 99]);
    assert!(
        client_store.slot_puts() > 0,
        "the retreat committed directly on the client's own store"
    );

    leader.stop().await;
}
