//! Leader-role integration tests: binding, announcement, forwarded sessions,
//! the constant-time handshake, chaining, and clean withdrawal.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures::stream::BoxStream;
use moraine_remote::{Client, Request, Response, WireCell, WireRowOperation};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::{sync::Notify, task::JoinHandle};

use super::{Leader, LeaderConfig, current_advert};
use crate::{
    catalog::{Catalog, CatalogOptions},
    store::{
        handle::ReadHandle,
        key::{Key, SysKey},
        read, value,
    },
};

/// Wraps [`InMemory`], counting conditional-create PUTs to `commits/` — one
/// per slot object, so a batch that coalesces several commits counts once.
#[derive(Debug)]
struct SlotCounter {
    inner: InMemory,
    slot_puts: AtomicU64,
}

impl SlotCounter {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            slot_puts: AtomicU64::new(0),
        }
    }

    fn slot_puts(&self) -> u64 {
        self.slot_puts.load(Ordering::Relaxed)
    }
}

impl std::fmt::Display for SlotCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlotCounter({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlotCounter {
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

/// Wraps [`InMemory`] and, on the Nth commit-bearing slot create, fails the
/// put once with a transport error while planting nothing — so the read-back
/// finds the slot absent and the funnel sees an ambiguous transport blip it
/// must re-race, not a lost slot it must conflict. The advert's empty-envelope
/// create at bind is create one, so `fail_on` two hits the first commit.
#[derive(Debug)]
struct BlipOnce {
    inner: InMemory,
    creates: AtomicU64,
    fail_on: u64,
    fired: AtomicBool,
}

impl BlipOnce {
    fn new(fail_on: u64) -> Self {
        Self {
            inner: InMemory::new(),
            creates: AtomicU64::new(0),
            fail_on,
            fired: AtomicBool::new(false),
        }
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::Relaxed)
    }
}

impl std::fmt::Display for BlipOnce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlipOnce({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for BlipOnce {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().starts_with("commits/") && matches!(opts.mode, PutMode::Create) {
            let nth = self.creates.fetch_add(1, Ordering::SeqCst) + 1;
            if nth == self.fail_on && !self.fired.swap(true, Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "BlipOnce",
                    source: "injected transport blip".into(),
                });
            }
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

/// A leader bound over `catalog`, its listener spawned; dropped by
/// [`stop`](Spawned::stop), which withdraws cleanly.
struct Spawned {
    addr: std::net::SocketAddr,
    secret: [u8; 32],
    instance: [u8; 16],
    shutdown: Arc<Notify>,
    join: JoinHandle<crate::Result<()>>,
}

impl Spawned {
    async fn bind(catalog: Arc<Catalog>) -> Self {
        let leader = Leader::bind(catalog, LeaderConfig::new("127.0.0.1:0", 16))
            .await
            .unwrap();
        let addr = leader.local_addr().unwrap();
        let secret = leader.secret();
        let instance = leader.instance();
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
        Self {
            addr,
            secret,
            instance,
            shutdown,
            join,
        }
    }

    async fn stop(self) {
        self.shutdown.notify_one();
        self.join.await.unwrap().unwrap();
    }
}

async fn open_catalog(store: &Arc<dyn ObjectStore>, window: Duration) -> Arc<Catalog> {
    Arc::new(
        Catalog::open(
            Arc::clone(store),
            CatalogOptions {
                commit_batch_window: window,
                ..CatalogOptions::default()
            },
        )
        .await
        .unwrap(),
    )
}

async fn connect(addr: std::net::SocketAddr) -> Client {
    Client::connect(addr, Duration::from_secs(5)).await.unwrap()
}

/// A reader opened at the current manifest, so a just-committed write is seen
/// rather than lagged behind the attach reader's poll.
async fn fresh_reader(catalog: &Arc<Catalog>) -> slatedb::DbReader {
    let store = catalog.slot_store();
    crate::store::open::StoreBuilder::new(&store.options.path, store.object_store.clone())
        .open_reader()
        .await
        .map(|(reader, _)| reader)
        .unwrap()
}

/// A staged head-preserving maintenance commit: one file scheduled for
/// deletion. Head-preserving, so several coalesce trivially.
fn gc_insert(data_file_id: u64) -> WireRowOperation {
    WireRowOperation::Insert {
        table: 19,
        cells: vec![
            WireCell::U64(data_file_id),
            WireCell::Str(format!("orphan-{data_file_id}.parquet")),
            WireCell::Bool(true),
            WireCell::I64(0),
        ],
    }
}

/// A snapshot-only minting commit that advances head to `new_id`.
fn snapshot_mint(new_id: u64) -> Vec<WireRowOperation> {
    vec![
        WireRowOperation::Insert {
            table: 0,
            cells: vec![
                WireCell::U64(new_id),
                WireCell::I64(0),
                WireCell::U64(0),
                WireCell::U64(1),
                WireCell::U64(0),
            ],
        },
        WireRowOperation::Insert {
            table: 1,
            cells: vec![
                WireCell::U64(new_id),
                WireCell::Str(String::new()),
                WireCell::Null,
                WireCell::Null,
                WireCell::Null,
            ],
        },
    ]
}

async fn hello(client: &mut Client, secret: [u8; 32]) -> Response {
    client
        .request(&Request::Hello {
            token: secret,
            protocol_version: 1,
        })
        .await
        .unwrap()
}

/// A full session that stages `ops` and commits, returning the commit response.
async fn commit_session(
    addr: std::net::SocketAddr,
    secret: [u8; 32],
    ops: Vec<WireRowOperation>,
) -> Response {
    let mut client = connect(addr).await;
    assert_eq!(hello(&mut client, secret).await, Response::Ok);
    assert_eq!(client.request(&Request::Begin).await.unwrap(), Response::Ok);
    for op in ops {
        assert_eq!(
            client.request(&Request::Stage(op)).await.unwrap(),
            Response::Ok
        );
    }
    client
        .request(&Request::Commit {
            transaction_id: *uuid::Uuid::new_v4().as_bytes(),
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn a_leader_binds_announces_and_serves_a_full_session() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = open_catalog(&store, Duration::ZERO).await;
    let leader = Spawned::bind(Arc::clone(&catalog)).await;

    // Binding announced an endpoint-carrying advert.
    let advert = current_advert(catalog.slot_store()).await.unwrap().unwrap();
    assert_eq!(advert.instance, leader.instance);
    assert!(
        advert.endpoint.is_some(),
        "an announcement carries an endpoint"
    );

    // A full session: handshake, begin, read, stage, commit.
    let mut client = connect(leader.addr).await;
    assert_eq!(hello(&mut client, leader.secret).await, Response::Ok);
    assert_eq!(client.request(&Request::Begin).await.unwrap(), Response::Ok);

    let Response::Snapshot(rows) = client.request(&Request::VisibleSnapshots).await.unwrap() else {
        panic!("a read returns snapshot rows");
    };
    assert!(!rows.is_empty(), "bootstrap leaves a visible snapshot");

    assert_eq!(
        client.request(&Request::Stage(gc_insert(1))).await.unwrap(),
        Response::Ok
    );
    let committed = client
        .request(&Request::Commit {
            transaction_id: *uuid::Uuid::new_v4().as_bytes(),
        })
        .await
        .unwrap();
    assert!(
        matches!(committed, Response::Committed { .. }),
        "{committed:?}"
    );

    leader.stop().await;

    // The forwarded commit is durable: a fresh attach sees the scheduled file.
    let reopened = Catalog::open(Arc::clone(&store), CatalogOptions::default())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .snapshot()
            .await
            .unwrap()
            .scheduled_deletions()
            .len(),
        1
    );
}

#[tokio::test]
async fn a_wrong_token_is_refused_before_any_catalog_work() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = open_catalog(&store, Duration::ZERO).await;
    let leader = Spawned::bind(Arc::clone(&catalog)).await;

    let mut client = connect(leader.addr).await;
    let response = client
        .request(&Request::Hello {
            token: [0xAB; 32],
            protocol_version: 1,
        })
        .await
        .unwrap();
    assert!(
        matches!(response, Response::Error { .. }),
        "a wrong token is refused: {response:?}"
    );

    // The session is closed: the next request finds the connection gone.
    assert!(client.request(&Request::Begin).await.is_err());

    // No slot was written for the refused session; only the announce slot exists.
    let tail = catalog.slot_store().slots.read_tail(1).await.unwrap();
    let committing = tail
        .slots
        .iter()
        .filter(|(_, envelope)| !envelope.commits.is_empty())
        .count();
    assert_eq!(committing, 0, "a refused handshake stages nothing");

    leader.stop().await;
}

#[tokio::test]
async fn a_dropped_connection_rolls_the_session_back_leaving_no_trace() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = open_catalog(&store, Duration::ZERO).await;
    let leader = Spawned::bind(Arc::clone(&catalog)).await;

    {
        let mut client = connect(leader.addr).await;
        assert_eq!(hello(&mut client, leader.secret).await, Response::Ok);
        assert_eq!(client.request(&Request::Begin).await.unwrap(), Response::Ok);
        assert_eq!(
            client.request(&Request::Stage(gc_insert(7))).await.unwrap(),
            Response::Ok
        );
        // Drop without committing.
    }

    // Nothing landed: no commit-bearing slot, and no scheduled deletion.
    let tail = catalog.slot_store().slots.read_tail(1).await.unwrap();
    let committing = tail
        .slots
        .iter()
        .filter(|(_, envelope)| !envelope.commits.is_empty())
        .count();
    assert_eq!(committing, 0, "an uncommitted session leaves no trace");
    assert_eq!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .scheduled_deletions()
            .len(),
        0
    );

    leader.stop().await;
}

#[tokio::test]
async fn concurrent_forwarded_commits_share_a_slot() {
    let counter = Arc::new(SlotCounter::new());
    let store: Arc<dyn ObjectStore> = Arc::clone(&counter) as Arc<dyn ObjectStore>;
    // A batch window wide enough that concurrent submits coalesce.
    let catalog = open_catalog(&store, Duration::from_millis(500)).await;
    let leader = Spawned::bind(Arc::clone(&catalog)).await;

    // Slots written by the announce, before any commit.
    let announced = counter.slot_puts();

    // Three sessions commit concurrently; each reads its premise through the
    // leader, so their head-preserving commits chain and coalesce.
    let count = 3;
    let mut handles = Vec::new();
    for id in 0..count {
        let addr = leader.addr;
        let secret = leader.secret;
        handles.push(tokio::spawn(async move {
            commit_session(addr, secret, vec![gc_insert(1000 + id)]).await
        }));
    }
    for handle in handles {
        assert!(matches!(handle.await.unwrap(), Response::Committed { .. }));
    }

    let commit_slots = counter.slot_puts() - announced;
    assert!(
        commit_slots < count,
        "coalescing: {count} commits landed in {commit_slots} slots"
    );
    assert!(commit_slots >= 1, "the commits landed in at least one slot");

    // All three scheduled files are durable.
    assert_eq!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .scheduled_deletions()
            .len(),
        3
    );

    leader.stop().await;
}

#[tokio::test]
async fn sequential_minting_commits_chain_through_the_leader() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = open_catalog(&store, Duration::ZERO).await;
    let leader = Spawned::bind(Arc::clone(&catalog)).await;

    // The first client authors snapshot 1 against head 0.
    let first = commit_session(leader.addr, leader.secret, snapshot_mint(1)).await;
    assert_eq!(first, Response::Committed { snapshot_id: 1 });

    // The second reads head 1 *through the leader* and authors snapshot 2:
    // its premise is the first commit's result, so the chain is by
    // construction.
    let second = commit_session(leader.addr, leader.secret, snapshot_mint(2)).await;
    assert_eq!(second, Response::Committed { snapshot_id: 2 });

    assert_eq!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .current_snapshot()
            .id
            .get(),
        2
    );

    leader.stop().await;
}

/// The funnel drives its batch through the commit loop with a hot retry, so an
/// ambiguous slot put — the object store errored the create while planting
/// nothing — is re-raced rather than surfaced as a hard failure. Under a
/// single-shot race the same blip would reach the client as a slot-log error;
/// here the forwarded commit still lands.
#[tokio::test]
async fn a_transport_blip_is_re_raced_hot_not_surfaced_as_a_failure() {
    let blip = Arc::new(BlipOnce::new(2));
    let store: Arc<dyn ObjectStore> = Arc::clone(&blip) as Arc<dyn ObjectStore>;
    let catalog = open_catalog(&store, Duration::ZERO).await;
    let leader = Spawned::bind(Arc::clone(&catalog)).await;

    // The first forwarded commit's slot create is create two (the advert was
    // one); the blip fires on it and the funnel re-races the same sequence.
    let committed = commit_session(leader.addr, leader.secret, vec![gc_insert(42)]).await;
    assert!(
        matches!(committed, Response::Committed { .. }),
        "a re-raced blip still commits: {committed:?}"
    );
    assert!(blip.fired(), "the injected blip fired");

    leader.stop().await;

    // The commit is durable: the re-race landed the slot the blip dropped.
    let reopened = Catalog::open(Arc::clone(&store), CatalogOptions::default())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .snapshot()
            .await
            .unwrap()
            .scheduled_deletions()
            .len(),
        1
    );
}

#[tokio::test]
async fn a_clean_shutdown_writes_a_withdrawal_distinct_from_no_advert() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = open_catalog(&store, Duration::ZERO).await;

    // Before any leader, the log names no leader at all.
    assert!(
        current_advert(catalog.slot_store())
            .await
            .unwrap()
            .is_none()
    );

    let leader = Spawned::bind(Arc::clone(&catalog)).await;
    let announce = current_advert(catalog.slot_store()).await.unwrap().unwrap();
    assert!(announce.endpoint.is_some(), "announced with an endpoint");

    leader.stop().await;

    // A clean shutdown left an explicit withdrawal — an advert with no
    // endpoint, distinct from the pre-leader `None`.
    let withdrawal = current_advert(catalog.slot_store()).await.unwrap();
    match withdrawal {
        Some(advert) => assert!(
            advert.endpoint.is_none(),
            "a clean shutdown withdraws: {advert:?}"
        ),
        None => panic!("a withdrawal is an endpoint-absent advert, not the absence of one"),
    }
}

/// The fold writes the freshest advert to `sys/leader`, so the discovery read
/// finds it there once folding has run.
#[tokio::test]
async fn a_withdrawal_folds_into_the_leader_key() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = open_catalog(&store, Duration::ZERO).await;
    let leader = Spawned::bind(Arc::clone(&catalog)).await;
    let instance = leader.instance;
    leader.stop().await;

    // Fold every slot; the freshest advert (the withdrawal) lands in sys/leader.
    catalog.fold_sprint(u64::MAX).await.unwrap();

    let reader = fresh_reader(&catalog).await;
    let folded = read::read_singleton::<crate::store::proto::LeaderValue>(
        ReadHandle::Reader(&reader),
        Key::Sys(SysKey::Leader),
    )
    .await
    .unwrap()
    .expect("the fold recorded the freshest advert");
    assert_eq!(folded.instance.as_slice(), instance.as_slice());
    assert!(
        folded.endpoint.is_none(),
        "the folded advert is the withdrawal"
    );
}

#[tokio::test]
async fn the_secret_is_minted_lazily_for_a_store_that_predates_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = open_catalog(&store, Duration::ZERO).await;

    // Simulate a store predating the token: delete sys/secret directly, the
    // state a migration (which does not add it) leaves.
    delete_secret(&store).await;
    let reader = fresh_reader(&catalog).await;
    assert!(
        read::read_secret(ReadHandle::Reader(&reader))
            .await
            .unwrap()
            .is_none(),
        "the store now has no token"
    );

    // Binding a leader mints it lazily.
    let leader = Spawned::bind(Arc::clone(&catalog)).await;
    let minted = leader.secret;
    leader.stop().await;

    let reader = fresh_reader(&catalog).await;
    let stored = read::read_secret(ReadHandle::Reader(&reader))
        .await
        .unwrap()
        .expect("first bind mints the token");
    assert_eq!(stored.token.as_slice(), minted.as_slice());
    assert_eq!(stored.token.len(), 32);
}

/// Deletes `sys/secret` through the fenced writer, standing in for a store
/// created before the token existed.
async fn delete_secret(store: &Arc<dyn ObjectStore>) {
    let (db, _) = crate::store::open::StoreBuilder::new("", Arc::clone(store))
        .open_writer()
        .await
        .unwrap();
    let tx = db.begin(slatedb::IsolationLevel::Snapshot).await.unwrap();
    tx.delete(Key::Sys(SysKey::Secret).encode()).unwrap();
    crate::transaction::commit::commit_durably(&db, tx)
        .await
        .unwrap();
    db.close().await.unwrap();
}

/// A `sys/*` singleton read straight from a fresh reader — the value codec the
/// leader mint round-trips through.
#[tokio::test]
async fn a_bootstrapped_store_mints_a_readable_secret() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = open_catalog(&store, Duration::ZERO).await;
    let reader = fresh_reader(&catalog).await;
    let secret = read::read_secret(ReadHandle::Reader(&reader))
        .await
        .unwrap()
        .expect("bootstrap mints a token");
    assert_eq!(secret.token.len(), 32);
    // The framed value round-trips.
    let framed = value::encode_value(&secret);
    assert_eq!(
        value::decode_value::<crate::store::proto::SecretValue>(&framed)
            .unwrap()
            .token,
        secret.token
    );
}
