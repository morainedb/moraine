//! Checks publication against real SlateDB with delayed manifest writes.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::sync::{Semaphore, watch};

#[derive(Debug)]
pub(super) struct GatedManifestStore {
    inner: Arc<InMemory>,
    path: Path,
    arrived: watch::Sender<bool>,
    release: Semaphore,
    identity: u64,
    armed: AtomicBool,
    skip: AtomicUsize,
}

impl GatedManifestStore {
    pub(super) fn new(inner: Arc<InMemory>, path: Path) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        Self {
            inner,
            path,
            arrived: watch::Sender::new(false),
            release: Semaphore::new(0),
            identity: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            armed: AtomicBool::new(false),
            skip: AtomicUsize::new(0),
        }
    }

    #[allow(clippy::unwrap_used)]
    pub(super) async fn arrival(&self) {
        let mut arrived = self.arrived.subscribe();
        while !*arrived.borrow_and_update() {
            arrived.changed().await.unwrap();
        }
    }

    pub(super) fn open(&self) {
        self.release.close();
    }

    async fn park(&self, location: &Path) {
        if self.armed.load(Ordering::Acquire) && location.as_ref().starts_with(self.path.as_ref()) {
            let skipped = self
                .skip
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |skip| {
                    skip.checked_sub(1)
                });
            if skipped.is_ok() {
                return;
            }
            self.arrived.send_replace(true);
            let _ = self.release.acquire().await;
        }
    }
}

impl std::fmt::Display for GatedManifestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedManifestStore({})", self.identity)
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedManifestStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.park(location).await;
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
    ) -> object_store::Result<Vec<bytes::Bytes>> {
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

use slatedb::config::Settings;

use super::*;

#[tokio::test]
async fn publication_waits_for_the_manifest_after_wal_durability() {
    let objects = Arc::new(GatedManifestStore::new(
        Arc::new(InMemory::new()),
        Path::from("manifest/"),
    ));
    let db = Db::builder("", objects.clone())
        .with_settings(Settings {
            flush_interval: None,
            manifest_poll_interval: Duration::from_hours(1),
            compactor_options: None,
            garbage_collector_options: None,
            ..Settings::default()
        })
        .build()
        .await
        .unwrap();
    let write = db.put(b"key", b"value").await.unwrap();
    db.flush().await.unwrap();
    write.await_durable().await.unwrap();
    let publisher = Arc::new(Publisher::new(
        db.clone(),
        slatedb::admin::AdminBuilder::new("", objects.clone()).build(),
    ));
    objects.armed.store(true, Ordering::Release);
    let publication = tokio::spawn({
        let publisher = publisher.clone();
        async move { publisher.through(write.seqnum()).await }
    });
    tokio::time::timeout(Duration::from_secs(5), objects.arrival())
        .await
        .unwrap();
    assert!(
        !publication.is_finished(),
        "WAL durability must not bypass manifest publication"
    );
    objects.open();
    publication.await.unwrap().unwrap();
    let checkpoints = publisher
        .keeper
        .admin
        .list_checkpoints(Some(PUBLISHED_NAME))
        .await
        .unwrap();
    assert_eq!(checkpoints.len(), 1);
    let reader = DbReader::open(
        "",
        objects,
        slatedb::DbReaderMode::Checkpoint(checkpoints[0].id),
        slatedb::config::DbReaderOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        reader.get(b"key").await.unwrap().as_deref(),
        Some(b"value".as_slice())
    );
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

/// A request publishes behind the caller, and coalesces: several durable
/// writes inside one interval cost one publication, not one each.
#[tokio::test]
async fn a_requested_publication_lands_within_the_interval() {
    let objects = Arc::new(InMemory::new());
    let db = uncollected_db(objects.clone()).await;
    let admin = slatedb::admin::AdminBuilder::new("", objects.clone()).build();
    let publisher = Arc::new(Publisher::with_pacing(
        db.clone(),
        slatedb::admin::AdminBuilder::new("", objects.clone()).build(),
        PUBLICATION_LIFETIME,
        REFRESH_TICK,
        Duration::from_millis(50),
    ));

    let mut sequence = 0;
    for key in [b"a", b"b", b"c"] {
        sequence = durable_write(&db, key, b"value").await;
        publisher.request(sequence);
    }
    assert!(
        published(&admin).await.is_empty(),
        "a request must not publish before the caller returns"
    );

    for _ in 0..100 {
        if !published(&admin).await.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let checkpoints = published(&admin).await;
    assert_eq!(
        checkpoints.len(),
        1,
        "three writes inside one interval are one publication"
    );

    let reader = DbReader::open(
        "",
        objects,
        slatedb::DbReaderMode::Checkpoint(checkpoints[0].id),
        slatedb::config::DbReaderOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        reader.get(b"c").await.unwrap().as_deref(),
        Some(b"value".as_slice()),
        "the publication covers the last sequence requested"
    );
    reader.close().await.unwrap();
    assert!(sequence > 0);
    db.close().await.unwrap();
}

async fn durable_write(db: &Db, key: &[u8], value: &[u8]) -> u64 {
    let write = db.put(key, value).await.unwrap();
    db.flush().await.unwrap();
    write.await_durable().await.unwrap();
    write.seqnum()
}

async fn published(admin: &Admin) -> Vec<slatedb::Checkpoint> {
    let mut checkpoints = admin.list_checkpoints(Some(PUBLISHED_NAME)).await.unwrap();
    checkpoints.sort_by_key(|checkpoint| checkpoint.manifest_id);
    checkpoints
}

/// Every publication carries the lifetime, so one superseded at any moment
/// still has the grace left for readers pinned to it.
#[tokio::test]
async fn a_superseded_publication_keeps_at_least_the_grace() {
    let objects = Arc::new(InMemory::new());
    let db = Db::open("", objects.clone()).await.unwrap();
    let admin = slatedb::admin::AdminBuilder::new("", objects.clone()).build();
    let publisher = Publisher::new(
        db.clone(),
        slatedb::admin::AdminBuilder::new("", objects.clone()).build(),
    );

    let first = durable_write(&db, b"key", b"one").await;
    publisher.through(first).await.unwrap();
    let second = durable_write(&db, b"key", b"two").await;
    publisher.through(second).await.unwrap();

    let checkpoints = published(&admin).await;
    assert_eq!(checkpoints.len(), 2);
    for checkpoint in &checkpoints {
        let left = remaining_until(checkpoint.expire_time.unwrap().timestamp_millis());
        assert!(
            left > READ_GRACE && left <= PUBLICATION_LIFETIME,
            "{left:?}"
        );
    }
    db.close().await.unwrap();
}

/// A writer with neither collector, compactor, nor manifest poll, so no
/// background task touches the manifest during a test. SlateDB keeps expiry
/// at whole seconds, so lifetimes stay several seconds.
async fn uncollected_db(objects: Arc<InMemory>) -> Db {
    Db::builder("", objects)
        .with_settings(Settings {
            garbage_collector_options: None,
            compactor_options: None,
            manifest_poll_interval: Duration::from_hours(1),
            ..Settings::default()
        })
        .build()
        .await
        .unwrap()
}

/// An idle writer refreshes its publication before half its lifetime is used.
#[tokio::test]
async fn an_idle_writer_refreshes_its_publication() {
    let objects = Arc::new(InMemory::new());
    let db = uncollected_db(objects.clone()).await;
    let admin = slatedb::admin::AdminBuilder::new("", objects.clone()).build();
    let publisher = Publisher::with_lifetime(
        db.clone(),
        slatedb::admin::AdminBuilder::new("", objects.clone()).build(),
        Duration::from_secs(4),
        Duration::from_millis(100),
    );

    let first = durable_write(&db, b"key", b"one").await;
    publisher.through(first).await.unwrap();
    let before = published(&admin).await.pop().unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    let after = loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let after = published(&admin).await.pop().unwrap();
        if after.expire_time.unwrap() > before.expire_time.unwrap() {
            break after;
        }
        assert!(
            Instant::now() < deadline,
            "the publication was not refreshed"
        );
    };
    assert_eq!(after.id, before.id);
    drop(publisher);
    db.close().await.unwrap();
}

/// A writer that has learnt it is fenced stops refreshing.
#[tokio::test]
async fn a_fenced_writer_stops_refreshing() {
    let objects = Arc::new(InMemory::new());
    let displaced = uncollected_db(objects.clone()).await;
    let admin = slatedb::admin::AdminBuilder::new("", objects.clone()).build();
    let publisher = Publisher::with_lifetime(
        displaced.clone(),
        slatedb::admin::AdminBuilder::new("", objects.clone()).build(),
        Duration::from_secs(4),
        Duration::from_millis(100),
    );
    let first = durable_write(&displaced, b"key", b"one").await;
    publisher.through(first).await.unwrap();
    let before = published(&admin).await.pop().unwrap();

    let successor = uncollected_db(objects.clone()).await;
    // Whichever of the write and the flush first meets the new epoch fails.
    let stale = async {
        displaced.put(b"key", b"stale").await?;
        displaced.flush().await
    }
    .await;
    assert!(stale.is_err(), "the displaced writer is fenced");
    assert!(displaced.status().close_reason.is_some());

    tokio::time::sleep(Duration::from_secs(3)).await;
    let after = published(&admin).await.pop().unwrap();
    assert_eq!(
        after.expire_time, before.expire_time,
        "no refresh after fencing"
    );
    drop(publisher);
    successor.close().await.unwrap();
}

/// A permanent publication an earlier writer left is given a lifetime by the
/// next writer's first publication.
#[tokio::test]
async fn an_earlier_writers_permanent_publication_is_given_a_lifetime() {
    let objects = Arc::new(InMemory::new());
    let admin = slatedb::admin::AdminBuilder::new("", objects.clone()).build();
    {
        let db = Db::open("", objects.clone()).await.unwrap();
        let publisher = Publisher::new(
            db.clone(),
            slatedb::admin::AdminBuilder::new("", objects.clone()).build(),
        );
        durable_write(&db, b"key", b"one").await;
        publisher.flushed().await.unwrap();
        db.close().await.unwrap();
    }
    let left = published(&admin).await;
    assert_eq!(left.len(), 1);
    assert!(
        left[0].expire_time.is_none(),
        "a close publishes permanently"
    );

    let db = Db::open("", objects.clone()).await.unwrap();
    let publisher = Publisher::new(
        db.clone(),
        slatedb::admin::AdminBuilder::new("", objects.clone()).build(),
    );
    let second = durable_write(&db, b"key", b"two").await;
    publisher.through(second).await.unwrap();

    let checkpoints = published(&admin).await;
    assert_eq!(checkpoints.len(), 2);
    assert!(
        checkpoints[0].expire_time.is_some(),
        "the leftover now expires"
    );
    assert!(checkpoints[1].expire_time.is_some());
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_pinned_reader_outlives_later_publications_within_the_grace() {
    let objects = Arc::new(InMemory::new());
    let db = Db::open("", objects.clone()).await.unwrap();
    let admin = slatedb::admin::AdminBuilder::new("", objects.clone()).build();
    let publisher = Publisher::new(
        db.clone(),
        slatedb::admin::AdminBuilder::new("", objects.clone()).build(),
    );

    let first = durable_write(&db, b"key", b"one").await;
    publisher.through(first).await.unwrap();
    let pinned = published(&admin).await.pop().unwrap();
    let reader = DbReader::open(
        "",
        objects.clone(),
        slatedb::DbReaderMode::Checkpoint(pinned.id),
        slatedb::config::DbReaderOptions::default(),
    )
    .await
    .unwrap();

    for value in [b"two".as_slice(), b"three".as_slice()] {
        let sequence = durable_write(&db, b"key", value).await;
        publisher.through(sequence).await.unwrap();
    }

    assert!(
        published(&admin)
            .await
            .iter()
            .any(|checkpoint| checkpoint.id == pinned.id)
    );
    assert_eq!(
        reader.get(b"key").await.unwrap().as_deref(),
        Some(b"one".as_slice())
    );
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_pinned_reader_refuses_reads_past_its_deadline() {
    let objects = Arc::new(InMemory::new());
    let db = Db::open("", objects.clone()).await.unwrap();
    db.put(b"key", b"value").await.unwrap();
    db.flush().await.unwrap();
    let published = db
        .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
        .await
        .unwrap();
    let reader = DbReader::open(
        "",
        objects,
        slatedb::DbReaderMode::Checkpoint(published.id),
        slatedb::config::DbReaderOptions::default(),
    )
    .await
    .unwrap();
    let listed = Instant::now();
    let scope = CheckpointReader::pinned(reader, published.manifest_id, listed, None);
    let left = scope.remaining().unwrap();
    assert!(left > READ_GRACE / 2 && left < READ_GRACE);

    let mut entries = crate::store::handle::ReadHandle::Checkpoint(&scope)
        .scan_prefix(b"", .., crate::store::handle::ScanShape::bounded().retain())
        .await
        .unwrap();
    assert_eq!(
        scope
            .reader()
            .unwrap()
            .get(b"key")
            .await
            .unwrap()
            .as_deref(),
        Some(b"value".as_slice())
    );

    scope.confirm(listed, Some(Duration::ZERO));
    assert!(scope.reader().is_err());
    assert!(
        entries.next().await.is_err(),
        "buffered scans must refuse an expired publication"
    );
    drop(entries);

    scope.confirm(Instant::now(), None);
    assert!(
        scope.reader().is_ok(),
        "a later listing extends the deadline"
    );
    drop(scope);
    db.close().await.unwrap();
}

#[tokio::test]
async fn closing_a_writer_publishes_the_flushed_manifest() {
    let objects = Arc::new(InMemory::new());
    let catalog = crate::Catalog::open(objects.clone(), crate::CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit_materialized(|tx| {
            tx.set_option(crate::OptionScope::Global, "close-probe", "persisted")
        })
        .await
        .unwrap();
    catalog.close().await.unwrap();
    let admin = slatedb::admin::AdminBuilder::new("", objects.clone()).build();
    let checkpoint = admin
        .list_checkpoints(Some(PUBLISHED_NAME))
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|checkpoint| checkpoint.manifest_id)
        .unwrap();
    assert!(
        checkpoint.expire_time.is_none(),
        "a close publishes permanently"
    );
    let reader = DbReader::builder("", objects)
        .with_segment_extractor(Arc::new(crate::store::segment::TagSegmentExtractor))
        .with_reader_mode(slatedb::DbReaderMode::Checkpoint(checkpoint.id))
        .with_options(slatedb::config::DbReaderOptions {
            skip_wal_replay: true,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let key = crate::store::key::Key::current(crate::store::key::EntityKey::Option {
        scope_kind: 0,
        scope_id: 0,
    });
    assert!(
        reader.get(key.encode()).await.unwrap().is_some(),
        "clean shutdown must publish SSTs instead of retaining the old WAL-heavy checkpoint"
    );
    reader.close().await.unwrap();
}

/// Records every object-store request by kind and path.
#[derive(Debug)]
struct RecordingStore {
    inner: Arc<InMemory>,
    log: std::sync::Mutex<Vec<String>>,
}

impl RecordingStore {
    fn new(inner: Arc<InMemory>) -> Self {
        Self {
            inner,
            log: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn note(&self, entry: String) {
        self.log.lock().unwrap().push(entry);
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.log.lock().unwrap())
    }
}

impl std::fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordingStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RecordingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.note(format!("PUT {location}"));
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.note(format!("PUT(multipart) {location}"));
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = self.inner.get_opts(location, options).await;
        let outcome = match &result {
            Ok(_) => "ok",
            Err(object_store::Error::NotFound { .. }) => "404",
            Err(object_store::Error::NotModified { .. }) => "304",
            Err(_) => "err",
        };
        self.note(format!("GET {location} -> {outcome}"));
        result
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.note(format!("GET(ranges) {location}"));
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.note("DELETE(stream)".to_owned());
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.note(format!(
            "LIST {}",
            prefix.map_or("/".to_owned(), ToString::to_string)
        ));
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.note(format!(
            "LIST(delimiter) {}",
            prefix.map_or("/".to_owned(), ToString::to_string)
        ));
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

/// A steady-state publication is one manifest write and lists nothing.
#[tokio::test]
async fn a_steady_state_publication_is_one_manifest_write() {
    let recording = Arc::new(RecordingStore::new(Arc::new(InMemory::new())));
    let objects: Arc<dyn ObjectStore> = recording.clone();
    let db = Db::builder("", objects.clone())
        .with_settings(Settings {
            garbage_collector_options: None,
            compactor_options: None,
            manifest_poll_interval: Duration::from_hours(1),
            ..Settings::default()
        })
        .build()
        .await
        .unwrap();
    let publisher = Publisher::new(
        db.clone(),
        slatedb::admin::AdminBuilder::new("", objects.clone()).build(),
    );

    for value in [b"one".as_slice(), b"two".as_slice()] {
        let sequence = durable_write(&db, b"key", value).await;
        publisher.through(sequence).await.unwrap();
    }

    let third = durable_write(&db, b"key", b"three").await;
    recording.take();
    publisher.through(third).await.unwrap();
    let trace = recording.take();

    let lists = trace
        .iter()
        .filter(|entry| entry.starts_with("LIST"))
        .count();
    let puts = trace
        .iter()
        .filter(|entry| entry.starts_with("PUT"))
        .count();
    assert_eq!(lists, 0, "{trace:#?}");
    assert_eq!(puts, 1, "{trace:#?}");
    assert!(trace.len() <= 4, "{trace:#?}");
    db.close().await.unwrap();
}
