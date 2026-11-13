//! Checks publication against real SlateDB with delayed manifest writes.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
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

#[tokio::test]
async fn superseded_publications_expire_and_the_newest_does_not() {
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
    let expires = checkpoints[0].expire_time.unwrap();
    let left = remaining_until(expires.timestamp_millis());
    assert!(left > READ_GRACE / 2 && left <= READ_GRACE);
    assert!(checkpoints[1].expire_time.is_none());
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
async fn expired_publications_are_deleted_on_the_next_publication() {
    let objects = Arc::new(InMemory::new());
    let db = Db::open("", objects.clone()).await.unwrap();
    let admin = slatedb::admin::AdminBuilder::new("", objects.clone()).build();
    let publisher = Publisher::with_grace(
        db.clone(),
        slatedb::admin::AdminBuilder::new("", objects.clone()).build(),
        Duration::from_millis(1),
    );

    let first = durable_write(&db, b"key", b"one").await;
    publisher.through(first).await.unwrap();
    let second = durable_write(&db, b"key", b"two").await;
    publisher.through(second).await.unwrap();
    assert_eq!(published(&admin).await.len(), 2);

    tokio::time::sleep(Duration::from_millis(5)).await;
    let third = durable_write(&db, b"key", b"three").await;
    publisher.through(third).await.unwrap();

    let checkpoints = published(&admin).await;
    assert_eq!(checkpoints.len(), 2, "the expired publication is deleted");
    assert!(checkpoints[0].expire_time.is_some());
    assert!(checkpoints[1].expire_time.is_none());
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
