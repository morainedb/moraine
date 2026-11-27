//! Reader acquisition against real SlateDB on an in-memory object store.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

use crate::{Catalog, CatalogOptions, OptionScope, ffi_support::catalog_read::global_option};

/// Counts the writes an object store receives.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<InMemory>,
    writes: AtomicU64,
}

impl CountingStore {
    fn new(inner: Arc<InMemory>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            writes: AtomicU64::new(0),
        })
    }

    fn writes(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
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
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.writes.fetch_add(1, Ordering::Relaxed);
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
        self.writes.fetch_add(1, Ordering::Relaxed);
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
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.copy_opts(from, to, options).await
    }
}

fn reader_options(poll: Duration) -> CatalogOptions {
    CatalogOptions {
        reader_poll_interval: poll,
        ..CatalogOptions::default()
    }
}

async fn set_probe(catalog: &Catalog, value: &str) {
    catalog
        .commit_materialized(|transaction| {
            transaction.set_option(OptionScope::Global, "probe", value)
        })
        .await
        .unwrap();
}

/// A read-only attach writes only the checkpoint that keeps collection off
/// what it reads: one per reader it opens, and nothing else. Under a zero
/// reuse window every scope opens its own, beside the attach's.
#[tokio::test]
async fn a_read_only_attach_writes_only_its_own_checkpoints() {
    let objects = Arc::new(InMemory::new());
    let catalog = Catalog::open(objects.clone(), CatalogOptions::default())
        .await
        .unwrap();
    set_probe(&catalog, "one").await;

    let counting = CountingStore::new(objects);
    let reader = Catalog::open_read_only(counting.clone(), reader_options(Duration::ZERO))
        .await
        .unwrap();
    let first = reader.index_read_scope().await.unwrap();
    assert_eq!(
        global_option(first.reads(), "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("one")
    );
    set_probe(&catalog, "two").await;
    let second = reader.index_read_scope().await.unwrap();
    assert_ne!(first.identity(), second.identity());
    assert_eq!(
        global_option(second.reads(), "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("two")
    );
    drop((first, second));
    reader.close().await.unwrap();
    catalog.close().await.unwrap();

    assert_eq!(
        counting.writes(),
        6,
        "three readers, each establishing and releasing a checkpoint of its own"
    );
}

/// A read-only attach's first scopes read through the reader the attach
/// opened: acquiring one opens no second reader, so it writes no second
/// checkpoint.
#[tokio::test]
async fn the_first_scope_shares_the_attach_reader() {
    let objects = Arc::new(InMemory::new());
    let catalog = Catalog::open(objects.clone(), CatalogOptions::default())
        .await
        .unwrap();
    set_probe(&catalog, "one").await;

    let counting = CountingStore::new(objects);
    let reader = Catalog::open_read_only(counting.clone(), reader_options(Duration::from_hours(1)))
        .await
        .unwrap();
    let attached = counting.writes();
    assert_eq!(attached, 1, "the attach keeps one checkpoint of its own");

    let scope = reader.index_read_scope().await.unwrap();
    assert_eq!(
        global_option(scope.reads(), "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("one")
    );
    assert_eq!(
        global_option(&reader, "probe").await.unwrap().as_deref(),
        Some("one")
    );
    assert_eq!(
        counting.writes(),
        attached,
        "the scopes opened no reader of their own"
    );
    drop(scope);
    reader.close().await.unwrap();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn a_shared_reader_is_reused_inside_the_interval() {
    let objects = Arc::new(InMemory::new());
    let catalog = Catalog::open(objects.clone(), CatalogOptions::default())
        .await
        .unwrap();
    set_probe(&catalog, "one").await;

    let reader = Catalog::open_read_only(objects, reader_options(Duration::from_hours(1)))
        .await
        .unwrap();
    let first = reader.index_read_scope().await.unwrap();
    set_probe(&catalog, "two").await;
    let second = reader.index_read_scope().await.unwrap();
    assert_eq!(first.identity(), second.identity());
    assert_eq!(
        global_option(second.reads(), "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("one")
    );
    drop((first, second));
    reader.close().await.unwrap();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn readers_see_the_last_commit_of_a_closed_writer() {
    let objects = Arc::new(InMemory::new());
    let catalog = Catalog::open(objects.clone(), CatalogOptions::default())
        .await
        .unwrap();
    set_probe(&catalog, "final").await;
    catalog.close().await.unwrap();

    let reader = Catalog::open_read_only(objects, reader_options(Duration::ZERO))
        .await
        .unwrap();
    let scope = reader.index_read_scope().await.unwrap();
    assert_eq!(
        global_option(scope.reads(), "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("final")
    );
    drop(scope);
    reader.close().await.unwrap();
}

#[tokio::test]
async fn a_committer_opens_a_reader_past_its_own_commit() {
    use super::super::index_read_scope::PinnedRead;
    use crate::store::{checkpoint::CheckpointReader, handle::OwnedReadHandle};

    let objects = Arc::new(InMemory::new());
    Catalog::open(objects.clone(), CatalogOptions::default())
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    // The peer's commits wait out a poll of its reader; the committer under
    // test never commits, so its interval only governs how long its scopes
    // may share one reader.
    let provider = Arc::new(crate::commit_log::test_log::TestProvider::default());
    let peer = Catalog::open(
        objects.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..reader_options(Duration::from_millis(10))
        },
    )
    .await
    .unwrap();
    set_probe(&peer, "one").await;
    let committer = Catalog::open(
        objects,
        CatalogOptions {
            commit_log: Some(Arc::new(provider.sharing())),
            ..reader_options(Duration::from_mins(1))
        },
    )
    .await
    .unwrap();

    // A committer's scope is never an interval behind the store: the one
    // after a commit reads past the one before it.
    let scopes = committer.reader_scopes.as_ref().unwrap();
    let first = scopes.acquire().await.unwrap();
    set_probe(&peer, "two").await;
    let second = scopes.acquire().await.unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    let pinned_to = |reader: &Arc<CheckpointReader>| {
        let mut reads = (*committer).clone();
        reads.pinned = Some(Arc::new(PinnedRead {
            handle: OwnedReadHandle::Checkpoint(reader.clone()),
            revision: reader.revision,
        }));
        reads
    };
    assert_eq!(
        global_option(&pinned_to(&first), "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("one")
    );
    assert_eq!(
        global_option(&pinned_to(&second), "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("two")
    );
    drop((first, second));
    committer.close().await.unwrap();
    peer.close().await.unwrap();
}

/// Refuses every write, as a store reached with read-only credentials does.
#[derive(Debug)]
struct ReadOnlyStore {
    inner: Arc<InMemory>,
}

impl std::fmt::Display for ReadOnlyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadOnlyStore")
    }
}

fn refused(path: &Path) -> object_store::Error {
    object_store::Error::PermissionDenied {
        path: path.to_string(),
        source: "read-only credentials".into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for ReadOnlyStore {
    async fn put_opts(
        &self,
        location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(refused(location))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(refused(location))
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
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(refused(from))
    }
}

/// A writer that commits and then vanishes, as a crash leaves a store.
async fn store_left_by_a_vanished_writer() -> Arc<InMemory> {
    let objects = Arc::new(InMemory::new());
    let writer = Catalog::open(objects.clone(), CatalogOptions::default())
        .await
        .unwrap();
    set_probe(&writer, "survives").await;
    drop(writer);
    objects
}

/// A reader protects what it reads with a checkpoint of its own.
#[tokio::test]
async fn a_reader_keeps_a_checkpoint_of_its_own() {
    let objects = store_left_by_a_vanished_writer().await;
    let reader = Catalog::open_read_only(objects.clone(), reader_options(Duration::from_secs(1)))
        .await
        .unwrap();
    let scope = reader.index_read_scope().await.unwrap();
    assert_eq!(
        global_option(scope.reads(), "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("survives")
    );
    let admin = slatedb::admin::AdminBuilder::new("", objects.clone()).build();
    let own = admin.list_checkpoints(None).await.unwrap();
    assert!(
        !own.is_empty(),
        "the reader protects its view with a checkpoint"
    );
    assert!(
        own.iter()
            .all(|checkpoint| checkpoint.expire_time.is_some())
    );
    drop(scope);
    reader.close().await.unwrap();
}

/// A reader whose credentials cannot write a checkpoint is refused, with
/// the manifest grant and the pinned attach named as the ways in.
#[tokio::test]
async fn a_reader_that_cannot_write_a_checkpoint_is_refused_with_guidance() {
    let objects = store_left_by_a_vanished_writer().await;
    let read_only: Arc<dyn ObjectStore> = Arc::new(ReadOnlyStore {
        inner: objects.clone(),
    });
    let refused = Catalog::open_read_only(read_only, reader_options(Duration::from_secs(1)))
        .await
        .expect_err("a reader that cannot keep a checkpoint is refused");
    let message = refused.to_string();
    assert!(message.contains("manifest/"), "{message}");
    assert!(message.contains("checkpoint"), "{message}");
    let admin = slatedb::admin::AdminBuilder::new("", objects.clone()).build();
    assert!(admin.list_checkpoints(None).await.unwrap().is_empty());
}

/// Providers that do not opt into reuse still open distinct protected views.
#[tokio::test]
async fn log_view_reuse_is_opt_in() {
    let objects = Arc::new(InMemory::new());
    let catalog = Catalog::open(
        objects,
        CatalogOptions {
            commit_log: Some(Arc::new(
                crate::commit_log::test_log::TestProvider::default(),
            )),
            ..CatalogOptions::default()
        },
    )
    .await
    .unwrap();
    let scopes = catalog.reader_scopes.as_ref().unwrap();
    let first = scopes.acquire().await.unwrap();
    let second = scopes.acquire().await.unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    drop((first, second));
    catalog.close().await.unwrap();
}
