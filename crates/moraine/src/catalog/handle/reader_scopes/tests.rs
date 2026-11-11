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

#[tokio::test]
async fn a_read_only_attach_writes_nothing() {
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

    assert_eq!(counting.writes(), 0, "a read-only attach must not write");
}

#[tokio::test]
async fn a_shared_reader_keeps_its_publication_inside_the_interval() {
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
async fn readers_acquire_the_newest_publication_with_the_writer_closed() {
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
async fn a_listing_that_confirms_the_publication_extends_the_deadline() {
    let objects = Arc::new(InMemory::new());
    let catalog = Catalog::open(objects.clone(), CatalogOptions::default())
        .await
        .unwrap();
    set_probe(&catalog, "one").await;

    let reader = Catalog::open_read_only(objects, reader_options(Duration::from_hours(1)))
        .await
        .unwrap();
    let scopes = reader.reader_scopes.as_ref().unwrap();
    let first = scopes.acquire().await.unwrap();
    first.set_deadline(tokio::time::Instant::now() + Duration::from_secs(1));

    // Little life left forces a listing inside the interval; the publication
    // is still current, so the same reader is confirmed and extended.
    let again = scopes.acquire().await.unwrap();
    assert!(Arc::ptr_eq(&first, &again));
    assert!(again.remaining().unwrap() > Duration::from_mins(5));
    drop((first, again));
    reader.close().await.unwrap();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn a_committer_lists_the_newest_publication_inside_the_interval() {
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
    // may reuse a publication.
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

    // A committer's scope is never a listing interval behind a publication:
    // the one after a commit reads past the one before it.
    let scopes = committer.reader_scopes.as_ref().unwrap();
    let first = scopes.acquire().await.unwrap();
    set_probe(&peer, "two").await;
    let second = scopes.acquire().await.unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    let pinned_to = |reader: &Arc<CheckpointReader>| {
        let mut reads = (*committer).clone();
        reads.pinned = Some(Arc::new(PinnedRead {
            handle: OwnedReadHandle::Checkpoint(reader.clone()),
            revision: reader.manifest,
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
