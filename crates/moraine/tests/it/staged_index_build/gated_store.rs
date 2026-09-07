//! Holds reads of one real object until a competing commit finishes.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::sync::{Semaphore, watch};

#[derive(Debug)]
pub(super) struct GatedReadStore {
    inner: Arc<InMemory>,
    path: Path,
    arrived: watch::Sender<bool>,
    release: Semaphore,
    identity: u64,
}

impl GatedReadStore {
    pub(super) fn new(inner: Arc<InMemory>, path: Path) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        Self {
            inner,
            path,
            arrived: watch::Sender::new(false),
            release: Semaphore::new(0),
            identity: NEXT_ID.fetch_add(1, Ordering::Relaxed),
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
        if location == &self.path {
            self.arrived.send_replace(true);
            let _ = self.release.acquire().await;
        }
    }
}

impl std::fmt::Display for GatedReadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedReadStore({})", self.identity)
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedReadStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
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
        self.park(location).await;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.park(location).await;
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
