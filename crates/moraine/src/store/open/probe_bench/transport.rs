//! Measured in-memory object storage with optional synthetic GET latency.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::sync::Semaphore;

#[derive(Debug)]
pub(super) struct Transport {
    pub(super) coalescing: AtomicBool,
    inner: InMemory,
    permits: Semaphore,
    limited: AtomicBool,
    delay_ms: AtomicU64,
    gets: AtomicU64,
    bytes: AtomicU64,
    active: AtomicU64,
    peak: AtomicU64,
}

impl Transport {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            coalescing: AtomicBool::new(false),
            inner: InMemory::new(),
            permits: Semaphore::new(32),
            limited: AtomicBool::new(false),
            delay_ms: AtomicU64::new(0),
            gets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            active: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        })
    }

    pub(super) fn configure(&self, delay_ms: u64, limited: bool) {
        self.delay_ms.store(delay_ms, Ordering::Relaxed);
        self.limited.store(limited, Ordering::Relaxed);
    }

    pub(super) fn reset(&self) {
        assert_eq!(self.active.load(Ordering::Relaxed), 0);
        self.gets.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
        self.peak.store(0, Ordering::Relaxed);
    }

    pub(super) async fn settle(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut quiet = 0;
            while quiet < 3 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                quiet = if self.active.load(Ordering::Relaxed) == 0 {
                    quiet + 1
                } else {
                    0
                };
            }
        })
        .await
        .unwrap();
    }

    pub(super) fn counters(&self) -> (u64, u64, u64) {
        (
            self.gets.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
            self.peak.load(Ordering::Relaxed),
        )
    }
}

impl fmt::Display for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "probe benchmark transport")
    }
}

struct Active<'a>(&'a AtomicU64);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl ObjectStore for Transport {
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

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(active, Ordering::Relaxed);
        let _active = Active(&self.active);
        let _permit = if self.limited.load(Ordering::Relaxed) {
            Some(self.permits.acquire().await.unwrap())
        } else {
            None
        };
        let delay = self.delay_ms.load(Ordering::Relaxed);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let result = self.inner.get_opts(location, opts).await?;
        self.bytes
            .fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
        Ok(result)
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
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}
