//! An object store that counts the reads issued through it.
//!
//! Some costs are only visible as traffic. A handle that rebuilds the
//! catalog on every read looks correct from the outside — it returns the
//! right answer every time — and differs only in how many objects it
//! fetched to do it. Counting is how a test tells those apart without
//! timing anything, so the assertion is exact rather than flaky.

use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

/// Wraps an in-memory store, tallying every read it serves.
#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<InMemory>,
    gets: AtomicU64,
    ranged_gets: AtomicU64,
    bytes: AtomicU64,
    touched: Mutex<HashSet<String>>,
}

impl CountingStore {
    pub fn new(inner: Arc<InMemory>) -> Self {
        Self {
            inner,
            gets: AtomicU64::new(0),
            ranged_gets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            touched: Mutex::new(HashSet::new()),
        }
    }

    /// Whole-object and ranged reads served since the last call, and
    /// resets the tally — the object-store traffic a read costs, which is
    /// the term that dominates against real storage.
    pub fn take_reads(&self) -> u64 {
        let gets = self.gets.swap(0, Ordering::SeqCst);
        let ranged = self.ranged_gets.swap(0, Ordering::SeqCst);
        gets + ranged
    }

    /// The distinct object paths read since the last call, sorted, and
    /// resets the set — which files a read actually touched, when a count
    /// alone cannot tell a scoped read from a wider one that happens to
    /// cost the same.
    pub fn take_touched_paths(&self) -> Vec<String> {
        let mut touched = self.touched.lock().unwrap_or_else(PoisonError::into_inner);
        let mut paths: Vec<String> = touched.drain().collect();
        paths.sort();
        paths
    }

    fn record_touch(&self, location: &Path) {
        let mut touched = self.touched.lock().unwrap_or_else(PoisonError::into_inner);
        touched.insert(location.to_string());
    }

    /// Bytes those reads carried since the last call, and resets the
    /// tally. What a read *fetches* is the grain question: a block-grained
    /// cache faults blocks where a part-grained one faults whole parts,
    /// and only the byte count tells them apart.
    #[allow(dead_code)]
    pub fn take_bytes(&self) -> u64 {
        self.bytes.swap(0, Ordering::SeqCst)
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
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.record_touch(location);
        let result = self.inner.get_opts(location, options).await?;
        self.bytes
            .fetch_add(result.range.end - result.range.start, Ordering::SeqCst);
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.ranged_gets
            .fetch_add(ranges.len() as u64, Ordering::SeqCst);
        self.record_touch(location);
        self.bytes.fetch_add(
            ranges.iter().map(|range| range.end - range.start).sum(),
            Ordering::SeqCst,
        );
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
