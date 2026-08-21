//! An object store that parks a chosen operation until the test lets it
//! through.
//!
//! Cancellation coverage has to place an interrupt with the commit's
//! durable write already in the air, and no amount of sleeping promises
//! that. This store makes the point observable instead: the slot write
//! parks, announces that it arrived, and stays parked until the test
//! opens the gate, so the interrupt lands at a known moment rather than a
//! hoped-for one. A parked write also holds the leader — one batch drives
//! the slot at a time — which is how the *pre*-write window is staged.

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

/// Wraps an in-memory store, holding gated operations at the gate.
#[derive(Debug)]
pub struct GatedStore {
    inner: Arc<InMemory>,
    gate_slot_writes: AtomicBool,
    /// How many operations have parked, ever. A test waits on this to
    /// learn that the operation it gated has reached the gate.
    arrivals: watch::Sender<u64>,
    /// Closed to open the gate: a closed semaphore fails every acquire at
    /// once, which releases everyone parked and lets everyone after them
    /// straight through.
    release: Semaphore,
    /// Slot objects written, counted on completion — so a put still parked
    /// at the gate has not been counted.
    slot_writes: AtomicU64,
}

impl GatedStore {
    /// A store with the gate open: every operation passes.
    pub fn open(inner: Arc<InMemory>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            gate_slot_writes: AtomicBool::new(false),
            arrivals: watch::Sender::new(0),
            release: Semaphore::new(0),
            slot_writes: AtomicU64::new(0),
        })
    }

    /// Parks every slot object write from here on — the leader's conditional
    /// put into the commit log, so this is the commit's point of no return.
    pub fn gate_slot_writes(&self) {
        self.gate_slot_writes.store(true, Ordering::SeqCst);
    }

    /// Lets everyone through, now and afterwards. Not reversible: a test
    /// opens the gate once, at the end of the window it was staging.
    pub fn open_gate(&self) {
        self.gate_slot_writes.store(false, Ordering::SeqCst);
        self.release.close();
    }

    /// Slot objects written to completion.
    pub fn slot_writes(&self) -> u64 {
        self.slot_writes.load(Ordering::SeqCst)
    }

    /// Resolves once at least one operation has parked at the gate.
    pub async fn arrival(&self) {
        let mut arrivals = self.arrivals.subscribe();
        while *arrivals.borrow_and_update() == 0 {
            if arrivals.changed().await.is_err() {
                return;
            }
        }
    }

    /// Announces this operation's arrival and waits for the gate to open.
    /// An `Err` is the gate having been opened, which is the signal to go.
    async fn park(&self) {
        self.arrivals.send_modify(|count| *count += 1);
        let _ = self.release.acquire().await;
    }

    /// Slot objects live under a `commits/` directory of the store root;
    /// every durable commit lands as one of them, written once with a
    /// create-if-absent conditional put.
    fn is_slot(location: &Path) -> bool {
        location.parts().any(|part| part.as_ref() == "commits")
    }
}

impl std::fmt::Display for GatedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let slot = Self::is_slot(location);
        if slot && self.gate_slot_writes.load(Ordering::SeqCst) {
            self.park().await;
        }
        let result = self.inner.put_opts(location, payload, opts).await;
        if slot && result.is_ok() {
            self.slot_writes.fetch_add(1, Ordering::SeqCst);
        }
        result
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
