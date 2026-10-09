//! The object store data files live in, named once for the caches, and
//! read through the retries a blipping transport needs.

use std::{
    future::Future,
    ops::Range,
    sync::{Arc, LazyLock},
};

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};

use crate::{CacheIdentity, store::retry};

/// The widest fan-out any single read path asks for: index upkeep reads
/// this many files at once. Admission must not sit below it.
pub(crate) const WIDEST_PATH_FAN_OUT: usize = 64;

/// The share of the process's descriptor budget data-file reads may hold at
/// once. The rest serves the engine's own connections and the metadata
/// store.
const DESCRIPTOR_SHARE: u64 = 8;

/// Reads admitted when the descriptor budget would allow fewer, so a low
/// `RLIMIT_NOFILE` narrows admission without closing it.
pub(super) const MINIMUM_IN_FLIGHT: usize = 8;

/// Data-file reads one process may have in flight, across every path that
/// issues one. Sized above the core count, floored at
/// [`WIDEST_PATH_FAN_OUT`], and capped by the descriptor share.
pub(crate) fn in_flight_reads() -> usize {
    let cores = std::thread::available_parallelism()
        .ok()
        .map(std::num::NonZero::get);
    admission_for(cores, descriptor_limit())
}

/// `cores` unknown falls back to the floor; `descriptors` unknown leaves
/// the cores term uncapped.
pub(super) fn admission_for(cores: Option<usize>, descriptors: Option<u64>) -> usize {
    let wanted = cores
        .map_or(WIDEST_PATH_FAN_OUT, |cores| cores.saturating_mul(8))
        .clamp(WIDEST_PATH_FAN_OUT, 256);
    let allowed = descriptors
        .map(|descriptors| descriptors / DESCRIPTOR_SHARE)
        .and_then(|share| usize::try_from(share).ok())
        .map_or(wanted, |share| share.max(MINIMUM_IN_FLIGHT));
    wanted.min(allowed)
}

/// The process's soft open-file limit, where the platform reports one.
fn descriptor_limit() -> Option<u64> {
    #[cfg(unix)]
    {
        rustix::process::getrlimit(rustix::process::Resource::Nofile).current
    }
    #[cfg(not(unix))]
    {
        None
    }
}

static ADMISSION: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(in_flight_reads())));

/// Runs one store read under admission. A retry inside `work` is the same
/// logical read and keeps its permit; the wait is named because a caller
/// held by a saturated store is otherwise silent.
async fn admitted<T>(
    work: impl Future<Output = object_store::Result<T>>,
) -> object_store::Result<T> {
    let _permit = crate::telemetry::reporting_phase(
        "data-read-admission",
        Arc::clone(&ADMISSION).acquire_owned(),
    )
    .await
    .map_err(|error| object_store::Error::Generic {
        store: "moraine data store",
        source: Box::new(error),
    })?;
    work.await
}

/// An object store holding data files, named once for the caches that key
/// on it. Build one per store and clone it to share cached reads.
#[derive(Clone)]
pub struct DataStore {
    store: Arc<dyn ObjectStore>,
    pub(super) identity: CacheIdentity,
}

impl DataStore {
    /// Gives `store` an isolated cache identity, regardless of its display
    /// name.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::with_cache_identity(store, CacheIdentity::default())
    }

    /// Shares cached reads with handles using `identity`, including across
    /// restarts when the identity names a stable object namespace.
    #[must_use]
    pub fn with_cache_identity(store: Arc<dyn ObjectStore>, identity: CacheIdentity) -> Self {
        Self { store, identity }
    }

    /// The store itself, as it was handed over. A read taken from it
    /// directly carries none of the retries this type wraps its own
    /// data-file reads in.
    #[must_use]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub(crate) fn cache_identity(&self) -> CacheIdentity {
        self.identity
    }

    /// Reads `range` of the file at `path`, taking the whole read again
    /// when the transport under it fails partway.
    pub(crate) async fn read_range(
        &self,
        path: &Path,
        range: Range<u64>,
    ) -> object_store::Result<Bytes> {
        admitted(retry::retrying("a data-file read", path, || {
            self.store.get_range(path, range.clone())
        }))
        .await
    }

    /// Reads at most `len` bytes from the start of `path`, returning what
    /// is there when the object is shorter. A caller that has not yet read
    /// a header cannot know the object's size, so this asks for more than
    /// it may get rather than taking a size first.
    pub(crate) async fn read_prefix(&self, path: &Path, len: u64) -> object_store::Result<Bytes> {
        let options = object_store::GetOptions {
            range: Some(object_store::GetRange::Bounded(0..len)),
            ..object_store::GetOptions::default()
        };
        admitted(retry::retrying("a sidecar read", path, || async {
            self.store
                .get_opts(path, options.clone())
                .await?
                .bytes()
                .await
        }))
        .await
    }

    /// Replaces whatever is at `path`. A sidecar's contents are a function
    /// of the file it describes, so a racing writer writes the same bytes.
    pub(crate) async fn write(&self, path: &Path, bytes: Bytes) -> object_store::Result<()> {
        retry::retrying("a sidecar write", path, || {
            self.store.put(path, bytes.clone().into())
        })
        .await
        .map(|_| ())
    }

    /// [`Self::read_range`] over several ranges in one request, so the
    /// store can still coalesce adjacent chunks.
    pub(crate) async fn read_ranges(
        &self,
        path: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        admitted(retry::retrying("a data-file read", path, || {
            self.store.get_ranges(path, ranges)
        }))
        .await
    }
}

impl std::fmt::Debug for DataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataStore({})", self.store)
    }
}
