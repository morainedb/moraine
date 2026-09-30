//! The object store data files live in, named once for the caches, and
//! read through the retries a blipping transport needs.

use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};

use crate::{CacheIdentity, store::retry};

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
        retry::retrying("a data-file read", path, || {
            self.store.get_range(path, range.clone())
        })
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
        retry::retrying("a sidecar read", path, || async {
            self.store
                .get_opts(path, options.clone())
                .await?
                .bytes()
                .await
        })
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
        retry::retrying("a data-file read", path, || {
            self.store.get_ranges(path, ranges)
        })
        .await
    }
}

impl std::fmt::Debug for DataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataStore({})", self.store)
    }
}
