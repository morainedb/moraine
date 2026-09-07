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
