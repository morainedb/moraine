//! The object store data files live in, named once for the caches, and
//! read through the retries a blipping transport needs.

use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use serde::{Deserialize, Serialize};

use crate::store::{cache, retry};

/// An object store holding data files, named once for the caches that key
/// on it. Build one per store and clone it: a durable store is named by
/// its location, an in-memory one at random, so two `new` calls on one
/// in-memory store name it twice.
#[derive(Clone)]
pub struct DataStore {
    store: Arc<dyn ObjectStore>,
    pub(super) identity: StoreIdentity,
}

impl DataStore {
    /// Names `store` for the caches.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        let identity = StoreIdentity::of(&store);
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

/// Which store a cached file belongs to. A durable store is named by its
/// location, so its entries outlive the process; an in-memory store holds
/// different contents in every instance, so it is named at random and
/// nothing recovered can match it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(super) enum StoreIdentity {
    Durable(u128),
    Ephemeral(u128),
}

/// How an in-memory object store names itself, alone or inside a wrapper.
const IN_MEMORY: &str = "InMemory";

impl StoreIdentity {
    pub(super) fn of(store: &Arc<dyn ObjectStore>) -> Self {
        let name = store.to_string();
        if name.contains(IN_MEMORY) {
            Self::Ephemeral(uuid::Uuid::new_v4().as_u128())
        } else {
            Self::Durable(cache::stable_name(&name).as_u128())
        }
    }
}
