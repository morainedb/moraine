//! The object store a catalog's write-ahead log is written to, when it is
//! not the catalog's own.

use std::sync::Arc;

use object_store::ObjectStore;

/// An object store holding a catalog's write-ahead log.
///
/// A commit waits on one PUT of the log, so a store with lower write
/// latency than the catalog's — an S3 Express One Zone directory bucket
/// beside a standard bucket — is paid back on every commit.
///
/// The `name` is recorded when the catalog is created and compared on
/// every later open, so a store opened against the wrong log — or against
/// none — is refused rather than replaying an empty log over live state.
/// Spell it as the caller addresses the store (the extension records its
/// `WAL_PATH` URI).
#[derive(Clone)]
pub struct WalStore {
    name: String,
    store: Arc<dyn ObjectStore>,
}

impl WalStore {
    /// A log store addressed as `name`, which is recorded and compared
    /// verbatim — give it something a later open can repeat exactly.
    #[must_use]
    pub fn new(name: impl Into<String>, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            name: name.into(),
            store,
        }
    }

    /// The name the catalog records for this store.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The store itself, as it was handed over.
    #[must_use]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }
}

impl std::fmt::Debug for WalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WalStore({}, {})", self.name, self.store)
    }
}
