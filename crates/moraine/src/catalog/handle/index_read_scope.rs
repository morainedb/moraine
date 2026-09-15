//! One pinned catalog and index read view.

use std::sync::{Arc, Weak};

use super::{ReadOnlyCatalog, Store};
use crate::{
    CatalogSnapshot, Result,
    store::handle::{ReadHandle, ReadSession},
};

pub(super) struct PinnedRead {
    pub(super) transaction: Arc<slatedb::DbTransaction>,
    pub(super) snapshot: Arc<CatalogSnapshot>,
}

/// An opaque read revision, separated from the resources pinning it.
#[derive(Clone)]
pub struct IndexReadIdentity {
    store: Weak<Store>,
    sequence: u64,
}

impl IndexReadIdentity {
    pub(crate) fn revision(&self) -> u64 {
        self.sequence
    }
}

impl PartialEq for IndexReadIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.store.ptr_eq(&other.store) && self.sequence == other.sequence
    }
}

impl Eq for IndexReadIdentity {}

impl std::fmt::Debug for IndexReadIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexReadIdentity")
            .field("sequence", &self.sequence)
            .finish_non_exhaustive()
    }
}

/// Index, file, and inline reads pinned to one committed store revision.
/// Dropping the scope and its read clones releases the pinned transaction.
#[derive(Clone, Debug)]
pub struct IndexReadScope {
    reads: ReadOnlyCatalog,
    identity: IndexReadIdentity,
}

impl IndexReadScope {
    /// The read surface backed by this scope's pinned transaction.
    #[must_use]
    pub fn reads(&self) -> &ReadOnlyCatalog {
        &self.reads
    }

    /// The identity used to validate immutable results without retaining a
    /// transaction.
    #[must_use]
    pub fn identity(&self) -> &IndexReadIdentity {
        &self.identity
    }
}

impl ReadOnlyCatalog {
    /// Pins catalog, index, and inline reads to one committed writer revision.
    /// Returns `None` for manifest-following readers, which cannot yet pin a
    /// read view.
    ///
    /// # Errors
    /// Returns a store error if the read transaction or catalog cannot be read.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example(catalog: &moraine::Catalog) -> moraine::Result<()> {
    /// if let Some(scope) = catalog.index_read_scope().await? {
    ///     let view = scope.reads().snapshot().await?;
    ///     assert!(view.schema_by_name("main").is_some());
    ///     // Further reads through scope.reads() observe this same revision.
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn index_read_scope(&self) -> Result<Option<IndexReadScope>> {
        let session = self.begin_read().await?;
        let transaction = match session {
            ReadSession::Tx(transaction) => Arc::new(transaction),
            ReadSession::Pinned(transaction) => transaction,
            ReadSession::Reader(_) => return Ok(None),
        };
        let identity = IndexReadIdentity {
            store: Arc::downgrade(&self.store),
            sequence: transaction.seqnum(),
        };
        let snapshot = self.load_head_view(ReadHandle::Tx(&transaction)).await?;
        let mut reads = self.clone();
        reads.pinned = Some(Arc::new(PinnedRead {
            transaction,
            snapshot,
        }));
        Ok(Some(IndexReadScope { reads, identity }))
    }
}
