//! One pinned catalog and index read view.

use std::sync::{Arc, Weak};

use super::{ReadOnlyCatalog, Store};
use crate::{
    Result,
    store::handle::{OwnedReadHandle, ReadSession},
};

pub(super) struct PinnedRead {
    pub(super) handle: OwnedReadHandle,
    pub(super) revision: u64,
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
/// Dropping the scope and its read clones releases its pinned read resources.
#[derive(Clone, Debug)]
pub struct IndexReadScope {
    reads: ReadOnlyCatalog,
    identity: IndexReadIdentity,
}

impl IndexReadScope {
    /// The read surface backed by this scope's fixed store revision.
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
    /// Pins catalog, index, and inline reads to one committed store revision.
    /// Independent readers share leased checkpoints published by the writer.
    ///
    /// # Errors
    /// Returns a store error if the read transaction or catalog cannot be read.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example(catalog: &moraine::Catalog) -> moraine::Result<()> {
    /// let scope = catalog.index_read_scope().await?;
    /// {
    ///     let view = scope.reads().read_scope().await?;
    ///     assert!(view.schema_by_name("main").await?.is_some());
    ///     // Further reads through scope.reads() observe this same revision.
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn index_read_scope(&self) -> Result<IndexReadScope> {
        self.refuse_if_closed()?;
        let (handle, revision) = if let Some(pinned) = &self.pinned {
            (pinned.handle.clone(), pinned.revision)
        } else if let Some(scopes) = &self.reader_scopes {
            if !scopes.captures_log() {
                self.begin_read().await?.finish();
            }
            let reader = Box::pin(scopes.acquire()).await?;
            let revision = reader.revision;
            (OwnedReadHandle::Checkpoint(reader), revision)
        } else if let Store::Writer { db, .. } = self.store.as_ref() {
            let snapshot = db.snapshot()?;
            let revision = snapshot.seq();
            (OwnedReadHandle::Snapshot(snapshot), revision)
        } else {
            match self.begin_read().await? {
                ReadSession::Tx(transaction) => {
                    let revision = transaction.seqnum();
                    (OwnedReadHandle::Tx(Arc::new(transaction)), revision)
                }
                ReadSession::Pinned(_) | ReadSession::Reader(_) => return Err(crate::Error::Constraint("a fixed read scope is unavailable; reopen the catalog with snapshot support".into())),
            }
        };
        crate::transaction::commit::refuse_mid_migration(handle.borrow()).await?;
        let identity = IndexReadIdentity {
            store: Arc::downgrade(&self.store),
            sequence: revision,
        };
        let mut reads = self.clone();
        reads.pinned = Some(Arc::new(PinnedRead { handle, revision }));
        tracing::debug!(store = ?Arc::as_ptr(&self.store), revision, "catalog read scope acquired");
        Ok(IndexReadScope { reads, identity })
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;
    use crate::{
        Catalog, CatalogOptions,
        store::key::{EntityKey, Key},
    };

    #[tokio::test]
    async fn read_scope_refuses_an_unpinned_read_surface() {
        let objects = Arc::new(InMemory::new());
        let catalog = Catalog::open(objects.clone(), CatalogOptions::default())
            .await
            .unwrap();
        let mut reader = Catalog::open_read_only(objects, CatalogOptions::default())
            .await
            .unwrap();
        reader.reader_scopes = None;
        assert!(reader.index_read_scope().await.is_err());
        reader.close().await.unwrap();
        catalog.close().await.unwrap();
    }

    /// An acknowledged commit is durable and is what a reader in another
    /// process resolves: the reader replays the write-ahead log past the
    /// manifest, so no publication stands between the two.
    #[tokio::test]
    async fn acknowledged_commits_are_what_another_process_resolves() {
        let objects = Arc::new(InMemory::new());
        let catalog = Catalog::open(objects.clone(), CatalogOptions::default())
            .await
            .unwrap();
        catalog
            .commit_materialized(|transaction| {
                transaction.set_option(crate::OptionScope::Global, "probe", "acknowledged")
            })
            .await
            .unwrap();

        let reader = Catalog::open_read_only(objects, CatalogOptions::default())
            .await
            .unwrap();
        assert_eq!(
            crate::ffi_support::catalog_read::global_option(&reader, "probe")
                .await
                .unwrap()
                .as_deref(),
            Some("acknowledged")
        );
        reader.close().await.unwrap();
        catalog.close().await.unwrap();
    }

    /// A scope holds its revision past the interval a reader would otherwise
    /// re-point on, which is the only thing that advances one.
    #[tokio::test]
    async fn a_held_scope_keeps_its_revision_past_a_reader_poll() {
        let objects = Arc::new(InMemory::new());
        let catalog = Catalog::open(objects.clone(), CatalogOptions::default())
            .await
            .unwrap();
        catalog
            .commit_materialized(|transaction| {
                transaction.set_option(crate::OptionScope::Global, "probe", "before")
            })
            .await
            .unwrap();
        let reader = Catalog::open_read_only(
            objects,
            CatalogOptions {
                reader_poll_interval: std::time::Duration::ZERO,
                ..CatalogOptions::default()
            },
        )
        .await
        .unwrap();

        let held = reader.index_read_scope().await.unwrap();
        catalog
            .commit_materialized(|transaction| {
                transaction.set_option(crate::OptionScope::Global, "probe", "after")
            })
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(12)).await;

        assert_eq!(
            crate::ffi_support::catalog_read::global_option(held.reads(), "probe")
                .await
                .unwrap()
                .as_deref(),
            Some("before"),
            "a held scope must not pick up a commit made after it opened"
        );

        drop(held);
        reader.close().await.unwrap();
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn read_only_scopes_keep_unversioned_values_and_rotate() {
        let objects = Arc::new(InMemory::new());
        let catalog = Catalog::open(objects.clone(), CatalogOptions::default())
            .await
            .unwrap();
        catalog
            .commit_materialized(|transaction| {
                transaction.set_option(crate::OptionScope::Global, "probe", "before")
            })
            .await
            .unwrap();
        let reader = Catalog::open_read_only(
            objects,
            CatalogOptions {
                reader_poll_interval: std::time::Duration::ZERO,
                ..CatalogOptions::default()
            },
        )
        .await
        .unwrap();
        let first = reader.index_read_scope().await.unwrap();
        let shared = reader.index_read_scope().await.unwrap();
        assert_eq!(first.identity(), shared.identity());
        catalog
            .commit_materialized(|transaction| {
                transaction.set_option(crate::OptionScope::Global, "probe", "after")
            })
            .await
            .unwrap();
        let next = reader.index_read_scope().await.unwrap();
        assert_ne!(first.identity(), next.identity());
        assert_eq!(
            crate::ffi_support::catalog_read::global_option(first.reads(), "probe")
                .await
                .unwrap()
                .as_deref(),
            Some("before"),
            "a held scope reads its own revision {:?}, not {:?}",
            first.identity(),
            next.identity()
        );
        assert_eq!(
            crate::ffi_support::catalog_read::global_option(next.reads(), "probe")
                .await
                .unwrap()
                .as_deref(),
            Some("after")
        );
        drop((first, shared, next));
        reader.close().await.unwrap();
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn opening_a_read_scope_does_not_decode_catalog_entities() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        let transaction = catalog.begin_write_tx().unwrap();
        transaction
            .put(
                Key::current(EntityKey::File {
                    table_id: 99,
                    data_file_id: 1,
                })
                .encode(),
                [0xff],
            )
            .unwrap();
        crate::transaction::commit::commit_durable(
            transaction,
            "test",
            crate::store::StagedBytes::default(),
            &catalog.store().commit_durability(),
        )
        .await
        .unwrap();
        let scope = catalog.index_read_scope().await.unwrap();
        assert_eq!(
            crate::ffi_support::dump_schemas(scope.reads())
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(scope.reads().materialized().await.is_err());
        drop(scope);
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_scope_keeps_unversioned_metadata_across_commits() {
        use crate::OptionScope;

        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        catalog
            .commit_materialized(|transaction| {
                transaction.set_option(OptionScope::Global, "probe", "before")
            })
            .await
            .unwrap();
        let scope = catalog.index_read_scope().await.unwrap();
        catalog
            .commit_materialized(|transaction| {
                transaction.set_option(OptionScope::Global, "probe", "after")
            })
            .await
            .unwrap();
        let pinned = crate::ffi_support::dump_options(scope.reads())
            .await
            .unwrap();
        let latest = crate::ffi_support::dump_options(&catalog).await.unwrap();
        assert!(
            pinned
                .iter()
                .any(|row| row.key == "probe" && row.value == "before")
        );
        assert!(
            latest
                .iter()
                .any(|row| row.key == "probe" && row.value == "after")
        );
        drop(scope);
        catalog.close().await.unwrap();
    }
    #[tokio::test]
    async fn releasing_a_committer_scope_releases_its_reader_generation() {
        let objects = Arc::new(InMemory::new());
        let initial = Catalog::open(objects.clone(), CatalogOptions::default())
            .await
            .unwrap();
        initial.close().await.unwrap();
        let provider = Arc::new(crate::commit_log::test_log::TestProvider::default());
        let folder = Catalog::open(
            objects.clone(),
            CatalogOptions {
                commit_log: Some(provider.clone()),
                reader_poll_interval: std::time::Duration::from_millis(10),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let committer = Catalog::open(
            objects,
            CatalogOptions {
                commit_log: Some(Arc::new(provider.sharing())),
                reader_poll_interval: std::time::Duration::from_millis(10),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(matches!(committer.store.as_ref(), Store::Committer { .. }));
        let scope = committer.index_read_scope().await.unwrap();
        let OwnedReadHandle::Checkpoint(reader) = &scope.reads().pinned.as_ref().unwrap().handle
        else {
            panic!("committer scope must own a checkpoint");
        };
        let reader = Arc::downgrade(reader);
        drop(scope);
        assert!(reader.upgrade().is_none());
        committer.close().await.unwrap();
        assert!(reader.upgrade().is_none());
        folder.close().await.unwrap();
    }
}
