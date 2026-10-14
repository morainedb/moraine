//! What a batch stages against and commits through: the folder's snapshot
//! transaction, or a committer's tailing reader, whose batch is a slot
//! alone. Every write a batch stages passes through here, so the slot the
//! batch becomes carries all of it, index entries included.

use std::sync::{Arc, Mutex, PoisonError};

use slatedb::{DbReader, DbTransaction, WriteHandle};

use crate::{
    error::{Error, Result},
    store::handle::ReadHandle,
    transaction::commit::StagedWrite,
};

/// One batch's session with the store.
pub(crate) struct CommitSession {
    inner: Inner,
    /// Every write staged, in order, for the slot the batch becomes.
    staged: Mutex<Vec<StagedWrite>>,
}

enum Inner {
    /// The folder: a snapshot-isolated transaction that reads the store and
    /// commits the batch into it.
    Folder(DbTransaction),
    /// A committer over a tailing reader: reads the store, commits nothing
    /// into it, and sees its own batch once the reader is served the slot.
    Committer(Arc<DbReader>),
}

impl std::fmt::Debug for CommitSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitSession")
            .field("folder", &self.is_folder())
            .finish_non_exhaustive()
    }
}

impl CommitSession {
    /// The folder's session over `tx`.
    pub(crate) fn folder(tx: DbTransaction) -> Self {
        Self {
            inner: Inner::Folder(tx),
            staged: Mutex::new(Vec::new()),
        }
    }

    /// A committer's session over `reader`.
    pub(crate) fn committer(reader: Arc<DbReader>) -> Self {
        Self {
            inner: Inner::Committer(reader),
            staged: Mutex::new(Vec::new()),
        }
    }

    /// Whether this session commits into the store itself.
    pub(crate) fn is_folder(&self) -> bool {
        matches!(self.inner, Inner::Folder(_))
    }

    /// The session's read side.
    pub(crate) fn handle(&self) -> ReadHandle<'_> {
        match &self.inner {
            Inner::Folder(tx) => ReadHandle::Tx(tx),
            Inner::Committer(reader) => ReadHandle::Reader(reader),
        }
    }

    /// Stages one put.
    ///
    /// # Errors
    ///
    /// The store's error if the folder's transaction refuses the write.
    pub(crate) fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let (key, value) = (key.as_ref(), value.as_ref());
        if let Inner::Folder(tx) = &self.inner {
            tx.put(key, value).map_err(Error::from)?;
        }
        self.record((key.to_vec(), Some(value.to_vec())));

        Ok(())
    }

    /// Stages one delete.
    ///
    /// # Errors
    ///
    /// The store's error if the folder's transaction refuses the write.
    pub(crate) fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        if let Inner::Folder(tx) = &self.inner {
            tx.delete(key).map_err(Error::from)?;
        }
        self.record((key.to_vec(), None));

        Ok(())
    }

    fn record(&self, write: StagedWrite) {
        self.staged
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(write);
    }

    /// Every write staged so far, in order.
    pub(crate) fn staged(&self) -> Vec<StagedWrite> {
        self.staged
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Discards the session.
    pub(crate) fn rollback(self) {
        if let Inner::Folder(tx) = self.inner {
            tx.rollback();
        }
    }

    /// Commits the folder's transaction into the store, returning the handle
    /// its durability is awaited on; a committer's session commits nothing
    /// here, since its batch is the slot.
    ///
    /// # Errors
    ///
    /// The store's error, kinds intact: a transaction conflict is a lost
    /// race to the caller.
    pub(crate) async fn commit(self) -> std::result::Result<Option<WriteHandle>, slatedb::Error> {
        match self.inner {
            Inner::Folder(tx) => tx.commit().await,
            Inner::Committer(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::{Db, DbReader};

    use super::CommitSession;

    /// A committer's session records every write it is handed and commits
    /// none of it into the store it reads.
    #[tokio::test]
    async fn a_committers_session_records_writes_and_commits_nothing() {
        let store = Arc::new(InMemory::new());
        let db = Db::builder("", store.clone()).build().await.unwrap();
        db.close().await.unwrap();
        let reader = Arc::new(DbReader::builder("", store).build().await.unwrap());

        let session = CommitSession::committer(reader.clone());
        assert!(!session.is_folder());
        session.put(b"k", b"v").unwrap();
        session.delete(b"gone").unwrap();
        assert_eq!(
            session.staged(),
            vec![
                (b"k".to_vec(), Some(b"v".to_vec())),
                (b"gone".to_vec(), None)
            ]
        );

        assert!(session.commit().await.unwrap().is_none());
        assert!(reader.get(b"k").await.unwrap().is_none());
        reader.close().await.unwrap();
    }
}
