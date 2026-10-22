//! What a batch stages against and commits through: the folder's snapshot
//! transaction, or a committer's tailing reader, whose batch is a slot
//! alone. Every write a batch stages passes through here, so the slot the
//! batch becomes carries all of it, index entries included.

use std::sync::{Arc, Mutex, PoisonError};

use slatedb::{DbReader, DbTransaction, WriteHandle};

use crate::{
    MemoryReservation,
    error::{Error, Result},
    store::handle::ReadHandle,
    transaction::commit::StagedWrite,
};

/// One batch's session with the store.
pub(crate) struct CommitSession {
    inner: Inner,
    /// Every write staged, in order, for the slot the batch becomes.
    staged: Mutex<Staged>,
    memory_limit: u64,
}

#[derive(Default)]
struct Staged {
    writes: Vec<StagedWrite>,
    memory: Option<MemoryReservation>,
    failure: Option<String>,
}

impl Staged {
    fn check(&self) -> Result<()> {
        match &self.failure {
            Some(error) => Err(Error::Constraint(error.clone())),
            None => Ok(()),
        }
    }
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
        Self::new(Inner::Folder(tx))
    }

    /// A committer's session over `reader`.
    pub(crate) fn committer(reader: Arc<DbReader>) -> Self {
        Self::new(Inner::Committer(reader))
    }

    fn new(inner: Inner) -> Self {
        let mut staged = Staged::default();
        let memory_limit = match std::env::var("MORAINE_COMMIT_MEMORY_BYTES") {
            Ok(value) => value.parse::<u64>().ok().filter(|value| *value > 0),
            Err(std::env::VarError::NotPresent) => Some(256 * 1024 * 1024),
            Err(_) => None,
        };
        if memory_limit.is_none() {
            staged.failure =
                Some("MORAINE_COMMIT_MEMORY_BYTES must be a positive byte count".into());
        }
        Self {
            inner,
            staged: Mutex::new(staged),
            memory_limit: memory_limit.unwrap_or(0),
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
        self.record(key.as_ref(), Some(value.as_ref()))
    }

    /// Stages one delete.
    ///
    /// # Errors
    ///
    /// The store's error if the folder's transaction refuses the write.
    pub(crate) fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        self.record(key.as_ref(), None)
    }

    fn record(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let mut staged = self.staged.lock().unwrap_or_else(PoisonError::into_inner);
        staged.check()?;
        let result: Result<()> = (|| {
            let added = (key.len() as u64)
                .saturating_add(value.map_or(0, |value| value.len() as u64))
                .saturating_add(128 + size_of::<StagedWrite>() as u64)
                .saturating_mul(2);
            self.admit(&mut staged, added)?;
            if let Inner::Folder(tx) = &self.inner {
                match value {
                    Some(value) => tx.put(key, value),
                    None => tx.delete(key),
                }
                .map_err(Error::from)?;
            }
            staged
                .writes
                .push((key.to_vec(), value.map(<[u8]>::to_vec)));
            Ok(())
        })();
        if let Err(error) = &result {
            staged.failure = Some(error.to_string());
        }
        result
    }

    fn admit(&self, staged: &mut Staged, added: u64) -> Result<()> {
        staged.check()?;
        let total = staged
            .memory
            .as_ref()
            .map_or(0, MemoryReservation::bytes)
            .saturating_add(added);
        if total > self.memory_limit {
            return Err(Error::Constraint(format!(
                "commit memory exceeds {} bytes; split the transaction or raise MORAINE_COMMIT_MEMORY_BYTES",
                self.memory_limit
            )));
        }
        match &mut staged.memory {
            Some(memory) => memory.resize(total),
            memory @ None => {
                MemoryReservation::reserve(total).map(|reserved| *memory = Some(reserved))
            }
        }
    }

    /// Admits translation scratch space before constructing the store batch.
    pub(crate) fn reserve_translation(&self, bytes: u64) -> Result<()> {
        let mut staged = self.staged.lock().unwrap_or_else(PoisonError::into_inner);
        let result = self.admit(&mut staged, bytes);
        if let Err(error) = &result {
            staged.failure = Some(error.to_string());
        }
        result
    }

    /// Transfers the log batch without copying it, retaining its admission.
    pub(crate) fn take_staged(&mut self) -> Result<(Vec<StagedWrite>, Option<MemoryReservation>)> {
        let staged = self
            .staged
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        staged.check()?;
        Ok((std::mem::take(&mut staged.writes), staged.memory.take()))
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
        if let Err(error) = self
            .staged
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .check()
        {
            return Err(slatedb::Error::invalid(error.to_string()));
        }
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

        let mut session = CommitSession::committer(reader.clone());
        assert!(!session.is_folder());
        session.put(b"k", b"v").unwrap();
        session.delete(b"gone").unwrap();
        assert_eq!(
            session.take_staged().unwrap().0,
            vec![
                (b"k".to_vec(), Some(b"v".to_vec())),
                (b"gone".to_vec(), None)
            ]
        );

        assert!(session.commit().await.unwrap().is_none());
        assert!(reader.get(b"k").await.unwrap().is_none());
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn refused_translation_prevents_committing_an_accepted_prefix() {
        let store = Arc::new(InMemory::new());
        let db = Db::builder("translation-budget", store)
            .build()
            .await
            .unwrap();
        let mut session =
            CommitSession::folder(db.begin(slatedb::IsolationLevel::Snapshot).unwrap());
        session.memory_limit = 1024;
        session.put(b"first", b"small").unwrap();
        assert!(session.put(b"large", vec![0; 4096]).is_err());
        assert!(session.commit().await.is_err());
        assert!(db.get(b"first").await.unwrap().is_none());
        db.close().await.unwrap();
    }
}
