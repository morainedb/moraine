//! What a batch stages against and commits through: the folder's snapshot
//! transaction, or a committer's tailing reader, whose batch is a slot
//! alone. Sessions that offer log slots record every write; direct writers
//! retain writes only in their store transaction.

use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use slatedb::{DbTransaction, WriteHandle};

use crate::{
    MemoryReservation,
    error::{Error, Result},
    store::handle::ReadHandle,
    transaction::commit::StagedWrite,
};

/// One batch's session with the store.
pub(crate) struct CommitSession {
    inner: Inner,
    /// Memory admission and any writes to be offered to a log slot.
    staged: Mutex<Staged>,
    memory_limit: u64,
    record_writes: bool,
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
    Committer(Arc<crate::store::checkpoint::CheckpointReader>),
}

impl std::fmt::Debug for CommitSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitSession")
            .field("folder", &self.is_folder())
            .finish_non_exhaustive()
    }
}

impl CommitSession {
    /// A direct store writer whose batch will not be offered to a commit log.
    pub(crate) fn direct(tx: DbTransaction) -> Self {
        let mut session = Self::folder(tx);
        session.record_writes = false;
        session
    }

    /// A store transaction that also records writes for a possible log slot.
    pub(crate) fn folder(tx: DbTransaction) -> Self {
        Self::new(Inner::Folder(tx))
    }

    /// A committer's session over `reader`.
    pub(crate) fn committer(reader: Arc<crate::store::checkpoint::CheckpointReader>) -> Self {
        Self::new(Inner::Committer(reader))
    }

    fn new(inner: Inner) -> Self {
        let mut staged = Staged::default();
        let memory_limit = crate::memory::budget_bytes(
            crate::memory::configured_memory().commit,
            "MORAINE_COMMIT_MEMORY_BYTES",
            256 * 1024 * 1024,
        );
        if let Err(error) = &memory_limit {
            staged.failure = Some(error.to_string());
        }
        let memory_limit = memory_limit.ok();
        Self {
            inner,
            staged: Mutex::new(staged),
            memory_limit: memory_limit.unwrap_or(0),
            record_writes: true,
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
            Inner::Committer(reader) => ReadHandle::Checkpoint(reader),
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

    /// Stages a put using owned buffers.
    pub(crate) fn put_bytes(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.with_admission(key.len(), value.len(), |staged| {
            if self.record_writes {
                staged.writes.push((key.to_vec(), Some(value.to_vec())));
            }
            if let Inner::Folder(tx) = &self.inner {
                tx.put_bytes(key, value).map_err(Error::from)?;
            }
            Ok(())
        })
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
        self.with_admission(key.len(), value.map_or(0, <[u8]>::len), |staged| {
            if let Inner::Folder(tx) = &self.inner {
                match value {
                    Some(value) => tx.put(key, value),
                    None => tx.delete(key),
                }
                .map_err(Error::from)?;
            }
            if self.record_writes {
                staged
                    .writes
                    .push((key.to_vec(), value.map(<[u8]>::to_vec)));
            }
            Ok(())
        })
    }

    fn with_admission(
        &self,
        key_bytes: usize,
        value_bytes: usize,
        write: impl FnOnce(&mut Staged) -> Result<()>,
    ) -> Result<()> {
        let mut staged = self.staged.lock().unwrap_or_else(PoisonError::into_inner);
        staged.check()?;
        let added = (key_bytes as u64)
            .saturating_add(value_bytes as u64)
            .saturating_add(128 + size_of::<StagedWrite>() as u64)
            .saturating_mul(2);
        let result = self
            .admit(&mut staged, added)
            .and_then(|()| write(&mut staged));
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
                "commit memory exceeds {} bytes; split the transaction or raise the commit memory budget",
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

        let mut session = CommitSession::committer(
            crate::store::checkpoint::CheckpointReader::external(reader.clone()),
        );
        assert!(!session.is_folder());
        session.put(b"k", b"v").unwrap();
        session.delete(b"gone").unwrap();
        session
            .put_bytes(
                bytes::Bytes::from_static(b"owned"),
                bytes::Bytes::from_static(b"buffer"),
            )
            .unwrap();
        assert_eq!(
            session.take_staged().unwrap().0,
            vec![
                (b"k".to_vec(), Some(b"v".to_vec())),
                (b"gone".to_vec(), None),
                (b"owned".to_vec(), Some(b"buffer".to_vec()))
            ]
        );

        assert!(session.commit().await.unwrap().is_none());
        assert!(reader.get(b"k").await.unwrap().is_none());
        assert!(reader.get(b"owned").await.unwrap().is_none());
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn direct_writer_keeps_writes_only_in_its_transaction() {
        let db = Db::builder("folder-ledger", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        let mut session =
            CommitSession::direct(db.begin(slatedb::IsolationLevel::Snapshot).unwrap());
        session.put(b"kept", b"value").unwrap();
        session.put(b"removed", b"old").unwrap();
        session.delete(b"removed").unwrap();
        assert_eq!(
            session.handle().get(b"kept").await.unwrap().unwrap(),
            &b"value"[..]
        );
        assert!(session.handle().get(b"removed").await.unwrap().is_none());
        let (writes, memory) = session.take_staged().unwrap();
        assert!(
            writes.is_empty(),
            "a direct writer needs no external-log ledger"
        );
        assert!(memory.unwrap().bytes() > 0, "store writes remain admitted");
        session.commit().await.unwrap();
        assert_eq!(db.get(b"kept").await.unwrap().unwrap(), &b"value"[..]);
        assert!(db.get(b"removed").await.unwrap().is_none());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn direct_writer_owned_put_preserves_the_value_buffer() {
        let db = Db::builder("owned-put", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        let session = CommitSession::direct(db.begin(slatedb::IsolationLevel::Snapshot).unwrap());
        let key = bytes::Bytes::from(vec![1; 32]);
        let value = bytes::Bytes::from(vec![2; 4096]);
        session.put_bytes(key.clone(), value.clone()).unwrap();
        let found = session.handle().get(&key).await.unwrap().unwrap();
        assert_eq!(found, value);
        assert_eq!(
            found.as_ptr(),
            value.as_ptr(),
            "staging must retain the owned buffer"
        );
        session.commit().await.unwrap();
        assert_eq!(db.get(&key).await.unwrap().unwrap(), value);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn refused_translation_prevents_committing_an_accepted_prefix() {
        let store = Arc::new(InMemory::new());
        let db = Db::builder("translation-budget", store)
            .build()
            .await
            .unwrap();
        let mut session =
            CommitSession::direct(db.begin(slatedb::IsolationLevel::Snapshot).unwrap());
        session.memory_limit = 1024;
        session.put(b"first", b"small").unwrap();
        assert!(
            session
                .put_bytes(
                    bytes::Bytes::from_static(b"large"),
                    bytes::Bytes::from(vec![0; 4096])
                )
                .is_err()
        );
        assert!(session.put(b"later", b"small").is_err());
        assert!(session.commit().await.is_err());
        assert!(db.get(b"first").await.unwrap().is_none());
        db.close().await.unwrap();
    }
}
