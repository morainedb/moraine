//! The process-wide commit-log registry: one registration per process, the
//! registered provider serves opens that carry none, and a per-open provider
//! outranks it. Its own binary, because the registry is global.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use moraine::{
    Catalog, CatalogOptions, CommitLogProvider, Error, SlotAdoption, SlotCommit, SlotCommitter,
    SlotOffer, register_commit_log,
};
use object_store::{ObjectStore, memory::InMemory};
use slatedb::wal::{
    WalError, WalFileRange, WalIterator, WalReader, WalRows, WriterInit, WriterInitResult,
    WriterManifest,
};

/// A provider over an empty log that counts the readers asked of it.
#[derive(Debug, Default)]
struct Counting {
    readers: Mutex<u64>,
}

impl Counting {
    fn readers(&self) -> u64 {
        *self.readers.lock().unwrap()
    }
}

impl CommitLogProvider for Counting {
    fn writer_init(&self, _log_store: Arc<dyn ObjectStore>, _path: &str) -> Box<dyn WriterInit> {
        Box::new(RefusingWriter)
    }

    fn reader(
        &self,
        _log_store: Arc<dyn ObjectStore>,
        _path: &str,
        _tail: bool,
    ) -> Arc<dyn WalReader> {
        *self.readers.lock().unwrap() += 1;
        Arc::new(EmptyLog)
    }

    fn committer(&self, _log_store: Arc<dyn ObjectStore>, _path: &str) -> Arc<dyn SlotCommitter> {
        Arc::new(NoCommits)
    }
}

/// A committer over a log nothing commits to.
#[derive(Debug)]
struct NoCommits;

#[async_trait]
impl SlotCommitter for NoCommits {
    async fn offer(&self, _commits: Vec<SlotCommit>) -> moraine::Result<SlotOffer> {
        Err(Error::Configuration(
            "the test log takes no commits".to_string(),
        ))
    }

    async fn adopted(&self) -> moraine::Result<bool> {
        Ok(false)
    }

    async fn adopt(&self, _stamp: SlotCommit) -> moraine::Result<SlotAdoption> {
        Err(Error::Configuration(
            "the test log takes no commits".to_string(),
        ))
    }

    fn mark_folded(&self, _sequence: u64) {}

    async fn await_served(&self, _sequence: u64, _timeout: std::time::Duration) -> bool {
        false
    }
}

struct RefusingWriter;

#[async_trait]
impl WriterInit for RefusingWriter {
    async fn fence_and_init(
        &self,
        _manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError> {
        Err(WalError::Unavailable(Arc::new(std::io::Error::other(
            "the test log has no writer",
        ))))
    }
}

struct EmptyLog;

#[async_trait]
impl WalReader for EmptyLog {
    async fn iterator(&self, _range: WalFileRange) -> Result<Box<dyn WalIterator>, WalError> {
        Ok(Box::new(Drained))
    }

    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        Ok(replay_after_wal_id)
    }
}

struct Drained;

#[async_trait]
impl WalIterator for Drained {
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        Ok(None)
    }
}

#[tokio::test]
async fn one_registration_serves_opens_without_a_provider_and_yields_to_one_with() {
    // Bootstrap before registering: once a provider is registered, a
    // read-write open adopts the log through it.
    let object_store = Arc::new(InMemory::new());
    let writer = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    writer.close().await.unwrap();

    let registered = Arc::new(Counting::default());
    register_commit_log(registered.clone()).unwrap();

    let second = register_commit_log(Arc::new(Counting::default()));
    assert!(
        matches!(second, Err(Error::Configuration(_))),
        "a second registration is refused: {second:?}"
    );

    let refused = Catalog::open(object_store.clone(), CatalogOptions::default()).await;
    assert!(
        refused.is_err(),
        "a read-write open reaches the registered provider, whose writer plug refuses to open"
    );

    // An open carrying no provider builds its reader over the registered one.
    let reader = Catalog::open_read_only(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    assert_eq!(registered.readers(), 1);
    reader.close().await.unwrap();

    // One carrying its own provider never consults the registry.
    let explicit = Arc::new(Counting::default());
    let mut options = CatalogOptions::default();
    options.commit_log = Some(explicit.clone());
    let reader = Catalog::open_read_only(object_store, options)
        .await
        .unwrap();
    assert_eq!(explicit.readers(), 1);
    assert_eq!(registered.readers(), 1);
    reader.close().await.unwrap();
}
