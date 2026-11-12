//! The process-wide commit-log registry: one registration per process, the
//! registered provider serves opens that carry none, and a per-open provider
//! outranks it. Its own binary, because the registry is global.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use moraine::{
    Catalog, CatalogHooks, CatalogOptions, CommitLogProvider, CommitStore, Error, ReaderSession,
    SlotAdoption, SlotCommit, SlotOffer, WriterSession, register_commit_log,
};
use object_store::{ObjectStore, memory::InMemory};
use slatedb::{
    Db, DbReader,
    wal::{
        WalError, WalFileRange, WalGc, WalIterator, WalReader, WalRows, WriterInit,
        WriterInitResult, WriterManifest,
    },
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
    fn writer_session(
        &self,
        _log_store: Arc<dyn ObjectStore>,
        _path: &str,
    ) -> Arc<dyn WriterSession> {
        Arc::new(EmptySession)
    }

    fn reader_session(
        &self,
        _log_store: Arc<dyn ObjectStore>,
        _path: &str,
        _tail: bool,
    ) -> Arc<dyn ReaderSession> {
        *self.readers.lock().unwrap() += 1;
        Arc::new(EmptySession)
    }

    fn holding(
        &self,
        _db: Db,
        _session: Arc<dyn WriterSession>,
        _hooks: CatalogHooks,
    ) -> Arc<dyn CommitStore> {
        Arc::new(NoCommits)
    }

    fn following(
        &self,
        _reader: Arc<DbReader>,
        _session: Arc<dyn ReaderSession>,
        _hooks: CatalogHooks,
    ) -> Arc<dyn CommitStore> {
        Arc::new(NoCommits)
    }

    fn garbage_collector(&self, _log_store: Arc<dyn ObjectStore>, _path: &str) -> Arc<dyn WalGc> {
        Arc::new(NoGc)
    }
}

/// A session over a log nothing has adopted: its writer refuses to open,
/// its reader finds nothing.
struct EmptySession;

#[async_trait]
impl WriterSession for EmptySession {
    fn writer_init(&self) -> Box<dyn WriterInit> {
        Box::new(RefusingWriter)
    }

    async fn adopted(&self) -> moraine::Result<bool> {
        Ok(false)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl ReaderSession for EmptySession {
    async fn open_fixed_reader(
        &self,
        _builder: slatedb::DbReaderBuilder<String>,
        _options: slatedb::config::DbReaderOptions,
        _admin: slatedb::admin::Admin,
    ) -> moraine::Result<DbReader> {
        Err(moraine::Error::Configuration(
            "empty test session has no read view".into(),
        ))
    }

    fn reader(&self) -> Arc<dyn WalReader> {
        Arc::new(EmptyLog)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A collector that deletes nothing.
struct NoGc;

#[async_trait]
impl WalGc for NoGc {
    async fn collect(
        &self,
        _referenced_ranges: Vec<WalFileRange>,
        _min_age: std::time::Duration,
        _dry_run: bool,
    ) -> Result<(), WalError> {
        Ok(())
    }
}

/// A store over a log nothing commits to.
#[derive(Debug)]
struct NoCommits;

#[async_trait]
impl CommitStore for NoCommits {
    async fn adopted(&self) -> moraine::Result<bool> {
        Ok(false)
    }

    async fn adopt(&self, _stamp: SlotCommit) -> moraine::Result<SlotAdoption> {
        Err(Error::Configuration(
            "the test log takes no commits".to_string(),
        ))
    }

    async fn write_at(&self, _sequence: u64, _commit: SlotCommit) -> moraine::Result<SlotOffer> {
        Err(Error::Configuration(
            "the test log takes no commits".to_string(),
        ))
    }

    async fn fold_tail(&self) -> moraine::Result<u64> {
        Ok(0)
    }

    async fn close(&self) -> moraine::Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
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
