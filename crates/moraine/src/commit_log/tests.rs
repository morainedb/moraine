//! The plug boundary: a provider's reader reaches a read-only open, a
//! read-write open with a provider is refused, and a store stamped for the
//! log is refused without one. The process-wide registry is covered by its
//! own test binary, since it is global.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use object_store::{ObjectStore, memory::InMemory};
use slatedb::wal::{
    WalError, WalFileRange, WalIterator, WalReader, WalRows, WriterInit, WriterInitResult,
    WriterManifest,
};

use crate::{
    Catalog, CatalogOptions, Error,
    commit_log::CommitLogProvider,
    store::{
        key::{Key, SysKey},
        open::StoreBuilder,
        proto, value,
    },
    transaction::commit::FORMAT_WITH_COMMIT_LOG,
};

/// A provider over an empty log that records what the catalog asked of it.
#[derive(Debug, Default)]
struct Recording {
    reader_paths: Mutex<Vec<String>>,
    tail_reads: Arc<Mutex<u64>>,
}

impl Recording {
    fn reader_paths(&self) -> Vec<String> {
        self.reader_paths.lock().unwrap().clone()
    }

    fn tail_reads(&self) -> u64 {
        *self.tail_reads.lock().unwrap()
    }
}

impl CommitLogProvider for Recording {
    fn writer_init(&self, _log_store: Arc<dyn ObjectStore>, _path: &str) -> Box<dyn WriterInit> {
        Box::new(RefusingWriter)
    }

    fn reader(&self, _log_store: Arc<dyn ObjectStore>, path: &str) -> Arc<dyn WalReader> {
        self.reader_paths.lock().unwrap().push(path.to_string());
        Arc::new(EmptyLog {
            tail_reads: Arc::clone(&self.tail_reads),
        })
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

/// A log with no slots: every tail read finds the fold current.
struct EmptyLog {
    tail_reads: Arc<Mutex<u64>>,
}

#[async_trait]
impl WalReader for EmptyLog {
    async fn iterator(&self, _range: WalFileRange) -> Result<Box<dyn WalIterator>, WalError> {
        Ok(Box::new(Drained))
    }

    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        *self.tail_reads.lock().unwrap() += 1;
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

fn options_at(path: &str) -> CatalogOptions {
    CatalogOptions {
        path: path.to_string(),
        ..CatalogOptions::default()
    }
}

/// Bootstraps a catalog at `path` and releases the writer.
async fn bootstrapped(object_store: Arc<InMemory>, path: &str) {
    let writer = Catalog::open(object_store, options_at(path)).await.unwrap();
    writer.close().await.unwrap();
}

/// A read-only open builds its store over the provider's reader, asking
/// for it by the catalog's path, and the store tails the log through it.
#[tokio::test]
async fn a_read_only_open_tails_the_log_through_the_provider() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;

    let provider = Arc::new(Recording::default());
    let reader = Catalog::open_read_only(
        object_store,
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();

    assert_eq!(provider.reader_paths(), vec!["lake".to_string()]);
    assert!(
        provider.tail_reads() >= 1,
        "the store resolves the log's end through the plug"
    );
    assert_eq!(
        reader.snapshot().await.unwrap().current_snapshot().id.get(),
        0
    );
    reader.close().await.unwrap();
}

/// A read-write open with a provider is refused before any store opens:
/// the committer that writes slots is not in this version.
#[tokio::test]
async fn a_read_write_open_with_a_provider_is_refused() {
    let object_store = Arc::new(InMemory::new());
    let provider = Arc::new(Recording::default());

    let refused = Catalog::open(
        object_store,
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await;

    match refused {
        Err(Error::Configuration(message)) => assert!(message.contains("commit log"), "{message}"),
        other => panic!("expected a configuration refusal, got {other:?}"),
    }
    assert!(
        provider.reader_paths().is_empty(),
        "a refused open asks the provider for nothing"
    );
}

/// A store stamped for the commit log is refused by every open that has no
/// provider, naming what it needs rather than reporting a newer format.
#[tokio::test]
async fn a_store_stamped_for_the_log_is_refused_without_a_provider() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;

    let (db, _counters) = StoreBuilder::new("lake", object_store.clone())
        .open_writer()
        .await
        .unwrap();
    db.put(
        &Key::Sys(SysKey::Format).encode(),
        &value::encode_value(&proto::FormatValue {
            format_version: FORMAT_WITH_COMMIT_LOG,
            writer_version: "test".to_string(),
        }),
    )
    .await
    .unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    for refused in [
        Catalog::open(object_store.clone(), options_at("lake"))
            .await
            .err()
            .map(|err| err.to_string()),
        Catalog::open_read_only(object_store, options_at("lake"))
            .await
            .err()
            .map(|err| err.to_string()),
    ] {
        let message = refused.expect("an open of a commit-log store must be refused");
        assert!(message.contains("commit-log library"), "{message}");
    }
}
