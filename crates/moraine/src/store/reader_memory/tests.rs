use std::sync::Arc;

use object_store::{memory::InMemory, path::Path};
use slatedb::{
    Db, DbReader, DbReaderMode,
    config::{CheckpointOptions, CheckpointScope},
    wal::SlateDbWalReaderBuilder,
};

use super::*;
use crate::{MemoryReservation, store::cache::retention::Budget};

#[tokio::test]
async fn overlapping_wal_readers_share_admission_and_release_it_with_rows() {
    let objects = Arc::new(InMemory::new());
    let db = Db::builder("catalog", objects.clone())
        .build()
        .await
        .unwrap();
    db.put(b"key", vec![b'x'; 32 * 1024]).await.unwrap();
    db.flush().await.unwrap();
    let checkpoint = db
        .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
        .await
        .unwrap();
    let budget = Budget::new(80 * 1024);
    let open = || {
        let wal = SlateDbWalReaderBuilder::new()
            .with_path(Path::from("catalog"))
            .with_object_store(objects.clone())
            .build()
            .unwrap();
        DbReader::builder("catalog", objects.clone())
            .with_reader_mode(DbReaderMode::Checkpoint(checkpoint.id))
            .with_wal_reader(Arc::new(AdmittedReader::new(
                Arc::new(wal),
                MemoryReservation::from_budget(0, budget.clone()).unwrap(),
            )))
            .build()
    };
    let reader = open().await.unwrap();
    let held = reader.get(b"key").await.unwrap().unwrap();
    assert_eq!(held.len(), 32 * 1024);
    assert!(budget.used() >= 64 * 1024);
    assert!(
        open().await.is_err(),
        "a second generation must not exceed the shared budget"
    );
    assert_eq!(reader.get(b"key").await.unwrap().unwrap(), held);
    reader.close().await.unwrap();
    drop(reader);
    assert!(budget.used() > 0, "returned bytes retain their allowance");
    drop(held);
    for _ in 0..100 {
        if budget.used() == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(budget.used(), 0);
    let reopened = open().await.unwrap();
    reopened.close().await.unwrap();
    drop(reopened);
    db.close().await.unwrap();
}

/// A reader a scope holds replays to the log boundary it saw when it opened
/// and never past it, however far the log has moved by its next poll.
#[tokio::test]
async fn a_fixed_reader_keeps_the_boundary_it_opened_on() {
    use std::sync::atomic::{AtomicU64, Ordering};

    use slatedb::wal::{WalError, WalFileRange, WalIterator, WalReader, WalRows};

    struct Advancing(AtomicU64);
    struct Nothing;

    #[async_trait::async_trait]
    impl WalIterator for Nothing {
        async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
            Ok(None)
        }
    }

    #[async_trait::async_trait]
    impl WalReader for Advancing {
        async fn iterator(&self, _: WalFileRange) -> Result<Box<dyn WalIterator>, WalError> {
            Ok(Box::new(Nothing))
        }

        async fn last_wal_file_id(&self, _: u64) -> Result<u64, WalError> {
            Ok(10 + self.0.fetch_add(1, Ordering::AcqRel))
        }
    }

    let following = AdmittedReader::new(
        Arc::new(Advancing(AtomicU64::new(0))),
        MemoryReservation::reserve(0).unwrap(),
    );
    assert_eq!(following.last_wal_file_id(0).await.unwrap(), 10);
    assert_eq!(following.last_wal_file_id(0).await.unwrap(), 11);

    let fixed = AdmittedReader::fixed(
        Arc::new(Advancing(AtomicU64::new(0))),
        MemoryReservation::reserve(0).unwrap(),
    );
    assert_eq!(fixed.last_wal_file_id(0).await.unwrap(), 10);
    assert_eq!(fixed.last_wal_file_id(0).await.unwrap(), 10);
    assert_eq!(fixed.last_wal_file_id(5).await.unwrap(), 10);
}
