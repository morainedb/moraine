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
