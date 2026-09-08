use slatedb::config::{CloseOptions, Settings};

use super::*;

/// Opens a real store whose WAL is flushed only by an explicit request.
async fn manual_flush_store(object_store: Arc<InMemory>) -> Db {
    Db::builder("", object_store)
        .with_settings(Settings {
            flush_interval: None,
            ..Settings::default()
        })
        .build()
        .await
        .unwrap()
}

/// A commit visible in memory stays pending until its WAL reaches storage.
#[tokio::test]
async fn durable_commit_waits_for_wal_flush() {
    let object_store = Arc::new(InMemory::new());
    let db = manual_flush_store(Arc::clone(&object_store)).await;
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    tx.put(b"key", b"value").unwrap();
    let mut commit = Box::pin(commit_durable(
        tx,
        "test",
        StagedBytes::default(),
        &CommitDurability::OnFlushInterval,
    ));

    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut commit)
            .await
            .is_err(),
        "a commit must not acknowledge an unflushed write"
    );
    assert_eq!(db.get(b"key").await.unwrap().unwrap().as_ref(), b"value");

    db.flush().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), commit)
            .await
            .unwrap()
            .unwrap()
            .is_some()
    );
    let reader = DbReader::builder("", object_store).build().await.unwrap();
    assert_eq!(
        reader.get(b"key").await.unwrap().unwrap().as_ref(),
        b"value"
    );
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

/// Closing without a flush fails a pending durability wait.
#[tokio::test]
async fn durable_commit_reports_close_before_flush() {
    let db = manual_flush_store(Arc::new(InMemory::new())).await;
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    tx.put(b"key", b"value").unwrap();
    let mut commit = Box::pin(commit_durable(
        tx,
        "test",
        StagedBytes::default(),
        &CommitDurability::OnFlushInterval,
    ));

    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut commit)
            .await
            .is_err()
    );
    assert!(db.get(b"key").await.unwrap().is_some());
    db.close_with_options(CloseOptions { flush_type: None })
        .await
        .unwrap();

    let error = tokio::time::timeout(Duration::from_secs(5), commit)
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(error.kind(), slatedb::ErrorKind::Closed(_)));
}

/// An empty commit needs no durability handle or WAL flush.
#[tokio::test]
async fn empty_durable_commit_completes_without_flush() {
    let db = manual_flush_store(Arc::new(InMemory::new())).await;
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        commit_durable(
            tx,
            "test",
            StagedBytes::default(),
            &CommitDurability::OnFlushInterval,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(result.is_none());
    db.close().await.unwrap();
}
