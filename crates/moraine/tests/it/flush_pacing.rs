//! Paced write-ahead-log flushes: one PUT per spacing at most, and a lone
//! commit waiting on nothing but its own.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use moraine::{Catalog, CatalogOptions};
use object_store::{ObjectStore, memory::InMemory};

use crate::counting_store::CountingStore;

#[allow(clippy::unwrap_used)]
async fn open(store: &Arc<CountingStore>, spacing: Duration, flush_on_commit: bool) -> Catalog {
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let mut options = CatalogOptions::default();
    options.flush_interval = spacing;
    options.flush_on_commit = flush_on_commit;
    let catalog = Catalog::open(object_store, options).await.unwrap();
    // The bootstrap flushed; start every tally after it.
    store.take_wal_puts();
    catalog
}

#[allow(clippy::unwrap_used)]
async fn commit_schema(catalog: &Catalog, name: &str) {
    let name = name.to_owned();
    catalog
        .commit(move |tx| tx.create_schema(&name).map(|_| ()))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_lone_commit_waits_only_for_its_own_flush() {
    let store = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let spacing = Duration::from_millis(300);
    let catalog = open(&store, spacing, false).await;
    // The bootstrap commit just flushed; let its spacing elapse so the
    // commit under measurement finds the spacing clear.
    tokio::time::sleep(spacing + Duration::from_millis(100)).await;

    let started = Instant::now();
    commit_schema(&catalog, "alone").await;
    let elapsed = started.elapsed();

    assert_eq!(store.take_wal_puts(), 1);
    assert!(
        elapsed < Duration::from_millis(200),
        "a lone commit waited {elapsed:?} against a {spacing:?} spacing"
    );
    catalog.close().await.unwrap();
}

/// Commits arriving within the floor behind a lone writer's flush share
/// the one flush the floor holds back, and wait the floor, not the
/// spacing.
#[tokio::test]
async fn commits_inside_the_floor_share_one_flush() {
    let store = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let spacing = Duration::from_millis(400);
    let catalog = Arc::new(open(&store, spacing, false).await);
    // A first commit flushes at once and opens the floor.
    commit_schema(&catalog, "first").await;
    store.take_wal_puts();
    let started = Instant::now();
    let mut commits = Vec::new();
    for i in 0..4 {
        let catalog = Arc::clone(&catalog);
        commits.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(3 * i)).await;
            commit_schema(&catalog, &format!("burst{i}")).await;
        }));
    }
    for commit in commits {
        commit.await.unwrap();
    }
    let elapsed = started.elapsed();
    let puts = store.take_wal_puts();
    assert!(
        puts <= 2,
        "the burst rode on the floor's one flush, {puts} flushes"
    );
    assert!(
        elapsed < spacing / 2,
        "the burst waited {elapsed:?} against a {spacing:?} spacing: the floor, not the spacing"
    );
    let catalog = Arc::try_unwrap(catalog).unwrap_or_else(|_| panic!("no other handle"));
    catalog.close().await.unwrap();
}

/// A lone writer in a tight loop flushes once per commit, but never
/// faster than the floor, a tenth of the spacing: the rate cap it keeps.
#[tokio::test]
async fn a_lone_writer_flushes_at_most_once_per_floor() {
    let store = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let spacing = Duration::from_millis(100);
    let floor = spacing / 10;
    let catalog = open(&store, spacing, false).await;
    let started = Instant::now();
    let mut commits = 0;
    while started.elapsed() < Duration::from_millis(650) {
        commit_schema(&catalog, &format!("s{commits}")).await;
        commits += 1;
    }
    let elapsed = started.elapsed();
    let puts = store.take_wal_puts();
    let allowed = elapsed.as_millis() / floor.as_millis() + 2;
    assert!(
        u128::from(puts) <= allowed,
        "{puts} flushes in {elapsed:?} exceed one per {floor:?} (allowed {allowed})"
    );
    assert!(
        u128::from(puts) > elapsed.as_millis() / spacing.as_millis() + 2,
        "{puts} flushes in {elapsed:?}: a lone writer no longer waits the whole spacing"
    );
    assert!(commits >= 2, "the loop committed {commits} times");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn flush_on_commit_flushes_every_commit() {
    let store = Arc::new(CountingStore::new(Arc::new(InMemory::new())));
    let catalog = open(&store, Duration::from_secs(5), true).await;

    let started = Instant::now();
    for i in 0..3 {
        commit_schema(&catalog, &format!("each{i}")).await;
    }
    let elapsed = started.elapsed();

    assert_eq!(store.take_wal_puts(), 3);
    assert!(
        elapsed < Duration::from_secs(1),
        "three flush-on-commit commits took {elapsed:?}"
    );
    catalog.close().await.unwrap();
}
