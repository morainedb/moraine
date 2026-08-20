//! The zero-write reader: a read-only catalog pinned to a checkpoint.
//!
//! A reader that follows the latest state writes a checkpoint of its own
//! into the manifest and refreshes it while it lives, so "read-only" still
//! needs write credentials. A reader given an existing checkpoint id writes
//! nothing at all — and in exchange sees exactly the cut that checkpoint
//! named, forever.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, Error};
use object_store::memory::InMemory;

use crate::crash_recovery::freezing_store::FreezingStore;

/// Options naming `checkpoint`.
fn pinned_to(checkpoint: &str) -> CatalogOptions {
    let mut options = CatalogOptions::default();
    options.checkpoint = Some(checkpoint.to_string());
    options
}

/// A catalog over a store that can be frozen, seeded with `sales` and a
/// checkpoint taken over it, then with `ops` committed after the checkpoint.
#[allow(clippy::unwrap_used)]
async fn seeded_with_checkpoint() -> (Arc<FreezingStore>, String) {
    let store = Arc::new(FreezingStore::thawed(Arc::new(InMemory::new())));
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    let checkpoint = catalog.create_checkpoint(None).await.unwrap();
    catalog
        .commit(|tx| tx.create_schema("ops").map(|_| ()))
        .await
        .unwrap();
    catalog.close().await.unwrap();
    (store, checkpoint)
}

/// The cut is fixed: a reader pinned to a checkpoint serves the state at
/// that checkpoint and never the commits that followed it, while a reader
/// following the latest state sees both.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_pinned_reader_serves_the_checkpoint_and_not_later_commits() {
    let (store, checkpoint) = seeded_with_checkpoint().await;

    let pinned = Catalog::open_read_only(store.clone(), pinned_to(&checkpoint))
        .await
        .unwrap();
    let view = pinned.snapshot().await.unwrap();
    assert!(view.schema_by_name("sales").is_some());
    assert!(
        view.schema_by_name("ops").is_none(),
        "a commit after the checkpoint must not be visible through it"
    );
    pinned.close().await.unwrap();

    let latest = Catalog::open_read_only(store, CatalogOptions::default())
        .await
        .unwrap();
    let view = latest.snapshot().await.unwrap();
    assert!(view.schema_by_name("ops").is_some());
    latest.close().await.unwrap();
}

/// The point of the mode: against a store that refuses every write, a
/// pinned reader opens, reads, and closes without attempting one.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_pinned_reader_opens_against_a_store_that_refuses_writes() {
    let (store, checkpoint) = seeded_with_checkpoint().await;
    store.freeze_after(0);

    let writes_before = store.writes_attempted();
    let pinned = Catalog::open_read_only(store.clone(), pinned_to(&checkpoint))
        .await
        .unwrap();
    assert!(
        pinned
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_some()
    );
    pinned.close().await.unwrap();
    assert_eq!(
        store.writes_attempted(),
        writes_before,
        "a pinned reader must not attempt a single write"
    );
}

/// The contrast that makes the mode worth having: a reader following the
/// latest state writes its own checkpoint into the manifest, so the same
/// open costs writes where a pinned one costs none.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_latest_following_reader_writes_where_a_pinned_one_does_not() {
    let (store, checkpoint) = seeded_with_checkpoint().await;

    let before = store.writes_attempted();
    Catalog::open_read_only(store.clone(), CatalogOptions::default())
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let latest_writes = store.writes_attempted() - before;

    let before = store.writes_attempted();
    Catalog::open_read_only(store.clone(), pinned_to(&checkpoint))
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let pinned_writes = store.writes_attempted() - before;

    assert!(
        latest_writes > 0,
        "a latest-following reader is expected to write its own checkpoint"
    );
    assert_eq!(pinned_writes, 0, "a pinned reader must write nothing");
}

/// A checkpoint pins a fixed cut; a writer's whole job is to move past one.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_writer_refuses_a_checkpoint() {
    let store = Arc::new(InMemory::new());
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let checkpoint = catalog.create_checkpoint(None).await.unwrap();
    catalog.close().await.unwrap();

    let err = Catalog::open(store, pinned_to(&checkpoint))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Configuration(_)), "{err:?}");
}

/// A checkpoint id that is not an id at all is a configuration error, named
/// as such rather than surfacing as a store failure.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_malformed_checkpoint_id_is_refused() {
    let (store, _checkpoint) = seeded_with_checkpoint().await;

    let err = Catalog::open_read_only(store, pinned_to("not-a-checkpoint"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Configuration(_)), "{err:?}");
}

/// A well-formed id that names no checkpoint is refused rather than
/// silently falling back to following the latest state.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn an_unknown_checkpoint_is_refused() {
    let (store, _checkpoint) = seeded_with_checkpoint().await;

    assert!(
        Catalog::open_read_only(store, pinned_to("00000000-0000-4000-8000-000000000000"))
            .await
            .is_err()
    );
}

/// Deleting a checkpoint releases what it pinned: a reader opened against
/// it afterwards is refused.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_deleted_checkpoint_no_longer_opens() {
    let (store, checkpoint) = seeded_with_checkpoint().await;

    Catalog::open_read_only(store.clone(), pinned_to(&checkpoint))
        .await
        .unwrap()
        .close()
        .await
        .unwrap();

    Catalog::delete_checkpoint(store.clone(), CatalogOptions::default(), &checkpoint)
        .await
        .unwrap();

    assert!(
        Catalog::open_read_only(store, pinned_to(&checkpoint))
            .await
            .is_err()
    );
}

/// Creating a checkpoint is a manifest write, so a read-only catalog cannot
/// do it — the reader mode consumes checkpoints, the writer mints them.
/// Minting a checkpoint is a writer's verb, and a read-only handle does not
/// carry it — `reader.create_checkpoint(..)` does not compile, which the
/// crate root pins with a `compile_fail` doctest. What a pinned reader can
/// still do is read the cut it was opened against.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_read_only_catalog_reads_the_cut_it_is_pinned_to() {
    let (store, checkpoint) = seeded_with_checkpoint().await;

    let reader = Catalog::open_read_only(store, pinned_to(&checkpoint))
        .await
        .unwrap();
    assert!(
        reader
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_some()
    );
    reader.close().await.unwrap();
}
