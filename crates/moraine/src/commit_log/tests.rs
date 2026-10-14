//! The plug boundary over the in-tree log: readers tail it, a folder adopts
//! it and commits through it, a lost race folds the winner, and a store
//! stamped for the log is refused without a provider. The process-wide
//! registry is covered by its own test binary, since it is global.

use std::sync::Arc;

use object_store::memory::InMemory;

use crate::{
    Catalog, CatalogOptions,
    commit_log::{CommitLogProvider, SlotCommit, SlotEnvelope, SlotWrite, test_log::TestProvider},
    store::{
        key::{Key, SysKey},
        open::StoreBuilder,
        proto, value,
    },
    transaction::commit::FORMAT_WITH_COMMIT_LOG,
};

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

/// A tailing read-only open builds its store over the provider's reader
/// and is served the slots the log holds past the fold.
#[tokio::test]
async fn a_tailing_read_only_open_is_served_the_logs_slots() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;

    // A folder adopts the log, so slots count from the store's own WAL.
    let provider = Arc::new(TestProvider::default());
    let folder = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    folder.close().await.unwrap();

    provider.log.land(
        2,
        SlotEnvelope {
            commits: vec![SlotCommit {
                transaction_id: [1; 16],
                validated_head: 0,
                changes_made: String::new(),
                writes: vec![SlotWrite {
                    key: b"\xffpeer".to_vec(),
                    value: Some(b"landed".to_vec()),
                }],
            }],
        },
    );

    let reader = Catalog::open_read_only(
        object_store,
        CatalogOptions {
            commit_log: Some(provider.clone()),
            commit_log_tail: true,
            ..options_at("lake")
        },
    )
    .await
    .unwrap();

    assert!(
        provider
            .committer(Arc::new(InMemory::new()), "lake")
            .await_served(2, std::time::Duration::from_secs(5))
            .await,
        "the tailing reader replayed the slot"
    );
    assert_eq!(
        reader.snapshot().await.unwrap().current_snapshot().id.get(),
        0
    );
    reader.close().await.unwrap();
}

/// A read-write open with a provider adopts the log — its format stamp is
/// the first slot — and from then on each commit is a slot, which a
/// tailing reader of the same log serves before the folder has flushed.
#[tokio::test]
async fn a_folder_adopts_the_log_and_commits_through_it() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let folder = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    let stamp = Key::Sys(SysKey::Format).encode();
    let slots = provider.log.slots();
    assert_eq!(slots.len(), 1, "{slots:?}");
    assert!(
        slots[0].1.writes().any(|write| write.key == stamp),
        "the first slot is the format stamp"
    );
    assert_eq!(provider.log.folded_sequence(), 1);

    folder
        .commit(|tx| tx.create_schema("s").map(|_| ()))
        .await
        .unwrap();
    let slots = provider.log.slots();
    assert_eq!(slots.len(), 2, "{slots:?}");
    let head = Key::Sys(SysKey::Head).encode();
    assert!(
        slots[1].1.writes().any(|write| write.key == head),
        "the commit's batch rode the second slot"
    );
    assert_eq!(provider.log.folded_sequence(), 2);

    let reader = Catalog::open_read_only(
        object_store,
        CatalogOptions {
            commit_log: Some(provider.clone()),
            commit_log_tail: true,
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(
        reader
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("s")
            .is_some(),
        "a tailing reader serves the slot before any flush"
    );
    reader.close().await.unwrap();
    folder.close().await.unwrap();
}

/// A commit that loses its slot race folds what beat it into the store and
/// retries, landing after the winner.
#[tokio::test]
async fn a_lost_race_folds_the_winner_before_the_retry_lands() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let folder = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();

    // A peer lands a slot this folder has not seen.
    provider.log.land(
        2,
        SlotEnvelope {
            commits: vec![SlotCommit {
                transaction_id: [7; 16],
                validated_head: 0,
                changes_made: String::new(),
                writes: vec![SlotWrite {
                    key: b"\xffpeer".to_vec(),
                    value: Some(b"landed".to_vec()),
                }],
            }],
        },
    );

    folder
        .commit(|tx| tx.create_schema("s").map(|_| ()))
        .await
        .unwrap();
    let slots = provider.log.slots();
    assert_eq!(
        slots
            .iter()
            .map(|(sequence, _)| *sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "the retry landed after the peer's slot"
    );
    assert_eq!(provider.log.folded_sequence(), 3);
    folder.close().await.unwrap();

    // The peer's write was folded into this store before the retry.
    let (db, _counters) = StoreBuilder::new("lake", object_store)
        .commit_log(Some(provider.clone()))
        .open_writer()
        .await
        .unwrap();
    assert_eq!(
        db.get(b"\xffpeer").await.unwrap().as_deref(),
        Some(&b"landed"[..])
    );
    db.close().await.unwrap();
}

/// An adopted store opens read-only through a provider and is refused
/// without one.
#[tokio::test]
async fn an_adopted_store_needs_a_provider_to_open() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let folder = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    folder.close().await.unwrap();

    let reader = Catalog::open_read_only(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    reader.close().await.unwrap();

    let refused = Catalog::open_read_only(object_store, options_at("lake"))
        .await
        .err()
        .map(|err| err.to_string())
        .unwrap_or_default();
    assert!(refused.contains("commit-log library"), "{refused}");
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
