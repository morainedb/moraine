//! The plug boundary over the in-tree log: readers tail it, a folder adopts
//! it and commits through it, a lost race folds the winner, and a store
//! stamped for the log is refused without a provider. The process-wide
//! registry is covered by its own test binary, since it is global.

use std::sync::Arc;

use object_store::memory::InMemory;

use crate::{
    Catalog, CatalogOptions, CommitLogRole,
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

/// A peer's slot at `sequence`: one write under `key`, with the head stamp
/// every slot carries, at the counter `sequence` implies.
fn peer_slot(provider: &TestProvider, sequence: u64, key: &[u8], value: &[u8]) -> SlotEnvelope {
    let format_key = Key::Sys(SysKey::Format).encode();
    let origin = provider.log.slots()[0]
        .1
        .writes()
        .find(|write| write.key == format_key)
        .and_then(|write| write.value.as_deref())
        .and_then(|bytes| value::decode_value::<proto::FormatValue>(bytes).ok())
        .and_then(|format| format.commit_log_batch_origin)
        .expect("the first slot records the batch origin");
    let (head_key, head_value) = crate::transaction::commit::head_stamp(0, origin + sequence - 2);

    SlotEnvelope {
        commits: vec![SlotCommit {
            transaction_id: [u8::try_from(sequence).unwrap(); 16],
            validated_head: 0,
            changes_made: String::new(),
            writes: vec![
                SlotWrite {
                    key: key.to_vec(),
                    value: Some(value.to_vec()),
                },
                SlotWrite {
                    key: head_key,
                    value: head_value,
                },
            ],
        }],
    }
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

    provider
        .log
        .land(2, peer_slot(&provider, 2, b"\xffpeer", b"landed"));

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
    assert_eq!(provider.folded_sequence(), 1);

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
    assert_eq!(provider.folded_sequence(), 2);

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
    provider
        .log
        .land(2, peer_slot(&provider, 2, b"\xffpeer", b"landed"));

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
    assert_eq!(provider.folded_sequence(), 3);
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

/// A second read-write process commits over a tailing reader: it needs no
/// store of its own, lands its batch as a slot, sees it once its reader is
/// served, and the folder folds it before the folder's next commit lands.
#[tokio::test]
async fn a_committer_over_a_reader_commits_through_the_log() {
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
    folder
        .commit(|tx| tx.create_schema("a").map(|_| ()))
        .await
        .unwrap();

    // Another process over the same log: its own cursor, the same slots.
    let committer_provider = Arc::new(provider.sharing());
    let committer = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(committer_provider.clone()),
            commit_log_role: CommitLogRole::Committer,
            commit_log_poll_interval: std::time::Duration::from_millis(10),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    committer
        .commit(|tx| tx.create_schema("b").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(
        provider
            .log
            .slots()
            .iter()
            .map(|(sequence, _)| *sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "the committer's batch is the third slot"
    );
    let seen = committer.snapshot().await.unwrap();
    assert!(
        seen.schema_by_name("a").is_some(),
        "the folder's commit, served"
    );
    assert!(seen.schema_by_name("b").is_some(), "its own commit, served");

    // The folder has not seen slot 3: its next commit loses and folds it.
    // Both commits touch the schema list, so the loss is a conflict, which
    // surfaces after the fold rather than being retried past.
    let conflict = folder.commit(|tx| tx.create_schema("c").map(|_| ())).await;
    assert!(
        matches!(conflict, Err(crate::Error::CommitConflict(_))),
        "{conflict:?}"
    );
    assert_eq!(provider.folded_sequence(), 3);
    let after_fold = folder.snapshot().await.unwrap();
    assert!(
        after_fold.schema_by_name("b").is_some(),
        "the committer's slot was folded"
    );

    // Re-driven against the folded state, it lands after the peer's slot.
    folder
        .commit(|tx| tx.create_schema("c").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(provider.log.slots().len(), 4);
    assert_eq!(provider.folded_sequence(), 4);

    // And the committer is served the folder's slot in turn.
    assert!(
        committer_provider
            .committer(Arc::new(InMemory::new()), "lake")
            .await_served(4, std::time::Duration::from_secs(5))
            .await
    );
    assert!(
        committer
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("c")
            .is_some()
    );

    committer.close().await.unwrap();
    folder.close().await.unwrap();
}

/// A committer whose plug reports a slot served before its reader has
/// applied it races the slot its stale head implies, and loses, rather
/// than the one past what it was told it had seen, which it would win.
#[tokio::test]
async fn a_committer_told_of_a_slot_it_has_not_applied_still_loses_to_it() {
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
    folder
        .commit(|tx| tx.create_schema("a").map(|_| ()))
        .await
        .unwrap();
    folder.close().await.unwrap();

    // The committer's reader never polls again, so it never applies what
    // lands after its open.
    let committer_provider = Arc::new(provider.sharing());
    let committer = Catalog::open(
        object_store,
        CatalogOptions {
            commit_log: Some(committer_provider.clone()),
            commit_log_role: CommitLogRole::Committer,
            commit_log_poll_interval: std::time::Duration::from_secs(200),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();

    // A peer lands slot 3 and the plug reports it served, applied or not.
    provider
        .log
        .land(3, peer_slot(&provider, 3, b"\xffpeer", b"landed"));
    committer_provider.report_served(3);

    let outcome = committer
        .commit(|tx| tx.create_schema("b").map(|_| ()))
        .await;
    assert!(
        outcome.is_err(),
        "a stale committer must not win: {outcome:?}"
    );
    assert_eq!(
        provider
            .log
            .slots()
            .iter()
            .map(|(sequence, _)| *sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "nothing landed past the slot the committer had not applied"
    );
    committer.close().await.unwrap();
}

/// A committer needs an adopted log: before any folder has adopted it,
/// the open is refused rather than left with nothing to commit against.
#[tokio::test]
async fn a_committer_is_refused_before_adoption() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;

    let refused = Catalog::open(
        object_store,
        CatalogOptions {
            commit_log: Some(Arc::new(TestProvider::default())),
            commit_log_role: CommitLogRole::Committer,
            ..options_at("lake")
        },
    )
    .await
    .err()
    .map(|err| err.to_string())
    .unwrap_or_default();
    assert!(refused.contains("folder"), "{refused}");
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
            commit_log_batch_origin: None,
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
