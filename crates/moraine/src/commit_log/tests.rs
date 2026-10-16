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

/// A peer's slot at `sequence`: one write under `key`, with the head stamp
/// every slot carries, at the counter `sequence` implies.
fn peer_slot(provider: &TestProvider, sequence: u64, key: &[u8], value: &[u8]) -> SlotEnvelope {
    // The adoption stamp records the origin; once truncation has taken it,
    // any slot's head stamp implies it, since slot k stamps origin + k - 1.
    let format_key = Key::Sys(SysKey::Format).encode();
    let head_key = Key::Sys(SysKey::Head).encode();
    let slots = provider.log.slots();
    let origin = slots
        .iter()
        .flat_map(|(_, envelope)| envelope.writes())
        .find(|write| write.key == format_key)
        .and_then(|write| write.value.as_deref())
        .and_then(|bytes| value::decode_value::<proto::FormatValue>(bytes).ok())
        .and_then(|format| format.commit_log_batch_origin)
        .or_else(|| {
            slots.iter().find_map(|(at, envelope)| {
                envelope
                    .writes()
                    .find(|write| write.key == head_key)
                    .and_then(|write| write.value.as_deref())
                    .and_then(|bytes| value::decode_value::<proto::HeadValue>(bytes).ok())
                    .map(|head| head.batch_seq + 1 - at)
            })
        })
        .expect("some slot records or implies the batch origin");
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
    assert!(folder.holds_store(), "the adopter holds the store");
    assert_eq!(
        folder
            .store()
            .commit_log()
            .and_then(crate::commit_log::LogCommitter::db)
            .unwrap()
            .get(b"\xffpeer")
            .await
            .unwrap()
            .as_deref(),
        Some(&b"landed"[..]),
        "the peer's write is in the held store before close"
    );
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
            commit_log_poll_interval: std::time::Duration::from_millis(10),
            reader_poll_interval: std::time::Duration::from_millis(10),
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

    // And the committer is served the folder's slot in turn: a read asks,
    // and the listing it causes serves the slot.
    committer.snapshot().await.unwrap();
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

/// A committer told a slot is served that its reader has not applied
/// refreshes before it offers, so it lands past that slot rather than
/// racing the sequence the slot already holds.
#[tokio::test]
async fn a_committer_told_of_an_unapplied_slot_applies_it_before_offering() {
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

    // The committer never takes the store, so only its own reading can
    // save it from offering against a state it has not caught up to.
    let committer_provider = Arc::new(provider.sharing());
    let committer = Catalog::open(
        object_store,
        CatalogOptions {
            commit_log: Some(committer_provider.clone()),
            commit_log_folding: Some(crate::Folding::Never),
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
        outcome.is_ok(),
        "the refresh before the offer applies the slot: {outcome:?}"
    );
    assert_eq!(
        provider.log.slots().last().map(|(sequence, _)| *sequence),
        Some(4),
        "and the commit lands past the slot it was told of"
    );
    committer.close().await.unwrap();
}

/// After the configured number of folded slots the folder writes its
/// memtable out and truncates the log: a reader following the folded store
/// alone sees the commits, and the slots below the replay point are gone.
#[tokio::test]
async fn folder_flushes_and_truncates_after_enough_folded_slots() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let folder = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            commit_log_flush_slots: 2,
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    // Two folds of this handle's own: the adoption was folded by the open.
    for name in ["a", "b"] {
        folder
            .commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }
    // This process's own tailing reader pins the log through its checkpoint
    // until its next poll moves the checkpoint past the flush, so the
    // collector truncates at the flush after that one.
    let polls = provider.polls();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while provider.polls() < polls + 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    for name in ["c", "d"] {
        folder
            .commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }

    let folded_only = Catalog::open_read_only(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            commit_log_tail: false,
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(
        folded_only
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("d")
            .is_some(),
        "the flush carried the commits into the manifest"
    );
    folded_only.close().await.unwrap();
    // The collector keeps the slot at the replay point itself and deletes
    // everything below it.
    let remaining: Vec<u64> = provider
        .log
        .slots()
        .iter()
        .map(|(sequence, _)| *sequence)
        .collect();
    assert!(
        remaining.iter().all(|sequence| *sequence >= 4) && remaining.last() == Some(&5),
        "slots below the previous flush point were truncated: {remaining:?}"
    );
    folder.close().await.unwrap();
}

/// An idle folder folds what peers land without losing a race to them.
#[tokio::test]
async fn an_idle_folder_polls_and_folds_peers_slots() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let folder = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            commit_log_fold_poll: std::time::Duration::from_millis(10),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    provider
        .log
        .land(2, peer_slot(&provider, 2, b"\xffpeer", b"landed"));

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while provider.folded_sequence() < 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        provider.folded_sequence(),
        2,
        "the poll folded the peer's slot"
    );
    assert_eq!(
        folder
            .store()
            .commit_log()
            .and_then(crate::commit_log::LogCommitter::db)
            .unwrap()
            .get(b"\xffpeer")
            .await
            .unwrap()
            .as_deref(),
        Some(&b"landed"[..])
    );

    // And the folder's next commit lands after it without a loss.
    folder
        .commit(|tx| tx.create_schema("c").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(provider.log.slots().len(), 3);
    folder.close().await.unwrap();
}

/// A committer closing on a tail past the sprint threshold takes the store,
/// folds it, and leaves the folded state current for the next arrival.
#[tokio::test]
async fn a_committer_sprints_at_close_when_the_tail_is_long() {
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

    let committer_provider = Arc::new(provider.sharing());
    let committer = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(committer_provider.clone()),
            commit_log_poll_interval: std::time::Duration::from_millis(10),
            commit_log_folding: Some(crate::Folding::Holding { slots: 1 }),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(
        !committer.holds_store(),
        "a short tail at attach is not taken"
    );
    // Peers land while the committer is attached; nothing it does checks.
    for sequence in [3, 4] {
        provider.log.land(
            sequence,
            peer_slot(&provider, sequence, b"\xffpeer", b"landed"),
        );
    }
    assert!(
        committer_provider.folded_sequence() < 4,
        "a committer folds nothing itself"
    );
    committer.close().await.unwrap();
    assert_eq!(
        committer_provider.folded_sequence(),
        4,
        "the sprint at close folded the tail"
    );

    let folded_only = Catalog::open_read_only(
        object_store,
        CatalogOptions {
            commit_log: Some(provider),
            commit_log_tail: false,
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    let snapshot = folded_only.snapshot().await.unwrap();
    assert!(snapshot.schema_by_name("a").is_some());
    folded_only.close().await.unwrap();
}

/// Rows a handle writes for itself, the sweeps and the status record,
/// land as head-preserving slots: one slot each, carrying the rows and
/// exactly one head stamp at the standing snapshot.
#[tokio::test]
async fn a_handles_own_rows_land_as_head_preserving_slots() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let folder = Catalog::open(
        object_store,
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
    let before = provider.log.slots().len();
    let head_before = folder.snapshot().await.unwrap().current_snapshot().id;

    folder
        .commit_own_rows(vec![(b"\xffown".to_vec(), Some(b"row".to_vec()))])
        .await
        .unwrap();

    let slots = provider.log.slots();
    assert_eq!(slots.len(), before + 1, "one slot per batch of own rows");
    let head_key = Key::Sys(SysKey::Head).encode();
    let (_, envelope) = &slots[before];
    let stamps: Vec<proto::HeadValue> = envelope
        .writes()
        .filter(|write| write.key == head_key)
        .map(|write| value::decode_value(write.value.as_ref().unwrap()).unwrap())
        .collect();
    assert_eq!(stamps.len(), 1);
    assert_eq!(stamps[0].snapshot_id, head_before.get(), "head-preserving");
    assert!(
        envelope.writes().any(|write| write.key == b"\xffown"),
        "the slot carries the rows"
    );
    assert_eq!(
        folder.snapshot().await.unwrap().current_snapshot().id,
        head_before
    );
    folder.close().await.unwrap();
}

/// A committer arriving at an unadopted log adopts it itself, taking the
/// store, and a committer arriving at a long tail takes the store at once;
/// either way it folds its own wins and peers' slots from then on.
#[tokio::test]
async fn a_committer_takes_the_store_when_the_tail_is_long_or_the_log_unadopted() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    // Unadopted: the committer adopts and holds.
    let first = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            commit_log_poll_interval: std::time::Duration::from_millis(10),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(first.holds_store(), "adopting takes the store");
    assert_eq!(provider.log.slots().len(), 1);
    first
        .commit(|tx| tx.create_schema("a").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(provider.folded_sequence(), 2, "a holder folds its own win");
    first.close().await.unwrap();

    // A long tail at attach: two peers landed past the flushed point.
    for sequence in [3, 4] {
        provider.log.land(
            sequence,
            peer_slot(&provider, sequence, b"\xffpeer", b"landed"),
        );
    }
    let late = Arc::new(provider.sharing());
    let second = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(late.clone()),
            commit_log_poll_interval: std::time::Duration::from_millis(10),
            commit_log_folding: Some(crate::Folding::Holding { slots: 1 }),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(second.holds_store(), "a long tail is taken at attach");
    assert_eq!(
        late.folded_sequence(),
        4,
        "taking the store replays the tail"
    );
    second.close().await.unwrap();

    // Neither process left anything unflushed.
    let folded_only = Catalog::open_read_only(
        object_store,
        CatalogOptions {
            commit_log: Some(provider),
            commit_log_tail: false,
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(
        folded_only
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("a")
            .is_some()
    );
    folded_only.close().await.unwrap();
}

/// A holder that first meets its fence at close closes cleanly: every
/// slot it folded is durable in the log, so there is nothing to report.
#[tokio::test]
async fn a_holder_fenced_unseen_closes_quietly() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let holder = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(holder.holds_store());
    holder
        .commit(|tx| tx.create_schema("h").map(|_| ()))
        .await
        .unwrap();

    // Another process takes the store; the holder has not flushed since.
    let usurper = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(Arc::new(provider.sharing())),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(usurper.take_store().await.unwrap());

    holder.close().await.unwrap();
    usurper.close().await.unwrap();

    let reader = Catalog::open_read_only(
        object_store,
        CatalogOptions {
            commit_log: Some(Arc::new(provider.sharing())),
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
            .schema_by_name("h")
            .is_some()
    );
    reader.close().await.unwrap();
}

/// A holder fenced by another process gives the store up and keeps
/// committing through its reader.
#[tokio::test]
async fn a_fenced_holder_drops_back_to_committing() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let holder = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            commit_log_poll_interval: std::time::Duration::from_millis(10),
            commit_log_fold_poll: std::time::Duration::from_millis(10),
            // A holder learns it was fenced at its next flush; flush every
            // fold so the fence surfaces at once.
            commit_log_flush_slots: 1,
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(holder.holds_store());

    // Another process takes the store outright, then commits past it.
    let usurper_provider = Arc::new(provider.sharing());
    let usurper = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(usurper_provider.clone()),
            commit_log_poll_interval: std::time::Duration::from_millis(10),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(usurper.take_store().await.unwrap());
    usurper
        .commit(|tx| tx.create_schema("u").map(|_| ()))
        .await
        .unwrap();

    // The holder meets the fence at its next fold, whether its poll folds
    // the usurper's slot first or its own commit loses to it; either way it
    // demotes. A loss surfaces as a conflict, since both commits touch the
    // schema list, and the re-driven commit lands through the log.
    match holder.commit(|tx| tx.create_schema("h").map(|_| ())).await {
        Ok(_) => {}
        Err(crate::Error::CommitConflict(_)) => {
            holder
                .commit(|tx| tx.create_schema("h").map(|_| ()))
                .await
                .unwrap();
        }
        Err(other) => panic!("{other:?}"),
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while holder.holds_store() && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(!holder.holds_store(), "fenced: back to committing");
    let seen = holder.snapshot().await.unwrap();
    assert!(seen.schema_by_name("u").is_some() && seen.schema_by_name("h").is_some());

    holder.close().await.unwrap();
    usurper.close().await.unwrap();
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

/// A read-only tailing reader follows its own clock, listing for a peer's
/// slot with nothing having asked: the gate is a committer's alone.
#[tokio::test]
async fn a_tailing_reader_follows_its_clock_unasked() {
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

    let reader_provider = Arc::new(provider.sharing());
    let reader = Catalog::open_read_only(
        object_store,
        CatalogOptions {
            commit_log: Some(reader_provider.clone()),
            commit_log_tail: true,
            reader_poll_interval: std::time::Duration::from_millis(10),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();

    provider
        .log
        .land(2, peer_slot(&provider, 2, b"\xffpeer", b"landed"));
    assert!(
        reader_provider
            .committer(Arc::new(InMemory::new()), "lake")
            .await_served(2, std::time::Duration::from_secs(5))
            .await,
        "the clock served the slot, unasked"
    );
    reader.close().await.unwrap();
}

/// A committer idle longer than its reader interval lists before it reads,
/// so a read is served state no staler than that interval however long the
/// catalog went unused.
#[tokio::test]
async fn a_committer_idle_past_its_interval_lists_before_it_reads() {
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

    let options = |provider: Arc<TestProvider>| CatalogOptions {
        commit_log: Some(provider),
        // The poll is slow enough that a read served before the listing it
        // asked for would miss the peer's commit.
        commit_log_poll_interval: std::time::Duration::from_millis(100),
        reader_poll_interval: std::time::Duration::from_millis(50),
        ..options_at("lake")
    };
    let idle_provider = Arc::new(provider.sharing());
    let idle = Catalog::open(object_store.clone(), options(idle_provider.clone()))
        .await
        .unwrap();
    idle.snapshot().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let listings = idle_provider.polls();

    let peer = Catalog::open(object_store, options(Arc::new(provider.sharing())))
        .await
        .unwrap();
    peer.commit(|tx| tx.create_schema("landed").map(|_| ()))
        .await
        .unwrap();

    assert!(
        idle.snapshot()
            .await
            .unwrap()
            .schema_by_name("landed")
            .is_some(),
        "the read waited for the listing it asked for"
    );
    assert!(
        idle_provider.polls() > listings,
        "which it would not have found without listing"
    );
    peer.close().await.unwrap();
    idle.close().await.unwrap();
}

/// Nothing lists the log on a clock: an idle committer makes no request
/// at all, a commit's wait refreshes until its slot is served, and the
/// committer reads its own commit straight after.
#[tokio::test]
async fn an_idle_committer_lists_nothing_and_a_commit_refreshes_until_served() {
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

    let committer_provider = Arc::new(provider.sharing());
    let committer = Catalog::open(
        object_store,
        CatalogOptions {
            commit_log: Some(committer_provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    // The reader's first poll fires on open rather than after the
    // interval; let it pass before the count is a baseline.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let idle = committer_provider.polls();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        committer_provider.polls(),
        idle,
        "an idle committer lists nothing: its poll keeps a checkpoint \
         alive, not a view"
    );

    committer
        .commit(|tx| tx.create_schema("b").map(|_| ()))
        .await
        .unwrap();
    assert!(
        committer_provider.polls() > idle,
        "the commit's wait refreshed until its slot was served"
    );
    assert!(
        committer
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("b")
            .is_some(),
        "the committer reads its own commit"
    );

    let settled = committer_provider.polls();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        committer_provider.polls(),
        settled,
        "served, the committer rests"
    );
    committer.close().await.unwrap();
}

/// A committer that does not fold never takes the store: it adopts an
/// unadopted log and gives it straight back, commits throughout, lost
/// races included, and leaves the whole tail for a folder of its own.
#[tokio::test]
async fn a_committer_that_does_not_fold_never_takes_the_store() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let unfolding = |provider: Arc<TestProvider>| CatalogOptions {
        commit_log: Some(provider),
        commit_log_poll_interval: std::time::Duration::from_millis(10),
        reader_poll_interval: std::time::Duration::from_millis(10),
        commit_log_folding: Some(crate::Folding::Never),
        ..options_at("lake")
    };

    // Adopting takes the store and gives it straight back; the tail of
    // three commits is left where it lands.
    let first = Catalog::open(object_store.clone(), unfolding(provider.clone()))
        .await
        .unwrap();
    assert!(
        !first.holds_store(),
        "adopting gives the store back at once"
    );
    assert_eq!(provider.log.slots().len(), 1, "the log was adopted");
    for name in ["a", "b", "c"] {
        first
            .commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
        assert!(!first.holds_store());
    }
    assert_eq!(
        provider.folded_sequence(),
        1,
        "nothing folded past the adoption"
    );

    // A long tail at attach is not taken; a lost race to a peer's commit is
    // folded through the reader, and still not taken.
    let late = Arc::new(provider.sharing());
    let second = Catalog::open(object_store.clone(), unfolding(late.clone()))
        .await
        .unwrap();
    assert!(!second.holds_store(), "a long tail is not taken at attach");
    let peer = Arc::new(provider.sharing());
    let third = Catalog::open(object_store.clone(), unfolding(peer.clone()))
        .await
        .unwrap();
    third
        .commit(|tx| tx.create_schema("d").map(|_| ()))
        .await
        .unwrap();
    // The loss folds the peer's slot through the reader and surfaces as a
    // conflict, since both touch the schema list; re-driven, it lands.
    let conflict = second.commit(|tx| tx.create_schema("e").map(|_| ())).await;
    assert!(
        matches!(conflict, Err(crate::Error::CommitConflict(_))),
        "{conflict:?}"
    );
    second
        .commit(|tx| tx.create_schema("e").map(|_| ()))
        .await
        .unwrap();
    assert!(!second.holds_store(), "nor after a lost race");
    assert!(!third.holds_store());
    assert_eq!(provider.log.slots().len(), 6);

    // No close sprints, so the folded store stands where adoption left it
    // and a reader following it alone sees none of the five commits.
    third.close().await.unwrap();
    second.close().await.unwrap();
    first.close().await.unwrap();
    assert_eq!(provider.folded_sequence(), 1, "no close folded the tail");
    let folded_only = |provider: Arc<TestProvider>| {
        Catalog::open_read_only(
            object_store.clone(),
            CatalogOptions {
                commit_log: Some(provider),
                commit_log_tail: false,
                ..options_at("lake")
            },
        )
    };
    let behind = folded_only(Arc::new(provider.sharing())).await.unwrap();
    let snapshot = behind.snapshot().await.unwrap();
    for name in ["a", "b", "c", "d", "e"] {
        assert!(snapshot.schema_by_name(name).is_none(), "{name} was folded");
    }
    behind.close().await.unwrap();

    // A sprint is what drains it, and the folded store is current after.
    let folded = Catalog::fold_sprint(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert_eq!(folded, 5, "the sprint folded every slot past the adoption");
    let caught_up = folded_only(provider).await.unwrap();
    let snapshot = caught_up.snapshot().await.unwrap();
    for name in ["a", "b", "c", "d", "e"] {
        assert!(snapshot.schema_by_name(name).is_some(), "{name} was folded");
    }
    caught_up.close().await.unwrap();
}

/// The unfolded tail is readable without folding it, so a lake whose
/// committers fold nothing can be watched rather than only discovered.
#[tokio::test]
async fn a_committer_reports_how_far_behind_the_folded_store_is() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let committer = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            commit_log_folding: Some(crate::Folding::Never),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    let behind = committer.unfolded_slots().await.unwrap();

    for name in ["a", "b", "c"] {
        committer
            .commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }
    assert_eq!(
        committer.unfolded_slots().await.unwrap(),
        behind.map(|slots| slots + 3),
        "three commits, three more slots the store lacks"
    );

    // A sprint drains it, and what the sprint reports is what was behind.
    let behind = committer.unfolded_slots().await.unwrap().unwrap();
    committer.close().await.unwrap();
    let folded = Catalog::fold_sprint(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert_eq!(folded, behind, "the sprint folded exactly what was behind");

    let drained = Catalog::open(
        object_store,
        CatalogOptions {
            commit_log: Some(provider),
            commit_log_folding: Some(crate::Folding::Never),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert_eq!(drained.unfolded_slots().await.unwrap(), Some(0));
    drained.close().await.unwrap();
}

/// A catalog with no commit log has no tail to be behind.
#[tokio::test]
async fn a_catalog_without_a_log_reports_no_tail() {
    let object_store = Arc::new(InMemory::new());
    let catalog = Catalog::open(object_store, options_at("lake"))
        .await
        .unwrap();
    assert_eq!(catalog.unfolded_slots().await.unwrap(), None);
    catalog.close().await.unwrap();
}

/// Two committers over one lake each read what the other just committed,
/// at the commit poll rather than the reader interval: a peer's slot is
/// what a reader needs soonest, and a session reading at all keeps
/// watching for one.
#[tokio::test]
async fn a_committer_reading_steadily_sees_a_peers_commit_at_once() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let options = |provider: Arc<TestProvider>| CatalogOptions {
        commit_log: Some(provider),
        commit_log_poll_interval: std::time::Duration::from_millis(10),
        ..options_at("lake")
    };

    let first = Catalog::open(object_store.clone(), options(provider.clone()))
        .await
        .unwrap();
    first
        .commit(|tx| tx.create_schema("from_first").map(|_| ()))
        .await
        .unwrap();

    let second = Catalog::open(object_store, options(Arc::new(provider.sharing())))
        .await
        .unwrap();
    assert!(
        second
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("from_first")
            .is_some(),
        "the second committer opens on what the first committed"
    );
    second
        .commit(|tx| tx.create_schema("from_second").map(|_| ()))
        .await
        .unwrap();

    // The first read after a peer's commit, with no idling in between.
    assert!(
        first
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("from_second")
            .is_some(),
        "a reading committer sees its peer's commit without waiting out \
         the reader interval"
    );
    first.close().await.unwrap();
    second.close().await.unwrap();
}

/// A provider whose deployment folds elsewhere settles it for every
/// application over it: a committer takes the store for nothing, folds
/// nothing at close, and the tail is a scheduled fold's to drain.
#[tokio::test]
async fn a_provider_that_folds_elsewhere_keeps_committers_off_the_store() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default().folded_elsewhere());
    let options = || CatalogOptions {
        commit_log: Some(provider.clone()),
        // Unset: the deployment's lead, which is to fold elsewhere.
        commit_log_folding: None,
        commit_log_poll_interval: std::time::Duration::from_millis(10),
        ..options_at("lake")
    };

    let folder_store = object_store.clone();
    let committer = Catalog::open(object_store.clone(), options())
        .await
        .unwrap();
    assert!(!committer.holds_store(), "adopting gives the store back");
    for name in ["a", "b", "c"] {
        committer
            .commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
        assert!(!committer.holds_store());
    }
    let behind = committer.unfolded_slots().await.unwrap();
    committer.close().await.unwrap();
    assert_eq!(
        provider.folded_sequence(),
        1,
        "no close sprinted: {behind:?} slots were left"
    );

    // What a scheduled fold is for.
    let folded = Catalog::fold_sprint(
        object_store,
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert_eq!(folded, 3, "the sprint drained what the committers left");

    // A process that says it is the sprinter is not overruled: it sprints
    // on the way out, where one following the deployment left the tail.
    let sprinter = Catalog::open(
        folder_store,
        CatalogOptions {
            commit_log: Some(provider.clone()),
            commit_log_folding: Some(crate::Folding::Holding { slots: 0 }),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    sprinter
        .commit(|tx| tx.create_schema("d").map(|_| ()))
        .await
        .unwrap();
    sprinter.close().await.unwrap();
    assert_eq!(
        provider.folded_sequence(),
        5,
        "asking to fold overrules the deployment"
    );
}
