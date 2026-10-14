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
    // lands after its open; and it never takes the store, so the rule under
    // test is the only thing that can save it.
    let committer_provider = Arc::new(provider.sharing());
    let committer = Catalog::open(
        object_store,
        CatalogOptions {
            commit_log: Some(committer_provider.clone()),
            commit_log_role: CommitLogRole::Committer,
            commit_log_poll_interval: std::time::Duration::from_secs(200),
            commit_log_sprint_slots: 0,
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
        "a stale committer must not win: {outcome:?}; holds={} folded={} slots={:?} head={:?}",
        committer.holds_store(),
        committer_provider.folded_sequence(),
        provider
            .log
            .slots()
            .iter()
            .map(|(sequence, _)| *sequence)
            .collect::<Vec<_>>(),
        committer
            .snapshot()
            .await
            .map(|s| s.current_snapshot().id.get()),
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

/// After the configured number of folded slots the folder writes its
/// memtable out and truncates the log: a reader following the folded store
/// alone sees the commits, and the slots below the replay point are gone.
#[tokio::test]
async fn the_folder_flushes_and_truncates_after_enough_folded_slots() {
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
    // Adoption is the first fold; this commit is the second, so it flushes.
    folder
        .commit(|tx| tx.create_schema("a").map(|_| ()))
        .await
        .unwrap();

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
            .schema_by_name("a")
            .is_some(),
        "the flush carried the commit into the manifest"
    );
    folded_only.close().await.unwrap();
    // The collector keeps the slot at the replay point itself and deletes
    // everything below it.
    assert_eq!(
        provider
            .log
            .slots()
            .iter()
            .map(|(sequence, _)| *sequence)
            .collect::<Vec<_>>(),
        vec![2],
        "slots below the replay point were truncated"
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
            .writer_db()
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
            commit_log_role: CommitLogRole::Committer,
            commit_log_poll_interval: std::time::Duration::from_millis(10),
            commit_log_sprint_slots: 1,
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    for name in ["b", "c"] {
        committer
            .commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
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
    assert!(snapshot.schema_by_name("b").is_some() && snapshot.schema_by_name("c").is_some());
    folded_only.close().await.unwrap();
}

/// The folder's own rows outside a slot must stay below the next replayed
/// slot's number: when they would not, the folder commits empty
/// head-preserving slots first, each buying another slot's worth.
#[tokio::test]
async fn the_folder_advances_the_log_ahead_of_its_own_rows() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default().with_rows_per_slot(64));

    // Before any replay the store's own rows sit far below the first
    // slot's number; the headroom only tightens once an open has replayed
    // a slot at its number. So: adopt, close, let a peer land, reopen.
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
    provider
        .log
        .land(3, peer_slot(&provider, 3, b"\xffpeer", b"landed"));
    let folder = Catalog::open(
        object_store,
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    let before = provider.log.slots().len();
    let head_before = folder.snapshot().await.unwrap().current_snapshot().id;

    // Three slots' worth of rows, in one reservation.
    folder.reserve_own_rows(200).await.unwrap();

    let slots = provider.log.slots();
    assert!(
        slots.len() >= before + 3,
        "advanced by empty slots: {} -> {}",
        before,
        slots.len()
    );
    let head_key = Key::Sys(SysKey::Head).encode();
    for (_, envelope) in &slots[before..] {
        let stamps: Vec<proto::HeadValue> = envelope
            .writes()
            .filter(|write| write.key == head_key)
            .map(|write| value::decode_value(write.value.as_ref().unwrap()).unwrap())
            .collect();
        assert_eq!(stamps.len(), 1);
        assert_eq!(stamps[0].snapshot_id, head_before.get(), "head-preserving");
    }
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
            commit_log_role: CommitLogRole::Committer,
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
            commit_log_role: CommitLogRole::Committer,
            commit_log_poll_interval: std::time::Duration::from_millis(10),
            commit_log_sprint_slots: 1,
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
            commit_log_role: CommitLogRole::Committer,
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

    // Another process takes the store outright.
    let usurper_provider = Arc::new(provider.sharing());
    let usurper = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(usurper_provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
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
