//! The seam over the in-tree log: readers tail it, a committer adopts it and
//! commits through it, a lost race is served the winner, the cadence takes
//! the store through the catalog's taker, and a store stamped for the log
//! is refused without a provider. The process-wide registry is covered by
//! its own test binary, since it is global.

use std::sync::Arc;

use object_store::memory::InMemory;

use crate::{
    Catalog, CatalogOptions,
    commit_log::{
        CommitLogProvider, SlotCommit, SlotEnvelope, SlotOffer, SlotWrite,
        test_log::{TestProvider, TestStore},
    },
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
        // A committer's reads wait out a poll of its reader.
        reader_poll_interval: std::time::Duration::from_millis(10),
        ..CatalogOptions::default()
    }
}

#[tokio::test]
async fn acknowledged_slots_are_available_in_fixed_read_scopes() {
    let objects = Arc::new(InMemory::new());
    bootstrapped(objects.clone(), "published").await;
    let provider = Arc::new(TestProvider::default());
    let options = CatalogOptions {
        commit_log: Some(provider),
        commit_log_fold_slots: 10_000,
        reader_poll_interval: std::time::Duration::ZERO,
        ..options_at("published")
    };
    let writer = Catalog::open(objects.clone(), options.clone())
        .await
        .unwrap();
    let reader = Catalog::open_read_only(objects, options).await.unwrap();
    writer
        .commit_materialized(|tx| tx.set_option(crate::OptionScope::Global, "probe", "first"))
        .await
        .unwrap();
    let first = reader.index_read_scope().await.unwrap();
    assert_eq!(
        crate::ffi_support::catalog_read::global_option(first.reads(), "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("first")
    );
    writer
        .commit_materialized(|tx| tx.set_option(crate::OptionScope::Global, "probe", "second"))
        .await
        .unwrap();
    let second = reader.index_read_scope().await.unwrap();
    assert_ne!(first.identity(), second.identity());
    assert_eq!(
        crate::ffi_support::catalog_read::global_option(first.reads(), "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("first")
    );
    assert_eq!(
        crate::ffi_support::catalog_read::global_option(second.reads(), "probe")
            .await
            .unwrap()
            .as_deref(),
        Some("second")
    );
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

/// Bootstraps a catalog at `path` and releases the writer.
async fn bootstrapped(object_store: Arc<InMemory>, path: &str) {
    let writer = Catalog::open(object_store, options_at(path)).await.unwrap();
    writer.close().await.unwrap();
}

/// The provider's log store, which the in-tree log ignores.
fn any_log_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(InMemory::new())
}

/// A peer's commit at `sequence`: one write under `key`, with the head stamp
/// every slot carries, at the counter `sequence` implies.
fn peer_commit(provider: &TestProvider, sequence: u64, key: &[u8], value: &[u8]) -> SlotCommit {
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

    SlotCommit {
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
    }
}

/// A peer's slot at `sequence`, as [`peer_commit`].
fn peer_slot(provider: &TestProvider, sequence: u64, key: &[u8], value: &[u8]) -> SlotEnvelope {
    SlotEnvelope {
        commits: vec![peer_commit(provider, sequence, key, value)],
    }
}

/// The sequences the log holds, in order.
fn sequences(provider: &TestProvider) -> Vec<u64> {
    provider
        .log
        .slots()
        .iter()
        .map(|(sequence, _)| *sequence)
        .collect()
}

/// A tailing read-only open builds its store over the provider's reader
/// and is served the slots the log holds past the fold.
#[tokio::test]
async fn a_tailing_read_only_open_is_served_the_logs_slots() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;

    // A committer adopts the log, so slots count from the store's own WAL.
    let provider = Arc::new(TestProvider::default());
    let committer = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    committer.close().await.unwrap();

    provider
        .log
        .land(2, peer_slot(&provider, 2, b"\xffpeer", b"landed"));

    let reader = Catalog::open_read_only(
        object_store,
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();

    assert!(
        provider
            .await_served(2, std::time::Duration::from_secs(5))
            .await,
        "the tailing reader replayed the slot"
    );
    assert_eq!(
        reader
            .materialized()
            .await
            .unwrap()
            .current_snapshot()
            .id
            .get(),
        0
    );
    reader.close().await.unwrap();
}

/// A read-write open with a provider adopts the log — its format stamp is
/// the first slot — and from then on each commit is a slot, which a
/// tailing reader of the same log serves before anything has folded.
#[tokio::test]
async fn a_committer_adopts_the_log_and_commits_through_it() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let committer = Catalog::open(
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
    assert_eq!(
        provider.folded_sequence(),
        1,
        "the adoption folded its own stamp into the store it held"
    );

    committer
        .commit_materialized(|tx| tx.create_schema("s").map(|_| ()))
        .await
        .unwrap();
    let slots = provider.log.slots();
    assert_eq!(slots.len(), 2, "{slots:?}");
    let head = Key::Sys(SysKey::Head).encode();
    assert!(
        slots[1].1.writes().any(|write| write.key == head),
        "the commit's batch rode the second slot"
    );
    assert_eq!(
        provider.folded_sequence(),
        2,
        "publication replays the acknowledged slot without requiring an L0 flush"
    );

    let reader = Catalog::open_read_only(
        object_store,
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(
        reader
            .materialized()
            .await
            .unwrap()
            .schema_by_name("s")
            .is_some(),
        "a tailing reader serves the slot before any flush"
    );
    reader.close().await.unwrap();
    committer.close().await.unwrap();
}

/// A commit prepared against a head a peer has moved past names a taken
/// slot and loses: the committer is served the winner and the retry lands
/// after it, and the next writer open replays the winner into the store.
#[tokio::test]
async fn a_lost_race_is_served_the_winner_before_the_retry_lands() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let committer = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();

    // A peer lands a slot in the moment after this committer last read.
    provider
        .log
        .land_unseen(2, peer_slot(&provider, 2, b"\xffpeer", b"landed"));

    committer
        .commit_materialized(|tx| tx.create_schema("s").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(
        sequences(&provider),
        vec![1, 2, 3],
        "the retry landed after the peer's slot"
    );
    assert_eq!(
        provider.folded_sequence(),
        3,
        "publication covers the winner and the acknowledged retry"
    );
    committer.close().await.unwrap();

    // The next writer open's replay is the fold: the peer's write is there.
    let (db, _counters) = StoreBuilder::new("lake", object_store)
        .writer_session(Some(provider.writer_session(any_log_store(), "lake")))
        .open_writer()
        .await
        .unwrap();
    assert_eq!(
        db.get(b"\xffpeer").await.unwrap().as_deref(),
        Some(&b"landed"[..])
    );
    db.close().await.unwrap();
}

/// Two read-write processes commit over tailing readers: neither needs a
/// store of its own, each lands its batch as a slot, and each is served
/// the other's the moment it reads.
#[tokio::test]
async fn two_committers_commit_through_one_log() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let first = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    first
        .commit_materialized(|tx| tx.create_schema("a").map(|_| ()))
        .await
        .unwrap();

    // Another process over the same log: its own cursor, the same slots.
    let second_provider = Arc::new(provider.sharing());
    let second = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(second_provider.clone()),
            reader_poll_interval: std::time::Duration::from_millis(10),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    second
        .commit_materialized(|tx| tx.create_schema("b").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(
        sequences(&provider),
        vec![1, 2, 3],
        "the second committer's batch is the third slot"
    );
    let seen = second.materialized().await.unwrap();
    assert!(
        seen.schema_by_name("a").is_some(),
        "the first committer's commit, served"
    );
    assert!(seen.schema_by_name("b").is_some(), "its own commit, served");

    // The first is served slot 3 as it reads, and lands after it.
    assert!(
        first
            .materialized()
            .await
            .unwrap()
            .schema_by_name("b")
            .is_some(),
        "a read is served the peer's slot"
    );
    first
        .commit_materialized(|tx| tx.create_schema("c").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(sequences(&provider), vec![1, 2, 3, 4]);
    assert_eq!(
        provider.folded_sequence(),
        4,
        "publication replays through the acknowledged retry"
    );

    // And the second is served the first's slot in turn: a read asks, and
    // the listing it causes serves the slot.
    second.materialized().await.unwrap();
    assert!(
        second_provider
            .await_served(4, std::time::Duration::from_secs(5))
            .await
    );
    assert!(
        second
            .materialized()
            .await
            .unwrap()
            .schema_by_name("c")
            .is_some()
    );

    second.close().await.unwrap();
    first.close().await.unwrap();
}

/// A committer told a slot is served that its reader has not applied
/// refreshes before it offers, so it lands past that slot rather than
/// racing the sequence the slot already holds.
#[tokio::test]
async fn a_committer_told_of_an_unapplied_slot_applies_it_before_offering() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let adopter = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    adopter
        .commit_materialized(|tx| tx.create_schema("a").map(|_| ()))
        .await
        .unwrap();
    adopter.close().await.unwrap();

    // The committer never takes the store, so only its own reading can
    // save it from offering against a state it has not caught up to.
    let committer_provider = Arc::new(provider.sharing());
    let committer = Catalog::open(
        object_store,
        CatalogOptions {
            commit_log: Some(committer_provider.clone()),
            commit_log_fold_slots: u64::MAX,
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
        .commit_materialized(|tx| tx.create_schema("b").map(|_| ()))
        .await;
    assert!(
        outcome.is_ok(),
        "the refresh before the offer applies the slot: {outcome:?}"
    );
    assert_eq!(
        sequences(&provider).last(),
        Some(&4),
        "and the commit lands past the slot it was told of"
    );
    committer.close().await.unwrap();
}

/// A follower that lands a slot the cadence names takes the store through
/// the catalog's taker, folds, writes the memtable out and truncates the
/// log, and lets go: a reader following the folded store alone sees the
/// commits, and the slots below the replay point are gone.
#[tokio::test]
async fn the_cadence_takes_the_store_folds_and_truncates() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let committer = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            // Every slot folds, and every fold writes out.
            commit_log_fold_slots: 1,
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    for name in ["a", "b"] {
        committer
            .commit_materialized(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }
    assert_eq!(
        provider.folded_sequence(),
        3,
        "each slot's fold took the store and replayed through it"
    );
    // This process's own tailing reader pins the log through its checkpoint
    // until its next poll moves the checkpoint past the flush, so the
    // collector truncates at the flush after that one.
    let polls = provider.polls();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while provider.polls() < polls + 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // A superseded publication keeps the slots it replays from until its
    // grace ends; stand in for that expiry so this flush may truncate.
    let admin = slatedb::admin::AdminBuilder::new("lake", object_store.clone()).build();
    for checkpoint in admin
        .list_checkpoints(Some(crate::store::checkpoint::PUBLISHED_NAME))
        .await
        .unwrap()
    {
        if checkpoint.expire_time.is_some() {
            admin.delete_checkpoint(checkpoint.id).await.unwrap();
        }
    }
    for name in ["c", "d"] {
        committer
            .commit_materialized(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }

    let folded_only = Catalog::open_read_only(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    assert!(
        folded_only
            .materialized()
            .await
            .unwrap()
            .schema_by_name("d")
            .is_some(),
        "the fold carried the commits into the manifest"
    );
    folded_only.close().await.unwrap();
    let remaining = sequences(&provider);
    assert!(
        remaining.first().is_some_and(|first| *first > 1) && remaining.last() == Some(&5),
        "slots below the replay point were truncated: {remaining:?}"
    );
    committer.close().await.unwrap();
}

/// Rows a handle writes for itself, the sweeps and the status record,
/// land as head-preserving slots: one slot each, carrying the rows and
/// exactly one head stamp at the standing snapshot.
#[tokio::test]
async fn a_handles_own_rows_land_as_head_preserving_slots() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let committer = Catalog::open(
        object_store,
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    committer
        .commit_materialized(|tx| tx.create_schema("a").map(|_| ()))
        .await
        .unwrap();
    let before = provider.log.slots().len();
    let head_before = committer
        .materialized()
        .await
        .unwrap()
        .current_snapshot()
        .id;

    committer
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
        committer
            .materialized()
            .await
            .unwrap()
            .current_snapshot()
            .id,
        head_before
    );
    committer.close().await.unwrap();
}

/// The holder side of the in-tree store, at the seam: built over the store
/// the catalog's taker opened, it folds a loss into the store and applies
/// its win, and its fold finds nothing more.
#[tokio::test]
async fn a_holder_folds_a_loss_and_applies_its_win() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let options = CatalogOptions {
        commit_log: Some(provider.clone()),
        ..options_at("lake")
    };

    let committer = Catalog::open(object_store.clone(), options.clone())
        .await
        .unwrap();
    committer
        .commit_materialized(|tx| tx.create_schema("a").map(|_| ()))
        .await
        .unwrap();
    committer.close().await.unwrap();

    let object_store: Arc<dyn object_store::ObjectStore> = object_store;
    let (db, session) = Catalog::store_taker(Arc::clone(&object_store), &options)
        .take()
        .await
        .unwrap()
        .expect("nothing stalls the take");
    let holder = provider.holding(
        db.clone(),
        session,
        Catalog::commit_log_hooks(&object_store, &options),
    );

    // A peer's slot lands after the take's replay: the offer meets it,
    // loses, and the holder folds the winner into the store it holds.
    provider
        .log
        .land_unseen(3, peer_slot(&provider, 3, b"\xffpeer", b"landed"));
    let lost = holder
        .write_at(3, peer_commit(&provider, 3, b"\xffmine", b"raced"))
        .await
        .unwrap();
    assert!(
        matches!(
            lost,
            SlotOffer::Lost {
                sequence: 3,
                through: 3,
                ..
            }
        ),
        "{lost:?}"
    );
    assert_eq!(
        db.get(b"\xffpeer").await.unwrap().as_deref(),
        Some(&b"landed"[..]),
        "the loss folded the winner"
    );
    assert_eq!(db.get(b"\xffmine").await.unwrap(), None);

    // Past the winner, the holder's own offer wins and is applied.
    let won = holder
        .write_at(4, peer_commit(&provider, 4, b"\xffmine", b"won"))
        .await
        .unwrap();
    assert_eq!(won, SlotOffer::Won { sequence: 4 });
    assert_eq!(
        db.get(b"\xffmine").await.unwrap().as_deref(),
        Some(&b"won"[..]),
        "the win was applied to the held store"
    );
    assert_eq!(holder.fold_tail().await.unwrap(), 0, "nothing is behind");
    assert_eq!(provider.folded_sequence(), 4);
    holder.close().await.unwrap();

    // The close wrote the fold out: a reader of the folded store alone
    // has what the holder folded and what it won.
    let (reader, _counters) = StoreBuilder::new("lake", object_store)
        .reader_session(Some(provider.reader_session(any_log_store(), "lake")))
        .open_reader()
        .await
        .unwrap();
    assert_eq!(
        reader.get(b"\xffpeer").await.unwrap().as_deref(),
        Some(&b"landed"[..])
    );
    assert_eq!(
        reader.get(b"\xffmine").await.unwrap().as_deref(),
        Some(&b"won"[..])
    );
    reader.close().await.unwrap();
}

/// An adopted store opens read-only through a provider and is refused
/// without one.
#[tokio::test]
async fn an_adopted_store_needs_a_provider_to_open() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());

    let committer = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    committer.close().await.unwrap();

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

/// A committer idle longer than its reader interval lists before it reads,
/// so a read is served state no staler than that interval however long the
/// catalog went unused.
#[tokio::test]
async fn a_committer_acquires_a_fresh_view_after_idle() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let adopter = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    adopter.close().await.unwrap();

    let options = |provider: Arc<TestProvider>| CatalogOptions {
        commit_log: Some(provider),
        // The fixed view must see the peer even between base-reader polls.
        reader_poll_interval: std::time::Duration::from_millis(50),
        ..options_at("lake")
    };
    let idle_provider = Arc::new(provider.sharing());
    let idle = Catalog::open(object_store.clone(), options(idle_provider.clone()))
        .await
        .unwrap();
    idle.materialized().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let peer = Catalog::open(object_store, options(Arc::new(provider.sharing())))
        .await
        .unwrap();
    peer.commit_materialized(|tx| tx.create_schema("landed").map(|_| ()))
        .await
        .unwrap();

    assert!(
        idle.materialized()
            .await
            .unwrap()
            .schema_by_name("landed")
            .is_some(),
        "the new fixed view includes the peer commit"
    );
    peer.close().await.unwrap();
    idle.close().await.unwrap();
}

/// The next fixed view includes the committer's acknowledged write.
#[tokio::test]
async fn a_committer_reads_its_commit_in_a_fresh_view() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let adopter = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    adopter.close().await.unwrap();

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
    committer
        .commit_materialized(|tx| tx.create_schema("b").map(|_| ()))
        .await
        .unwrap();
    assert!(
        committer
            .materialized()
            .await
            .unwrap()
            .schema_by_name("b")
            .is_some(),
        "the committer reads its own commit"
    );
    committer.close().await.unwrap();
}

async fn replay_point(objects: Arc<dyn object_store::ObjectStore>) -> u64 {
    slatedb::admin::AdminBuilder::new("lake", objects)
        .build()
        .read_manifest(None)
        .await
        .unwrap()
        .unwrap()
        .replay_after_wal_id()
}

/// A taker replays and flushes published WAL without a live catalog.
#[tokio::test]
async fn the_taker_folds_the_tail_without_a_live_catalog() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let options = CatalogOptions {
        commit_log: Some(provider.clone()),
        commit_log_fold_slots: u64::MAX,
        ..options_at("lake")
    };
    let committer = Catalog::open(object_store.clone(), options.clone())
        .await
        .unwrap();
    let before = replay_point(object_store.clone()).await;
    for name in ["a", "b", "c"] {
        committer
            .commit_materialized(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }
    assert_eq!(
        provider.folded_sequence(),
        4,
        "publication replays the slots without flushing them"
    );
    assert_eq!(replay_point(object_store.clone()).await, before);
    committer.close().await.unwrap();

    let object_store: Arc<dyn object_store::ObjectStore> = object_store;
    let (db, session) = Catalog::store_taker(Arc::clone(&object_store), &options)
        .take()
        .await
        .unwrap()
        .expect("nothing stalls the take");
    assert!(session.adopted().await.unwrap());
    assert_eq!(
        provider.folded_sequence(),
        4,
        "the take's replay folded exactly what was behind"
    );
    db.close().await.unwrap();
    assert!(replay_point(object_store.clone()).await > before);

    let folded_only = Catalog::open_read_only(object_store, CatalogOptions { ..options })
        .await
        .unwrap();
    assert!(
        folded_only
            .materialized()
            .await
            .unwrap()
            .schema_by_name("c")
            .is_some(),
        "the store's close wrote the fold out"
    );
    folded_only.close().await.unwrap();
}

/// A catalog's commit store is the library's, reachable through `Any`,
/// and only a catalog on a log has one.
#[tokio::test]
async fn the_commit_store_is_the_librarys_and_only_on_a_log() {
    let object_store = Arc::new(InMemory::new());
    let plain = Catalog::open(object_store.clone(), options_at("plain"))
        .await
        .unwrap();
    assert!(plain.commit_store().is_none());
    plain.close().await.unwrap();

    bootstrapped(object_store.clone(), "lake").await;
    let committer = Catalog::open(
        object_store,
        CatalogOptions {
            commit_log: Some(Arc::new(TestProvider::default())),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    let store = committer
        .commit_store()
        .expect("a committer commits through a store");
    assert!(
        store.as_any().downcast_ref::<TestStore>().is_some(),
        "the store is the provider's own: {store:?}"
    );
    assert!(store.adopted().await.unwrap());
    committer.close().await.unwrap();
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
        ..options_at("lake")
    };

    let first = Catalog::open(object_store.clone(), options(provider.clone()))
        .await
        .unwrap();
    first
        .commit_materialized(|tx| tx.create_schema("from_first").map(|_| ()))
        .await
        .unwrap();

    let second = Catalog::open(object_store, options(Arc::new(provider.sharing())))
        .await
        .unwrap();
    assert!(
        second
            .materialized()
            .await
            .unwrap()
            .schema_by_name("from_first")
            .is_some(),
        "the second committer opens on what the first committed"
    );
    second
        .commit_materialized(|tx| tx.create_schema("from_second").map(|_| ()))
        .await
        .unwrap();

    // The first read after a peer's commit, with no idling in between.
    assert!(
        first
            .materialized()
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

/// The slot a commit lands names whether it folds: every `fold_slots`th
/// one does, whichever committer wrote it, and no process measures
/// anything to find out. The fold moves the replay point to the slot and
/// lets the store go.
#[tokio::test]
async fn the_committer_that_lands_the_nth_slot_folds() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let options = |provider: Arc<TestProvider>| CatalogOptions {
        commit_log: Some(provider),
        // Every fourth slot is folded by whoever lands it.
        commit_log_fold_slots: 4,
        ..options_at("lake")
    };

    let committer = Catalog::open(object_store.clone(), options(provider.clone()))
        .await
        .unwrap();

    // Slot 1 is the adoption; these commits are slots 2..8, so slot 4 and
    // slot 8 are folded by whichever committer lands them.
    for index in 0..3 {
        committer
            .commit_materialized(|tx| tx.create_schema(&format!("s{index}")).map(|_| ()))
            .await
            .unwrap();
    }
    assert_eq!(provider.folded_sequence(), 4, "slot 4 folded");
    let after_four = replay_point(object_store.clone()).await;
    for index in 3..6 {
        committer
            .commit_materialized(|tx| tx.create_schema(&format!("s{index}")).map(|_| ()))
            .await
            .unwrap();
    }
    assert_eq!(provider.folded_sequence(), 7, "slots 5..7 are published");
    assert_eq!(replay_point(object_store.clone()).await, after_four);
    committer
        .commit_materialized(|tx| tx.create_schema("s6").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(
        provider.folded_sequence(),
        8,
        "the slot that landed last was a multiple, so nothing is behind"
    );
    assert!(replay_point(object_store.clone()).await > after_four);
    committer.close().await.unwrap();

    // The fold wrote out and let go: a reader of the folded store alone
    // sees every commit, and a writer opens with nothing to replay.
    let folded_only = Catalog::open_read_only(
        object_store,
        CatalogOptions {
            ..options(Arc::new(provider.sharing()))
        },
    )
    .await
    .unwrap();
    assert!(
        folded_only
            .materialized()
            .await
            .unwrap()
            .schema_by_name("s6")
            .is_some()
    );
    folded_only.close().await.unwrap();
}

/// A fixed read sees committed state without requesting a live-reader poll.
#[tokio::test]
async fn a_read_only_log_scope_does_not_refresh_the_live_reader() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let committer = Catalog::open(
        object_store.clone(),
        CatalogOptions {
            commit_log: Some(provider.clone()),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();

    let reader_provider = Arc::new(provider.sharing());
    let reader = Catalog::open_read_only(
        object_store,
        CatalogOptions {
            commit_log: Some(reader_provider.clone()),
            reader_poll_interval: std::time::Duration::from_mins(1),
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let idle = reader_provider.polls();
    committer
        .commit_materialized(|tx| tx.create_schema("landed").map(|_| ()))
        .await
        .unwrap();
    let view = reader.materialized().await.unwrap();
    assert!(view.schema_by_name("landed").is_some());
    assert_eq!(
        reader_provider.polls(),
        idle,
        "the fixed checkpoint needs no live-reader refresh"
    );

    reader.close().await.unwrap();
    committer.close().await.unwrap();
}

/// Demoting takes a lake off its log: the slots are folded, the stamp
/// drops below the log's format, and a binary with no provider — which
/// refused the lake before — opens it and reads what was committed.
#[tokio::test]
async fn a_demoted_lake_opens_without_its_provider() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let adopted = CatalogOptions {
        commit_log: Some(provider.clone()),
        ..options_at("lake")
    };

    let committer = Catalog::open(object_store.clone(), adopted.clone())
        .await
        .unwrap();
    for name in ["a", "b"] {
        committer
            .commit_materialized(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }
    committer.close().await.unwrap();

    // Adopted, so a plain open is refused.
    assert!(matches!(
        Catalog::open(object_store.clone(), options_at("lake")).await,
        Err(crate::Error::Migration(_))
    ));

    let demoted = Box::pin(Catalog::demote(object_store.clone(), adopted.clone()))
        .await
        .unwrap();
    assert_eq!(
        demoted.format,
        crate::transaction::commit::MAX_FORMAT_VERSION
    );

    // Again is a no-op, so a demote interrupted after the stamp can be rerun.
    let again = Box::pin(Catalog::demote(object_store.clone(), adopted.clone()))
        .await
        .unwrap();
    assert_eq!(again.folded, 0);

    // A plain open now succeeds, and sees what was committed through the log.
    let plain = Catalog::open(object_store.clone(), options_at("lake"))
        .await
        .unwrap();
    let snapshot = plain.materialized().await.unwrap();
    assert!(snapshot.schema_by_name("a").is_some());
    assert!(snapshot.schema_by_name("b").is_some());
    plain.close().await.unwrap();
}

/// Demoting without the provider is refused: the lake was adopted with
/// one, and only it can fold the log.
#[tokio::test]
async fn demoting_needs_the_provider() {
    let object_store = Arc::new(InMemory::new());
    bootstrapped(object_store.clone(), "lake").await;
    assert!(matches!(
        Box::pin(Catalog::demote(object_store, options_at("lake"))).await,
        Err(crate::Error::Configuration(_))
    ));
}

/// An acknowledged log commit is visible to a new fixed checkpoint reader.
#[tokio::test]
async fn acknowledged_log_commits_are_visible_to_fixed_readers() {
    let objects = Arc::new(InMemory::new());
    bootstrapped(objects.clone(), "lake").await;
    let provider = Arc::new(TestProvider::default());
    let catalog = Catalog::open(
        objects,
        CatalogOptions {
            commit_log: Some(provider),
            commit_log_fold_slots: 0,
            ..options_at("lake")
        },
    )
    .await
    .unwrap();
    catalog
        .commit_materialized(|transaction| transaction.create_schema("published").map(|_| ()))
        .await
        .unwrap();
    let scope = catalog.index_read_scope().await.unwrap();
    let snapshot = scope.reads().materialized().await.unwrap();
    assert!(snapshot.schema_by_name("published").is_some());
    drop(snapshot);
    drop(scope);
    catalog.close().await.unwrap();
}

mod name_migration;
