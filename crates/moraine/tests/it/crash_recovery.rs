//! What survives a process death: the places a crash can interrupt a
//! state-changing operation, and what must hold once a fresh handle opens
//! the store again.
//!
//! "Reopen" always means constructing a new catalog handle over the same
//! object store, with no in-memory carryover from the process that died.
//! Cases run against real SlateDB on in-memory `object_store`.

pub mod freezing_store;
mod racing_store;

use std::sync::Arc;

use moraine::{
    Catalog, CatalogOptions, ColumnId, CrashCase, CrashPoint, Error, IndexDef, IndexEntry,
    IndexKeyValue, IndexState, IntWidth, MaintenanceRequest, MigrationRequest, OptionScope,
    SchemaId, SyntheticMigration, TableId, inject_crash, install_migration,
};
use object_store::memory::InMemory;

use crate::{
    crash_recovery::{freezing_store::FreezingStore, racing_store::RacingStore},
    fixtures::{col, datafile},
};

/// Every case. [`CrashCase`] is the library's, since one case is crashed
/// from inside a library call and has to name the seams it stops at;
/// [`guarantee`] and [`blocked_on`] below are this suite's own table, and
/// their exhaustive matches make adding a case without deciding both a
/// compile error.
const CASES: [CrashCase; 11] = CrashCase::ALL;

/// Why a case is survivable. Which one applies follows from the path: a
/// path is either one batch or several, never both.
#[derive(Debug, PartialEq, Eq)]
enum Guarantee {
    /// One commit is one batch, so a crash leaves the whole commit or none
    /// of it. There is no torn intermediate to find — for the drop and
    /// genesis cases, proving that absence is the whole point.
    Atomicity,
    /// A long operation runs as several batches and is *not* atomic as a
    /// whole. Each batch is, and the operation persists how far it got, so
    /// a crash leaves a partial state that a re-run continues from —
    /// never one that restarts, double-counts, or serves reads early.
    Resumability,
}

fn guarantee(case: CrashCase) -> Guarantee {
    match case {
        CrashCase::CommitNotDurable
        | CrashCase::CommitDurableNotAcknowledged
        | CrashCase::MultiTombstoneDrop
        | CrashCase::GroupCommit
        | CrashCase::TakeoverMidCommit
        | CrashCase::FencedWriterResumes
        | CrashCase::GenesisInterrupted
        | CrashCase::ConcurrentGenesis => Guarantee::Atomicity,

        CrashCase::StagedBuildInterrupted
        | CrashCase::ReclamationInterrupted
        | CrashCase::MigrationInterrupted => Guarantee::Resumability,
    }
}

/// What must be built before a test here can crash the case, or `None`
/// when a test below already builds the pre-crash state, crashes, reopens,
/// and asserts what must hold. The return type is what a case that cannot
/// be driven from this suite has to fill in.
fn blocked_on(case: CrashCase) -> Option<&'static str> {
    match case {
        CrashCase::CommitNotDurable
        | CrashCase::CommitDurableNotAcknowledged
        | CrashCase::MultiTombstoneDrop
        | CrashCase::GroupCommit
        | CrashCase::TakeoverMidCommit
        | CrashCase::FencedWriterResumes
        | CrashCase::GenesisInterrupted
        | CrashCase::ConcurrentGenesis
        | CrashCase::StagedBuildInterrupted
        | CrashCase::ReclamationInterrupted => None,

        CrashCase::MigrationInterrupted => Some(
            "a fresh store bootstraps at the newest format, and this suite reaches the \
             catalog only through its public API, which offers no way to plant a store at \
             an older format for the migrate verb to carry forward. The driver's four \
             seams are covered against a caller-supplied registry in the migration \
             module's own tests, which restamp the base format directly",
        ),
    }
}

/// Pins which cases are driven and requires the rest to name what blocks
/// them, so a case cannot be quietly stubbed out. The exhaustive matches
/// in [`guarantee`] and [`blocked_on`] cover the other half: a new case
/// fails to compile until both decisions are made.
///
/// Every case is driven but one, and that one is blocked on the topology
/// rather than on the harness: a fresh store bootstraps at the newest
/// format, so this public-API suite cannot stage one for the migrate verb
/// to carry forward.
#[test]
fn every_case_declares_its_guarantee_and_coverage() {
    let driven: Vec<CrashCase> = CASES
        .into_iter()
        .filter(|case| blocked_on(*case).is_none())
        .collect();
    assert_eq!(
        driven,
        vec![
            CrashCase::CommitNotDurable,
            CrashCase::CommitDurableNotAcknowledged,
            CrashCase::MultiTombstoneDrop,
            CrashCase::GroupCommit,
            CrashCase::TakeoverMidCommit,
            CrashCase::FencedWriterResumes,
            CrashCase::GenesisInterrupted,
            CrashCase::ConcurrentGenesis,
            CrashCase::StagedBuildInterrupted,
            CrashCase::ReclamationInterrupted,
        ],
        "driven cases changed; update this list as cases land"
    );
    for expected in [Guarantee::Atomicity, Guarantee::Resumability] {
        assert!(
            driven.iter().any(|case| guarantee(*case) == expected),
            "{expected:?} has no driven case; a guarantee nothing exercises is a claim, not a test"
        );
    }

    for case in CASES {
        if let Some(reason) = blocked_on(case) {
            assert!(!reason.is_empty(), "{case:?} is blocked without a reason");
        }
    }
}

/// `CommitDurableNotAcknowledged` — the flush landed, then the process
/// died before the caller heard back. The commit is durable, so a caller
/// that re-drives the same logical operation runs against the *advanced*
/// head: ids never collide, and a guarded operation surfaces its guard.
/// The scope is deliberate — a data-only re-drive is a fresh commit and
/// lands a second time, because nothing in the protocol dedups it.
#[tokio::test]
async fn durable_commit_survives_a_crash_before_its_acknowledgement() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());

    let writer = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let ids = std::cell::Cell::new(None);
    writer
        .commit(|tx| {
            let schema = tx.create_schema("sales")?;
            let table = tx.create_table(schema, "orders", &[col("id")])?;
            ids.set(Some((schema, table)));
            Ok(())
        })
        .await
        .unwrap();
    let (schema, table) = ids.get().unwrap();
    writer
        .commit(move |tx| tx.register_data_file(table, datafile(100), &[]).map(|_| ()))
        .await
        .unwrap();
    let head = writer.snapshot().await.unwrap().current_snapshot().id;

    // Death between the durable flush and the caller's ack: drop the
    // handle without closing it, so nothing is flushed on the way out.
    drop(writer);

    let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let recovered = reopened.snapshot().await.unwrap();
    assert_eq!(
        recovered.current_snapshot().id,
        head,
        "the landed commit is durable across the crash"
    );
    assert!(
        recovered.schema_by_name("sales").is_some(),
        "and the snapshot resolves fully, not partially"
    );
    assert_eq!(recovered.data_files_of(table).len(), 1);

    // The caller re-drives the guarded operation it never got an ack for:
    // the guard surfaces instead of a duplicate or a corrupted commit.
    let err = reopened
        .commit(move |tx| tx.create_table(schema, "orders", &[col("id")]).map(|_| ()))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)), "got {err:?}");

    // A data-only re-drive is a fresh commit and lands a second time. The
    // duplicate is the caller's to prevent; what matters here is that it
    // lands *cleanly* — row id ranges stay dense and disjoint rather than
    // colliding with the pre-crash commit's.
    reopened
        .commit(move |tx| tx.register_data_file(table, datafile(100), &[]).map(|_| ()))
        .await
        .unwrap();
    let after = reopened.snapshot().await.unwrap();
    let mut starts: Vec<u64> = after
        .data_files_of(table)
        .iter()
        .filter_map(|file| file.row_id_start)
        .collect();
    starts.sort_unstable();
    assert_eq!(
        starts,
        vec![0, 100],
        "counters advanced with the landed commit, so ids never collide"
    );
    reopened.close().await.unwrap();
}

/// `ConcurrentGenesis` — two processes create the same empty catalog at
/// once. There is no create-if-absent primitive to lean on; the guards are
/// the real ones: SlateDB's writer epoch across processes (the second
/// `Db::open` fences the first, so the fenced initializer's genesis batch
/// writes nothing) and write-write conflict detection on `sys/format`
/// within one writer. At most one genesis lands, and reopen shows it —
/// never a second `sys/format`, a divergent genesis snapshot, or a
/// conflicting head.
///
/// The race runs repeatedly because its two guards trip at different
/// points and one round samples only one of them.
///
/// A round may still leave *both* initializers failed, and that is not a
/// torn store. A writer claims the writer epoch and its compactor claims
/// the compactor epoch in two races that are ordered independently, so the
/// handle that wins the writer epoch can lose the compactor epoch — and a
/// fenced compactor closes the handle it belongs to. A fenced genesis
/// re-attempts, which is why this is rare rather than routine, but the
/// re-attempts are bounded and initializers that keep arriving can spend
/// them. Genesis is whole or absent either way, and the recovery is the
/// same one this test then performs: open again.
///
/// [`a_genesis_fenced_mid_bootstrap_re_attempts_and_lands`] stages that
/// fence deterministically and pins the re-attempt, which a round here
/// samples too rarely to hold up on its own.
///
/// A loser fails typed, whichever guard caught it: it lost the manifest
/// race and never created the store ([`Error::OpenRaced`]), or it created
/// the store and was displaced ([`Error::Fenced`]). What it never does is
/// fail as an untyped store error, which "adopts it, or returns a typed
/// error" does not admit.
///
/// The typing rests on matching SlateDB's message text, which a round that
/// happens not to collide would not exercise —
/// [`a_lost_manifest_race_surfaces_typed_rather_than_as_a_store_error`]
/// stages the collision deterministically and pins the wording.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_genesis_leaves_exactly_one_catalog() {
    for round in 0..25 {
        let backing: Arc<InMemory> = Arc::new(InMemory::new());

        let first = backing.clone();
        let second = backing.clone();
        let left =
            tokio::spawn(async move { Catalog::open(first, CatalogOptions::default()).await });
        let right =
            tokio::spawn(async move { Catalog::open(second, CatalogOptions::default()).await });
        let results = [left.await.unwrap(), right.await.unwrap()];

        // A fresh store is not a true conflict, so a loser's failure is
        // benign. It must never be a *catalog* error: reporting corruption,
        // a duplicate, or a missing entity would mean genesis itself tore.
        for result in &results {
            match result {
                Ok(_) | Err(Error::OpenRaced(_) | Error::Fenced(_) | Error::CommitConflict(_)) => {}
                Err(other) => panic!("round {round}: the loser failed untyped: {other:?}"),
            }
        }

        // Reopen with no carryover from either initializer.
        drop(results);
        let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
            .await
            .unwrap();
        let snapshot = reopened.snapshot().await.unwrap();
        assert_eq!(
            snapshot.current_snapshot().id.get(),
            0,
            "round {round}: genesis leaves head at snapshot 0; a second genesis would advance it"
        );
        let schemas = snapshot.schemas();
        assert_eq!(
            schemas.len(),
            1,
            "round {round}: exactly one bootstrap `main` schema, not one per initializer"
        );
        assert_eq!(schemas[0].name, "main");
        reopened.close().await.unwrap();
    }
}

/// A genesis open displaced mid-bootstrap re-attempts instead of handing
/// the caller a fence. The staged genesis never landed, so the store still
/// has no catalog and the second attempt creates it — the caller sees the
/// catalog it asked for rather than an error it could only answer by
/// opening again itself.
///
/// The displacement is staged from outside: refusing the first batch the
/// writer flushes is what a writer that lost the epoch finds, so the fence
/// here is SlateDB's real one.
#[tokio::test]
async fn a_genesis_fenced_mid_bootstrap_re_attempts_and_lands() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());
    let store = Arc::new(RacingStore::losing_the_first_batch_write(backing.clone()));

    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .expect("the re-attempt creates the catalog the fenced attempt could not");

    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(
        snapshot.current_snapshot().id.get(),
        0,
        "one genesis, not one per attempt"
    );
    let schemas = snapshot.schemas();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, "main");
    catalog.close().await.unwrap();
}

/// The genesis race's loser fails **typed**, and this pins the one thread
/// holding that up.
///
/// A lost manifest compare-and-swap and a damaged manifest both reach
/// moraine as the same store-error kind, and the predicate separating them
/// is private to SlateDB — so the mapping matches SlateDB's message text
/// instead. Text is a fragile contract, which is exactly why it is pinned
/// here rather than left to the racing test above, where a round that
/// happens not to collide would prove nothing.
///
/// Losing the first manifest write is staged from outside, so the failure
/// is SlateDB's real one on every run. If a SlateDB bump rewords it, the
/// match stops firing and this fails with [`Error::Store`] — loudly, at
/// the seam that broke, instead of every genesis race quietly going
/// untyped again.
#[tokio::test]
async fn a_lost_manifest_race_surfaces_typed_rather_than_as_a_store_error() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());
    let store = Arc::new(RacingStore::losing_the_first_manifest_write(
        backing.clone(),
    ));

    let err = Catalog::open(store, CatalogOptions::default())
        .await
        .expect_err("an open that loses the manifest race cannot succeed");
    assert!(
        matches!(err, Error::OpenRaced(_)),
        "the loser must name the race it lost; got {err:?}"
    );

    // Benign, and the message says so: the store is untouched, so opening
    // it again creates the catalog rather than finding a half-made one.
    let reopened = Catalog::open(backing, CatalogOptions::default())
        .await
        .unwrap();
    let snapshot = reopened.snapshot().await.unwrap();
    assert_eq!(snapshot.current_snapshot().id.get(), 0);
    assert_eq!(snapshot.schemas().len(), 1);
    reopened.close().await.unwrap();
}

/// `TakeoverMidCommit` — a second writer opens the store while the first
/// is live and takes it over. The takeover adds no torn state of its own:
/// it changes only *who* observes the all-or-none result. Here the first
/// writer's commit landed durably before the takeover, so the second must
/// read the advanced head and continue from it. (The unlanded branch is
/// `CommitNotDurable`, which needs the write freeze.)
#[tokio::test]
async fn takeover_reads_the_durable_head_and_continues_from_it() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());

    let first = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    first
        .commit(|tx| tx.create_schema("landed").map(|_| ()))
        .await
        .unwrap();
    let head_at_first = first.snapshot().await.unwrap().current_snapshot().id;

    // The second writer never saw the first's memory, only the store.
    let second = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let taken_over = second.snapshot().await.unwrap();
    assert_eq!(
        taken_over.current_snapshot().id,
        head_at_first,
        "a durable commit survives the takeover intact"
    );
    assert!(
        taken_over.schema_by_name("landed").is_some(),
        "the new writer sees the landed commit, not a partial view of it"
    );

    second
        .commit(|tx| tx.create_schema("after_takeover").map(|_| ()))
        .await
        .unwrap();
    let advanced = second.snapshot().await.unwrap();
    assert_eq!(
        advanced.current_snapshot().id.get(),
        head_at_first.get() + 1,
        "the new writer advances the inherited head by one"
    );
    assert!(advanced.schema_by_name("landed").is_some());
    assert!(advanced.schema_by_name("after_takeover").is_some());
    second.close().await.unwrap();
}

/// `FencedWriterResumes` — a writer resumes against a log a peer moved past.
/// There is no commit-level fence: the multi-writer topology rebases the
/// stale commit onto the fresh head and lands it whole, so the resumed
/// writer joins the timeline rather than being turned away. The coherent
/// result is the whole point — every peer's commit present, head advanced
/// by the rebase, never a torn or half state.
#[tokio::test]
async fn a_resumed_writer_rebases_onto_the_moved_head() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());

    let writer_a = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    writer_a
        .commit(|tx| tx.create_schema("before").map(|_| ()))
        .await
        .unwrap();

    // A peer opens against the same store and advances the head under A.
    let writer_b = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    writer_b
        .commit(|tx| tx.create_schema("takeover").map(|_| ()))
        .await
        .unwrap();
    let head_after_takeover = writer_b.snapshot().await.unwrap().current_snapshot().id;

    // A resumes against a premise the peer already moved past. With no
    // commit-level fence, it rebases onto the fresh head and lands whole.
    let landed = writer_a
        .commit(|tx| tx.create_schema("resumed").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(
        landed.get(),
        head_after_takeover.get() + 1,
        "the resumed commit rebases onto the moved head, landing one past it"
    );

    // A fresh handle, no in-memory carryover, sees one coherent timeline.
    let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let snapshot = reopened.snapshot().await.unwrap();
    assert_eq!(
        snapshot.current_snapshot().id.get(),
        head_after_takeover.get() + 1,
        "head advanced by exactly the rebased commit, never a torn or double step"
    );
    assert!(
        snapshot.schema_by_name("before").is_some(),
        "the first writer's earlier commit survives"
    );
    assert!(
        snapshot.schema_by_name("takeover").is_some(),
        "the peer's commit survives the rebase"
    );
    assert!(
        snapshot.schema_by_name("resumed").is_some(),
        "the stale writer's commit rebased and landed, rather than being fenced away"
    );
    reopened.close().await.unwrap();
}

/// A unique index over the table's one column.
fn index_def() -> IndexDef {
    IndexDef {
        name: "by_a".into(),
        columns: vec![ColumnId::new(1)],
        unique: true,
    }
}

/// The index key for `value`.
fn key(value: u64) -> IndexKeyValue {
    IndexKeyValue::Int {
        value: i128::from(value),
        width: IntWidth::I64,
    }
}

/// The backfill entry pairing row `row_id` with key `row_id`.
fn entry(row_id: u64) -> IndexEntry {
    IndexEntry {
        row_id,
        values: vec![Some(key(row_id))],
    }
}

/// A catalog on `backing` holding one empty table, ready to index.
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn catalog_with_table(backing: &Arc<InMemory>) -> (Catalog, TableId) {
    let catalog = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "orders", &[col("a")])?;
            tx.register_data_file(table, datafile(7), &[])?;
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();
    (catalog, created.get().unwrap())
}

/// `StagedBuildInterrupted` — a staged index build runs as several
/// commits, so unlike a commit it *is* interruptible partway. What carries
/// it is the cursor persisted with each step: after a crash the index is
/// still building, serves no reads, and resumes from where it stopped
/// rather than restarting or double-counting the rows below the
/// watermark.
#[tokio::test]
async fn interrupted_index_build_resumes_from_its_persisted_cursor() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());
    let (catalog, table) = catalog_with_table(&backing).await;

    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            created.set(Some(tx.create_index_staged(table, &index_def())?));
            Ok(())
        })
        .await
        .unwrap();
    let index = created.get().unwrap();

    // One step lands durably, then the process dies mid-build.
    catalog
        .commit(move |tx| {
            tx.build_index_step(index, &[entry(0), entry(1), entry(2)], false)
                .map(|_| ())
        })
        .await
        .unwrap();
    drop(catalog);

    let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let building = reopened
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table)
        .remove(0);
    assert_eq!(
        building.state,
        IndexState::Building,
        "a half-built index must not present itself as ready"
    );
    assert_eq!(
        building.build_cursor,
        Some(2),
        "the cursor is durable, so the resume knows where to start"
    );

    // Building means unavailable, not empty: serving the rows it happens
    // to have would be a silently partial answer.
    let err = reopened
        .index_lookup(table, index, &[key(0)])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::IndexBuilding(_)), "got {err:?}");

    // Resume past the cursor and finish.
    reopened
        .commit(move |tx| {
            tx.build_index_step(index, &[entry(3), entry(4), entry(5), entry(6)], true)
                .map(|_| ())
        })
        .await
        .unwrap();

    let finished = reopened
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table)
        .remove(0);
    assert_eq!(finished.state, IndexState::Ready);
    for row_id in 0..7 {
        assert_eq!(
            reopened
                .index_lookup(table, index, &[key(row_id)])
                .await
                .unwrap()
                .len(),
            1,
            "row {row_id} is indexed exactly once across the crash"
        );
    }
    reopened.close().await.unwrap();
}

/// `ReclamationInterrupted` — reclaiming a dead index's entries runs one
/// batch per commit so a large sweep never holds the writer, which means a
/// crash can land between batches. The batches already committed stay
/// reclaimed, the rest survive to be found again, and a re-run converges
/// without re-reclaiming what is already gone.
#[tokio::test]
async fn interrupted_entry_reclamation_converges_on_re_run() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());
    let (catalog, table) = catalog_with_table(&backing).await;

    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            created.set(Some(tx.create_index_staged(table, &index_def())?));
            Ok(())
        })
        .await
        .unwrap();
    let index = created.get().unwrap();
    let entries: Vec<IndexEntry> = (0..7).map(entry).collect();
    catalog
        .commit(move |tx| tx.build_index_step(index, &entries, true).map(|_| ()))
        .await
        .unwrap();
    catalog
        .commit(move |tx| tx.drop_index(index))
        .await
        .unwrap();

    // One batch of the sweep lands, then the process dies.
    let first = catalog.reclaim_index_entries(index, 3).await.unwrap();
    assert_eq!(first, 3, "the interrupted pass reclaimed one batch");
    drop(catalog);

    let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let resumed = reopened
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(
        resumed.index_entries_reclaimed, 4,
        "the re-run finishes the remaining entries, never re-reclaiming the first batch"
    );
    assert_eq!(resumed.indexes_swept, 1);

    // Converged: nothing is left to do, and a further pass is a no-op.
    let again = reopened
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(again.index_entries_reclaimed, 0);
    assert_eq!(again.indexes_swept, 0);

    // The sweep touched only the dead index's entries.
    let snapshot = reopened.snapshot().await.unwrap();
    assert_eq!(snapshot.data_files_of(table).len(), 1);
    assert!(snapshot.table_by_id(table).is_some());
    reopened.close().await.unwrap();
}

/// How many schema-scoped option records the migration case plants. The
/// rewriting unit moves one per batch, so more than one record means the
/// crash lands with some moved and some not.
const PLANTED_OPTIONS: u64 = 3;

/// A catalog carrying [`PLANTED_OPTIONS`] schemas, each with a
/// schema-scoped `planted` option naming its own id, closed so a migrator
/// can take the writer.
#[allow(clippy::unwrap_used)]
async fn catalog_with_planted_options(backing: &Arc<InMemory>) -> Vec<SchemaId> {
    let catalog = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let created = std::cell::RefCell::new(Vec::new());
    catalog
        .commit(|tx| {
            for index in 1..=PLANTED_OPTIONS {
                let schema = tx.create_schema(&format!("s{index}"))?;
                tx.set_option(OptionScope::Schema(schema), "planted", &schema.to_string())?;
                created.borrow_mut().push(schema);
            }
            Ok(())
        })
        .await
        .unwrap();
    catalog.close().await.unwrap();
    // After the content, so a read-write attach cannot upgrade it back: the
    // synthetic migration needs a format gap to act on.
    moraine::stamp_base_format(backing.clone()).await;
    created.into_inner()
}

/// `MigrationInterrupted` — a structural format migration runs as a start
/// batch planting the marker, one batch per bounded piece of the rewrite,
/// and a finish batch flipping the format and clearing the marker together.
/// A crash can land at any of those boundaries.
///
/// The one case crashed from *inside* a library call: the boundaries are
/// internal to `Catalog::migrate`, so [`CrashPoint`] is the only way in.
/// The unit is installed rather than shipped — every format to date is
/// additive, so the registry is empty and no store in the world needs a
/// rewrite — but it runs through the shipped planner, and the verb driving
/// it is the public one.
///
/// What must hold, at every seam: while the marker is down no attach may
/// open the store at all, and a re-run resumes from what is durable and
/// moves every record exactly once.
#[tokio::test]
async fn interrupted_migration_refuses_readers_and_resumes_exactly_once() {
    for point in CrashCase::MigrationInterrupted.seams() {
        let backing: Arc<InMemory> = Arc::new(InMemory::new());
        let schemas = catalog_with_planted_options(&backing).await;

        install_migration(SyntheticMigration::MoveOptionScope);
        inject_crash(Some(*point));
        let crashed = Catalog::migrate(
            backing.clone(),
            CatalogOptions::default(),
            MigrationRequest::default(),
        )
        .await;

        // Every seam stops the call. `AfterFinish` stops one that had in
        // fact completed: the finish batch was already durable, so no
        // marker is left and the store is coherently new — the caller just
        // never heard so.
        assert!(crashed.is_err(), "{point:?}");
        let completed = *point == CrashPoint::AfterFinish;

        if !completed {
            // No reader sees the half-rewritten middle. The marker is down,
            // so the attach refuses rather than serving a keyspace in motion.
            let refused = Catalog::open(backing.clone(), CatalogOptions::default())
                .await
                .err()
                .unwrap_or_else(|| panic!("an attach mid-migration must refuse at {point:?}"));
            assert!(matches!(refused, Error::Migration(_)), "{point:?}");
        }

        inject_crash(None);
        let report = Catalog::migrate(
            backing.clone(),
            CatalogOptions::default(),
            MigrationRequest::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("resume after {point:?}: {error:?}"));
        assert_eq!(report.resumed, !completed, "{point:?}");
        if completed {
            // Nothing left to do: the format already names the target.
            assert_eq!(report.from_format, report.to_format, "{point:?}");
            assert!(report.units_run.is_empty(), "{point:?}");
        } else {
            assert!(report.to_format > report.from_format, "{point:?}");
            assert_eq!(report.units_run, vec!["move-option-scope"], "{point:?}");
        }

        // Coherent, and moved exactly once: every planted record now reads
        // back at the scope the rewrite moved it to, and none is left behind
        // at the one it came from.
        let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
            .await
            .unwrap_or_else(|error| panic!("attach after {point:?}: {error:?}"));
        let snapshot = reopened.snapshot().await.unwrap();
        for schema in &schemas {
            assert_eq!(
                snapshot.option(OptionScope::Table(TableId::new(schema.get())), "planted"),
                Some(schema.to_string()),
                "{point:?}"
            );
            assert_eq!(
                snapshot.option(OptionScope::Schema(*schema), "planted"),
                None,
                "{point:?}"
            );
        }
        assert_eq!(
            snapshot.schemas().len(),
            usize::try_from(PLANTED_OPTIONS).unwrap() + 1,
            "the rewrite touched options, not schemas, at {point:?}"
        );
        reopened.close().await.unwrap();
    }

    // The registry is thread-local; leave the thread as it was found, since
    // a single-threaded run reuses it for the next case.
    install_migration(SyntheticMigration::None);
}

/// The reader-side half of the migration gate: a marker planted by another
/// process *after* a read-only handle attached. The gate itself is shared —
/// every read opens its session through one place, which refuses — and the
/// read-write side is covered by the case above; what this stages is a
/// reader that was already live and healthy when the keyspace started
/// moving.
///
/// "Another process" here is another writer handle: the marker is durable
/// object-storage state, so a second handle over the same store is
/// indistinguishable from a second process to the reader polling it.
#[tokio::test]
async fn a_live_reader_refuses_once_another_writer_plants_a_marker() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());
    let schemas = catalog_with_planted_options(&backing).await;

    let mut options = CatalogOptions::default();
    options.reader_poll_interval = std::time::Duration::from_millis(5);
    let reader = Catalog::open_read_only(backing.clone(), options)
        .await
        .unwrap();
    assert_eq!(
        reader.snapshot().await.unwrap().schemas().len(),
        schemas.len() + 1,
        "the reader is healthy before the keyspace starts moving"
    );

    // A second handle starts a migration and dies at its first seam, leaving
    // the marker down and the store stamped old.
    install_migration(SyntheticMigration::MoveOptionScope);
    inject_crash(Some(CrashPoint::AfterStart));
    Catalog::migrate(
        backing.clone(),
        CatalogOptions::default(),
        MigrationRequest::default(),
    )
    .await
    .expect_err("the seam stops the migration with its marker durable");
    inject_crash(None);
    install_migration(SyntheticMigration::None);

    // The live reader polls, meets the marker, and refuses — the typed
    // error, not a stale view and not a partial one.
    let refused = poll_until_refused(&reader).await;
    assert!(matches!(refused, Error::Migration(_)), "{refused:?}");
    reader.close().await.unwrap();
}

/// Reads until one refuses, or panics after long enough that the reader was
/// plainly never going to notice. A read-only catalog polls object storage
/// on its own cadence, so the marker becomes visible a poll after it lands.
#[allow(clippy::unwrap_used)]
async fn poll_until_refused(reader: &moraine::ReadOnlyCatalog) -> Error {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match reader.snapshot().await {
            Err(error) => return error,
            Ok(_) => assert!(
                std::time::Instant::now() < deadline,
                "the reader never noticed the marker"
            ),
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Asserts an operation stopped short of success once the store froze. It
/// may hang retrying — a dying process never learns its outcome either —
/// or surface the write failure; both are a crash. Only success is
/// disqualifying, because it would mean the batch became durable after
/// all and the case tested nothing.
#[allow(clippy::panic)]
fn assert_never_landed<T: std::fmt::Debug>(
    outcome: Result<Result<T, Error>, tokio::time::error::Elapsed>,
) {
    if let Ok(Ok(value)) = outcome {
        panic!("operation reported success ({value:?}) though its writes could not land");
    }
}

/// How long to let a doomed operation run before calling it a crash.
const UNTIL_CRASH: std::time::Duration = std::time::Duration::from_millis(300);

/// `CommitNotDurable` — the batch was staged but its WAL flush never
/// reached object storage. All-or-none resolves to none: the head does not
/// move, and a handle opened afterwards sees no trace of the commit.
#[tokio::test]
async fn commit_whose_wal_never_landed_is_invisible() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());
    let store = Arc::new(FreezingStore::thawed(backing.clone()));

    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("durable").map(|_| ()))
        .await
        .unwrap();
    let head_before = catalog.snapshot().await.unwrap().current_snapshot().id;

    // The process dies: the batch can still be staged in memory, but
    // nothing it writes will ever reach object storage.
    store.freeze_after(0);
    assert_never_landed(
        tokio::time::timeout(
            UNTIL_CRASH,
            catalog.commit(|tx| tx.create_schema("lost").map(|_| ())),
        )
        .await,
    );
    drop(catalog);

    // A fresh handle over the same bytes, with no freeze in the way.
    let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let recovered = reopened.snapshot().await.unwrap();
    assert_eq!(
        recovered.current_snapshot().id,
        head_before,
        "the head must not move for a commit that never became durable"
    );
    assert!(
        recovered.schema_by_name("lost").is_none(),
        "no record of the undurable commit may be visible"
    );
    assert!(
        recovered.schema_by_name("durable").is_some(),
        "the commit that did land is untouched"
    );
    reopened.close().await.unwrap();
}

/// Builds a table with three columns and two registered files, and
/// returns it with the number of writes the store has served so far.
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn table_to_drop(catalog: &Catalog) -> TableId {
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "orders", &[col("a"), col("b"), col("c")])?;
            tx.register_data_file(table, datafile(10), &[])?;
            tx.register_data_file(table, datafile(20), &[])?;
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();
    created.get().unwrap()
}

/// `MultiTombstoneDrop` — a drop ends the table row, all three columns and
/// both files at once. Its job is to prove there is no reachable point
/// "after the first tombstone, before the last": the whole drop is one
/// batch, so stopping the store at *every* write the drop issues must
/// still leave the table wholly present or wholly gone. It fails if a drop
/// is ever split across batches.
#[tokio::test]
async fn multi_tombstone_drop_is_never_observed_half_done() {
    // How many writes the drop issues, measured on an unfrozen run: the
    // boundaries the sweep below has to cover.
    let probe_backing: Arc<InMemory> = Arc::new(InMemory::new());
    let probe_store = Arc::new(FreezingStore::thawed(probe_backing.clone()));
    let probe = Catalog::open(probe_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let table = table_to_drop(&probe).await;
    let before_drop = probe_store.writes_attempted();
    probe.commit(move |tx| tx.drop_table(table)).await.unwrap();
    let boundaries = probe_store.writes_attempted() - before_drop;
    probe.close().await.unwrap();
    assert!(
        boundaries > 0,
        "the drop must write something to be worth stopping"
    );

    let mut interrupted = 0;
    for stop_after in 0..=boundaries {
        let backing: Arc<InMemory> = Arc::new(InMemory::new());
        let store = Arc::new(FreezingStore::thawed(backing.clone()));
        let catalog = Catalog::open(store.clone(), CatalogOptions::default())
            .await
            .unwrap();
        let table = table_to_drop(&catalog).await;

        // Die partway through the drop, one write later each round. Once
        // the allowance covers every write the drop makes, it commits
        // normally — that is the end of the sweep, not a failure.
        store.freeze_after(i64::try_from(stop_after).unwrap());
        let outcome =
            tokio::time::timeout(UNTIL_CRASH, catalog.commit(move |tx| tx.drop_table(table))).await;
        if !matches!(outcome, Ok(Ok(_))) {
            interrupted += 1;
        }
        drop(catalog);

        let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
            .await
            .unwrap();
        let recovered = reopened.snapshot().await.unwrap();
        if recovered.table_by_id(table).is_some() {
            assert_eq!(
                recovered.columns_of(table).len(),
                3,
                "stopping after {stop_after} writes left a table missing columns"
            );
            assert_eq!(
                recovered.data_files_of(table).len(),
                2,
                "stopping after {stop_after} writes left a table missing files"
            );
        } else {
            assert!(
                recovered.columns_of(table).is_empty(),
                "stopping after {stop_after} writes dropped a table but kept its columns"
            );
            assert!(
                recovered.data_files_of(table).is_empty(),
                "stopping after {stop_after} writes dropped a table but kept its files"
            );
        }
        reopened.close().await.unwrap();
    }
    assert!(
        interrupted > 0,
        "no round actually stopped the drop, so the sweep proved nothing"
    );
}

/// `GenesisInterrupted` — an initializer dies partway through creating the
/// catalog. Genesis is one batch like any commit, so stopping the store at
/// every write it issues must leave the store either untouched (the next
/// open creates it from scratch) or wholly created. A `sys/format` with no
/// head would survive validation on the next open and then fail to
/// materialize, so this sweep is what forces genesis to stay one batch.
#[tokio::test]
async fn interrupted_genesis_is_never_observed_half_created() {
    let probe_backing: Arc<InMemory> = Arc::new(InMemory::new());
    let probe_store = Arc::new(FreezingStore::thawed(probe_backing.clone()));
    Catalog::open(probe_store.clone(), CatalogOptions::default())
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let boundaries = probe_store.writes_attempted();
    assert!(boundaries > 0, "genesis must write something");

    let mut interrupted = 0;
    for stop_after in 0..=boundaries {
        let backing: Arc<InMemory> = Arc::new(InMemory::new());
        let store = Arc::new(FreezingStore::thawed(backing.clone()));
        store.freeze_after(i64::try_from(stop_after).unwrap());

        // The initializer dies partway: it may fail outright, never
        // return, or — once the allowance covers all of genesis — finish.
        let outcome = tokio::time::timeout(
            UNTIL_CRASH,
            Catalog::open(store.clone(), CatalogOptions::default()),
        )
        .await;
        if !matches!(outcome, Ok(Ok(_))) {
            interrupted += 1;
        }
        drop(outcome);

        // Whatever it left, the next open must produce a coherent catalog
        // — either by finding one or by creating it now.
        let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "stopping genesis after {stop_after} writes left an unopenable store: {err:?}"
                )
            });
        let snapshot = reopened.snapshot().await.unwrap_or_else(|err| {
            panic!(
                "stopping genesis after {stop_after} writes left a half-created catalog: {err:?}"
            )
        });
        assert_eq!(snapshot.current_snapshot().id.get(), 0);
        let schemas = snapshot.schemas();
        assert_eq!(schemas.len(), 1, "stopping after {stop_after} writes");
        assert_eq!(schemas[0].name, "main");
        reopened.close().await.unwrap();
    }

    assert!(
        interrupted > 0,
        "no round actually stopped genesis, so the sweep proved nothing"
    );
}

// removed: group commit was superseded by the commit coalescer
