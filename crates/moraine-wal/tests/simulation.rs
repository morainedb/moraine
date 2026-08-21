//! A deterministic, seed-reproducible simulation of the commit-slot protocol.
//! This is where the protocol's correctness is established: `tokio::join!`
//! races hit the happy interleaving nearly every time and cannot reproduce a
//! failure they do find, whereas every schedule here comes from a seed.
//!
//! Each oracle below is one test over a seed sweep. Every per-run oracle, the
//! long sweep included, narrows to a single seed from the one the assertion
//! prints:
//!
//! ```text
//! MORAINE_WAL_SEED=1234 cargo test -p moraine-wal --test simulation
//! ```
//!
//! Two claims are aggregate — the backoff bias and the sweep's own coverage —
//! and deliberately ignore that override, since a distributional property over
//! one seed is not the property.
//!
//! The long sweep is `#[ignore]`d, as `moraine`'s `object_storage.rs` suite is:
//!
//! ```text
//! cargo test -p moraine-wal --test simulation -- --ignored
//! ```

// The tests-exempt lints (`clippy.toml`) reach `#[test]` functions and
// `#[cfg(test)]` modules, not an integration crate's plain helper functions —
// exempted here instead, crate-wide, as tests.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

// `tests/simulation.rs` is this target's crate root, so its submodules would
// resolve against `tests/` without this.
#[path = "simulation/sim.rs"]
mod sim;

use std::collections::{BTreeMap, BTreeSet};

use moraine_wal::{CursorStore, Envelope, SlotLog, drive_fold};
use sim::{
    CoalescingRun, FaultCensus, FoldState, RoundOutcome, Run, Scenario, Work, duel, ids, simulate,
    simulate_coalescing, works,
};

/// Seeds the inline sweep covers, kept to CI-affordable time.
const SWEEP: u64 = 512;

/// Seeds the `#[ignore]`d sweep covers.
const LONG_SWEEP: u64 = 32_768;

/// Seeds the aggregate backoff-bias claim is measured over.
const DUEL_SWEEP: u64 = 128;

/// Seeds the schedule-diversity tripwire measures one fixed scenario over.
const DIVERSITY_SEEDS: u64 = 64;

/// Distinct interleavings those seeds must produce.
const DIVERSITY_FLOOR: usize = 16;

/// Slots a log must exceed for reading it to have crossed a page boundary,
/// given the largest `list_page` a scenario draws.
const MULTI_PAGE: usize = 3;

/// The environment override that pins a sweep to one seed.
const SEED_OVERRIDE: &str = "MORAINE_WAL_SEED";

/// Substrings a SQL-shaped retry loop keys its retry decision on. No error
/// this crate surfaces may carry one.
const RETRY_SUBSTRINGS: [&str; 4] = ["conflict", "concurrent", "unique", "primary key"];

/// The seeds a per-run oracle visits: the whole range, or the single seed
/// [`SEED_OVERRIDE`] names, so a failure reproduces on its own.
fn seeds(width: u64) -> Vec<u64> {
    match std::env::var(SEED_OVERRIDE) {
        Ok(text) => vec![text.trim().parse().expect("MORAINE_WAL_SEED must be a u64")],
        Err(_) => (0..width).collect(),
    }
}

/// The seeds an aggregate claim visits, which [`SEED_OVERRIDE`] cannot narrow:
/// a distributional property over one seed is not the property, and asserting
/// it per seed is exactly the flakiness a seed sweep exists to remove.
fn aggregate_seeds(width: u64) -> std::ops::Range<u64> {
    0..width
}

/// The seeds the long sweep visits, narrowed by [`SEED_OVERRIDE`] like any
/// per-run oracle: a failure it reports must reproduce on its own.
fn long_seeds() -> Vec<u64> {
    seeds(LONG_SWEEP)
}

/// Runs `check` over every seed a per-run oracle visits.
async fn sweep<Check>(mut check: Check)
where
    Check: FnMut(&Run),
{
    for seed in seeds(SWEEP) {
        check(&simulate(Scenario::for_seed(seed)).await);
    }
}

/// Runs `check` over the whole sweep, override or not.
async fn whole_sweep<Check>(mut check: Check)
where
    Check: FnMut(&Run),
{
    for seed in aggregate_seeds(SWEEP) {
        check(&simulate(Scenario::for_seed(seed)).await);
    }
}

/// The determinism self-check, and the harness's own first test: one seed run
/// twice must produce byte-identical transcripts of every store operation
/// (kind, sequence, latency, fault drawn) and every task outcome.
///
/// Three disciplines make this hold, each a silent failure if missed: every
/// draw comes from an explicitly seeded generator (never `fastrand`'s
/// thread-local global, and never `RetryPolicy::default`, whose jitter is
/// seeded from entropy and which struct-update syntax evaluates in full);
/// nothing iterates a `HashMap`, whose `RandomState` is per-process; and the
/// toy committers mint ids from counters, never a clock or a v4 UUID.
///
/// If this fails for a cause outside those three, or if the crate gains a
/// dependency with internal concurrency, adopt a simulation runtime. That is
/// the decision rule, not a judgment call.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn one_seed_replays_byte_identically() {
    for seed in seeds(SWEEP) {
        let first = simulate(Scenario::for_seed(seed)).await;
        let second = simulate(Scenario::for_seed(seed)).await;

        assert_eq!(
            first.trace.len(),
            second.trace.len(),
            "seed {seed}: the two runs served different numbers of operations"
        );
        for (line, (left, right)) in first.trace.iter().zip(&second.trace).enumerate() {
            assert_eq!(left, right, "seed {seed}: traces diverge at line {line}");
        }
        assert_eq!(
            first.slots.len(),
            second.slots.len(),
            "seed {seed}: the two runs left different logs"
        );
        for ((sequence, left), (_, right)) in first.slots.iter().zip(&second.slots) {
            assert_eq!(left, right, "seed {seed}: slot {sequence} differs");
        }
    }
}

/// The determinism self-check, across processes, against a pinned digest.
/// `RandomState` is per-process, so a `HashMap` iterated anywhere in the
/// harness or the crate would keep an in-process replay byte-identical while
/// diverging between runs — which only a value fixed outside any one process
/// can catch.
///
/// The digest covers every store operation and task outcome of the whole
/// sweep, so it also gives the sweep's own churn a visible signal: any edit to
/// `Scenario::for_seed` renumbers every scenario, and this constant is where
/// that shows up. Update it deliberately, never to make a test pass.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_sweep_replays_identically_between_processes() {
    let mut digest = FNV_OFFSET;
    let mut lines = 0_u64;
    whole_sweep(|run| {
        for line in &run.trace {
            digest = fnv1a(digest, line.as_bytes());
            lines += 1;
        }
    })
    .await;

    assert_eq!(
        (digest, lines),
        (SWEEP_DIGEST, SWEEP_TRACE_LINES),
        "the sweep no longer replays the transcript this digest was taken from"
    );
}

/// The transcript digest of the whole inline sweep, pinned so determinism is
/// checked between processes and not only within one.
const SWEEP_DIGEST: u64 = 0x176B_258E_9028_0FAB;

/// Trace lines that digest covers.
const SWEEP_TRACE_LINES: u64 = 37_357;

/// FNV-1a's offset basis.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a over `bytes`, continuing from `digest`.
fn fnv1a(digest: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(digest, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Every oracle below rests on some fault or outcome being reachable, so the
/// sweep's own coverage is asserted rather than assumed. An edit to
/// `Scenario::for_seed` or to the fault draw that removed a class would
/// otherwise leave the oracle that needs it silently vacuous while every test
/// stayed green.
///
/// The load-bearing count is `ambiguous_landing_then_failed_read`: a put that
/// applied its object and then reported failure, whose read-back *also*
/// failed. That conjunction is the precondition of the one defect the
/// work-level exactly-once oracle exists to catch — neither half alone reaches
/// it, because a read-back that answers settles the attempt by transaction id.
/// A count of unknown outcomes cannot stand in for it: those are reachable from
/// a put that never landed, from a premature refusal, or from a failed
/// listing.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_sweep_reaches_every_fault_and_outcome_the_oracles_need() {
    let mut census = FaultCensus::default();
    let mut committed = 0;
    let mut conflicts = 0;
    let mut exhausted = 0;
    let mut unknown = 0;
    let mut nothing = 0;
    let mut unstarted = 0;
    let mut failed = 0;
    let mut multi_slot_runs = 0;
    let mut folded = 0;

    whole_sweep(|run| {
        census.merge(&run.census);
        for record in &run.rounds {
            match &record.outcome {
                RoundOutcome::Committed { .. } => committed += 1,
                RoundOutcome::Conflict { .. } => conflicts += 1,
                RoundOutcome::Exhausted => exhausted += 1,
                RoundOutcome::Unavailable { .. } => unknown += 1,
                RoundOutcome::Nothing => nothing += 1,
                RoundOutcome::Unstarted(_) => unstarted += 1,
                RoundOutcome::Failed(_) => failed += 1,
            }
        }
        if run.slots.len() > 2 {
            multi_slot_runs += 1;
        }
        folded += run
            .folds
            .iter()
            .filter(|fold| fold.state.cursor > 0)
            .count();
    })
    .await;

    assert!(
        census.ambiguous_landing_then_failed_read > 5,
        "a put that landed under a failure and then read back badly happened {} \
         times; the work-level exactly-once oracle is vacuous below this: {census:?}",
        census.ambiguous_landing_then_failed_read
    );
    assert!(census.fail_after > 20, "{census:?}");
    assert!(census.fail_before > 20, "{census:?}");
    assert!(census.landed_then_already_exists > 20, "{census:?}");
    // Offered only while a competing create is in flight, so this is the one
    // fault whose reachability depends on the schedule rather than the rates.
    assert!(census.premature_already_exists > 0, "{census:?}");
    assert!(census.get_faults > 50, "{census:?}");
    assert!(census.list_faults > 20, "{census:?}");
    assert!(census.extra_list_pages > 50, "{census:?}");

    assert!(committed > 100, "committed rounds: {committed}");
    assert!(conflicts > 5, "conflicting rounds: {conflicts}");
    assert!(exhausted > 0, "exhausted rounds: {exhausted}");
    assert!(unknown > 5, "rounds with an unknown outcome: {unknown}");
    assert!(nothing > 0, "rounds with nothing to commit: {nothing}");
    assert!(unstarted > 0, "rounds that never read a head: {unstarted}");
    // A driver error needs `Corruption`, which this model cannot produce: the
    // store writes atomically, and a single-commit envelope cannot land in
    // part. The unit tests own that path, so a nonzero count here means the
    // model changed under this claim.
    assert_eq!(failed, 0, "rounds the driver failed: {failed}");
    assert!(
        multi_slot_runs > 20,
        "runs past two slots: {multi_slot_runs}"
    );
    assert!(folded > 20, "fold tasks that applied a slot: {folded}");
}

/// The tripwire on the harness's own schedule. Randomising per-operation
/// latency randomises the interleaving *only above the timer's resolution*:
/// tokio's wheel has millisecond granularity, so every sub-millisecond sleep
/// expires on the same tick and the schedule collapses to task poll order. A
/// regression to microsecond or zero latencies would reduce the whole sweep to
/// one interleaving while every other test stayed green, because they key on
/// outcome classes that the fault draws alone can supply.
///
/// So it is asserted: one fixed scenario shape — no faults, no folders, backoff
/// pinned to zero so the jitter streams contribute nothing — run over many
/// store seeds. The only seed-dependent input left is latency, and the recorded
/// signature is which worker won each sequence. If latency stops varying the
/// schedule, the distinct signatures collapse toward one.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn latency_draws_actually_vary_the_schedule() {
    let mut signatures = BTreeSet::new();
    for seed in aggregate_seeds(DIVERSITY_SEEDS) {
        let run = simulate(Scenario::fixed_shape(seed)).await;
        assert_eq!(
            run.slots.len(),
            run.planned_rounds,
            "seed {seed}: the fixed shape must commit every round"
        );
        signatures.insert(run.win_order());
    }

    assert!(
        signatures.len() >= DIVERSITY_FLOOR,
        "{} distinct interleavings over {DIVERSITY_SEEDS} seeds of one fixed \
         scenario; latency has stopped randomising the schedule",
        signatures.len()
    );
}

/// One winner per sequence: no sequence holds two envelopes, no two rounds
/// claim the same one, and every reader of a sequence sees identical bytes.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn every_sequence_has_exactly_one_winner() {
    sweep(|run| {
        let mut winners: BTreeMap<u64, &Envelope> = BTreeMap::new();
        for (sequence, envelope) in &run.slots {
            assert!(
                winners.insert(*sequence, envelope).is_none(),
                "seed {}: sequence {sequence} listed twice",
                run.seed
            );
        }

        let mut claimed: BTreeMap<u64, Work> = BTreeMap::new();
        for record in &run.rounds {
            if let RoundOutcome::Committed { sequence } = record.outcome {
                assert!(
                    claimed.insert(sequence, record.work).is_none(),
                    "seed {}: sequence {sequence} was reported committed twice",
                    run.seed
                );
            }
        }
    })
    .await;

    // Re-reading each slot must yield the same bytes, which also validates
    // the simulator's own store.
    for seed in seeds(SWEEP) {
        let run = simulate(Scenario::for_seed(seed)).await;
        for (sequence, envelope) in &run.slots {
            let reread = run
                .log
                .read_slot(*sequence)
                .await
                .expect("a quiesced read")
                .expect("a listed slot is present");
            assert_eq!(&reread, envelope, "seed {seed}: slot {sequence} re-read");
        }
    }
}

/// The readers that matter are the fold tasks: they are the only ones that read
/// the log *while it grows*, through the deliberately paginated LIST and a GET
/// per slot. What each applied must be a prefix of the log's final work order,
/// with its cursor standing at that prefix's length.
///
/// This is what makes pagination oracle-visible. The quiesced read-back every
/// other oracle sees is a single page, so a page-boundary bug that dropped one
/// object per page would shorten only the in-run reads: a worker would start
/// lower, lose a race, and leave a log that is still dense with no hole to
/// report.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_folder_reading_a_growing_log_always_sees_a_prefix_of_it() {
    let mut complete = 0;

    sweep(|run| {
        let truth = run.work_order();
        for (index, fold) in run.folds.iter().enumerate() {
            let applied = &fold.state.applied;
            assert!(
                applied.len() <= truth.len(),
                "seed {}: fold {index} applied {} slots of {}",
                run.seed,
                applied.len(),
                truth.len()
            );
            assert_eq!(
                applied.as_slice(),
                &truth[..applied.len()],
                "seed {}: fold {index} applied {applied:?}, which is not a prefix of {truth:?}",
                run.seed
            );
            assert_eq!(
                fold.state.cursor,
                applied.len() as u64,
                "seed {}: fold {index} advanced its cursor past what it applied",
                run.seed
            );
            if applied.len() == truth.len() && truth.len() > MULTI_PAGE {
                complete += 1;
            }
        }
    })
    .await;

    // A prefix claim alone would pass a listing that silently stopped at a page
    // boundary, since a short read is a legitimate prefix. Some folder must
    // therefore reach the end of a log longer than one page can hold.
    assert!(
        complete > 0,
        "no fold task ever consumed a log past {MULTI_PAGE} slots, so a listing \
         that truncated at a page boundary would go unnoticed"
    );
}

/// Exactly-once, stated over the committer's *work* and not only its ids.
///
/// The id-level half is the weaker claim: these committers mint a fresh id per
/// attempt, so a round that commits the same work twice under two ids passes
/// it. The work-level half is what catches that — a round's logical unit may
/// appear in at most one slot, ever, including across the ambiguous-put
/// retries that exist to break it.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn no_work_and_no_transaction_id_is_committed_twice() {
    sweep(|run| {
        let mut work_slots: BTreeMap<Work, u64> = BTreeMap::new();
        let mut id_slots: BTreeMap<[u8; 16], u64> = BTreeMap::new();

        for (sequence, envelope) in &run.slots {
            for work in works(envelope) {
                if let Some(earlier) = work_slots.insert(work, *sequence) {
                    panic!(
                        "seed {}: work {work} is committed at both {earlier} and \
                         {sequence}; the round applied the same work twice",
                        run.seed
                    );
                }
            }
            for id in ids(envelope) {
                assert!(
                    id_slots.insert(id, *sequence).is_none(),
                    "seed {}: transaction id {id:?} appears in two slots",
                    run.seed
                );
            }
        }
    })
    .await;
}

/// No false outcome: a reported commit is a real one, a reported non-commit
/// left no trace, and whatever a round is told, `find_transaction` gives the
/// true answer — never "unknown". This is the exactly-once client contract.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn every_outcome_matches_the_log() {
    for seed in seeds(SWEEP) {
        let run = simulate(Scenario::for_seed(seed)).await;
        for record in &run.rounds {
            let landed = run.sequences_of(record.work);
            assert!(
                landed.len() <= 1,
                "seed {seed}: work {} landed in {landed:?}",
                record.work
            );

            match &record.outcome {
                RoundOutcome::Committed { sequence } => {
                    assert_eq!(
                        landed,
                        vec![*sequence],
                        "seed {seed}: work {} was told it holds {sequence}",
                        record.work
                    );
                    let envelope = envelope_at(&run, *sequence);
                    assert!(
                        record
                            .minted
                            .iter()
                            .any(|id| envelope.contains_transaction(*id)),
                        "seed {seed}: slot {sequence} carries none of the round's ids"
                    );
                }
                // A conflict stops the round without committing: an envelope
                // the round itself landed is resolved by transaction id
                // before the embedder is ever asked to judge it.
                RoundOutcome::Conflict { sequence } => {
                    assert!(
                        landed.is_empty(),
                        "seed {seed}: work {} was told slot {sequence} conflicts, \
                         yet it is committed at {landed:?}",
                        record.work
                    );
                }
                // A spent budget of lost races settles the same way: every
                // sequence the round left behind held another committer.
                RoundOutcome::Exhausted | RoundOutcome::Nothing | RoundOutcome::Unstarted(_) => {
                    assert!(
                        landed.is_empty(),
                        "seed {seed}: work {} reported no commit, yet holds {landed:?}",
                        record.work
                    );
                }
                // The one honest unknown: the last put may have landed, and
                // the client resolves it from the log rather than guessing.
                RoundOutcome::Unavailable { .. } | RoundOutcome::Failed(_) => {}
            }

            assert_resolvable(&run.log, &run, record).await;
            assert_no_retry_substrings(seed, &record.outcome);
        }
    }
}

/// Every id a round offered resolves to the truth, so an unknown outcome is
/// always answerable.
async fn assert_resolvable(log: &SlotLog, run: &Run, record: &sim::RoundRecord) {
    for id in &record.minted {
        let found = log
            .find_transaction(1, *id)
            .await
            .expect("a quiesced scan of a healthy log answers");
        assert_eq!(
            found,
            run.sequence_of_id(*id),
            "seed {}: find_transaction disagreed with the log for work {}",
            run.seed,
            record.work
        );
    }

    if record.outcome.outcome_may_be_unknown() {
        let landed = run.sequences_of(record.work);
        if let Some(sequence) = landed.first() {
            assert!(
                record
                    .minted
                    .iter()
                    .any(|id| run.sequence_of_id(*id) == Some(*sequence)),
                "seed {}: work {} landed at {sequence} under no id the round holds, \
                 so its client could never discover it",
                run.seed,
                record.work
            );
        }
    }
}

/// A terminal error's text reaches an embedder's retry loop, so none of the
/// store text the fault model injects may survive the wrapping. `Unavailable`
/// is the case that matters: it is the common faulted terminal outcome, and its
/// last error is what an embedder renders into the message a SQL retry loop
/// greps.
fn assert_no_retry_substrings(seed: u64, outcome: &RoundOutcome) {
    let text = match outcome {
        RoundOutcome::Unavailable { last_error: text }
        | RoundOutcome::Failed(text)
        | RoundOutcome::Unstarted(text) => text.to_ascii_lowercase(),
        _ => return,
    };

    for substring in RETRY_SUBSTRINGS {
        assert!(
            !text.contains(substring),
            "seed {seed}: {text:?} carries {substring:?}"
        );
    }
}

/// Dense sequences: the protocol never skips a slot, so the written sequences
/// are `1..=k` and `Tail::gap_at` firing on a healthy run would be a protocol
/// bug rather than damage. The same claim covers the fold tasks, which read
/// the tail through a deliberately paginated LIST: a mishandled page boundary
/// surfaces as a short tail or a phantom hole.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn sequences_are_dense_and_no_reader_sees_a_hole() {
    sweep(|run| {
        let sequences: Vec<u64> = run.slots.iter().map(|(sequence, _)| *sequence).collect();
        let expected: Vec<u64> = (1..=sequences.len() as u64).collect();
        assert_eq!(sequences, expected, "seed {}: sparse log", run.seed);
        assert_eq!(run.gap_at, None, "seed {}: hole in the tail", run.seed);

        for (index, fold) in run.folds.iter().enumerate() {
            assert!(
                fold.corruptions.is_empty(),
                "seed {}: fold {index} reported {:?}",
                run.seed,
                fold.corruptions
            );
        }
    })
    .await;
}

/// Self-consistent logs: every slot satisfies the continuity rule replay
/// enforces one layer up. The toy analogue of `validated_head` is the model
/// head the payload was built against, which must be the head the slots below
/// it produce — producer and consumer agree by construction.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn every_log_satisfies_its_own_replay_rule() {
    sweep(|run| {
        let mut head = 0;
        for (sequence, envelope) in &run.slots {
            for commit in &envelope.commits {
                assert_eq!(
                    commit.payload.validated_head, head,
                    "seed {}: slot {sequence} was validated against {} but replay \
                     reaches it at {head}",
                    run.seed, commit.payload.validated_head
                );
            }
            head = *sequence;
        }
    })
    .await;
}

/// Driver-level fold invisibility: folding to *any* prefix and replaying the
/// rest yields the state pure replay from zero does, and re-folding an
/// already-folded prefix changes nothing.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn folding_to_any_prefix_is_invisible() {
    for seed in seeds(SWEEP) {
        let run = simulate(Scenario::for_seed(seed)).await;
        let replayed = replay(&run.log, 0).await;

        for prefix in 0..=run.slots.len() as u64 {
            let folded = replay(&run.log, prefix).await;
            assert_eq!(
                folded, replayed,
                "seed {seed}: folding through {prefix} then replaying diverged"
            );
        }

        let mut state = replay(&run.log, 0).await;
        let before = state.clone();
        drive_fold(&run.log, &mut state, u64::MAX)
            .await
            .expect("re-folding a folded log");
        assert_eq!(before, state, "seed {seed}: re-folding changed the state");
    }
}

/// The multi-commit envelope shape the leader-side coalescer produces, first
/// reachable in simulation here: a batch of members lands as one envelope in
/// one slot. Every member of a committed batch shares that slot, no member
/// lands twice, and a task told it did not commit left no member behind. A
/// coverage floor keeps the oracle from passing vacuously on runs that never
/// coalesced.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_coalesced_batch_lands_as_one_multi_commit_slot() {
    let mut multi_commit_slots = 0;
    let mut census = FaultCensus::default();

    for seed in seeds(SWEEP) {
        let run = simulate_coalescing(seed).await;
        assert_eq!(run.gap_at, None, "seed {seed}: a healthy log has no hole");
        census.merge(&run.census);

        let mut work_slots: BTreeMap<Work, u64> = BTreeMap::new();
        for (sequence, envelope) in &run.slots {
            if envelope.commits.len() > 1 {
                multi_commit_slots += 1;
            }
            for work in works(envelope) {
                assert!(
                    work_slots.insert(work, *sequence).is_none(),
                    "seed {seed}: work {work} committed in two slots"
                );
            }
        }

        for record in &run.records {
            match &record.outcome {
                RoundOutcome::Committed { sequence } => {
                    for work in &record.batch {
                        assert_eq!(
                            work_slots.get(work),
                            Some(sequence),
                            "seed {seed}: batched work {work} did not land in its \
                             reported slot {sequence}",
                        );
                    }
                    // The whole batch is resolvable by any of its ids — the
                    // exactly-once client contract over a multi-commit slot.
                    for id in &record.minted {
                        let found = run
                            .log
                            .find_transaction(1, *id)
                            .await
                            .expect("a quiesced scan answers");
                        assert!(
                            found.is_none() || found == Some(*sequence),
                            "seed {seed}: id {id:?} of a committed batch resolves to \
                             {found:?}, not its slot {sequence}"
                        );
                    }
                }
                // A terminal non-commit leaves no member behind; an unknown
                // outcome (the last put's fate) may or may not have landed, so
                // it is not held to this.
                RoundOutcome::Conflict { .. }
                | RoundOutcome::Exhausted
                | RoundOutcome::Nothing
                | RoundOutcome::Unstarted(_) => {
                    for work in &record.batch {
                        assert!(
                            !work_slots.contains_key(work),
                            "seed {seed}: work {work} reported no commit yet holds a slot"
                        );
                    }
                }
                RoundOutcome::Unavailable { .. } | RoundOutcome::Failed(_) => {}
            }
        }
    }

    assert!(
        multi_commit_slots > 0,
        "no coalescing run produced a multi-commit envelope, so the multi-commit \
         shape stayed unreached"
    );
    // The ambiguous-put mechanic — a landed put whose acknowledgement was lost
    // — must actually fire over these multi-commit envelopes, or the
    // exactly-once resolution above is proven only on the easy path. Skipped
    // when the sweep is pinned to one seed, which may draw a gentle run.
    if std::env::var(SEED_OVERRIDE).is_err() {
        assert!(
            census.fail_after + census.landed_then_already_exists > 0,
            "no coalescing run produced an ambiguous landing, so multi-commit \
             exactly-once resolution stayed on the easy path"
        );
    }
}

/// Fold invisibility over multi-commit envelopes: folding a coalesced log to
/// any prefix and replaying the rest yields what pure replay does. The
/// single-commit sweep proves this for one commit per slot; here each slot
/// carries a whole batch.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_coalesced_log_folds_as_it_replays() {
    for seed in seeds(SWEEP) {
        let run: CoalescingRun = simulate_coalescing(seed).await;
        let replayed = replay(&run.log, 0).await;

        for prefix in 0..=run.slots.len() as u64 {
            let folded = replay(&run.log, prefix).await;
            assert_eq!(
                folded, replayed,
                "seed {seed}: folding a coalesced log through {prefix} diverged"
            );
        }
    }
}

/// Folds `prefix` slots one round at a time, then replays the rest.
async fn replay(log: &SlotLog, prefix: u64) -> FoldState {
    let mut state = FoldState::default();
    if prefix > 0 {
        drive_fold(log, &mut state, prefix)
            .await
            .expect("folding a prefix of a healthy log");
        assert_eq!(state.cursor().await.expect("the cursor"), prefix);
    }
    drive_fold(log, &mut state, u64::MAX)
        .await
        .expect("replaying the rest of a healthy log");

    state
}

/// Termination: every task reaches a terminal outcome — no livelock — and
/// every slot traces back to exactly one round. Exhaustion under contention is
/// a legitimate outcome, not a failure.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn every_task_terminates_and_every_slot_has_an_owner() {
    sweep(|run| {
        assert_eq!(
            run.rounds.len(),
            run.planned_rounds,
            "seed {}: a committer task did not finish its rounds",
            run.seed
        );

        for (sequence, envelope) in &run.slots {
            for work in works(envelope) {
                let owner = run
                    .rounds
                    .iter()
                    .find(|record| record.work == work)
                    .unwrap_or_else(|| {
                        panic!("seed {}: slot {sequence} holds unowned work", run.seed)
                    });
                let reported = matches!(
                    owner.outcome,
                    RoundOutcome::Committed { sequence: at } if at == *sequence
                );
                assert!(
                    reported || owner.outcome.outcome_may_be_unknown(),
                    "seed {}: slot {sequence} holds work {work}, whose round reported {}",
                    run.seed,
                    owner.outcome.label()
                );
            }
        }
    })
    .await;
}

/// Without faults there is nothing left unresolved, so the identity is exact:
/// the slot count equals the number of committed outcomes.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_fault_free_run_commits_exactly_as_many_slots_as_it_reports() {
    for seed in seeds(SWEEP) {
        let run = simulate(Scenario::for_seed(seed).without_faults()).await;
        let committed = run
            .rounds
            .iter()
            .filter(|record| matches!(record.outcome, RoundOutcome::Committed { .. }))
            .count();

        assert_eq!(
            run.slots.len(),
            committed,
            "seed {seed}: {} slots for {committed} committed outcomes",
            run.slots.len()
        );
    }
}

/// Backoff bias: biased, never starved. Two committers drive the same number of
/// rounds over one log, one retrying with a near-zero base delay and one on the
/// standard backoff. The metric is **progress share**, not a head-to-head win:
/// the two sides' round `i` are not contemporaneous, because the hot side runs
/// ahead, so what is counted is whose round `i` reached the *lower* sequence —
/// which is throughput under contention, and is the property the design
/// actually needs (a sustained process gets the slots, which is what makes the
/// forwarding trigger recruit itself).
///
/// Both halves are required. A test proving only that the hot policy is ahead
/// would pass a design that starves slow clients, which the design forbids:
/// delayed, never starved. So the steady side must also take a real share of
/// the sequences, and every one of its rounds must commit — never exhausted,
/// never unavailable.
///
/// Asserted over the aggregate of every seed, never per seed: a single seed may
/// legitimately favour the steady side, and asserting per seed would be the
/// flakiness a distributional claim on a live scheduler always is.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_hot_retry_policy_is_biased_but_starves_no_one() {
    let mut hot_ahead = 0_u32;
    let mut steady_ahead = 0_u32;

    for seed in aggregate_seeds(DUEL_SWEEP) {
        for (index, round) in duel(seed).await.iter().enumerate() {
            let (Some(hot), Some(steady)) = (round.hot, round.steady) else {
                panic!("seed {seed} round {index}: a duel round did not commit: {round:?}");
            };
            if hot < steady {
                hot_ahead += 1;
            } else {
                steady_ahead += 1;
            }
        }
    }

    let rounds = hot_ahead + steady_ahead;
    assert!(rounds > 0, "the duel produced no rounds");
    assert!(
        u64::from(hot_ahead) * 5 > u64::from(rounds) * 3,
        "the hot policy led {hot_ahead} of {rounds} rounds, which is not decisively \
         more than half"
    );
    assert!(
        u64::from(steady_ahead) * 10 > u64::from(rounds),
        "the steady policy led {steady_ahead} of {rounds} rounds, which is \
         starvation, not delay"
    );
}

/// The long sweep, for an xtask target: the same oracles over many more seeds
/// than CI can afford inline.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "long sweep; run with --ignored"]
async fn the_long_sweep_holds_every_oracle() {
    for seed in long_seeds() {
        let run = simulate(Scenario::for_seed(seed)).await;
        assert_long_sweep_oracles(&run).await;
    }
}

/// Every oracle above, as one pass over one run.
async fn assert_long_sweep_oracles(run: &Run) {
    let seed = run.seed;
    let sequences: Vec<u64> = run.slots.iter().map(|(sequence, _)| *sequence).collect();
    assert_eq!(sequences, (1..=sequences.len() as u64).collect::<Vec<_>>());
    assert_eq!(run.gap_at, None, "seed {seed}: hole in the tail");
    assert_eq!(run.rounds.len(), run.planned_rounds);

    let mut work_slots: BTreeMap<Work, u64> = BTreeMap::new();
    let mut head = 0;
    for (sequence, envelope) in &run.slots {
        for work in works(envelope) {
            assert!(
                work_slots.insert(work, *sequence).is_none(),
                "seed {seed}: work {work} is committed twice"
            );
        }
        for commit in &envelope.commits {
            assert_eq!(commit.payload.validated_head, head, "seed {seed}");
        }
        head = *sequence;
    }

    for record in &run.rounds {
        let landed = run.sequences_of(record.work);
        assert!(landed.len() <= 1, "seed {seed}");
        match &record.outcome {
            RoundOutcome::Committed { sequence } => assert_eq!(landed, vec![*sequence]),
            RoundOutcome::Unavailable { .. } | RoundOutcome::Failed(_) => {}
            _ => assert!(landed.is_empty(), "seed {seed}: {}", record.outcome.label()),
        }
        assert_resolvable(&run.log, run, record).await;
        assert_no_retry_substrings(seed, &record.outcome);
    }

    let truth = run.work_order();
    for fold in &run.folds {
        assert!(fold.corruptions.is_empty(), "seed {seed}");
        let applied = &fold.state.applied;
        assert!(applied.len() <= truth.len(), "seed {seed}");
        assert_eq!(applied.as_slice(), &truth[..applied.len()], "seed {seed}");
        assert_eq!(fold.state.cursor, applied.len() as u64, "seed {seed}");
    }

    let replayed = replay(&run.log, 0).await;
    for prefix in 0..=run.slots.len() as u64 {
        assert_eq!(replay(&run.log, prefix).await, replayed, "seed {seed}");
    }
}

/// The determinism self-check over the long sweep.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "long sweep; run with --ignored"]
async fn the_long_sweep_replays_byte_identically() {
    for seed in long_seeds() {
        let first = simulate(Scenario::for_seed(seed)).await;
        let second = simulate(Scenario::for_seed(seed)).await;
        assert_eq!(first.trace, second.trace, "seed {seed}: traces diverge");
    }
}

/// The envelope at a sequence the log is asserted to hold.
fn envelope_at(run: &Run, sequence: u64) -> &Envelope {
    match run.slots.iter().find(|(at, _)| *at == sequence) {
        Some((_, envelope)) => envelope,
        None => panic!("seed {}: no slot at {sequence}", run.seed),
    }
}
