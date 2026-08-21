//! The protocol's loops: the retry/rebase commit round, the fold round, and
//! the folder self-appointment rule. Each is generic over the embedder's
//! semantics — what a commit is made of, what a lost race means, and what
//! applying a slot does — so the loops themselves stay pure protocol.
//!
//! The log below them is clock-free. These loops are not: they wait between
//! attempts, and they draw the jitter in those waits from an explicitly
//! seeded [`Jitter`], so a run reproduces from its seed.

// The drivers are generic over their embedder and never take `dyn`, so each
// call site's concrete futures carry their own auto traits.
#![allow(async_fn_in_trait)]

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use crate::{
    envelope::Envelope,
    error::Error,
    slot::{CommitOutcome, Resolution, SlotLog, resolve},
};

/// Attempts [`RetryPolicy::default`] allows one commit round.
const DEFAULT_MAX_ATTEMPTS: usize = 10;

/// [`RetryPolicy::default`]'s delay before a second attempt.
const DEFAULT_BASE_DELAY: Duration = Duration::from_millis(2);

/// [`RetryPolicy::default`]'s ceiling on one attempt's delay.
const DEFAULT_MAX_DELAY: Duration = Duration::from_millis(50);

/// The odd increment one draw advances a [`Jitter`] stream by.
const POSITION_STEP: u64 = 0x9E37_79B9_7F4A_7C15;

/// A source of backoff jitter over an explicit stream position, never a
/// thread-local generator: equal seeds draw equal sequences.
#[derive(Debug)]
pub struct Jitter {
    position: AtomicU64,
}

impl Jitter {
    /// A stream pinned to `seed`.
    #[must_use]
    pub fn seeded(seed: u64) -> Self {
        Self {
            position: AtomicU64::new(seed),
        }
    }

    /// A stream seeded from system entropy; its draws do not reproduce.
    #[must_use]
    pub fn from_entropy() -> Self {
        Self::seeded(fastrand::u64(..))
    }

    /// A uniform draw from `Duration::ZERO..=upper`.
    #[must_use]
    pub fn draw(&self, upper: Duration) -> Duration {
        let micros = u64::try_from(upper.as_micros()).unwrap_or(u64::MAX);
        if micros == 0 {
            return Duration::ZERO;
        }

        Duration::from_micros(self.rng().u64(0..=micros))
    }

    /// A generator over the next stream position, which it claims.
    fn rng(&self) -> fastrand::Rng {
        fastrand::Rng::with_seed(self.position.fetch_add(POSITION_STEP, Ordering::Relaxed))
    }
}

impl Clone for Jitter {
    /// Seeds the clone from a value drawn out of this stream, which advances
    /// it: the clone continues no part of the parent's sequence.
    fn clone(&self) -> Self {
        Self::seeded(self.rng().u64(..))
    }
}

/// Retry shape for the commit round: jittered exponential backoff under a
/// bounded attempt budget.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Attempts one round may make, the first included.
    pub max_attempts: usize,
    /// The delay before a second attempt; each further attempt doubles it.
    pub base_delay: Duration,
    /// The ceiling on one attempt's delay, before jitter.
    pub max_delay: Duration,
    /// Where the jitter added to each delay is drawn from.
    pub jitter: Jitter,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_delay: DEFAULT_BASE_DELAY,
            max_delay: DEFAULT_MAX_DELAY,
            jitter: Jitter::from_entropy(),
        }
    }
}

impl RetryPolicy {
    /// The default knobs with the jitter stream pinned to `seed`.
    #[must_use]
    pub fn seeded(seed: u64) -> Self {
        Self {
            jitter: Jitter::seeded(seed),
            ..Self::default()
        }
    }

    /// How long a round waits before `attempt`, counted from zero: the first
    /// attempt never waits, and every later one waits an exponential step
    /// capped at [`RetryPolicy::max_delay`] plus jitter of up to
    /// [`RetryPolicy::base_delay`].
    fn backoff(&self, attempt: usize) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        let doublings = u32::try_from(attempt - 1).unwrap_or(u32::MAX).min(31);
        let step = self
            .base_delay
            .saturating_mul(1_u32 << doublings)
            .min(self.max_delay);

        step.saturating_add(self.jitter.draw(self.base_delay))
    }
}

/// What a lost race means for the commit that lost it, judged by the
/// embedder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Race {
    /// The commit can be re-assembled against the winner and retried.
    Benign,
    /// The winner invalidates this commit; the round stops.
    Conflict,
}

/// One embedder's commit semantics. The round owns racing, backoff, the
/// attempt budget, and the sequence cursor; the embedder owns assembly,
/// judgment, and absorbing winners into its head.
pub trait Committer {
    /// The embedder's error type.
    type Error: From<Error>;

    /// Assembles against the current head; `None` means nothing to commit.
    ///
    /// Called once per attempt, so it must be safe to re-run: re-assembling
    /// against an absorbed winner is what rebasing amounts to, and it is
    /// where an embedder re-validates.
    ///
    /// # Errors
    ///
    /// Returns the embedder's error if the head could not be read or the
    /// commit could not be built.
    async fn assemble(&mut self) -> Result<Option<Envelope>, Self::Error>;

    /// Judges the winner of a lost race against the last assembly.
    ///
    /// The winner is another committer's envelope. One that a previous attempt
    /// of this same commit landed — which a re-raced sequence can turn up — is
    /// resolved by transaction id before this is called, so an implementation
    /// never has to recognize its own work here.
    fn classify(&self, winner: &Envelope) -> Race;

    /// Folds a winner into the head before the next attempt.
    ///
    /// # Errors
    ///
    /// Returns the embedder's error if the winner could not be applied to the
    /// head.
    fn absorb(&mut self, sequence: u64, winner: Envelope) -> Result<(), Self::Error>;
}

/// How one commit round ended, and what it cost. Mapping an outcome to an
/// embedder error — and to whatever text contract that error carries — is the
/// caller's job.
#[derive(Debug)]
pub enum CommitDrive {
    /// The commit holds this sequence.
    Committed {
        /// The sequence won.
        sequence: u64,
        /// Attempts made, the winning one included.
        attempts: usize,
        /// Races lost on the way, each one another committer's commit.
        races_lost: usize,
    },
    /// Assembly found nothing to commit.
    Nothing,
    /// A winner the embedder judged incompatible holds this sequence.
    Conflict {
        /// The sequence the winner holds.
        sequence: u64,
        /// The envelope that won it.
        winner: Envelope,
    },
    /// The budget went to lost races without settling.
    Exhausted {
        /// Attempts made.
        attempts: usize,
        /// The sequence the last attempt raced.
        last_sequence: u64,
    },
    /// The budget went to a log that never answered, nothing contended.
    Unavailable {
        /// Attempts made.
        attempts: usize,
        /// The sequence the last attempt raced.
        last_sequence: u64,
        /// The failure the last attempt reported.
        last_error: Error,
    },
}

/// Drives one commit from `start_sequence`, advancing the sequence itself as
/// races are lost — sequencing is log mechanics, not embedder state.
///
/// Each attempt assembles, races the sequence, and on a lost race asks the
/// embedder to judge the winner: a conflict stops the round with that winner,
/// a benign loss absorbs it, backs off, and races the next sequence. A log
/// failure is retried at the *same* sequence, which the loser of a contended
/// race can legitimately see — a slot reported taken while the winner's object
/// is not yet readable. Corruption is terminal and never retried.
///
/// A log failure can also leave a put's outcome unknown, so the round keeps
/// the transaction ids of every attempt it could not attribute and matches
/// them against later winners: a sequence that turns out to hold one of those
/// attempts is reported as committed, never handed to
/// [`Committer::classify`] as a rival's work.
///
/// # Errors
///
/// Returns the embedder's error if assembly or absorption failed, if the log
/// reported corruption, or if a winner holds only part of one attempt's
/// transaction ids — an id reached two committers. A spent budget is an
/// outcome, not an error: [`CommitDrive::Exhausted`] for races lost,
/// [`CommitDrive::Unavailable`] for a log that never answered.
pub async fn drive_commit<C: Committer>(
    log: &SlotLog,
    committer: &mut C,
    start_sequence: u64,
    retry: &RetryPolicy,
) -> Result<CommitDrive, C::Error> {
    let mut sequence = start_sequence;
    let mut last_sequence = start_sequence;
    let mut attempts = 0;
    let mut races_lost = 0;
    let mut last_error = None;
    // The ids of attempts at `sequence` whose put could not be attributed,
    // one set per attempt: at most one of them can hold the sequence.
    let mut unattributed: Vec<Vec<[u8; 16]>> = Vec::new();

    while attempts < retry.max_attempts {
        let backoff = retry.backoff(attempts);
        if !backoff.is_zero() {
            tokio::time::sleep(backoff).await;
        }

        let Some(envelope) = committer.assemble().await? else {
            return Ok(CommitDrive::Nothing);
        };
        attempts += 1;
        last_sequence = sequence;

        match log.commit_slot(sequence, &envelope).await {
            Ok(CommitOutcome::Won) => {
                return Ok(CommitDrive::Committed {
                    sequence,
                    attempts,
                    races_lost,
                });
            }
            Ok(CommitOutcome::Lost(winner)) => {
                for attempt_ids in &unattributed {
                    match resolve(&winner, attempt_ids.iter().copied()) {
                        Resolution::ThisAttempt => {
                            return Ok(CommitDrive::Committed {
                                sequence,
                                attempts,
                                races_lost,
                            });
                        }
                        Resolution::PartlyLanded => {
                            return Err(Error::corruption(format!(
                                "slot {sequence} carries some but not all of an earlier \
                                 attempt's transaction ids; an id reached more than one \
                                 committer"
                            ))
                            .into());
                        }
                        Resolution::NoIdentity | Resolution::OtherEnvelope => {}
                    }
                }

                races_lost += 1;
                last_error = None;
                match committer.classify(&winner) {
                    Race::Conflict => return Ok(CommitDrive::Conflict { sequence, winner }),
                    Race::Benign => {
                        committer.absorb(sequence, winner)?;
                        sequence = sequence.saturating_add(1);
                        // The winner at the sequence just left is another
                        // committer's, so no earlier attempt of this commit
                        // landed there.
                        unattributed.clear();
                    }
                }
            }
            // A log failure leaves the sequence to be raced again, and may
            // leave this attempt's put unattributed. Anything else is damage
            // the round cannot work through.
            Err(failure) if matches!(failure, Error::Transport(_)) => {
                unattributed.push(envelope.transaction_ids().collect());
                last_error = Some(failure);
            }
            Err(damage) => return Err(damage.into()),
        }
    }

    Ok(match last_error {
        Some(last_error) => CommitDrive::Unavailable {
            attempts,
            last_sequence,
            last_error,
        },
        None => CommitDrive::Exhausted {
            attempts,
            last_sequence,
        },
    })
}

/// Derived state that absorbs slots, resuming from a durable cursor.
///
/// The implementation owns the atomicity of applying a slot and advancing the
/// cursor: they are one unit, or a crash between them double-applies on
/// resume. That is this trait's central obligation.
pub trait CursorStore {
    /// The embedder's error type.
    type Error: From<Error>;

    /// The highest sequence already applied.
    ///
    /// # Errors
    ///
    /// Returns the embedder's error if the cursor could not be read.
    async fn cursor(&mut self) -> Result<u64, Self::Error>;

    /// Applies one slot and advances the cursor to `sequence`, atomically.
    ///
    /// # Errors
    ///
    /// Returns the embedder's error if the slot could not be applied.
    async fn apply(&mut self, sequence: u64, envelope: &Envelope) -> Result<(), Self::Error>;

    /// End-of-round durability barrier; may be a no-op.
    ///
    /// # Errors
    ///
    /// Returns the embedder's error if the applied slots could not be made
    /// durable.
    async fn finish(&mut self) -> Result<(), Self::Error>;
}

/// What one fold round applied, and what it left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FoldReport {
    /// Slots applied by this round.
    pub slots_folded: u64,
    /// The sequence the cursor now stands at.
    pub folded_through: u64,
    /// Unapplied slots still in the tail.
    pub tail_remaining: u64,
}

/// Applies up to `limit` unapplied slots from the tail, in order, resuming
/// from the store's cursor, then calls [`CursorStore::finish`].
///
/// `limit` bounds what a round applies, not what it reads: hole detection
/// needs the whole contiguous tail, so one round reads it all.
///
/// # Examples
///
/// ```
/// # use std::sync::Arc;
/// # use moraine_wal::{Commit, CursorStore, Envelope, Error, SlotLog, SlotPayload, SlotWrite, drive_fold};
/// # use object_store::memory::InMemory;
/// /// Applied keys in slot order, plus the cursor they were applied with.
/// #[derive(Default)]
/// struct Applied {
///     cursor: u64,
///     keys: Vec<Vec<u8>>,
/// }
///
/// impl CursorStore for Applied {
///     type Error = Error;
///
///     async fn cursor(&mut self) -> Result<u64, Error> {
///         Ok(self.cursor)
///     }
///
///     // One unit: a real store writes the keys and the cursor in one batch.
///     async fn apply(&mut self, sequence: u64, envelope: &Envelope) -> Result<(), Error> {
///         for commit in &envelope.commits {
///             self.keys
///                 .extend(commit.payload.writes.iter().map(|write| write.key.clone()));
///         }
///         self.cursor = sequence;
///         Ok(())
///     }
///
///     async fn finish(&mut self) -> Result<(), Error> {
///         Ok(())
///     }
/// }
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// let log = SlotLog::new(Arc::new(InMemory::new()), "catalog");
/// for (sequence, key) in [(1, b"orders".as_slice()), (2, b"regions")] {
///     let envelope = Envelope { leader: None,
///         commits: vec![Commit {
///             transaction_id: [sequence as u8; 16],
///             payload: SlotPayload {
///                 validated_head: sequence - 1,
///                 changes_made: "created_table".to_string(),
///                 writes: vec![SlotWrite { key: key.to_vec(), value: Some(b"1".to_vec()) }],
///             },
///         }],
///     };
///     log.put_slot(sequence, &envelope).await?;
/// }
///
/// // A bounded round leaves the rest of the tail for the next one.
/// let mut applied = Applied::default();
/// let first = drive_fold(&log, &mut applied, 1).await?;
/// assert_eq!((first.slots_folded, first.folded_through, first.tail_remaining), (1, 1, 1));
///
/// // Resuming from the cursor never re-applies what the first round did.
/// let rest = drive_fold(&log, &mut applied, u64::MAX).await?;
/// assert_eq!((rest.slots_folded, rest.folded_through, rest.tail_remaining), (1, 2, 0));
/// assert_eq!(applied.keys, vec![b"orders".to_vec(), b"regions".to_vec()]);
/// # Ok::<(), Error>(()) }).unwrap();
/// ```
///
/// # Errors
///
/// Returns the embedder's error if the cursor, a slot read, or an apply
/// failed, or [`Error::Corruption`] if the tail has a hole: a sequence absent
/// while higher ones exist was destroyed outside the protocol, and folding
/// the prefix would hide committed state. Nothing is applied in that case.
pub async fn drive_fold<S: CursorStore>(
    log: &SlotLog,
    store: &mut S,
    limit: u64,
) -> Result<FoldReport, S::Error> {
    let cursor = store.cursor().await?;
    let from = cursor.saturating_add(1);
    let tail = log.read_tail(from).await?;

    if let Some(gap) = tail.gap_at {
        return Err(Error::corruption(format!(
            "the tail from {from} has a hole at {gap}; a destroyed slot cannot be folded past"
        ))
        .into());
    }

    let mut folded_through = cursor;
    let mut slots_folded = 0;
    for (sequence, envelope) in &tail.slots {
        if slots_folded >= limit {
            break;
        }

        store.apply(*sequence, envelope).await?;
        folded_through = *sequence;
        slots_folded += 1;
    }
    store.finish().await?;

    let listed = u64::try_from(tail.slots.len()).unwrap_or(u64::MAX);

    Ok(FoldReport {
        slots_folded,
        folded_through,
        tail_remaining: listed.saturating_sub(slots_folded),
    })
}

/// Acquiring the role that may apply slots. What acquiring means is the
/// embedder's — it is whatever exclusion its derived state runs under, and it
/// may refuse — but *when* to acquire it is the protocol's.
pub trait FolderRole {
    /// The embedder's error type.
    type Error: From<Error>;
    /// The derived state an acquired role may write.
    type Session: CursorStore<Error = Self::Error>;

    /// The applied cursor, read **without** acquiring the role.
    ///
    /// # Errors
    ///
    /// Returns the embedder's error if the cursor could not be read.
    async fn peek_cursor(&self) -> Result<u64, Self::Error>;

    /// Acquires the role and yields a session.
    ///
    /// # Errors
    ///
    /// Returns the embedder's error if the role could not be acquired.
    async fn open(&self) -> Result<Self::Session, Self::Error>;
}

/// The self-appointment rule, which turns a stampede of would-be folders into
/// one opener: if the unapplied tail exceeds `threshold`, wait `delay` plus
/// jitter of up to `delay`, re-read the cursor, and stand down (`None`) if
/// another folder advanced it; otherwise acquire the role and drive one fold
/// bounded by `limit`.
///
/// A tail at or below `threshold` returns at once, paying no delay and
/// acquiring nothing. The policy values are the caller's; the logic is the
/// protocol's.
///
/// # Errors
///
/// Returns the embedder's error if the cursor, the tail listing, acquiring
/// the role, or the fold itself failed.
pub async fn drive_fold_if_stalled<R: FolderRole>(
    log: &SlotLog,
    role: &R,
    threshold: u64,
    delay: Duration,
    limit: u64,
    jitter: &Jitter,
) -> Result<Option<FoldReport>, R::Error> {
    let cursor = role.peek_cursor().await?;
    let unapplied = log.tail_length(cursor.saturating_add(1)).await?;
    if unapplied <= threshold {
        return Ok(None);
    }

    tokio::time::sleep(delay.saturating_add(jitter.draw(delay))).await;

    if role.peek_cursor().await? > cursor {
        return Ok(None);
    }

    let mut session = role.open().await?;
    let report = drive_fold(log, &mut session, limit).await?;

    Ok(Some(report))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, atomic::AtomicUsize};

    use object_store::{ObjectStoreExt, memory::InMemory};

    use super::*;
    use crate::{
        envelope::{Commit, SlotPayload, SlotWrite},
        fault::{FaultyPut, PutFault},
    };

    /// Counter protocol: each commit carries one write whose value is the
    /// committer's u64, validated against the head it last absorbed.
    struct CounterCommitter {
        label: u8,
        assemblies: u8,
        value: u64,
        conflict_on_loss: bool,
        head: u64,
    }

    impl CounterCommitter {
        fn new(label: u8, value: u64, conflict_on_loss: bool) -> Self {
            Self {
                label,
                assemblies: 0,
                value,
                conflict_on_loss,
                head: 0,
            }
        }
    }

    impl Committer for CounterCommitter {
        type Error = Error;

        async fn assemble(&mut self) -> Result<Option<Envelope>, Error> {
            self.assemblies += 1;
            let mut transaction_id = [0; 16];
            transaction_id[0] = self.label;
            transaction_id[1] = self.assemblies;

            Ok(Some(Envelope {
                leader: None,
                commits: vec![Commit {
                    transaction_id,
                    payload: SlotPayload {
                        validated_head: self.head,
                        changes_made: "counted".to_string(),
                        writes: vec![SlotWrite {
                            key: b"total".to_vec(),
                            value: Some(self.value.to_be_bytes().to_vec()),
                        }],
                    },
                }],
            }))
        }

        fn classify(&self, _winner: &Envelope) -> Race {
            if self.conflict_on_loss {
                Race::Conflict
            } else {
                Race::Benign
            }
        }

        fn absorb(&mut self, sequence: u64, _winner: Envelope) -> Result<(), Error> {
            self.head = sequence;

            Ok(())
        }
    }

    /// An envelope holding one write of `value` under one fresh id.
    fn writing(value: &[u8]) -> Envelope {
        let mut transaction_id = [0; 16];
        transaction_id[..value.len().min(16)].copy_from_slice(&value[..value.len().min(16)]);

        Envelope {
            leader: None,
            commits: vec![Commit {
                transaction_id,
                payload: SlotPayload {
                    validated_head: 0,
                    changes_made: "counted".to_string(),
                    writes: vec![SlotWrite {
                        key: b"total".to_vec(),
                        value: Some(value.to_vec()),
                    }],
                },
            }],
        }
    }

    #[tokio::test]
    async fn a_lost_race_rebased_benignly_wins_the_next_slot() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "toy");
        log.put_slot(1, &writing(b"other")).await.unwrap();

        let mut committer = CounterCommitter::new(1, 7, false);
        let outcome = drive_commit(&log, &mut committer, 1, &RetryPolicy::seeded(1))
            .await
            .unwrap();

        assert!(
            matches!(
                outcome,
                CommitDrive::Committed {
                    sequence: 2,
                    attempts: 2,
                    races_lost: 1
                }
            ),
            "{outcome:?}"
        );
        assert_eq!(
            committer.head, 1,
            "the winner was absorbed before the retry"
        );
    }

    #[tokio::test]
    async fn a_conflicting_loss_stops_with_the_winner() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "toy");
        let other = writing(b"other");
        log.put_slot(1, &other).await.unwrap();

        let mut committer = CounterCommitter::new(1, 7, true);
        let outcome = drive_commit(&log, &mut committer, 1, &RetryPolicy::seeded(1))
            .await
            .unwrap();

        match outcome {
            CommitDrive::Conflict { sequence, winner } => {
                assert_eq!(sequence, 1);
                assert_eq!(winner, other);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(committer.assemblies, 1, "a conflict is not retried");
        assert!(log.read_slot(2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_spent_budget_reports_exhausted() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "toy");
        log.put_slot(1, &writing(b"other")).await.unwrap();

        let retry = RetryPolicy {
            max_attempts: 1,
            ..RetryPolicy::seeded(1)
        };
        let outcome = drive_commit(&log, &mut CounterCommitter::new(1, 7, false), 1, &retry)
            .await
            .unwrap();

        assert!(
            matches!(
                outcome,
                CommitDrive::Exhausted {
                    attempts: 1,
                    last_sequence: 1
                }
            ),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn nothing_to_assemble_commits_nothing() {
        struct Empty;

        impl Committer for Empty {
            type Error = Error;

            async fn assemble(&mut self) -> Result<Option<Envelope>, Error> {
                Ok(None)
            }

            fn classify(&self, _winner: &Envelope) -> Race {
                Race::Benign
            }

            fn absorb(&mut self, _sequence: u64, _winner: Envelope) -> Result<(), Error> {
                Ok(())
            }
        }

        let log = SlotLog::new(Arc::new(InMemory::new()), "toy");
        let outcome = drive_commit(&log, &mut Empty, 1, &RetryPolicy::seeded(1))
            .await
            .unwrap();

        assert!(matches!(outcome, CommitDrive::Nothing), "{outcome:?}");
    }

    /// A store may report a slot taken before the winner's object is
    /// readable, which reaches the driver as [`Error::Transport`]. That is an
    /// ordinary contended round on a healthy log: the round backs off and
    /// races the same sequence again, never propagating it as terminal.
    #[tokio::test]
    async fn a_transport_failure_is_retried_at_the_same_sequence() {
        for fault in [PutFault::PrematureAlreadyExists, PutFault::Unreachable] {
            let log = SlotLog::new(Arc::new(FaultyPut::failing(fault, 1)), "toy");
            let mut committer = CounterCommitter::new(1, 7, false);
            let outcome = drive_commit(&log, &mut committer, 1, &RetryPolicy::seeded(1))
                .await
                .unwrap();

            assert!(
                matches!(
                    outcome,
                    CommitDrive::Committed {
                        sequence: 1,
                        attempts: 2,
                        races_lost: 0
                    }
                ),
                "{fault:?}: {outcome:?}"
            );
        }
    }

    /// A budget spent on a log that never answers is reported apart from one
    /// spent on lost races: nothing was contended, the log was unreachable,
    /// and the two mean different things operationally.
    #[tokio::test]
    async fn a_budget_spent_on_transport_failures_reports_unavailable() {
        let log = SlotLog::new(Arc::new(FaultyPut::armed(PutFault::Unreachable)), "toy");
        let retry = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            ..RetryPolicy::seeded(1)
        };

        let outcome = drive_commit(&log, &mut CounterCommitter::new(1, 7, false), 4, &retry)
            .await
            .unwrap();

        match outcome {
            CommitDrive::Unavailable {
                attempts,
                last_sequence,
                last_error,
            } => {
                assert_eq!((attempts, last_sequence), (3, 4));
                assert!(matches!(last_error, Error::Transport(_)), "{last_error}");
            }
            other => panic!("{other:?}"),
        }
    }

    /// A put that landed under a lost response, whose read-back then failed,
    /// leaves the sequence held by this commit with nobody yet aware of it.
    /// The retry must recognize its own envelope: absorbing it as a rival's
    /// and committing again at the next sequence applies the same work twice.
    #[tokio::test]
    async fn a_retry_recognizes_the_envelope_an_unattributed_put_landed() {
        let store = Arc::new(FaultyPut::failing(PutFault::LostResponse, 1).failing_gets(1));
        let log = SlotLog::new(store, "toy");
        let mut committer = CounterCommitter::new(1, 7, false);

        let outcome = drive_commit(&log, &mut committer, 1, &RetryPolicy::seeded(1))
            .await
            .unwrap();

        assert!(
            matches!(
                outcome,
                CommitDrive::Committed {
                    sequence: 1,
                    attempts: 2,
                    races_lost: 0
                }
            ),
            "{outcome:?}"
        );
        assert!(
            log.read_slot(2).await.unwrap().is_none(),
            "the work must not be committed a second time"
        );
        assert_eq!(committer.head, 0, "nothing foreign was absorbed");
    }

    /// Corruption is terminal by definition: the round propagates it without
    /// spending another attempt.
    #[tokio::test]
    async fn corruption_is_never_retried() {
        let store = Arc::new(InMemory::new());
        let log = SlotLog::new(store.clone(), "toy");
        store
            .put(&log.slot_path(1), "not an envelope".into())
            .await
            .unwrap();

        let mut committer = CounterCommitter::new(1, 7, false);
        let err = drive_commit(&log, &mut committer, 1, &RetryPolicy::seeded(1))
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Corruption(_)), "{err}");
        assert_eq!(committer.assemblies, 1);
    }

    /// Contention biases who wins, never who makes progress: two committers
    /// racing the same sequences both commit every round, and neither spends
    /// its budget. Which policy wins the larger share is a distributional
    /// claim, proven over seeds by the protocol simulation.
    #[tokio::test]
    async fn contested_rounds_starve_neither_committer() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "toy");
        let hot = RetryPolicy {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            ..RetryPolicy::seeded(11)
        };
        let steady = RetryPolicy::seeded(22);
        let mut first = CounterCommitter::new(1, 1, false);
        let mut second = CounterCommitter::new(2, 2, false);

        for _ in 0..5 {
            let start = log.tail_length(1).await.unwrap() + 1;
            let (one, two) = tokio::join!(
                drive_commit(&log, &mut first, start, &hot),
                drive_commit(&log, &mut second, start, &steady),
            );

            for outcome in [one.unwrap(), two.unwrap()] {
                assert!(
                    matches!(outcome, CommitDrive::Committed { .. }),
                    "{outcome:?}"
                );
            }
        }

        assert_eq!(log.tail_length(1).await.unwrap(), 10);
    }

    #[test]
    fn backoff_waits_nothing_first_then_grows_within_the_cap() {
        let retry = RetryPolicy::seeded(3);
        assert_eq!(retry.backoff(0), Duration::ZERO);

        let ceiling = retry.max_delay + retry.base_delay;
        assert!((retry.base_delay..=retry.base_delay * 2).contains(&retry.backoff(1)));
        for attempt in 1..40 {
            assert!(retry.backoff(attempt) <= ceiling, "attempt {attempt}");
        }
        assert!(retry.backoff(30) >= retry.max_delay);

        let hot = RetryPolicy {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            ..RetryPolicy::seeded(3)
        };
        assert_eq!(hot.backoff(9), Duration::ZERO);
    }

    /// Jitter draws from an explicitly seeded generator, never a thread-local
    /// global: equal seeds give equal streams, which is what makes a seeded
    /// simulation reproducible.
    #[test]
    fn a_seeded_jitter_is_reproducible_and_bounded() {
        let bound = Duration::from_millis(2);
        let draws =
            |jitter: &Jitter| -> Vec<Duration> { (0..16).map(|_| jitter.draw(bound)).collect() };

        let first = Jitter::seeded(7);
        let second = Jitter::seeded(7);
        let forked = first.clone();
        let also_forked = second.clone();

        let stream = draws(&first);
        assert_eq!(stream, draws(&second));
        assert_eq!(draws(&forked), draws(&also_forked));
        // A clone's sequence is its own, not the parent's shifted by one.
        let parent = draws(&Jitter::seeded(7));
        let child = draws(&Jitter::seeded(7).clone());
        assert_ne!(parent, child);
        assert_ne!(parent[1..], child[..child.len() - 1]);
        assert!(stream.iter().all(|drawn| *drawn <= bound));
        assert!(stream.iter().any(|drawn| *drawn != stream[0]));
        assert_eq!(Jitter::seeded(7).draw(Duration::ZERO), Duration::ZERO);
    }

    /// Vec-backed cursor store: the applied values in order plus a cursor,
    /// advanced in the same step.
    #[derive(Debug, Default)]
    struct VecStore {
        cursor: u64,
        applied: Vec<Vec<u8>>,
        finishes: usize,
    }

    impl VecStore {
        /// Applying and advancing as one step, which is the trait's
        /// obligation.
        fn record(&mut self, sequence: u64, envelope: &Envelope) {
            for commit in &envelope.commits {
                for write in &commit.payload.writes {
                    self.applied.push(write.value.clone().unwrap_or_default());
                }
            }
            self.cursor = sequence;
        }
    }

    impl CursorStore for VecStore {
        type Error = Error;

        async fn cursor(&mut self) -> Result<u64, Error> {
            Ok(self.cursor)
        }

        async fn apply(&mut self, sequence: u64, envelope: &Envelope) -> Result<(), Error> {
            self.record(sequence, envelope);

            Ok(())
        }

        async fn finish(&mut self) -> Result<(), Error> {
            self.finishes += 1;

            Ok(())
        }
    }

    #[tokio::test]
    async fn drive_fold_applies_in_order_resumes_and_is_idempotent() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "toy");
        for (sequence, value) in [(1_u64, b"one".as_slice()), (2, b"two"), (3, b"three")] {
            log.put_slot(sequence, &writing(value)).await.unwrap();
        }

        let mut store = VecStore::default();
        let partial = drive_fold(&log, &mut store, 2).await.unwrap();
        assert_eq!(
            (
                partial.slots_folded,
                partial.folded_through,
                partial.tail_remaining
            ),
            (2, 2, 1)
        );

        let rest = drive_fold(&log, &mut store, u64::MAX).await.unwrap();
        assert_eq!(
            (rest.slots_folded, rest.folded_through, rest.tail_remaining),
            (1, 3, 0)
        );
        assert_eq!(
            store.applied,
            vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()],
            "in order, never double-applied"
        );

        let done = drive_fold(&log, &mut store, u64::MAX).await.unwrap();
        assert_eq!((done.slots_folded, done.folded_through), (0, 3));
        assert_eq!(store.applied.len(), 3);
        assert_eq!(store.finishes, 3, "every round ends at the barrier");
    }

    /// A hole below the head is a destroyed slot, not an end of log. Folding
    /// the prefix would hide committed state, so the round refuses before it
    /// applies anything.
    #[tokio::test]
    async fn drive_fold_refuses_a_holed_tail() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "toy");
        for sequence in [1_u64, 3] {
            log.put_slot(sequence, &writing(&sequence.to_be_bytes()))
                .await
                .unwrap();
        }

        let mut store = VecStore::default();
        let err = drive_fold(&log, &mut store, u64::MAX).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        assert!(store.applied.is_empty());
    }

    /// Toy role: hands out sessions over one shared store and counts how many
    /// times the role was acquired. `advance_on_peek` stands in for another
    /// folder making progress during the appointment delay.
    struct ToyRole {
        state: Arc<Mutex<VecStore>>,
        opens: AtomicUsize,
        advance_on_peek: bool,
    }

    struct ToySession {
        state: Arc<Mutex<VecStore>>,
    }

    impl ToyRole {
        fn new(advance_on_peek: bool) -> Self {
            Self {
                state: Arc::new(Mutex::new(VecStore::default())),
                opens: AtomicUsize::new(0),
                advance_on_peek,
            }
        }

        fn opens(&self) -> usize {
            self.opens.load(Ordering::Relaxed)
        }
    }

    impl FolderRole for ToyRole {
        type Error = Error;
        type Session = ToySession;

        async fn peek_cursor(&self) -> Result<u64, Error> {
            let mut state = self.state.lock().unwrap();
            let cursor = state.cursor;
            if self.advance_on_peek {
                state.cursor += 1;
            }

            Ok(cursor)
        }

        async fn open(&self) -> Result<ToySession, Error> {
            self.opens.fetch_add(1, Ordering::Relaxed);

            Ok(ToySession {
                state: Arc::clone(&self.state),
            })
        }
    }

    impl CursorStore for ToySession {
        type Error = Error;

        async fn cursor(&mut self) -> Result<u64, Error> {
            Ok(self.state.lock().unwrap().cursor)
        }

        async fn apply(&mut self, sequence: u64, envelope: &Envelope) -> Result<(), Error> {
            self.state.lock().unwrap().record(sequence, envelope);

            Ok(())
        }

        async fn finish(&mut self) -> Result<(), Error> {
            self.state.lock().unwrap().finishes += 1;

            Ok(())
        }
    }

    async fn stalled_log(slots: u64) -> SlotLog {
        let log = SlotLog::new(Arc::new(InMemory::new()), "toy");
        for sequence in 1..=slots {
            log.put_slot(sequence, &writing(&sequence.to_be_bytes()))
                .await
                .unwrap();
        }

        log
    }

    #[tokio::test(start_paused = true)]
    async fn appointment_stands_down_when_another_folder_makes_progress() {
        let log = stalled_log(3).await;
        let role = ToyRole::new(true);

        let outcome = drive_fold_if_stalled(
            &log,
            &role,
            2,
            Duration::from_millis(5),
            u64::MAX,
            &Jitter::seeded(5),
        )
        .await
        .unwrap();

        assert!(outcome.is_none());
        assert_eq!(role.opens(), 0, "never acquired the role");
    }

    #[tokio::test(start_paused = true)]
    async fn appointment_folds_when_the_tail_is_stalled_and_no_one_else_moves() {
        let log = stalled_log(3).await;
        let role = ToyRole::new(false);

        let report = drive_fold_if_stalled(
            &log,
            &role,
            2,
            Duration::from_millis(5),
            u64::MAX,
            &Jitter::seeded(5),
        )
        .await
        .unwrap()
        .expect("a stalled tail appoints a folder");

        assert_eq!(
            (
                report.slots_folded,
                report.folded_through,
                report.tail_remaining
            ),
            (3, 3, 0)
        );
        assert_eq!(role.opens(), 1, "exactly one open");
    }

    #[tokio::test(start_paused = true)]
    async fn a_short_tail_never_appoints() {
        let log = stalled_log(2).await;
        let role = ToyRole::new(false);
        let started = tokio::time::Instant::now();

        let outcome = drive_fold_if_stalled(
            &log,
            &role,
            2,
            Duration::from_secs(30),
            u64::MAX,
            &Jitter::seeded(5),
        )
        .await
        .unwrap();

        assert!(outcome.is_none());
        assert_eq!(role.opens(), 0);
        assert_eq!(started.elapsed(), Duration::ZERO, "no delay paid");
    }
}
