//! The rendezvous concurrent commits meet at, so several become one batch.
//!
//! A store admits one batch at a time. A commit that finds the store free
//! opens a batch and stages onto it; a commit that finds one forming joins
//! it; a commit that finds one in flight waits for the next. A batch seals
//! the moment no caller is on its way into it, so an uncontended commit
//! waits for nobody and takes the path it always took, while a contended
//! one rides a batch that grew while the previous flush was in the air.
//!
//! That is the whole of the batching policy: nothing here waits on a timer
//! or guesses at arrivals. The flush already in flight is the window, and
//! the arrival count says when it has closed.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use slatedb::{Db, DbTransaction, IsolationLevel};
use tokio::sync::{Mutex, MutexGuard, watch};

use super::{Landed, Prepared, StagedWrite, commit_batch, fold, head_view_for, prepare_and_stage};
use crate::{
    catalog::{CatalogSnapshot, SnapshotId, projection::ProjectionCache},
    error::{Error, Result},
    store::StagedBytes,
    transaction::{operations::ChangeSet, verbs::Transaction},
};

/// The most commits one batch carries.
///
/// A batch otherwise seals when nobody is on their way into it, which under
/// saturation is never: callers arriving faster than they can be staged
/// would keep the count above zero and the batch would grow without ever
/// flushing. This bounds that. It also bounds what one batch holds in
/// memory, though only loosely — each member's own limits still apply, and
/// a batch multiplies them by at most this. Sixty-four commits per flush is
/// already far past where the marginal member buys anything.
const MAX_BATCH_MEMBERS: usize = 64;

/// What a sealed batch did, told to every member that rode it.
///
/// A batch's write failure does not appear here as itself. The answer goes
/// to members that never saw the store and cannot be handed an error one
/// of them owns, so it arrives as [`Outcome::Nothing`] and is surfaced
/// typed by whichever member meets the same failure on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Committed. Every member's allocated ids stand.
    Committed,
    /// Lost the head race. Every member re-runs, classifying its own
    /// change set against the commits that won.
    LostRace,
    /// Nothing was written: the write failed, or the batch was abandoned
    /// before it sealed. Every member re-attempts.
    Nothing,
}

/// What one caller staged onto a batch.
pub(crate) struct Staged {
    /// One snapshot id per member of that caller, in member order.
    pub(crate) ids: Vec<SnapshotId>,
    /// One change set per member that staged writes, for conflict
    /// classification if the batch loses its race.
    pub(crate) ours: Vec<ChangeSet>,
    /// The head the batch's premise was read at.
    pub(crate) head_before: u64,
    /// Whether this caller put anything in the batch. A caller that did
    /// not is unaffected by the batch's fate.
    pub(crate) contributed: bool,
    /// The batch's outcome, once it lands.
    pub(crate) outcome: watch::Receiver<Option<Outcome>>,
}

/// A batch being formed: one open transaction carrying every member's
/// writes, and the premise the next member stages against.
struct Batch {
    db_tx: DbTransaction,
    /// The head view the batch opened at, and the base of the projection
    /// refresh once it lands.
    base: Arc<CatalogSnapshot>,
    /// `base` folded forward through the members staged so far. Left
    /// `None` until a second member needs it, so a batch nobody joins
    /// never clones the view.
    premise: Option<CatalogSnapshot>,
    /// How much of `writes` the premise already reflects.
    folded: usize,
    head_before: u64,
    /// The id the batch will leave at the head pointer: its last
    /// snapshot-minting member's.
    head: u64,
    /// Commits staged onto the batch, against [`MAX_BATCH_MEMBERS`].
    members: usize,
    writes: Vec<StagedWrite>,
    /// What the members have staged onto `db_tx`, index entries included.
    staged_bytes: StagedBytes,
    outcome: watch::Sender<Option<Outcome>>,
}

impl Batch {
    async fn open(db: &Db, projections: &std::sync::RwLock<ProjectionCache>) -> Result<Self> {
        let db_tx = db
            .begin(IsolationLevel::Snapshot)
            .await
            .map_err(Error::from)?;
        let base = match head_view_for(&db_tx, projections).await {
            Ok(base) => base,
            Err(err) => {
                db_tx.rollback();
                return Err(err);
            }
        };
        let head_before = base.snapshot.snapshot_id;

        Ok(Self {
            db_tx,
            base,
            premise: None,
            folded: 0,
            head_before,
            head: head_before,
            members: 0,
            writes: Vec::new(),
            staged_bytes: StagedBytes::default(),
            outcome: watch::Sender::new(None),
        })
    }

    /// Brings the premise up to date with everything staged since it was
    /// last folded, so the next member stages against what its
    /// predecessors left rather than against the head the batch opened at.
    fn refold(&mut self) -> Result<()> {
        if self.folded == self.writes.len() {
            return Ok(());
        }
        let mut premise = self.premise.take().unwrap_or_else(|| (*self.base).clone());
        fold::fold_batch(&mut premise, &self.writes[self.folded..])?;
        self.folded = self.writes.len();
        self.premise = Some(premise);
        Ok(())
    }

    /// Runs every member's closure in turn and stages the result. An error
    /// leaves the batch poisoned: some of the failing member's writes may
    /// already be on the transaction, so the caller discards the batch
    /// rather than commit a member that reported failure.
    async fn stage<F>(&mut self, members: &[F]) -> Result<(Vec<SnapshotId>, Vec<ChangeSet>)>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        let mut ids = Vec::with_capacity(members.len());
        let mut ours = Vec::new();

        for member in members {
            self.members += 1;
            self.refold()?;
            let premise = self.premise.as_ref().unwrap_or(&self.base);
            let prepared = prepare_and_stage(&self.db_tx, member, premise).await?;
            match prepared {
                Prepared::Nothing { head } => ids.push(SnapshotId::new(head)),
                Prepared::Staged {
                    ours: theirs,
                    commits,
                    writes,
                    staged_bytes,
                } => {
                    ids.push(SnapshotId::new(commits));
                    ours.push(*theirs);
                    self.head = commits;
                    self.writes.extend(writes);
                    self.staged_bytes.0 = self.staged_bytes.0.saturating_add(staged_bytes.0);
                }
            }
        }

        Ok((ids, ours))
    }
}

/// The forming batch and whether one is in flight. Both move together, so
/// they share one lock: a batch is taken out of `forming` and marked in
/// flight in the same breath, and no second batch can open against a head
/// the one in flight is about to move.
struct Shared {
    forming: Option<Batch>,
    in_flight: bool,
}

/// Where concurrent commits meet. One per store handle, shared by every
/// clone of the catalog.
pub(crate) struct Coalescer {
    shared: Mutex<Shared>,
    /// Callers that have arrived and not yet staged. A batch seals when
    /// this reaches zero, which is what makes the batch exactly as large
    /// as the contention and no larger.
    arriving: AtomicUsize,
    /// Bumped under `shared` whenever a batch leaves flight, waking the
    /// callers waiting for the store.
    flights: watch::Sender<u64>,
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
}

impl Coalescer {
    pub(crate) fn new(projections: Arc<std::sync::RwLock<ProjectionCache>>) -> Self {
        Self {
            shared: Mutex::new(Shared {
                forming: None,
                in_flight: false,
            }),
            arriving: AtomicUsize::new(0),
            flights: watch::Sender::new(0),
            projections,
        }
    }

    /// Registers a caller as on its way into the batch now forming.
    fn arrive(self: &Arc<Self>) -> Arrival {
        self.arriving.fetch_add(1, Ordering::AcqRel);
        Arrival {
            coalescer: Arc::clone(self),
            settled: false,
        }
    }

    /// Waits until no batch is in flight and returns the lock on the
    /// forming one. Callers are admitted one at a time, which is what lets
    /// each stage against the state its predecessors left.
    async fn admit(&self) -> MutexGuard<'_, Shared> {
        loop {
            let shared = self.shared.lock().await;
            if !shared.in_flight {
                return shared;
            }
            // Subscribing under the lock is what makes this wait safe: the
            // generation only moves under this same lock, so the flight
            // being waited on cannot end unnoticed in between.
            let mut flights = self.flights.subscribe();
            drop(shared);
            let _ = flights.changed().await;
        }
    }

    /// Stages `members` onto whichever batch is forming, opening one if
    /// none is, and seals the batch if this caller is the last one in.
    pub(crate) async fn stage<F>(self: &Arc<Self>, db: &Db, members: &[F]) -> Result<Staged>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        let arrival = self.arrive();
        let mut shared = self.admit().await;

        let mut batch = match shared.forming.take() {
            Some(batch) => batch,
            None => Batch::open(db, &self.projections).await?,
        };
        let staged_before = batch.writes.len();

        let staged = match batch.stage(members).await {
            Ok(staged) => staged,
            Err(err) => {
                // The batch is poisoned; discard it. Its members learn
                // that nothing landed when the outcome sender drops, and
                // re-run — theirs is work redone, never work lost.
                batch.db_tx.rollback();
                return Err(err);
            }
        };
        let (ids, ours) = staged;

        let outcome = batch.outcome.subscribe();
        let head_before = batch.head_before;
        let contributed = batch.writes.len() > staged_before;

        // Nobody else on their way in means nobody else to wait for; a
        // full batch stops waiting whether or not anyone is.
        let alone = arrival.settle() == 0;
        let full = batch.members >= MAX_BATCH_MEMBERS;
        if (alone || full) && !batch.writes.is_empty() {
            shared.in_flight = true;
            drop(shared);
            self.launch(batch);
        } else if batch.writes.is_empty() {
            // An empty batch is nothing to commit and nothing to hold
            // open; the next caller opens a fresh one.
            batch.db_tx.rollback();
        } else {
            shared.forming = Some(batch);
        }

        Ok(Staged {
            ids,
            ours,
            head_before,
            contributed,
            outcome,
        })
    }

    /// Commits `batch` on a task of its own, so the caller that sealed it
    /// cannot take the batch down with it: a host interrupt drops that
    /// caller's future, and every other member is owed an answer.
    fn launch(self: &Arc<Self>, batch: Batch) {
        let coalescer = Arc::clone(self);
        drop(tokio::spawn(async move { coalescer.land(batch).await }));
    }

    /// Commits one batch, tells its members, and reopens the store.
    async fn land(self: Arc<Self>, batch: Batch) {
        let Batch {
            db_tx,
            base,
            head_before,
            head,
            writes,
            staged_bytes,
            outcome,
            ..
        } = batch;

        let landed = match commit_batch(
            db_tx,
            head_before,
            head,
            &writes,
            staged_bytes,
            &base,
            &self.projections,
        )
        .await
        {
            Ok(Landed::Committed(_)) => Outcome::Committed,
            Ok(Landed::LostRace) => Outcome::LostRace,
            Err(err) => {
                // The only record of this error. Its members re-attempt,
                // and the one that meets it alone returns it typed.
                tracing::warn!(error = %err, "commit batch failed to write; its members retry");
                Outcome::Nothing
            }
        };
        // Every member may already have gone; the batch landed regardless.
        let _ = outcome.send(Some(landed));

        let mut shared = self.shared.lock().await;
        shared.in_flight = false;
        self.flights
            .send_modify(|flight| *flight = flight.wrapping_add(1));
    }

    /// Seals whatever batch is forming, if nothing else will. The path a
    /// caller that vanished before staging leaves behind: its batch is
    /// holding members who are waiting for an arrival that will never
    /// come.
    async fn seal_abandoned(self: Arc<Self>) {
        let mut shared = self.shared.lock().await;
        if shared.in_flight || self.arriving.load(Ordering::Acquire) > 0 {
            return;
        }
        let Some(batch) = shared.forming.take() else {
            return;
        };
        if batch.writes.is_empty() {
            batch.db_tx.rollback();
            return;
        }
        shared.in_flight = true;
        drop(shared);
        self.land(batch).await;
    }
}

/// Waits for the batch a caller staged onto to land.
///
/// A sender dropped with nothing published means the batch was abandoned
/// before it sealed — nothing was written, so the member re-runs.
pub(crate) async fn await_outcome(mut outcome: watch::Receiver<Option<Outcome>>) -> Outcome {
    loop {
        if let Some(landed) = *outcome.borrow_and_update() {
            return landed;
        }
        if outcome.changed().await.is_err() {
            // Re-read rather than assume: the sender may have published
            // and dropped between the borrow above and this wait.
            return outcome.borrow().unwrap_or(Outcome::Nothing);
        }
    }
}

/// Counts one caller from the moment it arrives until it has staged.
///
/// A batch seals when the count reaches zero, so a caller that goes away
/// in between — its future dropped by a host interrupt — must not leave
/// the batch waiting for a member that will never arrive. Dropping this
/// guard without settling both drops the count and seals what is left.
struct Arrival {
    coalescer: Arc<Coalescer>,
    settled: bool,
}

impl Arrival {
    /// Marks this caller as staged, reporting how many are still on their
    /// way in.
    fn settle(mut self) -> usize {
        self.settled = true;
        self.coalescer.arriving.fetch_sub(1, Ordering::AcqRel) - 1
    }
}

impl Drop for Arrival {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        if self.coalescer.arriving.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        // Outside a runtime there is nothing to spawn onto, and nothing to
        // rescue either: the next commit finds the batch and seals it.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let coalescer = Arc::clone(&self.coalescer);
            drop(runtime.spawn(async move { coalescer.seal_abandoned().await }));
        }
    }
}
