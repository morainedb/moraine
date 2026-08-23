//! The rendezvous concurrent commits meet at, so several become one batch.
//!
//! A store admits one batch at a time. A commit that finds the store free
//! opens a batch and stages onto it; a commit that finds one forming joins
//! it; a commit that finds one in flight waits for the next. A batch seals
//! the moment no caller is on its way into it; nothing waits on a timer.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use slatedb::{Db, DbTransaction, IsolationLevel};
use tokio::sync::{Mutex, MutexGuard, watch};

use super::{
    CommitDurability, HeadTransition, HeadViewUpdate, Landed, Prepared, StagedWrite, commit_batch,
    fold, head_view_for, prepare_and_stage,
};
use crate::{
    catalog::{CatalogSnapshot, SnapshotId, projection::ProjectionCache},
    error::{Error, Result},
    store::StagedBytes,
    transaction::{operations::ChangeSet, verbs::Transaction},
};

/// The most commits one batch carries; a full batch seals even while
/// callers are still arriving.
const MAX_BATCH_MEMBERS: usize = 64;

/// What a sealed batch did, told to every member that rode it. A write
/// failure arrives as [`Outcome::Nothing`], not as the error itself.
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
    /// Whether this caller put anything in the batch.
    pub(crate) contributed: bool,
    /// The batch's outcome, once it lands.
    pub(crate) outcome: watch::Receiver<Option<Outcome>>,
}

/// A batch being formed: one open transaction carrying every member's
/// writes, and the premise the next member stages against.
struct Batch {
    db_tx: DbTransaction,
    /// The head view the batch opened at.
    base: Arc<CatalogSnapshot>,
    /// `base` folded forward through the members staged so far; `None`
    /// until a second member needs it.
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
    /// last folded.
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
    /// leaves the batch poisoned: the caller must discard it.
    async fn stage<F>(
        &mut self,
        members: &[F],
        projections: &std::sync::RwLock<ProjectionCache>,
    ) -> Result<(Vec<SnapshotId>, Vec<ChangeSet>)>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        let mut ids = Vec::with_capacity(members.len());
        let mut ours = Vec::new();

        for member in members {
            self.members += 1;
            self.refold()?;
            let premise = self.premise.as_ref().unwrap_or(&self.base);
            let prepared = prepare_and_stage(&self.db_tx, projections, member, premise).await?;
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

/// The forming batch and whether one is in flight, under one lock so a
/// batch leaves `forming` and enters flight atomically.
struct Shared {
    forming: Option<Batch>,
    in_flight: bool,
}

/// Where concurrent commits meet. One per store handle, shared by every
/// clone of the catalog.
pub(crate) struct Coalescer {
    shared: Mutex<Shared>,
    /// Callers that have arrived and not yet staged; a batch seals when
    /// this reaches zero.
    arriving: AtomicUsize,
    /// Bumped under `shared` whenever a batch leaves flight.
    flights: watch::Sender<u64>,
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
    /// How a sealed batch reaches object storage.
    durability: CommitDurability,
}

impl Coalescer {
    pub(crate) fn new(
        projections: Arc<std::sync::RwLock<ProjectionCache>>,
        durability: CommitDurability,
    ) -> Self {
        Self {
            shared: Mutex::new(Shared {
                forming: None,
                in_flight: false,
            }),
            arriving: AtomicUsize::new(0),
            flights: watch::Sender::new(0),
            projections,
            durability,
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
    /// forming one. Callers are admitted one at a time.
    async fn admit(&self) -> MutexGuard<'_, Shared> {
        loop {
            let shared = self.shared.lock().await;
            if !shared.in_flight {
                return shared;
            }
            // Subscribed under the lock: the generation only moves under it,
            // so the flight cannot end unnoticed in between.
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

        let staged = match batch.stage(members, &self.projections).await {
            Ok(staged) => staged,
            Err(err) => {
                // The batch is poisoned; its members learn that nothing
                // landed when the outcome sender drops.
                batch.db_tx.rollback();
                return Err(err);
            }
        };
        let (ids, ours) = staged;

        let outcome = batch.outcome.subscribe();
        let head_before = batch.head_before;
        let contributed = batch.writes.len() > staged_before;

        let alone = arrival.settle() == 0;
        let full = batch.members >= MAX_BATCH_MEMBERS;
        if (alone || full) && !batch.writes.is_empty() {
            shared.in_flight = true;
            drop(shared);
            self.launch(batch);
        } else if batch.writes.is_empty() {
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
    /// cannot take the batch down with it.
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
            HeadTransition {
                before: head_before,
                after: head,
            },
            &writes,
            staged_bytes,
            HeadViewUpdate::Rebuild(base),
            &self.projections,
            &self.durability,
        )
        .await
        {
            Ok(Landed::Committed(_)) => Outcome::Committed,
            Ok(Landed::LostRace) => Outcome::LostRace,
            Err(err) => {
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

    /// Seals whatever batch is forming, if nothing else will: the path a
    /// caller that vanished before staging leaves behind.
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

/// Waits for the batch a caller staged onto to land. A sender dropped with
/// nothing published means the batch was abandoned before it sealed.
pub(crate) async fn await_outcome(mut outcome: watch::Receiver<Option<Outcome>>) -> Outcome {
    loop {
        if let Some(landed) = *outcome.borrow_and_update() {
            return landed;
        }
        if outcome.changed().await.is_err() {
            // The sender may have published and dropped since the borrow.
            return outcome.borrow().unwrap_or(Outcome::Nothing);
        }
    }
}

/// Counts one caller from the moment it arrives until it has staged.
/// Dropping it without settling both drops the count and seals what is
/// left, so a vanished caller cannot leave the batch waiting.
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
        // Outside a runtime, the next commit finds the batch and seals it.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let coalescer = Arc::clone(&self.coalescer);
            drop(runtime.spawn(async move { coalescer.seal_abandoned().await }));
        }
    }
}
