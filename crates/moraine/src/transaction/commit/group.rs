//! The rendezvous concurrent commits meet at, so several become one batch.
//!
//! A store admits one batch at a time. A commit that finds the store free
//! opens a batch and stages onto it; a commit that finds one forming joins
//! it; a commit that finds one being submitted waits for the next. A batch
//! seals the moment no caller is on its way into it; nothing waits on a
//! timer. A submitted batch is visible and folded before it is durable, so
//! the next batch forms and submits while it awaits its flush, and the
//! flush pacer lets the two share one.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use futures::FutureExt;
use slatedb::{Db, DbTransaction, IsolationLevel};
use tokio::{
    sync::{Mutex, MutexGuard, watch},
    time::Instant,
};

use super::{
    CommitDurability, HeadTransition, HeadViewUpdate, Landed, Prepared, StagedWrite, Submission,
    await_submitted, fold, head_view_for, prepare_and_stage, submit_batch,
};
use crate::{
    catalog::{CatalogSnapshot, SnapshotId, projection::ProjectionCache},
    error::{Error, Result},
    store::StagedBytes,
    telemetry::{STALL_INTERVAL, reporting_phase},
    transaction::{operations::ChangeSet, verbs::Transaction},
};

/// The most commits one batch carries; a full batch seals even while
/// callers are still arriving.
const MAX_BATCH_MEMBERS: usize = 64;

/// What a batch did, told to every member that rode it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Committed. Every member's allocated ids stand.
    Committed,
    /// Lost the head race. Every member re-runs, classifying its own
    /// change set against the commits that won.
    LostRace,
    /// Abandoned before submission; every member may re-attempt.
    Abandoned,
    /// Submitted, but durability was not acknowledged. Never re-attempt.
    Unknown(String),
    /// The writer lost its epoch and cannot submit further work.
    Fenced(String),
}

/// Submission distinguishes an abandoned batch from a lost acknowledgement.
#[derive(Clone)]
pub(crate) enum BatchState {
    Forming,
    Submitted,
    Finished(Outcome),
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
    /// Whether this caller's answer depends on the batch landing: it
    /// staged writes, or staged against writes already in it.
    pub(crate) rides: bool,
    /// The batch's outcome, once it lands.
    pub(crate) outcome: watch::Receiver<BatchState>,
}

/// A batch being formed: one open transaction carrying every member's
/// writes, and the premise the next member stages against.
pub(crate) struct Batch {
    pub(crate) db_tx: Arc<DbTransaction>,
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
    /// What every member staged so far changed, for a later member to
    /// classify itself against.
    change_sets: Vec<ChangeSet>,
    /// What the members have staged onto `db_tx`, index entries included.
    staged_bytes: StagedBytes,
    outcome: watch::Sender<BatchState>,
}

/// What staging onto a batch did.
pub(crate) enum Staging {
    /// One snapshot id per logical commit, and the change set of each
    /// that staged writes.
    Staged {
        ids: Vec<SnapshotId>,
        ours: Vec<ChangeSet>,
    },
    /// The member found it cannot ride this batch — a conflict with what
    /// is already on it — and left it untouched. The batch goes on; only
    /// this caller gets the error.
    Declined(Error),
}

/// Something that can stage itself onto a forming batch: a slice of verb
/// closures, or a staged-row member.
pub(crate) trait StageOnto {
    async fn stage_onto(
        &self,
        batch: &mut Batch,
        db: &Db,
        projections: &std::sync::RwLock<ProjectionCache>,
    ) -> Result<Staging>;
}

impl<F> StageOnto for [F]
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    async fn stage_onto(
        &self,
        batch: &mut Batch,
        _db: &Db,
        projections: &std::sync::RwLock<ProjectionCache>,
    ) -> Result<Staging> {
        let (ids, ours) = batch.stage(self, projections).await?;
        Ok(Staging::Staged { ids, ours })
    }
}

impl<F, const N: usize> StageOnto for [F; N]
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    async fn stage_onto(
        &self,
        batch: &mut Batch,
        db: &Db,
        projections: &std::sync::RwLock<ProjectionCache>,
    ) -> Result<Staging> {
        self.as_slice().stage_onto(batch, db, projections).await
    }
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
            db_tx: Arc::new(db_tx),
            base,
            premise: None,
            folded: 0,
            head_before,
            head: head_before,
            members: 0,
            writes: Vec::new(),
            change_sets: Vec::new(),
            staged_bytes: StagedBytes::default(),
            outcome: watch::Sender::new(BatchState::Forming),
        })
    }

    /// Brings the premise up to date with everything staged since it was
    /// last folded.
    pub(crate) fn refold(&mut self) -> Result<()> {
        if self.folded == self.writes.len() {
            return Ok(());
        }
        let mut premise = self.premise.take().unwrap_or_else(|| (*self.base).clone());
        fold::fold_batch(&mut premise, &self.writes[self.folded..])?;
        self.folded = self.writes.len();
        self.premise = Some(premise);
        Ok(())
    }

    /// The state the next member stages against: the base folded forward
    /// through every member so far. Call [`Self::refold`] first.
    pub(crate) fn premise(&self) -> &CatalogSnapshot {
        self.premise.as_ref().unwrap_or(&self.base)
    }

    /// What every member so far changed, in staging order.
    pub(crate) fn change_sets(&self) -> &[ChangeSet] {
        &self.change_sets
    }

    /// Records one more member's staged writes on the batch.
    pub(crate) fn admit_member(
        &mut self,
        commits: u64,
        ours: ChangeSet,
        writes: Vec<StagedWrite>,
        staged_bytes: StagedBytes,
    ) {
        self.head = commits;
        self.writes.extend(writes);
        self.change_sets.push(ours);
        self.staged_bytes.0 = self.staged_bytes.0.saturating_add(staged_bytes.0);
    }

    /// Counts a member against the batch's ceiling.
    pub(crate) fn count_member(&mut self) {
        self.members += 1;
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
            self.count_member();
            self.refold()?;
            let db_tx = Arc::clone(&self.db_tx);
            let prepared = prepare_and_stage(&db_tx, projections, member, self.premise()).await?;
            match prepared {
                Prepared::Nothing { head } => ids.push(SnapshotId::new(head)),
                Prepared::Staged {
                    ours: theirs,
                    commits,
                    writes,
                    staged_bytes,
                } => {
                    ids.push(SnapshotId::new(commits));
                    ours.push((*theirs).clone());
                    self.admit_member(commits, *theirs, writes, staged_bytes);
                }
            }
        }

        Ok((ids, ours))
    }
}

/// The forming batch, under a lock so it leaves `forming` and enters
/// flight atomically.
struct Shared {
    forming: Option<Batch>,
}

/// Where concurrent commits meet. One per store handle, shared by every
/// clone of the catalog.
pub(crate) struct Coalescer {
    shared: Mutex<Shared>,
    /// Callers that have arrived and not yet staged; a batch seals when
    /// this reaches zero.
    arriving: AtomicUsize,
    /// Whether a batch holds the store's one flight slot. Claimed under
    /// `shared`, released by dropping the [`Flight`] that claimed it.
    in_flight: AtomicBool,
    /// Bumped whenever a batch leaves flight.
    flights: watch::Sender<u64>,
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
    /// How a sealed batch reaches object storage.
    durability: CommitDurability,
    /// Until when arrivals hold before staging, so a commit that lost the
    /// head race on its own and is being re-driven from outside finds the
    /// head where it left it. A session committing without pause would
    /// otherwise beat every retry.
    yielding_until: std::sync::Mutex<Option<Instant>>,
}

/// How long the batch yields to a commit re-driven from outside: past
/// DuckLake's first retry backoff and the re-read behind it.
const RETRY_YIELD: std::time::Duration = std::time::Duration::from_millis(250);

impl Coalescer {
    pub(crate) fn new(
        projections: Arc<std::sync::RwLock<ProjectionCache>>,
        durability: CommitDurability,
    ) -> Self {
        Self {
            shared: Mutex::new(Shared { forming: None }),
            arriving: AtomicUsize::new(0),
            in_flight: AtomicBool::new(false),
            flights: watch::Sender::new(0),
            projections,
            durability,
            yielding_until: std::sync::Mutex::new(None),
        }
    }

    /// A commit that cannot join the batch lost the head race and is
    /// being re-driven from outside: the batch yields to it.
    pub(crate) fn yield_to_retry(&self) {
        let until = Instant::now() + RETRY_YIELD;
        let mut yielding = self
            .yielding_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if yielding.is_none_or(|current| current < until) {
            *yielding = Some(until);
        }
    }

    /// Waits out a yield in progress.
    async fn hold_for_yield(&self) {
        let until = *self
            .yielding_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(until) = until
            && until > Instant::now()
        {
            tokio::time::sleep_until(until).await;
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
            // Subscribed before the slot is read: a flight that ends in
            // between still bumps the generation, and `changed` reports
            // that bump rather than waiting for the one after it.
            let mut flights = self.flights.subscribe();
            let shared = self.shared.lock().await;
            if !self.in_flight.load(Ordering::Acquire) {
                return shared;
            }
            drop(shared);
            let _ = flights.changed().await;
        }
    }

    /// Claims the store's flight slot. The caller must hold `shared`, so
    /// the free slot it observed cannot be taken in between.
    fn enter_flight(self: &Arc<Self>) -> Flight {
        self.in_flight.store(true, Ordering::Release);
        Flight(Arc::clone(self))
    }

    /// Stages `members` onto whichever batch is forming, opening one if
    /// none is, and seals the batch if this caller is the last one in.
    pub(crate) async fn stage<M>(self: &Arc<Self>, db: &Db, members: &M) -> Result<Staged>
    where
        M: StageOnto + ?Sized,
    {
        let arrival = self.arrive();
        reporting_phase("yield", self.hold_for_yield()).await;
        let arrived = std::time::Instant::now();
        // Each await below is named, because none of them can be read from
        // a thread dump once the commit parks on it.
        let mut shared = reporting_phase("admit", self.admit()).await;
        let admitted = std::time::Instant::now();

        let mut batch = match shared.forming.take() {
            Some(batch) => batch,
            None => reporting_phase("open", Batch::open(db, &self.projections)).await?,
        };

        let staging = match reporting_phase(
            "stage",
            members.stage_onto(&mut batch, db, &self.projections),
        )
        .await
        {
            Ok(staging) => staging,
            Err(err) => {
                // The batch is poisoned; its members learn that nothing
                // landed when the outcome sender drops. Named here because
                // the error reaches only the member that raised it.
                tracing::warn!(
                    members = batch.members,
                    error = %err,
                    "a member failed to stage and took the forming batch with it"
                );
                super::rollback(batch.db_tx);
                return Err(err);
            }
        };

        tracing::debug!(
            members = batch.members,
            admit_ms = crate::telemetry::milliseconds(admitted.duration_since(arrived)),
            stage_ms = crate::telemetry::milliseconds(admitted.elapsed()),
            "member staged onto the forming batch"
        );
        let outcome = batch.outcome.subscribe();
        let head_before = batch.head_before;
        let rides = !batch.writes.is_empty();

        let alone = arrival.settle() == 0;
        let full = batch.members >= MAX_BATCH_MEMBERS;
        if (alone || full) && !batch.writes.is_empty() {
            let flight = self.enter_flight();
            drop(shared);
            self.launch(batch, flight);
        } else if batch.writes.is_empty() {
            super::rollback(batch.db_tx);
        } else {
            shared.forming = Some(batch);
        }

        // Sealing ran either way: a member that declined still counted as
        // an arrival, and the batch it left must not wait on it.
        match staging {
            Staging::Staged { ids, ours } => Ok(Staged {
                ids,
                ours,
                head_before,
                rides,
                outcome,
            }),
            Staging::Declined(err) => Err(err),
        }
    }

    /// Commits `batch` on a task of its own, so the caller that sealed it
    /// cannot take the batch down with it.
    fn launch(self: &Arc<Self>, batch: Batch, flight: Flight) {
        let coalescer = Arc::clone(self);
        drop(tokio::spawn(
            async move { coalescer.land(batch, flight).await },
        ));
    }

    /// Commits one batch, tells its members, and reopens the store.
    async fn land(self: Arc<Self>, batch: Batch, flight: Flight) {
        let Batch {
            db_tx,
            base,
            head_before,
            head,
            writes,
            staged_bytes,
            outcome,
            members,
            ..
        } = batch;

        outcome.send_replace(BatchState::Submitted);
        let submission = std::panic::AssertUnwindSafe(reporting_phase(
            "submit",
            submit_batch(
                db_tx,
                HeadTransition {
                    before: head_before,
                    after: head,
                },
                &writes,
                staged_bytes,
                HeadViewUpdate::Rebuild(base),
                &self.projections,
            ),
        ))
        .catch_unwind()
        .await;
        // Visible and folded, or decided: the store admits the next batch
        // while this one awaits its flush, so the two may share it.
        drop(flight);

        let landed = match submission {
            Ok(Ok(Submission::Submitted(submitted))) => {
                match std::panic::AssertUnwindSafe(reporting_phase(
                    "durable",
                    await_submitted(submitted, &self.projections, &self.durability, members),
                ))
                .catch_unwind()
                .await
                {
                    Ok(Ok(Landed::Committed(_))) => Outcome::Committed,
                    Ok(Ok(Landed::LostRace)) => Outcome::LostRace,
                    Ok(Err(Error::Fenced(reason))) => Outcome::Fenced(reason),
                    Ok(Err(err)) => {
                        tracing::warn!(error = %err, "commit batch outcome is unknown; its members must not retry");
                        Outcome::Unknown(err.to_string())
                    }
                    Err(_) => {
                        super::invalidate_head_view(&self.projections);
                        Outcome::Unknown(
                            "the submitted batch panicked before acknowledgement".into(),
                        )
                    }
                }
            }
            Ok(Ok(Submission::LostRace)) => Outcome::LostRace,
            Ok(Err(Error::Fenced(reason))) => Outcome::Fenced(reason),
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "commit batch outcome is unknown; its members must not retry");
                Outcome::Unknown(err.to_string())
            }
            Err(_) => {
                super::invalidate_head_view(&self.projections);
                Outcome::Unknown("the submitted batch panicked before acknowledgement".into())
            }
        };
        // Every member may already have gone; the batch landed regardless.
        let _ = outcome.send(BatchState::Finished(landed));
    }

    /// Seals whatever batch is forming, if nothing else will: the path a
    /// caller that vanished before staging leaves behind.
    async fn seal_abandoned(self: Arc<Self>) {
        let mut shared = self.shared.lock().await;
        if self.in_flight.load(Ordering::Acquire) || self.arriving.load(Ordering::Acquire) > 0 {
            return;
        }
        let Some(batch) = shared.forming.take() else {
            return;
        };
        if batch.writes.is_empty() {
            super::rollback(batch.db_tx);
            return;
        }
        let flight = self.enter_flight();
        drop(shared);
        self.land(batch, flight).await;
    }
}

/// Holds the store's one flight slot for as long as a batch needs it.
/// Dropping it admits the next batch, so a submission whose task is
/// dropped or which panics outside the guarded region cannot latch the
/// store closed.
struct Flight(Arc<Coalescer>);

impl Drop for Flight {
    fn drop(&mut self) {
        self.0.in_flight.store(false, Ordering::Release);
        self.0
            .flights
            .send_modify(|flight| *flight = flight.wrapping_add(1));
    }
}

/// Waits for acknowledgement the way [`await_outcome`] does, and seals the
/// batch itself if nothing else will.
///
/// A batch a caller staged onto without sealing is sealed by whichever
/// arrival leaves last, on a task of its own. A task that never runs leaves
/// the batch in `forming` with its outcome sender alive, so every member
/// riding it waits for a message no one will send, against nothing that is
/// running. The waiter cannot be the thing that went missing -- it is the
/// one blocked -- so it takes the work rather than delegating it, and the
/// caller that would otherwise have issued the next commit is exactly the
/// caller stuck here.
pub(crate) async fn await_outcome_sealing(
    coalescer: &Arc<Coalescer>,
    mut outcome: watch::Receiver<BatchState>,
) -> Outcome {
    loop {
        if let BatchState::Finished(landed) = &*outcome.borrow_and_update() {
            return landed.clone();
        }
        match tokio::time::timeout(STALL_INTERVAL, outcome.changed()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return finished_or_boundary(&outcome),
            // `seal_abandoned` stands down while another arrival is on its
            // way in or a batch holds the slot, so this only acts on a batch
            // nothing else will take.
            Err(_) => Arc::clone(coalescer).seal_abandoned().await,
        }
    }
}

/// What a batch's last observed state means once its sender is gone.
fn finished_or_boundary(outcome: &watch::Receiver<BatchState>) -> Outcome {
    match &*outcome.borrow() {
        BatchState::Forming => Outcome::Abandoned,
        BatchState::Submitted => Outcome::Unknown("the submitted batch stopped reporting".into()),
        BatchState::Finished(landed) => landed.clone(),
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

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;
    use crate::{Catalog, CatalogOptions};

    #[tokio::test]
    async fn a_lost_sender_preserves_the_submission_boundary() {
        for (state, expected) in [
            (BatchState::Forming, Outcome::Abandoned),
            (
                BatchState::Submitted,
                Outcome::Unknown("the submitted batch stopped reporting".into()),
            ),
            (BatchState::Finished(Outcome::Committed), Outcome::Committed),
        ] {
            let (sender, receiver) = watch::channel(state);
            drop(sender);
            assert_eq!(finished_or_boundary(&receiver), expected);
        }
    }

    #[tokio::test]
    async fn a_write_error_is_not_reported_as_safe_to_retry() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        let store = catalog.store();
        let db = store.writer_db().unwrap();
        let mut batch = Batch::open(db, catalog.projections()).await.unwrap();
        batch
            .stage(
                &[|tx: &mut Transaction| {
                    tx.create_schema("pending")?;
                    Ok(())
                }],
                catalog.projections(),
            )
            .await
            .unwrap();
        let outcome = batch.outcome.subscribe();
        catalog.close().await.unwrap();
        let coalescer = Arc::new(Coalescer::new(
            Arc::clone(catalog.projections()),
            catalog.store().commit_durability(),
        ));
        let flight = coalescer.enter_flight();
        Arc::clone(&coalescer).land(batch, flight).await;
        assert!(matches!(
            await_outcome_sealing(&coalescer, outcome).await,
            Outcome::Unknown(_)
        ));
    }

    /// A batch left forming by a caller that was not the last one in, whose
    /// sealing task never ran, is sealed by the member waiting on it rather
    /// than stranding that member for the life of the process.
    #[tokio::test(start_paused = true)]
    async fn a_waiter_seals_the_batch_no_one_else_will() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        let store = catalog.store();
        let db = store.writer_db().unwrap();
        // The store opens with no flush timer, so a durability wait has to
        // be paired with an explicit flush. That is what the store's own
        // pacer does; `OnFlushInterval` would wait for a timer that does
        // not exist.
        let coalescer = Arc::new(Coalescer::new(
            Arc::clone(catalog.projections()),
            store.commit_durability(),
        ));

        // Staged, not sealed: exactly what a caller leaves behind when it is
        // not the last arrival in.
        let mut batch = Batch::open(db, catalog.projections()).await.unwrap();
        batch
            .stage(
                &[|tx: &mut Transaction| {
                    tx.create_schema("stranded")?;
                    Ok(())
                }],
                catalog.projections(),
            )
            .await
            .unwrap();
        let outcome = batch.outcome.subscribe();
        coalescer.shared.lock().await.forming = Some(batch);

        // No arrival remains to seal it and no task was spawned to do it, so
        // nothing in the process will move this batch on its own.
        assert_eq!(coalescer.arriving.load(Ordering::Acquire), 0);
        assert!(!coalescer.in_flight.load(Ordering::Acquire));

        let landed = await_outcome_sealing(&coalescer, outcome).await;
        assert!(
            matches!(landed, Outcome::Committed),
            "the waiter must seal and land the batch it is riding, got {landed:?}"
        );
        catalog.close().await.unwrap();
    }

    /// A caller that stages nothing against a forming batch's uncommitted
    /// writes takes the batch's fate: its answer came from those writes,
    /// and the id it would report names a commit that can still lose.
    #[tokio::test]
    async fn a_no_op_member_rides_the_batch_it_read() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        let store = catalog.store();
        let db = store.writer_db().unwrap();
        let coalescer = Arc::new(Coalescer::new(
            Arc::clone(catalog.projections()),
            store.commit_durability(),
        ));

        // Left forming for a caller still on its way in.
        let pending = coalescer.arrive();
        let first = coalescer
            .stage(
                db,
                &[|tx: &mut Transaction| tx.create_schema("a").map(|_| ())],
            )
            .await
            .unwrap();

        // Another writer lands ahead of the batch.
        let rival = Arc::new(Coalescer::new(
            Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
            store.commit_durability(),
        ));
        super::super::commit_cycle(
            db,
            &[|tx: &mut Transaction| tx.create_schema("rival").map(|_| ())],
            &rival,
        )
        .await
        .unwrap();

        // The pending caller finds its schema already in the batch and
        // stages nothing.
        pending.settle();
        let outcome = super::super::attempt_group(
            db,
            &[|tx: &mut Transaction| {
                tx.schema_by_name("a")
                    .map(|_| ())
                    .ok_or_else(|| Error::NotFound("a".into()))
            }],
            &coalescer,
        )
        .await
        .unwrap();

        assert!(
            matches!(outcome, super::super::CommitOutcome::LostRace { .. }),
            "a member that read the batch's writes must share its lost race"
        );
        assert_eq!(
            await_outcome_sealing(&coalescer, first.outcome).await,
            Outcome::LostRace
        );
        catalog.close().await.unwrap();
    }

    /// A flight whose task never finishes still admits the next batch, so
    /// a lost submission cannot close the store to every later commit.
    #[tokio::test]
    async fn a_dropped_flight_admits_the_next_batch() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        let coalescer = Arc::new(Coalescer::new(
            Arc::clone(catalog.projections()),
            catalog.store().commit_durability(),
        ));

        // The slot is claimed and its holder vanishes without landing.
        let flight = coalescer.enter_flight();
        assert!(coalescer.in_flight.load(Ordering::Acquire));
        drop(flight);

        let admitted = tokio::time::timeout(std::time::Duration::from_secs(5), coalescer.admit())
            .await
            .is_ok();
        assert!(admitted, "a dropped flight must not latch the store closed");
        catalog.close().await.unwrap();
    }

    /// Admission is not lost when the flight ends between subscribing and
    /// reading the slot.
    #[tokio::test]
    async fn a_flight_ending_during_admission_still_wakes_the_waiter() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        let coalescer = Arc::new(Coalescer::new(
            Arc::clone(catalog.projections()),
            catalog.store().commit_durability(),
        ));

        let flight = coalescer.enter_flight();
        let waiting = tokio::spawn({
            let coalescer = Arc::clone(&coalescer);
            async move {
                drop(coalescer.admit().await);
            }
        });
        tokio::task::yield_now().await;
        drop(flight);

        tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .expect("the waiter must observe the flight ending")
            .unwrap();
        catalog.close().await.unwrap();
    }
}
