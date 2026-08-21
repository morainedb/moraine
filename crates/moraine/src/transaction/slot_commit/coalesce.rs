//! Group commit at the slot layer: this process's concurrent commits queue
//! here and coalesce into one multi-commit envelope, so N commits cost one
//! slot PUT rather than the ~N²/2 a race for the same sequence would.
//!
//! The closures stay in their callers' futures — each member runs its own
//! closure against the accumulating head when the batch driver asks it to, so
//! nothing that borrows a caller's stack ever crosses to the driver. The
//! driver owns racing the log; the members own assembling their commits.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use moraine_wal::{
    Commit, CommitDrive, Committer, Envelope, Overlay, Race, RetryPolicy, drive_commit,
};
use slatedb::DbReader;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tracing::{debug, warn};
use uuid::Uuid;

use super::{
    Admission, FOLD_STALL_THRESHOLD, SlotHead, admit, apply, classify_lost_race, release_reader,
    revalidated_slot_head, unfolded_tail_at,
};
use crate::{
    catalog::{CatalogSnapshot, SlotStore, SnapshotId},
    error::{Error, Result},
    store::handle::ReadHandle,
    transaction::{
        commit::{Assembled, Prepared, StagedWrite, assemble_commit, fold},
        index_maintenance::ProbeHandle,
        operations::ChangeSet,
        verbs::Transaction,
    },
};

/// Serializes this process's slot attempts and coalesces whatever is waiting
/// into one envelope. Shared across every clone of a catalog handle, so
/// commits from all of them batch together.
pub(crate) struct CommitCoalescer {
    window: Duration,
    shared: Mutex<Shared>,
}

/// The queue and the one-batch-at-a-time flag.
struct Shared {
    waiting: VecDeque<Arc<Member>>,
    driving: bool,
}

impl CommitCoalescer {
    /// A coalescer whose leader waits `window` to accumulate more before it
    /// races. Zero declines to wait but still batches whatever is queued.
    pub(crate) fn new(window: Duration) -> Self {
        Self {
            window,
            shared: Mutex::new(Shared {
                waiting: VecDeque::new(),
                driving: false,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Commits `f` through the batch: leads a new batch if none is running,
    /// else joins the running one and waits for its outcome.
    pub(crate) async fn commit<F>(&self, store: &SlotStore, f: &F) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        if store.read_only {
            return Err(Error::Constraint(
                "catalog attached read-only; writes are unavailable".to_string(),
            ));
        }

        let member = Arc::new(Member::new());
        let lead_now = {
            let mut shared = self.lock();
            shared.waiting.push_back(Arc::clone(&member));
            if shared.driving {
                false
            } else {
                shared.driving = true;
                member.leading.store(true, Ordering::SeqCst);
                true
            }
        };

        // The guard turns a cancelled caller into a dropped batch member rather
        // than a wedged handle: on an early drop it abandons the member, and if
        // the member was leading it hands the baton on.
        let mut guard = Participation {
            coalescer: self,
            member: Arc::clone(&member),
            armed: true,
        };
        let outcome = if lead_now {
            self.lead(store, f, member).await
        } else {
            self.participate(store, f, member).await
        };
        guard.disarm();

        outcome
    }

    /// Hands the baton to the next live waiter, else closes the batch. Shared
    /// by the leader's own end-of-batch handoff and the drop guard's.
    fn hand_off(shared: &mut Shared) {
        while let Some(next) = shared.waiting.pop_front() {
            if next.is_abandoned() {
                continue;
            }
            next.leading.store(true, Ordering::SeqCst);
            next.direct(Directive::Lead);
            next.resume.notify_one();
            return;
        }
        shared.driving = false;
    }

    /// Drives one batch to its slot, then hands the baton on. The leader
    /// contributes its own commit inline and orchestrates the followers.
    async fn lead<F>(&self, store: &SlotStore, f: &F, leader: Arc<Member>) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        if self.window.is_zero() {
            // Let any already-ready commit register before the batch drains,
            // so an opportunistic batch forms without paying a timer.
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(self.window).await;
        }

        // The leader is its own inline member; drop it from the queue so the
        // follower drain never doubles it.
        {
            let mut shared = self.lock();
            shared.waiting.retain(|m| !Arc::ptr_eq(m, &leader));
        }

        let outcome = self.drive_batch(store, f, leader).await;

        // Hand the baton to the next waiter, else close the batch.
        Self::hand_off(&mut self.lock());

        outcome
    }

    async fn drive_batch<F>(
        &self,
        store: &SlotStore,
        f: &F,
        leader: Arc<Member>,
    ) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        let head = match revalidated_slot_head(store).await {
            Ok(head) => head,
            Err(err) => return Err(err),
        };
        let original_head = head.view.snapshot.snapshot_id;
        let start_sequence = head.next_sequence;
        let materialized_tail = unfolded_tail_at(store, start_sequence).await;
        let base = Base::from_head(store, head);

        let mut committer = CoalescingCommitter {
            coalescer: self,
            leader_f: f,
            leader_slot: Slot::new(leader.txid),
            followers: Vec::new(),
            base,
            original_head,
            last_changes: Vec::new(),
            last_envelope: None,
            settled: false,
        };

        let drive = drive_commit(
            &store.slots,
            &mut committer,
            start_sequence,
            &RetryPolicy::default(),
        )
        .await;

        // A successful commit updates the handle's head cache directly with the
        // committed head, so a read on the same handle sees its own write
        // regardless of the refresh window.
        if let Ok(CommitDrive::Committed { sequence, .. }) = &drive {
            committer.cache_committed(store, *sequence);
        }

        narrate_commit(store, &drive, materialized_tail);

        let outcome = committer.settle(&drive);
        if committer.base.owns_reader {
            release_reader(Some(committer.base.reader.as_ref())).await;
        }
        outcome
    }

    /// A follower's life: assemble on request, then take its settled outcome
    /// — or, if the batch it joined ended before it was reached, lead the
    /// next one itself.
    async fn participate<F>(
        &self,
        store: &SlotStore,
        f: &F,
        member: Arc<Member>,
    ) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        loop {
            let directive = member.take_directive();
            match directive {
                Directive::Idle => member.resume.notified().await,
                Directive::Lead => return self.lead(store, f, member).await,
                Directive::Assemble { accum } => {
                    let product = assemble_member(f, member.txid, &accum).await;
                    member.produce(product);
                    member.reply.notify_one();
                    member.resume.notified().await;
                }
                Directive::Settle => return member.take_outcome(),
            }
        }
    }
}

/// Held across a caller's `commit` await. On a normal return it is disarmed and
/// does nothing; on an early drop — a cancelled caller, e.g. a `timeout` that
/// fired — it keeps the handle live: it abandons the member (so a leader
/// awaiting it stops waiting and drops it from the batch), unqueues it, wakes a
/// leader parked on its reply, and, if the member was leading, hands the baton
/// on so `driving` is never left set behind a vanished leader.
///
/// The remaining bounded case: a leader cancelled *during its winning slot PUT*
/// (the one await where the put's fate is unknown) treats the batch as
/// abandoned, so its co-batched members re-lead and re-run against the new
/// head; if the PUT did land, their re-run sees their own committed work and
/// returns `AlreadyExists` rather than success. That is rare (cancellation must
/// coincide with the winning put), never a wedge, and never corruption.
struct Participation<'a> {
    coalescer: &'a CommitCoalescer,
    member: Arc<Member>,
    armed: bool,
}

impl Participation<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for Participation<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.member.abandoned.store(true, Ordering::SeqCst);
        {
            let mut shared = self.coalescer.lock();
            shared.waiting.retain(|m| !Arc::ptr_eq(m, &self.member));
            if self.member.leading.load(Ordering::SeqCst) {
                CommitCoalescer::hand_off(&mut shared);
            }
        }
        // A leader may be parked on this member's reply; wake it to re-check and
        // find it abandoned.
        self.member.reply.notify_one();
    }
}

/// The evolving head a batch assembles against: the folded view plus every
/// slot the driver has absorbed on the way.
struct Base {
    view: CatalogSnapshot,
    overlay: Overlay,
    reader: Arc<DbReader>,
    /// The attach's projection cache, carried so an assembling member can
    /// consult the format floor.
    projections: Arc<std::sync::RwLock<crate::catalog::projection::ProjectionCache>>,
    /// Whether `reader` is one this batch opened past a truncated prefix, so
    /// closing it is the batch's to do.
    owns_reader: bool,
}

impl Base {
    fn from_head(store: &SlotStore, head: SlotHead) -> Self {
        let SlotHead {
            view,
            overlay,
            next_sequence: _,
            reader,
        } = head;
        match reader {
            Some(reader) => Self {
                view,
                overlay,
                reader: Arc::new(reader),
                projections: Arc::clone(&store.projections),
                owns_reader: true,
            },
            None => Self {
                view,
                overlay,
                reader: Arc::clone(&store.reader),
                projections: Arc::clone(&store.projections),
                owns_reader: false,
            },
        }
    }
}

/// One member's per-batch bookkeeping the driver keeps.
struct Slot {
    txid: Uuid,
    /// The snapshot the member's last assembly minted, meaningful only while
    /// `terminal` is `None`.
    minted: u64,
    /// Whether the member contributed a commit to the last round's envelope.
    staged: bool,
    /// Set once the member is out of the envelope for good: nothing to commit,
    /// or its own closure failed on re-run.
    terminal: Option<Result<SnapshotId>>,
}

impl Slot {
    fn new(txid: Uuid) -> Self {
        Self {
            txid,
            minted: 0,
            staged: false,
            terminal: None,
        }
    }
}

/// A follower and the driver's bookkeeping for it.
struct Follower {
    member: Arc<Member>,
    slot: Slot,
}

/// The batch driver plugged into the log's commit loop: each `assemble`
/// re-runs every live member against the accumulating head and returns the
/// whole batch as one envelope.
struct CoalescingCommitter<'a, F> {
    coalescer: &'a CommitCoalescer,
    leader_f: &'a F,
    leader_slot: Slot,
    followers: Vec<Follower>,
    base: Base,
    original_head: u64,
    // The change set of every member that staged this round, for judging a
    // lost race.
    last_changes: Vec<ChangeSet>,
    /// The envelope the last round assembled. On a win the driver returns
    /// without absorbing it (absorb runs only on a lost race), so the cache
    /// update applies it onto the base to reconstruct the committed head. Valid
    /// only while this process's coalescer is the sole assembler of its ids.
    last_envelope: Option<Envelope>,
    /// Set once [`settle`](Self::settle) has delivered outcomes. Its drop guard
    /// re-queues live followers only when this is unset — i.e. when the
    /// leader's own future was dropped mid-batch before it could settle
    /// them.
    settled: bool,
}

impl<F> CoalescingCommitter<'_, F> {
    /// Admits every commit that queued since the last round, so a joiner
    /// inherits the batch's attempt count rather than resetting it. A member
    /// cancelled while queued is let go here rather than admitted.
    fn admit_joiners(&mut self) {
        let mut shared = self.coalescer.lock();
        while let Some(member) = shared.waiting.pop_front() {
            if member.is_abandoned() {
                continue;
            }
            let slot = Slot::new(member.txid);
            self.followers.push(Follower { member, slot });
        }
    }

    /// Reconstructs the committed head and records it in the handle's cache.
    /// The base already carries every benign-lost winner (absorbed on the way);
    /// applying the winning envelope onto it yields the head this commit
    /// produced. A batch whose apply cannot chain clears the cache rather than
    /// caching a wrong head — the next read re-materializes.
    fn cache_committed(&mut self, store: &SlotStore, sequence: u64) {
        let Some(envelope) = self.last_envelope.take() else {
            return;
        };
        for commit in &envelope.commits {
            match admit(&self.base.view, commit, sequence) {
                Ok(Admission::Apply) => {
                    if apply(&mut self.base.view, commit, sequence).is_err() {
                        store.head_cache.invalidate();
                        return;
                    }
                }
                Ok(Admission::Skip) => {}
                Err(_) => {
                    store.head_cache.invalidate();
                    return;
                }
            }
        }
        self.base.overlay.absorb(&envelope);
        store.head_cache.record(
            &self.base.view,
            &self.base.overlay,
            sequence.saturating_add(1),
        );
    }

    /// Distributes the batch's outcome to every follower and returns the
    /// leader's own. A cancelled follower has no caller to serve and is
    /// skipped.
    fn settle(&mut self, drive: &Result<CommitDrive>) -> Result<SnapshotId> {
        self.settled = true;
        let verdict = Verdict::of(drive);
        let leader = outcome_for(&mut self.leader_slot, &verdict, self.original_head);

        for follower in &mut self.followers {
            if follower.member.is_abandoned() {
                continue;
            }
            let outcome = outcome_for(&mut follower.slot, &verdict, self.original_head);
            follower.member.settle(outcome);
            follower.member.resume.notify_one();
        }

        leader
    }
}

impl<F> Drop for CoalescingCommitter<'_, F> {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        // The leader's own commit future was dropped mid-batch. Return its live
        // followers to the queue so a promoted leader drives them; each is
        // parked awaiting `resume` and re-reads its directive on the next wake
        // (the promoted leader's `admit_joiners`/`request_assemble`, or the
        // baton the drop guard hands on). Cancelled followers are let go.
        let mut shared = self.coalescer.lock();
        for follower in self.followers.drain(..) {
            if !follower.member.is_abandoned() {
                shared.waiting.push_back(follower.member);
            }
        }
    }
}

impl<F> Committer for CoalescingCommitter<'_, F>
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    type Error = Error;

    async fn assemble(&mut self) -> Result<Option<Envelope>> {
        self.admit_joiners();
        self.last_changes.clear();

        let accum = Arc::new(AsyncMutex::new(Accum {
            view: self.base.view.clone(),
            overlay: self.base.overlay.clone(),
            reader: Arc::clone(&self.base.reader),
            commits: Vec::new(),
            projections: Arc::clone(&self.base.projections),
        }));

        if self.leader_slot.terminal.is_none() {
            let product = assemble_member(self.leader_f, self.leader_slot.txid, &accum).await;
            record(
                &mut self.leader_slot,
                product,
                &mut self.last_changes,
                self.original_head,
            );
        }

        for index in 0..self.followers.len() {
            if self.followers[index].slot.terminal.is_some()
                || self.followers[index].member.is_abandoned()
            {
                continue;
            }
            let member = Arc::clone(&self.followers[index].member);
            // A follower cancelled mid-assembly returns `None`: it leaves the
            // batch without folding into the accum, so the survivors stay
            // contiguous — the same isolation a failing member gets.
            if let Some(product) = request_assemble(&member, Arc::clone(&accum)).await {
                record(
                    &mut self.followers[index].slot,
                    product,
                    &mut self.last_changes,
                    self.original_head,
                );
            }
        }

        let commits = accum.lock().await.commits.clone();
        if commits.is_empty() {
            self.last_envelope = None;
            Ok(None)
        } else {
            let envelope = Envelope {
                leader: None,
                commits,
            };
            self.last_envelope = Some(envelope.clone());
            Ok(Some(envelope))
        }
    }

    fn classify(&self, winner: &Envelope) -> Race {
        for ours in &self.last_changes {
            if let Race::Conflict = classify_lost_race(Some(ours), winner) {
                return Race::Conflict;
            }
        }
        Race::Benign
    }

    fn absorb(&mut self, sequence: u64, winner: Envelope) -> Result<()> {
        for commit in &winner.commits {
            match admit(&self.base.view, commit, sequence)? {
                Admission::Apply => apply(&mut self.base.view, commit, sequence)?,
                Admission::Skip => {}
            }
        }
        self.base.overlay.absorb(&winner);
        Ok(())
    }
}

/// Folds one member's product into its slot bookkeeping and the round's change
/// list.
fn record(slot: &mut Slot, product: Product, changes: &mut Vec<ChangeSet>, original_head: u64) {
    match product {
        Product::Staged { minted, ours } => {
            slot.minted = minted;
            slot.staged = true;
            changes.push(*ours);
        }
        Product::Nothing => {
            slot.staged = false;
            slot.terminal = Some(Ok(SnapshotId::new(original_head)));
        }
        Product::Failed(err) => {
            slot.staged = false;
            slot.terminal = Some(Err(err));
        }
    }
}

/// The verdict a batch reached, owned so every member can be mapped from it.
enum Verdict {
    Committed,
    Nothing,
    Conflict {
        sequence: u64,
    },
    Exhausted {
        attempts: usize,
        last_sequence: u64,
    },
    Unavailable {
        attempts: usize,
        last_sequence: u64,
        last_error: String,
    },
    Failed(String),
}

impl Verdict {
    fn of(drive: &Result<CommitDrive>) -> Self {
        match drive {
            Ok(CommitDrive::Committed { .. }) => Self::Committed,
            Ok(CommitDrive::Nothing) => Self::Nothing,
            Ok(CommitDrive::Conflict { sequence, .. }) => Self::Conflict {
                sequence: *sequence,
            },
            Ok(CommitDrive::Exhausted {
                attempts,
                last_sequence,
            }) => Self::Exhausted {
                attempts: *attempts,
                last_sequence: *last_sequence,
            },
            Ok(CommitDrive::Unavailable {
                attempts,
                last_sequence,
                last_error,
            }) => Self::Unavailable {
                attempts: *attempts,
                last_sequence: *last_sequence,
                last_error: last_error.to_string(),
            },
            Err(err) => Self::Failed(err.to_string()),
        }
    }
}

/// Records one batch's contention on the handle's counters and narrates it. The
/// driver reports the numbers; moraine emits them, so the protocol crate stays
/// silent. Per-commit facts go at `debug`; the operator signals — a spent
/// budget, and fold lag past the stall threshold — go at `warn`.
fn narrate_commit(store: &SlotStore, drive: &Result<CommitDrive>, materialized_tail: u64) {
    match drive {
        Ok(CommitDrive::Committed {
            sequence,
            attempts,
            races_lost,
        }) => {
            store.contention.record_committed(*races_lost as u64);
            debug!(
                sequence = *sequence,
                attempts = *attempts,
                races_lost = *races_lost,
                materialized_tail,
                "commit batch won its slot"
            );
        }
        Ok(CommitDrive::Exhausted {
            attempts,
            last_sequence,
        }) => {
            store.contention.record_exhausted();
            warn!(
                attempts = *attempts,
                last_sequence = *last_sequence,
                materialized_tail,
                "commit batch spent its retry budget on lost slot races"
            );
        }
        _ => {}
    }

    if materialized_tail > FOLD_STALL_THRESHOLD {
        warn!(
            materialized_tail,
            threshold = FOLD_STALL_THRESHOLD,
            "unfolded slot tail exceeds the fold-stall threshold; folding lags commits"
        );
    }
}

/// One member's outcome: its own terminal result if it left the envelope, else
/// whatever the batch as a whole settled to. Consumes the slot's terminal —
/// each member is settled exactly once, so its own error moves to it verbatim
/// rather than being cloned and reclassified.
fn outcome_for(slot: &mut Slot, verdict: &Verdict, original_head: u64) -> Result<SnapshotId> {
    if let Some(terminal) = slot.terminal.take() {
        return terminal;
    }

    match verdict {
        Verdict::Committed => Ok(SnapshotId::new(slot.minted)),
        Verdict::Nothing => Ok(SnapshotId::new(original_head)),
        Verdict::Conflict { sequence } => Err(Error::CommitConflict(format!(
            "a concurrent commit won slot {sequence} from head snapshot {original_head} and \
             conflicts with this transaction"
        ))),
        Verdict::Exhausted {
            attempts,
            last_sequence,
        } => Err(Error::RetryBudgetExhausted(format!(
            "spent {attempts} attempts from head snapshot {original_head} without settling; \
             last raced slot {last_sequence}"
        ))),
        Verdict::Unavailable {
            attempts,
            last_sequence,
            last_error,
        } => Err(Error::SlotLog(format!(
            "commit-slot log unreachable after {attempts} attempts from head snapshot \
             {original_head} (last raced slot {last_sequence}): {last_error}"
        ))),
        Verdict::Failed(text) => Err(Error::Corruption(text.clone())),
    }
}

/// One assembly's result for a member.
enum Product {
    Staged { minted: u64, ours: Box<ChangeSet> },
    Nothing,
    Failed(Error),
}

/// The accumulating head a round chains onto: each member folds its writes in
/// before the next assembles, so member k+1's premise is member k's minted
/// snapshot.
struct Accum {
    view: CatalogSnapshot,
    overlay: Overlay,
    reader: Arc<DbReader>,
    commits: Vec<Commit>,
    /// The attach's projection cache, for the format floor the inline
    /// translation consults.
    projections: Arc<std::sync::RwLock<crate::catalog::projection::ProjectionCache>>,
}

/// Runs one member's closure against the accumulating head and folds its
/// commit in. Holds the accumulator across its own read, so members chain in
/// call order.
async fn assemble_member<F>(f: &F, txid: Uuid, accum: &AsyncMutex<Accum>) -> Product
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    let mut accum = accum.lock().await;
    let Accum {
        view,
        overlay,
        reader,
        commits,
        projections,
    } = &mut *accum;

    let probe = ProbeHandle::Overlaid {
        store: ReadHandle::Reader(reader.as_ref()),
        overlay,
    };
    let projections = &*projections;
    match assemble_commit(probe, f, view, projections, None, Some(txid.into_bytes())).await {
        Ok(Prepared::Nothing) => Product::Nothing,
        Ok(Prepared::Staged(assembled)) => {
            let commit = commit_from(txid, &assembled);
            let writes: Vec<StagedWrite> = assembled.writes.clone();
            if let Err(err) = fold::fold_batch(view, &writes) {
                return Product::Failed(Error::Corruption(format!(
                    "a coalesced commit could not chain onto its batch: {err}"
                )));
            }
            let folded = Envelope {
                leader: None,
                commits: vec![commit.clone()],
            };
            overlay.absorb(&folded);
            commits.push(commit);
            Product::Staged {
                minted: assembled.commits,
                ours: assembled.ours,
            }
        }
        Err(err) => Product::Failed(err),
    }
}

/// The one-commit shape a member contributes to the shared envelope.
fn commit_from(txid: Uuid, assembled: &Assembled) -> Commit {
    super::commit_from(
        txid.into_bytes(),
        assembled.head_before,
        assembled.ours.to_changes_made(),
        &assembled.writes,
    )
}

/// Asks one follower to assemble against `accum` and waits for its product, or
/// `None` if the follower's caller was cancelled before it produced one — its
/// drop guard sets `abandoned` and wakes this `reply`, so the wait never hangs
/// on a gone future.
async fn request_assemble(member: &Arc<Member>, accum: Arc<AsyncMutex<Accum>>) -> Option<Product> {
    member.direct(Directive::Assemble { accum });
    member.resume.notify_one();
    loop {
        if let Some(product) = member.take_product() {
            return Some(product);
        }
        if member.is_abandoned() {
            return None;
        }
        member.reply.notified().await;
    }
}

/// One queued commit's control block, shared between its own future and the
/// batch driver. The closure never appears here — it stays in the future.
struct Member {
    txid: Uuid,
    cell: Mutex<Cell>,
    resume: Notify,
    reply: Notify,
    /// Set when the member's own `commit` future was dropped before it
    /// completed — a cancelled caller. A cancelled member is dropped from the
    /// batch exactly like one whose closure failed: the leader stops waiting on
    /// it and the survivors commit.
    abandoned: AtomicBool,
    /// Whether this member currently holds the baton. Read by the drop guard to
    /// decide, on cancellation, whether it must hand the baton on rather than
    /// leave the handle wedged behind a dropped leader.
    leading: AtomicBool,
}

/// The mutable half of a member, always locked without an await held.
struct Cell {
    directive: Directive,
    product: Option<Product>,
    outcome: Option<Result<SnapshotId>>,
}

/// What the driver last told a member to do.
#[derive(Default)]
enum Directive {
    #[default]
    Idle,
    Lead,
    Assemble {
        accum: Arc<AsyncMutex<Accum>>,
    },
    Settle,
}

impl Member {
    fn new() -> Self {
        Self {
            txid: Uuid::new_v4(),
            cell: Mutex::new(Cell {
                directive: Directive::Idle,
                product: None,
                outcome: None,
            }),
            resume: Notify::new(),
            reply: Notify::new(),
            abandoned: AtomicBool::new(false),
            leading: AtomicBool::new(false),
        }
    }

    fn is_abandoned(&self) -> bool {
        self.abandoned.load(Ordering::SeqCst)
    }

    fn cell(&self) -> MutexGuard<'_, Cell> {
        self.cell.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn direct(&self, directive: Directive) {
        let mut cell = self.cell();
        cell.product = None;
        cell.directive = directive;
    }

    fn take_directive(&self) -> Directive {
        std::mem::take(&mut self.cell().directive)
    }

    fn produce(&self, product: Product) {
        self.cell().product = Some(product);
    }

    fn take_product(&self) -> Option<Product> {
        self.cell().product.take()
    }

    fn settle(&self, outcome: Result<SnapshotId>) {
        let mut cell = self.cell();
        cell.outcome = Some(outcome);
        cell.directive = Directive::Settle;
    }

    fn take_outcome(&self) -> Result<SnapshotId> {
        self.cell().outcome.take().unwrap_or_else(|| {
            Err(Error::Corruption(
                "a coalesced commit was settled with no outcome".to_string(),
            ))
        })
    }
}
