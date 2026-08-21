//! The leader's group-commit funnel: forwarded sessions' commits chain onto
//! one another and coalesce into one multi-commit envelope, so several clients
//! that read their premise through the leader cost one slot PUT.
//!
//! The chaining is by construction. Each session pins the funnel's logical
//! head at `begin`, and a commit is admitted only when it was authored against
//! that head — so an admitted commit advances the head in memory, and the next
//! session's read already reflects it. A batch of admitted commits therefore
//! replays as a valid chain (each commit's `validated_head` is the previous
//! one's result), which is exactly what lets them share a slot. This differs
//! from the verb coalescer, which re-runs closures against the accumulating
//! head; a forwarded session's rows are DuckLake-authored and cannot be re-run.
//!
//! No acknowledgement precedes a durable slot PUT: `submit` returns only once
//! the batch's `commit_slot` has acked, so a leader answers `Committed` after
//! the object landed, never before.

use std::{sync::Arc, time::Duration};

use moraine_wal::{
    Commit, CommitDrive, CommitOutcome, Committer, Envelope, LeaderAdvert, Overlay, Race,
    RetryPolicy, SlotLog, drive_commit,
};
use slatedb::DbReader;
use tokio::sync::{Mutex, Notify, oneshot};
use uuid::Uuid;

use crate::{
    catalog::{Catalog, CatalogSnapshot, SnapshotId},
    error::{Error, Result},
    transaction::{
        commit::fold,
        slot_commit::{commit_from, release_reader, revalidated_slot_head},
        staged::Assembly,
    },
};

/// A bound on the slot races an advert write makes before it gives up on a
/// contended log. A leader is the sole writer on a healthy log, so this is
/// only reached under heavy foreign contention.
const ADVERT_RACE_ATTEMPTS: usize = 16;

/// The funnel's retry shape: near-zero backoff so a sustained leader re-races a
/// contended slot hot, rather than yielding the sequence to a direct client
/// that backs off the standard jittered step. Both bounds are zero — the
/// retries this drives are ambiguous puts and contended-slot transport blips,
/// where a warm re-race costs nothing a starved client feels.
fn hot_retry() -> RetryPolicy {
    RetryPolicy {
        base_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        ..RetryPolicy::default()
    }
}

/// Drives the funnel's batch through the commit loop. A forwarded commit is
/// authored by its client against a fixed head and its rows carry
/// DuckLake-minted ids, so it cannot rebase onto a foreign winner: a lost race
/// is always a conflict the members re-forward, never a benign loss the funnel
/// re-chains. The loop's value here is the ambiguous-put resolution (a batch
/// whose slot landed while the ack was lost resolves as committed, not
/// conflicted) and the hot re-race of a contended transport blip.
struct FunnelCommitter {
    envelope: Envelope,
}

impl Committer for FunnelCommitter {
    type Error = Error;

    async fn assemble(&mut self) -> Result<Option<Envelope>> {
        if self.envelope.commits.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.envelope.clone()))
        }
    }

    fn classify(&self, _winner: &Envelope) -> Race {
        Race::Conflict
    }

    fn absorb(&mut self, _sequence: u64, _winner: Envelope) -> Result<()> {
        Ok(())
    }
}

/// A pinned logical head a session reads and authors against.
pub(crate) struct PinnedHead {
    pub(crate) view: CatalogSnapshot,
    pub(crate) overlay: Overlay,
    pub(crate) next_sequence: u64,
    pub(crate) reader: Arc<DbReader>,
}

/// The funnel's in-memory logical head: the folded view plus every commit
/// admitted-but-not-yet-durable, and the next slot sequence a batch races.
struct LogicalHead {
    view: CatalogSnapshot,
    overlay: Overlay,
    next_sequence: u64,
    reader: Arc<DbReader>,
}

/// One admitted commit awaiting its batch's durable PUT.
struct Pending {
    commit: Commit,
    snapshot_id: u64,
    result: oneshot::Sender<Result<SnapshotId>>,
}

/// The funnel's mutable state, guarded by one async mutex.
struct Inner {
    /// `None` forces a re-materialization: at start, and after a lost race
    /// invalidates the optimistically advanced head.
    base: Option<LogicalHead>,
    /// Commits admitted to the open batch, applied to `base` already.
    batch: Vec<Pending>,
    /// Whether a flush task is scheduled for the open batch.
    flush_scheduled: bool,
    /// Whether a batch is being raced right now; submits wait it out so only
    /// one optimistic advance is ever outstanding.
    flushing: bool,
}

/// The leader's commit funnel. Cloneable through its `Arc`; every session and
/// the flush tasks share the one queue.
pub(crate) struct CommitFunnel {
    catalog: Arc<Catalog>,
    slots: SlotLog,
    window: Duration,
    inner: Mutex<Inner>,
    /// Woken when a flush clears `flushing`, so waiting submits re-check.
    flush_done: Notify,
    stats: Arc<crate::leader::LeaderStats>,
}

impl CommitFunnel {
    /// A funnel over `catalog`, batching within `window`, publishing landed
    /// commit counts to `stats`.
    pub(crate) fn new(
        catalog: Arc<Catalog>,
        window: Duration,
        stats: Arc<crate::leader::LeaderStats>,
    ) -> Arc<Self> {
        let slots = catalog.slot_store().slots.clone();
        Arc::new(Self {
            catalog,
            slots,
            window,
            inner: Mutex::new(Inner {
                base: None,
                batch: Vec::new(),
                flush_scheduled: false,
                flushing: false,
            }),
            flush_done: Notify::new(),
            stats,
        })
    }

    /// The current logical head a session pins at `begin`: the folded view plus
    /// every commit already admitted to the open batch, so a later session
    /// authors against the earlier one's result and their commits chain.
    pub(crate) async fn pin_head(self: &Arc<Self>) -> Result<PinnedHead> {
        let mut inner = self.inner.lock().await;
        let base = self.ensure_base(&mut inner).await?;
        Ok(PinnedHead {
            view: base.view.clone(),
            overlay: base.overlay.clone(),
            next_sequence: base.next_sequence,
            reader: Arc::clone(&base.reader),
        })
    }

    /// Admits `assembly` — authored against `validated_head` — into the open
    /// batch and returns once its batch's slot PUT is durable. A commit that
    /// no longer chains onto the funnel head (a peer advanced it first)
    /// surfaces as [`Error::CommitConflict`]: the client re-forwards.
    pub(crate) async fn submit(
        self: &Arc<Self>,
        validated_head: u64,
        assembly: Assembly,
    ) -> Result<SnapshotId> {
        let rx = loop {
            let mut inner = self.inner.lock().await;
            if inner.flushing {
                drop(inner);
                self.flush_done.notified().await;
                continue;
            }

            let base = self.ensure_base(&mut inner).await?;
            if validated_head != base.view.snapshot.snapshot_id {
                return Err(Error::CommitConflict(format!(
                    "this transaction was authored against head snapshot {validated_head} but the \
                     leader has advanced to {}; re-drive it against the current head",
                    base.view.snapshot.snapshot_id
                )));
            }

            let transaction_id = assembly
                .transaction_id
                .unwrap_or_else(|| Uuid::new_v4().into_bytes());
            let snapshot_id = assembly.result_id;
            let commit = commit_from(
                transaction_id,
                validated_head,
                assembly.changes_made,
                &assembly.writes,
            );

            fold::fold_batch(&mut base.view, &assembly.writes).map_err(|err| {
                Error::Corruption(format!(
                    "a forwarded commit could not chain onto the batch: {err}"
                ))
            })?;
            base.overlay.absorb(&Envelope::new(vec![commit.clone()]));

            let (tx, rx) = oneshot::channel();
            inner.batch.push(Pending {
                commit,
                snapshot_id,
                result: tx,
            });
            self.schedule_flush(&mut inner);
            break rx;
        };

        rx.await.map_err(|_| {
            Error::SlotLog("the leader funnel dropped a commit before it landed".into())
        })?
    }

    /// Writes an advert-carrying slot and returns once it is durable — a
    /// standalone envelope so `announce`/`withdraw` do not wait on a client
    /// commit. Retries a contended slot at the next sequence.
    pub(crate) async fn write_advert(self: &Arc<Self>, advert: LeaderAdvert) -> Result<()> {
        for _ in 0..ADVERT_RACE_ATTEMPTS {
            let sequence = loop {
                let mut inner = self.inner.lock().await;
                if inner.flushing {
                    drop(inner);
                    self.flush_done.notified().await;
                    continue;
                }
                inner.flushing = true;
                let base = self.ensure_base(&mut inner).await?;
                break base.next_sequence;
            };

            let envelope = Envelope::new(Vec::new()).with_advert(advert.clone());
            let outcome = self.slots.commit_slot(sequence, &envelope).await;

            let mut inner = self.inner.lock().await;
            inner.flushing = false;
            self.flush_done.notify_waiters();
            match outcome {
                Ok(CommitOutcome::Won) => {
                    if let Some(base) = inner.base.as_mut() {
                        base.next_sequence = sequence.saturating_add(1);
                    }
                    return Ok(());
                }
                // An empty advert envelope has no identity, so a taken slot is
                // a contended race the log reports as transport: re-materialize
                // for a fresh frontier and try the next sequence.
                Ok(CommitOutcome::Lost(_)) | Err(moraine_wal::Error::Transport(_)) => {
                    inner.base = None;
                }
                Err(err) => return Err(err.into()),
            }
        }

        Err(Error::SlotLog(format!(
            "could not place a leader advert after {ADVERT_RACE_ATTEMPTS} contended slot races"
        )))
    }

    /// Ensures a fresh flush task is scheduled for the open batch.
    fn schedule_flush(self: &Arc<Self>, inner: &mut Inner) {
        if inner.flush_scheduled {
            return;
        }
        inner.flush_scheduled = true;
        let funnel = Arc::clone(self);
        tokio::spawn(async move { funnel.flush().await });
    }

    /// Waits the batch window, then races the whole open batch as one envelope
    /// and delivers each member its outcome — never before the PUT is durable.
    async fn flush(self: Arc<Self>) {
        tokio::time::sleep(self.window).await;

        let (round, sequence) = {
            let mut inner = self.inner.lock().await;
            inner.flushing = true;
            inner.flush_scheduled = false;
            let round = std::mem::take(&mut inner.batch);
            let sequence = inner.base.as_ref().map_or(1, |base| base.next_sequence);
            (round, sequence)
        };

        if round.is_empty() {
            let mut inner = self.inner.lock().await;
            inner.flushing = false;
            drop(inner);
            self.flush_done.notify_waiters();
            return;
        }

        let envelope = Envelope::new(round.iter().map(|pending| pending.commit.clone()).collect());
        let mut committer = FunnelCommitter { envelope };
        let drive = drive_commit(&self.slots, &mut committer, sequence, &hot_retry()).await;

        {
            let mut inner = self.inner.lock().await;
            inner.flushing = false;
            match &drive {
                Ok(CommitDrive::Committed { sequence: won, .. }) => {
                    if let Some(base) = inner.base.as_mut() {
                        base.next_sequence = won.saturating_add(1);
                        // The winning envelope holds every admitted client's
                        // commit, and the base already applied each of them, so
                        // recording it keeps the handle's commit cache correct
                        // for a multi-client envelope — not just this process's
                        // own last-assembled one.
                        self.catalog.slot_store().head_cache.record(
                            &base.view,
                            &base.overlay,
                            base.next_sequence,
                        );
                    }
                }
                // The optimistic advance is void: re-materialize next round.
                _ => inner.base = None,
            }
        }
        self.flush_done.notify_waiters();

        if let Ok(CommitDrive::Committed { .. }) = &drive {
            self.stats
                .record_forwarded(u64::try_from(round.len()).unwrap_or(u64::MAX));
        }

        deliver(round, &drive, sequence);
    }

    /// The logical head, materialized from the store on first use and after a
    /// lost race voided the optimistic advance.
    async fn ensure_base<'a>(&self, inner: &'a mut Inner) -> Result<&'a mut LogicalHead> {
        if inner.base.is_none() {
            let store = self.catalog.slot_store();
            let head = revalidated_slot_head(store).await?;
            let reader = Arc::clone(&store.reader);
            release_reader(head.reader.as_ref()).await;
            inner.base = Some(LogicalHead {
                view: head.view,
                overlay: head.overlay,
                next_sequence: head.next_sequence,
                reader,
            });
        }
        inner
            .base
            .as_mut()
            .ok_or_else(|| Error::Corruption("leader funnel lost its logical head".into()))
    }
}

/// Delivers a driven batch's outcome to every member. A member whose receiver
/// was dropped (a disconnected client) is skipped; its commit still landed.
/// A spent retry budget and an unreachable log both surface as their own error;
/// a lost race is a conflict the client re-forwards.
fn deliver(round: Vec<Pending>, drive: &Result<CommitDrive>, sequence: u64) {
    for pending in round {
        let member = match drive {
            Ok(CommitDrive::Committed { .. }) => Ok(SnapshotId::new(pending.snapshot_id)),
            Ok(CommitDrive::Conflict { sequence, .. }) => Err(Error::CommitConflict(format!(
                "a concurrent commit won slot {sequence}; this forwarded transaction must re-drive"
            ))),
            Ok(CommitDrive::Exhausted {
                attempts,
                last_sequence,
            }) => Err(Error::RetryBudgetExhausted(format!(
                "the leader spent {attempts} attempts on lost slot races; last raced slot \
                 {last_sequence}"
            ))),
            Ok(CommitDrive::Unavailable {
                attempts,
                last_sequence,
                last_error,
            }) => Err(Error::SlotLog(format!(
                "the leader could not reach the slot log after {attempts} attempts; last raced \
                 slot {last_sequence}: {last_error}"
            ))),
            // Assembly always yields the taken batch, so `Nothing` cannot arise
            // for a non-empty round; report it rather than silently succeed.
            Ok(CommitDrive::Nothing) => Err(Error::SlotLog(format!(
                "the leader funnel assembled nothing for slot {sequence}"
            ))),
            Err(err) => Err(Error::SlotLog(format!(
                "the leader could not drive slot {sequence}: {err}"
            ))),
        };
        let _ = pending.result.send(member);
    }
}
