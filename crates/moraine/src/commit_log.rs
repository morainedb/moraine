//! The plug boundary for a bucket-resident commit log: a library that keeps
//! a store's commits as slot objects supplies SlateDB write-ahead-log plugs
//! and a committer here, and the catalog opens its stores over them. This
//! crate holds the boundary and nothing of the log; a store stamped for the
//! log cannot be opened without a provider.
//!
//! A host supplies a provider one of two ways: per open, through
//! [`CatalogOptions::commit_log`](crate::CatalogOptions::commit_log), or
//! once per process, through [`register_commit_log`], which is how a
//! library linked into the DuckDB extension reaches an attach it never sees
//! the options of. The per-open provider outranks the registered one.
//!
//! Every byte that crosses the boundary is opaque to the library: a
//! [`SlotWrite`] is a key and a value the catalog encoded, and a
//! [`SlotCommit`] is a batch of them with what it was validated against.

use std::{
    fmt,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use object_store::ObjectStore;
use slatedb::wal::{WalGc, WalReader, WriterInit};

use crate::error::{Error, Result};

/// One write a slot carries: a key and its value, or a delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotWrite {
    /// The store key, as the catalog encodes it.
    pub key: Vec<u8>,
    /// The value written, or `None` to delete the key.
    pub value: Option<Vec<u8>>,
}

/// One commit offered to the log: a batch of writes under an id the
/// committer minted, with the head it was validated against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotCommit {
    /// The committer-minted id, unique across every committer sharing the
    /// log; an ambiguous put is resolved by matching it.
    pub transaction_id: [u8; 16],
    /// The snapshot id this commit was validated against.
    pub validated_head: u64,
    /// The commit's DuckLake `changes_made` classification, if it minted a
    /// snapshot; empty otherwise.
    pub changes_made: String,
    /// The writes, in the order they apply.
    pub writes: Vec<SlotWrite>,
}

/// What one slot holds: the commits that won it, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotEnvelope {
    /// The commits, in the order they apply.
    pub commits: Vec<SlotCommit>,
}

impl SlotEnvelope {
    /// Every write the envelope's commits carry, in order.
    pub fn writes(&self) -> impl Iterator<Item = &SlotWrite> + '_ {
        self.commits.iter().flat_map(|commit| commit.writes.iter())
    }
}

/// How an offer ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotOffer {
    /// The offered commits hold `sequence`. Apply them to the store, then
    /// report the sequence folded.
    Won {
        /// The sequence won.
        sequence: u64,
    },
    /// Other committers hold the sequence raced and possibly more beyond it.
    /// Fold every slot into the store, in order, before offering again.
    Lost {
        /// The slots landed from the sequence raced, in order.
        slots: Vec<(u64, SlotEnvelope)>,
    },
}

/// How an adoption ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotAdoption {
    /// This process's stamp holds the first slot: apply it, then report it
    /// folded.
    Adopted,
    /// A peer's stamp holds the first slot; its envelope is returned to
    /// apply and report folded instead.
    AdoptedByPeer(SlotEnvelope),
}

/// The log's commit side, for one store. Sequences are the log's: an offer
/// races the one after the last slot this process has folded or been served,
/// and a lost race hands back what to fold before the next.
#[async_trait]
pub trait SlotCommitter: fmt::Debug + Send + Sync + 'static {
    /// Offers `commits` as the slot at `sequence`, which the caller derives
    /// from the state the commits were prepared against: a stale state
    /// names a taken slot and loses, so the race is what guards it.
    ///
    /// # Errors
    ///
    /// [`Error::CommitOutcomeUnknown`] when the library could not reach the
    /// log or resolve a put whose outcome it could not attribute;
    /// [`Error::Corruption`] when a slot is not a valid envelope.
    async fn offer(&self, sequence: u64, commits: Vec<SlotCommit>) -> Result<SlotOffer>;

    /// Whether the log has been adopted: a store's stamp holds its first
    /// slot. A store not yet adopted keeps its commits in its own log,
    /// which must be drained into the tree before the plug replaces it.
    ///
    /// # Errors
    ///
    /// As [`offer`](Self::offer).
    async fn adopted(&self) -> Result<bool>;

    /// Races `stamp` at the log's first sequence, adopting the log for a
    /// store that has none yet. Slots are numbered past the store's own
    /// log: the plug records the store's next WAL id at open, and adoption
    /// fixes it as the base every slot's WAL id counts from.
    ///
    /// # Errors
    ///
    /// As [`offer`](Self::offer).
    async fn adopt(&self, stamp: SlotCommit) -> Result<SlotAdoption>;

    /// The slots landed past the last this process has folded, in order,
    /// whatever its reader has been served: what the store it holds still
    /// lacks.
    ///
    /// # Errors
    ///
    /// As [`offer`](Self::offer); a hole in the tail is [`Error::Corruption`].
    async fn tail(&self) -> Result<Vec<(u64, SlotEnvelope)>>;

    /// How many slots lie past `replay_point`, the manifest's last folded
    /// WAL id: what a cold open of the store would replay. One listing.
    ///
    /// # Errors
    ///
    /// As [`offer`](Self::offer).
    async fn unfolded(&self, replay_point: u64) -> Result<u64>;

    /// The row sequence numbers one slot reserves when replayed: a replayed
    /// slot's rows take `base + k * rows_per_slot` for slot `k`.
    fn rows_per_slot(&self) -> u64;

    /// How many rows the folder's store may write for itself before they
    /// reach the number the next replayed slot would take, as of the
    /// writer plug's open; rows past it are skipped by the next replay.
    /// Every slot folded since adds another `rows_per_slot`.
    fn headroom(&self) -> u64;

    /// Reports that the store has applied every slot through `sequence`.
    /// Apply before reporting: the store's next memtable freeze records the
    /// reported sequence as the replay point.
    fn mark_folded(&self, sequence: u64);

    /// Waits until the store has been handed every slot through `sequence`
    /// by its reader-side plug, or until `timeout` passes. Returns whether
    /// it was reached. A reader catches up once per manifest poll. A hint
    /// for when to read again, never a guard: the plug reports a slot
    /// served before the reader has applied it.
    async fn await_served(&self, sequence: u64, timeout: Duration) -> bool;
}

/// What a commit-log library hands the catalog: SlateDB write-ahead-log
/// plugs and the committer, for the store at a path on the store that
/// holds its log.
///
/// Every method receives the object store the catalog records as its log
/// store — the catalog store itself unless a separate one was configured —
/// already wrapped in the catalog's retrying transport, and the catalog's
/// path within it. The plugs and the committer for one path share one
/// fold cursor.
pub trait CommitLogProvider: fmt::Debug + Send + Sync + 'static {
    /// The writer-side plug one read-write open builds its store with: it
    /// replays the log past the store's replay point as the fold, and
    /// reports the fold cursor as the flushed id.
    fn writer_init(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Box<dyn WriterInit>;

    /// The reader-side plug a read-only open builds its store with. A
    /// `tail`ing reader replays the unfolded tail and sees a commit the
    /// moment its slot lands; one that does not follows the folded store.
    fn reader(&self, log_store: Arc<dyn ObjectStore>, path: &str, tail: bool)
    -> Arc<dyn WalReader>;

    /// The commit side for the store at `path`.
    fn committer(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Arc<dyn SlotCommitter>;

    /// The plug SlateDB's garbage collector truncates the log through: it
    /// deletes slots below every range a live manifest still references.
    fn garbage_collector(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Arc<dyn WalGc>;

    /// Whether another process compacts and collects the stores opened
    /// over this provider. A writer then starts neither SlateDB's
    /// compactor nor its collector, as [`CatalogOptions::external_compaction`]
    /// would make it; the two fence each other otherwise. Defaults to
    /// `false`.
    ///
    /// [`CatalogOptions::external_compaction`]: crate::CatalogOptions::external_compaction
    fn external_compaction(&self) -> bool {
        false
    }

    /// Whether another process folds the log into the stores opened over
    /// this provider. A committer then takes the store for nothing,
    /// whatever its [`CatalogOptions::commit_log_folding`] says, exactly
    /// as [`external_compaction`](Self::external_compaction) keeps it
    /// from compacting: a sprint and a resident folder fence each other,
    /// and a process that commits from anywhere should carry neither a
    /// store nor the work of one. Defaults to `false`.
    ///
    /// [`CatalogOptions::commit_log_folding`]: crate::CatalogOptions::commit_log_folding
    fn external_folding(&self) -> bool {
        false
    }
}

static REGISTERED: OnceLock<Arc<dyn CommitLogProvider>> = OnceLock::new();

/// Registers the process's commit-log provider, for opens that carry no
/// provider of their own. Refuses a second registration: one process, one
/// library.
///
/// # Errors
///
/// [`Error::Configuration`] when a provider is already registered.
pub fn register_commit_log(provider: Arc<dyn CommitLogProvider>) -> Result<()> {
    REGISTERED.set(provider).map_err(|_| {
        Error::Configuration(
            "a commit-log provider is already registered for this process".to_string(),
        )
    })
}

/// The provider registered for this process, if any.
pub(crate) fn registered_commit_log() -> Option<Arc<dyn CommitLogProvider>> {
    REGISTERED.get().map(Arc::clone)
}

/// A fresh committer-minted transaction id.
pub(crate) fn mint_transaction_id() -> [u8; 16] {
    *uuid::Uuid::new_v4().as_bytes()
}

/// How long a committer over a tailing reader waits for the reader to be
/// served a sequence before it reads again.
pub(crate) const READER_CATCHUP_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a commit waits between refreshes while its slot is not yet
/// served, so a peer's slot landing between them is not spun on.
const SLOT_REFRESH_BACKOFF: Duration = Duration::from_millis(2);

/// The log's commit side bound to a process's store: offers go to the
/// committer, and slots other committers won are applied to `db` when this
/// process is the folder, or awaited from the tailing reader when it is not.
#[derive(Clone)]
pub(crate) struct LogCommitter {
    pub(crate) committer: Arc<dyn SlotCommitter>,
    /// The store this process folds into, while it holds one. A process
    /// takes the store when the tail is long and keeps it until fenced.
    pub(crate) holder: Arc<std::sync::Mutex<Option<Holder>>>,
    /// Where this process reads: the store it holds, or a tailing reader.
    pub(crate) reads: ReadSide,
    /// How this process takes the store when the tail is long; `None` for
    /// a process that holds it from its open.
    pub(crate) taker: Option<Arc<dyn StoreTaker>>,
    /// The head's batch counter when the log was adopted; slot `k` carries
    /// the stamp at `batch_origin + k - 1`.
    pub(crate) batch_origin: u64,
    /// Serializes the folder's offers, the local commits that follow them,
    /// and the idle poll's folds, so the poll never folds a slot the
    /// folder is in the middle of committing itself.
    pub(crate) guard: Arc<tokio::sync::Mutex<()>>,
    /// Slots folded since this open, each buying the folder's own rows
    /// another slot's worth of sequence numbers.
    pub(crate) folds_since_open: Arc<AtomicU64>,
    /// How a holder flushes and truncates.
    pub(crate) flushing: Flushing,
    /// The reader this process reads the log through, refreshed while a
    /// commit waits to see its slot. `None` on a folder, which reads the
    /// store it holds.
    pub(crate) reader: Option<Arc<slatedb::DbReader>>,
}

/// The store a process holds, and the idle poll folding into it.
pub(crate) struct Holder {
    pub(crate) db: slatedb::Db,
    pub(crate) fold_poll: Option<FoldPoll>,
}

/// Where a process reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadSide {
    /// Through the store it holds; its own commits land there directly.
    Store,
    /// Through a tailing reader, which is served every slot, its own
    /// included, once the reader polls.
    Reader,
}

/// Opens the store for a process that found the tail long, so it can hold
/// and fold it. Implemented by the catalog, which owns the open.
#[async_trait]
pub(crate) trait StoreTaker: Send + Sync {
    /// Takes the store if the unfolded tail is past the threshold, handing
    /// it to `log` through [`LogCommitter::promote`]. Returns whether it
    /// did.
    async fn take_if_stalled(&self, log: &LogCommitter) -> Result<bool>;
}

/// The idle poll task and the switch that stops it.
pub(crate) struct FoldPoll {
    stop: tokio::sync::watch::Sender<bool>,
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl FoldPoll {
    pub(crate) fn spawn(log: LogCommitter, interval: Duration) -> Self {
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(log.poll_folds(interval, stopped));
        Self {
            stop,
            task: std::sync::Mutex::new(Some(task)),
        }
    }

    pub(crate) async fn stop(&self) {
        let _ = self.stop.send(true);
        let task = self
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    /// Tells the task to stop without waiting for it: what the task itself
    /// calls, since it cannot wait for its own end.
    fn stop_from_within(&self) {
        let _ = self.stop.send(true);
        drop(
            self.task
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
    }
}

/// The folder's flush policy: after `every` folded slots, write the
/// memtable out so the replay point moves, then run the collector so the
/// log is truncated below it.
#[derive(Clone)]
pub(crate) struct Flushing {
    pub(crate) every: u64,
    pub(crate) folded_since: Arc<AtomicU64>,
    pub(crate) collector: Collector,
}

/// What builds SlateDB's garbage collector for one store; built per run,
/// since the collector is configuration and holds nothing.
#[derive(Clone)]
pub(crate) struct Collector {
    pub(crate) path: String,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) log_store: Arc<dyn ObjectStore>,
    pub(crate) wal_gc: Arc<dyn WalGc>,
    /// How old a slot must be before truncation may delete it.
    pub(crate) min_age: Duration,
}

impl Collector {
    /// Runs the manifest and WAL collectors once.
    async fn run(&self) {
        let directory = slatedb::config::GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: self.min_age,
            dry_run: false,
        };
        let options = slatedb::config::GarbageCollectorOptions {
            manifest_options: Some(directory),
            wal_options: Some(directory),
            wal_fence_options: None,
            compacted_options: None,
            compactions_options: None,
            ..Default::default()
        };
        slatedb::GarbageCollectorBuilder::new(self.path.clone(), Arc::clone(&self.object_store))
            .with_wal_object_store(Arc::clone(&self.log_store))
            .with_wal_gc(Arc::clone(&self.wal_gc))
            .with_options(options)
            .build()
            .run_gc_once()
            .await;
    }
}

impl fmt::Debug for LogCommitter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogCommitter")
            .field("committer", &self.committer)
            .field("batch_origin", &self.batch_origin)
            .field("holds_store", &self.holds_store())
            .field("reads", &self.reads)
            .finish_non_exhaustive()
    }
}

impl LogCommitter {
    /// The store this process holds, if it does.
    pub(crate) fn db(&self) -> Option<slatedb::Db> {
        self.holder
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|holder| holder.db.clone())
    }

    /// Whether this process holds the store.
    pub(crate) fn holds_store(&self) -> bool {
        self.db().is_some()
    }

    /// Takes `db` as the store this process folds into, polling for peers'
    /// slots every `poll`.
    pub(crate) fn promote(&self, db: slatedb::Db, poll: Duration) {
        let fold_poll = FoldPoll::spawn(self.clone(), poll);
        *self
            .holder
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Holder {
            db,
            fold_poll: Some(fold_poll),
        });
        self.folds_since_open.store(0, Ordering::Release);
        tracing::info!("took the store");
    }

    /// Gives the store up: stops the poll and hands the store back for the
    /// caller to close. `None` when this process held none.
    pub(crate) async fn demote(&self) -> Option<slatedb::Db> {
        let holder = self
            .holder
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()?;
        if let Some(fold_poll) = holder.fold_poll {
            fold_poll.stop().await;
        }
        Some(holder.db)
    }

    /// Drops back to committing after another process took the store:
    /// nothing is lost, since every slot is durable and this process reads
    /// through its reader. A fence shows at the next flush, since the plug
    /// journals nothing; the folds made in memory before it are discarded
    /// with the store.
    async fn fenced(&self) {
        // The poll task may be the one here, so the poll is told to stop
        // rather than waited for.
        let holder = self
            .holder
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(holder) = holder else {
            return;
        };
        if let Some(fold_poll) = holder.fold_poll {
            fold_poll.stop_from_within();
        }
        tracing::warn!("another process took the store; back to committing");
        let _ = holder.db.close().await;
    }

    /// Takes the store if the tail is long and this process can: a process
    /// that already holds it, or opened without a taker, does nothing.
    pub(crate) async fn take_if_stalled(&self) {
        let Some(taker) = &self.taker else {
            return;
        };
        if self.holds_store() {
            return;
        }
        match taker.take_if_stalled(self).await {
            Ok(_) => {}
            Err(error) => tracing::warn!(%error, "could not take the store; committing on"),
        }
    }

    /// Reports `sequence` folded, and on the folder flushes and truncates
    /// once enough slots have folded since the last flush: the replay point
    /// moves, so a cold open replays at most that many slots.
    pub(crate) async fn folded(&self, sequence: u64) -> Result<()> {
        self.committer.mark_folded(sequence);
        self.folds_since_open.fetch_add(1, Ordering::AcqRel);
        let Some(db) = self.db() else {
            return Ok(());
        };
        let flushing = &self.flushing;
        if flushing.folded_since.fetch_add(1, Ordering::AcqRel) + 1 < flushing.every {
            return Ok(());
        }
        match crate::transaction::commit::flush_memtable(&db).await {
            Ok(()) => {}
            Err(Error::Fenced(_)) => {
                self.fenced().await;
                return Ok(());
            }
            Err(error) => return Err(error),
        }
        flushing.folded_since.store(0, Ordering::Release);
        flushing.collector.run().await;

        Ok(())
    }

    /// Writes the folder's memtable out and truncates the log, whatever the
    /// count since the last flush; the sprint's last act.
    pub(crate) async fn flush_now(&self) -> Result<()> {
        let Some(db) = self.db() else {
            return Ok(());
        };
        crate::transaction::commit::flush_memtable(&db).await?;
        self.flushing.folded_since.store(0, Ordering::Release);
        self.flushing.collector.run().await;

        Ok(())
    }

    /// Folds whatever has landed past this process's cursor. The folder's
    /// idle poll, and the sprint's catch-up.
    pub(crate) async fn fold_tail(&self) -> Result<u64> {
        let slots = self.committer.tail().await?;
        let count = u64::try_from(slots.len()).unwrap_or(u64::MAX);
        self.fold_slots(slots).await?;

        Ok(count)
    }

    /// Runs the idle poll until `stop` says so: every `interval`, under the
    /// guard, fold whatever peers have landed.
    pub(crate) async fn poll_folds(
        self,
        interval: Duration,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                () = tokio::time::sleep(interval) => {}
                _ = stop.changed() => return,
            }
            if *stop.borrow() {
                return;
            }
            let _held = self.guard.lock().await;
            match self.fold_tail().await {
                Ok(0) => {}
                Ok(count) => tracing::debug!(count, "folded peers' slots"),
                Err(error) => tracing::warn!(%error, "the idle fold failed; it retries next poll"),
            }
        }
    }

    /// The slot a batch stamping the head at `batch_seq` belongs to.
    pub(crate) fn sequence_of(&self, batch_seq: u64) -> u64 {
        batch_seq
            .saturating_sub(self.batch_origin)
            .saturating_add(1)
    }

    /// Waits for this process's reader to be served every slot through
    /// `sequence`; a folder has no reader to wait on. A timeout is reported
    /// and survived: the next read may lag, and the next offer re-validates.
    pub(crate) async fn await_served(&self, sequence: u64) -> bool {
        if self.reads == ReadSide::Store {
            return true;
        }
        let served = self.await_refreshed(sequence).await;
        if !served {
            tracing::warn!(
                sequence,
                "the reader was not served the slot within the catch-up timeout"
            );
        }
        served
    }

    /// Refreshes the reader until it has been served every slot through
    /// `sequence`, or the catch-up timeout passes. Each refresh lists the
    /// log and replays what it finds, so the wait is round trips rather
    /// than ticks of a clock.
    async fn await_refreshed(&self, sequence: u64) -> bool {
        let Some(reader) = &self.reader else {
            return self
                .committer
                .await_served(sequence, READER_CATCHUP_TIMEOUT)
                .await;
        };
        let deadline = std::time::Instant::now() + READER_CATCHUP_TIMEOUT;
        loop {
            if let Err(error) = reader.refresh().await {
                tracing::warn!(%error, "refreshing the reader failed; retrying");
            }
            // The plug reports a slot served as it hands the rows over, so
            // this is true by the time the refresh that found it returns.
            if self.committer.await_served(sequence, Duration::ZERO).await {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(SLOT_REFRESH_BACKOFF).await;
        }
    }

    /// Applies one slot's writes to the store this process holds, waiting
    /// on no durability — a slot is durable already — then reports
    /// `sequence` folded. A process holding no store applies nothing: its
    /// reader is served the slot. One fenced by another holder gives the
    /// store up and carries on the same way.
    pub(crate) async fn fold<'a>(
        &self,
        sequence: u64,
        writes: impl Iterator<Item = &'a SlotWrite>,
    ) -> Result<()> {
        let Some(db) = self.db() else {
            return Ok(());
        };
        let mut batch = slatedb::WriteBatch::new();
        let mut any = false;
        for write in writes {
            any = true;
            match &write.value {
                Some(value) => batch.put(&write.key, value),
                None => batch.delete(&write.key),
            }
        }
        if any {
            // The handle is the durability wait; a slot is durable already.
            match db
                .write_with_options(batch, &slatedb::config::WriteOptions::default())
                .await
                .map_err(Error::from)
            {
                Ok(handle) => drop(handle),
                Err(Error::Fenced(_)) => {
                    self.fenced().await;
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        self.folded(sequence).await?;

        Ok(())
    }

    /// Folds what beat a batch: everything past the fold cursor when this
    /// process holds the store, since its reader may have been served slots
    /// the store has not folded, else the slots the loss handed back.
    pub(crate) async fn fold_after_loss(&self, slots: Vec<(u64, SlotEnvelope)>) -> Result<()> {
        if self.holds_store() {
            self.fold_tail().await?;
            if let Some((last, _)) = slots.last() {
                self.await_served(*last).await;
            }
            return Ok(());
        }
        self.fold_slots(slots).await
    }

    /// Folds `slots` in order; a committer over a reader waits to be served
    /// the last of them instead. Every slot past the first carries exactly
    /// one head stamp, which is what lets a stamp name its slot; one that
    /// does not is corruption.
    pub(crate) async fn fold_slots(&self, slots: Vec<(u64, SlotEnvelope)>) -> Result<()> {
        let last = slots.last().map(|(sequence, _)| *sequence);
        for (sequence, envelope) in slots {
            let head_key = crate::store::key::Key::Sys(crate::store::key::SysKey::Head).encode();
            let stamps = envelope
                .writes()
                .filter(|write| write.key == head_key)
                .count();
            if sequence > 1 && stamps != 1 {
                return Err(Error::Corruption(format!(
                    "slot {sequence} carries {stamps} head stamps; every slot past the first \
                     carries one"
                )));
            }
            self.fold(sequence, envelope.writes()).await?;
        }
        if let Some(last) = last {
            self.await_served(last).await;
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test_log;
#[cfg(test)]
mod tests;
