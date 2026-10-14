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

    /// The slots landed past the last this process has folded or been
    /// served, in order: what a loss would hand back, without offering.
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
}

/// Which part a read-write open plays in a commit log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommitLogRole {
    /// Holds the store's one writer: replays the log on open, applies every
    /// slot as it lands, flushes on the paced cadence, and adopts a store
    /// not yet on the log. One per lake.
    #[default]
    Folder,
    /// Commits over a tailing reader: prepares against the reader, offers
    /// to the log, and waits to be served what lands. Any number per lake;
    /// needs an adopted log.
    Committer,
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

/// The log's commit side bound to a process's store: offers go to the
/// committer, and slots other committers won are applied to `db` when this
/// process is the folder, or awaited from the tailing reader when it is not.
#[derive(Clone)]
pub(crate) struct LogCommitter {
    pub(crate) committer: Arc<dyn SlotCommitter>,
    pub(crate) db: Option<slatedb::Db>,
    /// The head's batch counter when the log was adopted; slot `k` carries
    /// the stamp at `batch_origin + k - 1`.
    pub(crate) batch_origin: u64,
    /// Serializes the folder's offers, the local commits that follow them,
    /// and the idle poll's folds, so the poll never folds a slot the
    /// folder is in the middle of committing itself.
    pub(crate) guard: Arc<tokio::sync::Mutex<()>>,
    /// How the folder flushes and truncates; `None` for a committer.
    pub(crate) flushing: Option<Flushing>,
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
        let mut options = slatedb::config::GarbageCollectorOptions::default();
        options.manifest_options = Some(directory.clone());
        options.wal_options = Some(directory);
        options.wal_fence_options = None;
        options.compacted_options = None;
        options.compactions_options = None;
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
            .finish_non_exhaustive()
    }
}

impl LogCommitter {
    /// Reports `sequence` folded, and on the folder flushes and truncates
    /// once enough slots have folded since the last flush: the replay point
    /// moves, so a cold open replays at most that many slots.
    pub(crate) async fn folded(&self, sequence: u64) -> Result<()> {
        self.committer.mark_folded(sequence);
        let (Some(db), Some(flushing)) = (&self.db, &self.flushing) else {
            return Ok(());
        };
        if flushing.folded_since.fetch_add(1, Ordering::AcqRel) + 1 < flushing.every {
            return Ok(());
        }
        crate::transaction::commit::flush_memtable(db).await?;
        flushing.folded_since.store(0, Ordering::Release);
        flushing.collector.run().await;

        Ok(())
    }

    /// Writes the folder's memtable out and truncates the log, whatever the
    /// count since the last flush; the sprint's last act.
    pub(crate) async fn flush_now(&self) -> Result<()> {
        let (Some(db), Some(flushing)) = (&self.db, &self.flushing) else {
            return Ok(());
        };
        crate::transaction::commit::flush_memtable(db).await?;
        flushing.folded_since.store(0, Ordering::Release);
        flushing.collector.run().await;

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
                _ = tokio::time::sleep(interval) => {}
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
        if self.db.is_some() {
            return true;
        }
        let served = self
            .committer
            .await_served(sequence, READER_CATCHUP_TIMEOUT)
            .await;
        if !served {
            tracing::warn!(
                sequence,
                "the reader was not served the slot within the catch-up timeout"
            );
        }
        served
    }

    /// Applies one slot's writes to the store, waiting on no durability — a
    /// slot is durable already — then reports `sequence` folded. A committer
    /// over a reader applies nothing: its reader is served the slot.
    pub(crate) async fn fold<'a>(
        &self,
        sequence: u64,
        writes: impl Iterator<Item = &'a SlotWrite>,
    ) -> Result<()> {
        let Some(db) = &self.db else {
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
            drop(
                db.write_with_options(batch, &slatedb::config::WriteOptions::default())
                    .await
                    .map_err(Error::from)?,
            );
        }
        self.folded(sequence).await?;

        Ok(())
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
