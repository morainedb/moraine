//! The seam a bucket-resident commit log plugs into. A library that keeps a
//! store's commits as slot objects hands the catalog a [`CommitStore`]: the
//! write every process shares, over the SlateDB store or reader the catalog
//! opened. The catalog decides nothing about slots. It derives the sequence
//! a batch names, stamps the format, supplies the two hooks the library
//! needs — how to open the store as its writer, and what a folded slot must
//! carry — and calls through. This crate holds the seam and nothing of the
//! log; a store stamped for the log cannot be opened without a provider.
//!
//! A host supplies a provider one of two ways: per open, through
//! [`CatalogOptions::commit_log`](crate::CatalogOptions::commit_log), or
//! once per process, through [`register_commit_log`], which is how a
//! library linked into the DuckDB extension reaches an attach it never sees
//! the options of. The per-open provider outranks the registered one.
//!
//! Every byte that crosses the seam is opaque to the library: a
//! [`SlotWrite`] is a key and a value the catalog encoded, and a
//! [`SlotCommit`] is a batch of them with what it was validated against.

use std::{
    any::Any,
    fmt,
    sync::{Arc, OnceLock},
    time::Duration,
};

use async_trait::async_trait;
use object_store::ObjectStore;
use slatedb::{
    Db, DbReader,
    wal::{WalGc, WalReader, WriterInit},
};

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
    /// The commit holds `sequence`, and this process's store has applied
    /// it, or been served it.
    Won {
        /// The sequence won.
        sequence: u64,
    },
    /// Another committer holds the sequence raced. Everything that beat the
    /// offer is in this process's store already; the caller re-prepares
    /// against it.
    Lost {
        /// The sequence raced.
        sequence: u64,
        /// The envelope that holds it.
        winner: SlotEnvelope,
        /// The last sequence this process's store now covers.
        through: u64,
    },
}

/// How an adoption ended. Either way the stamp that holds the first slot is
/// in this process's store already.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotAdoption {
    /// This process's stamp holds the first slot.
    Adopted,
    /// A peer's stamp holds the first slot; here it is, for the catalog to
    /// check.
    AdoptedByPeer(SlotEnvelope),
}

/// The write every process shares, over the store or reader the catalog
/// opened. The library behind it offers, folds what beat an offer, flushes
/// and truncates on its cadence, takes the store when a follower's turn
/// comes, and gives it back; the catalog sees none of that.
#[async_trait]
pub trait CommitStore: Any + fmt::Debug + Send + Sync + 'static {
    /// Whether the log has been adopted: a store's stamp holds its first
    /// slot.
    ///
    /// # Errors
    ///
    /// [`Error::CommitOutcomeUnknown`] when the log could not be read.
    async fn adopted(&self) -> Result<bool>;

    /// Adopts a log nothing has adopted, with `stamp` as its first slot,
    /// numbering the slots past the store's own history. Only a process
    /// holding the store can. A peer may have adopted first; its stamp
    /// comes back, applied already.
    ///
    /// # Errors
    ///
    /// As [`write_at`](Self::write_at); [`Error::Constraint`] on a follower
    /// or a log adopted already.
    async fn adopt(&self, stamp: SlotCommit) -> Result<SlotAdoption>;

    /// Offers `commit` as the slot at `sequence`, which the caller derives
    /// from the state the commit was prepared against: a stale state names
    /// a taken slot and loses. The sequence may be at most one past what
    /// this process's store knows.
    ///
    /// # Errors
    ///
    /// [`Error::CommitOutcomeUnknown`] when the log could not be reached, a
    /// put could not be attributed, or the slot was won and this process's
    /// store could not take it, in which case the error names the sequence
    /// and a reopen folds it; [`Error::Fenced`] when another process has
    /// taken a held store; [`Error::Corruption`] when a slot is not a valid
    /// envelope or the log is damaged.
    async fn write_at(&self, sequence: u64, commit: SlotCommit) -> Result<SlotOffer>;

    /// Folds what has landed past this process's cursor into the store it
    /// holds, or refreshes the reader it follows. Returns how many slots
    /// that was.
    ///
    /// # Errors
    ///
    /// As [`write_at`](Self::write_at).
    async fn fold_tail(&self) -> Result<u64>;

    /// Writes a held store out and lets it go, and closes the store or
    /// reader this was built over. The catalog closes neither itself.
    ///
    /// # Errors
    ///
    /// The store's error on the close.
    async fn close(&self) -> Result<()>;

    /// The store as the crate that built it knows it, for everything the
    /// seam does not carry.
    fn as_any(&self) -> &dyn Any;
}

/// One writer open's cursor, shared by its plug and the store built over
/// it. The provider mints it, the catalog builds the SlateDB writer over
/// [`writer_init`](Self::writer_init), and hands both back to the provider.
#[async_trait]
pub trait WriterSession: Any + Send + Sync + 'static {
    /// The write-ahead-log plug the writer is built with.
    fn writer_init(&self) -> Box<dyn WriterInit>;

    /// Whether the log has been adopted, asked before the store is built:
    /// a store not yet adopted drains its own log into the tree first,
    /// since the plug replays the log in place of it.
    ///
    /// # Errors
    ///
    /// [`Error::CommitOutcomeUnknown`] when the log could not be read.
    async fn adopted(&self) -> Result<bool>;

    /// The session as the crate that minted it knows it.
    fn as_any(&self) -> &dyn Any;
}

/// One reader open's cursor, shared by its plug and the store built over
/// it.
pub trait ReaderSession: Any + Send + Sync + 'static {
    /// The write-ahead-log plug the reader is built with.
    fn reader(&self) -> Arc<dyn WalReader>;

    /// The session as the crate that minted it knows it.
    fn as_any(&self) -> &dyn Any;
}

/// How the library opens the store as its writer: for the fold a follower
/// makes on its turn and lets go of, or to hold. The catalog implements it
/// with its own writer open, over a fresh session from the provider, whose
/// replay is the fold.
#[async_trait]
pub trait TakeStore: Send + Sync + 'static {
    /// Opens the store as its writer, or declines for now — as when the
    /// store is at its limit of unmerged files and a fold would stall.
    ///
    /// # Errors
    ///
    /// As [`Catalog::open`](crate::Catalog::open).
    async fn take(&self) -> Result<Option<(Db, Arc<dyn WriterSession>)>>;
}

/// A check the library runs on every slot before it folds it. A slot the
/// check refuses is not folded, and the fold stops with the refusal.
pub type Inspect = Arc<dyn Fn(u64, &SlotEnvelope) -> Result<()> + Send + Sync>;

/// What the catalog hands the library with a store or reader: how to open
/// the store as its writer, what every folded slot must carry, where the
/// store lives, and the cadence it runs on.
#[derive(Clone)]
pub struct CatalogHooks {
    /// How to open the store as its writer.
    pub take: Arc<dyn TakeStore>,
    /// What every slot must carry to be folded.
    pub inspect: Inspect,
    /// The store's path, as it was opened: where the manifests are.
    pub path: String,
    /// The store's object store, as it was opened.
    pub object_store: Arc<dyn ObjectStore>,
    /// The cadence and the waits.
    pub settings: CommitLogSettings,
}

impl fmt::Debug for CatalogHooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogHooks")
            .field("path", &self.path)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// The commit-log options of [`CatalogOptions`](crate::CatalogOptions), as
/// values the library reads. The catalog interprets none of them.
#[derive(Debug, Clone)]
pub struct CommitLogSettings {
    /// Every this many slots, the follower that lands one takes the store,
    /// folds, writes it out, and lets go. Zero folds on no cadence.
    pub fold_every: u64,
    /// After this many folded slots a holder writes its memtable out and
    /// truncates the log below the replay point.
    pub flush_every: u64,
    /// How old a slot must be before truncation may delete it.
    pub retention: Duration,
    /// How often a holder looks for slots its peers landed.
    pub poll: Duration,
    /// How long a follower waits to be served a slot it knows landed.
    pub catch_up: Duration,
    /// How long a follower waits between refreshes while it waits.
    pub refresh_backoff: Duration,
}

/// What a commit-log library hands the catalog, for the store at a path on
/// the object store that holds its log.
///
/// Every method receives the object store the catalog records as its log
/// store — the catalog store itself unless a separate one was configured —
/// already wrapped in the catalog's retrying transport, and the catalog's
/// path within it. One session per open: the catalog builds its SlateDB
/// handle over the session's plug and hands both back.
pub trait CommitLogProvider: fmt::Debug + Send + Sync + 'static {
    /// A fresh writer session for one read-write open.
    fn writer_session(&self, log_store: Arc<dyn ObjectStore>, path: &str)
    -> Arc<dyn WriterSession>;

    /// A fresh reader session for one read-only open. A `tail`ing reader is
    /// served the unfolded tail and sees a commit the moment its slot
    /// lands; one that does not follows the folded store.
    fn reader_session(
        &self,
        log_store: Arc<dyn ObjectStore>,
        path: &str,
        tail: bool,
    ) -> Arc<dyn ReaderSession>;

    /// The commit store over a writer the catalog opened with `session`'s
    /// plug: a holder, into which every slot folds.
    fn holding(
        &self,
        db: Db,
        session: Arc<dyn WriterSession>,
        hooks: CatalogHooks,
    ) -> Arc<dyn CommitStore>;

    /// The commit store over a reader the catalog opened with `session`'s
    /// plug: a follower, whose writes land as slots and which takes the
    /// store on its turn through the hooks.
    fn following(
        &self,
        reader: Arc<DbReader>,
        session: Arc<dyn ReaderSession>,
        hooks: CatalogHooks,
    ) -> Arc<dyn CommitStore>;

    /// The plug SlateDB's garbage collector truncates the log through, for
    /// a maintenance process collecting on a store's behalf.
    fn garbage_collector(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Arc<dyn WalGc>;

    /// Whether another process compacts and collects the stores opened
    /// over this provider. A writer then starts neither SlateDB's
    /// compactor nor its collector, as
    /// [`CatalogOptions::external_compaction`] would make it; the two fence
    /// each other otherwise. Defaults to `false`.
    ///
    /// Returning `true` makes that process load-bearing. Each fold writes a
    /// file only a compactor merges away, so with none running the store
    /// reaches its limit of unmerged files and folding stops until one
    /// does. Commits are unaffected; the log grows in place of the fold,
    /// and the folded state falls behind.
    ///
    /// [`CatalogOptions::external_compaction`]: crate::CatalogOptions::external_compaction
    fn external_compaction(&self) -> bool {
        false
    }
}

/// The catalog's binding to the commit store it commits through, and the
/// batch counter the log's first slot stands on: slot `k` carries the head
/// stamp at `batch_origin + k - 1`, which is how a batch names its slot.
#[derive(Clone)]
pub(crate) struct LogCommitter {
    pub(crate) store: Arc<dyn CommitStore>,
    pub(crate) batch_origin: u64,
}

impl fmt::Debug for LogCommitter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogCommitter")
            .field("store", &self.store)
            .field("batch_origin", &self.batch_origin)
            .finish()
    }
}

impl LogCommitter {
    /// The slot a batch stamping the head at `batch_seq` belongs to.
    pub(crate) fn sequence_of(&self, batch_seq: u64) -> u64 {
        batch_seq
            .saturating_sub(self.batch_origin)
            .saturating_add(1)
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

/// How long a follower waits to be served a slot it knows landed before its
/// write returns anyway; the slot is durable either way.
pub(crate) const READER_CATCHUP_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) mod asked;
#[cfg(test)]
pub(crate) mod test_log;
#[cfg(test)]
mod tests;
