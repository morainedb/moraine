//! The slot log presented as SlateDB's write-ahead log.
//!
//! A store plugged in through [`SlotWal`] folds the log by replaying it: WAL
//! file ids are slot sequences, a slot's writes are one write batch at the
//! sequence number the slot's own ordinal gives it, and the store's
//! `replay_after_wal_id` is the fold cursor. A reader plugged in the same way
//! serves the unfolded tail without an overlay of its own.
//!
//! The log is written by committers racing conditional puts, never by the
//! store: [`WalWriter::append`] refuses, so a store that plugs this in must
//! run with SlateDB's own journaling off. It may still write rows of its own —
//! see [`slot_sequence`] for the numbers those take. Payload keys and values
//! stay opaque here; the one place meaning could leak in — what a leader
//! advert writes — is the embedder's [`AdvertProjection`].

use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::{
    RowEntry, ValueDeletable,
    wal::{
        FlushResultFuture, WalError, WalEvent, WalFileRange, WalGc, WalIterator, WalObserver,
        WalReader, WalRows, WalStatus, WalStatusListener, WalWriter, WriterInit, WriterInitResult,
        WriterManifest,
    },
};

use crate::{envelope::LeaderAdvert, error::Error, slot::SlotLog};

/// How long a tail-following iterator waits before re-reading a log it has
/// drained.
const DEFAULT_TAIL_POLL: Duration = Duration::from_millis(100);

/// The sequence number the log's first slot takes. SlateDB draws row sequence
/// numbers from one space, so the log starts above whatever a store wrote
/// before it adopted the log: that history stays ordered before every slot,
/// and no ordinal lands underneath it. A store whose own writes have already
/// reached this is refused rather than folded into a shadowed keyspace.
const SEQUENCE_BASE: u64 = 1 << 32;

/// How many sequence numbers each slot reserves. A store still writes some
/// rows itself — whatever derived state its maintenance keeps — and those take
/// the numbers between one slot and the next, so they order after every folded
/// slot and before every unfolded one. Crossing into the next slot's number
/// would make the fold skip that slot, which is why
/// [`slot_sequence`] is public: a store writing directly must check its own
/// writes against the ceiling.
const SEQUENCE_STRIDE: u64 = 1 << 20;

/// The row sequence number slot `ordinal` writes at. The numbers strictly
/// between this and `slot_sequence(ordinal + 1)` belong to the store's own
/// writes, and a store that exhausts them must fold before writing more.
#[must_use]
pub fn slot_sequence(ordinal: u64) -> u64 {
    SEQUENCE_BASE.saturating_add(ordinal.saturating_mul(SEQUENCE_STRIDE))
}

/// What a slot's leader advert writes into the store, if the embedder folds
/// adverts at all. The key and value are the embedder's; the advert is this
/// crate's.
pub type AdvertProjection = Arc<dyn Fn(&LeaderAdvert) -> Option<(Vec<u8>, Vec<u8>)> + Send + Sync>;

/// The slot log as a SlateDB WAL: the writer-side plug that makes a store
/// fold the log, the reader-side plug that serves the unfolded tail, and the
/// collector that truncates what neither still needs.
///
/// # A worked example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use moraine_wal::{SlotLog, SlotWal};
/// # use object_store::memory::InMemory;
/// # async fn plug() -> Result<(), Box<dyn std::error::Error>> {
/// let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
/// let wal = SlotWal::new(SlotLog::new(Arc::clone(&store), "catalog"));
///
/// // The store journals nothing of its own: the log is the journal, and
/// // replaying it is the fold.
/// let db = slatedb::Db::builder("catalog", Arc::clone(&store))
///     .with_wal_writer(wal.writer_init())
///     .build()
///     .await?;
///
/// // A reader replays the slots past the fold cursor on its own.
/// let reader = slatedb::DbReader::builder("catalog", store)
///     .with_wal_reader(wal.reader())
///     .build()
///     .await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct SlotWal {
    log: SlotLog,
    advert: Option<AdvertProjection>,
    tail_poll: Duration,
    replay_limit: Option<u64>,
}

impl std::fmt::Debug for SlotWal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotWal")
            .field("log", &self.log)
            .field("advert", &self.advert.is_some())
            .field("tail_poll", &self.tail_poll)
            .field("replay_limit", &self.replay_limit)
            .finish()
    }
}

impl SlotWal {
    /// The WAL over `log`, folding no adverts.
    #[must_use]
    pub fn new(log: SlotLog) -> Self {
        Self {
            log,
            advert: None,
            tail_poll: DEFAULT_TAIL_POLL,
            replay_limit: None,
        }
    }

    /// Bounds how many slots one store open replays, so a fold advances by at
    /// most `limit` and the next open resumes from where this one stopped.
    /// Unset, an open replays the whole tail.
    #[must_use]
    pub fn with_replay_limit(mut self, limit: Option<u64>) -> Self {
        self.replay_limit = limit;
        self
    }

    /// Folds each slot's leader advert into the write `projection` gives it,
    /// so an announcement outlives the slot that carried it.
    #[must_use]
    pub fn with_advert_projection(mut self, projection: AdvertProjection) -> Self {
        self.advert = Some(projection);
        self
    }

    /// Sets how long a tail-following iterator waits before re-reading a log
    /// it has drained. Shorter means fresher readers and more LISTs.
    #[must_use]
    pub fn with_tail_poll(mut self, tail_poll: Duration) -> Self {
        self.tail_poll = tail_poll;
        self
    }

    /// The writer-side plug:
    /// `Db::builder(..).with_wal_writer(wal.writer_init())`.
    #[must_use]
    pub fn writer_init(&self) -> Box<dyn WriterInit> {
        Box::new(SlotWriterInit { wal: self.clone() })
    }

    /// The reader-side plug:
    /// `DbReader::builder(..).with_wal_reader(wal.reader())`.
    #[must_use]
    pub fn reader(&self) -> Arc<dyn WalReader> {
        Arc::new(self.clone())
    }

    /// A reader-side WAL that replays no slot, so the store it opens serves
    /// what the fold has applied and nothing else. Pair it with the
    /// `replay_after_wal_id` that same open reports and the two describe one
    /// state, which is what separating folded rows from replayed ones needs.
    #[must_use]
    pub fn folded_only_reader() -> Arc<dyn WalReader> {
        Arc::new(FoldedOnly)
    }

    /// The collector SlateDB's garbage collector hands referenced ranges to:
    /// `GarbageCollectorBuilder::with_wal_gc(wal.garbage_collector())`.
    #[must_use]
    pub fn garbage_collector(&self) -> Arc<dyn WalGc> {
        Arc::new(self.clone())
    }

    /// The log this WAL is over.
    #[must_use]
    pub fn log(&self) -> &SlotLog {
        &self.log
    }

    /// The rows one slot contributes: its commits' writes folded
    /// last-writer-wins by key, plus whatever its advert projects, all at the
    /// sequence number the slot's ordinal gives them. A slot that writes
    /// nothing contributes no rows and still advances the cursor.
    ///
    /// The fold is what keeps a slot's rows a well-formed batch: two writes to
    /// one key inside a slot would otherwise land as two rows sharing a
    /// sequence number, which no order can separate.
    ///
    /// Rows are stamped with the moment the store read them rather than a time
    /// the slot carries: a committer writes no clock reading into the log, and
    /// the store requires its own ticks to advance.
    fn rows(&self, sequence: u64, envelope: &crate::envelope::Envelope) -> Vec<RowEntry> {
        let mut folded: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        for commit in &envelope.commits {
            for write in &commit.payload.writes {
                folded.insert(write.key.clone(), write.value.clone());
            }
        }
        if let (Some(projection), Some(advert)) = (self.advert.as_ref(), envelope.leader.as_ref())
            && let Some((key, value)) = projection(advert)
        {
            folded.insert(key, Some(value));
        }

        let read_at = unix_millis_now();
        folded
            .into_iter()
            .map(|(key, value)| RowEntry {
                key: Bytes::from(key),
                value: match value {
                    Some(value) => ValueDeletable::Value(Bytes::from(value)),
                    None => ValueDeletable::Tombstone,
                },
                seq: slot_sequence(sequence),
                create_ts: Some(read_at),
                expire_ts: None,
            })
            .collect()
    }

    /// The last sequence the contiguous run from `after + 1` reaches, or
    /// `after` when nothing follows it. A hole above `after` is damage, not an
    /// ending, so it refuses rather than reporting a short log.
    async fn tail_end(&self, after: u64) -> Result<u64, WalError> {
        let extent = self
            .log
            .tail_extent(after.saturating_add(1))
            .await
            .map_err(wal_error)?;
        if let Some(gap_at) = extent.gap_at {
            return Err(WalError::DataError(Arc::new(Error::corruption(format!(
                "slot {gap_at} is absent while higher slots are present; a committed slot was \
                 destroyed outside the protocol"
            )))));
        }

        Ok(extent.last.unwrap_or(after))
    }

    /// An iterator over `start..end` of the log.
    fn iterator(&self, start: u64, end: Bound<u64>) -> SlotWalIterator {
        SlotWalIterator {
            wal: self.clone(),
            next: start.max(1),
            end,
            consumed: false,
        }
    }
}

/// Maps a log failure onto the WAL's error model: an unreadable log is
/// unavailable, a damaged one is a data error.
fn wal_error(error: Error) -> WalError {
    match error {
        Error::Transport(_) => WalError::Unavailable(Arc::new(error)),
        Error::Corruption(_) => WalError::DataError(Arc::new(error)),
    }
}

/// The refusal a store meets when it writes through a log only committers may
/// append to.
#[derive(Debug, thiserror::Error)]
#[error(
    "the commit-slot log carries committed slots only, so a store write cannot be journaled \
     through it; open the store with SlateDB's own journaling disabled — the log is the journal"
)]
pub struct AppendRefused;

/// Fences nothing and resolves the replay range: the log admits every
/// committer, and which process may write the *store* is settled by the
/// manifest epoch SlateDB has already taken.
struct SlotWriterInit {
    wal: SlotWal,
}

#[async_trait]
impl WriterInit for SlotWriterInit {
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError> {
        let folded = manifest.replay_after_wal_id();

        // Adoption is one-way: before the first fold the store's own rows must
        // all sit below the log's first sequence, or folding would file every
        // slot underneath them and the store would serve its own history over
        // the log's.
        let last_row = manifest.manifest().last_l0_seq();
        if folded == 0 && last_row >= SEQUENCE_BASE {
            return Err(WalError::DataError(Arc::new(Error::corruption(format!(
                "the store has written {last_row} rows of its own, at or past the sequence \
                 {SEQUENCE_BASE} the log's first slot takes; it cannot adopt this log"
            )))));
        }

        let end = match self.wal.replay_limit {
            Some(limit) => self
                .wal
                .tail_end(folded)
                .await?
                .min(folded.saturating_add(limit)),
            None => self.wal.tail_end(folded).await?,
        };

        Ok(WriterInitResult {
            replay_iterator: Box::new(
                self.wal
                    .iterator(folded.saturating_add(1), Bound::Included(end)),
            ),
            wal_writer: Box::new(SlotWalWriter::new(end)),
        })
    }
}

/// The writer side of a log nothing writes through: it reports the tail the
/// replay reached, refuses appends, and has nothing to flush.
struct SlotWalWriter {
    state: Arc<Mutex<WriterState>>,
}

/// A writer's status and the listeners watching it.
struct WriterState {
    status: WalStatus,
    listeners: Vec<WalStatusListener>,
}

impl SlotWalWriter {
    fn new(last_flushed: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(WriterState {
                status: WalStatus {
                    closed_reason: None,
                    estimated_bytes: 0,
                    last_flushed_wal_id: last_flushed,
                    last_flushed_seq: (last_flushed > 0).then_some(last_flushed),
                    buffered_wal_entries_count: 0,
                },
                listeners: Vec::new(),
            })),
        }
    }
}

/// The status a state holds, as the WAL's traits report it: an open writer's
/// status is `Ok`, a closed one's is `Err` carrying the reason it closed.
fn reported(state: &Arc<Mutex<WriterState>>) -> Result<WalStatus, WalStatus> {
    let status = state
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .status
        .clone();
    match status.closed_reason {
        Some(_) => Err(status),
        None => Ok(status),
    }
}

#[async_trait]
impl WalWriter for SlotWalWriter {
    async fn append(&mut self, _write_batch: &[RowEntry]) -> Result<(), WalError> {
        Err(WalError::InternalError(Arc::new(AppendRefused)))
    }

    async fn flush(&mut self) -> Result<FlushResultFuture, WalError> {
        // Nothing is buffered here: a slot is durable when its committer's put
        // returns, long before any store sees it.
        Ok(Box::pin(std::future::ready(Ok(()))))
    }

    fn observer(&self) -> Box<dyn WalObserver> {
        Box::new(SlotWalObserver {
            state: Arc::clone(&self.state),
        })
    }

    fn status(&self) -> Result<WalStatus, WalStatus> {
        reported(&self.state)
    }

    async fn close(&mut self) -> Result<(), WalError> {
        let closed = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.status.closed_reason = Some(WalError::Closed);
            let closed = state.status.clone();
            let listeners = std::mem::take(&mut state.listeners);
            drop(state);
            for listener in &listeners {
                listener(WalEvent::WalClosed(closed.clone()));
            }
            closed
        };
        let _ = closed;

        Ok(())
    }
}

/// The observer over a writer's status. Nothing ever advances it — the fold's
/// whole input was durable before the store opened — so a subscriber hears
/// only the close.
struct SlotWalObserver {
    state: Arc<Mutex<WriterState>>,
}

#[async_trait]
impl WalObserver for SlotWalObserver {
    fn status(&self) -> Result<WalStatus, WalStatus> {
        reported(&self.state)
    }

    fn subscribe(&self, listener: WalStatusListener) -> Result<(), WalError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match state.status.closed_reason.clone() {
            Some(_) => {
                let closed = state.status.clone();
                drop(state);
                listener(WalEvent::WalClosed(closed));
            }
            None => state.listeners.push(listener),
        }

        Ok(())
    }
}

#[async_trait]
impl WalReader for SlotWal {
    async fn iterator(
        &self,
        wal_file_id_range: WalFileRange,
    ) -> Result<Box<dyn WalIterator>, WalError> {
        let start = match wal_file_id_range.0 {
            Bound::Included(start) => start,
            Bound::Excluded(start) => start.saturating_add(1),
            Bound::Unbounded => {
                return Err(WalError::InternalError(Arc::new(Error::corruption(
                    "a slot-log iterator needs a lower bound; the log's prefix is truncated, so \
                     there is no first slot to read from"
                        .to_string(),
                ))));
            }
        };

        Ok(Box::new(SlotWal::iterator(
            self,
            start,
            wal_file_id_range.1,
        )))
    }

    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        self.tail_end(replay_after_wal_id).await
    }
}

/// A reader-side WAL that replays nothing, so the store it opens serves the
/// folded state alone.
///
/// Every ordinary reader replays the tail, which is what makes it current —
/// and what leaves it unable to say which of its rows the fold put there and
/// which the replay did. Reading the two apart takes a view with no replay in
/// it, at the cursor that same open reports.
struct FoldedOnly;

#[async_trait]
impl WalReader for FoldedOnly {
    async fn iterator(
        &self,
        _wal_file_id_range: WalFileRange,
    ) -> Result<Box<dyn WalIterator>, WalError> {
        Ok(Box::new(EmptyWalIterator))
    }

    /// The cursor itself: nothing follows what the fold already applied.
    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        Ok(replay_after_wal_id)
    }
}

struct EmptyWalIterator;

#[async_trait]
impl WalIterator for EmptyWalIterator {
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        Ok(None)
    }
}

/// Reads slots in order, one GET each, and turns each into the batch its
/// writes make. An unbounded end follows the log as committers extend it.
struct SlotWalIterator {
    wal: SlotWal,
    next: u64,
    end: Bound<u64>,
    /// Whether this iterator has returned a slot. A run that has started and
    /// then meets an absent slot met damage; one that meets it at its first
    /// sequence met a truncated prefix.
    consumed: bool,
}

impl SlotWalIterator {
    /// Whether `next` has passed the range's end.
    fn exhausted(&self) -> bool {
        match self.end {
            Bound::Included(end) => self.next > end,
            Bound::Excluded(end) => self.next >= end,
            Bound::Unbounded => false,
        }
    }
}

#[async_trait]
impl WalIterator for SlotWalIterator {
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        loop {
            if self.exhausted() {
                return Ok(None);
            }

            if let Some(envelope) = self.wal.log.read_slot(self.next).await.map_err(wal_error)? {
                let rows = self.wal.rows(self.next, &envelope);
                let last_consumed_wal_file_id = self.next;
                self.next = self.next.saturating_add(1);
                self.consumed = true;

                return Ok(Some(WalRows {
                    rows,
                    last_consumed_wal_file_id,
                }));
            }

            // An absent slot is the end of the log, a prefix another process
            // truncated, or damage — and one LIST tells them apart.
            let extent = self
                .wal
                .log
                .tail_extent(self.next)
                .await
                .map_err(wal_error)?;
            match (extent.last, extent.gap_at) {
                // It landed between the GET and the LIST: an ordinary race
                // with the committer that was writing it.
                (Some(_), _) => {}
                (None, Some(_)) if self.consumed => {
                    return Err(WalError::DataError(Arc::new(Error::corruption(format!(
                        "slot {} is absent while higher slots are present; a committed slot was \
                         destroyed outside the protocol",
                        self.next
                    )))));
                }
                // Nothing consumed yet, so the log's prefix moved out from
                // under this range: truncation reclaims folded slots, and the
                // store's fold cursor has moved with it.
                (None, Some(_)) => return Err(WalError::WalTruncated(self.next)),
                (None, None) if matches!(self.end, Bound::Unbounded) => {
                    tokio::time::sleep(self.wal.tail_poll).await;
                }
                (None, None) => return Ok(None),
            }
        }
    }
}

#[async_trait]
impl WalGc for SlotWal {
    async fn collect(
        &self,
        referenced_ranges: Vec<WalFileRange>,
        min_age: Duration,
        dry_run: bool,
    ) -> Result<(), WalError> {
        // Every live manifest references a range that runs from its own fold
        // cursor upwards, so the deepest a truncation may reach is the lowest
        // of those starts: below it no store, and no reader following one,
        // still replays. No referenced range at all says nothing about the log
        // — it says no manifest was read — so nothing is deleted.
        let Some(horizon) = referenced_ranges.iter().map(range_start).min() else {
            return Ok(());
        };

        let now = unix_seconds_now();
        for slot in self
            .log
            .list_slots(0)
            .await
            .map_err(wal_error)?
            .into_iter()
            .filter(|slot| slot.sequence < horizon)
        {
            let age = now.saturating_sub(slot.written_unix_seconds);
            if u64::try_from(age).unwrap_or(0) < min_age.as_secs() {
                continue;
            }
            if !dry_run {
                self.log
                    .delete_slot(slot.sequence)
                    .await
                    .map_err(wal_error)?;
            }
        }

        Ok(())
    }
}

/// The lowest sequence a referenced range covers.
fn range_start(range: &WalFileRange) -> u64 {
    match range.0 {
        Bound::Included(start) => start,
        Bound::Excluded(start) => start.saturating_add(1),
        Bound::Unbounded => 0,
    }
}

/// Milliseconds since the Unix epoch; 0 if the clock is unreadable.
fn unix_millis_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

/// Seconds since the Unix epoch; 0 if the clock is unreadable, which retains
/// rather than deletes.
fn unix_seconds_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::{ObjectStore, memory::InMemory};
    use slatedb::{Db, DbReader, config::Settings};

    use super::*;
    use crate::envelope::{Commit, Envelope, SlotPayload, SlotWrite};

    /// A commit writing `writes`, under a transaction id derived from `id`.
    fn commit(id: u8, writes: &[(&[u8], Option<&[u8]>)]) -> Commit {
        Commit {
            transaction_id: [id; 16],
            payload: SlotPayload {
                validated_head: 0,
                changes_made: String::new(),
                writes: writes
                    .iter()
                    .map(|(key, value)| SlotWrite {
                        key: (*key).to_vec(),
                        value: value.map(<[u8]>::to_vec),
                    })
                    .collect(),
            },
        }
    }

    /// Wins `sequence` with one commit's writes.
    #[allow(clippy::unwrap_used)]
    async fn win(log: &SlotLog, sequence: u64, id: u8, writes: &[(&[u8], Option<&[u8]>)]) {
        log.put_slot(sequence, &Envelope::new(vec![commit(id, writes)]))
            .await
            .unwrap();
    }

    /// A store that journals nothing of its own: the log is the journal.
    fn folding_settings() -> Settings {
        Settings {
            wal_enabled: false,
            ..Settings::default()
        }
    }

    /// Opens the folding writer over `store` at `path`.
    #[allow(clippy::unwrap_used)]
    async fn open_folder(wal: &SlotWal, path: &str, store: Arc<dyn ObjectStore>) -> Db {
        Db::builder(path, store)
            .with_settings(folding_settings())
            .with_wal_writer(wal.writer_init())
            .build()
            .await
            .unwrap()
    }

    /// Opening the store replays the slots past its fold cursor, so the
    /// store's own reads answer from the log without anything having applied
    /// it: opening *is* the fold. Flushing makes that fold durable, which the
    /// truncated log then proves — the values survive with no slot left to
    /// replay.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn opening_the_store_folds_the_log_and_flushing_makes_it_durable() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log = SlotLog::new(Arc::clone(&store), "catalog");
        let wal = SlotWal::new(log.clone());

        win(&log, 1, 1, &[(b"orders", Some(b"first"))]).await;
        win(
            &log,
            2,
            2,
            &[(b"orders", Some(b"second")), (b"items", Some(b"x"))],
        )
        .await;

        let db = open_folder(&wal, "catalog", Arc::clone(&store)).await;
        assert_eq!(
            db.get(b"orders").await.unwrap().unwrap().as_ref(),
            b"second",
            "the replayed tail is the folded state"
        );
        db.flush().await.unwrap();
        db.close().await.unwrap();

        // The fold is durable: with every slot deleted there is nothing left
        // to replay, and the store still answers.
        assert_eq!(log.truncate_through(2).await.unwrap(), 2);
        let db = open_folder(&wal, "catalog", Arc::clone(&store)).await;
        assert_eq!(
            db.get(b"orders").await.unwrap().unwrap().as_ref(),
            b"second"
        );
        assert_eq!(db.get(b"items").await.unwrap().unwrap().as_ref(), b"x");
        db.close().await.unwrap();
    }

    /// A slot that deletes a key folds as a delete, not as a missing write.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn a_slots_delete_folds_as_a_delete() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log = SlotLog::new(Arc::clone(&store), "");
        let wal = SlotWal::new(log.clone());

        win(&log, 1, 1, &[(b"orders", Some(b"first"))]).await;
        win(&log, 2, 2, &[(b"orders", None)]).await;

        let db = open_folder(&wal, "", store).await;
        assert!(db.get(b"orders").await.unwrap().is_none());
        db.close().await.unwrap();
    }

    /// A reader plugged into the log serves what no fold has applied: the
    /// slots above the store's cursor are replayed into the reader itself.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn a_reader_serves_the_unfolded_tail() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log = SlotLog::new(Arc::clone(&store), "");
        let wal = SlotWal::new(log.clone());

        // A store exists, folded through nothing.
        let db = open_folder(&wal, "", Arc::clone(&store)).await;
        db.close().await.unwrap();
        win(&log, 1, 1, &[(b"orders", Some(b"unfolded"))]).await;

        let reader = DbReader::builder("", Arc::clone(&store))
            .with_wal_reader(wal.reader())
            .build()
            .await
            .unwrap();
        assert_eq!(
            reader.get(b"orders").await.unwrap().unwrap().as_ref(),
            b"unfolded",
            "the reader replays the tail the store has not folded"
        );
        reader.close().await.unwrap();
    }

    /// Two writes to one key inside a slot fold last-writer-wins, so the slot
    /// contributes one row per key at its own sequence number.
    #[test]
    fn a_slots_writes_fold_last_writer_wins() {
        let wal = SlotWal::new(SlotLog::new(Arc::new(InMemory::new()), ""));
        let envelope = Envelope::new(vec![
            commit(1, &[(b"k", Some(b"first"))]),
            commit(2, &[(b"k", Some(b"second"))]),
        ]);

        let rows = wal.rows(7, &envelope);
        assert_eq!(rows.len(), 1, "one row per key");
        assert_eq!(
            rows[0].seq,
            slot_sequence(7),
            "the slot's ordinal fixes the sequence number"
        );
        assert_eq!(
            rows[0].value,
            ValueDeletable::Value(Bytes::from_static(b"second"))
        );
    }

    /// An advert projects into a write when the embedder folds adverts, and
    /// into nothing when it does not.
    #[test]
    fn an_advert_folds_only_where_the_embedder_projects_it() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "");
        let envelope = Envelope::new(vec![]).with_advert(LeaderAdvert {
            instance: [3; 16],
            endpoint: Some("host:1".to_string()),
        });

        assert!(SlotWal::new(log.clone()).rows(1, &envelope).is_empty());

        let projecting = SlotWal::new(log).with_advert_projection(Arc::new(|advert| {
            advert
                .endpoint
                .as_ref()
                .map(|endpoint| (b"sys/leader".to_vec(), endpoint.as_bytes().to_vec()))
        }));
        let rows = projecting.rows(1, &envelope);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key.as_ref(), b"sys/leader");
    }

    /// The log takes committed slots only: a store that tries to journal its
    /// own write through it is refused rather than told the write is safe.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn a_store_write_cannot_be_journaled_through_the_log() {
        let mut writer = SlotWalWriter::new(0);
        let refused = writer.append(&[]).await;
        assert!(
            matches!(refused, Err(WalError::InternalError(_))),
            "an append must be refused, not accepted"
        );
    }

    /// A hole above the fold cursor is damage: the store refuses to open on
    /// it rather than folding the prefix and letting the next committer
    /// re-win the destroyed sequence.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn a_hole_above_the_fold_cursor_refuses_to_fold() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log = SlotLog::new(Arc::clone(&store), "");
        let wal = SlotWal::new(log.clone());

        win(&log, 1, 1, &[(b"a", Some(b"1"))]).await;
        win(&log, 2, 2, &[(b"b", Some(b"2"))]).await;
        log.delete_slot(1).await.unwrap();

        let opened = Db::builder("", store)
            .with_settings(folding_settings())
            .with_wal_writer(wal.writer_init())
            .build()
            .await;
        assert!(opened.is_err(), "a destroyed slot must refuse the fold");
    }

    /// Collection deletes below the lowest referenced range and nothing else:
    /// a slot a live manifest still replays outlives every collection.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn collection_deletes_only_below_the_lowest_referenced_range() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log = SlotLog::new(Arc::clone(&store), "");
        let wal = SlotWal::new(log.clone());
        for sequence in 1..=5u64 {
            win(&log, sequence, u8::try_from(sequence).unwrap(), &[]).await;
        }

        // No referenced range at all says nothing about the log, so nothing
        // goes.
        wal.collect(Vec::new(), Duration::ZERO, false)
            .await
            .unwrap();
        assert_eq!(log.tail_length(1).await.unwrap(), 5);

        // A dry run reports without deleting.
        let referenced = vec![WalFileRange(Bound::Included(3), Bound::Unbounded)];
        wal.collect(referenced.clone(), Duration::ZERO, true)
            .await
            .unwrap();
        assert_eq!(log.tail_length(1).await.unwrap(), 5);

        wal.collect(referenced, Duration::ZERO, false)
            .await
            .unwrap();
        assert!(log.read_slot(1).await.unwrap().is_none());
        assert!(log.read_slot(2).await.unwrap().is_none());
        assert!(log.read_slot(3).await.unwrap().is_some());
        assert_eq!(log.tail_length(3).await.unwrap(), 3);
    }

    /// A retention margin holds a slot back: one written this instant is
    /// younger than the minimum age, so it survives a collection that would
    /// otherwise take it.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn a_slot_younger_than_the_minimum_age_survives() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log = SlotLog::new(Arc::clone(&store), "");
        let wal = SlotWal::new(log.clone());
        win(&log, 1, 1, &[]).await;

        wal.collect(
            vec![WalFileRange(Bound::Included(9), Bound::Unbounded)],
            Duration::from_secs(3600),
            false,
        )
        .await
        .unwrap();
        assert!(
            log.read_slot(1).await.unwrap().is_some(),
            "a slot inside the retention margin is not deleted"
        );
    }

    /// A bounded iterator ends where its range does; an unbounded one waits
    /// for the committer instead of reporting the log finished.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn a_bounded_iterator_ends_and_an_unbounded_one_follows() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log = SlotLog::new(Arc::clone(&store), "");
        let wal = SlotWal::new(log.clone()).with_tail_poll(Duration::from_millis(1));
        win(&log, 1, 1, &[(b"a", Some(b"1"))]).await;

        let mut bounded =
            WalReader::iterator(&wal, WalFileRange(Bound::Included(1), Bound::Included(1)))
                .await
                .unwrap();
        assert_eq!(
            bounded
                .next()
                .await
                .unwrap()
                .unwrap()
                .last_consumed_wal_file_id,
            1
        );
        assert!(bounded.next().await.unwrap().is_none());

        let mut following =
            WalReader::iterator(&wal, WalFileRange(Bound::Included(2), Bound::Unbounded))
                .await
                .unwrap();
        let follow = tokio::spawn(async move { following.next().await });
        win(&log, 2, 2, &[(b"b", Some(b"2"))]).await;
        let rows = follow.await.unwrap().unwrap().unwrap();
        assert_eq!(rows.last_consumed_wal_file_id, 2);
        assert_eq!(rows.rows[0].key.as_ref(), b"b");
    }

    /// A truncated prefix and a destroyed slot are the same absence, and the
    /// difference is whether the run had started: a range that begins on the
    /// hole reports truncation (its reader refreshes and moves on), and one
    /// that meets a hole mid-run reports damage.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn a_hole_reads_as_truncation_at_the_range_start_and_damage_within_it() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log = SlotLog::new(Arc::clone(&store), "");
        let wal = SlotWal::new(log.clone());
        for sequence in 1..=3u64 {
            win(&log, sequence, u8::try_from(sequence).unwrap(), &[]).await;
        }
        log.delete_slot(2).await.unwrap();

        let mut from_the_hole =
            WalReader::iterator(&wal, WalFileRange(Bound::Included(2), Bound::Unbounded))
                .await
                .unwrap();
        assert!(matches!(
            from_the_hole.next().await,
            Err(WalError::WalTruncated(2))
        ));

        let mut across_the_hole =
            WalReader::iterator(&wal, WalFileRange(Bound::Included(1), Bound::Unbounded))
                .await
                .unwrap();
        assert!(across_the_hole.next().await.unwrap().is_some());
        assert!(matches!(
            across_the_hole.next().await,
            Err(WalError::DataError(_))
        ));
    }
    /// A store still writes some rows of its own, and the stride is what keeps
    /// those from shadowing the log: they take the numbers between the slot
    /// last folded and the next one, so a fold that follows them still lands,
    /// and the writes stay visible over everything folded before them.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn a_stores_own_writes_interleave_with_the_slots_it_folds() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log = SlotLog::new(Arc::clone(&store), "");
        let wal = SlotWal::new(log.clone());

        // The store takes writes of its own — a bootstrap would — and flushes
        // them, which carries their sequence numbers into L0.
        let db = open_folder(&wal, "", Arc::clone(&store)).await;
        for index in 0..6u8 {
            db.put(&[b'k', index], b"direct").await.unwrap();
        }
        db.flush().await.unwrap();
        db.close().await.unwrap();

        // Commits then arrive through the log and fold over them.
        for sequence in 1..=3u64 {
            win(
                &log,
                sequence,
                u8::try_from(sequence).unwrap(),
                &[(b"orders", Some(b"committed"))],
            )
            .await;
        }
        let db = open_folder(&wal, "", Arc::clone(&store)).await;
        assert_eq!(
            db.get(b"orders").await.unwrap().unwrap().as_ref(),
            b"committed",
            "the fold lands over the store's own writes"
        );

        // A write the store takes after that fold outranks what the fold
        // applied, and the slot that follows outranks the write.
        db.put(b"orders", b"maintenance").await.unwrap();
        db.flush().await.unwrap();
        assert_eq!(
            db.get(b"orders").await.unwrap().unwrap().as_ref(),
            b"maintenance"
        );
        db.close().await.unwrap();

        win(&log, 4, 4, &[(b"orders", Some(b"later commit"))]).await;
        let db = open_folder(&wal, "", store).await;
        assert_eq!(
            db.get(b"orders").await.unwrap().unwrap().as_ref(),
            b"later commit",
            "a slot folded after a direct write outranks it"
        );
        db.close().await.unwrap();
    }

    /// A store that has already written past the log's first sequence cannot
    /// adopt it: folding would file every slot under its own history.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn a_store_past_the_first_sequence_cannot_adopt_the_log() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log = SlotLog::new(Arc::clone(&store), "");
        let wal = SlotWal::new(log.clone());

        // A store whose own rows reach the log's first sequence: written with
        // an explicit sequence number, since no test writes four billion rows.
        let db = open_folder(&wal, "", Arc::clone(&store)).await;
        db.write_with_options(
            {
                let mut batch = slatedb::WriteBatch::new();
                batch.put(b"k", b"far along");
                batch
            },
            &slatedb::config::WriteOptions {
                seqnum: slot_sequence(1),
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let refused = Db::builder("", store)
            .with_settings(folding_settings())
            .with_wal_writer(wal.writer_init())
            .build()
            .await;
        assert!(refused.is_err(), "adoption must be refused, not folded");
    }
}
