//! An in-memory commit log implementing the whole seam: the plugs a store
//! opens over, and the commit store with the loop in miniature — offers,
//! folds after a loss, the fold cadence through the catalog's taker, flush
//! by count, and the idle poll. The reference the open repository tests the
//! seam against; it stands in for the library, not for the store, which
//! stays real SlateDB.

use std::{
    any::Any,
    collections::BTreeMap,
    ops::Bound,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::{
    Db, DbReader, RowEntry, ValueDeletable,
    wal::{
        WalError, WalEvent, WalFileRange, WalFlushResult, WalGc, WalIterator, WalObserver,
        WalReader, WalRows, WalStatus, WalStatusListener, WalWriter, WriterInit, WriterInitResult,
        WriterManifest,
    },
};

use super::{
    CatalogHooks, CommitLogProvider, CommitStore, ReaderSession, SlotAdoption, SlotCommit,
    SlotEnvelope, SlotOffer, WriterSession,
};
use crate::error::{Error, Result};

/// The row sequence the first slot takes, above anything a store wrote
/// before adopting the log.
const SEQUENCE_BASE: u64 = 1 << 32;
/// The row sequences one slot reserves, unless a test shrinks it.
const SEQUENCE_STRIDE: u64 = 1 << 20;

/// The row sequence slot `ordinal` writes at.
fn slot_sequence(stride: u64, ordinal: u64) -> u64 {
    SEQUENCE_BASE.saturating_add(ordinal.saturating_mul(stride))
}

/// One log: what the bucket would hold, shared by every process over it.
#[derive(Default)]
pub(crate) struct TestLog {
    slots: Mutex<BTreeMap<u64, SlotEnvelope>>,
    /// Slots a peer landed after this process last read: no listing shows
    /// them until an offer meets them.
    unseen: Mutex<BTreeMap<u64, SlotEnvelope>>,
    /// The WAL id slot 1 counts from, fixed at adoption.
    base: Mutex<Option<u64>>,
}

/// One process's view of the log: what it has folded or been served, the
/// base an adoption would fix, and the writer its folds notify.
#[derive(Default)]
struct Cursor {
    folded: AtomicU64,
    served: tokio::sync::watch::Sender<u64>,
    writer: Mutex<Option<Arc<Mutex<WriterState>>>>,
    /// The base an adoption would fix: past the store's own WAL, as the
    /// last writer open saw it.
    candidate: AtomicU64,
    /// How many times a reader over this cursor has asked for the log's end.
    polls: AtomicU64,
}

impl std::fmt::Debug for TestLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestLog")
            .field("slots", &self.slots())
            .finish_non_exhaustive()
    }
}

impl TestLog {
    /// The slots landed so far, in order.
    pub(crate) fn slots(&self) -> Vec<(u64, SlotEnvelope)> {
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(sequence, envelope)| (*sequence, envelope.clone()))
            .collect()
    }

    /// Lands `envelope` at `sequence` as a peer would, outside any
    /// committer; panics if the sequence is taken.
    pub(crate) fn land(&self, sequence: u64, envelope: SlotEnvelope) {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(
            slots.insert(sequence, envelope).is_none(),
            "slot {sequence} was already taken"
        );
        drop(slots);
    }

    /// Lands `envelope` at `sequence` as a peer would in the moment after
    /// this process last read the log: no reader is served it until an
    /// offer runs into it, which is how a race is lost.
    pub(crate) fn land_unseen(&self, sequence: u64, envelope: SlotEnvelope) {
        let taken = self
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&sequence);
        assert!(!taken, "slot {sequence} was already taken");
        let mut unseen = self.unseen.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(
            unseen.insert(sequence, envelope).is_none(),
            "slot {sequence} was already landed unseen"
        );
        drop(unseen);
    }

    /// An offer reaches the log itself, where every landed slot shows.
    fn reveal(&self) {
        let unseen =
            std::mem::take(&mut *self.unseen.lock().unwrap_or_else(PoisonError::into_inner));
        if unseen.is_empty() {
            return;
        }
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(unseen);
    }

    /// The WAL id slot 1 counts from: fixed at adoption, zero for a log
    /// born with its store, unknown before either.
    fn wal_base(&self) -> Option<u64> {
        let fixed = *self.base.lock().unwrap_or_else(PoisonError::into_inner);
        fixed.or_else(|| (self.last_sequence() > 0).then_some(0))
    }

    fn last_sequence(&self) -> u64 {
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0)
    }

    fn slot(&self, sequence: u64) -> Option<SlotEnvelope> {
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&sequence)
            .cloned()
    }

    fn rows(stride: u64, sequence: u64, envelope: &SlotEnvelope) -> Vec<RowEntry> {
        let mut folded: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        for write in envelope.writes() {
            folded.insert(write.key.clone(), write.value.clone());
        }
        let read_at = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);
        folded
            .into_iter()
            .map(|(key, value)| RowEntry {
                key: Bytes::from(key),
                value: value.map_or(ValueDeletable::Tombstone, |value| {
                    ValueDeletable::Value(Bytes::from(value))
                }),
                seq: slot_sequence(stride, sequence),
                create_ts: Some(read_at),
                expire_ts: None,
            })
            .collect()
    }
}

impl Cursor {
    fn status(&self, log: &TestLog, state: &WriterState) -> WalStatus {
        let base = log
            .wal_base()
            .unwrap_or_else(|| self.candidate.load(Ordering::Acquire));
        let last_flushed_wal_id = base.saturating_add(self.folded.load(Ordering::Acquire));
        WalStatus {
            closed_reason: state.closed_reason.clone(),
            estimated_bytes: 0,
            last_flushed_wal_id,
            last_flushed_seq: state.acknowledged,
            buffered_wal_entries_count: 0,
        }
    }

    fn known(&self) -> u64 {
        self.folded
            .load(Ordering::Acquire)
            .max(*self.served.borrow())
    }

    /// Waits for `count` further polls of the log's end, or `timeout`; a
    /// poll in flight when this starts does not count, so two cover one
    /// whole tick. Stands in for a refresh the reader cannot be asked for.
    async fn await_polls(&self, count: u64, timeout: Duration) {
        let target = self.polls.load(Ordering::Acquire).saturating_add(count);
        let deadline = Instant::now() + timeout;
        while self.polls.load(Ordering::Acquire) < target {
            if Instant::now() >= deadline {
                tracing::warn!(target, "the reader did not poll in time");
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn serve(&self, sequence: u64) {
        self.served.send_if_modified(|served| {
            let advanced = sequence > *served;
            if advanced {
                *served = sequence;
            }
            advanced
        });
    }

    /// Reports `sequence` folded, telling the writer its folds notify.
    fn mark_folded(&self, log: &TestLog, sequence: u64) {
        let before = self.folded.fetch_max(sequence, Ordering::AcqRel);
        if sequence <= before {
            return;
        }
        let writer = self
            .writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(state) = writer {
            let (status, listeners) = {
                let state = state.lock().unwrap_or_else(PoisonError::into_inner);
                (self.status(log, &state), state.listeners.clone())
            };
            for listener in &listeners {
                listener(WalEvent::WalFlushed(status.clone()));
            }
        }
    }
}

/// One process over a [`TestLog`], whatever store or path it is asked for.
pub(crate) struct TestProvider {
    pub(crate) log: Arc<TestLog>,
    cursor: Arc<Cursor>,
    external_compaction: bool,
    /// The row sequences one slot reserves.
    stride: u64,
}

impl Default for TestProvider {
    fn default() -> Self {
        Self {
            log: Arc::default(),
            cursor: Arc::default(),
            external_compaction: false,
            stride: SEQUENCE_STRIDE,
        }
    }
}

impl std::fmt::Debug for TestProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestProvider")
            .field("log", &self.log)
            .field("folded", &self.folded_sequence())
            .finish_non_exhaustive()
    }
}

impl TestProvider {
    /// Another process over the same log, with a cursor of its own.
    pub(crate) fn sharing(&self) -> Self {
        Self {
            log: Arc::clone(&self.log),
            cursor: Arc::new(Cursor::default()),
            external_compaction: self.external_compaction,
            stride: self.stride,
        }
    }

    /// A provider whose deployment compacts in another process.
    pub(crate) fn externally_compacted(self) -> Self {
        Self {
            external_compaction: true,
            ..self
        }
    }

    /// The last sequence this process reported applied.
    pub(crate) fn folded_sequence(&self) -> u64 {
        self.cursor.folded.load(Ordering::Acquire)
    }

    /// How many times this process's reader has polled the log's end: once
    /// at open, once at its first manifest poll, which fires at once on a
    /// background task, and once per interval after.
    pub(crate) fn polls(&self) -> u64 {
        self.cursor.polls.load(Ordering::Acquire)
    }

    /// Reports `sequence` served before this process's reader has applied
    /// it: the window a real plug opens for a moment, held open.
    pub(crate) fn report_served(&self, sequence: u64) {
        self.cursor.serve(sequence);
    }

    /// Waits until this process's reader has been served every slot
    /// through `sequence`, or `timeout` passes. Returns whether it was.
    pub(crate) async fn await_served(&self, sequence: u64, timeout: Duration) -> bool {
        let mut served = self.cursor.served.subscribe();
        tokio::time::timeout(timeout, served.wait_for(|reached| *reached >= sequence))
            .await
            .is_ok_and(|waited| waited.is_ok())
    }

    fn session(&self, tail: bool) -> Arc<TestSession> {
        Arc::new(TestSession {
            log: Arc::clone(&self.log),
            cursor: Arc::clone(&self.cursor),
            stride: self.stride,
            tail,
        })
    }
}

impl CommitLogProvider for TestProvider {
    fn external_compaction(&self) -> bool {
        self.external_compaction
    }

    fn writer_session(
        &self,
        _log_store: Arc<dyn ObjectStore>,
        _path: &str,
    ) -> Arc<dyn WriterSession> {
        self.session(false)
    }

    fn reader_session(
        &self,
        _log_store: Arc<dyn ObjectStore>,
        _path: &str,
        tail: bool,
    ) -> Arc<dyn ReaderSession> {
        self.session(tail)
    }

    fn holding(
        &self,
        db: Db,
        session: Arc<dyn WriterSession>,
        hooks: CatalogHooks,
    ) -> Arc<dyn CommitStore> {
        let session = own_session(session.as_any());
        TestStore::new(Side::Holder(db), session, hooks)
    }

    fn following(
        &self,
        reader: Arc<DbReader>,
        session: Arc<dyn ReaderSession>,
        hooks: CatalogHooks,
    ) -> Arc<dyn CommitStore> {
        let session = own_session(session.as_any());
        TestStore::new(Side::Follower(reader), session, hooks)
    }

    fn garbage_collector(&self, _log_store: Arc<dyn ObjectStore>, _path: &str) -> Arc<dyn WalGc> {
        Arc::new(TestGc {
            log: Arc::clone(&self.log),
        })
    }
}

/// The session as this log minted it; a catalog never pairs a store with
/// another library's session.
fn own_session(session: &dyn Any) -> TestSession {
    session
        .downcast_ref::<TestSession>()
        .expect("the session was minted by this provider")
        .clone()
}

/// One open's cursor over the log, shared by the plug the open builds its
/// store with and the commit store built over both.
#[derive(Clone)]
pub(crate) struct TestSession {
    log: Arc<TestLog>,
    cursor: Arc<Cursor>,
    stride: u64,
    tail: bool,
}

#[async_trait]
impl WriterSession for TestSession {
    fn writer_init(&self) -> Box<dyn WriterInit> {
        Box::new(TestWriterInit {
            log: Arc::clone(&self.log),
            cursor: Arc::clone(&self.cursor),
            stride: self.stride,
        })
    }

    async fn adopted(&self) -> Result<bool> {
        Ok(self.log.wal_base().is_some())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl ReaderSession for TestSession {
    async fn refresh(&self, _reader: &DbReader) -> Result<()> {
        self.cursor
            .await_polls(2, super::READER_CATCHUP_TIMEOUT)
            .await;
        Ok(())
    }

    fn reader(&self) -> Arc<dyn WalReader> {
        Arc::new(TestReader {
            log: Arc::clone(&self.log),
            cursor: Arc::clone(&self.cursor),
            stride: self.stride,
            tail: self.tail,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Which side of the log a store is on.
enum Side {
    Holder(Db),
    Follower(Arc<DbReader>),
}

/// The commit store over the test log: the loop in miniature.
pub(crate) struct TestStore {
    log: Arc<TestLog>,
    cursor: Arc<Cursor>,
    side: Side,
    hooks: CatalogHooks,
    /// Serializes offers and the folds that follow them.
    guard: tokio::sync::Mutex<()>,
    folded_since_flush: AtomicU64,
    closed: AtomicBool,
    poll: Mutex<
        Option<(
            tokio::sync::watch::Sender<bool>,
            tokio::task::JoinHandle<()>,
        )>,
    >,
}

impl std::fmt::Debug for TestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestStore")
            .field("holder", &matches!(self.side, Side::Holder(_)))
            .field("folded", &self.cursor.folded.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl TestStore {
    fn new(side: Side, session: TestSession, hooks: CatalogHooks) -> Arc<Self> {
        let store = Arc::new(Self {
            log: session.log,
            cursor: session.cursor,
            side,
            hooks,
            guard: tokio::sync::Mutex::new(()),
            folded_since_flush: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            poll: Mutex::new(None),
        });
        if matches!(store.side, Side::Holder(_)) && !store.hooks.settings.poll.is_zero() {
            let (stop, mut stopped) = tokio::sync::watch::channel(false);
            let weak = Arc::downgrade(&store);
            let interval = store.hooks.settings.poll;
            let task = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = tokio::time::sleep(interval) => {}
                        _ = stopped.changed() => return,
                    }
                    let Some(store) = weak.upgrade() else {
                        return;
                    };
                    let _held = tokio::select! {
                        biased;
                        _ = stopped.changed() => return,
                        held = store.guard.lock() => held,
                    };
                    if let Side::Holder(db) = &store.side
                        && let Err(error) = store.fold_from_cursor(db, &store.cursor).await
                    {
                        tracing::warn!(%error, "the idle fold failed; it retries next poll");
                    }
                }
            });
            *store.poll.lock().unwrap_or_else(PoisonError::into_inner) = Some((stop, task));
        }

        store
    }

    fn refuse_if_closed(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Constraint("the commit store is closed".to_string()));
        }

        Ok(())
    }

    /// Writes one slot's rows into `db`, last writer wins within it.
    async fn apply(&self, db: &Db, envelope: &SlotEnvelope) -> Result<()> {
        let mut collapsed: BTreeMap<&[u8], Option<&[u8]>> = BTreeMap::new();
        for write in envelope.writes() {
            collapsed.insert(&write.key, write.value.as_deref());
        }
        let mut batch = slatedb::WriteBatch::new();
        for (key, value) in collapsed {
            match value {
                Some(value) => batch.put(key, value),
                None => batch.delete(key),
            }
        }
        db.write_with_options(batch, &slatedb::config::WriteOptions::default())
            .await
            .map(|_| ())
            .map_err(Error::from)
    }

    /// Reports `sequence` folded, flushing and collecting on the count.
    async fn folded(&self, db: &Db, cursor: &Cursor, sequence: u64) -> Result<()> {
        cursor.mark_folded(&self.log, sequence);
        let since = self
            .folded_since_flush
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if since >= self.hooks.settings.flush_every {
            self.write_out(db).await?;
        }

        Ok(())
    }

    /// Writes the memtable out and collects the log below the replay point.
    async fn write_out(&self, db: &Db) -> Result<()> {
        db.flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .map_err(Error::from)?;
        self.hooks.publish(db).await?;
        self.folded_since_flush.store(0, Ordering::Release);
        let directory = slatedb::config::GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: self.hooks.settings.retention,
            dry_run: false,
        };
        slatedb::GarbageCollectorBuilder::new(
            self.hooks.path.clone(),
            Arc::clone(&self.hooks.object_store),
        )
        .with_wal_object_store(Arc::clone(&self.hooks.object_store))
        .with_wal_gc(Arc::new(TestGc {
            log: Arc::clone(&self.log),
        }))
        .with_options(slatedb::config::GarbageCollectorOptions {
            manifest_options: Some(directory),
            wal_options: Some(directory),
            wal_fence_options: None,
            compacted_options: None,
            compactions_options: None,
            ..Default::default()
        })
        .build()
        .run_gc_once()
        .await;

        Ok(())
    }

    /// Folds every slot past `cursor` into `db`, each passed by the
    /// catalog's check first. Returns how many.
    async fn fold_from_cursor(&self, db: &Db, cursor: &Cursor) -> Result<u64> {
        let mut count = 0;
        loop {
            let next = cursor.folded.load(Ordering::Acquire).saturating_add(1);
            let Some(envelope) = self.log.slot(next) else {
                return Ok(count);
            };
            (self.hooks.inspect)(next, &envelope)?;
            self.apply(db, &envelope).await?;
            self.folded(db, cursor, next).await?;
            count += 1;
        }
    }

    /// Waits until the reader's polls have served it through `sequence`,
    /// or the catch-up time passes.
    async fn await_served(&self, sequence: u64) {
        let mut served = self.cursor.served.subscribe();
        let waited = tokio::time::timeout(
            self.hooks.settings.catch_up,
            served.wait_for(|reached| *reached >= sequence),
        )
        .await;
        if !waited.is_ok_and(|waited| waited.is_ok()) {
            tracing::warn!(sequence, "the reader was not served the slot in time");
        }
    }

    async fn publish_through(&self, sequence: u64) -> Result<()> {
        let (db, session) = self.hooks.take.take_for_publication().await?;
        let session = own_session(session.as_any());
        let publication = if session.cursor.folded.load(Ordering::Acquire) >= sequence {
            self.hooks.publish(&db).await
        } else {
            Err(Error::CommitOutcomeUnknown(format!(
                "checkpoint publisher has not replayed committed slot {sequence}",
            )))
        };
        let closed = db
            .close_with_options(slatedb::config::CloseOptions { flush_type: None })
            .await;
        publication?;
        closed.map_err(Into::into)
    }

    /// A follower's turn: take the store through the catalog, fold what it
    /// lacks past the taken session's cursor, write out, and let go.
    async fn fold_on_cadence(&self, sequence: u64) {
        let taken = match self.hooks.take.take().await {
            Ok(Some(taken)) => taken,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(sequence, %error, "the fold could not take the store");
                return;
            }
        };
        let (db, session) = taken;
        let session = own_session(session.as_any());
        let folded = self.fold_from_cursor(&db, &session.cursor).await;
        let written = self.write_out(&db).await;
        let _ = db.close().await;
        match folded.and(written) {
            Ok(()) => tracing::debug!(sequence, "folded the log into the store"),
            Err(error) => tracing::warn!(sequence, %error, "the fold failed"),
        }
    }
}

#[async_trait]
impl CommitStore for TestStore {
    async fn adopted(&self) -> Result<bool> {
        Ok(self.log.wal_base().is_some())
    }

    async fn adopt(&self, stamp: SlotCommit) -> Result<SlotAdoption> {
        self.refuse_if_closed()?;
        let _held = self.guard.lock().await;
        self.log.reveal();
        let Side::Holder(db) = &self.side else {
            return Err(Error::Constraint(
                "a process holding no store cannot adopt the commit log".to_string(),
            ));
        };
        let adoption = {
            let mut slots = self
                .log
                .slots
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(winner) = slots.get(&1) {
                SlotAdoption::AdoptedByPeer(winner.clone())
            } else {
                let mut base = self.log.base.lock().unwrap_or_else(PoisonError::into_inner);
                if base.is_none() {
                    *base = Some(self.cursor.candidate.load(Ordering::Acquire));
                }
                drop(base);
                slots.insert(
                    1,
                    SlotEnvelope {
                        commits: vec![stamp],
                    },
                );
                SlotAdoption::Adopted
            }
        };
        self.fold_from_cursor(db, &self.cursor).await?;

        Ok(adoption)
    }

    async fn write_at(&self, sequence: u64, commit: SlotCommit) -> Result<SlotOffer> {
        self.refuse_if_closed()?;
        let _held = self.guard.lock().await;
        self.log.reveal();
        if let Side::Follower(_) = &self.side {
            self.cursor
                .await_polls(2, self.hooks.settings.catch_up)
                .await;
        }
        let next = self.cursor.known().saturating_add(1);
        if sequence > next {
            return Err(Error::Constraint(format!(
                "sequence {sequence} is past {next}, the next this store knows"
            )));
        }
        let lost = {
            let mut slots = self
                .log
                .slots
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(winner) = slots.get(&sequence) {
                Some(winner.clone())
            } else {
                slots.insert(
                    sequence,
                    SlotEnvelope {
                        commits: vec![commit],
                    },
                );
                None
            }
        };
        match (lost, &self.side) {
            (Some(winner), Side::Holder(db)) => {
                self.fold_from_cursor(db, &self.cursor).await?;
                Ok(SlotOffer::Lost {
                    sequence,
                    winner,
                    through: self.cursor.known(),
                })
            }
            (Some(winner), Side::Follower(_)) => {
                self.await_served(sequence).await;
                Ok(SlotOffer::Lost {
                    sequence,
                    winner,
                    through: self.cursor.known(),
                })
            }
            (None, Side::Holder(db)) => {
                let publication = async {
                    self.fold_from_cursor(db, &self.cursor).await?;
                    self.hooks.publish(db).await
                }
                .await;
                publication.map_err(|error| Error::CommitOutcomeUnknown(format!(
                    "slot {sequence} is durable but its checkpoint was not acknowledged: {error}",
                )))?;
                Ok(SlotOffer::Won { sequence })
            }
            (None, Side::Follower(_)) => {
                self.await_served(sequence).await;
                let every = self.hooks.settings.fold_every;
                if every != 0 && sequence.is_multiple_of(every) {
                    self.fold_on_cadence(sequence).await;
                }
                self.publish_through(sequence).await.map_err(|error| {
                    Error::CommitOutcomeUnknown(format!(
                        "slot {sequence} is durable but its checkpoint was not acknowledged: {error}",
                    ))
                })?;
                Ok(SlotOffer::Won { sequence })
            }
        }
    }

    async fn fold_tail(&self) -> Result<u64> {
        self.refuse_if_closed()?;
        let _held = self.guard.lock().await;
        match &self.side {
            Side::Holder(db) => self.fold_from_cursor(db, &self.cursor).await,
            Side::Follower(_) => {
                let before = *self.cursor.served.borrow();
                self.cursor
                    .await_polls(2, self.hooks.settings.catch_up)
                    .await;
                Ok(self.cursor.served.borrow().saturating_sub(before))
            }
        }
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        let poll = self
            .poll
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some((stop, task)) = poll {
            let _ = stop.send(true);
            let _ = task.await;
        }
        match &self.side {
            Side::Holder(db) => {
                let _held = self.guard.lock().await;
                if let Err(error) = self.write_out(db).await {
                    tracing::info!(%error, "the closing flush failed; the next open replays");
                }
                db.close().await.map_err(Error::from)
            }
            Side::Follower(reader) => reader.close().await.map_err(Error::from),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Truncation over the test log: deletes every slot below the lowest
/// referenced range, ignoring age, since the log keeps no clock.
struct TestGc {
    log: Arc<TestLog>,
}

#[async_trait]
impl WalGc for TestGc {
    async fn collect(
        &self,
        referenced_ranges: Vec<WalFileRange>,
        _min_age: Duration,
        dry_run: bool,
    ) -> std::result::Result<(), WalError> {
        let Some(horizon) = referenced_ranges
            .iter()
            .map(|range| match range.0 {
                Bound::Included(start) => start,
                Bound::Excluded(start) => start.saturating_add(1),
                Bound::Unbounded => 0,
            })
            .min()
        else {
            return Ok(());
        };
        let horizon = horizon.saturating_sub(self.log.wal_base().unwrap_or(0));
        if !dry_run {
            self.log
                .slots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .retain(|sequence, _| *sequence >= horizon);
        }

        Ok(())
    }
}

struct TestWriterInit {
    log: Arc<TestLog>,
    cursor: Arc<Cursor>,
    stride: u64,
}

#[async_trait]
impl WriterInit for TestWriterInit {
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> std::result::Result<WriterInitResult, WalError> {
        let replay_after = manifest.replay_after_wal_id();
        let next_wal = manifest.manifest().next_wal_sst_id();
        self.cursor.candidate.fetch_max(
            replay_after.max(next_wal.saturating_sub(1)),
            Ordering::AcqRel,
        );
        // Slots count from the base; an unadopted log replays nothing.
        let (first, end) = match self.log.wal_base() {
            Some(base) => {
                let folded = replay_after.saturating_sub(base);
                let end = self.log.last_sequence();
                self.cursor.folded.fetch_max(end, Ordering::AcqRel);
                (folded.saturating_add(1), end)
            }
            None => (1, 0),
        };

        let state = Arc::new(Mutex::new(WriterState::default()));
        *self
            .cursor
            .writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&state));

        Ok(WriterInitResult {
            replay_iterator: Box::new(TestIterator {
                log: Arc::clone(&self.log),
                cursor: Arc::clone(&self.cursor),
                stride: self.stride,
                next: first,
                end: Bound::Included(end),
            }),
            wal_writer: Box::new(TestWriter {
                log: Arc::clone(&self.log),
                cursor: Arc::clone(&self.cursor),
                state,
            }),
        })
    }
}

#[derive(Default)]
struct WriterState {
    acknowledged: Option<u64>,
    closed_reason: Option<WalError>,
    listeners: Vec<WalStatusListener>,
}

struct TestWriter {
    log: Arc<TestLog>,
    cursor: Arc<Cursor>,
    state: Arc<Mutex<WriterState>>,
}

fn reported(
    log: &TestLog,
    cursor: &Cursor,
    state: &Arc<Mutex<WriterState>>,
) -> std::result::Result<WalStatus, WalStatus> {
    let status = cursor.status(log, &state.lock().unwrap_or_else(PoisonError::into_inner));
    match status.closed_reason {
        Some(_) => Err(status),
        None => Ok(status),
    }
}

#[async_trait]
impl WalWriter for TestWriter {
    async fn append(&mut self, write_batch: &[RowEntry]) -> std::result::Result<(), WalError> {
        let Some(last) = write_batch.iter().map(|row| row.seq).max() else {
            return Ok(());
        };
        let (status, listeners) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.acknowledged = Some(state.acknowledged.map_or(last, |seen| seen.max(last)));
            (
                self.cursor.status(&self.log, &state),
                state.listeners.clone(),
            )
        };
        for listener in &listeners {
            listener(WalEvent::WalFlushed(status.clone()));
        }

        Ok(())
    }

    async fn flush(&mut self) -> std::result::Result<WalFlushResult, WalError> {
        // Nothing is buffered: a slot is durable when its put returns, so the
        // flush covers the fold cursor and completes at once.
        let wal_id = {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            self.cursor.status(&self.log, &state).last_flushed_wal_id
        };
        Ok(WalFlushResult {
            wal_id,
            completion: Box::pin(std::future::ready(Ok(()))),
        })
    }

    fn observer(&self) -> Box<dyn WalObserver> {
        Box::new(TestObserver {
            log: Arc::clone(&self.log),
            cursor: Arc::clone(&self.cursor),
            state: Arc::clone(&self.state),
        })
    }

    fn status(&self) -> std::result::Result<WalStatus, WalStatus> {
        reported(&self.log, &self.cursor, &self.state)
    }

    async fn close(&mut self) -> std::result::Result<(), WalError> {
        let (closed, listeners) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.closed_reason = Some(WalError::Closed);
            (
                self.cursor.status(&self.log, &state),
                std::mem::take(&mut state.listeners),
            )
        };
        for listener in &listeners {
            listener(WalEvent::WalClosed(closed.clone()));
        }
        let mut registered = self
            .cursor
            .writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if registered
            .as_ref()
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            *registered = None;
        }

        Ok(())
    }
}

struct TestObserver {
    log: Arc<TestLog>,
    cursor: Arc<Cursor>,
    state: Arc<Mutex<WriterState>>,
}

#[async_trait]
impl WalObserver for TestObserver {
    fn status(&self) -> std::result::Result<WalStatus, WalStatus> {
        reported(&self.log, &self.cursor, &self.state)
    }

    fn subscribe(&self, listener: WalStatusListener) -> std::result::Result<(), WalError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match state.closed_reason {
            Some(_) => {
                let closed = self.cursor.status(&self.log, &state);
                drop(state);
                listener(WalEvent::WalClosed(closed));
            }
            None => state.listeners.push(listener),
        }

        Ok(())
    }
}

struct TestReader {
    log: Arc<TestLog>,
    cursor: Arc<Cursor>,
    stride: u64,
    tail: bool,
}

#[async_trait]
impl WalReader for TestReader {
    async fn iterator(
        &self,
        range: WalFileRange,
    ) -> std::result::Result<Box<dyn WalIterator>, WalError> {
        // The range is in WAL ids; slots count from the base.
        let base = self.log.wal_base().unwrap_or(0);
        let next = match range.0 {
            Bound::Included(start) => start,
            Bound::Excluded(start) => start.saturating_add(1),
            Bound::Unbounded => 1,
        };
        let end = match range.1 {
            Bound::Included(end) => Bound::Included(end.saturating_sub(base)),
            Bound::Excluded(end) => Bound::Excluded(end.saturating_sub(base)),
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(Box::new(TestIterator {
            log: Arc::clone(&self.log),
            cursor: Arc::clone(&self.cursor),
            stride: self.stride,
            next: next.saturating_sub(base).max(1),
            end,
        }))
    }

    async fn last_wal_file_id(
        &self,
        replay_after_wal_id: u64,
    ) -> std::result::Result<u64, WalError> {
        self.cursor.polls.fetch_add(1, Ordering::AcqRel);
        let base = self.log.wal_base();
        // Everything below the replay point is in the store this reader
        // follows: served, as far as this process knows.
        if let Some(base) = base {
            self.cursor.serve(replay_after_wal_id.saturating_sub(base));
        }
        Ok(match (self.tail, base) {
            (true, Some(base)) => base
                .saturating_add(self.log.last_sequence())
                .max(replay_after_wal_id),
            _ => replay_after_wal_id,
        })
    }
}

struct TestIterator {
    log: Arc<TestLog>,
    cursor: Arc<Cursor>,
    stride: u64,
    next: u64,
    end: Bound<u64>,
}

impl TestIterator {
    fn exhausted(&self) -> bool {
        match self.end {
            Bound::Included(end) => self.next > end,
            Bound::Excluded(end) => self.next >= end,
            Bound::Unbounded => false,
        }
    }
}

#[async_trait]
impl WalIterator for TestIterator {
    async fn next(&mut self) -> std::result::Result<Option<WalRows>, WalError> {
        if self.exhausted() {
            return Ok(None);
        }
        let Some(envelope) = self.log.slot(self.next) else {
            return Ok(None);
        };

        let sequence = self.next;
        self.next = self.next.saturating_add(1);
        self.cursor.serve(sequence);

        // Rows are handed back in WAL ids: the slot counted from the base.
        let base = self.log.wal_base().unwrap_or(0);
        Ok(Some(WalRows {
            rows: TestLog::rows(self.stride, sequence, &envelope),
            last_consumed_wal_file_id: base.saturating_add(sequence),
        }))
    }
}
