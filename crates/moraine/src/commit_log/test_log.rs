//! An in-memory commit log implementing the whole boundary: the plugs a
//! store opens over, and the committer. The reference the open repository
//! tests the boundary against; it stands in for the library, not for the
//! store, which stays real SlateDB.

use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::{
    RowEntry, ValueDeletable,
    wal::{
        FlushResultFuture, WalError, WalEvent, WalFileRange, WalGc, WalIterator, WalObserver,
        WalReader, WalRows, WalStatus, WalStatusListener, WalWriter, WriterInit, WriterInitResult,
        WriterManifest,
    },
};

use super::{CommitLogProvider, SlotAdoption, SlotCommit, SlotCommitter, SlotEnvelope, SlotOffer};

/// The row sequence the first slot takes, above anything a store wrote
/// before adopting the log.
const SEQUENCE_BASE: u64 = 1 << 32;
/// The row sequences one slot reserves.
const SEQUENCE_STRIDE: u64 = 1 << 20;

/// The row sequence slot `ordinal` writes at.
fn slot_sequence(ordinal: u64) -> u64 {
    SEQUENCE_BASE.saturating_add(ordinal.saturating_mul(SEQUENCE_STRIDE))
}

/// One log: what the bucket would hold, shared by every process over it.
#[derive(Default)]
pub(crate) struct TestLog {
    slots: Mutex<BTreeMap<u64, SlotEnvelope>>,
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

    fn rows(sequence: u64, envelope: &SlotEnvelope) -> Vec<RowEntry> {
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
                seq: slot_sequence(sequence),
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
}

/// One process over a [`TestLog`], whatever store or path it is asked for.
#[derive(Default)]
pub(crate) struct TestProvider {
    pub(crate) log: Arc<TestLog>,
    cursor: Arc<Cursor>,
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
        }
    }

    /// The last sequence this process reported applied.
    pub(crate) fn folded_sequence(&self) -> u64 {
        self.cursor.folded.load(Ordering::Acquire)
    }

    /// Reports `sequence` served before this process's reader has applied
    /// it: the window a real plug opens for a moment, held open.
    pub(crate) fn report_served(&self, sequence: u64) {
        self.cursor.served.send_if_modified(|served| {
            let advanced = sequence > *served;
            if advanced {
                *served = sequence;
            }
            advanced
        });
    }
}

impl CommitLogProvider for TestProvider {
    fn writer_init(&self, _log_store: Arc<dyn ObjectStore>, _path: &str) -> Box<dyn WriterInit> {
        Box::new(TestWriterInit {
            log: Arc::clone(&self.log),
            cursor: Arc::clone(&self.cursor),
        })
    }

    fn reader(
        &self,
        _log_store: Arc<dyn ObjectStore>,
        _path: &str,
        tail: bool,
    ) -> Arc<dyn WalReader> {
        Arc::new(TestReader {
            log: Arc::clone(&self.log),
            cursor: Arc::clone(&self.cursor),
            tail,
        })
    }

    fn committer(&self, _log_store: Arc<dyn ObjectStore>, _path: &str) -> Arc<dyn SlotCommitter> {
        Arc::new(TestCommitter {
            log: Arc::clone(&self.log),
            cursor: Arc::clone(&self.cursor),
        })
    }

    fn garbage_collector(&self, _log_store: Arc<dyn ObjectStore>, _path: &str) -> Arc<dyn WalGc> {
        Arc::new(TestGc {
            log: Arc::clone(&self.log),
        })
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
    ) -> Result<(), WalError> {
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

struct TestCommitter {
    log: Arc<TestLog>,
    cursor: Arc<Cursor>,
}

impl std::fmt::Debug for TestCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestCommitter").finish_non_exhaustive()
    }
}

#[async_trait]
impl SlotCommitter for TestCommitter {
    async fn offer(&self, sequence: u64, commits: Vec<SlotCommit>) -> crate::Result<SlotOffer> {
        let mut slots = self
            .log
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if slots.contains_key(&sequence) {
            return Ok(SlotOffer::Lost {
                slots: slots
                    .range(sequence..)
                    .map(|(sequence, envelope)| (*sequence, envelope.clone()))
                    .collect(),
            });
        }
        slots.insert(sequence, SlotEnvelope { commits });

        Ok(SlotOffer::Won { sequence })
    }

    async fn adopted(&self) -> crate::Result<bool> {
        Ok(self.log.wal_base().is_some())
    }

    async fn unfolded(&self, replay_point: u64) -> crate::Result<u64> {
        let folded = self
            .log
            .wal_base()
            .map_or(0, |base| replay_point.saturating_sub(base));
        Ok(self.log.last_sequence().saturating_sub(folded))
    }

    async fn tail(&self) -> crate::Result<Vec<(u64, SlotEnvelope)>> {
        let known = self
            .cursor
            .folded
            .load(Ordering::Acquire)
            .max(*self.cursor.served.borrow());
        Ok(self
            .log
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .range(known.saturating_add(1)..)
            .map(|(sequence, envelope)| (*sequence, envelope.clone()))
            .collect())
    }

    async fn adopt(&self, stamp: SlotCommit) -> crate::Result<SlotAdoption> {
        let mut slots = self
            .log
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(winner) = slots.get(&1) {
            return Ok(SlotAdoption::AdoptedByPeer(winner.clone()));
        }
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

        Ok(SlotAdoption::Adopted)
    }

    fn mark_folded(&self, sequence: u64) {
        let before = self.cursor.folded.fetch_max(sequence, Ordering::AcqRel);
        if sequence <= before {
            return;
        }
        let writer = self
            .cursor
            .writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(state) = writer {
            let (status, listeners) = {
                let state = state.lock().unwrap_or_else(PoisonError::into_inner);
                (
                    self.cursor.status(&self.log, &state),
                    state.listeners.clone(),
                )
            };
            for listener in &listeners {
                listener(WalEvent::WalFlushed(status.clone()));
            }
        }
    }

    async fn await_served(&self, sequence: u64, timeout: Duration) -> bool {
        let mut served = self.cursor.served.subscribe();
        tokio::time::timeout(timeout, served.wait_for(|reached| *reached >= sequence))
            .await
            .is_ok_and(|waited| waited.is_ok())
    }
}

struct TestWriterInit {
    log: Arc<TestLog>,
    cursor: Arc<Cursor>,
}

#[async_trait]
impl WriterInit for TestWriterInit {
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError> {
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
) -> Result<WalStatus, WalStatus> {
    let status = cursor.status(log, &state.lock().unwrap_or_else(PoisonError::into_inner));
    match status.closed_reason {
        Some(_) => Err(status),
        None => Ok(status),
    }
}

#[async_trait]
impl WalWriter for TestWriter {
    async fn append(&mut self, write_batch: &[RowEntry]) -> Result<(), WalError> {
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

    async fn flush(&mut self) -> Result<FlushResultFuture, WalError> {
        Ok(Box::pin(std::future::ready(Ok(()))))
    }

    fn observer(&self) -> Box<dyn WalObserver> {
        Box::new(TestObserver {
            log: Arc::clone(&self.log),
            cursor: Arc::clone(&self.cursor),
            state: Arc::clone(&self.state),
        })
    }

    fn status(&self) -> Result<WalStatus, WalStatus> {
        reported(&self.log, &self.cursor, &self.state)
    }

    async fn close(&mut self) -> Result<(), WalError> {
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
    fn status(&self) -> Result<WalStatus, WalStatus> {
        reported(&self.log, &self.cursor, &self.state)
    }

    fn subscribe(&self, listener: WalStatusListener) -> Result<(), WalError> {
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
    tail: bool,
}

#[async_trait]
impl WalReader for TestReader {
    async fn iterator(&self, range: WalFileRange) -> Result<Box<dyn WalIterator>, WalError> {
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
            next: next.saturating_sub(base).max(1),
            end,
        }))
    }

    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        Ok(match (self.tail, self.log.wal_base()) {
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
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        if self.exhausted() {
            return Ok(None);
        }
        let envelope = self
            .log
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&self.next)
            .cloned();
        let Some(envelope) = envelope else {
            return Ok(None);
        };

        let sequence = self.next;
        self.next = self.next.saturating_add(1);
        self.cursor.served.send_if_modified(|served| {
            let advanced = sequence > *served;
            if advanced {
                *served = sequence;
            }
            advanced
        });

        // Rows are handed back in WAL ids: the slot counted from the base.
        let base = self.log.wal_base().unwrap_or(0);
        Ok(Some(WalRows {
            rows: TestLog::rows(sequence, &envelope),
            last_consumed_wal_file_id: base.saturating_add(sequence),
        }))
    }
}
