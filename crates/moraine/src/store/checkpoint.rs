//! Readers over the checkpoints they hold open.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use slatedb::DbReader;
use tokio::time::Instant;

use crate::store::open::READ_WINDOW;

/// Allowance for a reader's clock running behind the collector's.
const SKEW_MARGIN: Duration = Duration::from_mins(1);

/// A reader over one store state, held open for as long as a scope reads it.
pub(crate) struct CheckpointReader {
    reader: Arc<DbReader>,
    /// The durable sequence this reader resolved when it opened. It names
    /// the state read through it, which the manifest id does not: a commit
    /// writes no manifest.
    pub(crate) revision: u64,
    /// When retention stops guaranteeing the objects this reader reads.
    /// `None` for a checkpoint whose lifetime someone else owns.
    deadline: Option<Instant>,
    owned: bool,
    closed: AtomicBool,
}

impl CheckpointReader {
    /// Borrows the reader only while retention still covers what it reads.
    pub(crate) fn reader(&self) -> Result<&DbReader, slatedb::Error> {
        if self.remaining().is_some_and(|left| left.is_zero()) {
            return Err(slatedb::Error::unavailable(
                "read window elapsed; restart the transaction to read the current state".to_owned(),
            ));
        }
        Ok(&self.reader)
    }

    /// Time left before the deadline; `None` when no deadline governs.
    pub(crate) fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    /// The store reader itself, for status and shutdown. Reads go through
    /// [`reader`](Self::reader), which honours the deadline.
    pub(crate) fn store_reader(&self) -> &Arc<DbReader> {
        &self.reader
    }

    /// Uses a checkpoint whose lifecycle the caller already owns.
    pub(crate) fn external(reader: Arc<DbReader>) -> Arc<Self> {
        Arc::new(Self {
            revision: reader.status().durable_seq,
            reader,
            deadline: None,
            owned: false,
            closed: AtomicBool::new(false),
        })
    }

    /// Owns a reader that keeps a checkpoint of its own and refreshes it,
    /// so collection cannot take what it reads however long it reads for.
    pub(crate) fn fixed(reader: DbReader) -> Arc<Self> {
        Arc::new(Self {
            revision: reader.status().durable_seq,
            reader: Arc::new(reader),
            deadline: None,
            owned: true,
            closed: AtomicBool::new(false),
        })
    }

    /// Owns a reader that could not write a checkpoint and follows the
    /// latest manifest instead. Nothing but retention keeps what it reads,
    /// so it refuses reads once the read window has elapsed.
    pub(crate) fn following(reader: DbReader) -> Arc<Self> {
        Arc::new(Self {
            revision: reader.status().durable_seq,
            reader: Arc::new(reader),
            deadline: Some(Instant::now() + READ_WINDOW - SKEW_MARGIN),
            owned: true,
            closed: AtomicBool::new(false),
        })
    }

    /// Closes the reader now, releasing its checkpoint before the caller
    /// moves on. Once, however many times it is called; the drop then
    /// closes nothing.
    pub(crate) async fn close(&self) -> Result<(), slatedb::Error> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.reader.close().await
    }

    /// Releases an unshared reader before its replacement reserves replay
    /// memory.
    pub(crate) async fn retire(self) {
        let _ = self.close().await;
    }
}

impl Drop for CheckpointReader {
    fn drop(&mut self) {
        if self.owned
            && !self.closed.load(Ordering::Acquire)
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let reader = self.reader.clone();
            runtime.spawn(async move {
                let _ = reader.close().await;
            });
        }
    }
}
