//! Readers over the checkpoints they hold open.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use slatedb::DbReader;

/// A reader over one store state, held open for as long as a scope reads it.
/// Every reader holds a checkpoint, its own or one someone else owns, so
/// collection cannot take what it reads however long it reads for.
pub(crate) struct CheckpointReader {
    reader: Arc<DbReader>,
    /// The durable sequence this reader resolved when it opened. It names
    /// the state read through it, which the manifest id does not: a commit
    /// writes no manifest.
    pub(crate) revision: u64,
    owned: bool,
    closed: AtomicBool,
}

impl CheckpointReader {
    /// The reader to read through.
    pub(crate) fn reader(&self) -> &DbReader {
        &self.reader
    }

    /// The store reader itself, shared, for status and shutdown.
    pub(crate) fn store_reader(&self) -> &Arc<DbReader> {
        &self.reader
    }

    /// Uses a checkpoint whose lifecycle the caller already owns.
    pub(crate) fn external(reader: Arc<DbReader>) -> Arc<Self> {
        Arc::new(Self {
            revision: reader.status().durable_seq,
            reader,
            owned: false,
            closed: AtomicBool::new(false),
        })
    }

    /// Owns a reader that keeps a checkpoint of its own and refreshes it.
    pub(crate) fn fixed(reader: DbReader) -> Arc<Self> {
        Arc::new(Self {
            revision: reader.status().durable_seq,
            reader: Arc::new(reader),
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
