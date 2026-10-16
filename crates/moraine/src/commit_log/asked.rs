//! What a read-only reader's plug consults before it lists the log. The
//! reader's clock ticks on; the log is listed only on a tick a read has
//! asked for, so a catalog nothing reads lists nothing and one being
//! read is a tick behind at worst — the staleness its poll interval
//! promised anyway.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use async_trait::async_trait;
use slatedb::wal::{WalError, WalFileRange, WalIterator, WalReader};

/// Whether a read has asked for the log to be listed since it last was.
#[derive(Debug, Default)]
pub(crate) struct Asked(AtomicBool);

impl Asked {
    /// A read begins: the next tick lists.
    pub(crate) fn ask(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether a listing is wanted, taking the ask if so.
    fn take(&self) -> bool {
        self.0.swap(false, Ordering::AcqRel)
    }
}

/// The provider's reader behind an ask: lists the log on a tick a read
/// wanted, and otherwise reports the end it last saw, which the reader
/// has already replayed.
pub(crate) struct AskedReader {
    inner: Arc<dyn WalReader>,
    asked: Arc<Asked>,
    known_end: AtomicU64,
}

impl AskedReader {
    pub(crate) fn new(inner: Arc<dyn WalReader>, asked: Arc<Asked>) -> Self {
        Self {
            inner,
            asked,
            known_end: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl WalReader for AskedReader {
    async fn iterator(&self, range: WalFileRange) -> Result<Box<dyn WalIterator>, WalError> {
        self.inner.iterator(range).await
    }

    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        // The open's own replay is owed one, whatever has read yet.
        let first = self.known_end.load(Ordering::Acquire) == 0;
        if !first && !self.asked.take() {
            return Ok(self
                .known_end
                .load(Ordering::Acquire)
                .max(replay_after_wal_id));
        }
        let end = self.inner.last_wal_file_id(replay_after_wal_id).await?;
        self.known_end.fetch_max(end.max(1), Ordering::AcqRel);

        Ok(end.max(replay_after_wal_id))
    }
}
