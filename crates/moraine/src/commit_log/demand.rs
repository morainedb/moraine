//! What a committer's plug consults before it lists the log. A committer's
//! reader ticks at the commit poll so a commit sees its own slot quickly,
//! and a peer's slot is what a read needs soonest, so a session that is
//! reading keeps every tick listing. One that stops reading stops
//! listing, and an idle committer lists nothing at all.

use std::{
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use slatedb::wal::{WalError, WalFileRange, WalIterator, WalReader};
use tokio::sync::watch;

/// When reads last wanted the log watched, and when a listing last
/// answered.
#[derive(Debug, Default)]
struct Clock {
    /// Every tick lists until this passes.
    active_until: Option<Instant>,
    /// When the last listing ran.
    listed_at: Option<Instant>,
}

/// Who wants the log listed, how recently it was, and how far the reader
/// has replayed what a listing found.
pub(crate) struct ReadDemand {
    /// Reads begun, counting the open's own replay as the first.
    asked: AtomicU64,
    /// The last ask a listing answered.
    answered: AtomicU64,
    /// The last ask whose listing the reader has finished replaying.
    replayed: watch::Sender<u64>,
    /// Commits waiting to see their slot; while any, every tick lists.
    urgent: AtomicU64,
    clock: Mutex<Clock>,
    /// How long a read keeps the log watched after it.
    window: Duration,
    /// How old the last listing may be before a read waits for a fresh
    /// one. Longer than a tick, so a watched log is never waited on.
    stale_after: Duration,
}

impl ReadDemand {
    /// A demand owing its first listing to the open's own replay, which
    /// watches the log for `window` after every read.
    pub(crate) fn new(window: Duration, stale_after: Duration) -> Arc<Self> {
        Arc::new(Self {
            asked: AtomicU64::new(1),
            answered: AtomicU64::new(0),
            replayed: watch::Sender::new(0),
            urgent: AtomicU64::new(0),
            clock: Mutex::new(Clock::default()),
            window,
            stale_after,
        })
    }

    /// Watches the log for the window, and waits for a listing first
    /// unless it was being watched already, where every tick has been
    /// listing and the reader is a tick behind at worst. Only a session
    /// that stopped reading waits. A listing that does not arrive within
    /// `timeout` is given up on and the read goes ahead stale.
    pub(crate) async fn catch_up(self: &Arc<Self>, timeout: Duration) {
        let asked = self.asked.fetch_add(1, Ordering::AcqRel) + 1;
        let now = Instant::now();
        let watched = {
            let mut clock = self.clock.lock().unwrap_or_else(PoisonError::into_inner);
            // Already inside the window, so every tick has been listing —
            // unless the poll is wedged, which the staleness bound catches.
            let watched = clock.active_until.is_some_and(|until| now < until)
                && clock
                    .listed_at
                    .is_some_and(|at| now.saturating_duration_since(at) < self.stale_after);
            clock.active_until = Some(now + self.window);
            watched
        };
        if watched {
            return;
        }

        let _urgent = self.urgent();
        let mut replayed = self.replayed.subscribe();
        let _ = tokio::time::timeout(timeout, replayed.wait_for(|done| *done >= asked)).await;
    }

    /// A commit waits to see its slot: every tick lists until the guard
    /// drops.
    pub(crate) fn urgent(self: &Arc<Self>) -> Urgent {
        self.urgent.fetch_add(1, Ordering::AcqRel);
        Urgent(Arc::clone(self))
    }

    /// The ask a listing now would answer, if one is wanted.
    fn wanted(&self) -> Option<u64> {
        let asked = self.asked.load(Ordering::Acquire);
        if self.urgent.load(Ordering::Acquire) > 0 {
            return Some(asked);
        }
        let clock = self.clock.lock().unwrap_or_else(PoisonError::into_inner);
        if clock.listed_at.is_none() {
            // The open's own replay, which nothing has read for yet.
            return Some(asked);
        }
        match clock.active_until {
            Some(until) if Instant::now() < until => Some(asked),
            _ => None,
        }
    }

    fn answered(&self, asked: u64) {
        self.answered.fetch_max(asked, Ordering::AcqRel);
        self.clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .listed_at = Some(Instant::now());
    }

    /// Publishes the last answered ask as replayed. Called as a poll asks
    /// for the end of the log: the poll before it has replayed everything
    /// its own listing found and installed the state, since the reader
    /// runs its polls one after another.
    fn settle(&self) {
        let answered = self.answered.load(Ordering::Acquire);
        self.replayed.send_if_modified(|done| {
            let moved = answered > *done;
            if moved {
                *done = answered;
            }
            moved
        });
    }
}

/// Holds a demand urgent until dropped.
pub(crate) struct Urgent(Arc<ReadDemand>);

impl Drop for Urgent {
    fn drop(&mut self) {
        self.0.urgent.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The provider's reader behind a demand: lists the log when the demand
/// wants it, and otherwise reports the end it last saw, which the reader
/// has already replayed.
pub(crate) struct DemandedReader {
    inner: Arc<dyn WalReader>,
    demand: Arc<ReadDemand>,
    known_end: AtomicU64,
}

impl DemandedReader {
    pub(crate) fn new(inner: Arc<dyn WalReader>, demand: Arc<ReadDemand>) -> Self {
        Self {
            inner,
            demand,
            known_end: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl WalReader for DemandedReader {
    async fn iterator(&self, range: WalFileRange) -> Result<Box<dyn WalIterator>, WalError> {
        self.inner.iterator(range).await
    }

    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        self.demand.settle();
        let Some(asked) = self.demand.wanted() else {
            return Ok(self
                .known_end
                .load(Ordering::Acquire)
                .max(replay_after_wal_id));
        };
        let end = self.inner.last_wal_file_id(replay_after_wal_id).await?;
        self.known_end.fetch_max(end, Ordering::AcqRel);
        self.demand.answered(asked);

        Ok(end.max(replay_after_wal_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_millis(100);
    const STALE: Duration = Duration::from_millis(20);

    /// A read watches the log for the window; past it, nothing lists.
    #[tokio::test]
    async fn a_read_watches_the_log_for_the_window_and_no_longer() {
        let demand = ReadDemand::new(WINDOW, STALE);
        assert_eq!(demand.wanted(), Some(1), "the open's replay is owed");
        demand.answered(1);
        assert_eq!(demand.wanted(), None, "and nothing has read since");

        // No plug answers here, so the read gives up rather than wait out
        // the window it is about to be measured against.
        demand.catch_up(Duration::from_millis(5)).await;
        assert_eq!(demand.wanted(), Some(2), "a read watches the log");
        demand.answered(2);
        assert_eq!(
            demand.wanted(),
            Some(2),
            "and keeps watching it, listing every tick"
        );

        tokio::time::sleep(WINDOW * 2).await;
        assert_eq!(demand.wanted(), None, "the window passed");
    }

    /// A read inside the window waits for nothing: the log is being
    /// listed every tick already.
    #[tokio::test]
    async fn a_read_on_a_watched_log_waits_for_nothing() {
        let demand = ReadDemand::new(WINDOW, STALE);
        demand.answered(1);

        demand.catch_up(Duration::from_secs(5)).await;
    }

    /// A read whose last listing is older than the staleness bound — a
    /// session that stopped reading — waits for a listing and for the
    /// reader to replay it.
    #[tokio::test]
    async fn a_read_after_the_window_waits_for_a_fresh_listing() {
        let demand = ReadDemand::new(WINDOW, STALE);
        demand.answered(1);
        tokio::time::sleep(STALE * 2).await;

        let reading = tokio::spawn({
            let demand = Arc::clone(&demand);
            async move { demand.catch_up(Duration::from_secs(5)).await }
        });
        let asked = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(asked) = demand.wanted()
                    && asked > 1
                {
                    return asked;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the read asks for a listing");
        assert!(!reading.is_finished(), "and waits for it");

        demand.answered(asked);
        demand.settle();
        tokio::time::timeout(Duration::from_secs(5), reading)
            .await
            .expect("the read is released by the replay")
            .expect("the reading task did not panic");
    }

    /// A commit waiting for its slot lists every tick whatever the reads
    /// have done, and rests once the guard drops.
    #[tokio::test]
    async fn a_waiting_commit_lists_whatever_the_reads_have_done() {
        let demand = ReadDemand::new(WINDOW, STALE);
        demand.answered(1);
        assert_eq!(demand.wanted(), None, "nothing has read");

        let urgent = demand.urgent();
        assert_eq!(demand.wanted(), Some(1));
        demand.answered(1);
        assert_eq!(demand.wanted(), Some(1), "and again, until served");
        drop(urgent);
        assert_eq!(demand.wanted(), None);
    }

    /// A read gives up on a listing that does not arrive and goes ahead
    /// stale rather than failing.
    #[tokio::test]
    async fn a_read_gives_up_on_a_listing_that_never_lands() {
        let demand = ReadDemand::new(WINDOW, STALE);

        demand.catch_up(Duration::from_millis(10)).await;
    }
}
