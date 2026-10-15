//! What a committer's plug consults before it lists the log. A committer's
//! reader ticks at the commit poll so a commit sees its own slot quickly,
//! which is far faster than any read needs peers' slots; the log is listed
//! only when a read has asked for state fresher than the last listing
//! left, or a commit is waiting to see its slot. An idle committer lists
//! nothing.

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
    listed_at: Mutex<Option<Instant>>,
    /// The least time between listings reads alone may cause, and the
    /// staleness a read tolerates before it waits for one.
    floor: Duration,
}

impl ReadDemand {
    /// A demand owing its first listing to the open's own replay; reads
    /// alone cause one at most every `floor`.
    pub(crate) fn new(floor: Duration) -> Arc<Self> {
        Arc::new(Self {
            asked: AtomicU64::new(1),
            answered: AtomicU64::new(0),
            replayed: watch::Sender::new(0),
            urgent: AtomicU64::new(0),
            listed_at: Mutex::new(None),
            floor,
        })
    }

    /// Lists before the caller reads when the last listing is older than
    /// the floor, returning once the reader has replayed what it found,
    /// so no read is served state staler than the floor however long the
    /// catalog sat idle. Within the floor it returns at once. A listing
    /// that does not arrive within `timeout` is given up on and the read
    /// goes ahead stale.
    pub(crate) async fn catch_up(self: &Arc<Self>, timeout: Duration) {
        let asked = self.asked.fetch_add(1, Ordering::AcqRel) + 1;
        if self.wanted().is_none() {
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
        if asked <= self.answered.load(Ordering::Acquire) {
            return None;
        }
        let listed_at = self
            .listed_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match *listed_at {
            Some(at) if at.elapsed() < self.floor => None,
            _ => Some(asked),
        }
    }

    fn answered(&self, asked: u64) {
        self.answered.fetch_max(asked, Ordering::AcqRel);
        *self
            .listed_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
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

    /// Reads alone cause one listing per ask, spaced by the floor; a wait
    /// lists every time until it ends.
    #[test]
    fn a_demand_lists_for_asks_within_the_floor_and_always_while_urgent() {
        let demand = ReadDemand::new(Duration::from_mins(1));
        assert_eq!(demand.wanted(), Some(1), "the open's replay is owed");
        demand.answered(1);
        assert_eq!(demand.wanted(), None, "nothing asked since");

        demand.asked.fetch_add(1, Ordering::AcqRel);
        assert_eq!(demand.wanted(), None, "a read inside the floor waits");

        let urgent = demand.urgent();
        assert_eq!(demand.wanted(), Some(2), "a waiting commit lists at once");
        demand.answered(2);
        assert_eq!(demand.wanted(), Some(2), "and again, until served");
        drop(urgent);
        assert_eq!(demand.wanted(), None, "the ask was answered on the way");
    }

    /// A read inside the floor is served the listing a previous read
    /// caused, without waiting for one of its own.
    #[tokio::test]
    async fn a_read_inside_the_floor_waits_for_nothing() {
        let demand = ReadDemand::new(Duration::from_mins(1));
        demand.answered(1);

        demand.catch_up(Duration::from_mins(1)).await;
    }

    /// A read past the floor waits for a listing and for the reader to
    /// replay it, which the poll after the listing reports.
    #[tokio::test]
    async fn a_read_past_the_floor_waits_for_the_listing_it_causes() {
        let demand = ReadDemand::new(Duration::ZERO);
        demand.answered(1);

        let reading = tokio::spawn({
            let demand = Arc::clone(&demand);
            async move { demand.catch_up(Duration::from_mins(1)).await }
        });
        // The poll that lists, then the poll that reports it replayed.
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

    /// A read gives up on a listing that does not arrive and goes ahead
    /// stale rather than failing.
    #[tokio::test]
    async fn a_read_gives_up_on_a_listing_that_never_lands() {
        let demand = ReadDemand::new(Duration::ZERO);
        demand.answered(1);

        demand.catch_up(Duration::from_millis(10)).await;
    }
}
