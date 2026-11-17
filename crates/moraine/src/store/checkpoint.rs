//! Published read boundaries and the pinned readers that follow them.

use std::{
    sync::{Arc, Mutex as SyncMutex, PoisonError},
    time::Duration,
};

use slatedb::{
    Db, DbReader,
    admin::Admin,
    config::{CheckpointOptions, CheckpointScope},
};
use tokio::{sync::Mutex, task::JoinHandle, time::Instant};
use uuid::Uuid;

/// The writer's discoverable checkpoint; readers pin it directly.
pub(crate) const PUBLISHED_NAME: &str = "moraine-read-head-v1";
/// How long a superseded publication stays readable. Must exceed the longest
/// explicit transaction, as the retention window must.
pub(crate) const READ_GRACE: Duration = Duration::from_mins(15);
/// Allowance for a writer clock running ahead of a reader's.
const SKEW_MARGIN: Duration = Duration::from_mins(1);

#[cfg(test)]
mod tests;

/// A fixed reader over one publication, served until its deadline.
pub(crate) struct CheckpointReader {
    reader: Arc<DbReader>,
    pub(crate) manifest: u64,
    deadline: Option<SyncMutex<Instant>>,
    owned: bool,
}

impl CheckpointReader {
    /// Borrows the reader only while its publication is known to be protected.
    pub(crate) fn reader(&self) -> Result<&DbReader, slatedb::Error> {
        if self
            .remaining()
            .is_some_and(|remaining| remaining.is_zero())
        {
            return Err(slatedb::Error::unavailable(
                "read checkpoint expired; restart the transaction to acquire a new read scope"
                    .to_owned(),
            ));
        }
        Ok(&self.reader)
    }

    /// Time left before the deadline; `None` when no deadline governs.
    pub(crate) fn remaining(&self) -> Option<Duration> {
        self.deadline.as_ref().map(|deadline| {
            deadline
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .saturating_duration_since(Instant::now())
        })
    }

    /// Uses a checkpoint whose lifecycle the caller already owns.
    pub(crate) fn external(reader: Arc<DbReader>) -> Arc<Self> {
        Arc::new(Self {
            manifest: reader.manifest().id(),
            reader,
            deadline: None,
            owned: false,
        })
    }

    /// Owns a reader whose checkpoint lifetime is managed by its provider.
    pub(crate) fn fixed(reader: DbReader) -> Arc<Self> {
        Arc::new(Self {
            manifest: reader.manifest().id(),
            reader: Arc::new(reader),
            deadline: None,
            owned: true,
        })
    }

    /// Pins a publication that a listing at `listed` showed with `expires`
    /// left on its own expiry, or none.
    pub(crate) fn pinned(
        reader: DbReader,
        manifest: u64,
        listed: Instant,
        expires: Option<Duration>,
    ) -> Arc<Self> {
        let this = Self {
            reader: Arc::new(reader),
            manifest,
            deadline: Some(SyncMutex::new(listed)),
            owned: true,
        };
        this.confirm(listed, expires);
        Arc::new(this)
    }

    /// Moves the deadline for a listing at `listed` that still showed the
    /// publication. A publication current when listed cannot expire before
    /// `listed` plus the grace; one already expiring ends when it says.
    pub(crate) fn confirm(&self, listed: Instant, expires: Option<Duration>) {
        let Some(deadline) = &self.deadline else {
            return;
        };
        let mut until = listed + READ_GRACE;
        if let Some(expires) = expires {
            until = until.min(listed + expires);
        }
        *deadline.lock().unwrap_or_else(PoisonError::into_inner) =
            until.checked_sub(SKEW_MARGIN).unwrap_or(listed);
    }

    #[cfg(test)]
    pub(crate) fn set_deadline(&self, at: Instant) {
        if let Some(deadline) = &self.deadline {
            *deadline.lock().unwrap_or_else(PoisonError::into_inner) = at;
        }
    }

    /// Releases an unshared reader before its replacement reserves replay
    /// memory.
    pub(crate) async fn retire(self) {
        let _ = self.reader.close().await;
    }
}

impl Drop for CheckpointReader {
    fn drop(&mut self) {
        if self.owned
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let reader = self.reader.clone();
            runtime.spawn(async move {
                let _ = reader.close().await;
            });
        }
    }
}

/// Wall-clock time left until `expire_millis`, a Unix timestamp.
#[cfg(test)]
pub(crate) fn remaining_until(expire_millis: i64) -> Duration {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_millis()).ok())
        .unwrap_or(i64::MAX);
    Duration::from_millis(u64::try_from(expire_millis.saturating_sub(now)).unwrap_or(0))
}

/// How long a publication lives unless refreshed: twice the grace, so one
/// superseded while the writer keeps refreshing has at least the grace left.
pub(crate) const PUBLICATION_LIFETIME: Duration = Duration::from_mins(30);
/// How often an idle writer checks whether its publication needs refreshing.
const REFRESH_TICK: Duration = Duration::from_mins(1);
/// How long a commit's visibility to other processes may lag its
/// acknowledgement. A publication is a manifest write, and awaiting one per
/// commit is four of the five object-store requests a commit otherwise makes;
/// pacing them puts that cost behind the acknowledgement instead.
pub(crate) const PUBLICATION_INTERVAL: Duration = Duration::from_millis(100);

/// Serializes publication and coalesces writes covered by the same checkpoint.
///
/// Every publication is created with [`PUBLICATION_LIFETIME`]; a close
/// publishes without one. The current publication is refreshed in the
/// background once it ages past half its lifetime, so a superseded one always
/// has the grace left for the readers pinned to it, and SlateDB's collector
/// removes publications once they expire. Readers take the newest publication
/// whatever its expiry says.
pub(crate) struct Publisher {
    db: Db,
    published: Mutex<Option<u64>>,
    keeper: Arc<Keeper>,
    /// The highest durable sequence a commit has asked to be published, and
    /// the task that publishes it. Commits do not wait for either.
    pending: Arc<Pending>,
}

/// What a paced publisher owes its readers: the sequence to cover, and a
/// task covering it no more often than the interval.
struct Pending {
    sequence: SyncMutex<u64>,
    wake: tokio::sync::Notify,
    interval: Duration,
    task: SyncMutex<Option<JoinHandle<()>>>,
    /// Set by a close, after which nothing more is published: the store it
    /// would publish to is gone.
    stopped: SyncMutex<bool>,
}

impl Publisher {
    pub(crate) fn new(db: Db, admin: Admin) -> Self {
        Self::with_lifetime(db, admin, PUBLICATION_LIFETIME, REFRESH_TICK)
    }

    pub(crate) fn with_lifetime(db: Db, admin: Admin, lifetime: Duration, tick: Duration) -> Self {
        Self::with_pacing(db, admin, lifetime, tick, PUBLICATION_INTERVAL)
    }

    pub(crate) fn with_pacing(
        db: Db,
        admin: Admin,
        lifetime: Duration,
        tick: Duration,
        interval: Duration,
    ) -> Self {
        Self {
            keeper: Arc::new(Keeper::new(db.clone(), admin, lifetime, tick)),
            db,
            published: Mutex::new(None),
            pending: Arc::new(Pending {
                sequence: SyncMutex::new(0),
                wake: tokio::sync::Notify::new(),
                interval,
                task: SyncMutex::new(None),
                stopped: SyncMutex::new(false),
            }),
        }
    }

    /// Asks for `sequence` to be published and returns. The commit is
    /// durable; this is what makes it visible to a reader in another
    /// process, within one interval.
    pub(crate) fn request(self: &Arc<Self>, sequence: u64) {
        if *self
            .pending
            .stopped
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
        {
            return;
        }
        {
            let mut wanted = self
                .pending
                .sequence
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            *wanted = (*wanted).max(sequence);
        }

        let mut task = self
            .pending
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if task.as_ref().is_none_or(JoinHandle::is_finished) {
            // Weak, so the task cannot keep the writer it publishes for
            // alive: a dropped writer stops publishing.
            let publisher = Arc::downgrade(self);
            *task = Some(tokio::spawn(async move { Self::pace(publisher).await }));
        }
        self.pending.wake.notify_one();
    }

    /// Stops publishing, for a catalog closing: a paced task outliving its
    /// store would keep asking a closed database to publish.
    pub(crate) fn stop(&self) {
        *self
            .pending
            .stopped
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = true;
        if let Some(task) = self
            .pending
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }

    /// Publishes whatever commits have asked for, no more often than the
    /// interval, until nothing is pending.
    async fn pace(publisher: std::sync::Weak<Self>) {
        loop {
            let interval = match publisher.upgrade() {
                Some(publisher) => publisher.pending.interval,
                None => return,
            };
            tokio::time::sleep(interval).await;

            let Some(publisher) = publisher.upgrade() else {
                return;
            };
            let wanted = *publisher
                .pending
                .sequence
                .lock()
                .unwrap_or_else(PoisonError::into_inner);

            match publisher.through(wanted).await {
                Ok(()) => {}
                // A publication that fails leaves the commit durable and
                // committed; readers in other processes go on seeing the
                // previous one until a later attempt lands.
                Err(error) => tracing::warn!(%error, "a paced publication failed"),
            }

            let caught_up = publisher
                .published
                .lock()
                .await
                .is_some_and(|covered| covered >= wanted);
            let wake = Arc::clone(&publisher.pending);
            drop(publisher);
            if caught_up {
                // Nothing owed. Wait to be asked again rather than tick,
                // holding nothing but the pending state while idle.
                wake.wake.notified().await;
            }
        }
    }

    /// Publishes durable progress covering `sequence` before returning.
    pub(crate) async fn through(&self, sequence: u64) -> Result<(), slatedb::Error> {
        let mut published = self.published.lock().await;
        if published.is_some_and(|covered| covered >= sequence) {
            return Ok(());
        }
        let covered = self.db.status().durable_seq;
        if covered < sequence {
            return Err(slatedb::Error::invalid(
                "checkpoint publication preceded WAL durability".to_owned(),
            ));
        }
        self.publish(&mut published, covered, false).await
    }

    /// Publishes flushed SSTs permanently, for a writer that is closing: the
    /// publication stays discoverable with no writer alive to refresh it.
    pub(crate) async fn flushed(&self) -> Result<(), slatedb::Error> {
        let mut published = self.published.lock().await;
        self.db
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await?;
        self.publish(&mut published, self.db.status().durable_seq, true)
            .await
    }

    async fn publish(
        &self,
        published: &mut Option<u64>,
        covered: u64,
        permanent: bool,
    ) -> Result<(), slatedb::Error> {
        let checkpoint = self
            .db
            .create_checkpoint(
                CheckpointScope::Durable,
                &CheckpointOptions {
                    name: Some(PUBLISHED_NAME.to_owned()),
                    lifetime: (!permanent).then_some(self.keeper.lifetime),
                    ..CheckpointOptions::default()
                },
            )
            .await?;
        *published = Some(covered);
        Keeper::published(&self.keeper, checkpoint.id, permanent).await;
        Ok(())
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        if let Some(task) = self.keeper.state().task.take() {
            task.abort();
        }
        if let Some(task) = self
            .pending
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}

/// Keeps the current publication alive and adopts what earlier writers left.
struct Keeper {
    db: Db,
    admin: Arc<Admin>,
    lifetime: Duration,
    tick: Duration,
    state: SyncMutex<KeeperState>,
}

#[derive(Default)]
struct KeeperState {
    current: Option<Current>,
    adopted: bool,
    task: Option<JoinHandle<()>>,
}

/// The publication readers now find, and when its lifetime last started.
struct Current {
    id: Uuid,
    since: Instant,
    permanent: bool,
}

impl Keeper {
    fn new(db: Db, admin: Admin, lifetime: Duration, tick: Duration) -> Self {
        Self {
            db,
            admin: Arc::new(admin),
            lifetime,
            tick,
            state: SyncMutex::new(KeeperState::default()),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, KeeperState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records the new current publication, adopts leftovers on the first
    /// call, and keeps the refresher running while the current one expires.
    async fn published(this: &Arc<Self>, id: Uuid, permanent: bool) {
        let adopted = {
            let mut state = this.state();
            state.current = Some(Current {
                id,
                since: Instant::now(),
                permanent,
            });
            state.adopted
        };
        if !adopted {
            match this.adopt().await {
                Ok(()) => this.state().adopted = true,
                Err(error) => tracing::warn!(%error, "could not adopt earlier publications"),
            }
        }

        let mut state = this.state();
        if !permanent && state.task.as_ref().is_none_or(JoinHandle::is_finished) {
            let keeper = Arc::clone(this);
            state.task = Some(tokio::spawn(async move { keeper.run().await }));
        }
    }

    /// Gives every published checkpoint but the current one a lifetime if it
    /// has none, so an earlier writer's permanent publication expires.
    async fn adopt(&self) -> Result<(), slatedb::Error> {
        let current = self.state().current.as_ref().map(|current| current.id);
        for checkpoint in self.admin.list_checkpoints(Some(PUBLISHED_NAME)).await? {
            if Some(checkpoint.id) != current && checkpoint.expire_time.is_none() {
                self.admin
                    .refresh_checkpoint(checkpoint.id, Some(self.lifetime))
                    .await?;
            }
        }
        Ok(())
    }

    /// Refreshes the current publication once it has used half its lifetime,
    /// less two ticks of slack, until it is permanent or the writer is gone.
    async fn run(&self) {
        let refresh_after = (self.lifetime / 2).saturating_sub(self.tick * 2);
        loop {
            tokio::time::sleep(self.tick).await;
            if self.db.status().close_reason.is_some() {
                return;
            }
            let due = {
                let state = self.state();
                match &state.current {
                    Some(current) if current.permanent => return,
                    Some(current) if current.since.elapsed() >= refresh_after => Some(current.id),
                    _ => None,
                }
            };
            let Some(id) = due else {
                continue;
            };
            match self.admin.refresh_checkpoint(id, Some(self.lifetime)).await {
                Ok(()) => {
                    let mut state = self.state();
                    if let Some(current) = state.current.as_mut().filter(|current| current.id == id)
                    {
                        current.since = Instant::now();
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "could not refresh the current publication");
                }
            }
        }
    }
}
