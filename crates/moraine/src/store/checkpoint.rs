//! Published read boundaries and the pinned readers that follow them.

use std::{
    sync::{Arc, Mutex as SyncMutex, PoisonError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use slatedb::{
    Db, DbReader,
    admin::Admin,
    config::{CheckpointOptions, CheckpointScope},
};
use tokio::{sync::Mutex, time::Instant};

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

/// Wall-clock time left until `expire_millis`, a Unix timestamp.
pub(crate) fn remaining_until(expire_millis: i64) -> Duration {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_millis()).ok())
        .unwrap_or(i64::MAX);
    Duration::from_millis(u64::try_from(expire_millis.saturating_sub(now)).unwrap_or(0))
}

/// Serializes publication and coalesces writes covered by the same checkpoint.
pub(crate) struct Publisher {
    db: Db,
    admin: Arc<Admin>,
    published: Mutex<Option<u64>>,
    grace: Duration,
}

impl Publisher {
    pub(crate) fn new(db: Db, admin: Admin) -> Self {
        Self::with_grace(db, admin, READ_GRACE)
    }

    pub(crate) fn with_grace(db: Db, admin: Admin, grace: Duration) -> Self {
        Self {
            db,
            admin: Arc::new(admin),
            published: Mutex::new(None),
            grace,
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
        self.publish(&mut published, covered).await
    }

    /// Publishes flushed SSTs even when their sequence was already announced.
    pub(crate) async fn flushed(&self) -> Result<(), slatedb::Error> {
        let mut published = self.published.lock().await;
        self.db
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await?;
        self.publish(&mut published, self.db.status().durable_seq)
            .await
    }

    async fn publish(
        &self,
        published: &mut Option<u64>,
        covered: u64,
    ) -> Result<(), slatedb::Error> {
        let checkpoint = self
            .db
            .create_checkpoint(
                CheckpointScope::Durable,
                &CheckpointOptions {
                    name: Some(PUBLISHED_NAME.to_owned()),
                    ..CheckpointOptions::default()
                },
            )
            .await?;
        *published = Some(covered);

        // The current publication stays discoverable, without expiry, while
        // the writer is idle or offline. Readers pinned to a superseded one
        // keep it for the grace.
        if let Err(error) = self.retire_before(checkpoint.manifest_id).await {
            tracing::warn!(%error, "could not retire superseded read checkpoints");
        }
        Ok(())
    }

    async fn retire_before(&self, manifest: u64) -> Result<(), slatedb::Error> {
        for checkpoint in self.admin.list_checkpoints(Some(PUBLISHED_NAME)).await? {
            if checkpoint.manifest_id >= manifest {
                continue;
            }
            match checkpoint.expire_time {
                None => {
                    self.admin
                        .refresh_checkpoint(checkpoint.id, Some(self.grace))
                        .await?;
                }
                Some(expires) if remaining_until(expires.timestamp_millis()).is_zero() => {
                    self.admin.delete_checkpoint(checkpoint.id).await?;
                }
                Some(_) => {}
            }
        }
        Ok(())
    }
}
