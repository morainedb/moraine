//! Published read boundaries and independently renewed reader leases.

use std::{
    sync::{Arc, Mutex as SyncMutex, PoisonError},
    time::Duration,
};

use slatedb::{
    Db, DbReader,
    admin::Admin,
    config::{CheckpointOptions, CheckpointScope},
};
use tokio::{
    sync::{Mutex, watch},
    time::Instant,
};
use uuid::Uuid;

/// The writer's discoverable checkpoint; readers take their own leases on it.
pub(crate) const PUBLISHED_NAME: &str = "moraine-read-head-v1";
pub(crate) const LEASE_LIFETIME: Duration = Duration::from_mins(5);
const LEASE_REFRESH: Duration = Duration::from_mins(1);
const LEASE_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
mod tests;

/// A fixed reader whose independently renewed checkpoint outlives its cursors.
pub(crate) struct CheckpointReader {
    reader: Arc<DbReader>,
    pub(crate) manifest: u64,
    expires: Option<Arc<SyncMutex<Instant>>>,
    _stop: Option<watch::Sender<()>>,
}

impl CheckpointReader {
    /// Borrows the reader only while its checkpoint remains protected.
    pub(crate) fn reader(&self) -> Result<&DbReader, slatedb::Error> {
        if let Some(expires) = &self.expires
            && Instant::now() >= *expires.lock().unwrap_or_else(PoisonError::into_inner)
        {
            return Err(slatedb::Error::unavailable(
                "read checkpoint lease expired".to_owned(),
            ));
        }
        Ok(&self.reader)
    }

    /// Uses a checkpoint whose lifecycle the caller already owns.
    pub(crate) fn external(reader: Arc<DbReader>) -> Arc<Self> {
        Arc::new(Self {
            manifest: reader.manifest().id(),
            reader,
            expires: None,
            _stop: None,
        })
    }

    pub(crate) fn leased(
        reader: DbReader,
        manifest: u64,
        checkpoint: Uuid,
        admin: Arc<Admin>,
        created: Instant,
    ) -> Arc<Self> {
        let reader = Arc::new(reader);
        let expires = Arc::new(SyncMutex::new(created + LEASE_LIFETIME));
        let (stop, mut stopped) = watch::channel(());
        let renewal_reader = reader.clone();
        let renewal_expires = expires.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stopped.changed() => break,
                    () = tokio::time::sleep(LEASE_REFRESH) => {}
                }
                let started = Instant::now();
                match tokio::time::timeout(
                    LEASE_TIMEOUT,
                    admin.refresh_checkpoint(checkpoint, Some(LEASE_LIFETIME)),
                )
                .await
                {
                    Ok(Ok(())) => {
                        let mut expires = renewal_expires
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner);
                        if Instant::now() >= *expires {
                            break;
                        }
                        *expires = started + LEASE_LIFETIME;
                    }
                    result => {
                        tracing::warn!(?result, "read checkpoint renewal failed");
                        // Wake on the next interval while the existing lease
                        // remains valid.
                    }
                }
            }
            let _ = renewal_reader.close().await;
            let _ = tokio::time::timeout(LEASE_TIMEOUT, admin.delete_checkpoint(checkpoint)).await;
        });
        Arc::new(Self {
            reader,
            manifest,
            expires: Some(expires),
            _stop: Some(stop),
        })
    }
}

/// Serializes publication and coalesces writes covered by the same checkpoint.
pub(crate) struct Publisher {
    db: Db,
    admin: Arc<Admin>,
    published: Mutex<Option<u64>>,
}

impl Publisher {
    pub(crate) fn new(db: Db, admin: Admin) -> Self {
        Self {
            db,
            admin: Arc::new(admin),
            published: Mutex::new(None),
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

        // The current publication remains discoverable while the writer is idle
        // or offline. Older publications have independent reader-owned leases.
        if let Err(error) = self.retire_before(checkpoint.manifest_id).await {
            tracing::warn!(%error, "could not retire superseded read checkpoints");
        }
        Ok(())
    }

    async fn retire_before(&self, manifest: u64) -> Result<(), slatedb::Error> {
        for checkpoint in self.admin.list_checkpoints(Some(PUBLISHED_NAME)).await? {
            if checkpoint.manifest_id < manifest {
                self.admin.delete_checkpoint(checkpoint.id).await?;
            }
        }
        Ok(())
    }
}
