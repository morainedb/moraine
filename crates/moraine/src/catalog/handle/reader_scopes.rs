//! Shares fixed readers across transactions starting at a published revision.

use std::sync::{Arc, Weak};

use object_store::ObjectStore;
use slatedb::{DbReader, config::CheckpointOptions};
use tokio::{sync::Mutex, time::Instant};

use super::{CatalogOptions, reader_session};
use crate::{
    Error, Result,
    store::{
        checkpoint::{CheckpointReader, LEASE_LIFETIME, PUBLISHED_NAME},
        open::StoreBuilder,
    },
};

const MAX_READER_GENERATIONS: usize = 8;

#[derive(Default)]
struct Readers {
    latest: Option<Arc<CheckpointReader>>,
    generations: Vec<Weak<CheckpointReader>>,
}

pub(super) struct ReaderScopes {
    objects: Arc<dyn ObjectStore>,
    options: CatalogOptions,
    readers: Mutex<Readers>,
    explicit: Option<Arc<CheckpointReader>>,
}

impl ReaderScopes {
    pub(super) fn new(
        objects: Arc<dyn ObjectStore>,
        options: CatalogOptions,
        reader: Arc<DbReader>,
    ) -> Self {
        let explicit = options
            .checkpoint
            .as_ref()
            .map(|_| CheckpointReader::external(reader));
        Self {
            objects,
            options,
            readers: Mutex::new(Readers::default()),
            explicit,
        }
    }

    fn builder(&self) -> StoreBuilder<'_> {
        let builder = StoreBuilder::new(&self.options.path, self.objects.clone())
            .wal_object_store(
                self.options
                    .wal_store
                    .as_ref()
                    .map(|wal| wal.object_store().clone()),
            )
            .cache_dir(self.options.cache_dir.clone())
            .cache_identity(self.options.cache_identity)
            .cache_size(self.options.cache_size)
            .cache_memory(self.options.cache_memory)
            .cache_auxiliary_percent(self.options.cache_auxiliary_percent)
            .cache_puts(self.options.cache_puts)
            .cache_compaction_puts(self.options.cache_compaction_puts);
        let session = reader_session(self.objects.clone(), &self.options, true);
        builder.reader_session(session)
    }

    pub(super) async fn acquire(&self) -> Result<Arc<CheckpointReader>> {
        if let Some(explicit) = &self.explicit {
            return Ok(explicit.clone());
        }
        let mut readers = self.readers.lock().await;
        let admin = Arc::new(self.builder().admin());
        for _ in 0..crate::transaction::commit::MAX_COMMIT_ATTEMPTS {
            let published = admin.list_checkpoints(Some(PUBLISHED_NAME)).await?
                .into_iter().max_by_key(|checkpoint| checkpoint.manifest_id)
                .ok_or_else(|| Error::SnapshotExpired("no published read checkpoint; reopen the writer to publish recovered progress".into()))?;
            if let Some(latest) = &readers.latest
                && latest.manifest == published.manifest_id
                && latest.reader().is_ok()
            {
                return Ok(latest.clone());
            }
            let unused_latest = readers
                .latest
                .as_ref()
                .is_some_and(|reader| Arc::strong_count(reader) == 1);
            if unused_latest && let Some(latest) = readers.latest.take() {
                match Arc::try_unwrap(latest) {
                    Ok(latest) => latest.retire().await,
                    Err(latest) => readers.latest = Some(latest),
                }
            }
            readers
                .generations
                .retain(|reader| reader.strong_count() != 0);
            if readers.generations.len() >= MAX_READER_GENERATIONS {
                return Err(Error::Constraint(format!(
                    "catalog read generation limit ({MAX_READER_GENERATIONS}) reached; finish an older transaction before opening another"
                )));
            }
            let created = Instant::now();
            let lease = match admin
                .create_detached_checkpoint(&CheckpointOptions {
                    source: Some(published.id),
                    lifetime: Some(LEASE_LIFETIME),
                    ..CheckpointOptions::default()
                })
                .await
            {
                Ok(lease) => lease,
                Err(error) => {
                    // Retry only a publication that was retired during acquisition.
                    if admin
                        .list_checkpoints(Some(PUBLISHED_NAME))
                        .await?
                        .iter()
                        .any(|checkpoint| checkpoint.id == published.id)
                    {
                        return Err(error.into());
                    }
                    continue;
                }
            };
            let opened = self
                .builder()
                .checkpoint(Some(lease.id))
                .open_reader()
                .await;
            let (reader, _) = match opened {
                Ok(reader) => reader,
                Err(error) => {
                    let _ = admin.delete_checkpoint(lease.id).await;
                    return Err(error);
                }
            };
            let reader =
                CheckpointReader::leased(reader, published.manifest_id, lease.id, admin, created);
            reader.reader()?;
            readers.latest = Some(reader.clone());
            readers.generations.push(Arc::downgrade(&reader));
            return Ok(reader);
        }
        Err(Error::RetryBudgetExhausted(
            "published read checkpoints changed during acquisition".into(),
        ))
    }

    pub(super) async fn close(&self) {
        self.readers.lock().await.latest = None;
    }
}
