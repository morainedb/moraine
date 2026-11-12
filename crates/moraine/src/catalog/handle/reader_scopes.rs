//! Shares pinned readers across transactions starting at a published revision.

use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use object_store::ObjectStore;
use slatedb::DbReader;
use tokio::{sync::Mutex, time::Instant};

use super::{CatalogOptions, commit_log_provider, reader_session};
use crate::{
    Error, Result,
    store::{
        checkpoint::{CheckpointReader, PUBLISHED_NAME, READ_GRACE, remaining_until},
        open::StoreBuilder,
    },
};

const MAX_READER_GENERATIONS: usize = 8;

#[cfg(test)]
mod tests;

#[derive(Default)]
struct Readers {
    latest: Option<Arc<CheckpointReader>>,
    generations: Vec<Weak<CheckpointReader>>,
    /// When the publications were last listed.
    listed: Option<Instant>,
}

pub(crate) struct ReaderScopes {
    objects: Arc<dyn ObjectStore>,
    options: CatalogOptions,
    readers: Mutex<Readers>,
    explicit: Option<Arc<CheckpointReader>>,
    /// How long a scope may keep the publication its predecessor listed
    /// before listing again. Zero lists on every scope.
    reuse: Duration,
}

impl ReaderScopes {
    pub(super) fn new(
        objects: Arc<dyn ObjectStore>,
        options: CatalogOptions,
        reader: Arc<DbReader>,
        reuse: Duration,
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
            reuse,
        }
    }

    pub(crate) fn captures_log(&self) -> bool {
        commit_log_provider(&self.options).is_some()
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

    /// The shared reader, when it was listed inside the reuse window and has
    /// at least half the grace left.
    fn unexpired(&self, readers: &Readers) -> Option<Arc<CheckpointReader>> {
        let latest = readers.latest.as_ref()?;
        if readers.listed?.elapsed() >= self.reuse {
            return None;
        }
        if latest.remaining()? < READ_GRACE / 2 {
            return None;
        }
        latest.reader().ok()?;
        Some(latest.clone())
    }

    pub(crate) async fn acquire(&self) -> Result<Arc<CheckpointReader>> {
        if let Some(explicit) = &self.explicit {
            return Ok(explicit.clone());
        }
        let mut readers = self.readers.lock().await;
        if let Some(reader) = self.unexpired(&readers) {
            return Ok(reader);
        }
        if self.captures_log() {
            // Log scopes capture on every acquisition, including read-only catalogs.
            readers
                .generations
                .retain(|reader| reader.strong_count() != 0);
            if readers.generations.len() >= MAX_READER_GENERATIONS {
                return Err(Error::Constraint(
                    "catalog read generation limit reached; finish an older transaction".into(),
                ));
            }
            let (reader, _) = self.builder().open_fixed_reader().await?;
            let reader = CheckpointReader::fixed(reader);
            readers.generations.push(Arc::downgrade(&reader));
            return Ok(reader);
        }
        let admin = Arc::new(self.builder().admin());
        for _ in 0..crate::transaction::commit::MAX_COMMIT_ATTEMPTS {
            let listed = Instant::now();
            let published = admin.list_checkpoints(Some(PUBLISHED_NAME)).await?
                .into_iter().max_by_key(|checkpoint| checkpoint.manifest_id)
                .ok_or_else(|| Error::SnapshotExpired("no published read checkpoint; reopen the writer to publish recovered progress".into()))?;
            readers.listed = Some(listed);
            let expires = published
                .expire_time
                .map(|expires| remaining_until(expires.timestamp_millis()));
            if let Some(latest) = &readers.latest
                && latest.manifest == published.manifest_id
            {
                latest.confirm(listed, expires);
                if latest.reader().is_ok() {
                    return Ok(latest.clone());
                }
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
            let opened = self
                .builder()
                .checkpoint(Some(published.id))
                .open_reader()
                .await;
            let (reader, _) = match opened {
                Ok(reader) => reader,
                Err(error) => {
                    // Retry only a publication that lapsed during acquisition.
                    if admin
                        .list_checkpoints(Some(PUBLISHED_NAME))
                        .await?
                        .iter()
                        .any(|checkpoint| checkpoint.id == published.id)
                    {
                        return Err(error);
                    }
                    continue;
                }
            };
            let reader = CheckpointReader::pinned(reader, published.manifest_id, listed, expires);
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
