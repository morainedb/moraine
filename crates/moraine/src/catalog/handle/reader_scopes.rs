//! Acquires fixed log views or shares native store readers.

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
    store::{checkpoint::CheckpointReader, open::StoreBuilder},
};

const MAX_READER_GENERATIONS: usize = 8;

#[cfg(test)]
mod tests;

#[derive(Default)]
struct Readers {
    latest: Option<Arc<CheckpointReader>>,
    generations: Vec<Weak<CheckpointReader>>,
    /// When the shared reader was opened.
    opened: Option<Instant>,
}

pub(crate) struct ReaderScopes {
    objects: Arc<dyn ObjectStore>,
    options: CatalogOptions,
    readers: Mutex<Readers>,
    explicit: Option<Arc<CheckpointReader>>,
    /// How long a scope may share the reader its predecessor opened before
    /// opening a newer one. Zero opens one per scope.
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
        let session = reader_session(self.objects.clone(), &self.options);
        builder.reader_session(session)
    }

    /// The shared reader, while it is inside the reuse window. Past it a
    /// scope opens its own, which is what makes a commit by another process
    /// visible: a reader advances only on its own manifest poll.
    fn reusable(&self, readers: &Readers) -> Option<Arc<CheckpointReader>> {
        let latest = readers.latest.as_ref()?;
        if readers.opened?.elapsed() >= self.reuse {
            return None;
        }
        Some(latest.clone())
    }

    /// Opens a reader over the latest state, keeping a checkpoint of its own
    /// so collection cannot take what it reads. A store that refuses that
    /// write is followed on retention alone, under a read window.
    async fn open(&self, readers: &mut Readers) -> Result<Arc<CheckpointReader>> {
        readers
            .generations
            .retain(|reader| reader.strong_count() != 0);
        if readers.generations.len() >= MAX_READER_GENERATIONS {
            return Err(Error::Constraint(format!(
                "catalog read generation limit ({MAX_READER_GENERATIONS}) reached; finish an older transaction before opening another"
            )));
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

        let reader = match self
            .builder()
            .reader_mode(slatedb::DbReaderMode::ManagedCheckpoint)
            .open_reader()
            .await
        {
            Ok((opened, _)) => CheckpointReader::fixed(opened),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "could not keep a checkpoint of our own; following the latest manifest"
                );
                let (opened, _) = self
                    .builder()
                    .reader_mode(slatedb::DbReaderMode::FollowLatest)
                    .open_reader()
                    .await?;
                CheckpointReader::following(opened)
            }
        };
        readers.opened = Some(Instant::now());
        readers.latest = Some(reader.clone());
        readers.generations.push(Arc::downgrade(&reader));
        Ok(reader)
    }

    pub(crate) async fn acquire(&self) -> Result<Arc<CheckpointReader>> {
        if let Some(explicit) = &self.explicit {
            return Ok(explicit.clone());
        }
        if self.captures_log() {
            // Replay bytes are admitted by the process working-memory budget.
            let (reader, _) = self.builder().open_fixed_reader().await?;
            return Ok(CheckpointReader::fixed(reader));
        }
        let mut readers = self.readers.lock().await;
        if let Some(reader) = self.reusable(&readers) {
            return Ok(reader);
        }
        self.open(&mut readers).await
    }

    pub(super) async fn close(&self) {
        self.readers.lock().await.latest = None;
    }
}
