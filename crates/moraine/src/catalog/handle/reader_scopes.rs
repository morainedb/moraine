//! Acquires fixed log views or shares native store readers.

use std::{
    sync::{Arc, OnceLock, Weak},
    time::Duration,
};

use object_store::ObjectStore;
use slatedb::DbReader;
use tokio::{sync::Mutex, time::Instant};

use super::{CatalogOptions, commit_log_provider, reader_session};
use crate::{
    Error, Result,
    store::{
        cache::CacheCounters,
        checkpoint::CheckpointReader,
        open::{READ_WINDOW, StoreBuilder},
        retry,
    },
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
    /// The reader the attach opened, held for the catalog's life so the
    /// handle has one reader to address for status and shutdown. Rotation
    /// never retires it.
    anchor: Option<Arc<CheckpointReader>>,
}

/// Whether the store turned a request away for want of permission, which
/// SlateDB reports with the object store's error in the source chain.
fn refused_a_write(error: &slatedb::Error) -> bool {
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        let refused = |error: &object_store::Error| {
            matches!(
                error,
                object_store::Error::PermissionDenied { .. }
                    | object_store::Error::Unauthenticated { .. }
            )
        };
        if cause
            .downcast_ref::<object_store::Error>()
            .is_some_and(refused)
            || cause
                .downcast_ref::<Arc<object_store::Error>>()
                .is_some_and(|error| refused(error))
        {
            return true;
        }
        source = cause.source();
    }
    false
}

/// The refusal a store that would not take a reader's checkpoint write
/// gets, naming the two ways in.
fn checkpoint_refused(path: &str, error: &slatedb::Error) -> Error {
    Error::Configuration(format!(
        "the store refused the read-only open ({error}); a read-only open keeps a checkpoint \
         of its own, so its credentials must allow writes under `{path}/manifest/`, the only \
         objects a reader writes, or the open must pin a checkpoint the writer minted \
         (`CatalogOptions::checkpoint`, the extension's `CHECKPOINT`)"
    ))
}

pub(crate) struct ReaderScopes {
    objects: Arc<dyn ObjectStore>,
    wal_objects: Option<Arc<dyn ObjectStore>>,
    options: CatalogOptions,
    readers: Mutex<Readers>,
    explicit: Option<Arc<CheckpointReader>>,
    /// The attach's counters; every reader opened here reports to them.
    counters: Arc<CacheCounters>,
    session: OnceLock<Option<Arc<dyn crate::ReaderSession>>>,
    /// How long a scope may share the reader its predecessor opened before
    /// opening a newer one. Zero opens one per scope.
    reuse: Duration,
}

impl ReaderScopes {
    /// `explicit` is the reader a checkpoint-pinned open holds; every scope
    /// shares it and nothing else is ever opened.
    pub(super) fn new(
        objects: Arc<dyn ObjectStore>,
        options: CatalogOptions,
        explicit: Option<Arc<DbReader>>,
        reuse: Duration,
        counters: Arc<CacheCounters>,
    ) -> Self {
        let explicit = explicit.map(CheckpointReader::external);
        Self {
            session: OnceLock::new(),
            objects: retry::Store::wrap(objects),
            wal_objects: options
                .wal_store
                .as_ref()
                .map(|wal| retry::Store::wrap(wal.object_store().clone())),
            options,
            readers: Mutex::new(Readers::default()),
            explicit,
            counters,
            reuse,
        }
    }

    pub(crate) fn captures_log(&self) -> bool {
        commit_log_provider(&self.options).is_some()
    }

    /// The builder every scope reader opens through. Its poll interval is the
    /// read window: a reader that re-pointed mid-scope would advance state a
    /// transaction had already started reading.
    fn builder(&self) -> StoreBuilder<'_> {
        let builder = StoreBuilder::from_retrying_stores(
            &self.options.path,
            self.objects.clone(),
            self.wal_objects.clone(),
        )
        .counters(self.counters.clone())
        .poll_interval(READ_WINDOW)
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
    /// write refuses the open, with the ways in named.
    async fn open(
        &self,
        readers: &mut Readers,
    ) -> Result<(Arc<CheckpointReader>, Arc<CacheCounters>)> {
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

        let (reader, counters) = match self
            .builder()
            .reader_mode(slatedb::DbReaderMode::ManagedCheckpoint)
            .open_fixed_reader()
            .await
        {
            Ok((opened, counters)) => (CheckpointReader::fixed(opened), counters),
            Err(Error::Store(error)) if refused_a_write(&error) => {
                return Err(checkpoint_refused(&self.options.path, &error));
            }
            Err(error) => return Err(error),
        };
        readers.opened = Some(Instant::now());
        readers.latest = Some(reader.clone());
        readers.generations.push(Arc::downgrade(&reader));
        Ok((reader, counters))
    }

    /// Opens the reader a read-only attach reads through and holds it for
    /// the catalog's life. Scopes share it under the reuse window like any
    /// other, but it counts toward no transaction limit and rotation never
    /// retires it.
    pub(super) async fn anchor(&self) -> Result<(Arc<CheckpointReader>, Arc<CacheCounters>)> {
        let mut readers = self.readers.lock().await;
        let (reader, counters) = self.open(&mut readers).await?;
        let anchored = Arc::downgrade(&reader);
        readers
            .generations
            .retain(|generation| !Weak::ptr_eq(generation, &anchored));
        readers.anchor = Some(reader.clone());
        Ok((reader, counters))
    }

    pub(crate) async fn acquire(&self) -> Result<Arc<CheckpointReader>> {
        if let Some(explicit) = &self.explicit {
            return Ok(explicit.clone());
        }
        if let Some(session) = self
            .session
            .get_or_init(|| reader_session(self.objects.clone(), &self.options))
        {
            return Box::pin(self.acquire_logged(session.as_ref())).await;
        }
        let mut readers = self.readers.lock().await;
        if let Some(reader) = self.reusable(&readers) {
            return Ok(reader);
        }
        let (reader, _) = self.open(&mut readers).await?;
        Ok(reader)
    }

    async fn acquire_logged(
        &self,
        session: &dyn crate::ReaderSession,
    ) -> Result<Arc<CheckpointReader>> {
        let mut readers = self.readers.lock().await;
        if let Some(latest) = &readers.latest {
            let reader = latest.reader();
            if reader.status().close_reason.is_none() {
                if session.can_reuse(reader).await? {
                    return Ok(latest.clone());
                }
                if let Some((successor, _)) =
                    Box::pin(self.builder().open_incremental_reader(reader)).await?
                {
                    let successor = CheckpointReader::fixed(successor);
                    if session.can_reuse(successor.reader()).await? {
                        readers.latest = Some(successor.clone());
                    }
                    return Ok(successor);
                }
            }
        }
        // Release an unused replay allocation before reserving its replacement.
        if readers
            .latest
            .as_ref()
            .is_some_and(|reader| Arc::strong_count(reader) == 1)
            && let Some(latest) = readers.latest.take()
        {
            match Arc::try_unwrap(latest) {
                Ok(latest) => latest.retire().await,
                Err(latest) => readers.latest = Some(latest),
            }
        }
        // Distinct fixed views are bounded by the replay-memory budget.
        let (reader, _) = self.builder().open_fixed_reader().await?;
        let reader = CheckpointReader::fixed(reader);
        if session.can_reuse(reader.reader()).await? {
            readers.latest = Some(reader.clone());
        }
        Ok(reader)
    }

    /// Lets go of the shared reader and closes the attach's own, reporting
    /// whether there was one to close.
    pub(super) async fn close(&self) -> Result<bool> {
        let mut readers = self.readers.lock().await;
        readers.latest = None;
        match readers.anchor.take() {
            Some(anchor) => {
                anchor.close().await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}
