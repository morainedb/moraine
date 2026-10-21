//! One worker per attach, with one bounded batch waiting behind it.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use moraine::{DataStore, ReadOnlyCatalog, SnapshotId, TableId};
use tokio::{runtime::Runtime, sync::Notify, task::JoinHandle};
use tracing::warn;

const MAX_PENDING_TABLES: usize = 1024;

pub(super) struct Pass {
    /// `None` warms every table; an empty set skips warming.
    tables: Option<BTreeSet<TableId>>,
    /// `None` publishes every file; otherwise only files newer than this
    /// snapshot.
    since: Option<SnapshotId>,
}

impl Pass {
    pub(super) fn attach(preload: bool) -> Self {
        Self {
            tables: (!preload).then(BTreeSet::new),
            since: None,
        }
    }

    pub(super) fn commit(tables: Vec<TableId>, committed: SnapshotId) -> Self {
        Self {
            tables: (tables.len() <= MAX_PENDING_TABLES).then(|| tables.into_iter().collect()),
            since: Some(SnapshotId::new(committed.get().saturating_sub(1))),
        }
    }

    fn merge(&mut self, next: Self) {
        // A full backfill (`None`) dominates every snapshot cutoff.
        self.since = self.since.min(next.since);
        match (&mut self.tables, next.tables) {
            (Some(tables), Some(next)) => {
                tables.extend(next);
                if tables.len() > MAX_PENDING_TABLES {
                    self.tables = None;
                }
            }
            _ => self.tables = None,
        }
    }

    async fn run(self, catalog: &ReadOnlyCatalog, store: Option<&DataStore>, prefix: &str) {
        match self.tables {
            None => warm_all_tables(catalog).await,
            Some(tables) if !tables.is_empty() => {
                if let Err(error) = catalog
                    .warm_tables(&tables.into_iter().collect::<Vec<_>>())
                    .await
                {
                    warn!(%error, "table warming skipped this pass");
                }
            }
            Some(_) => {}
        }
        let Some(store) = store else { return };
        let published = match self.since {
            Some(since) => {
                catalog
                    .publish_row_summaries_since(store.clone(), prefix, since)
                    .await
            }
            None => {
                catalog
                    .publish_all_row_summaries(store.clone(), prefix)
                    .await
            }
        };
        if let Err(error) = published {
            warn!(%error, "row summary publishing skipped this pass");
        }
    }
}

#[derive(Default)]
struct Pending {
    pass: Mutex<Option<Pass>>,
    ready: Notify,
}

impl Pending {
    fn push(&self, next: Pass) {
        let mut pending = self
            .pass
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *pending {
            Some(pass) => pass.merge(next),
            None => *pending = Some(next),
        }
        self.ready.notify_one();
    }

    async fn next(&self) -> Pass {
        loop {
            self.ready.notified().await;
            if let Some(pass) = self
                .pass
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                return pass;
            }
        }
    }
}

pub(super) struct Worker {
    pending: Arc<Pending>,
    task: JoinHandle<()>,
}

impl Worker {
    pub(super) fn start(
        runtime: &Runtime,
        catalog: ReadOnlyCatalog,
        store: Option<DataStore>,
        prefix: String,
    ) -> Self {
        let pending = Arc::new(Pending::default());
        let queue = Arc::clone(&pending);
        let task = runtime.spawn(async move {
            loop {
                queue
                    .next()
                    .await
                    .run(&catalog, store.as_ref(), &prefix)
                    .await;
            }
        });
        Self { pending, task }
    }

    pub(super) fn push(&self, pass: Pass) {
        self.pending.push(pass);
    }

    pub(super) fn stop(mut self, runtime: &Runtime) {
        self.task.abort();
        let _ = runtime.block_on(&mut self.task);
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Warms every table in the current snapshot.
async fn warm_all_tables(catalog: &ReadOnlyCatalog) {
    let snapshot = match catalog.snapshot().await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(%error, "table warming skipped this pass");
            return;
        }
    };
    let tables = snapshot
        .schemas()
        .into_iter()
        .flat_map(|schema| snapshot.tables_in(schema.id))
        .map(|table| table.id)
        .collect::<Vec<_>>();

    if let Err(error) = catalog.warm_tables(&tables).await {
        warn!(%error, "table warming skipped this pass");
    }
}

#[cfg(test)]
mod tests;
