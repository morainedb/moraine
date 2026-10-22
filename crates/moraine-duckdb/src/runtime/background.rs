//! One worker per attach drives backfill and bounded commit batches
//! independently.

use std::{
    collections::BTreeSet,
    future::Future,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use moraine::{DataStore, ReadOnlyCatalog, SnapshotId, TableId};
use tokio::{
    runtime::Runtime,
    sync::{Notify, Semaphore},
    task::JoinHandle,
};
use tracing::warn;

const MAX_PENDING_TABLES: usize = 1024;

// Separate pools reserve progress for commits while backfills are saturated.
static COMMIT_SLOTS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(4));
static BACKFILL_SLOTS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(4));

#[derive(Clone, Copy)]
struct Policy {
    deadline: Duration,
    retry_initial: Duration,
    retry_maximum: Duration,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            deadline: Duration::from_mins(5),
            retry_initial: Duration::from_secs(1),
            retry_maximum: Duration::from_mins(1),
        }
    }
}

#[derive(Clone)]
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

    async fn run(self, catalog: &ReadOnlyCatalog, store: Option<&DataStore>, prefix: &str) -> bool {
        let tables = self
            .tables
            .map(|tables| tables.into_iter().collect::<Vec<_>>());
        let warmed = match &tables {
            None => warm_all_tables(catalog).await,
            Some(tables) if !tables.is_empty() => catalog.warm_tables(tables).await,
            Some(_) => Ok(()),
        };
        if let Err(error) = &warmed {
            warn!(%error, "table warming will retry");
        }
        let Some(store) = store else {
            return warmed.is_ok();
        };
        let published = match (self.since, tables) {
            (Some(since), Some(tables)) => {
                catalog
                    .publish_selected_row_summaries_since(store.clone(), prefix, tables, since)
                    .await
            }
            (Some(since), None) => {
                catalog
                    .publish_row_summaries_since(store.clone(), prefix, since)
                    .await
            }
            (None, _) => {
                catalog
                    .publish_all_row_summaries(store.clone(), prefix)
                    .await
            }
        };
        match published {
            Ok(outcome) => warmed.is_ok() && outcome.files_failed == 0,
            Err(error) => {
                warn!(%error, "row summary publishing will retry");
                false
            }
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

    async fn run_with<F: Future<Output = bool>>(
        &self,
        run: &impl Fn(Pass) -> F,
        slots: &Semaphore,
        policy: Policy,
    ) {
        loop {
            let mut pass = self.next().await;
            let mut delay = policy.retry_initial;
            loop {
                let Ok(permit) = slots.acquire().await else {
                    return;
                };
                let completed = tokio::time::timeout(policy.deadline, run(pass.clone())).await;
                drop(permit);
                if matches!(completed, Ok(true)) {
                    break;
                }
                warn!(
                    timed_out = completed.is_err(),
                    ?delay,
                    "background pass will retry"
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(policy.retry_maximum);
                if let Some(next) = self
                    .pass
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    pass.merge(next);
                }
            }
        }
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

#[derive(Default)]
struct Schedule {
    commits: Pending,
    backfill: Pending,
}

impl Schedule {
    fn push(&self, pass: Pass) {
        if pass.since.is_some() {
            self.commits.push(pass);
        } else {
            self.backfill.push(pass);
        }
    }

    async fn run<F: Future<Output = bool>>(&self, run: impl Fn(Pass) -> F) {
        // Keep polling backfill so it can release shared I/O permits.
        tokio::join!(
            self.commits
                .run_with(&run, &COMMIT_SLOTS, Policy::default()),
            self.backfill
                .run_with(&run, &BACKFILL_SLOTS, Policy::default()),
        );
    }
}

pub(super) struct Worker {
    pending: Arc<Schedule>,
    task: JoinHandle<()>,
}

impl Worker {
    pub(super) fn start(
        runtime: &Runtime,
        catalog: ReadOnlyCatalog,
        store: Option<DataStore>,
        prefix: String,
    ) -> Self {
        let pending = Arc::new(Schedule::default());
        let queue = Arc::clone(&pending);
        let task = runtime.spawn(async move {
            Box::pin(queue.run(|pass| pass.run(&catalog, store.as_ref(), &prefix))).await;
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
async fn warm_all_tables(catalog: &ReadOnlyCatalog) -> moraine::Result<()> {
    let snapshot = catalog.snapshot().await?;
    let tables = snapshot
        .schemas()
        .into_iter()
        .flat_map(|schema| snapshot.tables_in(schema.id))
        .map(|table| table.id)
        .collect::<Vec<_>>();

    catalog.warm_tables(&tables).await
}

#[cfg(test)]
mod tests;
