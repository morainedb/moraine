//! One worker per attach drives backfill and bounded commit batches
//! independently.

use std::{
    collections::{BTreeSet, VecDeque},
    future::Future,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use moraine::{DataStore, ReadOnlyCatalog, RowSummaryProgress, SnapshotId, TableId};
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

#[derive(Default)]
struct Progress {
    warming: Option<VecDeque<TableId>>,
    publishing: Option<RowSummaryProgress>,
}

#[derive(Clone)]
pub(super) struct Pass {
    /// `None` warms every table; an empty set skips warming.
    tables: Option<BTreeSet<TableId>>,
    /// `None` publishes every file; otherwise only files newer than this
    /// snapshot.
    since: Option<SnapshotId>,
    progress: Arc<tokio::sync::Mutex<Progress>>,
}

impl Pass {
    pub(super) fn attach(preload: bool) -> Self {
        Self {
            tables: (!preload).then(BTreeSet::new),
            since: None,
            progress: Arc::default(),
        }
    }

    pub(super) fn commit(tables: Vec<TableId>, committed: SnapshotId) -> Self {
        Self {
            tables: (tables.len() <= MAX_PENDING_TABLES).then(|| tables.into_iter().collect()),
            since: Some(SnapshotId::new(committed.get().saturating_sub(1))),
            progress: Arc::default(),
        }
    }

    fn merge(&mut self, next: Self) {
        self.progress = Arc::default();
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
        let mut progress = self.progress.lock().await;
        if progress.warming.is_none() {
            let tables = match &self.tables {
                Some(tables) => tables.iter().copied().collect(),
                None => match catalog.snapshot().await {
                    Ok(snapshot) => snapshot
                        .schemas()
                        .into_iter()
                        .flat_map(|schema| snapshot.tables_in(schema.id))
                        .map(|table| table.id)
                        .collect(),
                    Err(error) => {
                        warn!(%error, "table warming will retry");
                        return false;
                    }
                },
            };
            progress.warming = Some(tables);
        }
        let mut warmed = true;
        if let Some(tables) = &mut progress.warming {
            while let Some(&table) = tables.front() {
                if let Err(error) = catalog.warm_tables(&[table]).await {
                    warn!(%error, "table warming will retry");
                    warmed = false;
                    break;
                }
                tables.pop_front();
                tokio::task::yield_now().await;
            }
        }
        let Some(store) = store else {
            return warmed;
        };
        let publication = progress.publishing.get_or_insert_with(|| {
            let tables = self
                .since
                .and(self.tables)
                .map(|tables| tables.into_iter().collect());
            RowSummaryProgress::new(tables, self.since)
        });
        loop {
            match publication.advance(catalog, store, prefix).await {
                Ok(true) => return warmed,
                Ok(false) => tokio::task::yield_now().await,
                Err(error) => {
                    warn!(%error, "row summary publishing will retry");
                    return false;
                }
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

#[cfg(test)]
mod tests;
