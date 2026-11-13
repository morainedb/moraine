//! One worker per attach drives backfill and bounded commit batches
//! independently.

use std::{
    collections::{BTreeSet, VecDeque},
    future::Future,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use moraine::{
    DataStore, ReadOnlyCatalog, RowSummaryProgress, SnapshotId, TableId,
    ffi_support::{metadata_cursor::MetadataCursor, staged::TableKind},
};
use tokio::{
    runtime::Runtime,
    sync::{Notify, Semaphore},
    task::JoinHandle,
};
use tracing::{info, warn};

const MAX_PENDING_PASSES: usize = 128;

// Separate pools reserve progress for commits while backfills are saturated.
static COMMIT_SLOTS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(4));
static WARMING_SLOTS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(4));
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
    warming: Option<Warming>,
    publishing: Option<RowSummaryProgress>,
}

struct Warming {
    tables: VecDeque<TableId>,
    cursor: Option<MetadataCursor>,
    complete: bool,
}

impl Warming {
    fn new(tables: Option<&BTreeSet<TableId>>) -> Self {
        Self {
            tables: tables.into_iter().flatten().copied().collect(),
            cursor: None,
            complete: tables.is_some(),
        }
    }

    async fn next(&mut self, catalog: &ReadOnlyCatalog) -> moraine::Result<Option<TableId>> {
        if self.tables.is_empty() && !self.complete {
            // Cancellation discards the cursor; retrying may repeat warming,
            // but cannot skip a partially consumed page.
            let mut cursor = match self.cursor.take() {
                Some(cursor) => cursor,
                None => {
                    MetadataCursor::open(catalog, TableKind::Table, None, Some(u64::MAX)).await?
                }
            };
            let page = cursor.tables().await?;
            self.complete = page.records.is_empty();
            self.tables.extend(
                page.records
                    .into_iter()
                    .map(|table| TableId::new(table.table_id)),
            );
            if !self.complete {
                self.cursor = Some(cursor);
            }
        }
        Ok(self.tables.front().copied())
    }
}

#[derive(Clone)]
pub(super) struct Pass {
    /// `None` warms every table; an empty set skips warming.
    tables: Option<BTreeSet<TableId>>,
    /// `None` publishes every file; otherwise only files newer than this
    /// snapshot.
    since: Option<SnapshotId>,
    progress: Arc<tokio::sync::Mutex<Progress>>,
    files: Option<Arc<Vec<(TableId, moraine::DataFileId)>>>,
    reservations: Vec<Arc<moraine::MemoryReservation>>,
    warming_only: bool,
}

impl Pass {
    pub(super) fn attach(preload: bool) -> Self {
        Self {
            tables: (!preload).then(BTreeSet::new),
            since: None,
            progress: Arc::default(),
            files: None,
            reservations: Vec::new(),
            warming_only: false,
        }
    }

    pub(super) fn commit(
        files: Vec<(TableId, moraine::DataFileId)>,
        committed: SnapshotId,
    ) -> moraine::Result<Self> {
        let memory = moraine::MemoryReservation::reserve((files.len() as u64).saturating_mul(128))?;
        Ok(Self {
            tables: Some(files.iter().map(|(table, _)| *table).collect()),
            since: Some(SnapshotId::new(committed.get().saturating_sub(1))),
            progress: Arc::default(),
            files: Some(Arc::new(files)),
            reservations: vec![Arc::new(memory)],
            warming_only: false,
        })
    }

    async fn run(self, catalog: &ReadOnlyCatalog, store: Option<&DataStore>, prefix: &str) -> bool {
        let mut progress = self.progress.lock().await;
        if self.warming_only {
            let started = std::time::Instant::now();
            info!(tables = ?self.tables, "background warming started");
            let warming = progress
                .warming
                .get_or_insert_with(|| Warming::new(self.tables.as_ref()));
            let mut warmed = true;
            loop {
                let table = match warming.next(catalog).await {
                    Ok(Some(table)) => table,
                    Ok(None) => break,
                    Err(error) => {
                        warn!(%error, "table discovery will retry");
                        warmed = false;
                        break;
                    }
                };
                if let Err(error) = catalog.warm_tables(&[table]).await {
                    warn!(%error, "table warming will retry");
                    warmed = false;
                    break;
                }
                warming.tables.pop_front();
                tokio::task::yield_now().await;
            }
            info!(
                complete = warmed,
                elapsed_seconds = started.elapsed().as_secs_f64(),
                remaining_tables = warming.tables.len(),
                "background warming advanced"
            );
            return warmed;
        }
        let Some(store) = store else {
            return true;
        };
        let publication = progress.publishing.get_or_insert_with(|| {
            if let Some(files) = self.files {
                return RowSummaryProgress::for_files((*files).clone());
            }
            let tables = self
                .since
                .and(self.tables)
                .map(|tables| tables.into_iter().collect());
            RowSummaryProgress::new(tables, self.since)
        });
        loop {
            match publication.advance(catalog, store, prefix).await {
                Ok(true) => return true,
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
    passes: Mutex<VecDeque<Pass>>,
    ready: Notify,
}

impl Pending {
    fn push(&self, mut next: Pass) -> bool {
        let mut pending = self
            .passes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if next.warming_only {
            for queued in pending.iter_mut() {
                let unstarted = queued
                    .progress
                    .try_lock()
                    .is_ok_and(|progress| progress.warming.is_none());
                if queued.warming_only
                    && unstarted
                    && queued.reservations.len() + next.reservations.len() <= MAX_PENDING_PASSES
                {
                    match (&mut queued.tables, next.tables.take()) {
                        (Some(queued), Some(tables)) => queued.extend(tables),
                        (tables, None) => *tables = None,
                        (None, _) => {}
                    }
                    queued.reservations.append(&mut next.reservations);
                    return true;
                }
            }
        }
        if pending.len() >= MAX_PENDING_PASSES {
            return false;
        }
        pending.push_back(next);
        self.ready.notify_one();
        true
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
                    stage = if pass.warming_only { "warming" } else { "publication" },
                    since = ?pass.since,
                    timed_out = completed.is_err(),
                    ?delay,
                    "background pass will retry"
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(policy.retry_maximum);
                // Rotate existing work without admitting another queue entry.
                let mut pending = self
                    .passes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(next) = pending.pop_front() {
                    pending.push_back(pass);
                    pass = next;
                }
            }
        }
    }

    async fn next(&self) -> Pass {
        loop {
            if let Some(pass) = self
                .passes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
            {
                return pass;
            }
            self.ready.notified().await;
        }
    }
}

#[derive(Default)]
struct Schedule {
    commits: Pending,
    backfill: Pending,
    warming: Pending,
}

impl Schedule {
    fn push(&self, pass: Pass) {
        if pass.tables.as_ref().is_none_or(|tables| !tables.is_empty()) {
            let mut warming = pass.clone();
            warming.warming_only = true;
            warming.progress = Arc::default();
            warming.files = None;
            if !self.warming.push(warming) {
                warn!("background admission full; warming was not queued");
            }
        }
        if pass.since.is_some() {
            if !self.commits.push(pass) {
                warn!(
                    "background admission full; files remain readable and can be published by explicit backfill"
                );
            }
        } else if !self.backfill.push(pass) {
            warn!("background admission full; backfill was not queued");
        }
    }

    async fn run<F: Future<Output = bool>>(&self, run: impl Fn(Pass) -> F) {
        // Keep polling backfill so it can release shared I/O permits.
        tokio::join!(
            self.warming
                .run_with(&run, &WARMING_SLOTS, Policy::default()),
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
