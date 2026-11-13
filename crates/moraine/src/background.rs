//! Bounded background warming, publication, and backfill protocols.

use std::{
    collections::{BTreeSet, VecDeque},
    future::Future,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use tokio::sync::{Notify, Semaphore};
use tracing::{info, warn};

use crate::{
    DataStore, ReadOnlyCatalog, RowSummaryProgress, SnapshotId, TableId,
    store::read::{EntityRecord, EntityRecordKind, OwnedEntityCursor, Versions},
};

const MAX_PENDING_JOBS: usize = 128;

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

struct Warming {
    tables: VecDeque<TableId>,
    cursor: Option<OwnedEntityCursor>,
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

    async fn next(&mut self, catalog: &ReadOnlyCatalog) -> crate::Result<Option<TableId>> {
        if self.tables.is_empty() && !self.complete {
            // Cancellation discards the cursor; retrying may repeat warming,
            // but cannot skip a partially consumed page.
            let mut cursor = match self.cursor.take() {
                Some(cursor) => cursor,
                None => {
                    OwnedEntityCursor::open(
                        catalog.read_scope().await?.reads().owned_read_handle(),
                        EntityRecordKind::Table,
                        Versions::Live,
                        None,
                        crate::read_policy::CacheAdmission::ReuseOnly,
                    )
                    .await?
                }
            };
            for _ in 0..128 {
                let Some(record) = cursor.next().await? else {
                    self.complete = true;
                    break;
                };
                let EntityRecord::Table(table) = record.value() else {
                    return Err(crate::Error::Corruption(
                        "table cursor returned another kind".into(),
                    ));
                };
                self.tables.push_back(TableId::new(table.table_id));
            }
            if !self.complete {
                self.cursor = Some(cursor);
            }
        }
        Ok(self.tables.front().copied())
    }
}

/// An attach or committed-file notification for background maintenance.
/// File inputs hold a working-memory reservation until their jobs finish.
pub struct BackgroundRequest(Request);

enum Request {
    Attach {
        preload: bool,
    },
    Commit {
        files: Arc<Vec<(TableId, crate::DataFileId)>>,
        committed: SnapshotId,
        memory: Arc<crate::MemoryReservation>,
    },
}

impl BackgroundRequest {
    /// Requests full summary backfill and optional table warming on attach.
    #[must_use]
    pub fn attach(preload: bool) -> Self {
        Self(Request::Attach { preload })
    }

    /// Requests summary publication and warming for newly registered files.
    ///
    /// # Errors
    /// Returns an error if the input cannot fit the working-memory allowance.
    pub fn commit(
        files: Vec<(TableId, crate::DataFileId)>,
        committed: SnapshotId,
    ) -> crate::Result<Self> {
        let memory =
            crate::MemoryReservation::reserve((files.capacity() as u64).saturating_mul(128))?;
        Ok(Self(Request::Commit {
            files: Arc::new(files),
            committed,
            memory: Arc::new(memory),
        }))
    }
}

#[derive(Clone)]
struct WarmJob {
    tables: Option<BTreeSet<TableId>>,
    progress: Arc<tokio::sync::Mutex<Option<Warming>>>,
    reservations: Vec<Arc<crate::MemoryReservation>>,
}

#[derive(Clone)]
struct PublishJob {
    files: Arc<Vec<(TableId, crate::DataFileId)>>,
    committed: SnapshotId,
    progress: Arc<tokio::sync::Mutex<Option<RowSummaryProgress>>>,
    _memory: Arc<crate::MemoryReservation>,
}

#[derive(Clone, Default)]
struct BackfillJob {
    progress: Arc<tokio::sync::Mutex<Option<RowSummaryProgress>>>,
}

#[derive(Clone)]
enum Job {
    Warm(WarmJob),
    Publish(PublishJob),
    Backfill(BackfillJob),
}

impl Job {
    fn kind(&self) -> &'static str {
        match self {
            Self::Warm(_) => "warming",
            Self::Publish(_) => "publication",
            Self::Backfill(_) => "backfill",
        }
    }

    async fn run(self, catalog: &ReadOnlyCatalog, store: Option<&DataStore>, prefix: &str) -> bool {
        match self {
            Self::Warm(job) => job.run(catalog).await,
            Self::Publish(job) => {
                tracing::debug!(
                    snapshot = job.committed.get(),
                    files = job.files.len(),
                    "publishing committed files"
                );
                let mut progress = job.progress.lock().await;
                let progress = progress
                    .get_or_insert_with(|| RowSummaryProgress::for_files((*job.files).clone()));
                publish(progress, catalog, store, prefix).await
            }
            Self::Backfill(job) => {
                let mut progress = job.progress.lock().await;
                let progress = progress.get_or_insert_with(|| RowSummaryProgress::new(None, None));
                publish(progress, catalog, store, prefix).await
            }
        }
    }
}

impl WarmJob {
    async fn run(self, catalog: &ReadOnlyCatalog) -> bool {
        let mut progress = self.progress.lock().await;
        let warming = progress.get_or_insert_with(|| Warming::new(self.tables.as_ref()));
        let started = std::time::Instant::now();
        loop {
            let table = match warming.next(catalog).await {
                Ok(Some(table)) => table,
                Ok(None) => {
                    info!(
                        elapsed_seconds = started.elapsed().as_secs_f64(),
                        "background warming completed"
                    );
                    return true;
                }
                Err(error) => {
                    warn!(%error, "table discovery will retry");
                    return false;
                }
            };
            if let Err(error) = catalog.warm_tables(&[table]).await {
                warn!(%error, "table warming will retry");
                return false;
            }
            warming.tables.pop_front();
            tokio::task::yield_now().await;
        }
    }
}

async fn publish(
    progress: &mut RowSummaryProgress,
    catalog: &ReadOnlyCatalog,
    store: Option<&DataStore>,
    prefix: &str,
) -> bool {
    let Some(store) = store else {
        return true;
    };
    loop {
        match progress.advance(catalog, store, prefix).await {
            Ok(true) => return true,
            Ok(false) => tokio::task::yield_now().await,
            Err(error) => {
                warn!(%error, "row summary publishing will retry");
                return false;
            }
        }
    }
}

#[derive(Default)]
struct Pending {
    jobs: Mutex<VecDeque<Job>>,
    ready: Notify,
}

impl Pending {
    fn push(&self, mut next: Job) -> bool {
        let mut pending = self
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Job::Warm(next) = &mut next {
            for queued in pending.iter_mut() {
                let Job::Warm(queued) = queued else {
                    continue;
                };
                let unstarted = queued
                    .progress
                    .try_lock()
                    .is_ok_and(|progress| progress.is_none());
                if unstarted
                    && queued.reservations.len() + next.reservations.len() <= MAX_PENDING_JOBS
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
        if pending.len() >= MAX_PENDING_JOBS {
            return false;
        }
        pending.push_back(next);
        self.ready.notify_one();
        true
    }

    async fn run_with<F: Future<Output = bool>>(
        &self,
        run: &impl Fn(Job) -> F,
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
                    stage = pass.kind(),
                    timed_out = completed.is_err(),
                    ?delay,
                    "background pass will retry"
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(policy.retry_maximum);
                // Rotate existing work without admitting another queue entry.
                let mut pending = self
                    .jobs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(next) = pending.pop_front() {
                    pending.push_back(pass);
                    pass = next;
                }
            }
        }
    }

    async fn next(&self) -> Job {
        loop {
            if let Some(pass) = self
                .jobs
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

/// Per-attachment queues with independent warming, publication, and backfill
/// lanes. The caller spawns [`Self::run`] and cancels and joins it before
/// closing the catalog.
#[derive(Default)]
pub struct BackgroundWork {
    commits: Pending,
    backfill: Pending,
    warming: Pending,
}

impl BackgroundWork {
    /// Enqueues best-effort work. Full queues are reported; explicit backfill
    /// can recover publication work that could not be admitted.
    pub fn push(&self, request: BackgroundRequest) {
        let (warming, publication) = match request.0 {
            Request::Attach { preload } => (
                preload.then(|| WarmJob {
                    tables: None,
                    progress: Arc::default(),
                    reservations: Vec::new(),
                }),
                Job::Backfill(BackfillJob::default()),
            ),
            Request::Commit {
                files,
                committed,
                memory,
            } => {
                let warming = (!files.is_empty()).then(|| WarmJob {
                    tables: Some(files.iter().map(|(table, _)| *table).collect()),
                    progress: Arc::default(),
                    reservations: vec![memory.clone()],
                });
                (
                    warming,
                    Job::Publish(PublishJob {
                        files,
                        committed,
                        progress: Arc::default(),
                        _memory: memory,
                    }),
                )
            }
        };
        if let Some(warming) = warming
            && !self.warming.push(Job::Warm(warming))
        {
            warn!("background admission full; warming was not queued");
        }
        let queue = match &publication {
            Job::Publish(_) => &self.commits,
            Job::Backfill(_) => &self.backfill,
            Job::Warm(_) => return,
        };
        if !queue.push(publication) {
            warn!(
                "background admission full; files remain readable and can be published by explicit backfill"
            );
        }
    }

    /// Drives all lanes until cancelled. Dropping this future releases running
    /// attempts; dropping the queue also releases pending inputs and progress.
    pub async fn run(&self, catalog: &ReadOnlyCatalog, store: Option<&DataStore>, prefix: &str) {
        use tracing::Instrument as _;
        self.run_with(|job| {
            let span = tracing::info_span!("background_job", kind = job.kind());
            job.run(catalog, store, prefix).instrument(span)
        })
        .await;
    }

    async fn run_with<F: Future<Output = bool>>(&self, run: impl Fn(Job) -> F) {
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

#[cfg(test)]
mod tests;
