//! Bounded, cancellable prefetch of projected Parquet batches.

use std::{
    future::Future,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow::array::RecordBatch;
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinHandle,
};

use super::{ParquetFile, RowIdSource, RowPositions, ScopedRows, scoped_read_row_stream};
use crate::error::{Error, Result};

pub(crate) fn read_worker_limit() -> usize {
    std::thread::available_parallelism().map_or(1, |cores| (cores.get() / 2).clamp(1, 4))
}

static PERMITS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(read_worker_limit())));

/// Partitions exact file positions into nonempty row-group work units.
pub(crate) async fn row_group_selections(
    file: &ParquetFile,
    positions: &RowPositions,
) -> Result<Vec<RowPositions>> {
    let counts = super::row_group_row_counts(file).await?;
    let mut groups = Vec::new();
    let mut remaining = positions.as_slice();
    let mut end = 0_u64;
    for count in counts {
        end = end
            .checked_add(count)
            .ok_or_else(|| Error::Corruption("row-group positions overflow".into()))?;
        let length = remaining.partition_point(|&position| position < end);
        if length > 0 {
            groups.push(RowPositions::from_unsorted(remaining[..length].to_vec()));
            remaining = &remaining[length..];
        }
    }
    if !remaining.is_empty() {
        return Err(Error::Corruption(
            "selected position exceeds the file's row groups".into(),
        ));
    }
    Ok(groups)
}

#[derive(Default)]
pub(crate) struct ReadWorkers {
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl ReadWorkers {
    pub(crate) fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
    #[cfg(test)]
    pub(crate) fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }
}

struct ActiveWorker(Arc<ReadWorkers>);
impl Drop for ActiveWorker {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}

struct PendingWorker(Option<JoinHandle<()>>);
impl Drop for PendingWorker {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

async fn read_bounded<T>(
    workers: &Arc<ReadWorkers>,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    // Process-wide, so one catalog's reads starve every other one's; named
    // here because a caller parked on it is otherwise silent.
    let _permit =
        crate::telemetry::reporting_phase("read-worker-permit", PERMITS.clone().acquire_owned())
            .await
            .map_err(|error| Error::Interrupted(error.to_string()))?;
    let active = workers.active.fetch_add(1, Ordering::Relaxed) + 1;
    workers.peak.fetch_max(active, Ordering::Relaxed);
    let _active = ActiveWorker(workers.clone());
    // Named for the same reason as the wait above: a read that never
    // returns holds its permit against every other catalog.
    crate::telemetry::reporting_phase("read-worker-hold", work).await
}

pub(crate) fn prefetched_row_stream(
    file: ParquetFile,
    requested: Arc<Vec<usize>>,
    positions: RowPositions,
    source: RowIdSource,
    workers: Arc<ReadWorkers>,
) -> BoxStream<'static, Result<RecordBatch>> {
    stream::once(async move {
        let (sender, receiver) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            let result = async {
                let mut batches = read_bounded(
                    &workers,
                    scoped_read_row_stream(file, &requested, ScopedRows::At(&positions), source),
                )
                .await?;
                loop {
                    // Reserve before decoding: at most one queued batch per worker.
                    let Ok(slot) = sender.reserve().await else {
                        return Ok(());
                    };
                    let Some(batch) = read_bounded(&workers, batches.try_next()).await? else {
                        return Ok::<_, Error>(());
                    };
                    slot.send(Ok(batch));
                }
            }
            .await;
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
            }
        });
        stream::unfold(
            (receiver, PendingWorker(Some(task))),
            |(mut receiver, mut worker)| async move {
                if let Some(batch) = receiver.recv().await {
                    return Some((batch, (receiver, worker)));
                }
                if let Some(task) = worker.0.take()
                    && let Err(error) = task.await
                {
                    return Some((
                        Err(Error::Interrupted(format!(
                            "selective read worker: {error}"
                        ))),
                        (receiver, worker),
                    ));
                }
                None
            },
        )
    })
    .flatten()
    .boxed()
}
