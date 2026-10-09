//! Bounded, cancellable prefetch of projected Parquet batches.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow::array::RecordBatch;
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use tokio::{sync::mpsc, task::JoinHandle};

use super::{ParquetFile, RowIdSource, RowPositions, ScopedRows, scoped_read_row_stream};
use crate::error::{Error, Result};

/// Prefetch workers one cursor may run at once, per cursor rather than
/// per process.
///
/// A scoped read spends almost all of its time waiting on the store, so
/// the ceiling follows the requests an object store will serve at once
/// rather than the cores available to decode them. The floor keeps a
/// two-core machine overlapping at all; the ceiling stands until a
/// store-level admission bound replaces it.
pub(crate) fn read_worker_limit() -> usize {
    std::thread::available_parallelism().map_or(2, |cores| cores.get().clamp(2, 8))
}

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

/// Runs `work` as one accounted read worker, so the cursor can report what
/// it ran concurrently. Readers are bounded by the cursor's own
/// parallelism: a process-wide gate here would put one cursor's decoding
/// between another's and its first byte range.
async fn tracked<T>(
    workers: &Arc<ReadWorkers>,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    let active = workers.active.fetch_add(1, Ordering::Relaxed) + 1;
    workers.peak.fetch_max(active, Ordering::Relaxed);
    let _active = ActiveWorker(workers.clone());
    // Named because a read that never returns is otherwise silent.
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
                let mut batches = tracked(
                    &workers,
                    scoped_read_row_stream(file, &requested, ScopedRows::At(&positions), source),
                )
                .await?;
                loop {
                    // Reserve before decoding: at most one queued batch per worker.
                    let Ok(slot) = sender.reserve().await else {
                        return Ok(());
                    };
                    let Some(batch) = tracked(&workers, batches.try_next()).await? else {
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
