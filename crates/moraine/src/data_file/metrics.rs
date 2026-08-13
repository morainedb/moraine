//! What a commit's data-file reads cost, and the bound on the blocking
//! workers that encode them. A tally is per commit; the encoding permits
//! are process-wide.

use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::{
    error::{Error, Result},
    telemetry::nanoseconds,
};

/// Process-wide limit for Arrow-to-index encoding on blocking workers.
pub(super) const INDEX_ENCODING_CONCURRENCY: usize = 8;

static INDEX_ENCODING_PERMITS: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(INDEX_ENCODING_CONCURRENCY)));

/// Per-commit work performed by scoped Parquet and inline index reads.
#[derive(Default)]
pub(crate) struct ScopedReadMetrics {
    metadata_hits: AtomicU64,
    metadata_misses: AtomicU64,
    range_fetches: AtomicU64,
    ranges: AtomicU64,
    range_bytes: AtomicU64,
    range_nanoseconds: AtomicU64,
    encode_nanoseconds: AtomicU64,
    parquet_files: AtomicU64,
    inline_chunks: AtomicU64,
    arrow_batches: AtomicU64,
}

/// One consistent sample of [`ScopedReadMetrics`].
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ScopedReadTally {
    pub(crate) metadata_hits: u64,
    pub(crate) metadata_misses: u64,
    pub(crate) range_fetches: u64,
    pub(crate) ranges: u64,
    pub(crate) range_bytes: u64,
    pub(crate) range_duration: Duration,
    pub(crate) encode_duration: Duration,
    pub(crate) parquet_files: u64,
    pub(crate) inline_chunks: u64,
    pub(crate) arrow_batches: u64,
}

impl ScopedReadMetrics {
    pub(super) fn metadata_hit(&self) {
        self.metadata_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn metadata_miss(&self) {
        self.metadata_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn range_read(&self, ranges: usize, bytes: u64, duration: Duration) {
        self.range_fetches.fetch_add(1, Ordering::Relaxed);
        self.ranges
            .fetch_add(u64::try_from(ranges).unwrap_or(u64::MAX), Ordering::Relaxed);
        self.range_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.range_nanoseconds
            .fetch_add(nanoseconds(duration), Ordering::Relaxed);
    }

    pub(crate) fn encoded(&self, duration: Duration) {
        self.encode_nanoseconds
            .fetch_add(nanoseconds(duration), Ordering::Relaxed);
    }

    pub(super) fn parquet_file(&self) {
        self.parquet_files.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inline_chunk(&self) {
        self.inline_chunks.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn arrow_batch(&self) {
        self.arrow_batches.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn tally(&self) -> ScopedReadTally {
        ScopedReadTally {
            metadata_hits: self.metadata_hits.load(Ordering::Relaxed),
            metadata_misses: self.metadata_misses.load(Ordering::Relaxed),
            range_fetches: self.range_fetches.load(Ordering::Relaxed),
            ranges: self.ranges.load(Ordering::Relaxed),
            range_bytes: self.range_bytes.load(Ordering::Relaxed),
            range_duration: Duration::from_nanos(self.range_nanoseconds.load(Ordering::Relaxed)),
            encode_duration: Duration::from_nanos(self.encode_nanoseconds.load(Ordering::Relaxed)),
            parquet_files: self.parquet_files.load(Ordering::Relaxed),
            inline_chunks: self.inline_chunks.load(Ordering::Relaxed),
            arrow_batches: self.arrow_batches.load(Ordering::Relaxed),
        }
    }
}

/// Runs Arrow-to-index encoding off the async executor under one shared bound.
pub(crate) async fn run_bounded_index_encoding<T, F>(work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    let permit = Arc::clone(&INDEX_ENCODING_PERMITS)
        .acquire_owned()
        .await
        .map_err(|_| {
            Error::Interrupted("index encoding limiter stopped before work began".to_owned())
        })?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|error| Error::Interrupted(format!("index encoding worker stopped: {error}")))?
}
