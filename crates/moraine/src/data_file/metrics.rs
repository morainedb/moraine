//! What a commit's data-file reads cost, and the bound on the blocking
//! workers that encode them. A tally is per commit; the encoding permits
//! are process-wide.

use std::{
    num::NonZero,
    sync::{
        Arc, LazyLock, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::{
    data_file::usize_as_u64,
    error::{Error, Result},
    telemetry::nanoseconds,
};

/// The fewest and most blocking workers Arrow-to-index encoding may hold
/// at once, whatever the machine's core count.
const INDEX_ENCODING_CONCURRENCY_BOUNDS: (usize, usize) = (4, 32);

/// Process-wide limit for Arrow-to-index encoding on blocking workers: the
/// machine's core count within the bounds above, settled once per process.
pub(super) fn index_encoding_concurrency() -> usize {
    static CONCURRENCY: OnceLock<usize> = OnceLock::new();

    *CONCURRENCY.get_or_init(|| {
        let (floor, ceiling) = INDEX_ENCODING_CONCURRENCY_BOUNDS;
        std::thread::available_parallelism()
            .map_or(floor, NonZero::get)
            .clamp(floor, ceiling)
    })
}

static INDEX_ENCODING_PERMITS: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(index_encoding_concurrency())));

/// Data-store reads issued on behalf of one catalog handle, across every
/// scoped read that reports to it.
#[derive(Debug, Default)]
pub(crate) struct DataStoreCounters {
    gets: AtomicU64,
    bytes: AtomicU64,
}

impl DataStoreCounters {
    /// Byte ranges requested; a store may coalesce adjacent ranges into
    /// fewer requests.
    pub(crate) fn gets(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// Per-commit work performed by scoped Parquet and inline index reads.
#[derive(Default)]
pub(crate) struct ScopedReadMetrics {
    /// The handle's running totals, when the read is attributed to one.
    upstream: Option<Arc<DataStoreCounters>>,
    metadata_hits: AtomicU64,
    metadata_misses: AtomicU64,
    range_fetches: AtomicU64,
    ranges: AtomicU64,
    range_bytes: AtomicU64,
    range_nanoseconds: AtomicU64,
    encode_nanoseconds: AtomicU64,
    decode_nanoseconds: AtomicU64,
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
    pub(crate) decode_duration: Duration,
    pub(crate) parquet_files: u64,
    pub(crate) inline_chunks: u64,
    pub(crate) arrow_batches: u64,
}

impl ScopedReadMetrics {
    pub(super) fn decoded(&self, duration: Duration) {
        self.decode_nanoseconds
            .fetch_add(nanoseconds(duration), Ordering::Relaxed);
    }
    /// A tally whose reads also count towards `upstream`.
    pub(crate) fn reporting_to(upstream: Arc<DataStoreCounters>) -> Self {
        Self {
            upstream: Some(upstream),
            ..Self::default()
        }
    }

    pub(super) fn metadata_hit(&self) {
        self.metadata_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn metadata_miss(&self) {
        self.metadata_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn range_read(&self, ranges: usize, bytes: u64, duration: Duration) {
        self.range_fetches.fetch_add(1, Ordering::Relaxed);
        self.ranges
            .fetch_add(usize_as_u64(ranges), Ordering::Relaxed);
        self.range_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.range_nanoseconds
            .fetch_add(nanoseconds(duration), Ordering::Relaxed);
        if let Some(upstream) = &self.upstream {
            upstream
                .gets
                .fetch_add(usize_as_u64(ranges), Ordering::Relaxed);
            upstream.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
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
            decode_duration: Duration::from_nanos(self.decode_nanoseconds.load(Ordering::Relaxed)),
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
    // The permit is taken on a task of its own: the limiter is fair, so a
    // permit it frees is reserved for the oldest waiter until that waiter
    // is polled, and a caller parked by backpressure would hold that
    // reservation while every other encoder in the process starved. A task
    // is always polled, so the permit is taken and released whatever
    // becomes of the caller.
    let unit = tokio::spawn(async move {
        // Process-wide, so one catalog's encoding starves every other one's;
        // named here because a caller parked on it is otherwise silent.
        let permit = crate::telemetry::reporting_phase(
            "index-encoding-permit",
            Arc::clone(&INDEX_ENCODING_PERMITS).acquire_owned(),
        )
        .await
        .map_err(|_| {
            Error::Interrupted("index encoding limiter stopped before work began".to_owned())
        })?;
        // The permit is held until this blocking work returns, and a blocking
        // task cannot be cancelled: work that never returns removes a permit
        // from the process for good. Reporting the hold names the holder,
        // which the waiters above cannot.
        crate::telemetry::reporting_phase(
            "index-encoding-hold",
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                work()
            }),
        )
        .await
        .map_err(|error| Error::Interrupted(format!("index encoding worker stopped: {error}")))?
    });
    unit.await
        .map_err(|error| Error::Interrupted(format!("index encoding unit stopped: {error}")))?
}

/// Process-wide counts of what published row summaries have done, so a
/// deployment can tell "none published yet" from "published and refused".
static SIDECAR_HITS: AtomicU64 = AtomicU64::new(0);
static SIDECAR_MISSES: AtomicU64 = AtomicU64::new(0);
static SIDECAR_REFUSED: AtomicU64 = AtomicU64::new(0);
static SIDECAR_PUBLISHED: AtomicU64 = AtomicU64::new(0);
static SIDECAR_PUBLISH_FAILURES: AtomicU64 = AtomicU64::new(0);

/// What published row summaries have done for this process.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct SidecarTally {
    /// Summaries a published sidecar answered, each one a row-id column
    /// this process did not read.
    pub hits: u64,
    /// Summaries derived because no sidecar could be read, whether none
    /// was published or the store would not serve it.
    pub misses: u64,
    /// Sidecars read and then refused: a header naming another file, a
    /// version this build does not know, or bytes that would not decode.
    /// Every one of these was also a miss.
    pub refused: u64,
    /// Summaries this process published.
    pub published: u64,
    /// Publishes that failed, costing nothing but a later derivation.
    pub publish_failures: u64,
}

/// [`SidecarTally`] for this process, across every attached store.
#[must_use]
pub fn sidecar_tally() -> SidecarTally {
    SidecarTally {
        hits: SIDECAR_HITS.load(Ordering::Relaxed),
        misses: SIDECAR_MISSES.load(Ordering::Relaxed),
        refused: SIDECAR_REFUSED.load(Ordering::Relaxed),
        published: SIDECAR_PUBLISHED.load(Ordering::Relaxed),
        publish_failures: SIDECAR_PUBLISH_FAILURES.load(Ordering::Relaxed),
    }
}

/// Records a sidecar that answered.
pub(crate) fn sidecar_hit() {
    SIDECAR_HITS.fetch_add(1, Ordering::Relaxed);
}

/// Records a summary derived because no sidecar answered.
pub(crate) fn sidecar_miss() {
    SIDECAR_MISSES.fetch_add(1, Ordering::Relaxed);
}

/// Records a sidecar read and refused, which is also a miss.
pub(crate) fn sidecar_refused() {
    SIDECAR_REFUSED.fetch_add(1, Ordering::Relaxed);
}

/// Records the outcome of one publish.
pub(crate) fn sidecar_published(succeeded: bool) {
    if succeeded {
        SIDECAR_PUBLISHED.fetch_add(1, Ordering::Relaxed);
    } else {
        SIDECAR_PUBLISH_FAILURES.fetch_add(1, Ordering::Relaxed);
    }
}
