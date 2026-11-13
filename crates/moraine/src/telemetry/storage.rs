//! Object-store calls below Moraine caches, including each explicit retry.

use std::sync::atomic::{AtomicU64, Ordering};

use object_store::{GetOptions, GetResult, ObjectStore, path::Path};

#[derive(Default)]
struct Counts {
    attempts: AtomicU64,
    conditional: AtomicU64,
    unchanged: AtomicU64,
    missing: AtomicU64,
    errors: AtomicU64,
}

static COUNTS: Counts = Counts {
    attempts: AtomicU64::new(0),
    conditional: AtomicU64::new(0),
    unchanged: AtomicU64::new(0),
    missing: AtomicU64::new(0),
    errors: AtomicU64::new(0),
};

impl Counts {
    fn complete(&self, conditional: bool, result: &object_store::Result<GetResult>) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        self.conditional
            .fetch_add(u64::from(conditional), Ordering::Relaxed);
        let counter = match result {
            Err(object_store::Error::NotModified { .. }) => &self.unchanged,
            Err(object_store::Error::NotFound { .. }) => &self.missing,
            Err(_) => &self.errors,
            Ok(_) => return,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) async fn get(
    store: &dyn ObjectStore,
    path: &Path,
    options: GetOptions,
) -> object_store::Result<GetResult> {
    let conditional = options.if_none_match.is_some()
        || options.if_modified_since.is_some()
        || options.if_match.is_some()
        || options.if_unmodified_since.is_some();
    let started = std::time::Instant::now();
    let result = store.get_opts(path, options).await;
    COUNTS.complete(conditional, &result);
    tracing::debug!(store = %store, %path, conditional,
        unchanged = matches!(&result, Err(object_store::Error::NotModified { .. })),
        missing = matches!(&result, Err(object_store::Error::NotFound { .. })),
        elapsed_us = started.elapsed().as_micros(), "object-store GET attempt");
    super::read::report();
    result
}

static RANGE_BATCHES: AtomicU64 = AtomicU64::new(0);
static RANGE_BYTES: AtomicU64 = AtomicU64::new(0);

pub(crate) async fn ranges(
    store: &dyn ObjectStore,
    path: &Path,
    ranges: &[std::ops::Range<u64>],
) -> object_store::Result<Vec<bytes::Bytes>> {
    RANGE_BATCHES.fetch_add(1, Ordering::Relaxed);
    let result = store.get_ranges(path, ranges).await;
    if let Ok(bytes) = &result {
        RANGE_BYTES.fetch_add(
            bytes.iter().map(|bytes| bytes.len() as u64).sum::<u64>(),
            Ordering::Relaxed,
        );
    }
    tracing::debug!(store = %store, %path, ranges = ranges.len(), failed = result.is_err(), "object-store range batch");
    super::read::report();
    result
}

pub(super) fn report() {
    tracing::info!(
        range_batches = RANGE_BATCHES.load(Ordering::Relaxed),
        range_batch_bytes = RANGE_BYTES.load(Ordering::Relaxed),
        attempts = COUNTS.attempts.load(Ordering::Relaxed),
        conditional = COUNTS.conditional.load(Ordering::Relaxed),
        unchanged = COUNTS.unchanged.load(Ordering::Relaxed),
        missing = COUNTS.missing.load(Ordering::Relaxed),
        errors = COUNTS.errors.load(Ordering::Relaxed),
        "object-store GET activity: cumulative process totals"
    );
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, memory::InMemory};

    use super::*;

    #[tokio::test]
    async fn unchanged_and_missing_are_distinct_storage_outcomes() {
        let counts = Counts::default();
        let store = InMemory::new();
        let path = Path::from("manifest.boundary");
        let missing = store.get(&path).await;
        counts.complete(false, &missing);
        let written = store.put(&path, "boundary".into()).await.unwrap();
        let unchanged = store
            .get_opts(
                &path,
                GetOptions {
                    if_none_match: written.e_tag,
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(
            unchanged,
            Err(object_store::Error::NotModified { .. })
        ));
        counts.complete(true, &unchanged);
        let present = store.get(&path).await;
        counts.complete(false, &present);
        assert_eq!(counts.attempts.load(Ordering::Relaxed), 3);
        assert_eq!(counts.conditional.load(Ordering::Relaxed), 1);
        assert_eq!(counts.unchanged.load(Ordering::Relaxed), 1);
        assert_eq!(counts.missing.load(Ordering::Relaxed), 1);
        assert_eq!(counts.errors.load(Ordering::Relaxed), 0);
    }
}
