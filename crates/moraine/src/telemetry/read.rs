//! Logical read activity, separate from storage requests.

use std::{
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use crate::read_policy::CacheAdmission;

#[derive(Default)]
struct Activity {
    point_reads: AtomicU64,
    retaining_scans: AtomicU64,
    reuse_only_scans: AtomicU64,
    admitted_range_bytes: AtomicU64,
    bypassed_range_bytes: AtomicU64,
    decoded_records: AtomicU64,
    decoded_bytes: AtomicU64,
    reused_records: AtomicU64,
    reported_ms: AtomicU64,
}

static ACTIVITY: Activity = Activity {
    point_reads: AtomicU64::new(0),
    retaining_scans: AtomicU64::new(0),
    reuse_only_scans: AtomicU64::new(0),
    admitted_range_bytes: AtomicU64::new(0),
    bypassed_range_bytes: AtomicU64::new(0),
    decoded_records: AtomicU64::new(0),
    decoded_bytes: AtomicU64::new(0),
    reused_records: AtomicU64::new(0),
    reported_ms: AtomicU64::new(0),
};

pub(crate) fn point_read() {
    ACTIVITY.point_reads.fetch_add(1, Ordering::Relaxed);
    report();
}

pub(crate) fn scan_opened(admission: CacheAdmission) {
    let counter = if admission.retains() {
        &ACTIVITY.retaining_scans
    } else {
        &ACTIVITY.reuse_only_scans
    };
    counter.fetch_add(1, Ordering::Relaxed);
    report();
}

pub(crate) fn range_completed(admission: CacheAdmission, bytes: usize) {
    ACTIVITY.range_completed(admission, bytes);
    report();
}

impl Activity {
    fn range_completed(&self, admission: CacheAdmission, bytes: usize) {
        let counter = if admission.retains() {
            &self.admitted_range_bytes
        } else {
            &self.bypassed_range_bytes
        };
        counter.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

pub(crate) fn decoded_page(records: usize, bytes: usize) {
    ACTIVITY
        .decoded_records
        .fetch_add(records as u64, Ordering::Relaxed);
    ACTIVITY
        .decoded_bytes
        .fetch_add(bytes as u64, Ordering::Relaxed);
    report();
}

pub(crate) fn decoded_reused(records: usize) {
    ACTIVITY
        .reused_records
        .fetch_add(records as u64, Ordering::Relaxed);
    report();
}

/// Process totals, emitted at most once per ten seconds while work arrives.
pub(super) fn report() {
    static START: OnceLock<Instant> = OnceLock::new();
    let now = crate::telemetry::milliseconds(START.get_or_init(Instant::now).elapsed());
    let last = ACTIVITY.reported_ms.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 10_000
        || ACTIVITY
            .reported_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        return;
    }
    super::storage::report();
    tracing::info!(
        point_reads = ACTIVITY.point_reads.load(Ordering::Relaxed),
        retaining_scans = ACTIVITY.retaining_scans.load(Ordering::Relaxed),
        reuse_only_scans = ACTIVITY.reuse_only_scans.load(Ordering::Relaxed),
        admitted_parquet_range_bytes = ACTIVITY.admitted_range_bytes.load(Ordering::Relaxed),
        bypassed_parquet_range_bytes = ACTIVITY.bypassed_range_bytes.load(Ordering::Relaxed),
        publication_decoded_records = ACTIVITY.decoded_records.load(Ordering::Relaxed),
        publication_decoded_estimated_bytes = ACTIVITY.decoded_bytes.load(Ordering::Relaxed),
        publication_reused_records = ACTIVITY.reused_records.load(Ordering::Relaxed),
        "read cache activity: cumulative process totals"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_counts_bytes_without_counting_storage_requests() {
        let activity = Activity::default();
        activity.range_completed(CacheAdmission::Retain, 100);
        activity.range_completed(CacheAdmission::ReuseOnly, 200);
        assert_eq!(activity.admitted_range_bytes.load(Ordering::Relaxed), 100);
        assert_eq!(activity.bypassed_range_bytes.load(Ordering::Relaxed), 200);
        assert_eq!(activity.retaining_scans.load(Ordering::Relaxed), 0);
    }
}
