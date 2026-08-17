use std::sync::Arc;

use object_store::{ObjectStore, memory::InMemory, path::Path};

use super::auxiliary_cache::{AuxiliaryCache, FileSummaryKey, store_id};
use crate::data_file::row_set::{FileRowSet, FileRowSetKind};

fn summary(row_ids: Vec<u64>) -> Arc<FileRowSet> {
    Arc::new(FileRowSet::from_sorted(row_ids).unwrap())
}

/// A summary large enough to matter against a small allowance, built
/// fragmented so it takes the sorted form rather than collapsing to a range.
fn fragmented(count: u64) -> Arc<FileRowSet> {
    summary((0..count).map(|row| row * 2).collect())
}

fn key(path: &Path) -> FileSummaryKey<'_> {
    FileSummaryKey {
        table_id: 1,
        data_file_id: 1,
        path,
        file_size: 100,
    }
}

/// Two stores holding the same path keep separate entries.
#[test]
fn one_path_in_two_stores_is_two_entries() {
    let cache = AuxiliaryCache::new(1 << 20);
    let first: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let second: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("shared-name.parquet");

    cache.insert_summary(&first, &key(&path), &summary(vec![1, 2, 3]));

    assert!(cache.summary(&first, &key(&path)).is_some());
    assert!(cache.summary(&second, &key(&path)).is_none());
}

/// An address a dropped store leaves behind never resolves to its identity.
#[test]
fn a_dropped_store_does_not_lend_its_identity() {
    let first: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first_id = store_id(&first);
    drop(first);

    let mut seen = Vec::new();
    for _ in 0..64 {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seen.push(store_id(&store));
    }

    assert!(
        !seen.contains(&first_id),
        "a later store reused the dropped store's identity"
    );
}

/// One store keeps one identity across lookups.
#[test]
fn a_live_store_keeps_one_identity() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    assert_eq!(store_id(&store), store_id(&store));
}

/// A value the cache could never hold is not admitted, rather than
/// admitted and immediately evicted.
#[test]
fn a_summary_larger_than_the_cache_is_not_admitted() {
    let cache = AuxiliaryCache::new(4 * 1024);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("oversized.parquet");

    cache.insert_summary(&store, &key(&path), &fragmented(4_096));

    assert!(cache.summary(&store, &key(&path)).is_none());
    assert_eq!(cache.usage(), 0);
}

/// Settling to a smaller allowance than the cache was built with moves the
/// reported capacity and evicts to fit it — what an attach whose budget is
/// below the default does to a cache a read already warmed.
#[test]
fn resizing_down_evicts_to_the_new_capacity() {
    let cache = AuxiliaryCache::new(1 << 20);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let paths: Vec<Path> = (0..16)
        .map(|index| Path::from(format!("resize-{index}.parquet")))
        .collect();
    for path in &paths {
        cache.insert_summary(&store, &key(path), &fragmented(1_024));
    }
    let filled = cache.usage();
    assert!(filled > 32 * 1024, "expected a filled cache, got {filled}");

    cache.resize(16 * 1024);

    assert_eq!(cache.capacity(), 16 * 1024);
    assert!(
        cache.usage() <= 16 * 1024,
        "usage {} exceeds the new capacity",
        cache.usage()
    );
}

/// Settling to a larger allowance keeps what is already resident.
#[test]
fn resizing_up_keeps_resident_entries() {
    let cache = AuxiliaryCache::new(64 * 1024);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("kept.parquet");
    cache.insert_summary(&store, &key(&path), &fragmented(512));

    cache.resize(1 << 20);

    assert_eq!(cache.capacity(), 1 << 20);
    assert!(cache.summary(&store, &key(&path)).is_some());
}

/// The shape counts and summary bytes follow what is resident, and only
/// summaries: a footer entry contributes nothing.
#[test]
fn row_summary_occupancy_counts_each_shape_and_its_bytes() {
    let cache = AuxiliaryCache::new(1 << 20);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let range = summary((0..100).collect());
    let roaring = summary((0..2_000).map(|row| row * 3).collect());
    let sorted = fragmented(8);
    let range_path = Path::from("range.parquet");
    let roaring_path = Path::from("roaring.parquet");
    let sorted_paths: Vec<Path> = (0..3)
        .map(|index| Path::from(format!("sorted-{index}.parquet")))
        .collect();

    assert_eq!(range.kind(), FileRowSetKind::Range);
    assert_eq!(roaring.kind(), FileRowSetKind::Roaring);
    assert_eq!(sorted.kind(), FileRowSetKind::Sorted);

    cache.insert_summary(&store, &key(&range_path), &range);
    cache.insert_summary(&store, &key(&roaring_path), &roaring);
    for path in &sorted_paths {
        cache.insert_summary(&store, &key(path), &sorted);
    }

    let occupancy = cache.row_summaries();
    assert_eq!(
        (occupancy.range, occupancy.roaring, occupancy.sorted),
        (1, 1, 3)
    );
    assert_eq!(
        occupancy.bytes,
        range.estimated_bytes() + roaring.estimated_bytes() + 3 * sorted.estimated_bytes()
    );

    // Shrinking to nothing evicts everything, and the counts follow.
    cache.resize(1);
    let emptied = cache.row_summaries();
    assert_eq!(
        (
            emptied.range,
            emptied.roaring,
            emptied.sorted,
            emptied.bytes
        ),
        (0, 0, 0, 0)
    );
}
