use std::sync::Arc;

use object_store::{ObjectStore, memory::InMemory, path::Path};

use super::auxiliary_cache::{AuxiliaryCache, FileSummaryKey};
use crate::data_file::{
    DataStore,
    data_store::StoreIdentity,
    row_set::{FileRowSet, FileRowSetKind},
};

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
#[tokio::test]
async fn one_path_in_two_stores_is_two_entries() {
    let cache = AuxiliaryCache::new(1 << 20);
    let first = DataStore::new(Arc::new(InMemory::new()));
    let second = DataStore::new(Arc::new(InMemory::new()));
    let path = Path::from("shared-name.parquet");

    cache.insert_summary(&first, &key(&path), &summary(vec![1, 2, 3]));

    assert!(cache.summary(&first, &key(&path)).await.is_some());
    assert!(cache.summary(&second, &key(&path)).await.is_none());
}

/// A value the cache could never hold is not admitted, rather than
/// admitted and immediately evicted.
#[tokio::test]
async fn a_summary_larger_than_the_cache_is_not_admitted() {
    let cache = AuxiliaryCache::new(4 * 1024);
    let store = DataStore::new(Arc::new(InMemory::new()));
    let path = Path::from("oversized.parquet");

    cache.insert_summary(&store, &key(&path), &fragmented(4_096));

    assert!(cache.summary(&store, &key(&path)).await.is_none());
    assert_eq!(cache.usage(), 0);
}

/// Settling to a smaller allowance than the cache was built with moves the
/// reported capacity and evicts to fit it — what an attach whose budget is
/// below the default does to a cache a read already warmed.
#[test]
fn resizing_down_evicts_to_the_new_capacity() {
    let cache = AuxiliaryCache::new(1 << 20);
    let store = DataStore::new(Arc::new(InMemory::new()));
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
#[tokio::test]
async fn resizing_up_keeps_resident_entries() {
    let cache = AuxiliaryCache::new(64 * 1024);
    let store = DataStore::new(Arc::new(InMemory::new()));
    let path = Path::from("kept.parquet");
    cache.insert_summary(&store, &key(&path), &fragmented(512));

    cache.resize(1 << 20);

    assert_eq!(cache.capacity(), 1 << 20);
    assert!(cache.summary(&store, &key(&path)).await.is_some());
}

/// The shape counts and summary bytes follow what is resident, and only
/// summaries: a footer entry contributes nothing.
#[test]
fn row_summary_occupancy_counts_each_shape_and_its_bytes() {
    let cache = AuxiliaryCache::new(1 << 20);
    let store = DataStore::new(Arc::new(InMemory::new()));
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

/// A value written to disk comes back as what was cached: a footer with
/// its page index, and each summary in its own shape.
#[test]
fn values_round_trip_through_their_disk_form() {
    use arrow::array::{Int64Array, RecordBatch};
    use foyer::Code;
    use parquet::{
        arrow::ArrowWriter,
        file::{
            metadata::{PageIndexPolicy, ParquetMetaDataReader},
            properties::WriterProperties,
        },
    };

    use super::auxiliary_cache::{AuxiliaryValue, Weighed};

    let batch =
        RecordBatch::try_from_iter([("id", Arc::new((0..1_000).collect::<Int64Array>()) as _)])
            .unwrap();
    let mut file = Vec::new();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100))
        .build();
    let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&bytes::Bytes::from(file))
        .unwrap();
    assert!(metadata.offset_index().is_some());

    let round_trip = |value: AuxiliaryValue| {
        let weighed = Weighed::from(value);
        let mut encoded = Vec::new();
        weighed.encode(&mut encoded).unwrap();
        Weighed::decode(&mut encoded.as_slice()).unwrap()
    };

    let footer = round_trip(AuxiliaryValue::Metadata(Arc::new(metadata.clone())));
    let AuxiliaryValue::Metadata(decoded) = &footer.value else {
        panic!("a footer decoded as a summary");
    };
    assert_eq!(decoded.num_row_groups(), 10);
    assert_eq!(decoded.file_metadata().num_rows(), 1_000);
    assert_eq!(
        decoded.offset_index().map(Vec::len),
        metadata.offset_index().map(Vec::len)
    );
    assert!(decoded.column_index().is_some());

    for rows in [
        summary((0..100).collect()),
        summary((0..2_000).map(|row| row * 3).collect()),
        fragmented(8),
    ] {
        let decoded = round_trip(AuxiliaryValue::Summary(Arc::clone(&rows)));
        let AuxiliaryValue::Summary(decoded) = &decoded.value else {
            panic!("a summary decoded as a footer");
        };
        assert_eq!(decoded.kind(), rows.kind());
        let requested: Vec<u64> = (0..6_000).collect();
        assert_eq!(decoded.matching(&requested), rows.matching(&requested));
    }
}

/// A durable store's identity is its location, the same in every process;
/// an in-memory store's is its own.
#[test]
fn durable_stores_are_named_by_location() {
    let root = std::env::temp_dir();
    let local = || -> Arc<dyn ObjectStore> {
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&root).unwrap())
    };
    assert_eq!(StoreIdentity::of(&local()), StoreIdentity::of(&local()));
    assert!(matches!(
        StoreIdentity::of(&local()),
        StoreIdentity::Durable(_)
    ));

    let memory: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    assert!(matches!(
        StoreIdentity::of(&memory),
        StoreIdentity::Ephemeral(_)
    ));
    assert_ne!(StoreIdentity::of(&memory), StoreIdentity::of(&memory));
}
