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

mod blocks {
    use bytes::Bytes;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::{Arc, InMemory, ObjectStore, Path};
    use crate::data_file::{DataStore, ParquetFile, auxiliary_cache::AuxiliaryCache};

    /// A file of `size` bytes whose contents are their own offsets, so an
    /// assembled range is checkable against what was asked for.
    async fn seeded(size: usize) -> (DataStore, Path, Vec<u8>) {
        let bytes: Vec<u8> = (0..size)
            .map(|offset| u8::try_from(offset % 251).unwrap_or(0))
            .collect();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("blocks.parquet");
        store
            .put(&path, PutPayload::from_bytes(Bytes::from(bytes.clone())))
            .await
            .unwrap();
        (DataStore::new(store), path, bytes)
    }

    fn file(store: &DataStore, path: &Path, size: usize) -> ParquetFile {
        ParquetFile::new(store.clone(), path.clone(), size as u64, 0)
    }

    /// A repeated range is served from the cache: the bytes match and the
    /// second read issues no object-store fetch.
    #[tokio::test]
    async fn a_repeated_range_is_served_without_a_second_fetch() {
        let (store, path, bytes) = seeded(300_000).await;
        let cache = AuxiliaryCache::new(4 << 20);
        let file = file(&store, &path, bytes.len());

        let first = cache.range(&file, 1_000..5_000).await.unwrap();
        assert_eq!(first.as_ref(), &bytes[1_000..5_000]);
        let fetched = file.metrics().tally().range_fetches;
        assert!(fetched > 0, "the first read should have fetched");

        let second = cache.range(&file, 1_000..5_000).await.unwrap();
        assert_eq!(second.as_ref(), &bytes[1_000..5_000]);
        assert_eq!(
            file.metrics().tally().range_fetches,
            fetched,
            "the second read should have fetched nothing"
        );
    }

    /// A multi-range read fetches only the ranges that missed, and serves
    /// the rest from the cache.
    #[tokio::test]
    async fn a_multi_range_read_fetches_only_what_missed() {
        let (store, path, bytes) = seeded(300_000).await;
        let cache = AuxiliaryCache::new(4 << 20);
        let file = file(&store, &path, bytes.len());

        cache.range(&file, 1_000..2_000).await.unwrap();
        let fetched = file.metrics().tally().ranges;

        let served = cache
            .ranges(&file, vec![1_000..2_000, 8_000..9_000])
            .await
            .unwrap();

        assert_eq!(served[0].as_ref(), &bytes[1_000..2_000]);
        assert_eq!(served[1].as_ref(), &bytes[8_000..9_000]);
        assert_eq!(
            file.metrics().tally().ranges,
            fetched + 1,
            "only the range that missed should have been fetched"
        );
    }

    /// A large range and one running to the file's end both come back
    /// exactly.
    #[tokio::test]
    async fn large_ranges_and_the_file_tail_come_back_exactly() {
        let (store, path, bytes) = seeded(300_000).await;
        let cache = AuxiliaryCache::new(4 << 20);
        let file = file(&store, &path, bytes.len());

        let spanning = cache.range(&file, 100..290_000).await.unwrap();
        assert_eq!(spanning.as_ref(), &bytes[100..290_000]);

        let tail = cache.range(&file, 299_000..300_000).await.unwrap();
        assert_eq!(tail.as_ref(), &bytes[299_000..300_000]);
    }

    /// Blocks leave before footers and summaries under pressure: the cache
    /// holds metadata whose value is not re-fetchable as cheaply.
    #[tokio::test]
    async fn blocks_are_evicted_before_summaries() {
        let (store, path, bytes) = seeded(2 << 20).await;
        let cache = AuxiliaryCache::new(512 * 1024);
        let file = file(&store, &path, bytes.len());
        let summary_key = super::key(&path);
        cache.insert_summary(&store, &summary_key, &super::fragmented(1_024));

        // Enough distinct ranges to overrun the allowance several times.
        for block in 0..16 {
            let start = block * 128 * 1024;
            cache.range(&file, start..start + 64 * 1024).await.unwrap();
        }

        assert!(
            cache.summary(&store, &summary_key).await.is_some(),
            "the summary should outlive the blocks that flooded the cache"
        );
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
