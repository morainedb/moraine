use std::sync::Arc;

use object_store::{ObjectStore, memory::InMemory, path::Path};

use super::auxiliary_cache::{AuxiliaryCache, FileSummaryKey};
use crate::data_file::{
    DataStore,
    data_store::StoreIdentity,
    row_set::{FileRowSet, FileRowSetKind, PositionedRowSet, RowOrder},
};

fn summary(row_ids: Vec<u64>) -> Arc<PositionedRowSet> {
    Arc::new(PositionedRowSet {
        rows: FileRowSet::from_sorted(row_ids).unwrap(),
        order: RowOrder::Ascending,
    })
}

/// A summary large enough to matter against a small allowance, built
/// fragmented so it takes the sorted form rather than collapsing to a range.
fn fragmented(count: u64) -> Arc<PositionedRowSet> {
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

    assert_eq!(range.rows.kind(), FileRowSetKind::Range);
    assert_eq!(roaring.rows.kind(), FileRowSetKind::Roaring);
    assert_eq!(sorted.rows.kind(), FileRowSetKind::Sorted);

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
        assert_eq!(decoded.rows.kind(), rows.rows.kind());
        let requested: Vec<u64> = (0..6_000).collect();
        assert_eq!(
            decoded.rows.matching(&requested),
            rows.rows.matching(&requested)
        );
    }
}

/// A permuted summary — the shape an UPDATE's rewritten rows take — round
/// trips through its disk form with the same set and the same answers to
/// every requested position.
#[test]
fn a_permuted_summary_round_trips_through_its_disk_form() {
    use foyer::Code;

    use super::auxiliary_cache::{AuxiliaryValue, Weighed};

    for file_order in [
        // Sorted-shaped: fragmented ids out of file order.
        vec![30 << 16, 10 << 16, 20 << 16],
        // Roaring-shaped: sparse ids out of file order.
        {
            let mut ids: Vec<u64> = (0..10_000).map(|row| row * 10).collect();
            ids.swap(0, 1);
            ids
        },
    ] {
        let positioned = PositionedRowSet::from_file_order(file_order.clone()).unwrap();
        assert!(matches!(positioned.order, RowOrder::Permuted(_)));
        let expected_positions = positioned.positions_of(&file_order);

        let weighed = Weighed::from(AuxiliaryValue::Summary(Arc::new(positioned)));
        let mut encoded = Vec::new();
        weighed.encode(&mut encoded).unwrap();
        let decoded = Weighed::decode(&mut encoded.as_slice()).unwrap();
        let AuxiliaryValue::Summary(decoded) = &decoded.value else {
            panic!("a summary decoded as something else");
        };

        assert!(matches!(decoded.order, RowOrder::Permuted(_)));
        assert_eq!(decoded.positions_of(&file_order), expected_positions);
    }
}

/// A permuted set whose sorted ids happen to be contiguous must not
/// collapse to a bare range on disk: a range tag carries no permutation,
/// so decoding it would answer positions by arithmetic — silently wrong
/// for a file that rewrote a contiguous id range out of order.
#[test]
fn a_contiguous_permuted_summary_keeps_its_permutation_across_a_round_trip() {
    use foyer::Code;

    use super::auxiliary_cache::{AuxiliaryValue, Weighed};

    // ids 100..110, filed as two ascending runs: [105..110, 100..105].
    let file_order: Vec<u64> = (105..110).chain(100..105).collect();
    let positioned = PositionedRowSet::from_file_order(file_order.clone()).unwrap();
    assert!(matches!(positioned.order, RowOrder::Permuted(_)));
    let expected_positions = positioned.positions_of(&[100, 105]);

    let weighed = Weighed::from(AuxiliaryValue::Summary(Arc::new(positioned)));
    let mut encoded = Vec::new();
    weighed.encode(&mut encoded).unwrap();
    let decoded = Weighed::decode(&mut encoded.as_slice()).unwrap();
    let AuxiliaryValue::Summary(decoded) = &decoded.value else {
        panic!("a summary decoded as something else");
    };

    assert_eq!(decoded.positions_of(&[100, 105]), expected_positions);
}

/// A cached summary from before positions existed carries no order verdict:
/// decoding it is a parse failure, not a guessed position. Tag 1 is included
/// because pre-upgrade encoders wrote it for a contiguous-but-permuted file,
/// discarding the order a decoder would otherwise have to invent.
#[test]
fn a_legacy_summary_tag_fails_to_decode() {
    use foyer::Code;

    use super::auxiliary_cache::Weighed;

    for legacy_tag in [2_u8, 3_u8] {
        let encoded = vec![legacy_tag];
        assert!(
            Weighed::decode(&mut encoded.as_slice()).is_err(),
            "tag {legacy_tag} should not decode"
        );
    }

    // Tag 1 carries a body (a start/end pair), unlike 2 and 3 above: a
    // pre-upgrade encoder wrote it for a contiguous-but-permuted file,
    // discarding the permutation, so it must fail even with a well-formed
    // body rather than silently answer as an ascending range.
    let mut range_bodied = vec![1_u8];
    range_bodied.extend_from_slice(&0_u64.to_le_bytes());
    range_bodied.extend_from_slice(&99_u64.to_le_bytes());
    assert!(
        Weighed::decode(&mut range_bodied.as_slice()).is_err(),
        "tag 1 with a range body should not decode"
    );
}

/// The tag the current encoder writes for an ascending range round-trips,
/// distinct from the retired tag 1 above.
#[test]
fn a_range_ascending_summary_round_trips_through_its_disk_form() {
    use foyer::Code;

    use super::auxiliary_cache::{AuxiliaryValue, Weighed};

    let positioned = summary((0..1_000).collect());
    assert_eq!(positioned.rows.kind(), FileRowSetKind::Range);

    let weighed = Weighed::from(AuxiliaryValue::Summary(Arc::clone(&positioned)));
    let mut encoded = Vec::new();
    weighed.encode(&mut encoded).unwrap();
    assert_eq!(
        encoded.first().copied(),
        Some(12_u8),
        "expected tag 12 (RANGE_ASCENDING)"
    );

    let decoded = Weighed::decode(&mut encoded.as_slice()).unwrap();
    let AuxiliaryValue::Summary(decoded) = &decoded.value else {
        panic!("a summary decoded as something else");
    };
    assert_eq!(decoded.rows.kind(), FileRowSetKind::Range);
    assert!(matches!(decoded.order, RowOrder::Ascending));
}

/// A delete file's positions are a different kind from a summary: their
/// tags stay valid even though a summary predating positions cannot decode.
#[test]
fn delete_position_tags_still_decode() {
    use foyer::Code;

    use super::auxiliary_cache::{AuxiliaryValue, Weighed};

    for (positions, expected_kind) in [
        (
            FileRowSet::from_sorted((0..10_000).collect()).unwrap(),
            FileRowSetKind::Range,
        ),
        (
            FileRowSet::from_sorted((0..10_000).map(|row| row * 10).collect()).unwrap(),
            FileRowSetKind::Roaring,
        ),
        (
            FileRowSet::from_sorted((0..1_000).map(|row| row << 16).collect()).unwrap(),
            FileRowSetKind::Sorted,
        ),
    ] {
        assert_eq!(positions.kind(), expected_kind);
        let requested: Vec<u64> = (0..20_000).collect();
        let expected = positions.matching(&requested);

        let weighed = Weighed::from(AuxiliaryValue::DeletePositions(Arc::new(positions)));
        let mut encoded = Vec::new();
        weighed.encode(&mut encoded).unwrap();
        let decoded = Weighed::decode(&mut encoded.as_slice()).unwrap();
        let AuxiliaryValue::DeletePositions(decoded) = &decoded.value else {
            panic!("delete positions decoded as something else");
        };

        assert_eq!(decoded.matching(&requested), expected);
    }
}

mod proptests {
    use std::collections::HashSet;

    use foyer::Code;
    use proptest::prelude::*;

    use super::{Arc, PositionedRowSet, RowOrder};
    use crate::data_file::auxiliary_cache::{AuxiliaryValue, Weighed};

    /// Deduplicates while keeping each id's first-seen position, so the
    /// result is a valid (possibly unsorted) file order.
    fn unique_preserving_order(ids: Vec<u16>) -> Vec<u64> {
        let mut seen = HashSet::new();
        ids.into_iter()
            .map(u64::from)
            .filter(|id| seen.insert(*id))
            .collect()
    }

    /// A contiguous id range rotated by a generated offset — the shape
    /// that collapses to `FileRowSet::Range` when sorted, exercising the
    /// range/permutation interaction directly rather than leaving it to
    /// chance.
    fn contiguous_permuted_file_order() -> impl Strategy<Value = Vec<u64>> {
        (any::<u16>(), 2_u16..64).prop_flat_map(|(start, len)| {
            (0..len).prop_map(move |offset| {
                let mut ids: Vec<u64> = (0..len).map(|i| u64::from(start) + u64::from(i)).collect();
                ids.rotate_left(usize::from(offset));
                ids
            })
        })
    }

    fn file_order_strategy() -> impl Strategy<Value = Vec<u64>> {
        prop_oneof![
            proptest::collection::vec(any::<u16>(), 1..64).prop_map(unique_preserving_order),
            contiguous_permuted_file_order(),
        ]
    }

    proptest! {
        /// Encoding then decoding a summary built from an arbitrary file
        /// order preserves its order kind, membership, and every position.
        #[test]
        fn positioned_row_set_round_trips_through_its_disk_form(
            file_order in file_order_strategy(),
        ) {
            prop_assume!(!file_order.is_empty());

            let positioned = PositionedRowSet::from_file_order(file_order.clone()).unwrap();
            let was_ascending = matches!(positioned.order, RowOrder::Ascending);
            let expected_positions = positioned.positions_of(&file_order);

            let weighed = Weighed::from(AuxiliaryValue::Summary(Arc::new(positioned)));
            let mut encoded = Vec::new();
            weighed.encode(&mut encoded).unwrap();
            let decoded = Weighed::decode(&mut encoded.as_slice()).unwrap();
            let AuxiliaryValue::Summary(decoded) = &decoded.value else {
                panic!("a summary decoded as something else");
            };

            prop_assert_eq!(matches!(decoded.order, RowOrder::Ascending), was_ascending);
            prop_assert_eq!(decoded.positions_of(&file_order), expected_positions);
        }
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
