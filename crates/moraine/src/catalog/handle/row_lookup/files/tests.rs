use std::{cell::Cell, sync::Arc};

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use proptest::prelude::*;

use super::{DenseRanges, FIRST_RETRY_SKIP};
use crate::{Catalog, CatalogOptions, ColumnDef, DataFile, DataFileId, DataStore, TableId};

/// Writes `rows` values with no row-id column to `path`, returning the
/// file and footer sizes the catalog records.
async fn write_dense_file(store: &InMemory, path: &str, rows: u64) -> (u64, u64) {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let values: Vec<i64> = (0..i64::try_from(rows).unwrap()).collect();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap();

    let mut buffer = Vec::new();
    {
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let footer_offset = buffer.len() - 8;
    let footer_size = u64::from(u32::from_le_bytes(
        buffer[footer_offset..footer_offset + 4].try_into().unwrap(),
    ));
    let file_size = u64::try_from(buffer.len()).unwrap();
    store.put(&Path::from(path), buffer.into()).await.unwrap();

    (file_size, footer_size)
}

/// A dense file of `rows` rows named by `ordinal`, written under the
/// `main/t/` table directory.
async fn dense_file(data: &InMemory, ordinal: u64, rows: u64) -> DataFile {
    let name = format!("data-{ordinal}.parquet");
    let (file_size_bytes, footer_size) =
        write_dense_file(data, &format!("main/t/{name}"), rows).await;

    DataFile {
        path: name,
        path_is_relative: true,
        file_format: "parquet".into(),
        record_count: rows,
        file_size_bytes,
        footer_size,
        encryption_key: None,
        partition_values: vec![],
        column_stats: vec![],
    }
}

/// Creates table `t` holding `files`, returning their ids in registration
/// order.
async fn table_with(catalog: &Catalog, files: Vec<DataFile>) -> (TableId, Vec<DataFileId>) {
    let created = Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            let table = tx.create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    ..Default::default()
                }],
            )?;
            let mut ids = Vec::new();
            for file in files.clone() {
                ids.push(tx.register_data_file(table, file, &[])?);
            }
            created.set(Some((table, ids)));
            Ok(())
        })
        .await
        .unwrap();

    created.take().unwrap()
}

/// A warm table of four dense files of three rows each, and the one data
/// store its directory was built against.
struct WarmTable {
    catalog: Catalog,
    data: Arc<InMemory>,
    store: DataStore,
    table: TableId,
    ids: Vec<DataFileId>,
}

impl WarmTable {
    async fn locate(&self, row_ids: Vec<u64>) -> Vec<crate::catalog::FileRowCandidate> {
        self.catalog
            .locate_row_ids(Some(self.store.clone()), "", self.table, row_ids)
            .await
            .unwrap()
    }
}

async fn warm_table() -> WarmTable {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let data = Arc::new(InMemory::new());
    let mut files = Vec::new();
    for ordinal in 0..4 {
        files.push(dense_file(&data, ordinal, 3).await);
    }
    let (table, ids) = table_with(&catalog, files).await;
    let warm = WarmTable {
        store: DataStore::new(data.clone()),
        catalog,
        data,
        table,
        ids,
    };

    let found = warm.locate(vec![4]).await;
    assert_eq!(found[0].data_file_id, Some(warm.ids[1]));
    assert_eq!(warm.catalog.row_lookups.summarized_files(), 4);

    warm
}

/// Positioning reuses verified summaries only for the same immutable file and
/// scope.
#[tokio::test]
async fn retained_summaries_preserve_positions_and_validate_the_source() {
    let warm = warm_table().await;
    let directory = super::super::lookup(&warm.catalog.row_lookups.files, warm.table).unwrap();
    let file =
        crate::catalog::snapshot::data_file_info(directory.files.get(&warm.ids[1].get()).unwrap());
    let scope = super::DirectoryScope {
        store: &warm.store,
        data_prefix: "",
        table_prefix: &directory.table_prefix,
        table: warm.table,
    };
    let summary = directory.retained_summary(&scope, &file).unwrap();
    assert_eq!(
        summary.positions_of(&[3, 4, 5, 6]),
        [Some(0), Some(1), Some(2), None]
    );
    assert!(!summary.built);

    let mut changed = file.clone();
    changed.path = "replacement.parquet".into();
    assert!(directory.retained_summary(&scope, &changed).is_none());
    changed = file.clone();
    changed.row_id_start = Some(100);
    assert!(directory.retained_summary(&scope, &changed).is_none());
    let other = DataStore::new(warm.data.clone());
    assert!(
        directory
            .retained_summary(
                &super::DirectoryScope {
                    store: &other,
                    ..scope
                },
                &file
            )
            .is_none()
    );
    warm.catalog.close().await.unwrap();
}

/// Auxiliary eviction does not make positioning decode a retained summary
/// again.
#[tokio::test]
async fn retained_permuted_summaries_survive_auxiliary_eviction() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let data = Arc::new(InMemory::new());
    let row_ids: Vec<_> = (0..100_000).rev().map(|id| id * 2).collect();
    let (file_size_bytes, footer_size) =
        write_ids_file(&data, "main/t/permuted.parquet", &row_ids).await;
    let (table, ids) = table_with(
        &catalog,
        vec![DataFile {
            path: "permuted.parquet".into(),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: row_ids.len() as u64,
            file_size_bytes,
            footer_size,
            encryption_key: None,
            partition_values: vec![],
            column_stats: vec![],
        }],
    )
    .await;
    let store = DataStore::new(data);
    catalog
        .locate_row_ids(Some(store.clone()), "", table, vec![0, 198])
        .await
        .unwrap();
    let directory = super::super::lookup(&catalog.row_lookups.files, table).unwrap();
    let file =
        crate::catalog::snapshot::data_file_info(directory.files.get(&ids[0].get()).unwrap());
    crate::data_file::evict_summary(
        &store,
        table.get(),
        ids[0].get(),
        "main/t/permuted.parquet",
        file_size_bytes,
    );
    let start = std::time::Instant::now();
    let summaries = catalog
        .file_summaries(&store, "", &directory.table_prefix, table, vec![file])
        .await;
    let summary = summaries[0].1.as_ref().unwrap();
    assert!(
        !summary.built,
        "positioning decoded the row-id column again"
    );
    assert_eq!(
        summary.positions_of(&[0, 198, 1]),
        [Some(99_999), Some(99_900), None]
    );
    eprintln!(
        "100,000 permuted row IDs, positioning after auxiliary eviction: {:?}, no summary rebuild",
        start.elapsed()
    );
    catalog.close().await.unwrap();
}

/// A file registered against a warm table is the only one summarized.
#[tokio::test]
async fn a_registered_file_is_the_only_one_a_warm_directory_summarizes() {
    let warm = warm_table().await;
    let added = Cell::new(None);
    let file = dense_file(&warm.data, 4, 3).await;
    let table = warm.table;
    warm.catalog
        .commit(|tx| {
            added.set(Some(tx.register_data_file(table, file.clone(), &[])?));
            Ok(())
        })
        .await
        .unwrap();

    let found = warm.locate(vec![13, 4]).await;
    assert_eq!(found[0].data_file_id, added.get());
    assert!(found[1].data_file_id.is_some());
    assert_eq!(warm.catalog.row_lookups.summarized_files(), 5);
    warm.catalog.close().await.unwrap();
}

/// An expired file leaves a warm directory without any file being
/// summarized again.
#[tokio::test]
async fn an_expired_file_leaves_a_warm_directory_without_a_rebuild() {
    let warm = warm_table().await;
    let (table, expired) = (warm.table, warm.ids[2]);
    warm.catalog
        .commit(|tx| tx.expire_data_file(table, expired))
        .await
        .unwrap();

    let found = warm.locate(vec![7, 4]).await;
    assert_eq!(
        found[0].data_file_id, None,
        "the expired file still located a row"
    );
    assert_eq!(found[1].data_file_id, Some(warm.ids[1]));
    assert_eq!(warm.catalog.row_lookups.summarized_files(), 4);
    warm.catalog.close().await.unwrap();
}

/// Writes a file whose rows carry the ids in `row_ids` in DuckLake's
/// tagged row-id column, returning the file and footer sizes.
async fn write_ids_file(store: &InMemory, path: &str, row_ids: &[u64]) -> (u64, u64) {
    let row_id_field = Field::new("_ducklake_internal_row_id", DataType::Int64, false)
        .with_metadata(std::collections::HashMap::from([(
            parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
            "2147483540".to_string(),
        )]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        row_id_field,
    ]));
    let ids: Vec<i64> = row_ids
        .iter()
        .map(|id| i64::try_from(*id).unwrap())
        .collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(ids)),
        ],
    )
    .unwrap();

    let mut buffer = Vec::new();
    {
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let footer_offset = buffer.len() - 8;
    let footer_size = u64::from(u32::from_le_bytes(
        buffer[footer_offset..footer_offset + 4].try_into().unwrap(),
    ));
    let file_size = u64::try_from(buffer.len()).unwrap();
    store.put(&Path::from(path), buffer.into()).await.unwrap();

    (file_size, footer_size)
}

/// Files holding sparse ids over disjoint spans are each probed only for
/// rows inside their span.
#[tokio::test]
async fn sparse_files_with_disjoint_spans_are_probed_only_within_their_span() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let data = Arc::new(InMemory::new());
    let mut files = Vec::new();
    for (ordinal, ids) in [vec![0, 2, 4, 6], vec![10, 12, 14], vec![20, 22]]
        .iter()
        .enumerate()
    {
        let name = format!("sparse-{ordinal}.parquet");
        let (file_size_bytes, footer_size) =
            write_ids_file(&data, &format!("main/t/{name}"), ids).await;
        files.push(DataFile {
            path: name,
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: u64::try_from(ids.len()).unwrap(),
            file_size_bytes,
            footer_size,
            encryption_key: None,
            partition_values: vec![],
            column_stats: vec![],
        });
    }
    let (table, ids) = table_with(&catalog, files).await;
    let store = DataStore::new(data);

    let found = catalog
        .locate_row_ids(Some(store), "", table, vec![2, 12, 22, 5, 30])
        .await
        .unwrap();
    let located: Vec<_> = found.iter().map(|row| row.data_file_id).collect();
    assert_eq!(
        located,
        vec![Some(ids[0]), Some(ids[1]), Some(ids[2]), None, None]
    );
    assert_eq!(catalog.row_lookups.summarized_files(), 3);
    assert_eq!(
        catalog.row_lookups.summary_probes(),
        4,
        "rows outside every span were probed"
    );
    catalog.close().await.unwrap();
}

/// Overlapping summaries are probed only when their spans cover a requested
/// row.
#[tokio::test]
async fn overlapping_summary_spans_only_probe_files_covering_the_requested_rows() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let data = Arc::new(InMemory::new());
    let mut files = Vec::new();
    for ordinal in 0..64_u64 {
        let first = ordinal / 2 * 100 + ordinal % 2;
        let name = format!("overlap-{ordinal}.parquet");
        let (file_size_bytes, footer_size) = write_ids_file(
            &data,
            &format!("main/t/{name}"),
            &[first, first + 2, first + 4],
        )
        .await;
        files.push(DataFile {
            path: name,
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: 3,
            file_size_bytes,
            footer_size,
            encryption_key: None,
            partition_values: vec![],
            column_stats: vec![],
        });
    }
    let (table, ids) = table_with(&catalog, files).await;
    let store = DataStore::new(data);
    for lookup in 1..=2 {
        let found = catalog
            .locate_row_ids(Some(store.clone()), "", table, vec![2, 103, 10000])
            .await
            .unwrap();
        assert_eq!(
            found.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![Some(ids[0]), Some(ids[3]), None]
        );
        assert_eq!(catalog.row_lookups.summary_probes(), lookup * 4);
        assert_eq!(catalog.row_lookups.summarized_files(), 64);
    }
    catalog
        .commit(|tx| tx.expire_data_file(table, ids[0]))
        .await
        .unwrap();
    let found = catalog
        .locate_row_ids(Some(store), "", table, vec![2, 103])
        .await
        .unwrap();
    assert_eq!(
        found.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
        vec![None, Some(ids[3])]
    );
    assert_eq!(
        catalog.row_lookups.summarized_files(),
        64,
        "expiry rebuilt unchanged summaries"
    );
    assert_eq!(catalog.row_lookups.summary_probes(), 11);
    catalog.close().await.unwrap();
}

/// A row is placed by the dense range starting at or before it; a range
/// overlapping a live one is refused until that one leaves.
#[test]
fn dense_ranges_place_by_range_and_refuse_overlap() {
    let mut ranges = DenseRanges::default();
    assert!(ranges.insert(0, 2, 1));
    assert!(ranges.insert(3, 5, 2));
    assert!(ranges.insert(u64::MAX, u64::MAX, 3));
    assert!(!ranges.insert(5, 7, 4), "an overlapping range was admitted");
    assert!(!ranges.insert(1, 1, 5), "a nested range was admitted");

    assert_eq!(ranges.file_holding(2), Some(1));
    assert_eq!(ranges.file_holding(3), Some(2));
    assert_eq!(ranges.file_holding(6), None);
    assert_eq!(ranges.file_holding(u64::MAX), Some(3));

    ranges.remove(2);
    assert_eq!(ranges.file_holding(3), None);
    assert!(ranges.insert(5, 7, 4));
    assert_eq!(ranges.file_holding(5), Some(4));
}

proptest! {
    /// Placement after any inserts and removes matches exhaustive
    /// membership over the ranges that were admitted.
    #[test]
    fn dense_range_placement_matches_exhaustive_membership(
        ranges in prop::collection::vec((any::<u64>(), 0u64..64), 0..64),
        removed in prop::collection::vec(any::<prop::sample::Index>(), 0..8),
        probe in any::<prop::sample::Index>(),
        row in any::<u64>(),
    ) {
        let mut live: Vec<(u64, u64, u64)> = Vec::new();
        let mut index = DenseRanges::default();
        for (file, (start, length)) in ranges.into_iter().enumerate() {
            let end = start.saturating_add(length);
            let file = u64::try_from(file).unwrap();
            let overlaps = live.iter().any(|&(s, e, _)| s <= end && start <= e);
            prop_assert_eq!(index.insert(start, end, file), !overlaps);
            if !overlaps {
                live.push((start, end, file));
            }
        }
        for pick in removed {
            if live.is_empty() {
                break;
            }
            let (_, _, file) = live.remove(pick.index(live.len()));
            index.remove(file);
        }

        let mut rows = vec![row];
        if !live.is_empty() {
            let (start, end, _) = live[probe.index(live.len())];
            rows.push(start + (row % (end - start + 1)));
        }
        for row in rows {
            let expected = live
                .iter()
                .find(|&&(s, e, _)| s <= row && row <= e)
                .map(|&(_, _, f)| f);
            prop_assert_eq!(index.file_holding(row), expected);
        }
    }
}

/// A record for a file the data store does not hold, so summarizing it
/// fails.
fn missing_file(ordinal: u64) -> DataFile {
    DataFile {
        path: format!("missing-{ordinal}.parquet"),
        path_is_relative: true,
        file_format: "parquet".into(),
        record_count: 3,
        file_size_bytes: 1024,
        footer_size: 8,
        encryption_key: None,
        partition_values: vec![],
        column_stats: vec![],
    }
}

/// Registers a file that cannot be summarized against a warm table,
/// returning its id and the summary count once its read has failed once.
async fn warm_table_with_unreadable_file() -> (WarmTable, DataFileId, u64) {
    let warm = warm_table().await;
    let table = warm.table;
    let added = Cell::new(None);
    warm.catalog
        .commit(|tx| {
            added.set(Some(tx.register_data_file(table, missing_file(9), &[])?));
            Ok(())
        })
        .await
        .unwrap();

    let unreadable = added.get().unwrap();
    let found = warm.locate(vec![4]).await;
    assert!(
        found
            .iter()
            .any(|candidate| candidate.data_file_id == Some(unreadable)),
        "a file with no summary must stay a candidate for every row"
    );
    let summarized = warm.catalog.row_lookups.summarized_files();
    assert_eq!(summarized, 5);

    (warm, unreadable, summarized)
}

/// A file that cannot be summarized is not read again on every lookup.
#[tokio::test]
async fn an_unreadable_file_waits_before_it_is_read_again() {
    let (warm, unreadable, summarized) = warm_table_with_unreadable_file().await;

    for _ in 0..FIRST_RETRY_SKIP / 2 {
        let found = warm.locate(vec![4]).await;
        assert!(
            found
                .iter()
                .any(|candidate| candidate.data_file_id == Some(unreadable))
        );
    }

    assert_eq!(
        warm.catalog.row_lookups.summarized_files(),
        summarized,
        "an unreadable file was read again before its wait ended"
    );
    warm.catalog.close().await.unwrap();
}

/// The wait ends, so a failure that was only transient still heals.
#[tokio::test]
async fn an_unreadable_file_is_read_again_once_its_wait_ends() {
    let (warm, _, summarized) = warm_table_with_unreadable_file().await;

    for _ in 0..FIRST_RETRY_SKIP {
        warm.locate(vec![4]).await;
    }

    assert_eq!(
        warm.catalog.row_lookups.summarized_files(),
        summarized + 1,
        "an unreadable file was never read again"
    );
    warm.catalog.close().await.unwrap();
}

/// A readable file registered while another is failing is summarized
/// without the failing one being read again.
#[tokio::test]
async fn a_new_file_is_summarized_without_retrying_a_failing_one() {
    let (warm, _, summarized) = warm_table_with_unreadable_file().await;
    let table = warm.table;
    let file = dense_file(&warm.data, 5, 3).await;
    warm.catalog
        .commit(|tx| tx.register_data_file(table, file.clone(), &[]).map(|_| ()))
        .await
        .unwrap();

    warm.locate(vec![4]).await;
    assert_eq!(
        warm.catalog.row_lookups.summarized_files(),
        summarized + 1,
        "the failing file was read again alongside the new one"
    );
    warm.catalog.close().await.unwrap();
}

/// File-list churn cannot postpone retries of a failed summary indefinitely.
#[tokio::test]
async fn registering_files_does_not_restart_the_failed_summary_wait() {
    let (warm, _, summarized) = warm_table_with_unreadable_file().await;
    for ordinal in 0..FIRST_RETRY_SKIP {
        let file = dense_file(&warm.data, 100 + u64::from(ordinal), 3).await;
        warm.catalog
            .commit(|tx| {
                tx.register_data_file(warm.table, file.clone(), &[])
                    .map(|_| ())
            })
            .await
            .unwrap();
        warm.locate(vec![4]).await;
    }
    assert_eq!(
        warm.catalog.row_lookups.summarized_files(),
        summarized + u64::from(FIRST_RETRY_SKIP) + 1
    );
    warm.catalog.close().await.unwrap();
}

/// A restored immutable object clears its failed summary after the retry wait.
#[tokio::test]
async fn a_transient_summary_failure_recovers() {
    let warm = warm_table().await;
    let file = dense_file(&warm.data, 100, 3).await;
    let path = Path::from("main/t/data-100.parquet");
    let bytes = warm.data.get(&path).await.unwrap().bytes().await.unwrap();
    warm.data.delete(&path).await.unwrap();
    let added = Cell::new(None);
    warm.catalog
        .commit(|tx| {
            added.set(Some(tx.register_data_file(
                warm.table,
                file.clone(),
                &[],
            )?));
            Ok(())
        })
        .await
        .unwrap();
    let failed = added.get().unwrap();
    assert!(
        warm.locate(vec![4])
            .await
            .iter()
            .any(|row| row.data_file_id == Some(failed))
    );
    warm.data.put(&path, bytes.into()).await.unwrap();
    for _ in 0..FIRST_RETRY_SKIP {
        warm.locate(vec![4]).await;
    }
    assert!(
        !warm
            .locate(vec![4])
            .await
            .iter()
            .any(|row| row.data_file_id == Some(failed))
    );
    assert!(
        warm.locate(vec![12])
            .await
            .iter()
            .any(|row| row.data_file_id == Some(failed))
    );
    warm.catalog.close().await.unwrap();
}
