use std::{cell::Cell, sync::Arc};

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use proptest::prelude::*;

use super::DenseRanges;
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
