use std::sync::Arc;

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use object_store::{memory::InMemory, path::Path};

use super::{
    row_location::file_summary,
    tests::{tagged_row_id_field, write_fixture},
};
use crate::data_file::{DataStore, ParquetFile, metrics::ScopedReadMetrics};

fn batch_with_embedded_row_ids(row_ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        tagged_row_id_field(false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                (0..i64::try_from(row_ids.len()).unwrap()).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(row_ids.to_vec())),
        ],
    )
    .unwrap()
}

/// A file whose embedded row ids are two ascending runs answers positions
/// by file order rather than by ascending-id rank.
#[tokio::test]
async fn non_ascending_embedded_ids_position_by_file_order() {
    let store = Arc::new(InMemory::new());
    let path = Path::from("non-ascending.parquet");
    let batch = batch_with_embedded_row_ids(&[10, 11, 12, 3, 4, 5]);
    let file_size = write_fixture(&store, &path, &batch).await;

    let summary = file_summary(
        ParquetFile::new(DataStore::new(store), path, file_size, 0),
        1,
        1,
        None,
        6,
    )
    .await
    .unwrap();

    assert!(summary.built, "a cold summary must read and cache");
    assert_eq!(
        summary.matching(&[10, 11, 12, 3, 4, 5, 999]),
        vec![10, 11, 12, 3, 4, 5],
        "membership is unaffected by file order"
    );
    assert_eq!(
        summary.positions_of(&[10, 11, 12, 3, 4, 5, 999]),
        vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5), None,],
    );
}

/// A file whose embedded row ids are already ascending positions rows by
/// their ascending-id rank, which coincides with file order.
#[tokio::test]
async fn ascending_embedded_ids_position_by_rank() {
    let store = Arc::new(InMemory::new());
    let path = Path::from("ascending.parquet");
    let batch = batch_with_embedded_row_ids(&[3, 7, 11, 50]);
    let file_size = write_fixture(&store, &path, &batch).await;

    let summary = file_summary(
        ParquetFile::new(DataStore::new(store), path, file_size, 0),
        1,
        1,
        None,
        4,
    )
    .await
    .unwrap();

    assert_eq!(
        summary.matching(&[1, 3, 7, 12, 50]),
        vec![3, 7, 50],
        "membership is unaffected by ascending order"
    );
    assert_eq!(
        summary.positions_of(&[3, 7, 11, 50, 1]),
        vec![Some(0), Some(1), Some(2), Some(3), None],
    );
}

/// A file whose ids are its recorded dense range is remembered as such:
/// a later summary of it consults neither the cache's footer nor the
/// file.
#[tokio::test]
async fn a_dense_file_is_remembered_without_its_footer() {
    let store = Arc::new(InMemory::new());
    let path = Path::from("dense.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![0, 1, 2, 3]))]).unwrap();
    let file_size = write_fixture(&store, &path, &batch).await;
    let data = DataStore::new(store);
    let summarize = |metrics: &Arc<ScopedReadMetrics>| {
        file_summary(
            ParquetFile::new(data.clone(), path.clone(), file_size, 0)
                .with_metrics(Arc::clone(metrics)),
            1,
            1,
            Some(100),
            4,
        )
    };

    let cold = Arc::new(ScopedReadMetrics::default());
    let first = summarize(&cold).await.unwrap();
    assert_eq!(first.dense_range(), Some(100..104));
    assert_eq!(
        cold.tally().metadata_misses,
        1,
        "the footer proves the ids are dense"
    );

    let warm = Arc::new(ScopedReadMetrics::default());
    let second = summarize(&warm).await.unwrap();
    assert_eq!(second.dense_range(), Some(100..104));
    assert!(!second.built);
    let tally = warm.tally();
    assert_eq!(
        (tally.metadata_hits, tally.metadata_misses),
        (0, 0),
        "a remembered dense range needs no footer"
    );
}
