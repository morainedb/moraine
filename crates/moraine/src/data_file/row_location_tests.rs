use std::sync::Arc;

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

use super::{
    row_location::file_summary,
    tests::{tagged_row_id_field, write_fixture},
};
use crate::data_file::{
    DataStore, ParquetFile, Want, metrics::ScopedReadMetrics, publish_if_missing,
};

/// The sidecar a publishing pass wrote, which it awaited before
/// returning.
async fn published_sidecar(store: &Arc<InMemory>, data_file: &Path) -> Path {
    let path = Path::parse(format!("{data_file}.rowsum")).unwrap();
    store.head(&path).await.expect("a summary was published");
    path
}

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
        Want::Positions,
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

/// A summary derived from a file is published beside it, and answers a
/// later reader that cannot see the file at all.
#[tokio::test]
async fn a_derived_summary_is_published_and_answers_without_its_file() {
    let store = Arc::new(InMemory::new());
    let path = Path::from("published.parquet");
    let batch = batch_with_embedded_row_ids(&[10, 11, 12, 3, 4, 5]);
    let file_size = write_fixture(&store, &path, &batch).await;
    let requested = [10, 11, 12, 3, 4, 5, 999];

    let derived = file_summary(
        ParquetFile::new(DataStore::new(store.clone()), path.clone(), file_size, 0),
        1,
        1,
        None,
        6,
        Want::Positions,
    )
    .await
    .unwrap();
    assert!(derived.built, "a cold summary must read and cache");

    assert!(
        publish_if_missing(
            ParquetFile::new(DataStore::new(store.clone()), path.clone(), file_size, 0),
            1,
            1,
            None,
        )
        .await
        .unwrap(),
        "a file with no published summary must get one"
    );

    // Only what that read published remains: a summary now can have come
    // from nowhere else.
    published_sidecar(&store, &path).await;
    store.delete(&path).await.unwrap();

    let published = file_summary(
        ParquetFile::new(DataStore::new(store), path, file_size, 0),
        1,
        1,
        None,
        6,
        Want::Positions,
    )
    .await
    .unwrap();

    assert!(!published.built, "a published summary is not derived again");
    assert_eq!(
        published.positions_of(&requested),
        derived.positions_of(&requested),
    );
}

/// A lookup that resolves no positions reads a published summary's
/// membership and leaves its order unread.
#[tokio::test]
async fn a_membership_want_reads_no_order() {
    let store = Arc::new(InMemory::new());
    let path = Path::from("membership.parquet");
    let batch = batch_with_embedded_row_ids(&[10, 11, 12, 3, 4, 5]);
    let file_size = write_fixture(&store, &path, &batch).await;

    publish_if_missing(
        ParquetFile::new(DataStore::new(store.clone()), path.clone(), file_size, 0),
        1,
        1,
        None,
    )
    .await
    .unwrap();
    published_sidecar(&store, &path).await;
    store.delete(&path).await.unwrap();

    let membership = file_summary(
        ParquetFile::new(DataStore::new(store), path, file_size, 0),
        1,
        1,
        None,
        6,
        Want::Membership,
    )
    .await
    .unwrap();

    assert!(!membership.built, "a published membership is not derived");
    assert!(
        !membership.resolves_positions(),
        "membership alone carries no order"
    );
    assert_eq!(
        membership.matching(&[10, 11, 12, 3, 4, 5, 999]),
        vec![10, 11, 12, 3, 4, 5],
    );
    assert!(
        membership.visit_positions(10, |_| ()).is_err(),
        "a membership summary must refuse positions rather than invent them"
    );
}

/// A sidecar whose header names another file is ignored, not believed.
#[tokio::test]
async fn a_sidecar_from_another_file_does_not_answer() {
    let store = Arc::new(InMemory::new());
    let path = Path::from("mismatched.parquet");
    let batch = batch_with_embedded_row_ids(&[10, 11, 12, 3, 4, 5]);
    let file_size = write_fixture(&store, &path, &batch).await;

    publish_if_missing(
        ParquetFile::new(DataStore::new(store.clone()), path.clone(), file_size, 0),
        1,
        1,
        None,
    )
    .await
    .unwrap();
    published_sidecar(&store, &path).await;

    // The same bytes, read as though they described a different file.
    let summary = file_summary(
        ParquetFile::new(DataStore::new(store), path, file_size, 0),
        1,
        2,
        None,
        6,
        Want::Positions,
    )
    .await
    .unwrap();

    assert!(
        summary.built,
        "a sidecar naming another file must not be believed"
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
        Want::Positions,
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
            Want::Positions,
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

/// A second publishing pass over a file that already has a summary writes
/// nothing, which is what makes a backfill over a published lake cheap.
#[tokio::test]
async fn publishing_skips_a_file_that_already_has_a_summary() {
    let store = Arc::new(InMemory::new());
    let path = Path::from("twice.parquet");
    let batch = batch_with_embedded_row_ids(&[10, 11, 12, 3, 4, 5]);
    let file_size = write_fixture(&store, &path, &batch).await;
    let file = || ParquetFile::new(DataStore::new(store.clone()), path.clone(), file_size, 0);

    assert!(publish_if_missing(file(), 1, 1, None).await.unwrap());
    published_sidecar(&store, &path).await;

    assert!(
        !publish_if_missing(file(), 1, 1, None).await.unwrap(),
        "a published summary must not be derived again"
    );
}

/// A file whose ids the catalog already describes needs no summary, so
/// publishing leaves it alone.
#[tokio::test]
async fn publishing_skips_a_file_whose_ids_are_derivable() {
    let store = Arc::new(InMemory::new());
    let path = Path::from("dense-publish.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![0, 1, 2]))]).unwrap();
    let file_size = write_fixture(&store, &path, &batch).await;

    assert!(
        !publish_if_missing(
            ParquetFile::new(DataStore::new(store.clone()), path.clone(), file_size, 0),
            1,
            1,
            Some(100),
        )
        .await
        .unwrap()
    );
    assert!(
        store
            .head(&Path::from("dense-publish.parquet.rowsum"))
            .await
            .is_err(),
        "a dense file must not get a summary it does not need"
    );
}
