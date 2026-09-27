//! A flush's registered files must carry exactly the row ids of the
//! chunks it drains, one per physical row; a compaction's, exactly its
//! sources' (a rewrite may drop rows, never rename them).

use super::*;

/// Registers `path` as flush output of table 1 and drains its inline
/// chunks up to snapshot 4, minting snapshot 5. The drain is named
/// `drains` times, as DuckLake names it twice.
async fn commit_flush_draining(
    catalog: &Catalog,
    store: &Arc<InMemory>,
    path: &str,
    record_count: u64,
    size: ParquetSize,
    drains: usize,
) -> Result<()> {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let object_store: Arc<dyn object_store::ObjectStore> = store.clone();
    let mut tx =
        StagedTransaction::begin_detached_with_store(catalog, db_tx, DataStore::new(object_store));
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: rewrite_data_file_row(12, 3, path, record_count, size),
    });
    for _ in 0..drains {
        tx.stage(RowOperation::InlineFlushDelete {
            table_id: 1,
            schema_version: 0,
            flush_snapshot: 4,
        });
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(5, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(5, "deleted_from_table:1,inline_flush:1"),
    });
    tx.commit().await.map(|_| ())
}

async fn commit_flush(
    catalog: &Catalog,
    store: &Arc<InMemory>,
    path: &str,
    record_count: u64,
    size: ParquetSize,
) -> Result<()> {
    commit_flush_draining(catalog, store, path, record_count, size, 1).await
}

async fn chunk_count(catalog: &Catalog) -> usize {
    let tx = catalog.begin_write_tx().await.unwrap();
    store_inline::scan_inline_chunks(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap()
        .len()
}

/// Two chunks whose ids leave a gap: a file numbering the second densely
/// after the first names ids the chunks never held.
#[tokio::test]
async fn a_flush_that_renumbers_the_drained_rows_is_refused() {
    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    inline_insert(&catalog, 3, 0, &[0, 1, 2], true).await;
    inline_insert(&catalog, 4, 5, &[5, 6, 7], false).await;
    let store = Arc::new(InMemory::new());
    let size = write_parquet_with_row_ids(
        &store,
        "main/t/flush.parquet",
        &[0, 1, 2, 5, 6, 7],
        &[0, 1, 2, 3, 4, 5],
    )
    .await;

    let err = commit_flush(&catalog, &store, "flush.parquet", 6, size)
        .await
        .unwrap_err();

    assert!(matches!(err, Error::Corruption(_)), "{err}");
    assert!(
        err.to_string().contains("row id 3 where the chunks hold 5"),
        "{err}"
    );
    assert_eq!(
        chunk_count(&catalog).await,
        2,
        "a refused flush drains nothing"
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn a_flush_carrying_the_drained_row_ids_lands() {
    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    inline_insert(&catalog, 3, 0, &[0, 1, 2], true).await;
    inline_insert(&catalog, 4, 5, &[5, 6, 7], false).await;
    let store = Arc::new(InMemory::new());
    let size = write_parquet_with_row_ids(
        &store,
        "main/t/flush.parquet",
        &[0, 1, 2, 5, 6, 7],
        &[0, 1, 2, 5, 6, 7],
    )
    .await;

    commit_flush(&catalog, &store, "flush.parquet", 6, size)
        .await
        .unwrap();

    assert_eq!(chunk_count(&catalog).await, 0);
    catalog.close().await.unwrap();
}

/// A row re-inserted under its id has two versions inline; the flush
/// writes both, so the file must name the id twice.
#[tokio::test]
async fn a_flush_names_every_version_of_a_row() {
    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    inline_insert(&catalog, 3, 0, &[0, 1, 2], true).await;
    inline_insert(&catalog, 4, 1, &[1], false).await;
    let store = Arc::new(InMemory::new());

    let one_version =
        write_parquet_with_row_ids(&store, "main/t/one.parquet", &[0, 1, 2], &[0, 1, 2]).await;
    let err = commit_flush(&catalog, &store, "one.parquet", 3, one_version)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Corruption(_)), "{err}");

    let both_versions =
        write_parquet_with_row_ids(&store, "main/t/both.parquet", &[0, 1, 1, 2], &[0, 1, 1, 2])
            .await;
    commit_flush(&catalog, &store, "both.parquet", 4, both_versions)
        .await
        .unwrap();
    assert_eq!(chunk_count(&catalog).await, 0);
    catalog.close().await.unwrap();
}

/// DuckLake names the drain twice in one commit, once as the flush
/// finalizes and once more at commit; the rows count once.
#[tokio::test]
async fn a_drain_named_twice_counts_its_rows_once() {
    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    inline_insert(&catalog, 3, 0, &[0, 1, 2], true).await;
    let store = Arc::new(InMemory::new());
    let size =
        write_parquet_with_row_ids(&store, "main/t/flush.parquet", &[0, 1, 2], &[0, 1, 2]).await;

    commit_flush_draining(&catalog, &store, "flush.parquet", 3, size, 2)
        .await
        .unwrap();

    assert_eq!(chunk_count(&catalog).await, 0);
    catalog.close().await.unwrap();
}

/// Registers file 1 (`a.parquet`, ids 0..=2 from a dense start) and file
/// 2 (`b.parquet`, embedded ids 3 and 5, registered as a flush output
/// is: dense start 3, partial to snapshot 3) at snapshot 3.
async fn register_dense_and_sparse_sources(catalog: &Catalog) -> Arc<InMemory> {
    let store = Arc::new(InMemory::new());
    let (_, dense) = bigint_batch(&[10, 11, 12]);
    let dense_size = write_parquet(&store, "main/t/a.parquet", &dense).await;
    let sparse_size =
        write_parquet_with_row_ids(&store, "main/t/b.parquet", &[13, 15], &[3, 5]).await;
    let mut sparse = indexed_data_file_row_at(2, "b.parquet", 2, sparse_size, 3);
    sparse[15] = Cell::U64(3);

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx =
        StagedTransaction::begin_detached_with_store(catalog, db_tx, DataStore::new(store.clone()));
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: indexed_data_file_row_at(1, "a.parquet", 3, dense_size, 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: sparse,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();
    store
}

/// Merges files 1 and 2 into file 12 as DuckLake does: the output
/// backdated to snapshot 3, the sources hard-deleted and scheduled,
/// snapshot 4 minted.
async fn commit_merge(catalog: &Catalog, store: &Arc<InMemory>, output: Vec<Cell>) -> Result<()> {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let object_store: Arc<dyn object_store::ObjectStore> = store.clone();
    let mut tx =
        StagedTransaction::begin_detached_with_store(catalog, db_tx, DataStore::new(object_store));
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: output,
    });
    for (file, path) in [(1, "a.parquet"), (2, "b.parquet")] {
        tx.stage(RowOperation::Delete {
            table: TableKind::DataFile,
            cells: vec![Cell::U64(1), Cell::U64(file), Cell::Null],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::FilesScheduledForDeletion,
            cells: vec![
                Cell::U64(file),
                Cell::Str(path.to_string()),
                Cell::Bool(true),
                Cell::I64(1_000),
            ],
        });
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "merge_adjacent:1"),
    });
    tx.commit().await.map(|_| ())
}

/// Table 1's live data files as of `snapshot`, by id.
async fn live_file_ids(catalog: &Catalog, snapshot: u64) -> Vec<u64> {
    let view = catalog
        .snapshot_at(crate::catalog::SnapshotId::new(snapshot))
        .await
        .unwrap();
    view.data_files_of(crate::catalog::TableId::new(1))
        .iter()
        .map(|file| file.id.get())
        .collect()
}

/// A dense file chained onto a sparse flush output numbers the merged
/// file by position, so ids the sources never held appear and id 5 is
/// lost.
#[tokio::test]
async fn a_merge_that_renumbers_a_sparse_source_is_refused() {
    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    let store = register_dense_and_sparse_sources(&catalog).await;
    let (_, merged) = bigint_batch(&[10, 11, 12, 13, 15]);
    let size = write_parquet(&store, "main/t/merged.parquet", &merged).await;
    let mut output = rewrite_data_file_row(12, 3, "merged.parquet", 5, size);
    output[11] = Cell::U64(0);

    let err = commit_merge(&catalog, &store, output).await.unwrap_err();

    assert!(matches!(err, Error::Corruption(_)), "{err}");
    assert!(
        err.to_string()
            .contains("row id 4 where its sources hold 5"),
        "{err}"
    );
    assert_eq!(
        live_file_ids(&catalog, 3).await,
        vec![1, 2],
        "a refused merge keeps its sources"
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn a_merge_carrying_its_sources_row_ids_lands() {
    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    let store = register_dense_and_sparse_sources(&catalog).await;
    let size = write_parquet_with_row_ids(
        &store,
        "main/t/merged.parquet",
        &[10, 11, 12, 13, 15],
        &[0, 1, 2, 3, 5],
    )
    .await;

    commit_merge(
        &catalog,
        &store,
        rewrite_data_file_row(12, 3, "merged.parquet", 5, size),
    )
    .await
    .unwrap();

    assert_eq!(live_file_ids(&catalog, 4).await, vec![12]);
    catalog.close().await.unwrap();
}

/// Rewrites file 1 (ids 0..=2) into file 12 carrying `row_ids`: the
/// source ended, the replacement rebased, snapshot 4 minted.
async fn commit_rewrite(catalog: &Catalog, store: &Arc<InMemory>, row_ids: &[i64]) -> Result<()> {
    let values: Vec<i64> = row_ids.iter().map(|id| 10 * (id + 1)).collect();
    let size = write_parquet_with_row_ids(store, "main/t/rewrite.parquet", &values, row_ids).await;
    let count = u64::try_from(row_ids.len()).unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let object_store: Arc<dyn object_store::ObjectStore> = store.clone();
    let mut tx =
        StagedTransaction::begin_detached_with_store(catalog, db_tx, DataStore::new(object_store));
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: rewrite_data_file_row(12, 4, "rewrite.parquet", count, size),
    });
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(1), Cell::U64(4)],
    });
    tx.stage(RowOperation::UpdateSetBegin {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(12), Cell::U64(4)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "rewrite_delete:1"),
    });
    tx.commit().await.map(|_| ())
}

/// A rewrite drops the rows a delete file names and keeps the survivors'
/// ids, so its output may hold fewer ids than its source but never one
/// the source lacked.
#[tokio::test]
async fn a_rewrite_may_drop_rows_but_not_rename_them() {
    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;
    commit_rewrite(&catalog, &store, &[0, 2]).await.unwrap();
    assert_eq!(live_file_ids(&catalog, 4).await, vec![12]);
    catalog.close().await.unwrap();

    let (catalog, _) = catalog_with_indexed_inline_table(true).await;
    let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;
    let err = commit_rewrite(&catalog, &store, &[0, 3]).await.unwrap_err();
    assert!(matches!(err, Error::Corruption(_)), "{err}");
    assert!(err.to_string().contains("row id 3"), "{err}");
    assert_eq!(live_file_ids(&catalog, 3).await, vec![1]);
    catalog.close().await.unwrap();
}
