use object_store::ObjectStoreExt;

use super::*;
use crate::catalog::{ColumnId, TableId};

#[tokio::test]
async fn indexed_files_and_deletes_resolve_trailing_data_prefixes() {
    for prefix in ["org-123", "org-123/", "org-123///"] {
        let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
        let store = Arc::new(InMemory::new());
        let (_, batch) = bigint_batch(&[10, 20, 30]);
        let size = write_parquet(&store, "org-123/main/t/data.parquet", &batch).await;
        let mut tx = StagedTransaction::begin_detached_with_store(
            &catalog,
            catalog.begin_write_tx().await.unwrap(),
            DataStore::new(store.clone()),
        );
        tx.data_prefix = prefix.to_owned();
        tx.stage(RowOperation::Insert {
            table: TableKind::DataFile,
            cells: indexed_data_file_row(3, size),
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
        assert_eq!(index_entry_count(&catalog, true, index_id).await, 3);

        let delete_size = write_delete_file(&store, "deletes.parquet", "data.parquet", &[1]).await;
        store
            .rename(
                &Path::from("main/t/deletes.parquet"),
                &Path::from("org-123/main/t/deletes.parquet"),
            )
            .await
            .unwrap();
        let mut tx = StagedTransaction::begin_detached_with_store(
            &catalog,
            catalog.begin_write_tx().await.unwrap(),
            DataStore::new(store.clone()),
        );
        tx.data_prefix = prefix.to_owned();
        tx.stage(RowOperation::Insert {
            table: TableKind::DeleteFile,
            cells: delete_file_row_at(2, "deletes.parquet", 1, 1, delete_size),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(4, 1, 2),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(4, "deleted_from_table:1"),
        });
        tx.commit().await.unwrap();
        assert_eq!(index_entry_count(&catalog, true, index_id).await, 2);

        let entries = catalog
            .scoped_backfill_entries(
                DataStore::new(store),
                prefix,
                TableId::new(1),
                &[ColumnId::new(1)],
            )
            .await
            .unwrap();
        let mut rows: Vec<_> = entries.iter().map(|entry| entry.row_id).collect();
        rows.sort_unstable();
        assert_eq!(rows, vec![0, 2]);
        catalog.close().await.unwrap();
    }
}
