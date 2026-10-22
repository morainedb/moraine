use super::*;

struct BodyOwner {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for BodyOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for BodyOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// Shared bodies are released on success, rollback and failed index decoding.
#[tokio::test]
async fn staged_body_owner_is_released_on_every_transaction_outcome() {
    for outcome in ["commit", "rollback", "decode_error"] {
        let (catalog, index) = catalog_with_indexed_inline_table(false).await;
        let (schema, batch) = bigint_batch(&[10, 20, 30]);
        let encoded = inline_body(&batch);
        let drops = Arc::new(AtomicUsize::new(0));
        let body = Bytes::from_owner(BodyOwner {
            bytes: if outcome == "decode_error" {
                vec![0]
            } else {
                encoded.clone()
            },
            drops: drops.clone(),
        });
        let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap());
        tx.stage(RowOperation::InlineSchema {
            table_id: 1,
            schema_version: 0,
            arrow_schema: inline_schema_ipc(&schema),
        });
        tx.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 3,
            row_id_start: 0,
            row_count: 3,
            arrow_body: body,
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(3, 1, 2),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(3, "inlined_insert:1"),
        });
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        match outcome {
            "commit" => {
                tx.commit().await.unwrap();
            }
            "rollback" => tx.rollback(),
            _ => {
                assert!(tx.commit().await.is_err());
            }
        }
        assert_eq!(drops.load(Ordering::SeqCst), 1, "{outcome}");
        assert_eq!(
            index_entry_count(&catalog, false, index).await,
            if outcome == "commit" { 3 } else { 0 }
        );
        if outcome == "commit" {
            let rows = catalog.recent_rows(TableId::new(1)).await.unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].chunk_body.as_ref(), &encoded);
        }
        catalog.close().await.unwrap();
    }
}

#[tokio::test]
async fn staging_memory_failure_releases_rows_and_prevents_commit() {
    let catalog = open().await;
    let mut tx = StagedTransaction::begin_detached(&catalog, catalog.begin_write_tx().unwrap());
    tx.staging = super::super::memory::StagingMemory::with_limit(1024);
    tx.try_stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 1),
    })
    .unwrap();
    let error = tx
        .try_stage(RowOperation::InlineSchema {
            table_id: 1,
            schema_version: 0,
            arrow_schema: vec![0; 2048],
        })
        .unwrap_err();
    assert!(error.to_string().contains("staging memory"));
    assert!(tx.ops.is_empty());
    assert!(
        tx.try_stage(RowOperation::InlineDrop { table_id: 1 })
            .is_err()
    );
    assert!(tx.commit().await.is_err());
    assert_eq!(catalog.snapshot().await.unwrap().snapshot.snapshot_id, 0);
    catalog.close().await.unwrap();
}
