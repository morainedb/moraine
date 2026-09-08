use std::{cell::Cell, sync::Arc};

use object_store::memory::InMemory;

use super::*;
use crate::{Catalog, CatalogOptions, ColumnDef, IndexDef, transaction::commit};

#[tokio::test]
async fn range_and_null_scans_recheck_a_definition_removed_during_the_pass() {
    for null_lookup in [false, true] {
        let store = Arc::new(InMemory::new());
        let options = CatalogOptions {
            reader_poll_interval: Duration::from_millis(10),
            ..Default::default()
        };
        let writer = Catalog::open(store.clone(), options.clone()).await.unwrap();
        let ids = Cell::new(None);
        writer
            .commit(|tx| {
                let schema = tx.schema_by_name("main").unwrap().id;
                let table = tx.create_table(
                    schema,
                    "t",
                    &[ColumnDef {
                        name: "value".into(),
                        column_type: "BIGINT".into(),
                        ..Default::default()
                    }],
                )?;
                let index = tx.create_index(
                    table,
                    &IndexDef {
                        name: "by_value".into(),
                        columns: vec![tx.columns_of(table)[0].id],
                        unique: false,
                    },
                    &[],
                )?;
                ids.set(Some((table, index)));
                Ok(())
            })
            .await
            .unwrap();
        writer.close().await.unwrap();
        let (table, index) = ids.get().unwrap();
        let reader = Catalog::open_read_only(store.clone(), options.clone())
            .await
            .unwrap();
        let writer = Catalog::open(store, options).await.unwrap();
        let session = reader.begin_read().await.unwrap();
        let attempts = Cell::new(0);
        let result = reader
            .with_ready_index(session.handle(), table, index, async |info| {
                attempts.set(attempts.get() + 1);
                let head = writer.commit(|tx| tx.drop_index(index)).await?;
                writer.close().await?;
                tokio::time::timeout(Duration::from_secs(10), async {
                    while commit::read_head_value(session.handle())
                        .await
                        .unwrap()
                        .snapshot_id
                        != head.get()
                    {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                if null_lookup {
                    let key = encode_ordered_values(&[None], &info.directions, &info.nulls)?;
                    index_maintenance::null_prefix_row_ids(
                        session.handle(),
                        index.get(),
                        &key,
                        ScanOrder::Ascending,
                    )
                    .await
                } else {
                    index_maintenance::range_row_ids(
                        session.handle(),
                        index.get(),
                        info.unique,
                        NullOrder::Last,
                        Bound::Unbounded,
                        Bound::Unbounded,
                        ScanOrder::Ascending,
                    )
                    .await
                }
            })
            .await;
        assert!(matches!(result, Err(Error::NotFound(_))));
        assert_eq!(
            attempts.get(),
            1,
            "the next pass rejects the removed definition before scanning"
        );
        session.finish();
        reader.close().await.unwrap();
    }
}
