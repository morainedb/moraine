use std::{cell::Cell, sync::Arc};

use object_store::memory::InMemory;

use super::*;
use crate::{
    Catalog, CatalogOptions, ColumnDef, FileIndexEntry, FileRowCandidate, IndexDef, InlineChunk,
    IntWidth, transaction::commit,
};

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
        let mut session = None;
        let attempts = Cell::new(0);
        let result = reader
            .lookup_at_head(&mut session, table, index, async |handle, info| {
                attempts.set(attempts.get() + 1);
                let head = writer.commit(|tx| tx.drop_index(index)).await?;
                writer.close().await?;
                tokio::time::timeout(Duration::from_secs(10), async {
                    while commit::read_head_value(handle).await.unwrap().snapshot_id != head.get() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                if null_lookup {
                    let key = encode_ordered_values(&[None], &info.directions, &info.nulls)?;
                    index_maintenance::null_prefix_row_ids(
                        handle,
                        index.get(),
                        &key,
                        ScanOrder::Ascending,
                    )
                    .await
                } else {
                    index_maintenance::range_row_ids(
                        handle,
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
        finish_session(session);
        reader.close().await.unwrap();
    }
}

/// One non-unique index over an inlined table: rows 0 and 1 hold 7, row 2
/// holds NULL.
async fn indexed_inline_table(catalog: &Catalog) -> (TableId, IndexId) {
    let ids = Cell::new(None);
    catalog
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
            let entry = |ordinal, value: Option<i128>| FileIndexEntry {
                index,
                ordinal,
                values: vec![value.map(|value| IndexKeyValue::Int {
                    value,
                    width: IntWidth::I64,
                })],
            };
            tx.inline_insert(
                table,
                &InlineChunk {
                    schema_version: 0,
                    row_count: 3,
                    arrow_schema: b"schema".to_vec(),
                    arrow_body: b"rows".to_vec(),
                },
                &[entry(0, Some(7)), entry(1, Some(7)), entry(2, None)],
            )?;
            ids.set(Some((table, index)));
            Ok(())
        })
        .await
        .unwrap();

    ids.get().unwrap()
}

type Probed = (
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<FileRowCandidate>,
);

/// Every head-only accessor, once.
async fn probe(catalog: &ReadOnlyCatalog, table: TableId, index: IndexId) -> Probed {
    let seven = IndexKeyValue::Int {
        value: 7,
        width: IntWidth::I64,
    };
    (
        catalog
            .index_lookup(table, index, std::slice::from_ref(&seven))
            .await
            .unwrap(),
        catalog
            .index_lookup_many(table, index, &[vec![seven.clone()], vec![seven.clone()]])
            .await
            .unwrap(),
        catalog
            .index_range(table, index, Bound::Unbounded, Bound::Unbounded, false)
            .await
            .unwrap(),
        catalog
            .index_nulls(table, index, vec![None], false)
            .await
            .unwrap(),
        catalog
            .locate_row_ids(None, "", table, vec![0, 2, 9])
            .await
            .unwrap(),
    )
}

/// A warm read-write handle resolves lookups and row locations from its
/// held view and probes without a transaction, answering as a session does.
#[tokio::test]
async fn a_warm_writer_probes_without_a_head_read_or_a_transaction() {
    let store = Arc::new(InMemory::new());
    let options = CatalogOptions {
        reader_poll_interval: Duration::from_millis(10),
        ..Default::default()
    };
    let writer = Catalog::open(store.clone(), options.clone()).await.unwrap();
    let (table, index) = indexed_inline_table(&writer).await;

    let reader = Catalog::open_read_only(store, options).await.unwrap();
    let expected = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if reader.index_lookup_many(table, index, &[]).await.is_ok() {
                break probe(&reader, table, index).await;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(expected.0, [0, 1]);
    assert_eq!(expected.3, [2]);
    assert_eq!(expected.4.len(), 3);

    // The first pass verifies the inline directory; from there it is warm.
    assert_eq!(probe(&writer, table, index).await, expected);
    let (head_reads, read_transactions) = (writer.head_reads(), writer.read_transactions());

    for _ in 0..4 {
        assert_eq!(probe(&writer, table, index).await, expected);
    }
    assert_eq!(
        writer.head_reads(),
        head_reads,
        "a warm read-write handle read `sys/head`"
    );
    assert_eq!(
        writer.read_transactions(),
        read_transactions,
        "a warm read-write handle opened a read transaction"
    );

    reader.close().await.unwrap();
    writer.close().await.unwrap();
}
