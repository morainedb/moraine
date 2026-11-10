use std::{cell::Cell, sync::Arc};

use object_store::memory::InMemory;

use super::*;
use crate::{
    Catalog, CatalogOptions, ColumnDef, FileIndexEntry, FileRowCandidate, IndexDef, InlineChunk,
    IntWidth,
};

/// One non-unique index over an inlined table: rows 0 and 1 hold 7, row 2
/// holds NULL.
async fn indexed_inline_table(catalog: &Catalog) -> (TableId, IndexId) {
    let ids = Cell::new(None);
    catalog
        .commit_materialized(|tx| {
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
async fn repeated_probes_use_scoped_reads_without_retaining_catalog_views() {
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
    let read_transactions = writer.read_transactions();
    let retained = writer.memory_tally().projection_bytes;
    for _ in 0..4 {
        assert_eq!(probe(&writer, table, index).await, expected);
        assert_eq!(writer.memory_tally().projection_bytes, retained);
    }
    assert!(writer.read_transactions() > read_transactions);

    reader.close().await.unwrap();
    writer.close().await.unwrap();
}
