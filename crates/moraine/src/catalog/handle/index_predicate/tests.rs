use std::{cell::Cell, sync::Arc};

use IndexComparison::{Any, IsNull, Lower, Upper};
use object_store::memory::InMemory;

use super::*;
use crate::{Catalog, CatalogOptions, ColumnDef, FileIndexEntry, IndexDef, InlineChunk, IntWidth};

fn number(value: i128) -> IndexKeyValue {
    IndexKeyValue::Int {
        value,
        width: IntWidth::I64,
    }
}

#[tokio::test]
async fn composite_predicates_resolve_against_a_real_pinned_store() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let (table, a, b) = seed(&catalog, false).await;
    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    let reads = scope.reads();
    let snapshot = reads.snapshot().await.unwrap();
    for (comparisons, expected) in [
        (
            vec![(b, Any(vec![number(2)])), (a, Any(vec![number(0)]))],
            vec![1],
        ),
        (
            vec![
                (a, Any(vec![number(0), number(1)])),
                (b, Any(vec![number(1), number(3)])),
            ],
            vec![0, 2],
        ),
        (vec![(a, Any(vec![number(1)])), (b, IsNull)], vec![3]),
        (
            vec![
                (a, Any(vec![number(0)])),
                (b, Lower(number(1), false)),
                (b, Upper(number(2), true)),
            ],
            vec![1],
        ),
        (vec![(a, Any(vec![number(0)]))], vec![0, 1]),
        (vec![(a, Any(vec![]))], vec![]),
    ] {
        let predicates = comparisons
            .into_iter()
            .map(|(column, comparison)| IndexPredicate { column, comparison })
            .collect::<Vec<_>>();
        let result = reads
            .index_predicate_rows(&snapshot, table, &predicates)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.rows, expected);
        assert!(
            !result.certified_exact,
            "unverified index cannot replace predicates"
        );
    }
    assert!(
        reads
            .index_predicate_rows(
                &snapshot,
                table,
                &[IndexPredicate {
                    column: b,
                    comparison: Any(vec![number(2)])
                }]
            )
            .await
            .unwrap()
            .is_none()
    );
    drop(scope);
    catalog.close().await.unwrap();
}

async fn seed(catalog: &Catalog, unique: bool) -> (TableId, ColumnId, ColumnId) {
    let ids = Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            let table = tx.create_table(
                schema,
                "t",
                &["a", "b"].map(|name| ColumnDef {
                    name: name.into(),
                    column_type: "BIGINT".into(),
                    ..Default::default()
                }),
            )?;
            let columns = tx.columns_of(table);
            let index = tx.create_index(
                table,
                &IndexDef {
                    name: "by_ab".into(),
                    columns: columns.iter().map(|column| column.id).collect(),
                    unique,
                },
                &[],
            )?;
            let entries = [(0, Some(1)), (0, Some(2)), (1, Some(3)), (1, None)]
                .into_iter()
                .enumerate()
                .map(|(ordinal, (a, b))| FileIndexEntry {
                    index,
                    ordinal: ordinal as u64,
                    values: vec![Some(number(a)), b.map(number)],
                })
                .collect::<Vec<_>>();
            tx.inline_insert(
                table,
                &InlineChunk {
                    schema_version: 0,
                    row_count: 4,
                    arrow_schema: b"schema".to_vec(),
                    arrow_body: b"rows".to_vec(),
                },
                &entries,
            )?;
            ids.set(Some((table, columns[0].id, columns[1].id)));
            Ok(())
        })
        .await
        .unwrap();
    ids.get().unwrap()
}

#[tokio::test]
async fn a_unique_prefix_does_not_omit_null_bearing_trailing_keys() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let (table, a, b) = seed(&catalog, true).await;
    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    let snapshot = scope.reads().snapshot().await.unwrap();
    let predicate = IndexPredicate {
        column: a,
        comparison: Any(vec![number(1)]),
    };
    assert!(
        scope
            .reads()
            .index_predicate_rows(&snapshot, table, std::slice::from_ref(&predicate))
            .await
            .unwrap()
            .is_none()
    );
    let result = scope
        .reads()
        .index_predicate_rows(
            &snapshot,
            table,
            &[
                predicate,
                IndexPredicate {
                    column: b,
                    comparison: IsNull,
                },
            ],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![3]);
    drop(scope);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn broad_predicates_decline_instead_of_collecting_all_matches() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let (table, a, b) = seed(&catalog, false).await;
    let info = catalog
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table)
        .remove(0);
    let transaction = catalog.begin_write_tx().unwrap();
    for value in [Some(number(1)), None] {
        for row_id in 100..5100 {
            let (key, _) = crate::store::index_encoding::encode_ordered_index_entry(
                &[Some(number(0)), value.clone()],
                &info.directions,
                &info.nulls,
                info.id.get(),
                false,
                row_id,
            )
            .unwrap();
            transaction.put(key, []).unwrap();
        }
    }
    transaction.commit().await.unwrap();
    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    let reads = scope.reads();
    let snapshot = reads.snapshot().await.unwrap();
    for comparisons in [
        vec![(a, Any(vec![number(0)])), (b, Any(vec![number(1)]))],
        vec![(a, Lower(number(0), true))],
        vec![(a, Any(vec![number(0)])), (b, IsNull)],
    ] {
        let predicates = comparisons
            .into_iter()
            .map(|(column, comparison)| IndexPredicate { column, comparison })
            .collect::<Vec<_>>();
        assert!(
            reads
                .index_predicate_rows(&snapshot, table, &predicates)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(
        reads
            .index_lookup(table, info.id, &[number(0), number(1)])
            .await
            .unwrap()
            .len()
            > 4096
    );
    drop(scope);
    catalog.close().await.unwrap();
}
