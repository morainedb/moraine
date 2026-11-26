use std::{cell::Cell, sync::Arc};

use IndexComparison::{Any, IsNull, Lower, Upper};
use object_store::memory::InMemory;

use super::*;
use crate::{
    Catalog, CatalogOptions, ColumnDef, DataFile, FileIndexEntry, IndexDef, IndexId,
    IndexMaintenance, InlineChunk, IntWidth,
};

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
    let scope = catalog.index_read_scope().await.unwrap();
    let reads = scope.reads();
    let snapshot = reads.materialized().await.unwrap();
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
            .index_predicate_rows_projected(&snapshot, table, &predicates)
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
            .index_predicate_rows_projected(
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
        .commit_materialized(|tx| {
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
    let scope = catalog.index_read_scope().await.unwrap();
    let snapshot = scope.reads().materialized().await.unwrap();
    let predicate = IndexPredicate {
        column: a,
        comparison: Any(vec![number(1)]),
    };
    assert!(
        scope
            .reads()
            .index_predicate_rows_projected(&snapshot, table, std::slice::from_ref(&predicate))
            .await
            .unwrap()
            .is_none()
    );
    let result = scope
        .reads()
        .index_predicate_rows_projected(
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
        .materialized()
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
    let scope = catalog.index_read_scope().await.unwrap();
    let reads = scope.reads();
    let snapshot = reads.materialized().await.unwrap();
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
                .index_predicate_rows_projected(&snapshot, table, &predicates)
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

/// A single-column table with a unique index the catalog built from its own
/// reads, and the index's id.
async fn catalog_built_index(catalog: &Catalog) -> (TableId, ColumnId, IndexId) {
    let ids = Cell::new(None);
    catalog
        .commit_materialized(|tx| {
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
            ids.set(Some((table, tx.columns_of(table)[0].id)));
            Ok(())
        })
        .await
        .unwrap();
    let (table, a) = ids.get().unwrap();
    let index = catalog
        .create_index(
            table,
            &IndexDef {
                name: "by_a".into(),
                columns: vec![a],
                unique: true,
            },
            &[],
            IndexMaintenance::Synchronous,
            None,
            "",
        )
        .await
        .unwrap();
    (table, a, index)
}

/// The index's record and whether a full-key probe on `a = 7` is exact.
async fn derived_and_exact(
    catalog: &Catalog,
    table: TableId,
    a: ColumnId,
    index: IndexId,
) -> (bool, bool, usize) {
    let scope = catalog.index_read_scope().await.unwrap();
    let reads = scope.reads();
    let snapshot = reads.materialized().await.unwrap();
    let info = snapshot
        .indexes_of(table)
        .into_iter()
        .find(|info| info.id == index)
        .unwrap();
    assert!(info.verified_at_snapshot.is_none(), "no pass ran");
    let predicates = [IndexPredicate {
        column: a,
        comparison: Any(vec![number(7)]),
    }];
    let result = reads
        .index_predicate_rows_projected(&snapshot, table, &predicates)
        .await
        .unwrap()
        .unwrap();
    (info.derived_only, result.certified_exact, result.rows.len())
}

/// An index the catalog built from its own reads answers a full-key probe
/// exactly without a certificate.
#[tokio::test]
async fn a_catalog_built_index_is_exact_without_a_certificate() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let (table, a, index) = catalog_built_index(&catalog).await;
    assert_eq!(
        derived_and_exact(&catalog, table, a, index).await,
        (true, true, 0)
    );
    catalog.close().await.unwrap();
}

/// A writer-supplied entry withdraws the derived-only mark for good, and
/// with it the exactness nothing has checked.
#[tokio::test]
async fn a_writer_supplied_entry_withdraws_the_derived_mark() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let (table, a, index) = catalog_built_index(&catalog).await;
    catalog
        .commit_materialized(|tx| {
            tx.register_data_file(
                table,
                DataFile {
                    path: "t/f.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 1,
                    file_size_bytes: 1,
                    footer_size: 1,
                    encryption_key: None,
                    partition_values: vec![],
                    column_stats: vec![],
                },
                &[FileIndexEntry {
                    index,
                    ordinal: 0,
                    values: vec![Some(number(7))],
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        derived_and_exact(&catalog, table, a, index).await,
        (false, false, 1)
    );
    catalog.close().await.unwrap();
}

/// An index a writer backfilled and fed entries through the embedding API
/// is not exact until a verifier pass certifies it.
#[tokio::test]
async fn a_writer_built_index_is_not_exact_without_a_certificate() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let (table, a, b) = seed(&catalog, true).await;
    let scope = catalog.index_read_scope().await.unwrap();
    let reads = scope.reads();
    let snapshot = reads.materialized().await.unwrap();
    assert!(
        snapshot
            .indexes_of(table)
            .iter()
            .all(|info| !info.derived_only)
    );
    let predicates = [
        IndexPredicate {
            column: a,
            comparison: Any(vec![number(0)]),
        },
        IndexPredicate {
            column: b,
            comparison: Any(vec![number(2)]),
        },
    ];
    let result = reads
        .index_predicate_rows_projected(&snapshot, table, &predicates)
        .await
        .unwrap()
        .unwrap();
    assert!(!result.certified_exact);
    assert_eq!(result.rows.len(), 1);
    drop(scope);
    catalog.close().await.unwrap();
}

/// A key set names its columns in any order and resolves exactly the rows
/// holding one of its keys, never a row the keys' cross product would add.
#[tokio::test]
async fn key_sets_resolve_their_keys_and_nothing_the_cross_product_adds() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let (table, a, b) = seed(&catalog, false).await;
    let scope = catalog.index_read_scope().await.unwrap();
    let reads = scope.reads();
    let snapshot = reads.materialized().await.unwrap();

    let result = reads
        .index_key_rows_projected(
            &snapshot,
            table,
            &[b, a],
            &[vec![number(2), number(0)], vec![number(3), number(1)]],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![1, 2]);
    assert!(
        !result.certified_exact,
        "writer-supplied entries are untrusted"
    );

    // (0,1) and (1,2): the cross product holds (0,2), which row 1 carries.
    let result = reads
        .index_key_rows_projected(
            &snapshot,
            table,
            &[a, b],
            &[vec![number(0), number(1)], vec![number(1), number(2)]],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![0]);

    assert!(
        reads
            .index_key_rows_projected(&snapshot, table, &[a], &[vec![number(0)]])
            .await
            .unwrap()
            .is_none(),
        "a key set needs an index over exactly its columns"
    );
    assert!(matches!(
        reads
            .index_key_rows_projected(&snapshot, table, &[a, b], &[vec![number(0)]])
            .await,
        Err(crate::Error::Constraint(_))
    ));
    catalog.close().await.unwrap();
}

/// A key set wider than the planner collects declines like a broad predicate.
#[tokio::test]
async fn broad_key_sets_decline() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let (table, a, b) = seed(&catalog, false).await;
    let scope = catalog.index_read_scope().await.unwrap();
    let reads = scope.reads();
    let snapshot = reads.materialized().await.unwrap();
    let keys = (0..4097i128)
        .map(|key| vec![number(key), number(key)])
        .collect::<Vec<_>>();
    assert!(
        reads
            .index_key_rows_projected(&snapshot, table, &[a, b], &keys)
            .await
            .unwrap()
            .is_none()
    );
    catalog.close().await.unwrap();
}

/// A catalog-built index answers a key set exactly without a certificate.
#[tokio::test]
async fn a_catalog_built_index_answers_key_sets_exactly() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let (table, a, _) = catalog_built_index(&catalog).await;
    let scope = catalog.index_read_scope().await.unwrap();
    let reads = scope.reads();
    let snapshot = reads.materialized().await.unwrap();
    let result = reads
        .index_key_rows_projected(&snapshot, table, &[a], &[vec![number(7)]])
        .await
        .unwrap()
        .unwrap();
    assert!(result.certified_exact);
    assert!(result.rows.is_empty());
    catalog.close().await.unwrap();
}
