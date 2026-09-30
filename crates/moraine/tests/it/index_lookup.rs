use moraine::{IndexDef, IndexEntry, IndexKeyValue, IntWidth};

use crate::fixtures::{col, open_memory};

fn key(value: i128) -> IndexKeyValue {
    IndexKeyValue::Int {
        value,
        width: IntWidth::I64,
    }
}

/// A pinned scope keeps its catalog and index entries at one revision.
#[tokio::test]
async fn index_read_scope_survives_a_concurrent_index_drop() {
    let catalog = open_memory().await;
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            let table = tx.create_table(schema, "scoped", &[col("value")])?;
            let index = tx.create_index(
                table,
                &IndexDef {
                    name: "by_value".into(),
                    columns: vec![moraine::ColumnId::new(1)],
                    unique: true,
                },
                &[IndexEntry {
                    row_id: 7,
                    values: vec![Some(key(10))],
                }],
            )?;
            created.set(Some((table, index)));
            Ok(())
        })
        .await
        .unwrap();
    let (table, index) = created.get().unwrap();
    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    let unchanged = catalog.index_read_scope().await.unwrap().unwrap();
    assert_eq!(scope.identity(), unchanged.identity());
    catalog.commit(|tx| tx.drop_index(index)).await.unwrap();
    let changed = catalog.index_read_scope().await.unwrap().unwrap();
    assert_ne!(scope.identity(), changed.identity());
    assert_eq!(
        scope
            .reads()
            .index_lookup(table, index, &[key(10)])
            .await
            .unwrap(),
        [7]
    );
    assert!(
        scope
            .reads()
            .snapshot()
            .await
            .unwrap()
            .index_by_name(table, "by_value")
            .is_some()
    );
    assert!(
        changed
            .reads()
            .index_lookup(table, index, &[key(10)])
            .await
            .is_err()
    );
    drop((scope, unchanged, changed));
    catalog.close().await.unwrap();
}

/// An `IN` lookup is one logical read: duplicate and absent keys do not
/// duplicate or invent rows, while every distinct present key is returned.
#[tokio::test]
async fn index_lookup_many_returns_the_union_of_distinct_keys() {
    let catalog = open_memory().await;
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "items", &[col("value")])?;
            let index = tx.create_index(
                table,
                &IndexDef {
                    name: "by_value".to_owned(),
                    columns: vec![moraine::ColumnId::new(1)],
                    unique: true,
                },
                &[
                    IndexEntry {
                        row_id: 0,
                        values: vec![Some(key(10))],
                    },
                    IndexEntry {
                        row_id: 1,
                        values: vec![Some(key(20))],
                    },
                ],
            )?;
            created.set(Some((table, index)));
            Ok(())
        })
        .await
        .unwrap();
    let (table, index) = created.get().unwrap();

    let found = catalog
        .index_lookup_many(
            table,
            index,
            &[vec![key(20)], vec![key(10)], vec![key(20)], vec![key(99)]],
        )
        .await
        .unwrap();

    assert_eq!(found, vec![0, 1]);
}

/// Empty `IN` lists are valid and resolve to the empty set.
#[tokio::test]
async fn index_lookup_many_accepts_an_empty_key_set() {
    let catalog = open_memory().await;
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "items", &[col("value")])?;
            let index = tx.create_index(
                table,
                &IndexDef {
                    name: "by_value".to_owned(),
                    columns: vec![moraine::ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            created.set(Some((table, index)));
            Ok(())
        })
        .await
        .unwrap();
    let (table, index) = created.get().unwrap();

    assert!(
        catalog
            .index_lookup_many(table, index, &[])
            .await
            .unwrap()
            .is_empty()
    );
}

/// A batch large enough to divide across tasks answers exactly what the
/// same keys answer one at a time, through a pinned scope and without one.
/// Multi-valued keys, absent keys and duplicates all have to survive the
/// split, since a chunk boundary can fall anywhere among them.
#[tokio::test]
async fn a_chunked_batch_answers_what_single_key_lookups_answer() {
    const VALUES: i128 = 200;

    let catalog = open_memory().await;
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            let table = tx.create_table(schema, "chunked", &[col("value")])?;
            // Two rows per value, so a probe returns more than it is asked.
            let entries: Vec<_> = (0..VALUES)
                .flat_map(|value| {
                    [0u64, 1].map(|row| IndexEntry {
                        row_id: u64::try_from(value).unwrap() * 2 + row,
                        values: vec![Some(key(value))],
                    })
                })
                .collect();
            let index = tx.create_index(
                table,
                &IndexDef {
                    name: "by_value".into(),
                    columns: vec![moraine::ColumnId::new(1)],
                    unique: false,
                },
                &entries,
            )?;
            created.set(Some((table, index)));
            Ok(())
        })
        .await
        .unwrap();
    let (table, index) = created.get().unwrap();

    // Scattered, with duplicates and keys no row holds.
    let mut asked: Vec<Vec<IndexKeyValue>> = (0..128)
        .map(|i: i128| vec![key((i * 61) % (VALUES + 40))])
        .collect();
    asked.extend(asked.clone().into_iter().take(16));

    let mut expected = Vec::new();
    for one in &asked {
        expected.extend(catalog.index_lookup(table, index, one).await.unwrap());
    }
    expected.sort_unstable();
    expected.dedup();
    assert!(
        expected.len() > 200,
        "the fixture must be big enough to divide"
    );

    assert_eq!(
        catalog
            .index_lookup_many(table, index, &asked)
            .await
            .unwrap(),
        expected,
        "a batch resolved across tasks disagreed with single-key lookups"
    );

    let scope = catalog.index_read_scope().await.unwrap().unwrap();
    assert_eq!(
        scope
            .reads()
            .index_lookup_many(table, index, &asked)
            .await
            .unwrap(),
        expected,
        "a pinned scope's batch disagreed with single-key lookups"
    );
    drop(scope);

    catalog.close().await.unwrap();
}
