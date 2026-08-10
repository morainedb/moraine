use moraine::{IndexDef, IndexEntry, IndexKeyValue, IntWidth};

use crate::fixtures::{col, open_memory};

fn key(value: i128) -> IndexKeyValue {
    IndexKeyValue::Int {
        value,
        width: IntWidth::I64,
    }
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
