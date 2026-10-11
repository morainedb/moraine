//! Schema-level index naming through real catalog commits.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, Error, IndexDef};
use object_store::memory::InMemory;

#[tokio::test]
async fn index_names_are_case_insensitively_unique_within_a_schema() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let main = tx.schema_by_name("main").unwrap().id;
            let other = tx.create_schema("other")?;
            for (schema, table_name) in [(main, "one"), (main, "two"), (other, "three")] {
                let table = tx.create_table(
                    schema,
                    table_name,
                    &[ColumnDef {
                        name: "a".into(),
                        column_type: "BIGINT".into(),
                        ..Default::default()
                    }],
                )?;
                let definition = IndexDef {
                    name: if table_name == "two" { "BY_A" } else { "by_a" }.into(),
                    columns: vec![tx.columns_of(table)[0].id],
                    unique: false,
                };
                if table_name == "two" {
                    assert!(matches!(
                        tx.create_index(table, &definition, &[]),
                        Err(Error::AlreadyExists(_))
                    ));
                } else {
                    tx.create_index(table, &definition, &[])?;
                }
            }
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = catalog.snapshot().await.unwrap();
    let main = snapshot.schema_by_name("main").unwrap().id;
    let found = snapshot
        .index_by_schema_name(main, "BY_A")
        .unwrap()
        .unwrap();
    assert_eq!(
        found.table_id,
        snapshot.table_by_name(main, "one").unwrap().id
    );
    assert!(
        snapshot
            .index_by_schema_name(main, "missing")
            .unwrap()
            .is_none()
    );
    let one = snapshot.table_by_name(main, "one").unwrap().id;
    let two = snapshot.table_by_name(main, "two").unwrap().id;
    let definition = |table| IndexDef {
        name: "raced".into(),
        columns: vec![snapshot.columns_of(table)[0].id],
        unique: false,
    };
    let (first, second) = tokio::join!(
        catalog.commit(|tx| tx.create_index(one, &definition(one), &[]).map(|_| ())),
        catalog.commit(|tx| tx.create_index(two, &definition(two), &[]).map(|_| ())),
    );
    assert!(matches!(
        (&first, &second),
        (Ok(_), Err(Error::AlreadyExists(_))) | (Err(Error::AlreadyExists(_)), Ok(_))
    ));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn moving_tables_preserves_schema_index_names() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let main = tx.schema_by_name("main").unwrap().id;
            let other = tx.create_schema("other")?;
            for (schema, name) in [(main, "one"), (other, "two")] {
                let table = tx.create_table(
                    schema,
                    name,
                    &[ColumnDef {
                        name: "a".into(),
                        column_type: "BIGINT".into(),
                        ..Default::default()
                    }],
                )?;
                tx.create_index(
                    table,
                    &IndexDef {
                        name: if name == "one" { "by_a" } else { "BY_A" }.into(),
                        columns: vec![tx.columns_of(table)[0].id],
                        unique: false,
                    },
                    &[],
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = catalog.snapshot().await.unwrap();
    let main = snapshot.schema_by_name("main").unwrap().id;
    let other = snapshot.schema_by_name("other").unwrap().id;
    let one = snapshot.table_by_name(main, "one").unwrap().id;
    let two = snapshot.table_by_name(other, "two").unwrap().id;
    let result = catalog.commit(|tx| tx.set_table_schema(two, main)).await;
    assert!(matches!(result, Err(Error::AlreadyExists(_))), "{result:?}");
    assert_eq!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .table_by_name(other, "two")
            .unwrap()
            .id,
        two
    );
    catalog
        .commit(|tx| {
            tx.drop_index(tx.index_by_name(one, "by_a").unwrap().id)?;
            tx.set_table_schema(two, main)
        })
        .await
        .unwrap();
    assert_eq!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .index_by_schema_name(main, "by_a")
            .unwrap()
            .unwrap()
            .table_id,
        two
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn schema_moves_racing_index_creation_preserve_unique_names() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let main = tx.schema_by_name("main").unwrap().id;
            let other = tx.create_schema("other")?;
            for (schema, name) in [(main, "one"), (other, "two")] {
                let table = tx.create_table(
                    schema,
                    name,
                    &[ColumnDef {
                        name: "a".into(),
                        column_type: "BIGINT".into(),
                        ..Default::default()
                    }],
                )?;
                if name == "two" {
                    tx.create_index(
                        table,
                        &IndexDef {
                            name: "by_a".into(),
                            columns: vec![tx.columns_of(table)[0].id],
                            unique: false,
                        },
                        &[],
                    )?;
                }
            }
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = catalog.snapshot().await.unwrap();
    let main = snapshot.schema_by_name("main").unwrap().id;
    let other = snapshot.schema_by_name("other").unwrap().id;
    let one = snapshot.table_by_name(main, "one").unwrap().id;
    let two = snapshot.table_by_name(other, "two").unwrap().id;
    let definition = IndexDef {
        name: "BY_A".into(),
        columns: vec![snapshot.columns_of(one)[0].id],
        unique: false,
    };
    let (moved, created) = tokio::join!(
        catalog.commit(|tx| tx.set_table_schema(two, main)),
        catalog.commit(|tx| tx.create_index(one, &definition, &[]).map(|_| ())),
    );
    assert!(
        matches!(
            (&moved, &created),
            (Ok(_), Err(Error::AlreadyExists(_))) | (Err(Error::AlreadyExists(_)), Ok(_))
        ),
        "{moved:?}, {created:?}"
    );
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .index_by_schema_name(main, "by_a")
            .unwrap()
            .is_some()
    );
    catalog.close().await.unwrap();
}
