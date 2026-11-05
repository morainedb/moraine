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
        .commit(async |tx| {
            let main = tx.schema_by_name("main").await.unwrap().unwrap().id;
            let other = tx.create_schema("other").await?;
            for (schema, table_name) in [(main, "one"), (main, "two"), (other, "three")] {
                let table = tx
                    .create_table(
                        schema,
                        table_name,
                        &[ColumnDef {
                            name: "a".into(),
                            column_type: "BIGINT".into(),
                            ..Default::default()
                        }],
                    )
                    .await?;
                let definition = IndexDef {
                    name: if table_name == "two" { "BY_A" } else { "by_a" }.into(),
                    columns: vec![tx.columns_of(table).await.unwrap()[0].id],
                    unique: false,
                };
                if table_name == "two" {
                    assert!(matches!(
                        tx.create_index(table, &definition, &[]).await,
                        Err(Error::AlreadyExists(_))
                    ));
                } else {
                    tx.create_index(table, &definition, &[]).await?;
                }
            }
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = catalog.read_scope().await.unwrap();
    let main = snapshot.schema_by_name("main").await.unwrap().unwrap().id;
    let found = snapshot
        .index_by_schema_name(main, "BY_A")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        found.table_id,
        snapshot
            .table_by_name(main, "one")
            .await
            .unwrap()
            .unwrap()
            .id
    );
    assert!(
        snapshot
            .index_by_schema_name(main, "missing")
            .await
            .unwrap()
            .is_none()
    );
    let one = snapshot
        .table_by_name(main, "one")
        .await
        .unwrap()
        .unwrap()
        .id;
    let two = snapshot
        .table_by_name(main, "two")
        .await
        .unwrap()
        .unwrap()
        .id;
    let definition = async |table| IndexDef {
        name: "raced".into(),
        columns: vec![snapshot.columns_of(table).await.unwrap()[0].id],
        unique: false,
    };
    let (first, second) = tokio::join!(
        catalog.commit(async |tx| tx
            .create_index(one, &definition(one).await, &[])
            .await
            .map(|_| ())),
        catalog.commit(async |tx| tx
            .create_index(two, &definition(two).await, &[])
            .await
            .map(|_| ())),
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
        .commit(async |tx| {
            let main = tx.schema_by_name("main").await.unwrap().unwrap().id;
            let other = tx.create_schema("other").await?;
            for (schema, name) in [(main, "one"), (other, "two")] {
                let table = tx
                    .create_table(
                        schema,
                        name,
                        &[ColumnDef {
                            name: "a".into(),
                            column_type: "BIGINT".into(),
                            ..Default::default()
                        }],
                    )
                    .await?;
                let column = tx.columns_of(table).await?[0].id;
                tx.create_index(
                    table,
                    &IndexDef {
                        name: if name == "one" { "by_a" } else { "BY_A" }.into(),
                        columns: vec![column],
                        unique: false,
                    },
                    &[],
                )
                .await?;
            }
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = catalog.read_scope().await.unwrap();
    let main = snapshot.schema_by_name("main").await.unwrap().unwrap().id;
    let other = snapshot.schema_by_name("other").await.unwrap().unwrap().id;
    let one = snapshot
        .table_by_name(main, "one")
        .await
        .unwrap()
        .unwrap()
        .id;
    let two = snapshot
        .table_by_name(other, "two")
        .await
        .unwrap()
        .unwrap()
        .id;
    let result = catalog
        .commit(async |tx| tx.set_table_schema(two, main).await)
        .await;
    assert!(matches!(result, Err(Error::AlreadyExists(_))), "{result:?}");
    assert_eq!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .table_by_name(other, "two")
            .await
            .unwrap()
            .unwrap()
            .id,
        two
    );
    catalog
        .commit(async |tx| {
            let index = tx.index_by_name(one, "by_a").await?.unwrap().id;
            tx.drop_index(index).await?;
            tx.set_table_schema(two, main).await
        })
        .await
        .unwrap();
    assert_eq!(
        catalog
            .read_scope()
            .await
            .unwrap()
            .index_by_schema_name(main, "by_a")
            .await
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
        .commit(async |tx| {
            let main = tx.schema_by_name("main").await.unwrap().unwrap().id;
            let other = tx.create_schema("other").await?;
            for (schema, name) in [(main, "one"), (other, "two")] {
                let table = tx
                    .create_table(
                        schema,
                        name,
                        &[ColumnDef {
                            name: "a".into(),
                            column_type: "BIGINT".into(),
                            ..Default::default()
                        }],
                    )
                    .await?;
                if name == "two" {
                    let column = tx.columns_of(table).await?[0].id;
                    tx.create_index(
                        table,
                        &IndexDef {
                            name: "by_a".into(),
                            columns: vec![column],
                            unique: false,
                        },
                        &[],
                    )
                    .await?;
                }
            }
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = catalog.read_scope().await.unwrap();
    let main = snapshot.schema_by_name("main").await.unwrap().unwrap().id;
    let other = snapshot.schema_by_name("other").await.unwrap().unwrap().id;
    let one = snapshot
        .table_by_name(main, "one")
        .await
        .unwrap()
        .unwrap()
        .id;
    let two = snapshot
        .table_by_name(other, "two")
        .await
        .unwrap()
        .unwrap()
        .id;
    let definition = IndexDef {
        name: "BY_A".into(),
        columns: vec![snapshot.columns_of(one).await.unwrap()[0].id],
        unique: false,
    };
    let (moved, created) = tokio::join!(
        catalog.commit(async |tx| tx.set_table_schema(two, main).await),
        catalog.commit(async |tx| tx.create_index(one, &definition, &[]).await.map(|_| ())),
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
            .read_scope()
            .await
            .unwrap()
            .index_by_schema_name(main, "by_a")
            .await
            .unwrap()
            .is_some()
    );
    catalog.close().await.unwrap();
}
