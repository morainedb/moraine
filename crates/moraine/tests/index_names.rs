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
