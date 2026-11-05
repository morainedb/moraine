use std::sync::Arc;

use object_store::memory::InMemory;

use super::*;
use crate::{
    CatalogOptions, ColumnDef,
    store::key::{EntityKey, Key},
};

#[tokio::test]
async fn scoped_mutations_ignore_unrelated_statistics_and_preserve_options() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit_materialized(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            tx.create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    ..Default::default()
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let view = catalog.materialized().await.unwrap();
    let schema = view.schema_by_name("main").unwrap().id;
    let table = view.table_by_name(schema, "t").unwrap().id;
    let column = view.columns_of(table)[0].id;
    drop(view);
    let write = catalog.begin_write_tx().unwrap();
    write
        .put(
            Key::current(EntityKey::FileColumnStats {
                table_id: 999,
                data_file_id: 999,
                column_id: 1,
            })
            .encode(),
            b"unrelated invalid record",
        )
        .unwrap();
    catalog.commit_write_tx(write).await.unwrap();

    catalog
        .set_option(OptionScope::Global, "keep", "value")
        .await
        .unwrap();
    catalog.adopt_data_path("/lake/").await.unwrap();
    catalog.adopt_data_path("/lake").await.unwrap();
    assert!(matches!(
        catalog.adopt_data_path("/other/").await,
        Err(Error::Constraint(_))
    ));
    assert_eq!(
        crate::ffi_support::catalog_read::global_option(&catalog, "keep")
            .await
            .unwrap()
            .as_deref(),
        Some("value")
    );
    catalog
        .set_option(OptionScope::Table(table), "key", "table")
        .await
        .unwrap();
    assert!(matches!(
        catalog
            .set_option(OptionScope::Table(TableId::new(999)), "key", "missing")
            .await,
        Err(Error::NotFound(_))
    ));
    let definition = IndexDef {
        name: "by_a".into(),
        columns: vec![column],
        unique: false,
    };
    Box::pin(catalog.create_index(
        table,
        &definition,
        &[],
        IndexMaintenance::Synchronous,
        None,
        "",
    ))
    .await
    .unwrap();
    catalog.drop_index("main", "BY_A", false).await.unwrap();
    catalog.drop_index("main", "by_a", true).await.unwrap();
    assert!(matches!(
        catalog.drop_index("main", "by_a", false).await,
        Err(Error::NotFound(_))
    ));
    catalog.close().await.unwrap();
}
