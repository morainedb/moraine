//! A worked tour: open a catalog on (in-memory) object storage, evolve a
//! schema across commits, and time-travel back through the history.
//!
//! Run with: `cargo run -p moraine --example walkthrough`

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, Error, Result};
use object_store::memory::InMemory;

fn bigint(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
        children: Vec::new(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // A deployment is a bucket and credentials; here, in-memory.
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;

    let v1 = catalog
        .commit(async |tx| {
            let sales = tx.create_schema("sales").await?;
            tx.create_table(sales, "orders", &[bigint("id"), bigint("qty")])
                .await?;

            Ok(())
        })
        .await?;

    let v2 = catalog
        .commit(async |tx| {
            let sales = tx
                .schema_by_name("sales")
                .await?
                .ok_or_else(|| Error::NotFound("schema sales".to_string()))?;
            let orders = tx
                .table_by_name(sales.id, "orders")
                .await?
                .ok_or_else(|| Error::NotFound("table orders".to_string()))?;
            tx.add_column(orders.id, &bigint("discount")).await?;
            tx.rename_table(orders.id, "orders_v2").await?;

            Ok(())
        })
        .await?;

    // The head sees the evolved shape…
    let head = catalog.read_scope().await?;
    let sales = head
        .schema_by_name("sales")
        .await?
        .ok_or_else(|| Error::NotFound("schema sales".to_string()))?;
    for table in head.tables_in(sales.id).await? {
        println!(
            "at head:  {} ({} columns)",
            table.name,
            head.columns_of(table.id).await?.len()
        );
    }

    // …while the history remains queryable at every snapshot.
    for snapshot in [v1, v2] {
        let past = catalog.read_scope_at(snapshot).await?;
        let sales = past
            .schema_by_name("sales")
            .await?
            .ok_or_else(|| Error::NotFound("schema sales".to_string()))?;
        let mut names = Vec::new();
        for table in past.tables_in(sales.id).await? {
            names.push(format!(
                "{} ({} cols)",
                table.name,
                past.columns_of(table.id).await?.len()
            ));
        }
        println!("at {snapshot:?}: {}", names.join(", "));
    }

    catalog.close().await
}
