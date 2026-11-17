//! A bounded disk cache, in a binary of its own.
//!
//! One cache serves a whole process and the first store to open settles
//! whether there is a disk device at all, so a test that asks for one
//! cannot share a binary with tests that measure object-store reads: it
//! would decide their cache topology by winning a race.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef};
use object_store::memory::InMemory;

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
        children: Vec::new(),
    }
}

#[tokio::test]
async fn a_bounded_disk_cache_serves_a_writer_and_a_reader() {
    let object_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let cache = std::env::temp_dir().join(format!(
        "moraine-bounded-cache-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&cache);

    let mut options = CatalogOptions::default();
    options.cache_dir = Some(cache.clone());
    options.cache_size = Some(64 * 1024 * 1024);

    let writer = Catalog::open(Arc::clone(&object_store), options.clone())
        .await
        .unwrap();
    writer
        .commit(async |tx| {
            let schema = tx
                .schema_by_name("main")
                .await
                .unwrap()
                .expect("bootstrap")
                .id;
            tx.create_table(schema, "t", &[col("a")]).await?;
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let reader = Catalog::open_read_only(object_store, options)
        .await
        .unwrap();
    let view = reader.read_scope().await.unwrap();
    let schema = view
        .schema_by_name("main")
        .await
        .unwrap()
        .expect("bootstrap")
        .id;
    assert!(view.table_by_name(schema, "t").await.unwrap().is_some());
    reader.close().await.unwrap();

    let _ = std::fs::remove_dir_all(&cache);
}
