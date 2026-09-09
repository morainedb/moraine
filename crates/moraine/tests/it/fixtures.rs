//! Fixtures shared across the suite's modules.
//!
//! `unwrap_used` is a library-code lint, not exempted automatically for a
//! plain (non-`#[test]`) function even in an integration-test crate, so
//! the async helpers carry targeted allows.

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field},
};
use moraine::{Catalog, CatalogOptions, ColumnDef, DataFile, SchemaId, TableId};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

/// A nullable BIGINT column.
pub fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
        children: Vec::new(),
    }
}

/// A parquet data file whose path and sizes derive from its row count.
pub fn datafile(rows: u64) -> DataFile {
    DataFile {
        path: format!("data-{rows}.parquet"),
        path_is_relative: true,
        file_format: "parquet".into(),
        record_count: rows,
        file_size_bytes: rows * 10,
        footer_size: 4,
        encryption_key: None,
        partition_values: vec![],
        column_stats: vec![],
    }
}

/// Opens a fresh catalog over in-memory object storage.
#[allow(clippy::unwrap_used)]
pub async fn open_memory() -> Catalog {
    Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap()
}

/// Opens a catalog pre-seeded with tables `a` and `b` in schema `s`.
#[allow(clippy::unwrap_used)]
pub async fn seeded() -> (Catalog, SchemaId, TableId, TableId) {
    let catalog = open_memory().await;
    catalog
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "a", &[col("x")])?;
            tx.create_table(s, "b", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = catalog.snapshot().await.unwrap();
    let s = snapshot.schema_by_name("s").unwrap().id;
    let a = snapshot.table_by_name(s, "a").unwrap().id;
    let b = snapshot.table_by_name(s, "b").unwrap().id;
    (catalog, s, a, b)
}

/// DuckLake's reserved row-id column, tagged so discovery finds it.
pub fn row_id_field() -> Field {
    Field::new("_ducklake_internal_row_id", DataType::Int64, false).with_metadata(HashMap::from([
        (
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2147483540".to_string(),
        ),
    ]))
}

/// Writes `batch` as one Parquet object and returns the sizes the catalog
/// records for it.
#[allow(clippy::unwrap_used)]
pub async fn write_parquet(store: &dyn ObjectStore, path: &str, batch: &RecordBatch) -> (u64, u64) {
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    let footer_offset = buffer.len() - 8;
    let footer_size = u64::from(u32::from_le_bytes(
        buffer[footer_offset..footer_offset + 4].try_into().unwrap(),
    ));
    let file_size = u64::try_from(buffer.len()).unwrap();
    store.put(&Path::from(path), buffer.into()).await.unwrap();
    (file_size, footer_size)
}
