//! `Catalog::scoped_backfill_entries`: deriving index entries for a
//! table's registered files by scoped-reading them from a data store.

use std::sync::Arc;

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use moraine::{ColumnId, IndexKeyValue, IntWidth};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::ArrowWriter;

use crate::fixtures::{col, datafile, open_memory};

#[tokio::test]
async fn scoped_backfill_reads_registered_files_from_the_data_store() {
    let catalog = open_memory().await;
    // The data store holds the registered file under the table's data
    // directory (`<schema path><table path>` = `main/orders/`).
    let data = Arc::new(InMemory::new());
    let arrow_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
    )
    .unwrap();
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let footer_offset = buffer.len() - 8;
    let footer_size = u64::from(u32::from_le_bytes(
        buffer[footer_offset..footer_offset + 4].try_into().unwrap(),
    ));
    let file_size_bytes = u64::try_from(buffer.len()).unwrap();
    data.put(&Path::from("main/orders/data-3.parquet"), buffer.into())
        .await
        .unwrap();

    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "orders", &[col("a")])?;
            tx.register_data_file(
                table,
                moraine::DataFile {
                    file_size_bytes,
                    footer_size,
                    ..datafile(3)
                },
                &[],
            )?;
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();
    let table = created.get().unwrap();

    let entries = catalog
        .scoped_backfill_entries(
            moraine::DataStore::new(data),
            "",
            table,
            &[ColumnId::new(1)],
        )
        .await
        .unwrap();
    assert_eq!(entries.len(), 3);
    for (entry, (row_id, value)) in entries.iter().zip([(0, 10), (1, 20), (2, 30)]) {
        assert_eq!(entry.row_id, row_id);
        assert_eq!(
            entry.values,
            vec![Some(IndexKeyValue::Int {
                value,
                width: IntWidth::I64,
            })]
        );
    }
    catalog.close().await.unwrap();
}
