use std::{cell::Cell, time::Duration};

use super::{gated_store::GatedReadStore, *};

#[allow(clippy::unwrap_used)]
async fn seeded_files(files: usize) -> (Catalog, TableId, Arc<GatedReadStore>) {
    let catalog = open_memory().await;
    let data = Arc::new(InMemory::new());
    let mut registrations = Vec::new();
    for file in 0..files {
        let first = i64::try_from(file).unwrap() * 10;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![first, first + 1]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let footer_size = u64::from(u32::from_le_bytes(
            bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap(),
        ));
        let path = format!("source-{file}.parquet");
        let file_size_bytes = u64::try_from(bytes.len()).unwrap();
        data.put(&Path::from(format!("main/orders/{path}")), bytes.into())
            .await
            .unwrap();
        registrations.push(moraine::DataFile {
            path,
            file_size_bytes,
            footer_size,
            ..datafile(2)
        });
    }
    let created = Cell::new(None);
    catalog
        .commit(|tx| {
            let main = tx.schema_by_name("main").unwrap().id;
            let table = tx.create_table(main, "orders", &[col("a")])?;
            for file in &registrations {
                tx.register_data_file(table, file.clone(), &[])?;
            }
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();
    let gate = Arc::new(GatedReadStore::new(
        data,
        Path::from(format!("main/orders/source-{}.parquet", files - 1)),
    ));
    (catalog, created.get().unwrap(), gate)
}

#[tokio::test]
async fn missing_data_store_is_refused_before_creating_a_definition() {
    let (catalog, table, _) = seeded_files(1).await;
    let before = head(&catalog).await;
    let result = catalog
        .create_index_staged(table, &def(true), &[], None, "", None)
        .await;
    assert!(matches!(result, Err(Error::Constraint(_))), "{result:?}");
    assert_eq!(head(&catalog).await, before);
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .indexes_of(table)
            .is_empty()
    );
}

#[allow(clippy::unwrap_used)]
async fn expiry_during_build(files: usize, step: usize) {
    let (catalog, table, gate) = seeded_files(files).await;
    let snapshot = catalog.snapshot().await.unwrap();
    let expired = snapshot.data_files_of(table).last().unwrap().id;
    let definition = def(true);
    let build = Box::pin(catalog.create_index_staged(
        table,
        &definition,
        &[],
        Some(DataStore::new(gate.clone())),
        "",
        Some(by_entries(step)),
    ));
    let expire = async {
        gate.arrival().await;
        if files > 1 {
            let snapshot = catalog.snapshot().await.unwrap();
            let index = snapshot.index_by_name(table, "by_a").unwrap();
            assert_eq!(
                index.build_cursor,
                Some(0),
                "the first step must already be durable"
            );
        }
        catalog
            .commit(|tx| tx.expire_data_file(table, expired))
            .await
            .unwrap();
        gate.open();
    };
    let (index, ()) = Box::pin(tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(build, expire)
    }))
    .await
    .unwrap();
    let index = index.unwrap();
    let first_expired = i128::try_from(files - 1).unwrap() * 10;
    for value in [first_expired, first_expired + 1] {
        assert!(
            catalog
                .index_lookup(table, index, &[int(value)])
                .await
                .unwrap()
                .is_empty(),
            "expired value {value} was restored"
        );
    }
    if files > 1 {
        assert_eq!(
            catalog
                .index_lookup(table, index, &[int(1)])
                .await
                .unwrap()
                .len(),
            1
        );
    }
    catalog
        .commit(|tx| {
            tx.register_data_file(
                table,
                moraine::DataFile {
                    path: "replacement.parquet".into(),
                    ..datafile(1)
                },
                &[moraine::FileIndexEntry {
                    index,
                    ordinal: 0,
                    values: vec![Some(int(first_expired))],
                }],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn final_step_does_not_restore_expired_rows() {
    Box::pin(expiry_during_build(1, 10)).await;
}

#[tokio::test]
async fn intermediate_step_retries_its_whole_derivation() {
    Box::pin(expiry_during_build(1, 1)).await;
}

#[tokio::test]
async fn retries_resume_only_from_durable_progress() {
    Box::pin(expiry_during_build(2, 1)).await;
}

#[allow(clippy::unwrap_used)]
fn inline_chunk(value: i64) -> moraine::InlineChunk {
    use arrow::ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, StreamWriter,
    };

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .unwrap();
    let mut arrow_schema = Vec::new();
    StreamWriter::try_new(&mut arrow_schema, &schema)
        .unwrap()
        .finish()
        .unwrap();
    let (_, encoded) = IpcDataGenerator::default()
        .encode(
            &batch,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
            &mut IpcWriteContext::default(),
        )
        .unwrap();
    let mut arrow_body = u32::try_from(encoded.ipc_message.len())
        .unwrap()
        .to_le_bytes()
        .to_vec();
    arrow_body.extend(encoded.ipc_message);
    arrow_body.extend(encoded.arrow_data);
    moraine::InlineChunk {
        schema_version: 0,
        arrow_schema,
        arrow_body,
        row_count: 1,
    }
}

#[tokio::test]
async fn a_buffered_inline_row_is_not_restored_after_deletion() {
    let (catalog, table, gate) = seeded_files(1).await;
    catalog
        .commit(|tx| tx.inline_insert(table, &inline_chunk(99), &[]).map(|_| ()))
        .await
        .unwrap();
    let definition = def(true);
    let build = Box::pin(catalog.create_index_staged(
        table,
        &definition,
        &[],
        Some(DataStore::new(gate.clone())),
        "",
        Some(by_entries(10)),
    ));
    let delete = async {
        gate.arrival().await;
        catalog
            .commit(|tx| {
                let index = tx.index_by_name(table, "by_a").unwrap().id;
                tx.inline_delete(
                    table,
                    2,
                    &[moraine::FileIndexRemoval {
                        index,
                        row_id: 2,
                        values: vec![Some(int(99))],
                    }],
                )
            })
            .await
            .unwrap();
        gate.open();
    };
    let (index, ()) = Box::pin(tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(build, delete)
    }))
    .await
    .unwrap();
    let index = index.unwrap();
    assert!(
        catalog
            .index_lookup(table, index, &[int(99)])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        catalog
            .index_lookup(table, index, &[int(0)])
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        catalog
            .index_lookup(table, index, &[int(1)])
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn empty_and_inline_only_tables_need_no_data_store() {
    for inline in [false, true] {
        let (catalog, table, _) = seeded_files(1).await;
        let file = catalog.snapshot().await.unwrap().data_files_of(table)[0].id;
        catalog
            .commit(|tx| {
                tx.expire_data_file(table, file)?;
                if inline {
                    tx.inline_insert(table, &inline_chunk(99), &[])?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let index = catalog
            .create_index_staged(table, &def(true), &[], None, "", None)
            .await
            .unwrap();
        assert_eq!(
            catalog
                .index_lookup(table, index, &[int(99)])
                .await
                .unwrap()
                .len(),
            usize::from(inline)
        );
    }
}

#[tokio::test]
async fn a_missing_store_does_not_discard_an_existing_build() {
    let (catalog, table, _) = seeded_files(1).await;
    catalog
        .commit(|tx| tx.create_index_staged(table, &def(true)).map(|_| ()))
        .await
        .unwrap();
    let before = head(&catalog).await;
    let result = catalog
        .create_index_staged(table, &def(true), &[], None, "", None)
        .await;
    assert!(matches!(result, Err(Error::Constraint(_))));
    assert_eq!(head(&catalog).await, before);
    assert_eq!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .index_by_name(table, "by_a")
            .unwrap()
            .state,
        IndexState::Building
    );
}
