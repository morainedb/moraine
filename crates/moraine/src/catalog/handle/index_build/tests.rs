use std::{cell::Cell, sync::Arc};

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
    ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, StreamWriter,
    },
};
use object_store::memory::InMemory;

use super::*;
use crate::{CatalogOptions, ColumnDef, IndexEntry, InlineChunk};

fn chunk(values: Vec<i64>) -> InlineChunk {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let count = values.len();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
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
    InlineChunk {
        schema_version: 0,
        row_count: count as u64,
        arrow_schema,
        arrow_body,
    }
}

async fn fixture(chunks: &[InlineChunk]) -> (Catalog, TableId, IndexDef) {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let table = Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").unwrap().id;
            let id = tx.create_table(
                schema,
                "inline_build",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    ..Default::default()
                }],
            )?;
            for chunk in chunks {
                tx.inline_insert(id, chunk, &[])?;
            }
            table.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let table = table.get().unwrap();
    let column = catalog.snapshot().await.unwrap().columns_of(table)[0].id;
    let def = IndexDef {
        name: "by_a".into(),
        columns: vec![column],
        unique: true,
    };
    (catalog, table, def)
}

#[tokio::test]
async fn inline_steps_commit_before_later_chunks_are_decoded() {
    let mut invalid = chunk(vec![30]);
    invalid.arrow_body = b"invalid".to_vec();
    let (catalog, table, def) = fixture(&[chunk(vec![10, 20]), invalid]).await;
    let index = catalog
        .begin_staged_index(table, &def, &[], IndexMaintenance::Synchronous, false)
        .await
        .unwrap();
    let before = catalog.snapshot().await.unwrap().current_snapshot().id;
    let result = catalog
        .drive_staged_build(
            table,
            &def,
            index,
            None,
            "",
            BuildStep {
                entries: 1,
                bytes: 1024,
            },
        )
        .await;
    assert!(matches!(result, Err(Error::Corruption(_))), "{result:?}");
    assert!(
        catalog.snapshot().await.unwrap().current_snapshot().id > before,
        "inline derivation decoded a later chunk before committing its first step"
    );
    catalog.close().await.unwrap();
}

fn integer(value: i128) -> crate::IndexKeyValue {
    crate::IndexKeyValue::Int {
        value,
        width: crate::IntWidth::I64,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn inline_resume_skips_completed_chunks_without_a_row_watermark() {
    use crate::store::{
        key::{InlineKey, InlineOperation, Key},
        proto, value,
    };

    let mut first = chunk(vec![10, 20]);
    first.schema_version = 1;
    let (catalog, table, def) = fixture(&[first, chunk(vec![30, 40])]).await;
    let begin = catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .id
        .get();
    let index = catalog
        .begin_staged_index(table, &def, &[], IndexMaintenance::Synchronous, false)
        .await
        .unwrap();
    let cursor = InlineBuildCursorValue {
        schema_version: 0,
        begin_snapshot: begin,
        chunk_seq: 0,
        next_position: 2,
        chunk_finished: true,
        ..Default::default()
    };
    catalog
        .commit(|tx| {
            tx.build_index_source_step(
                index,
                &[
                    IndexEntry {
                        row_id: 2,
                        values: vec![Some(integer(30))],
                    },
                    IndexEntry {
                        row_id: 3,
                        values: vec![Some(integer(40))],
                    },
                ],
                false,
                None,
                Some(&cursor),
            )
            .map(|_| ())
        })
        .await
        .unwrap();
    assert_eq!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .index_by_name(table, &def.name)
            .unwrap()
            .build_cursor,
        None,
        "a row watermark would skip the lower ids in the remaining source"
    );

    let transaction = catalog.begin_write_tx().await.unwrap();
    transaction
        .put(
            Key::Inline(InlineKey::Live(InlineOperation::Insert {
                table_id: table.get(),
                schema_version: 0,
                begin_snapshot: begin,
                chunk_seq: 0,
            }))
            .encode(),
            value::encode_value(&proto::InlineChunkValue {
                body: b"already covered".to_vec().into(),
                row_id_start: 2,
                row_count: 2,
                data_file_id: None,
            }),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    let resumed = catalog
        .create_index_staged(
            table,
            &def,
            &[],
            None,
            "",
            Some(BuildStep {
                entries: 1,
                bytes: 1024,
            }),
        )
        .await
        .unwrap();
    assert_eq!(resumed, index);
    for (value, row) in [(10, 0), (20, 1), (30, 2), (40, 3)] {
        assert_eq!(
            catalog
                .index_lookup(table, index, &[integer(value)])
                .await
                .unwrap(),
            [row]
        );
    }
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn inline_resume_inside_a_chunk_excludes_tombstones() {
    let (catalog, table, def) = fixture(&[chunk(vec![10, 20, 30, 40])]).await;
    let begin = catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .id
        .get();
    catalog
        .commit(|tx| tx.inline_delete(table, 2, &[]))
        .await
        .unwrap();
    let index = catalog
        .begin_staged_index(table, &def, &[], IndexMaintenance::Synchronous, false)
        .await
        .unwrap();
    let cursor = InlineBuildCursorValue {
        begin_snapshot: begin,
        next_position: 1,
        ..Default::default()
    };
    catalog
        .commit(|tx| {
            tx.build_index_source_step(
                index,
                &[IndexEntry {
                    row_id: 0,
                    values: vec![Some(integer(10))],
                }],
                false,
                None,
                Some(&cursor),
            )
            .map(|_| ())
        })
        .await
        .unwrap();
    catalog
        .create_index_staged(
            table,
            &def,
            &[],
            None,
            "",
            Some(BuildStep {
                entries: 1,
                bytes: 1024,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        catalog
            .index_range(
                table,
                index,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                false
            )
            .await
            .unwrap(),
        [0, 1, 3]
    );
    assert!(
        catalog
            .index_lookup(table, index, &[integer(30)])
            .await
            .unwrap()
            .is_empty()
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_inline_build_resumes_its_committed_checkpoint() {
    let chunks: Vec<_> = (0..32)
        .map(|source| chunk((source * 4..source * 4 + 4).collect()))
        .collect();
    let (catalog, table, def) = fixture(&chunks).await;
    let mut build = Box::pin(catalog.create_index_staged(
        table,
        &def,
        &[],
        None,
        "",
        Some(BuildStep {
            entries: 1,
            bytes: 1024,
        }),
    ));
    let checkpoint = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let snapshot = catalog.snapshot().await.unwrap();
            if let Some(index) = snapshot.index_by_name(table, &def.name)
                && snapshot.indexes[&table.get()][&index.id.get()]
                    .build_inline_cursor
                    .is_some()
            {
                assert_eq!(index.state, IndexState::Building);
                break index.id;
            }
            tokio::task::yield_now().await;
        }
    });
    let index = tokio::select! {
        result = &mut build => panic!("build finished before cancellation: {result:?}"),
        index = checkpoint => index.unwrap(),
    };
    drop(build);
    let resumed = catalog
        .create_index_staged(
            table,
            &def,
            &[],
            None,
            "",
            Some(BuildStep {
                entries: 16,
                bytes: 4096,
            }),
        )
        .await
        .unwrap();
    assert_eq!(resumed, index);
    let rows = catalog
        .index_range(
            table,
            index,
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
            false,
        )
        .await
        .unwrap();
    assert!(rows.into_iter().eq(0..128));
    catalog.close().await.unwrap();
}

/// A resumed build learns the keys its earlier steps committed before it
/// skips any probe, so a later row duplicating a committed value is caught.
#[tokio::test]
async fn resumed_build_still_detects_a_duplicate_of_a_committed_row() {
    let (catalog, table, def) = fixture(&[chunk(vec![10, 20, 10])]).await;
    let begin = catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .id
        .get();
    let index = catalog
        .begin_staged_index(table, &def, &[], IndexMaintenance::Synchronous, false)
        .await
        .unwrap();
    let cursor = InlineBuildCursorValue {
        begin_snapshot: begin,
        next_position: 1,
        ..Default::default()
    };
    catalog
        .commit(|tx| {
            tx.build_index_source_step(
                index,
                &[IndexEntry {
                    row_id: 0,
                    values: vec![Some(integer(10))],
                }],
                false,
                None,
                Some(&cursor),
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let result = catalog
        .create_index_staged(
            table,
            &def,
            &[],
            None,
            "",
            Some(BuildStep {
                entries: 16,
                bytes: 4096,
            }),
        )
        .await;

    assert!(matches!(result, Err(Error::Constraint(_))), "{result:?}");
    catalog.close().await.unwrap();
}
