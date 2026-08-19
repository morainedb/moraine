//! Typed reads over the `inline/*` keyspace: chunks, tombstones, and
//! per-schema-version schema records. Mirrors `store::read`'s decode-only
//! contract — no DuckLake interpretation here.

use moraine_wal::Overlay;

use crate::{
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{
            InlineKey, InlineOperation, InlineOperationKind, Key, inline_live_table_prefix,
            inline_schema_prefix, inline_schema_table_prefix,
        },
        proto::{
            InlineChunkValue, InlineFileDeleteTableValue, InlineFileDeleteValue,
            InlineInlineDeleteValue, InlineSchemaValue,
        },
        read::{read_singleton, scan_decode_maybe},
        value,
    },
};

/// Every inlined-insert chunk for `table_id`, across all schema versions,
/// in key order (schema version, then commit snapshot, then chunk
/// sequence). `overlay` merges the unfolded tail on a slot-backed attach.
pub(crate) async fn scan_inline_chunks(
    handle: ReadHandle<'_>,
    overlay: Option<&Overlay>,
    table_id: u64,
) -> Result<Vec<(InlineOperation, InlineChunkValue)>> {
    scan_decode_maybe(
        handle,
        overlay,
        inline_live_table_prefix(InlineOperationKind::Insert, table_id),
        |key, bytes| match key {
            Key::Inline(InlineKey::Live(op @ InlineOperation::Insert { .. })) => {
                Ok((op, value::decode_value(bytes)?))
            }
            other => Err(Error::Corruption(format!(
                "non-insert key in inline chunk scan: {other:?}"
            ))),
        },
    )
    .await
}

/// Every inlined-insert-row tombstone for `table_id`, keyed by row id.
pub(crate) async fn scan_inline_inline_deletes(
    handle: ReadHandle<'_>,
    overlay: Option<&Overlay>,
    table_id: u64,
) -> Result<Vec<(u64, InlineInlineDeleteValue)>> {
    scan_decode_maybe(
        handle,
        overlay,
        inline_live_table_prefix(InlineOperationKind::InlineDelete, table_id),
        |key, bytes| match key {
            Key::Inline(InlineKey::Live(InlineOperation::InlineDelete { row_id, .. })) => {
                Ok((row_id, value::decode_value(bytes)?))
            }
            other => Err(Error::Corruption(format!(
                "non-inline_delete key in inline inline_delete scan: {other:?}"
            ))),
        },
    )
    .await
}

/// Every inlined Parquet-row delete for `table_id`, keyed by
/// `(data_file_id, row_id)`.
pub(crate) async fn scan_inline_file_deletes(
    handle: ReadHandle<'_>,
    overlay: Option<&Overlay>,
    table_id: u64,
) -> Result<Vec<(u64, u64, InlineFileDeleteValue)>> {
    scan_decode_maybe(
        handle,
        overlay,
        inline_live_table_prefix(InlineOperationKind::FileDelete, table_id),
        |key, bytes| match key {
            Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
                data_file_id,
                row_id,
                ..
            })) => Ok((data_file_id, row_id, value::decode_value(bytes)?)),
            other => Err(Error::Corruption(format!(
                "non-file_delete key in inline file delete scan: {other:?}"
            ))),
        },
    )
    .await
}

/// Whether `table_id`'s `ducklake_inlined_delete_<table_id>` has been
/// marked as existing.
pub(crate) async fn read_inline_file_delete_table(
    handle: ReadHandle<'_>,
    overlay: Option<&Overlay>,
    table_id: u64,
) -> Result<bool> {
    let key = Key::Inline(InlineKey::FileDeleteTable { table_id });
    if let Some(overlay) = overlay {
        match overlay.get(&key.encode()) {
            Some(Some(_)) => return Ok(true),
            Some(None) => return Ok(false),
            None => {}
        }
    }
    let marker: Option<InlineFileDeleteTableValue> = read_singleton(handle, key).await?;
    Ok(marker.is_some())
}

/// One table's Arrow IPC schema at `schema_version`, if recorded. `overlay`
/// resolves a tail write or tombstone ahead of the store on a slot-backed
/// attach.
pub(crate) async fn read_inline_schema(
    handle: ReadHandle<'_>,
    overlay: Option<&Overlay>,
    table_id: u64,
    schema_version: u64,
) -> Result<Option<InlineSchemaValue>> {
    let key = Key::Inline(InlineKey::Schema {
        table_id,
        schema_version,
    });
    if let Some(overlay) = overlay {
        match overlay.get(&key.encode()) {
            Some(Some(bytes)) => return Ok(Some(value::decode_value(bytes)?)),
            Some(None) => return Ok(None),
            None => {}
        }
    }
    read_singleton(handle, key).await
}

/// Every schema version recorded for `table_id`, in key order.
pub(crate) async fn scan_inline_schemas(
    handle: ReadHandle<'_>,
    overlay: Option<&Overlay>,
    table_id: u64,
) -> Result<Vec<(u64, InlineSchemaValue)>> {
    scan_decode_maybe(
        handle,
        overlay,
        inline_schema_table_prefix(table_id),
        |key, bytes| match key {
            Key::Inline(InlineKey::Schema { schema_version, .. }) => {
                Ok((schema_version, value::decode_value(bytes)?))
            }
            other => Err(Error::Corruption(format!(
                "non-schema key in inline schema scan: {other:?}"
            ))),
        },
    )
    .await
}

/// Every `inline/schema` record across every table, in key order
/// (`table_id`, then `schema_version`).
pub(crate) async fn scan_all_inline_schemas(
    handle: ReadHandle<'_>,
    overlay: Option<&Overlay>,
) -> Result<Vec<(u64, u64, InlineSchemaValue)>> {
    scan_decode_maybe(
        handle,
        overlay,
        inline_schema_prefix(),
        |key, bytes| match key {
            Key::Inline(InlineKey::Schema {
                table_id,
                schema_version,
            }) => Ok((table_id, schema_version, value::decode_value(bytes)?)),
            other => Err(Error::Corruption(format!(
                "non-schema key in all-table inline schema scan: {other:?}"
            ))),
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::store::open::StoreBuilder;

    /// Seeds inline records for two tables and one that overlaps chunk
    /// sequence numbers across two schema versions, then asserts every
    /// scan returns exactly its table's records, key-ordered, and that
    /// point reads work.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn scans_return_inline_records_in_key_order() {
        let db = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();

        let schema_v0 = InlineSchemaValue {
            arrow_schema: b"schema-v0".to_vec(),
        };
        let schema_v1 = InlineSchemaValue {
            arrow_schema: b"schema-v1".to_vec(),
        };

        let chunk_v0_seq0 = InlineChunkValue {
            body: b"chunk-v0-0".to_vec(),
            row_id_start: 0,
            row_count: 2,
            data_file_id: None,
        };
        let chunk_v0_seq1 = InlineChunkValue {
            body: b"chunk-v0-1".to_vec(),
            row_id_start: 2,
            row_count: 3,
            data_file_id: None,
        };
        let chunk_v1_seq0 = InlineChunkValue {
            body: b"chunk-v1-0".to_vec(),
            row_id_start: 5,
            row_count: 1,
            data_file_id: None,
        };
        let other_table_chunk = InlineChunkValue {
            body: b"other-table".to_vec(),
            row_id_start: 0,
            row_count: 1,
            data_file_id: None,
        };

        let inline_delete = InlineInlineDeleteValue { end_snapshot: 9 };
        let file_delete = InlineFileDeleteValue { begin_snapshot: 4 };

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            Key::Inline(InlineKey::Schema {
                table_id: 7,
                schema_version: 0,
            })
            .encode(),
            value::encode_value(&schema_v0),
        )
        .unwrap();
        tx.put(
            Key::Inline(InlineKey::Schema {
                table_id: 7,
                schema_version: 1,
            })
            .encode(),
            value::encode_value(&schema_v1),
        )
        .unwrap();
        tx.put(
            Key::Inline(InlineKey::Live(InlineOperation::Insert {
                table_id: 7,
                schema_version: 0,
                begin_snapshot: 1,
                chunk_seq: 0,
            }))
            .encode(),
            value::encode_value(&chunk_v0_seq0),
        )
        .unwrap();
        tx.put(
            Key::Inline(InlineKey::Live(InlineOperation::Insert {
                table_id: 7,
                schema_version: 0,
                begin_snapshot: 1,
                chunk_seq: 1,
            }))
            .encode(),
            value::encode_value(&chunk_v0_seq1),
        )
        .unwrap();
        tx.put(
            Key::Inline(InlineKey::Live(InlineOperation::Insert {
                table_id: 7,
                schema_version: 1,
                begin_snapshot: 2,
                chunk_seq: 0,
            }))
            .encode(),
            value::encode_value(&chunk_v1_seq0),
        )
        .unwrap();
        tx.put(
            Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
                table_id: 7,
                row_id: 3,
            }))
            .encode(),
            value::encode_value(&inline_delete),
        )
        .unwrap();
        tx.put(
            Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
                table_id: 7,
                data_file_id: 5,
                row_id: 1,
            }))
            .encode(),
            value::encode_value(&file_delete),
        )
        .unwrap();
        // A different table's chunk must not leak into table 7's scans.
        tx.put(
            Key::Inline(InlineKey::Live(InlineOperation::Insert {
                table_id: 8,
                schema_version: 0,
                begin_snapshot: 1,
                chunk_seq: 0,
            }))
            .encode(),
            value::encode_value(&other_table_chunk),
        )
        .unwrap();

        crate::transaction::commit::commit_durably(tx)
            .await
            .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();

        let chunks = scan_inline_chunks(ReadHandle::Tx(&tx), None, 7)
            .await
            .unwrap();
        assert_eq!(
            chunks,
            vec![
                (
                    InlineOperation::Insert {
                        table_id: 7,
                        schema_version: 0,
                        begin_snapshot: 1,
                        chunk_seq: 0,
                    },
                    chunk_v0_seq0,
                ),
                (
                    InlineOperation::Insert {
                        table_id: 7,
                        schema_version: 0,
                        begin_snapshot: 1,
                        chunk_seq: 1,
                    },
                    chunk_v0_seq1,
                ),
                (
                    InlineOperation::Insert {
                        table_id: 7,
                        schema_version: 1,
                        begin_snapshot: 2,
                        chunk_seq: 0,
                    },
                    chunk_v1_seq0,
                ),
            ]
        );

        let inline_deletes = scan_inline_inline_deletes(ReadHandle::Tx(&tx), None, 7)
            .await
            .unwrap();
        assert_eq!(inline_deletes, vec![(3, inline_delete)]);

        let file_deletes = scan_inline_file_deletes(ReadHandle::Tx(&tx), None, 7)
            .await
            .unwrap();
        assert_eq!(file_deletes, vec![(5, 1, file_delete)]);

        let schemas = scan_inline_schemas(ReadHandle::Tx(&tx), None, 7)
            .await
            .unwrap();
        assert_eq!(
            schemas,
            vec![(0, schema_v0.clone()), (1, schema_v1.clone())]
        );

        assert_eq!(
            read_inline_schema(ReadHandle::Tx(&tx), None, 7, 0)
                .await
                .unwrap(),
            Some(schema_v0)
        );
        assert_eq!(
            read_inline_schema(ReadHandle::Tx(&tx), None, 7, 1)
                .await
                .unwrap(),
            Some(schema_v1)
        );
        assert_eq!(
            read_inline_schema(ReadHandle::Tx(&tx), None, 7, 2)
                .await
                .unwrap(),
            None
        );

        let other_table_chunks = scan_inline_chunks(ReadHandle::Tx(&tx), None, 8)
            .await
            .unwrap();
        assert_eq!(
            other_table_chunks,
            vec![(
                InlineOperation::Insert {
                    table_id: 8,
                    schema_version: 0,
                    begin_snapshot: 1,
                    chunk_seq: 0,
                },
                other_table_chunk,
            )]
        );

        tx.rollback();
        db.close().await.unwrap();
    }

    /// `scan_all_inline_schemas` returns every table's schema records
    /// together, in `(table_id, schema_version)` order, unlike the
    /// table-scoped `scan_inline_schemas`.
    #[tokio::test]
    async fn scan_all_inline_schemas_covers_every_table() {
        let db = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();

        let table_one_v0 = InlineSchemaValue {
            arrow_schema: b"a-0".to_vec(),
        };
        let table_one_v1 = InlineSchemaValue {
            arrow_schema: b"a-1".to_vec(),
        };
        let table_two_v0 = InlineSchemaValue {
            arrow_schema: b"b-0".to_vec(),
        };

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for (table_id, schema_version, value) in [
            (1, 0, &table_one_v0),
            (1, 1, &table_one_v1),
            (2, 0, &table_two_v0),
        ] {
            tx.put(
                Key::Inline(InlineKey::Schema {
                    table_id,
                    schema_version,
                })
                .encode(),
                value::encode_value(value),
            )
            .unwrap();
        }
        crate::transaction::commit::commit_durably(tx)
            .await
            .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let all = scan_all_inline_schemas(ReadHandle::Tx(&tx), None)
            .await
            .unwrap();
        assert_eq!(
            all,
            vec![
                (1, 0, table_one_v0),
                (1, 1, table_one_v1),
                (2, 0, table_two_v0),
            ]
        );
        tx.rollback();
        db.close().await.unwrap();
    }
}
