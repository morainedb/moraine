//! The verb path's inline staging: what a commit closure accumulates in
//! the `inline` subspace, and its translation into store writes. Inline
//! records live outside the entity model and are never diffed.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use slatedb::DbTransaction;

use crate::{
    catalog::projection::ProjectionCache,
    error::{Error, Result},
    store::{handle::ReadHandle, inline as store_inline},
    transaction::{
        commit::StagedWrite,
        staged::inline::{
            inline_chunk_range_write, inline_inline_delete_write, inline_insert_write,
            inline_schema_write, translate_inline_flush_delete,
        },
    },
};

/// One inline-subspace mutation staged by a commit closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InlineStage {
    /// The Arrow IPC schema-only stream a version's chunks decode against.
    Schema {
        table_id: u64,
        schema_version: u64,
        arrow_schema: Vec<u8>,
    },
    /// One chunk of inlined rows.
    Insert {
        table_id: u64,
        schema_version: u64,
        begin_snapshot: u64,
        chunk_seq: u64,
        row_id_start: u64,
        row_count: u64,
        arrow_body: Vec<u8>,
    },
    /// A tombstone ending one inlined row.
    Tombstone {
        table_id: u64,
        row_id: u64,
        end_snapshot: u64,
    },
    /// A drain of every chunk of one `(table, schema_version)` committed
    /// below `flush_snapshot`, and of the tombstones those chunks consumed.
    Flush {
        table_id: u64,
        schema_version: u64,
        flush_snapshot: u64,
    },
}

/// Point reads one batch keeps in flight against the store.
const SCHEMA_READ_CONCURRENCY: usize = 16;

/// Per-table inline scans one batch's flushes keep in flight.
const FLUSH_SCAN_CONCURRENCY: usize = 8;

/// The recorded schema of every distinct version `ops` writes a schema
/// for, keyed by `(table_id, schema_version)`; read concurrently since the
/// key set is known up front.
async fn recorded_schemas(
    db_tx: &DbTransaction,
    ops: &[InlineStage],
) -> Result<HashMap<(u64, u64), Option<Bytes>>> {
    let mut versions = Vec::new();
    let mut seen = HashSet::new();
    for op in ops {
        if let InlineStage::Schema {
            table_id,
            schema_version,
            ..
        } = op
            && seen.insert((*table_id, *schema_version))
        {
            versions.push((*table_id, *schema_version));
        }
    }

    stream::iter(versions)
        .map(|(table_id, schema_version)| async move {
            let recorded =
                store_inline::read_inline_schema(ReadHandle::Tx(db_tx), table_id, schema_version)
                    .await?;
            Ok::<_, Error>(((table_id, schema_version), recorded))
        })
        .buffered(SCHEMA_READ_CONCURRENCY)
        .try_collect()
        .await
}

/// Every flush in `ops`, in op order, translated to its writes and the row
/// ids it drains. Flushes only read `db_tx`, so their table scans run
/// concurrently.
async fn translated_flushes(
    db_tx: &DbTransaction,
    projections: &std::sync::RwLock<ProjectionCache>,
    ops: &[InlineStage],
) -> Result<Vec<(Vec<StagedWrite>, HashSet<u64>)>> {
    let flushes: Vec<(u64, u64, u64)> = ops
        .iter()
        .filter_map(|op| match op {
            InlineStage::Flush {
                table_id,
                schema_version,
                flush_snapshot,
            } => Some((*table_id, *schema_version, *flush_snapshot)),
            _ => None,
        })
        .collect();

    stream::iter(flushes)
        .map(|(table_id, schema_version, flush_snapshot)| async move {
            let mut writes = Vec::new();
            let drained = translate_inline_flush_delete(
                db_tx,
                projections,
                table_id,
                schema_version,
                flush_snapshot,
                &mut writes,
            )
            .await?;

            Ok::<_, Error>((writes, drained))
        })
        .buffered(FLUSH_SCAN_CONCURRENCY)
        .try_collect()
        .await
}

/// The schema record a version owes, or nothing when it already has one.
/// A version's schema is fixed once written; a differing one is refused.
fn schema_write_if_new(
    recorded: Option<&Bytes>,
    table_id: u64,
    schema_version: u64,
    arrow_schema: &[u8],
) -> Result<Option<StagedWrite>> {
    match recorded {
        Some(recorded) if recorded == arrow_schema => Ok(None),
        Some(_) => Err(Error::Constraint(format!(
            "table {table_id} already records a different schema for version {schema_version}; \
             a version's schema is fixed once its first chunk is written"
        ))),
        None => Ok(Some(inline_schema_write(
            table_id,
            schema_version,
            arrow_schema,
        ))),
    }
}

/// Refuses a commit that tombstones a row its own flush drains: the
/// flushed file carries that row as live, so the tombstone would vanish.
/// Such a row is deleted with a delete file against the flushed file.
fn refuse_tombstones_of_drained_rows(
    table_id: u64,
    drained: &HashSet<u64>,
    tombstoned: &[(u64, u64)],
) -> Result<()> {
    for &(tombstoned_table, row_id) in tombstoned {
        if tombstoned_table == table_id && drained.contains(&row_id) {
            return Err(Error::Constraint(format!(
                "inline_delete of row {row_id} on table {table_id} in the commit that flushes \
                 it; the row is live in the flushed file, so delete it with a delete file \
                 against that file instead"
            )));
        }
    }

    Ok(())
}

/// Translates staged inline mutations into `inline/*` writes. `db_tx` is
/// read at its pre-commit state, so a drain sees the store as it stood
/// before this commit.
pub(crate) async fn stage_inline_writes(
    db_tx: &DbTransaction,
    projections: &std::sync::RwLock<ProjectionCache>,
    ops: &[InlineStage],
) -> Result<Vec<StagedWrite>> {
    let mut writes = Vec::new();
    let tombstoned: Vec<(u64, u64)> = ops
        .iter()
        .filter_map(|op| match op {
            InlineStage::Tombstone {
                table_id, row_id, ..
            } => Some((*table_id, *row_id)),
            _ => None,
        })
        .collect();
    let (recorded, flushes) = futures::try_join!(
        recorded_schemas(db_tx, ops),
        translated_flushes(db_tx, projections, ops)
    )?;
    let mut flushes = flushes.into_iter();
    // Versions whose schema record this batch has already settled.
    let mut settled: HashSet<(u64, u64)> = HashSet::new();
    for op in ops {
        match op {
            InlineStage::Schema {
                table_id,
                schema_version,
                arrow_schema,
            } => {
                if settled.insert((*table_id, *schema_version)) {
                    let recorded = recorded
                        .get(&(*table_id, *schema_version))
                        .and_then(Option::as_ref);
                    writes.extend(schema_write_if_new(
                        recorded,
                        *table_id,
                        *schema_version,
                        arrow_schema,
                    )?);
                }
            }
            InlineStage::Insert {
                table_id,
                schema_version,
                begin_snapshot,
                chunk_seq,
                row_id_start,
                row_count,
                arrow_body,
            } => {
                writes.push(inline_insert_write(
                    *table_id,
                    *schema_version,
                    *begin_snapshot,
                    *chunk_seq,
                    *row_id_start,
                    *row_count,
                    arrow_body,
                ));
                writes.push(inline_chunk_range_write(
                    *table_id,
                    *schema_version,
                    *begin_snapshot,
                    *chunk_seq,
                    *row_id_start,
                    *row_count,
                )?);
            }
            InlineStage::Tombstone {
                table_id,
                row_id,
                end_snapshot,
            } => writes.push(inline_inline_delete_write(
                *table_id,
                *row_id,
                *end_snapshot,
            )),
            InlineStage::Flush { table_id, .. } => {
                let (flush_writes, drained) = flushes.next().ok_or_else(|| {
                    Error::Corruption(format!(
                        "inline flush of table {table_id} was staged but never translated"
                    ))
                })?;
                writes.extend(flush_writes);
                refuse_tombstones_of_drained_rows(*table_id, &drained, &tombstoned)?;
            }
        }
    }

    Ok(writes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::store::{
        key::{InlineKey, Key},
        open::StoreBuilder,
        proto, value,
    };

    /// Schema records settle in op order: a version already recorded with
    /// the same bytes writes nothing, a new one writes once, and a repeat of
    /// a version inside the batch adds no second write.
    #[tokio::test]
    async fn schema_writes_keep_op_order_and_dedup_versions() {
        let (db, _) = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            Key::Inline(InlineKey::Schema {
                table_id: 7,
                schema_version: 1,
            })
            .encode(),
            value::encode_value(&proto::InlineSchemaValue {
                arrow_schema: b"v1".to_vec().into(),
                same_as_version: None,
            }),
        )
        .unwrap();
        tx.commit().await.unwrap();

        let schema = |schema_version: u64, bytes: &[u8]| InlineStage::Schema {
            table_id: 7,
            schema_version,
            arrow_schema: bytes.to_vec(),
        };
        let ops = vec![
            schema(3, b"v3"),
            schema(1, b"v1"),
            schema(2, b"v2"),
            schema(3, b"v3"),
        ];

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let projections = std::sync::RwLock::new(ProjectionCache::empty());
        let writes = stage_inline_writes(&tx, &projections, &ops).await.unwrap();
        assert_eq!(
            writes,
            vec![
                inline_schema_write(7, 3, b"v3"),
                inline_schema_write(7, 2, b"v2"),
            ]
        );

        let conflicting = stage_inline_writes(&tx, &projections, &[schema(1, b"other")]).await;
        assert!(matches!(conflicting, Err(Error::Constraint(_))));

        tx.rollback();
        db.close().await.unwrap();
    }
}
