//! The verb path's inline staging: what a commit closure accumulates in
//! the `inline` subspace, and its translation into store writes.
//!
//! Inline records live outside
//! [`CatalogSnapshot`](crate::catalog::CatalogSnapshot)'s entity model and are
//! never diffed, so they are staged as intents and translated here — the same
//! split the staged-row path makes, sharing its write builders.

use std::collections::HashSet;

use slatedb::DbTransaction;

use crate::{
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

/// The schema record a version owes, or nothing when it already has one.
///
/// A version's schema is written once and never rewritten, so a hot inline
/// path pays a point read rather than re-serializing the schema into every
/// commit's batch. A second, differing schema for a version is refused: the
/// chunks already written decode against the recorded one, so accepting it
/// would silently misread them.
async fn schema_write_if_new(
    db_tx: &DbTransaction,
    table_id: u64,
    schema_version: u64,
    arrow_schema: &[u8],
) -> Result<Option<StagedWrite>> {
    match store_inline::read_inline_schema(ReadHandle::Tx(db_tx), table_id, schema_version).await? {
        Some(recorded) if recorded.arrow_schema == arrow_schema => Ok(None),
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

/// Refuses a commit that tombstones a row its own flush drains.
///
/// The flushed file was written before the commit, so it carries that row
/// as live, and the tombstone's chunk is removed out from under it — the
/// delete would simply vanish. The way to delete a flushed row in the
/// commit that flushes it is a delete file against the id
/// `flush_inlined_data` returns.
///
/// Only the verb path enforces this. The staged path carries DuckLake's
/// own statements, which express a flush as the hard deletes it issues
/// rather than as this pair.
fn refuse_tombstones_of_drained_rows(
    table_id: u64,
    drained: &HashSet<u64>,
    ops: &[InlineStage],
) -> Result<()> {
    for op in ops {
        if let InlineStage::Tombstone {
            table_id: tombstoned_table,
            row_id,
            ..
        } = op
            && *tombstoned_table == table_id
            && drained.contains(row_id)
        {
            return Err(Error::Constraint(format!(
                "inline_delete of row {row_id} on table {table_id} in the commit that flushes \
                 it; the row is live in the flushed file, so delete it with a delete file \
                 against that file instead"
            )));
        }
    }

    Ok(())
}

/// Translates staged inline mutations into `inline/*` writes.
///
/// `db_tx` is read at its pre-commit state, so a drain sees the store as it
/// stood before this commit — which is why a transaction may not both inline
/// into a table and flush it.
pub(crate) async fn stage_inline_writes(
    db_tx: &DbTransaction,
    ops: &[InlineStage],
) -> Result<Vec<StagedWrite>> {
    let mut writes = Vec::new();
    // Versions this batch has already settled the schema record for, so a
    // commit inlining several chunks of one version resolves it once.
    let mut settled: HashSet<(u64, u64)> = HashSet::new();
    for op in ops {
        match op {
            InlineStage::Schema {
                table_id,
                schema_version,
                arrow_schema,
            } => {
                if settled.insert((*table_id, *schema_version)) {
                    writes.extend(
                        schema_write_if_new(db_tx, *table_id, *schema_version, arrow_schema)
                            .await?,
                    );
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
            InlineStage::Flush {
                table_id,
                schema_version,
                flush_snapshot,
            } => {
                let drained = translate_inline_flush_delete(
                    db_tx,
                    *table_id,
                    *schema_version,
                    *flush_snapshot,
                    &mut writes,
                )
                .await?;
                refuse_tombstones_of_drained_rows(*table_id, &drained, ops)?;
            }
        }
    }

    Ok(writes)
}
