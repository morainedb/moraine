//! Inline-data translation: turning staged inline operations into their
//! `inline/*` store writes.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use super::{
    DbTransaction, EntityKey, Error, HashMap, InlineKey, InlineOperation, InlineScanKind, Key,
    ReadHandle, Result, RowOperation, TableKind, commit, decode::decode_hard_delete,
    materialize_inline_rows, proto, store_inline, value,
};

/// Allocates `inline/insert` chunk sequence numbers within one commit: the
/// first [`RowOp::InlineInsert`] staged for a given `(table_id,
/// schema_version, begin_snapshot)` gets `chunk_seq` `0`, the next `1`,
/// and so on — disambiguating multiple chunks the same commit stages
/// against the same key prefix.
#[derive(Default)]
pub(super) struct ChunkSeqAllocator(HashMap<(u64, u64, u64), u64>);

impl ChunkSeqAllocator {
    fn next(&mut self, table_id: u64, schema_version: u64, begin_snapshot: u64) -> u64 {
        let seq = self
            .0
            .entry((table_id, schema_version, begin_snapshot))
            .or_insert(0);
        let allocated = *seq;
        *seq += 1;
        allocated
    }
}

/// Removes every `inline/insert` chunk begun at or before `flush_snapshot`
/// for `(table_id, schema_version)`, plus the `inline/inline_delete` tombstones
/// those chunks' rows consumed. Reads `db_tx`'s current (pre-commit)
/// inline records — the flush op only ever names the table and snapshot,
/// never the keys to remove.
///
/// Returns the row ids drained, which the flushed file now carries.
pub(crate) async fn translate_inline_flush_delete(
    db_tx: &DbTransaction,
    table_id: u64,
    schema_version: u64,
    flush_snapshot: u64,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<HashSet<u64>> {
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id).await?;
    let inline_deletes = store_inline::scan_inline_deletes(ReadHandle::Tx(db_tx), table_id).await?;

    let scoped: Vec<(InlineOperation, proto::InlineChunkValue)> = chunks
        .into_iter()
        .filter(
            |(op, _)| matches!(op, InlineOperation::Insert { schema_version: v, .. } if *v == schema_version),
        )
        .collect();

    for (op, chunk) in &scoped {
        if let InlineOperation::Insert { begin_snapshot, .. } = op
            && *begin_snapshot <= flush_snapshot
        {
            writes.push((Key::Inline(InlineKey::Live(*op)).encode(), None));
            writes.push(inline_chunk_range_delete(table_id, chunk)?);
        }
    }

    // The rows the flushed chunks carried, including already-tombstoned
    // ones (`ForFlush`) — their `inline/inline_delete` records become orphaned
    // once the owning chunk is gone above, and must go with it.
    let rows = materialize_inline_rows(&scoped, &inline_deletes);
    let mut drained = HashSet::new();
    for row in InlineScanKind::ForFlush.select(&rows, flush_snapshot, 0) {
        drained.insert(row.row_id);
        if row.end_snapshot.is_some() {
            writes.push((
                Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
                    table_id,
                    row_id: row.row_id,
                }))
                .encode(),
                None,
            ));
        }
    }

    Ok(drained)
}

/// Removes the named live `inline/file_delete` records, refusing any the
/// table does not carry.
///
/// The check is why this takes the whole batch rather than one record at
/// a time: it costs one prefix scan for the commit instead of one per
/// row. And it is worth its cost — a raw key delete of an absent key is a
/// silent no-op, so without it a removal naming a record that is not
/// there would pass, and the only symptom would be a row that DuckLake
/// believes it stopped deleting still reading as deleted. Every other
/// delete path in this translator fails loudly on a miss for the same
/// reason.
pub(super) async fn translate_inline_file_delete_removals(
    db_tx: &DbTransaction,
    table_id: u64,
    removals: &[(u64, u64)],
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    let live: HashSet<(u64, u64)> =
        store_inline::scan_inline_file_deletes(ReadHandle::Tx(db_tx), table_id)
            .await?
            .into_iter()
            .map(|(data_file_id, row_id, _)| (data_file_id, row_id))
            .collect();

    writes.push(inline_file_delete_table_write(table_id));
    for &(data_file_id, row_id) in removals {
        if !live.contains(&(data_file_id, row_id)) {
            return Err(Error::Corruption(format!(
                "no live inlined file-delete ({table_id}, {data_file_id}, {row_id}) to remove"
            )));
        }
        writes.push((
            Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
                table_id,
                data_file_id,
                row_id,
            }))
            .encode(),
            None,
        ));
    }
    Ok(())
}

/// The `(table_id, data_file_id)` of every data file this commit
/// hard-deletes from the catalog.
///
/// A hard delete, not an end. A merge subsumes its sources' whole
/// visibility history into the backdated replacement before pruning their
/// rows — current and history alike — so nothing can read those files
/// again. A rewrite instead *ends* its source into history precisely
/// because a reader below the rewrite must still see the rows it
/// materialized deletes for, and the deletions making those rows dead have
/// to remain readable alongside it.
fn gather_pruned_data_files(ops: &[RowOperation]) -> Result<BTreeSet<(u64, u64)>> {
    let mut pruned = BTreeSet::new();
    for op in ops {
        if let RowOperation::Delete {
            table: TableKind::DataFile,
            cells,
        } = op
            && let (
                EntityKey::File {
                    table_id,
                    data_file_id,
                },
                _,
            ) = decode_hard_delete(TableKind::DataFile, cells)?
        {
            pruned.insert((table_id, data_file_id));
        }
    }
    Ok(pruned)
}

/// Removes the live `inline/file_delete` records targeting any data file
/// this commit prunes. Silent on a miss, unlike the DuckLake-driven
/// removal above: this cascade names files, not records, and a pruned file
/// carrying no inlined deletion is the ordinary case rather than drift.
async fn translate_pruned_file_delete_cascade(
    db_tx: &DbTransaction,
    pruned: &BTreeSet<(u64, u64)>,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    let tables: BTreeSet<u64> = pruned.iter().map(|(table_id, _)| *table_id).collect();
    for table_id in tables {
        for (data_file_id, row_id, _) in
            store_inline::scan_inline_file_deletes(ReadHandle::Tx(db_tx), table_id).await?
        {
            if !pruned.contains(&(table_id, data_file_id)) {
                continue;
            }
            writes.push((
                Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
                    table_id,
                    data_file_id,
                    row_id,
                }))
                .encode(),
                None,
            ));
        }
    }
    Ok(())
}

/// Removes every `inline/*` record for `table_id`: schema, chunks, and
/// tombstones, read from `db_tx`'s current (pre-commit) state.
pub(super) async fn translate_inline_drop(
    db_tx: &DbTransaction,
    table_id: u64,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    for (op, _) in store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id).await? {
        writes.push((Key::Inline(InlineKey::Live(op)).encode(), None));
    }
    for row_id_end in
        store_inline::scan_inline_chunk_ranges(ReadHandle::Tx(db_tx), table_id).await?
    {
        writes.push((
            Key::Inline(InlineKey::ChunkRange {
                table_id,
                row_id_end,
            })
            .encode(),
            None,
        ));
    }
    for (row_id, _) in store_inline::scan_inline_deletes(ReadHandle::Tx(db_tx), table_id).await? {
        writes.push((
            Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
                table_id,
                row_id,
            }))
            .encode(),
            None,
        ));
    }
    for (data_file_id, row_id, _) in
        store_inline::scan_inline_file_deletes(ReadHandle::Tx(db_tx), table_id).await?
    {
        writes.push((
            Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
                table_id,
                data_file_id,
                row_id,
            }))
            .encode(),
            None,
        ));
    }
    for (schema_version, _) in
        store_inline::scan_inline_schemas(ReadHandle::Tx(db_tx), table_id).await?
    {
        writes.push((
            Key::Inline(InlineKey::Schema {
                table_id,
                schema_version,
            })
            .encode(),
            None,
        ));
    }
    // Unconditional: the drop removes everything, and deleting an absent
    // key is a no-op at the store.
    writes.push((
        Key::Inline(InlineKey::FileDeleteTable { table_id }).encode(),
        None,
    ));
    Ok(())
}

/// Translates every staged inline op into direct `inline/*` key writes —
/// a separate pass from [`translate`], since inline records live outside
/// `CatalogSnapshot`'s entity model and are never diffed. `db_tx` is read
/// (for `InlineFlushDelete`/`InlineDrop`, which name a table rather than
/// the keys to remove) at its pre-commit state, before any of this
/// commit's own writes are staged onto it.
pub(crate) fn inline_schema_write(
    table_id: u64,
    schema_version: u64,
    arrow_schema: &[u8],
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Schema {
            table_id,
            schema_version,
        })
        .encode(),
        Some(value::encode_value(&proto::InlineSchemaValue {
            arrow_schema: arrow_schema.to_vec().into(),
        })),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn inline_insert_write(
    table_id: u64,
    schema_version: u64,
    begin_snapshot: u64,
    chunk_seq: u64,
    row_id_start: u64,
    row_count: u64,
    arrow_body: &[u8],
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Live(InlineOperation::Insert {
            table_id,
            schema_version,
            begin_snapshot,
            chunk_seq,
        }))
        .encode(),
        Some(value::encode_value(&proto::InlineChunkValue {
            body: arrow_body.to_vec().into(),
            row_id_start,
            row_count,
            data_file_id: None,
        })),
    )
}

fn inline_chunk_row_id_end(row_id_start: u64, row_count: u64) -> Option<u64> {
    row_count
        .checked_sub(1)
        .and_then(|offset| row_id_start.checked_add(offset))
}

pub(crate) fn inline_chunk_range_write(
    table_id: u64,
    schema_version: u64,
    begin_snapshot: u64,
    chunk_seq: u64,
    row_id_start: u64,
    row_count: u64,
) -> Result<commit::StagedWrite> {
    let row_id_end = inline_chunk_row_id_end(row_id_start, row_count).ok_or_else(|| {
        Error::Constraint(format!(
            "inline chunk for table {table_id} has an empty or overflowing row-id range"
        ))
    })?;

    Ok((
        Key::Inline(InlineKey::ChunkRange {
            table_id,
            row_id_end,
        })
        .encode(),
        Some(value::encode_value(&proto::InlineChunkRangeValue {
            row_id_start,
            schema_version,
            begin_snapshot,
            chunk_seq,
        })),
    ))
}

fn inline_chunk_range_delete(
    table_id: u64,
    chunk: &proto::InlineChunkValue,
) -> Result<commit::StagedWrite> {
    let row_id_end =
        inline_chunk_row_id_end(chunk.row_id_start, chunk.row_count).ok_or_else(|| {
            Error::Corruption(format!(
                "inline chunk for table {table_id} has an empty or overflowing row-id range"
            ))
        })?;

    Ok((
        Key::Inline(InlineKey::ChunkRange {
            table_id,
            row_id_end,
        })
        .encode(),
        None,
    ))
}

pub(crate) fn inline_inline_delete_write(
    table_id: u64,
    row_id: u64,
    end_snapshot: u64,
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
            table_id,
            row_id,
        }))
        .encode(),
        Some(value::encode_value(&proto::InlineInlineDeleteValue {
            end_snapshot,
        })),
    )
}

/// The existence marker for a table's
/// `ducklake_inlined_delete_<table_id>`, written idempotently by every
/// path that proves the table exists — staging a deletion into it, or
/// removing one from it.
///
/// Written unconditionally rather than only when absent: it is one small
/// key, and a conditional would cost a read to save a write that is
/// already cheaper than the read. Writing it on the *removal* path too is
/// what heals a store written before the marker existed, whose deletions
/// would otherwise take the table's existence with them when a flush
/// cleared them.
/// Deregisters one `(table_id, schema_version)`'s Arrow schema.
pub(super) fn inline_schema_drop_write(table_id: u64, schema_version: u64) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Schema {
            table_id,
            schema_version,
        })
        .encode(),
        None,
    )
}

pub(super) fn inline_file_delete_table_write(table_id: u64) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::FileDeleteTable { table_id }).encode(),
        Some(value::encode_value(&proto::InlineFileDeleteTableValue {})),
    )
}

pub(super) fn inline_file_delete_write(
    table_id: u64,
    data_file_id: u64,
    row_id: u64,
    begin_snapshot: u64,
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
            table_id,
            data_file_id,
            row_id,
        }))
        .encode(),
        Some(value::encode_value(&proto::InlineFileDeleteValue {
            begin_snapshot,
        })),
    )
}

/// The batch's `InlineFileDeleteRemove` ops grouped by table, so the
/// liveness check behind them costs one prefix scan per table rather than
/// one per row.
fn gather_file_delete_removals(ops: &[RowOperation]) -> BTreeMap<u64, Vec<(u64, u64)>> {
    let mut removals: BTreeMap<u64, Vec<(u64, u64)>> = BTreeMap::new();
    for op in ops {
        if let RowOperation::InlineFileDeleteRemove {
            table_id,
            data_file_id,
            row_id,
        } = op
        {
            removals
                .entry(*table_id)
                .or_default()
                .push((*data_file_id, *row_id));
        }
    }
    removals
}

#[allow(clippy::too_many_lines)]
pub(super) async fn translate_inline(
    db_tx: &DbTransaction,
    ops: &[RowOperation],
) -> Result<Vec<commit::StagedWrite>> {
    let mut writes = Vec::new();
    let mut chunk_seqs = ChunkSeqAllocator::default();
    // One existence marker per table per commit, however many deletions
    // this batch stages into it.
    let mut file_delete_tables: HashSet<u64> = HashSet::new();

    for (table_id, removals) in gather_file_delete_removals(ops) {
        translate_inline_file_delete_removals(db_tx, table_id, &removals, &mut writes).await?;
    }

    let pruned = gather_pruned_data_files(ops)?;
    if !pruned.is_empty() {
        translate_pruned_file_delete_cascade(db_tx, &pruned, &mut writes).await?;
    }

    for op in ops {
        match op {
            RowOperation::InlineSchema {
                table_id,
                schema_version,
                arrow_schema,
            } => writes.push(inline_schema_write(
                *table_id,
                *schema_version,
                arrow_schema,
            )),
            RowOperation::InlineInsert {
                table_id,
                schema_version,
                begin_snapshot,
                row_id_start,
                row_count,
                arrow_body,
            } => {
                let chunk_seq = chunk_seqs.next(*table_id, *schema_version, *begin_snapshot);
                writes.push(inline_insert_write(
                    *table_id,
                    *schema_version,
                    *begin_snapshot,
                    chunk_seq,
                    *row_id_start,
                    *row_count,
                    arrow_body,
                ));
                writes.push(inline_chunk_range_write(
                    *table_id,
                    *schema_version,
                    *begin_snapshot,
                    chunk_seq,
                    *row_id_start,
                    *row_count,
                )?);
            }
            RowOperation::InlineInlineDelete {
                table_id,
                row_id,
                end_snapshot,
            } => writes.push(inline_inline_delete_write(
                *table_id,
                *row_id,
                *end_snapshot,
            )),
            RowOperation::InlineFileDelete {
                table_id,
                data_file_id,
                row_id,
                begin_snapshot,
            } => {
                // The marker rides the first deletion of each table in
                // this batch; the rest of them find it already staged.
                if file_delete_tables.insert(*table_id) {
                    writes.push(inline_file_delete_table_write(*table_id));
                }
                let write =
                    inline_file_delete_write(*table_id, *data_file_id, *row_id, *begin_snapshot);
                writes.push(write);
            }
            RowOperation::InlineFlushDelete {
                table_id,
                schema_version,
                flush_snapshot,
            } => {
                translate_inline_flush_delete(
                    db_tx,
                    *table_id,
                    *schema_version,
                    *flush_snapshot,
                    &mut writes,
                )
                .await?;
            }
            RowOperation::InlineDrop { table_id } => {
                translate_inline_drop(db_tx, *table_id, &mut writes).await?;
            }
            RowOperation::InlineSchemaDrop {
                table_id,
                schema_version,
            } => writes.push(inline_schema_drop_write(*table_id, *schema_version)),
            // Removals are handled above, in one pass per table; the
            // entity ops belong to `translate`, not here.
            RowOperation::InlineFileDeleteRemove { .. }
            | RowOperation::Insert { .. }
            | RowOperation::Delete { .. }
            | RowOperation::UpdateSetEnd { .. }
            | RowOperation::UpdateSetBegin { .. } => {}
        }
    }

    Ok(writes)
}
