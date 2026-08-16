//! Inline-data translation: turning staged inline operations into their
//! `inline/*` store writes.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use futures::{StreamExt, TryStreamExt, stream};

use super::{
    DbTransaction, EntityKey, Error, HashMap, InlineKey, InlineOperation, InlineScanKind, Key,
    ReadHandle, Result, RowOperation, TableKind, commit, decode::decode_hard_delete,
    materialize_inline_rows, proto, store_inline, value,
};

/// Independent per-table inline scans kept in flight during translation.
const INLINE_TRANSLATION_CONCURRENCY: usize = 8;

/// Allocates `inline/insert` chunk sequence numbers within one commit: the
/// first [`RowOperation::InlineInsert`] staged for a given `(table_id,
/// schema_version, begin_snapshot)` gets `chunk_seq` `0`, the next `1`,
/// and so on.
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
/// for `(table_id, schema_version)`, plus the `inline/inline_delete`
/// tombstones on those chunks' rows, reading `db_tx`'s pre-commit inline
/// records. Returns the row ids drained.
pub(crate) async fn translate_inline_flush_delete(
    db_tx: &DbTransaction,
    table_id: u64,
    schema_version: u64,
    flush_snapshot: u64,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<HashSet<u64>> {
    let (chunks, inline_deletes) = futures::try_join!(
        store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id),
        store_inline::scan_inline_deletes(ReadHandle::Tx(db_tx), table_id),
    )?;

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

    // Tombstones on the flushed chunks' rows (`ForFlush` includes them)
    // go with their chunk.
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
/// table does not carry. Takes a table's whole batch so the liveness
/// check costs one prefix scan.
pub(super) async fn translate_inline_file_delete_removals(
    db_tx: &DbTransaction,
    table_id: u64,
    removals: &[(u64, u64)],
) -> Result<Vec<commit::StagedWrite>> {
    let live: HashSet<(u64, u64)> =
        store_inline::scan_inline_file_deletes(ReadHandle::Tx(db_tx), table_id)
            .await?
            .into_iter()
            .map(|(data_file_id, row_id, _)| (data_file_id, row_id))
            .collect();

    let mut writes = vec![inline_file_delete_table_write(table_id)];
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
    Ok(writes)
}

/// The `(table_id, data_file_id)` of every data file this commit
/// hard-deletes (not merely ends) from the catalog.
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
/// this commit prunes. A pruned file carrying no inlined deletion is the
/// ordinary case, not an error.
async fn translate_pruned_file_delete_cascade(
    db_tx: &DbTransaction,
    pruned: &BTreeSet<(u64, u64)>,
) -> Result<Vec<commit::StagedWrite>> {
    let tables: BTreeSet<u64> = pruned.iter().map(|(table_id, _)| *table_id).collect();
    stream::iter(tables.into_iter().map(|table_id| async move {
        let mut writes = Vec::new();
        for (data_file_id, row_id, _) in
            store_inline::scan_inline_file_deletes(ReadHandle::Tx(db_tx), table_id).await?
        {
            if pruned.contains(&(table_id, data_file_id)) {
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
        Ok::<_, Error>(writes)
    }))
    .buffer_unordered(INLINE_TRANSLATION_CONCURRENCY)
    .try_fold(Vec::new(), |mut writes, table_writes| async move {
        writes.extend(table_writes);
        Ok(writes)
    })
    .await
}

/// Removes every `inline/*` record for `table_id`: schema, chunks, and
/// tombstones, read from `db_tx`'s current (pre-commit) state.
pub(super) async fn translate_inline_drop(
    db_tx: &DbTransaction,
    table_id: u64,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    let (chunks, ranges, inline_deletes, file_deletes, schemas) = futures::try_join!(
        store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id),
        store_inline::scan_inline_chunk_ranges(ReadHandle::Tx(db_tx), table_id),
        store_inline::scan_inline_deletes(ReadHandle::Tx(db_tx), table_id),
        store_inline::scan_inline_file_deletes(ReadHandle::Tx(db_tx), table_id),
        store_inline::scan_inline_schemas(ReadHandle::Tx(db_tx), table_id),
    )?;

    for (op, _) in chunks {
        writes.push((Key::Inline(InlineKey::Live(op)).encode(), None));
    }
    for row_id_end in ranges {
        writes.push((
            Key::Inline(InlineKey::ChunkRange {
                table_id,
                row_id_end,
            })
            .encode(),
            None,
        ));
    }
    for (row_id, _) in inline_deletes {
        writes.push((
            Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
                table_id,
                row_id,
            }))
            .encode(),
            None,
        ));
    }
    for (data_file_id, row_id, _) in file_deletes {
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
    for (schema_version, _) in schemas {
        writes.push((
            Key::Inline(InlineKey::Schema {
                table_id,
                schema_version,
            })
            .encode(),
            None,
        ));
    }
    writes.push((
        Key::Inline(InlineKey::FileDeleteTable { table_id }).encode(),
        None,
    ));
    Ok(())
}

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

/// The existence marker for a table's `ducklake_inlined_delete_<table_id>`,
/// written idempotently by every path that stages a deletion into it or
/// removes one from it (the removal path heals stores written before the
/// marker existed).
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

/// The batch's `InlineFileDeleteRemove` ops grouped by table.
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

/// Translates every staged inline op into direct `inline/*` key writes, a
/// separate pass from `translate` since inline records are never diffed.
/// `db_tx` is read at its pre-commit state, before any of this commit's
/// own writes are staged onto it.
#[allow(clippy::too_many_lines)]
pub(super) async fn translate_inline(
    db_tx: &DbTransaction,
    ops: &[RowOperation],
) -> Result<Vec<commit::StagedWrite>> {
    let mut writes = Vec::new();
    let mut chunk_seqs = ChunkSeqAllocator::default();
    // One existence marker per table per commit.
    let mut file_delete_tables: HashSet<u64> = HashSet::new();

    let removals = async {
        stream::iter(gather_file_delete_removals(ops).into_iter().map(
            |(table_id, removals)| async move {
                translate_inline_file_delete_removals(db_tx, table_id, &removals).await
            },
        ))
        .buffer_unordered(INLINE_TRANSLATION_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await
    };
    let pruned = gather_pruned_data_files(ops)?;
    let cascade = async {
        if pruned.is_empty() {
            Ok(Vec::new())
        } else {
            translate_pruned_file_delete_cascade(db_tx, &pruned).await
        }
    };
    let (removal_writes, cascade_writes) = futures::try_join!(removals, cascade)?;
    writes.extend(removal_writes.into_iter().flatten());
    writes.extend(cascade_writes);

    // Flushes and drops name a table and scan `db_tx` for the keys to
    // remove; they run together and land last.
    let mut table_scoped: Vec<&RowOperation> = Vec::new();

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
                if file_delete_tables.insert(*table_id) {
                    writes.push(inline_file_delete_table_write(*table_id));
                }
                let write =
                    inline_file_delete_write(*table_id, *data_file_id, *row_id, *begin_snapshot);
                writes.push(write);
            }
            RowOperation::InlineFlushDelete { .. } | RowOperation::InlineDrop { .. } => {
                table_scoped.push(op);
            }
            RowOperation::InlineSchemaDrop {
                table_id,
                schema_version,
            } => writes.push(inline_schema_drop_write(*table_id, *schema_version)),
            // Removals are handled above; entity ops belong to `translate`.
            RowOperation::InlineFileDeleteRemove { .. }
            | RowOperation::Insert { .. }
            | RowOperation::Delete { .. }
            | RowOperation::UpdateSetEnd { .. }
            | RowOperation::UpdateSetBegin { .. } => {}
        }
    }

    let scoped_writes = stream::iter(table_scoped.into_iter().map(|op| async move {
        let mut writes = Vec::new();
        match op {
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
            _ => {}
        }
        Ok::<_, Error>(writes)
    }))
    .buffer_unordered(INLINE_TRANSLATION_CONCURRENCY)
    .try_collect::<Vec<_>>()
    .await?;
    writes.extend(scoped_writes.into_iter().flatten());

    Ok(writes)
}
