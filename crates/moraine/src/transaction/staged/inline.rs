//! Inline-data translation: turning staged inline operations into their
//! `inline/*` store writes.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use futures::{StreamExt, TryStreamExt, stream};

use super::{
    DbTransaction, EntityKey, Error, HashMap, InlineKey, InlineOperation, InlineScanKind, Key,
    ReadHandle, Result, RowOperation, TableKind, commit, decode::decode_hard_delete,
    materialize_inline_rows, proto, store_inline, value,
};
use crate::catalog::{
    inline::materialize_locator_rows,
    projection::{self, ProjectionCache},
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
///
/// A directory known complete serves the walk from its locators alone;
/// otherwise the chunk bodies are scanned, the directory verified against
/// them (remembered when the store format locks out writers that predate
/// it), and any gaps healed onto this batch.
pub(crate) async fn translate_inline_flush_delete(
    db_tx: &DbTransaction,
    projections: &std::sync::RwLock<ProjectionCache>,
    table_id: u64,
    schema_version: u64,
    flush_snapshot: u64,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<HashSet<u64>> {
    let rows = if projection::inline_directory_complete(projections, table_id) {
        let (locators, inline_deletes) = futures::try_join!(
            store_inline::scan_inline_chunk_locators(ReadHandle::Tx(db_tx), table_id),
            store_inline::scan_inline_deletes(ReadHandle::Tx(db_tx), table_id),
        )?;

        let scoped: Vec<store_inline::InlineChunkLocator> = locators
            .into_iter()
            .filter(|locator| locator.schema_version() == Some(schema_version))
            .collect();
        for locator in &scoped {
            if let InlineOperation::Insert { begin_snapshot, .. } = locator.operation()
                && begin_snapshot <= flush_snapshot
            {
                writes.push((
                    Key::Inline(InlineKey::Live(locator.operation())).encode(),
                    None,
                ));
                writes.push((
                    chunk_range_key(table_id, locator.row_id_end(), locator.operation())?,
                    None,
                ));
            }
        }

        materialize_locator_rows(&scoped, &inline_deletes)
    } else {
        let (chunks, range_ends, legacy_ranges, inline_deletes) = futures::try_join!(
            store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id),
            store_inline::scan_inline_chunk_ranges(ReadHandle::Tx(db_tx), table_id),
            store_inline::scan_legacy_inline_chunk_ranges(ReadHandle::Tx(db_tx), table_id),
            store_inline::scan_inline_deletes(ReadHandle::Tx(db_tx), table_id),
        )?;

        let scoped: Vec<(InlineOperation, proto::InlineChunkValue)> = chunks
            .iter()
            .filter(
                |(op, _)| matches!(op, InlineOperation::Insert { schema_version: v, .. } if *v == schema_version),
            )
            .cloned()
            .collect();

        let mut deleted = BTreeSet::new();
        for (op, chunk) in &scoped {
            if let InlineOperation::Insert { begin_snapshot, .. } = op
                && *begin_snapshot <= flush_snapshot
            {
                writes.push((Key::Inline(InlineKey::Live(*op)).encode(), None));
                let (range_key, _) = inline_chunk_range_delete(table_id, *op, chunk)?;
                writes.push((range_key, None));
                deleted.insert(*op);
            }
        }

        // A store carried across the locator-key change still holds the
        // superseded keys; nothing reads them, so the repair sweeps them.
        for row_id_end in legacy_ranges {
            writes.push((
                Key::Inline(InlineKey::ChunkRange {
                    table_id,
                    row_id_end,
                })
                .encode(),
                None,
            ));
        }

        reconcile_chunk_directory(
            projections,
            table_id,
            &chunks,
            &range_ends,
            &deleted,
            writes,
        )?;

        materialize_inline_rows(&scoped, &inline_deletes)
    };

    // Tombstones on the flushed chunks' rows (`ForFlush` includes them)
    // go with their chunk.
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

/// Compares the walked chunks against the directory. Equality is
/// remembered — but only when the store format shuts out writers that
/// predate the directory, since only then does a chunk always arrive with
/// its locator. A gap stages the repair onto this batch instead: locators
/// for uncovered surviving chunks, deletions for locators naming no chunk.
/// Nothing is remembered off a repair still riding the batch.
fn reconcile_chunk_directory(
    projections: &std::sync::RwLock<ProjectionCache>,
    table_id: u64,
    chunks: &[(InlineOperation, proto::InlineChunkValue)],
    range_ends: &[(u64, InlineOperation)],
    deleted: &BTreeSet<InlineOperation>,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    if projection::format_floor(projections) < commit::FORMAT_WITH_INLINE_CHUNK_DIRECTORY {
        return Ok(());
    }

    // Coverage is judged by chunk identity, not by range end: two live
    // chunks can end at one row id, and each still owns its own locator.
    let chunk_identities: BTreeSet<InlineOperation> = chunks.iter().map(|(op, _)| *op).collect();
    let directory: BTreeSet<InlineOperation> = range_ends.iter().map(|(_, op)| *op).collect();

    let mut complete = true;
    for (op, chunk) in chunks {
        if directory.contains(op) {
            continue;
        }
        complete = false;
        if deleted.contains(op) {
            continue;
        }
        if let InlineOperation::Insert {
            schema_version,
            begin_snapshot,
            chunk_seq,
            ..
        } = op
        {
            writes.push(inline_chunk_range_write(
                table_id,
                *schema_version,
                *begin_snapshot,
                *chunk_seq,
                chunk.row_id_start,
                chunk.row_count,
            )?);
        }
    }
    for (end, op) in range_ends {
        if !chunk_identities.contains(op) {
            complete = false;
            writes.push((chunk_range_key(table_id, *end, *op)?, None));
        }
    }

    if complete {
        projection::note_inline_directory_complete(projections, table_id);
    }

    Ok(())
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
    let (chunks, ranges, inline_deletes, file_deletes, schemas, dropped_schemas) = futures::try_join!(
        store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id),
        store_inline::scan_inline_chunk_ranges(ReadHandle::Tx(db_tx), table_id),
        store_inline::scan_inline_deletes(ReadHandle::Tx(db_tx), table_id),
        store_inline::scan_inline_file_deletes(ReadHandle::Tx(db_tx), table_id),
        store_inline::scan_inline_schemas(ReadHandle::Tx(db_tx), table_id),
        store_inline::scan_inline_dropped_schemas(ReadHandle::Tx(db_tx), table_id),
    )?;

    for (op, _) in chunks {
        writes.push((Key::Inline(InlineKey::Live(op)).encode(), None));
    }
    for (row_id_end, operation) in ranges {
        writes.push((chunk_range_key(table_id, row_id_end, operation)?, None));
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
    for schema_version in dropped_schemas {
        writes.push((
            Key::Inline(InlineKey::SchemaDropped {
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
            same_as_version: None,
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
        Key::Inline(InlineKey::ChunkLocator {
            table_id,
            row_id_end,
            schema_version,
            begin_snapshot,
            chunk_seq,
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

/// The directory key naming one chunk: the range end leads so a scan still
/// seeks by row id, and the chunk's identity follows so two chunks ending
/// at one row id stay distinct.
pub(crate) fn chunk_range_key(
    table_id: u64,
    row_id_end: u64,
    operation: InlineOperation,
) -> Result<Vec<u8>> {
    let InlineOperation::Insert {
        schema_version,
        begin_snapshot,
        chunk_seq,
        ..
    } = operation
    else {
        return Err(Error::Corruption(
            "inline chunk directory names a non-insert operation".to_owned(),
        ));
    };

    Ok(Key::Inline(InlineKey::ChunkLocator {
        table_id,
        row_id_end,
        schema_version,
        begin_snapshot,
        chunk_seq,
    })
    .encode())
}

fn inline_chunk_range_delete(
    table_id: u64,
    operation: InlineOperation,
    chunk: &proto::InlineChunkValue,
) -> Result<commit::StagedWrite> {
    let row_id_end =
        inline_chunk_row_id_end(chunk.row_id_start, chunk.row_count).ok_or_else(|| {
            Error::Corruption(format!(
                "inline chunk for table {table_id} has an empty or overflowing row-id range"
            ))
        })?;

    Ok((chunk_range_key(table_id, row_id_end, operation)?, None))
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

/// Deregisters one `(table_id, schema_version)` from
/// `ducklake_inlined_data_tables`, retaining its Arrow schema so the
/// name still resolves. DuckLake caches an inlined table's existence for
/// the life of an attach and never re-probes, so a reader holding the
/// pre-flush registry keeps naming a version this drop removed; retaining
/// the schema turns that from a failed bind into an empty scan, and the
/// rows it wanted are in the file the flush wrote.
pub(super) fn inline_schema_drop_write(table_id: u64, schema_version: u64) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::SchemaDropped {
            table_id,
            schema_version,
        })
        .encode(),
        Some(value::encode_value(&proto::InlineSchemaDroppedValue {})),
    )
}

/// Points one version's schema record at another version of the same
/// table, whose bytes are identical.
fn inline_schema_reference_write(
    table_id: u64,
    schema_version: u64,
    same_as_version: u64,
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Schema {
            table_id,
            schema_version,
        })
        .encode(),
        Some(value::encode_value(&proto::InlineSchemaValue {
            arrow_schema: bytes::Bytes::new(),
            same_as_version: Some(same_as_version),
        })),
    )
}

/// The version whose bytes `schema_version`'s record can be replaced by a
/// reference to, if any. Resolution is one hop onto a version carrying
/// bytes, so a version that is already a reference and a version some
/// other reference resolves through are both left whole.
pub(super) fn inline_schema_collapse_target(
    schemas: &[(u64, proto::InlineSchemaValue)],
    schema_version: u64,
) -> Option<u64> {
    let (_, dropped) = schemas
        .iter()
        .find(|(version, _)| *version == schema_version)?;
    if dropped.same_as_version.is_some()
        || schemas
            .iter()
            .any(|(_, value)| value.same_as_version == Some(schema_version))
    {
        return None;
    }

    schemas
        .iter()
        .find(|(version, value)| {
            *version != schema_version
                && value.same_as_version.is_none()
                && value.arrow_schema == dropped.arrow_schema
        })
        .map(|(version, _)| *version)
}

/// Deregisters `schema_version` and, when its columns duplicate another
/// version's, collapses its record to a reference. Deregistered records
/// are retained for as long as their table lives, and a mint that changed
/// no column writes the same bytes again, so this is what keeps the
/// retention from growing with every mint.
///
/// A reference is a shape an older reader would decode as a schema with
/// no columns, so the batch that writes one carries the store to the
/// format that shuts those readers out — the same way a live index or an
/// inline chunk locator carries it to theirs. Reports whether it wrote
/// one, which is what owes that stamp.
async fn translate_inline_schema_drop(
    db_tx: &DbTransaction,
    table_id: u64,
    schema_version: u64,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<bool> {
    writes.push(inline_schema_drop_write(table_id, schema_version));

    let schemas = store_inline::scan_inline_schemas(ReadHandle::Tx(db_tx), table_id).await?;
    let Some(target) = inline_schema_collapse_target(&schemas, schema_version) else {
        return Ok(false);
    };
    writes.push(inline_schema_reference_write(
        table_id,
        schema_version,
        target,
    ));
    Ok(true)
}

/// Clears a version's drop marker, so registering it again cannot leave
/// it both listed and deregistered.
fn inline_schema_undrop_write(table_id: u64, schema_version: u64) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::SchemaDropped {
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
    projections: &std::sync::RwLock<ProjectionCache>,
    ops: &[RowOperation],
) -> Result<(Vec<commit::StagedWrite>, bool)> {
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
    // Flushes and drops name a table and scan `db_tx` for the keys to
    // remove; they run together and land last.
    let table_scoped: Vec<&RowOperation> = ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                RowOperation::InlineFlushDelete { .. }
                    | RowOperation::InlineDrop { .. }
                    | RowOperation::InlineSchemaDrop { .. }
            )
        })
        .collect();
    let scoped = stream::iter(table_scoped.into_iter().map(|op| async move {
        let mut writes = Vec::new();
        let mut referenced = false;
        match op {
            RowOperation::InlineFlushDelete {
                table_id,
                schema_version,
                flush_snapshot,
            } => {
                translate_inline_flush_delete(
                    db_tx,
                    projections,
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
            } => {
                referenced =
                    translate_inline_schema_drop(db_tx, *table_id, *schema_version, &mut writes)
                        .await?;
            }
            _ => {}
        }
        Ok::<_, Error>((writes, referenced))
    }))
    .buffer_unordered(INLINE_TRANSLATION_CONCURRENCY)
    .try_collect::<Vec<_>>();

    let (removal_writes, cascade_writes, scoped_writes) =
        futures::try_join!(removals, cascade, scoped)?;
    writes.extend(removal_writes.into_iter().flatten());
    writes.extend(cascade_writes);

    for op in ops {
        match op {
            RowOperation::InlineSchema {
                table_id,
                schema_version,
                arrow_schema,
            } => {
                writes.push(inline_schema_write(
                    *table_id,
                    *schema_version,
                    arrow_schema,
                ));
                writes.push(inline_schema_undrop_write(*table_id, *schema_version));
            }
            RowOperation::InlineInsert {
                table_id,
                schema_version,
                begin_snapshot,
                row_id_start,
                row_count,
                arrow_body,
            } => {
                let chunk_seq = chunk_seqs.next(*table_id, *schema_version, *begin_snapshot);
                // The locator scan bounds itself by the widest chunk a table
                // holds; raising it here keeps that bound sound for the
                // chunk this commit is about to add.
                projection::note_inline_chunk_width(
                    projections,
                    *table_id,
                    row_count.saturating_sub(1),
                );
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
            // Removals, flushes and drops are handled above; entity ops
            // belong to `translate`.
            RowOperation::InlineSchemaDrop { .. }
            | RowOperation::InlineFlushDelete { .. }
            | RowOperation::InlineDrop { .. }
            | RowOperation::InlineFileDeleteRemove { .. }
            | RowOperation::Insert { .. }
            | RowOperation::Delete { .. }
            | RowOperation::UpdateSetEnd { .. }
            | RowOperation::UpdateSetBegin { .. } => {}
        }
    }

    let mut uses_schema_reference = false;
    for (scoped, referenced) in scoped_writes {
        writes.extend(scoped);
        uses_schema_reference |= referenced;
    }

    Ok((writes, uses_schema_reference))
}
