//! Exact inline-version pruning and partial-chunk rewrites.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::Range,
};

use super::{
    CommitSession, DrainedRows, Error, InlineKey, InlineOperation, Key, ProjectionCache, Result,
    RowOperation, commit, inline_chunk_range_delete, inline_chunk_range_write, inline_insert_write,
    materialize_inline_rows, proto, prune_removed_tombstones, reconcile_chunk_directory,
    store_inline,
};
use crate::data_file::{decode_inline_schema, slice_inline_batch};

/// Selected `(schema_version, row_id, begin_snapshot)` identities per table.
pub(super) fn selections(ops: &[RowOperation]) -> BTreeMap<u64, BTreeSet<(u64, u64, u64)>> {
    let mut selections: BTreeMap<u64, BTreeSet<(u64, u64, u64)>> = BTreeMap::new();
    for op in ops {
        if let RowOperation::InlineRowsDelete {
            table_id,
            schema_version,
            rows,
        } = op
        {
            selections.entry(*table_id).or_default().extend(
                rows.iter()
                    .map(|&(row_id, begin)| (*schema_version, row_id, begin)),
            );
        }
    }
    selections
}

/// Combines exact selections and flush prefixes before rewriting any chunk.
pub(super) async fn translate(
    db_tx: &CommitSession,
    projections: &std::sync::RwLock<ProjectionCache>,
    table_id: u64,
    selected: &BTreeSet<(u64, u64, u64)>,
    ops: &[RowOperation],
) -> Result<(Vec<commit::StagedWrite>, DrainedRows)> {
    let (chunks, ranges, tombstones) = futures::try_join!(
        store_inline::scan_inline_chunks(db_tx.handle(), table_id),
        store_inline::scan_inline_chunk_ranges(db_tx.handle(), table_id),
        store_inline::scan_inline_deletes(db_tx.handle(), table_id),
    )?;
    let (prefixes, mut sequences) = prefixes_and_sequences(table_id, ops, &chunks)?;
    let rows = materialize_inline_rows(&chunks, &tombstones);
    let mut removed = HashSet::new();
    let mut found = BTreeSet::new();
    let mut drained = DrainedRows::new();
    for row in &rows {
        let InlineOperation::Insert { schema_version, .. } = chunks[row.chunk].0 else {
            continue;
        };
        let identity = (schema_version, row.row_id, row.begin_snapshot);
        let exact = selected.contains(&identity);
        if exact {
            found.insert(identity);
        }
        if exact
            || prefixes
                .get(&schema_version)
                .is_some_and(|prefix| row.begin_snapshot <= *prefix)
        {
            removed.insert((row.chunk, row.offset_in_chunk));
            drained
                .entry((table_id, schema_version))
                .or_default()
                .push(row.row_id);
        }
    }
    if let Some(missing) = selected.difference(&found).next() {
        return Err(Error::Corruption(format!(
            "no inline row version {table_id}/{missing:?} to remove"
        )));
    }

    let (mut writes, deleted) =
        rewrite_chunks(db_tx, table_id, &chunks, &removed, &mut sequences).await?;
    reconcile_chunk_directory(
        projections,
        table_id,
        &chunks,
        &ranges,
        &deleted,
        &mut writes,
    )?;
    prune_removed_tombstones(
        table_id,
        &rows,
        |row| removed.contains(&(row.chunk, row.offset_in_chunk)),
        &tombstones,
        &mut writes,
    );
    Ok((writes, drained))
}

type ChunkSequences = BTreeMap<(u64, u64), u64>;
type Chunk = (InlineOperation, proto::InlineChunkValue);
type ChunkWrites = (Vec<commit::StagedWrite>, BTreeSet<InlineOperation>);

fn prefixes_and_sequences(
    table_id: u64,
    ops: &[RowOperation],
    chunks: &[Chunk],
) -> Result<(BTreeMap<u64, u64>, ChunkSequences)> {
    let mut prefixes: BTreeMap<u64, u64> = BTreeMap::new();
    let mut sequences: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    for op in ops {
        match op {
            RowOperation::InlineFlushDelete {
                table_id: table,
                schema_version,
                flush_snapshot,
            } if *table == table_id => {
                let prefix = prefixes.entry(*schema_version).or_default();
                *prefix = (*prefix).max(*flush_snapshot);
            }
            RowOperation::InlineInsert {
                table_id: table,
                schema_version,
                begin_snapshot,
                ..
            } if *table == table_id => {
                *sequences
                    .entry((*schema_version, *begin_snapshot))
                    .or_default() += 1;
            }
            _ => {}
        }
    }
    for (op, _) in chunks {
        if let InlineOperation::Insert {
            schema_version,
            begin_snapshot,
            chunk_seq,
            ..
        } = op
        {
            let next = chunk_seq.checked_add(1).ok_or_else(sequence_overflow)?;
            let sequence = sequences
                .entry((*schema_version, *begin_snapshot))
                .or_default();
            *sequence = (*sequence).max(next);
        }
    }
    Ok((prefixes, sequences))
}

async fn rewrite_chunks(
    db_tx: &CommitSession,
    table_id: u64,
    chunks: &[Chunk],
    removed: &HashSet<(usize, u64)>,
    sequences: &mut ChunkSequences,
) -> Result<ChunkWrites> {
    let mut writes = Vec::new();
    let mut deleted = BTreeSet::new();
    let mut schemas = HashMap::new();
    for (index, (op, chunk)) in chunks.iter().enumerate() {
        let InlineOperation::Insert {
            schema_version,
            begin_snapshot,
            chunk_seq,
            ..
        } = *op
        else {
            continue;
        };
        let runs = surviving_runs(chunk.row_count, |offset| removed.contains(&(index, offset)))?;
        if runs.len() == 1 && runs[0].len() as u64 == chunk.row_count {
            continue;
        }
        deleted.insert(*op);
        writes.push((Key::Inline(InlineKey::Live(*op)).encode(), None));
        writes.push(inline_chunk_range_delete(table_id, *op, chunk)?);
        if runs.is_empty() {
            continue;
        }
        if let std::collections::hash_map::Entry::Vacant(entry) = schemas.entry(schema_version) {
            let schema = store_inline::read_inline_schema(db_tx.handle(), table_id, schema_version)
                .await?
                .ok_or_else(|| {
                    Error::Corruption(format!("missing inline schema {table_id}/{schema_version}"))
                })?;
            entry.insert(decode_inline_schema(schema)?);
        }
        let bodies = slice_inline_batch(
            schemas[&schema_version].clone(),
            &chunk.body,
            usize::try_from(chunk.row_count)
                .map_err(|_| Error::Corruption("inline chunk too large".into()))?,
            &runs,
        )?;
        for (run_index, (run, body)) in runs.into_iter().zip(bodies).enumerate() {
            let sequence = if run_index == 0 {
                chunk_seq
            } else {
                let next = sequences
                    .entry((schema_version, begin_snapshot))
                    .or_default();
                let sequence = *next;
                *next = next.checked_add(1).ok_or_else(sequence_overflow)?;
                sequence
            };
            let start = chunk
                .row_id_start
                .checked_add(run.start as u64)
                .ok_or_else(|| Error::Corruption("inline row id overflow".into()))?;
            let count = run.len() as u64;
            writes.push(inline_insert_write(
                table_id,
                schema_version,
                begin_snapshot,
                sequence,
                start,
                count,
                body,
            ));
            writes.push(inline_chunk_range_write(
                table_id,
                schema_version,
                begin_snapshot,
                sequence,
                start,
                count,
            )?);
        }
    }
    Ok((writes, deleted))
}

fn sequence_overflow() -> Error {
    Error::Corruption("inline chunk sequence overflow".into())
}

fn surviving_runs(count: u64, removed: impl Fn(u64) -> bool) -> Result<Vec<Range<usize>>> {
    let count =
        usize::try_from(count).map_err(|_| Error::Corruption("inline chunk too large".into()))?;
    let mut runs = Vec::new();
    let mut offset = 0;
    while offset < count {
        if removed(offset as u64) {
            offset += 1;
            continue;
        }
        let start = offset;
        while offset < count && !removed(offset as u64) {
            offset += 1;
        }
        runs.push(start..offset);
    }
    Ok(runs)
}
