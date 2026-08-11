//! Equality-index upkeep for staged commits: deriving entry adds and
//! removals from registered files and inline chunks by scoped-reading
//! them.

use std::{collections::BTreeSet, future::Future};

use futures::{StreamExt, TryStreamExt, stream};

use super::{
    Arc, CatalogSnapshot, Cell, ColumnInfo, DbTransaction, Error, HashMap, HashSet, IndexInfo,
    InlineOperation, ObjectStore, ReadHandle, Result, RowOperation, ScopedReadEntry, StagedEntries,
    StagedIndexEntry, TableId, TableKind,
    decode::{Cursor, decode_data_file, decode_delete_file},
    encode_ordered_values, proto, scoped_read, stage_index_entries, store_inline,
};
use crate::transaction::operations::ChangeSet;

/// How many files one upkeep phase reads at once. Each read is an
/// independent fetch of an independent file, so reading them one after
/// another costs a commit one round trip of store latency per file — the
/// term that dominates a commit touching many. Bounding the fan-out
/// instead keeps a single commit from monopolizing the store's request
/// budget.
const FILE_READ_CONCURRENCY: usize = 64;

/// Resolves independent reads concurrently and returns their results in
/// target order. Keeping the order stable makes staging and error selection
/// independent of remote completion timing.
async fn resolve_in_order<T, I, F>(futures: I, concurrency: usize) -> Result<Vec<T>>
where
    I: IntoIterator<Item = F>,
    F: Future<Output = Result<T>>,
{
    stream::iter(futures)
        .buffered(concurrency)
        .try_collect()
        .await
}

/// What a commit's registered and targeted files need before their entries
/// can be derived: where the bytes live, and which tables the commit merely
/// re-homes rows in.
pub(super) struct FileContext<'a> {
    /// The `DATA_PATH` store, absent when the caller supplied none.
    store: Option<&'a Arc<dyn ObjectStore>>,
    /// The store-relative prefix data files resolve against.
    prefix: &'a str,
    /// Tables this commit compacts and does nothing else to, whose
    /// registrations derive no entries at all.
    compacted: BTreeSet<u64>,
}

/// Derives the equality-index entries for one registered data file (a
/// `RowOperation::Insert` into `TableKind::DataFile`), by scoped-reading
/// the file. Empty if the file's table has no live index. Refuses (rather
/// than silently under-covering) when the file must be read but no store is
/// available.
///
/// Returns its entries rather than appending them, so the files one commit
/// registers can be read concurrently and their entries appended in stage
/// order afterwards.
pub(super) async fn data_file_index_entries(
    base: &CatalogSnapshot,
    cells: &[Cell],
    killed: Option<&KilledRows>,
    context: &FileContext<'_>,
) -> Result<Vec<StagedIndexEntry>> {
    let file = decode_data_file(cells)?;
    let table = TableId::new(file.table_id);
    let indexes: Vec<_> = base
        .indexes_of(table)
        .into_iter()
        .filter(|index| index.maintenance != crate::catalog::IndexMaintenance::Deferred)
        .collect();
    if indexes.is_empty() {
        return Ok(Vec::new());
    }

    // The file must be read to maintain the index; no store to read it means
    // the index would silently miss these rows.
    let data_store = context.store.ok_or_else(|| {
        Error::Constraint(format!(
            "data file {} on indexed table {} cannot be read to maintain its equality index: no \
             data-path store is available",
            file.data_file_id, file.table_id
        ))
    })?;

    let killed = match killed {
        Some(killed) => {
            refuse_out_of_range(killed, &file)?;
            Some(&killed.positions)
        }
        None => None,
    };
    let path = data_file_object_path(base, &file, context.prefix)?;
    // Every row is wanted here: a registration indexes the file's whole
    // contents, minus the few this same commit kills.
    let per_index = per_index_scoped_entries(
        base,
        &indexes,
        table,
        data_store,
        &file,
        &path,
        scoped_read::ScopedRows::All,
    )
    .await?;

    // A row this same commit deletes out of the file it also registers is
    // never indexed, rather than indexed and then removed. The two are not
    // interchangeable: an entry's key carries no file, so a removal beside
    // an add would be indistinguishable from an UPDATE's — which rewrites
    // a row into a new file under its preserved id and must keep its entry.
    let mut entries = Vec::new();
    for (index, scoped) in indexes.iter().zip(per_index) {
        let surviving = scoped
            .into_iter()
            .enumerate()
            .filter(|(ordinal, _)| !killed.is_some_and(|k| k.contains(&(*ordinal as u64))))
            .map(|(_, entry)| entry)
            .collect();
        push_index_entries(&mut entries, index, surviving, false)?;
    }
    Ok(entries)
}

/// One scoped read of `file` covering every index's columns at once — the
/// footer and any shared column chunks are fetched a single time — split
/// back into per-index entry lists, ordered as `indexes`. `rows` names which
/// of the file's rows to decode, so a caller wanting a few of them pays for
/// a few of them.
pub(super) async fn per_index_scoped_entries(
    base: &CatalogSnapshot,
    indexes: &[IndexInfo],
    table: TableId,
    data_store: &Arc<dyn ObjectStore>,
    file: &proto::DataFileValue,
    path: &object_store::path::Path,
    rows: scoped_read::ScopedRows<'_>,
) -> Result<Vec<Vec<ScopedReadEntry>>> {
    let live_columns = base.columns_of(table);
    let mut all_positions = Vec::new();
    let mut spans = Vec::with_capacity(indexes.len());
    for index in indexes {
        let positions = index_positions(&live_columns, index, table)?;
        spans.push((all_positions.len(), positions.len()));
        all_positions.extend(positions);
    }

    // Values come back ordered exactly as `all_positions`, so each index's
    // slice of a row's values is its own columns in its own order.
    let scoped = scoped_read::scoped_read_entries(
        Arc::clone(data_store),
        path,
        &all_positions,
        rows,
        scoped_read::RowIdSource::Resolve {
            row_id_start: file.row_id_start,
        },
        Some(file.file_size_bytes),
    )
    .await?;

    Ok(spans
        .into_iter()
        .map(|(start, len)| {
            scoped
                .iter()
                .map(|entry| ScopedReadEntry {
                    row_id: entry.row_id,
                    values: entry.values[start..start + len].to_vec(),
                })
                .collect()
        })
        .collect())
}

/// Returns the ids of any building indexes a duplicate poisoned — the
/// caller records the flag on their definitions — with the bytes the
/// entries put on the batch.
pub(super) async fn stage_index_maintenance(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    data_store: Option<&Arc<dyn ObjectStore>>,
    data_prefix: &str,
) -> Result<StagedEntries> {
    let pending_schemas = pending_inline_schemas(ops);
    let context = FileContext {
        store: data_store,
        prefix: data_prefix,
        compacted: compaction_only_tables(ops)?,
    };

    let mut entries: Vec<StagedIndexEntry> = Vec::new();
    let mut deferred: Vec<u64> = Vec::new();
    // Rows this commit kills, grouped by where their values must be read
    // from: an inline chunk, or a position range of one data file.
    let mut inline_deletes: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut file_deletes: HashMap<(u64, u64), KilledRows> = HashMap::new();

    // Deletes are collected before any add is derived: a delete against a
    // data file this same commit registers decides which of that file's
    // rows are indexed at all, so it has to be known by the time the file
    // is read. Nothing fixes the two rows' order within the batch.
    collect_deletes(base, ops, &context, &mut inline_deletes, &mut file_deletes).await?;

    let registered = stage_adds(
        db_tx,
        base,
        ops,
        &pending_schemas,
        &file_deletes,
        &context,
        &mut entries,
        &mut deferred,
    )
    .await?;

    // Every table's old indexed values live in a disjoint inline range. Read
    // those ranges together, then append them by table id so store latency is
    // overlapped without making staging or failures timing-dependent.
    let mut inline_targets: Vec<_> = inline_deletes.iter().collect();
    inline_targets.sort_by_key(|(table_id, _)| *table_id);
    let pending_schemas = &pending_schemas;
    let inline_removals = resolve_in_order(
        inline_targets
            .into_iter()
            .map(|(table_id, row_ids)| async move {
                let mut target_entries = Vec::new();
                stage_inline_delete_entries(
                    db_tx,
                    base,
                    ops,
                    pending_schemas,
                    *table_id,
                    row_ids,
                    &mut target_entries,
                )
                .await?;
                Ok(target_entries)
            }),
        FILE_READ_CONCURRENCY,
    )
    .await?;
    entries.extend(inline_removals.into_iter().flatten());
    // A target already resolved at the add is skipped: the rows that delete
    // kills were left out of the file's entries rather than staged and
    // removed. The rest are sorted before they are read, so which target a
    // failure names depends on neither the map's iteration order nor on
    // which read finished first.
    let mut targets: Vec<(&(u64, u64), &KilledRows)> = file_deletes
        .iter()
        .filter(|((table_id, data_file_id), _)| !registered.contains(&(*table_id, *data_file_id)))
        .collect();
    targets.sort_by_key(|(target, _)| *target);

    let mut removed = std::pin::pin!(
        stream::iter(targets)
            .map(|((table_id, data_file_id), killed)| {
                file_delete_index_entries(base, *table_id, *data_file_id, killed, &context)
            })
            .buffered(FILE_READ_CONCURRENCY)
    );
    while let Some(target_entries) = removed.try_next().await? {
        entries.extend(target_entries);
    }

    if entries.is_empty() {
        return Ok(StagedEntries {
            poisoned: Vec::new(),
            deferred,
            bytes: 0,
        });
    }
    let mut staged = stage_index_entries(db_tx, &entries).await?;
    deferred.sort_unstable();
    deferred.dedup();
    staged.deferred = deferred;
    Ok(staged)
}

/// Groups every row this commit kills: inlined rows by table, and file
/// rows by the `(table_id, data_file_id)` whose positions they name.
async fn collect_deletes(
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    context: &FileContext<'_>,
    inline_deletes: &mut HashMap<u64, Vec<u64>>,
    file_deletes: &mut HashMap<(u64, u64), KilledRows>,
) -> Result<()> {
    let mut delete_files: Vec<&[Cell]> = Vec::new();
    for op in ops {
        match op {
            RowOperation::Insert {
                table: TableKind::DeleteFile,
                cells,
            } => {
                delete_files.push(cells);
            }
            RowOperation::InlineFileDelete {
                table_id,
                data_file_id,
                row_id: position,
                ..
            } => {
                // An inlined file-delete names a physical position in the
                // file, exactly as a delete file's `pos` does.
                file_deletes
                    .entry((*table_id, *data_file_id))
                    .or_default()
                    .insert_position(*position);
            }
            RowOperation::InlineInlineDelete {
                table_id, row_id, ..
            } => {
                inline_deletes.entry(*table_id).or_default().push(*row_id);
            }
            _ => {}
        }
    }

    // Each delete file is its own fetch and names its own target, so the
    // whole commit's are read together and merged in stage order. A
    // target's positions are a set, so a merge order never changes them.
    let mut collected = std::pin::pin!(
        stream::iter(&delete_files)
            .map(|cells| delete_file_rows(base, cells, context))
            .buffered(FILE_READ_CONCURRENCY)
    );
    while let Some(killed) = collected.try_next().await? {
        if let Some((target, positions)) = killed {
            file_deletes
                .entry(target)
                .or_default()
                .positions
                .extend(positions);
        }
    }

    Ok(())
}

/// Derives the entry adds for every data file and inline chunk this commit
/// registers, in stage order, and returns the `(table_id, data_file_id)`
/// of the data files among them — the targets whose deletes are already
/// accounted for here rather than as removals. A file registered by a
/// commit that only compacts its table derives nothing and is not among
/// them.
#[allow(clippy::too_many_arguments)]
async fn stage_adds(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    pending_schemas: &HashMap<(u64, u64), &[u8]>,
    file_deletes: &HashMap<(u64, u64), KilledRows>,
    context: &FileContext<'_>,
    entries: &mut Vec<StagedIndexEntry>,
    deferred: &mut Vec<u64>,
) -> Result<HashSet<(u64, u64)>> {
    let mut registered: HashSet<(u64, u64)> = HashSet::new();
    let mut adds: Vec<Add<'_>> = Vec::new();
    for op in ops {
        match op {
            RowOperation::Insert {
                table: TableKind::DataFile,
                cells,
            } => {
                let file = decode_data_file(cells)?;
                // Compaction re-homes rows it neither renumbers nor
                // rewrites, so every entry this file would derive is
                // already stored under the same key. Skipped rather than
                // re-derived: a merge of a whole table would read every
                // merged file and stage one entry per row per index, which
                // past a few million rows exceeds what one commit may stage
                // at all. Left out of `registered` too — the commit stages
                // no delete against a file it compacts, and a claim that it
                // resolved one here would be false.
                if context.compacted.contains(&file.table_id) {
                    continue;
                }

                deferred.extend(
                    base.indexes_of(TableId::new(file.table_id))
                        .into_iter()
                        .filter(|index| {
                            index.maintenance == crate::catalog::IndexMaintenance::Deferred
                        })
                        .map(|index| index.id.get()),
                );

                let key = (file.table_id, file.data_file_id);
                registered.insert(key);
                adds.push(Add::File {
                    cells,
                    killed: file_deletes.get(&key),
                });
            }
            RowOperation::InlineInsert {
                table_id,
                schema_version,
                row_id_start,
                row_count,
                arrow_body,
                ..
            } => {
                deferred.extend(
                    base.indexes_of(TableId::new(*table_id))
                        .into_iter()
                        .filter(|index| {
                            index.maintenance == crate::catalog::IndexMaintenance::Deferred
                        })
                        .map(|index| index.id.get()),
                );
                adds.push(Add::Inline {
                    table_id: *table_id,
                    chunk: InlineChunk {
                        schema_version: *schema_version,
                        row_id_start: *row_id_start,
                        row_count: *row_count,
                        body: arrow_body,
                    },
                });
            }
            _ => {}
        }
    }

    // Every add reads its own bytes and none depends on another, so they
    // are read together: a commit registering many files costs one store
    // round trip per wave rather than one per file. Entries come back in
    // stage order, so which add a failure names does not depend on which
    // read happened to finish first.
    let mut derived = std::pin::pin!(
        stream::iter(&adds)
            .map(|add| async move {
                match add {
                    Add::File { cells, killed } => {
                        data_file_index_entries(base, cells, *killed, context).await
                    }
                    Add::Inline { table_id, chunk } => {
                        inline_chunk_index_entries(
                            db_tx,
                            base,
                            pending_schemas,
                            *table_id,
                            chunk,
                            None,
                        )
                        .await
                    }
                }
            })
            .buffered(FILE_READ_CONCURRENCY)
    );
    // Drained rather than collected: the reads stay in flight together
    // while each add's entries land in `entries` as they arrive, so the
    // batch's largest part is never held twice.
    while let Some(add_entries) = derived.try_next().await? {
        entries.extend(add_entries);
    }

    Ok(registered)
}

/// One add a commit derives entries from, held in stage order until every
/// add's bytes have been read.
enum Add<'a> {
    /// A registered data file, read from the data store.
    File {
        cells: &'a [Cell],
        killed: Option<&'a KilledRows>,
    },
    /// An inlined chunk, whose rows the commit already carries and whose
    /// schema comes from the catalog.
    Inline {
        table_id: u64,
        chunk: InlineChunk<'a>,
    },
}

/// The tables this commit compacts and otherwise leaves alone, read from
/// the `ducklake_snapshot_changes` row it stages. Empty for a commit that
/// mints no snapshot, which therefore claims no compaction.
fn compaction_only_tables(ops: &[RowOperation]) -> Result<BTreeSet<u64>> {
    let Some(cells) = ops.iter().find_map(|op| match op {
        RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells,
        } => Some(cells.as_slice()),
        _ => None,
    }) else {
        return Ok(BTreeSet::new());
    };

    // The row's own shape is validated in full when the snapshot record is
    // built; here only the leading id and the changes need decoding.
    let mut cursor = Cursor::new(TableKind::SnapshotChanges, cells);
    cursor.u64()?;
    Ok(ChangeSet::parse(&cursor.string()?).compaction_only_tables())
}

/// The inline schemas this commit registers, for a chunk whose
/// `inline/schema` record is not committed yet (the first insert).
pub(super) fn pending_inline_schemas(ops: &[RowOperation]) -> HashMap<(u64, u64), &[u8]> {
    ops.iter()
        .filter_map(|op| match op {
            RowOperation::InlineSchema {
                table_id,
                schema_version,
                arrow_schema,
            } => Some(((*table_id, *schema_version), arrow_schema.as_slice())),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod concurrency_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    /// Old-entry derivations for separate deleted tables have no dependency
    /// on one another. The shared collector overlaps them while retaining
    /// target order for deterministic staging and errors.
    #[tokio::test]
    async fn old_index_lookups_overlap_and_restore_target_order() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let futures = (0..3).map(|target| {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            async move {
                let held = active.fetch_add(1, Ordering::AcqRel) + 1;
                peak.fetch_max(held, Ordering::AcqRel);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::AcqRel);
                Ok(target)
            }
        });

        let resolved = resolve_in_order(futures, 3).await.unwrap();

        assert_eq!(resolved, vec![0, 1, 2]);
        assert_eq!(peak.load(Ordering::Relaxed), 3);
    }
}

/// One chunk version's Arrow IPC schema: this commit's staged record if it
/// registered one, else the committed one.
pub(super) async fn inline_schema_for<'a>(
    db_tx: &DbTransaction,
    pending_schemas: &HashMap<(u64, u64), &'a [u8]>,
    table_id: u64,
    schema_version: u64,
) -> Result<std::borrow::Cow<'a, [u8]>> {
    if let Some(bytes) = pending_schemas.get(&(table_id, schema_version)) {
        return Ok(std::borrow::Cow::Borrowed(bytes));
    }
    let stored = store_inline::read_inline_schema(ReadHandle::Tx(db_tx), table_id, schema_version)
        .await?
        .ok_or_else(|| {
            Error::Corruption(format!(
                "no inline schema for table {table_id} version {schema_version}"
            ))
        })?;
    Ok(std::borrow::Cow::Owned(stored.arrow_schema))
}

/// The index entries for one inline chunk's rows: every row when `held` is
/// `None` (the chunk is being inserted), or just those rows when it is
/// `Some` (they are being deleted, so their entries are removals).
pub(super) async fn inline_chunk_index_entries(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    pending_schemas: &HashMap<(u64, u64), &[u8]>,
    table_id: u64,
    chunk: &InlineChunk<'_>,
    held: Option<&HashSet<u64>>,
) -> Result<Vec<StagedIndexEntry>> {
    let table = TableId::new(table_id);
    let indexes: Vec<_> = base
        .indexes_of(table)
        .into_iter()
        .filter(|index| {
            held.is_some() || index.maintenance != crate::catalog::IndexMaintenance::Deferred
        })
        .collect();
    if indexes.is_empty() {
        return Ok(Vec::new());
    }

    let schema_ipc =
        inline_schema_for(db_tx, pending_schemas, table_id, chunk.schema_version).await?;
    let live_columns = base.columns_of(table);
    let mut entries = Vec::new();
    for index in &indexes {
        let positions = index_positions(&live_columns, index, table)?;
        let scoped = scoped_read::inline_batch_entries(
            &schema_ipc,
            chunk.body,
            &positions,
            chunk.row_id_start,
        )?
        .into_iter()
        .filter(|entry| held.is_none_or(|rows| rows.contains(&entry.row_id)))
        .collect();
        push_index_entries(&mut entries, index, scoped, held.is_some())?;
    }
    Ok(entries)
}

/// One inline chunk this commit can read: already committed, or staged by
/// the commit itself.
pub(super) struct InlineChunk<'a> {
    schema_version: u64,
    row_id_start: u64,
    row_count: u64,
    body: &'a [u8],
}

impl InlineChunk<'_> {
    fn holds(&self, row_id: u64) -> bool {
        row_id >= self.row_id_start && row_id < self.row_id_start.saturating_add(self.row_count)
    }
}

/// Derives the entry removals for rows tombstoned out of inline chunks. A
/// tombstoned row's indexed values come from the chunk holding it, so the
/// removal rides the same batch that kills the row.
pub(super) async fn stage_inline_delete_entries(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    pending_schemas: &HashMap<(u64, u64), &[u8]>,
    table_id: u64,
    row_ids: &[u64],
    entries: &mut Vec<StagedIndexEntry>,
) -> Result<()> {
    let table = TableId::new(table_id);
    let indexes = base.indexes_of(table);
    if indexes.is_empty() {
        return Ok(());
    }

    let committed = store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id).await?;
    let mut chunks: Vec<InlineChunk<'_>> = committed
        .iter()
        .filter_map(|(op, value)| match op {
            InlineOperation::Insert {
                schema_version: version,
                ..
            } => Some(InlineChunk {
                schema_version: *version,
                row_id_start: value.row_id_start,
                row_count: value.row_count,
                body: &value.body,
            }),
            _ => None,
        })
        .collect();
    // A row inserted and deleted inside one commit lives in a chunk that is
    // still only staged.
    chunks.extend(ops.iter().filter_map(|op| match op {
        RowOperation::InlineInsert {
            table_id: owner,
            schema_version,
            row_id_start,
            row_count,
            arrow_body,
            ..
        } if *owner == table_id => Some(InlineChunk {
            schema_version: *schema_version,
            row_id_start: *row_id_start,
            row_count: *row_count,
            body: arrow_body,
        }),
        _ => None,
    }));

    let mut covered: HashSet<u64> = HashSet::new();
    for chunk in &chunks {
        let held: HashSet<u64> = row_ids
            .iter()
            .copied()
            .filter(|&row_id| chunk.holds(row_id))
            .collect();
        if held.is_empty() {
            continue;
        }
        entries.extend(
            inline_chunk_index_entries(db_tx, base, pending_schemas, table_id, chunk, Some(&held))
                .await?,
        );
        covered.extend(held);
    }

    // A tombstone naming no live chunk would leave its entries behind, which
    // is the leak this derivation exists to prevent.
    if let Some(missing) = row_ids.iter().find(|row_id| !covered.contains(row_id)) {
        return Err(Error::Corruption(format!(
            "inline delete of row {missing} on indexed table {table_id} names no inline chunk, so \
             its index entries cannot be derived"
        )));
    }
    Ok(())
}

/// The physical row positions a commit kills inside one data file. Both a
/// delete file's `pos` column and an inlined file-delete name positions,
/// not row ids; the target's scoped read resolves each position to the row
/// it holds. Ordered, because the read selects on these positions and a
/// selection is built in file order.
#[derive(Debug, Default)]
pub(super) struct KilledRows {
    positions: BTreeSet<u64>,
}

impl KilledRows {
    /// Records a killed physical row position.
    pub(super) fn insert_position(&mut self, position: u64) {
        self.positions.insert(position);
    }
}

/// The target a staged `register_delete_file` names and the positions it
/// kills, read out of the delete file verbatim — resolving them to row ids
/// would bake in the dense assumption the target's scoped read decides.
/// `None` when the table carries no index, so nothing is read.
///
/// Returns them rather than recording them, so a commit's delete files can
/// be read concurrently and merged afterwards.
pub(super) async fn delete_file_rows(
    base: &CatalogSnapshot,
    cells: &[Cell],
    context: &FileContext<'_>,
) -> Result<Option<((u64, u64), Vec<u64>)>> {
    let delete_file = decode_delete_file(cells)?;
    let table = TableId::new(delete_file.table_id);
    if base.indexes_of(table).is_empty() {
        return Ok(None);
    }

    let data_store = context.store.ok_or_else(|| {
        Error::Constraint(format!(
            "delete file {} on indexed table {} cannot be read to maintain its equality index: no \
             data-path store is available",
            delete_file.delete_file_id, delete_file.table_id
        ))
    })?;

    let path = delete_file_object_path(base, &delete_file, context.prefix)?;
    let positions = scoped_read::delete_file_positions(data_store, &path).await?;
    Ok(Some((
        (delete_file.table_id, delete_file.data_file_id),
        positions,
    )))
}

/// Derives the entry removals for rows killed inside one data file the
/// committed head already holds, by scoped-reading the file and keeping
/// the rows this commit marks dead. A delete against a file this same
/// commit registers never reaches here — those rows are left unindexed at
/// the add instead.
///
/// Returns its entries rather than appending them, so a commit's targets
/// can be read concurrently and their entries appended in target order.
pub(super) async fn file_delete_index_entries(
    base: &CatalogSnapshot,
    table_id: u64,
    data_file_id: u64,
    killed: &KilledRows,
    context: &FileContext<'_>,
) -> Result<Vec<StagedIndexEntry>> {
    let table = TableId::new(table_id);
    let indexes = base.indexes_of(table);
    if indexes.is_empty() {
        return Ok(Vec::new());
    }

    let file = live_data_file(base, table_id, data_file_id)?;
    let data_store = context.store.ok_or_else(|| {
        Error::Constraint(format!(
            "data file {data_file_id} on indexed table {table_id} cannot be read to maintain its \
             equality index: no data-path store is available"
        ))
    })?;

    refuse_out_of_range(killed, &file)?;

    let path = data_file_object_path(base, &file, context.prefix)?;
    // An entry dies when a delete names its physical position, so only those
    // positions are read: a delete costs the rows it removes rather than the
    // file holding them. The scoped read resolves each position to the row it
    // holds — one rule for dense and per-row-id targets alike.
    let per_index = per_index_scoped_entries(
        base,
        &indexes,
        table,
        data_store,
        &file,
        &path,
        scoped_read::ScopedRows::At(&killed.positions),
    )
    .await?;
    let mut entries = Vec::new();
    for (index, scoped) in indexes.iter().zip(per_index) {
        push_index_entries(&mut entries, index, scoped, true)?;
    }
    Ok(entries)
}

/// One live data file of a table.
fn live_data_file(
    base: &CatalogSnapshot,
    table_id: u64,
    data_file_id: u64,
) -> Result<proto::DataFileValue> {
    base.data_files
        .get(&table_id)
        .and_then(|files| files.get(&data_file_id))
        .cloned()
        .ok_or_else(|| Error::NotFound(format!("data file {data_file_id} of table {table_id}")))
}

/// Positions are physical row ordinals read out of the delete file; one
/// naming a row the target does not hold could never match a scoped entry
/// and would silently orphan index rows, so refuse it.
fn refuse_out_of_range(killed: &KilledRows, file: &proto::DataFileValue) -> Result<()> {
    for &position in &killed.positions {
        if position >= file.record_count {
            return Err(Error::Constraint(format!(
                "delete file for data file {} on table {} names position {position} outside the \
                 file's record count {}",
                file.data_file_id, file.table_id, file.record_count
            )));
        }
    }
    Ok(())
}

/// The object path of a registered data file: its stored path relative to the
/// table's data directory (`<schema path><table path>`) under `DATA_PATH`,
/// whose bucket-relative prefix leads for an `s3://` store.
pub(super) fn data_file_object_path(
    base: &CatalogSnapshot,
    file: &proto::DataFileValue,
    data_prefix: &str,
) -> Result<object_store::path::Path> {
    table_object_path(
        base,
        file.table_id,
        &file.path,
        file.path_is_relative,
        data_prefix,
    )
}

/// The object path of a registered delete file, resolved exactly as its
/// target data file's is.
pub(super) fn delete_file_object_path(
    base: &CatalogSnapshot,
    file: &proto::DeleteFileValue,
    data_prefix: &str,
) -> Result<object_store::path::Path> {
    table_object_path(
        base,
        file.table_id,
        &file.path,
        file.path_is_relative,
        data_prefix,
    )
}

pub(super) fn table_object_path(
    base: &CatalogSnapshot,
    table_id: u64,
    path: &str,
    path_is_relative: bool,
    data_prefix: &str,
) -> Result<object_store::path::Path> {
    // A missing table here is corruption, not a caller mistake: the file
    // row itself named it.
    let table_prefix = base
        .table_data_prefix(TableId::new(table_id))
        .map_err(|err| match err {
            Error::NotFound(_) => {
                Error::Corruption(format!("registered file names unknown table {table_id}"))
            }
            other => other,
        })?;
    let relative = match (path_is_relative, data_prefix.is_empty()) {
        (false, _) => path.to_owned(),
        (true, true) => format!("{table_prefix}{path}"),
        (true, false) => format!("{data_prefix}/{table_prefix}{path}"),
    };
    Ok(object_store::path::Path::from(relative.as_str()))
}

/// The physical positions of an index's columns in a file or chunk written
/// under the current schema: each column's 0-based rank among the table's
/// columns (the order `columns_of` returns).
pub(super) fn index_positions(
    live_columns: &[ColumnInfo],
    index: &IndexInfo,
    table: TableId,
) -> Result<Vec<usize>> {
    index
        .columns
        .iter()
        .map(|column| {
            live_columns
                .iter()
                .position(|c| c.id == *column)
                .ok_or_else(|| Error::NotFound(format!("indexed column {column} of table {table}")))
        })
        .collect()
}

/// Turns scoped-read entries into staged index entries — puts when `delete`
/// is false, removals when it is true. A row with a NULL indexed value is
/// stored multi-shaped (so `IS NULL` finds it) rather than skipped.
pub(super) fn push_index_entries(
    entries: &mut Vec<StagedIndexEntry>,
    index: &IndexInfo,
    scoped: Vec<ScopedReadEntry>,
    delete: bool,
) -> Result<()> {
    for entry in scoped {
        // A row with any NULL indexed column is stored so `IS NULL` finds it,
        // but multi-shaped and collision-exempt — a unique index still admits
        // any number of NULL rows.
        let has_null = entry.values.iter().any(Option::is_none);
        entries.push(StagedIndexEntry {
            index_id: index.id.get(),
            unique: index.unique && !has_null,
            key: encode_ordered_values(&entry.values, &index.directions, &index.nulls)?,
            row_id: entry.row_id,
            delete,
            building: index.state != crate::catalog::IndexState::Ready,
        });
    }
    Ok(())
}
