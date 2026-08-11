//! Equality-index upkeep for staged commits: deriving entry adds and
//! removals from registered files and inline chunks by scoped-reading
//! them.

use std::{collections::BTreeSet, future::Future, sync::LazyLock};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use futures::{SinkExt, StreamExt, TryStreamExt, channel::mpsc, stream};

use super::{
    Arc, CatalogSnapshot, Cell, ColumnInfo, DbTransaction, Error, HashMap, HashSet, IndexInfo,
    InlineOperation, ObjectStore, ReadHandle, Result, RowOperation, StagedEntries,
    StagedIndexEntry, TableId, TableKind, commit,
    decode::{Cursor, decode_data_file, decode_delete_file},
    inline::inline_chunk_range_write,
    proto, scoped_read, stage_index_entry_stream, store_inline,
};
use crate::transaction::operations::ChangeSet;

/// How many files one upkeep phase reads at once. Each read is an
/// independent fetch of an independent file, so reading them one after
/// another costs a commit one round trip of store latency per file — the
/// term that dominates a commit touching many. Bounding the fan-out
/// instead keeps a single commit from monopolizing the store's request
/// budget.
const FILE_READ_CONCURRENCY: usize = 64;

/// How many derived additions may wait behind uniqueness probes. This keeps
/// file reads and Arrow decoding moving without retaining an unbounded
/// commit's entries in memory.
const DERIVED_ENTRY_BUFFER: usize = 512;

/// Entries each concurrently read source may hold while an earlier source
/// drains. Across the read window this equals [`DERIVED_ENTRY_BUFFER`].
const ADDITION_SOURCE_BUFFER: usize = DERIVED_ENTRY_BUFFER / FILE_READ_CONCURRENCY;

/// Process-wide limit for Arrow decoding performed on blocking workers.
/// Commits share it so concurrent writers cannot multiply CPU pressure.
const INLINE_DECODE_CONCURRENCY: usize = 8;

static INLINE_DECODE_PERMITS: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(INLINE_DECODE_CONCURRENCY)));

/// Decoded inline schemas shared by every chunk in one commit. Holding the
/// mutex through the first catalog read guarantees concurrent chunks of one
/// table version still decode its IPC exactly once.
struct InlineSchemaCache<'a> {
    pending: &'a HashMap<(u64, u64), &'a [u8]>,
    decoded: tokio::sync::Mutex<HashMap<(u64, u64), SchemaRef>>,
}

impl<'a> InlineSchemaCache<'a> {
    fn new(pending: &'a HashMap<(u64, u64), &'a [u8]>) -> Self {
        Self {
            pending,
            decoded: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn get(
        &self,
        db_tx: &DbTransaction,
        table_id: u64,
        schema_version: u64,
    ) -> Result<SchemaRef> {
        let key = (table_id, schema_version);
        let mut decoded = self.decoded.lock().await;
        if let Some(schema) = decoded.get(&key) {
            return Ok(Arc::clone(schema));
        }

        let schema_ipc = if let Some(bytes) = self.pending.get(&key) {
            Bytes::copy_from_slice(bytes)
        } else {
            let stored =
                store_inline::read_inline_schema(ReadHandle::Tx(db_tx), table_id, schema_version)
                    .await?
                    .ok_or_else(|| {
                        Error::Corruption(format!(
                            "no inline schema for table {table_id} version {schema_version}"
                        ))
                    })?;
            Bytes::from(stored.arrow_schema)
        };
        let schema = scoped_read::decode_inline_schema(schema_ipc)?;
        decoded.insert(key, Arc::clone(&schema));
        Ok(schema)
    }
}

/// Resolves independent reads concurrently and returns their results in
/// target order. Keeping the order stable makes staging and error selection
/// independent of remote completion timing.
#[cfg(test)]
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

/// Resolves independent entry groups in target order while moving each
/// completed group directly into the final allocation.
async fn resolve_entries_in_order<I, F>(
    futures: I,
    concurrency: usize,
) -> Result<Vec<StagedIndexEntry>>
where
    I: IntoIterator<Item = F>,
    F: Future<Output = Result<Vec<StagedIndexEntry>>>,
{
    let mut pending = std::pin::pin!(stream::iter(futures).buffered(concurrency));
    let mut entries = Vec::new();
    while let Some(group) = pending.try_next().await? {
        entries.extend(group);
    }
    Ok(entries)
}

/// Runs synchronous decode work off the async executor under a shared bound.
async fn run_bounded_blocking<T, F>(permits: Arc<tokio::sync::Semaphore>, work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    let permit = permits.acquire_owned().await.map_err(|_| {
        Error::Interrupted("inline index decode limiter stopped before work began".to_owned())
    })?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|err| Error::Interrupted(format!("inline index decode worker stopped: {err}")))?
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

/// Streams equality-index entries for one registered data file. Fused Arrow
/// encoding sends each bounded read batch before the reader advances, while
/// the caller overlaps independent files and drains their channels in stage
/// order.
async fn stream_data_file_index_entries(
    base: &CatalogSnapshot,
    cells: &[Cell],
    killed: Option<&KilledRows>,
    context: &FileContext<'_>,
    sender: &mut mpsc::Sender<Result<StagedIndexEntry>>,
) -> Result<()> {
    let file = decode_data_file(cells)?;
    let table = TableId::new(file.table_id);
    let indexes: Vec<_> = base
        .indexes_of(table)
        .into_iter()
        .filter(|index| index.maintenance != crate::catalog::IndexMaintenance::Deferred)
        .collect();
    if indexes.is_empty() {
        return Ok(());
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
    // A row this same commit deletes out of the file it also registers is
    // never indexed, rather than indexed and then removed. The two are not
    // interchangeable: an entry's key carries no file, so a removal beside
    // an add would be indistinguishable from an UPDATE's — which rewrites
    // a row into a new file under its preserved id and must keep its entry.
    let live_columns = base.columns_of(table);
    let projections = index_projections(&live_columns, &indexes, table)?;
    let mut consumer = IndexAdditionConsumer {
        indexes: &indexes,
        sender,
    };
    scoped_read::scoped_read_index_entry_batches(
        Arc::clone(data_store),
        &path,
        &projections,
        scoped_read::RowIdSource::Resolve {
            row_id_start: file.row_id_start,
        },
        Some(file.file_size_bytes),
        Some(file.footer_size),
        killed,
        &mut consumer,
    )
    .await
}

struct IndexAdditionConsumer<'a, 'b> {
    indexes: &'a [IndexInfo],
    sender: &'b mut mpsc::Sender<Result<StagedIndexEntry>>,
}

impl scoped_read::ScopedIndexEntryBatchConsumer for IndexAdditionConsumer<'_, '_> {
    async fn consume(&mut self, entries: Vec<scoped_read::ScopedIndexEntry>) -> Result<()> {
        for entry in staged_scoped_entries(self.indexes, entries, false)? {
            if self.sender.send(Ok(entry)).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}

/// One scoped read of `file` covering every index's columns at once. The
/// footer and shared column chunks are fetched once, and values become final
/// canonical keys while their Arrow arrays are borrowed.
#[allow(clippy::too_many_arguments)]
pub(super) async fn scoped_index_entries(
    base: &CatalogSnapshot,
    indexes: &[IndexInfo],
    table: TableId,
    data_store: &Arc<dyn ObjectStore>,
    file: &proto::DataFileValue,
    path: &object_store::path::Path,
    rows: scoped_read::ScopedRows<'_>,
    excluded_ordinals: Option<&BTreeSet<u64>>,
) -> Result<Vec<scoped_read::ScopedIndexEntry>> {
    let live_columns = base.columns_of(table);
    let projections = index_projections(&live_columns, indexes, table)?;
    scoped_read::scoped_read_index_entries(
        Arc::clone(data_store),
        path,
        &projections,
        rows,
        scoped_read::RowIdSource::Resolve {
            row_id_start: file.row_id_start,
        },
        Some(file.file_size_bytes),
        Some(file.footer_size),
        excluded_ordinals,
    )
    .await
}

/// Compiles one Arrow projection per index. Shared source positions are
/// deduplicated by the scoped reader before Parquet columns are fetched.
fn index_projections(
    live_columns: &[ColumnInfo],
    indexes: &[IndexInfo],
    table: TableId,
) -> Result<Vec<scoped_read::IndexProjection>> {
    indexes
        .iter()
        .map(|index| {
            Ok(scoped_read::IndexProjection {
                index_id: index.id.get(),
                unique: index.unique,
                positions: index_positions(live_columns, index, table)?,
                directions: index.directions.clone(),
                nulls: index.nulls.clone(),
            })
        })
        .collect()
}

/// Attaches index metadata to already encoded Arrow keys.
fn staged_scoped_entries(
    indexes: &[IndexInfo],
    scoped: Vec<scoped_read::ScopedIndexEntry>,
    delete: bool,
) -> Result<Vec<StagedIndexEntry>> {
    scoped
        .into_iter()
        .map(|entry| {
            let index = indexes.get(entry.index).ok_or_else(|| {
                Error::Corruption("scoped read returned an unknown index projection".to_owned())
            })?;

            Ok(StagedIndexEntry {
                index_id: index.id.get(),
                unique: entry.unique,
                key: entry.key,
                row_id: entry.row_id,
                delete,
                building: index.state != crate::catalog::IndexState::Ready,
            })
        })
        .collect()
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
    let inline_schemas = InlineSchemaCache::new(&pending_schemas);
    let context = FileContext {
        store: data_store,
        prefix: data_prefix,
        compacted: compaction_only_tables(ops)?,
    };

    // Rows this commit kills, grouped by where their values must be read
    // from: an inline chunk, or a position range of one data file.
    let mut inline_deletes: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut file_deletes: HashMap<(u64, u64), KilledRows> = HashMap::new();

    // Deletes are collected before any add is derived: a delete against a
    // data file this same commit registers decides which of that file's
    // rows are indexed at all, so it has to be known by the time the file
    // is read. Nothing fixes the two rows' order within the batch.
    collect_deletes(base, ops, &context, &mut inline_deletes, &mut file_deletes).await?;

    let AddPlan {
        registered,
        mut deferred,
        adds,
    } = plan_adds(base, ops, &file_deletes, &context)?;

    let mut inline_targets: Vec<_> = inline_deletes.iter().collect();
    inline_targets.sort_by_key(|(table_id, _)| *table_id);
    let mut targets: Vec<(&(u64, u64), &KilledRows)> = file_deletes
        .iter()
        .filter(|((table_id, data_file_id), _)| !registered.contains(&(*table_id, *data_file_id)))
        .collect();
    targets.sort_by_key(|(target, _)| *target);

    let mut addition_receivers = Vec::with_capacity(adds.len());
    let mut addition_futures = Vec::with_capacity(adds.len());
    for add in adds {
        let (sender, receiver) = mpsc::channel(ADDITION_SOURCE_BUFFER);
        addition_receivers.push(receiver);
        addition_futures.push(produce_addition(
            db_tx,
            base,
            &inline_schemas,
            &context,
            add,
            sender,
        ));
    }
    let produce_additions = stream::iter(addition_futures)
        .buffer_unordered(FILE_READ_CONCURRENCY)
        .collect::<Vec<_>>();
    let additions = stream::iter(addition_receivers).flatten();

    let derive_and_stage = async {
        let (inline_removals, file_removals) = futures::try_join!(
            derive_inline_removals(db_tx, base, ops, &inline_schemas, inline_targets),
            derive_file_removals(base, &context, targets),
        )?;
        let (inline_removals, locator_writes) = inline_removals;
        let deletions = inline_removals.into_iter().chain(file_removals);
        let mut staged = stage_index_entry_stream(db_tx, deletions, additions, 0).await?;
        let locator_bytes = commit::stage_writes(db_tx, &locator_writes)?;
        staged.bytes = staged.bytes.saturating_add(locator_bytes.0);
        staged.uses_inline_chunk_directory = !locator_writes.is_empty();
        Ok::<_, Error>(staged)
    };
    let ((), mut staged) = futures::try_join!(
        async {
            produce_additions.await;
            Ok::<_, Error>(())
        },
        derive_and_stage,
    )?;
    deferred.sort_unstable();
    deferred.dedup();
    staged.deferred = deferred;

    Ok(staged)
}

async fn produce_addition(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    inline_schemas: &InlineSchemaCache<'_>,
    context: &FileContext<'_>,
    add: Add<'_>,
    mut sender: mpsc::Sender<Result<StagedIndexEntry>>,
) {
    let result = match add {
        Add::File { cells, killed } => {
            stream_data_file_index_entries(base, cells, killed, context, &mut sender).await
        }
        Add::Inline { table_id, chunk } => {
            match inline_chunk_index_entries(db_tx, base, inline_schemas, table_id, &chunk, None)
                .await
            {
                Ok(entries) => {
                    for entry in entries {
                        if sender.send(Ok(entry)).await.is_err() {
                            return;
                        }
                    }
                    Ok(())
                }
                Err(error) => Err(error),
            }
        }
    };
    if let Err(error) = result {
        let _ = sender.send(Err(error)).await;
    }
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
            } if !context.compacted.contains(table_id) => {
                // An inlined file-delete names a physical position in the
                // file, exactly as a delete file's `pos` does.
                file_deletes
                    .entry((*table_id, *data_file_id))
                    .or_default()
                    .insert_position(*position);
            }
            RowOperation::InlineInlineDelete {
                table_id, row_id, ..
            } if !context.compacted.contains(table_id) => {
                inline_deletes.entry(*table_id).or_default().push(*row_id);
            }
            _ => {}
        }
    }

    // Each delete file is its own fetch and names its own target, so the
    // whole commit's are read together and merged in stage order. A
    // target's positions are a set, so a merge order never changes them.
    let futures: Vec<_> = delete_files
        .into_iter()
        .map(|cells| delete_file_rows(base, cells, context))
        .collect();
    let mut collected = std::pin::pin!(stream::iter(futures).buffered(FILE_READ_CONCURRENCY));
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

/// Plans entry additions without reading their bytes. The registered set is
/// available immediately, so old-file removals can start beside the reads
/// that derive additions.
fn plan_adds<'a>(
    base: &CatalogSnapshot,
    ops: &'a [RowOperation],
    file_deletes: &'a HashMap<(u64, u64), KilledRows>,
    context: &FileContext<'_>,
) -> Result<AddPlan<'a>> {
    let mut registered: HashSet<(u64, u64)> = HashSet::new();
    let mut deferred = Vec::new();
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

    Ok(AddPlan {
        registered,
        deferred,
        adds,
    })
}

/// Derives old entries for deleted inline rows, overlapping tables while
/// retaining table-id order.
async fn derive_inline_removals(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    inline_schemas: &InlineSchemaCache<'_>,
    targets: Vec<(&u64, &Vec<u64>)>,
) -> Result<(Vec<StagedIndexEntry>, Vec<commit::StagedWrite>)> {
    let futures: Vec<_> = targets
        .into_iter()
        .map(|(table_id, row_ids)| async move {
            stage_inline_delete_entries(db_tx, base, ops, inline_schemas, *table_id, row_ids).await
        })
        .collect();
    let resolved: Vec<_> = stream::iter(futures)
        .buffered(FILE_READ_CONCURRENCY)
        .try_collect()
        .await?;
    let mut entries = Vec::new();
    let mut locator_writes = Vec::new();
    for (group, writes) in resolved {
        entries.extend(group);
        locator_writes.extend(writes);
    }
    Ok((entries, locator_writes))
}

/// Derives old entries for rows deleted from committed data files.
async fn derive_file_removals(
    base: &CatalogSnapshot,
    context: &FileContext<'_>,
    targets: Vec<(&(u64, u64), &KilledRows)>,
) -> Result<Vec<StagedIndexEntry>> {
    let futures: Vec<_> = targets
        .into_iter()
        .map(|((table_id, data_file_id), killed)| {
            file_delete_index_entries(base, *table_id, *data_file_id, killed, context)
        })
        .collect();
    resolve_entries_in_order(futures, FILE_READ_CONCURRENCY).await
}

/// Addition reads and the metadata needed to exclude same-commit targets
/// from the removal branch.
struct AddPlan<'a> {
    registered: HashSet<(u64, u64)>,
    deferred: Vec<u64>,
    adds: Vec<Add<'a>>,
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

    /// Arrow decoding leaves the async executor and shares a process-wide
    /// permit budget, so several chunks use CPU workers without an
    /// unbounded task fan-out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_work_overlaps_under_its_permit_bound() {
        const PERMITS: usize = 2;
        const TASKS: usize = 4;

        let permits = Arc::new(tokio::sync::Semaphore::new(PERMITS));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let futures = (0..TASKS).map(|position| {
            let permits = Arc::clone(&permits);
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            async move {
                run_bounded_blocking(permits, move || {
                    let held = active.fetch_add(1, Ordering::AcqRel) + 1;
                    peak.fetch_max(held, Ordering::AcqRel);
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    active.fetch_sub(1, Ordering::AcqRel);
                    Ok(position)
                })
                .await
            }
        });

        let resolved = resolve_in_order(futures, TASKS).await.unwrap();

        assert_eq!(resolved, vec![0, 1, 2, 3]);
        assert_eq!(peak.load(Ordering::Relaxed), PERMITS);
    }
}

/// The index entries for one inline chunk's rows: every row when `held` is
/// `None` (the chunk is being inserted), or just those rows when it is
/// `Some` (they are being deleted, so their entries are removals).
async fn inline_chunk_index_entries(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    inline_schemas: &InlineSchemaCache<'_>,
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

    let schema = inline_schemas
        .get(db_tx, table_id, chunk.schema_version)
        .await?;
    let plan = InlineDecodePlan {
        schema,
        body: Bytes::copy_from_slice(chunk.body),
        live_columns: base.columns_of(table),
        indexes,
        row_id_start: chunk.row_id_start,
        held: held.cloned(),
    };

    run_bounded_blocking(Arc::clone(&INLINE_DECODE_PERMITS), move || plan.derive()).await
}

/// Owned input for one blocking Arrow decode. Ownership lets the work leave
/// the async executor without tying a worker to the transaction's borrows.
struct InlineDecodePlan {
    schema: SchemaRef,
    body: Bytes,
    live_columns: Vec<ColumnInfo>,
    indexes: Vec<IndexInfo>,
    row_id_start: u64,
    held: Option<HashSet<u64>>,
}

impl InlineDecodePlan {
    /// Decodes the chunk once and writes each borrowed Arrow scalar directly
    /// into its index's final canonical key.
    fn derive(self) -> Result<Vec<StagedIndexEntry>> {
        let table = self
            .indexes
            .first()
            .map(|index| index.table_id)
            .ok_or_else(|| Error::Corruption("inline index plan has no index".to_owned()))?;
        let projections = index_projections(&self.live_columns, &self.indexes, table)?;
        let scoped = scoped_read::inline_batch_index_entries(
            self.schema,
            &self.body,
            &projections,
            self.row_id_start,
            self.held.as_ref(),
        )?;
        let delete = self.held.is_some();
        staged_scoped_entries(&self.indexes, scoped, delete)
    }
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

async fn derive_located_inline_removals(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    inline_schemas: &InlineSchemaCache<'_>,
    table_id: u64,
    row_ids: &[u64],
    locators: Vec<store_inline::InlineChunkLocator>,
) -> Result<(Vec<StagedIndexEntry>, HashSet<u64>)> {
    let mut covered = HashSet::new();
    let mut futures = Vec::with_capacity(locators.len());
    for locator in locators {
        let held: HashSet<u64> = row_ids
            .iter()
            .copied()
            .filter(|&row_id| locator.holds(row_id))
            .collect();
        covered.extend(held.iter().copied());
        futures.push(async move {
            let (operation, value) =
                store_inline::read_inline_chunk_locator(ReadHandle::Tx(db_tx), table_id, locator)
                    .await?;
            let InlineOperation::Insert { schema_version, .. } = operation else {
                return Err(Error::Corruption(
                    "inline chunk locator names a non-insert operation".to_owned(),
                ));
            };
            let chunk = InlineChunk {
                schema_version,
                row_id_start: value.row_id_start,
                row_count: value.row_count,
                body: &value.body,
            };
            inline_chunk_index_entries(db_tx, base, inline_schemas, table_id, &chunk, Some(&held))
                .await
        });
    }
    let entries = resolve_entries_in_order(futures, INLINE_DECODE_CONCURRENCY).await?;
    Ok((entries, covered))
}

async fn derive_inline_chunk_removals(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    inline_schemas: &InlineSchemaCache<'_>,
    table_id: u64,
    row_ids: &[u64],
    chunks: &[InlineChunk<'_>],
) -> Result<(Vec<StagedIndexEntry>, HashSet<u64>)> {
    let mut covered = HashSet::new();
    let mut targets = Vec::new();
    for chunk in chunks {
        let held: HashSet<u64> = row_ids
            .iter()
            .copied()
            .filter(|&row_id| chunk.holds(row_id))
            .collect();
        if !held.is_empty() {
            covered.extend(held.iter().copied());
            targets.push((chunk, held));
        }
    }
    let futures = targets.into_iter().map(|(chunk, held)| async move {
        inline_chunk_index_entries(db_tx, base, inline_schemas, table_id, chunk, Some(&held)).await
    });
    let entries = resolve_entries_in_order(futures, INLINE_DECODE_CONCURRENCY).await?;
    Ok((entries, covered))
}

fn legacy_inline_locator_writes(
    table_id: u64,
    chunks: &[(InlineOperation, proto::InlineChunkValue)],
) -> Result<Vec<commit::StagedWrite>> {
    let mut writes = Vec::with_capacity(chunks.len());
    for (operation, chunk) in chunks {
        let InlineOperation::Insert {
            schema_version,
            begin_snapshot,
            chunk_seq,
            ..
        } = operation
        else {
            continue;
        };
        writes.push(inline_chunk_range_write(
            table_id,
            *schema_version,
            *begin_snapshot,
            *chunk_seq,
            chunk.row_id_start,
            chunk.row_count,
        )?);
    }
    Ok(writes)
}

/// Derives the entry removals for rows tombstoned out of inline chunks. A
/// tombstoned row's indexed values come from the chunk holding it, so the
/// removal rides the same batch that kills the row.
async fn stage_inline_delete_entries(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    inline_schemas: &InlineSchemaCache<'_>,
    table_id: u64,
    row_ids: &[u64],
) -> Result<(Vec<StagedIndexEntry>, Vec<commit::StagedWrite>)> {
    if base.indexes_of(TableId::new(table_id)).is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let locators =
        store_inline::find_inline_chunk_locators_for_rows(ReadHandle::Tx(db_tx), table_id, row_ids)
            .await?;
    let (mut entries, mut covered, locator_writes) = if let Some(locators) = locators {
        let (entries, covered) = derive_located_inline_removals(
            db_tx,
            base,
            inline_schemas,
            table_id,
            row_ids,
            locators,
        )
        .await?;
        (entries, covered, Vec::new())
    } else {
        let committed = store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id).await?;
        let locator_writes = legacy_inline_locator_writes(table_id, &committed)?;
        let chunks: Vec<_> = committed
            .iter()
            .filter_map(|(operation, value)| match operation {
                InlineOperation::Insert { schema_version, .. } => Some(InlineChunk {
                    schema_version: *schema_version,
                    row_id_start: value.row_id_start,
                    row_count: value.row_count,
                    body: &value.body,
                }),
                _ => None,
            })
            .collect();
        let (entries, covered) =
            derive_inline_chunk_removals(db_tx, base, inline_schemas, table_id, row_ids, &chunks)
                .await?;
        (entries, covered, locator_writes)
    };

    // A row inserted and deleted inside one commit lives in a chunk that is
    // still only staged.
    let staged_chunks: Vec<_> = ops
        .iter()
        .filter_map(|op| match op {
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
        })
        .collect();
    let (staged_entries, staged_covered) = derive_inline_chunk_removals(
        db_tx,
        base,
        inline_schemas,
        table_id,
        row_ids,
        &staged_chunks,
    )
    .await?;
    entries.extend(staged_entries);
    covered.extend(staged_covered);

    // A tombstone naming no live chunk would leave its entries behind, which
    // is the leak this derivation exists to prevent.
    if let Some(missing) = row_ids.iter().find(|row_id| !covered.contains(row_id)) {
        return Err(Error::Corruption(format!(
            "inline delete of row {missing} on indexed table {table_id} names no inline chunk, so \
             its index entries cannot be derived"
        )));
    }
    Ok((entries, locator_writes))
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
    if context.compacted.contains(&delete_file.table_id) {
        return Ok(None);
    }
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
    let scoped = scoped_index_entries(
        base,
        &indexes,
        table,
        data_store,
        &file,
        &path,
        scoped_read::ScopedRows::At(&killed.positions),
        None,
    )
    .await?;
    staged_scoped_entries(&indexes, scoped, true)
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
