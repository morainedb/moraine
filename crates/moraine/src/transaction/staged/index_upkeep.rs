//! Equality-index upkeep for staged commits: deriving entry adds and
//! removals from registered files and inline chunks by scoped-reading
//! them.

use std::{collections::BTreeSet, time::Instant};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use tokio::sync::OnceCell;

use super::{
    Arc, CatalogSnapshot, Cell, ColumnInfo, DataStore, DbTransaction, Error, HashMap, HashSet,
    IndexInfo, InlineOperation, ReadHandle, Result, RowOperation, StagedEntries, StagedIndexEntry,
    TableId, TableKind, commit, data_file,
    decode::{Cursor, decode_data_file, decode_delete_file},
    inline::inline_chunk_range_write,
    proto, stage_index_entry_stream, store_inline,
};
use crate::transaction::operations::ChangeSet;

/// How many files one upkeep phase reads at once.
const FILE_READ_CONCURRENCY: usize = 64;

/// Per-commit fan-out into the process-wide Arrow encoding worker pool.
const INLINE_DECODE_CONCURRENCY: usize = 8;

/// Inline schema point reads one prefetch keeps in flight, sized for a
/// remote object store.
const INLINE_READ_CONCURRENCY: usize = 16;

/// Decoded inline schemas shared by every chunk in one commit, one cell
/// per table version.
struct InlineSchemaCache<'a> {
    pending: &'a HashMap<(u64, u64), &'a [u8]>,
    decoded: tokio::sync::Mutex<HashMap<(u64, u64), Arc<SchemaCell>>>,
}

type SchemaCell = OnceCell<SchemaRef>;

impl<'a> InlineSchemaCache<'a> {
    fn new(pending: &'a HashMap<(u64, u64), &'a [u8]>) -> Self {
        Self {
            pending,
            decoded: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Fills the cells for `versions` concurrently so each chunk's `get` is a
    /// hit. Failures are left for that `get` to surface, since a version no
    /// chunk ends up decoding must not fail the commit.
    async fn prefetch(
        &self,
        db_tx: &DbTransaction,
        versions: impl IntoIterator<Item = (u64, u64)>,
    ) {
        let distinct: HashSet<(u64, u64)> = versions.into_iter().collect();
        stream::iter(distinct)
            .for_each_concurrent(
                INLINE_READ_CONCURRENCY,
                |(table_id, schema_version)| async move {
                    let _ = self.get(db_tx, table_id, schema_version).await;
                },
            )
            .await;
    }

    async fn get(
        &self,
        db_tx: &DbTransaction,
        table_id: u64,
        schema_version: u64,
    ) -> Result<SchemaRef> {
        let key = (table_id, schema_version);
        let cell = {
            let mut decoded = self.decoded.lock().await;
            Arc::clone(decoded.entry(key).or_default())
        };

        let schema = cell
            .get_or_try_init(|| async {
                let schema_ipc = if let Some(bytes) = self.pending.get(&key) {
                    Bytes::copy_from_slice(bytes)
                } else {
                    store_inline::read_inline_schema(
                        ReadHandle::Tx(db_tx),
                        table_id,
                        schema_version,
                    )
                    .await?
                    .ok_or_else(|| {
                        Error::Corruption(format!(
                            "no inline schema for table {table_id} version {schema_version}"
                        ))
                    })?
                    .arrow_schema
                };
                data_file::decode_inline_schema(schema_ipc)
            })
            .await?;
        Ok(Arc::clone(schema))
    }
}

/// What a commit's registered and targeted files need before their entries
/// can be derived.
pub(super) struct FileContext<'a> {
    /// The `DATA_PATH` store, absent when the caller supplied none.
    store: Option<&'a DataStore>,
    /// The store-relative prefix data files resolve against.
    prefix: &'a str,
    /// Tables this commit compacts and does nothing else to.
    compacted: BTreeSet<u64>,
    delete_file_permits: Arc<tokio::sync::Semaphore>,
    metrics: Arc<data_file::ScopedReadMetrics>,
    /// Per-table index metadata and compiled projections, built on first use.
    indexing: std::sync::Mutex<HashMap<u64, Arc<TableIndexing>>>,
}

impl FileContext<'_> {
    fn indexing(&self, base: &CatalogSnapshot, table: TableId) -> Result<Arc<TableIndexing>> {
        let mut cache = self.indexing.lock().map_err(|_| {
            Error::Interrupted("index metadata cache poisoned by a failed upkeep task".to_owned())
        })?;
        if let Some(indexing) = cache.get(&table.get()) {
            return Ok(Arc::clone(indexing));
        }

        let indexing = Arc::new(TableIndexing::build(base, table)?);
        cache.insert(table.get(), Arc::clone(&indexing));
        Ok(indexing)
    }
}

/// One table's indexes with their projections compiled once: every index
/// (removals) and the subset maintained on write (additions).
struct TableIndexing {
    all: Arc<IndexSet>,
    maintained: Arc<IndexSet>,
    deferred: Vec<u64>,
}

/// Indexes with the projection compiled for each, in parallel order.
struct IndexSet {
    indexes: Vec<IndexInfo>,
    projections: Vec<data_file::IndexProjection>,
}

impl IndexSet {
    fn build(live_columns: &[ColumnInfo], indexes: Vec<IndexInfo>, table: TableId) -> Result<Self> {
        let projections = index_projections(live_columns, &indexes, table)?;
        Ok(Self {
            indexes,
            projections,
        })
    }
}

impl TableIndexing {
    fn build(base: &CatalogSnapshot, table: TableId) -> Result<Self> {
        let all = base.indexes_of(table);
        let live_columns = base.columns_of(table);
        let maintained: Vec<_> = all
            .iter()
            .filter(|index| index.maintenance != crate::catalog::IndexMaintenance::Deferred)
            .cloned()
            .collect();
        let deferred = all
            .iter()
            .filter(|index| index.maintenance == crate::catalog::IndexMaintenance::Deferred)
            .map(|index| index.id.get())
            .collect();

        Ok(Self {
            all: Arc::new(IndexSet::build(&live_columns, all, table)?),
            maintained: Arc::new(IndexSet::build(&live_columns, maintained, table)?),
            deferred,
        })
    }
}

type IndexEntryStream<'a> = BoxStream<'a, Result<StagedIndexEntry>>;

fn staged_entry_stream(entries: Vec<StagedIndexEntry>) -> IndexEntryStream<'static> {
    stream::iter(entries.into_iter().map(Ok)).boxed()
}

/// Streams equality-index entries for one registered data file, one
/// bounded read batch at a time.
async fn stream_data_file_index_entries(
    base: &CatalogSnapshot,
    cells: &[Cell],
    killed: Option<&TargetDeletes>,
    context: &FileContext<'_>,
) -> Result<IndexEntryStream<'static>> {
    let file = decode_data_file(cells)?;
    let table = TableId::new(file.table_id);
    let indexes = Arc::clone(&context.indexing(base, table)?.maintained);
    if indexes.indexes.is_empty() {
        return Ok(stream::empty().boxed());
    }

    let data_store = context.store.ok_or_else(|| {
        Error::Constraint(format!(
            "data file {} on indexed table {} cannot be read to maintain its equality index: no \
             data-path store is available",
            file.data_file_id, file.table_id
        ))
    })?;

    let killed = match killed {
        Some(deletes) => {
            let killed = resolve_target_deletes(base, context, deletes).await?;
            refuse_out_of_range(&killed, &file)?;
            Some(killed)
        }
        None => None,
    };
    let path = data_file_object_path(base, &file, context.prefix)?;
    // A row this same commit deletes out of the file it also registers is
    // never indexed: an entry's key carries no file, so a removal beside an
    // add would be indistinguishable from an UPDATE's.
    let entries = data_file::scoped_read_index_entry_batches(
        data_file::ParquetFile::new(
            data_store.clone(),
            path,
            file.file_size_bytes,
            file.footer_size,
        )
        .with_metrics(Arc::clone(&context.metrics)),
        indexes.projections.clone(),
        data_file::ScopedRows::All,
        data_file::RowIdSource::Resolve {
            row_id_start: file.row_id_start,
        },
        killed.as_ref().map(|killed| &killed.positions),
    )
    .await?;
    Ok(entries
        .map(move |batch| {
            batch.and_then(|batch| staged_scoped_entries(&indexes.indexes, batch, false))
        })
        .map_ok(|batch| stream::iter(batch.into_iter().map(Ok)))
        .try_flatten()
        .boxed())
}

/// Compiles one Arrow projection per index.
fn index_projections(
    live_columns: &[ColumnInfo],
    indexes: &[IndexInfo],
    table: TableId,
) -> Result<Vec<data_file::IndexProjection>> {
    indexes
        .iter()
        .map(|index| {
            Ok(data_file::IndexProjection {
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
    scoped: Vec<data_file::ScopedIndexEntry>,
    delete: bool,
) -> Result<Vec<StagedIndexEntry>> {
    scoped
        .into_iter()
        .map(|entry| staged_scoped_entry(indexes, entry, delete))
        .collect()
}

fn staged_scoped_entry(
    indexes: &[IndexInfo],
    entry: data_file::ScopedIndexEntry,
    delete: bool,
) -> Result<StagedIndexEntry> {
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
}

/// Stages the commit's index entry adds and removals, returning the ids of
/// any building indexes a duplicate poisoned with the bytes the entries
/// put on the batch.
#[allow(clippy::too_many_lines)]
pub(super) async fn stage_index_maintenance(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    data_store: Option<&DataStore>,
    data_prefix: &str,
    metrics: Arc<data_file::ScopedReadMetrics>,
) -> Result<StagedEntries> {
    let pending_schemas = pending_inline_schemas(ops);
    let inline_schemas = InlineSchemaCache::new(&pending_schemas);
    let context = FileContext {
        store: data_store,
        prefix: data_prefix,
        compacted: compaction_only_tables(ops)?,
        delete_file_permits: Arc::new(tokio::sync::Semaphore::new(FILE_READ_CONCURRENCY)),
        metrics: Arc::clone(&metrics),
        indexing: std::sync::Mutex::new(HashMap::new()),
    };

    let DeletePlan {
        inline: inline_deletes,
        files: file_deletes,
    } = plan_deletes(base, ops, &context)?;

    let AddPlan {
        registered,
        deferred,
        adds,
    } = plan_adds(base, ops, &file_deletes, &context)?;

    let locator_writes = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let locator_writes_for_removals = Arc::clone(&locator_writes);
    let mut staged_chunks = staged_inline_chunks(ops);
    let mut staged_versions = Vec::new();
    for (table_id, chunks) in &staged_chunks {
        if !context
            .indexing(base, TableId::new(*table_id))?
            .all
            .indexes
            .is_empty()
        {
            staged_versions.extend(chunks.iter().map(|chunk| (*table_id, chunk.schema_version)));
        }
    }
    inline_schemas.prefetch(db_tx, staged_versions).await;
    let inline_schemas_ref = &inline_schemas;
    let context_ref = &context;
    let inline_deletions = stream::iter(inline_deletes)
        .map(move |(table_id, row_ids)| {
            let locator_writes = Arc::clone(&locator_writes_for_removals);
            let staged_chunks = staged_chunks.remove(&table_id).unwrap_or_default();
            async move {
                let (entries, writes) = stage_inline_delete_entries(
                    db_tx,
                    base,
                    inline_schemas_ref,
                    context_ref,
                    table_id,
                    &row_ids,
                    staged_chunks,
                )
                .await?;
                locator_writes.lock().await.extend(writes);
                Result::Ok(staged_entry_stream(entries))
            }
        })
        .buffer_unordered(FILE_READ_CONCURRENCY)
        .try_flatten_unordered(None);

    let context_ref = &context;
    let file_deletions =
        stream::iter(file_deletes.iter().filter(|((table_id, data_file_id), _)| {
            !registered.contains(&(*table_id, *data_file_id))
        }))
        .map(|((table_id, data_file_id), deletes)| async move {
            let killed = resolve_target_deletes(base, context_ref, deletes).await?;
            stream_file_delete_index_entries(base, *table_id, *data_file_id, &killed, context_ref)
                .await
        })
        .buffer_unordered(FILE_READ_CONCURRENCY)
        .try_flatten_unordered(None);

    let deletions = stream::select(inline_deletions, file_deletions);

    let inline_schemas_ref = &inline_schemas;
    let context_ref = &context;
    let additions = stream::iter(adds)
        .map(move |add| produce_addition(db_tx, base, inline_schemas_ref, context_ref, add))
        .buffer_unordered(FILE_READ_CONCURRENCY)
        .try_flatten_unordered(None);

    let mut staged = stage_index_entry_stream(db_tx, deletions, additions, 0).await?;
    staged.metrics.scoped_read = metrics.tally();
    let locator_writes = locator_writes.lock().await;
    {
        let locator_bytes = commit::stage_writes(db_tx, &locator_writes)?;
        staged.bytes = staged.bytes.saturating_add(locator_bytes.0);
        staged.uses_inline_chunk_directory = !locator_writes.is_empty();
    }
    staged.deferred = deferred.into_iter().collect();

    Ok(staged)
}

async fn produce_addition(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    inline_schemas: &InlineSchemaCache<'_>,
    context: &FileContext<'_>,
    add: Add<'_>,
) -> Result<IndexEntryStream<'static>> {
    match add {
        Add::File { cells, killed } => {
            stream_data_file_index_entries(base, cells, killed, context).await
        }
        Add::Inline { table_id, chunk } => {
            let entries = inline_chunk_index_entries(
                db_tx,
                base,
                inline_schemas,
                context,
                table_id,
                chunk,
                None,
            )
            .await?;
            Ok(staged_entry_stream(entries))
        }
    }
}

/// Classifies every staged delete without reading a data-store object;
/// each Parquet delete file stays attached to its target until that
/// target's producer resolves it.
fn plan_deletes(
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    context: &FileContext<'_>,
) -> Result<DeletePlan> {
    let mut plan = DeletePlan::default();
    for op in ops {
        match op {
            RowOperation::Insert {
                table: TableKind::DeleteFile,
                cells,
            } => {
                let delete_file = decode_delete_file(cells)?;
                let table = TableId::new(delete_file.table_id);
                if !context.compacted.contains(&delete_file.table_id)
                    && !context.indexing(base, table)?.all.indexes.is_empty()
                {
                    plan.files
                        .entry((delete_file.table_id, delete_file.data_file_id))
                        .or_default()
                        .files
                        .push(delete_file);
                }
            }
            RowOperation::InlineFileDelete {
                table_id,
                data_file_id,
                row_id: position,
                ..
            } if !context.compacted.contains(table_id) => {
                // An inlined file-delete names a physical position, as a
                // delete file's `pos` does.
                plan.files
                    .entry((*table_id, *data_file_id))
                    .or_default()
                    .positions
                    .push(*position);
            }
            RowOperation::InlineInlineDelete {
                table_id, row_id, ..
            } if !context.compacted.contains(table_id) => {
                plan.inline.entry(*table_id).or_default().push(*row_id);
            }
            _ => {}
        }
    }

    Ok(plan)
}

/// Plans entry additions without reading their bytes.
fn plan_adds<'a>(
    base: &CatalogSnapshot,
    ops: &'a [RowOperation],
    file_deletes: &'a HashMap<(u64, u64), TargetDeletes>,
    context: &FileContext<'_>,
) -> Result<AddPlan<'a>> {
    let mut registered: HashSet<(u64, u64)> = HashSet::new();
    let mut deferred = BTreeSet::new();
    let mut adds: Vec<Add<'_>> = Vec::new();

    for op in ops {
        match op {
            RowOperation::Insert {
                table: TableKind::DataFile,
                cells,
            } => {
                let file = decode_data_file(cells)?;
                // A compacted file's entries are already stored under the
                // same keys; it is left out of `registered` too, since the
                // commit stages no delete against it.
                if context.compacted.contains(&file.table_id) {
                    continue;
                }

                deferred.extend(
                    context
                        .indexing(base, TableId::new(file.table_id))?
                        .deferred
                        .iter()
                        .copied(),
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
                    context
                        .indexing(base, TableId::new(*table_id))?
                        .deferred
                        .iter()
                        .copied(),
                );
                adds.push(Add::Inline {
                    table_id: *table_id,
                    chunk: InlineChunk {
                        schema_version: *schema_version,
                        row_id_start: *row_id_start,
                        row_count: *row_count,
                        body: InlineBody::Borrowed(arrow_body),
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

#[derive(Default)]
struct DeletePlan {
    inline: HashMap<u64, Vec<u64>>,
    files: HashMap<(u64, u64), TargetDeletes>,
}

/// Every source of killed positions for one physical data file; delete
/// files stay unread until the target's producer starts.
#[derive(Default)]
struct TargetDeletes {
    positions: Vec<u64>,
    files: Vec<proto::DeleteFileValue>,
}

/// Addition reads and the metadata needed to exclude same-commit targets
/// from the removal branch.
struct AddPlan<'a> {
    registered: HashSet<(u64, u64)>,
    deferred: BTreeSet<u64>,
    adds: Vec<Add<'a>>,
}

/// One add a commit derives entries from.
enum Add<'a> {
    /// A registered data file, read from the data store.
    File {
        cells: &'a [Cell],
        killed: Option<&'a TargetDeletes>,
    },
    /// An inlined chunk the commit itself carries.
    Inline {
        table_id: u64,
        chunk: InlineChunk<'a>,
    },
}

/// The tables this commit compacts and otherwise leaves alone, read from
/// the `ducklake_snapshot_changes` row it stages; empty without one.
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

    // The row's full shape is validated when the snapshot record is built.
    let mut cursor = Cursor::new(TableKind::SnapshotChanges, cells);
    cursor.u64()?;
    Ok(ChangeSet::parse(&cursor.string()?).compaction_only_tables())
}

/// The inline chunks this commit stages, grouped by table.
fn staged_inline_chunks(ops: &[RowOperation]) -> HashMap<u64, Vec<InlineChunk<'_>>> {
    let mut chunks: HashMap<u64, Vec<InlineChunk<'_>>> = HashMap::new();
    for op in ops {
        if let RowOperation::InlineInsert {
            table_id,
            schema_version,
            row_id_start,
            row_count,
            arrow_body,
            ..
        } = op
        {
            chunks.entry(*table_id).or_default().push(InlineChunk {
                schema_version: *schema_version,
                row_id_start: *row_id_start,
                row_count: *row_count,
                body: InlineBody::Borrowed(arrow_body),
            });
        }
    }
    chunks
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

/// The index entries for one inline chunk's rows: every row when `held` is
/// `None` (an insert), or removals for just those rows when it is `Some`.
async fn inline_chunk_index_entries(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    inline_schemas: &InlineSchemaCache<'_>,
    context: &FileContext<'_>,
    table_id: u64,
    chunk: InlineChunk<'_>,
    held: Option<&HashSet<u64>>,
) -> Result<Vec<StagedIndexEntry>> {
    let indexing = context.indexing(base, TableId::new(table_id))?;
    let indexes = if held.is_some() {
        Arc::clone(&indexing.all)
    } else {
        Arc::clone(&indexing.maintained)
    };
    if indexes.indexes.is_empty() {
        return Ok(Vec::new());
    }

    let schema = inline_schemas
        .get(db_tx, table_id, chunk.schema_version)
        .await?;
    let plan = InlineDecodePlan {
        schema,
        body: chunk.body.into_bytes(),
        indexes,
        row_id_start: chunk.row_id_start,
        held: held.cloned(),
    };

    let metrics = Arc::clone(&context.metrics);
    data_file::run_bounded_index_encoding(move || {
        let started = Instant::now();
        metrics.inline_chunk();
        let result = plan.derive();
        metrics.encoded(started.elapsed());
        result
    })
    .await
}

/// Owned input for one blocking Arrow decode.
struct InlineDecodePlan {
    schema: SchemaRef,
    body: Bytes,
    indexes: Arc<IndexSet>,
    row_id_start: u64,
    held: Option<HashSet<u64>>,
}

impl InlineDecodePlan {
    /// Decodes the chunk and encodes each row's index keys.
    fn derive(self) -> Result<Vec<StagedIndexEntry>> {
        let scoped = data_file::inline_batch_index_entries(
            self.schema,
            &self.body,
            &self.indexes.projections,
            self.row_id_start,
            self.held.as_ref(),
        )?;
        let delete = self.held.is_some();
        staged_scoped_entries(&self.indexes.indexes, scoped, delete)
    }
}

/// One inline chunk this commit can read: already committed, or staged by
/// the commit itself.
pub(super) struct InlineChunk<'a> {
    schema_version: u64,
    row_id_start: u64,
    row_count: u64,
    body: InlineBody<'a>,
}

impl InlineChunk<'_> {
    /// The ids of `sorted_row_ids` this chunk holds; the input is sorted
    /// ascending, so they form one contiguous run.
    fn held<'r>(&self, sorted_row_ids: &'r [u64]) -> &'r [u64] {
        let row_id_end = self.row_id_start.saturating_add(self.row_count);
        let start = sorted_row_ids.partition_point(|&row_id| row_id < self.row_id_start);
        let end = sorted_row_ids.partition_point(|&row_id| row_id < row_id_end);
        &sorted_row_ids[start..end.max(start)]
    }
}

enum InlineBody<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

impl InlineBody<'_> {
    fn into_bytes(self) -> Bytes {
        match self {
            Self::Borrowed(body) => Bytes::copy_from_slice(body),
            Self::Owned(body) => body,
        }
    }
}

/// Derives removals for `sorted_row_ids` (sorted, unique) from the chunks
/// `locators` name; the locators arrive in ascending, non-overlapping row
/// order, so one forward walk assigns each its held run.
async fn derive_located_inline_removals(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    inline_schemas: &InlineSchemaCache<'_>,
    context: &FileContext<'_>,
    table_id: u64,
    sorted_row_ids: &[u64],
    locators: Vec<store_inline::InlineChunkLocator>,
) -> Result<(Vec<StagedIndexEntry>, HashSet<u64>)> {
    let mut next = 0usize;
    let resolved = stream::iter(locators)
        .map(|locator| {
            let start = next;
            while next < sorted_row_ids.len() && locator.holds(sorted_row_ids[next]) {
                next += 1;
            }
            let held: HashSet<u64> = sorted_row_ids[start..next].iter().copied().collect();

            async move {
                let (operation, value) = store_inline::read_inline_chunk_locator(
                    ReadHandle::Tx(db_tx),
                    table_id,
                    locator,
                )
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
                    body: InlineBody::Owned(value.body),
                };
                let entries = inline_chunk_index_entries(
                    db_tx,
                    base,
                    inline_schemas,
                    context,
                    table_id,
                    chunk,
                    Some(&held),
                )
                .await?;

                Ok::<_, Error>((entries, held))
            }
        })
        .buffer_unordered(INLINE_DECODE_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    let (derived_vec, covered_vec): (Vec<_>, Vec<_>) = resolved.into_iter().unzip();
    let derived = derived_vec.into_iter().flatten().collect::<Vec<_>>();
    let covered = covered_vec.into_iter().flatten().collect::<HashSet<_>>();
    Ok((derived, covered))
}

async fn derive_inline_chunk_removals(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    inline_schemas: &InlineSchemaCache<'_>,
    context: &FileContext<'_>,
    table_id: u64,
    sorted_row_ids: &[u64],
    chunks: Vec<InlineChunk<'_>>,
) -> Result<(Vec<StagedIndexEntry>, HashSet<u64>)> {
    let resolved = stream::iter(chunks)
        .filter_map(|chunk| {
            let held: HashSet<u64> = chunk.held(sorted_row_ids).iter().copied().collect();

            async move { (!held.is_empty()).then_some((chunk, held)) }
        })
        .map(|(chunk, held)| async move {
            let entries = inline_chunk_index_entries(
                db_tx,
                base,
                inline_schemas,
                context,
                table_id,
                chunk,
                Some(&held),
            )
            .await?;

            Ok::<_, Error>((entries, held))
        })
        .buffer_unordered(INLINE_DECODE_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    let (derived_vec, covered_vec): (Vec<_>, Vec<_>) = resolved.into_iter().unzip();
    let derived = derived_vec.into_iter().flatten().collect::<Vec<_>>();
    let covered = covered_vec.into_iter().flatten().collect::<HashSet<_>>();
    Ok((derived, covered))
}

fn legacy_inline_locator_writes(
    table_id: u64,
    chunks: &[(InlineOperation, proto::InlineChunkValue)],
) -> Result<Vec<commit::StagedWrite>> {
    chunks
        .iter()
        .filter_map(|(operation, chunk)| {
            if let InlineOperation::Insert {
                schema_version,
                begin_snapshot,
                chunk_seq,
                ..
            } = operation
            {
                Some(inline_chunk_range_write(
                    table_id,
                    *schema_version,
                    *begin_snapshot,
                    *chunk_seq,
                    chunk.row_id_start,
                    chunk.row_count,
                ))
            } else {
                None
            }
        })
        .collect()
}

/// Derives the entry removals for rows tombstoned out of inline chunks,
/// reading each row's indexed values from the chunk holding it.
async fn stage_inline_delete_entries(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    inline_schemas: &InlineSchemaCache<'_>,
    context: &FileContext<'_>,
    table_id: u64,
    row_ids: &[u64],
    staged_chunks: Vec<InlineChunk<'_>>,
) -> Result<(Vec<StagedIndexEntry>, Vec<commit::StagedWrite>)> {
    if context
        .indexing(base, TableId::new(table_id))?
        .all
        .indexes
        .is_empty()
    {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut sorted_row_ids = row_ids.to_vec();
    sorted_row_ids.sort_unstable();
    sorted_row_ids.dedup();

    let committed = async {
        let locators = store_inline::find_inline_chunk_locators_for_rows(
            ReadHandle::Tx(db_tx),
            table_id,
            row_ids,
        )
        .await?;
        if let Some(locators) = locators {
            inline_schemas
                .prefetch(
                    db_tx,
                    locators
                        .iter()
                        .filter_map(|locator| Some((table_id, locator.schema_version()?))),
                )
                .await;
            let (entries, covered) = derive_located_inline_removals(
                db_tx,
                base,
                inline_schemas,
                context,
                table_id,
                &sorted_row_ids,
                locators,
            )
            .await?;

            return Ok((entries, covered, Vec::new()));
        }

        let committed = store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id).await?;
        let locator_writes = legacy_inline_locator_writes(table_id, &committed)?;
        let chunks: Vec<_> = committed
            .into_iter()
            .filter_map(|(operation, value)| match operation {
                InlineOperation::Insert { schema_version, .. } => Some(InlineChunk {
                    schema_version,
                    row_id_start: value.row_id_start,
                    row_count: value.row_count,
                    body: InlineBody::Owned(value.body),
                }),
                _ => None,
            })
            .collect();
        inline_schemas
            .prefetch(
                db_tx,
                chunks.iter().map(|chunk| (table_id, chunk.schema_version)),
            )
            .await;
        let (entries, covered) = derive_inline_chunk_removals(
            db_tx,
            base,
            inline_schemas,
            context,
            table_id,
            &sorted_row_ids,
            chunks,
        )
        .await?;

        Ok::<_, Error>((entries, covered, locator_writes))
    };

    // A row inserted and deleted inside one commit lives in a chunk that is
    // still only staged.
    let staged = derive_inline_chunk_removals(
        db_tx,
        base,
        inline_schemas,
        context,
        table_id,
        &sorted_row_ids,
        staged_chunks,
    );
    let ((mut entries, mut covered, locator_writes), (staged_entries, staged_covered)) =
        futures::try_join!(committed, staged)?;
    entries.extend(staged_entries);
    covered.extend(staged_covered);

    // A tombstone naming no live chunk would leave its entries behind.
    if let Some(missing) = row_ids.iter().find(|row_id| !covered.contains(row_id)) {
        return Err(Error::Corruption(format!(
            "inline delete of row {missing} on indexed table {table_id} names no inline chunk, so \
             its index entries cannot be derived"
        )));
    }
    Ok((entries, locator_writes))
}

/// The physical row positions (not row ids) a commit kills inside one data
/// file, sorted and unique.
#[derive(Debug)]
pub(super) struct KilledRows {
    positions: data_file::RowPositions,
}

impl KilledRows {
    fn from_unsorted(positions: Vec<u64>) -> Self {
        Self {
            positions: data_file::RowPositions::from_unsorted(positions),
        }
    }
}

/// Resolves every delete source for one data file into one sorted, unique
/// position set.
async fn resolve_target_deletes(
    base: &CatalogSnapshot,
    context: &FileContext<'_>,
    deletes: &TargetDeletes,
) -> Result<KilledRows> {
    let positions = stream::iter(
        deletes
            .files
            .iter()
            .map(|delete_file| read_delete_file_positions(base, delete_file, context)),
    )
    .buffer_unordered(FILE_READ_CONCURRENCY)
    .try_fold(
        deletes.positions.clone(),
        |mut positions, discovered| async move {
            positions.extend(discovered);
            Ok(positions)
        },
    )
    .await?;

    Ok(KilledRows::from_unsorted(positions))
}

/// Reads physical positions from one already-decoded delete-file record.
async fn read_delete_file_positions(
    base: &CatalogSnapshot,
    delete_file: &proto::DeleteFileValue,
    context: &FileContext<'_>,
) -> Result<Vec<u64>> {
    let _permit = Arc::clone(&context.delete_file_permits)
        .acquire_owned()
        .await
        .map_err(|_| {
            Error::Interrupted(
                "delete-file read limiter stopped before position discovery".to_owned(),
            )
        })?;
    let data_store = context.store.ok_or_else(|| {
        Error::Constraint(format!(
            "delete file {} on indexed table {} cannot be read to maintain its equality index: no \
             data-path store is available",
            delete_file.delete_file_id, delete_file.table_id
        ))
    })?;

    let path = delete_file_object_path(base, delete_file, context.prefix)?;
    let positions = data_file::delete_file_positions(
        data_file::ParquetFile::new(
            data_store.clone(),
            path,
            delete_file.file_size_bytes,
            delete_file.footer_size,
        )
        .with_metrics(Arc::clone(&context.metrics)),
    )
    .await?;

    Ok(positions)
}

/// Streams the entry removals for rows killed inside one committed data
/// file, by scoped-reading the killed positions. A delete against a file
/// this same commit registers never reaches here; those rows are left
/// unindexed at the add.
pub(super) async fn stream_file_delete_index_entries(
    base: &CatalogSnapshot,
    table_id: u64,
    data_file_id: u64,
    killed: &KilledRows,
    context: &FileContext<'_>,
) -> Result<IndexEntryStream<'static>> {
    let table = TableId::new(table_id);
    let indexes = Arc::clone(&context.indexing(base, table)?.all);
    if indexes.indexes.is_empty() {
        return Ok(stream::empty().boxed());
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
    let entries = data_file::scoped_read_index_entry_batches(
        data_file::ParquetFile::new(
            data_store.clone(),
            path,
            file.file_size_bytes,
            file.footer_size,
        )
        .with_metrics(Arc::clone(&context.metrics)),
        indexes.projections.clone(),
        data_file::ScopedRows::At(&killed.positions),
        data_file::RowIdSource::Resolve {
            row_id_start: file.row_id_start,
        },
        None,
    )
    .await?;

    Ok(entries
        .map(move |batch| {
            batch.and_then(|batch| staged_scoped_entries(&indexes.indexes, batch, true))
        })
        .map_ok(|batch| stream::iter(batch.into_iter().map(Ok)))
        .try_flatten()
        .boxed())
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

/// Refuses a killed position the target file does not hold; it could never
/// match a scoped entry and would silently orphan index rows.
fn refuse_out_of_range(killed: &KilledRows, file: &proto::DataFileValue) -> Result<()> {
    if let Some(&position) = killed.positions.as_slice().last()
        && position >= file.record_count
    {
        return Err(Error::Constraint(format!(
            "delete file for data file {} on table {} names position {position} outside the \
             file's record count {}",
            file.data_file_id, file.table_id, file.record_count
        )));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_inline_body_keeps_its_allocation() {
        let body = vec![1, 2, 3, 4];
        let pointer = body.as_ptr();

        let bytes = InlineBody::Owned(Bytes::from(body)).into_bytes();

        assert_eq!(bytes.as_ptr(), pointer);
    }

    /// A prefetch that names a version with no schema record does not fail;
    /// only the chunk that decodes against it does, while the recorded
    /// versions it fetched are served without another store read.
    #[tokio::test]
    async fn prefetch_leaves_missing_schema_errors_to_the_chunk_that_needs_them() {
        use object_store::memory::InMemory;
        use slatedb::IsolationLevel;

        use crate::store::{
            key::{InlineKey, Key},
            open::StoreBuilder,
            value,
        };

        let (db, _) = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]);
        let schema_ipc = super::super::tests::inline_schema_ipc(&schema);
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            Key::Inline(InlineKey::Schema {
                table_id: 7,
                schema_version: 1,
            })
            .encode(),
            value::encode_value(&proto::InlineSchemaValue {
                arrow_schema: schema_ipc.into(),
            }),
        )
        .unwrap();
        tx.commit().await.unwrap();

        let pending = HashMap::new();
        let cache = InlineSchemaCache::new(&pending);
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        cache.prefetch(&tx, [(7, 1), (7, 2)]).await;

        let decoded = cache.get(&tx, 7, 1).await.unwrap();
        assert_eq!(decoded.fields().len(), 1);
        assert!(matches!(
            cache.get(&tx, 7, 2).await,
            Err(Error::Corruption(_))
        ));

        tx.rollback();
        db.close().await.unwrap();
    }
}
