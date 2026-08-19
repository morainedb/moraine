//! Inline row scans: materialized from the chunk-range directory when it
//! is known complete, with only the referenced chunk bodies point-read.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use futures::{StreamExt, TryStreamExt, stream};

use super::{ReadOnlyCatalog, backfill};
use crate::{
    catalog::{
        RecentRow, SnapshotId, TableId,
        inline::{InlineRow, InlineScanKind, materialize_inline_rows, materialize_locator_rows},
        projection,
    },
    error::{Error, Result},
    store::{
        handle::{ReadHandle, ReadSession},
        inline as store_inline,
        key::InlineOperation,
        proto::InlineChunkValue,
    },
    transaction::commit,
};

/// Chunk-body point reads kept in flight by a directory-served scan.
const CHUNK_READ_CONCURRENCY: usize = 8;

/// Where a table's inline rows came from: directory locators — row spans
/// without bodies — or the full chunk scan, bodies in hand.
pub(super) enum InlineRowSource {
    Locators(Vec<store_inline::InlineChunkLocator>),
    Chunks(Vec<(InlineOperation, InlineChunkValue)>),
}

impl InlineRowSource {
    /// Whether the chunk at `index` was written under `schema_version`.
    fn chunk_is_version(&self, index: usize, schema_version: u64) -> bool {
        let operation = match self {
            Self::Locators(locators) => locators[index].operation(),
            Self::Chunks(chunks) => chunks[index].0,
        };
        matches!(
            operation,
            InlineOperation::Insert { schema_version: version, .. } if version == schema_version
        )
    }

    /// Hands back the chunks `selected` references, remapping each row's
    /// `chunk` index into the returned set: point reads of exactly the
    /// referenced chunks in directory mode, a reference-order compaction
    /// of the scanned set otherwise. Directory mode is reached only
    /// through isolated sessions (completeness is never remembered
    /// elsewhere), so a locator naming a missing chunk is corruption, not
    /// a straddled commit.
    async fn resolve_chunks(
        self,
        handle: ReadHandle<'_>,
        table: TableId,
        mut selected: Vec<InlineRow>,
    ) -> Result<(Vec<InlineRow>, Vec<(InlineOperation, InlineChunkValue)>)> {
        let mut dense: HashMap<usize, usize> = HashMap::new();
        let mut referenced = Vec::new();
        for row in &mut selected {
            let next = dense.len();
            row.chunk = *dense.entry(row.chunk).or_insert_with(|| {
                referenced.push(row.chunk);
                next
            });
        }

        let chunks = match self {
            Self::Locators(locators) => {
                stream::iter(referenced.into_iter().map(|index| {
                    let locator = locators[index];
                    async move {
                        store_inline::read_inline_chunk_locator(handle, table.get(), locator).await
                    }
                }))
                .buffered(CHUNK_READ_CONCURRENCY)
                .try_collect()
                .await?
            }
            Self::Chunks(mut chunks) => referenced
                .into_iter()
                .map(|index| {
                    let (operation, chunk) = &mut chunks[index];
                    (*operation, std::mem::take(chunk))
                })
                .collect(),
        };

        Ok((selected, chunks))
    }
}

impl ReadOnlyCatalog {
    /// Every inline row of `table` — tombstoned included, for the caller's
    /// scan kind to select over — from the chunk-range directory when it
    /// is known complete, else from the chunk scan, verifying the
    /// directory as it passes so a later call is served from it.
    async fn inline_row_source(
        &self,
        handle: ReadHandle<'_>,
        table: TableId,
    ) -> Result<(InlineRowSource, Vec<InlineRow>)> {
        if projection::inline_directory_complete(&self.projections, table.get()) {
            let (locators, tombstones) = futures::try_join!(
                store_inline::scan_inline_chunk_locators(handle, table.get()),
                store_inline::scan_inline_deletes(handle, table.get()),
            )?;
            let rows = materialize_locator_rows(&locators, &tombstones);
            return Ok((InlineRowSource::Locators(locators), rows));
        }

        let (chunks, tombstones) = futures::try_join!(
            store_inline::scan_inline_chunks(handle, table.get()),
            store_inline::scan_inline_deletes(handle, table.get()),
        )?;
        self.verify_inline_directory(handle, table, &chunks).await?;
        let rows = materialize_inline_rows(&chunks, &tombstones);

        Ok((InlineRowSource::Chunks(chunks), rows))
    }

    /// `kind`'s selection over `table_id`'s inline rows at `snapshot`
    /// (windowed from `start`), each row's `chunk` indexing the returned
    /// chunk set — only the chunks the selected rows reference.
    /// `schema_version`, when set, drops rows of other versions before the
    /// chunks resolve, so their bodies are never fetched.
    pub(crate) async fn select_inline_rows(
        &self,
        table_id: u64,
        kind: InlineScanKind,
        snapshot: u64,
        start: u64,
        schema_version: Option<u64>,
    ) -> Result<(Vec<InlineRow>, Vec<(InlineOperation, InlineChunkValue)>)> {
        let table = TableId::new(table_id);
        let session = self.begin_read().await?;
        let outcome = async {
            let (source, rows) = self.inline_row_source(session.handle(), table).await?;
            let mut selected = kind.select(&rows, snapshot, start);
            if let Some(version) = schema_version {
                selected.retain(|row| source.chunk_is_version(row.chunk, version));
            }
            source
                .resolve_chunks(session.handle(), table, selected)
                .await
        }
        .await;
        session.finish();

        outcome
    }

    /// The inline rows of `table` live at `at` (head, when `None`), read
    /// through an open session.
    pub(super) async fn scan_recent_rows(
        &self,
        session: &ReadSession,
        table: TableId,
        at: Option<u64>,
    ) -> Result<Vec<RecentRow>> {
        let handle = session.handle();
        let read_at = async {
            match at {
                Some(_) => Ok(commit::resolve_read_snapshot(handle, at).await?.0),
                None => commit::read_head_id(handle).await,
            }
        };
        let (read_at, (source, rows)) =
            futures::try_join!(read_at, self.inline_row_source(handle, table))?;

        let live = InlineScanKind::Table.select(&rows, read_at, 0);
        let (live, chunks) = source.resolve_chunks(handle, table, live).await?;

        let schema_versions = live.iter().filter_map(|row| match &chunks[row.chunk].0 {
            InlineOperation::Insert { schema_version, .. } => Some(*schema_version),
            _ => None,
        });
        let schemas: HashMap<u64, Arc<Vec<u8>>> =
            backfill::read_inline_schemas(handle, table, schema_versions)
                .await?
                .into_iter()
                .map(|(version, record)| (version, Arc::new(record.arrow_schema.to_vec())))
                .collect();
        let mut bodies: HashMap<usize, Arc<Vec<u8>>> = HashMap::new();
        let mut rows = Vec::with_capacity(live.len());
        for row in live {
            let (operation, chunk) = &chunks[row.chunk];
            // Every chunk a row was materialized from is an insert.
            let InlineOperation::Insert { schema_version, .. } = operation else {
                return Err(Error::Corruption(format!(
                    "inline row {} of table {table} references a non-insert chunk",
                    row.row_id
                )));
            };
            let arrow_schema = Arc::clone(schemas.get(schema_version).ok_or_else(|| {
                Error::Corruption(format!(
                    "no inline schema for table {table} version {schema_version}"
                ))
            })?);
            let chunk_body = Arc::clone(
                bodies
                    .entry(row.chunk)
                    .or_insert_with(|| Arc::new(chunk.body.to_vec())),
            );

            rows.push(RecentRow {
                row_id: row.row_id,
                begin_snapshot: SnapshotId::new(row.begin_snapshot),
                schema_version: *schema_version,
                offset_in_chunk: row.offset_in_chunk,
                chunk_body,
                arrow_schema,
            });
        }

        Ok(rows)
    }

    pub(super) async fn scan_live_inline_row_ids(
        &self,
        session: &ReadSession,
        table: TableId,
    ) -> Result<Vec<u64>> {
        let handle = session.handle();
        let (head, (_, rows)) = futures::try_join!(
            commit::read_head_id(handle),
            self.inline_row_source(handle, table),
        )?;

        Ok(InlineScanKind::Table
            .select(&rows, head, 0)
            .into_iter()
            .map(|row| row.row_id)
            .collect())
    }

    /// Compares the walked chunks against the directory and remembers a
    /// complete one. Only an isolated session may judge — a
    /// manifest-following pass can straddle a commit — and only under a
    /// format that locks out writers that predate the directory. This path
    /// never writes, so a gap is simply left for a flush to heal.
    async fn verify_inline_directory(
        &self,
        handle: ReadHandle<'_>,
        table: TableId,
        chunks: &[(InlineOperation, InlineChunkValue)],
    ) -> Result<()> {
        if !handle.is_isolated()
            || projection::format_floor(&self.projections)
                < commit::FORMAT_WITH_INLINE_CHUNK_DIRECTORY
        {
            return Ok(());
        }

        let directory: BTreeSet<u64> = store_inline::scan_inline_chunk_ranges(handle, table.get())
            .await?
            .into_iter()
            .collect();
        let ends: Option<BTreeSet<u64>> = chunks
            .iter()
            .map(|(_, chunk)| {
                chunk
                    .row_count
                    .checked_sub(1)
                    .and_then(|count| chunk.row_id_start.checked_add(count))
            })
            .collect();
        if ends == Some(directory) {
            projection::note_inline_directory_complete(&self.projections, table.get());
        }

        Ok(())
    }
}
