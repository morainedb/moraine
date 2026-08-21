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
        handle::ReadHandle, inline as store_inline, key::InlineOperation, proto::InlineChunkValue,
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
        overlay: Option<&moraine_wal::Overlay>,
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
                        store_inline::read_inline_chunk_locator(
                            handle,
                            overlay,
                            table.get(),
                            locator,
                        )
                        .await
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
    /// `table`'s inline tombstones, served from the projection when they
    /// stand at this read's head. Every per-version scan of a base table asks
    /// for the same whole set, and the set cannot be scoped by version: a
    /// tombstone names a row id, not the version that wrote it.
    async fn inline_tombstones(
        &self,
        handle: ReadHandle<'_>,
        overlay: Option<&moraine_wal::Overlay>,
        table: TableId,
        head: &crate::store::proto::HeadValue,
    ) -> Result<projection::InlineTombstones> {
        if let Some(rows) = projection::inline_tombstones_at(&self.projections, table.get(), head) {
            return Ok(rows);
        }

        let rows = std::sync::Arc::new(
            store_inline::scan_inline_deletes(handle, overlay, table.get()).await?,
        );
        projection::install_inline_tombstones(
            &self.projections,
            table.get(),
            *head,
            std::sync::Arc::clone(&rows),
        );

        Ok(rows)
    }

    /// Every inline row of `table` — tombstoned included, for the caller's
    /// scan kind to select over — from the chunk-range directory when it
    /// is known complete, else from the chunk scan, verifying the
    /// directory as it passes so a later call is served from it.
    async fn inline_row_source(
        &self,
        handle: ReadHandle<'_>,
        overlay: Option<&moraine_wal::Overlay>,
        table: TableId,
        schema_version: Option<u64>,
        head: &crate::store::proto::HeadValue,
    ) -> Result<(InlineRowSource, Vec<InlineRow>)> {
        // A caller after one version's rows takes only that version's chunks:
        // the others are dropped before their bodies resolve anyway, and a
        // base-table scan asks once per registered version. The directory is
        // left unjudged, since a partial chunk set cannot say it is complete.
        if let Some(version) = schema_version
            && !projection::inline_directory_complete(&self.projections, table.get())
        {
            let (chunks, tombstones) = futures::try_join!(
                store_inline::scan_inline_chunks_of_version(handle, overlay, table.get(), version),
                self.inline_tombstones(handle, overlay, table, head),
            )?;
            let rows = materialize_inline_rows(&chunks, &tombstones);
            return Ok((InlineRowSource::Chunks(chunks), rows));
        }

        if projection::inline_directory_complete(&self.projections, table.get()) {
            let (locators, tombstones) = futures::try_join!(
                store_inline::scan_inline_chunk_locators(handle, overlay, table.get()),
                self.inline_tombstones(handle, overlay, table, head),
            )?;
            let rows = materialize_locator_rows(&locators, &tombstones);
            return Ok((InlineRowSource::Locators(locators), rows));
        }

        let (chunks, tombstones) = futures::try_join!(
            store_inline::scan_inline_chunks(handle, overlay, table.get()),
            self.inline_tombstones(handle, overlay, table, head),
        )?;
        self.verify_inline_directory(handle, overlay, table, &chunks)
            .await?;
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
        let read = self.begin_probe().await?;
        let head = read.head_value();
        let outcome = async {
            let (source, rows) = self
                .inline_row_source(read.handle(), read.tail(), table, schema_version, &head)
                .await?;
            let mut selected = kind.select(&rows, snapshot, start);
            if let Some(version) = schema_version {
                selected.retain(|row| source.chunk_is_version(row.chunk, version));
            }
            source
                .resolve_chunks(read.handle(), read.tail(), table, selected)
                .await
        }
        .await;
        read.finish().await;

        outcome
    }

    /// The inline rows of `table` live at `at` (the probe's head, when
    /// `None`), read through the probe's view of store plus unfolded tail.
    pub(super) async fn scan_recent_rows(
        &self,
        read: &super::ProbeRead,
        table: TableId,
        at: Option<u64>,
    ) -> Result<Vec<RecentRow>> {
        let handle = read.handle();
        let overlay = read.tail();
        let head_value = read.head_value();
        // The probe already resolved the head across store and tail, so only
        // time travel owes a store read.
        let read_at = match at {
            Some(_) => commit::resolve_read_snapshot(handle, at).await?.0,
            None => read.view().snapshot.snapshot_id,
        };
        let (source, rows) = self
            .inline_row_source(handle, overlay, table, None, &head_value)
            .await?;

        let live = InlineScanKind::Table.select(&rows, read_at, 0);
        let (live, chunks) = source.resolve_chunks(handle, overlay, table, live).await?;

        let schema_versions = live.iter().filter_map(|row| match &chunks[row.chunk].0 {
            InlineOperation::Insert { schema_version, .. } => Some(*schema_version),
            _ => None,
        });
        let schemas: HashMap<u64, Arc<Vec<u8>>> =
            backfill::read_inline_schemas(handle, overlay, table, schema_versions)
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
        read: &super::ProbeRead,
        table: TableId,
    ) -> Result<Vec<u64>> {
        let head = read.view().snapshot.snapshot_id;
        let head_value = read.head_value();
        let (_, rows) = self
            .inline_row_source(read.handle(), read.tail(), table, None, &head_value)
            .await?;

        Ok(InlineScanKind::Table
            .select(&rows, head, 0)
            .into_iter()
            .map(|row| row.row_id)
            .collect())
    }

    /// Compares the walked chunks against the directory and remembers a
    /// complete one, under a format that locks out writers predating the
    /// directory.
    ///
    /// The two scans must describe one store state. A transaction gives that
    /// outright; a manifest-following reader does not, so the head is read
    /// again afterwards and a judgement is only recorded when it has not
    /// moved — a commit landing between the scans leaves the directory
    /// unjudged rather than wrongly complete. This path never writes, so a
    /// gap is simply left for a flush to heal.
    async fn verify_inline_directory(
        &self,
        handle: ReadHandle<'_>,
        overlay: Option<&moraine_wal::Overlay>,
        table: TableId,
        chunks: &[(InlineOperation, InlineChunkValue)],
    ) -> Result<()> {
        if projection::format_floor(&self.projections) < commit::FORMAT_WITH_INLINE_CHUNK_DIRECTORY
        {
            return Ok(());
        }

        // Read before the directory scan, compared after it: the walked
        // chunks were taken at this head too, so an unmoved head means all
        // of it described one state.
        let before = if handle.is_isolated() {
            None
        } else {
            Some(commit::read_head_value(handle).await?)
        };

        let directory: BTreeSet<u64> =
            store_inline::scan_inline_chunk_ranges(handle, overlay, table.get())
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
        let steady = match &before {
            None => true,
            Some(before) => {
                let after = commit::read_head_value(handle).await?;
                after.snapshot_id == before.snapshot_id && after.batch_seq == before.batch_seq
            }
        };
        if steady && ends == Some(directory) {
            projection::note_inline_directory_complete(&self.projections, table.get());
        }

        Ok(())
    }
}
