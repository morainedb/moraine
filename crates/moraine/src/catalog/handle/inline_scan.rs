//! Inline row scans: the row set materialized from the chunk-range
//! directory when it is known complete, and the referenced chunk bodies
//! point-read a window at a time.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};

use super::{ReadOnlyCatalog, backfill};
use crate::{
    catalog::{
        RecentRow, SnapshotId, TableId,
        inline::{
            InlineBodies, InlineRow, InlineScanKind, materialize_inline_rows,
            materialize_locator_rows,
        },
        projection,
    },
    error::{Error, Result},
    store::{
        handle::{ReadHandle, ReadSession},
        inline as store_inline,
        key::InlineOperation,
        proto::InlineChunkValue,
        read,
    },
    transaction::commit,
};

/// Chunk-body point reads kept in flight by a directory-served scan.
const CHUNK_READ_CONCURRENCY: usize = 8;

/// Remaps each row's `chunk` to a dense index over the chunks these rows
/// reference, and returns those chunks' original indices in first-reference
/// order — the order the remapped indices name.
fn dense_chunk_references(rows: &mut [InlineRow]) -> Vec<usize> {
    let mut dense: HashMap<usize, usize> = HashMap::new();
    let mut referenced = Vec::new();
    for row in rows {
        let next = dense.len();
        row.chunk = *dense.entry(row.chunk).or_insert_with(|| {
            referenced.push(row.chunk);
            next
        });
    }

    referenced
}

/// A selection over one table's inline rows, served a window at a time:
/// the rows are chosen once, under one read session, and each window
/// fetches only the chunk bodies its own rows reference. A consumer that
/// drops each window holds the table's row spans, never its payload.
///
/// A read-only session follows the manifest rather than pinning a
/// snapshot, so a flush that removes a chunk between windows fails the
/// scan instead of serving a torn view.
pub(crate) struct InlineScan {
    session: ReadSession,
    table: TableId,
    /// Every chunk the table holds, indexed by [`InlineRow::chunk`].
    chunks: Vec<store_inline::InlineChunkLocator>,
    /// The selected rows, in the scan kind's order.
    selected: Vec<InlineRow>,
    bodies: InlineBodies,
    /// How far into `selected` the windows have reached.
    position: usize,
    /// The head this scan selected against, for a session that cannot pin
    /// one; `None` for an isolated session, which needs no witness.
    opened_head: Option<u64>,
}

impl InlineScan {
    /// The next window: at most `max_rows` rows in scan order, each row's
    /// `chunk` indexing the returned chunks — this window's referenced
    /// chunks alone, in first-reference order. An exhausted scan returns
    /// empty vectors.
    pub(crate) async fn next_window(
        &mut self,
        max_rows: usize,
    ) -> Result<(Vec<InlineRow>, Vec<(InlineOperation, Bytes)>)> {
        let end = self
            .position
            .saturating_add(max_rows)
            .min(self.selected.len());
        let mut window = self.selected[self.position..end].to_vec();
        self.position = end;

        let referenced: Vec<store_inline::InlineChunkLocator> = dense_chunk_references(&mut window)
            .into_iter()
            .map(|index| self.chunks[index])
            .collect();

        let table = self.table.get();
        let handle = self.session.handle();
        let chunks = match self.bodies {
            InlineBodies::Skip => referenced
                .into_iter()
                .map(|locator| (locator.operation(), Bytes::new()))
                .collect(),
            InlineBodies::Fetch => {
                let read = stream::iter(referenced.into_iter().map(|locator| async move {
                    store_inline::read_inline_chunk_locator(handle, table, locator).await
                }))
                .buffered(CHUNK_READ_CONCURRENCY)
                .map_ok(|(operation, chunk)| (operation, chunk.body))
                .try_collect()
                .await;

                match read {
                    Ok(chunks) => chunks,
                    Err(error) => return Err(self.classify_window_read(error).await),
                }
            }
        };

        Ok((window, chunks))
    }

    /// Separates a chunk a mid-scan flush removed from one that is
    /// genuinely missing: a head that moved since the selection means the
    /// view moved under an unpinned session, which is a read to re-run
    /// rather than damage to report.
    async fn classify_window_read(&self, error: Error) -> Error {
        let Some(opened_head) = self.opened_head else {
            return error;
        };
        if !matches!(error, Error::Corruption(_)) {
            return error;
        }
        match commit::read_head_id(self.session.handle()).await {
            Ok(head) if head != opened_head => Error::RetryBudgetExhausted(format!(
                "inline scan of table {} lost a chunk to a flush that landed mid-scan; \
                 re-run the read",
                self.table
            )),
            _ => error,
        }
    }

    /// Releases the scan's read session.
    pub(crate) fn finish(self) {
        self.session.finish();
    }
}

/// Where a table's inline rows came from: directory locators — row spans
/// without bodies — or the full chunk scan, bodies in hand.
pub(super) enum InlineRowSource {
    Locators(Vec<store_inline::InlineChunkLocator>),
    Chunks(Vec<(InlineOperation, InlineChunkValue)>),
}

impl InlineRowSource {
    /// Fetches referenced chunks and remaps selected rows into the returned
    /// set. Directory callers must use an isolated session or retry a
    /// changed head.
    pub(super) async fn resolve_chunks(
        self,
        handle: ReadHandle<'_>,
        table: TableId,
        mut selected: Vec<InlineRow>,
    ) -> Result<(Vec<InlineRow>, Vec<(InlineOperation, InlineChunkValue)>)> {
        let referenced = dense_chunk_references(&mut selected);

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
        let walked = chunks
            .iter()
            .filter_map(|(operation, chunk)| {
                store_inline::InlineChunkLocator::from_chunk(*operation, chunk).transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        self.verify_inline_directory(handle, table, &walked).await?;
        let rows = materialize_inline_rows(&chunks, &tombstones);

        Ok((InlineRowSource::Chunks(chunks), rows))
    }

    /// Every chunk of `table` as a body-free locator: the chunk-range
    /// directory when it is known complete, else a header walk of the
    /// chunks themselves, which verifies the directory as it passes so a
    /// later scan is served from it.
    async fn inline_chunk_directory(
        &self,
        handle: ReadHandle<'_>,
        table: TableId,
    ) -> Result<Vec<store_inline::InlineChunkLocator>> {
        if projection::inline_directory_complete(&self.projections, table.get()) {
            return store_inline::scan_inline_chunk_locators(handle, table.get()).await;
        }

        let headers = store_inline::scan_inline_chunk_headers(handle, table.get()).await?;
        self.verify_inline_directory(handle, table, &headers)
            .await?;

        Ok(headers)
    }

    /// Opens `kind`'s selection over `table_id`'s inline rows at
    /// `snapshot` (windowed from `start`) for windowed reading.
    /// `schema_version`, when set, drops rows of other versions before any
    /// body is fetched, so a caller serving one version's projection never
    /// reads another's.
    pub(crate) async fn open_inline_scan(
        &self,
        table_id: u64,
        kind: InlineScanKind,
        snapshot: u64,
        start: u64,
        schema_version: Option<u64>,
        bodies: InlineBodies,
    ) -> Result<InlineScan> {
        let table = TableId::new(table_id);
        let session = self.begin_read().await?;

        let selection = read::consistent(session.handle(), || async {
            // A session that follows the manifest cannot pin the state it
            // selected against, so it records it and compares later.
            let opened_head = if session.handle().is_isolated() {
                None
            } else {
                Some(commit::read_head_id(session.handle()).await?)
            };
            let handle = session.handle();
            let (chunks, tombstones) = futures::try_join!(
                self.inline_chunk_directory(handle, table),
                store_inline::scan_inline_deletes(handle, table.get()),
            )?;

            let rows = materialize_locator_rows(&chunks, &tombstones);
            let mut selected = kind.select(&rows, snapshot, start);
            if let Some(version) = schema_version {
                selected.retain(|row| chunks[row.chunk].schema_version() == Some(version));
            }

            Ok((chunks, selected, opened_head))
        })
        .await;

        match selection {
            Ok((chunks, selected, opened_head)) => Ok(InlineScan {
                session,
                table,
                chunks,
                selected,
                bodies,
                position: 0,
                opened_head,
            }),
            Err(error) => {
                session.finish();
                Err(error)
            }
        }
    }

    /// `kind`'s whole selection over `table_id`'s inline rows at
    /// `snapshot` (windowed from `start`) in one window, each row's
    /// `chunk` indexing the returned chunk set — only the chunks the
    /// selected rows reference.
    #[cfg(test)]
    pub(crate) async fn select_inline_rows(
        &self,
        table_id: u64,
        kind: InlineScanKind,
        snapshot: u64,
        start: u64,
        schema_version: Option<u64>,
    ) -> Result<(Vec<InlineRow>, Vec<(InlineOperation, Bytes)>)> {
        let mut scan = self
            .open_inline_scan(
                table_id,
                kind,
                snapshot,
                start,
                schema_version,
                InlineBodies::Fetch,
            )
            .await?;
        let window = scan.next_window(usize::MAX).await;
        scan.finish();

        window
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
        self.with_inline_rows(handle, table, async |source, rows| {
            let read_at = async {
                match at {
                    Some(_) => Ok(commit::resolve_read_snapshot(handle, at).await?.0),
                    None => commit::read_head_id(handle).await,
                }
            };
            let read_at = read_at.await?;

            let live = InlineScanKind::Table.select(&rows, read_at, 0);
            let (live, chunks) = source.resolve_chunks(handle, table, live).await?;

            self.recent_rows_from_chunks(handle, table, live, chunks)
                .await
        })
        .await
    }

    /// Keeps selection, body resolution, and schema reads in one stable pass.
    pub(super) async fn with_inline_rows<T>(
        &self,
        handle: ReadHandle<'_>,
        table: TableId,
        finish: impl AsyncFn(InlineRowSource, Vec<InlineRow>) -> Result<T>,
    ) -> Result<T> {
        read::consistent(handle, || async {
            let (source, rows) = self.inline_row_source(handle, table).await?;
            finish(source, rows).await
        })
        .await
    }

    pub(super) async fn recent_rows_from_chunks(
        &self,
        handle: ReadHandle<'_>,
        table: TableId,
        live: Vec<InlineRow>,
        chunks: Vec<(InlineOperation, InlineChunkValue)>,
    ) -> Result<Vec<RecentRow>> {
        let schema_versions = live.iter().filter_map(|row| match &chunks[row.chunk].0 {
            InlineOperation::Insert { schema_version, .. } => Some(*schema_version),
            _ => None,
        });
        let schemas: HashMap<u64, Arc<Vec<u8>>> =
            backfill::read_inline_schemas(handle, table, schema_versions)
                .await?
                .into_iter()
                .map(|(version, record)| (version, Arc::new(record.to_vec())))
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

    /// Compares the walked chunks against the directory and remembers a
    /// complete one. Only an isolated session may judge — a
    /// manifest-following pass can straddle a commit — and only under a
    /// format that locks out writers that predate the directory. This path
    /// never writes, so a gap is simply left for a flush to heal.
    pub(super) async fn verify_inline_directory(
        &self,
        handle: ReadHandle<'_>,
        table: TableId,
        chunks: &[store_inline::InlineChunkLocator],
    ) -> Result<()> {
        // The walk saw every chunk, so it knows the widest exactly. Raised
        // rather than replaced: a bound that could fall would silently skip
        // a chunk, and the looseness costs only a few extra entries.
        if let Some(widest) = chunks
            .iter()
            .map(|locator| locator.row_id_end() - locator.row_id_start())
            .max()
        {
            projection::note_inline_chunk_width(&self.projections, table.get(), widest);
        }

        if !handle.is_isolated()
            || projection::format_floor(&self.projections)
                < commit::FORMAT_WITH_INLINE_CHUNK_DIRECTORY
        {
            return Ok(());
        }

        // Compared by chunk identity, not by range end: two live chunks can
        // end at one row id, and each owns its own locator.
        let directory: BTreeSet<InlineOperation> =
            store_inline::scan_inline_chunk_ranges(handle, table.get())
                .await?
                .into_iter()
                .map(|(_, operation)| operation)
                .collect();
        let walked: BTreeSet<InlineOperation> =
            chunks.iter().map(|locator| locator.operation()).collect();
        if walked == directory {
            projection::note_inline_directory_complete(&self.projections, table.get());
        }

        Ok(())
    }
}
