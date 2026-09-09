//! Requested inline IDs selected before chunk bodies or row objects are
//! materialized.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
};

use super::{InlineDirectory, Intervals};
use crate::{
    catalog::{
        ReadOnlyCatalog, RecentRow, TableId, handle::inline_scan::InlineRowSource,
        inline::InlineRow, projection,
    },
    error::Result,
    store::{
        handle::{ReadHandle, ReadSession},
        inline::{self as store_inline, InlineChunkLocator, stream::InlineTombstones},
        key::InlineOperation,
        proto::{HeadValue, InlineChunkValue},
        read,
    },
    transaction::commit,
};

type ScannedChunks = Vec<(InlineOperation, InlineChunkValue)>;

impl ReadOnlyCatalog {
    async fn inline_lookup_directory(
        &self,
        session: &ReadSession,
        table: TableId,
        head: HeadValue,
    ) -> Result<(Arc<InlineDirectory>, Option<ScannedChunks>)> {
        let handle = session.handle();
        let cached = self
            .row_lookups
            .inline
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&table)
            .filter(|directory| directory.head == head)
            .cloned();
        if let Some(directory) = cached {
            return Ok((directory, None));
        }
        let mut scanned = None;
        let locators = if projection::inline_directory_complete(&self.projections, table.get()) {
            store_inline::scan_inline_chunk_locators(handle, table.get()).await?
        } else {
            let chunks = store_inline::scan_inline_chunks(handle, table.get()).await?;
            self.verify_inline_directory(handle, table, &chunks).await?;
            let locators = chunks
                .iter()
                .map(|(operation, chunk)| InlineChunkLocator::from_chunk(*operation, chunk))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect();
            if !handle.is_isolated() {
                scanned = Some(chunks);
            }
            locators
        };
        let directory = Arc::new(InlineDirectory {
            head,
            ranges: Intervals::new(
                locators
                    .into_iter()
                    .map(|locator| (locator.row_id_start(), locator.row_id_end(), locator)),
            ),
        });
        Ok((directory, scanned))
    }

    /// Selects the requested rows live at `visible_at` (the head when
    /// `None`) from the directory built at `head`.
    async fn requested_inline_rows(
        &self,
        session: &ReadSession,
        table: TableId,
        requested: &[u64],
        head: HeadValue,
        visible_at: Option<u64>,
    ) -> Result<(InlineRowSource, Vec<InlineRow>, Arc<InlineDirectory>)> {
        let (directory, scanned) = self.inline_lookup_directory(session, table, head).await?;
        let visible_at = visible_at.unwrap_or(directory.head.snapshot_id);
        let mut selected = Vec::new();
        let mut locators = Vec::new();
        let mut chunks = BTreeMap::new();
        for row in requested.iter().copied().collect::<BTreeSet<_>>() {
            let mut matches = Vec::new();
            directory
                .ranges
                .visit(row, |locator| matches.push(*locator));
            if matches.is_empty() {
                continue;
            }
            let deletion = latest_deletion(session.handle(), table, row, visible_at).await?;
            for locator in matches {
                let InlineOperation::Insert { begin_snapshot, .. } = locator.operation() else {
                    continue;
                };
                if begin_snapshot > visible_at || deletion.is_some_and(|end| begin_snapshot < end) {
                    continue;
                }
                let chunk = *chunks.entry(locator.operation()).or_insert_with(|| {
                    let index = locators.len();
                    locators.push(locator);
                    index
                });
                selected.push(InlineRow {
                    row_id: row,
                    begin_snapshot,
                    end_snapshot: None,
                    chunk,
                    offset_in_chunk: row - locator.row_id_start(),
                });
            }
        }
        selected.sort_unstable_by_key(|row| (row.row_id, row.begin_snapshot));
        let source = if let Some(scanned) = scanned {
            let mut referenced: Vec<_> = scanned
                .into_iter()
                .filter_map(|(operation, body)| {
                    chunks
                        .get(&operation)
                        .map(|index| (*index, (operation, body)))
                })
                .collect();
            referenced.sort_unstable_by_key(|(index, _)| *index);
            InlineRowSource::Chunks(referenced.into_iter().map(|(_, chunk)| chunk).collect())
        } else {
            InlineRowSource::Locators(locators)
        };
        Ok((source, selected, directory))
    }

    /// Selects and resolves rows from one stable manifest state, visible at
    /// `visible_at` (the head when `None`).
    async fn lookup_inline<T>(
        &self,
        session: &ReadSession,
        table: TableId,
        requested: &[u64],
        visible_at: Option<u64>,
        finish: impl AsyncFn(InlineRowSource, Vec<InlineRow>) -> Result<T>,
    ) -> Result<T> {
        let handle = session.handle();
        let (result, directory) = read::consistent(handle, || async {
            let head = commit::read_head_value(handle).await?;
            let (source, rows, directory) = self
                .requested_inline_rows(session, table, requested, head, visible_at)
                .await?;
            finish(source, rows).await.map(|result| (result, directory))
        })
        .await?;
        super::install(&self.row_lookups.inline, table, directory);
        Ok(result)
    }

    /// The ids among `requested` that are live inlined rows at `visible_at`
    /// (the head when `None`).
    pub(in crate::catalog::handle) async fn requested_inline_row_ids(
        &self,
        table: TableId,
        requested: &[u64],
        visible_at: Option<u64>,
    ) -> Result<HashSet<u64>> {
        if requested.is_empty() {
            return Ok(HashSet::new());
        }
        let session = self.begin_read().await?;
        let outcome = self
            .lookup_inline(&session, table, requested, visible_at, async |_, rows| {
                Ok(rows.into_iter().map(|row| row.row_id).collect())
            })
            .await;
        session.finish();
        outcome
    }

    /// The requested rows live at `visible_at` (the head when `None`), with
    /// only their chunks' bodies read.
    pub(in crate::catalog::handle) async fn requested_inline_recent_rows(
        &self,
        table: TableId,
        requested: &[u64],
        visible_at: Option<u64>,
    ) -> Result<Vec<RecentRow>> {
        if requested.is_empty() {
            return Ok(Vec::new());
        }
        let session = self.begin_read().await?;
        let outcome = self
            .lookup_inline(
                &session,
                table,
                requested,
                visible_at,
                async |source, rows| {
                    let (rows, chunks) =
                        source.resolve_chunks(session.handle(), table, rows).await?;
                    self.recent_rows_from_chunks(session.handle(), table, rows, chunks)
                        .await
                },
            )
            .await;
        session.finish();
        outcome
    }

    pub(in crate::catalog::handle) async fn scan_recent_row(
        &self,
        session: &ReadSession,
        table: TableId,
        row: u64,
    ) -> Result<Option<RecentRow>> {
        self.lookup_inline(session, table, &[row], None, async |source, rows| {
            let (rows, chunks) = source.resolve_chunks(session.handle(), table, rows).await?;
            Ok(self
                .recent_rows_from_chunks(session.handle(), table, rows, chunks)
                .await?
                .into_iter()
                .next())
        })
        .await
    }
}

async fn latest_deletion(
    handle: ReadHandle<'_>,
    table: TableId,
    row: u64,
    head: u64,
) -> Result<Option<u64>> {
    InlineTombstones::open(handle, table.get(), row, row)
        .await?
        .latest_at(row, head)
        .await
}

#[cfg(test)]
mod tests;
