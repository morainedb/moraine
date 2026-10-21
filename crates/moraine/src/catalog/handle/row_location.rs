//! Locating stable row ids in the physical files that currently hold them.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use futures::{StreamExt, TryStreamExt, stream};
use tracing::{debug, info, warn};

mod rows_at;

pub use rows_at::{ExcludedPositions, LocatedRowScan};

use super::{ReadOnlyCatalog, SUMMARY_READ_CONCURRENCY, WARM_TABLE_CONCURRENCY};
use crate::{
    catalog::{
        CatalogSnapshot, DataFileId, DataFileInfo, DeleteFileInfo, FileRowCandidate, SnapshotId,
        TableId, resolve_data_path,
        snapshot::{data_file_info, delete_file_info},
    },
    data_file::{self, DataStore, FileSummary, SidecarSweep, sidecar},
    error::{Error, Result},
    telemetry::milliseconds,
};

/// One requested row's exact position within one of a table's current data
/// files, with the delete file currently registered against it, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocatedDeletion {
    /// The data file the positions below are within.
    pub data_file_id: DataFileId,
    /// Positions of the requested rows within this file, ascending and
    /// duplicate-free.
    pub positions: Vec<u64>,
    /// Positions in the registered delete file, or `None` if there is none.
    pub existing_positions: Option<Vec<u64>>,
}

/// What [`ReadOnlyCatalog::locate_row_positions_at`] needs to resolve one
/// table's files against the data store, bundled to keep its helpers under
/// the argument-count lint.
struct LocationScope<'a> {
    store: &'a DataStore,
    data_prefix: &'a str,
    table_prefix: &'a str,
    table: TableId,
    snapshot: &'a CatalogSnapshot,
}

/// Exact physical positions before deletion visibility is applied.
#[derive(Clone)]
pub(super) struct PositionedFile {
    data_file_id: DataFileId,
    positions: Vec<u64>,
}

/// Deduplicates `pairs`, splitting them into rows grouped by named file and
/// rows naming no file.
fn group_deduped_pairs(
    pairs: impl IntoIterator<Item = (u64, Option<DataFileId>)>,
) -> (HashMap<DataFileId, Vec<u64>>, Vec<u64>) {
    let mut seen = HashSet::new();
    let mut by_file: HashMap<DataFileId, Vec<u64>> = HashMap::new();
    let mut null_rows = Vec::new();
    for (row_id, data_file_id) in pairs {
        if !seen.insert((row_id, data_file_id)) {
            continue;
        }
        match data_file_id {
            Some(id) => by_file.entry(id).or_default().push(row_id),
            None => null_rows.push(row_id),
        }
    }

    (by_file, null_rows)
}

/// The current [`DataFileInfo`] for every file id in `by_file`, in ascending
/// id order; a file id absent from the head view is a typed error naming
/// one of its rows.
fn current_files_for(
    snapshot: &CatalogSnapshot,
    table: TableId,
    by_file: &HashMap<DataFileId, Vec<u64>>,
) -> Result<Vec<DataFileInfo>> {
    let current = snapshot.data_files.get(&table.get());

    let mut file_ids: Vec<DataFileId> = by_file.keys().copied().collect();
    file_ids.sort_unstable();

    let mut files = Vec::with_capacity(file_ids.len());
    for file_id in file_ids {
        let Some(info) = current.and_then(|files| files.get(&file_id.get())) else {
            return Err(first_row_error(
                by_file,
                file_id,
                "not a current data file of this table",
            ));
        };
        files.push(crate::catalog::snapshot::data_file_info(info));
    }

    Ok(files)
}

/// A typed [`Error::RowPosition`] naming `file_id`'s first requested row.
fn first_row_error(
    by_file: &HashMap<DataFileId, Vec<u64>>,
    file_id: DataFileId,
    reason: &str,
) -> Error {
    let row_id = by_file
        .get(&file_id)
        .and_then(|rows| rows.first())
        .copied()
        .unwrap_or_default();
    Error::RowPosition {
        row_id,
        data_file_id: Some(file_id),
        reason: reason.to_owned(),
    }
}

/// Whether absent candidates fail exact positioning or are discarded by a scan.
#[derive(Clone, Copy)]
enum MissingRows {
    Reject,
    Omit,
}

/// The rows one positioning was asked for: the snapshot it is answered at,
/// whether rows a file does not hold are omitted, and the rows per file in
/// ascending file order.
pub(super) type PositioningKey = (u64, bool, Vec<(DataFileId, Vec<u64>)>);

/// The rows a positioning was asked for, in a form two callers asking for
/// the same rows produce identically. Rows keep the order they were named
/// in, which is the order the positions come back in.
fn positioning_key(
    visible_at: u64,
    missing: MissingRows,
    by_file: &HashMap<DataFileId, Vec<u64>>,
) -> PositioningKey {
    let mut files: Vec<(DataFileId, Vec<u64>)> = by_file
        .iter()
        .map(|(id, rows)| (*id, rows.clone()))
        .collect();
    files.sort_by_key(|(id, _)| *id);
    (visible_at, matches!(missing, MissingRows::Omit), files)
}

/// All physical positions, with missing or unreadable summaries failing closed.
fn positioned_rows(
    summaries: &HashMap<DataFileId, Result<FileSummary>>,
    file_id: DataFileId,
    rows: &[u64],
    missing: MissingRows,
) -> Result<Vec<u64>> {
    let first_row = rows.first().copied().unwrap_or_default();
    let summary = match summaries.get(&file_id) {
        Some(Ok(summary)) => summary,
        Some(Err(error)) => {
            return Err(Error::RowPosition {
                row_id: first_row,
                data_file_id: Some(file_id),
                reason: format!("file could not be read: {error}"),
            });
        }
        None => {
            return Err(Error::RowPosition {
                row_id: first_row,
                data_file_id: Some(file_id),
                reason: "file summary was not resolved".to_owned(),
            });
        }
    };

    let mut positions = Vec::with_capacity(rows.len());
    for row_id in rows {
        let found = summary.visit_positions(*row_id, |position| positions.push(position))?;
        if !found && matches!(missing, MissingRows::Reject) {
            return Err(Error::RowPosition {
                row_id: *row_id,
                data_file_id: Some(file_id),
                reason: "row is not held by this file".to_owned(),
            });
        }
    }
    positions.sort_unstable();
    positions.dedup();

    Ok(positions)
}

/// What one publishing pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowSummaryPublish {
    /// Current data files the pass looked at.
    pub files_considered: u64,
    /// Files whose row-id column it read, cached and published. The rest
    /// were already resident, answered from a summary published earlier,
    /// or answer from their dense range.
    pub summaries_built: u64,
    /// Files it could not summarize. They stay correct but cold: a later
    /// lookup leaves every requested row a candidate for them.
    pub files_failed: u64,
}

impl ReadOnlyCatalog {
    /// Resolves each of the table's current data files to its summary,
    /// pairing every result with the file it came from.
    pub(super) async fn file_summaries(
        &self,
        store: &DataStore,
        data_prefix: &str,
        table_prefix: &str,
        table: TableId,
        files: Vec<DataFileInfo>,
        want: data_file::Want,
    ) -> Vec<(DataFileId, Result<FileSummary>)> {
        stream::iter(files.into_iter().map(|file| {
            let retained = self
                .retained_file_summary(store, data_prefix, table_prefix, table, &file)
                .filter(|summary| {
                    // A summary held for membership alone cannot answer a
                    // caller resolving positions, which fetches instead.
                    want == data_file::Want::Membership || summary.resolves_positions()
                });
            let relative =
                resolve_data_path(data_prefix, table_prefix, &file.path, file.path_is_relative);
            let store = store.clone();
            let metrics = self.data_read_metrics();

            async move {
                if let Some(summary) = retained {
                    return (file.id, Ok(summary));
                }
                let path = match relative {
                    Ok(path) => path,
                    Err(error) => return (file.id, Err(error)),
                };
                let summary = data_file::file_summary(
                    data_file::ParquetFile::new(
                        store,
                        path,
                        file.file_size_bytes,
                        file.footer_size,
                    )
                    .with_metrics(metrics),
                    table.get(),
                    file.id.get(),
                    file.row_id_start,
                    file.record_count,
                    want,
                )
                .await;
                (file.id, summary)
            }
        }))
        .buffer_unordered(SUMMARY_READ_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
    }

    /// Locates `row_ids` among the table's current data files.
    ///
    /// Head-only, exactly as an equality lookup is. Every degraded path
    /// broadens the answer: a file that cannot be summarized leaves each
    /// requested id a candidate for it, and an id found in neither a
    /// current file nor current inline data comes back with no file. The
    /// result can therefore name a file that does not hold a row, but
    /// never omits one that does.
    ///
    /// `data_store` is the `DATA_PATH` object store; a table whose rows are
    /// entirely inlined needs none.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`](crate::Error::NotFound) if the table does
    /// not exist, or a store error if the head view cannot be read. A file
    /// that cannot be read is not an error: it broadens the answer instead.
    pub async fn locate_row_ids(
        &self,
        data_store: Option<DataStore>,
        data_prefix: &str,
        table: TableId,
        row_ids: Vec<u64>,
    ) -> Result<Vec<FileRowCandidate>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }

        // A rebind at the same revision asks for the same ids; the answer
        // has not moved, and the probe behind it was already reused.
        let revision = self.pinned_revision();
        if let Some(revision) = revision
            && let Some(located) = self.located_rows.get(revision, table, &row_ids)
        {
            debug!(
                table_id = table.get(),
                row_ids = row_ids.len(),
                "located row ids reused"
            );
            return Ok(located.as_ref().clone());
        }

        let started = Instant::now();
        let snapshot = self.snapshot().await?;
        let snapshot_ms = milliseconds(started.elapsed());

        // The two halves run concurrently, so their elapsed times overlap
        // each other and need not sum to the total.
        let file_placements = async {
            let started = Instant::now();
            let Some(store) = &data_store else {
                return Ok((HashMap::<u64, Vec<DataFileId>>::new(), 0));
            };
            let placements = self
                .locate_files(store, data_prefix, &snapshot, table, &row_ids)
                .await?;
            Ok::<_, Error>((placements, milliseconds(started.elapsed())))
        };

        // A row can be inlined and still hold an expired physical copy in a
        // current file, so the inlined copy is its own candidate. Only the
        // ids are needed, so the bodies stay unread.
        let inlined = async {
            let started = Instant::now();
            let rows = self.requested_inline_row_ids(table, &row_ids, None).await?;
            Ok::<_, Error>((rows, milliseconds(started.elapsed())))
        };
        let ((mut placements, files_ms), (inlined, inline_ms)) =
            futures::try_join!(file_placements, inlined)?;

        // Emit in the caller's order; locating must not reorder.
        let mut seen = HashSet::new();
        let located = row_ids
            .iter()
            .copied()
            .filter(|row_id| seen.insert(*row_id))
            .flat_map(|row_id| {
                // A live inlined row, and a row located nowhere at all, are
                // both reported without a file rather than
                // dropped.
                let inline_placements =
                    if inlined.contains(&row_id) || !placements.contains_key(&row_id) {
                        Some(FileRowCandidate {
                            row_id,
                            data_file_id: None,
                        })
                    } else {
                        None
                    };

                let file_placements = if let Some(files) = placements.get_mut(&row_id) {
                    // Summaries resolve concurrently, so this arrives in
                    // completion order; candidates must not vary run to run.
                    files.sort_unstable();
                    files.dedup();
                    files
                        .iter()
                        .map(|data_file_id| FileRowCandidate {
                            row_id,
                            data_file_id: Some(*data_file_id),
                        })
                        .collect()
                } else {
                    Vec::new()
                };

                file_placements.into_iter().chain(inline_placements)
            })
            .collect::<Vec<_>>();

        // The other half of an index read's bind: the lookup returns row ids,
        // this turns them into file candidates. `files_ms` is the summary
        // directory; a lookup slow here is a file-count question, not an
        // index one.
        info!(
            table_id = table.get(),
            row_ids = row_ids.len(),
            candidates = located.len(),
            snapshot_ms,
            files_ms,
            inline_ms,
            total_ms = milliseconds(started.elapsed()),
            "located row ids"
        );

        if let Some(revision) = revision {
            let bytes =
                size_of_val(row_ids.as_slice()).saturating_add(size_of_val(located.as_slice()));
            self.located_rows
                .put(revision, table, row_ids, Arc::new(located.clone()), bytes);
        }

        Ok(located)
    }

    /// Resolves committed file rows to exact positions at `snapshot`.
    /// Duplicate pairs collapse; existing delete offsets are returned per file.
    /// Use a pinned read scope when concurrent commits are possible.
    ///
    /// ```no_run
    /// # async fn locate(reads: &moraine::ReadOnlyCatalog, snapshot: &moraine::CatalogSnapshot,
    /// # store: &moraine::DataStore, table: moraine::TableId, file: moraine::DataFileId)
    /// # -> Result<(), moraine::Error> {
    /// let files = reads
    ///     .locate_row_positions_at(snapshot, store, "", table, &[(42, file)])
    ///     .await?;
    /// assert_eq!(files.len(), 1);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns [`Error::NotFound`] for an unknown table, a store error if a
    /// named file cannot be read, or [`Error::RowPosition`] if a row or file
    /// cannot be positioned exactly.
    pub async fn locate_row_positions_at(
        &self,
        snapshot: &CatalogSnapshot,
        data_store: &DataStore,
        data_prefix: &str,
        table: TableId,
        pairs: &[(u64, DataFileId)],
    ) -> Result<Vec<LocatedDeletion>> {
        if pairs.is_empty() {
            return Ok(Vec::new());
        }

        let started = Instant::now();
        let (by_file, _) = group_deduped_pairs(pairs.iter().map(|&(row, file)| (row, Some(file))));
        let requested_files_count = by_file.len();
        let table_prefix = snapshot.table_data_prefix(table)?;
        let requested_files = current_files_for(snapshot, table, &by_file)?;

        let scope = LocationScope {
            store: data_store,
            data_prefix,
            table_prefix: &table_prefix,
            table,
            snapshot,
        };

        let positioning_started = Instant::now();
        let (positioned, _) = self
            .position_requested_files(&scope, by_file, requested_files, MissingRows::Reject)
            .await?;
        let positioning_ms = milliseconds(positioning_started.elapsed());

        let existing_started = Instant::now();
        let existing_deletes = self.existing_delete_files(&scope, &positioned).await;
        let existing_ms = milliseconds(existing_started.elapsed());

        let deletions = positioned
            .into_iter()
            .zip(existing_deletes)
            .map(|(file, existing_delete)| {
                Ok(LocatedDeletion {
                    data_file_id: file.data_file_id,
                    positions: file.positions,
                    existing_positions: existing_delete?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // `existing_positions` is the deletion backlog this call had to read
        // back to rewrite: it grows with the table's history, not with the
        // rows being deleted, so a locate slow in `existing_ms` against a
        // small `pairs` is a compaction question, not a lookup one.
        let existing_positions: usize = deletions
            .iter()
            .filter_map(|deletion| deletion.existing_positions.as_ref())
            .map(Vec::len)
            .sum();
        // At `info`: one record per located delete, not per request, and the
        // level a caller can afford to leave on in production.
        info!(
            table_id = table.get(),
            pairs = pairs.len(),
            files = requested_files_count,
            existing_positions,
            positioning_ms,
            existing_ms,
            total_ms = milliseconds(started.elapsed()),
            "located row positions"
        );

        Ok(deletions)
    }

    /// Reads each requested file's summary and positions its rows, in
    /// ascending file-id order for a deterministic first failure.
    async fn position_requested_files(
        &self,
        scope: &LocationScope<'_>,
        by_file: HashMap<DataFileId, Vec<u64>>,
        requested_files: Vec<DataFileInfo>,
        missing: MissingRows,
    ) -> Result<(Vec<PositionedFile>, usize)> {
        // Only identical requests at the same pinned revision share positions.
        let revision = self.pinned_revision();
        let visible_at = scope.snapshot.current_snapshot().id.get();
        let key = positioning_key(visible_at, missing, &by_file);
        if let Some(revision) = revision
            && let Some(positioned) = self.positioned_files.get(revision, scope.table, &key)
        {
            debug!(
                table_id = scope.table.get(),
                files = positioned.len(),
                "positioned files reused"
            );
            return Ok((positioned.as_ref().clone(), 0));
        }

        let file_ids: Vec<_> = requested_files.iter().map(|file| file.id).collect();

        let summaries: HashMap<DataFileId, Result<FileSummary>> = self
            .file_summaries(
                scope.store,
                scope.data_prefix,
                scope.table_prefix,
                scope.table,
                requested_files,
                data_file::Want::Positions,
            )
            .await
            .into_iter()
            .collect();

        let summaries_built = summaries
            .values()
            .filter(|summary| summary.as_ref().is_ok_and(|summary| summary.built))
            .count();
        let mut located = Vec::with_capacity(file_ids.len());
        for file_id in file_ids {
            let rows = by_file.get(&file_id).cloned().unwrap_or_default();
            let positions = positioned_rows(&summaries, file_id, &rows, missing)?;

            located.push(PositionedFile {
                data_file_id: file_id,
                positions,
            });
        }

        if let Some(revision) = revision {
            let bytes = located
                .iter()
                .map(|file| size_of_val(file.positions.as_slice()))
                .fold(0usize, usize::saturating_add);
            self.positioned_files
                .put(revision, scope.table, key, Arc::new(located.clone()), bytes);
        }

        Ok((located, summaries_built))
    }

    /// The delete file currently registered against each positioned file,
    /// in the same order, with positions decoded for those that carry one.
    /// Reads run at the same bounded concurrency as a file summary read.
    async fn existing_delete_files(
        &self,
        scope: &LocationScope<'_>,
        files: &[PositionedFile],
    ) -> Vec<Result<Option<Vec<u64>>>> {
        stream::iter(files.iter().map(|file| {
            let delete_file = scope
                .snapshot
                .delete_files_targeting(scope.table.get(), file.data_file_id.get())
                .next()
                .map(delete_file_info);
            async move {
                match delete_file {
                    Some(delete_file) => self
                        .read_existing_delete_file(scope, delete_file)
                        .await
                        .map(Some),
                    None => Ok(None),
                }
            }
        }))
        .buffered(SUMMARY_READ_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
    }

    /// Reads and decodes one registered delete file's positions.
    async fn read_existing_delete_file(
        &self,
        scope: &LocationScope<'_>,
        delete_file: DeleteFileInfo,
    ) -> Result<Vec<u64>> {
        let relative = resolve_data_path(
            scope.data_prefix,
            scope.table_prefix,
            &delete_file.path,
            delete_file.path_is_relative,
        );
        let file = data_file::ParquetFile::new(
            scope.store.clone(),
            relative?,
            delete_file.file_size_bytes,
            delete_file.footer_size,
        )
        .with_metrics(self.data_read_metrics());

        data_file::delete_file_positions(file).await
    }

    /// Derives the summaries a lookup would otherwise build cold, publishing
    /// each one it had to read beside the file it describes.
    ///
    /// Best-effort and idempotent: a file already resident, or answering
    /// from its dense range, costs nothing, and a file that cannot be read
    /// is counted rather than raised.
    ///
    /// # Errors
    ///
    /// Returns a store error if the head view cannot be read. Per-file
    /// failures are reported in [`RowSummaryPublish::files_failed`].
    pub async fn publish_row_summaries(
        &self,
        data_store: DataStore,
        data_prefix: &str,
        table: TableId,
    ) -> Result<RowSummaryPublish> {
        let snapshot = self.snapshot().await?;

        self.publish_tables(&snapshot, &data_store, data_prefix, vec![table], None)
            .await
    }

    /// Derives and publishes the summaries of every file registered after
    /// `since`, across every table the catalog holds.
    ///
    /// # Errors
    ///
    /// Returns a store error if the head view cannot be read. Per-file
    /// failures are reported in [`RowSummaryPublish::files_failed`].
    pub async fn publish_row_summaries_since(
        &self,
        data_store: DataStore,
        data_prefix: &str,
        since: SnapshotId,
    ) -> Result<RowSummaryPublish> {
        let snapshot = self.snapshot().await?;
        let tables = snapshot
            .schemas()
            .into_iter()
            .flat_map(|schema| snapshot.tables_in(schema.id))
            .map(|table| table.id)
            .collect::<Vec<_>>();

        self.publish_tables(&snapshot, &data_store, data_prefix, tables, Some(since))
            .await
    }

    /// Deletes published row summaries whose data file is no longer under
    /// `data_prefix`, which is how a sidecar that outlived its file is
    /// reclaimed.
    ///
    /// # Errors
    ///
    /// Returns a store error if the prefix cannot be listed. Deletes that
    /// fail are counted in [`SidecarSweep::failed`] and left for the next
    /// sweep.
    pub async fn sweep_published_summaries(
        &self,
        data_store: DataStore,
        data_prefix: &str,
    ) -> Result<SidecarSweep> {
        let prefix = object_store::path::Path::parse(data_prefix.trim_end_matches('/'))
            .map_err(|error| Error::Corruption(format!("invalid data path: {error}")))?;

        sidecar::sweep(&data_store, &prefix).await
    }

    /// As [`publish_row_summaries`](Self::publish_row_summaries), for every
    /// table the catalog currently holds. Unbounded by design: prefer
    /// [`publish_row_summaries_since`](Self::publish_row_summaries_since)
    /// once a lake is already published.
    ///
    /// # Errors
    ///
    /// Returns a store error if the head view cannot be read. Per-file
    /// failures are reported in [`RowSummaryPublish::files_failed`].
    pub async fn publish_all_row_summaries(
        &self,
        data_store: DataStore,
        data_prefix: &str,
    ) -> Result<RowSummaryPublish> {
        let snapshot = self.snapshot().await?;
        let tables = snapshot
            .schemas()
            .into_iter()
            .flat_map(|schema| snapshot.tables_in(schema.id))
            .map(|table| table.id)
            .collect::<Vec<_>>();

        self.publish_tables(&snapshot, &data_store, data_prefix, tables, None)
            .await
    }

    /// Publishes summaries for the given tables without filling summary caches.
    ///
    /// # Errors
    ///
    /// Returns a store error if the head view cannot be read. Per-file
    /// failures are reported in [`RowSummaryPublish::files_failed`].
    pub async fn publish_selected_row_summaries(
        &self,
        data_store: DataStore,
        data_prefix: &str,
        tables: Vec<TableId>,
    ) -> Result<RowSummaryPublish> {
        let snapshot = self.snapshot().await?;
        self.publish_tables(&snapshot, &data_store, data_prefix, tables, None)
            .await
    }

    /// Publishes files in `tables` registered strictly after `since`.
    /// An empty table list publishes nothing.
    ///
    /// # Errors
    ///
    /// Returns a store error if the snapshot cannot be read. Per-file
    /// failures are counted in [`RowSummaryPublish::files_failed`].
    pub async fn publish_selected_row_summaries_since(
        &self,
        data_store: DataStore,
        data_prefix: &str,
        tables: Vec<TableId>,
        since: SnapshotId,
    ) -> Result<RowSummaryPublish> {
        if tables.is_empty() {
            return Ok(RowSummaryPublish::default());
        }
        let snapshot = self.snapshot().await?;
        self.publish_tables(&snapshot, &data_store, data_prefix, tables, Some(since))
            .await
    }

    async fn publish_tables(
        &self,
        snapshot: &CatalogSnapshot,
        data_store: &DataStore,
        data_prefix: &str,
        tables: Vec<TableId>,
        since: Option<SnapshotId>,
    ) -> Result<RowSummaryPublish> {
        let mut outcomes = stream::iter(tables)
            .map(|table| {
                let files = snapshot
                    .data_files
                    .get(&table.get())
                    .into_iter()
                    .flat_map(|files| files.values())
                    .filter(|file| since.is_none_or(|since| file.begin_snapshot > since.get()))
                    .map(data_file_info)
                    .collect();
                self.summarize_files(snapshot, data_store, data_prefix, table, files)
            })
            .buffer_unordered(WARM_TABLE_CONCURRENCY);
        let mut total = RowSummaryPublish::default();
        while let Some(table) = outcomes.try_next().await? {
            total.files_considered = total
                .files_considered
                .saturating_add(table.files_considered);
            total.summaries_built = total.summaries_built.saturating_add(table.summaries_built);
            total.files_failed = total.files_failed.saturating_add(table.files_failed);
        }
        Ok(total)
    }

    /// Publishes `files`' summaries, skipping those already published and
    /// those whose ids the catalog already describes.
    ///
    /// Concurrency matches a lookup's, and nothing here enters the summary
    /// caches: a pass over a lake must not evict the working set of the
    /// process running it.
    async fn summarize_files(
        &self,
        snapshot: &CatalogSnapshot,
        data_store: &DataStore,
        data_prefix: &str,
        table: TableId,
        files: Vec<DataFileInfo>,
    ) -> Result<RowSummaryPublish> {
        if files.is_empty() {
            return Ok(RowSummaryPublish::default());
        }
        let table_prefix = snapshot.table_data_prefix(table)?;
        let mut published = RowSummaryPublish {
            files_considered: u64::try_from(files.len()).unwrap_or(u64::MAX),
            ..RowSummaryPublish::default()
        };

        let mut outcomes = stream::iter(files.into_iter().map(|file| {
            let resolved = resolve_data_path(
                data_prefix,
                &table_prefix,
                &file.path,
                file.path_is_relative,
            );
            let store = data_store.clone();
            let metrics = self.data_read_metrics();

            async move {
                let written = match resolved {
                    Ok(path) => {
                        data_file::publish_if_missing(
                            data_file::ParquetFile::new(
                                store,
                                path,
                                file.file_size_bytes,
                                file.footer_size,
                            )
                            .with_metrics(metrics),
                            table.get(),
                            file.id.get(),
                            file.row_id_start,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                (file.id, written)
            }
        }))
        .buffer_unordered(SUMMARY_READ_CONCURRENCY);

        while let Some((data_file_id, written)) = outcomes.next().await {
            match written {
                Ok(true) => {
                    published.summaries_built = published.summaries_built.saturating_add(1);
                }
                Ok(false) => {}
                Err(error) => {
                    published.files_failed = published.files_failed.saturating_add(1);
                    warn!(
                        table_id = table.get(),
                        data_file_id = data_file_id.get(),
                        %error,
                        "publishing skipped a row summary it could not read"
                    );
                }
            }
        }

        debug!(
            table_id = table.get(),
            files_considered = published.files_considered,
            summaries_built = published.summaries_built,
            files_failed = published.files_failed,
            "row summaries published"
        );

        Ok(published)
    }
}
