//! Locating stable row ids in the physical files that currently hold them.

use std::collections::{HashMap, HashSet};

use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use tracing::{debug, warn};

use super::{Catalog, ReadOnlyCatalog, SUMMARY_READ_CONCURRENCY, WARM_TABLE_CONCURRENCY};
use crate::{
    catalog::{
        CatalogSnapshot, DataFileId, DataFileInfo, DeleteFile, DeleteFileId, DeleteFileInfo,
        FileIndexRemoval, FileRowCandidate, IndexInfo, SnapshotId, TableId, resolve_data_path,
    },
    data_file::{self, DataStore, FileSummary},
    error::{Error, Result},
    store::index_encoding::IndexKeyValue,
};

/// One requested row's exact position within one of a table's current data
/// files, with the delete file currently registered against it, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocatedDeletion {
    /// The data file the positions below are within.
    pub data_file_id: DataFileId,
    /// The file's recorded path.
    pub file_path: String,
    /// Positions of the requested rows within this file, ascending and
    /// duplicate-free.
    pub positions: Vec<u64>,
    /// The delete file currently registered against this data file, if any.
    pub existing_delete: Option<ExistingDeleteFile>,
}

/// What [`ReadOnlyCatalog::locate_row_positions`] resolved: one
/// [`LocatedDeletion`] per data file carrying a requested row, the row ids
/// resolved as live inlined rows, and — when any position was resolved —
/// the directory a new delete file for this table belongs in.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LocatedPositions {
    /// Per-data-file located positions and any delete file already
    /// registered against it.
    pub deletions: Vec<LocatedDeletion>,
    /// Row ids resolved as live inlined rows.
    pub inlined_rows: Vec<u64>,
    /// The absolute directory a new delete file for this table belongs in,
    /// resolved from the same snapshot `deletions` was positioned against.
    /// `None` when `deletions` is empty, since only then does a caller need
    /// to write nothing.
    pub write_directory: Option<String>,
}

/// A delete file already registered against a data file a caller is about
/// to delete more rows from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingDeleteFile {
    /// The delete file's id.
    pub delete_file_id: DeleteFileId,
    /// The file's recorded path.
    pub path: String,
    /// Its decoded positions, ascending and duplicate-free.
    pub positions: Vec<u64>,
}

/// What [`ReadOnlyCatalog::locate_row_positions`] needs to resolve one
/// table's files against the data store, bundled to keep its helpers under
/// the argument-count lint.
struct LocationScope<'a> {
    store: &'a DataStore,
    data_prefix: &'a str,
    table_prefix: &'a str,
    table: TableId,
    snapshot: &'a CatalogSnapshot,
}

/// Deduplicates `pairs`, splitting them into rows grouped by named file and
/// rows naming no file.
fn group_deduped_pairs(
    pairs: &[(u64, Option<DataFileId>)],
) -> (HashMap<DataFileId, Vec<u64>>, Vec<u64>) {
    let mut seen = HashSet::new();
    let mut by_file: HashMap<DataFileId, Vec<u64>> = HashMap::new();
    let mut null_rows = Vec::new();
    for &(row_id, data_file_id) in pairs {
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
    let current: HashMap<DataFileId, DataFileInfo> = snapshot
        .data_files_of(table)
        .into_iter()
        .map(|file| (file.id, file))
        .collect();

    let mut file_ids: Vec<DataFileId> = by_file.keys().copied().collect();
    file_ids.sort_unstable();

    let mut files = Vec::with_capacity(file_ids.len());
    for file_id in file_ids {
        let Some(info) = current.get(&file_id) else {
            return Err(first_row_error(
                by_file,
                file_id,
                "not a current data file of this table",
            ));
        };
        files.push(info.clone());
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

/// Positions `rows` within `file_id`'s summary, ascending and duplicate-free;
/// a missing summary, an unreadable file, or a row this file does not hold
/// is a typed error naming that row.
fn positioned_rows(
    summaries: &HashMap<DataFileId, Result<FileSummary>>,
    file_id: DataFileId,
    rows: &[u64],
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
    for (row_id, position) in rows.iter().zip(summary.positions_of(rows)) {
        match position {
            Some(position) => positions.push(position),
            None => {
                return Err(Error::RowPosition {
                    row_id: *row_id,
                    data_file_id: Some(file_id),
                    reason: "row is not held by this file".to_owned(),
                });
            }
        }
    }
    positions.sort_unstable();
    positions.dedup();

    Ok(positions)
}

/// What one warming pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowSummaryWarmth {
    /// Current data files the pass looked at.
    pub files_considered: u64,
    /// Files whose row-id column it read and cached. The rest were already
    /// resident or answer from their dense range.
    pub summaries_built: u64,
    /// Files it could not summarize. They stay correct but cold: a later
    /// lookup leaves every requested row a candidate for them.
    pub files_failed: u64,
}

impl ReadOnlyCatalog {
    /// Resolves each of the table's current data files to its summary,
    /// pairing every result with the file it came from.
    async fn file_summaries(
        &self,
        store: &DataStore,
        data_prefix: &str,
        table_prefix: &str,
        table: TableId,
        files: Vec<DataFileInfo>,
    ) -> Vec<(DataFileId, Result<FileSummary>)> {
        stream::iter(files.into_iter().map(|file| {
            let relative =
                resolve_data_path(data_prefix, table_prefix, &file.path, file.path_is_relative);
            let path = Path::from(relative.as_str());
            let store = store.clone();
            let metrics = self.data_read_metrics();

            async move {
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

        let snapshot = self.snapshot().await?;

        let file_placements = async {
            let Some(store) = &data_store else {
                return Ok(HashMap::<u64, Vec<DataFileId>>::new());
            };
            let table_prefix = snapshot.table_data_prefix(table)?;
            let files = snapshot.data_files_of(table);

            let mut placements = HashMap::<u64, Vec<DataFileId>>::new();
            for (data_file_id, summary) in self
                .file_summaries(store, data_prefix, &table_prefix, table, files)
                .await
            {
                let matched = match summary {
                    Ok(summary) => summary.matching(&row_ids),
                    Err(error) => {
                        warn!(
                            table_id = table.get(),
                            data_file_id = data_file_id.get(),
                            %error,
                            "row location fell back to every requested row for this file"
                        );
                        row_ids.clone()
                    }
                };
                for row_id in matched {
                    placements.entry(row_id).or_default().push(data_file_id);
                }
            }

            Ok(placements)
        };

        // A row can be inlined and still hold an expired physical copy in a
        // current file, so the inlined copy is its own candidate. Only the
        // ids are needed, so the bodies stay unread.
        let requested: HashSet<u64> = row_ids.iter().copied().collect();
        let inlined = async {
            Ok::<_, crate::Error>(
                self.live_inline_row_ids(table)
                    .await?
                    .into_iter()
                    .filter(|row_id| requested.contains(row_id))
                    .collect::<HashSet<u64>>(),
            )
        };
        let (mut placements, inlined) = futures::try_join!(file_placements, inlined)?;

        // Emit in the caller's order; locating must not reorder.
        let mut seen = HashSet::new();
        let located = row_ids
            .iter()
            .copied()
            .filter(|row_id| seen.insert(*row_id))
            .flat_map(|row_id| {
                // A live inlined row, and a row located nowhere at all, are both
                // reported without a file rather than dropped.
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
            .collect();

        Ok(located)
    }

    /// Resolves located rows — `(row_id, data_file_id)` pairs, as a lookup
    /// returns them — to their exact file positions, for deletion without a
    /// scan.
    ///
    /// Position resolution inverts locating's contract: locating may
    /// broaden, but a position may not, since a wrong position deletes a
    /// different row. Every pair is exact-or-failed: a row a named file
    /// does not hold, a file id absent from the head view, an unreadable
    /// file, or a `None` file id whose row is not a live inlined row, all
    /// fail the whole call rather than guess. `None`-file pairs whose row
    /// is live inlined are returned as inlined row ids instead. Duplicate
    /// pairs collapse to one answer.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist, a store
    /// error if the head view or a named file cannot be read, or
    /// [`Error::RowPosition`] for the first pair that cannot be positioned
    /// exactly.
    pub async fn locate_row_positions(
        &self,
        data_store: Option<DataStore>,
        data_prefix: &str,
        table: TableId,
        pairs: &[(u64, Option<DataFileId>)],
    ) -> Result<LocatedPositions> {
        if pairs.is_empty() {
            return Ok(LocatedPositions::default());
        }

        let snapshot = self.snapshot().await?;
        let (by_file, null_rows) = group_deduped_pairs(pairs);
        let inlined_rows = self.resolve_inlined(table, null_rows).await?;

        if by_file.is_empty() {
            return Ok(LocatedPositions {
                inlined_rows,
                ..LocatedPositions::default()
            });
        }

        let table_prefix = snapshot.table_data_prefix(table)?;
        let requested_files = current_files_for(&snapshot, table, &by_file)?;

        let Some(store) = data_store else {
            return Err(first_row_error(
                &by_file,
                requested_files[0].id,
                "no data store was supplied to read the file",
            ));
        };
        let scope = LocationScope {
            store: &store,
            data_prefix,
            table_prefix: &table_prefix,
            table,
            snapshot: &snapshot,
        };

        let deletions = self
            .position_requested_files(&scope, by_file, requested_files)
            .await?;
        // Same snapshot the positions above were resolved against, so the
        // directory names the table's current location, not a later one.
        let write_directory = Some(snapshot.table_write_directory(table)?);

        Ok(LocatedPositions {
            deletions,
            inlined_rows,
            write_directory,
        })
    }

    /// Confirms every row naming no file is a live inlined row, failing
    /// typed on the first that is neither.
    async fn resolve_inlined(&self, table: TableId, null_rows: Vec<u64>) -> Result<Vec<u64>> {
        self.require_live_inlined_rows(table, &null_rows).await?;
        Ok(null_rows)
    }

    /// Reads each requested file's summary and positions its rows, in
    /// ascending file-id order for a deterministic first failure.
    async fn position_requested_files(
        &self,
        scope: &LocationScope<'_>,
        by_file: HashMap<DataFileId, Vec<u64>>,
        requested_files: Vec<DataFileInfo>,
    ) -> Result<Vec<LocatedDeletion>> {
        // `current_files_for` already returned these in ascending id order;
        // carrying the pairs through avoids both a second sort and a
        // separate id-to-path lookup.
        let file_id_paths: Vec<(DataFileId, String)> = requested_files
            .iter()
            .map(|file| (file.id, file.path.clone()))
            .collect();

        let delete_files: HashMap<DataFileId, DeleteFileInfo> = scope
            .snapshot
            .delete_files_of(scope.table)
            .into_iter()
            .map(|file| (file.data_file_id, file))
            .collect();

        let summaries: HashMap<DataFileId, Result<FileSummary>> = self
            .file_summaries(
                scope.store,
                scope.data_prefix,
                scope.table_prefix,
                scope.table,
                requested_files,
            )
            .await
            .into_iter()
            .collect();

        let existing_deletes = self
            .existing_delete_files(scope, &file_id_paths, &delete_files)
            .await;

        let mut located = Vec::with_capacity(file_id_paths.len());
        for ((file_id, path), existing_delete) in file_id_paths.into_iter().zip(existing_deletes) {
            let rows = by_file.get(&file_id).cloned().unwrap_or_default();
            let positions = positioned_rows(&summaries, file_id, &rows)?;

            located.push(LocatedDeletion {
                data_file_id: file_id,
                file_path: path,
                positions,
                existing_delete: existing_delete?,
            });
        }

        Ok(located)
    }

    /// The delete file currently registered against each of `file_id_paths`,
    /// in the same order, with positions decoded for those that carry one.
    /// Reads run at the same bounded concurrency as a file summary read.
    async fn existing_delete_files(
        &self,
        scope: &LocationScope<'_>,
        file_id_paths: &[(DataFileId, String)],
        delete_files: &HashMap<DataFileId, DeleteFileInfo>,
    ) -> Vec<Result<Option<ExistingDeleteFile>>> {
        stream::iter(file_id_paths.iter().map(|(file_id, _)| {
            let delete_file = delete_files.get(file_id).cloned();
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
    ) -> Result<ExistingDeleteFile> {
        let relative = resolve_data_path(
            scope.data_prefix,
            scope.table_prefix,
            &delete_file.path,
            delete_file.path_is_relative,
        );
        let file = data_file::ParquetFile::new(
            scope.store.clone(),
            Path::from(relative.as_str()),
            delete_file.file_size_bytes,
            delete_file.footer_size,
        )
        .with_metrics(self.data_read_metrics());

        let positions = data_file::delete_file_positions(file).await?;

        Ok(ExistingDeleteFile {
            delete_file_id: delete_file.id,
            path: delete_file.path,
            positions,
        })
    }

    /// Builds and caches the summaries a lookup would otherwise build cold.
    ///
    /// Best-effort and idempotent: a file already resident, or answering
    /// from its dense range, costs nothing, and a file that cannot be read
    /// is counted rather than raised.
    ///
    /// # Errors
    ///
    /// Returns a store error if the head view cannot be read. Per-file
    /// failures are reported in [`RowSummaryWarmth::files_failed`].
    pub async fn warm_row_summaries(
        &self,
        data_store: DataStore,
        data_prefix: &str,
        table: TableId,
    ) -> Result<RowSummaryWarmth> {
        let snapshot = self.snapshot().await?;

        self.warm_table(&snapshot, &data_store, data_prefix, table)
            .await
    }

    /// Builds and caches the summaries a lookup would otherwise build cold,
    /// for every table the catalog currently holds.
    ///
    /// # Errors
    ///
    /// Returns a store error if the head view cannot be read. Per-file
    /// failures are reported in [`RowSummaryWarmth::files_failed`].
    pub async fn warm_all_row_summaries(
        &self,
        data_store: DataStore,
        data_prefix: &str,
    ) -> Result<RowSummaryWarmth> {
        let snapshot = self.snapshot().await?;
        let tables = snapshot
            .schemas()
            .into_iter()
            .flat_map(|schema| snapshot.tables_in(schema.id))
            .map(|table| table.id)
            .collect::<Vec<_>>();

        self.warm_selected_row_summaries(data_store, data_prefix, tables)
            .await
    }

    /// Builds and caches the summaries a lookup would otherwise build cold,
    /// for the given tables.
    ///
    /// # Errors
    ///
    /// Returns a store error if the head view cannot be read. Per-file
    /// failures are reported in [`RowSummaryWarmth::files_failed`].
    pub async fn warm_selected_row_summaries(
        &self,
        data_store: DataStore,
        data_prefix: &str,
        tables: Vec<TableId>,
    ) -> Result<RowSummaryWarmth> {
        let snapshot = self.snapshot().await?;
        let total = stream::iter(tables)
            .map(|table| self.warm_table(&snapshot, &data_store, data_prefix, table))
            .buffer_unordered(WARM_TABLE_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .fold(RowSummaryWarmth::default(), |acc, table| RowSummaryWarmth {
                files_considered: acc.files_considered.saturating_add(table.files_considered),
                summaries_built: acc.summaries_built.saturating_add(table.summaries_built),
                files_failed: acc.files_failed.saturating_add(table.files_failed),
            });

        Ok(total)
    }

    /// One table's warming pass against an already-resolved head view.
    async fn warm_table(
        &self,
        snapshot: &CatalogSnapshot,
        data_store: &DataStore,
        data_prefix: &str,
        table: TableId,
    ) -> Result<RowSummaryWarmth> {
        let table_prefix = snapshot.table_data_prefix(table)?;
        let files = snapshot.data_files_of(table);
        let mut warmth = RowSummaryWarmth {
            files_considered: u64::try_from(files.len()).unwrap_or(u64::MAX),
            ..RowSummaryWarmth::default()
        };

        for (data_file_id, summary) in self
            .file_summaries(data_store, data_prefix, &table_prefix, table, files)
            .await
        {
            match summary {
                Ok(summary) if summary.built => {
                    warmth.summaries_built = warmth.summaries_built.saturating_add(1);
                }
                Ok(_) => {}
                Err(error) => {
                    warmth.files_failed = warmth.files_failed.saturating_add(1);
                    warn!(
                        table_id = table.get(),
                        data_file_id = data_file_id.get(),
                        %error,
                        "row summary warm skipped a file it could not read"
                    );
                }
            }
        }

        debug!(
            table_id = table.get(),
            files_considered = warmth.files_considered,
            summaries_built = warmth.summaries_built,
            files_failed = warmth.files_failed,
            "row summaries warmed"
        );

        Ok(warmth)
    }
}

/// One delete file to land through [`Catalog::commit_located_deletion`]:
/// the caller has already written it — the union of an existing delete
/// file's positions (if any) with newly located deletions — and supplies
/// enough to register it in place of the file it replaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFileRegistration {
    /// The data file this delete file's positions apply to.
    pub data_file_id: DataFileId,
    /// The delete file's name within the table's own data directory (a
    /// bare file name, no path components) — the caller already wrote it
    /// there. `commit_located_deletion` registers it relative to the
    /// table, the same resolution DuckLake's own reader applies, so a
    /// scan can open it without going through this store.
    pub path: String,
    /// Total file size in bytes.
    pub file_size: u64,
    /// Parquet footer size in bytes. Not derivable from `file_size` or
    /// `delete_count`, so the writer — who already produced the file —
    /// supplies it directly.
    pub footer_size: u64,
    /// Number of positions the file records.
    pub delete_count: u64,
    /// The delete file this one replaces, expired in the same commit. A
    /// data file admits only one live delete file, so this must name the
    /// one currently registered against `data_file_id`, if any.
    pub expires: Option<DeleteFileId>,
    /// Physical positions within the data file that this registration
    /// newly marks dead — the delete file's own recorded positions minus
    /// whatever `expires` already carried, exactly the positions a lookup
    /// resolved through [`ReadOnlyCatalog::locate_row_positions`]. Read
    /// only to derive index entry removals for the table's live equality
    /// indexes; empty when the table carries none.
    pub new_positions: Vec<u64>,
}

/// The union of keys every index in `indexes` touches, plus each index's
/// positions within that union, preserving first-seen order so the union is
/// deterministic. Reading the union once and slicing per index avoids
/// re-reading the same columns once per index.
fn union_index_keys<T: Eq + std::hash::Hash + Copy>(
    indexes: &[IndexInfo],
    keys_of: impl Fn(&IndexInfo) -> Result<Vec<T>>,
) -> Result<(Vec<T>, Vec<Vec<usize>>)> {
    let mut union = Vec::new();
    let mut seen: HashMap<T, usize> = HashMap::new();
    let mut per_index = Vec::with_capacity(indexes.len());
    for index in indexes {
        let mapped = keys_of(index)?
            .into_iter()
            .map(|key| {
                *seen.entry(key).or_insert_with(|| {
                    union.push(key);
                    union.len() - 1
                })
            })
            .collect();
        per_index.push(mapped);
    }

    Ok((union, per_index))
}

/// Splits `entries` — each a row id with values in the union's key order —
/// into one [`FileIndexRemoval`] per live index per entry, using `per_index`
/// (as [`union_index_keys`] built it) to select that index's own values.
fn split_index_removals(
    indexes: &[IndexInfo],
    per_index: &[Vec<usize>],
    entries: impl IntoIterator<Item = (u64, Vec<Option<IndexKeyValue>>)>,
) -> Vec<FileIndexRemoval> {
    entries
        .into_iter()
        .flat_map(|(row_id, values)| {
            indexes
                .iter()
                .zip(per_index)
                .map(|(index, mapped)| FileIndexRemoval {
                    index: index.id,
                    row_id,
                    values: mapped
                        .iter()
                        .map(|&position| values[position].clone())
                        .collect(),
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// What deriving one registration's index removals needs, shared across a
/// table's registrations to keep a per-registration call under the
/// argument-count lint.
struct DeleteIndexScope<'a> {
    data_store: Option<&'a DataStore>,
    data_prefix: &'a str,
    table_prefix: &'a str,
    table: TableId,
    files: &'a HashMap<DataFileId, DataFileInfo>,
    indexes: &'a [IndexInfo],
    union_positions: &'a [usize],
    per_index: &'a [Vec<usize>],
}

impl ReadOnlyCatalog {
    /// The removal every live index needs for each registration's newly
    /// killed rows, one list per registration in input order — derived by
    /// one scoped read per registration's own data file at `new_positions`,
    /// at the union of every live index's columns, then split per index.
    /// Reads only the data files the registrations name, at bounded
    /// concurrency.
    async fn delete_file_index_removals(
        &self,
        data_store: Option<&DataStore>,
        data_prefix: &str,
        snapshot: &CatalogSnapshot,
        table: TableId,
        registrations: &[DeleteFileRegistration],
    ) -> Result<Vec<Vec<FileIndexRemoval>>> {
        let indexes = snapshot.indexes_of(table);
        if indexes.is_empty() {
            return Ok(vec![Vec::new(); registrations.len()]);
        }

        let table_prefix = snapshot.table_data_prefix(table)?;
        let files: HashMap<DataFileId, DataFileInfo> = snapshot
            .data_files_of(table)
            .into_iter()
            .map(|file| (file.id, file))
            .collect();
        let (union_positions, per_index) = union_index_keys(&indexes, |index| {
            snapshot.column_positions(table, &index.columns)
        })?;

        let scope = DeleteIndexScope {
            data_store,
            data_prefix,
            table_prefix: &table_prefix,
            table,
            files: &files,
            indexes: &indexes,
            union_positions: &union_positions,
            per_index: &per_index,
        };

        // Cloning into an owned future avoids the future's type depending on
        // an external borrow, which trips a higher-ranked lifetime the
        // buffered adapter cannot express for a `map` over borrowed items.
        stream::iter(registrations.iter().cloned().map(|registration| {
            let scope = &scope;
            async move { self.registration_index_removals(scope, &registration).await }
        }))
        .buffered(SUMMARY_READ_CONCURRENCY)
        .try_collect()
        .await
    }

    /// One registration's index removals: empty if it kills nothing, else
    /// one scoped read at the union of every live index's columns, split
    /// per index by [`split_index_removals`].
    async fn registration_index_removals(
        &self,
        scope: &DeleteIndexScope<'_>,
        registration: &DeleteFileRegistration,
    ) -> Result<Vec<FileIndexRemoval>> {
        if registration.new_positions.is_empty() {
            return Ok(Vec::new());
        }
        let file = scope.files.get(&registration.data_file_id).ok_or_else(|| {
            Error::NotFound(format!(
                "data file {} of table {}",
                registration.data_file_id, scope.table
            ))
        })?;
        let store = scope.data_store.ok_or_else(|| {
            Error::Constraint(format!(
                "commit_located_deletion: table {} carries live indexes; no data store was \
                 supplied to derive index entry removals for data file {}",
                scope.table, registration.data_file_id
            ))
        })?;
        let relative = resolve_data_path(
            scope.data_prefix,
            scope.table_prefix,
            &file.path,
            file.path_is_relative,
        );
        let path = Path::from(relative.as_str());
        let positions = data_file::RowPositions::from_unsorted(registration.new_positions.clone());
        let parquet = data_file::ParquetFile::new(
            store.clone(),
            path,
            file.file_size_bytes,
            file.footer_size,
        )
        .with_metrics(self.data_read_metrics());

        let entries = data_file::scoped_read_recorded_entries(
            parquet,
            scope.union_positions,
            data_file::ScopedRows::At(&positions),
            data_file::RowIdSource::Resolve {
                row_id_start: file.row_id_start,
            },
        )
        .await?;

        Ok(split_index_removals(
            scope.indexes,
            scope.per_index,
            entries
                .into_iter()
                .map(|entry| (entry.row_id, entry.values)),
        ))
    }

    /// Fails typed on any id in `inlined_rows` no longer a live inlined
    /// row: a flush settling between locate and commit would otherwise turn
    /// the tombstone into a silent no-op. A flush racing after this check
    /// conflicts at commit.
    async fn require_live_inlined_rows(&self, table: TableId, inlined_rows: &[u64]) -> Result<()> {
        if inlined_rows.is_empty() {
            return Ok(());
        }

        let live: HashSet<u64> = self.live_inline_row_ids(table).await?.into_iter().collect();
        for &row_id in inlined_rows {
            if !live.contains(&row_id) {
                return Err(Error::RowPosition {
                    row_id,
                    data_file_id: None,
                    reason: "row is not a live inlined row".to_owned(),
                });
            }
        }

        Ok(())
    }

    /// The removal every live index needs per inlined row, in
    /// `inlined_rows` order — derived from one pass over every live chunk
    /// at the union of every live index's columns, then split per index.
    /// Inline data has no per-file grain, so every live chunk is read.
    async fn inline_delete_index_removals(
        &self,
        snapshot: &CatalogSnapshot,
        table: TableId,
        inlined_rows: &[u64],
    ) -> Result<Vec<Vec<FileIndexRemoval>>> {
        let indexes = snapshot.indexes_of(table);
        if indexes.is_empty() || inlined_rows.is_empty() {
            return Ok(vec![Vec::new(); inlined_rows.len()]);
        }

        let (union_columns, per_index) =
            union_index_keys(&indexes, |index| Ok(index.columns.clone()))?;
        let wanted: HashSet<u64> = inlined_rows.iter().copied().collect();
        let entries = self.inline_backfill_entries(table, &union_columns).await?;

        let mut by_row: HashMap<u64, Vec<FileIndexRemoval>> = HashMap::new();
        for removal in split_index_removals(
            &indexes,
            &per_index,
            entries
                .into_iter()
                .filter(|entry| wanted.contains(&entry.row_id))
                .map(|entry| (entry.row_id, entry.values)),
        ) {
            by_row.entry(removal.row_id).or_default().push(removal);
        }

        Ok(inlined_rows
            .iter()
            .map(|row_id| by_row.remove(row_id).unwrap_or_default())
            .collect())
    }
}

impl Catalog {
    /// Lands located deletions in one autonomous commit: per registration,
    /// the replaced delete file is expired (a data file admits one live
    /// delete file), the new one — already written by the caller — is
    /// registered with the index entry removals its newly killed rows
    /// need, and each inlined row is tombstoned by
    /// [`inline_delete`](crate::Transaction::inline_delete). Removals are
    /// derived by scoped-reading the indexed columns at exactly the
    /// positions or rows going dead.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table is not live, if a
    /// registration's `data_file_id` is not a live data file, or if
    /// `expires` does not name a delete file currently live on the table.
    /// Returns [`Error::Constraint`] if the table carries live indexes and
    /// no `data_store` was supplied to derive a file registration's index
    /// entry removals. Returns [`Error::RowPosition`] if an id in
    /// `inlined_rows` is not a live inlined row of the table. Returns
    /// [`Error::CommitConflict`] if a concurrent commit touched the same
    /// table.
    pub async fn commit_located_deletion(
        &self,
        data_store: Option<DataStore>,
        data_prefix: &str,
        table: TableId,
        registrations: &[DeleteFileRegistration],
        inlined_rows: &[u64],
    ) -> Result<SnapshotId> {
        let snapshot = self.snapshot().await?;
        self.require_live_inlined_rows(table, inlined_rows).await?;
        let file_removals = self
            .delete_file_index_removals(
                data_store.as_ref(),
                data_prefix,
                &snapshot,
                table,
                registrations,
            )
            .await?;
        let inline_removals = self
            .inline_delete_index_removals(&snapshot, table, inlined_rows)
            .await?;

        self.commit(move |tx| {
            for (registration, removals) in registrations.iter().zip(file_removals.iter()) {
                if let Some(expires) = registration.expires {
                    tx.expire_delete_file(table, expires)?;
                }
                tx.register_delete_file(
                    table,
                    DeleteFile {
                        data_file_id: registration.data_file_id,
                        path: registration.path.clone(),
                        // `registration.path` is a bare file name: DuckLake's
                        // own reader resolves a delete file's path against
                        // the table's directory when `path_is_relative`, the
                        // same composition this store's own
                        // `resolve_data_path` applies for its reads — a
                        // caller-supplied absolute path (`false`) is not an
                        // object-store-absolute key here, so it round-trips
                        // through neither reader correctly.
                        path_is_relative: true,
                        format: "parquet".to_owned(),
                        delete_count: registration.delete_count,
                        file_size_bytes: registration.file_size,
                        footer_size: registration.footer_size,
                        encryption_key: None,
                    },
                    removals,
                )?;
            }
            for (&row_id, removals) in inlined_rows.iter().zip(inline_removals.iter()) {
                tx.inline_delete(table, row_id, removals)?;
            }
            Ok(())
        })
        .await
    }
}
