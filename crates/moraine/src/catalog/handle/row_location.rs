//! Locating stable row ids in the physical files that currently hold them.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectStore, path::Path};
use tracing::{debug, warn};

use super::{ReadOnlyCatalog, SUMMARY_READ_CONCURRENCY, WARM_TABLE_CONCURRENCY};
use crate::{
    catalog::{CatalogSnapshot, DataFileId, DataFileInfo, FileRowCandidate, TableId},
    data_file::{self, FileSummary},
    error::Result,
};

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
        store: &Arc<dyn ObjectStore>,
        data_prefix: &str,
        table_prefix: &str,
        table: TableId,
        files: Vec<DataFileInfo>,
    ) -> Vec<(DataFileId, Result<FileSummary>)> {
        stream::iter(files.into_iter().map(|file| {
            let relative = match (file.path_is_relative, data_prefix.is_empty()) {
                (false, _) => file.path,
                (true, true) => format!("{table_prefix}{}", file.path),
                (true, false) => format!("{data_prefix}/{table_prefix}{}", file.path),
            };
            let path = Path::from(relative.as_str());
            let store = Arc::clone(store);
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
        data_store: Option<Arc<dyn ObjectStore>>,
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
        // current file, so the inlined copy is its own candidate.
        let requested: HashSet<u64> = row_ids.iter().copied().collect();
        let inlined = async {
            Ok::<_, crate::Error>(
                self.recent_rows(table)
                    .await?
                    .into_iter()
                    .map(|row| row.row_id)
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
        data_store: Arc<dyn ObjectStore>,
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
        data_store: Arc<dyn ObjectStore>,
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
        data_store: Arc<dyn ObjectStore>,
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
        data_store: &Arc<dyn ObjectStore>,
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
