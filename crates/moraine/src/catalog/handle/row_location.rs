//! Locating stable row ids in the physical files that currently hold them.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use futures::{StreamExt, stream};
use object_store::{ObjectStore, path::Path};
use tracing::{debug, warn};

use super::{BACKFILL_FILE_READ_CONCURRENCY, ReadOnlyCatalog};
use crate::{
    catalog::{DataFileId, DataFileInfo, FileRowCandidate, TableId},
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
        store: &Arc<dyn ObjectStore>,
        data_prefix: &str,
        table_prefix: &str,
        table: TableId,
        files: Vec<DataFileInfo>,
    ) -> Vec<(DataFileId, Result<FileSummary>)> {
        let resolve = |path: &str, is_relative: bool| {
            let relative = match (is_relative, data_prefix.is_empty()) {
                (false, _) => path.to_owned(),
                (true, true) => format!("{table_prefix}{path}"),
                (true, false) => format!("{data_prefix}/{table_prefix}{path}"),
            };
            Path::from(relative.as_str())
        };

        stream::iter(files.into_iter().map(|file| {
            let path = resolve(&file.path, file.path_is_relative);
            let store = Arc::clone(store);
            async move {
                let summary = data_file::file_summary(
                    data_file::ParquetFile::new(
                        store,
                        path,
                        file.file_size_bytes,
                        file.footer_size,
                    ),
                    table.get(),
                    file.id.get(),
                    file.row_id_start,
                    file.record_count,
                )
                .await;
                (file.id, summary)
            }
        }))
        .buffer_unordered(BACKFILL_FILE_READ_CONCURRENCY)
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
        row_ids: &[u64],
    ) -> Result<Vec<FileRowCandidate>> {
        let mut requested = row_ids.to_vec();
        requested.sort_unstable();
        requested.dedup();
        if requested.is_empty() {
            return Ok(Vec::new());
        }

        let snapshot = self.snapshot().await?;
        let mut candidates = Vec::new();

        if let Some(store) = &data_store {
            let table_prefix = snapshot.table_data_prefix(table)?;
            let files = snapshot.data_files_of(table);
            for (data_file_id, summary) in
                Self::file_summaries(store, data_prefix, &table_prefix, table, files).await
            {
                let rows = match summary {
                    Ok(summary) => summary.matching(&requested),
                    Err(error) => {
                        warn!(
                            table_id = table.get(),
                            data_file_id = data_file_id.get(),
                            %error,
                            "row location fell back to every requested row for this file"
                        );
                        requested.clone()
                    }
                };
                candidates.extend(rows.into_iter().map(|row_id| FileRowCandidate {
                    row_id,
                    data_file_id: Some(data_file_id),
                }));
            }
        }

        let mut placements: HashMap<u64, Vec<DataFileId>> = HashMap::new();
        for candidate in candidates {
            if let Some(data_file_id) = candidate.data_file_id {
                placements
                    .entry(candidate.row_id)
                    .or_default()
                    .push(data_file_id);
            }
        }
        // A row can be inlined and still hold an expired physical copy in a
        // current file, so the live inlined copy is its own candidate rather
        // than an alternative to the file ones.
        let mut inlined: HashSet<u64> = HashSet::new();
        for row in self.recent_rows(table).await? {
            if requested.binary_search(&row.row_id).is_ok() {
                inlined.insert(row.row_id);
            }
        }

        // Emit in the caller's order: a range or NULL scan hands its row ids
        // over already ordered, and locating them must not reorder them.
        let mut seen = HashSet::new();
        let mut located = Vec::new();
        for row_id in row_ids
            .iter()
            .copied()
            .filter(|row_id| seen.insert(*row_id))
        {
            if let Some(files) = placements.get_mut(&row_id) {
                files.sort_unstable();
                files.dedup();
                located.extend(files.iter().map(|data_file_id| FileRowCandidate {
                    row_id,
                    data_file_id: Some(*data_file_id),
                }));
            }
            // A live inlined row, and a row located nowhere at all, are both
            // reported without a file rather than dropped.
            if inlined.contains(&row_id) || !placements.contains_key(&row_id) {
                located.push(FileRowCandidate {
                    row_id,
                    data_file_id: None,
                });
            }
        }
        let candidates = located;

        Ok(candidates)
    }

    /// Builds and caches the summaries a lookup would otherwise build cold.
    ///
    /// Best-effort and idempotent: a file already resident, or answering
    /// from its dense range, costs nothing, and a file that cannot be read
    /// is counted rather than raised. Nothing here is durable, so a pass
    /// that never runs changes only latency.
    ///
    /// Intended to be spawned after a commit that lands compaction outputs
    /// — the files carrying embedded row ids, which are exactly the ones a
    /// lookup pays to read. It warms this process only; a separate reader
    /// builds its own.
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
        let table_prefix = snapshot.table_data_prefix(table)?;
        let files = snapshot.data_files_of(table);
        let mut warmth = RowSummaryWarmth {
            files_considered: u64::try_from(files.len()).unwrap_or(u64::MAX),
            ..RowSummaryWarmth::default()
        };

        for (data_file_id, summary) in
            Self::file_summaries(&data_store, data_prefix, &table_prefix, table, files).await
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
