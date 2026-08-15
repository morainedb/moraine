//! Locating stable row ids within one immutable data file.

use std::sync::Arc;

use crate::{
    data_file::{
        ParquetFile, RowIdSource, ScopedRows, carries_embedded_row_ids, row_set::FileRowSet,
        row_set_cache, scoped_read_recorded_entries,
    },
    error::Result,
};

/// One file's row-id membership, and what it cost to obtain.
pub(crate) struct FileSummary {
    rows: Arc<FileRowSet>,
    /// Whether this call read the file's row-id column and cached the
    /// result. A dense range and a cache hit both read nothing.
    pub(crate) built: bool,
}

impl FileSummary {
    /// Which of `requested` this file holds, in request order.
    pub(crate) fn matching(&self, requested: &[u64]) -> Vec<u64> {
        self.rows.matching(requested)
    }
}

/// This file's row-id membership, from the cache when it is resident.
///
/// A file whose ids are the dense range its catalog row records answers
/// without reading any column, and takes none of the summary budget. One
/// carrying the reserved row-id column reads only that column, since its
/// ids may hold gaps the range would exclude, and its summary is cached.
pub(crate) async fn file_summary(
    file: ParquetFile,
    table_id: u64,
    data_file_id: u64,
    row_id_start: Option<u64>,
    record_count: u64,
) -> Result<FileSummary> {
    let path = file.path.clone();
    let file_size = file.file_size;
    let store = Arc::clone(&file.object_store);
    let key = row_set_cache::SummaryKey {
        table_id,
        data_file_id,
        path: &path,
        file_size,
    };

    if let Some(rows) = row_set_cache::cached(&store, &key) {
        return Ok(FileSummary { rows, built: false });
    }

    // Dense range
    if let Some(start) = row_id_start
        && !carries_embedded_row_ids(file.clone()).await?
    {
        Ok(FileSummary {
            rows: Arc::new(FileRowSet::range(start, record_count)?),
            built: false,
        })
    } else {
        let entries = scoped_read_recorded_entries(
            file,
            &[],
            ScopedRows::All,
            RowIdSource::Resolve { row_id_start },
        )
        .await?;
        let mut row_ids: Vec<u64> = entries.into_iter().map(|entry| entry.row_id).collect();
        row_ids.sort_unstable();
        row_ids.dedup();

        let rows = Arc::new(FileRowSet::from_sorted(row_ids)?);
        row_set_cache::store(&store, &key, &rows);

        Ok(FileSummary { rows, built: true })
    }
}
