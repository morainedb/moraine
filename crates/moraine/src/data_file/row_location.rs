//! Locating stable row ids within one immutable data file.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use futures::TryStreamExt;

use crate::{
    data_file::{
        ParquetFile, RowIdSource, ScopedRows, auxiliary_cache, carries_embedded_row_ids,
        row_set::{FileRowSet, PositionedRowSet, RowOrder},
        scoped_read_entry_batches,
    },
    error::Result,
};

/// One file's row-id membership, and what it cost to obtain.
pub(crate) struct FileSummary {
    rows: Arc<PositionedRowSet>,
    /// Whether this call read the file's row-id column and cached the
    /// result.
    pub(crate) built: bool,
}

impl FileSummary {
    /// Which of `requested` this file holds, in request order.
    pub(crate) fn matching(&self, requested: &[u64]) -> Vec<u64> {
        self.rows.rows.matching(requested)
    }

    /// File positions of `requested` rows within this file; `None` for rows
    /// this file does not hold.
    pub(crate) fn positions_of(&self, requested: &[u64]) -> Vec<Option<u64>> {
        self.rows.positions_of(requested)
    }
}

/// This file's row-id membership, from the cache when it is resident. A
/// file whose ids are the recorded dense range answers without a read; one
/// carrying the reserved row-id column reads only that column and is cached.
pub(crate) async fn file_summary(
    file: ParquetFile,
    table_id: u64,
    data_file_id: u64,
    row_id_start: Option<u64>,
    record_count: u64,
) -> Result<FileSummary> {
    let path = file.path.clone();
    let file_size = file.file_size;
    let store = file.store.clone();
    let key = auxiliary_cache::FileSummaryKey {
        table_id,
        data_file_id,
        path: &path,
        file_size,
    };

    if let Some(rows) = auxiliary_cache::shared().summary(&store, &key).await {
        return Ok(FileSummary { rows, built: false });
    }

    if let Some(start) = row_id_start
        && !carries_embedded_row_ids(file.clone()).await?
    {
        Ok(FileSummary {
            rows: Arc::new(PositionedRowSet {
                rows: FileRowSet::range(start, record_count)?,
                order: RowOrder::Ascending,
            }),
            built: false,
        })
    } else {
        let built = Arc::new(AtomicBool::new(false));
        let read = {
            let built = Arc::clone(&built);
            move || async move {
                built.store(true, Ordering::Relaxed);
                read_row_ids(file, row_id_start).await
            }
        };

        let rows = auxiliary_cache::shared()
            .fetch_summary(&store, &key, read)
            .await?;

        Ok(FileSummary {
            rows,
            built: built.load(Ordering::Relaxed),
        })
    }
}

async fn read_row_ids(
    file: ParquetFile,
    row_id_start: Option<u64>,
) -> Result<Arc<PositionedRowSet>> {
    let row_ids_in_file_order = scoped_read_entry_batches(
        file,
        &[],
        ScopedRows::All,
        RowIdSource::Resolve { row_id_start },
    )
    .await?
    .try_fold(Vec::new(), |mut row_ids, batch| async move {
        row_ids.extend(batch.iter().map(|entry| entry.row_id));
        Ok(row_ids)
    })
    .await?;

    Ok(Arc::new(PositionedRowSet::from_file_order(
        row_ids_in_file_order,
    )?))
}
