//! Located rows read back whole at the snapshot the caller pins.

mod scan;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use futures::{StreamExt, stream};
pub use scan::LocatedRowScan;

use super::{
    LocatedDeletion, LocationScope, MissingRows, current_files_for, first_row_error,
    group_deduped_pairs,
};
use crate::{
    catalog::{
        CatalogSnapshot, DataFileId, DeleteFileId, ReadOnlyCatalog, RecentRow, TableId,
        resolve_data_path, snapshot::data_file_info,
    },
    data_file::{self, DataStore, ReadColumn, RowIdSource, RowPositions, ScopedRows},
    error::{Error, Result},
    store::inline as store_inline,
};

/// One inline chunk's requested rows, decoded together.
struct InlineGroup {
    arrow_schema: Arc<Vec<u8>>,
    chunk_body: Arc<Vec<u8>>,
    begin_snapshot: u64,
    offsets: Vec<u64>,
    row_ids: Vec<u64>,
}

/// One file read: the rows still live at the snapshot and the columns its
/// schema resolves to.
struct FileRead {
    data_file_id: DataFileId,
    file: crate::catalog::DataFileInfo,
    columns: Vec<ReadColumn>,
    positions: Vec<u64>,
}

impl ReadOnlyCatalog {
    /// The positions a registered delete file marks dead as of `visible_at`.
    async fn deleted_positions_at(
        &self,
        scope: &LocationScope<'_>,
        table: TableId,
        delete_file_id: DeleteFileId,
        visible_at: u64,
    ) -> Result<Vec<u64>> {
        let delete_file = scope
            .snapshot
            .delete_files_of(table)
            .into_iter()
            .find(|file| file.id == delete_file_id)
            .ok_or_else(|| {
                Error::Corruption(format!(
                    "delete file {delete_file_id} vanished from the snapshot"
                ))
            })?;
        let path = resolve_data_path(
            scope.data_prefix,
            scope.table_prefix,
            &delete_file.path,
            delete_file.path_is_relative,
        )?;
        let file = data_file::ParquetFile::new(
            scope.store.clone(),
            path,
            delete_file.file_size_bytes,
            delete_file.footer_size,
        )
        .with_metrics(self.data_read_metrics());
        data_file::delete_file_positions_at(file, visible_at).await
    }

    /// Narrows each located file to the positions still live at
    /// `visible_at` and resolves the columns its schema reads as.
    async fn file_reads(
        &self,
        scope: &LocationScope<'_>,
        table: TableId,
        located: Vec<LocatedDeletion>,
        visible_at: u64,
    ) -> Result<Vec<FileRead>> {
        let snapshot = scope.snapshot;
        let inlined_deletes = self.inlined_file_deletes_at(table, visible_at).await?;
        let session = self.begin_read().await?;
        let mut reads = Vec::with_capacity(located.len());
        for deletion in located {
            let resolved = async {
                let value = snapshot
                    .data_files
                    .get(&table.get())
                    .and_then(|files| files.get(&deletion.data_file_id.get()))
                    .ok_or_else(|| {
                        Error::Corruption(format!(
                            "data file {} vanished from the snapshot",
                            deletion.data_file_id
                        ))
                    })?;
                let columns = snapshot
                    .file_read_columns_at(session.handle(), table, value)
                    .await?;
                Ok::<_, Error>((value, columns))
            }
            .await;
            let (value, columns) = match resolved {
                Ok(resolved) => resolved,
                Err(error) => {
                    session.finish();
                    return Err(error);
                }
            };

            // A replaced delete file embeds per-position snapshots and keeps
            // the earliest begin, so only positions deleted at `visible_at`
            // count; the positions decoded while positioning are unfiltered.
            let mut deleted: HashSet<u64> = HashSet::new();
            if let Some(existing) = deletion.existing_delete {
                let read = self
                    .deleted_positions_at(scope, table, existing.delete_file_id, visible_at)
                    .await;
                match read {
                    Ok(positions) => deleted.extend(positions),
                    Err(error) => {
                        session.finish();
                        return Err(error);
                    }
                }
            }
            if let Some(positions) = inlined_deletes.get(&deletion.data_file_id.get()) {
                deleted.extend(positions);
            }
            let positions: Vec<u64> = deletion
                .positions
                .into_iter()
                .filter(|position| !deleted.contains(position))
                .collect();
            if positions.is_empty() {
                continue;
            }
            reads.push(FileRead {
                data_file_id: deletion.data_file_id,
                file: data_file_info(value),
                columns,
                positions,
            });
        }
        session.finish();

        Ok(reads)
    }

    /// Positions per data file deleted through inlined file deletions
    /// committed at or before `visible_at`.
    async fn inlined_file_deletes_at(
        &self,
        table: TableId,
        visible_at: u64,
    ) -> Result<HashMap<u64, HashSet<u64>>> {
        let session = self.begin_read().await?;
        let deletes = store_inline::scan_inline_file_deletes(session.handle(), table.get()).await;
        session.finish();

        let mut by_file: HashMap<u64, HashSet<u64>> = HashMap::new();
        for (data_file_id, position, value) in deletes? {
            if value.begin_snapshot <= visible_at {
                by_file.entry(data_file_id).or_default().insert(position);
            }
        }
        Ok(by_file)
    }
}

/// Groups rows by the chunk body they decode from, in first-seen order.
fn group_by_chunk(rows: Vec<RecentRow>) -> Vec<InlineGroup> {
    let mut groups: Vec<InlineGroup> = Vec::new();
    for row in rows {
        match groups
            .iter_mut()
            .find(|group| Arc::ptr_eq(&group.chunk_body, &row.chunk_body))
        {
            Some(group) => {
                group.offsets.push(row.offset_in_chunk);
                group.row_ids.push(row.row_id);
            }
            None => groups.push(InlineGroup {
                arrow_schema: row.arrow_schema,
                chunk_body: row.chunk_body,
                begin_snapshot: row.begin_snapshot.get(),
                offsets: vec![row.offset_in_chunk],
                row_ids: vec![row.row_id],
            }),
        }
    }
    groups
}
