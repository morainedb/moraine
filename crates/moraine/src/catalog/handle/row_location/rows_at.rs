//! Located rows read back whole at the snapshot the caller pins.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use futures::{StreamExt, stream};

use super::{
    LocatedDeletion, LocationScope, current_files_for, first_row_error, group_deduped_pairs,
};
use crate::{
    catalog::{
        CatalogSnapshot, DataFileId, DeleteFileId, ReadOnlyCatalog, RecentRow, TableId,
        handle::SUMMARY_READ_CONCURRENCY, resolve_data_path, snapshot::data_file_info,
    },
    data_file::{self, DataStore, ReadColumn, RowIdSource, RowPositions, ScopedRows},
    error::{Error, Result},
    store::inline as store_inline,
};

/// What [`ReadOnlyCatalog::rows_at`] read: one Arrow IPC stream per batch,
/// each carrying its own schema and one record batch. The columns are the
/// table's top-level columns at the snapshot, in catalog order and under
/// their current names, then `row_id` (`UInt64`) and `data_file_id`
/// (`UInt64`, NULL for an inlined row).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocatedRows {
    /// Self-describing IPC streams holding one record batch each.
    pub batches: Vec<Vec<u8>>,
}

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
    /// Reads located rows — `(row_id, data_file_id)` pairs, as a lookup
    /// returns them — back whole at `snapshot`, without a scan.
    ///
    /// File rows are read at their exact positions, only the pages holding
    /// them; inlined rows decode from their chunk. Every column is matched
    /// by field id, so a file written before a column was added reads NULL
    /// for it. A row deleted at the snapshot is omitted. Positioning is
    /// exact-or-failed as [`Self::locate_row_positions_at`] is; a `None`
    /// file id must name an inlined row live at the snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist, a store
    /// error if a file or chunk cannot be read, or [`Error::RowPosition`]
    /// for the first pair that cannot be positioned exactly.
    pub async fn rows_at(
        &self,
        snapshot: &CatalogSnapshot,
        data_store: Option<DataStore>,
        data_prefix: &str,
        table: TableId,
        pairs: &[(u64, Option<DataFileId>)],
    ) -> Result<LocatedRows> {
        if pairs.is_empty() {
            return Ok(LocatedRows::default());
        }

        let visible_at = snapshot.current_snapshot().id.get();
        let (by_file, null_rows) = group_deduped_pairs(pairs);
        let columns = snapshot.columns_of(table);
        let (requested, names): (Vec<usize>, Vec<String>) = columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.parent_column.is_none())
            .map(|(index, column)| (index, column.name.clone()))
            .unzip();

        let mut batches = self
            .inline_rows_at(snapshot, table, &null_rows, visible_at, &requested, &names)
            .await?;
        if by_file.is_empty() {
            return Ok(LocatedRows { batches });
        }

        let table_prefix = snapshot.table_data_prefix(table)?;
        let requested_files = current_files_for(snapshot, table, &by_file)?;
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
            snapshot,
        };
        let located = self
            .position_requested_files(&scope, by_file, requested_files)
            .await?;
        let reads = self.file_reads(&scope, table, located, visible_at).await?;
        let file_batches = self
            .read_file_batches(&scope, reads, &requested, &names)
            .await?;
        batches.extend(file_batches);

        Ok(LocatedRows { batches })
    }

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

    /// Reads every file concurrently and encodes its batches, ordered by
    /// file id so the answer does not vary run to run.
    async fn read_file_batches(
        &self,
        scope: &LocationScope<'_>,
        reads: Vec<FileRead>,
        requested: &[usize],
        names: &[String],
    ) -> Result<Vec<Vec<u8>>> {
        let mut file_batches = stream::iter(reads.into_iter().map(|read| {
            let store = scope.store.clone();
            let metrics = self.data_read_metrics();
            async move {
                let path = resolve_data_path(
                    scope.data_prefix,
                    scope.table_prefix,
                    &read.file.path,
                    read.file.path_is_relative,
                )?;
                let file = data_file::ParquetFile::new(
                    store,
                    path,
                    read.file.file_size_bytes,
                    read.file.footer_size,
                )
                .with_columns(read.columns)
                .with_metrics(metrics);
                let positions = RowPositions::from_unsorted(read.positions);
                let batches = data_file::scoped_read_row_batches(
                    file,
                    requested,
                    ScopedRows::At(&positions),
                    RowIdSource::Resolve {
                        row_id_start: read.file.row_id_start,
                    },
                )
                .await?;
                let encoded = batches
                    .iter()
                    .map(|batch| {
                        data_file::encode_located_batch(batch, names, Some(read.data_file_id.get()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok::<_, Error>((read.data_file_id, encoded))
            }
        }))
        .buffer_unordered(SUMMARY_READ_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        file_batches.sort_by_key(|(data_file_id, _)| *data_file_id);

        Ok(file_batches
            .into_iter()
            .flat_map(|(_, encoded)| encoded)
            .collect())
    }

    /// The requested inlined rows, decoded chunk by chunk; every row must be
    /// live at `visible_at`.
    async fn inline_rows_at(
        &self,
        snapshot: &CatalogSnapshot,
        table: TableId,
        rows: &[u64],
        visible_at: u64,
        requested: &[usize],
        names: &[String],
    ) -> Result<Vec<Vec<u8>>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let recent = self
            .requested_inline_recent_rows(table, rows, Some(visible_at))
            .await?;
        let found: HashSet<u64> = recent.iter().map(|row| row.row_id).collect();
        if let Some(&row_id) = rows.iter().find(|row| !found.contains(row)) {
            return Err(Error::RowPosition {
                row_id,
                data_file_id: None,
                reason: "row is not a live inlined row".to_owned(),
            });
        }

        let session = self.begin_read().await?;
        let mut batches = Vec::new();
        for group in group_by_chunk(recent) {
            let columns = snapshot
                .inline_read_columns(session.handle(), table, group.begin_snapshot)
                .await;
            let batch = columns.and_then(|columns| {
                let schema =
                    data_file::decode_inline_schema(Bytes::copy_from_slice(&group.arrow_schema))?;
                let batch = data_file::inline_rows_batch(
                    schema,
                    &Bytes::copy_from_slice(&group.chunk_body),
                    &group.offsets,
                    &group.row_ids,
                    &columns,
                    requested,
                )?;
                data_file::encode_located_batch(&batch, names, None)
            });
            match batch {
                Ok(batch) => batches.push(batch),
                Err(error) => {
                    session.finish();
                    return Err(error);
                }
            }
        }
        session.finish();

        Ok(batches)
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
