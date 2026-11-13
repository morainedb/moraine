//! Scoped Parquet and inline index backfills.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;

use super::{
    BACKFILL_FILE_READ_CONCURRENCY, ReadOnlyCatalog,
    table_read::{TableRead, load_table},
};
use crate::{
    catalog::{ColumnId, FileIndexEntry, IndexEntry, IndexId, TableId, resolve_data_path},
    data_file::{self, DataStore},
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        inline as store_inline,
        key::InlineOperation,
        read::{EntityCursor, EntityRecord, EntityRecordKind, Versions},
    },
    transaction::file_targets::DeleteCursor,
};

#[derive(Clone, Copy)]
pub(super) struct BackfillSource<'a> {
    pub(super) snapshot: &'a crate::catalog::CatalogSnapshot,
    pub(super) table: TableId,
    pub(super) handle: ReadHandle<'a>,
}

async fn collect_immediate_backfill(
    object_store: DataStore,
    metrics: Arc<data_file::ScopedReadMetrics>,
    positions: &[usize],
    source: BackfillSource<'_>,
    resolve: impl Fn(&str, bool) -> Result<Path>,
) -> Result<Vec<IndexEntry>> {
    let cursor = EntityCursor::open(
        source.handle,
        EntityRecordKind::File,
        Versions::Live,
        Some(source.table.get()),
        crate::read_policy::CacheAdmission::ReuseOnly,
    )
    .await?;
    let files = stream::try_unfold(cursor, |mut cursor| async move {
        while let Some(record) = cursor.next().await? {
            if let EntityRecord::File(file) = record.value() {
                let file = file.clone();
                return Ok(Some((record.map(|_| file), cursor)));
            }
        }
        Ok(None)
    });
    let resolve = &resolve;
    files
        .map_ok(|file| {
            let object_store = object_store.clone();
            let metrics = Arc::clone(&metrics);
            async move {
                let columns = source
                    .snapshot
                    .file_read_columns_at(source.handle, source.table, file.value())
                    .await?;
                let dead = dead_positions(
                    source.handle,
                    source.table,
                    file.data_file_id,
                    &object_store,
                    Arc::clone(&metrics),
                    resolve,
                )
                .await?;
                let dead: data_file::RowPositions = dead.into_iter().collect();
                data_file::scoped_read_entry_batches(
                    data_file::ParquetFile::new(
                        object_store,
                        resolve(&file.path, file.path_is_relative)?,
                        file.file_size_bytes,
                        file.footer_size,
                    )
                    .with_columns(columns)
                    .with_metrics(metrics)
                    .with_range_admission(crate::read_policy::CacheAdmission::ReuseOnly),
                    positions,
                    data_file::ScopedRows::All,
                    data_file::RowIdSource::Resolve {
                        row_id_start: file.row_id_start,
                    },
                )
                .await?
                .try_fold(Vec::new(), move |mut entries, batch| {
                    let dead = dead.clone();
                    async move {
                        entries.extend(batch.into_iter().filter_map(|entry| {
                            (!dead.contains(entry.ordinal)).then_some(IndexEntry {
                                row_id: entry.row_id,
                                values: entry.values,
                            })
                        }));
                        Ok(entries)
                    }
                })
                .await
            }
        })
        .try_buffer_unordered(BACKFILL_FILE_READ_CONCURRENCY)
        .try_fold(Vec::new(), |mut entries, file_entries| async move {
            entries.extend(file_entries);
            Ok(entries)
        })
        .await
}

/// Reads only deletion records targeting this file, at the derivation read
/// point.
pub(super) async fn dead_positions(
    handle: ReadHandle<'_>,
    table: TableId,
    file: u64,
    object_store: &DataStore,
    metrics: Arc<data_file::ScopedReadMetrics>,
    resolve: &impl Fn(&str, bool) -> Result<Path>,
) -> Result<HashSet<u64>> {
    let inline = store_inline::stream::file_delete_positions(handle, table.get(), file);
    let external = async {
        let mut cursor = DeleteCursor::open(handle, table.get(), file).await?;
        let mut positions = Vec::new();
        while let Some((deletion, _memory)) = cursor.next().await? {
            positions.extend(
                data_file::delete_file_positions(
                    data_file::ParquetFile::new(
                        object_store.clone(),
                        resolve(&deletion.path, deletion.path_is_relative)?,
                        deletion.file_size_bytes,
                        deletion.footer_size,
                    )
                    .with_metrics(Arc::clone(&metrics)),
                )
                .await?,
            );
        }
        Ok::<_, Error>(positions)
    };
    let (mut positions, external) = futures::try_join!(inline, external)?;
    positions.extend(external);
    Ok(positions)
}

/// The recorded inline schema of each distinct version in `schema_versions`,
/// fetched concurrently.
pub(super) async fn read_inline_schemas(
    session_handle: ReadHandle<'_>,
    table: TableId,
    schema_versions: impl IntoIterator<Item = u64>,
) -> Result<HashMap<u64, Bytes>> {
    let schema_versions: BTreeSet<u64> = schema_versions.into_iter().collect();

    stream::iter(
        schema_versions
            .into_iter()
            .map(|schema_version| async move {
                let record =
                    store_inline::read_inline_schema(session_handle, table.get(), schema_version)
                        .await?
                        .ok_or_else(|| {
                            Error::Corruption(format!(
                                "no inline schema for table {table} version {schema_version}"
                            ))
                        })?;
                Ok::<_, Error>((schema_version, record))
            }),
    )
    .buffer_unordered(BACKFILL_FILE_READ_CONCURRENCY)
    .try_collect()
    .await
}

impl ReadOnlyCatalog {
    /// Derives the index entries for a file the extension path registers, by
    /// scoped-reading it. The caller resolves each of the index's columns to
    /// its physical position in the file and passes them in the index's
    /// column order. `file_size` and `footer_size` are DuckLake's recorded
    /// values. The returned entries feed
    /// [`crate::Transaction::register_data_file`].
    ///
    /// A file carrying an embedded row-id column is refused: its rows
    /// already have ids.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corruption`] if the file cannot be read or a column
    /// type does not match its Parquet type, or [`Error::Constraint`] for a
    /// non-indexable column type or a file carrying an embedded row-id
    /// column.
    pub async fn scoped_file_index_entries(
        &self,
        object_store: DataStore,
        path: &Path,
        file_size: u64,
        footer_size: u64,
        index: IndexId,
        indexed_positions: &[usize],
    ) -> Result<Vec<FileIndexEntry>> {
        let entries = data_file::scoped_read_recorded_entries(
            data_file::ParquetFile::new(object_store, path.clone(), file_size, footer_size)
                .with_metrics(self.data_read_metrics())
                .with_range_admission(crate::read_policy::CacheAdmission::ReuseOnly),
            indexed_positions,
            data_file::ScopedRows::All,
            data_file::RowIdSource::Ordinal,
        )
        .await?
        .into_iter()
        .map(|entry| FileIndexEntry {
            index,
            ordinal: entry.row_id,
            values: entry.values,
        })
        .collect();

        Ok(entries)
    }

    /// Backfills an index over a table's live data by scoped-reading every
    /// live file from `object_store` (the `DATA_PATH` store) and deriving one
    /// entry per row — the extension-path build for a table that already
    /// holds data. The returned entries feed `create_index`'s backfill.
    /// Each file resolves indexed field ids through its recorded mapping;
    /// missing columns use their initial defaults.
    ///
    /// Row ids resolve per file: the embedded row-id column when the file
    /// carries one (rewrite and flush output), else `row_id_start +
    /// ordinal`. Rows already dead — named by a delete file's positions or
    /// by an inline file-delete, both physical — are excluded, so entries
    /// stay live-only.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or a column is not live,
    /// [`Error::Constraint`] for a non-indexable type, or
    /// [`Error::Corruption`] if a file cannot be read or names no row-id
    /// source.
    pub async fn scoped_backfill_entries(
        &self,
        object_store: DataStore,
        data_prefix: &str,
        table: TableId,
        columns: &[ColumnId],
    ) -> Result<Vec<IndexEntry>> {
        let session = self.begin_read().await?;

        let snapshot = load_table(session.handle(), table, TableRead::Definition).await?;
        let positions = snapshot.column_positions(table, columns)?;

        let table_prefix = snapshot.table_data_prefix(table)?;
        let resolve = |path: &str, is_relative: bool| {
            resolve_data_path(data_prefix, &table_prefix, path, is_relative)
        };

        let metrics = self.data_read_metrics();

        let outcome = collect_immediate_backfill(
            object_store,
            metrics,
            &positions,
            BackfillSource {
                snapshot: &snapshot,
                table,
                handle: session.handle(),
            },
            resolve,
        )
        .await;
        session.finish();

        outcome
    }

    /// Backfill entries for a table's live **inline** rows, by scanning its
    /// inline chunks — the counterpart to [`Self::scoped_backfill_entries`]
    /// for rows moraine holds in the store rather than external files.
    /// Tombstoned (inline-deleted) rows are excluded; a NULL indexed value
    /// yields a `None`, so `IS NULL` finds the row. Reads the catalog store,
    /// so it needs no data object store.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if a column is not live, or [`Error::Corruption`]
    /// if a chunk names no recorded schema or cannot be decoded.
    pub async fn inline_backfill_entries(
        &self,
        table: TableId,
        columns: &[ColumnId],
    ) -> Result<Vec<IndexEntry>> {
        let session = self.begin_read().await?;

        let snapshot = load_table(session.handle(), table, TableRead::Definition).await?;
        let entries = inline_backfill_entries_at(
            BackfillSource {
                snapshot: &snapshot,
                table,
                handle: session.handle(),
            },
            columns,
        )
        .await?;
        session.finish();

        Ok(entries)
    }
}

/// Derives inline entries from the same read point as a build's file sources.
pub(super) async fn inline_backfill_entries_at(
    source: BackfillSource<'_>,
    columns: &[ColumnId],
) -> Result<Vec<IndexEntry>> {
    let BackfillSource {
        snapshot,
        table,
        handle,
    } = source;
    let positions = snapshot.column_positions(table, columns)?;

    // A tombstone ends only versions begun before it. UPDATE can
    // reinsert the same row id in the tombstone's snapshot, and that
    // newer value is live and must be indexed.
    let (dead, chunks) = futures::try_join!(
        async {
            Ok::<_, Error>(
                store_inline::scan_inline_deletes(handle, table.get())
                    .await?
                    .into_iter()
                    .map(|(row_id, deletion)| (row_id, deletion.end_snapshot))
                    .collect::<HashMap<u64, u64>>(),
            )
        },
        store_inline::scan_inline_chunks(handle, table.get()),
    )?;
    let schema_versions = chunks.iter().filter_map(|(operation, _)| match operation {
        InlineOperation::Insert { schema_version, .. } => Some(*schema_version),
        _ => None,
    });
    let schemas = read_inline_schemas(handle, table, schema_versions)
        .await?
        .into_iter()
        .map(|(schema_version, record)| {
            let schema = data_file::decode_inline_schema(record)?;
            Ok::<_, Error>((schema_version, schema))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    let mut entries = Vec::new();
    for (op, chunk) in chunks {
        let InlineOperation::Insert {
            schema_version,
            begin_snapshot,
            ..
        } = op
        else {
            continue;
        };
        let schema = schemas.get(&schema_version).cloned().ok_or_else(|| {
            Error::Corruption(format!(
                "no inline schema for table {table} version {schema_version}"
            ))
        })?;

        let scoped = data_file::inline_batch_entries(
            schema,
            &chunk.body,
            &positions,
            chunk.row_id_start,
            Some(
                &snapshot
                    .inline_read_columns(handle, table, begin_snapshot)
                    .await?,
            ),
        )?;
        entries.extend(
            scoped
                .into_iter()
                .filter(|entry| {
                    dead.get(&entry.row_id)
                        .is_none_or(|end_snapshot| begin_snapshot >= *end_snapshot)
                })
                .map(|entry| IndexEntry {
                    row_id: entry.row_id,
                    values: entry.values,
                }),
        );
    }
    Ok(entries)
}
