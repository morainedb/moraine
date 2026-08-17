//! Scoped Parquet and inline index backfills.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use futures::{Stream, StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use tokio::sync::Semaphore;

use super::{BACKFILL_FILE_READ_CONCURRENCY, ReadOnlyCatalog};
use crate::{
    catalog::{
        ColumnId, DataFileInfo, DeleteFileInfo, FileIndexEntry, IndexEntry, IndexId, TableId,
    },
    data_file::{self, DataStore},
    error::{Error, Result},
    store::{
        handle::ReadHandle, inline as store_inline, key::InlineOperation, proto::InlineSchemaValue,
    },
};

/// Keeps `guard` alive for exactly as long as `stream` is: it drops when the
/// stream is exhausted or dropped, never before.
fn stream_holding<S, G>(stream: S, guard: G) -> impl Stream<Item = S::Item>
where
    S: Stream + Unpin,
    G: Send,
{
    stream::unfold((stream, guard), |(mut stream, guard)| async move {
        let item = stream.next().await?;
        Some((item, (stream, guard)))
    })
}

async fn collect_immediate_backfill<'a>(
    files: impl Iterator<Item = DataFileInfo> + 'a,
    object_store: DataStore,
    metrics: Arc<data_file::ScopedReadMetrics>,
    positions: &[usize],
    resolve: impl Fn(&str, bool) -> Path,
    killed_positions: &'a HashMap<u64, HashSet<u64>>,
    killed_row_ids: &'a HashMap<u64, HashSet<u64>>,
) -> Result<Vec<IndexEntry>> {
    // Opening a file and draining the batches behind it are separate stages
    // of the pipeline below. One permit spans both, so the window bounds the
    // files being read rather than each stage holding a window of its own.
    let window = Arc::new(Semaphore::new(BACKFILL_FILE_READ_CONCURRENCY));

    stream::iter(files)
        .map(move |file| {
            let object_store = object_store.clone();
            let metrics = Arc::clone(&metrics);
            let window = Arc::clone(&window);
            let path = resolve(&file.path, file.path_is_relative);
            let dead_positions = killed_positions.get(&file.id.get());
            let dead_row_ids = killed_row_ids.get(&file.id.get());
            async move {
                let permit = window.acquire_owned().await.map_err(|_| {
                    Error::Interrupted(
                        "backfill read limiter stopped before the file was read".to_owned(),
                    )
                })?;

                let batches = data_file::scoped_read_entry_batches(
                    data_file::ParquetFile::new(
                        object_store,
                        path,
                        file.file_size_bytes,
                        file.footer_size,
                    )
                    .with_metrics(metrics),
                    positions,
                    data_file::ScopedRows::All,
                    data_file::RowIdSource::Resolve {
                        row_id_start: file.row_id_start,
                    },
                )
                .await?;

                let entries = batches.map(move |batch| {
                    let batch = batch?;
                    batch
                        .into_iter()
                        .map(|entry| {
                            let dead = dead_positions
                                .is_some_and(|positions| positions.contains(&entry.ordinal))
                                || dead_row_ids.is_some_and(|rows| rows.contains(&entry.row_id));
                            Ok((!dead).then_some(IndexEntry {
                                row_id: entry.row_id,
                                values: entry.values,
                            }))
                        })
                        .filter_map(Result::transpose)
                        .collect::<Result<Vec<_>>>()
                });
                let entries = stream_holding(entries, permit).boxed();

                Ok::<_, Error>(entries)
            }
        })
        .buffer_unordered(BACKFILL_FILE_READ_CONCURRENCY)
        .try_flatten_unordered(BACKFILL_FILE_READ_CONCURRENCY)
        .try_fold(Vec::new(), |mut entries, batch| async move {
            entries.extend(batch);
            Ok(entries)
        })
        .await
}

pub(super) async fn collect_inline_delete_positions(
    session_handle: ReadHandle<'_>,
    table_id: u64,
) -> Result<HashMap<u64, HashSet<u64>>> {
    let killed = store_inline::scan_inline_file_deletes(session_handle, table_id)
        .await?
        .into_iter()
        .fold(
            HashMap::<u64, HashSet<u64>>::new(),
            |mut killed, (data_file_id, row_id, _)| {
                killed.entry(data_file_id).or_default().insert(row_id);
                killed
            },
        );

    Ok(killed)
}

/// The recorded inline schema of each distinct version in `schema_versions`,
/// fetched concurrently.
pub(super) async fn read_inline_schemas(
    session_handle: ReadHandle<'_>,
    table: TableId,
    schema_versions: impl IntoIterator<Item = u64>,
) -> Result<HashMap<u64, InlineSchemaValue>> {
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

pub(super) async fn collect_delete_positions<'a>(
    files: impl Iterator<Item = DeleteFileInfo> + 'a,
    object_store: DataStore,
    metrics: Arc<data_file::ScopedReadMetrics>,
    resolve: &'a impl Fn(&str, bool) -> Path,
) -> Result<HashMap<u64, HashSet<u64>>> {
    stream::iter(files.map(|file| {
        let path = resolve(&file.path, file.path_is_relative);
        let object_store = object_store.clone();
        let metrics = Arc::clone(&metrics);
        async move {
            let positions = data_file::delete_file_positions(
                data_file::ParquetFile::new(
                    object_store,
                    path,
                    file.file_size_bytes,
                    file.footer_size,
                )
                .with_metrics(metrics),
            )
            .await?;
            Ok::<_, Error>((file.data_file_id.get(), positions))
        }
    }))
    .buffer_unordered(BACKFILL_FILE_READ_CONCURRENCY)
    .try_fold(
        HashMap::<u64, HashSet<u64>>::new(),
        |mut killed, (data_file_id, positions)| async move {
            killed.entry(data_file_id).or_default().extend(positions);
            Ok(killed)
        },
    )
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
                .with_metrics(self.data_read_metrics()),
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
    /// Indexed columns are located by resolving each field id to its physical
    /// position (the file's columns follow the table's column order).
    ///
    /// Row ids resolve per file: the embedded row-id column when the file
    /// carries one (rewrite and flush output), else `row_id_start +
    /// ordinal`. Rows already dead — named by a delete file's positions or an
    /// inline file-delete's row ids — are excluded, so entries stay live-only.
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

        let snapshot = self.head_view(session.handle()).await?;
        let positions = snapshot.column_positions(table, columns)?;

        let table_prefix = snapshot.table_data_prefix(table)?;
        let resolve = |path: &str, is_relative: bool| {
            let relative = match (is_relative, data_prefix.is_empty()) {
                (false, _) => path.to_owned(),
                (true, true) => format!("{table_prefix}{path}"),
                (true, false) => format!("{data_prefix}/{table_prefix}{path}"),
            };
            object_store::path::Path::from(relative.as_str())
        };

        // Entries are live-only: delete files name positions within their
        // target, inline file-deletes name row ids.
        let inline_deletes = collect_inline_delete_positions(session.handle(), table.get());
        let metrics = self.data_read_metrics();
        let delete_files = collect_delete_positions(
            snapshot.delete_files_of(table).into_iter(),
            object_store.clone(),
            Arc::clone(&metrics),
            &resolve,
        );
        let (killed_row_ids, killed_positions) = futures::try_join!(inline_deletes, delete_files)?;

        let outcome = collect_immediate_backfill(
            snapshot.data_files_of(table).into_iter(),
            object_store,
            metrics,
            &positions,
            resolve,
            &killed_positions,
            &killed_row_ids,
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

        let snapshot = self.head_view(session.handle()).await?;
        let positions = snapshot.column_positions(table, columns)?;

        // A tombstone ends only versions begun before it. UPDATE can
        // reinsert the same row id in the tombstone's snapshot, and that
        // newer value is live and must be indexed.
        let (dead, chunks) = futures::try_join!(
            async {
                Ok::<_, Error>(
                    store_inline::scan_inline_deletes(session.handle(), table.get())
                        .await?
                        .into_iter()
                        .map(|(row_id, deletion)| (row_id, deletion.end_snapshot))
                        .collect::<HashMap<u64, u64>>(),
                )
            },
            store_inline::scan_inline_chunks(session.handle(), table.get()),
        )?;
        let schema_versions = chunks.iter().filter_map(|(operation, _)| match operation {
            InlineOperation::Insert { schema_version, .. } => Some(*schema_version),
            _ => None,
        });
        let schemas = read_inline_schemas(session.handle(), table, schema_versions)
            .await?
            .into_iter()
            .map(|(schema_version, record)| {
                let schema = data_file::decode_inline_schema(record.arrow_schema)?;
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
        session.finish();

        Ok(entries)
    }
}
