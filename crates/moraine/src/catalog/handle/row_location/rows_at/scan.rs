//! Execution-time, projected reads of summary-resolved positions.

use std::sync::atomic::{AtomicU64, Ordering};

use futures::{TryStreamExt, stream::BoxStream};

use super::{
    Arc, Bytes, CatalogSnapshot, DataFileId, DataStore, Error, HashSet, InlineGroup, LocationScope,
    MissingRows, ReadColumn, ReadOnlyCatalog, Result, RowIdSource, RowPositions, ScopedRows,
    StreamExt, TableId, current_files_for, data_file, first_row_error, group_by_chunk,
    group_deduped_pairs, resolve_data_path, stream,
};

type FileSelection = (
    data_file::ParquetFile,
    DataFileId,
    RowPositions,
    Option<u64>,
);

fn requested_columns(
    snapshot: &CatalogSnapshot,
    table: TableId,
    columns: &[String],
) -> Result<Vec<usize>> {
    let definitions = snapshot.columns_of(table);
    columns
        .iter()
        .map(|name| {
            definitions
                .iter()
                .position(|column| column.parent_column.is_none() && column.name == *name)
                .ok_or_else(|| Error::Constraint(format!("unknown selective-read column {name}")))
        })
        .collect()
}

/// A selective reader emitting one projected Arrow IPC batch at a time.
/// Dropping the reader cancels its remaining reads.
pub struct LocatedRowScan {
    batches: BoxStream<'static, Result<Vec<u8>>>,
    files_read: Arc<AtomicU64>,
}

impl LocatedRowScan {
    /// Number of data files opened for payload reads, excluding summary and
    /// delete files.
    #[must_use]
    pub fn files_read(&self) -> u64 {
        self.files_read.load(Ordering::Relaxed)
    }

    /// Reads the next batch, or `None` at the end of the selection.
    ///
    /// # Errors
    /// Returns a data-store, Parquet decoding, or schema projection error.
    pub async fn next_batch(&mut self) -> Result<Option<Vec<u8>>> {
        self.batches.try_next().await
    }
}

impl ReadOnlyCatalog {
    /// Opens an exact selective reader for located pairs at `snapshot`.
    /// `columns` names top-level columns in output order; row and file IDs
    /// follow them. Duplicate pairs are emitted once; deleted rows and file
    /// candidates absent from a verified summary are omitted.
    ///
    /// Positioning and visibility are checked before opening the cursor.
    /// Data pages are decoded on demand, one file and record batch at a time.
    /// Use a pinned read scope when concurrent commits are possible.
    ///
    /// ```no_run
    /// # async fn read(catalog: &moraine::ReadOnlyCatalog, snapshot: &moraine::CatalogSnapshot,
    /// # store: moraine::DataStore, table: moraine::TableId, file: moraine::DataFileId)
    /// # -> Result<(), moraine::Error> {
    /// let mut scan = catalog
    ///     .scan_rows_at(
    ///         snapshot,
    ///         Some(store),
    ///         "",
    ///         table,
    ///         &[(42, Some(file))],
    ///         &["amount".into()],
    ///     )
    ///     .await?;
    /// while let Some(ipc_batch) = scan.next_batch().await? {
    ///     // Consume this projected batch before requesting the next one.
    ///     assert!(!ipc_batch.is_empty());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns a schema error for an unknown column, or the positioning and
    /// visibility errors documented by [`Self::rows_at`]. Partially visible
    /// historical files return [`Error::Unsupported`].
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_rows_at(
        &self,
        snapshot: &CatalogSnapshot,
        data_store: Option<DataStore>,
        data_prefix: &str,
        table: TableId,
        pairs: &[(u64, Option<DataFileId>)],
        columns: &[String],
    ) -> Result<LocatedRowScan> {
        let requested = requested_columns(snapshot, table, columns)?;
        let visible_at = snapshot.current_snapshot().id.get();
        let (by_file, null_rows) = group_deduped_pairs(pairs);
        let recent = self
            .requested_inline_recent_rows(table, &null_rows, Some(visible_at))
            .await?;
        let found: HashSet<u64> = recent.iter().map(|row| row.row_id).collect();
        if let Some(&row_id) = null_rows.iter().find(|row| !found.contains(row)) {
            return Err(Error::RowPosition {
                row_id,
                data_file_id: None,
                reason: "row is not a live inlined row".to_owned(),
            });
        }
        let mut inline = Vec::new();
        let session = self.begin_read().await?;
        for group in group_by_chunk(recent) {
            let resolved = snapshot
                .inline_read_columns(session.handle(), table, group.begin_snapshot)
                .await;
            match resolved {
                Ok(columns) => inline.push((group, columns)),
                Err(error) => {
                    session.finish();
                    return Err(error);
                }
            }
        }
        session.finish();

        let mut files = Vec::new();
        if !by_file.is_empty() {
            let table_prefix = snapshot.table_data_prefix(table)?;
            let requested_files = current_files_for(snapshot, table, &by_file)?;
            let store = data_store.ok_or_else(|| {
                first_row_error(
                    &by_file,
                    requested_files[0].id,
                    "no data store was supplied to read the file",
                )
            })?;
            let scope = LocationScope {
                store: &store,
                data_prefix,
                table_prefix: &table_prefix,
                table,
                snapshot,
            };
            let located = self
                .position_requested_files(&scope, by_file, requested_files, MissingRows::Omit)
                .await?;
            for read in self.file_reads(&scope, table, located, visible_at).await? {
                if read
                    .file
                    .partial_max
                    .is_some_and(|maximum| maximum.get() > visible_at)
                {
                    return Err(Error::Unsupported(
                        "selective reads of partially visible historical files".to_owned(),
                    ));
                }
                let path = resolve_data_path(
                    data_prefix,
                    &table_prefix,
                    &read.file.path,
                    read.file.path_is_relative,
                )?;
                let file = data_file::ParquetFile::new(
                    store.clone(),
                    path,
                    read.file.file_size_bytes,
                    read.file.footer_size,
                )
                .with_columns(read.columns)
                .with_metrics(self.data_read_metrics());
                files.push((
                    file,
                    read.data_file_id,
                    RowPositions::from_unsorted(read.positions),
                    read.file.row_id_start,
                ));
            }
        }

        files.sort_by_key(|(_, file_id, _, _)| *file_id);
        Ok(LocatedRowScan::new(
            inline,
            files,
            requested,
            columns.to_vec(),
        ))
    }
}

impl LocatedRowScan {
    fn new(
        inline: Vec<(InlineGroup, Vec<ReadColumn>)>,
        files: Vec<FileSelection>,
        requested: Vec<usize>,
        names: Vec<String>,
    ) -> Self {
        let files_read = Arc::new(AtomicU64::new(0));
        let opened = files_read.clone();
        let requested = Arc::new(requested);
        let names = Arc::new(names);
        let inline_requested = requested.clone();
        let inline_names = names.clone();
        let inline = stream::iter(inline).map(move |(group, columns)| {
            let schema =
                data_file::decode_inline_schema(Bytes::copy_from_slice(&group.arrow_schema))?;
            let batch = data_file::inline_rows_batch(
                schema,
                &Bytes::copy_from_slice(&group.chunk_body),
                &group.offsets,
                &group.row_ids,
                &columns,
                &inline_requested,
            )?;
            data_file::encode_located_batch(&batch, &inline_names, None)
        });
        let files = stream::iter(files)
            .then(move |(file, file_id, positions, row_id_start)| {
                let requested = requested.clone();
                let names = names.clone();
                opened.fetch_add(1, Ordering::Relaxed);
                async move {
                    let batches = data_file::scoped_read_row_stream(
                        file,
                        &requested,
                        ScopedRows::At(&positions),
                        RowIdSource::Resolve { row_id_start },
                    )
                    .await?;
                    Ok::<_, Error>(batches.map(move |batch| {
                        data_file::encode_located_batch(&batch?, &names, Some(file_id.get()))
                    }))
                }
            })
            .try_flatten();
        Self {
            batches: inline.chain(files).boxed(),
            files_read,
        }
    }
}
