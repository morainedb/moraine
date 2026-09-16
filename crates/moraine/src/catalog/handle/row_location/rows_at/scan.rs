//! Execution-time, projected reads of summary-resolved positions.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use arrow::array::RecordBatch;
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

/// A selective reader emitting one projected Arrow batch at a time.
/// Dropping the reader cancels its remaining reads.
pub struct LocatedRowScan {
    batches: BoxStream<'static, Result<RecordBatch>>,
    files_read: Arc<AtomicU64>,
    coverage_files: Vec<FileSelection>,
    requested: Vec<usize>,
    read_metrics: Arc<data_file::ScopedReadMetrics>,
    parallelism: Arc<AtomicUsize>,
    workers: Arc<data_file::ReadWorkers>,
    started: bool,
}

impl LocatedRowScan {
    /// Bounds concurrently prefetched read units by this value and the shared
    /// worker ceiling. Defaults to one; each worker queues at most one
    /// batch.
    ///
    /// # Errors
    /// Returns a constraint error if iteration has already started.
    pub fn set_parallelism(&mut self, maximum: usize) -> Result<()> {
        if self.started {
            return Err(Error::Constraint(
                "scan parallelism changed after iteration started".into(),
            ));
        }
        self.parallelism.store(
            maximum.clamp(1, data_file::read_worker_limit()),
            Ordering::Relaxed,
        );
        Ok(())
    }

    /// Maximum simultaneous prefetch workers for this cursor; zero for serial
    /// reads.
    #[must_use]
    pub fn peak_workers(&self) -> usize {
        self.workers.peak()
    }

    /// Estimates whether selected projected pages cost less than an ordinary
    /// scan. Reads cached footers/page indexes, never payload columns.
    ///
    /// # Errors
    /// Returns a metadata read or schema projection error.
    pub async fn prefers_selective_reads(&self) -> Result<bool> {
        let mut coverage = data_file::ReadCoverage::default();
        for (file, _, positions, row_id_start) in &self.coverage_files {
            coverage.add(
                data_file::read_coverage(file, &self.requested, positions, *row_id_start).await?,
            );
        }
        Ok(coverage.selective())
    }

    /// Payload and metadata bytes fetched from the data store by this cursor.
    #[must_use]
    pub fn bytes_read(&self) -> u64 {
        self.read_metrics.tally().range_bytes
    }

    /// Data-store ranges fetched by this cursor, before store-side coalescing.
    #[must_use]
    pub fn ranges_read(&self) -> u64 {
        self.read_metrics.tally().ranges
    }

    /// Time actively polling Parquet batches, including decoding and cache
    /// access but excluding asynchronous I/O waits; summed across workers.
    #[must_use]
    pub fn decode_seconds(&self) -> f64 {
        self.read_metrics.tally().decode_duration.as_secs_f64()
    }

    /// Data-store range-fetch elapsed time, summed across workers.
    #[must_use]
    pub fn fetch_seconds(&self) -> f64 {
        self.read_metrics.tally().range_duration.as_secs_f64()
    }
    /// Number of data files opened for payload reads, excluding summary and
    /// delete files.
    #[must_use]
    pub fn files_read(&self) -> u64 {
        self.files_read.load(Ordering::Relaxed)
    }

    /// Reads the next Arrow batch without IPC serialization. Its buffers remain
    /// valid after the cursor is dropped.
    ///
    /// # Errors
    /// Returns a data-store, Parquet decoding, or schema projection error.
    pub async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.started = true;
        self.batches.try_next().await
    }
}

impl ReadOnlyCatalog {
    /// Opens a whole-row cursor at `snapshot`, rejecting unpositionable pairs.
    /// Columns follow catalog order, then `row_id` and `data_file_id` (NULL for
    /// inline rows). Deleted file rows are omitted; inline pairs must be live.
    ///
    /// # Errors
    /// Returns a schema or store error, or [`Error::RowPosition`] for a pair
    /// that cannot be positioned exactly.
    pub async fn scan_rows_at_strict(
        &self,
        snapshot: &CatalogSnapshot,
        data_store: Option<DataStore>,
        data_prefix: &str,
        table: TableId,
        pairs: &[(u64, Option<DataFileId>)],
    ) -> Result<LocatedRowScan> {
        let columns = snapshot
            .columns_of(table)
            .iter()
            .filter(|column| column.parent_column.is_none())
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        self.scan_rows_at_with_mode(
            snapshot,
            data_store,
            data_prefix,
            table,
            pairs,
            &columns,
            MissingRows::Reject,
        )
        .await
    }

    /// Opens an exact selective reader for located pairs at `snapshot`.
    /// `columns` names top-level columns in output order; row and file IDs
    /// follow them. Duplicate pairs are emitted once; deleted rows and file
    /// candidates absent from a verified summary are omitted.
    ///
    /// Positioning and visibility are checked before opening the cursor.
    /// Data pages are decoded on demand, with bounded file/row-group prefetch.
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
    /// while let Some(batch) = scan.next_record_batch().await? {
    ///     // Consume this projected batch before requesting the next one.
    ///     assert!(batch.num_rows() > 0);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns a schema error for an unknown column, or the positioning and
    /// visibility errors documented by [`Self::scan_rows_at_strict`]. Partially
    /// visible historical files return [`Error::Unsupported`].
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
        self.scan_rows_at_with_mode(
            snapshot,
            data_store,
            data_prefix,
            table,
            pairs,
            columns,
            MissingRows::Omit,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn scan_rows_at_with_mode(
        &self,
        snapshot: &CatalogSnapshot,
        data_store: Option<DataStore>,
        data_prefix: &str,
        table: TableId,
        pairs: &[(u64, Option<DataFileId>)],
        columns: &[String],
        missing: MissingRows,
    ) -> Result<LocatedRowScan> {
        let requested = requested_columns(snapshot, table, columns)?;
        let read_metrics = self.data_read_metrics();
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
                .position_requested_files(&scope, by_file, requested_files, missing)
                .await?;
            for read in self.file_reads(&scope, table, located, visible_at).await? {
                if matches!(missing, MissingRows::Omit)
                    && read
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
                .with_metrics(read_metrics.clone());
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
            read_metrics,
        ))
    }
}

impl LocatedRowScan {
    fn new(
        inline: Vec<(InlineGroup, Vec<ReadColumn>)>,
        files: Vec<FileSelection>,
        requested: Vec<usize>,
        names: Vec<String>,
        read_metrics: Arc<data_file::ScopedReadMetrics>,
    ) -> Self {
        let coverage_files = files.clone();
        let coverage_requested = requested.clone();
        let parallelism = Arc::new(AtomicUsize::new(1));
        let read_parallelism = parallelism.clone();
        let workers = Arc::new(data_file::ReadWorkers::default());
        let read_workers = workers.clone();
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
            data_file::located_batch(&batch, &inline_names, None)
        });
        let files = stream::once(async move {
            let parallelism = read_parallelism.load(Ordering::Relaxed);
            let mut units = Vec::new();
            for (file, file_id, positions, row_id_start) in files {
                let started = Arc::new(AtomicBool::new(false));
                let groups = if parallelism > 1 && positions.as_slice().len() >= 1024 {
                    data_file::row_group_selections(&file, &positions).await?
                } else {
                    vec![positions]
                };
                for positions in groups {
                    units.push((
                        file.clone(),
                        file_id,
                        positions,
                        row_id_start,
                        started.clone(),
                    ));
                }
            }
            let parallelism = parallelism.min(units.len().max(1));
            Ok::<_, Error>(
                stream::iter(units)
                    .map(move |(file, file_id, positions, row_id_start, started)| {
                        let requested = requested.clone();
                        let names = names.clone();
                        let opened = opened.clone();
                        let workers = read_workers.clone();
                        stream::once(async move {
                            if !started.swap(true, Ordering::Relaxed) {
                                opened.fetch_add(1, Ordering::Relaxed);
                            }
                            let source = RowIdSource::Resolve { row_id_start };
                            let batches = if parallelism > 1 {
                                data_file::prefetched_row_stream(
                                    file, requested, positions, source, workers,
                                )
                            } else {
                                data_file::scoped_read_row_stream(
                                    file,
                                    &requested,
                                    ScopedRows::At(&positions),
                                    source,
                                )
                                .await?
                            };
                            Ok::<_, Error>(batches.map(move |batch| {
                                data_file::located_batch(&batch?, &names, Some(file_id.get()))
                            }))
                        })
                        .try_flatten()
                        .boxed()
                    })
                    .flatten_unordered(parallelism),
            )
        })
        .try_flatten();
        Self {
            batches: inline.chain(files).boxed(),
            files_read,
            coverage_files,
            requested: coverage_requested,
            read_metrics,
            parallelism,
            workers,
            started: false,
        }
    }
}
