//! Resumable, bounded staged index construction.

use std::{collections::HashMap, sync::Arc, time::Instant};

use futures::{StreamExt, TryStreamExt, stream};
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::{BACKFILL_FILE_READ_CONCURRENCY, Catalog, backfill};
use crate::{
    catalog::{
        BuildStep, CatalogSnapshot, ColumnId, ColumnOrder, DataFileId, IndexDef, IndexId,
        IndexInfo, IndexMaintenance, IndexState, SnapshotId, TableId, resolve_data_path, snapshot,
    },
    data_file::{self, DataStore, RowPositions, ScopedIndexEntry},
    error::{Error, Result},
    store::{
        index_encoding::{Direction, IndexKeyValue, NullOrder, encode_ordered_index_entry},
        proto::{DataFileValue, DeleteFileValue, InlineBuildCursorValue},
    },
    transaction::EncodedIndexEntry,
};

/// How many times a staged build re-derives after losing a race before
/// giving up.
const BUILD_DERIVATION_ATTEMPTS: usize = 8;

/// Files whose metadata and deletion state resolve ahead of their rows.
const FILE_PLAN_CONCURRENCY: usize = BACKFILL_FILE_READ_CONCURRENCY;

/// Row-group reads decoding and encoding at once, ahead of the one whose
/// entries the driver is consuming.
const UNIT_CONCURRENCY: usize = BACKFILL_FILE_READ_CONCURRENCY;

/// Encoded batches one row-group read may hold ahead of the driver.
const UNIT_BUFFERED_BATCHES: usize = 2;

#[expect(
    clippy::cast_precision_loss,
    reason = "the percentage is diagnostic; f64 is exact for every practical row count"
)]
fn build_progress_percent(completed: usize, total: usize) -> f64 {
    if total == 0 {
        100.0
    } else {
        completed as f64 * 100.0 / total as f64
    }
}

/// The per-column orders `orders` asks for, as a definition records them.
/// An empty list means ascending / NULLS LAST throughout.
fn requested_orders(orders: &[ColumnOrder], columns: usize) -> (Vec<Direction>, Vec<NullOrder>) {
    (0..columns)
        .map(|position| {
            orders
                .get(position)
                .map_or((Direction::Ascending, NullOrder::Last), |order| {
                    (order.direction, order.nulls)
                })
        })
        .unzip()
}

/// Encodes derived values to physical keys under one definition.
#[derive(Clone)]
struct IndexEncoder {
    index_id: u64,
    unique: bool,
    directions: Vec<Direction>,
    nulls: Vec<NullOrder>,
}

impl IndexEncoder {
    fn of(info: &IndexInfo) -> Self {
        Self {
            index_id: info.id.get(),
            unique: info.unique,
            directions: info.directions.clone(),
            nulls: info.nulls.clone(),
        }
    }

    fn encode(&self, row_id: u64, values: &[Option<IndexKeyValue>]) -> Result<EncodedIndexEntry> {
        let (key, unique) = encode_ordered_index_entry(
            values,
            &self.directions,
            &self.nulls,
            self.index_id,
            self.unique,
            row_id,
        )?;
        Ok(EncodedIndexEntry {
            row_id,
            key,
            unique,
        })
    }

    /// The Arrow projection encoding this index from columns at `positions`.
    fn projection(&self, positions: Vec<usize>) -> data_file::IndexProjection {
        data_file::IndexProjection {
            index_id: self.index_id,
            unique: self.unique,
            positions,
            directions: self.directions.clone(),
            nulls: self.nulls.clone(),
        }
    }
}

/// What one step's commit stages: the key and, for a unique entry, the
/// row id it holds.
fn staged_entry_bytes(entry: &EncodedIndexEntry) -> u64 {
    let value_bytes = if entry.unique { size_of::<u64>() } else { 0 };
    u64::try_from(entry.key.len().saturating_add(value_bytes)).unwrap_or(u64::MAX)
}

/// One full step, handed from derivation to the committer.
struct PendingStep {
    entries: Vec<EncodedIndexEntry>,
    is_final: bool,
    source: Option<(u64, u64)>,
    inline_cursor: Option<InlineBuildCursorValue>,
}

/// The derivation side of one pass: fills one bounded step at a time and
/// hands each full step to the committer.
struct StepBuffer {
    encoder: IndexEncoder,
    bound: BuildStep,
    entries: Vec<EncodedIndexEntry>,
    staged_bytes: u64,
    pending_source: Option<(u64, u64)>,
    inline_cursor: Option<InlineBuildCursorValue>,
    /// `None` once derivation has handed off its final step.
    steps: Option<mpsc::Sender<Result<PendingStep>>>,
    peak_buffered_entries: usize,
    peak_inline_body_bytes: usize,
    peak_inline_decoded_bytes: usize,
}

impl StepBuffer {
    fn cover_source(&mut self, file_id: u64, position: u64) {
        self.pending_source = Some((file_id, position));
    }

    /// Encodes and buffers an entry the driver derived itself.
    async fn push_values(&mut self, row_id: u64, values: &[Option<IndexKeyValue>]) -> Result<()> {
        let entry = self.encoder.encode(row_id, values)?;
        self.push(entry, None).await
    }

    async fn push(&mut self, entry: EncodedIndexEntry, source: Option<(u64, u64)>) -> Result<()> {
        let entry_bytes = staged_entry_bytes(&entry);
        let full = !self.entries.is_empty()
            && (self.entries.len() >= self.bound.entries
                || self.staged_bytes.saturating_add(entry_bytes) > self.bound.bytes);
        if full {
            self.hand_off(false).await?;
        }

        self.staged_bytes = self.staged_bytes.saturating_add(entry_bytes);
        self.entries.push(entry);
        if let Some((file_id, position)) = source {
            self.cover_source(file_id, position);
        }
        self.peak_buffered_entries = self.peak_buffered_entries.max(self.entries.len());
        Ok(())
    }

    /// Hands the buffered step to the committer, waiting while it still
    /// holds the previous one.
    async fn hand_off(&mut self, is_final: bool) -> Result<()> {
        let step = PendingStep {
            entries: std::mem::take(&mut self.entries),
            is_final,
            source: self.pending_source,
            inline_cursor: self.inline_cursor,
        };
        self.staged_bytes = 0;
        let steps = self.steps.as_ref().ok_or_else(|| {
            Error::Corruption("staged build derived past its final step".to_owned())
        })?;
        steps
            .send(Ok(step))
            .await
            .map_err(|_| Error::Interrupted("staged build committer stopped".to_owned()))
    }
}

/// The commit side of one pass: lands steps in order, each under the
/// snapshot the previous one produced.
struct Committer<'a> {
    catalog: &'a Catalog,
    table: TableId,
    index: IndexId,
    index_name: &'a str,
    derivation_attempt: usize,
    /// Derivation snapshot advanced only by this driver's committed steps.
    expected_snapshot: SnapshotId,
    total_entries: usize,
    completed_entries: usize,
}

impl Committer<'_> {
    async fn run(&mut self, mut steps: mpsc::Receiver<Result<PendingStep>>) -> Result<()> {
        while let Some(step) = steps.recv().await {
            self.commit(step?).await?;
        }
        Ok(())
    }

    async fn commit(&mut self, step: PendingStep) -> Result<()> {
        let PendingStep {
            entries,
            is_final,
            source,
            inline_cursor,
        } = step;
        let step_entries = entries.len();
        let build_cursor = entries.last().map(|entry| entry.row_id);
        let commit_started = Instant::now();

        let committed = self
            .catalog
            .commit(|tx| {
                if tx.current_snapshot().id != self.expected_snapshot {
                    return Err(Error::CommitConflict(
                        "catalog changed after staged index derivation".to_owned(),
                    ));
                }
                tx.build_index_encoded_step(
                    self.index,
                    &entries,
                    is_final,
                    source,
                    inline_cursor.as_ref(),
                )
                .map(|_| ())
            })
            .await?;
        self.expected_snapshot = committed;

        let state = self
            .catalog
            .snapshot()
            .await?
            .indexes_of(self.table)
            .into_iter()
            .find(|index| index.id == self.index)
            .ok_or_else(|| Error::NotFound(format!("index {}", self.index)))?
            .state;
        if state == IndexState::Poisoned {
            return Err(Error::Constraint(format!(
                "index {} was poisoned by a duplicate value",
                self.index
            )));
        }

        self.completed_entries = self.completed_entries.saturating_add(step_entries);
        info!(
            table = self.table.get(),
            index = self.index.get(),
            index_name = %self.index_name,
            derivation_attempt = self.derivation_attempt,
            step_entries,
            completed_entries = self.completed_entries,
            total_entries = self.total_entries,
            progress_percent = build_progress_percent(
                self.completed_entries,
                self.total_entries,
            ),
            build_cursor = ?build_cursor,
            source_file = ?source.map(|cursor| cursor.0),
            source_position = ?source.map(|cursor| cursor.1),
            is_final,
            commit_ms = crate::telemetry::milliseconds(commit_started.elapsed()),
            "staged index build step committed"
        );
        Ok(())
    }
}

/// One registered file's resolved read inputs, gathered ahead of its rows.
struct FilePlan {
    file: data_file::ParquetFile,
    file_id: u64,
    row_id_start: Option<u64>,
    dead: Option<RowPositions>,
    group_rows: Vec<u64>,
    /// First physical position this pass still has to cover.
    start: u64,
}

impl FilePlan {
    /// The row groups of this file still to read, in file order.
    fn units(self, projection: &data_file::IndexProjection) -> Vec<Unit> {
        let mut units = Vec::new();
        let mut group_start = 0u64;
        for (group, rows) in self.group_rows.iter().enumerate() {
            let end = group_start.saturating_add(*rows);
            if end > self.start && *rows > 0 {
                units.push(Unit {
                    file: self.file.clone(),
                    file_id: self.file_id,
                    group,
                    from: self.start.max(group_start),
                    end,
                    row_id_start: self.row_id_start,
                    dead: self.dead.clone(),
                    projection: projection.clone(),
                });
            }
            group_start = end;
        }
        units
    }
}

/// One row group of one file: the unit read and encoded off the driver.
struct Unit {
    file: data_file::ParquetFile,
    file_id: u64,
    group: usize,
    from: u64,
    /// One past the last physical position of the group.
    end: u64,
    row_id_start: Option<u64>,
    dead: Option<RowPositions>,
    projection: data_file::IndexProjection,
}

impl Unit {
    /// Starts reading on its own task, buffering at most
    /// `UNIT_BUFFERED_BATCHES` encoded batches ahead of the consumer.
    fn spawn(self) -> RunningUnit {
        let (sender, receiver) = mpsc::channel(UNIT_BUFFERED_BATCHES);
        let file_id = self.file_id;
        let end = self.end;
        let task = tokio::spawn(async move {
            let outcome = async {
                let mut batches = data_file::scoped_read_index_entry_batches(
                    self.file,
                    vec![self.projection],
                    data_file::ScopedRows::RowGroup {
                        group: self.group,
                        from: self.from,
                    },
                    data_file::RowIdSource::Resolve {
                        row_id_start: self.row_id_start,
                    },
                    self.dead.as_ref(),
                )
                .await?;
                while let Some(batch) = batches.try_next().await? {
                    if sender.send(Ok(batch)).await.is_err() {
                        break;
                    }
                }
                Ok(())
            }
            .await;
            if let Err(error) = outcome {
                let _ = sender.send(Err(error)).await;
            }
        });
        RunningUnit {
            file_id,
            end,
            batches: receiver,
            task: AbortOnDrop(task),
        }
    }
}

/// A unit in flight; dropping it stops the read.
struct RunningUnit {
    file_id: u64,
    end: u64,
    batches: mpsc::Receiver<Result<Vec<ScopedIndexEntry>>>,
    task: AbortOnDrop,
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Catalog {
    /// Creates an index by a staged (multi-commit) build, driving it to
    /// `ready` before returning — for a table whose backfill exceeds what
    /// one commit may stage.
    ///
    /// The definition lands `building` in its own commit; each pass then
    /// derives the table's live entries (external files through
    /// `data_store`, inline rows from the catalog store) in durable source
    /// order and commits them in steps bounded by `step`. Writers maintain
    /// entries from the first commit forward.
    ///
    /// Interrupting the call leaves the definition `building`: calling again
    /// with the same `def` resumes from the persisted cursor, and
    /// [`Transaction::drop_index`](crate::Transaction::drop_index) abandons
    /// the build. A concurrent write to the table conflicts with a step,
    /// which re-derives at a fresh snapshot rather than staging entries for
    /// rows the winner deleted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::AlreadyExists`] if the table already holds a ready
    /// index of this name, or [`Error::Constraint`] if either `step` bound
    /// is zero, the resumed definition differs from `def`, registered files
    /// require an absent data store, or the rows duplicate a unique value. A
    /// failed build drops its definition.
    pub async fn create_index_staged(
        &self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        data_store: Option<DataStore>,
        data_prefix: &str,
        step: Option<BuildStep>,
    ) -> Result<IndexId> {
        self.create_index_staged_with_maintenance(
            table,
            def,
            orders,
            IndexMaintenance::Synchronous,
            data_store,
            data_prefix,
            step,
        )
        .await
    }

    /// Creates an index by a staged build with the requested upkeep mode.
    /// Deferred upkeep is available only to non-unique indexes.
    ///
    /// # Errors
    ///
    /// As [`Self::create_index_staged`], plus [`Error::Constraint`] when
    /// deferred upkeep is requested for a unique index.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_index_staged_with_maintenance(
        &self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        maintenance: IndexMaintenance,
        data_store: Option<DataStore>,
        data_prefix: &str,
        step: Option<BuildStep>,
    ) -> Result<IndexId> {
        let step = step.unwrap_or_default();
        if step.entries == 0 || step.bytes == 0 {
            return Err(Error::Constraint(
                "a staged build's step must admit at least one entry and one byte".to_owned(),
            ));
        }

        let index = self
            .begin_staged_index(table, def, orders, maintenance, data_store.is_some())
            .await?;
        let outcome = self
            .drive_staged_build(table, def, index, data_store, data_prefix, step)
            .await;

        // A build that cannot finish leaves no half-covered index behind.
        // A cleanup that itself fails is logged, never substituted for the
        // failure that caused it.
        if outcome.is_err()
            && let Err(cleanup) = self.commit(|tx| tx.drop_index(index)).await
        {
            warn!(
                index = index.get(),
                error = %cleanup,
                "could not drop the definition of a failed staged build"
            );
        }
        outcome.map(|()| index)
    }

    /// Repairs every deferred non-unique index awaiting upkeep, using the
    /// same bounded streaming driver as an initial staged build.
    ///
    /// Returns the number of definitions flipped back to ready. A failed
    /// repair remains in `maintaining` state and serves no lookups, so a
    /// later call safely resumes it.
    ///
    /// # Errors
    ///
    /// Returns a store or derivation error, or [`Error::Constraint`] when a
    /// step bound is zero.
    pub async fn repair_deferred_indexes(
        &self,
        data_store: Option<DataStore>,
        data_prefix: &str,
        step: Option<BuildStep>,
    ) -> Result<u64> {
        let step = step.unwrap_or_default();
        if step.entries == 0 || step.bytes == 0 {
            return Err(Error::Constraint(
                "a deferred repair step must admit at least one entry and one byte".to_owned(),
            ));
        }

        let snapshot = self.snapshot().await?;
        let pending: Vec<_> = snapshot
            .indexes
            .values()
            .flat_map(|per_table| per_table.values())
            .map(snapshot::index_info)
            .filter(|index| index.state == IndexState::Maintaining)
            .collect();

        let mut repaired = 0u64;
        for index in pending {
            if data_store.is_none() && !snapshot.data_files_of(index.table_id).is_empty() {
                return Err(Error::Constraint(format!(
                    "deferred index {} cannot be repaired without a data-path store",
                    index.id
                )));
            }
            let def = IndexDef {
                name: index.name.clone(),
                columns: index.columns.clone(),
                unique: index.unique,
            };
            self.drive_staged_build(
                index.table_id,
                &def,
                index.id,
                data_store.clone(),
                data_prefix,
                step,
            )
            .await?;
            repaired = repaired.saturating_add(1);
        }
        Ok(repaired)
    }

    /// Commits the `building` definition, or adopts the one already there.
    /// A ready definition of the same name belongs to a finished index.
    async fn begin_staged_index(
        &self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        maintenance: IndexMaintenance,
        has_data_store: bool,
    ) -> Result<IndexId> {
        let snapshot = self.snapshot().await?;
        require_data_store(&snapshot, table, has_data_store)?;
        if let Some(existing) = snapshot.index_by_name(table, &def.name) {
            return match existing.state {
                IndexState::Ready => Err(Error::AlreadyExists(format!(
                    "index {} on table {table}",
                    def.name
                ))),
                IndexState::Building | IndexState::Poisoned => {
                    // Resuming adopts the stored definition, whose entries
                    // are encoded under its own orders.
                    let (directions, nulls) = requested_orders(orders, def.columns.len());
                    if existing.columns != def.columns
                        || existing.unique != def.unique
                        || existing.maintenance != maintenance
                        || existing.directions != directions
                        || existing.nulls != nulls
                    {
                        return Err(Error::Constraint(format!(
                            "index {} on table {table} is already building over a different \
                             definition; drop it to rebuild",
                            def.name
                        )));
                    }
                    Ok(existing.id)
                }
                IndexState::Maintaining => Err(Error::Constraint(format!(
                    "index {} on table {table} is awaiting deferred maintenance",
                    def.name
                ))),
            };
        }

        let index = std::cell::Cell::new(None);
        self.commit(|tx| {
            require_data_store(tx, table, has_data_store)?;
            let id =
                tx.create_index_staged_ordered_with_maintenance(table, def, orders, maintenance)?;
            index.set(Some(id));
            Ok(())
        })
        .await?;

        index
            .get()
            .ok_or_else(|| Error::Corruption("staged create returned no index id".to_owned()))
    }

    /// Derives the live backfill and commits it in bounded steps until the
    /// index is ready, re-deriving at a fresh snapshot after a lost race.
    /// Derivation runs beside the commits: a step lands while the next one
    /// is being derived.
    #[allow(clippy::too_many_lines)]
    async fn drive_staged_build(
        &self,
        table: TableId,
        def: &IndexDef,
        index: IndexId,
        data_store: Option<DataStore>,
        data_prefix: &str,
        step: BuildStep,
    ) -> Result<()> {
        for attempt in 1..=BUILD_DERIVATION_ATTEMPTS {
            info!(
                table = table.get(),
                index = index.get(),
                index_name = %def.name,
                derivation_attempt = attempt,
                "staged index backfill derivation started"
            );
            let derivation_started = Instant::now();
            let epoch = super::cache_epoch(&self.projections);
            let session = self.begin_read().await?;
            let snapshot = self.head_view(session.handle(), epoch).await?;
            require_data_store(&snapshot, table, data_store.is_some())?;
            let source = backfill::BackfillSource {
                snapshot: &snapshot,
                table,
                handle: session.handle(),
            };
            let info = snapshot
                .indexes_of(table)
                .into_iter()
                .find(|info| info.id == index)
                .ok_or_else(|| Error::NotFound(format!("index {index}")))?;
            if info.state == IndexState::Ready {
                return Ok(());
            }
            let total_entries = snapshot
                .table_stats(table)
                .and_then(|stats| usize::try_from(stats.record_count).ok())
                .unwrap_or(usize::MAX);
            let initial_file_cursor = info.build_file_cursor.map(DataFileId::get);
            let initial_position_cursor = info.build_position_cursor;
            let legacy_row_cursor = initial_file_cursor
                .is_none()
                .then_some(info.build_cursor)
                .flatten();
            let saved_inline = snapshot
                .indexes
                .get(&table.get())
                .and_then(|indexes| indexes.get(&index.get()))
                .and_then(|index| index.build_inline_cursor);
            let inline_cursor = saved_inline.filter(|cursor| {
                info.state != IndexState::Maintaining
                    || cursor.covered_snapshot == snapshot.current_snapshot().id.get()
            });
            let inline_row_cursor = (info.state != IndexState::Maintaining
                && inline_cursor.is_none())
            .then_some(info.build_cursor)
            .flatten();

            let (steps, pending_steps) = mpsc::channel(1);
            let mut buffer = StepBuffer {
                encoder: IndexEncoder::of(&info),
                bound: step,
                entries: Vec::new(),
                staged_bytes: 0,
                pending_source: initial_file_cursor.zip(initial_position_cursor),
                inline_cursor,
                steps: Some(steps.clone()),
                peak_buffered_entries: 0,
                peak_inline_body_bytes: 0,
                peak_inline_decoded_bytes: 0,
            };
            let mut committer = Committer {
                catalog: self,
                table,
                index,
                index_name: &def.name,
                derivation_attempt: attempt,
                expected_snapshot: snapshot.current_snapshot().id,
                total_entries,
                completed_entries: 0,
            };

            // A derivation failure reaches the committer behind the steps
            // already handed off, so those still land before it surfaces.
            let derive = async {
                let outcome = async {
                    let inline_done = buffer
                        .inline_cursor
                        .as_ref()
                        .is_some_and(|cursor| cursor.complete)
                        || (info.state != IndexState::Maintaining && initial_file_cursor.is_some());
                    if !inline_done {
                        stream_inline_sources(source, &def.columns, inline_row_cursor, &mut buffer)
                            .await?;
                    }
                    buffer.inline_cursor.get_or_insert_default().complete = true;

                    if let Some(store) = &data_store {
                        self.stream_backfill_files(
                            store.clone(),
                            data_prefix,
                            source,
                            &def.columns,
                            initial_file_cursor,
                            initial_position_cursor,
                            legacy_row_cursor,
                            &mut buffer,
                        )
                        .await?;
                    }
                    buffer.hand_off(true).await
                }
                .await;
                buffer.steps = None;
                if let Err(error) = outcome {
                    let _ = steps.send(Err(error)).await;
                }
                drop(steps);
                Ok(())
            };
            let pass = Box::pin(futures::future::try_join(
                derive,
                committer.run(pending_steps),
            ))
            .await
            .map(|((), ())| ());
            session.finish();

            match pass {
                Ok(()) => {
                    info!(
                        table = table.get(),
                        index = index.get(),
                        index_name = %def.name,
                        derivation_attempt = attempt,
                        total_entries = committer.completed_entries,
                        peak_buffered_entries = buffer.peak_buffered_entries,
                        peak_inline_body_bytes = buffer.peak_inline_body_bytes,
                        peak_inline_decoded_bytes = buffer.peak_inline_decoded_bytes,
                        derive_ms = crate::telemetry::milliseconds(derivation_started.elapsed()),
                        sort_ms = 0_u64,
                        "staged index backfill derived"
                    );
                    return Ok(());
                }
                Err(Error::CommitConflict(_)) => {
                    warn!(
                        table = table.get(),
                        index = index.get(),
                        index_name = %def.name,
                        derivation_attempt = attempt,
                        completed_entries = committer.completed_entries,
                        total_entries,
                        progress_percent = build_progress_percent(
                            committer.completed_entries,
                            total_entries,
                        ),
                        "staged index build step conflicted; re-deriving"
                    );
                }
                Err(other) => return Err(other),
            }
        }
        Err(Error::CommitConflict(format!(
            "staged build of index {index} lost its race {BUILD_DERIVATION_ATTEMPTS} times; \
             the table is under concurrent write"
        )))
    }

    /// Streams the external-file leg of a build into `buffer` in file and
    /// position order, excluding rows already dead at this pass's pinned
    /// snapshot. Files are planned and row groups read ahead of the
    /// position being consumed.
    #[allow(clippy::too_many_arguments)]
    async fn stream_backfill_files(
        &self,
        object_store: DataStore,
        data_prefix: &str,
        source: backfill::BackfillSource<'_>,
        columns: &[ColumnId],
        file_cursor: Option<u64>,
        position_cursor: Option<u64>,
        legacy_row_cursor: Option<u64>,
        buffer: &mut StepBuffer,
    ) -> Result<()> {
        let backfill::BackfillSource {
            snapshot,
            table,
            handle,
        } = source;
        let projection = buffer
            .encoder
            .projection(snapshot.column_positions(table, columns)?);

        let table_prefix = snapshot.table_data_prefix(table)?;
        let resolve = |path: &str, is_relative: bool| {
            resolve_data_path(data_prefix, &table_prefix, path, is_relative)
        };
        let metrics = self.data_read_metrics();
        let deletions_by_file = deletions_by_file(snapshot, table);

        let remaining = snapshot
            .data_files
            .get(&table.get())
            .into_iter()
            .flat_map(|files| files.values())
            .filter(|file| file_cursor.is_none_or(|cursor| file.data_file_id >= cursor))
            .filter_map(|file| {
                let start = if file_cursor == Some(file.data_file_id) {
                    position_cursor.map_or(0, |position| position.saturating_add(1))
                } else {
                    0
                };
                (start < file.record_count).then_some((file, start))
            });
        let plans = stream::iter(remaining)
            .map(|(file, start)| {
                plan_file(
                    snapshot,
                    table,
                    handle,
                    &object_store,
                    Arc::clone(&metrics),
                    &resolve,
                    deletions_by_file
                        .get(&file.data_file_id)
                        .map_or(&[][..], Vec::as_slice),
                    file,
                    start,
                )
            })
            .buffered(FILE_PLAN_CONCURRENCY);
        let mut units = plans
            .map_ok(|plan| stream::iter(plan.units(&projection).into_iter().map(Result::Ok)))
            .try_flatten();

        // The feeder starts units as the window admits them, so planning
        // and reading run ahead while the consumer drains the head unit.
        let (running, mut ready) = mpsc::channel::<RunningUnit>(UNIT_CONCURRENCY);
        // Owning the sender, the feeder closes the window when the last
        // unit is out, which is what ends the consumer.
        let feed = async move {
            while let Some(unit) = units.try_next().await? {
                if running.send(unit.spawn()).await.is_err() {
                    break;
                }
            }
            Ok(())
        };
        let consume = async {
            while let Some(mut unit) = ready.recv().await {
                while let Some(batch) = unit.batches.recv().await {
                    for entry in batch? {
                        let covered =
                            legacy_row_cursor.is_some_and(|cursor| entry.row_id <= cursor);
                        if covered {
                            buffer.cover_source(unit.file_id, entry.ordinal);
                            continue;
                        }
                        buffer
                            .push(
                                EncodedIndexEntry {
                                    row_id: entry.row_id,
                                    key: entry.key,
                                    unique: entry.unique,
                                },
                                Some((unit.file_id, entry.ordinal)),
                            )
                            .await?;
                    }
                }
                buffer.cover_source(unit.file_id, unit.end.saturating_sub(1));
                drop(unit.task);
            }
            Ok(())
        };
        futures::future::try_join(feed, consume)
            .await
            .map(|((), ())| ())
    }
}

/// A table's delete files, grouped by the data file each one targets.
fn deletions_by_file(
    snapshot: &CatalogSnapshot,
    table: TableId,
) -> HashMap<u64, Vec<&DeleteFileValue>> {
    let mut by_file: HashMap<u64, Vec<&DeleteFileValue>> = HashMap::new();
    for deletion in snapshot
        .delete_files
        .get(&table.get())
        .into_iter()
        .flat_map(|files| files.values())
    {
        by_file
            .entry(deletion.data_file_id)
            .or_default()
            .push(deletion);
    }
    by_file
}

/// Resolves one file's read columns, dead positions, and row-group sizes.
#[allow(clippy::too_many_arguments)]
async fn plan_file(
    snapshot: &CatalogSnapshot,
    table: TableId,
    handle: crate::store::handle::ReadHandle<'_>,
    object_store: &DataStore,
    metrics: Arc<data_file::ScopedReadMetrics>,
    resolve: &impl Fn(&str, bool) -> Result<object_store::path::Path>,
    deletions: &[&DeleteFileValue],
    file: &DataFileValue,
    start: u64,
) -> Result<FilePlan> {
    let file_id = file.data_file_id;
    let parquet = data_file::ParquetFile::new(
        object_store.clone(),
        resolve(&file.path, file.path_is_relative)?,
        file.file_size_bytes,
        file.footer_size,
    )
    .with_metrics(Arc::clone(&metrics));

    let columns = snapshot.file_read_columns_at(handle, table, file);
    let inline_dead =
        crate::store::inline::stream::file_delete_positions(handle, table.get(), file_id);
    let file_dead = async {
        let mut dead = Vec::new();
        for deletion in deletions {
            dead.extend(
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
        Ok(dead)
    };
    let (columns, mut dead, file_dead, group_rows) = futures::try_join!(
        columns,
        inline_dead,
        file_dead,
        data_file::row_group_row_counts(&parquet)
    )?;
    dead.extend(file_dead);

    Ok(FilePlan {
        file: parquet.with_columns(columns),
        file_id,
        row_id_start: file.row_id_start,
        dead: (!dead.is_empty()).then(|| dead.into_iter().collect()),
        group_rows,
        start,
    })
}

/// A build must be able to read every registered file before it can publish.
fn require_data_store(
    snapshot: &CatalogSnapshot,
    table: TableId,
    has_data_store: bool,
) -> Result<()> {
    if !has_data_store
        && snapshot
            .data_files
            .get(&table.get())
            .is_some_and(|files| !files.is_empty())
    {
        return Err(Error::Constraint(format!(
            "staged index build for table {table} requires a data-path store",
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

mod inline_sources;
use inline_sources::stream_inline_sources;
