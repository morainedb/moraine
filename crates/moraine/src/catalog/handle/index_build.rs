//! Resumable, bounded staged index construction.

use std::{sync::Arc, time::Instant};

use futures::TryStreamExt;
use object_store::{ObjectStore, path::Path};
use tracing::{info, warn};

use super::{Catalog, backfill};
use crate::{
    catalog::{
        BuildStep, ColumnId, ColumnOrder, DataFileId, IndexDef, IndexEntry, IndexId,
        IndexMaintenance, IndexState, TableId, snapshot,
    },
    data_file,
    error::{Error, Result},
    store::index_encoding::{Direction, NullOrder},
};

/// How many times a staged build re-derives after losing a race before
/// giving up.
const BUILD_DERIVATION_ATTEMPTS: usize = 8;

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

/// One streaming derivation pass's bounded entry buffer.
struct BuildStepBuffer<'a> {
    catalog: &'a Catalog,
    table: TableId,
    index: IndexId,
    index_name: &'a str,
    bound: BuildStep,
    derivation_attempt: usize,
    total_entries: usize,
    entries: Vec<IndexEntry>,
    nominal_bytes: u64,
    pending_source: Option<(u64, u64)>,
    completed_entries: usize,
    peak_buffered_entries: usize,
}

impl BuildStepBuffer<'_> {
    fn cover_source(&mut self, file_id: u64, position: u64) {
        self.pending_source = Some((file_id, position));
    }

    async fn push(&mut self, entry: IndexEntry, source: Option<(u64, u64)>) -> Result<()> {
        let entry_bytes = entry.nominal_bytes();
        let full = !self.entries.is_empty()
            && (self.entries.len() >= self.bound.entries
                || self.nominal_bytes.saturating_add(entry_bytes) > self.bound.bytes);
        if full {
            self.flush(false).await?;
        }

        self.nominal_bytes = self.nominal_bytes.saturating_add(entry_bytes);
        self.entries.push(entry);
        if let Some((file_id, position)) = source {
            self.cover_source(file_id, position);
        }
        self.peak_buffered_entries = self.peak_buffered_entries.max(self.entries.len());
        Ok(())
    }

    async fn flush(&mut self, is_final: bool) -> Result<()> {
        let entries = std::mem::take(&mut self.entries);
        self.nominal_bytes = 0;
        let step_entries = entries.len();
        let build_cursor = entries.last().map(|entry| entry.row_id);
        let source = self.pending_source;
        let commit_started = Instant::now();

        self.catalog
            .commit(|tx| {
                tx.build_index_source_step(self.index, &entries, is_final, source)
                    .map(|_| ())
            })
            .await?;

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
    /// is zero, the resumed definition differs from `def`, or the rows
    /// duplicate a unique value. A failed build drops its definition.
    pub async fn create_index_staged(
        &self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        data_store: Option<Arc<dyn ObjectStore>>,
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
        data_store: Option<Arc<dyn ObjectStore>>,
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
            .begin_staged_index(table, def, orders, maintenance)
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
        data_store: Option<Arc<dyn ObjectStore>>,
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
    ) -> Result<IndexId> {
        if let Some(existing) = self.snapshot().await?.index_by_name(table, &def.name) {
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
    #[allow(clippy::too_many_lines)]
    async fn drive_staged_build(
        &self,
        table: TableId,
        def: &IndexDef,
        index: IndexId,
        data_store: Option<Arc<dyn ObjectStore>>,
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
            let snapshot = self.snapshot().await?;
            let info = snapshot
                .indexes_of(table)
                .into_iter()
                .find(|info| info.id == index)
                .ok_or_else(|| Error::NotFound(format!("index {index}")))?;
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
            // Deferred UPDATE may reinsert an inline row under its preserved
            // id, so a repair re-derives the inline live set rather than
            // resuming by row-id watermark.
            let inline_row_cursor = (info.state != IndexState::Maintaining)
                .then_some(info.build_cursor)
                .flatten();
            let mut buffer = BuildStepBuffer {
                catalog: self,
                table,
                index,
                index_name: &def.name,
                bound: step,
                derivation_attempt: attempt,
                total_entries,
                entries: Vec::new(),
                nominal_bytes: 0,
                pending_source: initial_file_cursor.zip(initial_position_cursor),
                completed_entries: 0,
                peak_buffered_entries: 0,
            };

            // Inline rows precede file sources. Older builds that carry
            // only a row-id cursor resume this leg by that watermark.
            let mut inline = self.inline_backfill_entries(table, &def.columns).await?;
            inline.sort_unstable_by_key(|entry| entry.row_id);
            for entry in inline
                .into_iter()
                .filter(|entry| inline_row_cursor.is_none_or(|row| entry.row_id > row))
            {
                buffer.push(entry, None).await?;
            }

            if let Some(store) = &data_store {
                self.stream_backfill_files(
                    Arc::clone(store),
                    data_prefix,
                    table,
                    &def.columns,
                    initial_file_cursor,
                    initial_position_cursor,
                    legacy_row_cursor,
                    &mut buffer,
                )
                .await?;
            }
            let pass = buffer.flush(true).await;

            match pass {
                Ok(()) => {
                    info!(
                        table = table.get(),
                        index = index.get(),
                        index_name = %def.name,
                        derivation_attempt = attempt,
                        total_entries = buffer.completed_entries,
                        peak_buffered_entries = buffer.peak_buffered_entries,
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
                        completed_entries = buffer.completed_entries,
                        total_entries,
                        progress_percent = build_progress_percent(
                            buffer.completed_entries,
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

    /// Streams the external-file leg of a build into `buffer`, excluding
    /// rows already dead at this pass's pinned snapshot.
    #[allow(clippy::too_many_arguments)]
    async fn stream_backfill_files(
        &self,
        object_store: Arc<dyn ObjectStore>,
        data_prefix: &str,
        table: TableId,
        columns: &[ColumnId],
        file_cursor: Option<u64>,
        position_cursor: Option<u64>,
        legacy_row_cursor: Option<u64>,
        buffer: &mut BuildStepBuffer<'_>,
    ) -> Result<()> {
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
            Path::from(relative.as_str())
        };

        let inline_deletes =
            backfill::collect_inline_delete_positions(session.handle(), table.get());
        let metrics = self.data_read_metrics();
        let delete_files = backfill::collect_delete_positions(
            snapshot.delete_files_of(table).into_iter(),
            Arc::clone(&object_store),
            Arc::clone(&metrics),
            &resolve,
        );
        let (killed_row_ids, killed_positions) = futures::try_join!(inline_deletes, delete_files)?;

        for file in snapshot.data_files_of(table) {
            if file_cursor.is_some_and(|cursor| file.id.get() < cursor) {
                continue;
            }
            let start = if file_cursor == Some(file.id.get()) {
                position_cursor.map_or(0, |position| position.saturating_add(1))
            } else {
                0
            };
            if start >= file.record_count {
                continue;
            }
            let path = resolve(&file.path, file.path_is_relative);
            let file_id = file.id.get();
            let dead_positions = killed_positions.get(&file_id);
            let dead_row_ids = killed_row_ids.get(&file_id);
            let mut batches = data_file::scoped_read_entry_batches(
                data_file::ParquetFile::new(
                    Arc::clone(&object_store),
                    path,
                    file.file_size_bytes,
                    file.footer_size,
                )
                .with_metrics(Arc::clone(&metrics)),
                &positions,
                data_file::ScopedRows::From(start),
                data_file::RowIdSource::Resolve {
                    row_id_start: file.row_id_start,
                },
            )
            .await?;
            while let Some(batch) = batches.try_next().await? {
                for entry in batch {
                    let ordinal = entry.ordinal;
                    let dead = dead_positions.is_some_and(|positions| positions.contains(&ordinal))
                        || dead_row_ids.is_some_and(|rows| rows.contains(&entry.row_id));
                    let covered = legacy_row_cursor.is_some_and(|cursor| entry.row_id <= cursor);
                    if !dead && !covered {
                        buffer
                            .push(
                                IndexEntry {
                                    row_id: entry.row_id,
                                    values: entry.values,
                                },
                                Some((file_id, ordinal)),
                            )
                            .await?;
                    } else {
                        buffer.cover_source(file_id, ordinal);
                    }
                }
            }
        }

        session.finish();

        Ok(())
    }
}
