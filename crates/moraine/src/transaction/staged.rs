//! The staged-row commit path: DuckLake authors rows over the ABI instead
//! of driving [`crate::transaction::Transaction`]'s verbs, and this module
//! turns the accumulated rows into one atomic store write.
//!
//! Three rules bound this path: every value DuckLake supplies is stored
//! **verbatim** except one interpreted convention (an `UPDATE` setting
//! `end_snapshot` becomes current-delete + history-write); one commit is one
//! atomic batch, reusing [`super::commit::diff_writes`]; and a lost race at
//! commit is **never retried** — DuckLake authored the ids and counters in
//! the batch. The loser's error always carries the substring `conflict`
//! (via [`Error::CommitConflict`]'s `Display`), the wire contract
//! DuckLake's retry loop scans for.
//!
//! Translation applies every staged row to a cloned working
//! [`CatalogSnapshot`], then diffs it against the unmodified base exactly
//! as a verb-path commit diffs its closure's output. An `UPDATE ... SET
//! end_snapshot` row's value is validated, not trusted: DuckLake always
//! sets it to this commit's own new snapshot id — the value `diff_writes`
//! stamps on its own — so a mismatch is drift caught loudly.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use object_store::ObjectStore;
use slatedb::DbTransaction;
use tracing::debug;

use crate::{
    catalog::{
        CatalogSnapshot, ColumnInfo, IndexInfo, SnapshotId, TableId,
        inline::{InlineScanKind, materialize_inline_rows},
        projection::ProjectionCache,
    },
    data_file,
    error::{Error, Result},
    store::{
        StagedBytes,
        handle::ReadHandle,
        inline as store_inline,
        key::{EntityKey, InlineKey, InlineOperation, Key},
        proto, read, value,
    },
    transaction::{
        commit,
        index_maintenance::{
            IndexMaintenanceMetrics, StagedEntries, StagedIndexEntry, apply_deferred_maintenance,
            apply_poison, stage_index_entry_stream,
        },
    },
};

mod apply;
mod decode;
mod index_upkeep;
pub(crate) mod inline;
mod overlay;
#[cfg(test)]
mod tests;

use apply::{
    ChildRows, apply_op, build_snapshot_value, collect_child_rows, collect_hard_deletes,
    is_inline_op,
};
use decode::Cursor;
use index_upkeep::stage_index_maintenance;
use inline::translate_inline;

use crate::telemetry::milliseconds;

fn next_diagnostic_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

#[derive(Default)]
struct CommitPhases {
    head_view: Duration,
    inline: Duration,
    index_maintenance: Duration,
    translate: Duration,
    stage: Duration,
    land: Duration,
    index_metrics: IndexMaintenanceMetrics,
}

fn mints_snapshot(operations: &[RowOperation]) -> bool {
    operations.iter().any(|operation| {
        matches!(
            operation,
            RowOperation::Insert {
                table: TableKind::Snapshot,
                ..
            }
        )
    })
}

/// Which `ducklake_*` table a staged row targets. `Snapshot`,
/// `SnapshotChanges`, and `SchemaVersions` all fold into one moraine
/// `snapshot` record; every other kind maps to one `EntityKey` variant or one
/// of the three unversioned statistics kinds.
// The discriminants are the staged-write wire protocol (the ABI's
// `table_kind` values): declaration order is load-bearing, pinned by
// `ALL` and its order test. Insert new kinds at the end only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TableKind {
    /// `ducklake_snapshot`.
    Snapshot,
    /// `ducklake_snapshot_changes`.
    SnapshotChanges,
    /// `ducklake_schema`.
    Schema,
    /// `ducklake_table`.
    Table,
    /// `ducklake_view`.
    View,
    /// `ducklake_column`.
    Column,
    /// `ducklake_data_file`.
    DataFile,
    /// `ducklake_delete_file`.
    DeleteFile,
    /// `ducklake_table_stats`.
    TableStats,
    /// `ducklake_table_column_stats`.
    TableColumnStats,
    /// `ducklake_file_column_stats`.
    FileColumnStats,
    /// `ducklake_schema_versions`: per-table schema-change history, one
    /// `(begin_snapshot, schema_version, table_id)` row per
    /// created-or-schema-altered table per commit. The first two values
    /// are always the committing snapshot's own id and `schema_version`,
    /// validated against the `ducklake_snapshot` row in the same batch
    /// rather than trusted. Each row lands as a `schema_version` record of
    /// its own — the snapshot record names the same tables, but expiry
    /// deletes it and a data file older than every surviving snapshot
    /// still has to resolve the version it was written under.
    SchemaVersions,
    /// `ducklake_partition_info`.
    PartitionInfo,
    /// `ducklake_partition_column` — folded into its spec's record.
    PartitionColumn,
    /// `ducklake_file_partition_value` — folded into its file's record.
    FilePartitionValue,
    /// `ducklake_sort_info`.
    SortInfo,
    /// `ducklake_sort_expression` — folded into its spec's record.
    SortExpression,
    /// `ducklake_tag` — an entry in its object's container record.
    Tag,
    /// `ducklake_column_tag` — an entry embedded in its column's record.
    ColumnTag,
    /// `ducklake_files_scheduled_for_deletion` — the physical-deletion
    /// schedule, keyed by the scheduled file's id.
    FilesScheduledForDeletion,
    /// `ducklake_macro`.
    Macro,
    /// `ducklake_macro_impl` — folded into its macro's record.
    MacroImpl,
    /// `ducklake_macro_parameters` — folded into its macro's record.
    MacroParameters,
    /// `ducklake_column_mapping`.
    ColumnMapping,
    /// `ducklake_name_mapping` — folded into its mapping's record.
    NameMapping,
    /// `ducklake_metadata`: catalog options, keyed by `(key, scope,
    /// scope_id)`. Outside the snapshot protocol — DuckLake writes it
    /// within its metadata connection, minting no snapshot and bumping no
    /// schema version — so its rows overwrite the scope's option record in
    /// place, last write wins.
    Metadata,
}

impl TableKind {
    /// Every kind, in wire-discriminant order — the decode table for the
    /// ABI's `table_kind` values. A new variant added anywhere but the
    /// end fails the order test pinning `ALL[i] as i32 == i`.
    pub const ALL: [Self; 26] = [
        Self::Snapshot,
        Self::SnapshotChanges,
        Self::Schema,
        Self::Table,
        Self::View,
        Self::Column,
        Self::DataFile,
        Self::DeleteFile,
        Self::TableStats,
        Self::TableColumnStats,
        Self::FileColumnStats,
        Self::SchemaVersions,
        Self::PartitionInfo,
        Self::PartitionColumn,
        Self::FilePartitionValue,
        Self::SortInfo,
        Self::SortExpression,
        Self::Tag,
        Self::ColumnTag,
        Self::FilesScheduledForDeletion,
        Self::Macro,
        Self::MacroImpl,
        Self::MacroParameters,
        Self::ColumnMapping,
        Self::NameMapping,
        Self::Metadata,
    ];
}

impl TryFrom<i32> for TableKind {
    type Error = i32;

    /// Decodes a wire discriminant; the unrecognized value comes back as
    /// the error.
    fn try_from(value: i32) -> std::result::Result<Self, i32> {
        usize::try_from(value)
            .ok()
            .and_then(|index| Self::ALL.get(index).copied())
            .ok_or(value)
    }
}

/// One column value in a staged row, typed to the small set of primitive
/// kinds every `ducklake_*` column uses. Decoding into the right proto
/// shape happens here, table-kind by table-kind.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    /// SQL `NULL`.
    Null,
    /// An unsigned integer column (every id, counter, and count).
    U64(u64),
    /// A signed integer column (currently only `snapshot_time`, a
    /// `TIMESTAMPTZ` carried as microseconds since the epoch).
    I64(i64),
    /// A boolean column.
    Bool(bool),
    /// A text column (also used for `UUID`, carried as its text form).
    Str(String),
}

/// One staged row mutation, DuckLake-authored. `cells` are positional, in
/// the exact `ducklake_*` column order pinned in
/// `crates/moraine-duckdb/cpp/metadata_tables.cpp`.
#[derive(Debug, Clone)]
pub enum RowOperation {
    /// A new row: becomes a live `current` record (versioned kinds) or
    /// overwrites the current record in place (unversioned statistics
    /// kinds).
    Insert {
        /// The row's table.
        table: TableKind,
        /// The row's column values, in table order.
        cells: Vec<Cell>,
    },
    /// A row removed with no history mirror: the unversioned statistics
    /// kinds and the deletion schedule, plus the hard prunes maintenance
    /// issues — snapshot records, dead entity versions (`current` or
    /// `history`, named by their `end_snapshot`), and dead tag entries.
    Delete {
        /// The row's table.
        table: TableKind,
        /// The removed row's key columns, in table order (id columns
        /// only — see [`Self::Insert`]'s `cells` for the full row shape).
        cells: Vec<Cell>,
    },
    /// An `UPDATE ... SET end_snapshot = <v>` row: the one lifecycle
    /// convention this path interprets — ends the live version (moves it
    /// to `history`). Defined only for the six versioned kinds.
    UpdateSetEnd {
        /// The row's table.
        table: TableKind,
        /// The ended row's key columns, in table order, followed by the
        /// new `end_snapshot` value.
        cells: Vec<Cell>,
    },
    /// An `UPDATE ... SET begin_snapshot = <v>` row: rebases a data
    /// file's visibility window in place. DuckLake issues it only during
    /// a delete-rewrite, against the replacement file the same
    /// transaction just inserted — any other target is a shape error.
    UpdateSetBegin {
        /// The row's table (only `ducklake_data_file`).
        table: TableKind,
        /// The rebased row's key columns, followed by the new
        /// `begin_snapshot` value.
        cells: Vec<Cell>,
    },
    /// `inline/schema`: the Arrow IPC schema for one `(table_id,
    /// schema_version)`, written once at inline-table creation, stored
    /// verbatim.
    InlineSchema {
        /// Owning table.
        table_id: u64,
        /// Schema version the layout is pinned to.
        schema_version: u64,
        /// The Arrow IPC schema message, verbatim.
        arrow_schema: Vec<u8>,
    },
    /// `inline/insert`: one Arrow record-batch chunk of inlined rows.
    /// `chunk_seq` is not carried here — translation allocates it, so
    /// several `InlineInsert`s staged in one commit against the same
    /// `(table_id, schema_version, begin_snapshot)` land as sequential
    /// chunks in stage order.
    InlineInsert {
        /// Owning table.
        table_id: u64,
        /// Schema version the chunk was written under.
        schema_version: u64,
        /// Commit snapshot of the insert, DuckLake-authored verbatim.
        begin_snapshot: u64,
        /// The chunk's first row id; later rows are dense from here.
        row_id_start: u64,
        /// Row count carried by `arrow_body`.
        row_count: u64,
        /// The user-column cells, encoded as one Arrow IPC record-batch
        /// body (opaque bytes to this layer).
        arrow_body: Vec<u8>,
    },
    /// `inline/inline_delete`: tombstones one inlined-insert row.
    InlineInlineDelete {
        /// Owning table.
        table_id: u64,
        /// The tombstoned row.
        row_id: u64,
        /// The commit snapshot the row ends at.
        end_snapshot: u64,
    },
    /// `inline/file_delete`: an inlined delete against a Parquet-file row.
    InlineFileDelete {
        /// Owning table.
        table_id: u64,
        /// Targeted data file.
        data_file_id: u64,
        /// Deleted row.
        row_id: u64,
        /// The commit snapshot the delete takes effect at.
        begin_snapshot: u64,
    },
    /// Removes one live `inline/file_delete` record — the row-grain
    /// counterpart of [`Self::InlineFileDelete`], issued when a flush has
    /// materialized that inlined deletion into a real delete file and the
    /// inlined form must go, or the row would be counted deleted twice.
    ///
    /// Row-grain rather than table-wide on purpose: DuckLake's flush
    /// happens to clear the whole table, but the operation it issues is an
    /// ordinary SQL `DELETE`, and translating it per row means a filtered
    /// one removes exactly what it matched instead of everything.
    InlineFileDeleteRemove {
        /// Owning table.
        table_id: u64,
        /// The data file the removed deletion targeted.
        data_file_id: u64,
        /// The row the removed deletion killed.
        row_id: u64,
    },
    /// Removes every `inline/insert` chunk begun at or before
    /// `flush_snapshot` for `(table_id, schema_version)`, plus the
    /// `inline/inline_delete` tombstones those chunks' rows consumed — the
    /// flushed data survives only as the backdated `ducklake_data_file`
    /// DuckLake registers through the ordinary file path.
    InlineFlushDelete {
        /// Owning table.
        table_id: u64,
        /// Schema version being flushed.
        schema_version: u64,
        /// Chunks begun at or before this snapshot are flushed.
        flush_snapshot: u64,
    },
    /// Removes every `inline/*` record for `table_id`: schema, chunks,
    /// and tombstones.
    InlineDrop {
        /// The dropped table.
        table_id: u64,
    },
    /// Removes only the `inline/schema` record for one `(table_id,
    /// schema_version)` — the superseded-schema-version cleanup a flush
    /// issues once its chunks are gone, leaving any other schema
    /// version's `inline/*` records (a newer version accumulating
    /// concurrently) untouched. Distinct from [`Self::InlineDrop`], which
    /// is table-wide (the whole-table `DROP TABLE` cascade).
    InlineSchemaDrop {
        /// Owning table.
        table_id: u64,
        /// The schema version deregistered.
        schema_version: u64,
    },
}

type CommittedRecords = Arc<Vec<read::EntityRecord>>;
type CommittedRecordCache = tokio::sync::Mutex<
    HashMap<read::EntityRecordKind, Arc<tokio::sync::OnceCell<CommittedRecords>>>,
>;

/// A malformed staged row: wrong cell count or a cell of the wrong kind
/// for its column. Never produced by a correct shim translation; this
/// path fails loudly rather than guessing.
fn corrupt_row(table: TableKind, detail: impl std::fmt::Display) -> Error {
    Error::Corruption(format!("staged row for {table:?}: {detail}"))
}

/// A staged-row transaction: one SlateDB transaction opened by
/// `begin` (crate-internal; [`crate::ffi_support::staged::staged_begin`]
/// is the entry point outside this crate), accumulating [`RowOperation`]s via
/// [`stage`](Self::stage) until [`commit`](Self::commit) translates and
/// lands them all in one atomic batch, or [`rollback`](Self::rollback)
/// discards them.
pub struct StagedTransaction {
    diagnostic_id: u64,
    db_tx: DbTransaction,
    ops: Vec<RowOperation>,
    /// The committed records at this transaction's read point, scanned once
    /// per requested kind and shared by later reads of that kind.
    committed: CommittedRecordCache,
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
    /// The `DATA_PATH` object store and its bucket-relative prefix, present
    /// when the attach supplied `META_DATA_PATH`. Index maintenance
    /// scoped-reads registered data files through it; absent it is skipped.
    data_store: Option<Arc<dyn ObjectStore>>,
    data_prefix: String,
}

impl StagedTransaction {
    /// Opens a fresh transaction at the current head. Nothing is staged
    /// yet; [`stage`](Self::stage) accumulates rows in memory only. A
    /// successful commit folds its batch into `projections` (a catalog's
    /// shared maintained-projection state).
    pub(crate) fn begin(
        db_tx: DbTransaction,
        projections: Arc<std::sync::RwLock<ProjectionCache>>,
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: String,
    ) -> Self {
        Self {
            diagnostic_id: next_diagnostic_id(),
            db_tx,
            ops: Vec::new(),
            committed: tokio::sync::Mutex::new(HashMap::new()),
            projections,
            data_store,
            data_prefix,
        }
    }

    /// As [`begin`](Self::begin), but with a throwaway, never-served
    /// projection state and no `DATA_PATH` store — for tests that drive a
    /// `StagedTransaction` directly without a `Catalog`.
    #[cfg(test)]
    pub(crate) fn begin_detached(db_tx: DbTransaction) -> Self {
        Self::begin(
            db_tx,
            Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
            None,
            String::new(),
        )
    }

    /// As [`begin_detached`](Self::begin_detached), but sharing `catalog`'s
    /// projections — for tests that commit here and then read back through
    /// the handle. A throwaway state would leave that handle holding a view
    /// the store has moved past, which a staged commit through a `Catalog`
    /// never does.
    #[cfg(test)]
    pub(crate) fn begin_detached_on(catalog: &crate::Catalog, db_tx: DbTransaction) -> Self {
        Self::begin(
            db_tx,
            Arc::clone(catalog.projections()),
            None,
            String::new(),
        )
    }

    /// As [`begin_detached`](Self::begin_detached), but reading registered
    /// files from `data_store` — for tests that exercise the file paths.
    #[cfg(test)]
    pub(crate) fn begin_detached_with_store(
        db_tx: DbTransaction,
        data_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self::begin(
            db_tx,
            Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
            Some(data_store),
            String::new(),
        )
    }

    /// Accumulates one row mutation. Nothing touches the store until
    /// [`commit`](Self::commit).
    pub fn stage(&mut self, op: RowOperation) {
        self.ops.push(op);
    }

    /// Discards every staged row without writing anything.
    pub fn rollback(self) {
        self.db_tx.rollback();
    }

    /// Snapshot records as this transaction sees them: the committed
    /// rows at its read point, minus the snapshot deletes staged so far.
    /// The expiry cascade re-reads `ducklake_snapshot` after staging its
    /// deletes (its dead-row rule is `NOT EXISTS` over the survivors), so
    /// the projection must observe the transaction's own writes.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged snapshot-delete row
    /// is malformed.
    pub async fn visible_snapshots(&self) -> Result<Vec<proto::SnapshotValue>> {
        let committed = read::scan_snapshots(ReadHandle::Tx(&self.db_tx)).await?;

        let mut deleted = BTreeSet::new();
        for op in &self.ops {
            if let RowOperation::Delete {
                table: TableKind::Snapshot,
                cells,
            } = op
            {
                let mut c = Cursor::new(TableKind::Snapshot, cells);
                deleted.insert(c.u64()?);
                c.finish()?;
            }
        }

        Ok(committed
            .into_iter()
            .filter(|s| !deleted.contains(&s.snapshot_id))
            .collect())
    }

    /// The committed records of one kind at this transaction's read point,
    /// read through `db_tx` so current and history are one consistent cut.
    async fn committed_entities(&self, kind: read::EntityRecordKind) -> Result<CommittedRecords> {
        let cell = {
            let mut committed = self.committed.lock().await;
            Arc::clone(
                committed
                    .entry(kind)
                    .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new())),
            )
        };
        let records = cell
            .get_or_try_init(|| async {
                let started = Instant::now();
                let records = read::scan_entity_kind(ReadHandle::Tx(&self.db_tx), kind).await?;
                let history_records = records
                    .iter()
                    .filter(|record| {
                        record
                            .lifecycle()
                            .is_some_and(|(_, end_snapshot)| end_snapshot.is_some())
                    })
                    .count();
                let current_records = records.len().saturating_sub(history_records);
                debug!(
                    transaction_id = self.diagnostic_id,
                    ?kind,
                    current_records,
                    history_records,
                    records = records.len(),
                    elapsed_ms = milliseconds(started.elapsed()),
                    "scanned committed entities for staged transaction"
                );
                Ok::<_, Error>(Arc::new(records))
            })
            .await?;
        Ok(Arc::clone(records))
    }

    /// The committed records of one kind, as the overlay's starting point.
    async fn committed_rows<T>(
        &self,
        kind: read::EntityRecordKind,
        extract: impl Fn(&read::EntityRecord) -> Option<T>,
    ) -> Result<Vec<T>> {
        Ok(self
            .committed_entities(kind)
            .await?
            .iter()
            .filter_map(extract)
            .collect())
    }

    /// `ducklake_data_file` rows as this transaction sees them: committed
    /// rows at its read point with its own staged rows over them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_data_files(&self) -> Result<Vec<proto::DataFileValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::File, |r| match r {
                read::EntityRecord::File(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_versioned(&self.ops, committed)
    }

    /// `ducklake_delete_file` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_delete_files(&self) -> Result<Vec<proto::DeleteFileValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::DeleteFile, |r| match r {
                read::EntityRecord::DeleteFile(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_versioned(&self.ops, committed)
    }

    /// `ducklake_column` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_columns(&self) -> Result<Vec<proto::ColumnValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::Column, |r| match r {
                read::EntityRecord::Column(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_versioned(&self.ops, committed)
    }

    /// `ducklake_table` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_tables(&self) -> Result<Vec<proto::TableValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::Table, |r| match r {
                read::EntityRecord::Table(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_versioned(&self.ops, committed)
    }

    /// `ducklake_file_column_stats` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_file_column_stats(&self) -> Result<Vec<proto::FileColumnStatsValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::FileColumnStats, |r| match r {
                read::EntityRecord::FileColumnStats(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_unversioned(&self.ops, committed)
    }

    /// `ducklake_schema` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_schemas(&self) -> Result<Vec<proto::SchemaValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::Schema, |r| match r {
                read::EntityRecord::Schema(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_versioned(&self.ops, committed)
    }

    /// `ducklake_view` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_views(&self) -> Result<Vec<proto::ViewValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::View, |r| match r {
                read::EntityRecord::View(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_versioned(&self.ops, committed)
    }

    /// `ducklake_partition_info` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_partition_info(&self) -> Result<Vec<proto::PartitionValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::Partition, |r| match r {
                read::EntityRecord::Partition(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_versioned(&self.ops, committed)
    }

    /// `ducklake_sort_info` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_sort_info(&self) -> Result<Vec<proto::SortValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::Sort, |r| match r {
                read::EntityRecord::Sort(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_versioned(&self.ops, committed)
    }

    /// `ducklake_macro` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_macros(&self) -> Result<Vec<proto::MacroValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::Macro, |r| match r {
                read::EntityRecord::Macro(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_versioned(&self.ops, committed)
    }

    /// `ducklake_table_stats` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_table_stats(&self) -> Result<Vec<proto::TableStatsValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::TableStats, |r| match r {
                read::EntityRecord::TableStats(v) => Some(*v),
                _ => None,
            })
            .await?;
        overlay::overlay_unversioned(&self.ops, committed)
    }

    /// `ducklake_table_column_stats` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_table_column_stats(&self) -> Result<Vec<proto::TableColumnStatsValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::TableColumnStats, |r| match r {
                read::EntityRecord::TableColumnStats(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_unversioned(&self.ops, committed)
    }

    /// `ducklake_column_mapping` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_mappings(&self) -> Result<Vec<proto::MappingValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::Mapping, |r| match r {
                read::EntityRecord::Mapping(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_unversioned(&self.ops, committed)
    }

    /// The `ducklake_tag` container records as this transaction sees them.
    ///
    /// A tag row is an entry inside its object's container, so the
    /// transaction's staged tag rows are folded into those containers
    /// rather than overlaid as records of their own.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_tag_containers(&self) -> Result<Vec<proto::TagValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::Tag, |r| match r {
                read::EntityRecord::Tag(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_tag_containers(&self.ops, committed)
    }

    /// The option-scope records as this transaction sees them, as
    /// `(scope_kind, scope_id, options)` triples — the shape the
    /// `ducklake_metadata` projection flattens.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_option_scopes(&self) -> Result<Vec<(u64, u64, proto::OptionScopeValue)>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::Option, |r| match r {
                read::EntityRecord::Option {
                    scope_kind,
                    scope_id,
                    value,
                } => Some((*scope_kind, *scope_id, value.clone())),
                _ => None,
            })
            .await?;
        overlay::overlay_option_scopes(&self.ops, committed)
    }

    /// The rows this transaction staged for one embedded child kind,
    /// decoded and paired with the parent each names.
    ///
    /// An embedded row rides its parent's record, and a staged child always
    /// names a parent the same batch inserts — translation refuses one that
    /// does not — so a child projection is its parents' rows plus these.
    /// Deletes need no counterpart: an embedded row is only ever removed
    /// alongside the parent that carries it, which the parent's own overlay
    /// already drops.
    fn staged_children<T>(
        &self,
        kind: TableKind,
        decode: impl Fn(&[Cell]) -> Result<T>,
    ) -> Result<Vec<T>> {
        self.ops
            .iter()
            .filter_map(|op| match op {
                RowOperation::Insert { table, cells } if *table == kind => Some(decode(cells)),
                _ => None,
            })
            .collect()
    }

    /// The `ducklake_partition_column` rows this transaction staged, each
    /// with its spec's id. See [`staged_children`](Self::staged_children).
    ///
    /// # Errors
    ///
    /// Returns an error if a staged row is malformed.
    pub fn staged_partition_columns(&self) -> Result<Vec<(u64, proto::PartitionColumn)>> {
        self.staged_children(TableKind::PartitionColumn, decode::decode_partition_column)
    }

    /// The `ducklake_file_partition_value` rows this transaction staged,
    /// each with its `(table_id, data_file_id)`.
    ///
    /// # Errors
    ///
    /// Returns an error if a staged row is malformed.
    pub fn staged_file_partition_values(
        &self,
    ) -> Result<Vec<((u64, u64), proto::FilePartitionValue)>> {
        self.staged_children(
            TableKind::FilePartitionValue,
            decode::decode_file_partition_value,
        )
    }

    /// The `ducklake_sort_expression` rows this transaction staged, each
    /// with its spec's id.
    ///
    /// # Errors
    ///
    /// Returns an error if a staged row is malformed.
    pub fn staged_sort_expressions(&self) -> Result<Vec<(u64, proto::SortExpression)>> {
        self.staged_children(TableKind::SortExpression, decode::decode_sort_expression)
    }

    /// The `ducklake_column_tag` rows this transaction staged, each with
    /// its `(table_id, column_id)`.
    ///
    /// # Errors
    ///
    /// Returns an error if a staged row is malformed.
    pub fn staged_column_tags(&self) -> Result<Vec<((u64, u64), proto::ColumnTag)>> {
        self.staged_children(TableKind::ColumnTag, decode::decode_column_tag_row)
    }

    /// The `ducklake_macro_impl` rows this transaction staged, each with
    /// its macro's id.
    ///
    /// # Errors
    ///
    /// Returns an error if a staged row is malformed.
    pub fn staged_macro_impls(&self) -> Result<Vec<(u64, proto::MacroImplementation)>> {
        self.staged_children(TableKind::MacroImpl, decode::decode_macro_impl)
    }

    /// The `ducklake_macro_parameters` rows this transaction staged, each
    /// with its `(macro_id, impl_id)`.
    ///
    /// # Errors
    ///
    /// Returns an error if a staged row is malformed.
    pub fn staged_macro_parameters(&self) -> Result<Vec<((u64, u64), proto::MacroParameter)>> {
        self.staged_children(TableKind::MacroParameters, decode::decode_macro_parameter)
    }

    /// The `ducklake_name_mapping` rows this transaction staged, each with
    /// its mapping's id.
    ///
    /// # Errors
    ///
    /// Returns an error if a staged row is malformed.
    pub fn staged_name_mappings(&self) -> Result<Vec<(u64, proto::NameMapping)>> {
        self.staged_children(TableKind::NameMapping, decode::decode_name_mapping)
    }

    /// The `schema_version` records as this transaction sees them, as
    /// `(table_id, begin_snapshot, schema_version)` triples: the committed
    /// records with the transaction's own inserts and deletes over them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_schema_version_records(&self) -> Result<Vec<(u64, u64, u64)>> {
        let mut records = read::scan_schema_versions(ReadHandle::Tx(&self.db_tx)).await?;
        for op in &self.ops {
            match op {
                RowOperation::Insert { table, cells } if *table == TableKind::SchemaVersions => {
                    records.push(decode::decode_schema_version_row(cells)?);
                }
                RowOperation::Delete { table, cells } if *table == TableKind::SchemaVersions => {
                    let (table_id, begin_snapshot, _) = decode::decode_schema_version_row(cells)?;
                    records.retain(|(t, b, _)| !(*t == table_id && *b == begin_snapshot));
                }
                _ => {}
            }
        }

        Ok(records)
    }

    /// `ducklake_files_scheduled_for_deletion` rows as this transaction
    /// sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_scheduled_deletions(&self) -> Result<Vec<proto::GcFileValue>> {
        let committed = self
            .committed_rows(read::EntityRecordKind::GcFile, |r| match r {
                read::EntityRecord::GcFile(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_unversioned(&self.ops, committed)
    }

    /// Translates every staged row and lands them in one atomic batch.
    ///
    /// A commit with a `ducklake_snapshot` insert mints that snapshot and
    /// advances head. A commit **without** one is a maintenance commit —
    /// snapshot expiry / file cleanup, which DuckLake runs without
    /// minting a snapshot — and lands head-preserving: reclamation
    /// deletes only, no new snapshot record, `sys/head` untouched.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] or [`Error::Corruption`] if the
    /// staged rows are malformed, mutate entities without the required
    /// `ducklake_snapshot` / `ducklake_snapshot_changes` pair, or expire
    /// the head snapshot. Returns [`Error::CommitConflict`] — **never
    /// retried internally** — if a concurrent commit advanced the head
    /// first; the store is left unchanged by the loser.
    #[allow(clippy::too_many_lines)]
    pub async fn commit(self) -> Result<SnapshotId> {
        let started = Instant::now();
        let Self {
            diagnostic_id,
            db_tx,
            ops,
            committed: _,
            projections,
            data_store,
            data_prefix,
        } = self;
        let staged_rows = ops.len();
        let mut uses_inline_chunk_directory = ops
            .iter()
            .any(|operation| matches!(operation, RowOperation::InlineInsert { .. }));
        let mut phases = CommitPhases::default();

        let phase_started = Instant::now();
        let base = match commit::head_view_for(&db_tx, &projections).await {
            Ok(base) => base,
            Err(err) => {
                db_tx.rollback();
                return Err(err);
            }
        };
        phases.head_view = phase_started.elapsed();
        let base_ref: &CatalogSnapshot = &base;
        // Read before any write in this commit is staged: `InlineFlushDelete`
        // /`InlineDrop` name a table, not keys, and resolve against
        // `db_tx`'s current state exactly like `base` above.
        let phase_started = Instant::now();
        let inline_writes = match translate_inline(&db_tx, &ops).await {
            Ok(writes) => writes,
            Err(err) => {
                db_tx.rollback();
                return Err(err);
            }
        };
        phases.inline = phase_started.elapsed();

        let mints_snapshot = mints_snapshot(&ops);
        // Maintain equality-index entries for any data file this commit
        // registered on an indexed table, by scoped-reading it from
        // `DATA_PATH`. Gated: a no-op unless a live index covers the file's
        // table, so non-indexed writes are untouched. A Parquet file on an
        // indexed table with no store to read it aborts the commit rather
        // than under-covering the index. Staged before the translation so a
        // poisoned definition rides the writes it produces.
        let store = data_store.as_ref();
        let phase_started = Instant::now();
        let entries =
            match stage_index_maintenance(&db_tx, base_ref, &ops, store, &data_prefix).await {
                Ok(entries) => entries,
                Err(err) => {
                    db_tx.rollback();
                    return Err(err);
                }
            };
        phases.index_maintenance = phase_started.elapsed();
        let StagedEntries {
            poisoned,
            deferred,
            bytes: entry_bytes,
            uses_inline_chunk_directory: repaired_inline_chunk_directory,
            metrics: index_metrics,
        } = entries;
        phases.index_metrics = index_metrics;
        uses_inline_chunk_directory |= repaired_inline_chunk_directory;

        let phase_started = Instant::now();
        match translate_batch(base_ref, &ops, &poisoned, &deferred, mints_snapshot) {
            Ok((result_id, mut writes)) => {
                phases.translate = phase_started.elapsed();
                let phase_started = Instant::now();
                let staged_bytes = match stage_batch(
                    &db_tx,
                    &mut writes,
                    inline_writes,
                    result_id,
                    entry_bytes,
                    uses_inline_chunk_directory,
                )
                .await
                {
                    Ok(staged) => staged,
                    Err(err) => {
                        db_tx.rollback();
                        return Err(err);
                    }
                };
                phases.stage = phase_started.elapsed();
                // The same landing the verb path takes: one durable write,
                // one head-race classification, one projection refresh.
                // This path never retries the loss — DuckLake authored the
                // rows, so re-driving them is DuckLake's call.
                //
                // Spawned, not awaited inline: everything above is staged
                // in memory and a dropped future discards it, but the write
                // below cannot be retracted once issued, so a host
                // interrupt races the wait for it rather than the write
                // itself.
                let head_before = base_ref.snapshot.snapshot_id;
                let phase_started = Instant::now();
                let landed = commit::commit_batch_off_task(
                    db_tx,
                    head_before,
                    result_id,
                    writes,
                    staged_bytes,
                    Arc::clone(&base),
                    projections,
                )
                .await?;
                phases.land = phase_started.elapsed();
                match landed {
                    commit::Landed::Committed(commit_timings) => {
                        staged_landed(
                            diagnostic_id,
                            result_id,
                            staged_rows,
                            staged_bytes,
                            &phases,
                            commit_timings,
                            started,
                        );
                        Ok(SnapshotId::new(result_id))
                    }
                    commit::Landed::LostRace => Err(staged_lost_race(result_id, staged_rows)),
                }
            }
            Err(err) => {
                db_tx.rollback();
                Err(err)
            }
        }
    }
}

/// The writes a batch carries and the snapshot id it results in. A
/// snapshot-minting commit translates its operations and appends its own
/// snapshot record; a maintenance one reuses the standing id.
fn translate_batch(
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    poisoned: &[u64],
    deferred: &[u64],
    mints_snapshot: bool,
) -> Result<(u64, Vec<commit::StagedWrite>)> {
    if !mints_snapshot {
        return translate_maintenance(base, ops).map(|writes| (base.snapshot.snapshot_id, writes));
    }

    let (new_id, mut writes, snap) = translate(base, ops, poisoned, deferred)?;
    // Derived before the snapshot record joins the batch: the changelog
    // names `current` keys, and that record is not.
    let changelog = commit::changelog_writes(new_id, &writes);
    writes.push((
        Key::Snapshot {
            snapshot_id: new_id,
        }
        .encode(),
        Some(value::encode_value(&snap)),
    ));
    writes.extend(changelog);
    Ok((new_id, writes))
}

/// Stamps the head, folds in the inline writes, and stages the whole batch
/// onto `db_tx`, returning what the batch weighs — `entry_bytes` included,
/// since index entries stage directly and never join `writes`.
///
/// Every batch stamps the head, a maintenance one included, where it reuses
/// the standing snapshot id and only the batch count moves. That makes
/// `sys/head` the one conflict anchor every batch shares, and the stamp a
/// reader validates its cut against.
async fn stage_batch(
    db_tx: &DbTransaction,
    writes: &mut Vec<commit::StagedWrite>,
    inline_writes: Vec<commit::StagedWrite>,
    result_id: u64,
    entry_bytes: u64,
    uses_inline_chunk_directory: bool,
) -> Result<StagedBytes> {
    writes.push(commit::head_write(db_tx, result_id).await?);
    writes.extend(inline_writes);
    if uses_inline_chunk_directory
        && let Some(stamp) =
            commit::format_stamp_to(db_tx, commit::FORMAT_WITH_INLINE_CHUNK_DIRECTORY).await?
    {
        writes.push(stamp);
    }
    let mut staged = commit::stage_writes(db_tx, writes)?;
    staged.0 = staged.0.saturating_add(entry_bytes);
    Ok(staged)
}

/// One landed staged commit's summary event.
fn staged_landed(
    transaction_id: u64,
    result_id: u64,
    staged_rows: usize,
    staged_bytes: StagedBytes,
    phases: &CommitPhases,
    commit_timings: commit::CommitTimings,
    started: Instant,
) {
    debug!(
        transaction_id,
        snapshot = result_id,
        staged_rows,
        staged_bytes = staged_bytes.0,
        head_view_ms = milliseconds(phases.head_view),
        inline_ms = milliseconds(phases.inline),
        index_maintenance_ms = milliseconds(phases.index_maintenance),
        index_delete_derivation_ms = milliseconds(phases.index_metrics.deletion_derivation),
        index_add_derivation_ms = milliseconds(phases.index_metrics.addition_derivation),
        index_probe_window_ms = milliseconds(phases.index_metrics.probe_window),
        index_probe_service_ms = milliseconds(phases.index_metrics.probe_service),
        index_stage_ms = milliseconds(phases.index_metrics.staging),
        index_encode_ms = milliseconds(phases.index_metrics.scoped_read.encode_duration),
        index_parquet_read_ms = milliseconds(phases.index_metrics.scoped_read.range_duration),
        index_additions = phases.index_metrics.additions,
        index_deletions = phases.index_metrics.deletions,
        index_unique_probes = phases.index_metrics.unique_probes,
        index_probe_hits = phases.index_metrics.probe_hits,
        index_probe_misses = phases.index_metrics.probe_misses,
        index_probe_peak_in_flight = phases.index_metrics.probe_peak_in_flight,
        index_probes_completed_during_deletions =
            phases.index_metrics.probes_completed_during_deletions,
        index_metadata_hits = phases.index_metrics.scoped_read.metadata_hits,
        index_metadata_misses = phases.index_metrics.scoped_read.metadata_misses,
        index_range_fetches = phases.index_metrics.scoped_read.range_fetches,
        index_ranges = phases.index_metrics.scoped_read.ranges,
        index_range_bytes = phases.index_metrics.scoped_read.range_bytes,
        index_files = phases.index_metrics.scoped_read.parquet_files,
        index_inline_chunks = phases.index_metrics.scoped_read.inline_chunks,
        index_arrow_batches = phases.index_metrics.scoped_read.arrow_batches,
        translate_ms = milliseconds(phases.translate),
        stage_ms = milliseconds(phases.stage),
        land_ms = milliseconds(phases.land),
        durable_ms = milliseconds(commit_timings.durable),
        projection_ms = milliseconds(commit_timings.projection),
        elapsed_ms = milliseconds(started.elapsed()),
        "staged commit landed"
    );
}

/// The lost-race error for a staged commit, logged as it is built:
/// DuckLake's own loop re-drives the loser, so the log line is the only
/// visible trace of the race.
fn staged_lost_race(result_id: u64, staged_rows: usize) -> Error {
    debug!(
        attempted_snapshot = result_id,
        staged_rows, "staged commit lost a write-write race; DuckLake re-drives"
    );
    Error::CommitConflict(format!(
        "a concurrent commit changed state this one read or wrote \
         (attempted snapshot {result_id}); staged-row commits are never \
         retried internally"
    ))
}

/// Applies every op onto a clone of `base`, then diffs the two exactly as
/// a verb-path commit diffs its closure's output.
fn translate(
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    poisoned: &[u64],
    deferred: &[u64],
) -> Result<(u64, Vec<commit::StagedWrite>, proto::SnapshotValue)> {
    let snapshot = build_snapshot_value(ops)?;
    let new_id = snapshot.snapshot_id;

    // DuckLake mints the id from the head it read, so an id at or below the
    // head this commit lands on means another commit landed in between.
    // Landing it would overwrite a snapshot record and move the head
    // backwards, and every write below stamps `new_id` as the version it
    // begins at — so refuse it, as the lost race it is. Reporting anything
    // else would be a wire-contract bug rather than a wording one: DuckLake
    // re-drives on the text of the error, and a loser it does not recognize
    // is a transaction it abandons instead of re-running against the head
    // that won.
    if new_id <= base.snapshot.snapshot_id {
        return Err(staged_lost_race(new_id, ops.len()));
    }

    // Ends and deletes apply before inserts, independent of DuckLake's
    // emit order: a rename ends the old version and inserts a new one
    // under the same id, and the insert must win their shared `current` key —
    // an end applied afterward would delete the id and erase the new row.
    // Begin-rebases apply last: their target is the row an insert in this
    // same commit created. Inline ops are skipped here entirely —
    // `commit` translates them separately via `translate_inline`, since
    // `CatalogSnapshot` has no notion of inlined rows to diff.
    let mut state = base.clone();
    let mut children = collect_child_rows(ops)?;
    let mut direct = Vec::new();
    let hard_deleted = collect_hard_deletes(ops)?;

    for op in ops {
        if !is_inline_op(op)
            && !matches!(
                op,
                RowOperation::Insert { .. } | RowOperation::UpdateSetBegin { .. }
            )
        {
            apply_op(
                base,
                &mut state,
                op,
                new_id,
                &mut children,
                &mut direct,
                &hard_deleted,
            )?;
        }
    }

    for op in ops {
        if matches!(op, RowOperation::Insert { .. }) {
            apply_op(
                base,
                &mut state,
                op,
                new_id,
                &mut children,
                &mut direct,
                &hard_deleted,
            )?;
        }
    }

    for op in ops {
        if matches!(op, RowOperation::UpdateSetBegin { .. }) {
            apply_op(
                base,
                &mut state,
                op,
                new_id,
                &mut children,
                &mut direct,
                &hard_deleted,
            )?;
        }
    }

    refuse_orphaned_children(&children)?;

    // DuckLake authors column ids itself, so its inserts advance no
    // counter; float each table's field-id counter above every live id so
    // a later verb-path `add_column` can never re-allocate one.
    for (table_id, columns) in &state.columns {
        let Some(max_id) = columns.keys().max() else {
            continue;
        };
        if let Some(table) = state.tables.get_mut(table_id)
            && table.next_column_id <= *max_id
        {
            table.next_column_id = max_id + 1;
        }
    }

    apply_poison(&mut state, poisoned);
    apply_deferred_maintenance(base, &mut state, deferred, new_id);

    let mut writes = commit::diff_writes(base, &state, new_id);
    writes.extend(direct);
    // The `ducklake_schema_versions` rows this commit staged, as records of
    // their own: `snapshot` carries them too, but only until expiry deletes
    // it, and the files they describe outlive that.
    for table_id in &snapshot.schema_changed_table_ids {
        writes.push(commit::schema_version_write(
            *table_id,
            new_id,
            snapshot.schema_version,
        ));
    }

    Ok((new_id, writes, snapshot))
}

/// Refuses child rows left over after every parent was applied. An
/// embedded child whose parent is not in the same commit has nowhere to
/// live, and dropping it silently would lose a partition column or a macro
/// parameter DuckLake believes it wrote.
fn refuse_orphaned_children(children: &ChildRows) -> Result<()> {
    let orphans = [
        (
            children.partition_columns.is_empty(),
            TableKind::PartitionColumn,
            "partition_column rows without a matching partition_info insert in this commit",
        ),
        (
            children.sort_expressions.is_empty(),
            TableKind::SortExpression,
            "sort_expression rows without a matching sort_info insert in this commit",
        ),
        (
            children.file_partition_values.is_empty(),
            TableKind::FilePartitionValue,
            "file_partition_value rows without a matching data_file insert in this commit",
        ),
        (
            children.macro_implementations.is_empty(),
            TableKind::MacroImpl,
            "macro_impl rows without a matching macro insert in this commit",
        ),
        (
            children.macro_parameters.is_empty(),
            TableKind::MacroParameters,
            "macro_parameters rows without a matching macro_impl in this commit",
        ),
        (
            children.name_mappings.is_empty(),
            TableKind::NameMapping,
            "name_mapping rows without a matching column_mapping insert in this commit",
        ),
    ];
    for (applied, table, detail) in orphans {
        if !applied {
            return Err(corrupt_row(table, detail));
        }
    }
    Ok(())
}

/// Translates a head-preserving maintenance commit: snapshot expiry and
/// file cleanup arrive with no `ducklake_snapshot` insert (DuckLake mints
/// no snapshot for them), so nothing advances head and no snapshot record
/// is written. Only reclamation-shaped operations are legal — raw
/// deletes, schedule inserts, and the inline drops a dead table's cleanup
/// issues; any entity insert or lifecycle update without a snapshot row
/// is a constraint violation (DuckLake always mints a snapshot for real
/// catalog changes).
fn translate_maintenance(
    base: &CatalogSnapshot,
    ops: &[RowOperation],
) -> Result<Vec<commit::StagedWrite>> {
    let head = base.snapshot.snapshot_id;

    let mut state = base.clone();
    let mut children = ChildRows::default();
    let mut direct = Vec::new();
    let hard_deleted = collect_hard_deletes(ops)?;
    for op in ops {
        let allowed = matches!(
            op,
            RowOperation::Delete { .. }
                | RowOperation::Insert {
                    table: TableKind::FilesScheduledForDeletion
                        // An option write mints no snapshot by design —
                        // DuckLake writes `ducklake_metadata` within its
                        // metadata connection, outside the protocol — so it
                        // arrives here exactly as reclamation does.
                        | TableKind::Metadata,
                    ..
                }
        ) || is_inline_op(op);
        if !allowed {
            return Err(Error::Constraint(
                "a staged commit without a ducklake_snapshot insert may only reclaim state \
                 or set options (maintenance deletes, deletion-schedule inserts, and \
                 ducklake_metadata writes)"
                    .to_string(),
            ));
        }
        apply_op(
            base,
            &mut state,
            op,
            head,
            &mut children,
            &mut direct,
            &hard_deleted,
        )?;
    }

    let mut writes = commit::diff_writes(base, &state, head);
    writes.extend(direct);
    Ok(writes)
}
