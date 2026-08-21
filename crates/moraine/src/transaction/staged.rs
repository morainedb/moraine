//! The staged-row commit path: DuckLake authors rows over the ABI instead
//! of driving [`crate::transaction::Transaction`]'s verbs, and this module
//! turns the accumulated rows into one atomic store write.
//!
//! Three rules bound this path: every value DuckLake supplies is stored
//! **verbatim** except one interpreted convention (an `UPDATE` setting
//! `end_snapshot` becomes current-delete + history-write); one commit is one
//! atomic batch, reusing [`super::commit::diff_touched`]; and a lost race at
//! commit is **never retried** — DuckLake authored the ids and counters in
//! the batch. The loser's error always carries the substring `conflict`
//! (via [`Error::CommitConflict`]'s `Display`), the wire contract
//! DuckLake's retry loop scans for.
//!
//! Translation applies every staged row to a cloned working
//! [`CatalogSnapshot`], then diffs it against the unmodified base exactly
//! as a verb-path commit diffs its closure's output. An `UPDATE ... SET
//! end_snapshot` row's value is validated, not trusted: DuckLake always
//! sets it to this commit's own new snapshot id — the value `diff_touched`
//! stamps on its own — so a mismatch is drift caught loudly.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use moraine_wal::{CommitOutcome, Envelope, Overlay, SlotLog};
use slatedb::DbReader;
use tracing::debug;

use crate::{
    catalog::{
        CatalogSnapshot, ColumnInfo, IndexId, IndexInfo, SnapshotId, TableId,
        inline::{InlineScanKind, materialize_inline_rows},
    },
    data_file::{self, DataStore},
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        inline as store_inline,
        key::{EntityKey, InlineKey, InlineOperation, Key},
        proto, read, value,
    },
    telemetry::milliseconds,
    transaction::{
        commit::{self, StagedWrite},
        index_maintenance::{
            IndexMaintenanceMetrics, ProbeHandle, StagedEntries, StagedIndexEntry,
            plan_index_entry_stream,
        },
        slot_commit::{HeadCache, SlotHead, commit_from, release_reader},
    },
};

mod apply;
mod decode;
#[cfg(feature = "leader")]
pub(crate) mod forward;
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

/// A malformed staged row: wrong cell count or a cell of the wrong kind
/// for its column. Never produced by a correct shim translation; this
/// path fails loudly rather than guessing.
fn corrupt_row(table: TableKind, detail: impl std::fmt::Display) -> Error {
    Error::Corruption(format!("staged row for {table:?}: {detail}"))
}

/// What a staged transaction reads through and commits against: a pinned
/// slot-log head.
#[allow(clippy::large_enum_variant)]
pub(crate) enum StagedBacking {
    /// A pinned slot-log head: reads resolve over the reader overlaid with the
    /// unfolded tail, and a commit races one slot at `head.next_sequence`.
    /// Boxed: the head carries a full catalog view, which would otherwise
    /// dominate every future holding a staged transaction.
    Slots {
        head: Box<SlotHead>,
        reader: Arc<DbReader>,
        slots: SlotLog,
    },
}

impl StagedBacking {
    /// A slot-log backing over a pinned head, for the leader's per-session
    /// assembly (it races through the leader's funnel, not `commit_slots`).
    #[cfg(feature = "leader")]
    pub(crate) fn slots(head: Box<SlotHead>, reader: Arc<DbReader>, slots: SlotLog) -> Self {
        Self::Slots {
            head,
            reader,
            slots,
        }
    }

    /// The handle every scan and point read routes through: the reader the
    /// pinned head materialized from, overlaid with the unfolded tail.
    fn scan_handle(&self) -> ReadHandle<'_> {
        match self {
            Self::Slots { head, reader, .. } => head.handle(ReadHandle::Reader(reader)),
        }
    }

    /// The pinned head's snapshot id — the floor a forwarded commit's ambiguous
    /// outcome is resolved from (the landed snapshot is above it).
    #[cfg(feature = "leader")]
    fn head_floor(&self) -> u64 {
        match self {
            Self::Slots { head, .. } => head.view.snapshot.snapshot_id,
        }
    }

    /// Releases the reader the pinned head opened past a truncation, if any —
    /// the forwarded path's early return does not reach `commit_slots`, which
    /// otherwise owns this.
    #[cfg(feature = "leader")]
    async fn release_head(&self) {
        match self {
            Self::Slots { head, .. } => release_reader(head.reader.as_ref()).await,
        }
    }

    /// The uniqueness-probe handle: the store overlaid with the unfolded tail.
    fn probe(&self) -> ProbeHandle<'_> {
        match self {
            Self::Slots { head, reader, .. } => ProbeHandle::Overlaid {
                store: head.handle(ReadHandle::Reader(reader)),
                overlay: &head.overlay,
            },
        }
    }

    /// The unfolded tail to overlay on raw `inline/*` scans, which the catalog
    /// view does not model. `Some` for the overlay-accepting scan functions
    /// this feeds.
    #[allow(clippy::unnecessary_wraps)]
    fn scan_overlay(&self) -> Option<&Overlay> {
        match self {
            Self::Slots { head, .. } => Some(&head.overlay),
        }
    }

    /// The handle and unfolded-tail overlay for a committed-records scan: the
    /// folded store overlaid with the tail the pinned head carries.
    fn committed_scan(&self) -> (ReadHandle<'_>, Option<&Overlay>) {
        match self {
            Self::Slots { head, reader, .. } => {
                (head.handle(ReadHandle::Reader(reader)), Some(&head.overlay))
            }
        }
    }
}

/// A staged-row transaction: one commit backing opened by `begin`
/// (crate-internal; [`crate::ffi_support::staged::staged_begin`] is the entry
/// point outside this crate), accumulating [`RowOperation`]s via
/// [`stage`](Self::stage) until [`commit`](Self::commit) translates and
/// lands them all in one atomic batch, or [`rollback`](Self::rollback)
/// discards them.
pub struct StagedTransaction {
    backing: StagedBacking,
    ops: Vec<RowOperation>,
    /// The committed records at this transaction's read point, scanned once
    /// on the first `visible_*` call and shared by every later one. The head
    /// the backing pins is fixed, so a metadata population issuing one
    /// `visible_*` call per kind reads them all at one consistent cut.
    committed: tokio::sync::OnceCell<Arc<Vec<read::EntityRecord>>>,
    /// The `DATA_PATH` object store and its bucket-relative prefix, present
    /// when the attach supplied `META_DATA_PATH`. Index maintenance
    /// scoped-reads registered data files through it; absent it is skipped.
    data_store: Option<DataStore>,
    data_prefix: String,
    /// Where this transaction's scoped reads of registered data files are
    /// tallied, shared with the handle that opened it.
    data_reads: Arc<data_file::DataStoreCounters>,
    /// The handle's projection cache, for the format floor and the inline
    /// chunk-directory completeness this commit's translation consults.
    projections: Arc<std::sync::RwLock<crate::catalog::projection::ProjectionCache>>,
    /// The handle's head cache, updated on a successful commit so a read on the
    /// same handle sees this write regardless of the refresh window.
    head_cache: HeadCache,
    /// The shared contention counters this transaction arms forwarding through:
    /// a lost race increments `races_lost`, which the next transaction's
    /// forwarding trigger reads.
    contention: Arc<crate::transaction::slot_commit::ContentionCounters>,
    /// Set when a lost race armed forwarding and a leader is reachable: this
    /// transaction forwards its commit and, on an unreachable leader, ages the
    /// endpoint and falls back to a fresh direct attempt.
    #[cfg(feature = "leader")]
    forward: Option<forward::Forward>,
}

impl StagedTransaction {
    /// Opens a fresh transaction over a pinned slot-log head; a successful
    /// commit races one slot at `head.next_sequence`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin_slots(
        head: SlotHead,
        reader: Arc<DbReader>,
        slots: SlotLog,
        data_store: Option<DataStore>,
        data_prefix: String,
        head_cache: HeadCache,
        contention: Arc<crate::transaction::slot_commit::ContentionCounters>,
        data_reads: Arc<data_file::DataStoreCounters>,
        projections: Arc<std::sync::RwLock<crate::catalog::projection::ProjectionCache>>,
    ) -> Self {
        Self {
            backing: StagedBacking::Slots {
                head: Box::new(head),
                reader,
                slots,
            },
            ops: Vec::new(),
            committed: tokio::sync::OnceCell::new(),
            data_store,
            data_prefix,
            data_reads,
            projections,
            head_cache,
            contention,
            #[cfg(feature = "leader")]
            forward: None,
        }
    }

    /// Marks this transaction forwarded: its commit routes through `forward`'s
    /// leader, falling back to a direct race only if the leader is unreachable.
    #[cfg(feature = "leader")]
    pub(crate) fn forwarded(mut self, forward: forward::Forward) -> Self {
        self.forward = Some(forward);
        self
    }

    /// Accumulates one row mutation. Nothing touches the store until
    /// [`commit`](Self::commit).
    pub fn stage(&mut self, op: RowOperation) {
        self.ops.push(op);
    }

    /// The tables this transaction registered data files on, for a caller
    /// that warms their metadata before the commit lands.
    #[must_use]
    pub fn tables_with_staged_data_files(&self) -> Vec<TableId> {
        let mut tables: Vec<TableId> = self
            .ops
            .iter()
            .filter_map(|op| match op {
                RowOperation::Insert {
                    table: TableKind::DataFile,
                    cells,
                } => decode::decode_data_file(cells).ok(),
                _ => None,
            })
            .map(|file| TableId::new(file.table_id))
            .collect();
        tables.sort_unstable();
        tables.dedup();

        tables
    }

    /// Discards every staged row without writing anything. A slot-backed
    /// transaction stages only in memory until [`commit`](Self::commit), so a
    /// rollback drops its buffered rows and the pinned head with no store
    /// write to undo.
    pub fn rollback(self) {
        let StagedBacking::Slots { .. } = self.backing;
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
        let committed = match &self.backing {
            StagedBacking::Slots { head, reader, .. } => {
                read::scan_snapshots_overlaid(
                    head.handle(ReadHandle::Reader(reader)),
                    Some(&head.overlay),
                )
                .await?
            }
        };

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

    /// The committed entity records at this transaction's read point,
    /// `current` and `history` together — the same pair a `dump_*` scans,
    /// read through `db_tx` so the committed half and the staged half are
    /// one consistent cut. Scanned once and shared: a metadata population
    /// inside a write transaction asks for one kind after another.
    async fn committed_entities(&self) -> Result<&Arc<Vec<read::EntityRecord>>> {
        self.committed
            .get_or_try_init(|| async {
                let started = std::time::Instant::now();
                let (handle, overlay) = self.backing.committed_scan();
                let mut records = read::scan_current_entities_overlaid(handle, overlay).await?;
                let current_records = records.len();
                records.extend(read::scan_history_entities_overlaid(handle, overlay).await?);
                debug!(
                    current_records,
                    history_records = records.len().saturating_sub(current_records),
                    records = records.len(),
                    elapsed_ms = milliseconds(started.elapsed()),
                    "scanned committed entities for staged transaction"
                );
                Ok(Arc::new(records))
            })
            .await
    }

    /// The committed records of one kind, as the overlay's starting point.
    async fn committed_rows<T>(
        &self,
        extract: impl Fn(&read::EntityRecord) -> Option<T>,
    ) -> Result<Vec<T>> {
        Ok(self
            .committed_entities()
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
            .committed_rows(|r| match r {
                read::EntityRecord::File(v) => Some(v.clone()),
                _ => None,
            })
            .await?;
        overlay::overlay_versioned(&self.ops, committed)
    }

    /// `ducklake_data_file` rows as this transaction sees them, for a reader
    /// filtering to the versions live at `filter_snapshot`.
    ///
    /// `filter_snapshot` selects only how much of the store a scan must
    /// cover — `current` alone, or `current` and `history` — and this
    /// transaction's committed scan is cached whole either way, so it changes
    /// nothing here. The caller applies the version filter it wants.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_data_files_live_at(
        &self,
        filter_snapshot: Option<u64>,
    ) -> Result<Vec<proto::DataFileValue>> {
        let _ = filter_snapshot;
        self.visible_data_files().await
    }

    /// `ducklake_delete_file` rows as this transaction sees them.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged row is malformed.
    pub async fn visible_delete_files(&self) -> Result<Vec<proto::DeleteFileValue>> {
        let committed = self
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
            .committed_rows(|r| match r {
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
        let (handle, overlay) = self.backing.committed_scan();
        let mut records = read::scan_schema_versions_overlaid(handle, overlay).await?;
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
            .committed_rows(|r| match r {
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
    pub async fn commit(self) -> Result<SnapshotId> {
        self.commit_reporting()
            .await
            .map(|report| report.snapshot_id)
    }

    /// As [`commit`](Self::commit), also naming the deferred-maintenance
    /// indexes this commit left awaiting repair.
    ///
    /// # Errors
    ///
    /// As [`commit`](Self::commit).
    pub async fn commit_reporting(self) -> Result<CommitReport> {
        let started = std::time::Instant::now();
        let Self {
            backing,
            ops,
            committed: _,
            data_store,
            data_prefix,
            data_reads,
            projections,
            head_cache,
            contention,
            #[cfg(feature = "leader")]
            forward,
        } = self;
        let staged_rows = ops.len();

        // A forwarded transaction commits through the leader; only an
        // unreachable leader retreats to a fresh direct attempt below, so a
        // transaction's id never rides both paths.
        #[cfg(feature = "leader")]
        if let Some(forward) = &forward {
            match forward::forward_commit(forward, &ops, backing.head_floor()).await {
                forward::Forwarded::Committed(id) => {
                    // The commit advanced the shared log through the leader; the
                    // handle's cached head no longer reflects it, so drop it and
                    // let the next read or transaction re-materialize the tail.
                    head_cache.invalidate();
                    backing.release_head().await;
                    // The leader assembled the batch, so this process never saw
                    // which definitions it deferred.
                    return Ok(CommitReport {
                        snapshot_id: id,
                        deferred_indexes: Vec::new(),
                    });
                }
                forward::Forwarded::Surface(err) => {
                    backing.release_head().await;
                    return Err(err);
                }
                forward::Forwarded::FallBack => {}
            }
        }

        let read_metrics = Arc::new(data_file::ScopedReadMetrics::reporting_to(data_reads));
        let assembled = assemble(
            &backing,
            &ops,
            data_store.as_ref(),
            &data_prefix,
            &projections,
            read_metrics,
            None,
        )
        .await;

        match backing {
            StagedBacking::Slots { head, slots, .. } => {
                commit_slots(
                    *head,
                    slots,
                    assembled,
                    staged_rows,
                    started,
                    &head_cache,
                    &contention,
                )
                .await
            }
        }
    }
}

/// Everything one staged commit assembles before racing its slot.
/// What a landed commit produced.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitReport {
    /// The snapshot the commit's rows are visible at.
    pub snapshot_id: SnapshotId,
    /// Deferred-maintenance indexes this commit left `Maintaining`.
    pub deferred_indexes: Vec<IndexId>,
}

pub(crate) struct Assembly {
    /// The snapshot id a successful commit reports: the minted id, or the
    /// unchanged head for a maintenance commit.
    pub(crate) result_id: u64,
    /// The full batch to land: entity diff, index entries, inline writes, and
    /// (for a minting commit) the snapshot record and head advance.
    pub(crate) writes: Vec<StagedWrite>,
    /// The transaction id stamped into the minted snapshot and carried by the
    /// slot envelope.
    pub(crate) transaction_id: Option<[u8; 16]>,
    /// The commit's classification string, for the slot envelope a lost race
    /// judges against; empty for a maintenance commit.
    pub(crate) changes_made: String,
    /// Definitions this commit left awaiting deferred repair, reported back so
    /// the caller can schedule it.
    pub(crate) deferred_indexes: Vec<IndexId>,
    /// Key and value bytes the index entries staged, deletes included.
    pub(crate) index_bytes: u64,
    /// What index maintenance spent producing those entries.
    pub(crate) index_metrics: IndexMaintenanceMetrics,
}

/// Translates every staged row into the atomic batch the slot commit lands.
/// Reads route through the pinned slot-log head overlaid with its unfolded
/// tail.
pub(crate) async fn assemble(
    backing: &StagedBacking,
    ops: &[RowOperation],
    data_store: Option<&DataStore>,
    data_prefix: &str,
    projections: &std::sync::RwLock<crate::catalog::projection::ProjectionCache>,
    read_metrics: Arc<data_file::ScopedReadMetrics>,
    transaction_id: Option<[u8; 16]>,
) -> Result<Assembly> {
    let handle = backing.scan_handle();
    let overlay = backing.scan_overlay();
    let base: Arc<CatalogSnapshot> = match backing {
        StagedBacking::Slots { head, .. } => Arc::new(head.view.clone()),
    };
    let base_ref: &CatalogSnapshot = &base;

    // A slot-backed commit stamps a transaction id — the caller's own for a
    // forwarded commit the client must resolve by identity, else a fresh one —
    // so a lost race resolves by identity and a landed snapshot survives folding
    // for the dedup scan.
    let transaction_id = Some(transaction_id.unwrap_or_else(|| uuid::Uuid::new_v4().into_bytes()));

    // Read before any write is staged: `InlineFlushDelete`/`InlineDrop` name a
    // table, not keys, and resolve against the pre-commit state exactly like
    // `base`.
    let inline_writes = translate_inline(handle, overlay, projections, ops).await?;

    let mints_snapshot = ops.iter().any(|op| {
        matches!(
            op,
            RowOperation::Insert {
                table: TableKind::Snapshot,
                ..
            }
        )
    });

    // Maintain equality-index entries for any data file this commit registered
    // on an indexed table, by scoped-reading it from `DATA_PATH`. Gated: a
    // no-op unless a live index covers the file's table. Planned before the
    // translation so a poisoned definition rides the writes it produces, and
    // returned as writes rather than staged so the slot envelope carries them.
    let StagedEntries {
        writes: index_writes,
        poisoned,
        deferred,
        locator_writes,
        bytes: index_bytes,
        metrics: index_metrics,
    } = stage_index_maintenance(
        backing.probe(),
        base_ref,
        ops,
        data_store,
        data_prefix,
        read_metrics,
    )
    .await?;

    let (result_id, mut writes, changes_made) = if mints_snapshot {
        let (new_id, mut writes, mut snap) = translate(base_ref, ops, &poisoned, &deferred)?;
        snap.transaction_id = transaction_id.map(|id| id.to_vec());
        let changes_made = snap.changes_made.clone();
        writes.push((
            Key::Snapshot {
                snapshot_id: new_id,
            }
            .encode(),
            Some(value::encode_value(&snap)),
        ));
        writes.push(commit::head_stamp(new_id, base_ref.batch_seq));
        (new_id, writes, changes_made)
    } else {
        let mut writes = translate_maintenance(base_ref, ops)?;
        // A maintenance batch reuses the snapshot id, so the batch count is
        // the only thing that says the store moved.
        writes.push(commit::head_stamp(
            base_ref.snapshot.snapshot_id,
            base_ref.batch_seq,
        ));
        (base_ref.snapshot.snapshot_id, writes, String::new())
    };

    // Directory repairs go ahead of the inline writes, so a flush draining a
    // repaired chunk still deletes its locator. A maintenance-only commit
    // carries them too, so this sits outside the snapshot-minting branch.
    writes.extend(locator_writes);
    writes.extend(index_writes);
    writes.extend(inline_writes);

    // A head-preserving commit never marks a definition maintaining.
    let deferred_indexes = if mints_snapshot {
        deferred.into_iter().map(IndexId::new).collect()
    } else {
        Vec::new()
    };

    Ok(Assembly {
        result_id,
        writes,
        transaction_id,
        changes_made,
        deferred_indexes,
        index_bytes,
        index_metrics,
    })
}

/// Lands an assembled batch through one slot at `head.next_sequence` — a
/// single attempt, deliberately not `drive_commit`: DuckLake authored the
/// ids, so a lost race cannot be rebased and re-run, only surfaced. An
/// ambiguous put still resolves inside `commit_slot` by transaction-id
/// read-back; only *rebasing onto a winner* is forbidden here.
///
/// One consequence of the single attempt: unlike the verb path's
/// `drive_commit`, this does not back off and retry a transient slot
/// contention. A `commit_slot` that returns `Transport` — including the
/// "reported taken but reads absent" read-back on a healthy but contended log
/// — surfaces to DuckLake as [`Error::SlotLog`], terminal by design (it carries
/// none of DuckLake's retry substrings). DuckLake re-drives the whole
/// transaction, since it cannot re-derive its authored ids against a new head.
async fn commit_slots(
    head: SlotHead,
    slots: SlotLog,
    assembled: Result<Assembly>,
    staged_rows: usize,
    started: std::time::Instant,
    head_cache: &HeadCache,
    contention: &crate::transaction::slot_commit::ContentionCounters,
) -> Result<CommitReport> {
    let outcome = match assembled {
        Ok(assembly) => {
            let transaction_id = assembly
                .transaction_id
                .unwrap_or_else(|| uuid::Uuid::new_v4().into_bytes());
            let validated_head = head.view.snapshot.snapshot_id;
            let envelope = Envelope {
                leader: None,
                commits: vec![commit_from(
                    transaction_id,
                    validated_head,
                    assembly.changes_made,
                    &assembly.writes,
                )],
            };
            match slots.commit_slot(head.next_sequence, &envelope).await {
                Ok(CommitOutcome::Won) => {
                    record_committed_head(head_cache, &head, &envelope, &assembly.writes);
                    staged_landed(
                        assembly.result_id,
                        staged_rows,
                        assembly.index_bytes,
                        &assembly.index_metrics,
                        started,
                    );
                    Ok(CommitReport {
                        snapshot_id: SnapshotId::new(assembly.result_id),
                        deferred_indexes: assembly.deferred_indexes,
                    })
                }
                Ok(CommitOutcome::Lost(_)) => {
                    // The lost race is contention proof: it arms the next
                    // re-drive's forwarding trigger through the shared counters.
                    contention.record_lost();
                    Err(staged_lost_race(assembly.result_id, staged_rows))
                }
                Err(err) => Err(err.into()),
            }
        }
        Err(err) => Err(err),
    };

    release_reader(head.reader.as_ref()).await;
    outcome
}

/// Records the head this staged commit produced in the handle's cache, so the
/// next read on the same handle sees it regardless of the refresh window. The
/// writes were staged against `head.view`, so applying them onto it
/// reconstructs the committed view; a batch that cannot replay leaves the cache
/// cleared rather than wrong.
fn record_committed_head(
    head_cache: &HeadCache,
    head: &SlotHead,
    envelope: &Envelope,
    writes: &[StagedWrite],
) {
    let mut view = head.view.clone();
    if commit::fold::fold_batch(&mut view, writes).is_err() {
        head_cache.invalidate();
        return;
    }
    let mut overlay = head.overlay.clone();
    overlay.absorb(envelope);
    head_cache.record(&view, &overlay, head.next_sequence.saturating_add(1));
}

/// One landed staged commit's summary event.
fn staged_landed(
    result_id: u64,
    staged_rows: usize,
    index_bytes: u64,
    index: &IndexMaintenanceMetrics,
    started: std::time::Instant,
) {
    let read = &index.scoped_read;
    debug!(
        snapshot = result_id,
        staged_rows,
        index_bytes,
        index_delete_derivation_ms = milliseconds(index.deletion_derivation),
        index_add_derivation_ms = milliseconds(index.addition_derivation),
        index_probe_window_ms = milliseconds(index.probe_window),
        index_probe_service_ms = milliseconds(index.probe_service),
        index_stage_ms = milliseconds(index.staging),
        index_encode_ms = milliseconds(read.encode_duration),
        index_parquet_read_ms = milliseconds(read.range_duration),
        index_additions = index.additions,
        index_deletions = index.deletions,
        index_unique_probes = index.unique_probes,
        index_probe_hits = index.probe_hits,
        index_probe_misses = index.probe_misses,
        index_probe_peak_in_flight = index.probe_peak_in_flight,
        index_probes_completed_during_deletions = index.probes_completed_during_deletions,
        index_metadata_hits = read.metadata_hits,
        index_metadata_misses = read.metadata_misses,
        index_range_fetches = read.range_fetches,
        index_ranges = read.ranges,
        index_range_bytes = read.range_bytes,
        index_files = read.parquet_files,
        index_inline_chunks = read.inline_chunks,
        index_arrow_batches = read.arrow_batches,
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
    let mut touched = commit::Touched::default();
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
                &mut touched,
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
                &mut touched,
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
                &mut touched,
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

    crate::transaction::index_maintenance::apply_poison(&mut state, poisoned, &mut touched);
    crate::transaction::index_maintenance::apply_deferred_maintenance(
        base,
        &mut state,
        deferred,
        new_id,
        &mut touched,
    );

    let mut writes = commit::diff_touched(base, &state, new_id, &touched);
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
/// deletes, schedule inserts, per-file statistics a repair derives, and
/// the inline drops a dead table's cleanup issues; any entity insert or
/// lifecycle update without a snapshot row is a constraint violation
/// (DuckLake always mints a snapshot for real catalog changes).
fn translate_maintenance(
    base: &CatalogSnapshot,
    ops: &[RowOperation],
) -> Result<Vec<commit::StagedWrite>> {
    let head = base.snapshot.snapshot_id;

    let mut state = base.clone();
    let mut children = ChildRows::default();
    let mut direct = Vec::new();
    let mut touched = commit::Touched::default();
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
                        | TableKind::Metadata
                        // A row-id statistics backfill re-derives per-file
                        // statistics for files already registered. The rows
                        // carry no lifecycle of their own, so repairing them
                        // changes nothing a snapshot would record.
                        | TableKind::FileColumnStats,
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
            &mut touched,
        )?;
    }

    let mut writes = commit::diff_touched(base, &state, head, &touched);
    writes.extend(direct);
    Ok(writes)
}
