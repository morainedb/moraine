//! Frozen, operation-owned overlays for incremental metadata reads.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use super::{
    RowOperation, StagedTransaction, TableKind, decode,
    overlay::{self, Unversioned, Versioned},
};
use crate::{
    Error, MemoryReservation, Result,
    ffi_support::metadata_cursor::MetadataCursor,
    store::{key::EntityKey, proto, read::EntityRecord},
};

mod children;
mod containers;
mod parent_tags;
pub(crate) mod schema_versions;
mod tables;

pub(crate) struct Overlay {
    rows: Rows,
    children: Vec<EntityRecord>,
    _memory: MemoryReservation,
}

enum Rows {
    ParentTags(parent_tags::Parents),
    Containers(containers::Containers),
    Snapshots(HashSet<u64>),
    SchemaVersions(schema_versions::Changes),
    Tables(VersionedRows<proto::TableValue>),
    Files(VersionedRows<proto::DataFileValue>),
    DeleteFiles(VersionedRows<proto::DeleteFileValue>),
    Schemas(VersionedRows<proto::SchemaValue>),
    Columns(VersionedRows<proto::ColumnValue>),
    Views(VersionedRows<proto::ViewValue>),
    Partitions(VersionedRows<proto::PartitionValue>),
    Sorts(VersionedRows<proto::SortValue>),
    Macros(VersionedRows<proto::MacroValue>),
    FileStats(UnversionedRows<proto::FileColumnStatsValue>),
    TableStats(UnversionedRows<proto::TableStatsValue>),
    ColumnStats(UnversionedRows<proto::TableColumnStatsValue>),
    Mappings(UnversionedRows<proto::MappingValue>),
    Deletions(UnversionedRows<proto::GcFileValue>),
}

struct VersionedRows<T> {
    edits: BTreeMap<EntityKey, Vec<RowOperation>>,
    inserted: Vec<T>,
}

impl<T: Versioned + Clone> VersionedRows<T> {
    fn new(operations: &[RowOperation], include: impl Fn(&T) -> bool) -> Result<Self> {
        let mut edits: BTreeMap<EntityKey, Vec<RowOperation>> = BTreeMap::new();
        for operation in operations {
            let key = match operation {
                RowOperation::Delete { table, cells } if *table == T::KIND => {
                    decode::decode_hard_delete(*table, cells)?.0
                }
                RowOperation::UpdateSetEnd { table, cells } if *table == T::KIND => {
                    decode::decode_end(*table, cells)?.0
                }
                RowOperation::UpdateSetBegin { table, cells } if *table == T::KIND => {
                    overlay::decode_begin(*table, cells)?.0
                }
                _ => continue,
            };
            edits.entry(key).or_default().push(operation.clone());
        }
        let mut inserted = overlay::overlay_versioned::<T>(operations, Vec::new())?;
        inserted.retain(include);
        Ok(Self { edits, inserted })
    }

    fn apply(&self, row: Arc<T>) -> Result<Option<Arc<T>>> {
        let Some(edits) = self.edits.get(&row.key()) else {
            return Ok(Some(row));
        };
        Ok(overlay::overlay_versioned(edits, vec![(*row).clone()])?
            .into_iter()
            .next()
            .map(Arc::new))
    }
}

struct UnversionedRows<T: Unversioned> {
    touched: HashSet<T::Key>,
    inserted: Vec<T>,
}

impl<T: Unversioned> UnversionedRows<T> {
    fn new(operations: &[RowOperation], include: impl Fn(&T) -> bool) -> Result<Self> {
        let mut touched = HashSet::new();
        for operation in operations {
            match operation {
                RowOperation::Insert { table, cells } if *table == T::KIND => {
                    touched.insert(T::decode_row(cells)?.key());
                }
                RowOperation::Delete { table, cells } if *table == T::KIND => {
                    touched.insert(T::decode_key(cells)?);
                }
                _ => {}
            }
        }
        let mut inserted = overlay::overlay_unversioned::<T>(operations, Vec::new())?;
        inserted.retain(include);
        Ok(Self { touched, inserted })
    }

    fn apply(&self, row: Arc<T>) -> Option<Arc<T>> {
        (!self.touched.contains(&row.key())).then_some(row)
    }
}

impl Rows {
    async fn open(
        table: TableKind,
        operations: &[RowOperation],
        scope: Option<u64>,
        handle: crate::store::handle::OwnedReadHandle,
    ) -> Result<Self> {
        Ok(match table {
            TableKind::ColumnTag | TableKind::ViewColumnTag => {
                Rows::ParentTags(parent_tags::Parents::new(table, operations)?)
            }
            TableKind::SchemaVersions => {
                Rows::SchemaVersions(schema_versions::Changes::new(operations)?)
            }
            TableKind::Tag | TableKind::Metadata => Rows::Containers(
                containers::Containers::new(table, operations, handle.clone()).await?,
            ),
            TableKind::Snapshot | TableKind::SnapshotChanges => {
                let mut deleted = HashSet::new();
                for operation in operations {
                    if let RowOperation::Delete {
                        table: TableKind::Snapshot,
                        cells,
                    } = operation
                    {
                        let mut cursor = super::Cursor::new(TableKind::Snapshot, cells);
                        deleted.insert(cursor.u64()?);
                        cursor.finish()?;
                    }
                }
                Rows::Snapshots(deleted)
            }
            TableKind::Table => Rows::Tables(tables::rows(operations, handle.clone()).await?),
            TableKind::DataFile => Rows::Files(VersionedRows::<proto::DataFileValue>::new(
                operations,
                |row| scope.is_none_or(|table| row.table_id == table),
            )?),
            TableKind::DeleteFile => Rows::DeleteFiles(
                VersionedRows::<proto::DeleteFileValue>::new(operations, |row| {
                    scope.is_none_or(|table| row.table_id == table)
                })?,
            ),
            TableKind::Schema => Rows::Schemas(VersionedRows::<proto::SchemaValue>::new(
                operations,
                |_| true,
            )?),
            TableKind::Column => Rows::Columns(VersionedRows::<proto::ColumnValue>::new(
                operations,
                |row| scope.is_none_or(|table| row.table_id == table),
            )?),
            TableKind::View => {
                Rows::Views(VersionedRows::<proto::ViewValue>::new(operations, |_| {
                    true
                })?)
            }
            TableKind::PartitionInfo => Rows::Partitions(
                VersionedRows::<proto::PartitionValue>::new(operations, |row| {
                    scope.is_none_or(|table| row.table_id == table)
                })?,
            ),
            TableKind::SortInfo => {
                Rows::Sorts(VersionedRows::<proto::SortValue>::new(operations, |row| {
                    scope.is_none_or(|table| row.table_id == table)
                })?)
            }
            TableKind::Macro => {
                Rows::Macros(VersionedRows::<proto::MacroValue>::new(operations, |_| {
                    true
                })?)
            }
            TableKind::FileColumnStats => Rows::FileStats(UnversionedRows::<
                proto::FileColumnStatsValue,
            >::new(operations, |row| {
                scope.is_none_or(|table| row.table_id == table)
            })?),
            TableKind::TableStats => Rows::TableStats(
                UnversionedRows::<proto::TableStatsValue>::new(operations, |row| {
                    scope.is_none_or(|table| row.table_id == table)
                })?,
            ),
            TableKind::TableColumnStats => {
                Rows::ColumnStats(UnversionedRows::<proto::TableColumnStatsValue>::new(
                    operations,
                    |row| scope.is_none_or(|table| row.table_id == table),
                )?)
            }
            TableKind::ColumnMapping => Rows::Mappings(
                UnversionedRows::<proto::MappingValue>::new(operations, |row| {
                    scope.is_none_or(|table| row.table_id == table)
                })?,
            ),
            TableKind::FilesScheduledForDeletion => Rows::Deletions(UnversionedRows::<
                proto::GcFileValue,
            >::new(
                operations, |_| true
            )?),
            _ => {
                return Err(Error::Constraint(format!(
                    "{table:?} does not yet support staged paging"
                )));
            }
        })
    }
}

impl Overlay {
    async fn new(
        table: TableKind,
        operations: &[RowOperation],
        scope: Option<u64>,
        handle: crate::store::handle::OwnedReadHandle,
    ) -> Result<Arc<Self>> {
        let requested_table = table;
        let table = children::parent(table);
        let operation_table = match table {
            TableKind::SnapshotChanges => TableKind::Snapshot,
            TableKind::ColumnTag => TableKind::Column,
            TableKind::ViewColumnTag => TableKind::View,
            _ => table,
        };
        let bytes = operations
            .iter()
            .filter(|operation| matches!(operation_kind(operation), Some(kind) if kind == operation_table || kind == requested_table || (table == TableKind::SchemaVersions && matches!(kind, TableKind::DataFile | TableKind::Snapshot))))
            .fold(0u64, |bytes, operation| {
                bytes
                    .saturating_add(super::memory::payload_bytes(operation) as u64)
                    .saturating_add(512)
            })
            .saturating_mul(4);
        let memory = MemoryReservation::reserve(bytes)?;
        let rows = Rows::open(table, operations, scope, handle.clone()).await?;
        let mut overlay = Self {
            rows,
            children: Vec::new(),
            _memory: memory,
        };
        overlay.children =
            children::rows(requested_table, operations, &overlay, scope, handle).await?;
        Ok(Arc::new(overlay))
    }

    pub(crate) fn apply(&self, record: EntityRecord) -> Result<Option<EntityRecord>> {
        match (&self.rows, record) {
            (Rows::ParentTags(rows), record) => rows.apply(record),
            (Rows::SchemaVersions(changes), EntityRecord::File(row)) => changes
                .files
                .apply(row)
                .map(|row| row.map(EntityRecord::File)),
            (Rows::Containers(rows), record) => rows.apply(record),
            (Rows::Tables(rows), EntityRecord::Table(row)) => {
                rows.apply(row).map(|row| row.map(EntityRecord::Table))
            }
            (Rows::Files(rows), EntityRecord::File(row)) => {
                rows.apply(row).map(|row| row.map(EntityRecord::File))
            }
            (Rows::DeleteFiles(rows), EntityRecord::DeleteFile(row)) => {
                rows.apply(row).map(|row| row.map(EntityRecord::DeleteFile))
            }
            (Rows::Schemas(rows), EntityRecord::Schema(row)) => {
                rows.apply(row).map(|row| row.map(EntityRecord::Schema))
            }
            (Rows::Columns(rows), EntityRecord::Column(row)) => {
                rows.apply(row).map(|row| row.map(EntityRecord::Column))
            }
            (Rows::Views(rows), EntityRecord::View(row)) => {
                rows.apply(row).map(|row| row.map(EntityRecord::View))
            }
            (Rows::Partitions(rows), EntityRecord::Partition(row)) => {
                rows.apply(row).map(|row| row.map(EntityRecord::Partition))
            }
            (Rows::Sorts(rows), EntityRecord::Sort(row)) => {
                rows.apply(row).map(|row| row.map(EntityRecord::Sort))
            }
            (Rows::Macros(rows), EntityRecord::Macro(row)) => {
                rows.apply(row).map(|row| row.map(EntityRecord::Macro))
            }
            (Rows::FileStats(rows), EntityRecord::FileColumnStats(row)) => {
                Ok(rows.apply(row).map(EntityRecord::FileColumnStats))
            }
            (Rows::TableStats(rows), EntityRecord::TableStats(row)) => {
                Ok(rows.apply(row).map(EntityRecord::TableStats))
            }
            (Rows::ColumnStats(rows), EntityRecord::TableColumnStats(row)) => {
                Ok(rows.apply(row).map(EntityRecord::TableColumnStats))
            }
            (Rows::Mappings(rows), EntityRecord::Mapping(row)) => {
                Ok(rows.apply(row).map(EntityRecord::Mapping))
            }
            (Rows::Deletions(rows), EntityRecord::GcFile(row)) => {
                Ok(rows.apply(row).map(EntityRecord::GcFile))
            }
            _ => Err(Error::Corruption(
                "staged cursor received a different entity kind".into(),
            )),
        }
    }

    pub(crate) fn schema_changes(&self) -> Option<&schema_versions::Changes> {
        match &self.rows {
            Rows::SchemaVersions(changes) => Some(changes),
            _ => None,
        }
    }

    pub(crate) fn includes_snapshot(&self, snapshot_id: u64) -> bool {
        match &self.rows {
            Rows::Snapshots(deleted) => !deleted.contains(&snapshot_id),
            Rows::SchemaVersions(changes) => !changes.deleted_snapshots.contains(&snapshot_id),
            _ => true,
        }
    }

    pub(crate) async fn next_inserted(
        &self,
        index: usize,
        handle: crate::store::handle::OwnedReadHandle,
    ) -> Result<Option<(EntityRecord, MemoryReservation)>> {
        match &self.rows {
            Rows::ParentTags(rows) => rows.replacement(index, handle).await,
            _ => self.inserted(index),
        }
    }

    pub(crate) fn inserted(
        &self,
        index: usize,
    ) -> Result<Option<(EntityRecord, MemoryReservation)>> {
        let parent_count = match &self.rows {
            Rows::Partitions(rows) => rows.inserted.len(),
            Rows::Sorts(rows) => rows.inserted.len(),
            Rows::Files(rows) => rows.inserted.len(),
            Rows::FileStats(rows) => rows.inserted.len(),
            Rows::Macros(rows) => rows.inserted.len(),
            Rows::Mappings(rows) => rows.inserted.len(),
            _ => 0,
        };
        if let Some(record) = index
            .checked_sub(parent_count)
            .and_then(|offset| self.children.get(offset))
        {
            return children::export_record(record);
        }
        match &self.rows {
            Rows::Containers(rows) => rows.inserted(index),
            Rows::ParentTags(_) | Rows::Snapshots(_) | Rows::SchemaVersions(_) => Ok(None),
            Rows::Tables(rows) => export(rows.inserted.get(index), EntityRecord::Table),
            Rows::Files(rows) => export(rows.inserted.get(index), EntityRecord::File),
            Rows::DeleteFiles(rows) => export(rows.inserted.get(index), EntityRecord::DeleteFile),
            Rows::Schemas(rows) => export(rows.inserted.get(index), EntityRecord::Schema),
            Rows::Columns(rows) => export(rows.inserted.get(index), EntityRecord::Column),
            Rows::Views(rows) => export(rows.inserted.get(index), EntityRecord::View),
            Rows::Partitions(rows) => export(rows.inserted.get(index), EntityRecord::Partition),
            Rows::Sorts(rows) => export(rows.inserted.get(index), EntityRecord::Sort),
            Rows::Macros(rows) => export(rows.inserted.get(index), EntityRecord::Macro),
            Rows::FileStats(rows) => {
                export(rows.inserted.get(index), EntityRecord::FileColumnStats)
            }
            Rows::TableStats(rows) => export(rows.inserted.get(index), EntityRecord::TableStats),
            Rows::ColumnStats(rows) => {
                export(rows.inserted.get(index), EntityRecord::TableColumnStats)
            }
            Rows::Mappings(rows) => export(rows.inserted.get(index), EntityRecord::Mapping),
            Rows::Deletions(rows) => export(rows.inserted.get(index), EntityRecord::GcFile),
        }
    }
}

fn export<T: Clone + prost::Message>(
    row: Option<&T>,
    wrap: impl FnOnce(Arc<T>) -> EntityRecord,
) -> Result<Option<(EntityRecord, MemoryReservation)>> {
    let Some(row) = row else { return Ok(None) };
    let memory = MemoryReservation::reserve(
        (row.encoded_len() as u64)
            .saturating_mul(4)
            .saturating_add(size_of::<T>() as u64)
            .saturating_add(128),
    )?;
    Ok(Some((wrap(Arc::new(row.clone())), memory)))
}

fn operation_kind(operation: &RowOperation) -> Option<TableKind> {
    match operation {
        RowOperation::Insert { table, .. }
        | RowOperation::Delete { table, .. }
        | RowOperation::UpdateSetEnd { table, .. }
        | RowOperation::UpdateSetBegin { table, .. } => Some(*table),
        _ => None,
    }
}

impl StagedTransaction {
    /// Freezes authored rows and the read scope for a bounded metadata scan.
    ///
    /// # Errors
    /// Returns an error for an unsupported kind, malformed operation, or absent
    /// fixed scope.
    pub async fn metadata_cursor(
        &self,
        table: TableKind,
        table_id: Option<u64>,
        live_bound: Option<u64>,
    ) -> Result<MetadataCursor> {
        self.staging.check()?;
        let scope = self
            .read_scope
            .as_ref()
            .ok_or_else(|| Error::Constraint("staged paging requires a fixed read scope".into()))?;
        let overlay = Overlay::new(table, &self.ops, table_id, scope.owned_read_handle()).await?;
        Ok(MetadataCursor::open(scope, table, table_id, live_bound)
            .await?
            .with_overlay(overlay))
    }
}
