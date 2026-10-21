//! Entity-specific mappings for staged row overlays.

use super::{
    super::decode::{
        Cursor, StatsKey, decode_column, decode_column_mapping, decode_data_file,
        decode_delete_file, decode_delete_key, decode_file_column_stats, decode_gc_file_row,
        decode_hard_delete, decode_macro, decode_partition_info, decode_schema, decode_sort_info,
        decode_table, decode_table_column_stats, decode_table_stats, decode_view,
    },
    Cell, EntityKey, EntityRecord, EntityRecordKind, Error, Result, TableKind, TableScoped,
    Unversioned, Versioned, proto,
};

/// Shared identity and lifecycle fields; exceptional behavior stays in each
/// impl.
macro_rules! versioned_fields {
    ($kind:ident, $record:ident { $($key:ident),+ }) => {
        const KIND: TableKind = TableKind::$kind;
        const RECORD: EntityRecordKind = EntityRecordKind::$record;

        fn from_record(record: &EntityRecord) -> Option<Self> {
            match record {
                EntityRecord::$record(value) => Some(value.clone()),
                _ => None,
            }
        }

        fn key(&self) -> EntityKey {
            EntityKey::$record { $($key: self.$key),+ }
        }

        fn end_snapshot(&self) -> Option<u64> {
            self.end_snapshot
        }

        fn set_end_snapshot(&mut self, end: u64) {
            self.end_snapshot = Some(end);
        }
    };
}

impl Versioned for proto::DataFileValue {
    versioned_fields!(
        DataFile,
        File {
            table_id,
            data_file_id
        }
    );

    fn set_begin_snapshot(&mut self, begin: u64) {
        self.begin_snapshot = begin;
    }

    /// Partition values ride a child table of their own, so a staged file
    /// carries none here — the `ducklake_data_file` projection does not
    /// report them either.
    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_data_file(cells)
    }
}

impl Versioned for proto::DeleteFileValue {
    versioned_fields!(
        DeleteFile,
        DeleteFile {
            table_id,
            delete_file_id
        }
    );

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_delete_file(cells)
    }
}

impl Versioned for proto::ColumnValue {
    versioned_fields!(
        Column,
        Column {
            table_id,
            column_id
        }
    );

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_column(cells)
    }
}

impl Versioned for proto::TableValue {
    versioned_fields!(Table, Table { table_id });

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_table(cells)
    }

    /// `next_column_id` is not a DuckLake column: it is inherited from the
    /// table's committed row, or left at 1 for a table this transaction
    /// creates.
    fn carry_forward(&mut self, prior: &[&Self]) {
        if let Some(next_column_id) = prior.iter().map(|row| row.next_column_id).max() {
            self.next_column_id = next_column_id;
        }
    }
}

impl Unversioned for proto::FileColumnStatsValue {
    const KIND: TableKind = TableKind::FileColumnStats;

    const RECORD: EntityRecordKind = EntityRecordKind::FileColumnStats;

    fn from_record(record: &EntityRecord) -> Option<Self> {
        match record {
            EntityRecord::FileColumnStats(value) => Some(value.clone()),
            _ => None,
        }
    }

    type Key = (u64, u64, u64);

    fn key(&self) -> Self::Key {
        (self.table_id, self.data_file_id, self.column_id)
    }

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_file_column_stats(cells)
    }

    fn decode_key(cells: &[Cell]) -> Result<Self::Key> {
        match decode_delete_key(Self::KIND, cells)? {
            StatsKey::FileColumn(table_id, data_file_id, column_id) => {
                Ok((table_id, data_file_id, column_id))
            }
            _ => Err(stats_key_mismatch(Self::KIND)),
        }
    }
}

impl Unversioned for proto::GcFileValue {
    const KIND: TableKind = TableKind::FilesScheduledForDeletion;

    const RECORD: EntityRecordKind = EntityRecordKind::GcFile;

    fn from_record(record: &EntityRecord) -> Option<Self> {
        match record {
            EntityRecord::GcFile(value) => Some(value.clone()),
            _ => None,
        }
    }

    type Key = u64;

    fn key(&self) -> Self::Key {
        self.data_file_id
    }

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_gc_file_row(cells)
    }

    fn decode_key(cells: &[Cell]) -> Result<Self::Key> {
        let mut c = Cursor::new(Self::KIND, cells);
        let data_file_id = c.u64()?;
        c.finish()?;
        Ok(data_file_id)
    }
}

impl Versioned for proto::SchemaValue {
    versioned_fields!(Schema, Schema { schema_id });

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_schema(cells)
    }
}

impl Versioned for proto::ViewValue {
    versioned_fields!(View, View { view_id });

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_view(cells)
    }
}

impl Versioned for proto::PartitionValue {
    versioned_fields!(
        PartitionInfo,
        Partition {
            table_id,
            partition_id
        }
    );

    /// The spec's partition columns ride a child table of their own, so a
    /// staged spec carries none here; the `ducklake_partition_column`
    /// projection folds them back in from the staged child rows.
    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_partition_info(cells)
    }
}

impl Versioned for proto::SortValue {
    versioned_fields!(SortInfo, Sort { table_id, sort_id });

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_sort_info(cells)
    }
}

impl Versioned for proto::MacroValue {
    versioned_fields!(Macro, Macro { macro_id });

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_macro(cells)
    }
}

impl Unversioned for proto::TableStatsValue {
    const KIND: TableKind = TableKind::TableStats;

    const RECORD: EntityRecordKind = EntityRecordKind::TableStats;

    fn from_record(record: &EntityRecord) -> Option<Self> {
        match record {
            EntityRecord::TableStats(value) => Some(*value),
            _ => None,
        }
    }

    type Key = u64;

    fn key(&self) -> Self::Key {
        self.table_id
    }

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_table_stats(cells)
    }

    fn decode_key(cells: &[Cell]) -> Result<Self::Key> {
        match decode_delete_key(Self::KIND, cells)? {
            StatsKey::Table(table_id) => Ok(table_id),
            _ => Err(stats_key_mismatch(Self::KIND)),
        }
    }
}

impl Unversioned for proto::TableColumnStatsValue {
    const KIND: TableKind = TableKind::TableColumnStats;

    const RECORD: EntityRecordKind = EntityRecordKind::TableColumnStats;

    fn from_record(record: &EntityRecord) -> Option<Self> {
        match record {
            EntityRecord::TableColumnStats(value) => Some(value.clone()),
            _ => None,
        }
    }

    type Key = (u64, u64);

    fn key(&self) -> Self::Key {
        (self.table_id, self.column_id)
    }

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_table_column_stats(cells)
    }

    fn decode_key(cells: &[Cell]) -> Result<Self::Key> {
        match decode_delete_key(Self::KIND, cells)? {
            StatsKey::Column(table_id, column_id) => Ok((table_id, column_id)),
            _ => Err(stats_key_mismatch(Self::KIND)),
        }
    }
}

impl Unversioned for proto::MappingValue {
    const KIND: TableKind = TableKind::ColumnMapping;

    const RECORD: EntityRecordKind = EntityRecordKind::Mapping;

    fn from_record(record: &EntityRecord) -> Option<Self> {
        match record {
            EntityRecord::Mapping(value) => Some(value.clone()),
            _ => None,
        }
    }

    type Key = u64;

    fn key(&self) -> Self::Key {
        self.mapping_id
    }

    /// A mapping's name-mapping rows ride a child table of their own, so a
    /// staged mapping carries none here; the `ducklake_name_mapping`
    /// projection folds them back in from the staged child rows.
    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_column_mapping(cells)
    }

    /// A mapping delete is keyed like the versioned kinds'.
    fn decode_key(cells: &[Cell]) -> Result<Self::Key> {
        match decode_hard_delete(Self::KIND, cells)?.0 {
            EntityKey::Mapping { mapping_id, .. } => Ok(mapping_id),
            other => Err(Error::Constraint(format!(
                "column mapping delete decoded as {other:?}"
            ))),
        }
    }
}

impl TableScoped for proto::DataFileValue {
    fn table_id(&self) -> u64 {
        self.table_id
    }
}

impl TableScoped for proto::DeleteFileValue {
    fn table_id(&self) -> u64 {
        self.table_id
    }
}

impl TableScoped for proto::FileColumnStatsValue {
    fn table_id(&self) -> u64 {
        self.table_id
    }
}

/// A statistics delete that decoded as another kind's key.
fn stats_key_mismatch(kind: TableKind) -> Error {
    Error::Constraint(format!("{kind:?} delete decoded as another statistics key"))
}
