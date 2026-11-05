//! Authored schema-version records and file changes used to derive floors.

use super::{BTreeMap, HashSet, Result, RowOperation, TableKind, VersionedRows, decode, proto};

pub(crate) struct Changes {
    pub(crate) records: BTreeMap<(u64, u64), Option<u64>>,
    pub(super) deleted_snapshots: HashSet<u64>,
    pub(super) files: VersionedRows<proto::DataFileValue>,
}

impl Changes {
    pub(super) fn new(operations: &[RowOperation]) -> Result<Self> {
        let mut records = BTreeMap::new();
        let mut deleted_snapshots = HashSet::new();
        for operation in operations {
            match operation {
                RowOperation::Insert {
                    table: TableKind::SchemaVersions,
                    cells,
                } => {
                    let (table, begin, version) = decode::decode_schema_version_row(cells)?;
                    records.insert((table, begin), Some(version));
                }
                RowOperation::Delete {
                    table: TableKind::SchemaVersions,
                    cells,
                } => {
                    let (table, begin, _) = decode::decode_schema_version_row(cells)?;
                    records.insert((table, begin), None);
                }
                RowOperation::Delete {
                    table: TableKind::Snapshot,
                    cells,
                } => {
                    let mut cursor = super::super::Cursor::new(TableKind::Snapshot, cells);
                    deleted_snapshots.insert(cursor.u64()?);
                    cursor.finish()?;
                }
                _ => {}
            }
        }
        Ok(Self {
            records,
            deleted_snapshots,
            files: VersionedRows::new(operations, |_| true)?,
        })
    }

    pub(crate) fn file_floors(&self) -> BTreeMap<u64, u64> {
        let mut floors = BTreeMap::new();
        for file in &self.files.inserted {
            floors
                .entry(file.table_id)
                .and_modify(|begin: &mut u64| *begin = (*begin).min(file.begin_snapshot))
                .or_insert(file.begin_snapshot);
        }
        floors
    }
}
