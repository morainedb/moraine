//! Operation-owned records needed to translate one staged batch.

use std::collections::BTreeSet;

use super::{CatalogSnapshot, Cell, EntityKey, Error, Result, RowOperation, TableKind, decode};
use crate::store::{
    handle::{ReadHandle, ScanShape},
    key::{
        CurrentKey, EntityKind, Key, TableScopedKind, current_entity_kind_prefix,
        current_table_prefix,
    },
    read::{self, EntityRecord},
};

/// Reads the commit counters without decoding catalog entities.
pub(crate) async fn header(handle: ReadHandle<'_>) -> Result<CatalogSnapshot> {
    crate::transaction::commit::refuse_mid_migration(handle).await?;
    let head = crate::transaction::commit::read_head_value(handle).await?;
    let snapshot = read::read_snapshot(handle, head.snapshot_id)
        .await?
        .ok_or_else(|| Error::Corruption("catalog head has no snapshot".into()))?;
    Ok(CatalogSnapshot {
        snapshot,
        batch_seq: head.batch_seq,
        ..CatalogSnapshot::default()
    })
}

#[derive(Default)]
struct Needed {
    keys: BTreeSet<Key>,
    tables: BTreeSet<u64>,
    files: BTreeSet<(u64, u64)>,
    mappings: BTreeSet<u64>,
}

impl Needed {
    fn entity(&mut self, key: EntityKey) {
        match key {
            EntityKey::Table { table_id }
            | EntityKey::Column { table_id, .. }
            | EntityKey::DeleteFile { table_id, .. }
            | EntityKey::Partition { table_id, .. }
            | EntityKey::Sort { table_id, .. }
            | EntityKey::Mapping { table_id, .. }
            | EntityKey::Index { table_id, .. }
            | EntityKey::TableStats { table_id }
            | EntityKey::TableColumnStats { table_id, .. }
            | EntityKey::FileColumnStats { table_id, .. } => {
                self.tables.insert(table_id);
            }
            EntityKey::File {
                table_id,
                data_file_id,
            } => {
                self.tables.insert(table_id);
                self.files.insert((table_id, data_file_id));
            }
            _ => {}
        }
        self.keys.insert(Key::current(key));
    }

    #[allow(clippy::too_many_lines)]
    fn operation(&mut self, operation: &RowOperation) -> Result<()> {
        let (table, cells, insert) = match operation {
            RowOperation::Insert { table, cells } => (*table, cells, true),
            RowOperation::Delete { table, cells }
            | RowOperation::UpdateSetEnd { table, cells }
            | RowOperation::UpdateSetBegin { table, cells } => (*table, cells, false),
            RowOperation::InlineFileDelete {
                table_id,
                data_file_id,
                ..
            }
            | RowOperation::InlineFileDeleteRemove {
                table_id,
                data_file_id,
                ..
            } => {
                self.entity(EntityKey::File {
                    table_id: *table_id,
                    data_file_id: *data_file_id,
                });
                return Ok(());
            }
            RowOperation::InlineSchema { table_id, .. }
            | RowOperation::InlineInsert { table_id, .. }
            | RowOperation::InlineInlineDelete { table_id, .. }
            | RowOperation::InlineFileDeleteClear { table_id }
            | RowOperation::InlineFileDeleteTableDrop { table_id }
            | RowOperation::InlineRowsDelete { table_id, .. }
            | RowOperation::InlineFlushDelete { table_id, .. }
            | RowOperation::InlineDrop { table_id }
            | RowOperation::InlineSchemaDrop { table_id, .. } => {
                self.tables.insert(*table_id);
                return Ok(());
            }
        };
        if insert {
            return self.insert(table, cells);
        }
        let mut cursor = decode::Cursor::new(table, cells);
        let key = match table {
            TableKind::Snapshot | TableKind::SnapshotChanges | TableKind::SchemaVersions => {
                return Ok(());
            }
            TableKind::Metadata => {
                let ((scope_kind, scope_id), _) = decode::decode_metadata_key(cells)?;
                EntityKey::Option {
                    scope_kind,
                    scope_id,
                }
            }
            TableKind::Tag => EntityKey::Tag {
                object_id: cursor.u64()?,
            },
            TableKind::ViewColumnTag => EntityKey::View {
                view_id: cursor.u64()?,
            },
            TableKind::ColumnTag => EntityKey::Column {
                table_id: cursor.u64()?,
                column_id: cursor.u64()?,
            },
            TableKind::TableStats => EntityKey::TableStats {
                table_id: cursor.u64()?,
            },
            TableKind::TableColumnStats => EntityKey::TableColumnStats {
                table_id: cursor.u64()?,
                column_id: cursor.u64()?,
            },
            TableKind::FileColumnStats | TableKind::FileVariantStats => {
                let data_file_id = cursor.u64()?;
                EntityKey::FileColumnStats {
                    table_id: cursor.u64()?,
                    data_file_id,
                    column_id: cursor.u64()?,
                }
            }
            TableKind::FilesScheduledForDeletion => {
                self.keys.insert(Key::Current(CurrentKey::GcFile {
                    data_file_id: cursor.u64()?,
                }));
                return Ok(());
            }
            TableKind::PartitionColumn => {
                let partition_id = cursor.u64()?;
                EntityKey::Partition {
                    table_id: cursor.u64()?,
                    partition_id,
                }
            }
            TableKind::SortExpression => {
                let sort_id = cursor.u64()?;
                EntityKey::Sort {
                    table_id: cursor.u64()?,
                    sort_id,
                }
            }
            TableKind::FilePartitionValue => {
                let data_file_id = cursor.u64()?;
                EntityKey::File {
                    table_id: cursor.u64()?,
                    data_file_id,
                }
            }
            TableKind::MacroImpl | TableKind::MacroParameters => EntityKey::Macro {
                macro_id: cursor.u64()?,
            },
            TableKind::NameMapping => {
                self.mappings.insert(cursor.u64()?);
                return Ok(());
            }
            _ => match operation {
                RowOperation::UpdateSetEnd { .. } => decode::decode_end(table, cells)?.0,
                RowOperation::UpdateSetBegin { .. } => EntityKey::File {
                    table_id: cursor.u64()?,
                    data_file_id: cursor.u64()?,
                },
                _ => decode::decode_hard_delete(table, cells)?.0,
            },
        };
        self.entity(key);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn insert(&mut self, table: TableKind, cells: &[Cell]) -> Result<()> {
        let key = match table {
            TableKind::Schema => EntityKey::Schema {
                schema_id: decode::decode_schema(cells)?.schema_id,
            },
            TableKind::Table => EntityKey::Table {
                table_id: decode::decode_table(cells)?.table_id,
            },
            TableKind::View => EntityKey::View {
                view_id: decode::decode_view(cells)?.view_id,
            },
            TableKind::Column => {
                let value = decode::decode_column(cells)?;
                EntityKey::Column {
                    table_id: value.table_id,
                    column_id: value.column_id,
                }
            }
            TableKind::DataFile => {
                let value = decode::decode_data_file(cells)?;
                EntityKey::File {
                    table_id: value.table_id,
                    data_file_id: value.data_file_id,
                }
            }
            TableKind::DeleteFile => {
                let value = decode::decode_delete_file(cells)?;
                self.entity(EntityKey::File {
                    table_id: value.table_id,
                    data_file_id: value.data_file_id,
                });
                EntityKey::DeleteFile {
                    table_id: value.table_id,
                    delete_file_id: value.delete_file_id,
                }
            }
            TableKind::TableStats => EntityKey::TableStats {
                table_id: decode::decode_table_stats(cells)?.table_id,
            },
            TableKind::TableColumnStats => {
                let value = decode::decode_table_column_stats(cells)?;
                EntityKey::TableColumnStats {
                    table_id: value.table_id,
                    column_id: value.column_id,
                }
            }
            TableKind::FileColumnStats => {
                let value = decode::decode_file_column_stats(cells)?;
                EntityKey::FileColumnStats {
                    table_id: value.table_id,
                    data_file_id: value.data_file_id,
                    column_id: value.column_id,
                }
            }
            TableKind::PartitionInfo => {
                let value = decode::decode_partition_info(cells)?;
                EntityKey::Partition {
                    table_id: value.table_id,
                    partition_id: value.partition_id,
                }
            }
            TableKind::SortInfo => {
                let value = decode::decode_sort_info(cells)?;
                EntityKey::Sort {
                    table_id: value.table_id,
                    sort_id: value.sort_id,
                }
            }
            TableKind::Macro => EntityKey::Macro {
                macro_id: decode::decode_macro(cells)?.macro_id,
            },
            TableKind::ColumnMapping => {
                let value = decode::decode_column_mapping(cells)?;
                EntityKey::Mapping {
                    table_id: value.table_id,
                    mapping_id: value.mapping_id,
                }
            }
            TableKind::Metadata => {
                let ((scope_kind, scope_id), _, _) = decode::decode_metadata(cells)?;
                EntityKey::Option {
                    scope_kind,
                    scope_id,
                }
            }
            TableKind::Tag => EntityKey::Tag {
                object_id: decode::decode_tag_row(cells)?.0,
            },
            TableKind::ColumnTag => {
                let ((table_id, column_id), _) = decode::decode_column_tag_row(cells)?;
                EntityKey::Column {
                    table_id,
                    column_id,
                }
            }
            TableKind::ViewColumnTag => EntityKey::View {
                view_id: decode::decode_view_column_tag_row(cells)?.0,
            },
            TableKind::FilesScheduledForDeletion => {
                self.keys.insert(Key::Current(CurrentKey::GcFile {
                    data_file_id: decode::decode_gc_file_row(cells)?.data_file_id,
                }));
                return Ok(());
            }
            // These rows are embedded in parents inserted by the same batch.
            TableKind::Snapshot
            | TableKind::SnapshotChanges
            | TableKind::SchemaVersions
            | TableKind::PartitionColumn
            | TableKind::SortExpression
            | TableKind::FilePartitionValue
            | TableKind::FileVariantStats
            | TableKind::MacroImpl
            | TableKind::MacroParameters
            | TableKind::NameMapping => return Ok(()),
        };
        self.entity(key);
        Ok(())
    }
}

fn retain(view: &mut CatalogSnapshot, record: EntityRecord) -> Result<()> {
    view.admit(record.estimated_bytes().saturating_add(80))?;
    view.put_record(record);
    Ok(())
}

async fn point(handle: ReadHandle<'_>, view: &mut CatalogSnapshot, key: Key) -> Result<()> {
    if let Some(value) = handle.get(&key.encode()).await? {
        retain(view, read::decode_current_record(key, &value)?)?;
    }
    Ok(())
}

async fn scan(
    handle: ReadHandle<'_>,
    view: &mut CatalogSnapshot,
    prefix: Vec<u8>,
    include: impl Fn(&EntityRecord) -> bool,
) -> Result<()> {
    let mut cursor = handle.scan_prefix(prefix, .., ScanShape::Metadata).await?;
    while let Some(entry) = cursor.next().await? {
        handle.validate()?;
        let record = read::decode_current_record(Key::decode(&entry.key)?, &entry.value)?;
        if include(&record) {
            retain(view, record)?;
        }
    }
    Ok(())
}

/// Resolves authored keys, affected table descriptors, and targeted delete
/// files.
pub(super) async fn load(
    handle: ReadHandle<'_>,
    operations: &[RowOperation],
) -> Result<CatalogSnapshot> {
    read::consistent(handle, || load_once(handle, operations)).await
}

async fn load_once(handle: ReadHandle<'_>, operations: &[RowOperation]) -> Result<CatalogSnapshot> {
    let mut view = header(handle).await?;
    let mut needed = Needed::default();
    for operation in operations {
        needed.operation(operation)?;
    }
    for key in needed.keys {
        point(handle, &mut view, key).await?;
    }
    for table_id in needed.tables {
        point(
            handle,
            &mut view,
            Key::current(EntityKey::Table { table_id }),
        )
        .await?;
        point(
            handle,
            &mut view,
            Key::current(EntityKey::TableStats { table_id }),
        )
        .await?;
        for kind in [
            TableScopedKind::Column,
            TableScopedKind::Index,
            TableScopedKind::TableColumnStats,
        ] {
            scan(
                handle,
                &mut view,
                current_table_prefix(kind, table_id),
                |_| true,
            )
            .await?;
        }
        if needed.files.iter().any(|(table, _)| *table == table_id) {
            scan(handle, &mut view, current_table_prefix(TableScopedKind::DeleteFile, table_id), |record| {
                matches!(record, EntityRecord::DeleteFile(file) if needed.files.contains(&(table_id, file.data_file_id)))
            }).await?;
        }
    }
    let schemas: BTreeSet<_> = view.tables.values().map(|table| table.schema_id).collect();
    for schema_id in schemas {
        point(
            handle,
            &mut view,
            Key::current(EntityKey::Schema { schema_id }),
        )
        .await?;
    }
    let mappings: BTreeSet<_> = view
        .data_files
        .values()
        .flat_map(|files| files.values())
        .filter_map(|file| {
            file.mapping_id
                .map(|mapping_id| (file.table_id, mapping_id))
        })
        .collect();
    for (table_id, mapping_id) in mappings {
        point(
            handle,
            &mut view,
            Key::current(EntityKey::Mapping {
                table_id,
                mapping_id,
            }),
        )
        .await?;
    }
    if !needed.mappings.is_empty() {
        scan(handle, &mut view, current_entity_kind_prefix(EntityKind::Mapping), |record| {
            matches!(record, EntityRecord::Mapping(mapping) if needed.mappings.contains(&mapping.mapping_id))
        }).await?;
    }
    Ok(view)
}
