//! Authored child rows, represented by single-child projection containers.

use super::{
    Arc, BTreeMap, EntityRecord, Error, MemoryReservation, Overlay, Result, RowOperation,
    TableKind, decode, export, proto,
};
use crate::store::{
    handle::OwnedReadHandle,
    read::{EntityRecordKind, OwnedEntityCursor, Versions},
};

pub(super) fn parent(table: TableKind) -> TableKind {
    match table {
        TableKind::PartitionColumn => TableKind::PartitionInfo,
        TableKind::SortExpression => TableKind::SortInfo,
        TableKind::FilePartitionValue => TableKind::DataFile,
        TableKind::FileVariantStats => TableKind::FileColumnStats,
        TableKind::MacroImpl | TableKind::MacroParameters => TableKind::Macro,
        TableKind::NameMapping => TableKind::ColumnMapping,
        _ => table,
    }
}

pub(super) async fn rows(
    table: TableKind,
    operations: &[RowOperation],
    overlay: &Overlay,
    scope: Option<u64>,
    handle: OwnedReadHandle,
) -> Result<Vec<EntityRecord>> {
    if parent(table) == table {
        return Ok(Vec::new());
    }
    let owners = owners(table, operations, overlay, scope, handle).await?;
    let mut rows = Vec::new();
    for operation in operations {
        let RowOperation::Insert { table: kind, cells } = operation else {
            continue;
        };
        if *kind != table {
            continue;
        }
        let row = match table {
            TableKind::PartitionColumn => {
                let (partition_id, column) = decode::decode_partition_column(cells)?;
                let Some(Some(table_id)) = owners.get(&partition_id) else {
                    continue;
                };
                EntityRecord::Partition(Arc::new(proto::PartitionValue {
                    partition_id,
                    table_id: *table_id,
                    columns: vec![column],
                    ..Default::default()
                }))
            }
            TableKind::SortExpression => {
                let (sort_id, expression) = decode::decode_sort_expression(cells)?;
                let Some(Some(table_id)) = owners.get(&sort_id) else {
                    continue;
                };
                EntityRecord::Sort(Arc::new(proto::SortValue {
                    sort_id,
                    table_id: *table_id,
                    expressions: vec![expression],
                    ..Default::default()
                }))
            }
            TableKind::FilePartitionValue => {
                let ((table_id, data_file_id), value) = decode::decode_file_partition_value(cells)?;
                if scope.is_some_and(|scope| scope != table_id) {
                    continue;
                }
                EntityRecord::File(Arc::new(proto::DataFileValue {
                    table_id,
                    data_file_id,
                    partition_values: vec![value],
                    ..Default::default()
                }))
            }
            TableKind::FileVariantStats => {
                let ((table_id, data_file_id, column_id), value) =
                    decode::decode_file_variant_stats(cells)?;
                if scope.is_some_and(|scope| scope != table_id) {
                    continue;
                }
                EntityRecord::FileColumnStats(Arc::new(proto::FileColumnStatsValue {
                    table_id,
                    data_file_id,
                    column_id,
                    variant_stats: vec![value],
                    ..Default::default()
                }))
            }
            TableKind::MacroImpl => {
                let (macro_id, implementation) = decode::decode_macro_impl(cells)?;
                EntityRecord::Macro(Arc::new(proto::MacroValue {
                    macro_id,
                    implementations: vec![implementation],
                    ..Default::default()
                }))
            }
            TableKind::MacroParameters => {
                let ((macro_id, impl_id), parameter) = decode::decode_macro_parameter(cells)?;
                EntityRecord::Macro(Arc::new(proto::MacroValue {
                    macro_id,
                    implementations: vec![proto::MacroImplementation {
                        impl_id,
                        parameters: vec![parameter],
                        ..Default::default()
                    }],
                    ..Default::default()
                }))
            }
            TableKind::NameMapping => {
                let (mapping_id, mapping) = decode::decode_name_mapping(cells)?;
                EntityRecord::Mapping(Arc::new(proto::MappingValue {
                    mapping_id,
                    name_mappings: vec![mapping],
                    ..Default::default()
                }))
            }
            _ => return Err(Error::Corruption("invalid staged child kind".into())),
        };
        rows.push(row);
    }
    Ok(rows)
}

async fn owners(
    table: TableKind,
    operations: &[RowOperation],
    overlay: &Overlay,
    scope: Option<u64>,
    handle: OwnedReadHandle,
) -> Result<BTreeMap<u64, Option<u64>>> {
    let mut owners = BTreeMap::new();
    if matches!(
        table,
        TableKind::PartitionColumn | TableKind::SortExpression
    ) {
        for operation in operations {
            if let RowOperation::Insert { table: kind, cells } = operation
                && *kind == table
            {
                let id = if table == TableKind::PartitionColumn {
                    decode::decode_partition_column(cells)?.0
                } else {
                    decode::decode_sort_expression(cells)?.0
                };
                owners.insert(id, None);
            }
        }
        if !owners.is_empty() {
            let kind = if table == TableKind::PartitionColumn {
                EntityRecordKind::Partition
            } else {
                EntityRecordKind::Sort
            };
            let mut cursor = OwnedEntityCursor::open(
                handle,
                kind,
                Versions::LiveAndEnded,
                scope,
                crate::read_policy::CacheAdmission::ReuseOnly,
            )
            .await?;
            while let Some(held_record) = cursor.next().await? {
                let row = held_record.value().clone();
                if let Some(row) = overlay.apply(row)? {
                    record_owner(&mut owners, &row);
                }
            }
            let mut index = 0;
            while let Some((row, _memory)) = overlay.inserted(index)? {
                record_owner(&mut owners, &row);
                index += 1;
            }
        }
    }
    Ok(owners)
}

fn record_owner(owners: &mut BTreeMap<u64, Option<u64>>, record: &EntityRecord) {
    let (id, table) = match record {
        EntityRecord::Partition(row) => (row.partition_id, row.table_id),
        EntityRecord::Sort(row) => (row.sort_id, row.table_id),
        _ => return,
    };
    if let Some(owner) = owners.get_mut(&id) {
        *owner = Some(table);
    }
}

pub(super) fn export_record(
    record: &EntityRecord,
) -> Result<Option<(EntityRecord, MemoryReservation)>> {
    match record {
        EntityRecord::Partition(row) => export(Some(row.as_ref()), EntityRecord::Partition),
        EntityRecord::Sort(row) => export(Some(row.as_ref()), EntityRecord::Sort),
        EntityRecord::File(row) => export(Some(row.as_ref()), EntityRecord::File),
        EntityRecord::FileColumnStats(row) => {
            export(Some(row.as_ref()), EntityRecord::FileColumnStats)
        }
        EntityRecord::Macro(row) => export(Some(row.as_ref()), EntityRecord::Macro),
        EntityRecord::Mapping(row) => export(Some(row.as_ref()), EntityRecord::Mapping),
        _ => Err(Error::Corruption("invalid staged child container".into())),
    }
}
