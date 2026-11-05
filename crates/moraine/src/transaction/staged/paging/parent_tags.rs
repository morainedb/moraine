//! Authoritative tag parents after staged lifecycle edits.

use super::{
    Arc, BTreeMap, EntityKey, EntityRecord, Error, MemoryReservation, Result, RowOperation,
    TableKind, Versioned, VersionedRows, decode, export, operation_kind, overlay, proto,
};
use crate::store::{
    handle::OwnedReadHandle,
    read::{OwnedEntityCursor, Versions},
};

pub(super) struct Parents {
    edits: Vec<(EntityKey, Vec<RowOperation>)>,
}

impl Parents {
    pub(super) fn new(table: TableKind, operations: &[RowOperation]) -> Result<Self> {
        let parent = if table == TableKind::ColumnTag {
            TableKind::Column
        } else {
            TableKind::View
        };
        let mut edits: BTreeMap<EntityKey, Vec<RowOperation>> = BTreeMap::new();
        for operation in operations {
            let Some(kind) = operation_kind(operation) else {
                continue;
            };
            if kind != parent && kind != table {
                continue;
            }
            let key = if kind == parent {
                match operation {
                    RowOperation::Insert { cells, .. } if parent == TableKind::Column => {
                        decode::decode_column(cells)?.key()
                    }
                    RowOperation::Insert { cells, .. } => decode::decode_view(cells)?.key(),
                    RowOperation::Delete { cells, .. } => {
                        decode::decode_hard_delete(parent, cells)?.0
                    }
                    RowOperation::UpdateSetEnd { cells, .. } => {
                        decode::decode_end(parent, cells)?.0
                    }
                    RowOperation::UpdateSetBegin { cells, .. } => {
                        overlay::decode_begin(parent, cells)?.0
                    }
                    _ => continue,
                }
            } else {
                match operation {
                    RowOperation::Insert { cells, .. } if table == TableKind::ColumnTag => {
                        let ((table_id, column_id), _) = decode::decode_column_tag_row(cells)?;
                        EntityKey::Column {
                            table_id,
                            column_id,
                        }
                    }
                    RowOperation::Insert { cells, .. } => EntityKey::View {
                        view_id: decode::decode_view_column_tag_row(cells)?.0,
                    },
                    RowOperation::Delete { cells, .. }
                    | RowOperation::UpdateSetEnd { cells, .. }
                        if table == TableKind::ViewColumnTag =>
                    {
                        let mut cursor = super::super::Cursor::new(table, cells);
                        EntityKey::View {
                            view_id: cursor.u64()?,
                        }
                    }
                    _ => continue,
                }
            };
            edits.entry(key).or_default().push(operation.clone());
        }
        Ok(Self {
            edits: edits.into_iter().collect(),
        })
    }

    pub(super) fn apply(&self, record: EntityRecord) -> Result<Option<EntityRecord>> {
        let key = match &record {
            EntityRecord::Column(row) => row.key(),
            EntityRecord::View(row) => row.key(),
            _ => {
                return Err(Error::Corruption(
                    "tag cursor received a different parent kind".into(),
                ));
            }
        };
        Ok(self
            .edits
            .binary_search_by_key(&key, |(key, _)| *key)
            .is_err()
            .then_some(record))
    }

    pub(super) async fn replacement(
        &self,
        index: usize,
        handle: OwnedReadHandle,
    ) -> Result<Option<(EntityRecord, MemoryReservation)>> {
        let Some((key, operations)) = self.edits.get(index) else {
            return Ok(None);
        };
        match *key {
            EntityKey::Column {
                table_id,
                column_id,
            } => {
                let selected = latest::<proto::ColumnValue>(handle, *key, operations).await?;
                let mut row = selected.as_ref().map_or_else(
                    || proto::ColumnValue {
                        table_id,
                        column_id,
                        ..Default::default()
                    },
                    |(row, _)| row.clone(),
                );
                for operation in operations {
                    if let RowOperation::Insert {
                        table: TableKind::ColumnTag,
                        cells,
                    } = operation
                    {
                        row.tags.push(decode::decode_column_tag_row(cells)?.1);
                    }
                }
                export(Some(&row), EntityRecord::Column)
            }
            EntityKey::View { view_id } => {
                let selected = latest::<proto::ViewValue>(handle, *key, operations).await?;
                let mut row = selected.as_ref().map_or_else(
                    || proto::ViewValue {
                        view_id,
                        ..Default::default()
                    },
                    |(row, _)| row.clone(),
                );
                row.column_tags = overlay::overlay_view_column_tags(
                    operations,
                    row.column_tags
                        .into_iter()
                        .map(|tag| (view_id, tag))
                        .collect(),
                )?
                .into_iter()
                .map(|(_, tag)| tag)
                .collect();
                export(Some(&row), EntityRecord::View)
            }
            _ => Err(Error::Corruption("invalid staged tag parent".into())),
        }
    }
}

async fn latest<T: Versioned + Clone + prost::Message>(
    handle: OwnedReadHandle,
    key: EntityKey,
    operations: &[RowOperation],
) -> Result<Option<(T, MemoryReservation)>> {
    let rows = VersionedRows::<T>::new(operations, |_| true)?;
    let mut cursor = OwnedEntityCursor::open_entity(handle, key, Versions::LiveAndEnded).await?;
    let mut selected = None;
    while let Some((record, _memory)) = cursor.next().await? {
        let row = T::from_record(&record)
            .ok_or_else(|| Error::Corruption("invalid tag parent version".into()))?;
        if let Some(row) = rows.apply(Arc::new(row))? {
            select(&mut selected, row.as_ref())?;
        }
    }
    for row in &rows.inserted {
        select(&mut selected, row)?;
    }
    Ok(selected)
}

fn select<T: Versioned + Clone + prost::Message>(
    selected: &mut Option<(T, MemoryReservation)>,
    row: &T,
) -> Result<()> {
    let newer = selected.as_ref().is_none_or(|(prior, _)| {
        match (row.end_snapshot(), prior.end_snapshot()) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(end), Some(prior)) => end > prior,
        }
    });
    if newer {
        let memory = MemoryReservation::reserve(
            (row.encoded_len() as u64)
                .saturating_mul(4)
                .saturating_add(size_of::<T>() as u64)
                .saturating_add(128),
        )?;
        *selected = Some((row.clone(), memory));
    }
    Ok(())
}
