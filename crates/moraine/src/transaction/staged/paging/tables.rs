//! Field-id inheritance for inserted table versions, without retaining history.

use std::collections::{BTreeMap, BTreeSet};

use super::{RowOperation, TableKind, VersionedRows, decode, overlay, proto};
use crate::{
    Error, Result,
    store::{
        handle::OwnedReadHandle,
        key::EntityKey,
        read::{EntityRecord, OwnedEntityCursor, Versions},
    },
};

enum Action {
    Insert(usize),
    End(u64),
    Delete(Option<u64>),
}

pub(super) async fn rows(
    operations: &[RowOperation],
    handle: OwnedReadHandle,
) -> Result<VersionedRows<proto::TableValue>> {
    let mut rows = VersionedRows::<proto::TableValue>::new(operations, |_| true)?;
    let mut actions: BTreeMap<EntityKey, Vec<Action>> = BTreeMap::new();
    let mut inserted = BTreeSet::new();
    for (index, operation) in operations.iter().enumerate() {
        let (key, action) = match operation {
            RowOperation::Insert {
                table: TableKind::Table,
                cells,
            } => {
                let table = decode::decode_table(cells)?;
                let key = EntityKey::Table {
                    table_id: table.table_id,
                };
                inserted.insert(key);
                (key, Action::Insert(index))
            }
            RowOperation::UpdateSetEnd {
                table: TableKind::Table,
                cells,
            } => {
                let (key, end) = decode::decode_end(TableKind::Table, cells)?;
                (key, Action::End(end))
            }
            RowOperation::Delete {
                table: TableKind::Table,
                cells,
            } => {
                let (key, end) = decode::decode_hard_delete(TableKind::Table, cells)?;
                (key, Action::Delete(end))
            }
            _ => continue,
        };
        actions.entry(key).or_default().push(action);
    }
    let mut inherited = BTreeMap::new();
    for entity in inserted {
        let Some(actions) = actions.get(&entity) else {
            continue;
        };
        let mut cursor =
            OwnedEntityCursor::open_entity(handle.clone(), entity, Versions::LiveAndEnded).await?;
        while let Some(record_owner) = cursor.next().await? {
            let record = record_owner.value().clone();
            let EntityRecord::Table(table) = record else {
                return Err(Error::Corruption(
                    "table inheritance read a different entity".into(),
                ));
            };
            observe(
                actions,
                table.end_snapshot,
                table.next_column_id,
                &mut inherited,
            );
        }
    }
    rows.inserted = overlay::overlay_versioned_with_inheritance::<proto::TableValue>(
        operations,
        Vec::new(),
        |index, table| {
            if let Some(counter) = inherited.get(&index) {
                table.next_column_id = table.next_column_id.max(*counter);
            }
        },
    )?;
    Ok(rows)
}

fn observe(
    actions: &[Action],
    mut end: Option<u64>,
    counter: u64,
    inherited: &mut BTreeMap<usize, u64>,
) {
    let mut alive = true;
    for action in actions {
        match action {
            Action::Insert(index) if alive => {
                let prior = inherited.entry(*index).or_default();
                *prior = (*prior).max(counter);
            }
            Action::End(snapshot) if alive && end.is_none() => end = Some(*snapshot),
            Action::Delete(snapshot) if *snapshot == end => alive = false,
            _ => {}
        }
    }
}
