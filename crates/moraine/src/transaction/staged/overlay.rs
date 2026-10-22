//! Read-your-writes for the `dump_*` projections: the committed records a
//! staged transaction can see, with its own uncommitted rows applied over
//! them in stage order, decoded by the same decoders translation uses.
//!
//! The overlay is not translation: it never validates, never diffs, and
//! never touches the store. A row translation would refuse still shows up
//! here and fails at commit.

mod records;

use std::collections::{BTreeMap, HashMap};

use super::{
    Cell, EntityKey, Error, Result, RowOperation, TableKind,
    decode::{
        Cursor, decode_end, decode_hard_delete, decode_metadata, decode_metadata_key,
        decode_tag_row,
    },
    proto,
};
use crate::store::read::{EntityRecord, EntityRecordKind};

/// One versioned kind's overlay vocabulary: how a record names itself, how
/// its lifecycle moves, and how a staged row becomes one.
pub(super) trait Versioned: Sized {
    /// The staged-row kind whose rows apply to this record type.
    const KIND: TableKind;

    /// The committed-record kind these rows are read from.
    const RECORD: EntityRecordKind;

    /// `record` as one of these rows, or `None` for another kind's.
    fn from_record(record: &EntityRecord) -> Option<Self>;

    /// The entity a delete or a lifecycle update names.
    fn key(&self) -> EntityKey;

    /// `None` for the live version, `Some` for a history one.
    fn end_snapshot(&self) -> Option<u64>;

    fn set_end_snapshot(&mut self, end: u64);

    /// Rebases the live version's `begin_snapshot`. Defaulted to a no-op;
    /// only `ducklake_data_file` reaches it.
    fn set_begin_snapshot(&mut self, begin: u64) {
        let _ = begin;
    }

    /// Builds a record from one staged insert's cells.
    fn decode_row(cells: &[Cell]) -> Result<Self>;

    /// Fills the fields DuckLake does not supply from the versions already
    /// visible for the same entity. Defaulted to a no-op; only
    /// `ducklake_table` reaches it, to carry its field-id counter forward.
    fn carry_forward(&mut self, prior: &[&Self]) {
        let _ = prior;
    }
}

/// `rows` with the transaction's staged rows for `T::KIND` applied, in
/// stage order: inserts append, a hard delete drops exactly the version it
/// names, an `UPDATE ... SET end_snapshot` ends the live version, and an
/// `UPDATE ... SET begin_snapshot` rebases it.
pub(super) fn overlay_versioned<T: Versioned>(
    ops: &[RowOperation],
    rows: Vec<T>,
) -> Result<Vec<T>> {
    // A delete vacates its slot rather than shifting the rest down, so the
    // positions an entity holds stay valid for the whole pass. Every op
    // reaches its entity's versions through `positions` instead of walking
    // the rows, which is what keeps a flush off the length of the lake.
    let mut rows: Vec<Option<T>> = rows.into_iter().map(Some).collect();
    let mut positions: BTreeMap<EntityKey, Vec<usize>> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        if let Some(row) = row {
            positions.entry(row.key()).or_default().push(index);
        }
    }

    for op in ops {
        match op {
            RowOperation::Insert { table, cells } if *table == T::KIND => {
                let mut row = T::decode_row(cells)?;
                let key = row.key();
                let slots = positions.entry(key).or_default();
                let prior: Vec<&T> = slots
                    .iter()
                    .filter_map(|&index| rows[index].as_ref())
                    .collect();
                row.carry_forward(&prior);
                slots.push(rows.len());
                rows.push(Some(row));
            }
            RowOperation::Delete { table, cells } if *table == T::KIND => {
                let (key, end_snapshot) = decode_hard_delete(*table, cells)?;
                for &index in positions.get(&key).into_iter().flatten() {
                    if rows[index]
                        .as_ref()
                        .is_some_and(|row| row.end_snapshot() == end_snapshot)
                    {
                        rows[index] = None;
                    }
                }
            }
            RowOperation::UpdateSetEnd { table, cells } if *table == T::KIND => {
                let (key, end_snapshot) = decode_end(*table, cells)?;
                for &index in positions.get(&key).into_iter().flatten() {
                    if let Some(row) = rows[index].as_mut()
                        && row.end_snapshot().is_none()
                    {
                        row.set_end_snapshot(end_snapshot);
                    }
                }
            }
            RowOperation::UpdateSetBegin { table, cells } if *table == T::KIND => {
                let (key, begin_snapshot) = decode_begin(*table, cells)?;
                for &index in positions.get(&key).into_iter().flatten() {
                    if let Some(row) = rows[index].as_mut()
                        && row.end_snapshot().is_none()
                    {
                        row.set_begin_snapshot(begin_snapshot);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(rows.into_iter().flatten().collect())
}

/// The `(entity, new begin_snapshot)` an `UPDATE ... SET begin_snapshot`
/// names. DuckLake issues one only against `ducklake_data_file`, during a
/// delete-rewrite.
fn decode_begin(table: TableKind, cells: &[Cell]) -> Result<(EntityKey, u64)> {
    if table != TableKind::DataFile {
        return Err(Error::Constraint(format!(
            "update_set_begin is not defined for {table:?}"
        )));
    }
    let mut c = Cursor::new(table, cells);
    let key = EntityKey::File {
        table_id: c.u64()?,
        data_file_id: c.u64()?,
    };
    let begin_snapshot = c.u64()?;
    c.finish()?;
    Ok((key, begin_snapshot))
}

/// One unversioned kind's overlay vocabulary. These rows carry no
/// lifecycle: they are overwritten in place and removed outright, so the
/// whole vocabulary is a key.
pub(super) trait Unversioned: Sized {
    /// The staged-row kind whose rows apply to this record type.
    const KIND: TableKind;

    /// The committed-record kind these rows are read from.
    const RECORD: EntityRecordKind;

    /// `record` as one of these rows, or `None` for another kind's.
    fn from_record(record: &EntityRecord) -> Option<Self>;

    /// What identifies a row for overwrite and for removal.
    type Key: Eq + std::hash::Hash + Copy;

    fn key(&self) -> Self::Key;

    fn decode_row(cells: &[Cell]) -> Result<Self>;

    /// The key a delete row names — its key columns only, so this is not
    /// [`Self::decode_row`] followed by [`Self::key`].
    fn decode_key(cells: &[Cell]) -> Result<Self::Key>;
}

/// `rows` with the transaction's staged rows for `T::KIND` applied, in
/// stage order: an insert overwrites the row it keys and otherwise appends,
/// a delete removes it.
pub(super) fn overlay_unversioned<T: Unversioned>(
    ops: &[RowOperation],
    rows: Vec<T>,
) -> Result<Vec<T>> {
    // Every op on a key replaces whatever that key held, so one pass over
    // the ops decides each touched key outright. Re-appending put a
    // touched key after the untouched ones, in the order of the *last* op
    // naming it, which is what `staged_order` reconstructs.
    let mut staged: HashMap<T::Key, Option<T>> = HashMap::new();
    let mut order: Vec<T::Key> = Vec::new();
    for op in ops {
        let (key, row) = match op {
            RowOperation::Insert { table, cells } if *table == T::KIND => {
                let row = T::decode_row(cells)?;
                (row.key(), Some(row))
            }
            RowOperation::Delete { table, cells } if *table == T::KIND => {
                (T::decode_key(cells)?, None)
            }
            _ => continue,
        };
        staged.insert(key, row);
        order.push(key);
    }
    if staged.is_empty() {
        return Ok(rows);
    }

    let mut overlaid: Vec<T> = rows
        .into_iter()
        .filter(|existing| !staged.contains_key(&existing.key()))
        .collect();
    overlaid.extend(
        staged_order(order)
            .into_iter()
            .filter_map(|key| staged.remove(&key).flatten()),
    );

    Ok(overlaid)
}

/// `keys` reduced to one entry apiece, each at its last occurrence — the
/// position a key that was removed and re-appended ends up in.
fn staged_order<K: Eq + std::hash::Hash + Copy>(keys: Vec<K>) -> Vec<K> {
    let mut seen = std::collections::HashSet::new();
    let mut last: Vec<K> = keys
        .into_iter()
        .rev()
        .filter(|key| seen.insert(*key))
        .collect();
    last.reverse();
    last
}

/// A kind keyed by table first, whose rows a reader may narrow to one
/// table's.
pub(super) trait TableScoped {
    fn table_id(&self) -> u64;
}

/// `containers` with the transaction's staged `ducklake_tag` rows folded
/// in: an insert adds an entry (minting the container if needed), an
/// `UPDATE ... SET end_snapshot` ends the live entry with that key, and a
/// delete removes the entry it names, taking an emptied container with it.
/// Naming an absent entry leaves the view unchanged.
pub(super) fn overlay_tag_containers(
    ops: &[RowOperation],
    mut containers: Vec<proto::TagValue>,
) -> Result<Vec<proto::TagValue>> {
    for op in ops {
        match op {
            RowOperation::Insert { table, cells } if *table == TableKind::Tag => {
                let (object_id, entry) = decode_tag_row(cells)?;
                match containers.iter_mut().find(|c| c.object_id == object_id) {
                    Some(container) => container.entries.push(entry),
                    None => containers.push(proto::TagValue {
                        object_id,
                        entries: vec![entry],
                    }),
                }
            }
            RowOperation::UpdateSetEnd { table, cells } if *table == TableKind::Tag => {
                let mut cursor = Cursor::new(TableKind::Tag, cells);
                let object_id = cursor.u64()?;
                let key = cursor.string()?;
                let end_snapshot = cursor.u64()?;
                cursor.finish()?;
                if let Some(container) = containers.iter_mut().find(|c| c.object_id == object_id)
                    && let Some(entry) = container
                        .entries
                        .iter_mut()
                        .find(|e| e.key == key && e.end_snapshot.is_none())
                {
                    entry.end_snapshot = Some(end_snapshot);
                }
            }
            RowOperation::Delete { table, cells } if *table == TableKind::Tag => {
                let mut cursor = Cursor::new(TableKind::Tag, cells);
                let object_id = cursor.u64()?;
                let key = cursor.string()?;
                let begin_snapshot = cursor.u64()?;
                cursor.finish()?;
                if let Some(container) = containers.iter_mut().find(|c| c.object_id == object_id) {
                    container
                        .entries
                        .retain(|e| !(e.key == key && e.begin_snapshot == begin_snapshot));
                }
                containers.retain(|c| !c.entries.is_empty());
            }
            _ => {}
        }
    }

    Ok(containers)
}

/// `scopes` with the transaction's staged `ducklake_metadata` rows applied:
/// an insert sets its key in the scope's record (minting the scope if
/// needed) and a delete removes it, taking an emptied scope with it. A
/// delete of an absent key is a no-op.
pub(super) fn overlay_option_scopes(
    ops: &[RowOperation],
    mut scopes: Vec<(u64, u64, proto::OptionScopeValue)>,
) -> Result<Vec<(u64, u64, proto::OptionScopeValue)>> {
    for op in ops {
        let (components, key, value) = match op {
            RowOperation::Insert { table, cells } if *table == TableKind::Metadata => {
                let (components, key, value) = decode_metadata(cells)?;
                (components, key, Some(value))
            }
            RowOperation::Delete { table, cells } if *table == TableKind::Metadata => {
                let (components, key) = decode_metadata_key(cells)?;
                (components, key, None)
            }
            _ => continue,
        };

        let at = scopes
            .iter()
            .position(|(scope_kind, scope_id, _)| (*scope_kind, *scope_id) == components);
        match (at, value) {
            (Some(at), Some(value)) => {
                scopes[at].2.options.insert(key, value);
            }
            (Some(at), None) => {
                scopes[at].2.options.remove(&key);
            }
            (None, Some(value)) => scopes.push((
                components.0,
                components.1,
                proto::OptionScopeValue {
                    options: std::collections::HashMap::from([(key, value)]),
                },
            )),
            (None, None) => {}
        }
        scopes.retain(|(.., value)| !value.options.is_empty());
    }

    Ok(scopes)
}

/// Applies view-column tag inserts, lifecycle updates, and deletes in order.
pub(super) fn overlay_view_column_tags(
    ops: &[RowOperation],
    mut rows: Vec<(u64, proto::ViewColumnTag)>,
) -> Result<Vec<(u64, proto::ViewColumnTag)>> {
    for op in ops {
        match op {
            RowOperation::Insert {
                table: TableKind::ViewColumnTag,
                cells,
            } => {
                rows.push(super::decode::decode_view_column_tag_row(cells)?);
            }
            RowOperation::UpdateSetEnd {
                table: TableKind::ViewColumnTag,
                cells,
            }
            | RowOperation::Delete {
                table: TableKind::ViewColumnTag,
                cells,
            } => {
                let mut cursor = Cursor::new(TableKind::ViewColumnTag, cells);
                let view_id = cursor.u64()?;
                let column_name = cursor.string()?;
                let key = cursor.string()?;
                let snapshot = cursor.u64()?;
                cursor.finish()?;
                if matches!(op, RowOperation::Delete { .. }) {
                    rows.retain(|(id, tag)| {
                        !(*id == view_id
                            && tag.column_name == column_name
                            && tag.key == key
                            && tag.begin_snapshot == snapshot)
                    });
                } else {
                    for (id, tag) in &mut rows {
                        if *id == view_id
                            && tag.column_name == column_name
                            && tag.key == key
                            && tag.end_snapshot.is_none()
                        {
                            tag.end_snapshot = Some(snapshot);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(rows)
}
