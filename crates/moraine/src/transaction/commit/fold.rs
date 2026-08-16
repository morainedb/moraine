//! Folding a committed batch forward into a materialized view: put the
//! decoded value at each `current` key, remove it on a delete. Deletes
//! mirror the store key by key with no cascade; the batch carries each
//! ended entity's own delete.

use super::StagedWrite;
use crate::{
    catalog::CatalogSnapshot,
    error::{Error, Result},
    store::{
        key::{CurrentKey, EntityKey, Key, SysKey},
        proto::{HeadValue, SnapshotValue},
        read::decode_entity,
        value,
    },
};

/// Folds one committed batch into `view`, advancing it to the batch's head.
/// A write whose key cannot be decoded, or a deletion of an immutable kind,
/// is refused; the caller must drop the cached view.
pub(crate) fn fold_batch(view: &mut CatalogSnapshot, writes: &[StagedWrite]) -> Result<()> {
    let mut new_head: Option<u64> = None;
    let mut minted: Option<SnapshotValue> = None;

    for (encoded_key, write) in writes {
        let bytes = write.as_deref();
        match Key::decode(encoded_key)? {
            Key::Current(CurrentKey::Entity(entity)) => match bytes {
                Some(bytes) => view.put_record(decode_entity(entity, bytes)?),
                None => remove_entity(view, entity)?,
            },
            Key::Current(CurrentKey::GcFile { data_file_id }) => match bytes {
                Some(bytes) => view.put_gc_file(value::decode_value(bytes)?),
                None => view.remove_gc_file(data_file_id),
            },
            Key::Snapshot { .. } => {
                if let Some(bytes) = bytes {
                    minted = Some(value::decode_value(bytes)?);
                }
            }
            Key::Sys(SysKey::Head) => {
                if let Some(bytes) = bytes {
                    let head: HeadValue = value::decode_value(bytes)?;
                    new_head = Some(head.snapshot_id);
                    view.batch_seq = head.batch_seq;
                }
            }
            // History mirrors, index entries, inline data, and the format
            // stamp do not appear in the current head view.
            _ => {}
        }
    }

    // A head-preserving batch keeps the current snapshot metadata.
    if let (Some(head), Some(snapshot)) = (new_head, minted)
        && snapshot.snapshot_id == head
    {
        view.snapshot = snapshot;
    }

    Ok(())
}

/// Removes exactly the entity at one `current` key, keeping the name
/// indexes coherent and cascading to nothing.
fn remove_entity(view: &mut CatalogSnapshot, entity: EntityKey) -> Result<()> {
    match entity {
        EntityKey::Schema { schema_id } => view.remove_schema_only(schema_id),
        EntityKey::Table { table_id } => view.remove_table_only(table_id),
        EntityKey::View { view_id } => view.delete_view(view_id),
        EntityKey::Column {
            table_id,
            column_id,
        } => view.remove_column_only(table_id, column_id),
        EntityKey::File {
            table_id,
            data_file_id,
        } => view.delete_data_file(table_id, data_file_id),
        EntityKey::DeleteFile {
            table_id,
            delete_file_id,
        } => view.delete_delete_file(table_id, delete_file_id),
        EntityKey::Partition {
            table_id,
            partition_id,
        } => view.delete_partition(table_id, partition_id),
        EntityKey::Sort { table_id, sort_id } => view.delete_sort(table_id, sort_id),
        EntityKey::Macro { macro_id } => view.delete_macro(macro_id),
        EntityKey::Index { table_id, index_id } => view.delete_index(table_id, index_id),
        EntityKey::FileColumnStats {
            table_id,
            data_file_id,
            column_id,
        } => view.remove_file_column_stats(table_id, data_file_id, column_id),
        EntityKey::TableStats { table_id } => view.remove_table_stats(table_id),
        EntityKey::TableColumnStats {
            table_id,
            column_id,
        } => view.remove_table_column_stats(table_id, column_id),
        EntityKey::Option {
            scope_kind,
            scope_id,
        } => view.remove_option_record((scope_kind, scope_id)),
        EntityKey::Tag { object_id } => view.remove_tag(object_id),
        EntityKey::Mapping { .. } => {
            return Err(Error::Corruption(
                "column-mapping deletion is unexpected; mappings are immutable".to_string(),
            ));
        }
    }
    Ok(())
}
