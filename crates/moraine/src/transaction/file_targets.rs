//! Current delete-file target maintenance and scoped reads.

use std::{collections::BTreeMap, sync::Arc};

use super::{
    commit::{FORMAT_WITH_DELETE_TARGETS, StagedWrite},
    session::CommitSession,
};
use crate::{
    Error, MemoryReservation, Result,
    store::{
        StagedBytes,
        handle::{ReadHandle, ScanIterator, ScanShape},
        key::{
            CurrentKey, EntityKey, EntityKind, Key, current_entity_kind_prefix,
            delete_target_prefix,
        },
        proto::DeleteFileValue,
        read::{self, EntityCursor, EntityRecord, EntityRecordKind, Versions},
        value,
    },
};

pub(crate) fn entry(entity: EntityKey, bytes: &[u8]) -> Result<Option<Key>> {
    let EntityKey::DeleteFile {
        table_id,
        delete_file_id,
    } = entity
    else {
        return Err(Error::Corruption("invalid delete-target source key".into()));
    };
    let row: DeleteFileValue = value::decode_value(bytes)?;
    if row.table_id != table_id || row.delete_file_id != delete_file_id {
        return Err(Error::Corruption(
            "delete-target source IDs disagree".into(),
        ));
    }
    Ok(row.end_snapshot.is_none().then_some(Key::DeleteTarget {
        table_id,
        data_file_id: row.data_file_id,
        delete_file_id,
    }))
}

pub(crate) async fn stage(session: &CommitSession, writes: &[StagedWrite]) -> Result<StagedBytes> {
    let prefix = current_entity_kind_prefix(EntityKind::DeleteFile);
    let mut changed = BTreeMap::new();
    for (key, bytes) in writes {
        if key.starts_with(&prefix) {
            let Key::Current(CurrentKey::Entity(entity)) = Key::decode(key)? else {
                return Err(Error::Corruption("invalid delete-target source".into()));
            };
            changed.insert(entity, bytes.as_deref());
        }
    }
    if changed.is_empty()
        || read::read_format(session.handle())
            .await?
            .is_none_or(|stamp| stamp.format_version < FORMAT_WITH_DELETE_TARGETS)
    {
        return Ok(StagedBytes::default());
    }
    let mut staged = StagedBytes::default();
    for (entity, after) in changed {
        let before = session.handle().get(Key::current(entity).encode()).await?;
        let _memory = MemoryReservation::reserve(
            (before.as_ref().map_or(0, bytes::Bytes::len) as u64)
                .saturating_add(after.map_or(0, <[u8]>::len) as u64)
                .saturating_mul(4)
                .saturating_add(256),
        )?;
        let before = before
            .as_deref()
            .map(|bytes| entry(entity, bytes))
            .transpose()?
            .flatten();
        let after = after
            .map(|bytes| entry(entity, bytes))
            .transpose()?
            .flatten();
        if before == after {
            continue;
        }
        if let Some(before) = before {
            let key = before.encode();
            staged.add(key.len(), 0);
            session.delete(key)?;
        }
        if let Some(after) = after {
            let key = after.encode();
            staged.add(key.len(), 0);
            session.put(key, [])?;
        }
    }
    Ok(staged)
}

enum Records<'a> {
    Indexed(ScanIterator),
    Legacy(EntityCursor<'a>),
}

/// One target's live delete records, with legacy scan compatibility.
pub(crate) struct DeleteCursor<'a> {
    handle: ReadHandle<'a>,
    table: u64,
    file: u64,
    records: Records<'a>,
}

impl<'a> DeleteCursor<'a> {
    pub(crate) async fn open(handle: ReadHandle<'a>, table: u64, file: u64) -> Result<Self> {
        let indexed = read::read_format(handle)
            .await?
            .is_some_and(|stamp| stamp.format_version >= FORMAT_WITH_DELETE_TARGETS);
        let records = if indexed {
            Records::Indexed(
                handle
                    .scan_prefix(
                        delete_target_prefix(table, file),
                        ..,
                        ScanShape::bounded().reuse_only(),
                    )
                    .await?,
            )
        } else {
            Records::Legacy(
                EntityCursor::open(
                    handle,
                    EntityRecordKind::DeleteFile,
                    Versions::Live,
                    Some(table),
                    crate::read_policy::CacheAdmission::Retain,
                )
                .await?,
            )
        };
        Ok(Self {
            handle,
            table,
            file,
            records,
        })
    }

    pub(crate) async fn next(
        &mut self,
    ) -> Result<Option<(Arc<DeleteFileValue>, MemoryReservation)>> {
        match &mut self.records {
            Records::Legacy(cursor) => {
                while let Some((record, memory)) = cursor
                    .next()
                    .await?
                    .map(crate::memory::Reserved::into_parts)
                {
                    if let EntityRecord::DeleteFile(row) = record
                        && row.data_file_id == self.file
                        && row.end_snapshot.is_none()
                    {
                        return Ok(Some((row, memory)));
                    }
                }
                Ok(None)
            }
            Records::Indexed(cursor) => {
                let Some(record) = cursor.next().await? else {
                    return Ok(None);
                };
                let target = Key::decode(&record.key)?;
                let Key::DeleteTarget {
                    table_id,
                    data_file_id,
                    delete_file_id,
                } = target
                else {
                    return Err(Error::Corruption(
                        "invalid delete-target directory key".into(),
                    ));
                };
                if table_id != self.table || data_file_id != self.file {
                    return Err(Error::Corruption("delete-target prefix disagrees".into()));
                }
                let bytes = self
                    .handle
                    .get(
                        Key::current(EntityKey::DeleteFile {
                            table_id,
                            delete_file_id,
                        })
                        .encode(),
                    )
                    .await?
                    .ok_or_else(|| {
                        Error::Corruption(
                            "delete target references an absent current record".into(),
                        )
                    })?;
                let memory = MemoryReservation::reserve(
                    (bytes.len() as u64).saturating_mul(4).saturating_add(256),
                )?;
                let row: DeleteFileValue = value::decode_value(&bytes)?;
                if row.table_id != table_id
                    || row.data_file_id != data_file_id
                    || row.delete_file_id != delete_file_id
                    || row.end_snapshot.is_some()
                {
                    return Err(Error::Corruption(
                        "delete target disagrees with current record".into(),
                    ));
                }
                Ok(Some((Arc::new(row), memory)))
            }
        }
    }
}

#[cfg(test)]
mod tests;
