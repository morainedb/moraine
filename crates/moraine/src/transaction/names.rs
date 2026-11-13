//! Atomic current-name directory upkeep and exact-name resolution.

use std::collections::BTreeMap;

use super::{
    commit::{FORMAT_WITH_NAMES, StagedWrite},
    session::CommitSession,
};
use crate::{
    Error, MemoryReservation, Result,
    store::{
        StagedBytes,
        handle::{ReadHandle, ScanOrder, ScanShape},
        key::{
            CurrentKey, EntityKey, EntityKind, Key, NameKey, NameKind, current_entity_kind_prefix,
            name_prefix,
        },
        proto,
        read::{self, EntityCursor, EntityRecord, EntityRecordKind, Versions},
        value,
    },
};

pub(crate) fn entry(key: EntityKey, bytes: &[u8]) -> Result<Option<NameKey>> {
    Ok(match key {
        EntityKey::Schema { schema_id } => {
            let row: proto::SchemaValue = value::decode_value(bytes)?;
            if row.schema_id != schema_id {
                return Err(Error::Corruption(
                    "schema name entry has a different entity ID".into(),
                ));
            }
            row.end_snapshot.is_none().then_some(NameKey {
                kind: NameKind::Schema,
                parent: 0,
                name: row.schema_name,
                entity_id: schema_id,
            })
        }
        EntityKey::Table { table_id } => {
            let row: proto::TableValue = value::decode_value(bytes)?;
            if row.table_id != table_id {
                return Err(Error::Corruption(
                    "table name entry has a different entity ID".into(),
                ));
            }
            row.end_snapshot.is_none().then_some(NameKey {
                kind: NameKind::Table,
                parent: row.schema_id,
                name: row.table_name,
                entity_id: table_id,
            })
        }
        _ => None,
    })
}

pub(crate) async fn stage(session: &CommitSession, writes: &[StagedWrite]) -> Result<StagedBytes> {
    let prefixes = [
        current_entity_kind_prefix(EntityKind::Schema),
        current_entity_kind_prefix(EntityKind::Table),
    ];
    let mut changed = BTreeMap::new();
    for (key, bytes) in writes {
        if prefixes.iter().any(|prefix| key.starts_with(prefix)) {
            let Key::Current(CurrentKey::Entity(entity)) = Key::decode(key)? else {
                return Err(Error::Corruption("invalid current name source".into()));
            };
            changed.insert(entity, bytes.as_deref());
        }
    }
    if changed.is_empty()
        || read::read_format(session.handle())
            .await?
            .is_none_or(|stamp| stamp.format_version < FORMAT_WITH_NAMES)
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
            let key = Key::Name(before).encode();
            staged.add(key.len(), 0);
            session.delete(key)?;
        }
        if let Some(after) = after {
            let key = Key::Name(after).encode();
            staged.add(key.len(), 0);
            session.put(key, [])?;
        }
    }
    Ok(staged)
}

pub(crate) async fn lookup(
    handle: ReadHandle<'_>,
    kind: NameKind,
    parent: u64,
    name: &str,
) -> Result<Option<u64>> {
    let indexed = read::read_format(handle)
        .await?
        .is_some_and(|stamp| stamp.format_version >= FORMAT_WITH_NAMES);
    if !indexed {
        return scan(handle, kind, parent, name).await;
    }
    let mut candidates = handle
        .scan_prefix_ordered(
            name_prefix(kind, parent, name),
            ..,
            ScanShape::seek().retain(),
            ScanOrder::Descending,
        )
        .await?;
    let Some(candidate) = candidates.next().await? else {
        return Ok(None);
    };
    let Key::Name(indexed) = Key::decode(&candidate.key)? else {
        return Err(Error::Corruption("invalid name directory key".into()));
    };
    let entity = match kind {
        NameKind::Schema => EntityKey::Schema {
            schema_id: indexed.entity_id,
        },
        NameKind::Table => EntityKey::Table {
            table_id: indexed.entity_id,
        },
    };
    let current = handle
        .get(Key::current(entity).encode())
        .await?
        .ok_or_else(|| {
            Error::Corruption("name entry references an absent current entity".into())
        })?;
    let _memory =
        MemoryReservation::reserve((current.len() as u64).saturating_mul(4).saturating_add(256))?;
    if entry(entity, &current)?.as_ref() != Some(&indexed) {
        return Err(Error::Corruption(
            "name entry disagrees with its current entity".into(),
        ));
    }
    Ok(Some(indexed.entity_id))
}

async fn scan(
    handle: ReadHandle<'_>,
    kind: NameKind,
    parent: u64,
    name: &str,
) -> Result<Option<u64>> {
    let record = match kind {
        NameKind::Schema => EntityRecordKind::Schema,
        NameKind::Table => EntityRecordKind::Table,
    };
    let mut cursor = EntityCursor::open(handle, record, Versions::Live, None).await?;
    let mut found = None;
    while let Some((record, _memory)) = cursor.next().await? {
        match record {
            EntityRecord::Schema(row) if row.schema_name == name => found = Some(row.schema_id),
            EntityRecord::Table(row) if row.schema_id == parent && row.table_name == name => {
                found = Some(row.table_id);
            }
            _ => {}
        }
    }
    Ok(found)
}
