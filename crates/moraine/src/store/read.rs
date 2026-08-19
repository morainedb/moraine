//! Typed reads over an open transaction: decode keys and values into the
//! wire types. No interpretation — the domain layer owns meaning.

use crate::{
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{CurrentKey, EntityKey, Key, Subspace, SysKey, subspace_prefix},
        proto::{
            ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue, FormatValue,
            GcFileValue, HeadValue, IndexValue, MacroValue, MappingValue, MigrationValue,
            OptionScopeValue, PartitionValue, SchemaValue, SchemaVersionValue, SecretValue,
            SnapshotValue, SortValue, TableColumnStatsValue, TableStatsValue, TableValue, TagValue,
            ViewValue,
        },
        value,
    },
};

/// A decoded entity record of a kind the catalog currently models.
/// Reading a kind outside this set fails loudly rather than dropping it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EntityRecord {
    /// A schema record.
    Schema(SchemaValue),
    /// A table record.
    Table(TableValue),
    /// A view record.
    View(ViewValue),
    /// A column record.
    Column(ColumnValue),
    /// A data file record.
    File(DataFileValue),
    /// A delete file record.
    DeleteFile(DeleteFileValue),
    /// A partition spec record.
    Partition(PartitionValue),
    /// A sort spec record.
    Sort(SortValue),
    /// A macro record.
    Macro(MacroValue),
    /// A column-mapping record.
    Mapping(MappingValue),
    /// An equality-index definition record.
    Index(IndexValue),
    /// File-level column statistics record.
    FileColumnStats(FileColumnStatsValue),
    /// Table-level statistics record.
    TableStats(TableStatsValue),
    /// Table-level column statistics record.
    TableColumnStats(TableColumnStatsValue),
    /// An option-scope record; the scope lives in the key, not the value.
    Option {
        /// Scope kind: global = 0, schema = 1, table = 2.
        scope_kind: u64,
        /// Scope id (0 for global).
        scope_id: u64,
        /// The scope's options map.
        value: OptionScopeValue,
    },
    /// A tag container: one record per tagged object, entries embedded.
    Tag(TagValue),
    /// A `ducklake_files_scheduled_for_deletion` row — `current`-only
    /// bookkeeping, not a temporal entity.
    GcFile(GcFileValue),
}

impl EntityRecord {
    /// The record's begin/end lifecycle; `None` for unversioned kinds,
    /// which are live at any time-travel target.
    pub(crate) fn lifecycle(&self) -> Option<(u64, Option<u64>)> {
        match self {
            Self::Schema(s) => Some((s.begin_snapshot, s.end_snapshot)),
            Self::Table(t) => Some((t.begin_snapshot, t.end_snapshot)),
            Self::View(v) => Some((v.begin_snapshot, v.end_snapshot)),
            Self::Column(c) => Some((c.begin_snapshot, c.end_snapshot)),
            Self::File(f) => Some((f.begin_snapshot, f.end_snapshot)),
            Self::DeleteFile(d) => Some((d.begin_snapshot, d.end_snapshot)),
            Self::Partition(p) => Some((p.begin_snapshot, p.end_snapshot)),
            Self::Sort(s) => Some((s.begin_snapshot, s.end_snapshot)),
            Self::Macro(m) => Some((m.begin_snapshot, m.end_snapshot)),
            Self::Index(i) => Some((i.begin_snapshot, i.end_snapshot)),
            Self::Mapping(_)
            | Self::FileColumnStats(_)
            | Self::TableStats(_)
            | Self::TableColumnStats(_)
            | Self::Option { .. }
            | Self::Tag(_)
            | Self::GcFile(_) => None,
        }
    }
}

/// Point read of one key, decoded, `None` when absent.
pub(crate) async fn read_singleton<M: prost::Message + Default>(
    handle: ReadHandle<'_>,
    key: Key,
) -> Result<Option<M>> {
    handle
        .get(key.encode())
        .await?
        .map(|bytes| value::decode_value(&bytes))
        .transpose()
}

/// Scans every key under `prefix`, decoding each entry with `extract`;
/// `extract` rejects keys of the wrong kind with its scan's corruption
/// error.
pub(crate) async fn scan_decode<T>(
    handle: ReadHandle<'_>,
    prefix: Vec<u8>,
    mut extract: impl FnMut(Key, &[u8]) -> Result<T>,
) -> Result<Vec<T>> {
    let mut iter = handle.scan_prefix(prefix, ..).await?;
    let mut records = Vec::new();
    while let Some(entry) = iter.next().await? {
        records.push(extract(Key::decode(&entry.key)?, &entry.value)?);
    }

    Ok(records)
}

/// As [`scan_decode`], but with the unfolded tail overlaid over the store:
/// a tail write shadows the stored value, a tail delete hides it. The merge
/// is last-writer-wins by key, so a slot-backed dump sees a winner no folder
/// has applied yet.
pub(crate) async fn scan_decode_overlaid<T>(
    handle: ReadHandle<'_>,
    overlay: &moraine_wal::Overlay,
    prefix: Vec<u8>,
    mut extract: impl FnMut(Key, &[u8]) -> Result<T>,
) -> Result<Vec<T>> {
    let mut merged: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
        std::collections::BTreeMap::new();
    let mut iter = handle.scan_prefix(prefix.clone(), ..).await?;
    while let Some(entry) = iter.next().await? {
        merged.insert(entry.key.to_vec(), entry.value.to_vec());
    }
    for (key, value) in overlay.prefixed(&prefix) {
        match value {
            Some(bytes) => {
                merged.insert(key.to_vec(), bytes.to_vec());
            }
            None => {
                merged.remove(key);
            }
        }
    }

    merged
        .iter()
        .map(|(key, value)| extract(Key::decode(key)?, value))
        .collect()
}

/// [`scan_decode`], overlaying the unfolded tail when one is given: `Some`
/// merges the tail (three-state, last-writer-wins), `None` scans the store
/// alone. The one dispatch point for scans that must reflect a slot-backed
/// attach's tail on a `Slots` backing and behave unchanged everywhere else.
pub(crate) async fn scan_decode_maybe<T>(
    handle: ReadHandle<'_>,
    overlay: Option<&moraine_wal::Overlay>,
    prefix: Vec<u8>,
    extract: impl FnMut(Key, &[u8]) -> Result<T>,
) -> Result<Vec<T>> {
    match overlay {
        Some(overlay) => scan_decode_overlaid(handle, overlay, prefix, extract).await,
        None => scan_decode(handle, prefix, extract).await,
    }
}

/// The layout-format stamp, if the store has been initialized.
pub(crate) async fn read_format(handle: ReadHandle<'_>) -> Result<Option<FormatValue>> {
    read_singleton(handle, Key::Sys(SysKey::Format)).await
}

/// The structural-migration marker, present only mid-migration.
pub(crate) async fn read_migration(handle: ReadHandle<'_>) -> Result<Option<MigrationValue>> {
    read_singleton(handle, Key::Sys(SysKey::Migration)).await
}

/// The head pointer: the latest committed snapshot id and batch count.
pub(crate) async fn read_head(handle: ReadHandle<'_>) -> Result<Option<HeadValue>> {
    read_singleton(handle, Key::Sys(SysKey::Head)).await
}

/// Reads the forwarding token; `None` on a store predating it (a leader mints
/// it lazily on first bind).
// Read by the leader role and its bootstrap-mint test; the leaderless build
// still writes it, but reads it back only in the test.
#[cfg_attr(not(feature = "leader"), allow(dead_code))]
pub(crate) async fn read_secret(handle: ReadHandle<'_>) -> Result<Option<SecretValue>> {
    read_singleton(handle, Key::Sys(SysKey::Secret)).await
}

/// One snapshot record.
pub(crate) async fn read_snapshot(
    handle: ReadHandle<'_>,
    snapshot_id: u64,
) -> Result<Option<SnapshotValue>> {
    read_singleton(handle, Key::Snapshot { snapshot_id }).await
}

/// [`read_snapshot`] with the unfolded tail overlaid: a tail write shadows the
/// stored record. A tail *delete*, though, is an expiry the folder has not
/// applied — the store still holds the record — so time travel resolves it from
/// there until a fold prunes it, the one place the unfolded view outlives the
/// folded one rather than matching it.
pub(crate) async fn read_snapshot_overlaid(
    handle: ReadHandle<'_>,
    overlay: &moraine_wal::Overlay,
    snapshot_id: u64,
) -> Result<Option<SnapshotValue>> {
    match overlay.get(&Key::Snapshot { snapshot_id }.encode()) {
        Some(Some(bytes)) => Ok(Some(value::decode_value(bytes)?)),
        Some(None) | None => read_snapshot(handle, snapshot_id).await,
    }
}

/// The snapshot id a transaction committed at, scanning the snapshot subspace
/// ascending from `floor + 1` for the record carrying `transaction_id`; `None`
/// when no record above `floor` carries it. Forward-only: transaction ids are
/// unique, so the first match settles it.
pub(crate) async fn snapshot_of_transaction(
    handle: ReadHandle<'_>,
    floor: u64,
    transaction_id: &[u8],
) -> Result<Option<u64>> {
    let prefix = subspace_prefix(Subspace::Snapshot);
    let start = Key::Snapshot {
        snapshot_id: floor.saturating_add(1),
    }
    .encode();
    let suffix = start[prefix.len()..].to_vec();

    let mut iter = handle.scan_prefix(prefix, suffix..).await?;
    while let Some(entry) = iter.next().await? {
        let snapshot: SnapshotValue = value::decode_value(&entry.value)?;
        if snapshot.transaction_id.as_deref() == Some(transaction_id) {
            return Ok(Some(snapshot.snapshot_id));
        }
    }

    Ok(None)
}

/// Decodes one entry of a `ducklake_snapshot` scan.
fn extract_snapshot(key: Key, bytes: &[u8]) -> Result<SnapshotValue> {
    match key {
        Key::Snapshot { .. } => value::decode_value(bytes),
        other => Err(Error::Corruption(format!(
            "non-snapshot key in snapshot scan: {other:?}"
        ))),
    }
}

/// Every committed snapshot record (`ducklake_snapshot` +
/// `ducklake_snapshot_changes`, merged), in key order.
pub(crate) async fn scan_snapshots(handle: ReadHandle<'_>) -> Result<Vec<SnapshotValue>> {
    scan_decode(
        handle,
        subspace_prefix(Subspace::Snapshot),
        extract_snapshot,
    )
    .await
}

/// [`scan_snapshots`] with the unfolded tail overlaid, for a slot-backed
/// attach whose folder has not applied every committed snapshot yet.
pub(crate) async fn scan_snapshots_overlaid(
    handle: ReadHandle<'_>,
    overlay: &moraine_wal::Overlay,
) -> Result<Vec<SnapshotValue>> {
    scan_decode_overlaid(
        handle,
        overlay,
        subspace_prefix(Subspace::Snapshot),
        extract_snapshot,
    )
    .await
}

pub(crate) fn decode_entity(entity: EntityKey, bytes: &[u8]) -> Result<EntityRecord> {
    match entity {
        EntityKey::Schema { .. } => Ok(EntityRecord::Schema(value::decode_value(bytes)?)),
        EntityKey::Table { .. } => Ok(EntityRecord::Table(value::decode_value(bytes)?)),
        EntityKey::View { .. } => Ok(EntityRecord::View(value::decode_value(bytes)?)),
        EntityKey::Column { .. } => Ok(EntityRecord::Column(value::decode_value(bytes)?)),
        EntityKey::File { .. } => Ok(EntityRecord::File(value::decode_value(bytes)?)),
        EntityKey::DeleteFile { .. } => Ok(EntityRecord::DeleteFile(value::decode_value(bytes)?)),
        EntityKey::Partition { .. } => Ok(EntityRecord::Partition(value::decode_value(bytes)?)),
        EntityKey::Sort { .. } => Ok(EntityRecord::Sort(value::decode_value(bytes)?)),
        EntityKey::Macro { .. } => Ok(EntityRecord::Macro(value::decode_value(bytes)?)),
        EntityKey::Mapping { .. } => Ok(EntityRecord::Mapping(value::decode_value(bytes)?)),
        EntityKey::Index { .. } => Ok(EntityRecord::Index(value::decode_value(bytes)?)),
        EntityKey::FileColumnStats { .. } => {
            Ok(EntityRecord::FileColumnStats(value::decode_value(bytes)?))
        }
        EntityKey::TableStats { .. } => Ok(EntityRecord::TableStats(value::decode_value(bytes)?)),
        EntityKey::TableColumnStats { .. } => {
            Ok(EntityRecord::TableColumnStats(value::decode_value(bytes)?))
        }
        EntityKey::Option {
            scope_kind,
            scope_id,
        } => Ok(EntityRecord::Option {
            scope_kind,
            scope_id,
            value: value::decode_value(bytes)?,
        }),
        EntityKey::Tag { .. } => Ok(EntityRecord::Tag(value::decode_value(bytes)?)),
    }
}

/// Decodes one entry of a `current`-subspace scan.
fn extract_current(key: Key, bytes: &[u8]) -> Result<EntityRecord> {
    match key {
        Key::Current(CurrentKey::Entity(entity)) => decode_entity(entity, bytes),
        Key::Current(CurrentKey::GcFile { .. }) => {
            Ok(EntityRecord::GcFile(value::decode_value(bytes)?))
        }
        other => Err(Error::Corruption(format!(
            "non-current key in current scan: {other:?}"
        ))),
    }
}

/// Every `ducklake_schema_versions` record as `(table_id,
/// begin_snapshot, schema_version)`, in key order. Retained across
/// snapshot expiry, so this is the projection's durable source — the
/// snapshot records only carry the same rows for as long as they live.
pub(crate) async fn scan_schema_versions(handle: ReadHandle<'_>) -> Result<Vec<(u64, u64, u64)>> {
    scan_decode(
        handle,
        subspace_prefix(Subspace::SchemaVersion),
        extract_schema_version,
    )
    .await
}

/// [`scan_schema_versions`] with the unfolded tail overlaid, for a slot-backed
/// attach whose folder has not applied every committed schema-version row yet.
pub(crate) async fn scan_schema_versions_overlaid(
    handle: ReadHandle<'_>,
    overlay: &moraine_wal::Overlay,
) -> Result<Vec<(u64, u64, u64)>> {
    scan_decode_overlaid(
        handle,
        overlay,
        subspace_prefix(Subspace::SchemaVersion),
        extract_schema_version,
    )
    .await
}

fn extract_schema_version(key: Key, bytes: &[u8]) -> Result<(u64, u64, u64)> {
    match key {
        Key::SchemaVersion {
            table_id,
            begin_snapshot,
        } => {
            let value: SchemaVersionValue = value::decode_value(bytes)?;
            Ok((table_id, begin_snapshot, value.schema_version))
        }
        other => Err(Error::Corruption(format!(
            "non-schema-version key in schema-version scan: {other:?}"
        ))),
    }
}

/// Every live entity record.
pub(crate) async fn scan_current_entities(handle: ReadHandle<'_>) -> Result<Vec<EntityRecord>> {
    scan_decode(handle, subspace_prefix(Subspace::Current), extract_current).await
}

/// [`scan_current_entities`] with the unfolded tail overlaid.
pub(crate) async fn scan_current_entities_overlaid(
    handle: ReadHandle<'_>,
    overlay: &moraine_wal::Overlay,
) -> Result<Vec<EntityRecord>> {
    scan_decode_overlaid(
        handle,
        overlay,
        subspace_prefix(Subspace::Current),
        extract_current,
    )
    .await
}

/// Every ended entity-version record. Unversioned kinds
/// ([`EntityKey::is_versioned`]) are overwritten in place and never
/// mirrored to history; finding one there is store damage, refused here —
/// before any consumer, snapshot build or raw dump, could replay it over
/// the live record.
pub(crate) async fn scan_history_entities(handle: ReadHandle<'_>) -> Result<Vec<EntityRecord>> {
    scan_decode(handle, subspace_prefix(Subspace::History), extract_history).await
}

/// [`scan_history_entities`] with the unfolded tail overlaid.
pub(crate) async fn scan_history_entities_overlaid(
    handle: ReadHandle<'_>,
    overlay: &moraine_wal::Overlay,
) -> Result<Vec<EntityRecord>> {
    scan_decode_overlaid(
        handle,
        overlay,
        subspace_prefix(Subspace::History),
        extract_history,
    )
    .await
}

/// Decodes one entry of a `history`-subspace scan, refusing an unversioned
/// kind mirrored there.
fn extract_history(key: Key, bytes: &[u8]) -> Result<EntityRecord> {
    match key {
        Key::History(history) if !history.entity.is_versioned() => Err(Error::Corruption(format!(
            "unversioned key in history scan: {:?}",
            history.entity
        ))),
        Key::History(history) => decode_entity(history.entity, bytes),
        other => Err(Error::Corruption(format!(
            "non-history key in history scan: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::store::open::StoreBuilder;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn reads_decode_what_was_written() {
        let db = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();

        let head = HeadValue {
            snapshot_id: 3,
            batch_seq: 1,
        };
        let schema = SchemaValue {
            schema_id: 1,
            schema_uuid: "u".into(),
            begin_snapshot: 1,
            end_snapshot: None,
            schema_name: "main".into(),
            path: "main/".into(),
            path_is_relative: true,
        };
        let ended = SchemaValue {
            schema_id: 0,
            end_snapshot: Some(2),
            ..schema.clone()
        };

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(Key::Sys(SysKey::Head).encode(), value::encode_value(&head))
            .unwrap();
        tx.put(
            Key::current(EntityKey::Schema { schema_id: 1 }).encode(),
            value::encode_value(&schema),
        )
        .unwrap();
        tx.put(
            Key::history(EntityKey::Schema { schema_id: 0 }, 2).encode(),
            value::encode_value(&ended),
        )
        .unwrap();
        let tstat = TableStatsValue {
            table_id: 7,
            record_count: 10,
            next_row_id: 10,
            file_size_bytes: 1024,
        };
        tx.put(
            Key::current(EntityKey::TableStats { table_id: 7 }).encode(),
            value::encode_value(&tstat),
        )
        .unwrap();
        let file = DataFileValue {
            data_file_id: 3,
            table_id: 7,
            begin_snapshot: 1,
            end_snapshot: None,
            file_order: None,
            path: "f.parquet".into(),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: 10,
            file_size_bytes: 1024,
            footer_size: 64,
            row_id_start: Some(0),
            partition_id: None,
            encryption_key: None,
            mapping_id: None,
            partial_max: None,
            partition_values: vec![],
        };
        tx.put(
            Key::current(EntityKey::File {
                table_id: 7,
                data_file_id: 3,
            })
            .encode(),
            value::encode_value(&file),
        )
        .unwrap();

        let view = ViewValue {
            view_id: 4,
            view_uuid: "uv".into(),
            begin_snapshot: 1,
            end_snapshot: None,
            schema_id: 1,
            view_name: "v".into(),
            dialect: "duckdb".into(),
            sql: "SELECT 1".into(),
            column_aliases: None,
        };
        tx.put(
            Key::current(EntityKey::View { view_id: 4 }).encode(),
            value::encode_value(&view),
        )
        .unwrap();

        let mut options = std::collections::HashMap::new();
        options.insert("key1".into(), "value1".into());
        let option = OptionScopeValue { options };
        tx.put(
            Key::current(EntityKey::Option {
                scope_kind: 0,
                scope_id: 0,
            })
            .encode(),
            value::encode_value(&option),
        )
        .unwrap();

        crate::transaction::commit::commit_durably(&db, tx)
            .await
            .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert_eq!(read_head(ReadHandle::Tx(&tx)).await.unwrap(), Some(head));
        assert_eq!(read_format(ReadHandle::Tx(&tx)).await.unwrap(), None);
        assert_eq!(read_migration(ReadHandle::Tx(&tx)).await.unwrap(), None);
        assert_eq!(read_snapshot(ReadHandle::Tx(&tx), 0).await.unwrap(), None);

        let current = scan_current_entities(ReadHandle::Tx(&tx)).await.unwrap();
        assert_eq!(current.len(), 5);
        assert!(current.contains(&EntityRecord::Schema(schema)));
        assert!(current.contains(&EntityRecord::File(file)));
        assert!(current.contains(&EntityRecord::TableStats(tstat)));
        assert!(current.contains(&EntityRecord::View(view)));
        assert!(current.contains(&EntityRecord::Option {
            scope_kind: 0,
            scope_id: 0,
            value: option,
        }));
        let history = scan_history_entities(ReadHandle::Tx(&tx)).await.unwrap();
        assert_eq!(history, vec![EntityRecord::Schema(ended)]);
        tx.rollback();
        db.close().await.unwrap();
    }

    /// Unversioned kinds are never written to history; one found there is
    /// store damage. Refusing it keeps the later current-then-history
    /// replay from silently overwriting the live record.
    #[tokio::test]
    async fn unversioned_kind_in_history_is_refused() {
        let db = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();

        let stats = TableStatsValue {
            table_id: 7,
            record_count: 10,
            next_row_id: 10,
            file_size_bytes: 1024,
        };
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            Key::history(EntityKey::TableStats { table_id: 7 }, 2).encode(),
            value::encode_value(&stats),
        )
        .unwrap();
        crate::transaction::commit::commit_durably(&db, tx)
            .await
            .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let err = scan_history_entities(ReadHandle::Tx(&tx))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        tx.rollback();
        db.close().await.unwrap();
    }

    /// Mappings are write-once and never mirrored to history; one found
    /// there is refused like every other unversioned kind.
    #[tokio::test]
    async fn mapping_in_history_is_refused() {
        let db = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();

        let mapping = MappingValue {
            mapping_id: 21,
            table_id: 4,
            map_type: "map_by_name".into(),
            name_mappings: vec![],
        };
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            Key::history(
                EntityKey::Mapping {
                    table_id: 4,
                    mapping_id: 21,
                },
                2,
            )
            .encode(),
            value::encode_value(&mapping),
        )
        .unwrap();
        crate::transaction::commit::commit_durably(&db, tx)
            .await
            .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let err = scan_history_entities(ReadHandle::Tx(&tx))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        tx.rollback();
        db.close().await.unwrap();
    }
}
