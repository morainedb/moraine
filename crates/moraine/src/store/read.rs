//! Typed reads over an open transaction: decode keys and values into the
//! wire types. No interpretation — the domain layer owns meaning.

use std::time::Instant;

use bytes::Bytes;
use prost::Message as _;
use tracing::debug;

use crate::{
    error::{Error, Result},
    store::{
        handle::{ReadHandle, ScanShape},
        key::{
            CurrentKey, EntityKey, EntityKind, Key, SPLIT_SCAN_KINDS, Subspace, SysKey,
            current_entity_kind_prefix, current_gc_file_prefix, history_entity_kind_prefix,
            subspace_prefix,
        },
        proto::{
            ChangelogValue, ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue,
            FormatValue, GcFileValue, HeadValue, IndexValue, MacroValue, MaintenanceStatusValue,
            MappingValue, MigrationValue, OptionScopeValue, PartitionValue, SchemaValue,
            SchemaVersionValue, SnapshotValue, SortValue, TableColumnStatsValue, TableStatsValue,
            TableValue, TagValue, ViewValue,
        },
        value,
    },
    telemetry::milliseconds,
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
    /// A cheap estimate of the record's footprint: its encoded length plus
    /// the enum's own size. Understates the decoded form.
    pub(crate) fn estimated_bytes(&self) -> u64 {
        let encoded = match self {
            Self::Schema(value) => value.encoded_len(),
            Self::Table(value) => value.encoded_len(),
            Self::View(value) => value.encoded_len(),
            Self::Column(value) => value.encoded_len(),
            Self::File(value) => value.encoded_len(),
            Self::DeleteFile(value) => value.encoded_len(),
            Self::Partition(value) => value.encoded_len(),
            Self::Sort(value) => value.encoded_len(),
            Self::Macro(value) => value.encoded_len(),
            Self::Mapping(value) => value.encoded_len(),
            Self::Index(value) => value.encoded_len(),
            Self::FileColumnStats(value) => value.encoded_len(),
            Self::TableStats(value) => value.encoded_len(),
            Self::TableColumnStats(value) => value.encoded_len(),
            Self::Option { value, .. } => value.encoded_len(),
            Self::Tag(value) => value.encoded_len(),
            Self::GcFile(value) => value.encoded_len(),
        };
        u64::try_from(encoded).unwrap_or(u64::MAX)
            + u64::try_from(std::mem::size_of::<Self>()).unwrap_or(0)
    }
}

/// One decoded entity-record kind, used to read only the catalog table a
/// transactional metadata scan requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum EntityRecordKind {
    Schema,
    Table,
    View,
    Column,
    File,
    DeleteFile,
    Partition,
    Sort,
    Macro,
    Mapping,
    FileColumnStats,
    TableStats,
    TableColumnStats,
    Option,
    Tag,
    GcFile,
}

impl EntityRecordKind {
    fn entity_kind(self) -> Option<EntityKind> {
        match self {
            Self::Schema => Some(EntityKind::Schema),
            Self::Table => Some(EntityKind::Table),
            Self::View => Some(EntityKind::View),
            Self::Column => Some(EntityKind::Column),
            Self::File => Some(EntityKind::File),
            Self::DeleteFile => Some(EntityKind::DeleteFile),
            Self::Partition => Some(EntityKind::Partition),
            Self::Sort => Some(EntityKind::Sort),
            Self::Macro => Some(EntityKind::Macro),
            Self::Mapping => Some(EntityKind::Mapping),
            Self::FileColumnStats => Some(EntityKind::FileColumnStats),
            Self::TableStats => Some(EntityKind::TableStats),
            Self::TableColumnStats => Some(EntityKind::TableColumnStats),
            Self::Option => Some(EntityKind::Option),
            Self::Tag => Some(EntityKind::Tag),
            Self::GcFile => None,
        }
    }
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
        .map(value::decode_owned)
        .transpose()
}

/// Scans every key under `prefix`, moving each store value into `extract`.
/// Extractors may retain shared fields from the store allocation or borrow
/// the value for an ordinary decode.
pub(crate) async fn scan_decode<T>(
    handle: ReadHandle<'_>,
    prefix: Vec<u8>,
    shape: ScanShape,
    mut extract: impl FnMut(Key, Bytes) -> Result<T>,
) -> Result<Vec<T>> {
    let mut iter = handle.scan_prefix(prefix, .., shape).await?;
    let mut records = Vec::new();
    while let Some(entry) = iter.next().await? {
        records.push(extract(Key::decode(&entry.key)?, entry.value)?);
    }

    Ok(records)
}

/// As [`scan_decode`], splitting each of `splits` (the data-scaled kinds'
/// prefixes under `prefix`) into concurrent sub-ranges once it outgrows one
/// read-ahead; the records still come back in key order.
async fn scan_decode_split<T>(
    handle: ReadHandle<'_>,
    prefix: Vec<u8>,
    splits: &[Vec<u8>],
    mut extract: impl FnMut(Key, Bytes) -> Result<T>,
) -> Result<Vec<T>> {
    handle
        .scan_prefix_split(&prefix, splits, ScanShape::Bulk)
        .await?
        .into_iter()
        .map(|entry| extract(Key::decode(&entry.key)?, entry.value))
        .collect()
}

/// As [`scan_decode`] over a whole subspace, splitting the data-scaled
/// kinds (`kind_prefix` names each one's prefix).
async fn scan_decode_subspace<T>(
    handle: ReadHandle<'_>,
    subspace: Subspace,
    kind_prefix: fn(EntityKind) -> Vec<u8>,
    extract: impl FnMut(Key, Bytes) -> Result<T>,
) -> Result<Vec<T>> {
    let splits = SPLIT_SCAN_KINDS.map(kind_prefix);

    scan_decode_split(handle, subspace_prefix(subspace), &splits, extract).await
}

/// As [`scan_decode`] over one kind's prefix, split when the kind scales
/// with the data.
async fn scan_decode_kind<T>(
    handle: ReadHandle<'_>,
    kind: EntityKind,
    prefix: Vec<u8>,
    extract: impl FnMut(Key, Bytes) -> Result<T>,
) -> Result<Vec<T>> {
    if !SPLIT_SCAN_KINDS.contains(&kind) {
        return scan_decode(handle, prefix, ScanShape::Bulk, extract).await;
    }
    let splits = [prefix.clone()];

    scan_decode_split(handle, prefix, &splits, extract).await
}

/// The layout-format stamp, if the store has been initialized.
pub(crate) async fn read_format(handle: ReadHandle<'_>) -> Result<Option<FormatValue>> {
    read_singleton(handle, Key::Sys(SysKey::Format)).await
}

/// The structural-migration marker, present only mid-migration.
pub(crate) async fn read_migration(handle: ReadHandle<'_>) -> Result<Option<MigrationValue>> {
    read_singleton(handle, Key::Sys(SysKey::Migration)).await
}

/// The bounded durable history of completed maintenance passes.
pub(crate) async fn read_maintenance_status(
    handle: ReadHandle<'_>,
) -> Result<Option<MaintenanceStatusValue>> {
    read_singleton(handle, Key::Sys(SysKey::MaintenanceStatus)).await
}

/// The head pointer: the latest committed snapshot id and batch count.
pub(crate) async fn read_head(handle: ReadHandle<'_>) -> Result<Option<HeadValue>> {
    read_singleton(handle, Key::Sys(SysKey::Head)).await
}

/// How many times a read-only pass is re-run before its instability is
/// reported.
const STABLE_READ_ATTEMPTS: usize = 8;

/// Runs `read` so that everything it observes belongs to one store state.
/// A transaction handle runs it once; a manifest-following reader compares
/// the head record before and after the pass (every batch moves it) and
/// re-runs a pass that straddled a commit.
///
/// Retrying only helps while a pass fits inside the interval between
/// commits: a pass that outlasts it straddles every attempt, and no
/// budget saves it. Shortening the pass is the lever, so each straddle is
/// logged with what it cost and the exhausted error reports the same.
///
/// # Errors
///
/// Returns [`Error::RetryBudgetExhausted`] if the store moved under every
/// attempt, or whatever `read` itself returns.
pub(crate) async fn consistent<T, F, Fut>(handle: ReadHandle<'_>, read: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    if handle.is_isolated() {
        return read().await;
    }

    let started = Instant::now();
    for attempt in 1..=STABLE_READ_ATTEMPTS {
        let pass_started = Instant::now();
        let before = read_head(handle).await?;
        let value = read().await?;
        if read_head(handle).await? == before {
            return Ok(value);
        }
        debug!(
            attempt,
            attempts = STABLE_READ_ATTEMPTS,
            pass_ms = milliseconds(pass_started.elapsed()),
            "a read-only pass straddled a commit; re-reading"
        );
    }

    Err(Error::RetryBudgetExhausted(format!(
        "a read-only pass could not observe a single store state in \
         {STABLE_READ_ATTEMPTS} attempts over {}ms; the catalog is committing \
         faster than this pass reads, which a larger budget cannot fix",
        milliseconds(started.elapsed())
    )))
}

/// One commit's changelog, absent once it falls out of the retained
/// window (or if the commit recorded none).
pub(crate) async fn read_changelog(
    handle: ReadHandle<'_>,
    snapshot_id: u64,
) -> Result<Option<ChangelogValue>> {
    read_singleton(handle, Key::Changelog { snapshot_id }).await
}

/// One snapshot record.
pub(crate) async fn read_snapshot(
    handle: ReadHandle<'_>,
    snapshot_id: u64,
) -> Result<Option<SnapshotValue>> {
    read_singleton(handle, Key::Snapshot { snapshot_id }).await
}

/// Every committed snapshot record (`ducklake_snapshot` +
/// `ducklake_snapshot_changes`, merged), in key order.
pub(crate) async fn scan_snapshots(handle: ReadHandle<'_>) -> Result<Vec<SnapshotValue>> {
    scan_decode(
        handle,
        subspace_prefix(Subspace::Snapshot),
        ScanShape::Bulk,
        |key, bytes| match key {
            Key::Snapshot { .. } => value::decode_value(&bytes),
            other => Err(Error::Corruption(format!(
                "non-snapshot key in snapshot scan: {other:?}"
            ))),
        },
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

/// Every `ducklake_schema_versions` record as `(table_id,
/// begin_snapshot, schema_version)`, in key order. Retained across
/// snapshot expiry.
pub(crate) async fn scan_schema_versions(handle: ReadHandle<'_>) -> Result<Vec<(u64, u64, u64)>> {
    scan_decode(
        handle,
        subspace_prefix(Subspace::SchemaVersion),
        ScanShape::Bulk,
        |key, bytes| match key {
            Key::SchemaVersion {
                table_id,
                begin_snapshot,
            } => {
                let value: SchemaVersionValue = value::decode_value(&bytes)?;
                Ok((table_id, begin_snapshot, value.schema_version))
            }
            other => Err(Error::Corruption(format!(
                "non-schema-version key in schema-version scan: {other:?}"
            ))),
        },
    )
    .await
}

/// Every live entity record.
pub(crate) async fn scan_current_entities(handle: ReadHandle<'_>) -> Result<Vec<EntityRecord>> {
    scan_decode_subspace(
        handle,
        Subspace::Current,
        current_entity_kind_prefix,
        |key, bytes| match key {
            Key::Current(CurrentKey::Entity(entity)) => decode_entity(entity, &bytes),
            Key::Current(CurrentKey::GcFile { .. }) => {
                Ok(EntityRecord::GcFile(value::decode_value(&bytes)?))
            }
            other => Err(Error::Corruption(format!(
                "non-current key in current scan: {other:?}"
            ))),
        },
    )
    .await
}

/// Every ended entity-version record. Unversioned kinds
/// ([`EntityKey::is_versioned`]) are never mirrored to history; finding one
/// there is store damage and is refused.
pub(crate) async fn scan_history_entities(handle: ReadHandle<'_>) -> Result<Vec<EntityRecord>> {
    scan_decode_subspace(
        handle,
        Subspace::History,
        history_entity_kind_prefix,
        |key, bytes| match key {
            Key::History(history) if !history.entity.is_versioned() => Err(Error::Corruption(
                format!("unversioned key in history scan: {:?}", history.entity),
            )),
            Key::History(history) => decode_entity(history.entity, &bytes),
            other => Err(Error::Corruption(format!(
                "non-history key in history scan: {other:?}"
            ))),
        },
    )
    .await
}

/// Every current and ended record of one catalog kind. Unversioned kinds
/// read only `current`; scheduled-file records use their sibling key kind
/// there rather than an [`EntityKey`].
/// Which versions of a kind a scan reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Versions {
    /// Both subspaces: every version the store still holds.
    LiveAndEnded,
    /// `current` only, for a reader that has been shown no version in
    /// `history` can match it. Unversioned kinds read this way regardless.
    Live,
}

/// Whether a reader filtering to the versions live at `filter_snapshot`
/// can be served from `current` alone.
///
/// A record reaches `history` by being ended, stamped with the snapshot
/// that ended it, which is never past the head. A reader that keeps a row
/// only while `filter_snapshot < end_snapshot` therefore matches nothing
/// there once `filter_snapshot` has reached the head — and every history
/// record carries an `end_snapshot`, so none of them is kept by the
/// `IS NULL` arm either. Time travel reads behind the head and keeps the
/// full scan.
pub(crate) fn versions_for(filter_snapshot: Option<u64>, head: Option<u64>) -> Versions {
    match (filter_snapshot, head) {
        (Some(filter_snapshot), Some(head)) if filter_snapshot >= head => Versions::Live,
        _ => Versions::LiveAndEnded,
    }
}

pub(crate) async fn scan_entity_kind(
    handle: ReadHandle<'_>,
    kind: EntityRecordKind,
    versions: Versions,
) -> Result<Vec<EntityRecord>> {
    let Some(entity_kind) = kind.entity_kind() else {
        return scan_decode(
            handle,
            current_gc_file_prefix(),
            ScanShape::Bulk,
            |key, bytes| match key {
                Key::Current(CurrentKey::GcFile { .. }) => {
                    Ok(EntityRecord::GcFile(value::decode_value(&bytes)?))
                }
                other => Err(Error::Corruption(format!(
                    "non-gc-file key in scheduled-file scan: {other:?}"
                ))),
            },
        )
        .await;
    };

    let current = scan_decode_kind(
        handle,
        entity_kind,
        current_entity_kind_prefix(entity_kind),
        |key, bytes| match key {
            Key::Current(CurrentKey::Entity(entity)) => decode_entity(entity, &bytes),
            other => Err(Error::Corruption(format!(
                "non-entity key in {kind:?} current scan: {other:?}"
            ))),
        },
    );
    if !entity_kind.is_versioned() || versions == Versions::Live {
        return current.await;
    }

    let history = scan_decode_kind(
        handle,
        entity_kind,
        history_entity_kind_prefix(entity_kind),
        |key, bytes| match key {
            Key::History(history) => decode_entity(history.entity, &bytes),
            other => Err(Error::Corruption(format!(
                "non-history key in {kind:?} history scan: {other:?}"
            ))),
        },
    );
    let (mut records, history) = futures::try_join!(current, history)?;
    records.extend(history);

    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::{IsolationLevel, config::WriteOptions};

    use super::*;
    use crate::store::open::StoreBuilder;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn reads_decode_what_was_written() {
        let (db, _) = StoreBuilder::new("t", Arc::new(InMemory::new()))
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

        tx.commit_with_options(&WriteOptions {
            await_durable: true,
            ..Default::default()
        })
        .await
        .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert_eq!(read_head(ReadHandle::Tx(&tx)).await.unwrap(), Some(head));
        assert_eq!(read_format(ReadHandle::Tx(&tx)).await.unwrap(), None);
        assert_eq!(read_migration(ReadHandle::Tx(&tx)).await.unwrap(), None);
        assert_eq!(read_snapshot(ReadHandle::Tx(&tx), 0).await.unwrap(), None);

        let current = scan_current_entities(ReadHandle::Tx(&tx)).await.unwrap();
        assert_eq!(current.len(), 5);
        assert!(current.contains(&EntityRecord::Schema(schema.clone())));
        assert!(current.contains(&EntityRecord::File(file.clone())));
        assert!(current.contains(&EntityRecord::TableStats(tstat)));
        assert!(current.contains(&EntityRecord::View(view)));
        assert!(current.contains(&EntityRecord::Option {
            scope_kind: 0,
            scope_id: 0,
            value: option,
        }));
        let history = scan_history_entities(ReadHandle::Tx(&tx)).await.unwrap();
        assert_eq!(history, vec![EntityRecord::Schema(ended.clone())]);

        assert_eq!(
            scan_entity_kind(
                ReadHandle::Tx(&tx),
                EntityRecordKind::Schema,
                Versions::LiveAndEnded
            )
            .await
            .unwrap(),
            vec![EntityRecord::Schema(schema), EntityRecord::Schema(ended),]
        );
        assert_eq!(
            scan_entity_kind(
                ReadHandle::Tx(&tx),
                EntityRecordKind::File,
                Versions::LiveAndEnded
            )
            .await
            .unwrap(),
            vec![EntityRecord::File(file)]
        );
        assert_eq!(
            scan_entity_kind(
                ReadHandle::Tx(&tx),
                EntityRecordKind::TableStats,
                Versions::LiveAndEnded
            )
            .await
            .unwrap(),
            vec![EntityRecord::TableStats(tstat)]
        );
        tx.rollback();
        db.close().await.unwrap();
    }

    /// A split scan returns exactly what one iterator over the same prefix
    /// returns, in the same order — records of every kind, in and out of
    /// the split kinds, across the sub-range cuts.
    #[tokio::test]
    async fn a_split_scan_equals_a_single_scan() {
        let (db, _) = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for table_id in [3_u64, 7, 9] {
            tx.put(
                Key::current(EntityKey::Table { table_id }).encode(),
                value::encode_value(&TableValue {
                    table_id,
                    ..TableValue::default()
                }),
            )
            .unwrap();
            for data_file_id in 0..(table_id * 40) {
                let file = DataFileValue {
                    data_file_id,
                    table_id,
                    ..DataFileValue::default()
                };
                tx.put(
                    Key::current(EntityKey::File {
                        table_id,
                        data_file_id,
                    })
                    .encode(),
                    value::encode_value(&file),
                )
                .unwrap();
                tx.put(
                    Key::history(
                        EntityKey::File {
                            table_id,
                            data_file_id,
                        },
                        2,
                    )
                    .encode(),
                    value::encode_value(&file),
                )
                .unwrap();
            }
        }
        tx.commit_with_options(&WriteOptions {
            await_durable: true,
            ..Default::default()
        })
        .await
        .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let handle = ReadHandle::Tx(&tx);
        let single = |subspace| {
            scan_decode(
                handle,
                subspace_prefix(subspace),
                ScanShape::Bulk,
                |key, bytes| match key {
                    Key::Current(CurrentKey::Entity(entity)) => decode_entity(entity, &bytes),
                    Key::History(history) => decode_entity(history.entity, &bytes),
                    other => Err(Error::Corruption(format!("{other:?}"))),
                },
            )
        };

        let current = scan_current_entities(handle).await.unwrap();
        assert_eq!(current.len(), 3 + 40 * (3 + 7 + 9));
        assert_eq!(current, single(Subspace::Current).await.unwrap());
        assert_eq!(
            scan_history_entities(handle).await.unwrap(),
            single(Subspace::History).await.unwrap()
        );

        let files = scan_entity_kind(handle, EntityRecordKind::File, Versions::LiveAndEnded)
            .await
            .unwrap();
        assert_eq!(files.len(), 2 * 40 * (3 + 7 + 9));
        let expected: Vec<_> = current
            .iter()
            .chain(&scan_history_entities(handle).await.unwrap())
            .filter(|record| matches!(record, EntityRecord::File(_)))
            .cloned()
            .collect();
        assert_eq!(files, expected);
        tx.rollback();
        db.close().await.unwrap();
    }

    /// The bound decides the halves: at or past the head nothing in
    /// history can match, behind it the ended versions are still needed,
    /// and an absent bound or head keeps the full scan.
    #[test]
    fn versions_narrow_only_at_or_past_the_head() {
        assert_eq!(versions_for(Some(7), Some(7)), Versions::Live);
        assert_eq!(versions_for(Some(8), Some(7)), Versions::Live);
        assert_eq!(versions_for(Some(6), Some(7)), Versions::LiveAndEnded);
        assert_eq!(versions_for(None, Some(7)), Versions::LiveAndEnded);
        assert_eq!(versions_for(Some(7), None), Versions::LiveAndEnded);
    }

    /// A live-only scan returns exactly the full scan's rows that a reader
    /// bounded at the head would have kept — the ended ones it drops are
    /// the ones that reader discards anyway.
    #[tokio::test]
    async fn a_live_scan_drops_only_what_a_head_bounded_reader_discards() {
        let (db, _) = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();

        // One live file and one ended in snapshot 4.
        let live = DataFileValue {
            table_id: 1,
            data_file_id: 1,
            begin_snapshot: 1,
            ..DataFileValue::default()
        };
        let ended = DataFileValue {
            table_id: 1,
            data_file_id: 2,
            begin_snapshot: 1,
            end_snapshot: Some(4),
            ..DataFileValue::default()
        };
        let entity = |data_file_id| EntityKey::File {
            table_id: 1,
            data_file_id,
        };
        tx.put(
            Key::current(entity(1)).encode(),
            crate::store::value::encode_value(&live),
        )
        .unwrap();
        tx.put(
            Key::history(entity(2), 4).encode(),
            crate::store::value::encode_value(&ended),
        )
        .unwrap();

        let handle = ReadHandle::Tx(&tx);
        let full = scan_entity_kind(handle, EntityRecordKind::File, Versions::LiveAndEnded)
            .await
            .unwrap();
        let live_only = scan_entity_kind(handle, EntityRecordKind::File, Versions::Live)
            .await
            .unwrap();
        assert_eq!(full.len(), 2);
        assert_eq!(live_only.len(), 1);

        // What the reader at the head keeps out of the full scan is
        // exactly what the live-only scan returned.
        let kept: Vec<_> = full
            .iter()
            .filter(|record| {
                record
                    .lifecycle()
                    .is_none_or(|(_, end)| end.is_none_or(|end| 4 < end))
            })
            .cloned()
            .collect();
        assert_eq!(kept, live_only);

        tx.rollback();
        db.close().await.unwrap();
    }

    /// An unversioned kind found in history is refused as store damage.
    #[tokio::test]
    async fn unversioned_kind_in_history_is_refused() {
        let (db, _) = StoreBuilder::new("t", Arc::new(InMemory::new()))
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
        tx.commit_with_options(&WriteOptions {
            await_durable: true,
            ..Default::default()
        })
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

    /// A mapping found in history is refused like every unversioned kind.
    #[tokio::test]
    async fn mapping_in_history_is_refused() {
        let (db, _) = StoreBuilder::new("t", Arc::new(InMemory::new()))
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
        tx.commit_with_options(&WriteOptions {
            await_durable: true,
            ..Default::default()
        })
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
