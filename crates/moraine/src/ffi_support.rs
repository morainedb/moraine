//! Internal, unstable seam for the `moraine-duckdb` ABI crate.
//!
//! `#[doc(hidden)]` despite the `pub` visibility: not part of the crate's
//! semver contract — shape and presence may change without notice.
//!
//! Each `dump_*` function returns every record of one kind, current and history
//! together, as the wire value type that kind encodes to
//! (`crate::store::proto`) — row-faithful and unfiltered; unversioned
//! kinds yield only their single current row since they are never mirrored
//! to history. DuckLake filters lifecycles in SQL over these rows.
//!
//! Every function opens one fresh read session, and most are served from a
//! maintained projection when its head matches rather than by rescanning.
//! Views spanning several `dump_*` calls are not snapshot-consistent: each
//! call reads at whatever the current head is when it runs. Opening that
//! session refuses a store undergoing a structural migration, so every
//! function here can return [`crate::Error::Migration`] however it would
//! otherwise have succeeded.
//!
//! All of them report **committed** state. A caller inside an open staged
//! transaction that must see its own uncommitted rows asks that transaction
//! instead — [`staged::StagedTransaction`]'s `visible_*` family, which
//! overlays the staged rows onto the same records these functions scan.

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    catalog::{ReadOnlyCatalog, projection, projection::ProjectionCache},
    error::{Error, Result},
    store::{
        proto::{
            ColumnTag, ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue,
            GcFileValue, HeadValue, MacroValue, MappingValue, OptionScopeValue, PartitionValue,
            SchemaValue, SnapshotValue, SortValue, TableColumnStatsValue, TableStatsValue,
            TableValue, TagValue, ViewValue,
        },
        read::{
            EntityRecord, read_head, scan_current_entities, scan_history_entities,
            scan_schema_versions, scan_snapshots,
        },
    },
};

/// The head snapshot id inside an open read session, or `None` on a
/// store that has no head yet (mid-bootstrap).
async fn session_head(
    catalog: &ReadOnlyCatalog,
    session: &crate::store::handle::ReadSession,
) -> Result<Option<HeadValue>> {
    catalog.note_head_read();
    read_head(session.handle()).await
}

/// The stamp of the view a read-write handle already holds, usable as a
/// projection lookup key without a head read. `None` on a read-only handle
/// or a writer whose view is not held. Only a lookup key: a miss falls
/// through to the session path.
fn writer_head(catalog: &ReadOnlyCatalog) -> Result<Option<HeadValue>> {
    let Some(view) = catalog.writer_head_view() else {
        return Ok(None);
    };
    catalog.refuse_if_closed()?;

    Ok(Some(crate::catalog::projection::view_head(&view)))
}

/// Locks the shared projection state for reading, recovering a poisoned
/// lock.
fn projections_read(catalog: &ReadOnlyCatalog) -> std::sync::RwLockReadGuard<'_, ProjectionCache> {
    catalog
        .projections()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// As [`projections_read`], for writing (installs).
fn projections_write(
    catalog: &ReadOnlyCatalog,
) -> std::sync::RwLockWriteGuard<'_, ProjectionCache> {
    catalog
        .projections()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[doc(hidden)]
pub mod index;
#[doc(hidden)]
pub mod inline;
#[doc(hidden)]
pub mod staged;

// Re-exported so the ABI crate can name the record types its dumps convert
// from.
#[doc(hidden)]
pub use crate::store::proto::{
    ColumnValue as ColumnRecord, DataFileValue as DataFileRecord,
    DeleteFileValue as DeleteFileRecord, FileColumnStatsValue as FileColumnStatsRecord,
    GcFileValue as GcFileRecord, MacroValue as MacroRecord, MappingValue as MappingRecord,
    PartitionValue as PartitionRecord, SchemaValue as SchemaRecord,
    SnapshotValue as SnapshotRecord, SortValue as SortRecord,
    TableColumnStatsValue as TableColumnStatsRecord, TableStatsValue as TableStatsRecord,
    TableValue as TableRecord, ViewValue as ViewRecord,
};

/// The store state every dump's rows stand at: the head snapshot id and
/// batch count, or `None` on a store that has no head yet. A caller holding
/// rows compares this against the stamp they were served at and re-dumps
/// only on a move; both halves matter, since a maintenance batch reuses the
/// snapshot id.
#[doc(hidden)]
pub async fn head_stamp(catalog: &ReadOnlyCatalog) -> Result<Option<HeadValue>> {
    if let Some(head) = writer_head(catalog)? {
        return Ok(Some(head));
    }

    let session = catalog.begin_read().await?;
    let head = session_head(catalog, &session).await;
    session.finish();
    head
}

/// Whether a caller needs the ended half, and if it depends on the head,
/// what decides it.
#[derive(Debug, Clone, Copy)]
enum HistoryNeed {
    /// The full set, whatever the head.
    Always,
    /// `current` alone — the unversioned kinds, which have no ended half.
    Never,
    /// The full set only while the head is past the reader's filter
    /// snapshot; see [`crate::store::read::versions_for`].
    UnlessLiveAt(u64),
}

impl HistoryNeed {
    /// Resolved against the head this read observed. An unknown head is a
    /// store with no commits, where neither half holds anything.
    fn wants_history(self, head: Option<&HeadValue>) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::UnlessLiveAt(filter_snapshot) => {
                crate::store::read::versions_for(
                    Some(filter_snapshot),
                    head.map(|head| head.snapshot_id),
                ) == crate::store::read::Versions::LiveAndEnded
            }
        }
    }
}

/// The shared record set's two halves at the session head, each served
/// from the projection cache when its stamp matches and scanned at most
/// once otherwise.
///
/// Whether the ended half is read is settled against the head this call
/// observes, never a head read earlier — a commit landing in between would
/// otherwise let a bound that was at the head fall behind one, and the
/// rows it ended would be dropped from a reader that still matches them.
async fn entity_halves(
    catalog: &ReadOnlyCatalog,
    history: HistoryNeed,
) -> Result<(
    Option<HeadValue>,
    Arc<Vec<EntityRecord>>,
    Arc<Vec<EntityRecord>>,
)> {
    // A read-write handle's held view is at head, so a read served wholly
    // from the cache needs neither a session nor a head read.
    if let Some(head) = writer_head(catalog)? {
        let want_history = history.wants_history(Some(&head));
        let projections = projections_read(catalog);
        if let Some(current) = projections.current_entities_at(&head) {
            let held_history = projections.history_entities_at(&head);
            if !want_history {
                return Ok((Some(head), current, Arc::new(Vec::new())));
            }
            if let Some(history) = held_history {
                return Ok((Some(head), current, history));
            }
        }
    }

    let session = catalog.begin_read().await?;
    let head = session_head(catalog, &session).await?;
    let want_history = history.wants_history(head.as_ref());

    let (held_current, held_history) = match &head {
        Some(head) => {
            let projections = projections_read(catalog);
            (
                projections.current_entities_at(head),
                projections.history_entities_at(head),
            )
        }
        None => (None, None),
    };
    let history_settled = !want_history || held_history.is_some();
    if let Some(current) = &held_current
        && history_settled
    {
        session.finish();
        let history = held_history.unwrap_or_default();
        return Ok((head, Arc::clone(current), history));
    }

    // Scan only the missing halves, under one consistent cut. The closure
    // may re-run on a read-only handle, so it clones what it captures.
    let handle = session.handle();
    let current_held = held_current.clone();
    let history_held = held_history.clone();
    let scanned = crate::store::read::consistent(handle, || {
        let current_held = current_held.clone();
        let history_held = history_held.clone();
        async move {
            let current = async {
                if let Some(records) = current_held {
                    return Ok(records);
                }
                catalog.tally_current_scan();
                Ok::<_, Error>(Arc::new(scan_current_entities(handle).await?))
            };
            let history = async {
                match (&history_held, want_history) {
                    (Some(records), _) => Ok(Arc::clone(records)),
                    (None, true) => {
                        catalog.tally_history_scan();
                        Ok::<_, Error>(Arc::new(scan_history_entities(handle).await?))
                    }
                    (None, false) => Ok(Arc::new(Vec::new())),
                }
            };
            futures::try_join!(current, history)
        }
    })
    .await;
    session.finish();
    let (current, history) = scanned?;

    if let Some(head) = &head {
        let mut projections = projections_write(catalog);
        if held_current.is_none() {
            projections.install_current_entities(*head, Arc::clone(&current));
        }
        if want_history && held_history.is_none() {
            projections.install_history_entities(*head, Arc::clone(&history));
        }
    }

    Ok((head, current, history))
}

/// Scans `current` then `history` through the shared record set, keeping
/// only the records `extract` maps to `Some`. `extract` borrows and clones
/// what it keeps, so a dump copies only the rows it returns.
async fn dump_entities<T>(
    catalog: &ReadOnlyCatalog,
    extract: impl Fn(&EntityRecord) -> Option<T>,
) -> Result<Vec<T>> {
    let (_, current, history) = entity_halves(catalog, HistoryNeed::Always).await?;
    Ok(current
        .iter()
        .chain(history.iter())
        .filter_map(extract)
        .collect())
}

/// As [`dump_entities`], for the unversioned kinds (statistics, tags,
/// mappings, scheduled deletions), which are never mirrored to `history`.
async fn dump_current_entities<T>(
    catalog: &ReadOnlyCatalog,
    extract: impl Fn(&EntityRecord) -> Option<T>,
) -> Result<Vec<T>> {
    let (_, current, _) = entity_halves(catalog, HistoryNeed::Never).await?;
    Ok(current.iter().filter_map(extract).collect())
}

/// Every `ducklake_schema` row, current and history.
#[doc(hidden)]
pub async fn dump_schemas(catalog: &ReadOnlyCatalog) -> Result<Vec<SchemaValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Schema(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_table` row, current and history.
#[doc(hidden)]
pub async fn dump_tables(catalog: &ReadOnlyCatalog) -> Result<Vec<TableValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Table(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_view` row, current and history.
#[doc(hidden)]
pub async fn dump_views(catalog: &ReadOnlyCatalog) -> Result<Vec<ViewValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::View(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_macro` row, current and history, implementations and
/// their parameters embedded in `impl_id`/`column_id` order.
#[doc(hidden)]
pub async fn dump_macros(catalog: &ReadOnlyCatalog) -> Result<Vec<MacroValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Macro(m) => Some(m.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_column_mapping` row with its embedded
/// `ducklake_name_mapping` rows in `column_id` order. Unversioned, so
/// always exactly the live rows.
#[doc(hidden)]
pub async fn dump_mappings(catalog: &ReadOnlyCatalog) -> Result<Vec<MappingValue>> {
    dump_current_entities(catalog, |r| match r {
        EntityRecord::Mapping(m) => Some(m.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_column` row, current and history.
#[doc(hidden)]
pub async fn dump_columns(catalog: &ReadOnlyCatalog) -> Result<Vec<ColumnValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Column(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_data_file` row, current and history.
#[doc(hidden)]
pub async fn dump_data_files(catalog: &ReadOnlyCatalog) -> Result<Vec<DataFileValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::File(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// As [`dump_data_files`], for a caller that keeps a row only while
/// `filter_snapshot < end_snapshot` (or it is null) — the shape every
/// DuckLake read of this table carries.
///
/// Once `filter_snapshot` reaches the head this call observes, no ended
/// version can satisfy that, so the ended half is not read and the rows
/// returned are the same ones. A bound behind the head is a time-travel
/// read and gets the full set.
///
/// # Errors
///
/// As [`dump_data_files`].
#[doc(hidden)]
pub async fn dump_data_files_live_at(
    catalog: &ReadOnlyCatalog,
    filter_snapshot: u64,
) -> Result<Vec<DataFileValue>> {
    let (_, current, history) =
        entity_halves(catalog, HistoryNeed::UnlessLiveAt(filter_snapshot)).await?;
    Ok(current
        .iter()
        .chain(history.iter())
        .filter_map(|r| match r {
            EntityRecord::File(v) => Some(v.clone()),
            _ => None,
        })
        .collect())
}

/// Every `ducklake_delete_file` row, current and history.
#[doc(hidden)]
pub async fn dump_delete_files(catalog: &ReadOnlyCatalog) -> Result<Vec<DeleteFileValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::DeleteFile(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_partition_info` row (with its embedded partition
/// columns), current and history.
#[doc(hidden)]
pub async fn dump_partition_info(catalog: &ReadOnlyCatalog) -> Result<Vec<PartitionValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Partition(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_sort_info` row (with its embedded sort expressions),
/// current and history.
#[doc(hidden)]
pub async fn dump_sort_info(catalog: &ReadOnlyCatalog) -> Result<Vec<SortValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Sort(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Serves one unversioned kind from its maintained projection when the
/// head matches; a miss derives the rows from the shared `current` half
/// and installs them for the next call.
async fn dump_projected_current<K: Ord, T: Clone>(
    catalog: &ReadOnlyCatalog,
    read: impl Fn(&ProjectionCache, &HeadValue) -> Option<Arc<BTreeMap<K, T>>>,
    install: impl Fn(&mut ProjectionCache, HeadValue, Vec<T>),
    extract: impl Fn(&EntityRecord) -> Option<T>,
) -> Result<Vec<T>> {
    // Each serve is bound in a statement of its own so the cache lock is
    // released before the rows are copied out of it.
    if let Some(head) = writer_head(catalog)? {
        let served = read(&projections_read(catalog), &head);
        if let Some(rows) = served {
            return Ok(projection::materialize(&rows));
        }
    }

    let session = catalog.begin_read().await?;
    let head = session_head(catalog, &session).await?;
    if let Some(head) = &head {
        let served = read(&projections_read(catalog), head);
        if let Some(rows) = served {
            session.finish();
            return Ok(projection::materialize(&rows));
        }
    }
    session.finish();

    // Installed at the stamp the halves were served at, which may be newer
    // than the head read above but never mismatched with the rows.
    let (served_at, current, _) = entity_halves(catalog, HistoryNeed::Never).await?;
    let rows: Vec<T> = current.iter().filter_map(extract).collect();
    if let Some(head) = served_at {
        install(&mut projections_write(catalog), head, rows.clone());
    }
    Ok(rows)
}

/// Every `ducklake_table_stats` row. Unversioned, so always exactly the
/// live rows.
#[doc(hidden)]
pub async fn dump_table_stats(catalog: &ReadOnlyCatalog) -> Result<Vec<TableStatsValue>> {
    dump_projected_current(
        catalog,
        ProjectionCache::table_stats_at,
        ProjectionCache::install_table_stats,
        |r| match r {
            EntityRecord::TableStats(v) => Some(*v),
            _ => None,
        },
    )
    .await
}

/// Every `ducklake_table_column_stats` row. Unversioned, as
/// [`dump_table_stats`].
#[doc(hidden)]
pub async fn dump_table_column_stats(
    catalog: &ReadOnlyCatalog,
) -> Result<Vec<TableColumnStatsValue>> {
    dump_projected_current(
        catalog,
        ProjectionCache::table_column_stats_at,
        ProjectionCache::install_table_column_stats,
        |r| match r {
            EntityRecord::TableColumnStats(v) => Some(v.clone()),
            _ => None,
        },
    )
    .await
}

/// Every `ducklake_file_column_stats` row. Unversioned, as
/// [`dump_table_stats`].
#[doc(hidden)]
pub async fn dump_file_column_stats(
    catalog: &ReadOnlyCatalog,
) -> Result<Vec<FileColumnStatsValue>> {
    dump_current_entities(catalog, |r| match r {
        EntityRecord::FileColumnStats(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// One table's `ducklake_file_column_stats` rows, in the order
/// [`dump_file_column_stats`] would emit them.
#[doc(hidden)]
pub async fn dump_file_column_stats_of(
    catalog: &ReadOnlyCatalog,
    table_id: u64,
) -> Result<Vec<FileColumnStatsValue>> {
    dump_current_entities(catalog, |record| match record {
        EntityRecord::FileColumnStats(value) if value.table_id == table_id => Some(value.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_snapshot`/`ducklake_snapshot_changes` row (merged).
/// Snapshots are append-only, so this is the full history.
#[doc(hidden)]
pub async fn dump_snapshots(catalog: &ReadOnlyCatalog) -> Result<Vec<SnapshotValue>> {
    // Bound in a statement of its own so the cache lock is released before
    // the rows are copied out of it.
    if let Some(head) = writer_head(catalog)? {
        let served = projections_read(catalog).snapshots_at(&head);
        if let Some(rows) = served {
            return Ok(projection::materialize(&rows));
        }
    }

    let session = catalog.begin_read().await?;
    let head = session_head(catalog, &session).await?;
    if let Some(head) = head {
        let served = projections_read(catalog).snapshots_at(&head);
        if let Some(rows) = served {
            session.finish();
            return Ok(projection::materialize(&rows));
        }
        let result = scan_snapshots(session.handle()).await;
        session.finish();
        let rows = result?;
        projections_write(catalog).install_snapshots(head, rows.clone());
        return Ok(rows);
    }
    let result = scan_snapshots(session.handle()).await;
    session.finish();
    result
}

/// One `ducklake_schema_versions` row: `(begin_snapshot, schema_version,
/// table_id)` — the snapshot a table's shape changed in, and the catalog
/// schema version that snapshot minted.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaVersionRow {
    /// The snapshot the schema change landed in.
    pub begin_snapshot: u64,
    /// That snapshot's `schema_version`.
    pub schema_version: u64,
    /// The created-or-schema-altered table.
    pub table_id: u64,
}

/// Every `ducklake_schema_versions` row, in `(table_id, begin_snapshot)`
/// order: the `schema_version` records, plus the same rows still folded
/// into older snapshot records, plus a floor row for any table whose oldest
/// data file predates every row it has left. Every live data file must be
/// covered: DuckLake's compaction planner joins `begin_snapshot` against
/// these rows and aborts on a NULL.
#[doc(hidden)]
pub async fn dump_schema_versions(catalog: &ReadOnlyCatalog) -> Result<Vec<SchemaVersionRow>> {
    let records = async {
        let session = catalog.begin_read().await?;
        let records = scan_schema_versions(session.handle()).await;
        session.finish();
        records
    };
    let file_snapshots = dump_entities(catalog, |r| match r {
        EntityRecord::File(v) => Some((v.table_id, v.begin_snapshot)),
        _ => None,
    });
    let (records, snapshots, file_snapshots) =
        futures::try_join!(records, dump_snapshots(catalog), file_snapshots)?;

    Ok(schema_version_rows_from(
        records,
        &snapshots,
        &file_snapshots,
    ))
}

/// The `ducklake_schema_versions` projection assembled from its three
/// inputs: the `schema_version` records, the snapshot records that still
/// fold the same rows in, and each data file's `(table_id, begin_snapshot)`
/// for the floor rows.
#[doc(hidden)]
#[must_use]
pub fn schema_version_rows_from(
    records: Vec<(u64, u64, u64)>,
    snapshots: &[SnapshotValue],
    file_snapshots: &[(u64, u64)],
) -> Vec<SchemaVersionRow> {
    let mut rows: BTreeMap<(u64, u64), u64> = records
        .into_iter()
        .map(|(table_id, begin_snapshot, schema_version)| {
            ((table_id, begin_snapshot), schema_version)
        })
        .collect();
    for snapshot in snapshots {
        for table_id in &snapshot.schema_changed_table_ids {
            rows.entry((*table_id, snapshot.snapshot_id))
                .or_insert(snapshot.schema_version);
        }
    }
    rows.extend(schema_version_floors(&rows, snapshots, file_snapshots));

    rows.into_iter()
        .map(
            |((table_id, begin_snapshot), schema_version)| SchemaVersionRow {
                begin_snapshot,
                schema_version,
                table_id,
            },
        )
        .collect()
}

/// The floor rows for tables whose oldest live data file sits below every
/// row they have left: one row at that file's `begin_snapshot`, carrying
/// the table's oldest surviving schema version, or the catalog's current
/// one when the table has no row at all.
fn schema_version_floors(
    rows: &BTreeMap<(u64, u64), u64>,
    snapshots: &[SnapshotValue],
    file_snapshots: &[(u64, u64)],
) -> BTreeMap<(u64, u64), u64> {
    let mut oldest_file: BTreeMap<u64, u64> = BTreeMap::new();
    for &(table_id, begin_snapshot) in file_snapshots {
        oldest_file
            .entry(table_id)
            .and_modify(|oldest| *oldest = (*oldest).min(begin_snapshot))
            .or_insert(begin_snapshot);
    }

    let uncovered: Vec<(u64, u64)> = oldest_file
        .into_iter()
        .filter(|&(table_id, file_begin_snapshot)| {
            rows.range((table_id, 0)..=(table_id, file_begin_snapshot))
                .next()
                .is_none()
        })
        .collect();
    if uncovered.is_empty() {
        return BTreeMap::new();
    }

    let current_schema_version = snapshots
        .last()
        .map_or(0, |snapshot| snapshot.schema_version);
    uncovered
        .into_iter()
        .map(|(table_id, file_begin_snapshot)| {
            let schema_version = rows
                .range((table_id, 0)..=(table_id, u64::MAX))
                .next()
                .map_or(current_schema_version, |(_, version)| *version);
            ((table_id, file_begin_snapshot), schema_version)
        })
        .collect()
}

/// Every `ducklake_files_scheduled_for_deletion` row: exactly the rows
/// awaiting physical deletion.
#[doc(hidden)]
pub async fn dump_scheduled_deletions(catalog: &ReadOnlyCatalog) -> Result<Vec<GcFileValue>> {
    dump_current_entities(catalog, |r| match r {
        EntityRecord::GcFile(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// One `ducklake_tag` row, flattened from its object's container record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagRow {
    /// The tagged object (a schema/table/view id).
    pub object_id: u64,
    /// Snapshot at which this tag value became visible.
    pub begin_snapshot: u64,
    /// Snapshot at which it was superseded, if it has been.
    pub end_snapshot: Option<u64>,
    /// Tag key.
    pub key: String,
    /// Tag value.
    pub value: String,
}

/// Every `ducklake_tag` row: one per embedded entry, ended entries
/// included.
#[doc(hidden)]
pub async fn dump_tags(catalog: &ReadOnlyCatalog) -> Result<Vec<TagRow>> {
    Ok(tag_rows_from(
        dump_current_entities(catalog, |r| match r {
            EntityRecord::Tag(v) => Some(v.clone()),
            _ => None,
        })
        .await?,
    ))
}

/// The `ducklake_tag` rows carried by `containers`, flattened.
#[doc(hidden)]
#[must_use]
pub fn tag_rows_from(containers: Vec<TagValue>) -> Vec<TagRow> {
    containers
        .into_iter()
        .flat_map(|container| {
            let object_id = container.object_id;
            container.entries.into_iter().map(move |e| TagRow {
                object_id,
                begin_snapshot: e.begin_snapshot,
                end_snapshot: e.end_snapshot,
                key: e.key,
                value: e.value,
            })
        })
        .collect()
}

/// One `ducklake_metadata` row: a catalog option, flattened from its
/// scope's record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionRow {
    /// Option key.
    pub key: String,
    /// Option value.
    pub value: String,
    /// `None` for a global option, else `"schema"` or `"table"`.
    pub scope: Option<String>,
    /// The scoped object's id; `None` alongside a `None` scope.
    pub scope_id: Option<u64>,
}

/// Every stored `ducklake_metadata` row: options carry no lifecycle, so
/// this is what is set now.
#[doc(hidden)]
pub async fn dump_options(catalog: &ReadOnlyCatalog) -> Result<Vec<OptionRow>> {
    Ok(option_rows_from(
        dump_current_entities(catalog, |r| match r {
            EntityRecord::Option {
                scope_kind,
                scope_id,
                value,
            } => Some((*scope_kind, *scope_id, value.clone())),
            _ => None,
        })
        .await?,
    ))
}

/// The `ducklake_metadata` rows carried by `scopes`, flattened and ordered.
#[doc(hidden)]
#[must_use]
pub fn option_rows_from(scopes: Vec<(u64, u64, OptionScopeValue)>) -> Vec<OptionRow> {
    let mut rows: Vec<OptionRow> = scopes
        .into_iter()
        .flat_map(|(scope_kind, scope_id, value)| {
            let scope = match scope_kind {
                1 => Some("schema".to_string()),
                2 => Some("table".to_string()),
                _ => None,
            };
            let scope_id = scope.as_ref().map(|_| scope_id);
            value
                .options
                .into_iter()
                .map(move |(key, value)| OptionRow {
                    key,
                    value,
                    scope: scope.clone(),
                    scope_id,
                })
        })
        .collect();
    rows.sort_by(|a, b| (&a.scope, a.scope_id, &a.key).cmp(&(&b.scope, b.scope_id, &b.key)));
    rows
}

/// One `ducklake_column_tag` row, flattened from its column's record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnTagRow {
    /// The tagged column's table.
    pub table_id: u64,
    /// The tagged column.
    pub column_id: u64,
    /// Snapshot at which this tag value became visible.
    pub begin_snapshot: u64,
    /// Snapshot at which it was superseded, if it has been.
    pub end_snapshot: Option<u64>,
    /// Tag key.
    pub key: String,
    /// Tag value.
    pub value: String,
}

/// The tags one column record carries, with what places the record among
/// its column's versions.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnTags {
    /// The owning table.
    pub table_id: u64,
    /// The tagged column.
    pub column_id: u64,
    /// Snapshot at which this column record was superseded, if it has been.
    pub end_snapshot: Option<u64>,
    /// The record's tags.
    pub tags: Vec<ColumnTag>,
}

impl From<&ColumnValue> for ColumnTags {
    fn from(column: &ColumnValue) -> Self {
        Self {
            table_id: column.table_id,
            column_id: column.column_id,
            end_snapshot: column.end_snapshot,
            tags: column.tags.clone(),
        }
    }
}

/// The `ducklake_column_tag` rows carried by `columns`, flattened.
#[doc(hidden)]
#[must_use]
pub fn column_tag_rows_from(columns: impl IntoIterator<Item = ColumnTags>) -> Vec<ColumnTagRow> {
    let mut latest: BTreeMap<(u64, u64), ColumnTags> = BTreeMap::new();
    for column in columns {
        match latest.entry((column.table_id, column.column_id)) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(column);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                // Later than the incumbent: live beats ended, higher end
                // beats lower.
                let newer = match (column.end_snapshot, entry.get().end_snapshot) {
                    (None, _) => true,
                    (Some(_), None) => false,
                    (Some(a), Some(b)) => a > b,
                };
                if newer {
                    entry.insert(column);
                }
            }
        }
    }

    latest
        .into_values()
        .flat_map(|column| {
            column.tags.into_iter().map(move |t| ColumnTagRow {
                table_id: column.table_id,
                column_id: column.column_id,
                begin_snapshot: t.begin_snapshot,
                end_snapshot: t.end_snapshot,
                key: t.key,
                value: t.value,
            })
        })
        .collect()
}

/// Every `ducklake_column_tag` row, emitted from each column's latest
/// record only, which carries the authoritative entries.
#[doc(hidden)]
pub async fn dump_column_tags(catalog: &ReadOnlyCatalog) -> Result<Vec<ColumnTagRow>> {
    let columns = dump_entities(catalog, |r| match r {
        EntityRecord::Column(v) => Some(ColumnTags::from(v)),
        _ => None,
    })
    .await?;

    Ok(column_tag_rows_from(columns))
}

/// One `ducklake_macro_impl` row, flattened from its macro's record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroImplRow {
    /// The owning macro.
    pub macro_id: u64,
    /// Implementation id within the macro.
    pub impl_id: u64,
    /// SQL dialect the implementation targets.
    pub dialect: String,
    /// Implementation body.
    pub sql: String,
    /// `"scalar"` or `"table"`.
    pub macro_type: String,
}

/// The `MacroImpl` rows carried by `parents`, flattened.
#[doc(hidden)]
#[must_use]
pub fn macro_impl_rows_from(parents: Vec<MacroValue>) -> Vec<MacroImplRow> {
    parents
        .into_iter()
        .flat_map(|value| {
            let macro_id = value.macro_id;
            value
                .implementations
                .into_iter()
                .map(move |implementation| MacroImplRow {
                    macro_id,
                    impl_id: implementation.impl_id,
                    dialect: implementation.dialect,
                    sql: implementation.sql,
                    macro_type: implementation.macro_type,
                })
        })
        .collect()
}

/// Every `ducklake_macro_impl` row, current and history.
#[doc(hidden)]
pub async fn dump_macro_impl_rows(catalog: &ReadOnlyCatalog) -> Result<Vec<MacroImplRow>> {
    Ok(macro_impl_rows_from(dump_macros(catalog).await?))
}

/// One `ducklake_macro_parameters` row, flattened from its macro's
/// record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroParameterRow {
    /// The owning macro.
    pub macro_id: u64,
    /// The owning implementation.
    pub impl_id: u64,
    /// Parameter position.
    pub column_id: u64,
    /// Parameter name.
    pub parameter_name: String,
    /// Parameter type.
    pub parameter_type: String,
    /// Default value, if declared.
    pub default_value: Option<String>,
    /// Type of the default value.
    pub default_value_type: String,
}

/// The `MacroParameter` rows carried by `parents`, flattened.
#[doc(hidden)]
#[must_use]
pub fn macro_parameter_rows_from(parents: Vec<MacroValue>) -> Vec<MacroParameterRow> {
    parents
        .into_iter()
        .flat_map(|value| {
            let macro_id = value.macro_id;
            value.implementations.into_iter().flat_map(move |i| {
                let impl_id = i.impl_id;
                i.parameters.into_iter().map(move |p| MacroParameterRow {
                    macro_id,
                    impl_id,
                    column_id: p.column_id,
                    parameter_name: p.parameter_name,
                    parameter_type: p.parameter_type,
                    default_value: p.default_value,
                    default_value_type: p.default_value_type,
                })
            })
        })
        .collect()
}

/// Every `ducklake_macro_parameters` row, current and history.
#[doc(hidden)]
pub async fn dump_macro_parameter_rows(
    catalog: &ReadOnlyCatalog,
) -> Result<Vec<MacroParameterRow>> {
    Ok(macro_parameter_rows_from(dump_macros(catalog).await?))
}

/// One `ducklake_name_mapping` row, flattened from its mapping's record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameMappingRow {
    /// The owning column mapping.
    pub mapping_id: u64,
    /// The mapped column.
    pub column_id: u64,
    /// Source name in the file.
    pub source_name: String,
    /// Target field id.
    pub target_field_id: u64,
    /// Parent column for nested fields, if any.
    pub parent_column: Option<u64>,
    /// Whether the source column is a partition column.
    pub is_partition: bool,
}

/// The `NameMapping` rows carried by `parents`, flattened.
#[doc(hidden)]
#[must_use]
pub fn name_mapping_rows_from(parents: Vec<MappingValue>) -> Vec<NameMappingRow> {
    parents
        .into_iter()
        .flat_map(|value| {
            let mapping_id = value.mapping_id;
            value
                .name_mappings
                .into_iter()
                .map(move |row| NameMappingRow {
                    mapping_id,
                    column_id: row.column_id,
                    source_name: row.source_name,
                    target_field_id: row.target_field_id,
                    parent_column: row.parent_column,
                    is_partition: row.is_partition,
                })
        })
        .collect()
}

/// Every `ducklake_name_mapping` row (mappings are unversioned).
#[doc(hidden)]
pub async fn dump_name_mapping_rows(catalog: &ReadOnlyCatalog) -> Result<Vec<NameMappingRow>> {
    Ok(name_mapping_rows_from(dump_mappings(catalog).await?))
}

/// One `ducklake_partition_column` row, flattened from its spec's record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionColumnRow {
    /// The owning partition spec.
    pub partition_id: u64,
    /// The spec's table.
    pub table_id: u64,
    /// Position within the partition key.
    pub partition_key_index: u64,
    /// The partitioning column.
    pub column_id: u64,
    /// Partition transform.
    pub transform: String,
}

/// The `PartitionColumn` rows carried by `parents`, flattened.
#[doc(hidden)]
#[must_use]
pub fn partition_column_rows_from(parents: Vec<PartitionValue>) -> Vec<PartitionColumnRow> {
    parents
        .into_iter()
        .flat_map(|spec| {
            let (partition_id, table_id) = (spec.partition_id, spec.table_id);
            spec.columns
                .into_iter()
                .map(move |column| PartitionColumnRow {
                    partition_id,
                    table_id,
                    partition_key_index: column.partition_key_index,
                    column_id: column.column_id,
                    transform: column.transform,
                })
        })
        .collect()
}

/// Every `ducklake_partition_column` row, current and history.
#[doc(hidden)]
pub async fn dump_partition_column_rows(
    catalog: &ReadOnlyCatalog,
) -> Result<Vec<PartitionColumnRow>> {
    Ok(partition_column_rows_from(
        dump_partition_info(catalog).await?,
    ))
}

/// One `ducklake_file_partition_value` row, flattened from its file's
/// record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilePartitionValueRow {
    /// The owning data file.
    pub data_file_id: u64,
    /// The file's table.
    pub table_id: u64,
    /// Position within the partition key.
    pub partition_key_index: u64,
    /// The value, rendered as text.
    pub partition_value: String,
}

/// The `FilePartitionValue` rows carried by `parents`, flattened.
#[doc(hidden)]
#[must_use]
pub fn file_partition_value_rows_from(parents: Vec<DataFileValue>) -> Vec<FilePartitionValueRow> {
    parents
        .into_iter()
        .flat_map(|file| {
            let (data_file_id, table_id) = (file.data_file_id, file.table_id);
            file.partition_values
                .into_iter()
                .map(move |value| FilePartitionValueRow {
                    data_file_id,
                    table_id,
                    partition_key_index: value.partition_key_index,
                    partition_value: value.partition_value,
                })
        })
        .collect()
}

/// Every `ducklake_file_partition_value` row, current and history.
#[doc(hidden)]
pub async fn dump_file_partition_value_rows(
    catalog: &ReadOnlyCatalog,
) -> Result<Vec<FilePartitionValueRow>> {
    Ok(file_partition_value_rows_from(
        dump_data_files(catalog).await?,
    ))
}

/// One `ducklake_sort_expression` row, flattened from its spec's record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortExpressionRow {
    /// The owning sort spec.
    pub sort_id: u64,
    /// The spec's table.
    pub table_id: u64,
    /// Position within the sort key.
    pub sort_key_index: u64,
    /// The sort expression.
    pub expression: String,
    /// SQL dialect of the expression.
    pub dialect: String,
    /// `ASC`/`DESC`.
    pub sort_direction: String,
    /// Null ordering.
    pub null_order: String,
}

/// The `SortExpression` rows carried by `parents`, flattened.
#[doc(hidden)]
#[must_use]
pub fn sort_expression_rows_from(parents: Vec<SortValue>) -> Vec<SortExpressionRow> {
    parents
        .into_iter()
        .flat_map(|spec| {
            let (sort_id, table_id) = (spec.sort_id, spec.table_id);
            spec.expressions
                .into_iter()
                .map(move |expression| SortExpressionRow {
                    sort_id,
                    table_id,
                    sort_key_index: expression.sort_key_index,
                    expression: expression.expression,
                    dialect: expression.dialect,
                    sort_direction: expression.sort_direction,
                    null_order: expression.null_order,
                })
        })
        .collect()
}

/// Every `ducklake_sort_expression` row, current and history.
#[doc(hidden)]
pub async fn dump_sort_expression_rows(
    catalog: &ReadOnlyCatalog,
) -> Result<Vec<SortExpressionRow>> {
    Ok(sort_expression_rows_from(dump_sort_info(catalog).await?))
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;
    use crate::catalog::{
        Catalog, CatalogOptions, ColumnDef, ColumnStats, DataFile, DeleteFile, FileColumnStats,
        MacroImplementationDef, MacroParameterDef,
    };

    /// Seeds a store whose second commit renames a table — the fixture
    /// every assertion below reads from, so a table, a schema, a view, a
    /// data file, a delete file, and every statistics kind all carry both
    /// a current row and (for the versioned kinds) a history row with exact
    /// lifecycle values.
    async fn seed() -> Catalog {
        seed_on(Arc::new(InMemory::new())).await
    }

    /// As [`seed`], over a store the caller keeps — for the tests that open
    /// a second handle on it.
    async fn seed_on(store: Arc<InMemory>) -> Catalog {
        let catalog = Catalog::open(store, CatalogOptions::default())
            .await
            .unwrap();

        catalog
            .commit(|tx| {
                let schema = tx.create_schema("sales")?;
                let table = tx.create_table(
                    schema,
                    "orders",
                    &[
                        ColumnDef {
                            name: "id".into(),
                            column_type: "BIGINT".into(),
                            nulls_allowed: false,
                            default_value: None,
                            children: Vec::new(),
                        },
                        ColumnDef {
                            name: "amount".into(),
                            column_type: "DOUBLE".into(),
                            nulls_allowed: true,
                            default_value: None,
                            children: Vec::new(),
                        },
                    ],
                )?;
                let column = tx.columns_of(table)[0].id;
                let file = tx.register_data_file(
                    table,
                    DataFile {
                        path: "orders/data-1.parquet".into(),
                        path_is_relative: true,
                        file_format: "parquet".into(),
                        record_count: 10,
                        file_size_bytes: 1024,
                        footer_size: 64,
                        encryption_key: None,
                        partition_values: vec![],
                        column_stats: vec![FileColumnStats {
                            column_id: column,
                            column_size_bytes: 100,
                            value_count: 10,
                            null_count: 0,
                            min_value: Some("1".into()),
                            max_value: Some("10".into()),
                            contains_nan: None,
                            extra_stats: None,
                        }],
                    },
                    &[],
                )?;
                tx.register_delete_file(
                    table,
                    DeleteFile {
                        data_file_id: file,
                        path: "orders/delete-1.parquet".into(),
                        path_is_relative: true,
                        format: "parquet".into(),
                        delete_count: 2,
                        file_size_bytes: 128,
                        footer_size: 32,
                        encryption_key: None,
                    },
                    &[],
                )?;
                tx.update_column_stats(
                    table,
                    column,
                    ColumnStats {
                        contains_null: Some(false),
                        contains_nan: None,
                        min_value: Some("1".into()),
                        max_value: Some("10".into()),
                        extra_stats: None,
                    },
                )?;
                tx.create_view(schema, "orders_v", "duckdb", "select * from orders")?;
                Ok(())
            })
            .await
            .unwrap();

        catalog
            .commit(|tx| {
                let table = tx.tables_in(tx.schemas()[1].id)[0].id;
                tx.rename_table(table, "orders2")
            })
            .await
            .unwrap();

        catalog
    }

    /// The head stamp stands still while nothing commits and moves for
    /// every batch, snapshot-minting or not.
    #[tokio::test]
    async fn the_head_stamp_moves_for_every_batch() {
        use crate::catalog::OptionScope;

        let catalog = seed().await;

        let first = head_stamp(&catalog).await.unwrap().unwrap();
        let repeat = head_stamp(&catalog).await.unwrap().unwrap();
        assert_eq!(first, repeat, "the stamp moved with no commit");

        catalog
            .commit(|tx| tx.create_schema("more").map(|_| ()))
            .await
            .unwrap();
        let minted = head_stamp(&catalog).await.unwrap().unwrap();
        assert_eq!(minted.snapshot_id, first.snapshot_id + 1);
        assert_eq!(minted.batch_seq, first.batch_seq + 1);

        // Reuses the snapshot id; only the batch count says the store moved.
        catalog
            .commit(|tx| tx.set_option(OptionScope::Global, "answer", "42"))
            .await
            .unwrap();
        let reused = head_stamp(&catalog).await.unwrap().unwrap();
        assert_eq!(reused.snapshot_id, minted.snapshot_id);
        assert_eq!(reused.batch_seq, minted.batch_seq + 1);
    }

    /// At one head stamp, `current` and `history` are each scanned at most
    /// once across every logical cache: the head view and the full dump
    /// spread all derive from one shared record set.
    #[tokio::test]
    async fn one_scan_pair_serves_the_head_view_and_every_dump() {
        let catalog = seed().await;

        let before = catalog.entity_scan_tallies();
        let _ = catalog.snapshot().await.unwrap();
        dump_schemas(&catalog).await.unwrap();
        dump_tables(&catalog).await.unwrap();
        dump_columns(&catalog).await.unwrap();
        dump_data_files(&catalog).await.unwrap();
        dump_delete_files(&catalog).await.unwrap();
        dump_views(&catalog).await.unwrap();
        dump_table_stats(&catalog).await.unwrap();
        dump_table_column_stats(&catalog).await.unwrap();
        let after = catalog.entity_scan_tallies();

        assert!(
            after.0 - before.0 <= 1,
            "current scanned {} times at one head",
            after.0 - before.0
        );
        assert!(
            after.1 - before.1 <= 1,
            "history scanned {} times at one head",
            after.1 - before.1
        );

        // A second pass at the same head scans nothing at all.
        let _ = catalog.snapshot().await.unwrap();
        dump_schemas(&catalog).await.unwrap();
        dump_table_stats(&catalog).await.unwrap();
        let again = catalog.entity_scan_tallies();
        assert_eq!(again, after, "a warm pass re-scanned");
    }

    /// A head-view materialization alone never scans `history` — the view
    /// needs `current` only, and sharing must not grow it a scan.
    #[tokio::test]
    async fn a_head_view_alone_scans_no_history() {
        let catalog = seed().await;

        let before = catalog.entity_scan_tallies();
        let _ = catalog.snapshot().await.unwrap();
        let after = catalog.entity_scan_tallies();

        assert_eq!(after.1, before.1, "a head view scanned history");
    }

    /// Unversioned kinds (statistics, tags, scheduled deletions) live only
    /// in `current`; their dumps must serve exactly the live rows on a
    /// catalog whose history is non-empty.
    #[tokio::test]
    async fn unversioned_dumps_serve_live_rows_on_a_history_bearing_catalog() {
        let catalog = seed().await;

        let stats = dump_table_stats(&catalog).await.unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].record_count, 10);

        let column_stats = dump_table_column_stats(&catalog).await.unwrap();
        assert_eq!(column_stats.len(), 1);
        assert_eq!(column_stats[0].min_value.as_deref(), Some("1"));

        let file_stats = dump_file_column_stats(&catalog).await.unwrap();
        assert_eq!(file_stats.len(), 1);

        let deletions = dump_scheduled_deletions(&catalog).await.unwrap();
        assert!(deletions.is_empty());
    }

    /// Plants a `sys/migration` marker under a live catalog handle: the
    /// shape a reader meets when a migrator starts after it attached and
    /// its open-time format check has already passed. As a real start
    /// batch does, the marker rides a head stamp.
    async fn plant_migration_marker(catalog: &Catalog) {
        use crate::{
            store::{
                handle::ReadHandle,
                key::{Key, SysKey},
                proto::MigrationValue,
                read,
                value::encode_value,
            },
            transaction::commit,
        };

        let tx = catalog.begin_write_tx().await.unwrap();
        let head = read::read_head(ReadHandle::Tx(&tx)).await.unwrap().unwrap();
        tx.put(
            Key::Sys(SysKey::Migration).encode(),
            encode_value(&MigrationValue {
                from_format: 1,
                to_format: 2,
                cursor: Vec::new(),
            }),
        )
        .unwrap();
        let (key, stamp) = commit::head_stamp(head.snapshot_id, head.batch_seq);
        tx.put(key, stamp.unwrap()).unwrap();
        tx.commit_with_options(&commit::durable()).await.unwrap();
    }

    fn refuses<T>(outcome: &Result<T>) -> bool {
        matches!(outcome, Err(crate::error::Error::Migration(_)))
    }

    /// Every dump refuses a store undergoing a migration.
    #[tokio::test]
    async fn a_planted_marker_refuses_every_read_seam() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let writer = seed_on(Arc::clone(&store)).await;

        // The handle a migration can start under: a migrator takes the
        // writer epoch, which fences a read-write handle rather than
        // leaving it reading, so the reader is the one that meets a marker
        // it did not already refuse to open against.
        let options = CatalogOptions {
            reader_poll_interval: std::time::Duration::from_millis(20),
            ..CatalogOptions::default()
        };
        let catalog = Catalog::open_read_only(store, options).await.unwrap();
        catalog.snapshot().await.unwrap();

        plant_migration_marker(&writer).await;
        for _ in 0..100 {
            if catalog.snapshot().await.is_err() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        assert!(refuses(&catalog.snapshot().await), "snapshot");
        assert!(refuses(&dump_schemas(&catalog).await), "dump_schemas");
        assert!(refuses(&dump_mappings(&catalog).await), "dump_mappings");
        assert!(
            refuses(&dump_table_stats(&catalog).await),
            "dump_table_stats"
        );
        assert!(refuses(&dump_snapshots(&catalog).await), "dump_snapshots");

        assert!(
            refuses(&inline::scan_inline(&catalog, 1, inline::InlineScanKind::Table, 1, 0).await),
            "scan_inline"
        );
        assert!(
            refuses(&inline::inline_schemas(&catalog, 1).await),
            "inline_schemas"
        );
        assert!(
            refuses(&inline::inline_registered_tables(&catalog).await),
            "inline_registered_tables"
        );
        assert!(
            refuses(&inline::inline_file_delete_table_exists(&catalog, 1).await),
            "inline_file_delete_table_exists"
        );
        assert!(
            refuses(&inline::inline_file_deletes(&catalog, 1).await),
            "inline_file_deletes"
        );
    }

    /// Expires every snapshot below the head and, when `records`, the
    /// schema-version records too, staged the way DuckLake stages them.
    async fn expire_snapshots_below_head(catalog: &Catalog, records: bool) {
        use crate::transaction::staged::{Cell, RowOperation, TableKind};

        let head = catalog.snapshot().await.unwrap().current_snapshot().id;
        let mut tx = staged::staged_begin(catalog, None, String::new())
            .await
            .unwrap();
        for snapshot in dump_snapshots(catalog).await.unwrap() {
            if snapshot.snapshot_id == head.get() {
                continue;
            }
            tx.stage(RowOperation::Delete {
                table: TableKind::Snapshot,
                cells: vec![Cell::U64(snapshot.snapshot_id)],
            });
        }
        if records {
            for row in dump_schema_versions(catalog).await.unwrap() {
                tx.stage(RowOperation::Delete {
                    table: TableKind::SchemaVersions,
                    cells: vec![
                        Cell::U64(row.begin_snapshot),
                        Cell::U64(row.schema_version),
                        Cell::U64(row.table_id),
                    ],
                });
            }
        }
        tx.commit().await.unwrap();
    }

    /// Schema-version rows outlive the snapshots that wrote them: expiry
    /// takes the snapshot records, and the rows a data file resolves its
    /// schema version through stay exactly as they were.
    #[tokio::test]
    async fn schema_version_rows_outlive_the_snapshots_that_wrote_them() {
        let catalog = seed().await;

        let before = dump_schema_versions(&catalog).await.unwrap();
        assert!(
            before
                .iter()
                .any(|row| row.begin_snapshot == 1 && row.schema_version == 1),
            "the creating commit's rows must be recorded: {before:?}"
        );

        expire_snapshots_below_head(&catalog, false).await;
        assert_eq!(dump_snapshots(&catalog).await.unwrap().len(), 1);
        assert_eq!(dump_schema_versions(&catalog).await.unwrap(), before);
    }

    /// A store written before schema-version records existed lost its rows
    /// with the snapshots that carried them, leaving its data files with
    /// no row to resolve against. The projection floors each such table at
    /// its oldest file, carrying the oldest schema version still known.
    #[tokio::test]
    async fn a_store_whose_schema_version_rows_are_gone_gets_a_floor_row() {
        let catalog = seed().await;
        let file = dump_data_files(&catalog).await.unwrap()[0].clone();
        assert_eq!(file.begin_snapshot, 1);

        expire_snapshots_below_head(&catalog, true).await;

        // The head snapshot survives expiry and still carries its own
        // row (the rename's); the file predates it, so the floor covers
        // the gap between the file and that row.
        assert_eq!(
            dump_schema_versions(&catalog).await.unwrap(),
            vec![
                SchemaVersionRow {
                    begin_snapshot: file.begin_snapshot,
                    schema_version: 2,
                    table_id: file.table_id,
                },
                SchemaVersionRow {
                    begin_snapshot: 2,
                    schema_version: 2,
                    table_id: file.table_id,
                },
            ]
        );
    }

    #[tokio::test]
    async fn dump_schemas_returns_bootstrap_and_seeded_schemas_with_no_history() {
        let catalog = seed().await;
        let rows = dump_schemas(&catalog).await.unwrap();
        // `main` (bootstrap) and `sales`; the rename touched only the
        // table, so neither schema ever moved to history.
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.end_snapshot.is_none()));
        let names: Vec<&str> = rows.iter().map(|r| r.schema_name.as_str()).collect();
        assert_eq!(names, vec!["main", "sales"]);
    }

    #[tokio::test]
    async fn dump_tables_returns_both_versions_of_a_renamed_table() {
        let catalog = seed().await;
        let rows = dump_tables(&catalog).await.unwrap();
        assert_eq!(
            rows.len(),
            2,
            "rename must yield exactly one current + one history row"
        );

        let ended = rows.iter().find(|r| r.end_snapshot.is_some()).unwrap();
        let live = rows.iter().find(|r| r.end_snapshot.is_none()).unwrap();
        assert_eq!(ended.table_name, "orders");
        assert_eq!(live.table_name, "orders2");
        // Same entity, same uuid, exact lifecycle stitching: the history
        // row's end_snapshot is the live row's (new) begin_snapshot.
        assert_eq!(ended.table_id, live.table_id);
        assert_eq!(ended.table_uuid, live.table_uuid);
        assert_eq!(ended.end_snapshot, Some(live.begin_snapshot));
        assert!(live.begin_snapshot > ended.begin_snapshot);
    }

    #[tokio::test]
    async fn dump_columns_and_views_are_row_faithful() {
        let catalog = seed().await;

        let columns = dump_columns(&catalog).await.unwrap();
        assert_eq!(columns.len(), 2);
        assert!(columns.iter().all(|c| c.end_snapshot.is_none()));
        let mut names: Vec<&str> = columns.iter().map(|c| c.column_name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["amount", "id"]);

        let views = dump_views(&catalog).await.unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].sql, "select * from orders");
        assert!(views[0].end_snapshot.is_none());
    }

    /// A mapping staged through the DuckLake row path dumps back
    /// row-faithfully, embedded rows in `column_id` order.
    #[tokio::test]
    async fn dump_mappings_serves_embedded_rows() {
        use crate::transaction::staged::{Cell, RowOperation, StagedTransaction, TableKind};

        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut tx = StagedTransaction::begin_detached(db_tx);
        tx.stage(RowOperation::Insert {
            table: TableKind::ColumnMapping,
            cells: vec![Cell::U64(21), Cell::U64(1), Cell::Str("map_by_name".into())],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::NameMapping,
            cells: vec![
                Cell::U64(21),
                Cell::U64(0),
                Cell::Str("id".into()),
                Cell::U64(1),
                Cell::Null,
                Cell::Bool(false),
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: vec![
                Cell::U64(1),
                Cell::I64(1),
                Cell::U64(1),
                Cell::U64(11),
                Cell::U64(22),
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: vec![
                Cell::U64(1),
                Cell::Str("inserted_into_table:1".into()),
                Cell::Null,
                Cell::Null,
                Cell::Null,
            ],
        });
        tx.commit().await.unwrap();

        let mappings = dump_mappings(&catalog).await.unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].mapping_id, 21);
        assert_eq!(mappings[0].table_id, 1);
        assert_eq!(mappings[0].map_type, "map_by_name");
        assert_eq!(mappings[0].name_mappings.len(), 1);
        assert_eq!(mappings[0].name_mappings[0].source_name, "id");
    }

    /// An ended macro keeps serving its implementation and parameter
    /// rows: the whole record — children included — mirrors to history,
    /// where time travel still reads it.
    #[tokio::test]
    async fn dump_macros_serves_children_current_and_history() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        catalog
            .commit(|tx| {
                let schema = tx.create_schema("s")?;
                tx.create_macro(
                    schema,
                    "add",
                    &[
                        MacroImplementationDef {
                            dialect: "duckdb".into(),
                            sql: "(a + 1)".into(),
                            macro_type: "scalar".into(),
                            parameters: vec![MacroParameterDef {
                                name: "a".into(),
                                parameter_type: "unknown".into(),
                                default_value: None,
                                default_value_type: "unknown".into(),
                            }],
                        },
                        MacroImplementationDef {
                            dialect: "duckdb".into(),
                            sql: "(a + b)".into(),
                            macro_type: "scalar".into(),
                            parameters: vec![
                                MacroParameterDef {
                                    name: "a".into(),
                                    parameter_type: "unknown".into(),
                                    default_value: None,
                                    default_value_type: "unknown".into(),
                                },
                                MacroParameterDef {
                                    name: "b".into(),
                                    parameter_type: "unknown".into(),
                                    default_value: Some("5".into()),
                                    default_value_type: "int32".into(),
                                },
                            ],
                        },
                    ],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let head = catalog.snapshot().await.unwrap();
        let schema = head.schema_by_name("s").unwrap();
        let created = head.macro_by_name(schema.id, "add").unwrap();
        catalog
            .commit(move |tx| tx.drop_macro(created.id))
            .await
            .unwrap();

        let macros = dump_macros(&catalog).await.unwrap();
        assert_eq!(macros.len(), 1);
        let ended = &macros[0];
        assert!(ended.end_snapshot.is_some());
        assert_eq!(ended.implementations.len(), 2);
        assert_eq!(ended.implementations[0].impl_id, 0);
        assert_eq!(ended.implementations[1].parameters[1].parameter_name, "b");
        assert_eq!(
            ended.implementations[1].parameters[1]
                .default_value
                .as_deref(),
            Some("5")
        );
    }

    #[tokio::test]
    async fn dump_data_and_delete_files_carry_registration_values_verbatim() {
        let catalog = seed().await;

        let files = dump_data_files(&catalog).await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "orders/data-1.parquet");
        assert_eq!(files[0].record_count, 10);
        assert_eq!(files[0].row_id_start, Some(0));
        assert!(files[0].end_snapshot.is_none());

        let deletes = dump_delete_files(&catalog).await.unwrap();
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].path, "orders/delete-1.parquet");
        assert_eq!(deletes[0].data_file_id, files[0].data_file_id);
        assert_eq!(deletes[0].delete_count, 2);
    }

    #[tokio::test]
    async fn dump_statistics_kinds_are_unversioned_single_rows() {
        let catalog = seed().await;

        // The rename commit did not touch statistics, and stats are
        // never mirrored to history regardless — one row each, live values.
        let table_rows = dump_table_stats(&catalog).await.unwrap();
        assert_eq!(table_rows.len(), 1);
        assert_eq!(table_rows[0].record_count, 10);

        let table_col_rows = dump_table_column_stats(&catalog).await.unwrap();
        assert_eq!(table_col_rows.len(), 1);
        assert_eq!(table_col_rows[0].contains_null, Some(false));

        let file_col_rows = dump_file_column_stats(&catalog).await.unwrap();
        assert_eq!(file_col_rows.len(), 1);
        assert_eq!(file_col_rows[0].min_value.as_deref(), Some("1"));
        assert_eq!(file_col_rows[0].max_value.as_deref(), Some("10"));
    }

    /// A scoped file-statistics dump is the contiguous run its offset
    /// names in the unscoped one.
    ///
    /// This is what lets the extension scope a scan without renumbering its
    /// rows: a scoped row's position in the whole is `offset + index`, so
    /// row ids keep meaning what they meant. If the dump ever stopped being
    /// key-ordered table-major, this is what would catch it.
    #[tokio::test]
    async fn scoped_file_column_stats_are_a_contiguous_run_of_the_whole() {
        let catalog = seed().await;

        // A second table, so one table's rows are a strict subset and the
        // offset has something to skip.
        catalog
            .commit(|tx| {
                let schema = tx.schemas()[0].id;
                let table = tx.create_table(
                    schema,
                    "returns",
                    &[ColumnDef {
                        name: "id".into(),
                        column_type: "BIGINT".into(),
                        nulls_allowed: false,
                        default_value: None,
                        children: Vec::new(),
                    }],
                )?;
                let column = tx.columns_of(table)[0].id;
                for (ordinal, path) in ["returns/a.parquet", "returns/b.parquet"]
                    .iter()
                    .enumerate()
                {
                    tx.register_data_file(
                        table,
                        DataFile {
                            path: (*path).into(),
                            path_is_relative: true,
                            file_format: "parquet".into(),
                            record_count: 4,
                            file_size_bytes: 64,
                            footer_size: 8,
                            encryption_key: None,
                            partition_values: vec![],
                            column_stats: vec![FileColumnStats {
                                column_id: column,
                                column_size_bytes: 10,
                                value_count: 4,
                                null_count: 0,
                                min_value: Some(ordinal.to_string()),
                                max_value: Some("9".into()),
                                contains_nan: None,
                                extra_stats: None,
                            }],
                        },
                        &[],
                    )?;
                }
                Ok(())
            })
            .await
            .unwrap();

        let whole = dump_file_column_stats(&catalog).await.unwrap();
        assert_eq!(whole.len(), 3, "one from the seed, two from `returns`");

        let table_ids: std::collections::BTreeSet<u64> =
            whole.iter().map(|row| row.table_id).collect();
        assert_eq!(table_ids.len(), 2);

        let mut tiled = Vec::new();
        for table_id in table_ids {
            let scoped = dump_file_column_stats_of(&catalog, table_id).await.unwrap();
            assert!(!scoped.is_empty(), "table {table_id} has statistics");
            tiled.extend(scoped);
        }
        assert_eq!(
            tiled, whole,
            "the scopes must tile the whole dump, in order"
        );

        assert!(
            dump_file_column_stats_of(&catalog, u64::MAX)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn dump_snapshots_returns_every_committed_snapshot_in_order() {
        let catalog = seed().await;
        let rows = dump_snapshots(&catalog).await.unwrap();
        // Bootstrap (0) + the two commits `seed` makes.
        assert_eq!(rows.len(), 3);
        let ids: Vec<u64> = rows.iter().map(|r| r.snapshot_id).collect();
        assert_eq!(ids, vec![0, 1, 2]);
        // Bootstrap records minting `main`, exactly as DuckLake's own
        // initialization writes it; both real commits record something.
        assert_eq!(rows[0].changes_made, "created_schema:\"main\"");
        assert!(!rows[1].changes_made.is_empty());
        assert!(!rows[2].changes_made.is_empty());
    }
}
