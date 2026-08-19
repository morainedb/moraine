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
    catalog::{Catalog, projection::ProjectionCache},
    error::Result,
    store::{
        proto::{
            ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue, GcFileValue,
            MacroValue, MappingValue, OptionScopeValue, PartitionValue, SchemaValue, SnapshotValue,
            SortValue, TableColumnStatsValue, TableStatsValue, TableValue, TagValue, ViewValue,
        },
        read::{
            EntityRecord, scan_current_entities, scan_current_entities_overlaid,
            scan_history_entities, scan_history_entities_overlaid, scan_schema_versions,
            scan_schema_versions_overlaid, scan_snapshots, scan_snapshots_overlaid,
        },
    },
};

/// Locks the shared projection state for reading, recovering a poisoned
/// lock (folds never panic mid-flight, so the state is whole).
fn projections_read(catalog: &Catalog) -> std::sync::RwLockReadGuard<'_, ProjectionCache> {
    catalog
        .projections()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// As [`projections_read`], for writing (installs).
fn projections_write(catalog: &Catalog) -> std::sync::RwLockWriteGuard<'_, ProjectionCache> {
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
// from, instead of mirroring them as anonymous tuples. The transaction-aware
// dumps convert from the same types, so both share one conversion.
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

/// The full current+history record set at the session head, served from
/// the maintained entity projection when its head matches; a fresh scan
/// pair installs it otherwise. Populating DuckLake's metadata tables
/// issues ~two dozen `dump_*` calls, and this collapses their store cost
/// to one scan pair per head.
async fn all_entities(catalog: &Catalog) -> Result<Arc<Vec<EntityRecord>>> {
    let read = catalog.begin_dump().await?;
    let head = read.head_id();

    let cache_at = match (catalog.maintains_projections(), head) {
        (true, Some(head)) => {
            let cached = projections_read(catalog).entities_at(head);
            if let Some(records) = cached {
                read.finish().await;
                return Ok(records);
            }
            Some(head)
        }
        _ => None,
    };

    let (current, history) = match read.overlay() {
        Some(overlay) => (
            scan_current_entities_overlaid(read.handle(), overlay).await,
            scan_history_entities_overlaid(read.handle(), overlay).await,
        ),
        None => (
            scan_current_entities(read.handle()).await,
            scan_history_entities(read.handle()).await,
        ),
    };
    read.finish().await;
    let mut records = current?;
    records.extend(history?);
    let records = Arc::new(records);
    if let Some(head) = cache_at {
        projections_write(catalog).install_entities(head, records.as_ref().clone());
    }

    Ok(records)
}

/// Scans `current` then `history` (through the entity projection),
/// keeping only the records `extract` maps to `Some` — the shared engine
/// every *versioned* entity-kind `dump_*` function below is a thin,
/// concretely typed wrapper over.
///
/// `extract` borrows and clones what it keeps, so a dump copies only the
/// rows it returns. Taking it by value would clone the whole shared record
/// set per call, and one population of DuckLake's metadata tables issues
/// two dozen of them.
async fn dump_entities<T>(
    catalog: &Catalog,
    extract: impl Fn(&EntityRecord) -> Option<T>,
) -> Result<Vec<T>> {
    let records = all_entities(catalog).await?;
    Ok(records.iter().filter_map(extract).collect())
}

/// As [`dump_entities`], for the unversioned kinds (statistics, tags,
/// mappings, scheduled deletions). They are overwritten in place and
/// never mirrored to `history` — a history record of one is refused as
/// corruption at scan — so the merged record set holds exactly their
/// live rows and the shared entity projection serves them too. A
/// read-only catalog (no projections) scans `current` only, where the
/// history scan would be pure waste.
async fn dump_current_entities<T>(
    catalog: &Catalog,
    extract: impl Fn(&EntityRecord) -> Option<T>,
) -> Result<Vec<T>> {
    if catalog.maintains_projections() {
        return dump_entities(catalog, extract).await;
    }

    let read = catalog.begin_dump().await?;
    let current = match read.overlay() {
        Some(overlay) => scan_current_entities_overlaid(read.handle(), overlay).await,
        None => scan_current_entities(read.handle()).await,
    };
    read.finish().await;
    Ok(current?.iter().filter_map(extract).collect())
}

/// Every `ducklake_schema` row, current and history.
#[doc(hidden)]
pub async fn dump_schemas(catalog: &Catalog) -> Result<Vec<SchemaValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Schema(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_table` row, current and history.
#[doc(hidden)]
pub async fn dump_tables(catalog: &Catalog) -> Result<Vec<TableValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Table(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_view` row, current and history.
#[doc(hidden)]
pub async fn dump_views(catalog: &Catalog) -> Result<Vec<ViewValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::View(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_macro` row, current and history, implementations and
/// their parameters embedded in `impl_id`/`column_id` order.
#[doc(hidden)]
pub async fn dump_macros(catalog: &Catalog) -> Result<Vec<MacroValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Macro(m) => Some(m.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_column_mapping` row with its embedded
/// `ducklake_name_mapping` rows in `column_id` order. Unversioned
/// (create-only, never mirrored), so this is always exactly the live
/// rows.
#[doc(hidden)]
pub async fn dump_mappings(catalog: &Catalog) -> Result<Vec<MappingValue>> {
    dump_current_entities(catalog, |r| match r {
        EntityRecord::Mapping(m) => Some(m.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_column` row, current and history.
#[doc(hidden)]
pub async fn dump_columns(catalog: &Catalog) -> Result<Vec<ColumnValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Column(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_data_file` row, current and history.
#[doc(hidden)]
pub async fn dump_data_files(catalog: &Catalog) -> Result<Vec<DataFileValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::File(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_delete_file` row, current and history.
#[doc(hidden)]
pub async fn dump_delete_files(catalog: &Catalog) -> Result<Vec<DeleteFileValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::DeleteFile(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_partition_info` row (with its embedded partition
/// columns), current and history.
#[doc(hidden)]
pub async fn dump_partition_info(catalog: &Catalog) -> Result<Vec<PartitionValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Partition(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_sort_info` row (with its embedded sort expressions),
/// current and history.
#[doc(hidden)]
pub async fn dump_sort_info(catalog: &Catalog) -> Result<Vec<SortValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Sort(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Scans `current` for one unversioned kind, serving and maintaining the
/// projection cache: rows come from the projection when its head matches,
/// and a scan on a miss installs them for the next call.
async fn dump_projected_current<T: Clone>(
    catalog: &Catalog,
    read: impl Fn(&ProjectionCache, u64) -> Option<Vec<T>>,
    install: impl Fn(&mut ProjectionCache, u64, Vec<T>),
    extract: impl Fn(EntityRecord) -> Option<T>,
) -> Result<Vec<T>> {
    let dump = catalog.begin_dump().await?;
    let head = dump.head_id();

    let cache_at = match (catalog.maintains_projections(), head) {
        (true, Some(head)) => {
            let cached = read(&projections_read(catalog), head);
            if let Some(rows) = cached {
                dump.finish().await;
                return Ok(rows);
            }
            Some(head)
        }
        _ => None,
    };

    let current = match dump.overlay() {
        Some(overlay) => scan_current_entities_overlaid(dump.handle(), overlay).await,
        None => scan_current_entities(dump.handle()).await,
    };
    dump.finish().await;
    let rows: Vec<T> = current?.into_iter().filter_map(extract).collect();
    if let Some(head) = cache_at {
        install(&mut projections_write(catalog), head, rows.clone());
    }
    Ok(rows)
}

/// Every `ducklake_table_stats` row. Unversioned (overwritten in place,
/// never mirrored to history), so this is always exactly the live rows.
/// Served from the maintained projection when its head matches; a fresh
/// scan installs it otherwise.
#[doc(hidden)]
pub async fn dump_table_stats(catalog: &Catalog) -> Result<Vec<TableStatsValue>> {
    dump_projected_current(
        catalog,
        ProjectionCache::table_stats_at,
        ProjectionCache::install_table_stats,
        |r| match r {
            EntityRecord::TableStats(v) => Some(v),
            _ => None,
        },
    )
    .await
}

/// Every `ducklake_table_column_stats` row. Unversioned, as
/// [`dump_table_stats`], and served from the maintained projection the
/// same way.
#[doc(hidden)]
pub async fn dump_table_column_stats(catalog: &Catalog) -> Result<Vec<TableColumnStatsValue>> {
    dump_projected_current(
        catalog,
        ProjectionCache::table_column_stats_at,
        ProjectionCache::install_table_column_stats,
        |r| match r {
            EntityRecord::TableColumnStats(v) => Some(v),
            _ => None,
        },
    )
    .await
}

/// Every `ducklake_file_column_stats` row. Unversioned, as
/// [`dump_table_stats`].
#[doc(hidden)]
pub async fn dump_file_column_stats(catalog: &Catalog) -> Result<Vec<FileColumnStatsValue>> {
    dump_current_entities(catalog, |r| match r {
        EntityRecord::FileColumnStats(v) => Some(v.clone()),
        _ => None,
    })
    .await
}

/// Every `ducklake_snapshot`/`ducklake_snapshot_changes` row (merged).
/// Snapshots are append-only and carry no begin/end lifecycle of their
/// own — this is the full history, not a current/history split. Served
/// from the maintained projection when its head matches; a fresh scan
/// installs it otherwise.
#[doc(hidden)]
pub async fn dump_snapshots(catalog: &Catalog) -> Result<Vec<SnapshotValue>> {
    let dump = catalog.begin_dump().await?;
    let head = dump.head_id();
    if let (true, Some(head)) = (catalog.maintains_projections(), head) {
        let cached = projections_read(catalog).snapshots_at(head);
        if let Some(rows) = cached {
            dump.finish().await;
            return Ok(rows);
        }
        let result = scan_snapshots(dump.handle()).await;
        dump.finish().await;
        let rows = result?;
        projections_write(catalog).install_snapshots(head, rows.clone());
        return Ok(rows);
    }
    let result = match dump.overlay() {
        Some(overlay) => scan_snapshots_overlaid(dump.handle(), overlay).await,
        None => scan_snapshots(dump.handle()).await,
    };
    dump.finish().await;
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
/// into snapshot records written before those records existed, plus a
/// floor row for any table whose oldest data file predates every row it
/// has left.
///
/// The last two are what a store carries out of the era when this
/// projection was derived from snapshot records alone. Expiry deletes
/// snapshots; DuckLake's own catalogs never delete a schema-version row
/// for a live table, and its compaction planner reads a data file's
/// schema version by joining `begin_snapshot` against these rows — an
/// uncovered file makes that join yield NULL and aborts the planner
/// before it does any work. The floor row is the repair a store whose
/// rows are already gone needs: it carries the oldest schema version
/// still known for the table (the current one when none is), which is
/// also what stock DuckLake resolves for a file that old once the
/// columns of its era have themselves expired.
#[doc(hidden)]
pub async fn dump_schema_versions(catalog: &Catalog) -> Result<Vec<SchemaVersionRow>> {
    let dump = catalog.begin_dump().await?;
    let records = match dump.overlay() {
        Some(overlay) => scan_schema_versions_overlaid(dump.handle(), overlay).await,
        None => scan_schema_versions(dump.handle()).await,
    };
    dump.finish().await;

    Ok(schema_version_rows_from(
        records?,
        &dump_snapshots(catalog).await?,
        &dump_data_files(catalog).await?,
    ))
}

/// The `ducklake_schema_versions` projection assembled from its three
/// inputs: the `schema_version` records, the snapshot records that still
/// fold the same rows in, and the data files the floor rows repair. Shared
/// by the committed dump and the transaction-aware one, which differ only
/// in whether those three are overlaid.
#[doc(hidden)]
#[must_use]
pub fn schema_version_rows_from(
    records: Vec<(u64, u64, u64)>,
    snapshots: &[SnapshotValue],
    data_files: &[DataFileValue],
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
    rows.extend(schema_version_floors(&rows, snapshots, data_files));

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

/// The floor rows [`dump_schema_versions`] adds for tables whose oldest
/// live data file sits below every row they have left: one row at that
/// file's `begin_snapshot`, carrying the table's oldest surviving schema
/// version, or the catalog's current one when the table has no row at
/// all. A table whose rows are intact — every store this binary has
/// written from the start — produces nothing here.
fn schema_version_floors(
    rows: &BTreeMap<(u64, u64), u64>,
    snapshots: &[SnapshotValue],
    data_files: &[DataFileValue],
) -> BTreeMap<(u64, u64), u64> {
    let mut oldest_file: BTreeMap<u64, u64> = BTreeMap::new();
    for file in data_files {
        oldest_file
            .entry(file.table_id)
            .and_modify(|oldest| *oldest = (*oldest).min(file.begin_snapshot))
            .or_insert(file.begin_snapshot);
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

/// Every `ducklake_files_scheduled_for_deletion` row. Live bookkeeping
/// with no temporal lifecycle: always exactly the rows awaiting physical
/// deletion.
#[doc(hidden)]
pub async fn dump_scheduled_deletions(catalog: &Catalog) -> Result<Vec<GcFileValue>> {
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
/// included — each row carries its lifecycle verbatim and DuckLake
/// filters in SQL.
#[doc(hidden)]
pub async fn dump_tags(catalog: &Catalog) -> Result<Vec<TagRow>> {
    Ok(tag_rows_from(
        dump_current_entities(catalog, |r| match r {
            EntityRecord::Tag(v) => Some(v.clone()),
            _ => None,
        })
        .await?,
    ))
}

/// The `ducklake_tag` rows carried by `containers`, flattened. Shared by
/// the committed dump and the transaction-aware one.
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
    /// The scope's name — `None` for a global option, else `"schema"` or
    /// `"table"`, matching the vocabulary DuckLake writes and reads.
    pub scope: Option<String>,
    /// The scoped object's id; `None` alongside a `None` scope.
    pub scope_id: Option<u64>,
}

/// Every stored `ducklake_metadata` row. Options carry no lifecycle —
/// they live outside the snapshot protocol, last write wins — so this is
/// simply what is set now.
#[doc(hidden)]
pub async fn dump_options(catalog: &Catalog) -> Result<Vec<OptionRow>> {
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
/// Shared by the committed dump and the transaction-aware one.
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
    // A stable order: the store's map iteration is not one callers should
    // depend on, and DuckLake reads these back by key.
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

/// The `ducklake_column_tag` rows carried by `columns`, flattened. Shared by
/// the committed dump and the transaction-aware one.
#[doc(hidden)]
#[must_use]
pub fn column_tag_rows_from(columns: &[ColumnValue]) -> Vec<ColumnTagRow> {
    let mut latest: std::collections::BTreeMap<(u64, u64), &ColumnValue> =
        std::collections::BTreeMap::new();
    for column in columns {
        let entry = latest
            .entry((column.table_id, column.column_id))
            .or_insert(column);
        // Later than the incumbent: live beats ended, higher end beats
        // lower.
        let newer = match (column.end_snapshot, entry.end_snapshot) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(a), Some(b)) => a > b,
        };
        if newer {
            *entry = column;
        }
    }

    latest
        .into_values()
        .flat_map(|column| {
            column.tags.iter().map(|t| ColumnTagRow {
                table_id: column.table_id,
                column_id: column.column_id,
                begin_snapshot: t.begin_snapshot,
                end_snapshot: t.end_snapshot,
                key: t.key.clone(),
                value: t.value.clone(),
            })
        })
        .collect()
}

/// Every `ducklake_column_tag` row. Entries are authoritative on each
/// column's latest record (a version transition carries them forward),
/// so rows are emitted from that record only — emitting from every
/// version would duplicate them.
#[doc(hidden)]
pub async fn dump_column_tags(catalog: &Catalog) -> Result<Vec<ColumnTagRow>> {
    Ok(column_tag_rows_from(&dump_columns(catalog).await?))
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

/// The `MacroImpl` rows carried by `parents`, flattened. Shared by the
/// committed dump and the transaction-aware one, so a staged parent's rows
/// and a committed one's are shaped by the same code.
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
pub async fn dump_macro_impl_rows(catalog: &Catalog) -> Result<Vec<MacroImplRow>> {
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

/// The `MacroParameter` rows carried by `parents`, flattened. Shared by the
/// committed dump and the transaction-aware one, so a staged parent's rows
/// and a committed one's are shaped by the same code.
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
pub async fn dump_macro_parameter_rows(catalog: &Catalog) -> Result<Vec<MacroParameterRow>> {
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

/// The `NameMapping` rows carried by `parents`, flattened. Shared by the
/// committed dump and the transaction-aware one, so a staged parent's rows
/// and a committed one's are shaped by the same code.
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
pub async fn dump_name_mapping_rows(catalog: &Catalog) -> Result<Vec<NameMappingRow>> {
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

/// The `PartitionColumn` rows carried by `parents`, flattened. Shared by the
/// committed dump and the transaction-aware one, so a staged parent's rows
/// and a committed one's are shaped by the same code.
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
pub async fn dump_partition_column_rows(catalog: &Catalog) -> Result<Vec<PartitionColumnRow>> {
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

/// The `FilePartitionValue` rows carried by `parents`, flattened. Shared by the
/// committed dump and the transaction-aware one, so a staged parent's rows
/// and a committed one's are shaped by the same code.
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
    catalog: &Catalog,
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

/// The `SortExpression` rows carried by `parents`, flattened. Shared by the
/// committed dump and the transaction-aware one, so a staged parent's rows
/// and a committed one's are shaped by the same code.
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
pub async fn dump_sort_expression_rows(catalog: &Catalog) -> Result<Vec<SortExpressionRow>> {
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
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
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
    /// its open-time format check has already passed.
    async fn plant_migration_marker(catalog: &Catalog) {
        use slatedb::IsolationLevel;

        use crate::{
            store::{
                key::{Key, SysKey},
                proto::MigrationValue,
                value::encode_value,
            },
            transaction::commit,
        };

        catalog
            .with_folder_writer(async |db| {
                let tx = db
                    .begin(IsolationLevel::Snapshot)
                    .await
                    .map_err(crate::error::Error::from)?;
                tx.put(
                    Key::Sys(SysKey::Migration).encode(),
                    encode_value(&MigrationValue {
                        from_format: 1,
                        to_format: 2,
                        cursor: Vec::new(),
                    }),
                )
                .map_err(crate::error::Error::from)?;
                commit::commit_durably(db, tx)
                    .await
                    .map_err(crate::error::Error::from)
            })
            .await
            .unwrap();
    }

    fn refuses<T>(outcome: &Result<T>) -> bool {
        matches!(outcome, Err(crate::error::Error::Migration(_)))
    }

    /// A migration is moving keys as these functions scan, so each must be
    /// unavailable rather than serve a catalog with a hole in it. The
    /// `dump_*` and inline seams scan the store directly instead of
    /// through a materialization, so the read-session gate is the only
    /// thing standing between them and a silently shrinking view.
    #[tokio::test]
    async fn a_planted_marker_refuses_every_read_seam() {
        let catalog = seed().await;
        plant_migration_marker(&catalog).await;

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
    /// schema-version records too — the two DELETE streams DuckLake's
    /// expiry and dead-table cleanup issue, staged the way DuckLake stages
    /// them. Dropping the records models a store written before they
    /// existed.
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
        use crate::transaction::staged::{Cell, RowOperation, TableKind};

        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        let mut tx = catalog.begin_staged(None, String::new()).await.unwrap();
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

    /// On a slot-backed attach, a staged commit lands only in the log until a
    /// folder applies it. The raw dumps must read it through the tail overlay,
    /// or DuckLake's conflict matrix would miss a committed winner.
    #[tokio::test]
    async fn dumps_reflect_an_unfolded_tail_on_a_slot_backed_attach() {
        use crate::transaction::staged::{Cell, RowOperation, TableKind};

        let options = CatalogOptions::default();
        let catalog = Catalog::open(Arc::new(InMemory::new()), options)
            .await
            .unwrap();

        let mut tx = crate::ffi_support::staged::staged_begin(&catalog, None, String::new())
            .await
            .unwrap();
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: vec![
                Cell::U64(1),
                Cell::I64(1),
                Cell::U64(1),
                Cell::U64(2),
                Cell::U64(0),
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: vec![
                Cell::U64(1),
                Cell::Str(r#"created_schema:"sales""#.into()),
                Cell::Null,
                Cell::Null,
                Cell::Null,
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Schema,
            cells: vec![
                Cell::U64(1),
                Cell::Str("uuid-1".into()),
                Cell::U64(1),
                Cell::Null,
                Cell::Str("sales".into()),
                Cell::Str("sales/".into()),
                Cell::Bool(true),
            ],
        });
        tx.commit().await.unwrap();

        let snapshots = dump_snapshots(&catalog).await.unwrap();
        assert_eq!(
            snapshots.iter().map(|s| s.snapshot_id).collect::<Vec<_>>(),
            vec![0, 1]
        );
        let schemas = dump_schemas(&catalog).await.unwrap();
        assert!(schemas.iter().any(|s| s.schema_name == "sales"));
        let deletions = dump_scheduled_deletions(&catalog).await.unwrap();
        assert!(deletions.is_empty());
    }

    /// Opens a slot-backed (multi-writer) catalog: its commits land in the
    /// log and are served through the unfolded tail until a folder applies
    /// them.
    async fn open_slots() -> Catalog {
        let options = CatalogOptions::default();
        Catalog::open(Arc::new(InMemory::new()), options)
            .await
            .unwrap()
    }

    /// A tail tombstone hides a folded record: a maintenance commit that
    /// expires the bootstrap snapshot 0 — already folded into the store —
    /// removes it from the dump, exercising the overlay's delete (`None`)
    /// branch that no other slot-backed test covers.
    #[tokio::test]
    async fn a_tail_tombstone_hides_a_folded_snapshot_on_a_slot_backed_attach() {
        use crate::transaction::staged::{Cell, RowOperation, TableKind};

        let catalog = open_slots().await;

        // Advance head off the folded bootstrap snapshot 0 by minting 1.
        let mut mint = crate::ffi_support::staged::staged_begin(&catalog, None, String::new())
            .await
            .unwrap();
        mint.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: vec![
                Cell::U64(1),
                Cell::I64(1),
                Cell::U64(1),
                Cell::U64(2),
                Cell::U64(0),
            ],
        });
        mint.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: vec![
                Cell::U64(1),
                Cell::Str(r#"created_schema:"sales""#.into()),
                Cell::Null,
                Cell::Null,
                Cell::Null,
            ],
        });
        mint.stage(RowOperation::Insert {
            table: TableKind::Schema,
            cells: vec![
                Cell::U64(1),
                Cell::Str("uuid-1".into()),
                Cell::U64(1),
                Cell::Null,
                Cell::Str("sales".into()),
                Cell::Str("sales/".into()),
                Cell::Bool(true),
            ],
        });
        mint.commit().await.unwrap();

        // Head-preserving maintenance: expire the now-non-head snapshot 0.
        let mut expire = crate::ffi_support::staged::staged_begin(&catalog, None, String::new())
            .await
            .unwrap();
        expire.stage(RowOperation::Delete {
            table: TableKind::Snapshot,
            cells: vec![Cell::U64(0)],
        });
        expire.stage(RowOperation::Delete {
            table: TableKind::SnapshotChanges,
            cells: vec![Cell::U64(0)],
        });
        expire.commit().await.unwrap();

        let snapshots = dump_snapshots(&catalog).await.unwrap();
        assert_eq!(
            snapshots.iter().map(|s| s.snapshot_id).collect::<Vec<_>>(),
            vec![1],
            "the tail tombstone hides the folded snapshot 0"
        );
    }

    /// A rename committed through a slot ends the old table version and writes
    /// a new one. The overlaid history scan must surface the ended version —
    /// real content for `scan_history_entities_overlaid`, which the prior
    /// slot-backed dump test left empty.
    #[tokio::test]
    async fn history_scan_reflects_an_ended_table_version_on_a_slot_backed_attach() {
        use crate::transaction::staged::{Cell, RowOperation, TableKind};

        let catalog = open_slots().await;

        // Create table t_old (id 1) in the bootstrap schema `main` (id 0).
        let mut create = crate::ffi_support::staged::staged_begin(&catalog, None, String::new())
            .await
            .unwrap();
        create.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: vec![
                Cell::U64(1),
                Cell::Str("uuid-t1".into()),
                Cell::U64(1),
                Cell::Null,
                Cell::U64(0),
                Cell::Str("t_old".into()),
                Cell::Str("t_old/".into()),
                Cell::Bool(true),
            ],
        });
        create.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: vec![
                Cell::U64(1),
                Cell::I64(1),
                Cell::U64(1),
                Cell::U64(2),
                Cell::U64(0),
            ],
        });
        create.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: vec![
                Cell::U64(1),
                Cell::Str(r#"created_table:"main"."t_old""#.into()),
                Cell::Null,
                Cell::Null,
                Cell::Null,
            ],
        });
        create.commit().await.unwrap();

        // Rename: end the old version at snapshot 2, insert t_new.
        let mut rename = crate::ffi_support::staged::staged_begin(&catalog, None, String::new())
            .await
            .unwrap();
        rename.stage(RowOperation::UpdateSetEnd {
            table: TableKind::Table,
            cells: vec![Cell::U64(1), Cell::U64(2)],
        });
        rename.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: vec![
                Cell::U64(1),
                Cell::Str("uuid-t1".into()),
                Cell::U64(2),
                Cell::Null,
                Cell::U64(0),
                Cell::Str("t_new".into()),
                Cell::Str("t_new/".into()),
                Cell::Bool(true),
            ],
        });
        rename.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: vec![
                Cell::U64(2),
                Cell::I64(1),
                Cell::U64(1),
                Cell::U64(2),
                Cell::U64(0),
            ],
        });
        rename.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: vec![
                Cell::U64(2),
                Cell::Str("altered_table:1".into()),
                Cell::Null,
                Cell::Null,
                Cell::Null,
            ],
        });
        rename.commit().await.unwrap();

        let tables = dump_tables(&catalog).await.unwrap();
        let ended: Vec<_> = tables
            .iter()
            .filter(|t| t.end_snapshot == Some(2))
            .collect();
        assert_eq!(
            ended.len(),
            1,
            "the ended old version is in overlaid history: {tables:?}"
        );
        assert_eq!(ended[0].table_name, "t_old");
        assert!(
            tables
                .iter()
                .any(|t| t.table_name == "t_new" && t.end_snapshot.is_none()),
            "the renamed live version is in overlaid current: {tables:?}"
        );
    }

    /// `dump_projected_current` (behind `dump_table_stats`) must reflect the
    /// unfolded tail on a slot-backed attach, where no projection cache is
    /// maintained and the read falls to an overlaid `current` scan.
    #[tokio::test]
    async fn dump_projected_current_reflects_unfolded_stats_on_a_slot_backed_attach() {
        use crate::transaction::staged::{Cell, RowOperation, TableKind};

        let catalog = open_slots().await;

        let mut tx = crate::ffi_support::staged::staged_begin(&catalog, None, String::new())
            .await
            .unwrap();
        tx.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: vec![
                Cell::U64(1),
                Cell::Str("uuid-t1".into()),
                Cell::U64(1),
                Cell::Null,
                Cell::U64(0),
                Cell::Str("t".into()),
                Cell::Str("t/".into()),
                Cell::Bool(true),
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Column,
            cells: vec![
                Cell::U64(1),
                Cell::U64(0),
                Cell::Null,
                Cell::U64(1),
                Cell::U64(0),
                Cell::Str("a".into()),
                Cell::Str("BIGINT".into()),
                Cell::Null,
                Cell::Null,
                Cell::Bool(true),
                Cell::Null,
                Cell::Null,
                Cell::Null,
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::TableStats,
            cells: vec![Cell::U64(1), Cell::U64(20), Cell::U64(20), Cell::U64(2048)],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: vec![
                Cell::U64(1),
                Cell::I64(1),
                Cell::U64(1),
                Cell::U64(2),
                Cell::U64(0),
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: vec![
                Cell::U64(1),
                Cell::Str(r#"created_table:"main"."t""#.into()),
                Cell::Null,
                Cell::Null,
                Cell::Null,
            ],
        });
        tx.commit().await.unwrap();

        let stats = dump_table_stats(&catalog).await.unwrap();
        assert_eq!(
            stats.len(),
            1,
            "the unfolded table-stats row is served: {stats:?}"
        );
        assert_eq!(stats[0].table_id, 1);
        assert_eq!(stats[0].record_count, 20);
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
