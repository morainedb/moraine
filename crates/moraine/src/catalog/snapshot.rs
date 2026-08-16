//! An immutable, materialized catalog view. Built once from a consistent
//! store scan; every accessor is an in-memory lookup afterwards.

use std::collections::{BTreeMap, HashMap};

use prost::Message as _;

use crate::{
    catalog::types::{
        ColumnId, ColumnInfo, ColumnStats, DataFileId, DataFileInfo, DeleteFileId, DeleteFileInfo,
        IndexId, IndexInfo, IndexState, MacroId, MacroImplementationDef, MacroInfo,
        MacroParameterDef, MappingId, MappingInfo, NameMappingDef, OptionScope, PartitionColumnDef,
        PartitionId, PartitionSpec, ScheduledDeletion, SchemaId, SchemaInfo, SnapshotId,
        SnapshotInfo, SortId, SortKeyDef, SortSpec, TableId, TableInfo, TableStats, TagEntry,
        Timestamp, ViewId, ViewInfo,
    },
    error::{Error, Result},
    store::{
        proto::{
            ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue, GcFileValue,
            IndexValue, MacroValue, MappingValue, OptionScopeValue, PartitionValue, SchemaValue,
            SnapshotValue, SortValue, TableColumnStatsValue, TableStatsValue, TableValue, TagValue,
            ViewValue,
        },
        read::EntityRecord,
    },
};

/// An immutable catalog view at one snapshot.
///
/// Reads issue no store I/O after the view is built — a `CatalogSnapshot`
/// is a value, not a cursor. The default value is an empty view at
/// snapshot 0.
#[derive(Debug, Clone, Default)]
pub struct CatalogSnapshot {
    pub(crate) snapshot: SnapshotValue,
    /// The head record's batch count when this view was built, zero for a
    /// time-travel view. A maintenance batch changes committed state
    /// without minting a snapshot, so the snapshot id alone does not say
    /// which store state a head view stands at; this does.
    pub(crate) batch_seq: u64,
    pub(crate) schemas: BTreeMap<u64, SchemaValue>,
    pub(crate) tables: BTreeMap<u64, TableValue>,
    pub(crate) views: BTreeMap<u64, ViewValue>,
    pub(crate) macros: BTreeMap<u64, MacroValue>,
    pub(crate) columns: BTreeMap<u64, BTreeMap<u64, ColumnValue>>,
    pub(crate) schema_names: HashMap<String, u64>,
    pub(crate) table_names: ScopedNames,
    pub(crate) view_names: ScopedNames,
    pub(crate) macro_names: ScopedNames,
    pub(crate) data_files: BTreeMap<u64, BTreeMap<u64, DataFileValue>>,
    pub(crate) delete_files: BTreeMap<u64, BTreeMap<u64, DeleteFileValue>>,
    pub(crate) partitions: BTreeMap<u64, BTreeMap<u64, PartitionValue>>,
    pub(crate) sorts: BTreeMap<u64, BTreeMap<u64, SortValue>>,
    pub(crate) mappings: BTreeMap<u64, BTreeMap<u64, MappingValue>>,
    pub(crate) indexes: BTreeMap<u64, BTreeMap<u64, IndexValue>>,
    pub(crate) index_names: ScopedNames,
    pub(crate) table_stats: BTreeMap<u64, TableStatsValue>,
    pub(crate) table_column_stats: BTreeMap<u64, BTreeMap<u64, TableColumnStatsValue>>,
    pub(crate) file_column_stats: BTreeMap<u64, BTreeMap<(u64, u64), FileColumnStatsValue>>,
    pub(crate) options: BTreeMap<(u64, u64), OptionScopeValue>,
    pub(crate) tags: BTreeMap<u64, TagValue>,
    pub(crate) gc_files: BTreeMap<u64, GcFileValue>,
}

/// A per-scope name index: schema or table id, then name, to entity id.
pub(crate) type ScopedNames = HashMap<u64, HashMap<String, u64>>;

/// The id `name` resolves to inside `scope`, if any.
fn scoped_name(names: &ScopedNames, scope: u64, name: &str) -> Option<u64> {
    names.get(&scope)?.get(name).copied()
}

fn remove_scoped_name(names: &mut ScopedNames, scope: u64, name: &str) {
    if let Some(per_scope) = names.get_mut(&scope) {
        per_scope.remove(name);
        if per_scope.is_empty() {
            names.remove(&scope);
        }
    }
}

/// Inserts a name-scoped entity, first unlinking the replaced version's
/// name so a rename never leaves a stale name entry.
fn put_named<V>(
    values: &mut BTreeMap<u64, V>,
    names: &mut ScopedNames,
    value: V,
    identity: impl for<'v> Fn(&'v V) -> (u64, u64, &'v str),
) {
    let (id, scope, name) = identity(&value);
    let (scope, name) = (scope, name.to_owned());
    if let Some(old) = values.get(&id) {
        let (_, old_scope, old_name) = identity(old);
        let (old_scope, old_name) = (old_scope, old_name.to_owned());
        remove_scoped_name(names, old_scope, &old_name);
    }
    names.entry(scope).or_default().insert(name, id);
    values.insert(id, value);
}

/// Removes a name-scoped entity and its name entry, if present.
fn delete_named<V>(
    values: &mut BTreeMap<u64, V>,
    names: &mut ScopedNames,
    id: u64,
    identity: impl for<'v> Fn(&'v V) -> (u64, u64, &'v str),
) {
    if let Some(old) = values.remove(&id) {
        let (_, scope, name) = identity(&old);
        remove_scoped_name(names, scope, name);
    }
}

fn table_identity(value: &TableValue) -> (u64, u64, &str) {
    (value.table_id, value.schema_id, &value.table_name)
}

fn view_identity(value: &ViewValue) -> (u64, u64, &str) {
    (value.view_id, value.schema_id, &value.view_name)
}

fn macro_identity(value: &MacroValue) -> (u64, u64, &str) {
    (value.macro_id, value.schema_id, &value.macro_name)
}

fn index_identity(value: &IndexValue) -> (u64, u64, &str) {
    (value.index_id, value.table_id, &value.index_name)
}

/// Inserts into a table-scoped nested map.
fn put_nested<K: Ord, V>(map: &mut BTreeMap<u64, BTreeMap<K, V>>, table_id: u64, key: K, value: V) {
    map.entry(table_id).or_default().insert(key, value);
}

/// Removes from a table-scoped nested map, if present.
fn remove_nested<K: Ord, V>(map: &mut BTreeMap<u64, BTreeMap<K, V>>, table_id: u64, key: &K) {
    if let Some(per_table) = map.get_mut(&table_id) {
        per_table.remove(key);
    }
}

/// Encoded bytes of one map's values, keys included.
fn flat_bytes<K, V: prost::Message>(map: &BTreeMap<K, V>) -> u64 {
    map.values()
        .map(|value| u64::try_from(value.encoded_len()).unwrap_or(u64::MAX))
        .sum::<u64>()
        + u64::try_from(map.len() * std::mem::size_of::<K>()).unwrap_or(0)
}

/// As [`flat_bytes`], for a map of maps.
fn nested_bytes<K1, K2, V: prost::Message>(map: &BTreeMap<K1, BTreeMap<K2, V>>) -> u64 {
    map.values().map(flat_bytes).sum::<u64>()
        + u64::try_from(map.len() * std::mem::size_of::<K1>()).unwrap_or(0)
}

impl CatalogSnapshot {
    /// Roughly what this view holds, in bytes.
    ///
    /// Sums the encoded length of every entity it carries. The name indexes
    /// are derived and small beside the file maps, and are not counted; an
    /// encoded length also understates its decoded form. Read to report a
    /// handle's footprint, never to decide an eviction.
    pub(crate) fn estimated_bytes(&self) -> u64 {
        [
            flat_bytes(&self.schemas),
            flat_bytes(&self.tables),
            flat_bytes(&self.views),
            flat_bytes(&self.macros),
            flat_bytes(&self.table_stats),
            flat_bytes(&self.tags),
            flat_bytes(&self.gc_files),
            flat_bytes(&self.options),
            nested_bytes(&self.columns),
            nested_bytes(&self.data_files),
            nested_bytes(&self.delete_files),
            nested_bytes(&self.partitions),
            nested_bytes(&self.sorts),
            nested_bytes(&self.mappings),
            nested_bytes(&self.indexes),
            nested_bytes(&self.table_column_stats),
            nested_bytes(&self.file_column_stats),
            u64::try_from(self.snapshot.encoded_len()).unwrap_or(u64::MAX),
        ]
        .into_iter()
        .fold(0_u64, u64::saturating_add)
    }

    /// Builds the view. With `at: None` every `current` record is live and
    /// `history` is unused (allocation reads the table's persisted counter,
    /// not history); with `at: Some(s)` a record is included iff
    /// `begin_snapshot <= s` and it had not ended by `s` (`current` records
    /// never have; `history` records carry their end).
    pub(crate) fn build(
        snapshot: SnapshotValue,
        current: &[EntityRecord],
        history: &[EntityRecord],
        at: Option<u64>,
    ) -> Self {
        let mut view = Self {
            snapshot,
            ..Self::default()
        };

        let included = |begin: u64, end: Option<u64>| match at {
            None => end.is_none(),
            Some(s) => begin <= s && end.is_none_or(|e| e > s),
        };
        for record in current.iter().chain(history).cloned() {
            // Unversioned kinds (no lifecycle) are live at any time-travel
            // target: mappings are immutable, tag entries filter at read,
            // stats/options/gc rows are current-state bookkeeping.
            if record
                .lifecycle()
                .is_some_and(|(begin, end)| !included(begin, end))
            {
                continue;
            }
            view.put_record(record);
        }
        view
    }

    /// How many `current` records this view holds — the size a full
    /// rematerialization would have to read and decode, and so the scale an
    /// incremental refresh's churn is weighed against.
    pub(crate) fn live_entity_count(&self) -> usize {
        fn nested<K, V>(map: &BTreeMap<u64, BTreeMap<K, V>>) -> usize {
            map.values().map(BTreeMap::len).sum()
        }

        self.schemas.len()
            + self.tables.len()
            + self.views.len()
            + self.macros.len()
            + nested(&self.columns)
            + nested(&self.data_files)
            + nested(&self.delete_files)
            + nested(&self.partitions)
            + nested(&self.sorts)
            + nested(&self.mappings)
            + nested(&self.indexes)
            + self.table_stats.len()
            + nested(&self.table_column_stats)
            + nested(&self.file_column_stats)
            + self.options.len()
            + self.tags.len()
            + self.gc_files.len()
    }

    /// Inserts one decoded record into the maps it belongs to, keeping the
    /// name indexes coherent. Shared by [`build`](Self::build) and the
    /// commit-time fold that folds a batch forward without rescanning.
    pub(crate) fn put_record(&mut self, record: EntityRecord) {
        match record {
            EntityRecord::Schema(s) => self.put_schema(s),
            EntityRecord::Table(t) => self.put_table(t),
            EntityRecord::Column(c) => self.put_column(c),
            EntityRecord::File(f) => self.put_data_file(f),
            EntityRecord::DeleteFile(d) => self.put_delete_file(d),
            EntityRecord::View(v) => self.put_view(v),
            EntityRecord::Partition(p) => self.put_partition(p),
            EntityRecord::Sort(s) => self.put_sort(s),
            EntityRecord::Macro(m) => self.put_macro(m),
            EntityRecord::Index(i) => self.put_index(i),
            EntityRecord::Mapping(m) => self.put_mapping(m),
            EntityRecord::FileColumnStats(fcs) => self.put_file_column_stats(fcs),
            EntityRecord::TableStats(ts) => self.put_table_stats(ts),
            EntityRecord::TableColumnStats(tcs) => self.put_table_column_stats(tcs),
            EntityRecord::Option {
                scope_kind,
                scope_id,
                value,
            } => self.set_option_record((scope_kind, scope_id), value),
            EntityRecord::Tag(t) => self.put_tag(t),
            EntityRecord::GcFile(g) => self.put_gc_file(g),
        }
    }

    /// Snapshot identity and metadata of this view.
    #[must_use]
    pub fn current_snapshot(&self) -> SnapshotInfo {
        SnapshotInfo {
            id: SnapshotId::new(self.snapshot.snapshot_id),
            time: Timestamp::from_micros(self.snapshot.snapshot_time_micros),
            schema_version: self.snapshot.schema_version,
        }
    }

    /// All live schemas, ordered by id.
    #[must_use]
    pub fn schemas(&self) -> Vec<SchemaInfo> {
        self.schemas.values().map(schema_info).collect()
    }

    /// One schema by name.
    #[must_use]
    pub fn schema_by_name(&self, name: &str) -> Option<SchemaInfo> {
        self.schema_names
            .get(name)
            .and_then(|id| self.schemas.get(id))
            .map(schema_info)
    }

    /// One schema by id.
    #[must_use]
    pub fn schema_by_id(&self, id: SchemaId) -> Option<SchemaInfo> {
        self.schemas.get(&id.get()).map(schema_info)
    }

    /// The live tables of a schema, ordered by id.
    #[must_use]
    pub fn tables_in(&self, schema: SchemaId) -> Vec<TableInfo> {
        self.tables
            .values()
            .filter(|t| t.schema_id == schema.get())
            .map(table_info)
            .collect()
    }

    /// One table by name within a schema.
    #[must_use]
    pub fn table_by_name(&self, schema: SchemaId, name: &str) -> Option<TableInfo> {
        scoped_name(&self.table_names, schema.get(), name)
            .and_then(|id| self.tables.get(&id))
            .map(table_info)
    }

    /// One table by id.
    #[must_use]
    pub fn table_by_id(&self, id: TableId) -> Option<TableInfo> {
        self.tables.get(&id.get()).map(table_info)
    }

    /// The live columns of a table, ordered by position.
    #[must_use]
    pub fn columns_of(&self, table: TableId) -> Vec<ColumnInfo> {
        let Some(columns) = self.columns.get(&table.get()) else {
            return Vec::new();
        };

        let mut infos: Vec<ColumnInfo> = columns
            .values()
            .map(|c| ColumnInfo {
                id: ColumnId::new(c.column_id),
                name: c.column_name.clone(),
                column_type: c.column_type.clone(),
                nulls_allowed: c.nulls_allowed,
                default_value: c.default_value.clone(),
                position: c.column_order,
                parent_column: c.parent_column.map(ColumnId::new),
            })
            .collect();
        infos.sort_by_key(|c| c.position);

        infos
    }

    /// The schema's live views, ordered by id.
    #[must_use]
    pub fn views_in(&self, schema: SchemaId) -> Vec<ViewInfo> {
        self.views
            .values()
            .filter(|v| v.schema_id == schema.get())
            .map(view_info)
            .collect()
    }

    /// One view by name within a schema.
    #[must_use]
    pub fn view_by_name(&self, schema: SchemaId, name: &str) -> Option<ViewInfo> {
        scoped_name(&self.view_names, schema.get(), name)
            .and_then(|id| self.views.get(&id))
            .map(view_info)
    }

    /// One view by id.
    #[must_use]
    pub fn view_by_id(&self, id: ViewId) -> Option<ViewInfo> {
        self.views.get(&id.get()).map(view_info)
    }

    /// The live macros of a schema.
    #[must_use]
    pub fn macros_in(&self, schema: SchemaId) -> Vec<MacroInfo> {
        self.macros
            .values()
            .filter(|m| m.schema_id == schema.get())
            .map(macro_info)
            .collect()
    }

    /// One macro by name within a schema. Macros have their own name
    /// namespace, separate from tables and views.
    #[must_use]
    pub fn macro_by_name(&self, schema: SchemaId, name: &str) -> Option<MacroInfo> {
        scoped_name(&self.macro_names, schema.get(), name)
            .and_then(|id| self.macros.get(&id))
            .map(macro_info)
    }

    /// One macro by id.
    #[must_use]
    pub fn macro_by_id(&self, id: MacroId) -> Option<MacroInfo> {
        self.macros.get(&id.get()).map(macro_info)
    }

    /// A table's column mappings in `mapping_id` order. Mappings are
    /// unversioned: any time-travel view serves the full set.
    #[must_use]
    pub fn mappings_of(&self, table: TableId) -> Vec<MappingInfo> {
        self.mappings
            .get(&table.get())
            .map(|per_table| per_table.values().map(mapping_info).collect())
            .unwrap_or_default()
    }

    /// The table's partition spec live at this view's snapshot, or `None`
    /// when the table is unpartitioned. A table has at most one live spec;
    /// a store carrying more (only reachable through the staged path, which
    /// stores DuckLake's rows verbatim) yields the lowest-numbered, the one
    /// a scan in key order meets first.
    #[must_use]
    pub fn partitioning_of(&self, table: TableId) -> Option<PartitionSpec> {
        self.partitions
            .get(&table.get())
            .and_then(|per_table| per_table.values().next())
            .map(partition_spec)
    }

    /// The table's sort spec live at this view's snapshot, or `None` when
    /// the table is unsorted. A table has at most one live spec; a store
    /// carrying more (only reachable through the staged path, which stores
    /// DuckLake's rows verbatim) yields the lowest-numbered, the one a scan
    /// in key order meets first.
    #[must_use]
    pub fn sorting_of(&self, table: TableId) -> Option<SortSpec> {
        self.sorts
            .get(&table.get())
            .and_then(|per_table| per_table.values().next())
            .map(sort_spec)
    }

    /// A table's live equality indexes, ordered by id.
    #[must_use]
    pub fn indexes_of(&self, table: TableId) -> Vec<IndexInfo> {
        self.indexes
            .get(&table.get())
            .map(|per_table| per_table.values().map(index_info).collect())
            .unwrap_or_default()
    }

    /// One equality index by name within a table.
    #[must_use]
    pub fn index_by_name(&self, table: TableId, name: &str) -> Option<IndexInfo> {
        scoped_name(&self.index_names, table.get(), name)
            .and_then(|id| self.indexes.get(&table.get()).and_then(|m| m.get(&id)))
            .map(index_info)
    }

    /// The table's data directory (`<schema path><table path>`), itself
    /// relative to `DATA_PATH` — the prefix a relative data-file path
    /// resolves under.
    pub(crate) fn table_data_prefix(&self, table: TableId) -> Result<String> {
        let table_value = self
            .tables
            .get(&table.get())
            .ok_or_else(|| Error::NotFound(format!("table {table}")))?;
        let schema_value = self.schemas.get(&table_value.schema_id).ok_or_else(|| {
            Error::Corruption(format!("table {table} references a missing schema"))
        })?;
        Ok(format!("{}{}", schema_value.path, table_value.path))
    }

    /// One equality index by id within a table.
    #[must_use]
    pub fn index_by_id(&self, table: TableId, id: IndexId) -> Option<IndexInfo> {
        self.indexes
            .get(&table.get())
            .and_then(|per_table| per_table.get(&id.get()))
            .map(index_info)
    }

    pub(crate) fn put_index(&mut self, value: IndexValue) {
        let per_table = self.indexes.entry(value.table_id).or_default();
        if let Some(old) = per_table.get(&value.index_id) {
            let (_, old_scope, old_name) = index_identity(old);
            let (old_scope, old_name) = (old_scope, old_name.to_owned());
            remove_scoped_name(&mut self.index_names, old_scope, &old_name);
        }
        self.index_names
            .entry(value.table_id)
            .or_default()
            .insert(value.index_name.clone(), value.index_id);
        per_table.insert(value.index_id, value);
    }

    pub(crate) fn delete_index(&mut self, table_id: u64, index_id: u64) {
        if let Some(per_table) = self.indexes.get_mut(&table_id)
            && let Some(old) = per_table.remove(&index_id)
        {
            let (_, scope, name) = index_identity(&old);
            remove_scoped_name(&mut self.index_names, scope, name);
        }
    }

    /// A resolved option value: table falls back to its schema, then
    /// global; schema falls back to global. Options are unversioned: a
    /// time-traveled snapshot resolves current values.
    #[must_use]
    pub fn option(&self, scope: OptionScope, key: &str) -> Option<String> {
        let mut candidates = vec![scope.key_components()];
        match scope {
            OptionScope::Table(t) => {
                if let Some(table) = self.tables.get(&t.get()) {
                    candidates
                        .push(OptionScope::Schema(SchemaId::new(table.schema_id)).key_components());
                }
                candidates.push(OptionScope::Global.key_components());
            }
            OptionScope::Schema(_) => candidates.push(OptionScope::Global.key_components()),
            OptionScope::Global => {}
        }
        candidates.into_iter().find_map(|c| {
            self.options
                .get(&c)
                .and_then(|v| v.options.get(key).cloned())
        })
    }

    /// The lake's data root, as recorded in the global `data_path` metadata
    /// option when the store was created. `None` when no data path was given
    /// at bootstrap. DuckLake's `DATA_PATH`; index maintenance resolves data
    /// files against it.
    #[must_use]
    pub fn data_path(&self) -> Option<String> {
        self.option(OptionScope::Global, "data_path")
    }

    /// Every tag row on `object_id` (a schema/table/view id), ended
    /// entries included — each entry carries its own begin/end, so
    /// callers filter by lifecycle where it matters.
    #[must_use]
    pub fn tags_of(&self, object_id: u64) -> Vec<TagEntry> {
        self.tags
            .get(&object_id)
            .map(|container| {
                container
                    .entries
                    .iter()
                    .map(|e| TagEntry {
                        begin_snapshot: e.begin_snapshot,
                        end_snapshot: e.end_snapshot,
                        key: e.key.clone(),
                        value: e.value.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn put_tag(&mut self, value: TagValue) {
        self.tags.insert(value.object_id, value);
    }

    /// Files scheduled for physical deletion, ascending by file id.
    #[must_use]
    pub fn scheduled_deletions(&self) -> Vec<ScheduledDeletion> {
        self.gc_files
            .values()
            .map(|g| ScheduledDeletion {
                data_file_id: g.data_file_id,
                path: g.path.clone(),
                path_is_relative: g.path_is_relative,
                schedule_start: Timestamp::from_micros(g.schedule_start_micros),
            })
            .collect()
    }

    pub(crate) fn put_gc_file(&mut self, value: GcFileValue) {
        self.gc_files.insert(value.data_file_id, value);
    }

    pub(crate) fn put_schema(&mut self, value: SchemaValue) {
        if let Some(old) = self.schemas.get(&value.schema_id) {
            self.schema_names.remove(&old.schema_name);
        }
        self.schema_names
            .insert(value.schema_name.clone(), value.schema_id);
        self.schemas.insert(value.schema_id, value);
    }

    pub(crate) fn delete_schema(&mut self, schema_id: u64) {
        if let Some(old) = self.schemas.remove(&schema_id) {
            self.schema_names.remove(&old.schema_name);
        }
        self.remove_option_record(OptionScope::Schema(SchemaId::new(schema_id)).key_components());
    }

    pub(crate) fn put_table(&mut self, value: TableValue) {
        put_named(
            &mut self.tables,
            &mut self.table_names,
            value,
            table_identity,
        );
    }

    pub(crate) fn delete_table(&mut self, table_id: u64) {
        delete_named(
            &mut self.tables,
            &mut self.table_names,
            table_id,
            table_identity,
        );
        self.columns.remove(&table_id);
        self.data_files.remove(&table_id);
        self.delete_files.remove(&table_id);
        self.partitions.remove(&table_id);
        self.sorts.remove(&table_id);
        self.indexes.remove(&table_id);
        self.index_names.remove(&table_id);
        self.table_stats.remove(&table_id);
        self.table_column_stats.remove(&table_id);
        self.remove_option_record(OptionScope::Table(TableId::new(table_id)).key_components());
        // file_column_stats is kept: per-file stats outlive the file's
        // live version until its history is garbage-collected. mappings
        // are kept for the same reason: historical file reads still
        // resolve through them.
    }

    pub(crate) fn put_view(&mut self, value: ViewValue) {
        put_named(&mut self.views, &mut self.view_names, value, view_identity);
    }

    pub(crate) fn delete_view(&mut self, view_id: u64) {
        delete_named(
            &mut self.views,
            &mut self.view_names,
            view_id,
            view_identity,
        );
    }

    pub(crate) fn put_mapping(&mut self, value: MappingValue) {
        put_nested(&mut self.mappings, value.table_id, value.mapping_id, value);
    }

    pub(crate) fn put_macro(&mut self, value: MacroValue) {
        put_named(
            &mut self.macros,
            &mut self.macro_names,
            value,
            macro_identity,
        );
    }

    pub(crate) fn delete_macro(&mut self, macro_id: u64) {
        delete_named(
            &mut self.macros,
            &mut self.macro_names,
            macro_id,
            macro_identity,
        );
    }

    pub(crate) fn put_column(&mut self, value: ColumnValue) {
        put_nested(&mut self.columns, value.table_id, value.column_id, value);
    }

    pub(crate) fn delete_column(&mut self, table_id: u64, column_id: u64) {
        remove_nested(&mut self.columns, table_id, &column_id);
        // Column stats describe current state and die with the column.
        remove_nested(&mut self.table_column_stats, table_id, &column_id);
    }

    /// The table's data files live at this view's snapshot, ordered by id.
    #[must_use]
    pub fn data_files_of(&self, table: TableId) -> Vec<DataFileInfo> {
        let Some(files) = self.data_files.get(&table.get()) else {
            return Vec::new();
        };

        files.values().map(data_file_info).collect()
    }

    /// The table's delete files live at this view's snapshot, ordered by id.
    #[must_use]
    pub fn delete_files_of(&self, table: TableId) -> Vec<DeleteFileInfo> {
        let Some(files) = self.delete_files.get(&table.get()) else {
            return Vec::new();
        };

        files.values().map(delete_file_info).collect()
    }

    /// Statistics for a table. Unversioned: a time-travel view serves
    /// the current statistics.
    #[must_use]
    pub fn table_stats(&self, table: TableId) -> Option<TableStats> {
        self.table_stats
            .get(&table.get())
            .map(table_stats_from_proto)
    }

    /// Statistics for a column. Unversioned: a time-travel view serves
    /// the current statistics.
    #[must_use]
    pub fn column_stats(&self, table: TableId, column: ColumnId) -> Option<ColumnStats> {
        self.table_column_stats
            .get(&table.get())
            .and_then(|cols| cols.get(&column.get()))
            .map(column_stats_from_proto)
    }

    pub(crate) fn put_data_file(&mut self, value: DataFileValue) {
        put_nested(
            &mut self.data_files,
            value.table_id,
            value.data_file_id,
            value,
        );
    }

    pub(crate) fn delete_data_file(&mut self, table_id: u64, data_file_id: u64) {
        remove_nested(&mut self.data_files, table_id, &data_file_id);
    }

    pub(crate) fn put_partition(&mut self, value: PartitionValue) {
        put_nested(
            &mut self.partitions,
            value.table_id,
            value.partition_id,
            value,
        );
    }

    pub(crate) fn delete_partition(&mut self, table_id: u64, partition_id: u64) {
        remove_nested(&mut self.partitions, table_id, &partition_id);
    }

    pub(crate) fn put_sort(&mut self, value: SortValue) {
        put_nested(&mut self.sorts, value.table_id, value.sort_id, value);
    }

    pub(crate) fn delete_sort(&mut self, table_id: u64, sort_id: u64) {
        remove_nested(&mut self.sorts, table_id, &sort_id);
    }

    pub(crate) fn put_delete_file(&mut self, value: DeleteFileValue) {
        put_nested(
            &mut self.delete_files,
            value.table_id,
            value.delete_file_id,
            value,
        );
    }

    pub(crate) fn delete_delete_file(&mut self, table_id: u64, delete_file_id: u64) {
        remove_nested(&mut self.delete_files, table_id, &delete_file_id);
    }

    pub(crate) fn put_table_stats(&mut self, value: TableStatsValue) {
        self.table_stats.insert(value.table_id, value);
    }

    pub(crate) fn put_table_column_stats(&mut self, value: TableColumnStatsValue) {
        put_nested(
            &mut self.table_column_stats,
            value.table_id,
            value.column_id,
            value,
        );
    }

    pub(crate) fn put_file_column_stats(&mut self, value: FileColumnStatsValue) {
        put_nested(
            &mut self.file_column_stats,
            value.table_id,
            (value.data_file_id, value.column_id),
            value,
        );
    }

    pub(crate) fn set_option_record(&mut self, components: (u64, u64), value: OptionScopeValue) {
        self.options.insert(components, value);
    }

    pub(crate) fn remove_option_record(&mut self, components: (u64, u64)) {
        self.options.remove(&components);
    }

    /// Removes a schema and its name entry, without the option-scope
    /// cascade [`delete_schema`](Self::delete_schema) applies. The
    /// commit-time fold mirrors the store key by key, so a cascade would
    /// drop rows the batch keeps.
    pub(crate) fn remove_schema_only(&mut self, schema_id: u64) {
        if let Some(old) = self.schemas.remove(&schema_id) {
            self.schema_names.remove(&old.schema_name);
        }
    }

    /// Removes a table and its name entry, without the child cascade
    /// [`delete_table`](Self::delete_table) applies.
    pub(crate) fn remove_table_only(&mut self, table_id: u64) {
        delete_named(
            &mut self.tables,
            &mut self.table_names,
            table_id,
            table_identity,
        );
    }

    /// Removes one column, without the column-stats cascade
    /// [`delete_column`](Self::delete_column) applies.
    pub(crate) fn remove_column_only(&mut self, table_id: u64, column_id: u64) {
        remove_nested(&mut self.columns, table_id, &column_id);
    }

    pub(crate) fn remove_table_stats(&mut self, table_id: u64) {
        self.table_stats.remove(&table_id);
    }

    pub(crate) fn remove_table_column_stats(&mut self, table_id: u64, column_id: u64) {
        remove_nested(&mut self.table_column_stats, table_id, &column_id);
    }

    pub(crate) fn remove_file_column_stats(
        &mut self,
        table_id: u64,
        data_file_id: u64,
        column_id: u64,
    ) {
        remove_nested(
            &mut self.file_column_stats,
            table_id,
            &(data_file_id, column_id),
        );
    }

    pub(crate) fn remove_tag(&mut self, object_id: u64) {
        self.tags.remove(&object_id);
    }

    pub(crate) fn remove_gc_file(&mut self, data_file_id: u64) {
        self.gc_files.remove(&data_file_id);
    }
}

fn schema_info(value: &SchemaValue) -> SchemaInfo {
    SchemaInfo {
        id: SchemaId::new(value.schema_id),
        name: value.schema_name.clone(),
    }
}

fn table_info(value: &TableValue) -> TableInfo {
    TableInfo {
        id: TableId::new(value.table_id),
        schema_id: SchemaId::new(value.schema_id),
        name: value.table_name.clone(),
    }
}

fn view_info(value: &ViewValue) -> ViewInfo {
    ViewInfo {
        id: ViewId::new(value.view_id),
        schema_id: SchemaId::new(value.schema_id),
        name: value.view_name.clone(),
        dialect: value.dialect.clone(),
        sql: value.sql.clone(),
    }
}

fn mapping_info(value: &MappingValue) -> MappingInfo {
    MappingInfo {
        id: MappingId::new(value.mapping_id),
        table_id: TableId::new(value.table_id),
        map_type: value.map_type.clone(),
        name_mappings: value
            .name_mappings
            .iter()
            .map(|row| NameMappingDef {
                column_id: row.column_id,
                source_name: row.source_name.clone(),
                target_field_id: row.target_field_id,
                parent_column: row.parent_column,
                is_partition: row.is_partition,
            })
            .collect(),
    }
}

fn sort_spec(value: &SortValue) -> SortSpec {
    let mut expressions: Vec<&crate::store::proto::SortExpression> =
        value.expressions.iter().collect();
    expressions.sort_by_key(|expression| expression.sort_key_index);
    SortSpec {
        id: SortId::new(value.sort_id),
        keys: expressions
            .into_iter()
            .map(|expression| SortKeyDef {
                expression: expression.expression.clone(),
                dialect: expression.dialect.clone(),
                sort_direction: expression.sort_direction.clone(),
                null_order: expression.null_order.clone(),
            })
            .collect(),
    }
}

fn partition_spec(value: &PartitionValue) -> PartitionSpec {
    let mut columns: Vec<&crate::store::proto::PartitionColumn> = value.columns.iter().collect();
    columns.sort_by_key(|column| column.partition_key_index);
    PartitionSpec {
        id: PartitionId::new(value.partition_id),
        columns: columns
            .into_iter()
            .map(|column| PartitionColumnDef {
                column: ColumnId::new(column.column_id),
                transform: column.transform.clone(),
            })
            .collect(),
    }
}

pub(crate) fn index_info(value: &IndexValue) -> IndexInfo {
    use crate::{
        catalog::IndexMaintenance,
        store::index_encoding::{Direction, NullOrder},
    };
    let state = if value.poisoned == Some(true) {
        IndexState::Poisoned
    } else if value.build_state.as_deref() == Some("maintaining") {
        IndexState::Maintaining
    } else if value.build_state.is_some() {
        IndexState::Building
    } else {
        IndexState::Ready
    };
    let columns: Vec<ColumnId> = value
        .column_ids
        .iter()
        .copied()
        .map(ColumnId::new)
        .collect();
    // Absent per-column orders default to ascending / NULLS LAST, so an
    // ascending index reads the same whether or not it recorded them.
    let directions = (0..columns.len())
        .map(|i| {
            if value.column_descending.get(i).copied().unwrap_or(false) {
                Direction::Descending
            } else {
                Direction::Ascending
            }
        })
        .collect();
    let nulls = (0..columns.len())
        .map(|i| {
            if value.column_nulls_first.get(i).copied().unwrap_or(false) {
                NullOrder::First
            } else {
                NullOrder::Last
            }
        })
        .collect();

    IndexInfo {
        id: IndexId::new(value.index_id),
        table_id: TableId::new(value.table_id),
        name: value.index_name.clone(),
        columns,
        directions,
        nulls,
        unique: value.unique,
        maintenance: if value.deferred_maintenance == Some(true) {
            IndexMaintenance::Deferred
        } else {
            IndexMaintenance::Synchronous
        },
        state,
        build_cursor: value.build_cursor_row_id,
        build_file_cursor: value.build_cursor_file.map(DataFileId::new),
        build_position_cursor: value.build_cursor_position,
    }
}

fn macro_info(value: &MacroValue) -> MacroInfo {
    MacroInfo {
        id: MacroId::new(value.macro_id),
        schema_id: SchemaId::new(value.schema_id),
        name: value.macro_name.clone(),
        implementations: value
            .implementations
            .iter()
            .map(|implementation| MacroImplementationDef {
                dialect: implementation.dialect.clone(),
                sql: implementation.sql.clone(),
                macro_type: implementation.macro_type.clone(),
                parameters: implementation
                    .parameters
                    .iter()
                    .map(|parameter| MacroParameterDef {
                        name: parameter.parameter_name.clone(),
                        parameter_type: parameter.parameter_type.clone(),
                        default_value: parameter.default_value.clone(),
                        default_value_type: parameter.default_value_type.clone(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn data_file_info(value: &DataFileValue) -> DataFileInfo {
    DataFileInfo {
        id: DataFileId::new(value.data_file_id),
        path: value.path.clone(),
        path_is_relative: value.path_is_relative,
        file_format: value.file_format.clone(),
        record_count: value.record_count,
        file_size_bytes: value.file_size_bytes,
        footer_size: value.footer_size,
        row_id_start: value.row_id_start,
        encryption_key: value.encryption_key.clone(),
        partition_id: value.partition_id.map(PartitionId::new),
        partition_values: partition_values(value),
        partial_max: value.partial_max.map(SnapshotId::new),
    }
}

/// A file's partition values in key order. Stored order is the writer's;
/// the index is the column, so it decides.
fn partition_values(value: &DataFileValue) -> Vec<String> {
    let mut values: Vec<&crate::store::proto::FilePartitionValue> =
        value.partition_values.iter().collect();
    values.sort_by_key(|value| value.partition_key_index);

    values
        .into_iter()
        .map(|value| value.partition_value.clone())
        .collect()
}

fn delete_file_info(value: &DeleteFileValue) -> DeleteFileInfo {
    DeleteFileInfo {
        id: DeleteFileId::new(value.delete_file_id),
        data_file_id: DataFileId::new(value.data_file_id),
        path: value.path.clone(),
        path_is_relative: value.path_is_relative,
        format: value.format.clone(),
        delete_count: value.delete_count,
        file_size_bytes: value.file_size_bytes,
        footer_size: value.footer_size,
        encryption_key: value.encryption_key.clone(),
    }
}

fn table_stats_from_proto(value: &TableStatsValue) -> TableStats {
    TableStats {
        record_count: value.record_count,
        file_size_bytes: value.file_size_bytes,
        next_row_id: value.next_row_id,
    }
}

fn column_stats_from_proto(value: &TableColumnStatsValue) -> ColumnStats {
    ColumnStats {
        contains_null: value.contains_null,
        contains_nan: value.contains_nan,
        min_value: value.min_value.clone(),
        max_value: value.max_value.clone(),
        extra_stats: value.extra_stats.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::DataFileId;

    fn snap(id: u64) -> SnapshotValue {
        SnapshotValue {
            snapshot_id: id,
            snapshot_time_micros: 1,
            schema_version: 0,
            next_catalog_id: 10,
            next_file_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
            deleted_data_file_ids: Vec::new(),
        }
    }

    fn schema_rec(id: u64, name: &str, begin: u64, end: Option<u64>) -> SchemaValue {
        SchemaValue {
            schema_id: id,
            schema_uuid: format!("uuid-{id}"),
            begin_snapshot: begin,
            end_snapshot: end,
            schema_name: name.into(),
            path: format!("{name}/"),
            path_is_relative: true,
        }
    }

    fn table_rec(id: u64, schema: u64, name: &str, begin: u64) -> TableValue {
        TableValue {
            table_id: id,
            table_uuid: format!("uuid-t{id}"),
            begin_snapshot: begin,
            end_snapshot: None,
            schema_id: schema,
            table_name: name.into(),
            path: format!("{name}/"),
            path_is_relative: true,
            next_column_id: 0,
        }
    }

    fn column_rec(table: u64, id: u64, name: &str, order: u64, begin: u64) -> ColumnValue {
        ColumnValue {
            column_id: id,
            begin_snapshot: begin,
            end_snapshot: None,
            table_id: table,
            column_order: order,
            column_name: name.into(),
            column_type: "BIGINT".into(),
            initial_default: None,
            default_value: None,
            nulls_allowed: true,
            parent_column: None,
            default_value_type: None,
            default_value_dialect: None,
            tags: vec![],
        }
    }

    fn file_rec(table: u64, id: u64, rows: u64, begin: u64, end: Option<u64>) -> DataFileValue {
        DataFileValue {
            data_file_id: id,
            table_id: table,
            begin_snapshot: begin,
            end_snapshot: end,
            file_order: None,
            path: format!("f{id}.parquet"),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: rows,
            file_size_bytes: rows * 10,
            footer_size: 4,
            row_id_start: Some(0),
            partition_id: None,
            encryption_key: None,
            mapping_id: None,
            partial_max: None,
            partition_values: vec![],
        }
    }

    fn view_rec(id: u64, schema: u64, name: &str, begin: u64, end: Option<u64>) -> ViewValue {
        ViewValue {
            view_id: id,
            view_uuid: format!("uuid-v{id}"),
            begin_snapshot: begin,
            end_snapshot: end,
            schema_id: schema,
            view_name: name.into(),
            dialect: "duckdb".into(),
            sql: format!("select * from {name}"),
            column_aliases: None,
        }
    }

    fn macro_rec(id: u64, schema: u64, name: &str, begin: u64, end: Option<u64>) -> MacroValue {
        MacroValue {
            macro_id: id,
            begin_snapshot: begin,
            end_snapshot: end,
            schema_id: schema,
            macro_name: name.into(),
            implementations: vec![crate::store::proto::MacroImplementation {
                impl_id: 0,
                dialect: "duckdb".into(),
                sql: "1".into(),
                macro_type: "scalar".into(),
                parameters: vec![],
            }],
        }
    }

    fn tstat_rec(table: u64, count: u64, next_row: u64) -> TableStatsValue {
        TableStatsValue {
            table_id: table,
            record_count: count,
            next_row_id: next_row,
            file_size_bytes: count * 10,
        }
    }

    #[test]
    fn head_view_indexes_live_entities() {
        let current = vec![
            EntityRecord::Schema(schema_rec(0, "main", 1, None)),
            EntityRecord::Table(table_rec(1, 0, "orders", 2)),
            EntityRecord::Column(column_rec(1, 0, "id", 0, 2)),
            EntityRecord::Column(column_rec(1, 1, "amount", 1, 2)),
        ];
        let view = CatalogSnapshot::build(snap(2), &current, &[], None);

        let s = view.schema_by_name("main").unwrap();
        assert_eq!(s.id, SchemaId::new(0));
        let t = view.table_by_name(s.id, "orders").unwrap();
        assert_eq!(t.id, TableId::new(1));
        assert_eq!(view.tables_in(s.id), vec![t.clone()]);
        let cols = view.columns_of(t.id);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[1].position, 1);
        assert_eq!(view.current_snapshot().id, SnapshotId::new(2));
    }

    #[test]
    fn time_travel_filters_by_begin_and_end() {
        // Schema 0 lived [1, 3); its replacement version lives [3, ∞).
        // Table 1 was created at 4.
        let current = vec![
            EntityRecord::Schema(schema_rec(0, "renamed", 3, None)),
            EntityRecord::Table(table_rec(1, 0, "orders", 4)),
        ];
        let history = vec![EntityRecord::Schema(schema_rec(0, "original", 1, Some(3)))];

        let at2 = CatalogSnapshot::build(snap(2), &current.clone(), &history.clone(), Some(2));
        assert!(at2.schema_by_name("original").is_some());
        assert!(at2.schema_by_name("renamed").is_none());
        assert!(at2.table_by_id(TableId::new(1)).is_none());

        let at4 = CatalogSnapshot::build(snap(4), &current, &history, Some(4));
        assert!(at4.schema_by_name("renamed").is_some());
        assert!(at4.table_by_id(TableId::new(1)).is_some());
    }

    #[test]
    fn history_only_records_are_excluded_from_the_head_view() {
        // Column allocation now reads the table's persisted counter, not
        // `history` — so `build` at head must not resurrect a dropped column
        // (or any other purely-historical record) into the live view.
        let history = vec![EntityRecord::Column(ColumnValue {
            end_snapshot: Some(3),
            ..column_rec(1, 5, "gone", 0, 1)
        })];
        let view = CatalogSnapshot::build(snap(3), &[], &history, None);
        assert!(view.columns_of(TableId::new(1)).is_empty());
    }

    /// Both projected instants are `Timestamp`, and the projection is total:
    /// a pre-epoch count survives it unchanged.
    #[test]
    fn instants_project_unchanged_including_before_the_epoch() {
        let mut value = snap(7);
        value.snapshot_time_micros = -1_000;

        let mut view = CatalogSnapshot::build(value, &[], &[], None);
        view.put_gc_file(GcFileValue {
            data_file_id: 9,
            path: "f9.parquet".into(),
            path_is_relative: true,
            schedule_start_micros: -2_000,
        });

        assert_eq!(view.current_snapshot().time, Timestamp::from_micros(-1_000));
        assert_eq!(
            view.scheduled_deletions()[0].schedule_start,
            Timestamp::from_micros(-2_000)
        );
    }

    #[test]
    fn mutation_helpers_keep_indexes_coherent() {
        let mut view = CatalogSnapshot::build(snap(1), &[], &[], None);
        view.put_schema(schema_rec(0, "main", 2, None));
        view.put_table(table_rec(1, 0, "orders", 2));
        view.put_column(column_rec(1, 0, "id", 0, 2));
        assert!(view.table_by_name(SchemaId::new(0), "orders").is_some());

        let mut renamed = view.tables[&1].clone();
        renamed.table_name = "orders_v2".into();
        view.put_table(renamed);
        assert!(view.table_by_name(SchemaId::new(0), "orders").is_none());
        assert!(view.table_by_name(SchemaId::new(0), "orders_v2").is_some());

        view.delete_table(1);
        assert!(view.table_by_id(TableId::new(1)).is_none());
        assert!(view.columns_of(TableId::new(1)).is_empty());

        view.delete_schema(0);
        assert!(view.schema_by_name("main").is_none());
    }

    #[test]
    fn data_files_filter_by_version_but_stats_do_not() {
        let current = vec![
            EntityRecord::File(file_rec(1, 10, 100, 5, None)),
            EntityRecord::TableStats(tstat_rec(1, 100, 100)),
        ];
        let history = vec![EntityRecord::File(file_rec(1, 9, 50, 1, Some(5)))];

        // Head view: only the live file; stats present.
        let head = CatalogSnapshot::build(snap(6), &current.clone(), &[], None);
        let files = head.data_files_of(TableId::new(1));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, DataFileId::new(10));
        assert_eq!(head.table_stats(TableId::new(1)).unwrap().record_count, 100);

        // Time travel to snapshot 3: the ended file was live, the new one
        // not yet; stats are unversioned and served as-is.
        let past = CatalogSnapshot::build(snap(3), &current, &history, Some(3));
        let files = past.data_files_of(TableId::new(1));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, DataFileId::new(9));
        assert_eq!(past.table_stats(TableId::new(1)).unwrap().next_row_id, 100);
    }

    #[test]
    fn stats_accessors_and_helpers() {
        let mut view = CatalogSnapshot::build(snap(1), &[], &[], None);
        assert!(view.table_stats(TableId::new(1)).is_none());
        assert!(
            view.column_stats(TableId::new(1), ColumnId::new(1))
                .is_none()
        );

        view.put_table_stats(tstat_rec(1, 7, 7));
        view.put_table_column_stats(TableColumnStatsValue {
            table_id: 1,
            column_id: 2,
            contains_null: Some(true),
            contains_nan: None,
            min_value: Some("9".into()),
            max_value: Some("10".into()),
            extra_stats: None,
        });
        // Verbatim strings: '9'/'10' come back untouched, never compared.
        let stats = view
            .column_stats(TableId::new(1), ColumnId::new(2))
            .unwrap();
        assert_eq!(stats.min_value.as_deref(), Some("9"));
        assert_eq!(stats.max_value.as_deref(), Some("10"));

        view.put_data_file(file_rec(1, 3, 10, 1, None));
        view.put_delete_file(DeleteFileValue {
            delete_file_id: 4,
            table_id: 1,
            begin_snapshot: 1,
            end_snapshot: None,
            data_file_id: 3,
            path: "d.parquet".into(),
            path_is_relative: true,
            format: "parquet".into(),
            delete_count: 2,
            file_size_bytes: 20,
            footer_size: 4,
            encryption_key: None,
            partial_max: None,
        });
        assert_eq!(
            view.delete_files_of(TableId::new(1))[0].data_file_id,
            DataFileId::new(3)
        );

        view.put_file_column_stats(FileColumnStatsValue {
            data_file_id: 3,
            table_id: 1,
            column_id: 2,
            column_size_bytes: 10,
            value_count: 10,
            null_count: 0,
            min_value: Some("1".into()),
            max_value: Some("2".into()),
            contains_nan: None,
            extra_stats: None,
            variant_stats: vec![],
        });

        // Dropping the table clears every per-table map except
        // file_column_stats, which outlives the table's live version
        // until the file's history is garbage-collected.
        view.delete_table(1);
        assert!(view.data_files_of(TableId::new(1)).is_empty());
        assert!(view.delete_files_of(TableId::new(1)).is_empty());
        assert!(view.table_stats(TableId::new(1)).is_none());
        assert!(
            view.column_stats(TableId::new(1), ColumnId::new(2))
                .is_none()
        );
        assert!(
            view.file_column_stats
                .get(&1)
                .is_some_and(|cols| cols.contains_key(&(3, 2)))
        );
    }

    #[test]
    fn views_are_versioned_and_indexed() {
        let current = vec![EntityRecord::View(view_rec(3, 0, "v2", 4, None))];
        let history = vec![EntityRecord::View(view_rec(3, 0, "v1", 1, Some(4)))];
        let head = CatalogSnapshot::build(snap(5), &current.clone(), &[], None);
        assert_eq!(
            head.view_by_name(SchemaId::new(0), "v2").unwrap().id,
            ViewId::new(3)
        );
        assert!(head.view_by_name(SchemaId::new(0), "v1").is_none());
        let past = CatalogSnapshot::build(snap(2), &current, &history, Some(2));
        assert_eq!(past.view_by_id(ViewId::new(3)).unwrap().name, "v1");
    }

    #[test]
    fn mappings_are_table_scoped_and_survive_time_travel() {
        let mapping = MappingValue {
            mapping_id: 21,
            table_id: 4,
            map_type: "map_by_name".into(),
            name_mappings: vec![crate::store::proto::NameMapping {
                column_id: 0,
                source_name: "id".into(),
                target_field_id: 1,
                parent_column: None,
                is_partition: false,
            }],
        };
        // Unversioned: included regardless of the time-travel target.
        let past =
            CatalogSnapshot::build(snap(12), &[EntityRecord::Mapping(mapping)], &[], Some(1));
        let mappings = past.mappings_of(TableId::new(4));
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].id, MappingId::new(21));
        assert_eq!(mappings[0].map_type, "map_by_name");
        assert_eq!(mappings[0].name_mappings[0].source_name, "id");
        assert!(past.mappings_of(TableId::new(5)).is_empty());
    }

    #[test]
    fn macros_are_versioned_and_indexed() {
        let current = vec![EntityRecord::Macro(macro_rec(1, 5, "add", 10, None))];
        let history = vec![EntityRecord::Macro(macro_rec(2, 5, "old", 1, Some(10)))];
        let head = CatalogSnapshot::build(snap(12), &current.clone(), &[], None);
        assert_eq!(
            head.macro_by_name(SchemaId::new(5), "add").unwrap().id,
            MacroId::new(1)
        );
        assert!(head.macro_by_name(SchemaId::new(5), "old").is_none());
        assert_eq!(head.macros_in(SchemaId::new(5)).len(), 1);
        let past = CatalogSnapshot::build(snap(2), &current, &history, Some(2));
        assert_eq!(past.macro_by_id(MacroId::new(2)).unwrap().name, "old");
        assert_eq!(
            past.macro_by_id(MacroId::new(2)).unwrap().implementations[0].macro_type,
            "scalar"
        );
    }

    fn index_rec(
        id: u64,
        table: u64,
        name: &str,
        columns: Vec<u64>,
        unique: bool,
        begin: u64,
        end: Option<u64>,
    ) -> crate::store::proto::IndexValue {
        crate::store::proto::IndexValue {
            index_id: id,
            table_id: table,
            begin_snapshot: begin,
            end_snapshot: end,
            index_name: name.into(),
            column_ids: columns,
            unique,
            column_descending: Vec::new(),
            column_nulls_first: Vec::new(),
            build_state: None,
            build_cursor_file: None,
            build_cursor_row_id: None,
            build_cursor_position: None,
            build_deletes_scanned: None,
            poisoned: None,
            ducklake_index_id: None,
            deferred_maintenance: None,
        }
    }

    #[test]
    fn indexes_are_versioned_and_table_scoped() {
        use crate::catalog::types::{IndexId, IndexState};
        let current = vec![EntityRecord::Index(index_rec(
            7,
            1,
            "by_id",
            vec![0, 1],
            true,
            4,
            None,
        ))];
        let history = vec![EntityRecord::Index(index_rec(
            8,
            1,
            "old",
            vec![0],
            false,
            1,
            Some(4),
        ))];

        let head = CatalogSnapshot::build(snap(5), &current.clone(), &[], None);
        let infos = head.indexes_of(TableId::new(1));
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].id, IndexId::new(7));
        assert_eq!(infos[0].columns, vec![ColumnId::new(0), ColumnId::new(1)]);
        assert!(infos[0].unique);
        assert_eq!(infos[0].state, IndexState::Ready);
        assert_eq!(
            head.index_by_name(TableId::new(1), "by_id").unwrap().id,
            IndexId::new(7)
        );
        assert!(head.index_by_name(TableId::new(1), "old").is_none());
        assert!(head.indexes_of(TableId::new(2)).is_empty());

        // Time travel to snapshot 2: the ended index was live, the new one
        // not yet.
        let past = CatalogSnapshot::build(snap(2), &current, &history, Some(2));
        assert_eq!(past.indexes_of(TableId::new(1))[0].id, IndexId::new(8));
    }

    #[test]
    fn dropping_a_table_clears_its_indexes() {
        let mut view = CatalogSnapshot::build(snap(1), &[], &[], None);
        view.put_table(table_rec(1, 0, "t", 1));
        view.put_index(index_rec(7, 1, "by_id", vec![0], true, 1, None));
        assert!(view.index_by_name(TableId::new(1), "by_id").is_some());

        view.delete_table(1);
        assert!(view.indexes_of(TableId::new(1)).is_empty());
        assert!(view.index_by_name(TableId::new(1), "by_id").is_none());
    }

    #[test]
    fn building_maintaining_and_poisoned_states_are_projected() {
        use crate::catalog::types::{IndexMaintenance, IndexState};
        let building = crate::store::proto::IndexValue {
            build_state: Some("building".into()),
            ..index_rec(7, 1, "b", vec![0], true, 1, None)
        };
        let poisoned = crate::store::proto::IndexValue {
            build_state: Some("building".into()),
            poisoned: Some(true),
            ..index_rec(8, 1, "p", vec![0], true, 1, None)
        };
        let maintaining = crate::store::proto::IndexValue {
            build_state: Some("maintaining".into()),
            deferred_maintenance: Some(true),
            ..index_rec(9, 1, "m", vec![0], false, 1, None)
        };
        let view = CatalogSnapshot::build(
            snap(1),
            &[
                EntityRecord::Index(building),
                EntityRecord::Index(poisoned),
                EntityRecord::Index(maintaining),
            ],
            &[],
            None,
        );
        let by_name = |n: &str| view.index_by_name(TableId::new(1), n).unwrap().state;
        assert_eq!(by_name("b"), IndexState::Building);
        assert_eq!(by_name("p"), IndexState::Poisoned);
        assert_eq!(by_name("m"), IndexState::Maintaining);
        assert_eq!(
            view.index_by_name(TableId::new(1), "m")
                .unwrap()
                .maintenance,
            IndexMaintenance::Deferred
        );
    }

    #[test]
    fn option_resolution_cascades() {
        let mut view = CatalogSnapshot::build(snap(1), &[], &[], None);
        view.put_schema(schema_rec(0, "s", 1, None));
        view.put_table(table_rec(1, 0, "t", 1));
        let mk = |pairs: &[(&str, &str)]| OptionScopeValue {
            options: pairs
                .iter()
                .map(|(k, v)| ((*k).into(), (*v).into()))
                .collect(),
        };
        view.set_option_record((0, 0), mk(&[("a", "global"), ("b", "global")]));
        view.set_option_record((1, 0), mk(&[("a", "schema")]));
        view.set_option_record((2, 1), mk(&[("c", "table")]));
        let t = TableId::new(1);
        assert_eq!(
            view.option(OptionScope::Table(t), "c").as_deref(),
            Some("table")
        );
        assert_eq!(
            view.option(OptionScope::Table(t), "a").as_deref(),
            Some("schema")
        );
        assert_eq!(
            view.option(OptionScope::Table(t), "b").as_deref(),
            Some("global")
        );
        assert_eq!(
            view.option(OptionScope::Schema(SchemaId::new(0)), "a")
                .as_deref(),
            Some("schema")
        );
        assert_eq!(
            view.option(OptionScope::Global, "a").as_deref(),
            Some("global")
        );
        assert!(view.option(OptionScope::Global, "zz").is_none());

        view.delete_table(1);
        assert!(
            !view.options.contains_key(&(2, 1)),
            "table options die with the table"
        );
    }
}
