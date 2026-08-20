//! Turning a (base, staged) snapshot pair into the store writes that
//! transition one to the other: versioned kinds mint history mirrors,
//! unversioned kinds are overwritten in place.
//!
//! The diff walks a [`Scope`] of entity ids rather than the catalog: a
//! caller that recorded what it touched pays for its own commit, one that
//! did not pays for the whole catalog. Both share the staging below, so
//! the two scopes differ only in which ids they visit.

use std::collections::{BTreeMap, BTreeSet};

use super::StagedWrite;
use crate::{
    catalog::CatalogSnapshot,
    store::{
        key::{CurrentKey, EntityKey, Key},
        proto, value,
    },
};

/// The entities a translation mutated, per kind.
///
/// Over-recording is free — an id whose two sides match stages no write —
/// but under-recording drops that entity's write entirely, so every
/// mutation site records the id it touches.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Touched {
    schemas: BTreeSet<u64>,
    tables: BTreeSet<u64>,
    views: BTreeSet<u64>,
    macros: BTreeSet<u64>,
    columns: BTreeSet<(u64, u64)>,
    data_files: BTreeSet<(u64, u64)>,
    delete_files: BTreeSet<(u64, u64)>,
    partitions: BTreeSet<(u64, u64)>,
    sorts: BTreeSet<(u64, u64)>,
    indexes: BTreeSet<(u64, u64)>,
    mappings: BTreeSet<(u64, u64)>,
    table_stats: BTreeSet<u64>,
    table_column_stats: BTreeSet<(u64, u64)>,
    file_column_stats: BTreeSet<(u64, (u64, u64))>,
    options: BTreeSet<(u64, u64)>,
    tags: BTreeSet<u64>,
    gc_files: BTreeSet<u64>,
}

impl Touched {
    /// Records the entity `key` names.
    pub(crate) fn touch(&mut self, key: EntityKey) {
        match key {
            EntityKey::Schema { schema_id } => {
                self.schemas.insert(schema_id);
            }
            EntityKey::Table { table_id } => {
                self.tables.insert(table_id);
            }
            EntityKey::View { view_id } => {
                self.views.insert(view_id);
            }
            EntityKey::Macro { macro_id } => {
                self.macros.insert(macro_id);
            }
            EntityKey::Column {
                table_id,
                column_id,
            } => {
                self.columns.insert((table_id, column_id));
            }
            EntityKey::File {
                table_id,
                data_file_id,
            } => {
                self.data_files.insert((table_id, data_file_id));
            }
            EntityKey::DeleteFile {
                table_id,
                delete_file_id,
            } => {
                self.delete_files.insert((table_id, delete_file_id));
            }
            EntityKey::Partition {
                table_id,
                partition_id,
            } => {
                self.partitions.insert((table_id, partition_id));
            }
            EntityKey::Sort { table_id, sort_id } => {
                self.sorts.insert((table_id, sort_id));
            }
            EntityKey::Index { table_id, index_id } => {
                self.indexes.insert((table_id, index_id));
            }
            EntityKey::Mapping {
                table_id,
                mapping_id,
            } => {
                self.mappings.insert((table_id, mapping_id));
            }
            EntityKey::TableStats { table_id } => {
                self.table_stats.insert(table_id);
            }
            EntityKey::TableColumnStats {
                table_id,
                column_id,
            } => {
                self.table_column_stats.insert((table_id, column_id));
            }
            EntityKey::FileColumnStats {
                table_id,
                data_file_id,
                column_id,
            } => {
                self.file_column_stats
                    .insert((table_id, (data_file_id, column_id)));
            }
            EntityKey::Option {
                scope_kind,
                scope_id,
            } => {
                self.options.insert((scope_kind, scope_id));
            }
            EntityKey::Tag { object_id } => {
                self.tags.insert(object_id);
            }
        }
    }

    /// Records a deletion-schedule row, which is keyed beside the entities
    /// rather than as one.
    pub(crate) fn touch_gc_file(&mut self, data_file_id: u64) {
        self.gc_files.insert(data_file_id);
    }
}

/// Which entity ids a diff visits.
#[derive(Clone, Copy)]
pub(crate) enum Scope<'a> {
    /// Every id either side holds, for a caller that did not record.
    All,
    /// Only what a translation recorded touching.
    Touched(&'a Touched),
}

/// The ids to visit in one flat map pair.
fn flat_ids<K: Copy + Ord, M, N>(
    scope: Scope<'_>,
    base: &BTreeMap<K, M>,
    state: &BTreeMap<K, N>,
    select: impl Fn(&Touched) -> &BTreeSet<K>,
) -> Vec<K> {
    match scope {
        Scope::Touched(touched) => select(touched).iter().copied().collect(),
        Scope::All => base
            .keys()
            .chain(state.keys())
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}

/// The `(table_id, id)` pairs to visit in one nested map pair. Table-major
/// then id, the order a whole-catalog walk visits them in.
fn nested_ids<K: Copy + Ord, M, N>(
    scope: Scope<'_>,
    base: &BTreeMap<u64, BTreeMap<K, M>>,
    state: &BTreeMap<u64, BTreeMap<K, N>>,
    select: impl Fn(&Touched) -> &BTreeSet<(u64, K)>,
) -> Vec<(u64, K)> {
    match scope {
        Scope::Touched(touched) => select(touched).iter().copied().collect(),
        Scope::All => {
            let mut ids = BTreeSet::new();
            for (&table_id, inner) in base {
                ids.extend(inner.keys().map(|&id| (table_id, id)));
            }
            for (&table_id, inner) in state {
                ids.extend(inner.keys().map(|&id| (table_id, id)));
            }
            ids.into_iter().collect()
        }
    }
}

fn stage_transition<M: prost::Message + Clone + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    entity: EntityKey,
    base: Option<&M>,
    state: Option<&M>,
    new_snapshot: u64,
    set_end: impl Fn(&M) -> M,
) {
    match (base, state) {
        (Some(base), None) => {
            writes.push((Key::current(entity).encode(), None));
            writes.push((
                Key::history(entity, new_snapshot).encode(),
                Some(value::encode_value(&set_end(base))),
            ));
        }
        (Some(base), Some(state)) if base != state => {
            writes.push((
                Key::history(entity, new_snapshot).encode(),
                Some(value::encode_value(&set_end(base))),
            ));
            writes.push((
                Key::current(entity).encode(),
                Some(value::encode_value(state)),
            ));
        }
        (None, Some(state)) => {
            writes.push((
                Key::current(entity).encode(),
                Some(value::encode_value(state)),
            ));
        }
        _ => {}
    }
}

/// Stages an unversioned record: overwritten in place, never mirrored to
/// history.
fn stage_overwrite<M: prost::Message + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    entity: EntityKey,
    base: Option<&M>,
    state: Option<&M>,
) {
    match (base, state) {
        (Some(_), None) => writes.push((Key::current(entity).encode(), None)),
        (base, Some(state)) if base != Some(state) => {
            writes.push((
                Key::current(entity).encode(),
                Some(value::encode_value(state)),
            ));
        }
        _ => {}
    }
}

/// Stages the transition of every in-scope entity in one id-keyed map pair.
fn diff_versioned_map<K: Copy + Ord, M: prost::Message + Clone + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    ids: Vec<K>,
    base: &BTreeMap<K, M>,
    state: &BTreeMap<K, M>,
    make_key: impl Fn(K) -> EntityKey,
    new_snapshot: u64,
    set_end: impl Fn(&M) -> M,
) {
    for id in ids {
        stage_transition(
            writes,
            make_key(id),
            base.get(&id),
            state.get(&id),
            new_snapshot,
            &set_end,
        );
    }
}

/// Stages the transition of every in-scope entity in one table-scoped
/// nested map pair (`table_id` → `id` → record).
fn diff_nested_versioned<K: Copy + Ord, M: prost::Message + Clone + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    ids: Vec<(u64, K)>,
    base: &BTreeMap<u64, BTreeMap<K, M>>,
    state: &BTreeMap<u64, BTreeMap<K, M>>,
    make_key: impl Fn(u64, K) -> EntityKey,
    new_snapshot: u64,
    set_end: impl Fn(&M) -> M,
) {
    for (table_id, id) in ids {
        stage_transition(
            writes,
            make_key(table_id, id),
            base.get(&table_id).and_then(|inner| inner.get(&id)),
            state.get(&table_id).and_then(|inner| inner.get(&id)),
            new_snapshot,
            &set_end,
        );
    }
}

/// Stages the in-place overwrite of every in-scope record in one id-keyed
/// map pair.
fn diff_overwrite_map<K: Copy + Ord, M: prost::Message + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    ids: Vec<K>,
    base: &BTreeMap<K, M>,
    state: &BTreeMap<K, M>,
    make_key: impl Fn(K) -> EntityKey,
) {
    for id in ids {
        stage_overwrite(writes, make_key(id), base.get(&id), state.get(&id));
    }
}

fn diff_schemas(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_versioned_map(
        writes,
        flat_ids(scope, &base.schemas, &state.schemas, |t| &t.schemas),
        &base.schemas,
        &state.schemas,
        |schema_id| EntityKey::Schema { schema_id },
        new_snapshot,
        |prior| proto::SchemaValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

/// As [`stage_transition`], except a change confined to the fields
/// `internal_free` zeroes overwrites the current record in place with no
/// history mint.
fn stage_transition_with_internal<M: prost::Message + Clone + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    entity: EntityKey,
    base: Option<&M>,
    state: Option<&M>,
    new_snapshot: u64,
    set_end: impl Fn(&M) -> M,
    internal_free: impl Fn(&M) -> M,
) {
    if let (Some(prior), Some(next)) = (base, state)
        && prior != next
        && internal_free(prior) == internal_free(next)
    {
        writes.push((
            Key::current(entity).encode(),
            Some(value::encode_value(next)),
        ));
        return;
    }

    stage_transition(writes, entity, base, state, new_snapshot, set_end);
}

fn diff_tables(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    // The field-id counter alone never mints history.
    fn sans_counter(value: &proto::TableValue) -> proto::TableValue {
        proto::TableValue {
            next_column_id: 0,
            ..value.clone()
        }
    }

    for table_id in flat_ids(scope, &base.tables, &state.tables, |t| &t.tables) {
        stage_transition_with_internal(
            writes,
            EntityKey::Table { table_id },
            base.tables.get(&table_id),
            state.tables.get(&table_id),
            new_snapshot,
            |prior| proto::TableValue {
                end_snapshot: Some(new_snapshot),
                ..prior.clone()
            },
            sans_counter,
        );
    }
}

fn diff_views(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_versioned_map(
        writes,
        flat_ids(scope, &base.views, &state.views, |t| &t.views),
        &base.views,
        &state.views,
        |view_id| EntityKey::View { view_id },
        new_snapshot,
        |prior| proto::ViewValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

pub(super) fn diff_options(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    diff_overwrite_map(
        writes,
        flat_ids(scope, &base.options, &state.options, |t| &t.options),
        &base.options,
        &state.options,
        |(scope_kind, scope_id)| EntityKey::Option {
            scope_kind,
            scope_id,
        },
    );
}

fn diff_tags(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    diff_overwrite_map(
        writes,
        flat_ids(scope, &base.tags, &state.tags, |t| &t.tags),
        &base.tags,
        &state.tags,
        |object_id| EntityKey::Tag { object_id },
    );
}

/// Deletion-schedule rows under `current/gcfile`, overwritten or removed
/// in place.
fn diff_gc_files(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    for data_file_id in flat_ids(scope, &base.gc_files, &state.gc_files, |t| &t.gc_files) {
        let key = Key::Current(CurrentKey::GcFile { data_file_id });
        match (
            base.gc_files.get(&data_file_id),
            state.gc_files.get(&data_file_id),
        ) {
            (Some(_), None) => writes.push((key.encode(), None)),
            (prior, Some(next)) if prior != Some(next) => {
                writes.push((key.encode(), Some(value::encode_value(next))));
            }
            _ => {}
        }
    }
}

fn diff_macros(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_versioned_map(
        writes,
        flat_ids(scope, &base.macros, &state.macros, |t| &t.macros),
        &base.macros,
        &state.macros,
        |macro_id| EntityKey::Macro { macro_id },
        new_snapshot,
        |prior| proto::MacroValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

/// Mappings are immutable, create-only records: never removed from the
/// working state, so `state` alone is iterated.
fn diff_mappings(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    let ids = match scope {
        Scope::Touched(touched) => touched.mappings.iter().copied().collect(),
        Scope::All => {
            let mut ids = Vec::new();
            for (&table_id, per_table) in &state.mappings {
                ids.extend(per_table.keys().map(|&mapping_id| (table_id, mapping_id)));
            }
            ids
        }
    };
    for (table_id, mapping_id) in ids {
        // A mapping is never removed from the working state, so a touched
        // id absent from it was recorded by a delete staged directly.
        let Some(value) = state
            .mappings
            .get(&table_id)
            .and_then(|per_table| per_table.get(&mapping_id))
        else {
            continue;
        };
        stage_overwrite(
            writes,
            EntityKey::Mapping {
                table_id,
                mapping_id,
            },
            base.mappings
                .get(&table_id)
                .and_then(|b| b.get(&mapping_id)),
            Some(value),
        );
    }
}

fn diff_columns(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    // Tag entries carry their own begin/end, so a tags-only change never
    // mints history.
    fn sans_tags(value: &proto::ColumnValue) -> proto::ColumnValue {
        proto::ColumnValue {
            tags: Vec::new(),
            ..value.clone()
        }
    }

    let empty = BTreeMap::new();
    for (table_id, column_id) in nested_ids(scope, &base.columns, &state.columns, |t| &t.columns) {
        let base_cols = base.columns.get(&table_id).unwrap_or(&empty);
        let state_cols = state.columns.get(&table_id).unwrap_or(&empty);
        stage_transition_with_internal(
            writes,
            EntityKey::Column {
                table_id,
                column_id,
            },
            base_cols.get(&column_id),
            state_cols.get(&column_id),
            new_snapshot,
            |prior| proto::ColumnValue {
                end_snapshot: Some(new_snapshot),
                ..prior.clone()
            },
            sans_tags,
        );
    }
}

fn diff_data_files(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
        nested_ids(scope, &base.data_files, &state.data_files, |t| {
            &t.data_files
        }),
        &base.data_files,
        &state.data_files,
        |table_id, data_file_id| EntityKey::File {
            table_id,
            data_file_id,
        },
        new_snapshot,
        |prior| proto::DataFileValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

fn diff_delete_files(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
        nested_ids(scope, &base.delete_files, &state.delete_files, |t| {
            &t.delete_files
        }),
        &base.delete_files,
        &state.delete_files,
        |table_id, delete_file_id| EntityKey::DeleteFile {
            table_id,
            delete_file_id,
        },
        new_snapshot,
        |prior| proto::DeleteFileValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

fn diff_partitions(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
        nested_ids(scope, &base.partitions, &state.partitions, |t| {
            &t.partitions
        }),
        &base.partitions,
        &state.partitions,
        |table_id, partition_id| EntityKey::Partition {
            table_id,
            partition_id,
        },
        new_snapshot,
        |prior| proto::PartitionValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

fn diff_sorts(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
        nested_ids(scope, &base.sorts, &state.sorts, |t| &t.sorts),
        &base.sorts,
        &state.sorts,
        |table_id, sort_id| EntityKey::Sort { table_id, sort_id },
        new_snapshot,
        |prior| proto::SortValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

fn diff_indexes(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
        nested_ids(scope, &base.indexes, &state.indexes, |t| &t.indexes),
        &base.indexes,
        &state.indexes,
        |table_id, index_id| EntityKey::Index { table_id, index_id },
        new_snapshot,
        |prior| proto::IndexValue {
            end_snapshot: Some(new_snapshot),
            ..prior.clone()
        },
    );
}

fn diff_table_stats(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    diff_overwrite_map(
        writes,
        flat_ids(scope, &base.table_stats, &state.table_stats, |t| {
            &t.table_stats
        }),
        &base.table_stats,
        &state.table_stats,
        |table_id| EntityKey::TableStats { table_id },
    );
}

fn diff_table_column_stats(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    let empty = BTreeMap::new();
    for (table_id, column_id) in nested_ids(
        scope,
        &base.table_column_stats,
        &state.table_column_stats,
        |t| &t.table_column_stats,
    ) {
        stage_overwrite(
            writes,
            EntityKey::TableColumnStats {
                table_id,
                column_id,
            },
            base.table_column_stats
                .get(&table_id)
                .unwrap_or(&empty)
                .get(&column_id),
            state
                .table_column_stats
                .get(&table_id)
                .unwrap_or(&empty)
                .get(&column_id),
        );
    }
}

fn diff_file_column_stats(
    writes: &mut Vec<StagedWrite>,
    scope: Scope<'_>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    let empty_stats = BTreeMap::new();
    let empty_files = BTreeMap::new();
    for (table_id, (data_file_id, column_id)) in nested_ids(
        scope,
        &base.file_column_stats,
        &state.file_column_stats,
        |t| &t.file_column_stats,
    ) {
        {
            let base_cols = base
                .file_column_stats
                .get(&table_id)
                .unwrap_or(&empty_stats);
            let state_cols = state
                .file_column_stats
                .get(&table_id)
                .unwrap_or(&empty_stats);
            let base_files = base.data_files.get(&table_id).unwrap_or(&empty_files);
            let state_files = state.data_files.get(&table_id).unwrap_or(&empty_files);
            let base_stats = base_cols.get(&(data_file_id, column_id));
            let state_stats = state_cols.get(&(data_file_id, column_id));
            let file_is_known =
                base_files.contains_key(&data_file_id) || state_files.contains_key(&data_file_id);
            let retiring = base_stats.is_some() && state_stats.is_none();
            // A file in neither side's data files may not *gain*
            // statistics — it was registered and expired within this
            // commit. Losing them still stages: this arm is the only one
            // that can retire a statistics row, so anything it skips is
            // stranded for good. Expiring the snapshots that registered a
            // file takes it out of both sides, which is exactly when a
            // caller is deleting its statistics.
            if !file_is_known && !retiring {
                continue;
            }
            stage_overwrite(
                writes,
                EntityKey::FileColumnStats {
                    table_id,
                    data_file_id,
                    column_id,
                },
                base_stats,
                state_stats,
            );
        }
    }
}

/// The write set turning `base` into `state` at `new_snapshot`: ended
/// versions move to history, new and changed versions land live, and
/// chained mutations of one entity collapse to a single transition.
pub(crate) fn diff_writes(
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) -> Vec<StagedWrite> {
    diff_scoped(base, state, new_snapshot, Scope::All)
}

/// As [`diff_writes`], over only what a translation recorded touching.
///
/// Identical output to a whole-catalog diff whenever `touched` covers
/// every entity the translation mutated, which is the contract every
/// mutation site upholds; debug builds assert the two agree.
pub(crate) fn diff_touched(
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
    touched: &Touched,
) -> Vec<StagedWrite> {
    let writes = diff_scoped(base, state, new_snapshot, Scope::Touched(touched));
    debug_assert_eq!(
        writes,
        diff_writes(base, state, new_snapshot),
        "a translation mutated an entity it did not record touching"
    );
    writes
}

/// The write set for one [`Scope`] of entity ids. Kind order is the store
/// key order a whole-catalog walk produces, so the two scopes are
/// comparable write for write.
fn diff_scoped(
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
    scope: Scope<'_>,
) -> Vec<StagedWrite> {
    let mut writes = Vec::new();
    diff_schemas(&mut writes, scope, base, state, new_snapshot);
    diff_tables(&mut writes, scope, base, state, new_snapshot);
    diff_views(&mut writes, scope, base, state, new_snapshot);
    diff_columns(&mut writes, scope, base, state, new_snapshot);
    diff_data_files(&mut writes, scope, base, state, new_snapshot);
    diff_delete_files(&mut writes, scope, base, state, new_snapshot);
    diff_partitions(&mut writes, scope, base, state, new_snapshot);
    diff_sorts(&mut writes, scope, base, state, new_snapshot);
    diff_indexes(&mut writes, scope, base, state, new_snapshot);
    diff_macros(&mut writes, scope, base, state, new_snapshot);
    diff_mappings(&mut writes, scope, base, state);
    diff_table_stats(&mut writes, scope, base, state);
    diff_table_column_stats(&mut writes, scope, base, state);
    diff_file_column_stats(&mut writes, scope, base, state);
    diff_options(&mut writes, scope, base, state);
    diff_tags(&mut writes, scope, base, state);
    diff_gc_files(&mut writes, scope, base, state);
    writes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(table_id: u64, data_file_id: u64, path: &str) -> proto::DataFileValue {
        proto::DataFileValue {
            table_id,
            data_file_id,
            path: path.to_string(),
            ..proto::DataFileValue::default()
        }
    }

    /// Two files change; only the recorded one is walked, so only its
    /// write is staged. The whole-catalog scope stages both — the
    /// difference is the narrowing itself, not the staging.
    #[test]
    fn a_touched_scope_walks_only_what_it_records() {
        let mut base = CatalogSnapshot::default();
        base.data_files.insert(
            7,
            [
                (1, file(7, 1, "one.parquet")),
                (2, file(7, 2, "two.parquet")),
            ]
            .into_iter()
            .collect(),
        );

        let mut state = base.clone();
        for (id, path) in [
            (1u64, "one-rewritten.parquet"),
            (2, "two-rewritten.parquet"),
        ] {
            if let Some(value) = state.data_files.get_mut(&7).and_then(|f| f.get_mut(&id)) {
                value.path = path.to_string();
            }
        }

        let mut touched = Touched::default();
        touched.touch(EntityKey::File {
            table_id: 7,
            data_file_id: 1,
        });

        let narrowed = diff_scoped(&base, &state, 9, Scope::Touched(&touched));
        let whole = diff_scoped(&base, &state, 9, Scope::All);

        // One history mint and one current put per changed file.
        assert_eq!(narrowed.len(), 2);
        assert_eq!(whole.len(), 4);
        let only_file_one = diff_scoped(
            &base,
            &{
                let mut one = base.clone();
                if let Some(value) = one.data_files.get_mut(&7).and_then(|f| f.get_mut(&1)) {
                    value.path = "one-rewritten.parquet".to_string();
                }
                one
            },
            9,
            Scope::All,
        );
        assert_eq!(narrowed, only_file_one);
    }
}
