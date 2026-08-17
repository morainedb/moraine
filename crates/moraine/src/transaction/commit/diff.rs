//! Turning a (base, staged) snapshot pair into the store writes that
//! transition one to the other: versioned kinds mint history mirrors,
//! unversioned kinds are overwritten in place.

use std::collections::{BTreeMap, BTreeSet};

use super::StagedWrite;
use crate::{
    catalog::CatalogSnapshot,
    store::{
        key::{CurrentKey, EntityKey, Key},
        proto, value,
    },
};

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

/// Stages the transition of every entity in one id-keyed map pair.
fn diff_versioned_map<K: Copy + Ord, M: prost::Message + Clone + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    base: &BTreeMap<K, M>,
    state: &BTreeMap<K, M>,
    make_key: impl Fn(K) -> EntityKey,
    new_snapshot: u64,
    set_end: impl Fn(&M) -> M,
) {
    for &id in base.keys().chain(state.keys()).collect::<BTreeSet<_>>() {
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

/// Stages the transition of every entity in one table-scoped nested map
/// pair (`table_id` → `id` → record).
fn diff_nested_versioned<K: Copy + Ord, M: prost::Message + Clone + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    base: &BTreeMap<u64, BTreeMap<K, M>>,
    state: &BTreeMap<u64, BTreeMap<K, M>>,
    make_key: impl Fn(u64, K) -> EntityKey,
    new_snapshot: u64,
    set_end: impl Fn(&M) -> M,
) {
    let empty = BTreeMap::new();
    for &table_id in base.keys().chain(state.keys()).collect::<BTreeSet<_>>() {
        diff_versioned_map(
            writes,
            base.get(&table_id).unwrap_or(&empty),
            state.get(&table_id).unwrap_or(&empty),
            |id| make_key(table_id, id),
            new_snapshot,
            &set_end,
        );
    }
}

/// Stages the in-place overwrite of every record in one id-keyed map pair.
fn diff_overwrite_map<K: Copy + Ord, M: prost::Message + PartialEq>(
    writes: &mut Vec<StagedWrite>,
    base: &BTreeMap<K, M>,
    state: &BTreeMap<K, M>,
    make_key: impl Fn(K) -> EntityKey,
) {
    for &id in base.keys().chain(state.keys()).collect::<BTreeSet<_>>() {
        stage_overwrite(writes, make_key(id), base.get(&id), state.get(&id));
    }
}

fn diff_schemas(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_versioned_map(
        writes,
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

    let table_ids = base.tables.keys().chain(state.tables.keys());
    for &table_id in table_ids.collect::<BTreeSet<_>>() {
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
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_versioned_map(
        writes,
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
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    diff_overwrite_map(
        writes,
        &base.options,
        &state.options,
        |(scope_kind, scope_id)| EntityKey::Option {
            scope_kind,
            scope_id,
        },
    );
}

fn diff_tags(writes: &mut Vec<StagedWrite>, base: &CatalogSnapshot, state: &CatalogSnapshot) {
    diff_overwrite_map(writes, &base.tags, &state.tags, |object_id| {
        EntityKey::Tag { object_id }
    });
}

/// Deletion-schedule rows under `current/gcfile`, overwritten or removed
/// in place.
fn diff_gc_files(writes: &mut Vec<StagedWrite>, base: &CatalogSnapshot, state: &CatalogSnapshot) {
    let file_ids = base.gc_files.keys().chain(state.gc_files.keys());
    for &data_file_id in file_ids.collect::<BTreeSet<_>>() {
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
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_versioned_map(
        writes,
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
fn diff_mappings(writes: &mut Vec<StagedWrite>, base: &CatalogSnapshot, state: &CatalogSnapshot) {
    for (&table_id, per_table) in &state.mappings {
        for (&mapping_id, value) in per_table {
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
}

fn diff_columns(
    writes: &mut Vec<StagedWrite>,
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
    let column_tables = base.columns.keys().chain(state.columns.keys());
    for &table_id in column_tables.collect::<BTreeSet<_>>() {
        let base_cols = base.columns.get(&table_id).unwrap_or(&empty);
        let state_cols = state.columns.get(&table_id).unwrap_or(&empty);
        for &column_id in base_cols
            .keys()
            .chain(state_cols.keys())
            .collect::<BTreeSet<_>>()
        {
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
}

fn diff_data_files(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
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
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
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
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
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
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
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
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
    new_snapshot: u64,
) {
    diff_nested_versioned(
        writes,
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
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    diff_overwrite_map(writes, &base.table_stats, &state.table_stats, |table_id| {
        EntityKey::TableStats { table_id }
    });
}

fn diff_table_column_stats(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    let empty = BTreeMap::new();
    let table_ids = base
        .table_column_stats
        .keys()
        .chain(state.table_column_stats.keys());
    for &table_id in table_ids.collect::<BTreeSet<_>>() {
        diff_overwrite_map(
            writes,
            base.table_column_stats.get(&table_id).unwrap_or(&empty),
            state.table_column_stats.get(&table_id).unwrap_or(&empty),
            |column_id| EntityKey::TableColumnStats {
                table_id,
                column_id,
            },
        );
    }
}

fn diff_file_column_stats(
    writes: &mut Vec<StagedWrite>,
    base: &CatalogSnapshot,
    state: &CatalogSnapshot,
) {
    let empty_stats = BTreeMap::new();
    let empty_files = BTreeMap::new();
    let table_ids = base
        .file_column_stats
        .keys()
        .chain(state.file_column_stats.keys());
    for &table_id in table_ids.collect::<BTreeSet<_>>() {
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
        for &(data_file_id, column_id) in base_cols
            .keys()
            .chain(state_cols.keys())
            .collect::<BTreeSet<_>>()
        {
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
    let mut writes = Vec::new();
    diff_schemas(&mut writes, base, state, new_snapshot);
    diff_tables(&mut writes, base, state, new_snapshot);
    diff_views(&mut writes, base, state, new_snapshot);
    diff_columns(&mut writes, base, state, new_snapshot);
    diff_data_files(&mut writes, base, state, new_snapshot);
    diff_delete_files(&mut writes, base, state, new_snapshot);
    diff_partitions(&mut writes, base, state, new_snapshot);
    diff_sorts(&mut writes, base, state, new_snapshot);
    diff_indexes(&mut writes, base, state, new_snapshot);
    diff_macros(&mut writes, base, state, new_snapshot);
    diff_mappings(&mut writes, base, state);
    diff_table_stats(&mut writes, base, state);
    diff_table_column_stats(&mut writes, base, state);
    diff_file_column_stats(&mut writes, base, state);
    diff_options(&mut writes, base, state);
    diff_tags(&mut writes, base, state);
    diff_gc_files(&mut writes, base, state);
    writes
}
