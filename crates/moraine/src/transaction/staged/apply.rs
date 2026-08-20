//! Snapshot application: replaying decoded row operations onto the
//! working `CatalogSnapshot`, and assembling the commit's snapshot
//! record.

use super::{
    BTreeSet, CatalogSnapshot, Cell, EntityKey, Error, HashMap, Key, Result, RowOperation,
    TableKind, commit,
    commit::Touched,
    corrupt_row,
    decode::{
        Cursor, StatsKey, decode_column, decode_column_mapping, decode_column_tag_row,
        decode_data_file, decode_delete_file, decode_delete_key, decode_end,
        decode_file_column_stats, decode_file_partition_value, decode_gc_file_row,
        decode_hard_delete, decode_macro, decode_macro_impl, decode_macro_parameter,
        decode_metadata, decode_metadata_key, decode_name_mapping, decode_partition_column,
        decode_partition_info, decode_schema, decode_sort_expression, decode_sort_info,
        decode_table, decode_table_column_stats, decode_table_stats, decode_tag_row, decode_view,
        table_value,
    },
    proto,
};

/// Child-table rows collected before the insert pass: each is folded
/// into its parent record when the parent's insert applies, and a
/// leftover after the pass means a child row named a parent this commit
/// never inserted — a shape error.
#[derive(Default)]
pub(super) struct ChildRows {
    pub(super) partition_columns: HashMap<u64, Vec<proto::PartitionColumn>>,
    pub(super) sort_expressions: HashMap<u64, Vec<proto::SortExpression>>,
    pub(super) file_partition_values: HashMap<(u64, u64), Vec<proto::FilePartitionValue>>,
    pub(super) macro_implementations: HashMap<u64, Vec<proto::MacroImplementation>>,
    pub(super) macro_parameters: HashMap<(u64, u64), Vec<proto::MacroParameter>>,
    pub(super) name_mappings: HashMap<u64, Vec<proto::NameMapping>>,
}

/// The entities this batch hard-deletes, gathered before anything is
/// applied. A hard delete leaves the working state untouched, so a parent
/// still reads as live there; embedded-child deletes consult this set
/// instead.
pub(super) fn collect_hard_deletes(ops: &[RowOperation]) -> Result<BTreeSet<EntityKey>> {
    ops.iter()
        .filter_map(|op| match op {
            RowOperation::Delete { table, cells }
                if matches!(
                    table,
                    TableKind::Schema
                        | TableKind::Table
                        | TableKind::View
                        | TableKind::Column
                        | TableKind::DataFile
                        | TableKind::DeleteFile
                        | TableKind::PartitionInfo
                        | TableKind::SortInfo
                        | TableKind::Macro
                        | TableKind::ColumnMapping
                ) =>
            {
                Some(decode_hard_delete(*table, cells).map(|(entity, _)| entity))
            }
            _ => None,
        })
        .collect()
}

pub(super) fn collect_child_rows(ops: &[RowOperation]) -> Result<ChildRows> {
    let mut children = ChildRows::default();

    for op in ops {
        if let RowOperation::Insert { table, cells } = op {
            match table {
                TableKind::PartitionColumn => {
                    let (partition_id, column) = decode_partition_column(cells)?;
                    children
                        .partition_columns
                        .entry(partition_id)
                        .or_default()
                        .push(column);
                }
                TableKind::SortExpression => {
                    let (sort_id, expression) = decode_sort_expression(cells)?;
                    children
                        .sort_expressions
                        .entry(sort_id)
                        .or_default()
                        .push(expression);
                }
                TableKind::FilePartitionValue => {
                    let (file, value) = decode_file_partition_value(cells)?;
                    children
                        .file_partition_values
                        .entry(file)
                        .or_default()
                        .push(value);
                }
                TableKind::MacroImpl => {
                    let (macro_id, implementation) = decode_macro_impl(cells)?;
                    children
                        .macro_implementations
                        .entry(macro_id)
                        .or_default()
                        .push(implementation);
                }
                TableKind::MacroParameters => {
                    let (key, parameter) = decode_macro_parameter(cells)?;
                    children
                        .macro_parameters
                        .entry(key)
                        .or_default()
                        .push(parameter);
                }
                TableKind::NameMapping => {
                    let (mapping_id, row) = decode_name_mapping(cells)?;
                    children
                        .name_mappings
                        .entry(mapping_id)
                        .or_default()
                        .push(row);
                }
                _ => {}
            }
        }
    }

    for columns in children.partition_columns.values_mut() {
        columns.sort_by_key(|c| c.partition_key_index);
    }

    for expressions in children.sort_expressions.values_mut() {
        expressions.sort_by_key(|e| e.sort_key_index);
    }

    for values in children.file_partition_values.values_mut() {
        values.sort_by_key(|v| v.partition_key_index);
    }

    for implementations in children.macro_implementations.values_mut() {
        implementations.sort_by_key(|i| i.impl_id);
    }

    for parameters in children.macro_parameters.values_mut() {
        parameters.sort_by_key(|p| p.column_id);
    }

    for rows in children.name_mappings.values_mut() {
        rows.sort_by_key(|r| r.column_id);
    }

    Ok(children)
}

/// Applies one staged row to the working snapshot. `new_id` is this
/// commit's own snapshot id, the only value an `UpdateSetEnd` row's
/// `end_snapshot` cell may carry.
#[allow(clippy::too_many_arguments)]
pub(super) fn apply_op(
    base: &CatalogSnapshot,
    state: &mut CatalogSnapshot,
    op: &RowOperation,
    new_id: u64,
    children: &mut ChildRows,
    direct: &mut Vec<commit::StagedWrite>,
    hard_deleted: &BTreeSet<EntityKey>,
    touched: &mut Touched,
) -> Result<()> {
    match op {
        RowOperation::Insert { table, cells } => {
            apply_insert(base, state, *table, cells, children, touched)
        }
        RowOperation::UpdateSetEnd { table, cells } => {
            apply_update_set_end(state, *table, cells, new_id, touched)
        }
        RowOperation::UpdateSetBegin { table, cells } => {
            apply_update_set_begin(base, state, *table, cells, new_id, touched)
        }
        RowOperation::Delete { table, cells } => {
            apply_delete(state, *table, cells, direct, hard_deleted, touched)
        }
        // Inline ops are translated by `translate_inline`, not the diff.
        RowOperation::InlineSchema { .. }
        | RowOperation::InlineInsert { .. }
        | RowOperation::InlineInlineDelete { .. }
        | RowOperation::InlineFileDelete { .. }
        | RowOperation::InlineFileDeleteRemove { .. }
        | RowOperation::InlineFlushDelete { .. }
        | RowOperation::InlineDrop { .. }
        | RowOperation::InlineSchemaDrop { .. } => Ok(()),
    }
}

/// Whether `op` is one of the inline variants `translate_inline` handles.
pub(super) fn is_inline_op(op: &RowOperation) -> bool {
    matches!(
        op,
        RowOperation::InlineSchema { .. }
            | RowOperation::InlineInsert { .. }
            | RowOperation::InlineInlineDelete { .. }
            | RowOperation::InlineFileDelete { .. }
            | RowOperation::InlineFileDeleteRemove { .. }
            | RowOperation::InlineFlushDelete { .. }
            | RowOperation::InlineDrop { .. }
            | RowOperation::InlineSchemaDrop { .. }
    )
}

/// Folds a `ducklake_macro` insert with its collected impl and parameter
/// rows into one record: at least one impl, ordinals contiguous from
/// zero, one `macro_type` across the macro.
pub(super) fn apply_macro_insert(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    children: &mut ChildRows,
    touched: &mut Touched,
) -> Result<()> {
    let mut value = decode_macro(cells)?;
    let mut implementations = children
        .macro_implementations
        .remove(&value.macro_id)
        .unwrap_or_default();
    if implementations.is_empty() {
        return Err(corrupt_row(
            TableKind::Macro,
            "a macro insert requires at least one macro_impl row in the same commit",
        ));
    }
    for (index, implementation) in implementations.iter().enumerate() {
        if implementation.impl_id != index as u64 {
            return Err(corrupt_row(
                TableKind::MacroImpl,
                "impl_id values must be contiguous from zero",
            ));
        }
        if implementation.macro_type != implementations[0].macro_type {
            return Err(corrupt_row(
                TableKind::MacroImpl,
                "all implementations of one macro must share a type",
            ));
        }
    }
    for implementation in &mut implementations {
        let parameters = children
            .macro_parameters
            .remove(&(value.macro_id, implementation.impl_id))
            .unwrap_or_default();
        for (index, parameter) in parameters.iter().enumerate() {
            if parameter.column_id != index as u64 {
                return Err(corrupt_row(
                    TableKind::MacroParameters,
                    "column_id values must be contiguous from zero",
                ));
            }
        }
        implementation.parameters = parameters;
    }
    value.implementations = implementations;
    touched.touch(EntityKey::Macro {
        macro_id: value.macro_id,
    });
    state.put_macro(value);

    Ok(())
}

/// Folds a `ducklake_column_mapping` insert with its collected
/// name-mapping rows into one record: at least one row, unique ordinals,
/// parents preceding children, and a mapping id never written before.
pub(super) fn apply_mapping_insert(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    children: &mut ChildRows,
    touched: &mut Touched,
) -> Result<()> {
    let mut value = decode_column_mapping(cells)?;
    let rows = children
        .name_mappings
        .remove(&value.mapping_id)
        .unwrap_or_default();
    if rows.is_empty() {
        return Err(corrupt_row(
            TableKind::ColumnMapping,
            "a column_mapping insert requires name_mapping rows in the same commit",
        ));
    }
    for (index, row) in rows.iter().enumerate() {
        if index > 0 && rows[index - 1].column_id == row.column_id {
            return Err(corrupt_row(
                TableKind::NameMapping,
                "column_id values must be unique within a mapping",
            ));
        }
        if row
            .parent_column
            .is_some_and(|parent| parent >= row.column_id)
        {
            return Err(corrupt_row(
                TableKind::NameMapping,
                "parent_column must reference an earlier column_id",
            ));
        }
    }
    if state
        .mappings
        .get(&value.table_id)
        .is_some_and(|per_table| per_table.contains_key(&value.mapping_id))
    {
        return Err(corrupt_row(
            TableKind::ColumnMapping,
            "mapping_id already exists for this table",
        ));
    }
    value.name_mappings = rows;
    touched.touch(EntityKey::Mapping {
        table_id: value.table_id,
        mapping_id: value.mapping_id,
    });
    state.put_mapping(value);

    Ok(())
}

/// Refuses an insert whose id already names a live row, for the kinds
/// DuckLake's schema declares a `PRIMARY KEY` on. Checked against the
/// working state (ends apply first, freeing the id). The message must not
/// contain `conflict`: DuckLake would retry, and re-driving reproduces the
/// row.
fn refuse_live_id(table: TableKind, live: bool, named: &str) -> Result<()> {
    if live {
        return Err(Error::Constraint(format!(
            "{table:?}: {named} is already live; DuckLake authors this id and \
             its own catalog would refuse the duplicate row"
        )));
    }
    Ok(())
}

fn apply_schema_insert(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    touched: &mut Touched,
) -> Result<()> {
    let value = decode_schema(cells)?;
    refuse_live_id(
        TableKind::Schema,
        state.schemas.contains_key(&value.schema_id),
        &format!("schema_id {}", value.schema_id),
    )?;
    touched.touch(EntityKey::Schema {
        schema_id: value.schema_id,
    });
    state.put_schema(value);
    Ok(())
}

fn apply_data_file_insert(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    children: &mut ChildRows,
    touched: &mut Touched,
) -> Result<()> {
    let mut value = decode_data_file(cells)?;
    refuse_live_id(
        TableKind::DataFile,
        state
            .data_files
            .get(&value.table_id)
            .is_some_and(|files| files.contains_key(&value.data_file_id)),
        &format!("data_file_id {}", value.data_file_id),
    )?;
    value.partition_values = children
        .file_partition_values
        .remove(&(value.table_id, value.data_file_id))
        .unwrap_or_default();
    touched.touch(EntityKey::File {
        table_id: value.table_id,
        data_file_id: value.data_file_id,
    });
    state.put_data_file(value);
    Ok(())
}

fn apply_delete_file_insert(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    touched: &mut Touched,
) -> Result<()> {
    let value = decode_delete_file(cells)?;
    refuse_live_id(
        TableKind::DeleteFile,
        state
            .delete_files
            .get(&value.table_id)
            .is_some_and(|files| files.contains_key(&value.delete_file_id)),
        &format!("delete_file_id {}", value.delete_file_id),
    )?;
    touched.touch(EntityKey::DeleteFile {
        table_id: value.table_id,
        delete_file_id: value.delete_file_id,
    });
    state.put_delete_file(value);
    Ok(())
}

/// Applies one tag insert: a container entry, or an entry embedded in
/// the column record it annotates.
fn apply_tag_insert(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    touched: &mut Touched,
) -> Result<()> {
    if table == TableKind::Tag {
        let (object_id, entry) = decode_tag_row(cells)?;
        touched.touch(EntityKey::Tag { object_id });
        state
            .tags
            .entry(object_id)
            .or_insert_with(|| proto::TagValue {
                object_id,
                entries: Vec::new(),
            })
            .entries
            .push(entry);
        return Ok(());
    }

    let ((table_id, column_id), tag) = decode_column_tag_row(cells)?;
    let Some(column) = state
        .columns
        .get_mut(&table_id)
        .and_then(|cols| cols.get_mut(&column_id))
    else {
        return Err(corrupt_row(
            table,
            format!("column tag names an absent column ({table_id}, {column_id})"),
        ));
    };
    column.tags.push(tag);
    touched.touch(EntityKey::Column {
        table_id,
        column_id,
    });
    Ok(())
}

/// Applies one unversioned statistics insert.
fn apply_stats_insert(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    touched: &mut Touched,
) -> Result<()> {
    match table {
        TableKind::TableStats => {
            let value = decode_table_stats(cells)?;
            touched.touch(EntityKey::TableStats {
                table_id: value.table_id,
            });
            state.put_table_stats(value);
        }
        TableKind::TableColumnStats => {
            let value = decode_table_column_stats(cells)?;
            touched.touch(EntityKey::TableColumnStats {
                table_id: value.table_id,
                column_id: value.column_id,
            });
            state.put_table_column_stats(value);
        }
        TableKind::FileColumnStats => {
            let value = decode_file_column_stats(cells)?;
            touched.touch(EntityKey::FileColumnStats {
                table_id: value.table_id,
                data_file_id: value.data_file_id,
                column_id: value.column_id,
            });
            state.put_file_column_stats(value);
        }
        _ => return Err(corrupt_row(table, "not a statistics kind")),
    }
    Ok(())
}

pub(super) fn apply_insert(
    base: &CatalogSnapshot,
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    children: &mut ChildRows,
    touched: &mut Touched,
) -> Result<()> {
    match table {
        // Snapshot rows fold into the snapshot record; child rows fold into
        // their parent records via `collect_child_rows`.
        TableKind::Snapshot
        | TableKind::SnapshotChanges
        | TableKind::SchemaVersions
        | TableKind::PartitionColumn
        | TableKind::SortExpression
        | TableKind::FilePartitionValue
        | TableKind::MacroImpl
        | TableKind::MacroParameters
        | TableKind::NameMapping => {}
        // An option row overwrites its key in the scope's record.
        TableKind::Metadata => {
            let (components, key, value) = decode_metadata(cells)?;
            touched.touch(EntityKey::Option {
                scope_kind: components.0,
                scope_id: components.1,
            });
            state
                .options
                .entry(components)
                .or_default()
                .options
                .insert(key, value);
        }
        TableKind::Schema => apply_schema_insert(state, cells, touched)?,
        TableKind::Table => {
            let value = table_value(base, decode_table(cells)?);
            touched.touch(EntityKey::Table {
                table_id: value.table_id,
            });
            state.put_table(value);
        }
        TableKind::View => {
            let value = decode_view(cells)?;
            touched.touch(EntityKey::View {
                view_id: value.view_id,
            });
            state.put_view(value);
        }
        TableKind::Column => {
            let mut value = decode_column(cells)?;
            crate::catalog::inline_policy::ensure_inlinable(
                &value.column_name,
                &value.column_type,
            )?;
            // Column tags outlive column versions: a new version carries
            // the prior version's entries forward.
            if let Some(prior) = base
                .columns
                .get(&value.table_id)
                .and_then(|cols| cols.get(&value.column_id))
            {
                value.tags.clone_from(&prior.tags);
            }
            touched.touch(EntityKey::Column {
                table_id: value.table_id,
                column_id: value.column_id,
            });
            state.put_column(value);
        }
        TableKind::DataFile => apply_data_file_insert(state, cells, children, touched)?,
        TableKind::DeleteFile => apply_delete_file_insert(state, cells, touched)?,
        TableKind::PartitionInfo => {
            let mut value = decode_partition_info(cells)?;
            value.columns = children
                .partition_columns
                .remove(&value.partition_id)
                .unwrap_or_default();
            touched.touch(EntityKey::Partition {
                table_id: value.table_id,
                partition_id: value.partition_id,
            });
            state.put_partition(value);
        }
        TableKind::SortInfo => {
            let mut value = decode_sort_info(cells)?;
            value.expressions = children
                .sort_expressions
                .remove(&value.sort_id)
                .unwrap_or_default();
            touched.touch(EntityKey::Sort {
                table_id: value.table_id,
                sort_id: value.sort_id,
            });
            state.put_sort(value);
        }
        TableKind::Macro => apply_macro_insert(state, cells, children, touched)?,
        TableKind::ColumnMapping => apply_mapping_insert(state, cells, children, touched)?,
        TableKind::TableStats | TableKind::TableColumnStats | TableKind::FileColumnStats => {
            apply_stats_insert(state, table, cells, touched)?;
        }
        TableKind::FilesScheduledForDeletion => {
            let value = decode_gc_file_row(cells)?;
            touched.touch_gc_file(value.data_file_id);
            state.put_gc_file(value);
        }
        TableKind::Tag | TableKind::ColumnTag => apply_tag_insert(state, table, cells, touched)?,
    }
    Ok(())
}

/// Ends the first entry of `entries` matching `is_match`, in place;
/// `false` means none matched.
pub(super) fn end_live_entry<E>(
    entries: &mut [E],
    is_match: impl Fn(&E) -> bool,
    set_end: impl Fn(&mut E),
) -> bool {
    for entry in entries.iter_mut() {
        if is_match(entry) {
            set_end(entry);
            return true;
        }
    }
    false
}

/// Rejects a set-end cell whose `end_snapshot` is not this commit's own
/// snapshot id.
pub(super) fn check_end_snapshot(table: TableKind, end_snapshot: u64, new_id: u64) -> Result<()> {
    if end_snapshot == new_id {
        Ok(())
    } else {
        Err(corrupt_row(
            table,
            format!(
                "end_snapshot {end_snapshot} does not match this commit's snapshot id {new_id}"
            ),
        ))
    }
}

/// Ends a `ducklake_tag` row: the live entry named by `(object_id, key)`
/// gets its `end_snapshot` set, in place — containers never move to
/// history.
pub(super) fn apply_tag_set_end(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    new_id: u64,
    touched: &mut Touched,
) -> Result<()> {
    let mut c = Cursor::new(TableKind::Tag, cells);
    let object_id = c.u64()?;
    let key = c.string()?;
    let end_snapshot = c.u64()?;
    c.finish()?;
    check_end_snapshot(TableKind::Tag, end_snapshot, new_id)?;

    touched.touch(EntityKey::Tag { object_id });
    let ended = state.tags.get_mut(&object_id).is_some_and(|container| {
        end_live_entry(
            &mut container.entries,
            |e| e.key == key && e.end_snapshot.is_none(),
            |e| e.end_snapshot = Some(new_id),
        )
    });
    if ended {
        Ok(())
    } else {
        Err(corrupt_row(
            TableKind::Tag,
            format!("no live tag entry ({object_id}, {key:?}) to end"),
        ))
    }
}

/// Ends a `ducklake_column_tag` row: the live entry named by
/// `(table_id, column_id, key)` on the current column record.
pub(super) fn apply_column_tag_set_end(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    new_id: u64,
    touched: &mut Touched,
) -> Result<()> {
    let mut c = Cursor::new(TableKind::ColumnTag, cells);
    let table_id = c.u64()?;
    let column_id = c.u64()?;
    let key = c.string()?;
    let end_snapshot = c.u64()?;
    c.finish()?;
    check_end_snapshot(TableKind::ColumnTag, end_snapshot, new_id)?;

    touched.touch(EntityKey::Column {
        table_id,
        column_id,
    });
    let ended = state
        .columns
        .get_mut(&table_id)
        .and_then(|cols| cols.get_mut(&column_id))
        .is_some_and(|column| {
            end_live_entry(
                &mut column.tags,
                |t| t.key == key && t.end_snapshot.is_none(),
                |t| t.end_snapshot = Some(new_id),
            )
        });
    if ended {
        Ok(())
    } else {
        Err(corrupt_row(
            TableKind::ColumnTag,
            format!("no live column tag ({table_id}, {column_id}, {key:?}) to end"),
        ))
    }
}

pub(super) fn apply_update_set_end(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    new_id: u64,
    touched: &mut Touched,
) -> Result<()> {
    match table {
        TableKind::Tag => return apply_tag_set_end(state, cells, new_id, touched),
        TableKind::ColumnTag => return apply_column_tag_set_end(state, cells, new_id, touched),
        _ => {}
    }

    let (key, end_snapshot) = decode_end(table, cells)?;
    check_end_snapshot(table, end_snapshot, new_id)?;
    touched.touch(key);
    // End only the one row named, never a cascade: a rename ends the table
    // row but keeps its columns live.
    let ended = match key {
        EntityKey::Schema { schema_id } => state.schemas.remove(&schema_id).is_some(),
        EntityKey::Table { table_id } => state.tables.remove(&table_id).is_some(),
        EntityKey::View { view_id } => state.views.remove(&view_id).is_some(),
        EntityKey::Column {
            table_id,
            column_id,
        } => state
            .columns
            .get_mut(&table_id)
            .is_some_and(|columns| columns.remove(&column_id).is_some()),
        EntityKey::File {
            table_id,
            data_file_id,
        } => state
            .data_files
            .get_mut(&table_id)
            .is_some_and(|files| files.remove(&data_file_id).is_some()),
        EntityKey::DeleteFile {
            table_id,
            delete_file_id,
        } => state
            .delete_files
            .get_mut(&table_id)
            .is_some_and(|files| files.remove(&delete_file_id).is_some()),
        EntityKey::Partition {
            table_id,
            partition_id,
        } => {
            let live = state
                .partitions
                .get(&table_id)
                .is_some_and(|specs| specs.contains_key(&partition_id));
            state.delete_partition(table_id, partition_id);
            live
        }
        EntityKey::Sort { table_id, sort_id } => {
            let live = state
                .sorts
                .get(&table_id)
                .is_some_and(|specs| specs.contains_key(&sort_id));
            state.delete_sort(table_id, sort_id);
            live
        }
        EntityKey::Macro { macro_id } => state.macros.remove(&macro_id).is_some(),
        // decode_end only ever returns the keys matched above.
        _ => return Err(corrupt_row(table, "unreachable entity key")),
    };
    if !ended {
        return Err(corrupt_row(
            table,
            format!("no live row to end for {key:?}"),
        ));
    }

    Ok(())
}

/// Rebases a data file's `begin_snapshot` in place. The target must have
/// been inserted by this same transaction (absent from `base`).
pub(super) fn apply_update_set_begin(
    base: &CatalogSnapshot,
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    new_id: u64,
    touched: &mut Touched,
) -> Result<()> {
    if table != TableKind::DataFile {
        return Err(Error::Constraint(format!(
            "update_set_begin is not defined for {table:?}"
        )));
    }
    let mut c = Cursor::new(table, cells);
    let table_id = c.u64()?;
    let data_file_id = c.u64()?;
    let begin_snapshot = c.u64()?;
    c.finish()?;
    if begin_snapshot != new_id {
        return Err(corrupt_row(
            table,
            format!(
                "begin_snapshot {begin_snapshot} does not match this commit's snapshot id {new_id}"
            ),
        ));
    }
    if base
        .data_files
        .get(&table_id)
        .is_some_and(|files| files.contains_key(&data_file_id))
    {
        return Err(corrupt_row(
            table,
            format!("file ({table_id}, {data_file_id}) predates this commit and cannot be rebased"),
        ));
    }

    touched.touch(EntityKey::File {
        table_id,
        data_file_id,
    });
    let Some(file) = state
        .data_files
        .get_mut(&table_id)
        .and_then(|files| files.get_mut(&data_file_id))
    else {
        return Err(corrupt_row(
            table,
            format!("no data file ({table_id}, {data_file_id}) to rebase"),
        ));
    };
    file.begin_snapshot = begin_snapshot;
    Ok(())
}

/// Removes a row from the physical-deletion schedule; a missing entry is
/// an error.
fn apply_schedule_delete(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    touched: &mut Touched,
) -> Result<()> {
    let mut c = Cursor::new(table, cells);
    let data_file_id = c.u64()?;
    c.finish()?;
    touched.touch_gc_file(data_file_id);
    if state.gc_files.remove(&data_file_id).is_none() {
        return Err(corrupt_row(
            table,
            format!("no scheduled deletion for file {data_file_id}"),
        ));
    }
    Ok(())
}

/// Removes an option row's key from its scope, and the scope's record with
/// it once the last key goes. An absent key is a no-op.
fn apply_option_delete(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    touched: &mut Touched,
) -> Result<()> {
    let (components, key) = decode_metadata_key(cells)?;
    touched.touch(EntityKey::Option {
        scope_kind: components.0,
        scope_id: components.1,
    });
    let Some(record) = state.options.get_mut(&components) else {
        return Ok(());
    };
    record.options.remove(&key);
    if record.options.is_empty() {
        state.remove_option_record(components);
    }
    Ok(())
}

/// Deletes one `schema_version` record; the copy inside the snapshot
/// record is left for expiry.
fn apply_schema_version_delete(
    cells: &[Cell],
    direct: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    let mut c = Cursor::new(TableKind::SchemaVersions, cells);
    let begin_snapshot = c.u64()?;
    let _schema_version = c.u64()?;
    let table_id = c.u64()?;
    c.finish()?;

    direct.push((
        Key::SchemaVersion {
            table_id,
            begin_snapshot,
        }
        .encode(),
        None,
    ));
    Ok(())
}

/// A raw `DELETE` row. Unversioned records (statistics, the deletion
/// schedule, options) leave the working state and reach the store through
/// the diff; versioned rows and snapshot records are pruned with direct
/// key deletes; embedded rows (tag entries, spec columns) rewrite or ride
/// their parent.
pub(super) fn apply_delete(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    direct: &mut Vec<commit::StagedWrite>,
    hard_deleted: &BTreeSet<EntityKey>,
    touched: &mut Touched,
) -> Result<()> {
    match table {
        TableKind::TableStats | TableKind::TableColumnStats | TableKind::FileColumnStats => {
            apply_stats_delete(state, table, cells, touched)
        }
        TableKind::FilesScheduledForDeletion => apply_schedule_delete(state, table, cells, touched),
        // The paired `ducklake_snapshot_changes` delete stages nothing.
        TableKind::Snapshot => {
            let mut c = Cursor::new(table, cells);
            let snapshot_id = c.u64()?;
            c.finish()?;
            if snapshot_id == state.snapshot.snapshot_id {
                return Err(Error::Constraint(format!(
                    "snapshot {snapshot_id} is the head and cannot be expired"
                )));
            }
            direct.push((Key::Snapshot { snapshot_id }.encode(), None));
            Ok(())
        }
        TableKind::SnapshotChanges => {
            let mut c = Cursor::new(table, cells);
            let _snapshot_id = c.u64()?;
            c.finish()?;
            Ok(())
        }
        TableKind::Metadata => apply_option_delete(state, cells, touched),
        // Mappings have no history mirror: a direct `current` key delete.
        TableKind::ColumnMapping => {
            let mut c = Cursor::new(table, cells);
            let mapping_id = c.u64()?;
            let table_id = c.u64()?;
            c.finish()?;
            direct.push((
                Key::current(EntityKey::Mapping {
                    table_id,
                    mapping_id,
                })
                .encode(),
                None,
            ));
            Ok(())
        }
        TableKind::Schema
        | TableKind::Table
        | TableKind::View
        | TableKind::Column
        | TableKind::DataFile
        | TableKind::DeleteFile
        | TableKind::PartitionInfo
        | TableKind::SortInfo
        | TableKind::Macro => {
            let (entity, end_snapshot) = decode_hard_delete(table, cells)?;
            let key = match end_snapshot {
                Some(end) => Key::history(entity, end),
                None => Key::current(entity),
            };
            // The working state is left alone: removing the row would make
            // the diff stage an end-transition on top of this delete.
            direct.push((key.encode(), None));
            Ok(())
        }
        TableKind::Tag => apply_tag_delete(state, cells, touched),
        // A column-tag entry on a still-current column rewrites the column
        // in place; on a pruned column there is nothing to rewrite.
        TableKind::ColumnTag => {
            let mut c = Cursor::new(table, cells);
            let table_id = c.u64()?;
            let column_id = c.u64()?;
            let key = c.string()?;
            let begin_snapshot = c.u64()?;
            c.finish()?;
            touched.touch(EntityKey::Column {
                table_id,
                column_id,
            });
            if let Some(column) = state
                .columns
                .get_mut(&table_id)
                .and_then(|cols| cols.get_mut(&column_id))
            {
                column
                    .tags
                    .retain(|t| !(t.key == key && t.begin_snapshot == begin_snapshot));
            }
            Ok(())
        }
        TableKind::PartitionColumn
        | TableKind::SortExpression
        | TableKind::FilePartitionValue
        | TableKind::MacroImpl
        | TableKind::MacroParameters
        | TableKind::NameMapping => apply_embedded_delete(state, table, cells, hard_deleted),
        TableKind::SchemaVersions => apply_schema_version_delete(cells, direct),
    }
}

/// Removes a dead `ducklake_tag` entry from its container; a container
/// left empty is removed outright.
pub(super) fn apply_tag_delete(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    touched: &mut Touched,
) -> Result<()> {
    let mut c = Cursor::new(TableKind::Tag, cells);
    let object_id = c.u64()?;
    let key = c.string()?;
    let begin_snapshot = c.u64()?;
    c.finish()?;

    touched.touch(EntityKey::Tag { object_id });

    let removed = state.tags.get_mut(&object_id).is_some_and(|container| {
        let before = container.entries.len();
        container
            .entries
            .retain(|e| !(e.key == key && e.begin_snapshot == begin_snapshot));
        container.entries.len() < before
    });
    if !removed {
        return Err(corrupt_row(
            TableKind::Tag,
            format!("no tag entry ({object_id}, {key:?}, {begin_snapshot}) to delete"),
        ));
    }
    if state
        .tags
        .get(&object_id)
        .is_some_and(|container| container.entries.is_empty())
    {
        state.tags.remove(&object_id);
    }
    Ok(())
}

/// An embedded-row delete: the parent must be dead already or dying in
/// this batch, since an embedded row cannot die alone.
pub(super) fn apply_embedded_delete(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    hard_deleted: &BTreeSet<EntityKey>,
) -> Result<()> {
    let mut c = Cursor::new(table, cells);
    let parent = match table {
        TableKind::PartitionColumn => {
            let partition_id = c.u64()?;
            let table_id = c.u64()?;
            let live = state
                .partitions
                .get(&table_id)
                .is_some_and(|specs| specs.contains_key(&partition_id));
            (
                live,
                EntityKey::Partition {
                    table_id,
                    partition_id,
                },
            )
        }
        TableKind::SortExpression => {
            let sort_id = c.u64()?;
            let table_id = c.u64()?;
            let live = state
                .sorts
                .get(&table_id)
                .is_some_and(|specs| specs.contains_key(&sort_id));
            (live, EntityKey::Sort { table_id, sort_id })
        }
        TableKind::FilePartitionValue => {
            let data_file_id = c.u64()?;
            let table_id = c.u64()?;
            let live = state
                .data_files
                .get(&table_id)
                .is_some_and(|files| files.contains_key(&data_file_id));
            (
                live,
                EntityKey::File {
                    table_id,
                    data_file_id,
                },
            )
        }
        TableKind::MacroImpl | TableKind::MacroParameters => {
            let macro_id = c.u64()?;
            (
                state.macros.contains_key(&macro_id),
                EntityKey::Macro { macro_id },
            )
        }
        TableKind::NameMapping => {
            let mapping_id = c.u64()?;
            let owner = state
                .mappings
                .iter()
                .find(|(_, per_table)| per_table.contains_key(&mapping_id))
                .map(|(table_id, _)| *table_id);
            (
                owner.is_some(),
                EntityKey::Mapping {
                    table_id: owner.unwrap_or_default(),
                    mapping_id,
                },
            )
        }
        _ => return Err(corrupt_row(table, "not an embedded kind")),
    };
    let (parent_is_current, parent_key) = parent;
    if parent_is_current && !hard_deleted.contains(&parent_key) {
        return Err(corrupt_row(
            table,
            "embedded row deleted while its parent record is still live",
        ));
    }
    Ok(())
}

/// Removes a statistics row; an absent row is a no-op, since dead-table
/// cleanup deletes by table id without reading first.
pub(super) fn apply_stats_delete(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    touched: &mut Touched,
) -> Result<()> {
    match decode_delete_key(table, cells)? {
        StatsKey::Table(table_id) => {
            touched.touch(EntityKey::TableStats { table_id });
            state.table_stats.remove(&table_id);
        }
        StatsKey::Column(table_id, column_id) => {
            touched.touch(EntityKey::TableColumnStats {
                table_id,
                column_id,
            });
            if let Some(cols) = state.table_column_stats.get_mut(&table_id) {
                cols.remove(&column_id);
            }
        }
        StatsKey::FileColumn(table_id, data_file_id, column_id) => {
            touched.touch(EntityKey::FileColumnStats {
                table_id,
                data_file_id,
                column_id,
            });
            if let Some(cols) = state.file_column_stats.get_mut(&table_id) {
                cols.remove(&(data_file_id, column_id));
            }
        }
    }
    Ok(())
}

/// The `ducklake_snapshot` and `ducklake_snapshot_changes` rows DuckLake
/// always inserts as one pair; both are required for a staged commit.
pub(super) fn find_snapshot_rows(ops: &[RowOperation]) -> Result<(&[Cell], &[Cell])> {
    let mut snapshot = None;
    let mut changes = None;
    for op in ops {
        if let RowOperation::Insert { table, cells } = op {
            match table {
                TableKind::Snapshot => snapshot = Some(cells.as_slice()),
                TableKind::SnapshotChanges => changes = Some(cells.as_slice()),
                _ => {}
            }
        }
    }
    let snapshot = snapshot.ok_or_else(|| {
        Error::Constraint("staged commit requires a ducklake_snapshot insert".to_string())
    })?;
    let changes = changes.ok_or_else(|| {
        Error::Constraint("staged commit requires a ducklake_snapshot_changes insert".to_string())
    })?;
    Ok((snapshot, changes))
}

pub(super) fn build_snapshot_value(ops: &[RowOperation]) -> Result<proto::SnapshotValue> {
    let (snapshot_cells, changes_cells) = find_snapshot_rows(ops)?;

    let mut s = Cursor::new(TableKind::Snapshot, snapshot_cells);
    let snapshot_id = s.u64()?;
    let snapshot_time_micros = s.i64()?;
    let schema_version = s.u64()?;
    let next_catalog_id = s.u64()?;
    let next_file_id = s.u64()?;
    s.finish()?;

    let mut c = Cursor::new(TableKind::SnapshotChanges, changes_cells);
    let changes_snapshot_id = c.u64()?;
    let changes_made = c.string()?;
    let author = c.opt_string()?;
    let commit_message = c.opt_string()?;
    let commit_extra_info = c.opt_string()?;
    c.finish()?;

    if changes_snapshot_id != snapshot_id {
        return Err(corrupt_row(
            TableKind::SnapshotChanges,
            format!(
                "snapshot_id {changes_snapshot_id} does not match ducklake_snapshot's {snapshot_id}"
            ),
        ));
    }

    // `ducklake_schema_versions` rows must carry this commit's own
    // snapshot id and schema version.
    let schema_changed_table_ids = ops
        .iter()
        .filter_map(|op| match op {
            RowOperation::Insert {
                table: TableKind::SchemaVersions,
                cells,
            } => Some(cells),
            _ => None,
        })
        .map(|cells| {
            let mut cursor = Cursor::new(TableKind::SchemaVersions, cells);
            let begin_snapshot = cursor.u64()?;
            let row_schema_version = cursor.u64()?;
            let table_id = cursor.u64()?;
            cursor.finish()?;
            if begin_snapshot != snapshot_id || row_schema_version != schema_version {
                return Err(corrupt_row(
                    TableKind::SchemaVersions,
                    format!(
                        "(begin_snapshot {begin_snapshot}, schema_version {row_schema_version}) \
                         does not match ducklake_snapshot's ({snapshot_id}, {schema_version})"
                    ),
                ));
            }
            Ok(table_id)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(proto::SnapshotValue {
        snapshot_id,
        snapshot_time_micros,
        schema_version,
        next_catalog_id,
        next_file_id,
        changes_made,
        author,
        commit_message,
        commit_extra_info,
        schema_changed_table_ids,
        transaction_id: None,
        // DuckLake names only the tables it deleted from; a later commit
        // classifies against this snapshot at table grain.
        deleted_data_file_ids: Vec::new(),
    })
}
