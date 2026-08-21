//! The mutation handle passed to a commit closure.

use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    ops::Deref,
};

use uuid::Uuid;

use crate::{
    catalog::{
        CatalogSnapshot, ColumnAlteration, ColumnDef, ColumnId, ColumnOrder, ColumnStats, DataFile,
        DataFileId, DeleteFile, DeleteFileId, FileIndexEntry, FileIndexRemoval, FlushedDataFile,
        IndexDef, IndexEntry, IndexId, IndexMaintenance, IndexState, InlineChunk, MacroId,
        MacroImplementationDef, OptionScope, PartitionColumnDef, PartitionId, SchemaId, SnapshotId,
        SortId, SortKeyDef, TableId, TagTarget, ViewId, inline_policy::ensure_inlinable,
    },
    error::{Error, Result},
    store::{
        index_encoding::{Direction, IndexKeyValue, NullOrder, encode_ordered_index_entry},
        proto::{
            ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue, FilePartitionValue,
            IndexValue, MacroImplementation, MacroParameter, MacroValue, PartitionColumn,
            PartitionValue, SchemaValue, SortExpression, SortValue, TableColumnStatsValue,
            TableStatsValue, TableValue, TagEntry, TagValue, ViewValue,
        },
    },
    transaction::{
        index_maintenance::StagedIndexEntry, inline::InlineStage, operations::Operation,
    },
};

/// What staging an entry against an index needs to know about it.
struct IndexShape {
    /// Whether the index enforces uniqueness.
    unique: bool,
    /// How many values an entry must carry.
    column_count: usize,
    /// The declared per-column sort directions; empty means all ascending.
    directions: Vec<Direction>,
    /// The declared per-column null orders; empty means all NULLS LAST.
    nulls: Vec<NullOrder>,
    /// Whether a staged build is still running.
    building: bool,
}

impl IndexShape {
    /// The shape of a stored index definition. Orders the record omits are
    /// ascending / NULLS LAST.
    fn of_value(value: &IndexValue) -> Self {
        Self {
            unique: value.unique,
            column_count: value.column_ids.len(),
            directions: value
                .column_descending
                .iter()
                .map(|&descending| {
                    if descending {
                        Direction::Descending
                    } else {
                        Direction::Ascending
                    }
                })
                .collect(),
            nulls: value
                .column_nulls_first
                .iter()
                .map(|&first| {
                    if first {
                        NullOrder::First
                    } else {
                        NullOrder::Last
                    }
                })
                .collect(),
            building: value.build_state.is_some(),
        }
    }

    /// The shape of an index being created from its definition and orders.
    fn of_definition(def: &IndexDef, orders: &[ColumnOrder]) -> Self {
        Self {
            unique: def.unique,
            column_count: def.columns.len(),
            directions: orders.iter().map(|order| order.direction).collect(),
            nulls: orders.iter().map(|order| order.nulls).collect(),
            building: false,
        }
    }
}

/// The mutation handle a commit closure receives.
///
/// Dereferences to [`CatalogSnapshot`]; reads observe the transaction's
/// own staged mutations. Nothing touches the store until the closure
/// returns.
pub struct Transaction {
    state: CatalogSnapshot,
    ops: Vec<Operation>,
    index_entries: Vec<StagedIndexEntry>,
    inline_ops: Vec<InlineStage>,
    /// Next `chunk_seq` per `(table_id, schema_version)`.
    chunk_seqs: HashMap<(u64, u64), u64>,
    /// Per table, the row-id counter as it stood before this commit first
    /// allocated from it.
    inherited_row_ids: HashMap<u64, u64>,
    next_catalog_id: u64,
    next_file_id: u64,
    new_snapshot_id: u64,
}

/// A finished transaction's accumulated products, consumed by the commit
/// protocol.
pub(crate) struct TransactionParts {
    pub(crate) operations: Vec<Operation>,
    pub(crate) index_entries: Vec<StagedIndexEntry>,
    pub(crate) inline_ops: Vec<InlineStage>,
    pub(crate) state: CatalogSnapshot,
    pub(crate) next_catalog_id: u64,
    pub(crate) next_file_id: u64,
}

impl Deref for Transaction {
    type Target = CatalogSnapshot;

    fn deref(&self) -> &CatalogSnapshot {
        &self.state
    }
}

impl Transaction {
    pub(crate) fn new(state: CatalogSnapshot, new_snapshot_id: u64) -> Self {
        let next_catalog_id = state.snapshot.next_catalog_id;
        let next_file_id = state.snapshot.next_file_id;

        Self {
            state,
            ops: Vec::new(),
            index_entries: Vec::new(),
            inline_ops: Vec::new(),
            chunk_seqs: HashMap::new(),
            inherited_row_ids: HashMap::new(),
            next_catalog_id,
            next_file_id,
            new_snapshot_id,
        }
    }

    pub(crate) fn into_parts(self) -> TransactionParts {
        TransactionParts {
            operations: self.ops,
            index_entries: self.index_entries,
            inline_ops: self.inline_ops,
            state: self.state,
            next_catalog_id: self.next_catalog_id,
            next_file_id: self.next_file_id,
        }
    }

    fn alloc_catalog_id(&mut self) -> u64 {
        let id = self.next_catalog_id;
        self.next_catalog_id += 1;
        id
    }

    fn alloc_file_id(&mut self) -> u64 {
        let id = self.next_file_id;
        self.next_file_id += 1;
        id
    }

    /// Creates a schema.
    ///
    /// # Errors
    ///
    /// Returns [`Error::AlreadyExists`] if a schema with that name already
    /// exists, or [`Error::Constraint`] if the name is empty or unsafe in
    /// the storage path derived from it.
    pub fn create_schema(&mut self, name: &str) -> Result<SchemaId> {
        path_safe_name("schema", name)?;
        if self.state.schema_names.contains_key(name) {
            return Err(Error::AlreadyExists(format!("schema {name}")));
        }
        let schema_id = self.alloc_catalog_id();
        self.state.put_schema(SchemaValue {
            schema_id,
            schema_uuid: Uuid::new_v4().to_string(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            schema_name: name.to_owned(),
            path: format!("{name}/"),
            path_is_relative: true,
        });
        self.ops.push(Operation::CreateSchema {
            schema_id,
            name: name.to_owned(),
        });
        Ok(SchemaId::new(schema_id))
    }

    /// Drops a schema.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the schema does not exist.
    /// Returns [`Error::Constraint`] if the schema still contains live
    /// tables, views or macros, or is the bootstrap `main` schema.
    pub fn drop_schema(&mut self, schema: SchemaId) -> Result<()> {
        // Schema id 0 is the bootstrap `main` schema.
        if schema.get() == 0 {
            return Err(Error::Constraint(
                "the bootstrap `main` schema cannot be dropped".to_string(),
            ));
        }
        if !self.state.schemas.contains_key(&schema.get()) {
            return Err(Error::NotFound(format!("schema {schema}")));
        }
        let occupied = [
            (
                "tables",
                self.state
                    .tables
                    .values()
                    .any(|t| t.schema_id == schema.get()),
            ),
            (
                "views",
                self.state
                    .views
                    .values()
                    .any(|v| v.schema_id == schema.get()),
            ),
            (
                "macros",
                self.state
                    .macros
                    .values()
                    .any(|m| m.schema_id == schema.get()),
            ),
        ];
        if let Some((kind, _)) = occupied.iter().find(|(_, taken)| *taken) {
            return Err(Error::Constraint(format!(
                "schema {schema} still contains {kind}"
            )));
        }
        self.state.delete_schema(schema.get());
        self.ops.push(Operation::DropSchema {
            schema_id: schema.get(),
        });
        Ok(())
    }

    /// Creates a table with its columns.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the schema does not exist.
    /// Returns [`Error::AlreadyExists`] if a table with that name already
    /// exists in the schema.
    /// Returns [`Error::Constraint`] if the column list is empty or contains
    /// duplicate column names.
    /// Returns [`Error::Unsupported`] if a column's type is one moraine
    /// cannot store.
    pub fn create_table(
        &mut self,
        schema: SchemaId,
        name: &str,
        columns: &[ColumnDef],
    ) -> Result<TableId> {
        if !self.state.schemas.contains_key(&schema.get()) {
            return Err(Error::NotFound(format!("schema {schema}")));
        }

        path_safe_name("table", name)?;
        self.relation_name_free(schema.get(), name)?;
        if columns.is_empty() {
            return Err(Error::Constraint(format!(
                "table {name} needs at least one column"
            )));
        }
        validate_column_defs(columns)?;

        let table_id = self.alloc_catalog_id();
        self.state.put_table(TableValue {
            table_id,
            table_uuid: Uuid::new_v4().to_string(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            schema_id: schema.get(),
            table_name: name.to_owned(),
            path: format!("{name}/"),
            path_is_relative: true,
            next_column_id: column_node_count(columns) + 1,
        });
        // Field ids and positions are both assigned from 1 in pre-order.
        let mut next_id = 1;
        let mut next_order = 1;
        for def in columns {
            self.stage_column_tree(table_id, def, None, &mut next_id, &mut next_order);
        }
        self.state.put_table_stats(TableStatsValue {
            table_id,
            record_count: 0,
            next_row_id: 0,
            file_size_bytes: 0,
        });
        let schema_name = self.state.schemas[&schema.get()].schema_name.clone();
        self.ops.push(Operation::CreateTable {
            schema_id: schema.get(),
            table_id,
            schema_name,
            table_name: name.to_owned(),
        });
        Ok(TableId::new(table_id))
    }

    fn live_table(&self, table: TableId) -> Result<TableValue> {
        self.state
            .tables
            .get(&table.get())
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("table {table}")))
    }

    /// Tables and views share one name namespace per schema.
    fn relation_name_free(&self, schema_id: u64, name: &str) -> Result<()> {
        let taken = |names: &crate::catalog::ScopedNames| {
            names
                .get(&schema_id)
                .is_some_and(|per_schema| per_schema.contains_key(name))
        };
        if taken(&self.state.table_names) || taken(&self.state.view_names) {
            return Err(Error::AlreadyExists(format!("relation {name}")));
        }
        Ok(())
    }

    fn mark_altered(&mut self, table_id: u64) {
        self.ops.push(Operation::AlterTable { table_id });
    }

    /// Renames a table within its schema. Renaming to the current name
    /// errors, matching SQL engines.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist.
    /// Returns [`Error::AlreadyExists`] if a table with that name already
    /// exists in the same schema (including this table itself).
    pub fn rename_table(&mut self, table: TableId, new_name: &str) -> Result<()> {
        path_safe_name("table", new_name)?;
        let value = self.live_table(table)?;
        self.relation_name_free(value.schema_id, new_name)?;
        self.state.put_table(TableValue {
            table_name: new_name.to_owned(),
            begin_snapshot: self.new_snapshot_id,
            ..value
        });
        self.mark_altered(table.get());

        Ok(())
    }

    /// Moves a table to another schema. Moving to the current schema
    /// errors, matching SQL engines.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or target schema does not
    /// exist.
    /// Returns [`Error::AlreadyExists`] if a table with the same name
    /// already exists in the target schema (including this table itself
    /// when the target is its current schema).
    pub fn set_table_schema(&mut self, table: TableId, new_schema: SchemaId) -> Result<()> {
        if !self.state.schemas.contains_key(&new_schema.get()) {
            return Err(Error::NotFound(format!("schema {new_schema}")));
        }
        let value = self.live_table(table)?;
        self.relation_name_free(new_schema.get(), &value.table_name)?;
        self.state.put_table(TableValue {
            schema_id: new_schema.get(),
            begin_snapshot: self.new_snapshot_id,
            ..value
        });
        self.mark_altered(table.get());
        Ok(())
    }

    /// Drops a table and its columns.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist.
    pub fn drop_table(&mut self, table: TableId) -> Result<()> {
        self.live_table(table)?;
        self.state.delete_table(table.get());
        self.ops.push(Operation::DropTable {
            table_id: table.get(),
        });

        Ok(())
    }

    /// Adds a column. Field ids and positions are never reused.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist.
    /// Returns [`Error::AlreadyExists`] if a column with that name already
    /// exists in the table.
    /// Returns [`Error::Unsupported`] if the column's type is one moraine
    /// cannot store.
    pub fn add_column(&mut self, table: TableId, def: &ColumnDef) -> Result<ColumnId> {
        validate_column_defs(std::slice::from_ref(def))?;
        let value = self.live_table(table)?;
        self.sibling_name_free(table, None, &def.name)?;
        let live_columns = self.state.columns.get(&table.get());
        let live_max_id = live_columns
            .and_then(|cols| cols.keys().max())
            .copied()
            .unwrap_or(0);
        let mut next_id = value.next_column_id.max(live_max_id + 1);
        // Positions start at 1 and continue past the highest live one.
        let mut next_order = live_columns
            .and_then(|cols| cols.values().map(|c| c.column_order).max())
            .map_or(1, |max| max + 1);
        let column_id =
            self.stage_column_tree(table.get(), def, None, &mut next_id, &mut next_order);
        self.state.put_table(TableValue {
            next_column_id: next_id,
            begin_snapshot: self.new_snapshot_id,
            ..value
        });
        self.mark_altered(table.get());

        Ok(ColumnId::new(column_id))
    }

    /// Stages one column and, in pre-order, every field beneath it,
    /// advancing both counters. Returns the root's field id.
    fn stage_column_tree(
        &mut self,
        table_id: u64,
        def: &ColumnDef,
        parent_column: Option<u64>,
        next_id: &mut u64,
        next_order: &mut u64,
    ) -> u64 {
        let column_id = *next_id;
        let column_order = *next_order;
        *next_id += 1;
        *next_order += 1;
        self.state.put_column(new_column(
            table_id,
            column_id,
            column_order,
            self.new_snapshot_id,
            parent_column,
            def,
        ));
        for child in &def.children {
            self.stage_column_tree(table_id, child, Some(column_id), next_id, next_order);
        }

        column_id
    }

    fn live_column(&self, table: TableId, column: ColumnId) -> Result<ColumnValue> {
        self.state
            .columns
            .get(&table.get())
            .and_then(|cols| cols.get(&column.get()))
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("column {column} of table {table}")))
    }

    /// Whether a live index of the table references this column by field id.
    fn column_is_indexed(&self, table: TableId, column: ColumnId) -> bool {
        self.state
            .indexes
            .get(&table.get())
            .is_some_and(|per_table| {
                per_table
                    .values()
                    .any(|index| index.column_ids.contains(&column.get()))
            })
    }

    /// Refuses a name already taken among `parent`'s fields — the table's
    /// top-level columns when `parent` is `None`.
    fn sibling_name_free(&self, table: TableId, parent: Option<u64>, name: &str) -> Result<()> {
        let taken = self.state.columns.get(&table.get()).is_some_and(|cols| {
            cols.values()
                .any(|c| c.parent_column == parent && c.column_name == name)
        });
        if taken {
            return Err(Error::AlreadyExists(format!("column {name}")));
        }

        Ok(())
    }

    /// Renames a column. Renaming to the current name errors, matching
    /// SQL engines.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or column does not exist.
    /// Returns [`Error::AlreadyExists`] if a column with that name already
    /// exists in the table (including this column itself).
    pub fn rename_column(
        &mut self,
        table: TableId,
        column: ColumnId,
        new_name: &str,
    ) -> Result<()> {
        nonempty_name("column", new_name)?;
        let current = self.live_column(table, column)?;
        self.sibling_name_free(table, current.parent_column, new_name)?;
        let value = ColumnValue {
            begin_snapshot: self.new_snapshot_id,
            column_name: new_name.to_string(),
            ..current
        };
        self.state.put_column(value);
        self.mark_altered(table.get());

        Ok(())
    }

    /// Alters a column's type, nullability, and/or default.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or column does not exist.
    /// Returns [`Error::Unsupported`] if the new type is one moraine cannot
    /// store.
    pub fn alter_column(
        &mut self,
        table: TableId,
        column: ColumnId,
        alteration: ColumnAlteration,
    ) -> Result<()> {
        let value = self.live_column(table, column)?;
        let ColumnAlteration {
            column_type,
            nulls_allowed,
            default_value,
        } = alteration;
        if let Some(new_type) = &column_type {
            ensure_inlinable(&value.column_name, new_type)?;
        }
        // The index encoding is type-bound, so a type change on an indexed
        // column would invalidate its entries.
        if column_type.is_some() && self.column_is_indexed(table, column) {
            return Err(Error::Constraint(format!(
                "column {column} of table {table} is indexed; drop the index before changing its type"
            )));
        }
        self.state.put_column(ColumnValue {
            column_type: column_type.unwrap_or(value.column_type),
            nulls_allowed: nulls_allowed.unwrap_or(value.nulls_allowed),
            default_value: default_value.unwrap_or(value.default_value),
            begin_snapshot: self.new_snapshot_id,
            ..value
        });
        self.mark_altered(table.get());

        Ok(())
    }

    /// Drops a column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or column does not exist.
    /// Returns [`Error::Constraint`] if this is the last live top-level
    /// column of the table, or if the column or any field beneath it is
    /// indexed.
    ///
    /// Dropping a nested column drops every field beneath it.
    pub fn drop_column(&mut self, table: TableId, column: ColumnId) -> Result<()> {
        let value = self.live_column(table, column)?;
        let doomed = self.column_subtree(table, column);
        if let Some(indexed) = doomed
            .iter()
            .find(|id| self.column_is_indexed(table, ColumnId::new(**id)))
        {
            return Err(Error::Constraint(format!(
                "column {indexed} of table {table} is indexed; drop the index first"
            )));
        }
        // Only top-level columns count: dropping the last field of a struct
        // leaves the struct.
        let live_top_level = self.state.columns.get(&table.get()).map_or(0, |cols| {
            cols.values().filter(|c| c.parent_column.is_none()).count()
        });
        if value.parent_column.is_none() && live_top_level <= 1 {
            return Err(Error::Constraint(format!(
                "column {column} is the last column of table {table}"
            )));
        }
        for id in doomed {
            self.state.delete_column(table.get(), id);
        }
        self.mark_altered(table.get());

        Ok(())
    }

    /// `column` and every field beneath it, parents before their children.
    fn column_subtree(&self, table: TableId, column: ColumnId) -> Vec<u64> {
        let Some(columns) = self.state.columns.get(&table.get()) else {
            return Vec::new();
        };
        let mut subtree = vec![column.get()];
        let mut at = 0;
        while at < subtree.len() {
            let parent = subtree[at];
            at += 1;
            subtree.extend(
                columns
                    .values()
                    .filter(|c| c.parent_column == Some(parent))
                    .map(|c| c.column_id),
            );
        }

        subtree
    }

    /// The table's live partition spec id, if it has one.
    fn live_partition_id(&self, table: TableId) -> Option<u64> {
        self.state
            .partitions
            .get(&table.get())
            .and_then(|per_table| per_table.keys().next().copied())
    }

    /// Sets a table's partition spec, replacing any spec already live; the
    /// old spec ends into history and files written under it keep
    /// referencing it. Transforms are stored verbatim and never parsed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or any referenced column is
    /// not live, or [`Error::Constraint`] if `columns` is empty (use
    /// [`Self::clear_partitioning`]) or names one column twice.
    pub fn set_partitioning(
        &mut self,
        table: TableId,
        columns: &[PartitionColumnDef],
    ) -> Result<PartitionId> {
        self.live_table(table)?;
        if columns.is_empty() {
            return Err(Error::Constraint(format!(
                "partition spec for table {table} needs at least one column; \
                 use clear_partitioning to unpartition"
            )));
        }
        let mut seen = HashSet::with_capacity(columns.len());
        for key in columns {
            self.live_column(table, key.column)?;
            if !seen.insert(key.column) {
                return Err(Error::Constraint(format!(
                    "partition spec for table {table} names column {} twice",
                    key.column
                )));
            }
        }

        if let Some(partition_id) = self.live_partition_id(table) {
            self.state.delete_partition(table.get(), partition_id);
        }
        let partition_id = self.alloc_catalog_id();
        self.state.put_partition(PartitionValue {
            partition_id,
            table_id: table.get(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            columns: columns
                .iter()
                .enumerate()
                .map(|(index, key)| PartitionColumn {
                    partition_key_index: index as u64,
                    column_id: key.column.get(),
                    transform: key.transform.clone(),
                })
                .collect(),
        });
        self.mark_altered(table.get());

        Ok(PartitionId::new(partition_id))
    }

    /// Unpartitions a table: its live spec ends into history.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist or carries
    /// no live partition spec.
    pub fn clear_partitioning(&mut self, table: TableId) -> Result<()> {
        self.live_table(table)?;
        let partition_id = self
            .live_partition_id(table)
            .ok_or_else(|| Error::NotFound(format!("partition spec of table {table}")))?;
        self.state.delete_partition(table.get(), partition_id);
        self.mark_altered(table.get());

        Ok(())
    }

    /// The live sort spec's id for `table`, if it has one.
    fn live_sort_id(&self, table: TableId) -> Option<u64> {
        self.state
            .sorts
            .get(&table.get())
            .and_then(|per_table| per_table.keys().next().copied())
    }

    /// Sets a table's sort spec, replacing any spec already live; the old
    /// spec ends into history. Expressions, dialects, directions and null
    /// orders are stored verbatim and never parsed. Not a schema change:
    /// the catalog's schema version does not advance.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table is not live, or
    /// [`Error::Constraint`] if `keys` is empty (use
    /// [`Self::clear_sorting`]).
    pub fn set_sorting(&mut self, table: TableId, keys: &[SortKeyDef]) -> Result<SortId> {
        self.live_table(table)?;
        if keys.is_empty() {
            return Err(Error::Constraint(format!(
                "sort spec for table {table} needs at least one key; \
                 use clear_sorting to unsort"
            )));
        }

        if let Some(sort_id) = self.live_sort_id(table) {
            self.state.delete_sort(table.get(), sort_id);
        }
        let sort_id = self.alloc_catalog_id();
        self.state.put_sort(SortValue {
            sort_id,
            table_id: table.get(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            expressions: keys
                .iter()
                .enumerate()
                .map(|(index, key)| SortExpression {
                    sort_key_index: index as u64,
                    expression: key.expression.clone(),
                    dialect: key.dialect.clone(),
                    sort_direction: key.sort_direction.clone(),
                    null_order: key.null_order.clone(),
                })
                .collect(),
        });
        self.ops.push(Operation::AlterTableSorting {
            table_id: table.get(),
        });

        Ok(SortId::new(sort_id))
    }

    /// Unsorts a table: its live spec ends into history.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist or carries
    /// no live sort spec.
    pub fn clear_sorting(&mut self, table: TableId) -> Result<()> {
        self.live_table(table)?;
        let sort_id = self
            .live_sort_id(table)
            .ok_or_else(|| Error::NotFound(format!("sort spec of table {table}")))?;
        self.state.delete_sort(table.get(), sort_id);
        self.ops.push(Operation::AlterTableSorting {
            table_id: table.get(),
        });

        Ok(())
    }

    fn index_name_free(&self, table: TableId, name: &str) -> Result<()> {
        let taken = self
            .state
            .indexes
            .get(&table.get())
            .is_some_and(|per_table| per_table.values().any(|i| i.index_name == name));
        if taken {
            return Err(Error::AlreadyExists(format!("index {name}")));
        }
        Ok(())
    }

    /// Encodes one writer-supplied entry and stages it. A row with any NULL
    /// indexed column is exempt from the unique-value collision.
    fn stage_index_entry(
        &mut self,
        index_id: u64,
        shape: &IndexShape,
        row_id: u64,
        values: &[Option<IndexKeyValue>],
        delete: bool,
    ) -> Result<()> {
        if values.len() != shape.column_count {
            return Err(Error::Constraint(format!(
                "index entry has {} values, expected {}",
                values.len(),
                shape.column_count
            )));
        }
        let (key, unique) = encode_ordered_index_entry(
            values,
            &shape.directions,
            &shape.nulls,
            index_id,
            shape.unique,
            row_id,
        )?;
        self.index_entries.push(StagedIndexEntry {
            index_id,
            unique,
            key,
            row_id,
            delete,
            building: shape.building,
        });
        Ok(())
    }

    /// Creates an equality index over a table, staging entries for the
    /// writer-supplied backfill in the same commit. The first index on a
    /// store stamps its format at 2.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or any referenced column is
    /// not live. Returns [`Error::AlreadyExists`] if an index with that name
    /// already exists on the table. Returns [`Error::Constraint`] if the
    /// column list is empty, an entry's value count does not match the
    /// column count, an indexed value exceeds the size cap, or the backfill
    /// contains a duplicate of a unique value.
    pub fn create_index(
        &mut self,
        table: TableId,
        def: &IndexDef,
        backfill: &[IndexEntry],
    ) -> Result<IndexId> {
        self.create_index_ordered_with_maintenance(
            table,
            def,
            &[],
            IndexMaintenance::Synchronous,
            backfill,
        )
    }

    /// Creates an index with explicit per-column sort orders — ascending or
    /// descending, NULLS FIRST or LAST. `orders` runs parallel to
    /// `def.columns`; an empty slice is all-ascending / NULLS LAST, exactly
    /// [`Self::create_index`].
    ///
    /// # Errors
    ///
    /// As [`Self::create_index`], plus [`Error::Constraint`] if `orders` is
    /// non-empty and its length does not match the column count.
    pub fn create_index_ordered(
        &mut self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        backfill: &[IndexEntry],
    ) -> Result<IndexId> {
        self.create_index_ordered_with_maintenance(
            table,
            def,
            orders,
            IndexMaintenance::Synchronous,
            backfill,
        )
    }

    /// Creates an index with explicit sort orders and upkeep mode.
    /// Deferred upkeep is available only to non-unique indexes.
    ///
    /// # Errors
    ///
    /// As [`Self::create_index_ordered`], plus [`Error::Constraint`] when
    /// deferred upkeep is requested for a unique index.
    pub fn create_index_ordered_with_maintenance(
        &mut self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        maintenance: IndexMaintenance,
        backfill: &[IndexEntry],
    ) -> Result<IndexId> {
        let index_id = self.insert_index_definition(table, def, maintenance, None, orders)?;
        let shape = IndexShape::of_definition(def, orders);
        for entry in backfill {
            self.stage_index_entry(index_id, &shape, entry.row_id, &entry.values, false)?;
        }
        self.mark_altered(table.get());
        Ok(IndexId::new(index_id))
    }

    /// Validates an index definition against the live table and stages its
    /// definition record. `build_state` is `None` for a single-commit build
    /// and `Some("building")` for a staged one. `orders` gives the per-column
    /// sort orders; empty means all-ascending / NULLS LAST.
    fn insert_index_definition(
        &mut self,
        table: TableId,
        def: &IndexDef,
        maintenance: IndexMaintenance,
        build_state: Option<String>,
        orders: &[ColumnOrder],
    ) -> Result<u64> {
        nonempty_name("index", &def.name)?;
        self.live_table(table)?;
        if def.columns.is_empty() {
            return Err(Error::Constraint(format!(
                "index {} needs at least one column",
                def.name
            )));
        }
        for &column in &def.columns {
            let live = self.live_column(table, column)?;
            crate::catalog::index_policy::ensure_indexable(&live.column_name, &live.column_type)?;
        }
        self.index_name_free(table, &def.name)?;

        if maintenance == IndexMaintenance::Deferred && def.unique {
            return Err(Error::Constraint(
                "deferred maintenance is supported only for non-unique indexes".to_owned(),
            ));
        }

        if !orders.is_empty() && orders.len() != def.columns.len() {
            return Err(Error::Constraint(format!(
                "index {} has {} columns but {} sort orders",
                def.name,
                def.columns.len(),
                orders.len()
            )));
        }
        // Per-column orders are recorded only when they diverge from the
        // default.
        let column_descending = if orders.iter().all(|o| o.direction == Direction::Ascending) {
            Vec::new()
        } else {
            orders
                .iter()
                .map(|o| o.direction == Direction::Descending)
                .collect()
        };
        let column_nulls_first = if orders.iter().all(|o| o.nulls == NullOrder::Last) {
            Vec::new()
        } else {
            orders.iter().map(|o| o.nulls == NullOrder::First).collect()
        };

        let index_id = self.alloc_catalog_id();
        self.state.put_index(IndexValue {
            index_id,
            table_id: table.get(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            index_name: def.name.clone(),
            column_ids: def.columns.iter().map(|c| c.get()).collect(),
            unique: def.unique,
            column_descending,
            column_nulls_first,
            build_state,
            build_cursor_file: None,
            build_cursor_row_id: None,
            build_cursor_position: None,
            build_deletes_scanned: None,
            poisoned: None,
            ducklake_index_id: None,
            deferred_maintenance: (maintenance == IndexMaintenance::Deferred).then_some(true),
        });
        Ok(index_id)
    }

    /// Drops an equality index. The definition ends into history; its
    /// entries are orphaned and reclaimed lazily.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the index does not exist.
    pub fn drop_index(&mut self, index: IndexId) -> Result<()> {
        let (table_id, _) = self.live_index(index)?;
        self.state.delete_index(table_id, index.get());
        self.mark_altered(table_id);
        Ok(())
    }

    /// The owning table id and definition of a live index.
    fn live_index(&self, index: IndexId) -> Result<(u64, IndexValue)> {
        self.state
            .indexes
            .iter()
            .find_map(|(&table_id, per_table)| {
                per_table
                    .get(&index.get())
                    .map(|value| (table_id, value.clone()))
            })
            .ok_or_else(|| Error::NotFound(format!("index {index}")))
    }

    /// Begins a staged (multi-commit) index build. The definition lands in
    /// `building` state, serving no lookups and stamping the store format
    /// at 3; the host streams backfill batches through
    /// [`Self::build_index_step`] until a final step flips it ready.
    /// Writers maintain entries from this commit forward.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or any referenced column is
    /// not live, [`Error::AlreadyExists`] if the name is taken, or
    /// [`Error::Constraint`] if the column list is empty.
    pub fn create_index_staged(&mut self, table: TableId, def: &IndexDef) -> Result<IndexId> {
        self.create_index_staged_ordered(table, def, &[])
    }

    /// Begins a staged build with explicit per-column sort orders, the
    /// staged counterpart to [`Self::create_index_ordered`].
    ///
    /// # Errors
    ///
    /// As [`Self::create_index_staged`], plus [`Error::Constraint`] if
    /// `orders` is non-empty and its length differs from the column list's.
    pub fn create_index_staged_ordered(
        &mut self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
    ) -> Result<IndexId> {
        self.create_index_staged_ordered_with_maintenance(
            table,
            def,
            orders,
            IndexMaintenance::Synchronous,
        )
    }

    /// Begins a staged build with explicit sort orders and upkeep mode.
    ///
    /// # Errors
    ///
    /// As [`Self::create_index_staged_ordered`], plus [`Error::Constraint`]
    /// when deferred upkeep is requested for a unique index.
    pub fn create_index_staged_ordered_with_maintenance(
        &mut self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        maintenance: IndexMaintenance,
    ) -> Result<IndexId> {
        let index_id = self.insert_index_definition(
            table,
            def,
            maintenance,
            Some("building".to_owned()),
            orders,
        )?;
        self.mark_altered(table.get());

        Ok(IndexId::new(index_id))
    }

    /// Advances a staged build by one batch of writer-supplied entries,
    /// persisting a row-id cursor. With `is_final`, flips the index ready
    /// in this same commit and advances the catalog schema version; a
    /// non-final step preserves it. Returns the resulting state.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the index does not exist,
    /// [`Error::Constraint`] if it is not building, an entry's value count
    /// is wrong, a value exceeds the size cap, or the batch introduces a
    /// duplicate of a unique value (the build fails; its driver drops it).
    pub fn build_index_step(
        &mut self,
        index: IndexId,
        batch: &[IndexEntry],
        is_final: bool,
    ) -> Result<IndexState> {
        self.build_index_step_at(index, batch, is_final, None)
    }

    /// Advances a staged build and persists the file position covered by
    /// this step.
    pub(crate) fn build_index_source_step(
        &mut self,
        index: IndexId,
        batch: &[IndexEntry],
        is_final: bool,
        source: Option<(u64, u64)>,
    ) -> Result<IndexState> {
        self.build_index_step_at(index, batch, is_final, source)
    }

    fn build_index_step_at(
        &mut self,
        index: IndexId,
        batch: &[IndexEntry],
        is_final: bool,
        source: Option<(u64, u64)>,
    ) -> Result<IndexState> {
        let (table_id, mut value) = self.live_index(index)?;
        if !matches!(
            value.build_state.as_deref(),
            Some("building" | "maintaining")
        ) {
            return Err(Error::Constraint(format!("index {index} is not building")));
        }

        let maintenance_repair = value.build_state.as_deref() == Some("maintaining");
        let shape = IndexShape::of_value(&value);
        let mut cursor = value.build_cursor_row_id.unwrap_or(0);
        for entry in batch {
            cursor = cursor.max(entry.row_id);
            self.stage_index_entry(index.get(), &shape, entry.row_id, &entry.values, false)?;
        }

        value.begin_snapshot = self.new_snapshot_id;
        if !batch.is_empty() {
            value.build_cursor_row_id = Some(cursor);
        }
        if let Some((file_id, position)) = source {
            let covered = value.build_cursor_file.zip(value.build_cursor_position);
            if covered.is_none_or(|covered| (file_id, position) > covered) {
                value.build_cursor_file = Some(file_id);
                value.build_cursor_position = Some(position);
            }
        }
        let resulting_state = if is_final {
            value.build_state = None;
            IndexState::Ready
        } else if maintenance_repair {
            IndexState::Maintaining
        } else {
            IndexState::Building
        };
        self.state.put_index(value);
        if is_final {
            if maintenance_repair {
                self.ops
                    .push(Operation::FinishIndexMaintenance { table_id });
            } else {
                self.mark_altered(table_id);
            }
        } else {
            self.ops.push(Operation::AdvanceIndexBuild { table_id });
        }
        Ok(resulting_state)
    }

    /// Advances a staged build's cursor to `covered_through` and, with
    /// `is_final`, flips the index ready — staging no entries, for a build
    /// whose entry batch was written straight into the store under the folder
    /// role. The cursor only ever advances.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the index does not exist, or
    /// [`Error::Constraint`] if it is not building.
    pub fn advance_index_build(
        &mut self,
        index: IndexId,
        covered_through: Option<u64>,
        is_final: bool,
    ) -> Result<IndexState> {
        let (table_id, mut value) = self.live_index(index)?;
        if value.build_state.as_deref() != Some("building") {
            return Err(Error::Constraint(format!("index {index} is not building")));
        }

        value.begin_snapshot = self.new_snapshot_id;
        if let Some(row_id) = covered_through {
            value.build_cursor_row_id = Some(value.build_cursor_row_id.unwrap_or(0).max(row_id));
        }
        let resulting_state = if is_final {
            value.build_state = None;
            IndexState::Ready
        } else {
            IndexState::Building
        };
        self.state.put_index(value);
        self.mark_altered(table_id);
        Ok(resulting_state)
    }

    /// Poisons a staged build's definition — terminal, for a duplicate the
    /// folder-role backfill discovered. The flag flips through this commit so
    /// replay and every peer see the build poisoned; its driver then drops it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the index does not exist.
    pub fn poison_index_build(&mut self, index: IndexId) -> Result<()> {
        let (table_id, mut value) = self.live_index(index)?;
        value.begin_snapshot = self.new_snapshot_id;
        value.poisoned = Some(true);
        self.state.put_index(value);
        self.mark_altered(table_id);
        Ok(())
    }

    fn live_table_stats(&self, table: TableId) -> Result<TableStatsValue> {
        self.state
            .table_stats
            .get(&table.get())
            .copied()
            .ok_or_else(|| Error::Corruption(format!("table {table} has no statistics record")))
    }

    /// The first row id `table` has left to mint, recording the counter's
    /// pre-commit mark on first use. The caller advances the counter in
    /// the statistics it writes.
    fn allocate_row_ids(&mut self, table: TableId, tstat: &TableStatsValue) -> u64 {
        self.inherited_row_ids
            .entry(table.get())
            .or_insert(tstat.next_row_id);

        tstat.next_row_id
    }

    /// The row-id counter as it stood before this commit ran: the ceiling
    /// a flushed file's rows must stay under.
    fn inherited_row_ids(&self, table: TableId, tstat: &TableStatsValue) -> u64 {
        self.inherited_row_ids
            .get(&table.get())
            .copied()
            .unwrap_or(tstat.next_row_id)
    }

    /// The live partition spec a file registered now falls under, after
    /// checking it carries one value per key.
    fn resolve_file_partition(&self, table: TableId, values: &[String]) -> Result<Option<u64>> {
        let live = self
            .state
            .partitions
            .get(&table.get())
            .and_then(|per_table| per_table.values().next());
        let Some(spec) = live else {
            if !values.is_empty() {
                return Err(Error::Constraint(format!(
                    "register_data_file: {} partition values on table {table}, which has no live \
                     partition spec",
                    values.len()
                )));
            }
            return Ok(None);
        };
        if values.len() != spec.columns.len() {
            return Err(Error::Constraint(format!(
                "register_data_file: {} partition values on table {table}, whose live spec has \
                 {} keys",
                values.len(),
                spec.columns.len()
            )));
        }

        Ok(Some(spec.partition_id))
    }

    /// Registers a data file, allocating its dense row-id range from the
    /// table's row-id counter and folding its size into the table's
    /// statistics.
    ///
    /// A partitioned table's file carries `file.partition_values`, one per
    /// key of the live spec in key order, and the record names that spec.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist, or if any
    /// entry in `file.column_stats` names a column that is not live on the
    /// table, or if an `index_entries` entry names an index not live on the
    /// table.
    /// Returns [`Error::Constraint`] if the table has live indexes and a
    /// non-empty file supplies no `index_entries`, an entry's `ordinal` is
    /// outside the file's rows, a supplied indexed value exceeds the size
    /// cap, the entries duplicate a unique value, or the file's partition
    /// values do not match the live spec's keys one for one.
    /// Returns [`Error::Corruption`] if the table has no statistics record.
    pub fn register_data_file(
        &mut self,
        table: TableId,
        file: DataFile,
        index_entries: &[FileIndexEntry],
    ) -> Result<DataFileId> {
        self.live_table(table)?;
        for entry in &file.column_stats {
            self.live_column(table, entry.column_id)?;
        }
        let live_index_count = self
            .state
            .indexes
            .get(&table.get())
            .map_or(0, BTreeMap::len);
        if live_index_count > 0 && file.record_count > 0 && index_entries.is_empty() {
            return Err(Error::Constraint(format!(
                "register_data_file on indexed table {table} must supply index entries"
            )));
        }
        // An entry's row is `row_id_start + ordinal`.
        for entry in index_entries {
            if entry.ordinal >= file.record_count {
                return Err(Error::Constraint(format!(
                    "register_data_file: index entry ordinal {} is outside file record count {} \
                     on table {table}",
                    entry.ordinal, file.record_count
                )));
            }
        }
        let partition_id = self.resolve_file_partition(table, &file.partition_values)?;

        let data_file_id = self.alloc_file_id();
        let tstat = self.live_table_stats(table)?;
        let row_id_start = self.allocate_row_ids(table, &tstat);
        self.state.put_table_stats(TableStatsValue {
            next_row_id: tstat.next_row_id.saturating_add(file.record_count),
            record_count: tstat.record_count.saturating_add(file.record_count),
            file_size_bytes: tstat.file_size_bytes.saturating_add(file.file_size_bytes),
            ..tstat
        });
        self.state.put_data_file(DataFileValue {
            data_file_id,
            table_id: table.get(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            file_order: None,
            path: file.path,
            path_is_relative: file.path_is_relative,
            file_format: file.file_format,
            record_count: file.record_count,
            file_size_bytes: file.file_size_bytes,
            footer_size: file.footer_size,
            row_id_start: Some(row_id_start),
            partition_id,
            encryption_key: file.encryption_key,
            mapping_id: None,
            partial_max: None,
            partition_values: file_partition_values(&file.partition_values),
        });
        for entry in file.column_stats {
            self.state.put_file_column_stats(FileColumnStatsValue {
                data_file_id,
                table_id: table.get(),
                column_id: entry.column_id.get(),
                column_size_bytes: entry.column_size_bytes,
                value_count: entry.value_count,
                null_count: entry.null_count,
                min_value: entry.min_value,
                max_value: entry.max_value,
                contains_nan: entry.contains_nan,
                extra_stats: entry.extra_stats,
                variant_stats: vec![],
            });
        }
        self.stage_file_index_entries(table, row_id_start, index_entries)?;
        self.ops.push(Operation::RegisterDataFile {
            table_id: table.get(),
        });
        Ok(DataFileId::new(data_file_id))
    }

    /// Stages file-supplied index entries, each for the row at
    /// `row_id_start + ordinal`; overflow is refused.
    fn stage_file_index_entries(
        &mut self,
        table: TableId,
        row_id_start: u64,
        index_entries: &[FileIndexEntry],
    ) -> Result<()> {
        let mut shapes = HashMap::new();
        for file_entry in index_entries {
            let shape = self.cached_index_shape(&mut shapes, table, file_entry.index)?;
            let row_id = row_id_start
                .checked_add(file_entry.ordinal)
                .ok_or_else(|| {
                    Error::Constraint(format!(
                        "index entry ordinal {} overflows the row-id range on table {table}",
                        file_entry.ordinal
                    ))
                })?;
            self.stage_index_entry(
                file_entry.index.get(),
                shape,
                row_id,
                &file_entry.values,
                false,
            )?;
        }
        Ok(())
    }

    /// Expires a data file, removing it and subtracting its contribution
    /// from the table's statistics.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the file is not live on the table.
    pub fn expire_data_file(&mut self, table: TableId, file: DataFileId) -> Result<()> {
        let data_file = self
            .state
            .data_files
            .get(&table.get())
            .and_then(|files| files.get(&file.get()))
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("data file {file} of table {table}")))?;

        let cascaded: Vec<u64> = self
            .state
            .delete_files
            .get(&table.get())
            .into_iter()
            .flat_map(BTreeMap::values)
            .filter(|d| d.data_file_id == file.get())
            .map(|d| d.delete_file_id)
            .collect();
        for delete_file_id in cascaded {
            self.state.delete_delete_file(table.get(), delete_file_id);
        }

        self.state.delete_data_file(table.get(), file.get());

        let tstat = self.live_table_stats(table)?;
        self.state.put_table_stats(TableStatsValue {
            record_count: tstat.record_count.saturating_sub(data_file.record_count),
            file_size_bytes: tstat
                .file_size_bytes
                .saturating_sub(data_file.file_size_bytes),
            ..tstat
        });

        self.ops.push(Operation::ExpireDataFile {
            table_id: table.get(),
        });
        Ok(())
    }

    /// Registers a delete file targeting a live data file's rows, removing
    /// the equality-index entries of the rows it kills. Each removal names
    /// the killed row by id — inside a dense target's row-id range, or a
    /// per-row-id target's embedded id — with the values the dead row was
    /// indexed under. Table statistics are unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist, if
    /// `file.data_file_id` is not live on the table, or if an
    /// `index_entries` entry names an index not live on the table.
    /// Returns [`Error::Constraint`] if the table has live indexes and a
    /// non-empty delete file supplies no `index_entries`, an empty delete
    /// file supplies any, an entry's row id is outside a dense target's
    /// row-id range, or an entry's value count does not match its index's
    /// column count.
    pub fn register_delete_file(
        &mut self,
        table: TableId,
        file: DeleteFile,
        index_entries: &[FileIndexRemoval],
    ) -> Result<DeleteFileId> {
        self.live_table(table)?;
        let data_file = self
            .state
            .data_files
            .get(&table.get())
            .and_then(|files| files.get(&file.data_file_id.get()))
            .cloned()
            .ok_or_else(|| {
                Error::NotFound(format!("data file {} of table {table}", file.data_file_id))
            })?;

        // One live delete file per data file.
        let already_targeted = self
            .state
            .delete_files
            .get(&table.get())
            .is_some_and(|files| {
                files
                    .values()
                    .any(|existing| existing.data_file_id == file.data_file_id.get())
            });
        if already_targeted {
            return Err(Error::Constraint(format!(
                "data file {} of table {table} already has a live delete file; \
                 expire it first",
                file.data_file_id
            )));
        }
        let live_index_count = self
            .state
            .indexes
            .get(&table.get())
            .map_or(0, BTreeMap::len);
        if live_index_count > 0 && file.delete_count > 0 && index_entries.is_empty() {
            return Err(Error::Constraint(format!(
                "register_delete_file on indexed table {table} must supply index entries"
            )));
        }
        if file.delete_count == 0 && !index_entries.is_empty() {
            return Err(Error::Constraint(format!(
                "register_delete_file on table {table} supplies index entries but deletes no rows"
            )));
        }

        // A dense target's ids are `row_id_start..row_id_start +
        // record_count`; a per-row-id target's are the writer's to supply.
        if let Some(start) = data_file.row_id_start {
            let end = start.checked_add(data_file.record_count).ok_or_else(|| {
                Error::Constraint(format!(
                    "register_delete_file: target data file {}'s row-id range overflows u64 \
                     on table {table}",
                    file.data_file_id
                ))
            })?;
            for entry in index_entries {
                if entry.row_id < start || entry.row_id >= end {
                    return Err(Error::Constraint(format!(
                        "register_delete_file: index entry row id {} is outside the target \
                         file's row-id range on table {table}",
                        entry.row_id
                    )));
                }
            }
        }

        let delete_file_id = self.alloc_file_id();
        self.state.put_delete_file(DeleteFileValue {
            delete_file_id,
            table_id: table.get(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            data_file_id: file.data_file_id.get(),
            path: file.path,
            path_is_relative: file.path_is_relative,
            format: file.format,
            delete_count: file.delete_count,
            file_size_bytes: file.file_size_bytes,
            footer_size: file.footer_size,
            encryption_key: file.encryption_key,
            partial_max: None,
        });

        self.stage_delete_file_index_entries(table, index_entries)?;

        self.ops.push(Operation::RegisterDeleteFile {
            table_id: table.get(),
            data_file_id: file.data_file_id.get(),
        });

        Ok(DeleteFileId::new(delete_file_id))
    }

    /// Stages the entry removals a delete file's index entries name, each
    /// under its writer-supplied row id.
    fn stage_delete_file_index_entries(
        &mut self,
        table: TableId,
        index_entries: &[FileIndexRemoval],
    ) -> Result<()> {
        let mut shapes = HashMap::new();
        for entry in index_entries {
            let shape = self.cached_index_shape(&mut shapes, table, entry.index)?;
            self.stage_index_entry(entry.index.get(), shape, entry.row_id, &entry.values, true)?;
        }
        Ok(())
    }

    /// A live index's shape, resolved once per `shapes`.
    fn cached_index_shape<'a>(
        &self,
        shapes: &'a mut HashMap<IndexId, IndexShape>,
        table: TableId,
        index: IndexId,
    ) -> Result<&'a IndexShape> {
        match shapes.entry(index) {
            Entry::Occupied(occupied) => Ok(occupied.into_mut()),
            Entry::Vacant(vacant) => Ok(vacant.insert(self.live_index_shape(table, index)?)),
        }
    }

    /// The shape of a live index on the table.
    fn live_index_shape(&self, table: TableId, index: IndexId) -> Result<IndexShape> {
        let value = self
            .state
            .indexes
            .get(&table.get())
            .and_then(|per_table| per_table.get(&index.get()))
            .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;
        Ok(IndexShape::of_value(value))
    }

    /// Expires a delete file, removing it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the file is not live on the table.
    pub fn expire_delete_file(&mut self, table: TableId, file: DeleteFileId) -> Result<()> {
        let live = self
            .state
            .delete_files
            .get(&table.get())
            .is_some_and(|files| files.contains_key(&file.get()));
        if !live {
            return Err(Error::NotFound(format!(
                "delete file {file} of table {table}"
            )));
        }
        self.state.delete_delete_file(table.get(), file.get());
        self.ops.push(Operation::ExpireDeleteFile {
            table_id: table.get(),
        });

        Ok(())
    }

    /// Overrides a table's row-count and size statistics; `next_row_id` is
    /// preserved.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist.
    /// Returns [`Error::Corruption`] if the table has no statistics
    /// record.
    pub fn update_table_stats(
        &mut self,
        table: TableId,
        record_count: u64,
        file_size_bytes: u64,
    ) -> Result<()> {
        self.live_table(table)?;
        let tstat = self.live_table_stats(table)?;
        self.state.put_table_stats(TableStatsValue {
            record_count,
            file_size_bytes,
            ..tstat
        });
        self.ops.push(Operation::UpdateStats {
            table_id: table.get(),
        });

        Ok(())
    }

    /// Overrides a column's table-level statistics, verbatim.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or column does not exist.
    pub fn update_column_stats(
        &mut self,
        table: TableId,
        column: ColumnId,
        stats: ColumnStats,
    ) -> Result<()> {
        self.live_column(table, column)?;
        self.state.put_table_column_stats(TableColumnStatsValue {
            table_id: table.get(),
            column_id: column.get(),
            contains_null: stats.contains_null,
            contains_nan: stats.contains_nan,
            min_value: stats.min_value,
            max_value: stats.max_value,
            extra_stats: stats.extra_stats,
        });
        self.ops.push(Operation::UpdateStats {
            table_id: table.get(),
        });

        Ok(())
    }

    /// Creates a view.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the schema does not exist.
    /// Returns [`Error::AlreadyExists`] if a relation with that name
    /// already exists in the schema.
    pub fn create_view(
        &mut self,
        schema: SchemaId,
        name: &str,
        dialect: &str,
        sql: &str,
    ) -> Result<ViewId> {
        path_safe_name("view", name)?;
        let Some(schema_rec) = self.state.schemas.get(&schema.get()) else {
            return Err(Error::NotFound(format!("schema {schema}")));
        };
        let schema_name = schema_rec.schema_name.clone();
        self.relation_name_free(schema.get(), name)?;
        let view_id = self.alloc_catalog_id();

        self.state.put_view(ViewValue {
            view_id,
            view_uuid: Uuid::new_v4().to_string(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            schema_id: schema.get(),
            view_name: name.to_owned(),
            dialect: dialect.to_owned(),
            sql: sql.to_owned(),
            column_aliases: None,
        });
        self.ops.push(Operation::CreateView {
            schema_id: schema.get(),
            view_id,
            schema_name,
            view_name: name.to_owned(),
        });

        Ok(ViewId::new(view_id))
    }

    /// Replaces a view's definition as a new version.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the view does not exist.
    pub fn alter_view(&mut self, view: ViewId, dialect: &str, sql: &str) -> Result<()> {
        let value = self
            .state
            .views
            .get(&view.get())
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("view {view}")))?;
        self.state.put_view(ViewValue {
            dialect: dialect.to_owned(),
            sql: sql.to_owned(),
            begin_snapshot: self.new_snapshot_id,
            ..value
        });
        self.ops.push(Operation::AlterView {
            view_id: view.get(),
        });
        Ok(())
    }

    /// Drops a view.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the view does not exist.
    pub fn drop_view(&mut self, view: ViewId) -> Result<()> {
        if !self.state.views.contains_key(&view.get()) {
            return Err(Error::NotFound(format!("view {view}")));
        }
        self.state.delete_view(view.get());
        self.ops.push(Operation::DropView {
            view_id: view.get(),
        });
        Ok(())
    }

    /// Creates a macro with its implementations. Names are namespaced per
    /// `macro_type`; every implementation must carry the same one.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the schema does not exist.
    /// Returns [`Error::Constraint`] if `implementations` is empty or
    /// mixes `macro_type`s.
    /// Returns [`Error::AlreadyExists`] if a live macro with that name
    /// and type already exists in the schema.
    pub fn create_macro(
        &mut self,
        schema: SchemaId,
        name: &str,
        implementations: &[MacroImplementationDef],
    ) -> Result<MacroId> {
        let Some(schema_rec) = self.state.schemas.get(&schema.get()) else {
            return Err(Error::NotFound(format!("schema {schema}")));
        };
        let schema_name = schema_rec.schema_name.clone();
        let Some(first) = implementations.first() else {
            return Err(Error::Constraint(format!(
                "macro {name} needs at least one implementation"
            )));
        };
        if implementations
            .iter()
            .any(|i| i.macro_type != first.macro_type)
        {
            return Err(Error::Constraint(format!(
                "macro {name}: all implementations must share one macro_type"
            )));
        }
        let name_taken = self.state.macros.values().any(|m| {
            m.schema_id == schema.get()
                && m.macro_name == name
                && m.implementations
                    .first()
                    .is_some_and(|i| i.macro_type == first.macro_type)
        });
        if name_taken {
            return Err(Error::AlreadyExists(format!("macro {name}")));
        }
        let macro_id = self.alloc_catalog_id();

        self.state.put_macro(MacroValue {
            macro_id,
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            schema_id: schema.get(),
            macro_name: name.to_owned(),
            implementations: implementations
                .iter()
                .enumerate()
                .map(|(impl_id, def)| MacroImplementation {
                    impl_id: impl_id as u64,
                    dialect: def.dialect.clone(),
                    sql: def.sql.clone(),
                    macro_type: def.macro_type.clone(),
                    parameters: def
                        .parameters
                        .iter()
                        .enumerate()
                        .map(|(column_id, p)| MacroParameter {
                            column_id: column_id as u64,
                            parameter_name: p.name.clone(),
                            parameter_type: p.parameter_type.clone(),
                            default_value: p.default_value.clone(),
                            default_value_type: p.default_value_type.clone(),
                        })
                        .collect(),
                })
                .collect(),
        });
        self.ops.push(Operation::CreateMacro {
            schema_id: schema.get(),
            macro_id,
            schema_name,
            macro_name: name.to_owned(),
            macro_type: first.macro_type.clone(),
        });

        Ok(MacroId::new(macro_id))
    }

    /// Drops a macro, ending its live version.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the macro does not exist.
    pub fn drop_macro(&mut self, macro_id: MacroId) -> Result<()> {
        let Some(record) = self.state.macros.get(&macro_id.get()) else {
            return Err(Error::NotFound(format!("macro {macro_id}")));
        };
        // Creation requires at least one implementation.
        let macro_type = record
            .implementations
            .first()
            .map(|i| i.macro_type.clone())
            .unwrap_or_default();
        self.state.delete_macro(macro_id.get());
        self.ops.push(Operation::DropMacro {
            macro_id: macro_id.get(),
            macro_type,
        });
        Ok(())
    }

    /// Sets an option in a scope. Last-write-wins; an options-only
    /// commit mints no snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the scope's schema or table does
    /// not exist, or [`Error::Constraint`] for the reserved global
    /// `encrypted` key.
    pub fn set_option(&mut self, scope: OptionScope, key: &str, value: &str) -> Result<()> {
        nonempty_name("option key", key)?;
        self.live_scope(scope)?;
        reserved_option(scope, key)?;
        let components = scope.key_components();
        let mut record = self
            .state
            .options
            .get(&components)
            .cloned()
            .unwrap_or_default();
        record.options.insert(key.to_owned(), value.to_owned());
        self.state.set_option_record(components, record);
        Ok(())
    }

    /// Removes an option from a scope; absent keys are a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the scope's schema or table does
    /// not exist, or [`Error::Constraint`] for the reserved global
    /// `encrypted` key.
    pub fn unset_option(&mut self, scope: OptionScope, key: &str) -> Result<()> {
        self.live_scope(scope)?;
        reserved_option(scope, key)?;
        let components = scope.key_components();
        let Some(mut record) = self.state.options.get(&components).cloned() else {
            return Ok(());
        };
        if record.options.remove(key).is_none() {
            return Ok(());
        }
        if record.options.is_empty() {
            self.state.remove_option_record(components);
        } else {
            self.state.set_option_record(components, record);
        }
        Ok(())
    }

    /// Records the mutation of a tag target, erroring if it is not live.
    /// Tables and views classify as alterations; schemas carry no
    /// change-set entry.
    fn mark_tagged(&mut self, target: TagTarget) -> Result<()> {
        let op = match target {
            TagTarget::Schema(schema) => {
                if !self.state.schemas.contains_key(&schema.get()) {
                    return Err(Error::NotFound(format!("schema {schema}")));
                }
                Operation::AlterSchema {
                    schema_id: schema.get(),
                }
            }
            TagTarget::Table(table) => {
                self.live_table(table)?;
                Operation::AlterTable {
                    table_id: table.get(),
                }
            }
            TagTarget::View(view) => {
                if !self.state.views.contains_key(&view.get()) {
                    return Err(Error::NotFound(format!("view {view}")));
                }
                Operation::AlterView {
                    view_id: view.get(),
                }
            }
        };
        self.ops.push(op);

        Ok(())
    }

    /// Ends the live entry for `key` on `object_id`, if there is one.
    /// Returns whether one was ended.
    fn end_live_tag(&mut self, object_id: u64, key: &str) -> bool {
        let new_snapshot_id = self.new_snapshot_id;
        let Some(container) = self.state.tags.get_mut(&object_id) else {
            return false;
        };
        let Some(live) = container
            .entries
            .iter_mut()
            .find(|entry| entry.key == key && entry.end_snapshot.is_none())
        else {
            return false;
        };
        live.end_snapshot = Some(new_snapshot_id);

        true
    }

    /// Sets a tag on a schema, table, or view. An existing value for the
    /// key ends into the object's tag history and the new value begins at
    /// this commit's snapshot. Column tags are not reachable here.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the target does not exist, or
    /// [`Error::Constraint`] if the key is empty.
    pub fn set_tag(&mut self, target: TagTarget, key: &str, value: &str) -> Result<()> {
        nonempty_name("tag key", key)?;
        self.mark_tagged(target)?;

        let object_id = target.object_id();
        self.end_live_tag(object_id, key);
        let entry = TagEntry {
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            key: key.to_owned(),
            value: value.to_owned(),
        };
        self.state
            .tags
            .entry(object_id)
            .or_insert_with(|| TagValue {
                object_id,
                entries: Vec::new(),
            })
            .entries
            .push(entry);

        Ok(())
    }

    /// Removes a tag from a schema, table, or view: its live entry ends at
    /// this commit's snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the target does not exist, or if it
    /// carries no live entry for the key.
    pub fn remove_tag(&mut self, target: TagTarget, key: &str) -> Result<()> {
        self.mark_tagged(target)?;
        if self.end_live_tag(target.object_id(), key) {
            Ok(())
        } else {
            Err(Error::NotFound(format!(
                "tag {key:?} on object {}",
                target.object_id()
            )))
        }
    }

    fn live_scope(&self, scope: OptionScope) -> Result<()> {
        match scope {
            OptionScope::Global => Ok(()),
            OptionScope::Schema(s) => {
                if self.state.schemas.contains_key(&s.get()) {
                    Ok(())
                } else {
                    Err(Error::NotFound(format!("schema {s}")))
                }
            }
            OptionScope::Table(t) => {
                if self.state.tables.contains_key(&t.get()) {
                    Ok(())
                } else {
                    Err(Error::NotFound(format!("table {t}")))
                }
            }
        }
    }

    /// Inlines a chunk of rows into the catalog's `inline` subspace instead
    /// of a data file. Row ids are allocated as for a data file: the
    /// returned id is the chunk's first and its rows run densely from
    /// there. The rows count toward `record_count` from here on; a later
    /// [`flush`](Self::flush_inlined_data) does not recount them.
    ///
    /// `index_entries` covers the chunk's rows for every live equality
    /// index, positioned by ordinal within the chunk, as
    /// [`Self::register_data_file`] covers a file's.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table is not live, or if an
    /// `index_entries` entry names an index not live on it.
    /// Returns [`Error::Constraint`] if the chunk carries no rows, if the
    /// table has live indexes and no entries are supplied, if an entry's
    /// ordinal is outside the chunk, or if a supplied value exceeds the
    /// indexed-value size cap.
    /// Returns [`Error::Corruption`] if the table has no statistics record.
    pub fn inline_insert(
        &mut self,
        table: TableId,
        chunk: &InlineChunk,
        index_entries: &[FileIndexEntry],
    ) -> Result<u64> {
        self.live_table(table)?;
        if chunk.row_count == 0 {
            return Err(Error::Constraint(format!(
                "inline_insert on table {table} carries no rows"
            )));
        }
        let live_index_count = self
            .state
            .indexes
            .get(&table.get())
            .map_or(0, BTreeMap::len);
        if live_index_count > 0 && index_entries.is_empty() {
            return Err(Error::Constraint(format!(
                "inline_insert on indexed table {table} must supply index entries"
            )));
        }
        for entry in index_entries {
            if entry.ordinal >= chunk.row_count {
                return Err(Error::Constraint(format!(
                    "inline_insert: index entry ordinal {} is outside the chunk's {} rows \
                     on table {table}",
                    entry.ordinal, chunk.row_count
                )));
            }
        }

        let tstat = self.live_table_stats(table)?;
        let row_id_start = self.allocate_row_ids(table, &tstat);
        self.state.put_table_stats(TableStatsValue {
            next_row_id: tstat.next_row_id.saturating_add(chunk.row_count),
            record_count: tstat.record_count.saturating_add(chunk.row_count),
            ..tstat
        });

        let chunk_seq = self
            .chunk_seqs
            .entry((table.get(), chunk.schema_version))
            .or_insert(0);
        let allocated_seq = *chunk_seq;
        *chunk_seq += 1;

        self.inline_ops.push(InlineStage::Schema {
            table_id: table.get(),
            schema_version: chunk.schema_version,
            arrow_schema: chunk.arrow_schema.clone(),
        });
        self.inline_ops.push(InlineStage::Insert {
            table_id: table.get(),
            schema_version: chunk.schema_version,
            begin_snapshot: self.new_snapshot_id,
            chunk_seq: allocated_seq,
            row_id_start,
            row_count: chunk.row_count,
            arrow_body: chunk.arrow_body.clone(),
        });
        self.stage_file_index_entries(table, row_id_start, index_entries)?;
        self.ops.push(Operation::InlineInsert {
            table_id: table.get(),
        });

        Ok(row_id_start)
    }

    /// Tombstones one inlined row: the row stays visible to reads below
    /// this commit's snapshot and disappears from here on. Table
    /// statistics are unchanged.
    ///
    /// `index_entries` names the values the dead row was indexed under, for
    /// every live equality index; each entry's row id must be `row_id`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table is not live, or if an
    /// `index_entries` entry names an index not live on it.
    /// Returns [`Error::Constraint`] if the table has live indexes and no
    /// entries are supplied, or if an entry names a row other than
    /// `row_id`.
    pub fn inline_delete(
        &mut self,
        table: TableId,
        row_id: u64,
        index_entries: &[FileIndexRemoval],
    ) -> Result<()> {
        self.live_table(table)?;
        let live_index_count = self
            .state
            .indexes
            .get(&table.get())
            .map_or(0, BTreeMap::len);
        if live_index_count > 0 && index_entries.is_empty() {
            return Err(Error::Constraint(format!(
                "inline_delete on indexed table {table} must supply index entries"
            )));
        }
        for entry in index_entries {
            if entry.row_id != row_id {
                return Err(Error::Constraint(format!(
                    "inline_delete: index entry names row {} but the tombstoned row is {row_id} \
                     on table {table}",
                    entry.row_id
                )));
            }
        }

        self.inline_ops.push(InlineStage::Tombstone {
            table_id: table.get(),
            row_id,
            end_snapshot: self.new_snapshot_id,
        });
        self.stage_delete_file_index_entries(table, index_entries)?;
        self.ops.push(Operation::InlineDelete {
            table_id: table.get(),
        });

        Ok(())
    }

    /// Drains a table's inlined rows into data files the caller already
    /// wrote, registering those files and removing every `inline/insert`
    /// chunk of `schema_version` committed before this commit, plus the
    /// tombstones those chunks' rows consumed.
    ///
    /// A flushed file keeps the row ids its rows were inlined under and is
    /// backdated to the earliest snapshot among them; its rows are already
    /// counted in the table's statistics, so registering it adds only its
    /// bytes. Passing no files drains the chunks alone.
    ///
    /// A commit may inline into the same table it flushes: the chunk it
    /// stages is not drained, and a flushed file may not claim a row id
    /// this commit minted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table is not live or a file's
    /// column statistics name a column that is not.
    /// Returns [`Error::Constraint`] if a file's backdated snapshot is not
    /// below this commit's, or if a file's row-id range runs past the ids
    /// the table had allocated when this commit began.
    pub fn flush_inlined_data(
        &mut self,
        table: TableId,
        schema_version: u64,
        flushed: &[FlushedDataFile],
    ) -> Result<Vec<DataFileId>> {
        self.live_table(table)?;

        let tstat = self.live_table_stats(table)?;
        let ceiling = self.inherited_row_ids(table, &tstat);
        let ids = flushed
            .iter()
            .map(|flush| self.register_flushed_file(table, flush, ceiling))
            .collect::<Result<Vec<_>>>()?;

        self.inline_ops.push(InlineStage::Flush {
            table_id: table.get(),
            schema_version,
            flush_snapshot: self.new_snapshot_id,
        });
        self.ops.push(Operation::FlushInlinedData {
            table_id: table.get(),
        });

        Ok(ids)
    }

    /// Registers one flushed file: row ids preserved, record backdated, and
    /// only the file's bytes folded into the table's statistics.
    /// `inherited_row_ids` is the ceiling its rows must stay under.
    fn register_flushed_file(
        &mut self,
        table: TableId,
        flush: &FlushedDataFile,
        inherited_row_ids: u64,
    ) -> Result<DataFileId> {
        if flush.begin_snapshot.get() >= self.new_snapshot_id {
            return Err(Error::Constraint(format!(
                "flush_inlined_data: file {} is backdated to snapshot {}, which is not below \
                 this commit's {} — the rows it carries were inlined before it",
                flush.file.path, flush.begin_snapshot, self.new_snapshot_id
            )));
        }
        if let Some(partial_max) = flush.partial_max
            && !(flush.begin_snapshot..SnapshotId::new(self.new_snapshot_id)).contains(&partial_max)
        {
            return Err(Error::Constraint(format!(
                "flush_inlined_data: file {}'s partial_max {partial_max} is outside the \
                 snapshots its rows were inlined at ({} to below {})",
                flush.file.path, flush.begin_snapshot, self.new_snapshot_id
            )));
        }
        let end = flush
            .row_id_start
            .checked_add(flush.file.record_count)
            .ok_or_else(|| {
                Error::Constraint(format!(
                    "flush_inlined_data: file {}'s row-id range overflows u64",
                    flush.file.path
                ))
            })?;
        if end > inherited_row_ids {
            return Err(Error::Constraint(format!(
                "flush_inlined_data: file {} covers row ids up to {end}, past the \
                 {inherited_row_ids} table {table} had allocated when this commit began",
                flush.file.path
            )));
        }
        for entry in &flush.file.column_stats {
            self.live_column(table, entry.column_id)?;
        }
        let partition_id = self.resolve_file_partition(table, &flush.file.partition_values)?;

        let data_file_id = self.alloc_file_id();
        let tstat = self.live_table_stats(table)?;
        self.state.put_table_stats(TableStatsValue {
            file_size_bytes: tstat
                .file_size_bytes
                .saturating_add(flush.file.file_size_bytes),
            ..tstat
        });
        self.state.put_data_file(DataFileValue {
            data_file_id,
            table_id: table.get(),
            begin_snapshot: flush.begin_snapshot.get(),
            end_snapshot: None,
            file_order: None,
            path: flush.file.path.clone(),
            path_is_relative: flush.file.path_is_relative,
            file_format: flush.file.file_format.clone(),
            record_count: flush.file.record_count,
            file_size_bytes: flush.file.file_size_bytes,
            footer_size: flush.file.footer_size,
            row_id_start: Some(flush.row_id_start),
            partition_id,
            encryption_key: flush.file.encryption_key.clone(),
            mapping_id: None,
            partial_max: flush.partial_max.map(SnapshotId::get),
            partition_values: file_partition_values(&flush.file.partition_values),
        });
        for entry in &flush.file.column_stats {
            self.state.put_file_column_stats(FileColumnStatsValue {
                data_file_id,
                table_id: table.get(),
                column_id: entry.column_id.get(),
                column_size_bytes: entry.column_size_bytes,
                value_count: entry.value_count,
                null_count: entry.null_count,
                min_value: entry.min_value.clone(),
                max_value: entry.max_value.clone(),
                contains_nan: entry.contains_nan,
                extra_stats: entry.extra_stats.clone(),
                variant_stats: vec![],
            });
        }

        Ok(DataFileId::new(data_file_id))
    }

    #[cfg(test)]
    pub(crate) fn state(&self) -> &CatalogSnapshot {
        &self.state
    }
}

/// Refuses the global `encrypted` key, which is fixed at catalog creation.
fn reserved_option(scope: OptionScope, key: &str) -> Result<()> {
    if scope == OptionScope::Global && key == "encrypted" {
        return Err(Error::Constraint(
            "the global `encrypted` option is fixed at catalog creation".to_string(),
        ));
    }
    Ok(())
}

/// Refuses an empty name; `what` names the rejected item in the error.
fn nonempty_name(what: &str, name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Constraint(format!("{what} name must not be empty")));
    }
    Ok(())
}

/// Refuses a relation name unsafe in the storage path derived from it: a
/// path separator or a dot segment.
fn path_safe_name(what: &str, name: &str) -> Result<()> {
    nonempty_name(what, name)?;
    if name.contains(['/', '\\']) || name == "." || name == ".." {
        return Err(Error::Constraint(format!(
            "{what} name {name:?} is unsafe in a storage path"
        )));
    }
    Ok(())
}

/// A file's partition values as stored records, indexed by key position.
fn file_partition_values(values: &[String]) -> Vec<FilePartitionValue> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| FilePartitionValue {
            partition_key_index: index as u64,
            partition_value: value.clone(),
        })
        .collect()
}

fn new_column(
    table_id: u64,
    column_id: u64,
    column_order: u64,
    begin_snapshot: u64,
    parent_column: Option<u64>,
    def: &ColumnDef,
) -> ColumnValue {
    ColumnValue {
        column_id,
        begin_snapshot,
        end_snapshot: None,
        table_id,
        column_order,
        column_name: def.name.clone(),
        column_type: def.column_type.clone(),
        initial_default: None,
        default_value: def.default_value.clone(),
        nulls_allowed: def.nulls_allowed,
        parent_column,
        default_value_type: None,
        default_value_dialect: None,
        tags: vec![],
    }
}

/// Validates every node of `defs`: names non-empty and unique among
/// siblings, types storable.
fn validate_column_defs(defs: &[ColumnDef]) -> Result<()> {
    let mut seen = HashSet::with_capacity(defs.len());
    for def in defs {
        nonempty_name("column", &def.name)?;
        ensure_inlinable(&def.name, &def.column_type)?;
        if !seen.insert(&def.name) {
            return Err(Error::Constraint(format!("duplicate column {}", def.name)));
        }
        validate_column_defs(&def.children)?;
    }

    Ok(())
}

/// How many column records `defs` becomes: one per node of every tree.
fn column_node_count(defs: &[ColumnDef]) -> u64 {
    defs.iter()
        .map(|def| 1 + column_node_count(&def.children))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::{CatalogSnapshot, FileColumnStats},
        store::{
            index_encoding::{IndexKeyValue, IntWidth},
            proto::SnapshotValue,
        },
    };

    fn empty_transaction() -> Transaction {
        let snapshot = SnapshotValue {
            snapshot_id: 4,
            snapshot_time_micros: 1,
            schema_version: 2,
            next_catalog_id: 10,
            next_file_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
            transaction_id: None,
            deleted_data_file_ids: Vec::new(),
        };
        Transaction::new(CatalogSnapshot::build(snapshot, &[], &[], None), 5)
    }

    /// A nested column: `name` of DuckLake's `struct` marker, holding
    /// `fields` as its children.
    fn nested(name: &str, marker: &str, fields: &[ColumnDef]) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            column_type: marker.into(),
            nulls_allowed: true,
            default_value: None,
            children: fields.to_vec(),
        }
    }

    /// Every live column as `(id, order, name, parent)`, in id order — the
    /// shape a `ducklake_column` probe against a real DuckLake catalog
    /// returns, so the expectations below can be read straight off one.
    fn column_layout(tx: &Transaction, table: TableId) -> Vec<(u64, u64, String, Option<u64>)> {
        let mut layout: Vec<(u64, u64, String, Option<u64>)> = tx
            .columns_of(table)
            .into_iter()
            .map(|c| {
                (
                    c.id.get(),
                    c.position,
                    c.name,
                    c.parent_column.map(ColumnId::get),
                )
            })
            .collect();
        layout.sort_by_key(|(id, ..)| *id);
        layout
    }

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            column_type: "BIGINT".into(),
            nulls_allowed: true,
            default_value: None,
            children: Vec::new(),
        }
    }

    #[test]
    fn a_retried_index_step_never_regresses_its_source_cursor() {
        let mut transaction = empty_transaction();
        let schema = transaction.create_schema("s").unwrap();
        let table = transaction.create_table(schema, "t", &[col("a")]).unwrap();
        let index = transaction
            .create_index_staged(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                },
            )
            .unwrap();

        transaction
            .build_index_source_step(index, &[], false, Some((9, 90)))
            .unwrap();
        transaction
            .build_index_source_step(index, &[], false, Some((9, 20)))
            .unwrap();

        let value = &transaction.state.indexes[&table.get()][&index.get()];
        assert_eq!(value.build_cursor_file, Some(9));
        assert_eq!(value.build_cursor_position, Some(90));
    }

    #[test]
    fn a_final_deferred_repair_uses_the_non_schema_alter_fence() {
        let mut transaction = empty_transaction();
        let schema = transaction.create_schema("s").unwrap();
        let table = transaction.create_table(schema, "t", &[col("a")]).unwrap();
        let index = transaction
            .create_index_staged(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                },
            )
            .unwrap();
        transaction
            .state
            .indexes
            .get_mut(&table.get())
            .unwrap()
            .get_mut(&index.get())
            .unwrap()
            .build_state = Some("maintaining".to_owned());
        transaction.ops.clear();

        transaction
            .build_index_source_step(index, &[], true, Some((9, 90)))
            .unwrap();

        assert_eq!(
            transaction.ops,
            vec![Operation::FinishIndexMaintenance {
                table_id: table.get()
            }]
        );
        assert!(!transaction.ops[0].is_schema_changing());
    }

    /// Nested field ids are allocated in **pre-order**, from the same two
    /// counters top-level columns draw from: a field takes the next id after
    /// its parent, before its parent's next sibling, and `column_order`
    /// tracks it step for step.
    ///
    /// The expectation is stock DuckLake's, read off a real catalog fed
    /// `CREATE TABLE t(a BIGINT, s STRUCT(x BIGINT, y VARCHAR), l BIGINT[])`:
    /// ids 1..6 as `a, s, x, y, l, element`, each field naming its parent.
    #[test]
    fn nested_columns_allocate_field_ids_in_pre_order() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction
            .create_table(
                s,
                "t",
                &[
                    col("a"),
                    nested("s", "STRUCT", &[col("x"), col("y")]),
                    nested("l", "LIST", &[col("element")]),
                ],
            )
            .unwrap();

        assert_eq!(
            column_layout(&transaction, t),
            vec![
                (1, 1, "a".to_string(), None),
                (2, 2, "s".to_string(), None),
                (3, 3, "x".to_string(), Some(2)),
                (4, 4, "y".to_string(), Some(2)),
                (5, 5, "l".to_string(), None),
                (6, 6, "element".to_string(), Some(5)),
            ]
        );
    }

    /// A nested `add_column` continues both counters past the table's high
    /// water mark rather than filling a dropped column's gap, and nests to
    /// arbitrary depth. Stock DuckLake, after dropping `a` from the table
    /// above and adding `m STRUCT(p BIGINT, q STRUCT(r BIGINT))`, allocates
    /// 7..10 — never reusing 1.
    #[test]
    fn a_nested_add_column_continues_past_the_high_water_mark() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction
            .create_table(
                s,
                "t",
                &[
                    col("a"),
                    nested("s", "STRUCT", &[col("x"), col("y")]),
                    nested("l", "LIST", &[col("element")]),
                ],
            )
            .unwrap();

        transaction.drop_column(t, ColumnId::new(1)).unwrap();
        let added = transaction
            .add_column(
                t,
                &nested(
                    "m",
                    "STRUCT",
                    &[col("p"), nested("q", "STRUCT", &[col("r")])],
                ),
            )
            .unwrap();

        assert_eq!(added, ColumnId::new(7), "the root's id is the one returned");
        assert_eq!(
            column_layout(&transaction, t),
            vec![
                (2, 2, "s".to_string(), None),
                (3, 3, "x".to_string(), Some(2)),
                (4, 4, "y".to_string(), Some(2)),
                (5, 5, "l".to_string(), None),
                (6, 6, "element".to_string(), Some(5)),
                (7, 7, "m".to_string(), None),
                (8, 8, "p".to_string(), Some(7)),
                (9, 9, "q".to_string(), Some(7)),
                (10, 10, "r".to_string(), Some(9)),
            ]
        );
    }

    /// Dropping a nested column drops every field beneath it, to any depth —
    /// stock DuckLake ends the parent and its whole subtree in one snapshot,
    /// because a field whose parent is gone is not a column anyone can name.
    #[test]
    fn dropping_a_nested_column_takes_its_whole_subtree() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction
            .create_table(
                s,
                "t",
                &[
                    col("a"),
                    nested(
                        "m",
                        "STRUCT",
                        &[col("p"), nested("q", "STRUCT", &[col("r")])],
                    ),
                ],
            )
            .unwrap();

        transaction.drop_column(t, ColumnId::new(2)).unwrap();

        assert_eq!(
            column_layout(&transaction, t),
            vec![(1, 1, "a".to_string(), None)],
            "the struct, its field, its nested struct and that struct's field all go"
        );
    }

    /// A field's name is scoped to its parent, so two structs may each hold
    /// an `x` — pinned against stock DuckLake, which allocates both. A
    /// *top-level* collision is still refused.
    #[test]
    fn nested_field_names_are_scoped_to_their_parent() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction
            .create_table(
                s,
                "t",
                &[
                    nested("s", "STRUCT", &[col("x")]),
                    nested("s2", "STRUCT", &[col("x")]),
                ],
            )
            .unwrap();
        assert_eq!(transaction.columns_of(t).len(), 4);

        // Renaming a field to its own sibling's name is still a collision.
        assert!(matches!(
            transaction.rename_column(t, ColumnId::new(2), "x"),
            Err(Error::AlreadyExists(_))
        ));
        // But to a name only another parent's field holds, it is not.
        transaction
            .rename_column(t, ColumnId::new(4), "x2")
            .unwrap();

        // Two siblings sharing a name is refused where the tree enters.
        let duplicate =
            transaction.add_column(t, &nested("d", "STRUCT", &[col("dup"), col("dup")]));
        assert!(matches!(duplicate, Err(Error::Constraint(_))));
    }

    #[test]
    fn create_index_refuses_a_non_indexable_column() {
        // The indexability rule is enforced by the verb, so the embedding
        // path refuses a HUGEINT index just as the extension path does.
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction
            .create_table(
                s,
                "t",
                &[ColumnDef {
                    name: "big".into(),
                    column_type: "HUGEINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                    children: Vec::new(),
                }],
            )
            .unwrap();
        let col_big = transaction.state.columns_of(t)[0].id;
        let err = transaction
            .create_index(
                t,
                &IndexDef {
                    name: "by_big".into(),
                    columns: vec![col_big],
                    unique: true,
                },
                &[],
            )
            .unwrap_err();
        assert!(matches!(err, Error::Constraint(_)), "{err}");
    }

    #[test]
    fn create_read_your_own_writes_and_id_allocation() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("sales").unwrap();
        assert_eq!(s, SchemaId::new(10));
        let t = transaction
            .create_table(s, "orders", &[col("id"), col("qty")])
            .unwrap();
        assert_eq!(t, TableId::new(11));
        // Reads on the Transaction see staged state (Deref to the working view).
        assert_eq!(transaction.schema_by_name("sales").unwrap().id, s);
        let cols = transaction.columns_of(t);
        assert_eq!(cols[0].id, ColumnId::new(1));
        assert_eq!(cols[1].id, ColumnId::new(2));
        // Records are stamped with the commit's snapshot id.
        assert_eq!(transaction.state().tables[&11].begin_snapshot, 5);
        // The counter is seeded past the ids just handed out.
        assert_eq!(transaction.state().tables[&11].next_column_id, 3);

        let parts = transaction.into_parts();
        let ops = parts.operations;
        assert_eq!(parts.next_catalog_id, 12);
        assert_eq!(
            ops,
            vec![
                Operation::CreateSchema {
                    schema_id: 10,
                    name: "sales".into(),
                },
                Operation::CreateTable {
                    schema_id: 10,
                    table_id: 11,
                    schema_name: "sales".into(),
                    table_name: "orders".into(),
                },
            ]
        );
    }

    #[test]
    fn name_collisions_and_missing_entities() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("sales").unwrap();
        assert!(matches!(
            transaction.create_schema("sales"),
            Err(Error::AlreadyExists(_))
        ));
        transaction.create_table(s, "orders", &[col("id")]).unwrap();
        assert!(matches!(
            transaction.create_table(s, "orders", &[col("id")]),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            transaction.create_table(SchemaId::new(99), "x", &[col("id")]),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            transaction.rename_table(TableId::new(99), "y"),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn constraints() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("sales").unwrap();
        let t = transaction.create_table(s, "orders", &[col("id")]).unwrap();
        // A schema with live tables cannot be dropped.
        assert!(matches!(
            transaction.drop_schema(s),
            Err(Error::Constraint(_))
        ));
        // The last live column cannot be dropped.
        assert!(matches!(
            transaction.drop_column(t, ColumnId::new(1)),
            Err(Error::Constraint(_))
        ));
        // Tables need at least one column, without duplicate names.
        assert!(matches!(
            transaction.create_table(s, "empty", &[]),
            Err(Error::Constraint(_))
        ));
        assert!(matches!(
            transaction.create_table(s, "dup", &[col("a"), col("a")]),
            Err(Error::Constraint(_))
        ));
        // Drop the table, then the schema drop succeeds.
        transaction.drop_table(t).unwrap();
        transaction.drop_schema(s).unwrap();
    }

    #[test]
    fn column_order_numbers_from_one_and_keeps_gaps() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction
            .create_table(s, "t", &[col("a"), col("b"), col("c")])
            .unwrap();
        assert_eq!(
            transaction
                .columns_of(t)
                .iter()
                .map(|c| c.position)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        transaction.drop_column(t, ColumnId::new(2)).unwrap();
        transaction.add_column(t, &col("d")).unwrap();
        assert_eq!(
            transaction
                .columns_of(t)
                .iter()
                .map(|c| c.position)
                .collect::<Vec<_>>(),
            vec![1, 3, 4]
        );
    }

    #[test]
    fn column_ddl_allocates_fresh_field_ids() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction
            .create_table(s, "t", &[col("a"), col("b")])
            .unwrap();
        transaction.drop_column(t, ColumnId::new(2)).unwrap();
        // The dropped column's field id is never reused.
        let c = transaction.add_column(t, &col("c")).unwrap();
        assert_eq!(c, ColumnId::new(3));
        let cols = transaction.columns_of(t);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[1].position, 2);

        transaction
            .rename_column(t, ColumnId::new(1), "a2")
            .unwrap();
        assert_eq!(transaction.columns_of(t)[0].name, "a2");
        assert!(matches!(
            transaction.rename_column(t, ColumnId::new(1), "c"),
            Err(Error::AlreadyExists(_))
        ));

        transaction
            .alter_column(
                t,
                c,
                ColumnAlteration {
                    column_type: Some("VARCHAR".into()),
                    nulls_allowed: Some(false),
                    default_value: Some(Some("''".into())),
                },
            )
            .unwrap();
        let altered = &transaction.columns_of(t)[1];
        assert_eq!(altered.column_type, "VARCHAR");
        assert!(!altered.nulls_allowed);
        assert_eq!(altered.default_value, Some("''".into()));
    }

    #[test]
    fn table_moves_and_renames_validate_against_target() {
        let mut transaction = empty_transaction();
        let s1 = transaction.create_schema("a").unwrap();
        let s2 = transaction.create_schema("b").unwrap();
        let t1 = transaction.create_table(s1, "t", &[col("x")]).unwrap();
        let _t2 = transaction.create_table(s2, "t", &[col("x")]).unwrap();
        // Moving into a schema that already has a table of that name fails.
        assert!(matches!(
            transaction.set_table_schema(t1, s2),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            transaction.set_table_schema(t1, SchemaId::new(99)),
            Err(Error::NotFound(_))
        ));
        transaction.rename_table(t1, "t_renamed").unwrap();
        transaction.set_table_schema(t1, s2).unwrap();
        assert_eq!(transaction.tables_in(s2).len(), 2);
        // Each mutation of an existing table classifies as an alter.
        let ops = transaction.into_parts().operations;
        let alters = ops
            .iter()
            .filter(|op| matches!(op, Operation::AlterTable { table_id } if *table_id == t1.get()))
            .count();
        assert_eq!(alters, 2);
    }

    #[test]
    fn self_targeted_renames_and_moves_error() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        assert!(matches!(
            transaction.rename_table(t, "t"),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            transaction.set_table_schema(t, s),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            transaction.rename_column(t, ColumnId::new(1), "a"),
            Err(Error::AlreadyExists(_))
        ));
    }

    #[test]
    fn add_column_floors_allocation_at_live_ids() {
        // A table version authored without the counter (next_column_id
        // absent, i.e. 0) must not let allocation regress below the ids
        // already live on the table.
        use crate::{catalog::CatalogSnapshot, store::read::EntityRecord};

        let snapshot = SnapshotValue {
            snapshot_id: 4,
            snapshot_time_micros: 1,
            schema_version: 0,
            next_catalog_id: 10,
            next_file_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
            transaction_id: None,
            deleted_data_file_ids: Vec::new(),
        };
        let table = TableValue {
            table_id: 1,
            table_uuid: "uuid-t1".into(),
            begin_snapshot: 1,
            end_snapshot: None,
            schema_id: 0,
            table_name: "t".into(),
            path: "t/".into(),
            path_is_relative: true,
            next_column_id: 0,
        };
        let columns = [1u64, 2].map(|id| {
            EntityRecord::Column(ColumnValue {
                column_id: id,
                begin_snapshot: 1,
                end_snapshot: None,
                table_id: 1,
                column_order: id,
                column_name: format!("c{id}"),
                column_type: "BIGINT".into(),
                initial_default: None,
                default_value: None,
                nulls_allowed: true,
                parent_column: None,
                default_value_type: None,
                default_value_dialect: None,
                tags: vec![],
            })
        });
        let mut current = vec![EntityRecord::Table(table)];
        current.extend(columns);
        let state = CatalogSnapshot::build(snapshot, &current, &[], None);
        let mut transaction = Transaction::new(state, 5);
        let c = transaction.add_column(TableId::new(1), &col("c")).unwrap();
        assert_eq!(c, ColumnId::new(3));
    }

    fn datafile(rows: u64, stats: Vec<FileColumnStats>) -> DataFile {
        DataFile {
            path: "f.parquet".into(),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: rows,
            file_size_bytes: rows * 10,
            footer_size: 4,
            encryption_key: None,
            partition_values: vec![],
            column_stats: stats,
        }
    }

    #[test]
    fn register_allocates_row_ids_and_maintains_stats() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        // create_table minted the stats record.
        let stats = transaction.table_stats(t).unwrap();
        assert_eq!((stats.record_count, stats.next_row_id), (0, 0));

        let f1 = transaction
            .register_data_file(t, datafile(100, vec![]), &[])
            .unwrap();
        let f2 = transaction
            .register_data_file(t, datafile(50, vec![]), &[])
            .unwrap();
        assert_ne!(f1, f2);
        let files = transaction.data_files_of(t);
        assert_eq!(files[0].row_id_start, Some(0));
        assert_eq!(files[1].row_id_start, Some(100));
        let stats = transaction.table_stats(t).unwrap();
        assert_eq!(stats.record_count, 150);
        assert_eq!(stats.next_row_id, 150);
        assert_eq!(stats.file_size_bytes, 1500);
    }

    #[test]
    fn register_validates_table_and_stat_columns() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        assert!(matches!(
            transaction.register_data_file(TableId::new(99), datafile(1, vec![]), &[]),
            Err(Error::NotFound(_))
        ));
        let bad_stats = vec![FileColumnStats {
            column_id: ColumnId::new(99),
            column_size_bytes: 1,
            value_count: 1,
            null_count: 0,
            min_value: None,
            max_value: None,
            contains_nan: None,
            extra_stats: None,
        }];
        assert!(matches!(
            transaction.register_data_file(t, datafile(1, bad_stats), &[]),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn register_rejects_an_out_of_range_index_ordinal() {
        use crate::store::index_encoding::{IndexKeyValue, IntWidth};

        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        let col_a = transaction.state.columns_of(t)[0].id;
        let index = transaction
            .create_index(
                t,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![col_a],
                    unique: true,
                },
                &[],
            )
            .unwrap();

        // The file has three rows (ordinals 0..=2); an entry at ordinal 3
        // would index a row id outside the file's range.
        let entry = FileIndexEntry {
            index,
            ordinal: 3,
            values: vec![Some(IndexKeyValue::Int {
                value: 7,
                width: IntWidth::I64,
            })],
        };
        assert!(matches!(
            transaction.register_data_file(t, datafile(3, vec![]), &[entry]),
            Err(Error::Constraint(_))
        ));
    }

    #[test]
    fn expire_cascades_delete_files_and_preserves_next_row_id() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        let f = transaction
            .register_data_file(t, datafile(100, vec![]), &[])
            .unwrap();
        let d = transaction
            .register_delete_file(
                t,
                DeleteFile {
                    data_file_id: f,
                    path: "d.parquet".into(),
                    path_is_relative: true,
                    format: "parquet".into(),
                    delete_count: 5,
                    file_size_bytes: 50,
                    footer_size: 4,
                    encryption_key: None,
                },
                &[],
            )
            .unwrap();
        assert_eq!(transaction.delete_files_of(t)[0].id, d);

        transaction.expire_data_file(t, f).unwrap();
        assert!(transaction.data_files_of(t).is_empty());
        assert!(
            transaction.delete_files_of(t).is_empty(),
            "delete file cascades"
        );
        let stats = transaction.table_stats(t).unwrap();
        assert_eq!(stats.record_count, 0);
        // The row-id counter never regresses.
        assert_eq!(stats.next_row_id, 100);
        let f2 = transaction
            .register_data_file(t, datafile(10, vec![]), &[])
            .unwrap();
        assert_eq!(transaction.data_files_of(t)[0].row_id_start, Some(100));
        let _ = f2;
    }

    #[test]
    fn delete_file_requires_live_data_file() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        assert!(matches!(
            transaction.register_delete_file(
                t,
                DeleteFile {
                    data_file_id: DataFileId::new(99),
                    path: "d.parquet".into(),
                    path_is_relative: true,
                    format: "parquet".into(),
                    delete_count: 1,
                    file_size_bytes: 10,
                    footer_size: 4,
                    encryption_key: None,
                },
                &[],
            ),
            Err(Error::NotFound(_))
        ));
    }

    /// One live delete file per data file: a second registration against
    /// the same target is refused until the first is expired (a new delete
    /// file carries all deletes and supersedes its predecessor).
    #[test]
    fn second_live_delete_file_for_same_data_file_is_refused() {
        let delete_file = |f| DeleteFile {
            data_file_id: f,
            path: "d.parquet".into(),
            path_is_relative: true,
            format: "parquet".into(),
            delete_count: 1,
            file_size_bytes: 10,
            footer_size: 4,
            encryption_key: None,
        };

        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        let f = transaction
            .register_data_file(t, datafile(100, vec![]), &[])
            .unwrap();

        let first = transaction
            .register_delete_file(t, delete_file(f), &[])
            .unwrap();
        assert!(matches!(
            transaction.register_delete_file(t, delete_file(f), &[]),
            Err(Error::Constraint(_))
        ));

        // Expiring the predecessor frees the slot.
        transaction.expire_delete_file(t, first).unwrap();
        transaction
            .register_delete_file(t, delete_file(f), &[])
            .unwrap();
    }

    #[test]
    fn stats_verbs_update_verbatim_and_preserve_row_counter() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        transaction
            .register_data_file(t, datafile(100, vec![]), &[])
            .unwrap();
        transaction.update_table_stats(t, 42, 420).unwrap();
        let stats = transaction.table_stats(t).unwrap();
        assert_eq!((stats.record_count, stats.file_size_bytes), (42, 420));
        assert_eq!(
            stats.next_row_id, 100,
            "override cannot regress the counter"
        );

        transaction
            .update_column_stats(
                t,
                ColumnId::new(1),
                ColumnStats {
                    contains_null: Some(false),
                    contains_nan: None,
                    min_value: Some("9".into()),
                    max_value: Some("10".into()),
                    extra_stats: None,
                },
            )
            .unwrap();
        let cs = transaction.column_stats(t, ColumnId::new(1)).unwrap();
        assert_eq!(cs.min_value.as_deref(), Some("9"));
        assert!(matches!(
            transaction.update_column_stats(t, ColumnId::new(9), ColumnStats::default()),
            Err(Error::NotFound(_))
        ));

        // Dropping a column removes its table-level stats too, symmetric
        // with delete_table removing table_stats.
        let c2 = transaction.add_column(t, &col("b")).unwrap();
        transaction
            .update_column_stats(
                t,
                c2,
                ColumnStats {
                    contains_null: Some(true),
                    contains_nan: None,
                    min_value: Some("1".into()),
                    max_value: Some("2".into()),
                    extra_stats: None,
                },
            )
            .unwrap();
        assert!(transaction.column_stats(t, c2).is_some());
        transaction.drop_column(t, c2).unwrap();
        assert!(transaction.column_stats(t, c2).is_none());
    }

    #[test]
    fn views_share_the_relation_namespace() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "orders", &[col("a")]).unwrap();
        assert!(matches!(
            transaction.create_view(s, "orders", "duckdb", "SELECT 1"),
            Err(Error::AlreadyExists(_))
        ));
        let v = transaction
            .create_view(s, "v_orders", "duckdb", "SELECT 1")
            .unwrap();
        assert!(matches!(
            transaction.create_table(s, "v_orders", &[col("a")]),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            transaction.rename_table(t, "v_orders"),
            Err(Error::AlreadyExists(_))
        ));
        assert_eq!(transaction.view_by_name(s, "v_orders").unwrap().id, v);
        // A schema with live views cannot be dropped.
        transaction.drop_table(t).unwrap();
        assert!(matches!(
            transaction.drop_schema(s),
            Err(Error::Constraint(_))
        ));
        transaction.drop_view(v).unwrap();
        transaction.drop_schema(s).unwrap();
    }

    #[test]
    fn alter_view_replaces_definition() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let v = transaction
            .create_view(s, "v", "duckdb", "SELECT 1")
            .unwrap();
        transaction.alter_view(v, "duckdb", "SELECT 2").unwrap();
        assert_eq!(transaction.view_by_id(v).unwrap().sql, "SELECT 2");
        assert!(matches!(
            transaction.alter_view(ViewId::new(99), "d", "s"),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn options_set_unset_and_validate_scopes() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        transaction
            .set_option(OptionScope::Global, "k", "g")
            .unwrap();
        transaction
            .set_option(OptionScope::Table(t), "k", "t")
            .unwrap();
        assert_eq!(
            transaction.option(OptionScope::Table(t), "k").as_deref(),
            Some("t")
        );
        transaction
            .unset_option(OptionScope::Table(t), "k")
            .unwrap();
        assert_eq!(
            transaction.option(OptionScope::Table(t), "k").as_deref(),
            Some("g")
        );
        transaction
            .unset_option(OptionScope::Table(t), "missing")
            .unwrap();
        assert!(matches!(
            transaction.set_option(OptionScope::Table(TableId::new(99)), "k", "v"),
            Err(Error::NotFound(_))
        ));
        // Option mutations stage no ops; the two DDL ops remain.
        let ops = transaction.into_parts().operations;
        assert_eq!(ops.len(), 2);
    }

    /// The bootstrap `main` schema (id 0) is the catalog's default —
    /// DuckDB resolves unqualified names against it — and must survive
    /// every commit. Only the verb path could drop it; DuckDB refuses the
    /// SQL-level drop before it ever reaches the staged path.
    #[test]
    fn bootstrap_main_schema_cannot_be_dropped() {
        let snapshot = SnapshotValue {
            snapshot_id: 4,
            snapshot_time_micros: 1,
            schema_version: 2,
            next_catalog_id: 10,
            next_file_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
            transaction_id: None,
            deleted_data_file_ids: Vec::new(),
        };
        let main = SchemaValue {
            schema_id: 0,
            schema_uuid: "u".into(),
            begin_snapshot: 0,
            end_snapshot: None,
            schema_name: "main".into(),
            path: "main/".into(),
            path_is_relative: true,
        };
        let mut transaction = Transaction::new(
            CatalogSnapshot::build(
                snapshot,
                &[crate::store::read::EntityRecord::Schema(main)],
                &[],
                None,
            ),
            5,
        );

        assert!(matches!(
            transaction.drop_schema(SchemaId::new(0)),
            Err(Error::Constraint(_))
        ));

        // Any other empty schema still drops.
        let other = transaction.create_schema("other").unwrap();
        transaction.drop_schema(other).unwrap();
    }

    /// Relation names flow into derived storage paths: a separator nests
    /// or collides prefixes and a dot segment escapes the catalog root,
    /// so schema, table, and view names refuse them.
    #[test]
    fn path_unsafe_names_are_refused() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();

        for name in ["a/b", "a\\b", ".", ".."] {
            assert!(
                matches!(transaction.create_schema(name), Err(Error::Constraint(_))),
                "schema {name:?}"
            );
            assert!(
                matches!(
                    transaction.create_table(s, name, &[col("a")]),
                    Err(Error::Constraint(_))
                ),
                "table {name:?}"
            );
            assert!(
                matches!(transaction.rename_table(t, name), Err(Error::Constraint(_))),
                "rename {name:?}"
            );
            assert!(
                matches!(
                    transaction.create_view(s, name, "duckdb", "SELECT 1"),
                    Err(Error::Constraint(_))
                ),
                "view {name:?}"
            );
        }
    }

    /// Every name-taking verb refuses the empty string: an empty name is
    /// unaddressable and an empty schema name persists the path `"/"`.
    #[test]
    fn empty_names_are_refused() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        let column = transaction.columns_of(t)[0].id;

        let refused = [
            transaction.create_schema("").err(),
            transaction.create_table(s, "", &[col("a")]).err(),
            transaction.create_table(s, "t2", &[col("")]).err(),
            transaction.rename_table(t, "").err(),
            transaction.add_column(t, &col("")).err(),
            transaction.rename_column(t, column, "").err(),
            transaction.create_view(s, "", "duckdb", "SELECT 1").err(),
            transaction.set_option(OptionScope::Global, "", "v").err(),
        ];
        for err in refused {
            assert!(matches!(err, Some(Error::Constraint(_))), "{err:?}");
        }
    }

    /// The global `encrypted` option is fixed at catalog creation: set and
    /// unset both refuse it, while a non-global `encrypted` key (or any
    /// other global key) stays writable.
    #[test]
    fn global_encrypted_option_is_reserved() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();

        assert!(matches!(
            transaction.set_option(OptionScope::Global, "encrypted", "true"),
            Err(Error::Constraint(_))
        ));
        assert!(matches!(
            transaction.unset_option(OptionScope::Global, "encrypted"),
            Err(Error::Constraint(_))
        ));

        transaction
            .set_option(OptionScope::Table(t), "encrypted", "x")
            .unwrap();
        transaction
            .set_option(OptionScope::Global, "other", "v")
            .unwrap();
    }

    /// A `DeleteFile` over `data_file_id` with `delete_count` deletes —
    /// the repeated literal the removal tests share.
    fn delete_file_over(data_file_id: DataFileId, delete_count: u64) -> DeleteFile {
        DeleteFile {
            data_file_id,
            path: "d.parquet".into(),
            path_is_relative: true,
            format: "parquet".into(),
            delete_count,
            file_size_bytes: 50,
            footer_size: 4,
            encryption_key: None,
        }
    }

    /// A `DataFileValue` carrying per-row ids, placed directly in state —
    /// only DuckLake's staged path can create one, but the embedding API
    /// must still maintain deletes against it.
    fn seed_per_row_id_file(transaction: &mut Transaction, table: TableId, data_file_id: u64) {
        transaction.state.put_data_file(DataFileValue {
            data_file_id,
            table_id: table.get(),
            begin_snapshot: 1,
            end_snapshot: None,
            file_order: None,
            path: "rewrite.parquet".into(),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: 3,
            file_size_bytes: 1024,
            footer_size: 64,
            row_id_start: None,
            partition_id: None,
            encryption_key: None,
            mapping_id: None,
            partial_max: None,
            partition_values: vec![],
        });
    }

    /// One `BIGINT` removal value.
    fn removal_value(value: i64) -> Vec<Option<IndexKeyValue>> {
        vec![Some(IndexKeyValue::Int {
            value: i128::from(value),
            width: IntWidth::I64,
        })]
    }

    #[test]
    fn delete_removals_land_verbatim_against_a_per_row_id_target() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        let col_a = transaction.state.columns_of(t)[0].id;
        let index = transaction
            .create_index(
                t,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![col_a],
                    unique: false,
                },
                &[],
            )
            .unwrap();
        seed_per_row_id_file(&mut transaction, t, 77);

        transaction
            .register_delete_file(
                t,
                delete_file_over(DataFileId::new(77), 1),
                &[FileIndexRemoval {
                    index,
                    row_id: 42,
                    values: removal_value(7),
                }],
            )
            .unwrap();

        assert!(
            transaction
                .index_entries
                .iter()
                .any(|e| e.delete && e.row_id == 42),
            "the removal is staged under the supplied row id"
        );
    }

    #[test]
    fn delete_removals_are_range_checked_against_a_dense_target() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        let col_a = transaction.state.columns_of(t)[0].id;
        let index = transaction
            .create_index(
                t,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![col_a],
                    unique: false,
                },
                &[],
            )
            .unwrap();
        // Rows 0..=2 land as ids 0..=2; the file's entries ride
        // registration.
        let file = transaction
            .register_data_file(
                t,
                datafile(3, vec![]),
                &[
                    FileIndexEntry {
                        index,
                        ordinal: 0,
                        values: removal_value(10),
                    },
                    FileIndexEntry {
                        index,
                        ordinal: 1,
                        values: removal_value(20),
                    },
                    FileIndexEntry {
                        index,
                        ordinal: 2,
                        values: removal_value(30),
                    },
                ],
            )
            .unwrap();

        // An id inside the range [0, 3) stages; row id 1 kills value 20.
        transaction
            .register_delete_file(
                t,
                delete_file_over(file, 1),
                &[FileIndexRemoval {
                    index,
                    row_id: 1,
                    values: removal_value(20),
                }],
            )
            .unwrap();
        assert!(
            transaction
                .index_entries
                .iter()
                .any(|e| e.delete && e.row_id == 1)
        );
    }

    #[test]
    fn delete_removals_outside_a_dense_targets_range_are_refused() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        let col_a = transaction.state.columns_of(t)[0].id;
        let index = transaction
            .create_index(
                t,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![col_a],
                    unique: false,
                },
                &[],
            )
            .unwrap();
        let file = transaction
            .register_data_file(
                t,
                datafile(3, vec![]),
                &[
                    FileIndexEntry {
                        index,
                        ordinal: 0,
                        values: removal_value(10),
                    },
                    FileIndexEntry {
                        index,
                        ordinal: 1,
                        values: removal_value(20),
                    },
                    FileIndexEntry {
                        index,
                        ordinal: 2,
                        values: removal_value(30),
                    },
                ],
            )
            .unwrap();

        // The file holds ids 0..=2, so row id 3 names no row of it.
        let err = transaction
            .register_delete_file(
                t,
                delete_file_over(file, 1),
                &[FileIndexRemoval {
                    index,
                    row_id: 3,
                    values: removal_value(30),
                }],
            )
            .unwrap_err();
        assert!(matches!(err, Error::Constraint(_)), "{err}");
    }

    #[test]
    fn delete_removals_against_a_dense_target_whose_range_overflows_are_refused() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        let col_a = transaction.state.columns_of(t)[0].id;
        let index = transaction
            .create_index(
                t,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![col_a],
                    unique: false,
                },
                &[],
            )
            .unwrap();
        // A dense file whose recorded range end runs off the top of u64:
        // start + record_count cannot be represented, so no id is in range.
        transaction.state.put_data_file(DataFileValue {
            data_file_id: 88,
            table_id: t.get(),
            begin_snapshot: 1,
            end_snapshot: None,
            file_order: None,
            path: "dense.parquet".into(),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: 3,
            file_size_bytes: 1024,
            footer_size: 64,
            row_id_start: Some(u64::MAX - 1),
            partition_id: None,
            encryption_key: None,
            mapping_id: None,
            partial_max: None,
            partition_values: vec![],
        });

        let err = transaction
            .register_delete_file(
                t,
                delete_file_over(DataFileId::new(88), 1),
                &[FileIndexRemoval {
                    index,
                    row_id: u64::MAX,
                    values: removal_value(30),
                }],
            )
            .unwrap_err();
        assert!(matches!(err, Error::Constraint(_)), "{err}");
    }
}
