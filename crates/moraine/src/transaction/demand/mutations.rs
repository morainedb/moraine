//! Fallible operations over demand-loaded mutation premises.

use super::{
    ColumnAlteration, ColumnDef, ColumnId, ColumnOrder, ColumnStats, DataFile, DataFileId,
    DeleteFile, DeleteFileId, EntityKey, FileIndexEntry, FileIndexRemoval, FlushedDataFile,
    IndexDef, IndexEntry, IndexId, IndexMaintenance, IndexState, InlineChunk, Kind, MacroId,
    MacroImplementationDef, OptionScope, PartitionColumnDef, PartitionId, Result, SchemaId,
    SnapshotId, SortId, SortKeyDef, TableId, TagTarget, Transaction, ViewId,
};

impl Transaction {
    /// Creates a schema.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::AlreadyExists`] if a schema with that name
    /// already exists, or [`crate::Error::Constraint`] if the name is empty
    /// or unsafe in the storage path derived from it.
    pub async fn create_schema(&mut self, name: &str) -> Result<SchemaId> {
        self.namespace(Kind::Schema, 0, Some(name)).await?;
        self.inner.create_schema(name)
    }
    /// Drops a schema.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the schema does not exist.
    /// Returns [`crate::Error::Constraint`] if the schema still contains live
    /// tables, views or macros, or is the bootstrap `main` schema.
    pub async fn drop_schema(&mut self, schema: SchemaId) -> Result<()> {
        self.point(EntityKey::Schema {
            schema_id: schema.get(),
        })
        .await?;
        for kind in [Kind::Table, Kind::View, Kind::Macro] {
            self.namespace(kind, schema.get(), None).await?;
        }
        self.inner.drop_schema(schema)
    }
    /// Creates a table with its columns.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the schema does not exist.
    /// Returns [`crate::Error::AlreadyExists`] if a table with that name
    /// already exists in the schema.
    /// Returns [`crate::Error::Constraint`] if the column list is empty or
    /// contains duplicate column names.
    /// Returns [`crate::Error::Unsupported`] if a column's type is one moraine
    /// cannot store.
    pub async fn create_table(
        &mut self,
        schema: SchemaId,
        name: &str,
        columns: &[ColumnDef],
    ) -> Result<TableId> {
        self.relation(schema, name).await?;
        self.inner.create_table(schema, name, columns)
    }
    /// Renames a table within its schema. Renaming to the current name
    /// errors, matching SQL engines.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table does not exist.
    /// Returns [`crate::Error::AlreadyExists`] if a table with that name
    /// already exists in the same schema (including this table itself).
    pub async fn rename_table(&mut self, table: TableId, new_name: &str) -> Result<()> {
        self.definition(table).await?;
        if let Some(value) = self.inner.table_by_id(table) {
            self.relation(value.schema_id, new_name).await?;
        }
        self.inner.rename_table(table, new_name)
    }
    /// Moves a table to another schema. Moving to the current schema
    /// errors, matching SQL engines.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table or target schema does
    /// not exist.
    /// Returns [`crate::Error::AlreadyExists`] if a table with the same name
    /// already exists in the target schema (including this table itself
    /// when the target is its current schema), or an index name collides
    /// with one in the target schema.
    pub async fn set_table_schema(&mut self, table: TableId, new_schema: SchemaId) -> Result<()> {
        self.definition(table).await?;
        if let Some(value) = self.inner.table_by_id(table) {
            self.relation(new_schema, &value.name).await?;
        }
        self.index_namespace(new_schema).await?;
        self.inner.set_table_schema(table, new_schema)
    }
    /// Drops a table and its columns.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table does not exist.
    pub async fn drop_table(&mut self, table: TableId) -> Result<()> {
        self.definition(table).await?;
        for kind in [Kind::File, Kind::DeleteFile, Kind::TableColumnStats] {
            self.range(kind, Some(table.get())).await?;
        }
        self.options(OptionScope::Table(table)).await?;
        self.inner.drop_table(table)
    }
    /// Adds a column. Field ids and positions are never reused.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table does not exist.
    /// Returns [`crate::Error::AlreadyExists`] if a column with that name
    /// already exists in the table.
    /// Returns [`crate::Error::Unsupported`] if the column's type is one
    /// moraine cannot store.
    pub async fn add_column(&mut self, table: TableId, def: &ColumnDef) -> Result<ColumnId> {
        self.definition(table).await?;
        self.inner.add_column(table, def)
    }
    /// Renames a column. Renaming to the current name errors, matching
    /// SQL engines.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table or column does not
    /// exist. Returns [`crate::Error::AlreadyExists`] if a column with that
    /// name already exists in the table (including this column itself).
    pub async fn rename_column(
        &mut self,
        table: TableId,
        column: ColumnId,
        new_name: &str,
    ) -> Result<()> {
        self.definition(table).await?;
        self.inner.rename_column(table, column, new_name)
    }
    /// Alters a column's type, nullability, and/or default.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table or column does not
    /// exist. Returns [`crate::Error::Unsupported`] if the new type is one
    /// moraine cannot store.
    pub async fn alter_column(
        &mut self,
        table: TableId,
        column: ColumnId,
        alteration: ColumnAlteration,
    ) -> Result<()> {
        self.definition(table).await?;
        self.inner.alter_column(table, column, alteration)
    }
    /// Drops a column.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table or column does not
    /// exist. Returns [`crate::Error::Constraint`] if this is the last live
    /// top-level column of the table, or if the column or any field beneath
    /// it is indexed.
    ///
    /// Dropping a nested column drops every field beneath it.
    pub async fn drop_column(&mut self, table: TableId, column: ColumnId) -> Result<()> {
        self.definition(table).await?;
        self.range(Kind::TableColumnStats, Some(table.get()))
            .await?;
        self.inner.drop_column(table, column)
    }
    /// Sets a table's partition spec, replacing any spec already live; the
    /// old spec ends into history and files written under it keep
    /// referencing it. Transforms are stored verbatim and never parsed.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table or any referenced column
    /// is not live, or [`crate::Error::Constraint`] if `columns` is empty
    /// (use [`Self::clear_partitioning`]) or names one column twice.
    pub async fn set_partitioning(
        &mut self,
        table: TableId,
        columns: &[PartitionColumnDef],
    ) -> Result<PartitionId> {
        self.definition(table).await?;
        self.inner.set_partitioning(table, columns)
    }
    /// Unpartitions a table: its live spec ends into history.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table does not exist or
    /// carries no live partition spec.
    pub async fn clear_partitioning(&mut self, table: TableId) -> Result<()> {
        self.definition(table).await?;
        self.inner.clear_partitioning(table)
    }
    /// Sets a table's sort spec, replacing any spec already live; the old
    /// spec ends into history. Expressions, dialects, directions and null
    /// orders are stored verbatim and never parsed. Not a schema change:
    /// the catalog's schema version does not advance.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table is not live, or
    /// [`crate::Error::Constraint`] if `keys` is empty (use
    /// [`Self::clear_sorting`]).
    pub async fn set_sorting(&mut self, table: TableId, keys: &[SortKeyDef]) -> Result<SortId> {
        self.definition(table).await?;
        self.inner.set_sorting(table, keys)
    }
    /// Unsorts a table: its live spec ends into history.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table does not exist or
    /// carries no live sort spec.
    pub async fn clear_sorting(&mut self, table: TableId) -> Result<()> {
        self.definition(table).await?;
        self.inner.clear_sorting(table)
    }
    /// Creates an equality index over a table, staging entries for the
    /// writer-supplied backfill in the same commit.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table or any referenced column
    /// is not live. Returns [`crate::Error::AlreadyExists`] if an index
    /// with that name already exists in the schema (case-insensitive).
    /// Returns [`crate::Error::Constraint`] if the column list is empty, an
    /// entry's value count does not match the column count, an indexed
    /// value exceeds the size cap, or the backfill contains a duplicate of
    /// a unique value.
    pub async fn create_index(
        &mut self,
        table: TableId,
        def: &IndexDef,
        backfill: &[IndexEntry],
    ) -> Result<IndexId> {
        self.definition(table).await?;
        if let Some(value) = self.inner.table_by_id(table) {
            self.index_namespace(value.schema_id).await?;
        }
        self.inner.create_index(table, def, backfill)
    }
    /// Creates an index with explicit per-column sort orders — ascending or
    /// descending, NULLS FIRST or LAST. `orders` runs parallel to
    /// `def.columns`; an empty slice is all-ascending / NULLS LAST, exactly
    /// [`Self::create_index`].
    ///
    /// # Errors
    ///
    /// As [`Self::create_index`], plus [`crate::Error::Constraint`] if `orders`
    /// is non-empty and its length does not match the column count.
    pub async fn create_index_ordered(
        &mut self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        backfill: &[IndexEntry],
    ) -> Result<IndexId> {
        self.definition(table).await?;
        if let Some(value) = self.inner.table_by_id(table) {
            self.index_namespace(value.schema_id).await?;
        }
        self.inner
            .create_index_ordered(table, def, orders, backfill)
    }
    /// Create index ordered with maintenance after loading its validation
    /// dependencies.
    ///
    /// # Errors
    /// Returns a read, validation, or memory-admission error.
    pub async fn create_index_ordered_with_maintenance(
        &mut self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        maintenance: IndexMaintenance,
        backfill: &[IndexEntry],
    ) -> Result<IndexId> {
        self.definition(table).await?;
        if let Some(value) = self.inner.table_by_id(table) {
            self.index_namespace(value.schema_id).await?;
        }
        self.inner
            .create_index_ordered_with_maintenance(table, def, orders, maintenance, backfill)
    }
    /// Drops an equality index. The definition ends into history; its
    /// entries are orphaned and reclaimed lazily.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the index does not exist.
    pub async fn drop_index(&mut self, index: IndexId) -> Result<()> {
        self.index(index).await?;
        self.inner.drop_index(index)
    }
    /// The owning table id and definition of a live index.
    /// Records that a verifier pass agreed with the rows at `at`, or
    /// withdraws an earlier agreement when `at` is `None`. A plan that
    /// would let the index answer a predicate without the scan re-checking
    /// it reads this.
    ///
    /// `observed` is the definition version the pass read, and the call is
    /// refused if the definition has moved since — a rebuild landing
    /// between the pass and this commit must not inherit its agreement.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the index is not live, or
    /// [`crate::Error::Constraint`] if it is not ready — a building or
    /// repairing index is short of the rows by design — or if the
    /// definition moved.
    pub async fn record_index_verification(
        &mut self,
        index: IndexId,
        observed: SnapshotId,
        at: Option<SnapshotId>,
    ) -> Result<()> {
        self.index(index).await?;
        self.inner.record_index_verification(index, observed, at)
    }
    /// Begins a staged (multi-commit) index build. The definition lands in
    /// `building` state, serving no lookups; the host streams backfill
    /// batches through [`Self::build_index_step`] until a final step flips it
    /// ready. Writers maintain entries from this commit forward.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table or any referenced column
    /// is not live, [`crate::Error::AlreadyExists`] if the name is taken,
    /// or [`crate::Error::Constraint`] if the column list is empty.
    pub async fn create_index_staged(&mut self, table: TableId, def: &IndexDef) -> Result<IndexId> {
        self.definition(table).await?;
        if let Some(value) = self.inner.table_by_id(table) {
            self.index_namespace(value.schema_id).await?;
        }
        self.inner.create_index_staged(table, def)
    }
    /// Begins a staged build with explicit per-column sort orders, the
    /// staged counterpart to [`Self::create_index_ordered`].
    ///
    /// # Errors
    ///
    /// As [`Self::create_index_staged`], plus [`crate::Error::Constraint`] if
    /// `orders` is non-empty and its length differs from the column list's.
    pub async fn create_index_staged_ordered(
        &mut self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
    ) -> Result<IndexId> {
        self.definition(table).await?;
        if let Some(value) = self.inner.table_by_id(table) {
            self.index_namespace(value.schema_id).await?;
        }
        self.inner.create_index_staged_ordered(table, def, orders)
    }
    /// Create index staged ordered with maintenance after loading its
    /// validation dependencies.
    ///
    /// # Errors
    /// Returns a read, validation, or memory-admission error.
    pub async fn create_index_staged_ordered_with_maintenance(
        &mut self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        maintenance: IndexMaintenance,
    ) -> Result<IndexId> {
        self.definition(table).await?;
        if let Some(value) = self.inner.table_by_id(table) {
            self.index_namespace(value.schema_id).await?;
        }
        self.inner
            .create_index_staged_ordered_with_maintenance(table, def, orders, maintenance)
    }
    /// Advances a staged build by one batch of writer-supplied entries,
    /// persisting a row-id cursor. With `is_final`, flips the index ready
    /// in this same commit and advances the catalog schema version; a
    /// non-final step preserves it. Returns the resulting state.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the index does not exist,
    /// [`crate::Error::Constraint`] if it is not building, an entry's value
    /// count is wrong, a value exceeds the size cap, or the batch
    /// introduces a duplicate of a unique value (the build fails; its
    /// driver drops it).
    pub async fn build_index_step(
        &mut self,
        index: IndexId,
        batch: &[IndexEntry],
        is_final: bool,
    ) -> Result<IndexState> {
        self.index(index).await?;
        self.inner.build_index_step(index, batch, is_final)
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
    /// Returns [`crate::Error::NotFound`] if the table does not exist, or if
    /// any entry in `file.column_stats` names a column that is not live on
    /// the table, or if an `index_entries` entry names an index not live on
    /// the table.
    /// Returns [`crate::Error::Constraint`] if the table has live indexes and a
    /// non-empty file supplies no `index_entries`, an entry's `ordinal` is
    /// outside the file's rows, a supplied indexed value exceeds the size
    /// cap, the entries duplicate a unique value, or the file's partition
    /// values do not match the live spec's keys one for one.
    /// Returns [`crate::Error::Corruption`] if the table has no statistics
    /// record.
    pub async fn register_data_file(
        &mut self,
        table: TableId,
        file: DataFile,
        index_entries: &[FileIndexEntry],
    ) -> Result<DataFileId> {
        self.definition(table).await?;
        self.inner.register_data_file(table, file, index_entries)
    }
    /// Expires a data file, removing it and subtracting its contribution
    /// from the table's statistics.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the file is not live on the table.
    pub async fn expire_data_file(&mut self, table: TableId, file: DataFileId) -> Result<()> {
        self.definition(table).await?;
        self.point(EntityKey::File {
            table_id: table.get(),
            data_file_id: file.get(),
        })
        .await?;
        self.range(Kind::DeleteFile, Some(table.get())).await?;
        self.inner.expire_data_file(table, file)
    }
    /// Registers a delete file targeting a live data file's rows, removing
    /// the equality-index entries of the rows it kills. Each removal names
    /// the killed row by id — inside a dense target's row-id range, or a
    /// per-row-id target's embedded id — with the values the dead row was
    /// indexed under. Table statistics are unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table does not exist, if
    /// `file.data_file_id` is not live on the table, or if an
    /// `index_entries` entry names an index not live on the table.
    /// Returns [`crate::Error::Constraint`] if the table has live indexes and a
    /// non-empty delete file supplies no `index_entries`, an empty delete
    /// file supplies any, an entry's row id is outside a dense target's
    /// row-id range, or an entry's value count does not match its index's
    /// column count.
    pub async fn register_delete_file(
        &mut self,
        table: TableId,
        file: DeleteFile,
        index_entries: &[FileIndexRemoval],
    ) -> Result<DeleteFileId> {
        self.definition(table).await?;
        self.point(EntityKey::File {
            table_id: table.get(),
            data_file_id: file.data_file_id.get(),
        })
        .await?;
        self.range(Kind::DeleteFile, Some(table.get())).await?;
        self.inner.register_delete_file(table, file, index_entries)
    }
    /// Expires a delete file, removing it.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the file is not live on the table.
    pub async fn expire_delete_file(&mut self, table: TableId, file: DeleteFileId) -> Result<()> {
        self.definition(table).await?;
        self.point(EntityKey::DeleteFile {
            table_id: table.get(),
            delete_file_id: file.get(),
        })
        .await?;
        self.inner.expire_delete_file(table, file)
    }
    /// Overrides a table's row-count and size statistics; `next_row_id` is
    /// preserved.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table does not exist.
    /// Returns [`crate::Error::Corruption`] if the table has no statistics
    /// record.
    pub async fn update_table_stats(
        &mut self,
        table: TableId,
        record_count: u64,
        file_size_bytes: u64,
    ) -> Result<()> {
        self.definition(table).await?;
        self.inner
            .update_table_stats(table, record_count, file_size_bytes)
    }
    /// Overrides a column's table-level statistics, verbatim.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the table or column does not
    /// exist.
    pub async fn update_column_stats(
        &mut self,
        table: TableId,
        column: ColumnId,
        stats: ColumnStats,
    ) -> Result<()> {
        self.definition(table).await?;
        self.range(Kind::TableColumnStats, Some(table.get()))
            .await?;
        self.inner.update_column_stats(table, column, stats)
    }
    /// Creates a view.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the schema does not exist.
    /// Returns [`crate::Error::AlreadyExists`] if a relation with that name
    /// already exists in the schema.
    pub async fn create_view(
        &mut self,
        schema: SchemaId,
        name: &str,
        dialect: &str,
        sql: &str,
    ) -> Result<ViewId> {
        self.relation(schema, name).await?;
        self.inner.create_view(schema, name, dialect, sql)
    }
    /// Replaces a view's definition as a new version.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the view does not exist.
    pub async fn alter_view(&mut self, view: ViewId, dialect: &str, sql: &str) -> Result<()> {
        self.point(EntityKey::View {
            view_id: view.get(),
        })
        .await?;
        self.inner.alter_view(view, dialect, sql)
    }
    /// Drops a view.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the view does not exist.
    pub async fn drop_view(&mut self, view: ViewId) -> Result<()> {
        self.point(EntityKey::View {
            view_id: view.get(),
        })
        .await?;
        self.inner.drop_view(view)
    }
    /// Creates a macro with its implementations. Names are namespaced per
    /// `macro_type`; every implementation must carry the same one.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the schema does not exist.
    /// Returns [`crate::Error::Constraint`] if `implementations` is empty or
    /// mixes `macro_type`s.
    /// Returns [`crate::Error::AlreadyExists`] if a live macro with that name
    /// and type already exists in the schema.
    pub async fn create_macro(
        &mut self,
        schema: SchemaId,
        name: &str,
        implementations: &[MacroImplementationDef],
    ) -> Result<MacroId> {
        self.point(EntityKey::Schema {
            schema_id: schema.get(),
        })
        .await?;
        self.namespace(Kind::Macro, schema.get(), Some(name))
            .await?;
        self.inner.create_macro(schema, name, implementations)
    }
    /// Drops a macro, ending its live version.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the macro does not exist.
    pub async fn drop_macro(&mut self, macro_id: MacroId) -> Result<()> {
        self.point(EntityKey::Macro {
            macro_id: macro_id.get(),
        })
        .await?;
        self.inner.drop_macro(macro_id)
    }
    /// Sets an option in a scope. Last-write-wins; an options-only
    /// commit mints no snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the scope's schema or table does
    /// not exist, or [`crate::Error::Constraint`] for a reserved global key
    /// (`encrypted`, `version`).
    pub async fn set_option(&mut self, scope: OptionScope, key: &str, value: &str) -> Result<()> {
        self.options(scope).await?;
        self.inner.set_option(scope, key, value)
    }
    /// Removes an option from a scope; absent keys are a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the scope's schema or table does
    /// not exist, or [`crate::Error::Constraint`] for the reserved global
    /// `encrypted` key.
    pub async fn unset_option(&mut self, scope: OptionScope, key: &str) -> Result<()> {
        self.options(scope).await?;
        self.inner.unset_option(scope, key)
    }
    /// Sets a tag on a schema, table, or view. An existing value for the
    /// key ends into the object's tag history and the new value begins at
    /// this commit's snapshot. Column tags are not reachable here.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the target does not exist, or
    /// [`crate::Error::Constraint`] if the key is empty.
    pub async fn set_tag(&mut self, target: TagTarget, key: &str, value: &str) -> Result<()> {
        self.point(EntityKey::Tag {
            object_id: target.object_id(),
        })
        .await?;
        self.point(match target {
            TagTarget::Schema(id) => EntityKey::Schema {
                schema_id: id.get(),
            },
            TagTarget::Table(id) => EntityKey::Table { table_id: id.get() },
            TagTarget::View(id) => EntityKey::View { view_id: id.get() },
        })
        .await?;
        self.inner.set_tag(target, key, value)
    }
    /// Removes a tag from a schema, table, or view: its live entry ends at
    /// this commit's snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotFound`] if the target does not exist, or if
    /// it carries no live entry for the key.
    pub async fn remove_tag(&mut self, target: TagTarget, key: &str) -> Result<()> {
        self.point(EntityKey::Tag {
            object_id: target.object_id(),
        })
        .await?;
        self.point(match target {
            TagTarget::Schema(id) => EntityKey::Schema {
                schema_id: id.get(),
            },
            TagTarget::Table(id) => EntityKey::Table { table_id: id.get() },
            TagTarget::View(id) => EntityKey::View { view_id: id.get() },
        })
        .await?;
        self.inner.remove_tag(target, key)
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
    /// Returns [`crate::Error::NotFound`] if the table is not live, or if an
    /// `index_entries` entry names an index not live on it.
    /// Returns [`crate::Error::Constraint`] if the chunk carries no rows, if
    /// the table has live indexes and no entries are supplied, if an
    /// entry's ordinal is outside the chunk, or if a supplied value exceeds
    /// the indexed-value size cap.
    /// Returns [`crate::Error::Corruption`] if the table has no statistics
    /// record.
    pub async fn inline_insert(
        &mut self,
        table: TableId,
        chunk: &InlineChunk,
        index_entries: &[FileIndexEntry],
    ) -> Result<u64> {
        self.definition(table).await?;
        self.inner.inline_insert(table, chunk, index_entries)
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
    /// Returns [`crate::Error::NotFound`] if the table is not live, or if an
    /// `index_entries` entry names an index not live on it.
    /// Returns [`crate::Error::Constraint`] if the table has live indexes and
    /// no entries are supplied, or if an entry names a row other than
    /// `row_id`.
    pub async fn inline_delete(
        &mut self,
        table: TableId,
        row_id: u64,
        index_entries: &[FileIndexRemoval],
    ) -> Result<()> {
        self.definition(table).await?;
        self.inner.inline_delete(table, row_id, index_entries)
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
    /// Returns [`crate::Error::NotFound`] if the table is not live or a file's
    /// column statistics name a column that is not.
    /// Returns [`crate::Error::Constraint`] if a file's backdated snapshot is
    /// not below this commit's, or if a file's row-id range runs past the
    /// ids the table had allocated when this commit began.
    pub async fn flush_inlined_data(
        &mut self,
        table: TableId,
        schema_version: u64,
        flushed: &[FlushedDataFile],
    ) -> Result<Vec<DataFileId>> {
        self.definition(table).await?;
        self.inner
            .flush_inlined_data(table, schema_version, flushed)
    }
}
