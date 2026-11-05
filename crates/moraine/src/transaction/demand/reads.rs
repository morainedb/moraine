//! Reads including staged mutations.

use super::{
    ColumnId, ColumnInfo, ColumnStats, DataFileInfo, DeleteFileInfo, EntityKey, IndexId, IndexInfo,
    Kind, MacroId, MacroInfo, MappingInfo, OptionScope, PartitionSpec, Result, SchemaId,
    SchemaInfo, Selection, SnapshotInfo, SortSpec, TableId, TableInfo, TableStats, TagEntry,
    Transaction, ViewId, ViewInfo,
};

impl Transaction {
    /// The committed snapshot this transaction started from.
    #[must_use]
    pub fn current_snapshot(&self) -> SnapshotInfo {
        self.inner.current_snapshot()
    }
    /// Reads schemas including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn schemas(&mut self) -> Result<Vec<SchemaInfo>> {
        self.ensure(Selection::Range(Kind::Schema, None)).await?;
        Ok(self.inner.schemas())
    }
    /// Reads schema by name including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn schema_by_name(&mut self, name: &str) -> Result<Option<SchemaInfo>> {
        self.ensure(Selection::Namespace(Kind::Schema, 0, Some(name.into())))
            .await?;
        Ok(self.inner.schema_by_name(name))
    }
    /// Reads schema by id including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn schema_by_id(&mut self, id: SchemaId) -> Result<Option<SchemaInfo>> {
        self.ensure(Selection::Point(EntityKey::Schema {
            schema_id: id.get(),
        }))
        .await?;
        Ok(self.inner.schema_by_id(id))
    }
    /// Reads tables in a schema including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn tables_in(&mut self, schema: SchemaId) -> Result<Vec<TableInfo>> {
        self.ensure(Selection::Namespace(Kind::Table, schema.get(), None))
            .await?;
        Ok(self.inner.tables_in(schema))
    }
    /// Reads table by name including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn table_by_name(
        &mut self,
        schema: SchemaId,
        name: &str,
    ) -> Result<Option<TableInfo>> {
        self.ensure(Selection::Namespace(
            Kind::Table,
            schema.get(),
            Some(name.into()),
        ))
        .await?;
        Ok(self.inner.table_by_name(schema, name))
    }
    /// Reads table by id including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn table_by_id(&mut self, id: TableId) -> Result<Option<TableInfo>> {
        self.ensure(Selection::Point(EntityKey::Table { table_id: id.get() }))
            .await?;
        Ok(self.inner.table_by_id(id))
    }
    /// Reads columns of a table including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn columns_of(&mut self, table: TableId) -> Result<Vec<ColumnInfo>> {
        self.ensure(Selection::Range(Kind::Column, Some(table.get())))
            .await?;
        Ok(self.inner.columns_of(table))
    }
    /// Reads views in a schema including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn views_in(&mut self, schema: SchemaId) -> Result<Vec<ViewInfo>> {
        self.ensure(Selection::Namespace(Kind::View, schema.get(), None))
            .await?;
        Ok(self.inner.views_in(schema))
    }
    /// Reads view by name including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn view_by_name(&mut self, schema: SchemaId, name: &str) -> Result<Option<ViewInfo>> {
        self.ensure(Selection::Namespace(
            Kind::View,
            schema.get(),
            Some(name.into()),
        ))
        .await?;
        Ok(self.inner.view_by_name(schema, name))
    }
    /// Reads view by id including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn view_by_id(&mut self, id: ViewId) -> Result<Option<ViewInfo>> {
        self.ensure(Selection::Point(EntityKey::View { view_id: id.get() }))
            .await?;
        Ok(self.inner.view_by_id(id))
    }
    /// Reads macros in a schema including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn macros_in(&mut self, schema: SchemaId) -> Result<Vec<MacroInfo>> {
        self.ensure(Selection::Namespace(Kind::Macro, schema.get(), None))
            .await?;
        Ok(self.inner.macros_in(schema))
    }
    /// Reads macro by name including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn macro_by_name(
        &mut self,
        schema: SchemaId,
        name: &str,
    ) -> Result<Option<MacroInfo>> {
        self.ensure(Selection::Namespace(
            Kind::Macro,
            schema.get(),
            Some(name.into()),
        ))
        .await?;
        Ok(self.inner.macro_by_name(schema, name))
    }
    /// Reads macro by id including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn macro_by_id(&mut self, id: MacroId) -> Result<Option<MacroInfo>> {
        self.ensure(Selection::Point(EntityKey::Macro { macro_id: id.get() }))
            .await?;
        Ok(self.inner.macro_by_id(id))
    }
    /// Reads mappings of a table including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn mappings_of(&mut self, table: TableId) -> Result<Vec<MappingInfo>> {
        self.ensure(Selection::Range(Kind::Mapping, Some(table.get())))
            .await?;
        Ok(self.inner.mappings_of(table))
    }
    /// Reads partitioning of a table including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn partitioning_of(&mut self, table: TableId) -> Result<Option<PartitionSpec>> {
        self.ensure(Selection::Range(Kind::Partition, Some(table.get())))
            .await?;
        Ok(self.inner.partitioning_of(table))
    }
    /// Reads sorting of a table including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn sorting_of(&mut self, table: TableId) -> Result<Option<SortSpec>> {
        self.ensure(Selection::Range(Kind::Sort, Some(table.get())))
            .await?;
        Ok(self.inner.sorting_of(table))
    }
    /// Reads indexes of a table including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn indexes_of(&mut self, table: TableId) -> Result<Vec<IndexInfo>> {
        self.ensure(Selection::Range(Kind::Index, Some(table.get())))
            .await?;
        Ok(self.inner.indexes_of(table))
    }
    /// Reads index by name including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn index_by_name(&mut self, table: TableId, name: &str) -> Result<Option<IndexInfo>> {
        self.ensure(Selection::Range(Kind::Index, Some(table.get())))
            .await?;
        Ok(self.inner.index_by_name(table, name))
    }
    /// Reads index by schema name including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn index_by_schema_name(
        &mut self,
        schema: SchemaId,
        name: &str,
    ) -> Result<Option<IndexInfo>> {
        self.index_namespace(schema).await?;
        self.inner.index_by_schema_name(schema, name)
    }
    /// Reads index on columns including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn index_on_columns(
        &mut self,
        table: TableId,
        columns: &[ColumnId],
    ) -> Result<Option<IndexInfo>> {
        self.ensure(Selection::Range(Kind::Index, Some(table.get())))
            .await?;
        Ok(self.inner.index_on_columns(table, columns))
    }
    /// Reads index by id including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn index_by_id(&mut self, table: TableId, id: IndexId) -> Result<Option<IndexInfo>> {
        self.ensure(Selection::Range(Kind::Index, Some(table.get())))
            .await?;
        Ok(self.inner.index_by_id(table, id))
    }
    /// Reads option including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn option(&mut self, scope: OptionScope, key: &str) -> Result<Option<String>> {
        self.options(scope).await?;
        self.options(OptionScope::Global).await?;
        if let OptionScope::Table(table) = scope
            && let Some(table) = self.inner.table_by_id(table)
        {
            self.options(OptionScope::Schema(table.schema_id)).await?;
        }
        Ok(self.inner.option(scope, key))
    }
    /// Reads data path including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn data_path(&mut self) -> Result<Option<String>> {
        self.ensure(Selection::Point(EntityKey::Option {
            scope_kind: OptionScope::Global.key_components().0,
            scope_id: OptionScope::Global.key_components().1,
        }))
        .await?;
        Ok(self.inner.data_path())
    }
    /// Reads tags for a target including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn tags_of(&mut self, object_id: u64) -> Result<Vec<TagEntry>> {
        self.ensure(Selection::Point(EntityKey::Tag { object_id }))
            .await?;
        Ok(self.inner.tags_of(object_id))
    }
    /// Reads data files of a table including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn data_files_of(&mut self, table: TableId) -> Result<Vec<DataFileInfo>> {
        self.ensure(Selection::Range(Kind::File, Some(table.get())))
            .await?;
        Ok(self.inner.data_files_of(table))
    }
    /// Reads delete files of a table including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn delete_files_of(&mut self, table: TableId) -> Result<Vec<DeleteFileInfo>> {
        self.ensure(Selection::Range(Kind::DeleteFile, Some(table.get())))
            .await?;
        Ok(self.inner.delete_files_of(table))
    }
    /// Reads table stats including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn table_stats(&mut self, table: TableId) -> Result<Option<TableStats>> {
        self.ensure(Selection::Point(EntityKey::TableStats {
            table_id: table.get(),
        }))
        .await?;
        Ok(self.inner.table_stats(table))
    }
    /// Reads column stats including staged changes.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn column_stats(
        &mut self,
        table: TableId,
        column: ColumnId,
    ) -> Result<Option<ColumnStats>> {
        self.ensure(Selection::Point(EntityKey::TableColumnStats {
            table_id: table.get(),
            column_id: column.get(),
        }))
        .await?;
        Ok(self.inner.column_stats(table, column))
    }
}
