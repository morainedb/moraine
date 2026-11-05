//! Fixed, demand-loaded catalog metadata reads.

mod cursor;
mod rows;
pub use cursor::CatalogCursor;

use crate::{
    Error, Result,
    catalog::{
        CatalogSnapshot, ColumnId, ColumnInfo, ColumnStats, DataFileId, DataFileInfo,
        DeleteFileInfo, IndexId, IndexInfo, IndexReadScope, MacroId, MacroInfo, MappingInfo,
        OptionScope, PartitionSpec, ReadOnlyCatalog, ScheduledDeletion, SchemaId, SchemaInfo,
        SnapshotId, SnapshotInfo, SortSpec, TableId, TableInfo, TableStats, TagEntry, ViewId,
        ViewInfo,
    },
    store::{
        key::EntityKey,
        read::{EntityRecordKind as Kind, OwnedEntityCursor, Versions},
    },
    transaction::{commit, demand::Selection, table_read::retain},
};

/// Catalog metadata at one fixed store revision and optional historical
/// snapshot. Each lookup reads only its requested records; the scope retains no
/// decoded catalog.
#[derive(Clone, Debug)]
pub struct ReadScope {
    scope: IndexReadScope,
    header: crate::store::proto::SnapshotValue,
    historical: bool,
}

impl ReadOnlyCatalog {
    /// Opens a fixed read scope at the latest committed snapshot.
    ///
    /// # Errors
    /// Returns a store or checkpoint-acquisition error.
    pub async fn read_scope(&self) -> Result<ReadScope> {
        self.open_read_scope(None).await
    }

    /// Opens a fixed scope for a retained historical snapshot.
    /// Unversioned options and statistics reflect the pinned store revision.
    ///
    /// # Errors
    /// Returns an error for a future or expired snapshot, or a failed read.
    pub async fn read_scope_at(&self, snapshot: SnapshotId) -> Result<ReadScope> {
        self.open_read_scope(Some(snapshot.get())).await
    }

    async fn open_read_scope(&self, at: Option<u64>) -> Result<ReadScope> {
        let scope = self.index_read_scope().await?;
        let handle = scope.reads().owned_read_handle();
        let (_, header) = commit::resolve_read_snapshot(handle.borrow(), at).await?;
        Ok(ReadScope {
            scope,
            header,
            historical: at.is_some(),
        })
    }
}

impl ReadScope {
    /// The fixed read surface for index, row, and file operations.
    #[must_use]
    pub fn reads(&self) -> &ReadOnlyCatalog {
        self.scope.reads()
    }

    async fn projection(&self, selections: &[Selection]) -> Result<CatalogSnapshot> {
        let mut view = CatalogSnapshot {
            snapshot: self.header.clone(),
            ..CatalogSnapshot::default()
        };
        let handle = self.reads().owned_read_handle();
        for selection in selections {
            if !self.historical {
                selection.load(handle.borrow(), &mut view).await?;
                continue;
            }
            let versions = Versions::LiveAndEnded;
            let mut cursor = match selection {
                Selection::Point(entity) => {
                    OwnedEntityCursor::open_entity(
                        handle.clone(),
                        *entity,
                        if entity.is_versioned() {
                            versions
                        } else {
                            Versions::Live
                        },
                    )
                    .await?
                }
                Selection::Range(kind, table) => {
                    OwnedEntityCursor::open(handle.clone(), *kind, versions, *table).await?
                }
                Selection::Namespace(kind, ..) => {
                    OwnedEntityCursor::open(handle.clone(), *kind, versions, None).await?
                }
            };
            while let Some((record, _memory)) = cursor.next().await? {
                if selection.matches(&record)
                    && record.lifecycle().is_none_or(|(begin, end)| {
                        begin <= self.header.snapshot_id
                            && end.is_none_or(|end| end > self.header.snapshot_id)
                    })
                {
                    retain(&mut view, record)?;
                }
            }
        }
        Ok(view)
    }
    /// The snapshot metadata fixed when this scope opened.
    #[must_use]
    pub fn current_snapshot(&self) -> SnapshotInfo {
        SnapshotInfo {
            id: SnapshotId::new(self.header.snapshot_id),
            time: crate::Timestamp::from_micros(self.header.snapshot_time_micros),
            schema_version: self.header.schema_version,
        }
    }
    /// Reads schemas at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn schemas(&self) -> Result<Vec<SchemaInfo>> {
        let view = self
            .projection(&[Selection::Range(Kind::Schema, None)])
            .await?;
        Ok(view.schemas())
    }
    /// Reads schema by name at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn schema_by_name(&self, name: &str) -> Result<Option<SchemaInfo>> {
        let view = self
            .projection(&[Selection::Namespace(Kind::Schema, 0, Some(name.into()))])
            .await?;
        Ok(view.schema_by_name(name))
    }
    /// Reads schema by id at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn schema_by_id(&self, id: SchemaId) -> Result<Option<SchemaInfo>> {
        let view = self
            .projection(&[Selection::Point(EntityKey::Schema {
                schema_id: id.get(),
            })])
            .await?;
        Ok(view.schema_by_id(id))
    }
    /// Reads tables in a schema at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn tables_in(&self, schema: SchemaId) -> Result<Vec<TableInfo>> {
        let view = self
            .projection(&[Selection::Namespace(Kind::Table, schema.get(), None)])
            .await?;
        Ok(view.tables_in(schema))
    }
    /// Reads table by name at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn table_by_name(&self, schema: SchemaId, name: &str) -> Result<Option<TableInfo>> {
        let view = self
            .projection(&[Selection::Namespace(
                Kind::Table,
                schema.get(),
                Some(name.into()),
            )])
            .await?;
        Ok(view.table_by_name(schema, name))
    }
    /// Reads table by id at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn table_by_id(&self, id: TableId) -> Result<Option<TableInfo>> {
        let view = self
            .projection(&[Selection::Point(EntityKey::Table { table_id: id.get() })])
            .await?;
        Ok(view.table_by_id(id))
    }
    /// Reads columns of a table at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn columns_of(&self, table: TableId) -> Result<Vec<ColumnInfo>> {
        let view = self
            .projection(&[Selection::Range(Kind::Column, Some(table.get()))])
            .await?;
        Ok(view.columns_of(table))
    }
    /// Reads views in a schema at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn views_in(&self, schema: SchemaId) -> Result<Vec<ViewInfo>> {
        let view = self
            .projection(&[Selection::Namespace(Kind::View, schema.get(), None)])
            .await?;
        Ok(view.views_in(schema))
    }
    /// Reads view by name at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn view_by_name(&self, schema: SchemaId, name: &str) -> Result<Option<ViewInfo>> {
        let view = self
            .projection(&[Selection::Namespace(
                Kind::View,
                schema.get(),
                Some(name.into()),
            )])
            .await?;
        Ok(view.view_by_name(schema, name))
    }
    /// Reads view by id at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn view_by_id(&self, id: ViewId) -> Result<Option<ViewInfo>> {
        let view = self
            .projection(&[Selection::Point(EntityKey::View { view_id: id.get() })])
            .await?;
        Ok(view.view_by_id(id))
    }
    /// Reads macros in a schema at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn macros_in(&self, schema: SchemaId) -> Result<Vec<MacroInfo>> {
        let view = self
            .projection(&[Selection::Namespace(Kind::Macro, schema.get(), None)])
            .await?;
        Ok(view.macros_in(schema))
    }
    /// Reads macro by name at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn macro_by_name(&self, schema: SchemaId, name: &str) -> Result<Option<MacroInfo>> {
        let view = self
            .projection(&[Selection::Namespace(
                Kind::Macro,
                schema.get(),
                Some(name.into()),
            )])
            .await?;
        Ok(view.macro_by_name(schema, name))
    }
    /// Reads macro by id at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn macro_by_id(&self, id: MacroId) -> Result<Option<MacroInfo>> {
        let view = self
            .projection(&[Selection::Point(EntityKey::Macro { macro_id: id.get() })])
            .await?;
        Ok(view.macro_by_id(id))
    }
    /// Reads mappings of a table at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn mappings_of(&self, table: TableId) -> Result<Vec<MappingInfo>> {
        let view = self
            .projection(&[Selection::Range(Kind::Mapping, Some(table.get()))])
            .await?;
        Ok(view.mappings_of(table))
    }
    /// Reads partitioning of a table at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn partitioning_of(&self, table: TableId) -> Result<Option<PartitionSpec>> {
        let view = self
            .projection(&[Selection::Range(Kind::Partition, Some(table.get()))])
            .await?;
        Ok(view.partitioning_of(table))
    }
    /// Reads sorting of a table at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn sorting_of(&self, table: TableId) -> Result<Option<SortSpec>> {
        let view = self
            .projection(&[Selection::Range(Kind::Sort, Some(table.get()))])
            .await?;
        Ok(view.sorting_of(table))
    }
    /// Reads indexes of a table at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn indexes_of(&self, table: TableId) -> Result<Vec<IndexInfo>> {
        let view = self
            .projection(&[Selection::Range(Kind::Index, Some(table.get()))])
            .await?;
        Ok(view.indexes_of(table))
    }
    /// Reads index by name at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn index_by_name(&self, table: TableId, name: &str) -> Result<Option<IndexInfo>> {
        let view = self
            .projection(&[Selection::Range(Kind::Index, Some(table.get()))])
            .await?;
        Ok(view.index_by_name(table, name))
    }
    /// Reads index by schema name at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn index_by_schema_name(
        &self,
        schema: SchemaId,
        name: &str,
    ) -> Result<Option<IndexInfo>> {
        let tables = self.tables_in(schema).await?;
        let mut selections = vec![Selection::Namespace(Kind::Table, schema.get(), None)];
        selections.extend(
            tables
                .iter()
                .map(|table| Selection::Range(Kind::Index, Some(table.id.get()))),
        );
        let view = self.projection(&selections).await?;
        view.index_by_schema_name(schema, name)
    }
    /// Reads index on columns at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn index_on_columns(
        &self,
        table: TableId,
        columns: &[ColumnId],
    ) -> Result<Option<IndexInfo>> {
        let view = self
            .projection(&[Selection::Range(Kind::Index, Some(table.get()))])
            .await?;
        Ok(view.index_on_columns(table, columns))
    }
    /// Reads index by id at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn index_by_id(&self, table: TableId, id: IndexId) -> Result<Option<IndexInfo>> {
        let view = self
            .projection(&[Selection::Range(Kind::Index, Some(table.get()))])
            .await?;
        Ok(view.index_by_id(table, id))
    }
    /// Reads option at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn option(&self, scope: OptionScope, key: &str) -> Result<Option<String>> {
        let mut selections = vec![Selection::Point(EntityKey::Option {
            scope_kind: 0,
            scope_id: 0,
        })];
        let (scope_kind, scope_id) = scope.key_components();
        selections.push(Selection::Point(EntityKey::Option {
            scope_kind,
            scope_id,
        }));
        if let OptionScope::Table(table) = scope {
            selections.push(Selection::Point(EntityKey::Table {
                table_id: table.get(),
            }));
            if let Some(table) = self.table_by_id(table).await? {
                selections.push(Selection::Point(EntityKey::Option {
                    scope_kind: 1,
                    scope_id: table.schema_id.get(),
                }));
            }
        }
        Ok(self.projection(&selections).await?.option(scope, key))
    }
    /// Reads data path at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn data_path(&self) -> Result<Option<String>> {
        let view = self
            .projection(&[Selection::Point(EntityKey::Option {
                scope_kind: OptionScope::Global.key_components().0,
                scope_id: OptionScope::Global.key_components().1,
            })])
            .await?;
        Ok(view.data_path())
    }
    /// Reads tags for a target at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn tags_of(&self, object_id: u64) -> Result<Vec<TagEntry>> {
        let view = self
            .projection(&[Selection::Point(EntityKey::Tag { object_id })])
            .await?;
        Ok(view.tags_of(object_id))
    }
    /// Reads scheduled deletions at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn scheduled_deletions(&self) -> Result<Vec<ScheduledDeletion>> {
        let view = self
            .projection(&[Selection::Range(Kind::GcFile, None)])
            .await?;
        Ok(view.scheduled_deletions())
    }
    /// Reads data files of a table at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn data_files_of(&self, table: TableId) -> Result<Vec<DataFileInfo>> {
        let view = self
            .projection(&[Selection::Range(Kind::File, Some(table.get()))])
            .await?;
        Ok(view.data_files_of(table))
    }
    /// Reads delete files of a table at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn delete_files_of(&self, table: TableId) -> Result<Vec<DeleteFileInfo>> {
        let view = self
            .projection(&[Selection::Range(Kind::DeleteFile, Some(table.get()))])
            .await?;
        Ok(view.delete_files_of(table))
    }
    /// Reads table stats at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn table_stats(&self, table: TableId) -> Result<Option<TableStats>> {
        let view = self
            .projection(&[Selection::Point(EntityKey::TableStats {
                table_id: table.get(),
            })])
            .await?;
        Ok(view.table_stats(table))
    }
    /// Reads column stats at this scope's fixed revision.
    ///
    /// # Errors
    /// Returns a read or memory-admission error.
    pub async fn column_stats(
        &self,
        table: TableId,
        column: ColumnId,
    ) -> Result<Option<ColumnStats>> {
        let view = self
            .projection(&[Selection::Point(EntityKey::TableColumnStats {
                table_id: table.get(),
                column_id: column.get(),
            })])
            .await?;
        Ok(view.column_stats(table, column))
    }
}
