//! Incremental typed metadata enumeration.

use super::{
    CatalogSnapshot, ColumnInfo, DataFileInfo, DeleteFileInfo, Error, IndexInfo, Kind, MacroInfo,
    MappingInfo, OwnedEntityCursor, ReadScope, Result, SchemaId, SchemaInfo, Selection, TableId,
    TableInfo, Versions, ViewInfo, retain,
};

type Convert<T> = Box<dyn Fn(&CatalogSnapshot) -> Option<T> + Send + Sync>;

/// A metadata cursor owning its fixed store revision.
/// Dropping it releases its checkpoint lease and buffered store page.
pub struct CatalogCursor<T> {
    cursor: OwnedEntityCursor,
    selection: Selection,
    snapshot: crate::store::proto::SnapshotValue,
    convert: Convert<T>,
}

impl<T> CatalogCursor<T> {
    /// Reads the next matching item, decoding at most one record at a time.
    ///
    /// # Errors
    /// Returns a store, decoding, or memory-admission error.
    pub async fn next(&mut self) -> Result<Option<T>> {
        while let Some((record, _memory)) = self.cursor.next().await? {
            if !self.selection.matches(&record)
                || record.lifecycle().is_some_and(|(begin, end)| {
                    begin > self.snapshot.snapshot_id
                        || end.is_some_and(|end| end <= self.snapshot.snapshot_id)
                })
            {
                continue;
            }
            let mut view = CatalogSnapshot {
                snapshot: self.snapshot.clone(),
                ..CatalogSnapshot::default()
            };
            retain(&mut view, record)?;
            if let Some(item) = (self.convert)(&view) {
                return Ok(Some(item));
            }
        }
        Ok(None)
    }
}

impl ReadScope {
    async fn cursor<T>(
        &self,
        selection: Selection,
        convert: impl Fn(&CatalogSnapshot) -> Option<T> + Send + Sync + 'static,
    ) -> Result<CatalogCursor<T>> {
        let (kind, table) = match &selection {
            Selection::Range(kind, table) => (*kind, *table),
            Selection::Namespace(kind, ..) => (*kind, None),
            Selection::Point(_) => {
                return Err(Error::Constraint("enumeration needs a range".into()));
            }
        };
        let cursor = OwnedEntityCursor::open(
            self.reads().owned_read_handle(),
            kind,
            if self.historical {
                Versions::LiveAndEnded
            } else {
                Versions::Live
            },
            table,
        )
        .await?;
        Ok(CatalogCursor {
            cursor,
            selection,
            snapshot: self.header.clone(),
            convert: Box::new(convert),
        })
    }
}

impl ReadScope {
    /// Streams schemas without collecting their metadata.
    ///
    /// # Errors
    /// Returns a store or memory-admission error when opening the cursor.
    pub async fn scan_schemas(&self) -> Result<CatalogCursor<SchemaInfo>> {
        self.cursor(Selection::Range(Kind::Schema, None), move |view| {
            view.schemas().into_iter().next()
        })
        .await
    }
    /// Streams tables in a schema without collecting their metadata.
    ///
    /// # Errors
    /// Returns a store or memory-admission error when opening the cursor.
    pub async fn scan_tables_in(&self, schema: SchemaId) -> Result<CatalogCursor<TableInfo>> {
        self.cursor(
            Selection::Namespace(Kind::Table, schema.get(), None),
            move |view| view.tables_in(schema).into_iter().next(),
        )
        .await
    }
    /// Streams views in a schema without collecting their metadata.
    ///
    /// # Errors
    /// Returns a store or memory-admission error when opening the cursor.
    pub async fn scan_views_in(&self, schema: SchemaId) -> Result<CatalogCursor<ViewInfo>> {
        self.cursor(
            Selection::Namespace(Kind::View, schema.get(), None),
            move |view| view.views_in(schema).into_iter().next(),
        )
        .await
    }
    /// Streams macros in a schema without collecting their metadata.
    ///
    /// # Errors
    /// Returns a store or memory-admission error when opening the cursor.
    pub async fn scan_macros_in(&self, schema: SchemaId) -> Result<CatalogCursor<MacroInfo>> {
        self.cursor(
            Selection::Namespace(Kind::Macro, schema.get(), None),
            move |view| view.macros_in(schema).into_iter().next(),
        )
        .await
    }
    /// Streams columns of a table without collecting their metadata.
    ///
    /// # Errors
    /// Returns a store or memory-admission error when opening the cursor.
    pub async fn scan_columns_of(&self, table: TableId) -> Result<CatalogCursor<ColumnInfo>> {
        self.cursor(
            Selection::Range(Kind::Column, Some(table.get())),
            move |view| view.columns_of(table).into_iter().next(),
        )
        .await
    }
    /// Streams data files of a table without collecting their metadata.
    ///
    /// # Errors
    /// Returns a store or memory-admission error when opening the cursor.
    pub async fn scan_data_files_of(&self, table: TableId) -> Result<CatalogCursor<DataFileInfo>> {
        self.cursor(
            Selection::Range(Kind::File, Some(table.get())),
            move |view| view.data_files_of(table).into_iter().next(),
        )
        .await
    }
    /// Streams delete files of a table without collecting their metadata.
    ///
    /// # Errors
    /// Returns a store or memory-admission error when opening the cursor.
    pub async fn scan_delete_files_of(
        &self,
        table: TableId,
    ) -> Result<CatalogCursor<DeleteFileInfo>> {
        self.cursor(
            Selection::Range(Kind::DeleteFile, Some(table.get())),
            move |view| view.delete_files_of(table).into_iter().next(),
        )
        .await
    }
    /// Streams indexes of a table without collecting their metadata.
    ///
    /// # Errors
    /// Returns a store or memory-admission error when opening the cursor.
    pub async fn scan_indexes_of(&self, table: TableId) -> Result<CatalogCursor<IndexInfo>> {
        self.cursor(
            Selection::Range(Kind::Index, Some(table.get())),
            move |view| view.indexes_of(table).into_iter().next(),
        )
        .await
    }
    /// Streams mappings of a table without collecting their metadata.
    ///
    /// # Errors
    /// Returns a store or memory-admission error when opening the cursor.
    pub async fn scan_mappings_of(&self, table: TableId) -> Result<CatalogCursor<MappingInfo>> {
        self.cursor(
            Selection::Range(Kind::Mapping, Some(table.get())),
            move |view| view.mappings_of(table).into_iter().next(),
        )
        .await
    }
}
