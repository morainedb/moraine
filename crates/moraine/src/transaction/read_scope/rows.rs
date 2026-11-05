//! Row operations resolve only metadata belonging to their requested sources.

use super::{
    CatalogSnapshot, DataFileId, EntityKey, Error, Kind, ReadOnlyCatalog, ReadScope, Result,
    Selection, TableId, retain,
};
use crate::{DataStore, ExcludedPositions, LocatedDeletion, LocatedRowScan};

impl ReadScope {
    async fn sources(
        &self,
        table: TableId,
        files: impl Iterator<Item = DataFileId>,
    ) -> Result<CatalogSnapshot> {
        let mut selections = vec![
            Selection::Point(EntityKey::Table {
                table_id: table.get(),
            }),
            Selection::Range(Kind::Column, Some(table.get())),
        ];
        selections.extend(files.map(|file| {
            Selection::Point(EntityKey::File {
                table_id: table.get(),
                data_file_id: file.get(),
            })
        }));
        selections.push(Selection::Range(Kind::DeleteFile, Some(table.get())));
        let mut view = self.projection(&selections).await?;
        let schema_id = view
            .table_by_id(table)
            .ok_or_else(|| Error::NotFound(format!("table {table}")))?
            .schema_id
            .get();
        let schema = self
            .projection(&[Selection::Point(EntityKey::Schema { schema_id })])
            .await?;
        for record in schema.live_records(Kind::Schema) {
            retain(&mut view, record)?;
        }
        Ok(view)
    }
}

impl ReadOnlyCatalog {
    /// Opens a row scan using the supplied scope's fixed revision.
    ///
    /// # Errors
    /// Returns an error for missing metadata, invalid columns, or unreadable
    /// data.
    pub async fn scan_rows_at(
        &self,
        scope: &ReadScope,
        data_store: Option<DataStore>,
        data_prefix: &str,
        table: TableId,
        pairs: &[(u64, Option<DataFileId>)],
        columns: &[String],
    ) -> Result<LocatedRowScan> {
        self.scan_rows_at_excluding(scope, data_store, data_prefix, table, pairs, columns, &[])
            .await
    }

    /// Opens a scoped row scan excluding staged deletions at file positions.
    ///
    /// # Errors
    /// Returns an error for missing metadata, invalid columns, or unreadable
    /// data.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_rows_at_excluding(
        &self,
        scope: &ReadScope,
        data_store: Option<DataStore>,
        data_prefix: &str,
        table: TableId,
        pairs: &[(u64, Option<DataFileId>)],
        columns: &[String],
        excluded: &[ExcludedPositions],
    ) -> Result<LocatedRowScan> {
        let view = scope
            .sources(table, pairs.iter().filter_map(|(_, file)| *file))
            .await?;
        scope
            .reads()
            .scan_rows_at_excluding_projected(
                &view,
                data_store,
                data_prefix,
                table,
                pairs,
                columns,
                excluded,
            )
            .await
    }

    /// Resolves row IDs to positions using the supplied scope's fixed revision.
    ///
    /// # Errors
    /// Returns an error for missing metadata or a row that cannot be
    /// positioned.
    pub async fn locate_row_positions_at(
        &self,
        scope: &ReadScope,
        data_store: &DataStore,
        data_prefix: &str,
        table: TableId,
        pairs: &[(u64, DataFileId)],
    ) -> Result<Vec<LocatedDeletion>> {
        let view = scope
            .sources(table, pairs.iter().map(|(_, file)| *file))
            .await?;
        scope
            .reads()
            .locate_row_positions_at_projected(&view, data_store, data_prefix, table, pairs)
            .await
    }
}

impl ReadOnlyCatalog {
    /// Selects an index for predicates using one fixed catalog revision.
    ///
    /// # Errors
    /// Returns a read or index-probe error.
    pub async fn index_predicate_rows(
        &self,
        scope: &ReadScope,
        table: TableId,
        predicates: &[crate::IndexPredicate],
    ) -> Result<Option<crate::IndexPredicateRows>> {
        let view = scope
            .projection(&[Selection::Range(Kind::Index, Some(table.get()))])
            .await?;
        scope
            .reads()
            .index_predicate_rows_projected(&view, table, predicates)
            .await
    }
}
