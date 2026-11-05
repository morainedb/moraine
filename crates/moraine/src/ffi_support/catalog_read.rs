//! Operation-owned catalog descriptor projections.

mod export;

use crate::{
    CatalogSnapshot, Error, ReadOnlyCatalog, Result,
    store::read::{self, EntityCursor, EntityRecord, EntityRecordKind, Versions},
};

/// The records needed by one descriptor-listing call.
#[derive(Clone, Copy)]
pub enum SnapshotProjection {
    /// All live schemas.
    Schemas,
    /// Live tables belonging to a schema.
    TablesIn(u64),
    /// Live columns belonging to a table.
    ColumnsOf(u64),
    /// Live views belonging to a schema.
    ViewsIn(u64),
}

impl SnapshotProjection {
    fn kind(self) -> EntityRecordKind {
        match self {
            Self::Schemas => EntityRecordKind::Schema,
            Self::TablesIn(_) => EntityRecordKind::Table,
            Self::ColumnsOf(_) => EntityRecordKind::Column,
            Self::ViewsIn(_) => EntityRecordKind::View,
        }
    }

    fn includes(self, record: &EntityRecord) -> bool {
        match (self, record) {
            (Self::TablesIn(schema), EntityRecord::Table(table)) => table.schema_id == schema,
            (Self::ViewsIn(schema), EntityRecord::View(view)) => view.schema_id == schema,
            _ => true,
        }
    }
}

/// Builds only the requested descriptor projection; never installs it as a head
/// view.
pub async fn project_snapshot(
    catalog: &ReadOnlyCatalog,
    projection: SnapshotProjection,
) -> Result<CatalogSnapshot> {
    let session = catalog.begin_read().await?;
    let result = read::consistent(session.handle(), || async {
        crate::transaction::commit::refuse_mid_migration(session.handle()).await?;
        let head = read::read_head(session.handle())
            .await?
            .ok_or_else(|| Error::Corruption("catalog has no head".to_owned()))?;
        let snapshot = read::read_snapshot(session.handle(), head.snapshot_id)
            .await?
            .ok_or_else(|| Error::Corruption("catalog head has no snapshot".to_owned()))?;
        let mut view = CatalogSnapshot {
            snapshot,
            batch_seq: head.batch_seq,
            ..CatalogSnapshot::default()
        };
        let table = match projection {
            SnapshotProjection::ColumnsOf(table) => Some(table),
            _ => None,
        };
        let mut cursor =
            EntityCursor::open(session.handle(), projection.kind(), Versions::Live, table).await?;
        while let Some((record, _memory)) = cursor.next().await? {
            if projection.includes(&record) {
                view.admit(record.estimated_bytes().saturating_add(80))?;
                view.put_record(record);
            }
        }
        Ok(view)
    })
    .await;
    session.finish();
    result
}

/// Reads a global option without materializing catalog entities.
pub async fn global_option(catalog: &ReadOnlyCatalog, name: &str) -> Result<Option<String>> {
    let session = catalog.begin_read().await?;
    let result = read::consistent(session.handle(), || async {
        crate::transaction::commit::refuse_mid_migration(session.handle()).await?;
        read::read_global_options(session.handle())
            .await
            .map(|options| options.and_then(|options| options.options.get(name).cloned()))
    })
    .await;
    session.finish();
    result
}

/// Resolves a named table and loads the descriptors needed by an index call.
/// Creation also includes the schema's index names and the target's file
/// presence.
///
/// # Errors
/// Returns an error when either name is absent or a dependency cannot be read.
pub async fn index_table(
    catalog: &ReadOnlyCatalog,
    schema: &str,
    table: &str,
    creating: bool,
) -> Result<CatalogSnapshot> {
    use crate::transaction::table_read::TableRead;
    let selection = if creating {
        TableRead::IndexCreation
    } else {
        TableRead::Definition
    };
    named_table(catalog, schema, table, selection).await
}

async fn named_table(
    catalog: &ReadOnlyCatalog,
    schema: &str,
    table: &str,
    selection: crate::transaction::table_read::TableRead,
) -> Result<CatalogSnapshot> {
    use crate::{
        store::key::NameKind,
        transaction::{names, table_read::load_table},
    };
    let session = catalog.begin_read().await?;
    let result = read::consistent(session.handle(), || async {
        crate::transaction::commit::refuse_mid_migration(session.handle()).await?;
        let schema_id = names::lookup(session.handle(), NameKind::Schema, 0, schema)
            .await?
            .ok_or_else(|| Error::NotFound(format!("schema {schema}")))?;
        let table_id = names::lookup(session.handle(), NameKind::Table, schema_id, table)
            .await?
            .ok_or_else(|| Error::NotFound(format!("table {schema}.{table}")))?;
        load_table(session.handle(), crate::TableId::new(table_id), selection).await
    })
    .await;
    session.finish();
    result
}

/// Loads one table's descriptors and registered sources for position
/// resolution.
///
/// # Errors
/// Returns an error when the table or its dependencies cannot be read.
pub async fn table_sources(
    catalog: &ReadOnlyCatalog,
    schema: &str,
    table: &str,
) -> Result<CatalogSnapshot> {
    named_table(
        catalog,
        schema,
        table,
        crate::transaction::table_read::TableRead::Sources,
    )
    .await
}

#[cfg(test)]
mod tests;
