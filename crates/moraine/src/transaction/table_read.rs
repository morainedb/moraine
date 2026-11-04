//! Operation-owned table descriptors for index work and file reads.

use crate::{
    Catalog, CatalogSnapshot, Error, ReadOnlyCatalog, Result, SnapshotId, TableId, Transaction,
    store::{
        handle::ReadHandle,
        key::{EntityKey, Key},
        read::{self, EntityCursor, EntityRecord, EntityRecordKind, Versions},
    },
    transaction::commit,
};

/// The dependencies an internal table operation may inspect.
#[derive(Clone, Copy)]
pub(crate) enum TableRead {
    /// The table, schema, columns, indexes, and table statistics.
    Definition,
    /// Definitions plus registered files, delete files, and mappings.
    Sources,
    /// Definitions, file presence, and the schema's other table and index
    /// names.
    IndexCreation,
}

fn retain(view: &mut CatalogSnapshot, record: EntityRecord) -> Result<()> {
    view.admit(record.estimated_bytes().saturating_add(80))?;
    view.put_record(record);
    Ok(())
}

async fn point(handle: ReadHandle<'_>, view: &mut CatalogSnapshot, key: EntityKey) -> Result<()> {
    let key = Key::current(key);
    if let Some(value) = handle.get(key.encode()).await? {
        retain(view, read::decode_current_record(key, &value)?)?;
    }
    Ok(())
}

async fn records(
    handle: ReadHandle<'_>,
    view: &mut CatalogSnapshot,
    kind: EntityRecordKind,
    table: Option<u64>,
) -> Result<()> {
    let mut cursor = EntityCursor::open(handle, kind, Versions::Live, table).await?;
    while let Some((record, _memory)) = cursor.next().await? {
        retain(view, record)?;
    }
    Ok(())
}

/// Loads only the dependencies the selected table operation can use.
pub(crate) async fn load_table(
    handle: ReadHandle<'_>,
    table: TableId,
    selection: TableRead,
) -> Result<CatalogSnapshot> {
    read::consistent(handle, || async {
        let mut view = crate::transaction::staged::dependencies::header(handle).await?;
        let table_id = table.get();
        point(handle, &mut view, EntityKey::Table { table_id }).await?;
        let schema_id = view
            .tables
            .get(&table_id)
            .ok_or_else(|| Error::NotFound(format!("table {table}")))?
            .schema_id;
        point(handle, &mut view, EntityKey::Schema { schema_id }).await?;
        point(handle, &mut view, EntityKey::TableStats { table_id }).await?;
        for kind in [EntityRecordKind::Column, EntityRecordKind::Index] {
            records(handle, &mut view, kind, Some(table_id)).await?;
        }
        if matches!(selection, TableRead::Sources) {
            for kind in [
                EntityRecordKind::File,
                EntityRecordKind::DeleteFile,
                EntityRecordKind::Mapping,
            ] {
                records(handle, &mut view, kind, Some(table_id)).await?;
            }
        }
        if matches!(selection, TableRead::IndexCreation) {
            let mut files = EntityCursor::open(
                handle,
                EntityRecordKind::File,
                Versions::Live,
                Some(table_id),
            )
            .await?;
            if let Some((record, _memory)) = files.next().await? {
                retain(&mut view, record)?;
            }
            let mut cursor =
                EntityCursor::open(handle, EntityRecordKind::Table, Versions::Live, None).await?;
            while let Some((record, _memory)) = cursor.next().await? {
                if matches!(&record, EntityRecord::Table(value) if value.schema_id == schema_id) {
                    retain(&mut view, record)?;
                }
            }
            let tables: Vec<_> = view
                .tables
                .keys()
                .copied()
                .filter(|id| *id != table_id)
                .collect();
            for other in tables {
                records(handle, &mut view, EntityRecordKind::Index, Some(other)).await?;
            }
        }
        Ok(view)
    })
    .await
}

impl ReadOnlyCatalog {
    pub(crate) async fn table_snapshot(
        &self,
        table: TableId,
        selection: TableRead,
    ) -> Result<CatalogSnapshot> {
        let session = self.begin_read().await?;
        let result = load_table(session.handle(), table, selection).await;
        session.finish();
        result
    }
}

impl Catalog {
    /// Commits an internal closure restricted to the selected dependencies.
    pub(crate) async fn commit_table<F>(
        &self,
        table: TableId,
        selection: TableRead,
        mutate: F,
    ) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        let member = commit::TableMember {
            table,
            selection,
            mutate,
        };
        let ids = commit::commit_cycle(&self.store(), &member, self.commits()).await?;
        ids.first()
            .copied()
            .ok_or_else(|| Error::Corruption("table commit returned no snapshot".into()))
    }
}
