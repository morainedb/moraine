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
    /// Definitions plus registered data and delete files.
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
            for kind in [EntityRecordKind::File, EntityRecordKind::DeleteFile] {
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

/// Reads requested files and streams delete records, retaining only their
/// targets.
pub(crate) async fn load_sources_for(
    handle: ReadHandle<'_>,
    view: &mut CatalogSnapshot,
    table: TableId,
    files: &[crate::DataFileId],
) -> Result<()> {
    let _memory = crate::MemoryReservation::reserve((size_of_val(files) as u64).saturating_mul(2))?;
    let mut files = files.to_vec();
    files.sort_unstable();
    files.dedup();
    for file in &files {
        point(
            handle,
            view,
            EntityKey::File {
                table_id: table.get(),
                data_file_id: file.get(),
            },
        )
        .await?;
    }
    if !files.is_empty() {
        let mut cursor = EntityCursor::open(
            handle,
            EntityRecordKind::DeleteFile,
            Versions::Live,
            Some(table.get()),
        )
        .await?;
        while let Some((record, _memory)) = cursor.next().await? {
            if matches!(&record, EntityRecord::DeleteFile(row) if files.binary_search(&crate::DataFileId::new(row.data_file_id)).is_ok())
            {
                retain(view, record)?;
            }
        }
    }
    Ok(())
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
