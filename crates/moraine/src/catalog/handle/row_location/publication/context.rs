//! One table's resolved data prefix, retained only within a publication read
//! point.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::{MemoryReservation, Result, TableId, memory::WorkingValue, store::handle::ReadHandle};

#[derive(Default)]
pub(crate) struct TableContext {
    current: tokio::sync::Mutex<Option<(TableId, WorkingValue<String>)>>,
    loaded: AtomicU64,
    reused: AtomicU64,
}

impl TableContext {
    pub(crate) async fn resolve(
        &self,
        handle: ReadHandle<'_>,
        table: TableId,
    ) -> Result<WorkingValue<String>> {
        let mut current = self.current.lock().await;
        if let Some((held, prefix)) = &*current
            && *held == table
        {
            self.reused.fetch_add(1, Ordering::Relaxed);
            return Ok(prefix.clone());
        }
        let prefix = super::ReadOnlyCatalog::publication_table_prefix(handle, table).await?;
        let memory = MemoryReservation::reserve(prefix.capacity() as u64 + 128)?;
        let prefix = WorkingValue::new(prefix, memory);
        *current = Some((table, prefix.clone()));
        self.loaded.fetch_add(1, Ordering::Relaxed);
        Ok(prefix)
    }

    pub(crate) fn clear(&mut self) {
        self.current.get_mut().take();
    }
    pub(crate) fn loads(&self) -> u64 {
        self.loaded.load(Ordering::Relaxed)
    }
    pub(crate) fn reuses(&self) -> u64 {
        self.reused.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{
        key::{EntityKey, Key},
        proto::{SchemaValue, TableValue},
        value::encode_value,
    };

    #[tokio::test]
    async fn concurrent_files_share_context_and_reset_reloads_it() {
        let catalog = crate::Catalog::open(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            crate::CatalogOptions::default(),
        )
        .await
        .unwrap();
        let tx = catalog.begin_write_tx().unwrap();
        tx.put(
            Key::current(EntityKey::Schema { schema_id: 10 }).encode(),
            encode_value(&SchemaValue {
                schema_id: 10,
                path: "schema/".into(),
                ..Default::default()
            }),
        )
        .unwrap();
        tx.put(
            Key::current(EntityKey::Table { table_id: 20 }).encode(),
            encode_value(&TableValue {
                table_id: 20,
                schema_id: 10,
                path: "table/".into(),
                ..Default::default()
            }),
        )
        .unwrap();
        catalog.commit_write_tx(tx).await.unwrap();
        let session = catalog.begin_read().await.unwrap();
        let mut context = TableContext::default();
        let (first, second) = tokio::join!(
            context.resolve(session.handle(), TableId::new(20)),
            context.resolve(session.handle(), TableId::new(20))
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.as_str(), "schema/table/");
        assert_eq!(first.as_ptr(), second.as_ptr());
        assert_eq!((context.loads(), context.reuses()), (1, 1));
        drop(session);
        let tx = catalog.begin_write_tx().unwrap();
        tx.put(
            Key::current(EntityKey::Schema { schema_id: 10 }).encode(),
            encode_value(&SchemaValue {
                schema_id: 10,
                path: "moved/".into(),
                ..Default::default()
            }),
        )
        .unwrap();
        catalog.commit_write_tx(tx).await.unwrap();
        context.clear();
        let session = catalog.begin_read().await.unwrap();
        let refreshed = context
            .resolve(session.handle(), TableId::new(20))
            .await
            .unwrap();
        assert_eq!(refreshed.as_str(), "moved/table/");
        assert_eq!(first.as_str(), "schema/table/");
        assert_eq!(context.loads(), 2);
        drop(session);
        catalog.close().await.unwrap();
    }
}
