//! Single-key writes staged outside the commit protocol, addressed by
//! decoded key.

use slatedb::DbTransaction;

use crate::{
    error::Result,
    store::key::{EntityKey, Key},
};

/// Stages the deletion of `entity`'s live record on `tx`.
pub(crate) fn delete_current(tx: &DbTransaction, entity: EntityKey) -> Result<()> {
    Ok(tx.delete(Key::current(entity).encode())?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::store::{handle::ReadHandle, open::StoreBuilder};

    #[tokio::test]
    async fn delete_current_removes_exactly_the_named_record() {
        let (db, _) = StoreBuilder::new("write/delete", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let kept = EntityKey::Schema { schema_id: 1 };
        let removed = EntityKey::Schema { schema_id: 2 };

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(Key::current(kept).encode(), b"kept").unwrap();
        tx.put(Key::current(removed).encode(), b"removed").unwrap();
        tx.commit().await.unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        delete_current(&tx, removed).unwrap();
        tx.commit().await.unwrap();

        let handle = ReadHandle::Writer(&db);
        assert!(
            handle
                .get(Key::current(kept).encode())
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            handle
                .get(Key::current(removed).encode())
                .await
                .unwrap()
                .is_none()
        );
    }
}
