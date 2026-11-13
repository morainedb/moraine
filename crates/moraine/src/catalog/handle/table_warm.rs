//! Warming one table's probe ranges into the block cache.

use futures::{StreamExt, stream};
use tracing::debug;

use super::ReadOnlyCatalog;
use crate::{
    catalog::TableId,
    error::Result,
    store::{
        handle::{ReadHandle, ScanShape},
        key::{IndexKind, index_index_prefix, table_probe_prefixes},
        read::{EntityCursor, EntityRecord, EntityRecordKind, Versions},
    },
};

/// Ranges one table warm keeps in flight, sized for a remote object store.
const WARM_RANGE_CONCURRENCY: usize = 8;
static WARM_SLOTS: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(WARM_RANGE_CONCURRENCY));

/// Reads the first entry under `prefix` in seek shape, admitting the SST
/// metadata and first block a probe there would fetch.
async fn warm_prefix(handle: ReadHandle<'_>, prefix: Vec<u8>) -> Result<()> {
    let _permit = WARM_SLOTS
        .acquire()
        .await
        .map_err(|error| crate::Error::Interrupted(format!("warming admission: {error}")))?;
    let mut iterator = handle
        .scan_prefix(prefix, .., ScanShape::seek().retain())
        .await?;
    iterator.next().await?;
    Ok(())
}

impl ReadOnlyCatalog {
    /// Warms the store ranges a lookup on each of `tables` probes — its
    /// indexes' entries and its inlined data — into the block cache, so a
    /// cold table pays one burst of reads rather than a round trip per probe.
    /// Attempts every range and returns the first failure so background callers
    /// can retry.
    ///
    /// # Errors
    ///
    /// Returns a store error if an index definition or probe range cannot be
    /// read.
    pub async fn warm_tables(&self, tables: &[TableId]) -> Result<()> {
        let session = self.begin_read().await?;
        let handle = session.handle();
        let (mut ranges, mut failed) = (0, 0);
        let mut outcome = Ok(());
        for table in tables {
            let mut cursor = EntityCursor::open(
                handle,
                EntityRecordKind::Index,
                Versions::Live,
                Some(table.get()),
                crate::read_policy::CacheAdmission::Retain,
            )
            .await?;
            let mut prefixes = table_probe_prefixes(table.get(), []);
            loop {
                let next = cursor.next().await?;
                let ended = next.is_none();
                if let Some(owner) = next
                    && let EntityRecord::Index(index) = owner.value()
                {
                    let kind = if index.unique {
                        IndexKind::Unique
                    } else {
                        IndexKind::Multi
                    };
                    prefixes.push(index_index_prefix(kind, index.index_id));
                }
                if prefixes.len() >= WARM_RANGE_CONCURRENCY || ended {
                    let mut results = stream::iter(std::mem::take(&mut prefixes))
                        .map(|prefix| warm_prefix(handle, prefix))
                        .buffer_unordered(WARM_RANGE_CONCURRENCY);
                    while let Some(result) = results.next().await {
                        ranges += 1;
                        if result.is_err() {
                            failed += 1;
                            if outcome.is_ok() {
                                outcome = result;
                            }
                        }
                    }
                }
                if ended {
                    break;
                }
            }
        }
        debug!(
            tables = tables.len(),
            ranges, failed, "warmed table probe ranges"
        );

        session.finish();

        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn table_warming_does_not_decode_unrelated_catalog_records() {
        let catalog = crate::Catalog::open(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            crate::CatalogOptions::default(),
        )
        .await
        .unwrap();
        let tx = catalog.begin_write_tx().unwrap();
        tx.put(
            crate::store::key::Key::current(crate::store::key::EntityKey::View { view_id: 999 })
                .encode(),
            b"corrupt",
        )
        .unwrap();
        catalog.commit_write_tx(tx).await.unwrap();
        catalog.warm_tables(&[TableId::new(1)]).await.unwrap();
        catalog.close().await.unwrap();
    }
}
