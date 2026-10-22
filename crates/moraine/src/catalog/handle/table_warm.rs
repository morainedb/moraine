//! Warming one table's probe ranges into the block cache.

use futures::{StreamExt, stream};
use tracing::debug;

use super::ReadOnlyCatalog;
use crate::{
    catalog::{CatalogSnapshot, TableId},
    error::Result,
    store::{
        handle::{ReadHandle, ScanShape},
        key::{IndexKind, table_probe_prefixes},
    },
};

/// Ranges one table warm keeps in flight, sized for a remote object store.
const WARM_RANGE_CONCURRENCY: usize = 32;

/// The prefixes a lookup on `table` probes: each of its indexes' entries and
/// its inlined data.
fn probe_prefixes(view: &CatalogSnapshot, table: TableId) -> Vec<Vec<u8>> {
    let indexes = view.indexes_of(table).into_iter().map(|index| {
        let kind = if index.unique {
            IndexKind::Unique
        } else {
            IndexKind::Multi
        };
        (kind, index.id.get())
    });

    table_probe_prefixes(table.get(), indexes)
}

/// Reads the first entry under `prefix` in seek shape, admitting the SST
/// metadata and first block a probe there would fetch.
async fn warm_prefix(handle: ReadHandle<'_>, prefix: Vec<u8>) -> Result<()> {
    let mut iterator = handle.scan_prefix(prefix, .., ScanShape::Seek).await?;
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
    /// Returns a store error if the head view or any probe range cannot be
    /// read.
    pub async fn warm_tables(&self, tables: &[TableId]) -> Result<()> {
        let epoch = super::cache_epoch(&self.projections);
        let session = self.begin_read().await?;
        let handle = session.handle();
        let view = self.head_view(handle, epoch).await?;
        let mut tables_to_warm = tables.iter();
        let mut current = Vec::new().into_iter();
        let prefixes = std::iter::from_fn(|| {
            loop {
                if let Some(prefix) = current.next() {
                    return Some(prefix);
                }
                current = probe_prefixes(&view, *tables_to_warm.next()?).into_iter();
            }
        });
        let mut results = stream::iter(prefixes)
            .map(|prefix| warm_prefix(handle, prefix))
            .buffer_unordered(WARM_RANGE_CONCURRENCY);
        let (mut ranges, mut failed) = (0, 0);
        let mut outcome = Ok(());
        while let Some(result) = results.next().await {
            ranges += 1;
            if result.is_err() {
                failed += 1;
                if outcome.is_ok() {
                    outcome = result;
                }
            }
        }
        drop(results);
        debug!(
            tables = tables.len(),
            ranges, failed, "warmed table probe ranges"
        );

        session.finish();

        outcome
    }
}
