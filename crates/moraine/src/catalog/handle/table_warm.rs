//! Warming one table's probe ranges into the block cache.

use futures::{StreamExt, stream};
use tracing::{debug, trace};

use super::ReadOnlyCatalog;
use crate::{
    catalog::{CatalogSnapshot, TableId},
    error::Result,
    store::{
        handle::{ReadHandle, ScanShape},
        key::{
            IndexKind, InlineOperationKind, index_index_prefix, inline_chunk_range_table_prefix,
            inline_live_table_prefix, inline_schema_table_prefix,
        },
    },
};

/// Ranges one table warm keeps in flight.
const WARM_RANGE_CONCURRENCY: usize = 8;

/// The prefixes a lookup on `table` probes: each of its indexes' entries and
/// its inlined data.
fn probe_prefixes(view: &CatalogSnapshot, table: TableId) -> Vec<Vec<u8>> {
    let table_id = table.get();
    let mut prefixes = view
        .indexes_of(table)
        .into_iter()
        .map(|index| {
            let kind = if index.unique {
                IndexKind::Unique
            } else {
                IndexKind::Multi
            };
            index_index_prefix(kind, index.id.get())
        })
        .collect::<Vec<_>>();
    prefixes.extend([
        inline_live_table_prefix(InlineOperationKind::Insert, table_id),
        inline_live_table_prefix(InlineOperationKind::InlineDelete, table_id),
        inline_live_table_prefix(InlineOperationKind::FileDelete, table_id),
        inline_schema_table_prefix(table_id),
        inline_chunk_range_table_prefix(table_id),
    ]);
    prefixes
}

/// Reads the first entry under `prefix` in probe shape, admitting the SST
/// metadata and first block a probe there would fetch.
async fn warm_prefix(handle: ReadHandle<'_>, prefix: Vec<u8>) -> Result<()> {
    let mut iterator = handle.scan_prefix(prefix, .., ScanShape::Probe).await?;
    iterator.next().await?;
    Ok(())
}

impl ReadOnlyCatalog {
    /// Warms the store ranges a lookup on each of `tables` probes — its
    /// indexes' entries and its inlined data — into the block cache, so a
    /// cold table pays one burst of reads rather than a round trip per probe.
    /// Best-effort: a range that fails to read is skipped and counted in the
    /// log.
    ///
    /// # Errors
    ///
    /// Returns a store error if the head view cannot be read.
    pub async fn warm_tables(&self, tables: &[TableId]) -> Result<()> {
        let session = self.begin_read().await?;
        let handle = session.handle();
        let view = self.head_view(handle).await?;
        let prefixes = tables
            .iter()
            .flat_map(|table| probe_prefixes(&view, *table))
            .collect::<Vec<_>>();
        let ranges = prefixes.len();

        let failed = stream::iter(prefixes)
            .map(|prefix| warm_prefix(handle, prefix))
            .buffer_unordered(WARM_RANGE_CONCURRENCY)
            .filter(|result| std::future::ready(result.is_err()))
            .count()
            .await;
        debug!(
            tables = tables.len(),
            ranges, failed, "warmed table probe ranges"
        );

        session.finish();

        Ok(())
    }

    /// Warms `table` in the background the first time this handle touches
    /// it; later touches are free. Nothing happens outside a tokio runtime
    /// or when the warm fails.
    pub(crate) fn warm_table_on_first_touch(&self, table: TableId) {
        let first_touch = self
            .warmed_tables
            .lock()
            .is_ok_and(|mut warmed| warmed.insert(table));
        if !first_touch {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };

        let catalog = self.clone();
        runtime.spawn(async move {
            if let Err(error) = catalog.warm_tables(&[table]).await {
                trace!(table_id = table.get(), %error, "table warm skipped");
            }
        });
    }
}
