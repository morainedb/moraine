//! A read handle over either a read-write transaction or a read-only reader.
//!
//! Every typed read in `store` takes a [`ReadHandle`] and dispatches to a
//! `DbTransaction` or a `DbReader`; the reader never fences a live writer.

use std::sync::Arc;

use bytes::Bytes;
use slatedb::{
    ByteRangeBounds, DbIterator, DbReader, DbTransaction, IterationOrder, config::ScanOptions,
};

/// Read-ahead for a scan, in bytes, rounded up to a block by SlateDB.
const SCAN_READ_AHEAD_BYTES: usize = 4 * 1024 * 1024;

/// How many block fetches a scan may have in flight.
const SCAN_FETCH_TASKS: usize = 8;

/// The shape of a scan, which decides its block-cache admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanShape {
    /// A whole-subspace walk; its blocks are not admitted.
    Bulk,
    /// A targeted lookup; its blocks are admitted.
    Probe,
}

/// The direction SlateDB traverses a scan range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanOrder {
    /// Lowest encoded key first.
    Ascending,
    /// Highest encoded key first.
    Descending,
}

impl ScanOrder {
    /// Selects the direction requested by an index read.
    pub(crate) fn from_reverse(reverse: bool) -> Self {
        if reverse {
            Self::Descending
        } else {
            Self::Ascending
        }
    }

    fn iteration_order(self) -> IterationOrder {
        match self {
            Self::Ascending => IterationOrder::Ascending,
            Self::Descending => IterationOrder::Descending,
        }
    }
}

/// Scan options for a whole-subspace walk; blocks are not admitted.
fn bulk_scan_options() -> ScanOptions {
    ScanOptions {
        read_ahead_bytes: SCAN_READ_AHEAD_BYTES,
        max_fetch_tasks: SCAN_FETCH_TASKS,
        cache_blocks: false,
        ..ScanOptions::default()
    }
}

/// Scan options for a targeted lookup; blocks are admitted.
fn probe_scan_options() -> ScanOptions {
    ScanOptions {
        read_ahead_bytes: SCAN_READ_AHEAD_BYTES,
        max_fetch_tasks: SCAN_FETCH_TASKS,
        cache_blocks: true,
        ..ScanOptions::default()
    }
}

impl ScanShape {
    fn options(self, order: ScanOrder) -> ScanOptions {
        let mut options = match self {
            Self::Bulk => bulk_scan_options(),
            Self::Probe => probe_scan_options(),
        };
        options.order = order.iteration_order();
        options
    }
}

/// A borrowed read over a read-write transaction or a read-only reader.
#[derive(Clone, Copy)]
pub(crate) enum ReadHandle<'a> {
    /// A snapshot-isolated read-write transaction (`Db::begin`).
    Tx(&'a DbTransaction),
    /// A read-only reader following the manifest.
    Reader(&'a DbReader),
}

impl ReadHandle<'_> {
    /// Point read of one key.
    pub(crate) async fn get<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<Bytes>, slatedb::Error> {
        match self {
            Self::Tx(tx) => tx.get(key).await,
            Self::Reader(reader) => reader.get(key).await,
        }
    }

    /// Whether several reads through this handle observe a single store
    /// state: true for a transaction, false for a manifest-following reader.
    pub(crate) fn is_isolated(&self) -> bool {
        matches!(self, Self::Tx(_))
    }

    /// Scan keys sharing `prefix`, restricted to `subrange`, with the
    /// admission behaviour `shape` names.
    pub(crate) async fn scan_prefix<P, T>(
        &self,
        prefix: P,
        subrange: T,
        shape: ScanShape,
    ) -> Result<DbIterator, slatedb::Error>
    where
        P: AsRef<[u8]> + Send,
        T: ByteRangeBounds + Send,
    {
        self.scan_prefix_ordered(prefix, subrange, shape, ScanOrder::Ascending)
            .await
    }

    /// Scan keys sharing `prefix` in the requested key order.
    pub(crate) async fn scan_prefix_ordered<P, T>(
        &self,
        prefix: P,
        subrange: T,
        shape: ScanShape,
        order: ScanOrder,
    ) -> Result<DbIterator, slatedb::Error>
    where
        P: AsRef<[u8]> + Send,
        T: ByteRangeBounds + Send,
    {
        let options = shape.options(order);
        match self {
            Self::Tx(tx) => {
                tx.scan_prefix_with_options(prefix, subrange, &options)
                    .await
            }
            Self::Reader(reader) => {
                reader
                    .scan_prefix_with_options(prefix, subrange, &options)
                    .await
            }
        }
    }
}

/// An owned read session backing one materialization. Borrow a
/// [`ReadHandle`] from it, then [`finish`](Self::finish) it.
pub(crate) enum ReadSession {
    /// A read-write transaction, rolled back on `finish`.
    Tx(DbTransaction),
    /// A read-only reader, shared with the catalog.
    Reader(Arc<DbReader>),
}

impl ReadSession {
    /// Borrows a read handle over this session.
    pub(crate) fn handle(&self) -> ReadHandle<'_> {
        match self {
            Self::Tx(tx) => ReadHandle::Tx(tx),
            Self::Reader(reader) => ReadHandle::Reader(reader),
        }
    }

    /// Releases the session, rolling back a read-write transaction.
    pub(crate) fn finish(self) {
        if let Self::Tx(tx) = self {
            tx.rollback();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bulk scan reads ahead but admits nothing.
    #[test]
    fn bulk_scans_read_ahead_and_admit_nothing() {
        let options = ScanShape::Bulk.options(ScanOrder::Ascending);
        assert_eq!(options.read_ahead_bytes, SCAN_READ_AHEAD_BYTES);
        assert_eq!(options.max_fetch_tasks, SCAN_FETCH_TASKS);
        assert!(!options.cache_blocks);
    }

    /// A probe admits its blocks and keeps the same read-ahead.
    #[test]
    fn probe_scans_admit_their_blocks() {
        let options = ScanShape::Probe.options(ScanOrder::Ascending);
        assert_eq!(options.read_ahead_bytes, SCAN_READ_AHEAD_BYTES);
        assert_eq!(options.max_fetch_tasks, SCAN_FETCH_TASKS);
        assert!(options.cache_blocks);
    }

    /// A descending probe asks SlateDB to iterate backwards.
    #[test]
    fn descending_probes_request_descending_iteration() {
        let options = ScanShape::Probe.options(ScanOrder::Descending);
        assert!(matches!(options.order, IterationOrder::Descending));
    }
}
