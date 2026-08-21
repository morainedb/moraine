//! A read handle over either a read-write transaction or a read-only reader.
//!
//! Every typed read in `store` takes a [`ReadHandle`] and dispatches to a
//! `DbTransaction` or a `DbReader`; the reader never fences a live writer.

use std::{ops::Bound, sync::Arc};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use slatedb::{
    ByteRangeBounds, DbIterator, DbReader, DbTransaction, IterationOrder, KeyValue,
    config::ScanOptions,
};

use crate::store::key;

/// Read-ahead for a scan, in bytes, rounded up to a block by SlateDB
const SCAN_READ_AHEAD_BYTES: usize = 8 * 1024 * 1024;

/// How many block fetches a scan may have in flight
const SCAN_FETCH_TASKS: usize = 32;

/// Sub-ranges a split bulk scan keeps in flight; sized for a remote object
/// store, where each iterator's seek is a round trip.
pub(crate) const SCAN_SPLIT: usize = 8;

/// Bytes a data-scaled kind streams through one iterator before its
/// remainder is split: one read-ahead, the span one round trip covers.
const SCAN_SPLIT_BYTES: usize = SCAN_READ_AHEAD_BYTES;

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

impl ScanShape {
    /// Scan options for this shape: a bulk walk admits no blocks, a probe
    /// admits its blocks.
    fn options(self, order: ScanOrder) -> ScanOptions {
        ScanOptions {
            read_ahead_bytes: SCAN_READ_AHEAD_BYTES,
            max_fetch_tasks: SCAN_FETCH_TASKS,
            cache_blocks: matches!(self, Self::Probe),
            order: order.iteration_order(),
            ..ScanOptions::default()
        }
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

    /// The highest key suffix under `prefix`, or `None` when it holds no
    /// key: one seek from the end.
    async fn highest_suffix(
        &self,
        prefix: &[u8],
        shape: ScanShape,
    ) -> Result<Option<Vec<u8>>, slatedb::Error> {
        let mut descending = self
            .scan_prefix_ordered(prefix, .., shape, ScanOrder::Descending)
            .await?;
        Ok(descending
            .next()
            .await?
            .map(|entry| entry.key.get(prefix.len()..).unwrap_or(&[]).to_vec()))
    }

    /// Every entry of `range` under `prefix`, in key order.
    async fn collect_range(
        &self,
        prefix: &[u8],
        range: key::SuffixRange,
        shape: ScanShape,
    ) -> Result<Vec<KeyValue>, slatedb::Error> {
        let mut iterator = self.scan_prefix(prefix, range, shape).await?;
        let mut entries = Vec::new();
        while let Some(entry) = iterator.next().await? {
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Every entry under `prefix`, in key order, splitting each of the
    /// `splits` (prefixes extending it, in key order) that streams past
    /// `threshold` bytes into [`SCAN_SPLIT`] concurrent sub-ranges for its
    /// remainder. Below the threshold this is one iterator.
    pub(crate) async fn scan_prefix_split_over(
        &self,
        prefix: &[u8],
        splits: &[Vec<u8>],
        shape: ScanShape,
        threshold: usize,
    ) -> Result<Vec<KeyValue>, slatedb::Error> {
        let suffix_of = |bytes: &[u8]| bytes.get(prefix.len()..).unwrap_or(&[]).to_vec();
        let mut entries = Vec::new();
        let mut cursor = Bound::Unbounded;

        'resume: loop {
            let mut iterator = self
                .scan_prefix(prefix, (cursor.clone(), Bound::Unbounded), shape)
                .await?;
            let mut streamed = (None, 0_usize);
            while let Some(entry) = iterator.next().await? {
                let split = splits.iter().find(|split| entry.key.starts_with(split));
                if split != streamed.0 {
                    streamed = (split, 0);
                }
                streamed.1 = streamed
                    .1
                    .saturating_add(entry.key.len() + entry.value.len());
                let last = suffix_of(&entry.key);
                entries.push(entry);

                let Some(split) = split.filter(|_| streamed.1 > threshold) else {
                    continue;
                };
                // The rest of this split runs as concurrent ranges; the walk
                // resumes past it (or ends, when nothing sorts above it).
                let split_suffix = suffix_of(split);
                let end = key::increment_prefix(split_suffix.clone());
                let upper = end.clone().map_or(Bound::Unbounded, Bound::Excluded);
                let points = match self.highest_suffix(split, shape).await? {
                    Some(high) => {
                        let low = last.get(split_suffix.len()..).unwrap_or(&[]);
                        key::even_points_between(low, &high, SCAN_SPLIT)
                            .into_iter()
                            .map(|point| [split_suffix.as_slice(), &point].concat())
                            .collect()
                    }
                    None => Vec::new(),
                };
                let ranges = key::split_between(Bound::Excluded(last), upper, points);
                let remainder = stream::iter(ranges)
                    .map(|range| self.collect_range(prefix, range, shape))
                    .buffered(SCAN_SPLIT)
                    .try_concat()
                    .await?;
                entries.extend(remainder);

                match end {
                    Some(end) => {
                        cursor = Bound::Included(end);
                        continue 'resume;
                    }
                    None => break 'resume,
                }
            }
            break;
        }

        Ok(entries)
    }

    /// [`scan_prefix_split_over`](Self::scan_prefix_split_over) at the
    /// standing threshold: a split that outgrows one read-ahead.
    pub(crate) async fn scan_prefix_split(
        &self,
        prefix: &[u8],
        splits: &[Vec<u8>],
        shape: ScanShape,
    ) -> Result<Vec<KeyValue>, slatedb::Error> {
        self.scan_prefix_split_over(prefix, splits, shape, SCAN_SPLIT_BYTES)
            .await
    }
}

/// An owned read session backing one materialization. Borrow a
/// [`ReadHandle`] from it, then [`finish`](Self::finish) it.
pub(crate) enum ReadSession {
    /// A read-only reader, shared with the catalog.
    Reader(Arc<DbReader>),
}

impl ReadSession {
    /// Borrows a read handle over this session.
    pub(crate) fn handle(&self) -> ReadHandle<'_> {
        match self {
            Self::Reader(reader) => ReadHandle::Reader(reader),
        }
    }

    /// Releases the session.
    pub(crate) fn finish(self) {
        let Self::Reader(_) = self;
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use slatedb::{IsolationLevel, config::WriteOptions};

    use super::*;
    use crate::store::{
        key::{EntityKey, EntityKind, Key, Subspace},
        open::StoreBuilder,
    };

    /// A split scan yields exactly the entries one iterator yields, in the
    /// same order, whether the split kinds sit first, between, or last, are
    /// empty, hold one entry, or are cut into every sub-range.
    #[tokio::test]
    async fn a_split_scan_yields_what_one_iterator_yields() {
        let (db, _) = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for table_id in [3_u64, 7, 9] {
            tx.put(Key::current(EntityKey::Table { table_id }).encode(), b"t")
                .unwrap();
            tx.put(
                Key::current(EntityKey::TableStats { table_id }).encode(),
                b"s",
            )
            .unwrap();
            for column_id in 0..table_id {
                tx.put(
                    Key::current(EntityKey::Column {
                        table_id,
                        column_id,
                    })
                    .encode(),
                    b"c",
                )
                .unwrap();
            }
            for data_file_id in 0..(table_id * 30) {
                tx.put(
                    Key::current(EntityKey::File {
                        table_id,
                        data_file_id,
                    })
                    .encode(),
                    b"f",
                )
                .unwrap();
            }
        }
        // One delete file only: too few keys to spread any cut between.
        tx.put(
            Key::current(EntityKey::DeleteFile {
                table_id: 3,
                delete_file_id: 1,
            })
            .encode(),
            b"d",
        )
        .unwrap();
        tx.commit_with_options(&WriteOptions::default())
            .await
            .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let handle = ReadHandle::Tx(&tx);
        let prefix = key::subspace_prefix(Subspace::Current);
        let plain = handle
            .collect_range(
                &prefix,
                (Bound::Unbounded, Bound::Unbounded),
                ScanShape::Bulk,
            )
            .await
            .unwrap();
        assert_eq!(plain.len(), 3 + 3 + (3 + 7 + 9) + 30 * (3 + 7 + 9) + 1);
        let splits = key::SPLIT_SCAN_KINDS.map(key::current_entity_kind_prefix);

        for threshold in [0, 64, usize::MAX] {
            let split = handle
                .scan_prefix_split_over(&prefix, &splits, ScanShape::Bulk, threshold)
                .await
                .unwrap();
            assert_eq!(split.len(), plain.len(), "threshold {threshold}");
            assert!(
                split
                    .iter()
                    .zip(&plain)
                    .all(|(split, plain)| split.key == plain.key && split.value == plain.value),
                "threshold {threshold} reordered or altered the scan"
            );
        }

        // One kind on its own, split against its own prefix.
        let files = key::current_entity_kind_prefix(EntityKind::File);
        let plain_files = handle
            .collect_range(
                &files,
                (Bound::Unbounded, Bound::Unbounded),
                ScanShape::Bulk,
            )
            .await
            .unwrap();
        let split_files = handle
            .scan_prefix_split_over(&files, std::slice::from_ref(&files), ScanShape::Bulk, 0)
            .await
            .unwrap();
        assert_eq!(split_files.len(), plain_files.len());
        assert!(
            split_files
                .iter()
                .zip(&plain_files)
                .all(|(split, plain)| split.key == plain.key)
        );

        tx.rollback();
        db.close().await.unwrap();
    }

    /// A bulk scan reads ahead but admits nothing.
    #[test]
    fn bulk_scans_read_ahead_and_admit_nothing() {
        let options = ScanShape::Bulk.options(ScanOrder::Ascending);
        assert_eq!(options.read_ahead_bytes, SCAN_READ_AHEAD_BYTES);
        assert_eq!(options.max_fetch_tasks, SCAN_FETCH_TASKS);
        assert!(!options.cache_blocks);
    }

    /// A probe admits its blocks and keeps the read-ahead.
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
