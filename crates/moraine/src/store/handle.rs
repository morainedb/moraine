//! A read handle over either a read-write transaction or a read-only reader.
//!
//! Every typed read in `store` takes a [`ReadHandle`] and dispatches to a
//! `DbTransaction` or a `DbReader`; the reader never fences a live writer.

use std::{ops::Bound, sync::Arc};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use slatedb::{
    ByteRangeBounds, Db, DbIterator, DbReader, DbTransaction, IterationOrder, KeyValue,
    config::ScanOptions,
};

use crate::store::key;

/// Read-ahead for a scan, in bytes, rounded up to a block by SlateDB
pub(crate) const SCAN_READ_AHEAD_BYTES: usize = 8 * 1024 * 1024;

/// How many block fetches a scan may have in flight
const SCAN_FETCH_TASKS: usize = 32;

/// Read-ahead for a streaming scan, in bytes: one round trip's worth of
/// blocks ahead of a sequential consumer, bounded so an SST's iterator
/// holds at most a few hundred kibibytes in flight.
const STREAM_READ_AHEAD_BYTES: usize = 256 * 1024;

/// How many read-ahead fetches a streaming scan keeps in flight.
const STREAM_FETCH_TASKS: usize = 2;

/// Equality probes retain useful coalescing for high-fanout keys.
const EQUALITY_READ_AHEAD_BYTES: usize = 1024 * 1024;

/// Sub-ranges a split bulk scan keeps in flight; sized for a remote object
/// store, where each iterator's seek is a round trip.
pub(crate) const SCAN_SPLIT: usize = 8;

/// Bytes a data-scaled kind streams through one iterator before its
/// remainder is split: one read-ahead, the span one round trip covers.
const SCAN_SPLIT_BYTES: usize = SCAN_READ_AHEAD_BYTES;

/// Fetch geometry and admission, selected independently at each call site.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ScanShape {
    fetch: FetchShape,
    admission: crate::read_policy::CacheAdmission,
}

/// Read-ahead geometry without an implicit retention policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FetchShape {
    Wide,
    Bounded,
    Equality,
    Seek,
}

impl FetchShape {
    pub(crate) fn retain(self) -> ScanShape {
        ScanShape {
            fetch: self,
            admission: crate::read_policy::CacheAdmission::Retain,
        }
    }

    pub(crate) fn reuse_only(self) -> ScanShape {
        ScanShape {
            fetch: self,
            admission: crate::read_policy::CacheAdmission::ReuseOnly,
        }
    }
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
    pub(crate) fn wide() -> FetchShape {
        FetchShape::Wide
    }
    pub(crate) fn bounded() -> FetchShape {
        FetchShape::Bounded
    }
    pub(crate) fn equality() -> FetchShape {
        FetchShape::Equality
    }
    pub(crate) fn seek() -> FetchShape {
        FetchShape::Seek
    }

    fn options(self, order: ScanOrder) -> ScanOptions {
        let (read_ahead_bytes, max_fetch_tasks) = match self.fetch {
            FetchShape::Wide => (SCAN_READ_AHEAD_BYTES, SCAN_FETCH_TASKS),
            FetchShape::Bounded => (STREAM_READ_AHEAD_BYTES, STREAM_FETCH_TASKS),
            FetchShape::Equality => (EQUALITY_READ_AHEAD_BYTES, 2),
            FetchShape::Seek => (1, 1),
        };
        ScanOptions {
            read_ahead_bytes,
            max_fetch_tasks,
            cache_blocks: self.admission.retains(),
            order: order.iteration_order(),
            ..ScanOptions::default()
        }
    }
}

/// A borrowed read over a read-write transaction, a read-only reader, or
/// the writer's latest state.
#[derive(Clone, Copy)]
pub(crate) enum ReadHandle<'a> {
    /// A snapshot-isolated read-write transaction (`Db::begin`).
    Tx(&'a DbTransaction),
    /// A read-only snapshot owned by the writer process.
    Snapshot(&'a slatedb::DbSnapshot),
    /// A read-only reader following the manifest.
    Reader(&'a DbReader),
    /// A fixed reader with an independently renewed checkpoint.
    Checkpoint(&'a Arc<super::checkpoint::CheckpointReader>),
    /// The writer itself, read without a transaction; each read is atomic
    /// on its own.
    Writer(&'a Db),
}

/// A scan that retains and checks its checkpoint protection between batches.
pub(crate) struct ScanIterator {
    iterator: DbIterator,
    checkpoint: Option<Arc<super::checkpoint::CheckpointReader>>,
}

impl ScanIterator {
    pub(crate) async fn next(&mut self) -> Result<Option<KeyValue>, slatedb::Error> {
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.reader()?;
        }
        let next = self.iterator.next().await?;
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.reader()?;
        }
        Ok(next)
    }
}

impl ReadHandle<'_> {
    /// Refuses a checkpoint whose lease no longer protects its backing files.
    pub(crate) fn validate(&self) -> Result<(), slatedb::Error> {
        if let Self::Checkpoint(reader) = self {
            reader.reader()?;
        }
        Ok(())
    }
    /// Point read of one key.
    pub(crate) async fn get<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<Bytes>, slatedb::Error> {
        crate::telemetry::read::point_read();
        match self {
            Self::Tx(tx) => tx.get(key).await,
            Self::Snapshot(snapshot) => snapshot.get(key).await,
            Self::Reader(reader) => reader.get(key).await,
            Self::Checkpoint(reader) => {
                let value = reader.reader()?.get(key).await?;
                reader.reader()?;
                Ok(value)
            }
            Self::Writer(db) => db.get(key).await,
        }
    }

    /// Whether several reads through this handle observe a single store
    /// state: true for a transaction, false for a manifest-following reader
    /// and for the writer's latest state.
    pub(crate) fn is_isolated(&self) -> bool {
        matches!(self, Self::Tx(_) | Self::Snapshot(_) | Self::Checkpoint(_))
    }

    /// Scan keys sharing `prefix`, restricted to `subrange`, with the
    /// admission behaviour `shape` names.
    pub(crate) async fn scan_prefix<P, T>(
        &self,
        prefix: P,
        subrange: T,
        shape: ScanShape,
    ) -> Result<ScanIterator, slatedb::Error>
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
    ) -> Result<ScanIterator, slatedb::Error>
    where
        P: AsRef<[u8]> + Send,
        T: ByteRangeBounds + Send,
    {
        let options = shape.options(order);
        let iterator = match self {
            Self::Tx(tx) => {
                tx.scan_prefix_with_options(prefix, subrange, &options)
                    .await
            }
            Self::Snapshot(snapshot) => {
                snapshot
                    .scan_prefix_with_options(prefix, subrange, &options)
                    .await
            }
            Self::Reader(reader) => {
                reader
                    .scan_prefix_with_options(prefix, subrange, &options)
                    .await
            }
            Self::Checkpoint(reader) => {
                reader
                    .reader()?
                    .scan_prefix_with_options(prefix, subrange, &options)
                    .await
            }
            Self::Writer(db) => {
                db.scan_prefix_with_options(prefix, subrange, &options)
                    .await
            }
        }?;
        crate::telemetry::read::scan_opened(shape.admission);
        Ok(ScanIterator {
            iterator,
            checkpoint: match self {
                Self::Checkpoint(reader) => Some((*reader).clone()),
                _ => None,
            },
        })
    }

    /// The highest key suffix under `prefix`, or `None` when it holds no
    /// key: one seek from the end.
    async fn highest_suffix(&self, prefix: &[u8]) -> Result<Option<Vec<u8>>, slatedb::Error> {
        let mut descending = self
            .scan_prefix_ordered(
                prefix,
                ..,
                ScanShape::seek().retain(),
                ScanOrder::Descending,
            )
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
            entries.push(admit_entry(entry)?);
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
                entries.push(admit_entry(entry)?);

                let Some(split) = split.filter(|_| streamed.1 > threshold) else {
                    continue;
                };
                // The rest of this split runs as concurrent ranges; the walk
                // resumes past it (or ends, when nothing sorts above it).
                let split_suffix = suffix_of(split);
                let end = key::increment_prefix(split_suffix.clone());
                let upper = end.clone().map_or(Bound::Unbounded, Bound::Excluded);
                let points = match self.highest_suffix(split).await? {
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

/// A read handle owning what it reads through, so work on it can run on a
/// task the caller does not outlive. Only the forms that are already shared
/// or cheap to clone have one; a session owning its transaction outright
/// does not, since rolling that back needs the sole reference back.
#[derive(Clone)]
pub(crate) enum OwnedReadHandle {
    /// A transaction shared by every read in one pinned scope.
    Tx(Arc<DbTransaction>),
    /// A read-only snapshot shared by the scopes using its sequence.
    Snapshot(Arc<slatedb::DbSnapshot>),
    /// A read-only reader following the manifest.
    Reader(Arc<DbReader>),
    /// A fixed reader shared by the scopes using its checkpoint.
    Checkpoint(Arc<super::checkpoint::CheckpointReader>),
    /// The writer's latest state. `Db` is a handle, so this clones one.
    Writer(Db),
}

impl OwnedReadHandle {
    /// Borrows a read handle over what this owns.
    pub(crate) fn borrow(&self) -> ReadHandle<'_> {
        match self {
            Self::Tx(transaction) => ReadHandle::Tx(transaction),
            Self::Snapshot(snapshot) => ReadHandle::Snapshot(snapshot),
            Self::Reader(reader) => ReadHandle::Reader(reader),
            Self::Checkpoint(reader) => ReadHandle::Checkpoint(reader),
            Self::Writer(db) => ReadHandle::Writer(db),
        }
    }
}

/// An owned read session backing one materialization. Borrow a
/// [`ReadHandle`] from it, then [`finish`](Self::finish) it.
pub(crate) enum ReadSession {
    /// A read-write transaction, rolled back on `finish`.
    Tx(DbTransaction),
    /// A read transaction shared by every read in one pinned scope.
    Pinned(OwnedReadHandle),
    /// A read-only reader, shared with the catalog.
    Reader(Arc<DbReader>),
}

impl ReadSession {
    /// Borrows a read handle over this session.
    pub(crate) fn handle(&self) -> ReadHandle<'_> {
        match self {
            Self::Tx(tx) => ReadHandle::Tx(tx),
            Self::Pinned(handle) => handle.borrow(),
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

/// Retains admission for scan buffers through decoding and cache installation.
fn admit_entry(mut entry: KeyValue) -> Result<KeyValue, slatedb::Error> {
    let bytes = (entry.key.len() as u64)
        .saturating_add(entry.value.len() as u64)
        .saturating_add(64);
    let memory = crate::MemoryReservation::reserve(bytes)
        .map_err(|error| slatedb::Error::invalid(error.to_string()))?;
    entry.value = memory
        .into_bytes(entry.value)
        .map_err(|error| slatedb::Error::invalid(error.to_string()))?;
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use slatedb::IsolationLevel;

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
        let policy = ScanShape::wide().reuse_only();
        let (db, _) = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let tx = db.begin(IsolationLevel::Snapshot).unwrap();
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
        let write = tx.commit().await.unwrap().unwrap();
        write.await_durable().await.unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).unwrap();
        let handle = ReadHandle::Tx(&tx);
        let prefix = key::subspace_prefix(Subspace::Current);
        let plain = handle
            .collect_range(&prefix, (Bound::Unbounded, Bound::Unbounded), policy)
            .await
            .unwrap();
        assert_eq!(plain.len(), 3 + 3 + (3 + 7 + 9) + 30 * (3 + 7 + 9) + 1);
        let splits = key::SPLIT_SCAN_KINDS.map(key::current_entity_kind_prefix);

        for threshold in [0, 64, usize::MAX] {
            let split = handle
                .scan_prefix_split_over(&prefix, &splits, policy, threshold)
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
            .collect_range(&files, (Bound::Unbounded, Bound::Unbounded), policy)
            .await
            .unwrap();
        let split_files = handle
            .scan_prefix_split_over(&files, std::slice::from_ref(&files), policy, 0)
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

    #[test]
    fn admission_does_not_change_fetch_geometry() {
        for fetch in [
            ScanShape::wide(),
            ScanShape::bounded(),
            ScanShape::equality(),
            ScanShape::seek(),
        ] {
            let retained = fetch.retain().options(ScanOrder::Ascending);
            let transient = fetch.reuse_only().options(ScanOrder::Ascending);
            assert_eq!(retained.read_ahead_bytes, transient.read_ahead_bytes);
            assert_eq!(retained.max_fetch_tasks, transient.max_fetch_tasks);
            assert!(retained.cache_blocks);
            assert!(!transient.cache_blocks);
        }
    }

    /// A bulk scan reads ahead but admits nothing.
    #[test]
    fn bulk_scans_read_ahead_and_admit_nothing() {
        let options = ScanShape::wide().reuse_only().options(ScanOrder::Ascending);
        assert_eq!(options.read_ahead_bytes, SCAN_READ_AHEAD_BYTES);
        assert_eq!(options.max_fetch_tasks, SCAN_FETCH_TASKS);
        assert!(!options.cache_blocks);
    }

    /// A probe admits its blocks and keeps the read-ahead.
    #[test]
    fn probe_scans_admit_their_blocks() {
        let options = ScanShape::wide().retain().options(ScanOrder::Ascending);
        assert_eq!(options.read_ahead_bytes, SCAN_READ_AHEAD_BYTES);
        assert_eq!(options.max_fetch_tasks, SCAN_FETCH_TASKS);
        assert!(options.cache_blocks);
    }

    /// Equality scans bound buffering independently of range probes.
    #[test]
    fn equality_scans_bound_read_ahead_and_admit_blocks() {
        let options = ScanShape::equality().retain().options(ScanOrder::Ascending);
        assert_eq!(options.read_ahead_bytes, 1024 * 1024);
        assert_eq!(options.max_fetch_tasks, 2);
        assert!(options.cache_blocks);
    }

    /// A streaming scan reads ahead a bounded window, two fetches at a time,
    /// and admits nothing.
    #[test]
    fn streaming_scans_read_ahead_a_bounded_window_and_admit_nothing() {
        let options = ScanShape::bounded()
            .reuse_only()
            .options(ScanOrder::Ascending);
        assert_eq!(options.read_ahead_bytes, 256 * 1024);
        assert_eq!(options.max_fetch_tasks, 2);
        assert!(!options.cache_blocks);
    }

    /// A seek fetches one block, one fetch at a time, and admits it.
    #[test]
    fn seeks_fetch_one_block_and_admit_it() {
        let options = ScanShape::seek().retain().options(ScanOrder::Ascending);
        assert_eq!(options.read_ahead_bytes, 1);
        assert_eq!(options.max_fetch_tasks, 1);
        assert!(options.cache_blocks);
    }

    /// A descending probe asks SlateDB to iterate backwards.
    #[test]
    fn descending_probes_request_descending_iteration() {
        let options = ScanShape::wide().retain().options(ScanOrder::Descending);
        assert!(matches!(options.order, IterationOrder::Descending));
    }
}
