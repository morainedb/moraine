//! The process-wide cache of parsed Parquet footers and decoded file row-id
//! summaries, sharing one byte allowance reserved from the store cache's
//! budget, and the store cache's directory when it has one. Concurrent
//! misses on one footer share a single fill.

use std::{
    future::Future,
    io::{Read, Write},
    path::{Path as FilePath, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use bytes::Bytes;
use foyer::{Cache, HybridCache};
use object_store::path::Path;
use parquet::{
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{
        PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter,
    },
};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    data_file::{
        DataStore, ParquetFile,
        data_store::StoreIdentity,
        reader::ObjectStoreReader,
        row_set::{FileRowSet, FileRowSetKind, PositionedRowSet, RowOrder},
        usize_as_u64,
    },
    error::{Error, Result},
    store::cache::{self, RowSummaryOccupancy},
};

/// Whether a cached footer carries the page index. Mirrors
/// [`PageIndexPolicy`], which is not hashable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(super) enum PageIndex {
    Skip,
    Optional,
    Required,
}

impl From<PageIndexPolicy> for PageIndex {
    fn from(policy: PageIndexPolicy) -> Self {
        match policy {
            PageIndexPolicy::Skip => Self::Skip,
            PageIndexPolicy::Optional => Self::Optional,
            PageIndexPolicy::Required => Self::Required,
        }
    }
}

/// One file's identity within its store. The path and recorded size guard
/// against a reused catalog file id: a mismatch misses.
pub(super) struct FileSummaryKey<'a> {
    pub(super) table_id: u64,
    pub(super) data_file_id: u64,
    pub(super) path: &'a Path,
    pub(super) file_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum AuxiliaryKey {
    Metadata {
        store: StoreIdentity,
        path: String,
        file_size: u64,
        page_index: PageIndex,
    },
    Summary {
        store: StoreIdentity,
        table_id: u64,
        data_file_id: u64,
        path: String,
        file_size: u64,
    },
    /// One byte range of a file, exactly as a read asked for it. The
    /// recorded size guards against a path reused at another length, as it
    /// does for the other two.
    Range {
        store: StoreIdentity,
        path: String,
        file_size: u64,
        start: u64,
        end: u64,
    },
    /// One delete file's decoded `pos` column. Keyed by the object alone:
    /// the positions are a function of its bytes, not of the catalog
    /// record naming it.
    DeletePositions {
        store: StoreIdentity,
        path: String,
        file_size: u64,
    },
}

#[derive(Debug, Clone)]
pub(super) enum AuxiliaryValue {
    Metadata(Arc<ParquetMetaData>),
    Summary(Arc<PositionedRowSet>),
    Block(Bytes),
    DeletePositions(Arc<FileRowSet>),
}

/// A value with its charge measured once, at insertion.
#[derive(Debug, Clone)]
pub(super) struct Weighed {
    pub(super) value: AuxiliaryValue,
    bytes: usize,
}

impl From<AuxiliaryValue> for Weighed {
    fn from(value: AuxiliaryValue) -> Self {
        let bytes = match &value {
            AuxiliaryValue::Metadata(metadata) => metadata.memory_size(),
            AuxiliaryValue::Summary(positioned) => {
                usize::try_from(positioned.estimated_bytes()).unwrap_or(usize::MAX)
            }
            AuxiliaryValue::DeletePositions(rows) => {
                usize::try_from(rows.estimated_bytes()).unwrap_or(usize::MAX)
            }
            AuxiliaryValue::Block(bytes) => bytes.len(),
        };

        Self { value, bytes }
    }
}

/// The disk form of a value: a tag byte, then a footer as the Parquet
/// metadata writer lays it out (page index included) or a row set in its
/// own shape. Delete positions carry no order and use one tag per shape;
/// a summary's tag also names its [`RowOrder`], since a cached summary from
/// before positions existed cannot answer them.
mod tag {
    pub(super) const METADATA: u8 = 0;
    /// Retired for summaries: a pre-upgrade encoder wrote this for any
    /// dense range, discarding whether the file order was ascending or a
    /// contiguous-but-permuted run. Still decoded as a parse failure so it
    /// misses and rebuilds rather than lying about order. Never written
    /// again; superseded for ascending ranges by [`RANGE_ASCENDING`].
    pub(super) const RANGE: u8 = 1;
    /// Retired for summaries: an entry predating positions. Still decoded
    /// as a parse failure so it misses and rebuilds rather than lying
    /// about order. Never written again.
    pub(super) const LEGACY_ROARING: u8 = 2;
    /// See [`LEGACY_ROARING`].
    pub(super) const LEGACY_SORTED: u8 = 3;
    pub(super) const BLOCK: u8 = 4;
    pub(super) const DELETE_RANGE: u8 = 5;
    pub(super) const DELETE_ROARING: u8 = 6;
    pub(super) const DELETE_SORTED: u8 = 7;
    pub(super) const ROARING_ASCENDING: u8 = 8;
    pub(super) const SORTED_ASCENDING: u8 = 9;
    pub(super) const ROARING_PERMUTED: u8 = 10;
    pub(super) const SORTED_PERMUTED: u8 = 11;
    /// A dense ascending range: positional by arithmetic. Distinct from the
    /// retired [`RANGE`], which could also have been written for a
    /// contiguous-but-permuted file.
    pub(super) const RANGE_ASCENDING: u8 = 12;

    /// The `(range, roaring, sorted)` tags a delete-position set writes.
    pub(super) const DELETE_SHAPES: [u8; 3] = [DELETE_RANGE, DELETE_ROARING, DELETE_SORTED];
}

/// Writes `rows` as one of `shapes`, whichever representation it took.
fn encode_row_set(
    rows: &FileRowSet,
    shapes: [u8; 3],
    writer: &mut impl Write,
) -> foyer::Result<()> {
    let io = foyer::Error::io_error;
    let [range, roaring, sorted] = shapes;
    match rows {
        FileRowSet::Range { start, end } => {
            writer.write_all(&[range]).map_err(io)?;
            writer.write_all(&start.to_le_bytes()).map_err(io)?;
            writer.write_all(&end.to_le_bytes()).map_err(io)
        }
        FileRowSet::Roaring(bitmap) => {
            writer.write_all(&[roaring]).map_err(io)?;
            bitmap.serialize_into(writer).map_err(io)
        }
        FileRowSet::Sorted(row_ids) => {
            writer.write_all(&[sorted]).map_err(io)?;
            writer
                .write_all(&usize_as_u64(row_ids.len()).to_le_bytes())
                .map_err(io)?;
            row_ids
                .iter()
                .try_for_each(|row_id| writer.write_all(&row_id.to_le_bytes()))
                .map_err(io)
        }
    }
}

/// Reads back the body [`encode_row_set`] wrote, given which shape the
/// consumed tag named.
fn decode_row_set(shape: RowSetShape, reader: &mut impl Read) -> foyer::Result<FileRowSet> {
    Ok(match shape {
        RowSetShape::Range => FileRowSet::Range {
            start: read_u64(reader)?,
            end: read_u64(reader)?,
        },
        RowSetShape::Roaring => FileRowSet::Roaring(read_roaring_body(reader)?),
        RowSetShape::Sorted => FileRowSet::Sorted(read_sorted_body(reader)?),
    })
}

fn read_roaring_body(reader: &mut impl Read) -> foyer::Result<RoaringTreemap> {
    RoaringTreemap::deserialize_from(reader).map_err(foyer::Error::io_error)
}

fn read_sorted_body(reader: &mut impl Read) -> foyer::Result<Vec<u64>> {
    let count = usize::try_from(read_u64(reader)?).unwrap_or(usize::MAX);
    (0..count)
        .map(|_| read_u64(reader))
        .collect::<foyer::Result<Vec<u64>>>()
}

#[derive(Clone, Copy)]
enum RowSetShape {
    Range,
    Roaring,
    Sorted,
}

/// Writes `positioned`'s set with [`encode_row_set`], then — for a permuted
/// order — its rank-to-position permutation, count-prefixed.
fn encode_positioned_row_set(
    positioned: &PositionedRowSet,
    writer: &mut impl Write,
) -> foyer::Result<()> {
    // Unreachable by construction: `PositionedRowSet::from_file_order` never
    // pairs a permutation with a range. A bare range tag carries no
    // permutation, so writing one here would silently answer positions by
    // arithmetic on decode.
    if matches!(
        (&positioned.rows, &positioned.order),
        (FileRowSet::Range { .. }, RowOrder::Permuted(_))
    ) {
        return Err(foyer::Error::new(
            foyer::ErrorKind::Parse,
            "a permuted row set cannot encode as a range",
        ));
    }

    let shapes = match positioned.order {
        RowOrder::Ascending => [
            tag::RANGE_ASCENDING,
            tag::ROARING_ASCENDING,
            tag::SORTED_ASCENDING,
        ],
        // The range slot is unused here: a permuted range was already
        // rejected above.
        RowOrder::Permuted(_) => [
            tag::RANGE_ASCENDING,
            tag::ROARING_PERMUTED,
            tag::SORTED_PERMUTED,
        ],
    };
    encode_row_set(&positioned.rows, shapes, writer)?;

    if let RowOrder::Permuted(permutation) = &positioned.order {
        write_permutation(permutation, writer)?;
    }

    Ok(())
}

fn write_permutation(permutation: &[u32], writer: &mut impl Write) -> foyer::Result<()> {
    let io = foyer::Error::io_error;
    writer
        .write_all(&usize_as_u64(permutation.len()).to_le_bytes())
        .map_err(io)?;
    permutation
        .iter()
        .try_for_each(|position| writer.write_all(&position.to_le_bytes()))
        .map_err(io)
}

/// Reads back a permutation [`write_permutation`] wrote, failing unless its
/// count matches `expected_len` — the set's own cardinality.
fn read_permutation(expected_len: usize, reader: &mut impl Read) -> foyer::Result<Vec<u32>> {
    let count = usize::try_from(read_u64(reader)?).unwrap_or(usize::MAX);
    if count != expected_len {
        return Err(foyer::Error::new(
            foyer::ErrorKind::Parse,
            format!("permutation length {count} does not match set cardinality {expected_len}"),
        ));
    }

    let mut buffer = [0; 4];
    (0..count)
        .map(|_| {
            reader
                .read_exact(&mut buffer)
                .map_err(foyer::Error::io_error)?;
            Ok(u32::from_le_bytes(buffer))
        })
        .collect()
}

/// Reads back the body [`encode_positioned_row_set`] wrote, given the
/// consumed tag.
fn decode_positioned_row_set(tag: u8, reader: &mut impl Read) -> foyer::Result<PositionedRowSet> {
    match tag {
        tag::RANGE_ASCENDING => Ok(PositionedRowSet {
            rows: decode_row_set(RowSetShape::Range, reader)?,
            order: RowOrder::Ascending,
        }),
        tag::ROARING_ASCENDING => Ok(PositionedRowSet {
            rows: decode_row_set(RowSetShape::Roaring, reader)?,
            order: RowOrder::Ascending,
        }),
        tag::SORTED_ASCENDING => Ok(PositionedRowSet {
            rows: decode_row_set(RowSetShape::Sorted, reader)?,
            order: RowOrder::Ascending,
        }),
        tag::ROARING_PERMUTED => {
            let bitmap = read_roaring_body(reader)?;
            // On a 32-bit target a cardinality that overflows `usize`
            // clamps to `usize::MAX`, which can never equal a real
            // permutation length: `read_permutation`'s length check fails
            // closed rather than wrapping or truncating.
            let expected_len = usize::try_from(bitmap.len()).unwrap_or(usize::MAX);
            let permutation = read_permutation(expected_len, reader)?;
            Ok(PositionedRowSet {
                rows: FileRowSet::Roaring(bitmap),
                order: RowOrder::Permuted(permutation),
            })
        }
        tag::SORTED_PERMUTED => {
            let row_ids = read_sorted_body(reader)?;
            let permutation = read_permutation(row_ids.len(), reader)?;
            Ok(PositionedRowSet {
                rows: FileRowSet::Sorted(row_ids),
                order: RowOrder::Permuted(permutation),
            })
        }
        tag::RANGE | tag::LEGACY_ROARING | tag::LEGACY_SORTED => Err(foyer::Error::new(
            foyer::ErrorKind::Parse,
            "a row summary predates positions and cannot answer them",
        )),
        other => Err(foyer::Error::new(
            foyer::ErrorKind::Parse,
            format!("unknown row-summary tag {other}"),
        )),
    }
}

fn parse_failed(error: impl std::error::Error + Send + Sync + 'static) -> foyer::Error {
    foyer::Error::new(foyer::ErrorKind::Parse, "auxiliary cache value").with_source(error)
}

fn read_u64(reader: &mut impl Read) -> foyer::Result<u64> {
    let mut buffer = [0; 8];
    reader
        .read_exact(&mut buffer)
        .map_err(foyer::Error::io_error)?;
    Ok(u64::from_le_bytes(buffer))
}

impl foyer::Code for Weighed {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        let io = foyer::Error::io_error;
        match &self.value {
            AuxiliaryValue::Metadata(metadata) => {
                writer.write_all(&[tag::METADATA]).map_err(io)?;
                let mut buffer = Vec::new();
                ParquetMetaDataWriter::new(&mut buffer, metadata)
                    .finish()
                    .map_err(parse_failed)?;
                writer.write_all(&buffer).map_err(io)
            }
            AuxiliaryValue::Summary(positioned) => encode_positioned_row_set(positioned, writer),
            AuxiliaryValue::DeletePositions(positions) => {
                encode_row_set(positions, tag::DELETE_SHAPES, writer)
            }
            AuxiliaryValue::Block(bytes) => {
                writer.write_all(&[tag::BLOCK]).map_err(io)?;
                writer.write_all(bytes).map_err(io)
            }
        }
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let io = foyer::Error::io_error;
        let mut tag = [0];
        reader.read_exact(&mut tag).map_err(io)?;

        let value = match tag[0] {
            tag::METADATA => {
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).map_err(io)?;
                let metadata = ParquetMetaDataReader::new()
                    .with_page_index_policy(PageIndexPolicy::Optional)
                    .parse_and_finish(&Bytes::from(buffer))
                    .map_err(parse_failed)?;
                AuxiliaryValue::Metadata(Arc::new(metadata))
            }
            tag::RANGE
            | tag::LEGACY_ROARING
            | tag::LEGACY_SORTED
            | tag::ROARING_ASCENDING
            | tag::SORTED_ASCENDING
            | tag::ROARING_PERMUTED
            | tag::SORTED_PERMUTED
            | tag::RANGE_ASCENDING => {
                AuxiliaryValue::Summary(Arc::new(decode_positioned_row_set(tag[0], reader)?))
            }
            tag::DELETE_RANGE => AuxiliaryValue::DeletePositions(Arc::new(decode_row_set(
                RowSetShape::Range,
                reader,
            )?)),
            tag::DELETE_ROARING => AuxiliaryValue::DeletePositions(Arc::new(decode_row_set(
                RowSetShape::Roaring,
                reader,
            )?)),
            tag::DELETE_SORTED => AuxiliaryValue::DeletePositions(Arc::new(decode_row_set(
                RowSetShape::Sorted,
                reader,
            )?)),
            tag::BLOCK => {
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).map_err(io)?;
                AuxiliaryValue::Block(Bytes::from(buffer))
            }
            other => {
                return Err(foyer::Error::new(
                    foyer::ErrorKind::Parse,
                    format!("unknown auxiliary cache tag {other}"),
                ));
            }
        };

        Ok(Self::from(value))
    }

    fn estimated_size(&self) -> usize {
        self.bytes
    }
}

/// Resident row summaries by shape, and their bytes together. Counted up
/// on insertion and down as entries leave, whatever the reason.
#[derive(Default)]
struct RowSummaryCounters {
    range: AtomicU64,
    roaring: AtomicU64,
    sorted: AtomicU64,
    bytes: AtomicU64,
}

impl RowSummaryCounters {
    fn of(&self, kind: FileRowSetKind) -> &AtomicU64 {
        match kind {
            FileRowSetKind::Range => &self.range,
            FileRowSetKind::Roaring => &self.roaring,
            FileRowSetKind::Sorted => &self.sorted,
        }
    }

    fn entered(&self, weighed: &Weighed) {
        if let AuxiliaryValue::Summary(positioned) = &weighed.value {
            self.of(positioned.rows.kind())
                .fetch_add(1, Ordering::Relaxed);
            self.bytes
                .fetch_add(usize_as_u64(weighed.bytes), Ordering::Relaxed);
        }
    }

    fn left(&self, weighed: &Weighed) {
        if let AuxiliaryValue::Summary(positioned) = &weighed.value {
            saturating_decrement(self.of(positioned.rows.kind()), 1);
            saturating_decrement(&self.bytes, usize_as_u64(weighed.bytes));
        }
    }

    fn occupancy(&self) -> RowSummaryOccupancy {
        RowSummaryOccupancy {
            range: self.range.load(Ordering::Relaxed),
            roaring: self.roaring.load(Ordering::Relaxed),
            sorted: self.sorted.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

fn saturating_decrement(counter: &AtomicU64, by: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(by))
    });
}

type MemoryTier = Cache<AuxiliaryKey, Weighed>;
type HybridTier = HybridCache<AuxiliaryKey, Weighed>;

/// The cache in whichever tiering the process configured. Only the memory
/// tier is resized; a disk device keeps the capacity it opened with.
enum Tier {
    Memory(MemoryTier),
    Hybrid(HybridTier),
}

impl Tier {
    fn usage(&self) -> usize {
        match self {
            Self::Memory(cache) => cache.usage(),
            Self::Hybrid(cache) => cache.memory().usage(),
        }
    }

    fn resize(&self, capacity: usize) -> std::result::Result<(), foyer::Error> {
        match self {
            Self::Memory(cache) => cache.resize(capacity),
            Self::Hybrid(cache) => cache.memory().resize(capacity),
        }
    }

    async fn get(&self, key: &AuxiliaryKey) -> Option<Weighed> {
        match self {
            Self::Memory(cache) => cache.get(key).map(|entry| entry.value().clone()),
            Self::Hybrid(cache) => match cache.get(key).await {
                Ok(entry) => entry.map(|entry| entry.value().clone()),
                Err(error) => {
                    warn!(%error, "an auxiliary cache read failed; treating it as a miss");
                    None
                }
            },
        }
    }

    async fn get_or_fetch<F, Fut, E>(
        &self,
        key: &AuxiliaryKey,
        fill: F,
    ) -> std::result::Result<Weighed, foyer::Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = std::result::Result<Weighed, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Self::Memory(cache) => cache
                .get_or_fetch(key, fill)
                .await
                .map(|entry| entry.value().clone()),
            Self::Hybrid(cache) => cache
                .get_or_fetch(key, fill)
                .await
                .map(|entry| entry.value().clone()),
        }
    }

    #[cfg(test)]
    fn insert(&self, key: AuxiliaryKey, value: Weighed) {
        match self {
            Self::Memory(cache) => {
                cache.insert(key, value);
            }
            Self::Hybrid(cache) => {
                cache.insert(key, value);
            }
        }
    }

    /// Admits a re-fetchable block, hinted so it leaves before the
    /// metadata sharing this allowance.
    fn insert_block(&self, key: AuxiliaryKey, value: Weighed) {
        match self {
            Self::Memory(cache) => {
                cache.insert_with_properties(
                    key,
                    value,
                    foyer::CacheProperties::default().with_hint(foyer::Hint::Low),
                );
            }
            Self::Hybrid(cache) => {
                cache.insert_with_properties(
                    key,
                    value,
                    foyer::HybridCacheProperties::default().with_hint(foyer::Hint::Low),
                );
            }
        }
    }
}

/// One weighted cache over both footers and summaries.
pub(super) struct AuxiliaryCache {
    tier: Tier,
    capacity: Arc<AtomicUsize>,
    summaries: Arc<RowSummaryCounters>,
}

/// The parts of a build both tiers share.
struct Parts {
    admitted: Arc<AtomicUsize>,
    summaries: Arc<RowSummaryCounters>,
    listener: Arc<dyn foyer::EventListener<Key = AuxiliaryKey, Value = Weighed>>,
}

impl Parts {
    fn new(capacity: usize) -> Self {
        let summaries = Arc::new(RowSummaryCounters::default());
        Self {
            admitted: Arc::new(AtomicUsize::new(capacity)),
            listener: Arc::new(EvictionCounter {
                summaries: Arc::clone(&summaries),
            }),
            summaries,
        }
    }

    /// Refuses a value the allowance could never hold.
    fn filter(&self) -> impl Fn(&AuxiliaryKey, &Weighed) -> bool + Send + Sync + 'static {
        let limit = Arc::clone(&self.admitted);
        move |_: &AuxiliaryKey, value: &Weighed| {
            let limit = limit.load(Ordering::Relaxed);
            limit > 0 && value.bytes <= limit
        }
    }
}

impl AuxiliaryCache {
    pub(super) fn new(capacity: usize) -> Self {
        let parts = Parts::new(capacity);

        // One shard: foyer splits capacity across shards, and a footer must
        // fit within one.
        // LRU is the policy that honours the hint separating re-fetchable
        // blocks from metadata.
        let cache = foyer::CacheBuilder::new(capacity)
            .with_shards(1)
            .with_eviction_config(cache::eviction_config())
            .with_weighter(|_: &AuxiliaryKey, value: &Weighed| value.bytes)
            .with_filter(parts.filter())
            .with_event_listener(Arc::clone(&parts.listener))
            .build();

        Self {
            tier: Tier::Memory(cache),
            capacity: parts.admitted,
            summaries: parts.summaries,
        }
    }

    /// A cache of `capacity` bytes over a disk device of `disk` bytes at
    /// `dir`; `None` if the device will not open.
    pub(super) async fn hybrid(capacity: usize, dir: &FilePath, disk: u64) -> Option<Self> {
        let parts = Parts::new(capacity);
        let memory = foyer::HybridCacheBuilder::new()
            .with_event_listener(Arc::clone(&parts.listener))
            .with_policy(foyer::HybridCachePolicy::WriteOnInsertion)
            .memory(capacity)
            .with_shards(1)
            .with_eviction_config(cache::eviction_config())
            .with_weighter(|_: &AuxiliaryKey, value: &Weighed| value.bytes)
            .with_filter(parts.filter());

        Some(Self {
            tier: Tier::Hybrid(cache::disk_tier(memory, dir, disk).await?),
            capacity: parts.admitted,
            summaries: parts.summaries,
        })
    }

    /// This file's parsed footer, loading it through `reader` on a miss.
    /// Concurrent misses on one key share the single fill.
    pub(super) async fn metadata(
        &self,
        reader: &ObjectStoreReader,
        prefetch: Option<usize>,
    ) -> ParquetResult<Arc<ParquetMetaData>> {
        let key = AuxiliaryKey::Metadata {
            store: reader.file.store.identity,
            path: reader.file.path.to_string(),
            file_size: reader.file.file_size,
            page_index: reader.page_index.into(),
        };

        if let Some(entry) = self.tier.get(&key).await {
            reader.file.metrics.metadata_hit();
            return metadata_of(&entry.value);
        }
        reader.file.metrics.metadata_miss();

        let mut loader = reader.clone();
        let file_size = reader.file.file_size;
        let page_index = reader.page_index;
        let fill = move || async move {
            ParquetMetaDataReader::new()
                .with_page_index_policy(page_index)
                .with_prefetch_hint(prefetch)
                .load_and_finish(&mut loader, file_size)
                .await
                .map(|metadata| Weighed::from(AuxiliaryValue::Metadata(Arc::new(metadata))))
        };

        let entry = self
            .tier
            .get_or_fetch(&key, fill)
            .await
            .map_err(|error| ParquetError::General(error.to_string()))?;

        metadata_of(&entry.value)
    }

    pub(super) async fn summary(
        &self,
        store: &DataStore,
        key: &FileSummaryKey<'_>,
    ) -> Option<Arc<PositionedRowSet>> {
        summary_of(
            &self
                .tier
                .get(&Self::file_summary_key(store, key))
                .await?
                .value,
        )
    }

    /// This file's row-id summary, building it through `fill` on a miss.
    /// Concurrent misses on one key share the single fill.
    pub(super) async fn fetch_summary<F, Fut>(
        &self,
        store: &DataStore,
        key: &FileSummaryKey<'_>,
        fill: F,
    ) -> Result<Arc<PositionedRowSet>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Arc<PositionedRowSet>>> + Send + 'static,
    {
        let fill = fill();
        let summaries = Arc::clone(&self.summaries);
        let fill = || async move {
            fill.await.map(|positioned| {
                let weighed = Weighed::from(AuxiliaryValue::Summary(positioned));
                summaries.entered(&weighed);
                weighed
            })
        };

        let entry = self
            .tier
            .get_or_fetch(&Self::file_summary_key(store, key), fill)
            .await
            .map_err(|error| {
                let cause = std::error::Error::source(&error)
                    .map_or_else(|| error.to_string(), ToString::to_string);
                Error::Corruption(format!("row summary: {cause}"))
            })?;

        summary_of(&entry.value).ok_or_else(|| {
            Error::Corruption("a footer was cached under a row summary key".to_owned())
        })
    }

    /// One delete file's positions, decoding them through `decode` on a
    /// miss. Concurrent misses on one object share the single decode.
    ///
    /// A delete file is immutable and the commit path reads the same one
    /// again on every later delete against its target, so the decode is
    /// worth keeping even though the bytes it decodes are cached already.
    pub(super) async fn delete_positions<F, Fut>(
        &self,
        file: &ParquetFile,
        decode: F,
    ) -> Result<Arc<FileRowSet>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Arc<FileRowSet>>> + Send + 'static,
    {
        let pending = decode();
        let fetch = || async move {
            pending
                .await
                .map(|positions| Weighed::from(AuxiliaryValue::DeletePositions(positions)))
        };

        let entry = self
            .tier
            .get_or_fetch(&Self::delete_positions_key(file), fetch)
            .await
            .map_err(|error| {
                let cause = std::error::Error::source(&error)
                    .map_or_else(|| error.to_string(), ToString::to_string);
                Error::Corruption(format!("delete-file positions: {cause}"))
            })?;

        match &entry.value {
            AuxiliaryValue::DeletePositions(positions) => Ok(Arc::clone(positions)),
            _ => Err(Error::Corruption(
                "another value was cached under a delete-position key".to_owned(),
            )),
        }
    }

    #[cfg(test)]
    pub(super) fn insert_summary(
        &self,
        store: &DataStore,
        key: &FileSummaryKey<'_>,
        positioned: &Arc<PositionedRowSet>,
    ) {
        let weighed = Weighed::from(AuxiliaryValue::Summary(Arc::clone(positioned)));
        self.summaries.entered(&weighed);
        self.tier
            .insert(Self::file_summary_key(store, key), weighed);
    }

    /// The file's bytes over `range`, from the cache when that exact range
    /// is resident and from the store otherwise.
    ///
    /// Keyed by the range as asked for, not by a fixed window: a selective
    /// read fetches the pages it selected and no more, which is the
    /// property the row selection exists to buy. Repeat reads of a file
    /// ask for the same pages, so they still hit.
    pub(super) async fn range(
        &self,
        file: &ParquetFile,
        range: std::ops::Range<u64>,
    ) -> ParquetResult<Bytes> {
        if let Some(bytes) = self.cached_range(file, &range).await {
            return Ok(bytes);
        }

        let started = Instant::now();
        let fetched = file
            .store
            .read_range(&file.path, range.clone())
            .await
            .map_err(|error| ParquetError::External(Box::new(error)))?;
        file.metrics
            .range_read(1, usize_as_u64(fetched.len()), started.elapsed());
        self.admit_range(file, &range, &fetched);
        Ok(fetched)
    }

    /// [`Self::range`] over several ranges, fetching those that miss in one
    /// request so the store can still coalesce adjacent chunks.
    pub(super) async fn ranges(
        &self,
        file: &ParquetFile,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> ParquetResult<Vec<Bytes>> {
        let mut out: Vec<Option<Bytes>> = Vec::with_capacity(ranges.len());
        let mut missing = Vec::new();
        for range in &ranges {
            let cached = self.cached_range(file, range).await;
            if cached.is_none() {
                missing.push(range.clone());
            }
            out.push(cached);
        }

        if !missing.is_empty() {
            let started = Instant::now();
            let fetched = file
                .store
                .read_ranges(&file.path, &missing)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            let total = fetched.iter().fold(0_u64, |sum, bytes| {
                sum.saturating_add(usize_as_u64(bytes.len()))
            });
            file.metrics
                .range_read(missing.len(), total, started.elapsed());

            let mut filled = missing.iter().zip(fetched);
            for (slot, range) in out.iter_mut().zip(&ranges) {
                if slot.is_some() {
                    continue;
                }
                let Some((_, bytes)) = filled.next() else {
                    return Err(ParquetError::General(format!(
                        "the store returned fewer ranges than {} asked for",
                        file.path
                    )));
                };
                self.admit_range(file, range, &bytes);
                *slot = Some(bytes);
            }
        }

        out.into_iter()
            .map(|bytes| {
                bytes.ok_or_else(|| {
                    ParquetError::General(format!("a range of {} went unfilled", file.path))
                })
            })
            .collect()
    }

    async fn cached_range(
        &self,
        file: &ParquetFile,
        range: &std::ops::Range<u64>,
    ) -> Option<Bytes> {
        match self.tier.get(&Self::range_key(file, range)).await?.value {
            AuxiliaryValue::Block(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// Memoizes `bytes` as the file's content over `range`, unless they
    /// fall short of filling it: a read that stopped early would otherwise
    /// be served to every later reader of that range.
    fn admit_range(&self, file: &ParquetFile, range: &std::ops::Range<u64>, bytes: &Bytes) {
        if usize_as_u64(bytes.len()) != range.end.saturating_sub(range.start) {
            return;
        }

        self.tier.insert_block(
            Self::range_key(file, range),
            Weighed::from(AuxiliaryValue::Block(bytes.clone())),
        );
    }

    fn range_key(file: &ParquetFile, range: &std::ops::Range<u64>) -> AuxiliaryKey {
        AuxiliaryKey::Range {
            store: file.store.identity,
            path: file.path.to_string(),
            file_size: file.file_size,
            start: range.start,
            end: range.end,
        }
    }

    fn delete_positions_key(file: &ParquetFile) -> AuxiliaryKey {
        AuxiliaryKey::DeletePositions {
            store: file.store.identity,
            path: file.path.to_string(),
            file_size: file.file_size,
        }
    }

    fn file_summary_key(store: &DataStore, key: &FileSummaryKey<'_>) -> AuxiliaryKey {
        AuxiliaryKey::Summary {
            store: store.identity,
            table_id: key.table_id,
            data_file_id: key.data_file_id,
            path: key.path.to_string(),
            file_size: key.file_size,
        }
    }

    /// Settles the allowance, evicting whatever no longer fits. A refusal
    /// leaves the previous capacity in force.
    pub(super) fn resize(&self, capacity: usize) {
        match self.tier.resize(capacity) {
            Ok(()) => {
                self.capacity.store(capacity, Ordering::Relaxed);
            }
            Err(error) => warn!(
                capacity,
                %error,
                "could not resize the auxiliary cache; its previous capacity stands"
            ),
        }
    }

    /// Tracked here because foyer's `Cache::capacity` does not follow
    /// `resize`.
    pub(super) fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    pub(super) fn usage(&self) -> usize {
        self.tier.usage()
    }

    pub(super) fn row_summaries(&self) -> RowSummaryOccupancy {
        self.summaries.occupancy()
    }
}

fn summary_of(value: &AuxiliaryValue) -> Option<Arc<PositionedRowSet>> {
    match value {
        AuxiliaryValue::Summary(positioned) => Some(Arc::clone(positioned)),
        AuxiliaryValue::Metadata(_)
        | AuxiliaryValue::Block(_)
        | AuxiliaryValue::DeletePositions(_) => None,
    }
}

fn metadata_of(value: &AuxiliaryValue) -> ParquetResult<Arc<ParquetMetaData>> {
    match value {
        AuxiliaryValue::Metadata(metadata) => Ok(Arc::clone(metadata)),
        AuxiliaryValue::Summary(_)
        | AuxiliaryValue::Block(_)
        | AuxiliaryValue::DeletePositions(_) => Err(ParquetError::General(
            "a row set or block was cached under a metadata key".to_owned(),
        )),
    }
}

struct EvictionCounter {
    summaries: Arc<RowSummaryCounters>,
}

impl foyer::EventListener for EvictionCounter {
    type Key = AuxiliaryKey;
    type Value = Weighed;

    fn on_leave(&self, reason: foyer::Event, _: &AuxiliaryKey, value: &Weighed) {
        self.summaries.left(value);
        if reason == foyer::Event::Evict {
            cache::auxiliary_evicted();
        }
    }
}

/// The process's cache: the one the first attach installs, or a memory
/// cache at the default allowance if a read comes first.
static SHARED: OnceLock<AuxiliaryCache> = OnceLock::new();

pub(super) fn shared() -> &'static AuxiliaryCache {
    SHARED.get_or_init(|| AuxiliaryCache::new(cache::auxiliary_metadata_memory()))
}

/// The allowance's current size and occupancy, for the process's cache
/// report.
pub(crate) fn occupancy() -> (u64, u64) {
    let cache = shared();
    (usize_as_u64(cache.capacity()), usize_as_u64(cache.usage()))
}

/// The resident row summaries by shape, for the process's cache report.
pub(crate) fn row_summary_occupancy() -> RowSummaryOccupancy {
    shared().row_summaries()
}

/// Installs the cache the first attach sized: over a disk device of `disk`
/// bytes at `dir` when given, else in memory. A cache a read already built
/// is resized instead and stays in memory.
pub(crate) async fn install(capacity: usize, dir: Option<PathBuf>, disk: u64) {
    if let Some(existing) = SHARED.get() {
        existing.resize(capacity);
        return;
    }

    let built = match dir {
        Some(dir) => AuxiliaryCache::hybrid(capacity, &dir, disk).await,
        None => None,
    };
    let built = built.unwrap_or_else(|| AuxiliaryCache::new(capacity));

    if let Err(built) = SHARED.set(built) {
        drop(built);
        shared().resize(capacity);
    }
}
