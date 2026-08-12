//! Scoped Parquet read: the extension path derives index entries by
//! reading only the indexed columns, the row positions, and — when the
//! file carries one — the row-id column of a registered data file. A
//! bounded, merge-free projection, not the scan path the no-Parquet-read
//! rule guards.

use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, LazyLock, Mutex, Weak},
};

use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    },
    buffer::Buffer,
    datatypes::{DataType, Schema, SchemaRef, TimeUnit},
    ipc::{
        reader::{StreamReader, read_record_batch},
        root_as_message,
    },
};
use bytes::Bytes;
use futures::{
    StreamExt,
    future::{BoxFuture, FutureExt},
};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection},
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
};

use crate::{
    error::{Error, Result},
    store::{
        cache,
        index_encoding::{
            BorrowedIndexKeyValue, CanonicalKeyBuilder, Direction, IndexKeyValue, IntWidth,
            NullOrder,
        },
    },
};

/// One entry derived from a registered file: the row id and the canonical
/// values of the indexed columns, in the index's column order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ScopedReadEntry {
    /// The row this entry points at.
    pub(crate) row_id: u64,
    /// The indexed column values; a `None` is SQL NULL (stored multi-shaped).
    pub(crate) values: Vec<Option<IndexKeyValue>>,
}

/// One index's projection over an Arrow batch. Positions name Arrow columns
/// in index-column order; ordering vectors run in parallel to them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexProjection {
    /// Persistent id used in the physical entry prefix.
    pub(crate) index_id: u64,
    /// Whether non-NULL values use the unique entry shape.
    pub(crate) unique: bool,
    /// Arrow column positions in index-column order.
    pub(crate) positions: Vec<usize>,
    /// Per-column sort directions.
    pub(crate) directions: Vec<Direction>,
    /// Per-column NULL placement.
    pub(crate) nulls: Vec<NullOrder>,
}

/// A final physical index key derived directly from an Arrow row.
pub(crate) struct ScopedIndexEntry {
    /// Position of the owning index in the supplied projection plans.
    pub(crate) index: usize,
    /// Stable row id carried by the entry.
    pub(crate) row_id: u64,
    /// Fully encoded SlateDB entry key.
    pub(crate) key: Bytes,
    /// Whether the key uses the unique physical shape.
    pub(crate) unique: bool,
}

/// DuckDB's reserved Parquet field id tagging the embedded row-id column
/// (`_ducklake_internal_row_id`) that rewrite and flush files carry.
const ROW_ID_FIELD_ID: i32 = 2_147_483_540;

/// How a scoped read resolves each row's id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowIdSource {
    /// The embedded row-id column when the file carries one — preferred
    /// even over a recorded dense start, since flushed files carry both
    /// and their embedded ids may hold gaps — else `start + ordinal`,
    /// else refusal.
    Resolve {
        /// The catalog row's dense start, if it records one.
        row_id_start: Option<u64>,
    },
    /// Row ids are file ordinals from 0. A file carrying the embedded
    /// column is refused: its rows already have ids, and renumbering
    /// them would fork the file's identity.
    Ordinal,
}

/// Which of a file's rows a scoped read decodes.
///
/// [`At`](Self::At) becomes a Parquet row selection, so deriving the entries
/// of a few rows out of a large file costs those rows rather than the file —
/// the difference between a delete that pays for what it deletes and one
/// that pays for the table.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ScopedRows<'a> {
    /// Every row, in file order.
    All,
    /// Only the rows at these physical positions. Entries come back in
    /// position order, and the set's ordering is what lets the selection be
    /// built without sorting.
    At(&'a RowPositions),
}

impl<'a> ScopedRows<'a> {
    /// The positions to read, or `None` when that is all of them.
    fn positions(self) -> Option<&'a [u64]> {
        match self {
            Self::All => None,
            Self::At(wanted) => Some(wanted.as_slice()),
        }
    }

    /// Whether the read can be answered without touching the store.
    fn is_empty(self) -> bool {
        matches!(self, Self::At(wanted) if wanted.is_empty())
    }

    /// A selection reads the page index so it can skip whole pages; a full
    /// read has nothing to skip and does not pay for it.
    fn page_index_policy(self) -> PageIndexPolicy {
        match self {
            Self::All => PageIndexPolicy::Skip,
            Self::At(_) => PageIndexPolicy::Optional,
        }
    }
}

/// Physical Parquet row positions, sorted and duplicate-free. Every selected
/// read shares this representation, so constructing a row selection never
/// repeats a tree-to-vector copy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RowPositions(Vec<u64>);

impl RowPositions {
    /// Establishes the sorted/unique invariant for positions collected from
    /// one or more delete sources.
    pub(crate) fn from_unsorted(mut positions: Vec<u64>) -> Self {
        positions.sort_unstable();
        positions.dedup();
        Self(positions)
    }

    /// The ordered positions used by Parquet row selection and ordinal
    /// resolution.
    pub(crate) fn as_slice(&self) -> &[u64] {
        &self.0
    }

    /// Whether no physical row is selected.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether the selected set contains `position`.
    pub(crate) fn contains(&self, position: u64) -> bool {
        self.0.binary_search(&position).is_ok()
    }
}

impl FromIterator<u64> for RowPositions {
    fn from_iter<T: IntoIterator<Item = u64>>(iter: T) -> Self {
        Self::from_unsorted(iter.into_iter().collect())
    }
}

/// Maps the nth row a read emits to the file ordinal it sits at.
#[derive(Debug, Clone, Copy)]
enum Ordinals<'a> {
    /// Every row was read, so the nth emitted row is the nth row.
    Dense,
    /// A full-file read resumed at this physical position.
    Offset(u64),
    /// Only these positions were read, in this order.
    Selected(&'a [u64]),
}

impl Ordinals<'_> {
    /// The file ordinal of the `nth` row emitted.
    fn at(self, nth: usize) -> Result<u64> {
        match self {
            Self::Dense => Ok(usize_as_u64(nth)),
            Self::Offset(start) => Ok(start.saturating_add(usize_as_u64(nth))),
            Self::Selected(positions) => positions.get(nth).copied().ok_or_else(|| {
                Error::Corruption(
                    "scoped read: the reader emitted more rows than the selection named".to_owned(),
                )
            }),
        }
    }
}

/// The row selection naming `positions` in a file of `total_rows`.
///
/// Positions arrive ascending and unique (they come from [`RowPositions`]) and
/// are bounds-checked here, so the consecutive ranges handed over are
/// ordered and in range — the two things the builder requires of them.
fn row_selection(positions: &[u64], total_rows: usize) -> Result<RowSelection> {
    let mut ranges = Vec::with_capacity(positions.len());
    for &position in positions {
        let start = usize::try_from(position)
            .ok()
            .filter(|start| *start < total_rows)
            .ok_or_else(|| {
                Error::Corruption(format!(
                    "scoped read: selected row {position} is past the file's {total_rows} rows"
                ))
            })?;
        ranges.push(start..start.saturating_add(1));
    }
    Ok(RowSelection::from_consecutive_ranges(
        ranges.into_iter(),
        total_rows,
    ))
}

/// A file's row count, as the selection builder counts it.
fn total_rows(metadata: &ParquetMetaData, path: &Path) -> Result<usize> {
    usize::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| Error::Corruption(format!("scoped read: {path} reports a negative row count")))
}

/// The root position of the embedded row-id column, if the file has one.
fn embedded_row_id_position(schema: &parquet::schema::types::SchemaDescriptor) -> Option<usize> {
    schema.root_schema().get_fields().iter().position(|field| {
        let info = field.get_basic_info();
        info.has_id() && info.id() == ROW_ID_FIELD_ID
    })
}

/// Resolves `source` against the file's schema into the projection
/// inputs: the row-id column's position (when it is to be read) and the
/// dense start.
fn resolve_row_id_source(
    schema: &parquet::schema::types::SchemaDescriptor,
    source: RowIdSource,
    path: &Path,
) -> Result<(Option<usize>, u64)> {
    match (source, embedded_row_id_position(schema)) {
        (RowIdSource::Ordinal, Some(_)) => Err(Error::Constraint(format!(
            "scoped read: {path} carries an embedded row-id column and cannot be read by ordinal"
        ))),
        (RowIdSource::Ordinal, None) => Ok((None, 0)),
        (RowIdSource::Resolve { .. }, Some(position)) => Ok((Some(position), 0)),
        (
            RowIdSource::Resolve {
                row_id_start: Some(start),
            },
            None,
        ) => Ok((None, start)),
        (RowIdSource::Resolve { row_id_start: None }, None) => Err(Error::Corruption(format!(
            "scoped read: {path} is recorded as carrying per-row ids but has no embedded \
             row-id column"
        ))),
    }
}

fn downcast<A: 'static>(array: &dyn Array) -> Result<&A> {
    array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| Error::Corruption("scoped read: parquet column type mismatch".to_owned()))
}

/// The canonical value of `array` at `row`, or `None` for NULL.
fn array_value(array: &dyn Array, row: usize) -> Result<Option<IndexKeyValue>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let signed = |value: i128, width| IndexKeyValue::Int { value, width };
    let unsigned = |value: u128, width| IndexKeyValue::UInt { value, width };
    let value = match array.data_type() {
        DataType::Int8 => signed(
            i128::from(downcast::<Int8Array>(array)?.value(row)),
            IntWidth::I8,
        ),
        DataType::Int16 => signed(
            i128::from(downcast::<Int16Array>(array)?.value(row)),
            IntWidth::I16,
        ),
        DataType::Int32 => signed(
            i128::from(downcast::<Int32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::Int64 => signed(
            i128::from(downcast::<Int64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::UInt8 => unsigned(
            u128::from(downcast::<UInt8Array>(array)?.value(row)),
            IntWidth::I8,
        ),
        DataType::UInt16 => unsigned(
            u128::from(downcast::<UInt16Array>(array)?.value(row)),
            IntWidth::I16,
        ),
        DataType::UInt32 => unsigned(
            u128::from(downcast::<UInt32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::UInt64 => unsigned(
            u128::from(downcast::<UInt64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Float32 => IndexKeyValue::F32(downcast::<Float32Array>(array)?.value(row)),
        DataType::Float64 => IndexKeyValue::F64(downcast::<Float64Array>(array)?.value(row)),
        DataType::Boolean => IndexKeyValue::Bool(downcast::<BooleanArray>(array)?.value(row)),
        DataType::Utf8 => IndexKeyValue::Str(downcast::<StringArray>(array)?.value(row).to_owned()),
        DataType::LargeUtf8 => {
            IndexKeyValue::Str(downcast::<LargeStringArray>(array)?.value(row).to_owned())
        }
        DataType::LargeBinary => {
            IndexKeyValue::Bytes(downcast::<LargeBinaryArray>(array)?.value(row).to_vec())
        }
        DataType::Binary => {
            IndexKeyValue::Bytes(downcast::<BinaryArray>(array)?.value(row).to_vec())
        }
        // Fixed-width blobs, e.g. a 16-byte `UUID`.
        DataType::FixedSizeBinary(_) => {
            IndexKeyValue::Bytes(downcast::<FixedSizeBinaryArray>(array)?.value(row).to_vec())
        }
        // Temporal types index by their underlying integer representation.
        DataType::Date32 => signed(
            i128::from(downcast::<Date32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::Date64 => signed(
            i128::from(downcast::<Date64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        // Each timestamp width indexes by its own underlying `i64` count —
        // seconds, milli-, micro-, or nanoseconds — read from the array of
        // the matching unit. The count is the same on the inline path (whose
        // schema carries the same unit), so the two paths agree.
        DataType::Timestamp(TimeUnit::Second, _) => signed(
            i128::from(downcast::<TimestampSecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => signed(
            i128::from(downcast::<TimestampMillisecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => signed(
            i128::from(downcast::<TimestampMicrosecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => signed(
            i128::from(downcast::<TimestampNanosecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        other => {
            return Err(Error::Constraint(format!(
                "scoped read: column type {other:?} is not indexable"
            )));
        }
    };
    Ok(Some(value))
}

/// Borrows one Arrow scalar for immediate canonical-key construction.
fn borrowed_array_value(
    array: &dyn Array,
    row: usize,
) -> Result<Option<BorrowedIndexKeyValue<'_>>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let signed = |value: i128, width| BorrowedIndexKeyValue::Int { value, width };
    let unsigned = |value: u128, width| BorrowedIndexKeyValue::UInt { value, width };
    let value = match array.data_type() {
        DataType::Int8 => signed(
            i128::from(downcast::<Int8Array>(array)?.value(row)),
            IntWidth::I8,
        ),
        DataType::Int16 => signed(
            i128::from(downcast::<Int16Array>(array)?.value(row)),
            IntWidth::I16,
        ),
        DataType::Int32 => signed(
            i128::from(downcast::<Int32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::Int64 => signed(
            i128::from(downcast::<Int64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::UInt8 => unsigned(
            u128::from(downcast::<UInt8Array>(array)?.value(row)),
            IntWidth::I8,
        ),
        DataType::UInt16 => unsigned(
            u128::from(downcast::<UInt16Array>(array)?.value(row)),
            IntWidth::I16,
        ),
        DataType::UInt32 => unsigned(
            u128::from(downcast::<UInt32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::UInt64 => unsigned(
            u128::from(downcast::<UInt64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Float32 => {
            BorrowedIndexKeyValue::F32(downcast::<Float32Array>(array)?.value(row))
        }
        DataType::Float64 => {
            BorrowedIndexKeyValue::F64(downcast::<Float64Array>(array)?.value(row))
        }
        DataType::Boolean => {
            BorrowedIndexKeyValue::Bool(downcast::<BooleanArray>(array)?.value(row))
        }
        DataType::Utf8 => BorrowedIndexKeyValue::Str(downcast::<StringArray>(array)?.value(row)),
        DataType::LargeUtf8 => {
            BorrowedIndexKeyValue::Str(downcast::<LargeStringArray>(array)?.value(row))
        }
        DataType::LargeBinary => {
            BorrowedIndexKeyValue::Bytes(downcast::<LargeBinaryArray>(array)?.value(row))
        }
        DataType::Binary => {
            BorrowedIndexKeyValue::Bytes(downcast::<BinaryArray>(array)?.value(row))
        }
        DataType::FixedSizeBinary(_) => {
            BorrowedIndexKeyValue::Bytes(downcast::<FixedSizeBinaryArray>(array)?.value(row))
        }
        DataType::Date32 => signed(
            i128::from(downcast::<Date32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::Date64 => signed(
            i128::from(downcast::<Date64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Second, _) => signed(
            i128::from(downcast::<TimestampSecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => signed(
            i128::from(downcast::<TimestampMillisecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => signed(
            i128::from(downcast::<TimestampMicrosecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => signed(
            i128::from(downcast::<TimestampNanosecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        other => {
            return Err(Error::Constraint(format!(
                "scoped read: column type {other:?} is not indexable"
            )));
        }
    };
    Ok(Some(value))
}

/// Reads a row id or row position at `row` as a `u64` (`Int64`/`UInt64`).
fn row_id_value(array: &dyn Array, row: usize) -> Result<u64> {
    if array.is_null(row) {
        return Err(Error::Corruption(
            "scoped read: row-id column holds a NULL".to_owned(),
        ));
    }
    match array.data_type() {
        DataType::Int64 => u64::try_from(downcast::<Int64Array>(array)?.value(row))
            .map_err(|_| Error::Corruption("scoped read: negative row id".to_owned())),
        DataType::UInt64 => Ok(downcast::<UInt64Array>(array)?.value(row)),
        other => Err(Error::Corruption(format!(
            "scoped read: row-id column has non-integer type {other:?}"
        ))),
    }
}

/// An [`AsyncFileReader`] over moraine's own object store: the Parquet
/// footer and the projected column chunks arrive as byte-range reads, so a
/// scoped read never downloads the whole data file. Deliberately not
/// `parquet`'s built-in `object_store` integration, which pins a different
/// `object_store` major than the workspace.
struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    /// The object's total length, required to locate the footer.
    file_size: u64,
    /// The serialized Parquet metadata length recorded by DuckLake. The
    /// trailing length and magic occupy another eight bytes.
    footer_size: u64,
    /// Whether to load the page index with the footer. A row selection
    /// needs it to skip pages; without one it is footer bytes for nothing.
    page_index: PageIndexPolicy,
}

/// Parsed metadata from immutable Parquet objects, bounded by its reported
/// heap footprint. A weak store reference both prevents this process-wide
/// cache from keeping catalogs alive and distinguishes equal paths belonging
/// to different stores.
struct MetadataCacheEntry {
    store: Weak<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    page_index: PageIndexPolicy,
    metadata: Arc<ParquetMetaData>,
    bytes: usize,
}

type MetadataInFlightResult = std::result::Result<Arc<ParquetMetaData>, Arc<str>>;

enum MetadataLookup {
    Cached(Arc<ParquetMetaData>),
    InFlight(MetadataInFlightLease),
}

/// One shared fill for a cache key. The cell owns only the parsed result;
/// readers keep their own file reader and projected-column path.
struct MetadataInFlight {
    store: Weak<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    page_index: PageIndexPolicy,
    result: Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
}

struct MetadataInFlightLease {
    store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    page_index: PageIndexPolicy,
    result: Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
}

impl Drop for MetadataInFlightLease {
    fn drop(&mut self) {
        release_metadata_in_flight(
            &self.store,
            &self.path,
            self.file_size,
            self.page_index,
            &self.result,
        );
    }
}

#[derive(Default)]
struct MetadataCache {
    entries: VecDeque<MetadataCacheEntry>,
    bytes: usize,
    in_flight: Vec<MetadataInFlight>,
}

impl MetadataCache {
    fn lookup(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        file_size: u64,
        page_index: PageIndexPolicy,
    ) -> MetadataLookup {
        match self.get(store, path, file_size, page_index) {
            Some(metadata) => MetadataLookup::Cached(metadata),
            None => MetadataLookup::InFlight(self.in_flight(store, path, file_size, page_index)),
        }
    }

    fn get(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        file_size: u64,
        page_index: PageIndexPolicy,
    ) -> Option<Arc<ParquetMetaData>> {
        self.entries.retain(|entry| entry.store.strong_count() > 0);
        self.bytes = self.entries.iter().map(|entry| entry.bytes).sum();
        let capacity = cache::auxiliary_metadata_memory();
        while self.bytes > capacity {
            let evicted = self.entries.pop_front()?;
            self.bytes = self.bytes.saturating_sub(evicted.bytes);
        }

        let weak = Arc::downgrade(store);
        let position = self.entries.iter().position(|entry| {
            Weak::ptr_eq(&entry.store, &weak)
                && entry.path == *path
                && entry.file_size == file_size
                && entry.page_index == page_index
        })?;
        let entry = self.entries.remove(position)?;
        let metadata = Arc::clone(&entry.metadata);
        self.entries.push_back(entry);
        Some(metadata)
    }

    fn insert(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: Path,
        file_size: u64,
        page_index: PageIndexPolicy,
        metadata: Arc<ParquetMetaData>,
    ) {
        let bytes = metadata.memory_size();
        let capacity = cache::auxiliary_metadata_memory();
        if capacity == 0 || bytes > capacity {
            return;
        }

        let weak = Arc::downgrade(store);
        if let Some(position) = self.entries.iter().position(|entry| {
            Weak::ptr_eq(&entry.store, &weak)
                && entry.path == path
                && entry.file_size == file_size
                && entry.page_index == page_index
        }) && let Some(replaced) = self.entries.remove(position)
        {
            self.bytes = self.bytes.saturating_sub(replaced.bytes);
        }

        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.push_back(MetadataCacheEntry {
            store: weak,
            path,
            file_size,
            page_index,
            metadata,
            bytes,
        });
        while self.bytes > capacity {
            let Some(evicted) = self.entries.pop_front() else {
                self.bytes = 0;
                break;
            };
            self.bytes = self.bytes.saturating_sub(evicted.bytes);
        }
    }

    fn in_flight(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        file_size: u64,
        page_index: PageIndexPolicy,
    ) -> MetadataInFlightLease {
        self.in_flight
            .retain(|in_flight| in_flight.store.strong_count() > 0);
        let weak = Arc::downgrade(store);
        let result = if let Some(in_flight) = self.in_flight.iter().find(|in_flight| {
            Weak::ptr_eq(&in_flight.store, &weak)
                && in_flight.path == *path
                && in_flight.file_size == file_size
                && in_flight.page_index == page_index
        }) {
            Arc::clone(&in_flight.result)
        } else {
            let result = Arc::new(tokio::sync::OnceCell::new());
            self.in_flight.push(MetadataInFlight {
                store: weak,
                path: path.clone(),
                file_size,
                page_index,
                result: Arc::clone(&result),
            });
            result
        };
        MetadataInFlightLease {
            store: Arc::clone(store),
            path: path.clone(),
            file_size,
            page_index,
            result,
        }
    }

    fn finish_in_flight(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        file_size: u64,
        page_index: PageIndexPolicy,
        result: &Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
    ) {
        let weak = Arc::downgrade(store);
        self.in_flight.retain(|in_flight| {
            !(Weak::ptr_eq(&in_flight.store, &weak)
                && in_flight.path == *path
                && in_flight.file_size == file_size
                && in_flight.page_index == page_index
                && Arc::ptr_eq(&in_flight.result, result))
        });
    }

    fn release_in_flight(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        file_size: u64,
        page_index: PageIndexPolicy,
        result: &Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
    ) {
        if result.get().is_some() || Arc::strong_count(result) == 2 {
            self.finish_in_flight(store, path, file_size, page_index, result);
        }
    }
}

static METADATA_CACHE: LazyLock<Mutex<MetadataCache>> =
    LazyLock::new(|| Mutex::new(MetadataCache::default()));

fn metadata_lookup(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    file_size: u64,
    page_index: PageIndexPolicy,
) -> MetadataLookup {
    METADATA_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .lookup(store, path, file_size, page_index)
}

fn cache_metadata(
    store: &Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    page_index: PageIndexPolicy,
    metadata: Arc<ParquetMetaData>,
) {
    METADATA_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(store, path, file_size, page_index, metadata);
}

fn finish_metadata_in_flight(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    file_size: u64,
    page_index: PageIndexPolicy,
    result: &Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
) {
    METADATA_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .finish_in_flight(store, path, file_size, page_index, result);
}

fn release_metadata_in_flight(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    file_size: u64,
    page_index: PageIndexPolicy,
    result: &Arc<tokio::sync::OnceCell<MetadataInFlightResult>>,
) {
    METADATA_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .release_in_flight(store, path, file_size, page_index, result);
}

fn footer_prefetch_size(footer_size: u64, file_size: u64) -> Option<usize> {
    (footer_size > 0)
        .then_some(footer_size)
        .and_then(|size| size.checked_add(8))
        .filter(|size| *size <= file_size)
        .and_then(|size| usize::try_from(size).ok())
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        async move {
            store
                .get_range(&path, range)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        // One `get_ranges` call, so the store can coalesce adjacent chunks.
        async move {
            store
                .get_ranges(&path, &ranges)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let in_flight =
            match metadata_lookup(&self.store, &self.path, self.file_size, self.page_index) {
                MetadataLookup::Cached(metadata) => return async move { Ok(metadata) }.boxed(),
                MetadataLookup::InFlight(in_flight) => in_flight,
            };

        let file_size = self.file_size;
        let prefetch_size = footer_prefetch_size(self.footer_size, file_size);
        let page_index = self.page_index;
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        async move {
            let fill_store = Arc::clone(&store);
            let fill_path = path.clone();
            let result = in_flight
                .result
                .get_or_init(|| async move {
                    let metadata = ParquetMetaDataReader::new()
                        .with_page_index_policy(page_index)
                        .with_prefetch_hint(prefetch_size)
                        .load_and_finish(&mut *self, file_size)
                        .await
                        .map(Arc::new)
                        .map_err(|error| Arc::<str>::from(error.to_string()))?;
                    cache_metadata(
                        &fill_store,
                        fill_path,
                        file_size,
                        page_index,
                        Arc::clone(&metadata),
                    );
                    Ok(metadata)
                })
                .await
                .clone();
            finish_metadata_in_flight(&store, &path, file_size, page_index, &in_flight.result);
            result.map_err(|error| ParquetError::General(error.to_string()))
        }
        .boxed()
    }
}

/// A `map_err` builder for corruption context `what`: turns any displayable
/// reader error into `Error::Corruption("{what}: {err}")`. One place for the
/// shape every scoped/delete/inline read shares.
fn corrupt<E: std::fmt::Display>(what: &'static str) -> impl Fn(E) -> Error {
    move |err| Error::Corruption(format!("{what}: {err}"))
}

/// A `usize` row count or offset as a `u64`. Infallible on every target
/// moraine supports (`usize` is at most 64 bits), so the conversion cannot
/// fail — a failure would mean a wider-than-64-bit platform, which would
/// otherwise silently mint a wrong row id.
fn usize_as_u64(value: usize) -> u64 {
    #[allow(clippy::expect_used)]
    u64::try_from(value).expect("usize fits in u64 on supported targets")
}

/// Derives one [`ScopedReadEntry`] per row of the Parquet file at `path`
/// that `rows` names, fetching only the footer, the columns at
/// `indexed_positions` (the indexed columns, in the index's column order),
/// and the embedded row-id column when the file carries one — byte-range
/// reads, never the whole object. Row ids resolve per `row_id_source`: the
/// field-id-tagged embedded column if present — rewrite files from UPDATE
/// and compaction preserve old ids there — else `row_id_start + ordinal`,
/// else refusal. This test helper deliberately exercises the discovery path;
/// production reads require DuckLake's recorded file and footer sizes.
#[cfg(test)]
async fn scoped_read_entries(
    object_store: Arc<dyn ObjectStore>,
    path: &Path,
    indexed_positions: &[usize],
    rows: ScopedRows<'_>,
    row_id_source: RowIdSource,
    file_size: Option<u64>,
) -> Result<Vec<ScopedReadEntry>> {
    let file_size = match file_size {
        Some(file_size) => file_size,
        None => {
            object_store
                .head(path)
                .await
                .map_err(corrupt("scoped read"))?
                .size
        }
    };
    scoped_read_entries_inner(
        object_store,
        path,
        indexed_positions,
        rows,
        row_id_source,
        file_size,
        0,
    )
    .await
}

/// Reads entries from one recorded Parquet object.
pub(crate) async fn scoped_read_recorded_entries(
    file: ParquetFile,
    indexed_positions: &[usize],
    rows: ScopedRows<'_>,
    row_id_source: RowIdSource,
) -> Result<Vec<ScopedReadEntry>> {
    scoped_read_entries_inner(
        file.object_store,
        &file.path,
        indexed_positions,
        rows,
        row_id_source,
        file.file_size,
        file.footer_size,
    )
    .await
}

async fn scoped_read_entries_inner(
    object_store: Arc<dyn ObjectStore>,
    path: &Path,
    indexed_positions: &[usize],
    rows: ScopedRows<'_>,
    row_id_source: RowIdSource,
    file_size: u64,
    footer_size: u64,
) -> Result<Vec<ScopedReadEntry>> {
    // A selection naming no row is answerable without reading anything.
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let selected = rows.positions();
    let ordinals = selected.map_or(Ordinals::Dense, Ordinals::Selected);

    if file_size < WHOLE_OBJECT_THRESHOLD {
        return whole_object_entries(
            object_store.as_ref(),
            path,
            indexed_positions,
            selected,
            ordinals,
            row_id_source,
        )
        .await;
    }

    let reader = ObjectStoreReader {
        store: object_store,
        path: path.clone(),
        file_size,
        footer_size,
        page_index: rows.page_index_policy(),
    };
    let options = ArrowReaderOptions::new().with_page_index_policy(rows.page_index_policy());
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .map_err(corrupt("scoped read"))?;
    let (row_id_position, row_id_start) =
        resolve_row_id_source(builder.parquet_schema(), row_id_source, path)?;
    let (mask, indexed_output, row_id_output) =
        projection(builder.parquet_schema(), indexed_positions, row_id_position)?;
    let selection = match selected {
        Some(positions) => Some(row_selection(
            positions,
            total_rows(builder.metadata(), path)?,
        )?),
        None => None,
    };

    let mut builder = builder.with_projection(mask);
    if let Some(selection) = selection {
        // The reader widens a selection to whole batches before deciding
        // which pages to fetch, so a default-sized batch around a handful of
        // scattered rows pulls back pages none of them sit in. Sizing the
        // batch to the selection keeps the widening to what was asked for.
        builder = builder.with_row_selection(selection);
    }
    let mut stream = builder.build().map_err(corrupt("scoped read"))?;

    let mut entries = Vec::new();
    let mut emitted = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(corrupt("scoped read"))?;
        entries.extend(record_batch_entries(
            &batch,
            &indexed_output,
            row_id_output,
            row_id_start,
            ordinals,
            emitted,
        )?);
        emitted = emitted.saturating_add(batch.num_rows());
    }

    Ok(entries)
}

/// Receives one bounded group of already encoded index entries from a
/// scoped read.
pub(crate) trait ScopedIndexEntryBatchConsumer {
    /// Consumes one decoded group before the reader advances to the next.
    async fn consume(&mut self, entries: Vec<ScopedIndexEntry>) -> Result<()>;
}

/// One immutable Parquet object's location and recorded metadata. Keeping
/// both sizes beside the path prevents a read path from silently dropping
/// the footer size and reintroducing metadata round trips.
pub(crate) struct ParquetFile {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    footer_size: u64,
}

impl ParquetFile {
    /// Describes one object using DuckLake's recorded sizes.
    pub(crate) fn new(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        file_size: u64,
        footer_size: u64,
    ) -> Self {
        Self {
            object_store,
            path,
            file_size,
            footer_size,
        }
    }
}

/// Streams a file's fused index entries to `consumer` in bounded Arrow
/// batches. Shared columns are projected once and borrowed directly into
/// their final keys without retaining the whole file's encoded entries.
#[allow(clippy::too_many_lines)]
pub(crate) async fn scoped_read_index_entry_batches<C: ScopedIndexEntryBatchConsumer>(
    file: ParquetFile,
    projections: &[IndexProjection],
    rows: ScopedRows<'_>,
    row_id_source: RowIdSource,
    excluded_ordinals: Option<&RowPositions>,
    consumer: &mut C,
) -> Result<()> {
    if rows.is_empty() || projections.is_empty() {
        return Ok(());
    }

    let file_size = file.file_size;
    let source_positions = index_positions(projections);
    let selected = rows.positions();
    let ordinals = selected.map_or(Ordinals::Dense, Ordinals::Selected);

    if file_size < WHOLE_OBJECT_THRESHOLD {
        let bytes: Bytes = file
            .object_store
            .get(&file.path)
            .await
            .map_err(corrupt("scoped read"))?
            .bytes()
            .await
            .map_err(corrupt("scoped read"))?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(corrupt("scoped read"))?;
        let (row_id_position, row_id_start) =
            resolve_row_id_source(builder.parquet_schema(), row_id_source, &file.path)?;
        let (mask, indexed_output, row_id_output) =
            projection(builder.parquet_schema(), &source_positions, row_id_position)?;
        let projections = remap_index_projections(projections, &source_positions, &indexed_output)?;
        let selection = match selected {
            Some(positions) => Some(row_selection(
                positions,
                total_rows(builder.metadata(), &file.path)?,
            )?),
            None => None,
        };
        let mut builder = builder
            .with_projection(mask)
            .with_batch_size(BUILD_READ_BATCH_ROWS);
        if let Some(selection) = selection {
            builder = builder.with_row_selection(selection);
        }
        let reader = builder.build().map_err(corrupt("scoped read"))?;
        let mut emitted = 0usize;
        for batch in reader {
            let batch = batch.map_err(corrupt("scoped read"))?;
            let entries = record_batch_index_entries(
                &batch,
                &projections,
                row_id_output,
                row_id_start,
                ordinals,
                emitted,
                excluded_ordinals,
                None,
            )?;
            consumer.consume(entries).await?;
            emitted = emitted.saturating_add(batch.num_rows());
        }
        return Ok(());
    }

    let reader = ObjectStoreReader {
        store: file.object_store,
        path: file.path.clone(),
        file_size,
        footer_size: file.footer_size,
        page_index: rows.page_index_policy(),
    };
    let options = ArrowReaderOptions::new().with_page_index_policy(rows.page_index_policy());
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .map_err(corrupt("scoped read"))?;
    let (row_id_position, row_id_start) =
        resolve_row_id_source(builder.parquet_schema(), row_id_source, &file.path)?;
    let (mask, indexed_output, row_id_output) =
        projection(builder.parquet_schema(), &source_positions, row_id_position)?;
    let projections = remap_index_projections(projections, &source_positions, &indexed_output)?;
    let selection = match selected {
        Some(positions) => Some(row_selection(
            positions,
            total_rows(builder.metadata(), &file.path)?,
        )?),
        None => None,
    };
    let mut builder = builder
        .with_projection(mask)
        .with_batch_size(BUILD_READ_BATCH_ROWS);
    if let Some(selection) = selection {
        builder = builder.with_row_selection(selection);
    }
    let mut stream = builder.build().map_err(corrupt("scoped read"))?;
    let mut emitted = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(corrupt("scoped read"))?;
        let entries = record_batch_index_entries(
            &batch,
            &projections,
            row_id_output,
            row_id_start,
            ordinals,
            emitted,
            excluded_ordinals,
            None,
        )?;
        consumer.consume(entries).await?;
        emitted = emitted.saturating_add(batch.num_rows());
    }
    Ok(())
}

/// Receives one bounded group of entries from a full-file scoped read.
/// `first_ordinal` is the physical position of the group's first row.
pub(crate) trait ScopedEntryBatchConsumer {
    /// Consumes one decoded group before the reader advances to the next.
    async fn consume(&mut self, entries: Vec<ScopedReadEntry>, first_ordinal: u64) -> Result<()>;
}

/// Rows decoded at once by the staged-build reader. The build step may be
/// smaller; its consumer splits this group before staging.
const BUILD_READ_BATCH_ROWS: usize = 8_192;

/// Streams a file's projected index entries to `consumer` in bounded Arrow
/// batches, starting at physical row `start_ordinal`. Unlike
/// [`scoped_read_recorded_entries`], this never collects the whole file.
/// DuckLake's recorded footer size is available to the metadata reader.
#[allow(clippy::too_many_lines)]
pub(crate) async fn scoped_read_entry_batches<C: ScopedEntryBatchConsumer>(
    file: ParquetFile,
    indexed_positions: &[usize],
    row_id_source: RowIdSource,
    start_ordinal: u64,
    consumer: &mut C,
) -> Result<()> {
    let ParquetFile {
        object_store,
        path,
        file_size,
        footer_size,
    } = file;

    if file_size < WHOLE_OBJECT_THRESHOLD {
        let bytes: Bytes = object_store
            .get(&path)
            .await
            .map_err(corrupt("scoped read"))?
            .bytes()
            .await
            .map_err(corrupt("scoped read"))?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(corrupt("scoped read"))?;
        let total = total_rows(builder.metadata(), &path)?;
        let start = usize::try_from(start_ordinal)
            .ok()
            .filter(|start| *start <= total)
            .ok_or_else(|| {
                Error::Corruption(format!(
                    "scoped read: resume row {start_ordinal} is past the file's {total} rows"
                ))
            })?;
        if start == total {
            return Ok(());
        }
        let (row_id_position, row_id_start) =
            resolve_row_id_source(builder.parquet_schema(), row_id_source, &path)?;
        let (mask, indexed_output, row_id_output) =
            projection(builder.parquet_schema(), indexed_positions, row_id_position)?;
        let selection = RowSelection::from_consecutive_ranges(std::iter::once(start..total), total);
        let reader = builder
            .with_projection(mask)
            .with_batch_size(BUILD_READ_BATCH_ROWS)
            .with_row_selection(selection)
            .build()
            .map_err(corrupt("scoped read"))?;
        let mut emitted = 0usize;
        for batch in reader {
            let batch = batch.map_err(corrupt("scoped read"))?;
            let entries = record_batch_entries(
                &batch,
                &indexed_output,
                row_id_output,
                row_id_start,
                Ordinals::Offset(start_ordinal),
                emitted,
            )?;
            consumer
                .consume(entries, start_ordinal.saturating_add(usize_as_u64(emitted)))
                .await?;
            emitted = emitted.saturating_add(batch.num_rows());
        }
        return Ok(());
    }

    let reader = ObjectStoreReader {
        store: object_store,
        path: path.clone(),
        file_size,
        footer_size,
        page_index: PageIndexPolicy::Optional,
    };
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .map_err(corrupt("scoped read"))?;
    let total = total_rows(builder.metadata(), &path)?;
    let start = usize::try_from(start_ordinal)
        .ok()
        .filter(|start| *start <= total)
        .ok_or_else(|| {
            Error::Corruption(format!(
                "scoped read: resume row {start_ordinal} is past the file's {total} rows"
            ))
        })?;
    if start == total {
        return Ok(());
    }
    let (row_id_position, row_id_start) =
        resolve_row_id_source(builder.parquet_schema(), row_id_source, &path)?;
    let (mask, indexed_output, row_id_output) =
        projection(builder.parquet_schema(), indexed_positions, row_id_position)?;
    let selection = RowSelection::from_consecutive_ranges(std::iter::once(start..total), total);
    let mut stream = builder
        .with_projection(mask)
        .with_batch_size(BUILD_READ_BATCH_ROWS)
        .with_row_selection(selection)
        .build()
        .map_err(corrupt("scoped read"))?;

    let mut emitted = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(corrupt("scoped read"))?;
        let entries = record_batch_entries(
            &batch,
            &indexed_output,
            row_id_output,
            row_id_start,
            Ordinals::Offset(start_ordinal),
            emitted,
        )?;
        consumer
            .consume(entries, start_ordinal.saturating_add(usize_as_u64(emitted)))
            .await?;
        emitted = emitted.saturating_add(batch.num_rows());
    }
    Ok(())
}

/// Below this object size the whole file is fetched in one request and
/// decoded from memory: on a remote store the range reader's footer and
/// chunk round trips cost more than the bytes they save (measured crossover
/// ≈ 6 MiB at 30 ms per request and 100 MB/s, and DuckLake's small
/// per-insert files sit far below it).
const WHOLE_OBJECT_THRESHOLD: u64 = 4 * 1024 * 1024;

/// The pre-threshold read path: one whole-object fetch, then decode only
/// the projected columns. The batches decode from memory, so this stays
/// bounded by the threshold above.
async fn whole_object_entries(
    object_store: &dyn ObjectStore,
    path: &Path,
    indexed_positions: &[usize],
    selected: Option<&[u64]>,
    ordinals: Ordinals<'_>,
    row_id_source: RowIdSource,
) -> Result<Vec<ScopedReadEntry>> {
    let bytes: Bytes = object_store
        .get(path)
        .await
        .map_err(corrupt("scoped read"))?
        .bytes()
        .await
        .map_err(corrupt("scoped read"))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(corrupt("scoped read"))?;
    let (row_id_position, row_id_start) =
        resolve_row_id_source(builder.parquet_schema(), row_id_source, path)?;
    let (mask, indexed_output, row_id_output) =
        projection(builder.parquet_schema(), indexed_positions, row_id_position)?;
    // The bytes are already here, so a selection saves decode rather than
    // transfer — which is the larger half of the cost either way.
    let selection = match selected {
        Some(positions) => Some(row_selection(
            positions,
            total_rows(builder.metadata(), path)?,
        )?),
        None => None,
    };

    let mut builder = builder.with_projection(mask);
    if let Some(selection) = selection {
        builder = builder.with_row_selection(selection);
    }
    let reader = builder.build().map_err(corrupt("scoped read"))?;

    let mut entries = Vec::new();
    let mut emitted = 0usize;
    for batch in reader {
        let batch = batch.map_err(corrupt("scoped read"))?;
        entries.extend(record_batch_entries(
            &batch,
            &indexed_output,
            row_id_output,
            row_id_start,
            ordinals,
            emitted,
        )?);
        emitted = emitted.saturating_add(batch.num_rows());
    }
    Ok(entries)
}

/// The projection over `schema` covering only the indexed and row-id
/// columns, with each requested position mapped to its index in the
/// projected output batch.
fn projection(
    schema: &parquet::schema::types::SchemaDescriptor,
    indexed_positions: &[usize],
    row_id_position: Option<usize>,
) -> Result<(ProjectionMask, Vec<usize>, Option<usize>)> {
    let mut projected: Vec<usize> = indexed_positions.to_vec();
    if let Some(position) = row_id_position {
        projected.push(position);
    }
    projected.sort_unstable();
    projected.dedup();
    let mask = ProjectionMask::roots(schema, projected.iter().copied());
    // Output-batch column index for an original file position.
    let output_index = |position: usize| {
        projected
            .iter()
            .position(|&candidate| candidate == position)
            .ok_or_else(|| Error::Corruption("scoped read: projected column vanished".to_owned()))
    };
    let indexed_output = indexed_positions
        .iter()
        .map(|&position| output_index(position))
        .collect::<Result<Vec<_>>>()?;
    let row_id_output = row_id_position.map(output_index).transpose()?;
    Ok((mask, indexed_output, row_id_output))
}

/// Derives one entry per row of `batch`: the values of the columns at
/// `positions` (direct indices into `batch`, in the index's column order),
/// and a row id read from `row_id_position` when present, else
/// `row_id_start` plus the row's file ordinal. `emitted` is how many rows
/// the read produced before this batch, which `ordinals` resolves against —
/// a selected read's rows are not at the positions it emits them in.
fn record_batch_entries(
    batch: &RecordBatch,
    positions: &[usize],
    row_id_position: Option<usize>,
    row_id_start: u64,
    ordinals: Ordinals<'_>,
    emitted: usize,
) -> Result<Vec<ScopedReadEntry>> {
    (0..batch.num_rows())
        .map(|row| {
            let values = positions
                .iter()
                .map(|&position| array_value(batch.column(position).as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            let row_id = match row_id_position {
                Some(position) => row_id_value(batch.column(position).as_ref(), row)?,
                None => row_id_start.saturating_add(ordinals.at(emitted.saturating_add(row))?),
            };
            Ok(ScopedReadEntry { row_id, values })
        })
        .collect()
}

/// Unique Arrow positions needed by every index plan, sorted for stable
/// Parquet projection and cheap remapping.
fn index_positions(projections: &[IndexProjection]) -> Vec<usize> {
    let mut positions: Vec<_> = projections
        .iter()
        .flat_map(|projection| projection.positions.iter().copied())
        .collect();
    positions.sort_unstable();
    positions.dedup();
    positions
}

/// Remaps plans from source-schema positions to columns in a projected
/// `RecordBatch`. This runs once per reader, never once per row.
fn remap_index_projections(
    projections: &[IndexProjection],
    source_positions: &[usize],
    output_positions: &[usize],
) -> Result<Vec<IndexProjection>> {
    projections
        .iter()
        .map(|projection| {
            let positions = projection
                .positions
                .iter()
                .map(|position| {
                    let source = source_positions.binary_search(position).map_err(|_| {
                        Error::Corruption(
                            "scoped read: index column vanished from projection".to_owned(),
                        )
                    })?;
                    output_positions.get(source).copied().ok_or_else(|| {
                        Error::Corruption(
                            "scoped read: projected index column has no output".to_owned(),
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(IndexProjection {
                index_id: projection.index_id,
                unique: projection.unique,
                positions,
                directions: projection.directions.clone(),
                nulls: projection.nulls.clone(),
            })
        })
        .collect()
}

/// Encodes every requested index key straight from one Arrow batch. Only the
/// final canonical key survives the row: strings and blobs remain borrowed
/// while their escaped bytes are appended.
#[allow(clippy::too_many_arguments)]
fn record_batch_index_entries(
    batch: &RecordBatch,
    projections: &[IndexProjection],
    row_id_position: Option<usize>,
    row_id_start: u64,
    ordinals: Ordinals<'_>,
    emitted: usize,
    excluded_ordinals: Option<&RowPositions>,
    included_row_ids: Option<&HashSet<u64>>,
) -> Result<Vec<ScopedIndexEntry>> {
    let mut entries = Vec::with_capacity(batch.num_rows().saturating_mul(projections.len()));
    for row in 0..batch.num_rows() {
        let ordinal = ordinals.at(emitted.saturating_add(row))?;
        let row_id = match row_id_position {
            Some(position) => row_id_value(batch.column(position).as_ref(), row)?,
            None => row_id_start.saturating_add(ordinal),
        };
        if excluded_ordinals.is_some_and(|excluded| excluded.contains(ordinal))
            || included_row_ids.is_some_and(|included| !included.contains(&row_id))
        {
            continue;
        }
        for (index, projection) in projections.iter().enumerate() {
            let mut builder = CanonicalKeyBuilder::new();
            for (column, &position) in projection.positions.iter().enumerate() {
                builder.append(
                    borrowed_array_value(batch.column(position).as_ref(), row)?,
                    projection
                        .directions
                        .get(column)
                        .copied()
                        .unwrap_or(Direction::Ascending),
                    projection
                        .nulls
                        .get(column)
                        .copied()
                        .unwrap_or(NullOrder::Last),
                )?;
            }
            let (key, unique) =
                builder.finish_index_entry(projection.index_id, projection.unique, row_id);
            entries.push(ScopedIndexEntry {
                index,
                row_id,
                key,
                unique,
            });
        }
    }
    Ok(entries)
}

/// The position of a delete file's `pos` column.
fn delete_position(schema: &Schema) -> Result<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == "pos")
        .ok_or_else(|| Error::Corruption("delete file has no `pos` column".to_owned()))
}

/// Appends the non-NULL positions in `batch` to `positions`.
fn append_delete_positions(batch: &RecordBatch, positions: &mut Vec<u64>) -> Result<()> {
    let column = batch.column(0).as_ref();
    for row in 0..batch.num_rows() {
        if column.is_null(row) {
            return Err(Error::Corruption(
                "delete file has a NULL position".to_owned(),
            ));
        }
        positions.push(row_id_value(column, row)?);
    }
    Ok(())
}

/// The row positions a DuckLake delete file marks dead, read from its `pos`
/// column. A delete file names positions within one data file, so its
/// `file_path` column carries no information the caller lacks. Small files
/// cost one whole-object read; large files use the recorded sizes and shared
/// metadata cache to fetch only the position column.
pub(crate) async fn delete_file_positions(file: ParquetFile) -> Result<Vec<u64>> {
    let ParquetFile {
        object_store,
        path,
        file_size,
        footer_size,
    } = file;
    let mut positions = Vec::new();

    if file_size < WHOLE_OBJECT_THRESHOLD {
        let bytes: Bytes = object_store
            .get(&path)
            .await
            .map_err(corrupt("delete-file read"))?
            .bytes()
            .await
            .map_err(corrupt("delete-file read"))?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(corrupt("delete-file read"))?;
        let position = delete_position(builder.schema().as_ref())?;
        let mask = ProjectionMask::roots(builder.parquet_schema(), [position]);
        let reader = builder
            .with_projection(mask)
            .build()
            .map_err(corrupt("delete-file read"))?;
        for batch in reader {
            append_delete_positions(&batch.map_err(corrupt("delete-file read"))?, &mut positions)?;
        }
        return Ok(positions);
    }

    let reader = ObjectStoreReader {
        store: object_store,
        path: path.clone(),
        file_size,
        footer_size,
        page_index: PageIndexPolicy::Skip,
    };
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .map_err(corrupt("delete-file read"))?;
    let position = delete_position(builder.schema().as_ref())?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), [position]);
    let mut stream = builder
        .with_projection(mask)
        .build()
        .map_err(corrupt("delete-file read"))?;
    while let Some(batch) = stream.next().await {
        append_delete_positions(&batch.map_err(corrupt("delete-file read"))?, &mut positions)?;
    }
    Ok(positions)
}

/// Decodes one schema-only inline IPC stream. Callers cache the resulting
/// Arrow schema per table version so chunks sharing it do not repeat the
/// `FlatBuffer` work.
pub(crate) fn decode_inline_schema(schema_ipc: Bytes) -> Result<SchemaRef> {
    #[cfg(test)]
    record_inline_schema_decode(&schema_ipc);

    Ok(
        StreamReader::try_new(std::io::Cursor::new(schema_ipc), None)
            .map_err(corrupt("inline schema"))?
            .schema(),
    )
}

/// Decodes an inline-insert Arrow body — `[u32-le message length][record-
/// batch message][arrow data buffers]` — against its already-decoded table
/// schema. The body becomes Arrow's immutable buffer by slicing the owning
/// `Bytes`, so decoding does not copy its potentially large data region.
fn decode_inline_batch(schema: SchemaRef, body: &Bytes) -> Result<RecordBatch> {
    if body.len() < 4 {
        return Err(Error::Corruption("inline body truncated".to_owned()));
    }
    let message_len = u32::from_le_bytes(
        body[0..4]
            .try_into()
            .map_err(|_| Error::Corruption("inline body length prefix".to_owned()))?,
    ) as usize;
    let message_end = 4 + message_len;
    if message_end > body.len() {
        return Err(Error::Corruption("inline body truncated".to_owned()));
    }
    let message = root_as_message(&body[4..message_end]).map_err(corrupt("inline message"))?;
    let record_batch = message
        .header_as_record_batch()
        .ok_or_else(|| Error::Corruption("inline body is not a record batch".to_owned()))?;
    let version = message.version();
    let buffer = Buffer::from(body.slice(message_end..));
    read_record_batch(
        &buffer,
        record_batch,
        schema,
        &std::collections::HashMap::new(),
        None,
        &version,
    )
    .map_err(corrupt("inline batch"))
}

/// Derives entries from an inline-insert chunk: decodes its body against the
/// schema, then reads the columns at `positions` per row with dense
/// `row_id_start + ordinal` ids (inline chunks carry no row-id column).
pub(crate) fn inline_batch_entries(
    schema: SchemaRef,
    body: &Bytes,
    positions: &[usize],
    row_id_start: u64,
) -> Result<Vec<ScopedReadEntry>> {
    #[cfg(test)]
    record_inline_batch_decode(row_id_start);

    let batch = decode_inline_batch(schema, body)?;
    record_batch_entries(&batch, positions, None, row_id_start, Ordinals::Dense, 0)
}

/// Decodes one inline Arrow chunk and constructs every requested index key
/// directly from its arrays. Each chunk is decoded once regardless of how
/// many indexes overlap its columns.
pub(crate) fn inline_batch_index_entries(
    schema: SchemaRef,
    body: &Bytes,
    projections: &[IndexProjection],
    row_id_start: u64,
    included_row_ids: Option<&HashSet<u64>>,
) -> Result<Vec<ScopedIndexEntry>> {
    #[cfg(test)]
    record_inline_batch_decode(row_id_start);

    let batch = decode_inline_batch(schema, body)?;
    record_batch_index_entries(
        &batch,
        projections,
        None,
        row_id_start,
        Ordinals::Dense,
        0,
        None,
        included_row_ids,
    )
}

#[cfg(test)]
fn inline_batch_decodes() -> &'static std::sync::Mutex<std::collections::HashMap<u64, usize>> {
    static COUNTS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<u64, usize>>> =
        std::sync::OnceLock::new();
    COUNTS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn record_inline_batch_decode(row_id_start: u64) {
    let mut counts = inline_batch_decodes()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *counts.entry(row_id_start).or_default() += 1;
}

#[cfg(test)]
pub(crate) fn inline_batch_decode_count(row_id_start: u64) -> usize {
    inline_batch_decodes()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&row_id_start)
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
fn inline_schema_decodes() -> &'static std::sync::Mutex<std::collections::HashMap<u64, usize>> {
    static COUNTS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<u64, usize>>> =
        std::sync::OnceLock::new();
    COUNTS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn inline_schema_fingerprint(schema_ipc: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    schema_ipc.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
fn record_inline_schema_decode(schema_ipc: &[u8]) {
    let mut counts = inline_schema_decodes()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *counts
        .entry(inline_schema_fingerprint(schema_ipc))
        .or_default() += 1;
}

/// How often the named schema IPC bytes were decoded in this test process.
#[cfg(test)]
pub(crate) fn inline_schema_decode_count(schema_ipc: &[u8]) -> usize {
    inline_schema_decodes()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&inline_schema_fingerprint(schema_ipc))
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    };
    use parquet::arrow::ArrowWriter;

    use super::*;

    /// Wraps an [`InMemory`] store, counting the payload bytes and requests
    /// served by object/range reads — so a test can assert how much of a
    /// file the scoped read actually fetched. `head` probes are not counted:
    /// they carry no payload.
    #[derive(Debug)]
    struct CountingStore {
        inner: InMemory,
        fetched_bytes: AtomicU64,
        fetch_requests: AtomicU64,
        fetch_delay: Option<Duration>,
    }

    impl CountingStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                fetched_bytes: AtomicU64::new(0),
                fetch_requests: AtomicU64::new(0),
                fetch_delay: None,
            }
        }

        fn with_fetch_delay(fetch_delay: Duration) -> Self {
            Self {
                fetch_delay: Some(fetch_delay),
                ..Self::new()
            }
        }

        fn fetched_bytes(&self) -> u64 {
            self.fetched_bytes.load(Ordering::Relaxed)
        }

        fn fetch_requests(&self) -> u64 {
            self.fetch_requests.load(Ordering::Relaxed)
        }
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if let Some(delay) = self.fetch_delay {
                tokio::time::sleep(delay).await;
            }
            let head = options.head;
            let result = self.inner.get_opts(location, options).await?;
            if !head {
                self.fetch_requests.fetch_add(1, Ordering::Relaxed);
                self.fetched_bytes
                    .fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
            }
            Ok(result)
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[std::ops::Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            if let Some(delay) = self.fetch_delay {
                tokio::time::sleep(delay).await;
            }
            let results = self.inner.get_ranges(location, ranges).await?;
            self.fetch_requests.fetch_add(1, Ordering::Relaxed);
            let total: u64 = results.iter().map(|bytes| bytes.len() as u64).sum();
            self.fetched_bytes.fetch_add(total, Ordering::Relaxed);
            Ok(results)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A file wide enough that its first column is a small fraction of its
    /// bytes: one `Int64` value plus seven fat `Utf8` payload columns, `rows`
    /// rows. Returns the written object's size and footer size.
    async fn write_wide_named_fixture_with_footer(
        store: &dyn ObjectStore,
        path: &Path,
        rows: usize,
        first_column: &str,
    ) -> (u64, u64) {
        let mut fields = vec![Field::new(first_column, DataType::Int64, false)];
        for i in 0..7 {
            fields.push(Field::new(format!("payload{i}"), DataType::Utf8, false));
        }
        let schema = Arc::new(Schema::new(fields));

        let ids: Vec<i64> = (0..i64::try_from(rows).unwrap()).collect();
        let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(ids))];
        for i in 0..7 {
            // Row-unique text so the payload columns stay large on disk.
            let values: Vec<String> = (0..rows)
                .map(|row| format!("payload-{i}-row-{row:08}-abcdefghijklmnopqrstuvwxyz"))
                .collect();
            columns.push(Arc::new(StringArray::from(values)));
        }
        let batch = RecordBatch::try_new(schema, columns).unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let object_len = buffer.len() as u64;
        let footer_offset = buffer.len() - 8;
        let footer_size = u64::from(u32::from_le_bytes(
            buffer[footer_offset..footer_offset + 4].try_into().unwrap(),
        ));
        store.put(path, buffer.into()).await.unwrap();
        (object_len, footer_size)
    }

    async fn write_wide_fixture_with_footer(
        store: &dyn ObjectStore,
        path: &Path,
        rows: usize,
    ) -> (u64, u64) {
        write_wide_named_fixture_with_footer(store, path, rows, "id").await
    }

    async fn write_wide_fixture(store: &dyn ObjectStore, path: &Path, rows: usize) -> u64 {
        write_wide_fixture_with_footer(store, path, rows).await.0
    }

    /// DuckLake's recorded footer size collapses the footer-length probe,
    /// and immutable-file metadata is reused on the next read. The second
    /// read therefore performs only the projected-column fetch.
    #[tokio::test]
    async fn recorded_footer_and_metadata_cache_remove_metadata_round_trips() {
        let store = Arc::new(CountingStore::new());
        let path = Path::from("cached-wide.parquet");
        let (object_len, footer_size) =
            write_wide_fixture_with_footer(store.as_ref(), &path, 20_000).await;
        assert!(object_len >= WHOLE_OBJECT_THRESHOLD);
        let wanted: RowPositions = [7, 19_000].into_iter().collect();

        let first = scoped_read_recorded_entries(
            ParquetFile::new(store.clone(), path.clone(), object_len, footer_size),
            &[0],
            ScopedRows::At(&wanted),
            RowIdSource::Ordinal,
        )
        .await
        .unwrap();
        assert_eq!(first.len(), 2);
        let first_requests = store.fetch_requests();

        let second = scoped_read_recorded_entries(
            ParquetFile::new(store.clone(), path.clone(), object_len, footer_size),
            &[0],
            ScopedRows::At(&wanted),
            RowIdSource::Ordinal,
        )
        .await
        .unwrap();
        assert_eq!(second, first);

        let second_requests = store.fetch_requests() - first_requests;
        assert_eq!(first_requests, 3, "footer, page index, projected columns");
        assert_eq!(second_requests, 1, "projected columns only");
    }

    /// A large delete file uses the range reader because only its `pos`
    /// column is relevant. Its immutable footer is retained for the next
    /// pass, while the position column remains an ordinary projected read.
    #[tokio::test]
    async fn large_delete_file_reads_only_positions_and_reuses_metadata() {
        let store = Arc::new(CountingStore::new());
        let path = Path::from("cached-wide-delete.parquet");
        let (object_len, footer_size) =
            write_wide_named_fixture_with_footer(store.as_ref(), &path, 20_000, "pos").await;
        assert!(object_len >= WHOLE_OBJECT_THRESHOLD);

        let first = delete_file_positions(ParquetFile::new(
            store.clone(),
            path.clone(),
            object_len,
            footer_size,
        ))
        .await
        .unwrap();
        assert_eq!(first.len(), 20_000);
        assert_eq!(first[0], 0);
        assert_eq!(first[19_999], 19_999);
        let first_requests = store.fetch_requests();
        let first_bytes = store.fetched_bytes();

        let second = delete_file_positions(ParquetFile::new(
            store.clone(),
            path.clone(),
            object_len,
            footer_size,
        ))
        .await
        .unwrap();
        assert_eq!(second, first);
        let second_requests = store.fetch_requests() - first_requests;

        assert_eq!(first_requests, 2, "footer and projected position column");
        assert_eq!(second_requests, 1, "projected position column only");
        assert!(
            first_bytes < object_len / 4,
            "fetched {first_bytes} of {object_len} bytes"
        );
    }

    /// A delete file below the crossover stays on one whole-object request;
    /// projecting it through the range reader would add latency without
    /// saving meaningful transfer.
    #[tokio::test]
    async fn small_delete_file_stays_on_one_whole_object_read() {
        let store = Arc::new(CountingStore::new());
        let path = Path::from("small-delete.parquet");
        let (object_len, footer_size) =
            write_wide_named_fixture_with_footer(store.as_ref(), &path, 3, "pos").await;
        assert!(object_len < WHOLE_OBJECT_THRESHOLD);

        let positions = delete_file_positions(ParquetFile::new(
            store.clone(),
            path,
            object_len,
            footer_size,
        ))
        .await
        .unwrap();

        assert_eq!(positions, vec![0, 1, 2]);
        assert_eq!(store.fetch_requests(), 1);
        assert_eq!(store.fetched_bytes(), object_len);
    }

    /// Two readers missing the same immutable footer at once share one
    /// metadata fill. Each still reads its own projected data column, while
    /// the footer and page index are fetched only once between them.
    #[tokio::test]
    async fn concurrent_metadata_misses_share_one_in_flight_fill() {
        let store = Arc::new(CountingStore::with_fetch_delay(Duration::from_millis(10)));
        let path = Path::from("concurrent-cached-wide.parquet");
        let (object_len, footer_size) =
            write_wide_fixture_with_footer(store.as_ref(), &path, 20_000).await;
        assert!(object_len >= WHOLE_OBJECT_THRESHOLD);
        let wanted: RowPositions = [7, 19_000].into_iter().collect();

        let first = scoped_read_recorded_entries(
            ParquetFile::new(store.clone(), path.clone(), object_len, footer_size),
            &[0],
            ScopedRows::At(&wanted),
            RowIdSource::Ordinal,
        );
        let second = scoped_read_recorded_entries(
            ParquetFile::new(store.clone(), path.clone(), object_len, footer_size),
            &[0],
            ScopedRows::At(&wanted),
            RowIdSource::Ordinal,
        );
        let (first, second) = tokio::join!(first, second);

        assert_eq!(first.unwrap(), second.unwrap());
        assert_eq!(
            store.fetch_requests(),
            4,
            "one footer, one page-index, and two projected-column reads"
        );
    }

    /// A failed shared fill wakes its callers but is not retained as a cache
    /// entry. Immutable objects do not change in production; replacing the
    /// fixture here proves a transient read failure can be retried.
    #[tokio::test]
    async fn failed_metadata_in_flight_fill_is_retryable() {
        let store = Arc::new(CountingStore::new());
        let path = Path::from("retry-cached-wide.parquet");
        let (object_len, footer_size) =
            write_wide_fixture_with_footer(store.as_ref(), &path, 20_000).await;
        assert!(object_len >= WHOLE_OBJECT_THRESHOLD);
        let valid = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
        let mut corrupt = valid.to_vec();
        *corrupt.last_mut().unwrap() ^= 0xff;
        store.inner.put(&path, corrupt.into()).await.unwrap();
        let wanted: RowPositions = [7].into_iter().collect();

        let failed = scoped_read_recorded_entries(
            ParquetFile::new(store.clone(), path.clone(), object_len, footer_size),
            &[0],
            ScopedRows::At(&wanted),
            RowIdSource::Ordinal,
        )
        .await;
        assert!(failed.is_err());

        store.inner.put(&path, valid.into()).await.unwrap();
        let retried = scoped_read_recorded_entries(
            ParquetFile::new(store, path, object_len, footer_size),
            &[0],
            ScopedRows::At(&wanted),
            RowIdSource::Ordinal,
        )
        .await
        .unwrap();
        assert_eq!(retried.len(), 1);
    }

    #[test]
    fn abandoned_metadata_in_flight_entry_is_removed() {
        let store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new());
        let path = Path::from("abandoned-in-flight.parquet");
        let lookup = metadata_lookup(&store, &path, 100, PageIndexPolicy::Skip);
        let MetadataLookup::InFlight(in_flight) = lookup else {
            panic!("a unique store and path must start cold");
        };
        assert!(
            METADATA_CACHE
                .lock()
                .unwrap()
                .in_flight
                .iter()
                .any(|entry| Arc::ptr_eq(&entry.result, &in_flight.result))
        );

        drop(in_flight);

        assert!(
            !METADATA_CACHE
                .lock()
                .unwrap()
                .in_flight
                .iter()
                .any(|entry| entry.path == path)
        );
    }

    /// Above the whole-object threshold the read must fetch only the footer
    /// and the projected columns' chunks, not the whole object — and values
    /// must follow the requested position order on this path too.
    #[tokio::test]
    async fn scoped_read_fetches_only_projected_columns() {
        let store = Arc::new(CountingStore::new());
        let path = Path::from("wide.parquet");
        // 20k rows ≈ 7.9 MB: above `WHOLE_OBJECT_THRESHOLD`, so the range
        // reader (not the small-file whole-object fallback) serves this.
        let object_len = write_wide_fixture(store.as_ref(), &path, 20_000).await;
        assert!(object_len >= WHOLE_OBJECT_THRESHOLD);

        let entries = scoped_read_entries(
            store.clone(),
            &path,
            &[1, 0],
            ScopedRows::All,
            RowIdSource::Ordinal,
            None,
        )
        .await
        .unwrap();
        assert_eq!(entries.len(), 20_000);
        assert_eq!(
            entries[19_999].values,
            vec![
                Some(IndexKeyValue::Str(
                    "payload-0-row-00019999-abcdefghijklmnopqrstuvwxyz".to_owned()
                )),
                Some(IndexKeyValue::Int {
                    value: 19_999,
                    width: IntWidth::I64,
                }),
            ],
        );

        let fetched = store.fetched_bytes();
        assert!(
            fetched < object_len * 2 / 5,
            "fetched {fetched} of {object_len} bytes ({} requests) — the scoped read should \
             range-read only the footer and the projected columns",
            store.fetch_requests(),
        );
    }

    /// A narrow file shaped like DuckLake's small per-insert output: an
    /// `Int64` id and one short `Utf8` column, `rows` rows. Returns the
    /// written object's size.
    async fn write_narrow_fixture(store: &dyn ObjectStore, path: &Path, rows: usize) -> u64 {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = (0..i64::try_from(rows).unwrap()).collect();
        let names: Vec<String> = (0..rows).map(|row| format!("name-{row}")).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let object_len = u64::try_from(buffer.len()).unwrap();
        store.put(path, buffer.into()).await.unwrap();
        object_len
    }

    /// Prints the exact bytes and requests the range reader issues per
    /// fixture shape. Run with:
    /// `cargo test -p moraine --lib -- --ignored --nocapture
    /// prints_fetch_profile`
    #[tokio::test]
    #[ignore = "fetch-profile probe; run manually with --nocapture"]
    async fn prints_fetch_profile() {
        // (label, wide fixture?, rows, indexed positions)
        let shapes: [(&str, bool, usize, &[usize]); 4] = [
            ("wide 8-col x 20k rows, 1 indexed col ", true, 20_000, &[0]),
            (
                "wide 8-col x 20k rows, 2 indexed cols",
                true,
                20_000,
                &[0, 1],
            ),
            ("narrow 2-col x 100 rows, 1 indexed   ", false, 100, &[0]),
            ("narrow 2-col x 10 rows, 1 indexed    ", false, 10, &[0]),
        ];
        for (label, wide, rows, positions) in shapes {
            let store = Arc::new(CountingStore::new());
            let path = Path::from("probe.parquet");
            let object_len = if wide {
                write_wide_fixture(store.as_ref(), &path, rows).await
            } else {
                write_narrow_fixture(store.as_ref(), &path, rows).await
            };
            let entries = scoped_read_entries(
                store.clone(),
                &path,
                positions,
                ScopedRows::All,
                RowIdSource::Ordinal,
                None,
            )
            .await
            .unwrap();
            assert_eq!(entries.len(), rows);
            println!(
                "{label}: object {object_len:>8} B, fetched {:>7} B ({:>2}%), {} requests",
                store.fetched_bytes(),
                store.fetched_bytes() * 100 / object_len,
                store.fetch_requests(),
            );
        }
    }

    /// Per-request latency a [`LatencyStore`] charges, modelling a remote
    /// store round trip.
    const REQUEST_LATENCY: std::time::Duration = std::time::Duration::from_millis(30);

    /// Wraps an [`InMemory`] store, modelling a remote one: every read
    /// request pays [`REQUEST_LATENCY`] plus transfer at ~100 MB/s (10 ns
    /// per byte).
    #[derive(Debug)]
    struct LatencyStore {
        inner: InMemory,
    }

    impl LatencyStore {
        async fn charge(bytes: u64) {
            tokio::time::sleep(REQUEST_LATENCY + std::time::Duration::from_nanos(bytes * 10)).await;
        }
    }

    impl std::fmt::Display for LatencyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "LatencyStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for LatencyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let head = options.head;
            let result = self.inner.get_opts(location, options).await?;
            let bytes = if head {
                0
            } else {
                result.range.end - result.range.start
            };
            Self::charge(bytes).await;
            Ok(result)
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[std::ops::Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            let results = self.inner.get_ranges(location, ranges).await?;
            Self::charge(results.iter().map(|bytes| bytes.len() as u64).sum()).await;
            Ok(results)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// The pre-range-reader read, reproduced for comparison: fetch the whole
    /// object, then decode only the projected columns.
    async fn whole_file_entries(
        store: &dyn ObjectStore,
        path: &Path,
        indexed_positions: &[usize],
    ) -> Vec<ScopedReadEntry> {
        let bytes: Bytes = store.get(path).await.unwrap().bytes().await.unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mask =
            ProjectionMask::roots(builder.parquet_schema(), indexed_positions.iter().copied());
        let reader = builder.with_projection(mask).build().unwrap();

        let output_positions: Vec<usize> = (0..indexed_positions.len()).collect();
        let mut entries = Vec::new();
        let mut emitted = 0usize;
        for batch in reader {
            let batch = batch.unwrap();
            entries.extend(
                record_batch_entries(&batch, &output_positions, None, 0, Ordinals::Dense, emitted)
                    .unwrap(),
            );
            emitted += batch.num_rows();
        }
        entries
    }

    /// Wall-clock comparison of the whole-object read against the range
    /// reader on a simulated remote store (30 ms per request, ~100 MB/s).
    /// Run with:
    /// `cargo test -p moraine --lib -- --ignored --nocapture simulated_remote`
    #[tokio::test]
    #[ignore = "timing probe; run manually with --nocapture"]
    async fn simulated_remote_store_bench() {
        // (label, wide fixture?, rows) — narrow-small is DuckLake's typical
        // per-insert file; wide-large is the backfill/bulk-maintenance case.
        let shapes = [
            ("narrow 2-col x 100 rows ", false, 100),
            ("wide 8-col x 50k rows   ", true, 50_000),
        ];
        for (label, wide, rows) in shapes {
            let store = Arc::new(LatencyStore {
                inner: InMemory::new(),
            });
            let path = Path::from("bench.parquet");
            let object_len = if wide {
                write_wide_fixture(store.as_ref(), &path, rows).await
            } else {
                write_narrow_fixture(store.as_ref(), &path, rows).await
            };

            let started = std::time::Instant::now();
            let old_entries = whole_file_entries(store.as_ref(), &path, &[0]).await;
            let whole_file = started.elapsed();

            let started = std::time::Instant::now();
            let new_entries = scoped_read_entries(
                store.clone(),
                &path,
                &[0],
                ScopedRows::All,
                RowIdSource::Ordinal,
                None,
            )
            .await
            .unwrap();
            let range_read = started.elapsed();

            assert_eq!(
                old_entries, new_entries,
                "both paths derive the same entries"
            );
            println!(
                "{label} ({object_len:>8} B): whole-file {whole_file:>9.2?}, range-read \
                 {range_read:>9.2?}"
            );
        }
    }

    async fn write_fixture(object_store: &InMemory, path: &Path, batch: &RecordBatch) {
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
            writer.write(batch).unwrap();
            writer.close().unwrap();
        }
        object_store.put(path, buffer.into()).await.unwrap();
    }

    fn fixture_batch() -> RecordBatch {
        // Columns: id (indexed), name (indexed, one NULL), row_id, and an
        // unindexed `payload` column the read must not touch.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("row_id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(Int64Array::from(vec![100, 101, 102])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn sub_microsecond_timestamps_index_by_their_own_count() {
        // A millisecond timestamp indexes by its millisecond count, a
        // nanosecond one by its nanosecond count — not misread as micros.
        let millis = arrow::array::TimestampMillisecondArray::from(vec![1_700_000_000_123i64]);
        assert_eq!(
            array_value(&millis, 0).unwrap(),
            Some(IndexKeyValue::Int {
                value: 1_700_000_000_123,
                width: IntWidth::I64
            }),
        );
        let nanos =
            arrow::array::TimestampNanosecondArray::from(vec![1_700_000_000_123_456_789i64]);
        assert_eq!(
            array_value(&nanos, 0).unwrap(),
            Some(IndexKeyValue::Int {
                value: 1_700_000_000_123_456_789,
                width: IntWidth::I64
            }),
        );
    }

    #[test]
    fn fixed_size_binary_indexes_as_bytes() {
        // A `UUID` reaches the read as a 16-byte `FixedSizeBinary`.
        let uuid = [0xABu8; 16];
        let array = arrow::array::FixedSizeBinaryArray::try_from_iter([uuid].into_iter()).unwrap();
        assert_eq!(
            array_value(&array, 0).unwrap(),
            Some(IndexKeyValue::Bytes(uuid.to_vec())),
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn fused_arrow_encoding_matches_owned_path_for_every_indexable_type() {
        use arrow::array::{
            ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
            Float64Array, Int8Array, Int16Array, Int32Array, LargeBinaryArray, LargeStringArray,
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        };

        let fixed = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some(vec![0, 1, 2, 3]), None].into_iter(),
            4,
        )
        .unwrap();
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("i8", Arc::new(Int8Array::from(vec![Some(-8), None]))),
            ("i16", Arc::new(Int16Array::from(vec![Some(-16), None]))),
            ("i32", Arc::new(Int32Array::from(vec![Some(-32), None]))),
            ("i64", Arc::new(Int64Array::from(vec![Some(-64), None]))),
            ("u8", Arc::new(UInt8Array::from(vec![Some(8), None]))),
            ("u16", Arc::new(UInt16Array::from(vec![Some(16), None]))),
            ("u32", Arc::new(UInt32Array::from(vec![Some(32), None]))),
            ("u64", Arc::new(UInt64Array::from(vec![Some(64), None]))),
            (
                "f32",
                Arc::new(Float32Array::from(vec![
                    Some(f32::from_bits(0xffc0_0001)),
                    None,
                ])),
            ),
            (
                "f64",
                Arc::new(Float64Array::from(vec![
                    Some(f64::from_bits(0x7ff8_0000_0000_0001)),
                    None,
                ])),
            ),
            ("bool", Arc::new(BooleanArray::from(vec![Some(true), None]))),
            (
                "utf8",
                Arc::new(StringArray::from(vec![Some("a\0\u{1}z"), None])),
            ),
            (
                "large_utf8",
                Arc::new(LargeStringArray::from(vec![Some("large"), None])),
            ),
            (
                "binary",
                Arc::new(BinaryArray::from(vec![Some(&[0_u8, 1, 0xff][..]), None])),
            ),
            (
                "large_binary",
                Arc::new(LargeBinaryArray::from(vec![Some(&[1_u8, 0, 2][..]), None])),
            ),
            ("fixed_binary", Arc::new(fixed)),
            ("date32", Arc::new(Date32Array::from(vec![Some(12), None]))),
            ("date64", Arc::new(Date64Array::from(vec![Some(13), None]))),
            (
                "timestamp_s",
                Arc::new(TimestampSecondArray::from(vec![Some(14), None])),
            ),
            (
                "timestamp_ms",
                Arc::new(TimestampMillisecondArray::from(vec![Some(15), None])),
            ),
            (
                "timestamp_us",
                Arc::new(TimestampMicrosecondArray::from(vec![Some(16), None])),
            ),
            (
                "timestamp_ns",
                Arc::new(TimestampNanosecondArray::from(vec![Some(17), None])),
            ),
        ];
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        let all_positions: Vec<_> = (0..batch.num_columns()).collect();
        let projections = vec![
            IndexProjection {
                index_id: 37,
                unique: true,
                directions: all_positions
                    .iter()
                    .enumerate()
                    .map(|(position, _)| {
                        if position % 2 == 0 {
                            Direction::Ascending
                        } else {
                            Direction::Descending
                        }
                    })
                    .collect(),
                nulls: all_positions
                    .iter()
                    .enumerate()
                    .map(|(position, _)| {
                        if position % 3 == 0 {
                            NullOrder::First
                        } else {
                            NullOrder::Last
                        }
                    })
                    .collect(),
                positions: all_positions,
            },
            // Overlapping text/blob/composite use verifies a shared Arrow
            // column can feed several final keys without an owned scalar.
            IndexProjection {
                index_id: 38,
                unique: false,
                positions: vec![11, 13, 11],
                directions: vec![
                    Direction::Descending,
                    Direction::Ascending,
                    Direction::Ascending,
                ],
                nulls: vec![NullOrder::Last, NullOrder::First, NullOrder::Last],
            },
        ];

        let actual = record_batch_index_entries(
            &batch,
            &projections,
            None,
            100,
            Ordinals::Dense,
            0,
            None,
            None,
        )
        .unwrap();
        let mut expected = Vec::new();
        for row in 0..batch.num_rows() {
            for (index, projection) in projections.iter().enumerate() {
                let values = projection
                    .positions
                    .iter()
                    .map(|&position| array_value(batch.column(position).as_ref(), row))
                    .collect::<Result<Vec<_>>>()
                    .unwrap();
                let row_id = 100 + u64::try_from(row).unwrap();
                let (key, unique) = crate::store::index_encoding::encode_ordered_index_entry(
                    &values,
                    &projection.directions,
                    &projection.nulls,
                    projection.index_id,
                    projection.unique,
                    row_id,
                )
                .unwrap();
                expected.push((index, row_id, key, unique));
            }
        }

        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert_eq!(actual.index, expected.0);
            assert_eq!(actual.row_id, expected.1);
            assert_eq!(actual.key, expected.2);
            assert_eq!(actual.unique, expected.3);
        }
    }

    #[test]
    fn fused_arrow_encoding_normalizes_signed_zero_identically() {
        use arrow::array::{ArrayRef, Float32Array, Float64Array};

        let batch = RecordBatch::try_from_iter(vec![
            (
                "f32",
                Arc::new(Float32Array::from(vec![-0.0, 0.0])) as ArrayRef,
            ),
            (
                "f64",
                Arc::new(Float64Array::from(vec![-0.0, 0.0])) as ArrayRef,
            ),
        ])
        .unwrap();
        let projections = [IndexProjection {
            index_id: 39,
            unique: true,
            positions: vec![0, 1],
            directions: vec![Direction::Descending, Direction::Ascending],
            nulls: vec![NullOrder::First, NullOrder::Last],
        }];
        let entries = record_batch_index_entries(
            &batch,
            &projections,
            None,
            0,
            Ordinals::Dense,
            0,
            None,
            None,
        )
        .unwrap();

        assert_eq!(entries[0].key, entries[1].key);
        for (row, entry) in entries.iter().enumerate() {
            let values = [
                array_value(batch.column(0).as_ref(), row).unwrap(),
                array_value(batch.column(1).as_ref(), row).unwrap(),
            ];
            assert_eq!(
                entry.key,
                crate::store::index_encoding::encode_ordered_index_entry(
                    &values,
                    &projections[0].directions,
                    &projections[0].nulls,
                    projections[0].index_id,
                    projections[0].unique,
                    u64::try_from(row).unwrap(),
                )
                .unwrap()
                .0
            );
        }
    }

    /// The row-id column DuckLake's rewrite and flush writers append:
    /// BIGINT, tagged with the reserved field id — at any position.
    fn tagged_row_id_field(nullable: bool) -> Field {
        Field::new("_ducklake_internal_row_id", DataType::Int64, nullable).with_metadata(
            std::collections::HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                "2147483540".to_string(),
            )]),
        )
    }

    /// `fixture_batch` with the row-id column carrying the field id, so
    /// discovery finds it (at position 2, not trailing).
    fn tagged_fixture_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            tagged_row_id_field(false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(Int64Array::from(vec![100, 101, 102])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn reads_indexed_columns_and_embedded_row_ids() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        write_fixture(&store, &path, &tagged_fixture_batch()).await;

        // Index over (id, name); the file carries the field-id-tagged
        // row-id column at position 2, found without a caller hint.
        let entries = scoped_read_entries(
            store.clone(),
            &path,
            &[0, 1],
            ScopedRows::All,
            RowIdSource::Resolve { row_id_start: None },
            None,
        )
        .await
        .unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[0],
            ScopedReadEntry {
                row_id: 100,
                values: vec![
                    Some(IndexKeyValue::Int {
                        value: 10,
                        width: IntWidth::I64,
                    }),
                    Some(IndexKeyValue::Str("a".to_owned())),
                ],
            }
        );
        // Row 1's name is NULL → no value for that component.
        assert_eq!(entries[1].row_id, 101);
        assert_eq!(entries[1].values[1], None);
        assert_eq!(entries[2].row_id, 102);
    }

    struct IndexBatchCollector {
        batches: Vec<Vec<ScopedIndexEntry>>,
    }

    impl ScopedIndexEntryBatchConsumer for IndexBatchCollector {
        async fn consume(&mut self, entries: Vec<ScopedIndexEntry>) -> Result<()> {
            self.batches.push(entries);
            Ok(())
        }
    }

    /// Fused index encoding reaches its consumer one Arrow batch at a time;
    /// a full-file upkeep read does not retain every encoded entry first.
    #[tokio::test]
    async fn streams_fused_index_entries_in_bounded_batches() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("streamed-index.parquet");
        let (file_size, footer_size) =
            write_wide_fixture_with_footer(store.as_ref(), &path, 20_000).await;
        assert!(file_size > WHOLE_OBJECT_THRESHOLD);
        let projections = [IndexProjection {
            index_id: 7,
            unique: false,
            positions: vec![0],
            directions: vec![Direction::Ascending],
            nulls: vec![NullOrder::First],
        }];
        let mut collector = IndexBatchCollector {
            batches: Vec::new(),
        };

        scoped_read_index_entry_batches(
            ParquetFile::new(store, path, file_size, footer_size),
            &projections,
            ScopedRows::All,
            RowIdSource::Resolve {
                row_id_start: Some(100),
            },
            None,
            &mut collector,
        )
        .await
        .unwrap();

        assert_eq!(collector.batches.len(), 3);
        assert!(
            collector
                .batches
                .iter()
                .all(|batch| batch.len() <= BUILD_READ_BATCH_ROWS)
        );
        assert_eq!(
            collector.batches.iter().map(Vec::len).sum::<usize>(),
            20_000
        );
        assert_eq!(collector.batches[0][0].row_id, 100);
        assert_eq!(collector.batches[2].last().unwrap().row_id, 20_099);
    }

    #[test]
    fn row_positions_sort_and_deduplicate_once() {
        let positions = RowPositions::from_unsorted(vec![9, 2, 9, 4, 2]);

        assert_eq!(positions.as_slice(), &[2, 4, 9]);
    }

    /// Selected-row reads use the same bounded fused pipeline as full-file
    /// additions instead of collecting the target's entries before its
    /// consumer can begin.
    #[tokio::test]
    async fn streams_selected_fused_index_entries() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("streamed-selected-index.parquet");
        let (file_size, footer_size) =
            write_wide_fixture_with_footer(store.as_ref(), &path, 20_000).await;
        assert!(file_size > WHOLE_OBJECT_THRESHOLD);
        let projections = [IndexProjection {
            index_id: 7,
            unique: false,
            positions: vec![0],
            directions: vec![Direction::Ascending],
            nulls: vec![NullOrder::First],
        }];
        let selected = RowPositions::from_unsorted(vec![19_999, 8_193, 1, 8_193]);
        let mut collector = IndexBatchCollector {
            batches: Vec::new(),
        };

        scoped_read_index_entry_batches(
            ParquetFile::new(store, path, file_size, footer_size),
            &projections,
            ScopedRows::At(&selected),
            RowIdSource::Resolve {
                row_id_start: Some(100),
            },
            None,
            &mut collector,
        )
        .await
        .unwrap();

        let row_ids: Vec<_> = collector
            .batches
            .into_iter()
            .flatten()
            .map(|entry| entry.row_id)
            .collect();
        assert_eq!(row_ids, vec![101, 8_293, 20_099]);
    }

    /// Values come back ordered as the requested positions — duplicates and
    /// all. The merged multi-index read (one fetch per file, split back per
    /// index) relies on exactly this.
    #[tokio::test]
    async fn values_follow_requested_position_order() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        write_fixture(&store, &path, &fixture_batch()).await;

        let entries = scoped_read_entries(
            store.clone(),
            &path,
            &[1, 0, 0],
            ScopedRows::All,
            RowIdSource::Ordinal,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            entries[0].values,
            vec![
                Some(IndexKeyValue::Str("a".to_owned())),
                Some(IndexKeyValue::Int {
                    value: 10,
                    width: IntWidth::I64,
                }),
                Some(IndexKeyValue::Int {
                    value: 10,
                    width: IntWidth::I64,
                }),
            ],
        );
    }

    #[tokio::test]
    async fn derives_row_ids_from_start_plus_ordinal_when_absent() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        write_fixture(&store, &path, &fixture_batch()).await;

        // `fixture_batch`'s "row_id" column carries no field id, so it is
        // not the embedded column — names mean nothing to discovery — and
        // ids fall back to row_id_start (500) + ordinal.
        let entries = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            ScopedRows::All,
            RowIdSource::Resolve {
                row_id_start: Some(500),
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            entries.iter().map(|e| e.row_id).collect::<Vec<_>>(),
            vec![500, 501, 502]
        );
    }

    /// The embedded column wins even when a dense start is recorded:
    /// flushed files carry both, and their ids may hold gaps.
    #[tokio::test]
    async fn embedded_row_id_column_wins_over_dense_start() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("rewrite.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            tagged_row_id_field(false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![5, 9, 12])),
            ],
        )
        .unwrap();
        write_fixture(&store, &path, &batch).await;

        let entries = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            ScopedRows::All,
            RowIdSource::Resolve {
                row_id_start: Some(100),
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            entries.iter().map(|e| e.row_id).collect::<Vec<_>>(),
            vec![5, 9, 12],
            "ids come from the column, not 100 + ordinal"
        );
    }

    /// A per-row-id catalog row over a file lacking the column is a
    /// disagreement between catalog and file.
    #[tokio::test]
    async fn resolve_with_neither_source_fails_corruption() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("plain.parquet");
        write_fixture(&store, &path, &fixture_batch()).await;

        let err = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            ScopedRows::All,
            RowIdSource::Resolve { row_id_start: None },
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }

    /// Ordinal mode refuses a file that already carries row ids —
    /// renumbering its rows would fork their identity.
    #[tokio::test]
    async fn ordinal_mode_refuses_an_embedded_row_id_column() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("rewrite.parquet");
        write_fixture(&store, &path, &tagged_fixture_batch()).await;

        let err = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            ScopedRows::All,
            RowIdSource::Ordinal,
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::Constraint(_)), "{err}");
    }

    /// A selective read decodes only the named rows, and their ids follow
    /// the file ordinal each row actually sits at — not its position in the
    /// selected output.
    #[tokio::test]
    async fn selective_read_returns_only_the_named_rows() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("narrow.parquet");
        write_narrow_fixture(store.as_ref(), &path, 10).await;

        let wanted: RowPositions = [2, 5, 7].into_iter().collect();
        let entries = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            ScopedRows::At(&wanted),
            RowIdSource::Resolve {
                row_id_start: Some(500),
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            entries.iter().map(|entry| entry.row_id).collect::<Vec<_>>(),
            vec![502, 505, 507],
        );
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.values[0].clone())
                .collect::<Vec<_>>(),
            [2, 5, 7]
                .into_iter()
                .map(|value| Some(IndexKeyValue::Int {
                    value,
                    width: IntWidth::I64,
                }))
                .collect::<Vec<_>>(),
        );
    }

    /// With an embedded row-id column the ids come from the column, so a
    /// selection must carry each selected row's own id.
    #[tokio::test]
    async fn selective_read_resolves_embedded_row_ids() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("tagged.parquet");
        write_fixture(&store, &path, &tagged_fixture_batch()).await;

        let wanted: RowPositions = [0, 2].into_iter().collect();
        let entries = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            ScopedRows::At(&wanted),
            RowIdSource::Resolve { row_id_start: None },
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            entries.iter().map(|entry| entry.row_id).collect::<Vec<_>>(),
            vec![100, 102],
        );
    }

    /// Selecting nothing reads nothing: a commit whose delete names no
    /// surviving position must not touch the store at all.
    #[tokio::test]
    async fn an_empty_selection_reads_nothing() {
        let store = Arc::new(CountingStore::new());
        let path = Path::from("wide.parquet");
        write_wide_fixture(store.as_ref(), &path, 20_000).await;

        let entries = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            ScopedRows::At(&RowPositions::default()),
            RowIdSource::Ordinal,
            None,
        )
        .await
        .unwrap();

        assert!(entries.is_empty());
        assert_eq!(store.fetch_requests(), 0, "no selection, no read");
    }

    /// The delete-path regression: deriving the entries of a handful of rows
    /// must not cost what deriving the whole file costs. Before the read
    /// took a selection, a delete decoded every row of its target and threw
    /// all but the killed ones away — cost set by the file's size rather
    /// than the delete's.
    ///
    /// Bytes are the observable proxy. They do not fall as far as the decode
    /// does: a selective read still pays for the page index it needs to skip
    /// pages with, and for the column's dictionary page, which any page of
    /// the chunk needs to decode. Measured ≈3x here; the assertion leaves
    /// room, since the point is that the selection reaches the reader at all.
    #[tokio::test]
    async fn selective_read_costs_a_fraction_of_a_full_read() {
        const ROWS: usize = 400_000;
        let path = Path::from("paged.parquet");
        let wanted: RowPositions = [7, 200_000, 399_999].into_iter().collect();

        let full_store = Arc::new(CountingStore::new());
        let object_len = write_paged_fixture(full_store.as_ref(), &path, ROWS).await;
        // Below the threshold the whole object is fetched either way, and
        // the comparison would measure nothing.
        assert!(object_len >= WHOLE_OBJECT_THRESHOLD);
        let full = scoped_read_entries(
            full_store.clone(),
            &path,
            &[0],
            ScopedRows::All,
            RowIdSource::Ordinal,
            None,
        )
        .await
        .unwrap();
        assert_eq!(full.len(), ROWS);
        let full_bytes = full_store.fetched_bytes();

        let few_store = Arc::new(CountingStore::new());
        write_paged_fixture(few_store.as_ref(), &path, ROWS).await;
        let few = scoped_read_entries(
            few_store.clone(),
            &path,
            &[0],
            ScopedRows::At(&wanted),
            RowIdSource::Ordinal,
            None,
        )
        .await
        .unwrap();

        // The selected rows, and only those, with the values the full read
        // derived for the same ordinals.
        assert_eq!(
            few,
            wanted
                .as_slice()
                .iter()
                .map(|&position| full[usize::try_from(position).unwrap()].clone())
                .collect::<Vec<_>>(),
        );

        let few_bytes = few_store.fetched_bytes();
        assert!(
            few_bytes * 2 < full_bytes,
            "selecting 3 of {ROWS} rows fetched {few_bytes} B against {full_bytes} B for the \
             whole file — the selection is not reaching the reader",
        );
    }

    /// A narrow file above [`WHOLE_OBJECT_THRESHOLD`], written in small data
    /// pages: the indexed column is most of the object and a row selection
    /// has pages to skip. The writer's default would put every row of this
    /// column in one page, leaving a selection nothing to skip.
    async fn write_paged_fixture(store: &dyn ObjectStore, path: &Path, rows: usize) -> u64 {
        let properties = parquet::file::properties::WriterProperties::builder()
            .set_data_page_row_count_limit(512)
            .build();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = (0..i64::try_from(rows).unwrap()).collect();
        let names: Vec<String> = (0..rows).map(|row| format!("name-{row}")).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer =
                ArrowWriter::try_new(&mut buffer, batch.schema(), Some(properties)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let object_len = u64::try_from(buffer.len()).unwrap();
        store.put(path, buffer.into()).await.unwrap();
        object_len
    }

    /// A NULL embedded row id has no dense fallback to hide behind.
    #[tokio::test]
    async fn null_embedded_row_id_fails_corruption() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("null-id.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            tagged_row_id_field(true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![Some(5), None])),
            ],
        )
        .unwrap();
        write_fixture(&store, &path, &batch).await;

        let err = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            ScopedRows::All,
            RowIdSource::Resolve { row_id_start: None },
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }
}
