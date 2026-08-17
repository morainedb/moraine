use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::{
    array::{Array, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
};
use parquet::arrow::{ArrowWriter, ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};

use super::*;
use crate::{
    data_file::{selection::Ordinals, values::array_value},
    store::index_encoding::IntWidth,
};

/// Derives one [`ScopedReadEntry`] per row of the Parquet file at `path`
/// that `rows` names, fetching only the footer, the columns at
/// `indexed_positions` (the indexed columns, in the index's column order),
/// and the embedded row-id column when the file carries one — byte-range
/// reads, never the whole object. Row ids resolve per `row_id_source`: the
/// field-id-tagged embedded column if present — rewrite files from UPDATE
/// and compaction preserve old ids there — else `row_id_start + ordinal`,
/// else refusal. This test helper deliberately exercises the discovery path;
/// production reads require DuckLake's recorded file and footer sizes.
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
    scoped_read_recorded_entries(
        ParquetFile::new(DataStore::new(object_store), path.clone(), file_size, 0),
        indexed_positions,
        rows,
        row_id_source,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_encoding_workers_share_one_process_bound() {
    const TASKS: usize = INDEX_ENCODING_CONCURRENCY * 2;

    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let work = (0..TASKS).map(|position| {
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        async move {
            run_bounded_index_encoding(move || {
                let held = active.fetch_add(1, Ordering::AcqRel) + 1;
                peak.fetch_max(held, Ordering::AcqRel);
                std::thread::sleep(Duration::from_millis(10));
                active.fetch_sub(1, Ordering::AcqRel);
                Ok(position)
            })
            .await
        }
    });
    let mut completed = stream::iter(work)
        .buffer_unordered(TASKS)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    completed.sort_unstable();

    assert_eq!(completed, (0..TASKS).collect::<Vec<_>>());
    assert!(peak.load(Ordering::Relaxed) <= INDEX_ENCODING_CONCURRENCY);
    assert!(peak.load(Ordering::Relaxed) > 1);
}

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

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
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
    let data = DataStore::new(store.clone());
    let path = Path::from("cached-wide.parquet");
    let (object_len, footer_size) =
        write_wide_fixture_with_footer(store.as_ref(), &path, 20_000).await;
    let wanted: RowPositions = [7, 19_000].into_iter().collect();

    let first = scoped_read_recorded_entries(
        ParquetFile::new(data.clone(), path.clone(), object_len, footer_size),
        &[0],
        ScopedRows::At(&wanted),
        RowIdSource::Ordinal,
    )
    .await
    .unwrap();
    assert_eq!(first.len(), 2);
    let first_requests = store.fetch_requests();

    let second = scoped_read_recorded_entries(
        ParquetFile::new(data.clone(), path.clone(), object_len, footer_size),
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

/// A delete-file read fetches only its `pos` column. Its immutable footer
/// is retained for the next pass, while the position column remains an
/// ordinary projected read.
#[tokio::test]
async fn large_delete_file_reads_only_positions_and_reuses_metadata() {
    let store = Arc::new(CountingStore::new());
    let data = DataStore::new(store.clone());
    let path = Path::from("cached-wide-delete.parquet");
    let (object_len, footer_size) =
        write_wide_named_fixture_with_footer(store.as_ref(), &path, 20_000, "pos").await;
    let first = delete_file_positions(ParquetFile::new(
        data.clone(),
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
        data.clone(),
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

/// Two readers missing the same immutable footer at once share one
/// metadata fill. Each still reads its own projected data column, while
/// the footer and page index are fetched only once between them.
#[tokio::test]
async fn concurrent_metadata_misses_share_one_in_flight_fill() {
    let store = Arc::new(CountingStore::with_fetch_delay(Duration::from_millis(10)));
    let data = DataStore::new(store.clone());
    let path = Path::from("concurrent-cached-wide.parquet");
    let (object_len, footer_size) =
        write_wide_fixture_with_footer(store.as_ref(), &path, 20_000).await;
    let wanted: RowPositions = [7, 19_000].into_iter().collect();

    let first = scoped_read_recorded_entries(
        ParquetFile::new(data.clone(), path.clone(), object_len, footer_size),
        &[0],
        ScopedRows::At(&wanted),
        RowIdSource::Ordinal,
    );
    let second = scoped_read_recorded_entries(
        ParquetFile::new(data.clone(), path.clone(), object_len, footer_size),
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
    let data = DataStore::new(store.clone());
    let path = Path::from("retry-cached-wide.parquet");
    let (object_len, footer_size) =
        write_wide_fixture_with_footer(store.as_ref(), &path, 20_000).await;
    let valid = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
    let mut corrupt = valid.to_vec();
    *corrupt.last_mut().unwrap() ^= 0xff;
    store.inner.put(&path, corrupt.into()).await.unwrap();
    let wanted: RowPositions = [7].into_iter().collect();

    let failed = scoped_read_recorded_entries(
        ParquetFile::new(data.clone(), path.clone(), object_len, footer_size),
        &[0],
        ScopedRows::At(&wanted),
        RowIdSource::Ordinal,
    )
    .await;
    assert!(failed.is_err());

    store.inner.put(&path, valid.into()).await.unwrap();
    let retried = scoped_read_recorded_entries(
        ParquetFile::new(DataStore::new(store), path, object_len, footer_size),
        &[0],
        ScopedRows::At(&wanted),
        RowIdSource::Ordinal,
    )
    .await
    .unwrap();
    assert_eq!(retried.len(), 1);
}

/// The read fetches only the footer and projected column chunks, and
/// values follow the requested position order.
#[tokio::test]
async fn scoped_read_fetches_only_projected_columns() {
    let store = Arc::new(CountingStore::new());
    let path = Path::from("wide.parquet");
    let object_len = write_wide_fixture(store.as_ref(), &path, 20_000).await;

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

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
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
    let mask = ProjectionMask::roots(builder.parquet_schema(), indexed_positions.iter().copied());
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
    let nanos = arrow::array::TimestampNanosecondArray::from(vec![1_700_000_000_123_456_789i64]);
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
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, LargeBinaryArray, LargeStringArray,
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
            ordinal: 0,
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

/// Fused index encoding yields one Arrow batch at a time; a full-file
/// upkeep read does not retain every encoded entry first.
#[tokio::test]
async fn streams_fused_index_entries_in_bounded_batches() {
    let store = Arc::new(InMemory::new());
    let path = Path::from("streamed-index.parquet");
    let (file_size, footer_size) =
        write_wide_fixture_with_footer(store.as_ref(), &path, 20_000).await;
    let projections = vec![IndexProjection {
        index_id: 7,
        unique: false,
        positions: vec![0],
        directions: vec![Direction::Ascending],
        nulls: vec![NullOrder::First],
    }];
    let batches = scoped_read_index_entry_batches(
        ParquetFile::new(DataStore::new(store), path, file_size, footer_size),
        projections,
        ScopedRows::All,
        RowIdSource::Resolve {
            row_id_start: Some(100),
        },
        None,
    )
    .await
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    assert_eq!(batches.len(), 3);
    assert!(
        batches
            .iter()
            .all(|batch| batch.len() <= BUILD_READ_BATCH_ROWS)
    );
    assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), 20_000);
    assert_eq!(batches[0][0].row_id, 100);
    assert_eq!(batches[2].last().unwrap().row_id, 20_099);
}

#[test]
fn row_positions_sort_and_deduplicate_once() {
    let positions = RowPositions::from_unsorted(vec![9, 2, 9, 4, 2]);

    assert_eq!(positions.as_slice(), &[2, 4, 9]);
}

/// Selected-row reads use the same bounded fused pipeline as full-file
/// additions instead of collecting the target's entries first.
#[tokio::test]
async fn streams_selected_fused_index_entries() {
    let store = Arc::new(InMemory::new());
    let path = Path::from("streamed-selected-index.parquet");
    let (file_size, footer_size) =
        write_wide_fixture_with_footer(store.as_ref(), &path, 20_000).await;
    let projections = vec![IndexProjection {
        index_id: 7,
        unique: false,
        positions: vec![0],
        directions: vec![Direction::Ascending],
        nulls: vec![NullOrder::First],
    }];
    let selected = RowPositions::from_unsorted(vec![19_999, 8_193, 1, 8_193]);
    let batches = scoped_read_index_entry_batches(
        ParquetFile::new(DataStore::new(store), path, file_size, footer_size),
        projections,
        ScopedRows::At(&selected),
        RowIdSource::Resolve {
            row_id_start: Some(100),
        },
        None,
    )
    .await
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    let row_ids: Vec<_> = batches
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
    write_paged_fixture(full_store.as_ref(), &path, ROWS).await;
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

/// A narrow file written in small data pages: the indexed column is most
/// of the object and a row selection has pages to skip. The writer's
/// default would put every row in one page, leaving nothing to skip.
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
