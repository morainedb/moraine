//! Fixtures shared across the suite's modules.
//!
//! `unwrap_used` is a library-code lint, not exempted automatically for a
//! plain (non-`#[test]`) function even in an integration-test crate, so
//! the async helpers carry targeted allows.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use futures::{StreamExt, stream::BoxStream};
use moraine::{Catalog, CatalogOptions, ColumnDef, DataFile, SchemaId, TableId};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

/// A nullable BIGINT column.
pub fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
        children: Vec::new(),
    }
}

/// A parquet data file whose path and sizes derive from its row count.
pub fn datafile(rows: u64) -> DataFile {
    DataFile {
        path: format!("data-{rows}.parquet"),
        path_is_relative: true,
        file_format: "parquet".into(),
        record_count: rows,
        file_size_bytes: rows * 10,
        footer_size: 4,
        encryption_key: None,
        partition_values: vec![],
        column_stats: vec![],
    }
}

/// Opens a fresh catalog over in-memory object storage.
#[allow(clippy::unwrap_used)]
pub async fn open_memory() -> Catalog {
    Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap()
}

/// Opens a catalog pre-seeded with tables `a` and `b` in schema `s`.
#[allow(clippy::unwrap_used)]
pub async fn seeded() -> (Catalog, SchemaId, TableId, TableId) {
    let catalog = open_memory().await;
    catalog
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "a", &[col("x")])?;
            tx.create_table(s, "b", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = catalog.snapshot().await.unwrap();
    let s = snapshot.schema_by_name("s").unwrap().id;
    let a = snapshot.table_by_name(s, "a").unwrap().id;
    let b = snapshot.table_by_name(s, "b").unwrap().id;
    (catalog, s, a, b)
}

/// Object-store decorator over [`InMemory`] that tallies requests by class,
/// so a test can weigh a commit path by the object-store operations it costs
/// — PUT and LIST share object storage's expensive tier, GET its cheap one.
/// `head` probes carry no payload and are counted apart from full GETs.
#[derive(Debug, Default)]
pub struct CountingStore {
    inner: InMemory,
    puts: Arc<AtomicU64>,
    gets: Arc<AtomicU64>,
    heads: Arc<AtomicU64>,
    lists: Arc<AtomicU64>,
    deletes: Arc<AtomicU64>,
}

impl CountingStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put_count(&self) -> u64 {
        self.puts.load(Ordering::Relaxed)
    }

    pub fn get_count(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }

    pub fn head_count(&self) -> u64 {
        self.heads.load(Ordering::Relaxed)
    }

    pub fn list_count(&self) -> u64 {
        self.lists.load(Ordering::Relaxed)
    }

    pub fn delete_count(&self) -> u64 {
        self.deletes.load(Ordering::Relaxed)
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
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head {
            self.heads.fetch_add(1, Ordering::Relaxed);
        } else {
            self.gets.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let deletes = Arc::clone(&self.deletes);
        let counted = locations.inspect(move |outcome| {
            if outcome.is_ok() {
                deletes.fetch_add(1, Ordering::Relaxed);
            }
        });
        self.inner.delete_stream(counted.boxed())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.lists.fetch_add(1, Ordering::Relaxed);
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
