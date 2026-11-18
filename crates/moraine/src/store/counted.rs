//! An object store whose requests land in a catalog's counters, for the
//! write-ahead-log readers moraine builds itself, which SlateDB's own
//! instrumentation does not reach.

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};

use super::cache::{CacheCounters, RequestOp, RequestStore};

/// Wraps a store so every request is tallied, with its duration, under
/// `store`'s columns, exactly as SlateDB tallies the requests it makes
/// itself.
#[derive(Debug)]
pub(crate) struct CountedStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<CacheCounters>,
    store: RequestStore,
}

impl CountedStore {
    pub(crate) fn wrap(
        inner: Arc<dyn ObjectStore>,
        counters: Arc<CacheCounters>,
        store: RequestStore,
    ) -> Arc<dyn ObjectStore> {
        Arc::new(Self {
            inner,
            counters,
            store,
        })
    }

    async fn timed<T>(
        &self,
        op: RequestOp,
        request: impl Future<Output = object_store::Result<T>>,
    ) -> object_store::Result<T> {
        let started = Instant::now();
        let result = request.await;
        self.counters
            .record_request(self.store, op, started.elapsed(), result.is_err());
        result
    }
}

impl std::fmt::Display for CountedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.timed(RequestOp::Put, self.inner.put_opts(location, payload, opts))
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.timed(
            RequestOp::Put,
            self.inner.put_multipart_opts(location, opts),
        )
        .await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let op = if options.head {
            RequestOp::Head
        } else {
            RequestOp::Get
        };
        self.timed(op, self.inner.get_opts(location, options)).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.timed(RequestOp::Get, self.inner.get_ranges(location, ranges))
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        // Deletes stream, so each counts as it completes and none is timed.
        let counters = Arc::clone(&self.counters);
        let store = self.store;
        self.inner
            .delete_stream(locations)
            .inspect(move |deleted| {
                counters.record_request(
                    store,
                    RequestOp::Delete,
                    std::time::Duration::ZERO,
                    deleted.is_err(),
                );
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.counters.record_request(
            self.store,
            RequestOp::List,
            std::time::Duration::ZERO,
            false,
        );
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.counters.record_request(
            self.store,
            RequestOp::List,
            std::time::Duration::ZERO,
            false,
        );
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.counters.record_request(
            self.store,
            RequestOp::List,
            std::time::Duration::ZERO,
            false,
        );
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

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, memory::InMemory};

    use super::*;
    use crate::store::cache::store_counters;

    /// Every request kind lands in the column SlateDB would put it in,
    /// misses included, and listings carry no duration.
    #[tokio::test]
    async fn requests_land_in_the_columns_slatedb_uses() {
        let counters = store_counters();
        let store = CountedStore::wrap(
            Arc::new(InMemory::new()),
            Arc::clone(&counters),
            RequestStore::Main,
        );
        let present = Path::from("wal/00000000000000000001.sst");

        store
            .put(&present, PutPayload::from_static(b"log"))
            .await
            .unwrap();
        store.get(&present).await.unwrap();
        store.head(&present).await.unwrap();
        assert!(store.head(&Path::from("wal/missing.sst")).await.is_err());
        let listed = store.list(Some(&Path::from("wal"))).count().await;
        assert_eq!(listed, 1);
        store.delete(&present).await.unwrap();

        let tally = counters.object_store_tally();
        assert_eq!(tally.main_puts, 1);
        assert_eq!(tally.main_gets, 1);
        assert_eq!(tally.main_heads, 2);
        assert_eq!(tally.main_lists, 1);
        assert_eq!(tally.main_deletes, 1);
        assert_eq!(tally.errors, 1);
        assert!(tally.main_head_duration > std::time::Duration::ZERO);
        assert_eq!(tally.wal_gets + tally.wal_heads + tally.wal_lists, 0);
    }

    /// A separate log store tallies under the WAL columns.
    #[tokio::test]
    async fn a_log_store_tallies_under_the_wal_columns() {
        let counters = store_counters();
        let store = CountedStore::wrap(
            Arc::new(InMemory::new()),
            Arc::clone(&counters),
            RequestStore::Wal,
        );
        assert!(store.head(&Path::from("wal/missing.sst")).await.is_err());

        let tally = counters.object_store_tally();
        assert_eq!(tally.wal_heads, 1);
        assert_eq!(tally.main_heads, 0);
    }
}
