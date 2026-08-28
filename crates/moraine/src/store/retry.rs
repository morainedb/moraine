//! Reads taken again when the transport under them fails.
//!
//! [`retrying`] runs a read until it settles, and a caller that owns its
//! own reads — a data file's — calls it directly. The store SlateDB is
//! handed does not own its reads, so [`Store`] wraps it, covering the one
//! read SlateDB's own retries leave out.

use std::{sync::Arc, time::Duration};

use futures::{StreamExt, stream, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use tracing::warn;

/// Attempts one read is given before its error surfaces; the first is not
/// a retry.
const MAX_READ_ATTEMPTS: u32 = 6;

/// Delay before a read's second attempt; each further one doubles it, up
/// to [`MAX_READ_DELAY`].
const BASE_READ_DELAY: Duration = Duration::from_millis(100);

/// Ceiling on one retry's delay.
const MAX_READ_DELAY: Duration = Duration::from_secs(2);

/// Runs `attempt` until it succeeds, fails terminally, or spends
/// [`MAX_READ_ATTEMPTS`]. An attempt must yield the bytes themselves, so
/// that a body dying partway fails the attempt and not the read. `what`
/// names the read in the retry warning.
pub(crate) async fn retrying<T, F, Fut>(
    what: &str,
    location: &Path,
    attempt: F,
) -> object_store::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = object_store::Result<T>>,
{
    for spent in 1..MAX_READ_ATTEMPTS {
        match attempt().await {
            Ok(read) => return Ok(read),
            Err(error) if transient(&error) => {
                let delay = read_backoff(spent);
                warn!(
                    %location,
                    attempt = spent,
                    delay_ms = delay.as_millis(),
                    error = %error,
                    "{what} failed on a transient store error; retrying"
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
    attempt().await
}

/// How long to wait after a read's `spent`th attempt: exponential from
/// [`BASE_READ_DELAY`] to [`MAX_READ_DELAY`].
fn read_backoff(spent: u32) -> Duration {
    let doublings = spent.saturating_sub(1).min(31);
    BASE_READ_DELAY
        .saturating_mul(1 << doublings)
        .min(MAX_READ_DELAY)
}

/// Whether `error` is worth another attempt: everything except the
/// terminal answers a store gives about the object itself, the operation,
/// or the credentials.
fn transient(error: &object_store::Error) -> bool {
    !matches!(
        error,
        object_store::Error::NotFound { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotModified { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::NotSupported { .. }
            | object_store::Error::InvalidPath { .. }
            | object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. }
            | object_store::Error::UnknownConfigurationKey { .. }
    )
}

/// An object store that takes an unranged read again when its body dies
/// partway, so the bytes arrive whole or not at all.
///
/// For the store SlateDB is handed, and shaped to complement the retries
/// SlateDB wraps it in: those drain a ranged read's body inside the
/// attempt but leave an unranged one — the manifest read every commit
/// takes — to be drained after the attempt returns, where a dying body is
/// nobody's retry. Ranged and metadata-only reads therefore pass straight
/// through, since retrying them here would multiply every request the
/// layer above already retries.
#[derive(Debug)]
pub(crate) struct Store {
    inner: Arc<dyn ObjectStore>,
}

impl Store {
    /// Wraps `inner`, so that its reads retry.
    pub(crate) fn wrap(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(Self { inner })
    }

    /// One read attempt, body included: a body that fails partway
    /// through, or that ends short of the range the store said it was
    /// serving, fails this attempt rather than the caller's read.
    async fn read(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let result = self.inner.get_opts(location, options).await?;

        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let bytes = result.bytes().await?;

        // Nothing below reports a body that simply stopped early: the
        // collector takes the served length as a capacity hint alone.
        let served = range.end.saturating_sub(range.start);
        if bytes.len() as u64 != served {
            return Err(object_store::Error::Generic {
                store: "moraine",
                source: format!(
                    "read of {location} ended after {} of the {served} bytes it served",
                    bytes.len()
                )
                .into(),
            });
        }

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes,
            extensions,
        })
    }
}

/// The caches key on the store's name, so the wrapper answers with the
/// name of the store it wraps.
impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Store {
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
        if options.head || options.range.is_some() {
            return self.inner.get_opts(location, options).await;
        }
        retrying("a read", location, || self.read(location, options.clone())).await
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

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
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

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use bytes::Bytes;
    use futures::{StreamExt, stream};
    use object_store::{
        GetOptions, GetResult, GetResultPayload, ObjectStore, ObjectStoreExt, PutPayload,
        memory::InMemory, path::Path,
    };

    use super::{MAX_READ_ATTEMPTS, Store};

    const PAYLOAD: &[u8] = b"the body the transport drops halfway through";

    /// A store that answers its first `failures` reads with a partial
    /// body: one chunk, then either the error a dropped connection raises
    /// or — when `silent` — a clean end short of what it served.
    #[derive(Debug)]
    struct DroppedBodies {
        inner: Arc<dyn ObjectStore>,
        failures: AtomicUsize,
        reads: AtomicUsize,
        silent: bool,
    }

    impl DroppedBodies {
        fn new(failures: usize) -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(InMemory::new()),
                failures: AtomicUsize::new(failures),
                reads: AtomicUsize::new(0),
                silent: false,
            })
        }

        /// Bodies that stop early without raising anything.
        fn silent(failures: usize) -> Arc<Self> {
            Arc::new(Self {
                silent: true,
                ..Arc::into_inner(Self::new(failures)).expect("the arc is unshared")
            })
        }

        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    impl std::fmt::Display for DroppedBodies {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "DroppedBodies({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for DroppedBodies {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let result = self.inner.get_opts(location, options).await?;
            let dropping = self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok();
            if !dropping {
                return Ok(result);
            }

            let first = Ok(Bytes::copy_from_slice(&PAYLOAD[..1]));
            let body: Vec<object_store::Result<Bytes>> = if self.silent {
                vec![first]
            } else {
                vec![
                    first,
                    Err(object_store::Error::Generic {
                        store: "DroppedBodies",
                        source: "connection closed before the body ended".into(),
                    }),
                ]
            };
            Ok(GetResult {
                payload: GetResultPayload::Stream(stream::iter(body).boxed()),
                ..result
            })
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    async fn written(store: &Arc<DroppedBodies>) -> Path {
        let path = Path::from("body");
        store
            .inner
            .put(&path, PutPayload::from_static(PAYLOAD))
            .await
            .expect("the in-memory write cannot fail");
        path
    }

    /// A body that dies mid-stream is re-read, not reported.
    #[tokio::test(start_paused = true)]
    async fn a_read_whose_body_dies_partway_is_taken_again() {
        let flaky = DroppedBodies::new(2);
        let path = written(&flaky).await;
        let store = Store::wrap(Arc::clone(&flaky) as Arc<dyn ObjectStore>);

        let read = store.get(&path).await.expect("the read must survive");
        let bytes = read.bytes().await.expect("the body must survive");

        assert_eq!(bytes, Bytes::from_static(PAYLOAD));
        assert_eq!(flaky.reads(), 3, "two dropped bodies, then the whole one");
    }

    /// A body that never settles surfaces its own error, once the budget
    /// is spent.
    #[tokio::test(start_paused = true)]
    async fn a_body_that_never_settles_surfaces_its_error() {
        let flaky = DroppedBodies::new(usize::MAX);
        let path = written(&flaky).await;
        let store = Store::wrap(Arc::clone(&flaky) as Arc<dyn ObjectStore>);

        let error = store.get(&path).await.expect_err("the body never settles");

        assert!(
            error.to_string().contains("connection closed"),
            "the store's own error must survive the retries: {error}"
        );
        assert_eq!(flaky.reads(), MAX_READ_ATTEMPTS as usize);
    }

    /// A missing object is an answer, not a failure: it is not retried.
    #[tokio::test(start_paused = true)]
    async fn a_missing_object_is_reported_without_a_retry() {
        let flaky = DroppedBodies::new(0);
        let store = Store::wrap(Arc::clone(&flaky) as Arc<dyn ObjectStore>);

        let error = store
            .get(&Path::from("absent"))
            .await
            .expect_err("a missing object cannot be read");

        assert!(
            matches!(error, object_store::Error::NotFound { .. }),
            "a missing object must surface as such: {error}"
        );
        assert_eq!(flaky.reads(), 1, "a terminal answer is not retried");
    }

    /// A body that ends short of what the store said it was serving is
    /// read again: nothing under the wrapper reports it.
    #[tokio::test(start_paused = true)]
    async fn a_body_that_ends_short_is_taken_again() {
        let flaky = DroppedBodies::silent(2);
        let path = written(&flaky).await;
        let store = Store::wrap(Arc::clone(&flaky) as Arc<dyn ObjectStore>);

        let read = store.get(&path).await.expect("the read must survive");
        let bytes = read.bytes().await.expect("the body must survive");

        assert_eq!(bytes, Bytes::from_static(PAYLOAD));
        assert_eq!(flaky.reads(), 3, "two short bodies, then the whole one");
    }

    /// A ranged read passes through: the layer above drains those inside
    /// its own retries, and taking them again here would double them.
    #[tokio::test(start_paused = true)]
    async fn a_ranged_read_is_left_to_the_layer_above() {
        let flaky = DroppedBodies::new(usize::MAX);
        let path = written(&flaky).await;
        let store = Store::wrap(Arc::clone(&flaky) as Arc<dyn ObjectStore>);

        let read = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some((0..4).into()),
                    ..GetOptions::default()
                },
            )
            .await
            .expect("the headers arrive");
        read.bytes().await.expect_err("the body still dies");

        assert_eq!(flaky.reads(), 1, "the wrapper took the read once");
    }

    /// The caches key on the store's name, so wrapping must not rename it.
    #[test]
    fn the_wrapper_answers_with_the_name_of_the_store_it_wraps() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let name = inner.to_string();

        assert_eq!(Store::wrap(inner).to_string(), name);
    }
}
