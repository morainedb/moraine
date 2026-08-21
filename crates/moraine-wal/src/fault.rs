//! A fault-injecting [`ObjectStore`] over a real [`InMemory`] store: the
//! three ways a conditional put's outcome can be unreadable. Test support.

use std::sync::atomic::{AtomicU64, Ordering};

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

/// How a put fails, relative to the object landing.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PutFault {
    /// The object lands, then the response is lost.
    LostResponse,
    /// The put never reaches the store, so the slot stays absent.
    Unreachable,
    /// The store answers `AlreadyExists` with nothing written. Real S3
    /// returns 409 while a competing conditional create is in flight, and
    /// `object_store` maps every 409 to `AlreadyExists`.
    PrematureAlreadyExists,
}

/// Wraps a real [`InMemory`] store and fails `put_opts` at [`PutFault`], and
/// optionally `get_opts`, while faults remain; every other operation forwards
/// untouched.
#[derive(Debug)]
pub(crate) struct FaultyPut {
    inner: InMemory,
    fault: PutFault,
    puts: AtomicU64,
    gets: AtomicU64,
}

impl FaultyPut {
    /// Faults every put until disarmed.
    pub(crate) fn armed(fault: PutFault) -> Self {
        Self::failing(fault, u64::MAX)
    }

    /// Forwards every put until armed.
    pub(crate) fn disarmed(fault: PutFault) -> Self {
        Self::failing(fault, 0)
    }

    /// Faults the next `puts` puts, then forwards.
    pub(crate) fn failing(fault: PutFault, puts: u64) -> Self {
        Self {
            inner: InMemory::new(),
            fault,
            puts: AtomicU64::new(puts),
            gets: AtomicU64::new(0),
        }
    }

    /// Also fails the next `gets` gets, as a response that cannot be read.
    pub(crate) fn failing_gets(self, gets: u64) -> Self {
        self.gets.store(gets, Ordering::Relaxed);

        self
    }

    pub(crate) fn arm(&self) {
        self.puts.store(u64::MAX, Ordering::Relaxed);
    }

    pub(crate) fn disarm(&self) {
        self.puts.store(0, Ordering::Relaxed);
    }

    /// Claims one fault out of `budget`, if any remain. `u64::MAX` is sticky,
    /// so an armed store faults every operation.
    fn claim(budget: &AtomicU64) -> bool {
        let previous = budget
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                Some(if remaining == u64::MAX {
                    u64::MAX
                } else {
                    remaining.saturating_sub(1)
                })
            })
            .unwrap_or(0);

        previous > 0
    }
}

impl std::fmt::Display for FaultyPut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultyPut({})", self.inner)
    }
}

/// What a lost response looks like from the caller's side: no information
/// about whether the object exists. Its text carries every substring a retry
/// loop keys on, so the wrapping is exercised too.
pub(crate) fn unknown_outcome() -> object_store::Error {
    object_store::Error::Generic {
        store: "fault",
        source: "the put's outcome is unknown: conflict, concurrent, unique, primary key".into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultyPut {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if !Self::claim(&self.puts) {
            return self.inner.put_opts(location, payload, opts).await;
        }

        match self.fault {
            PutFault::LostResponse => {
                self.inner.put_opts(location, payload, opts).await?;
                Err(unknown_outcome())
            }
            PutFault::Unreachable => Err(unknown_outcome()),
            PutFault::PrematureAlreadyExists => Err(object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: "409 while a competing conditional create is in flight".into(),
            }),
        }
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
        if Self::claim(&self.gets) {
            return Err(unknown_outcome());
        }

        self.inner.get_opts(location, options).await
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
