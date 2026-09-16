//! Experimental coalescing of physical reads without changing exact probes.

use std::{
    collections::BTreeMap,
    fmt,
    ops::Range,
    sync::{Arc, Mutex, atomic::Ordering},
};

use bytes::Bytes;
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};

use super::transport::Transport;

const MAX_PENDING: usize = 1024;
const MAX_BATCH_BYTES: u64 = 1024 * 1024;

#[derive(Debug)]
struct Pending {
    range: Range<u64>,
    result: tokio::sync::oneshot::Sender<object_store::Result<GetResult>>,
}

#[derive(Debug, Default)]
struct Queues {
    requests: BTreeMap<Path, Vec<Pending>>,
    pending: usize,
}

#[derive(Debug)]
pub(super) struct ReadBatch {
    inner: Arc<Transport>,
    queues: Arc<Mutex<Queues>>,
}

impl ReadBatch {
    pub(super) fn new(inner: Arc<Transport>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            queues: Arc::default(),
        })
    }

    async fn read(&self, path: &Path, range: Range<u64>) -> object_store::Result<GetResult> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let leader = {
            let mut queues = self.queues.lock().unwrap();
            if queues.pending == MAX_PENDING {
                None
            } else {
                queues.pending += 1;
                let queue = queues.requests.entry(path.clone()).or_default();
                let leader = queue.is_empty();
                queue.push(Pending {
                    range: range.clone(),
                    result: sender,
                });
                Some(leader)
            }
        };
        let Some(leader) = leader else {
            return self.inner.get_opts(path, options(range)).await;
        };
        if leader {
            let (inner, queues, path) = (self.inner.clone(), self.queues.clone(), path.clone());
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                let pending = {
                    let mut queues = queues.lock().unwrap();
                    let pending = queues.requests.remove(&path).unwrap_or_default();
                    queues.pending -= pending.len();
                    pending
                };
                let groups = partition(pending);
                // Keep disjoint byte ranges independent, with no global read cap.
                futures::future::join_all(
                    groups
                        .into_iter()
                        .map(|group| deliver(&inner, &path, group)),
                )
                .await;
            });
        }
        receiver
            .await
            .map_err(|error| object_store::Error::Generic {
                store: "experimental read batch",
                source: Box::new(error),
            })?
    }
}

fn eligible(path: &Path, options: &GetOptions) -> Option<Range<u64>> {
    if !std::path::Path::new(path.as_ref())
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"))
        || options.head
        || options.version.is_some()
        || options.if_match.is_some()
        || options.if_none_match.is_some()
        || options.if_modified_since.is_some()
        || options.if_unmodified_since.is_some()
        || !options.extensions.is_empty()
    {
        return None;
    }
    match &options.range {
        Some(GetRange::Bounded(range))
            if range.start < range.end && range.end - range.start <= MAX_BATCH_BYTES =>
        {
            Some(range.clone())
        }
        _ => None,
    }
}

fn options(range: Range<u64>) -> GetOptions {
    GetOptions {
        range: Some(GetRange::Bounded(range)),
        ..Default::default()
    }
}

fn partition(mut pending: Vec<Pending>) -> Vec<Vec<Pending>> {
    pending.retain(|pending| !pending.result.is_closed());
    pending.sort_by_key(|pending| pending.range.start);
    let mut groups: Vec<Vec<Pending>> = Vec::new();
    let mut bounds = 0..0;
    for request in pending {
        if !groups.is_empty()
            && request.range.start <= bounds.end
            && request.range.end.max(bounds.end) - bounds.start <= MAX_BATCH_BYTES
        {
            bounds.end = bounds.end.max(request.range.end);
            groups.last_mut().unwrap().push(request);
        } else {
            bounds = request.range.clone();
            groups.push(vec![request]);
        }
    }
    groups
}

async fn deliver(inner: &Transport, path: &Path, mut group: Vec<Pending>) {
    if group.len() == 1 {
        let pending = group.pop().unwrap();
        if !pending.result.is_closed() {
            let result = inner.get_opts(path, options(pending.range)).await;
            let _ = pending.result.send(result);
        }
        return;
    }
    let start = group[0].range.start;
    let end = group.iter().map(|pending| pending.range.end).max().unwrap();
    let combined = async {
        let result = inner.get_opts(path, options(start..end)).await?;
        let meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let range = result.range.clone();
        let bytes = result.bytes().await?;
        Ok::<_, object_store::Error>((meta, attributes, extensions, range, bytes))
    }
    .await;
    for pending in group {
        if pending.result.is_closed() {
            continue;
        }
        let result = if let Ok((meta, attributes, extensions, range, bytes)) = &combined
            && pending.range.start >= range.start
            && pending.range.start < range.end
        {
            let returned = pending.range.start..pending.range.end.min(range.end);
            let start = usize::try_from(returned.start - range.start).unwrap();
            let end = usize::try_from(returned.end - range.start).unwrap();
            if let Some(slice) = bytes.get(start..end) {
                // A cached 4 KiB block must not retain a whole merged response.
                let body = Bytes::copy_from_slice(slice);
                Ok(GetResult {
                    meta: meta.clone(),
                    attributes: attributes.clone(),
                    extensions: extensions.clone(),
                    range: returned,
                    payload: GetResultPayload::Stream(stream::once(async { Ok(body) }).boxed()),
                })
            } else {
                inner.get_opts(path, options(pending.range)).await
            }
        } else {
            // Preserve the original request's errors and bounds on any merge failure.
            inner.get_opts(path, options(pending.range)).await
        };
        let _ = pending.result.send(result);
    }
}

impl fmt::Display for ReadBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "experimental exact-probe read batching")
    }
}

#[async_trait::async_trait]
impl ObjectStore for ReadBatch {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if self.inner.coalescing.load(Ordering::Relaxed)
            && let Some(range) = eligible(path, &options)
        {
            return self.read(path, range).await;
        }
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
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

#[cfg(test)]
mod tests {
    use object_store::{GetRange, ObjectStoreExt};

    use super::*;

    #[tokio::test]
    async fn adjacent_ranges_share_a_get_without_sharing_response_bounds() {
        let inner = Transport::new();
        inner.coalescing.store(true, Ordering::Relaxed);
        let store = ReadBatch::new(inner.clone());
        let path = Path::from("sst/data.sst");
        store
            .put(&path, PutPayload::from_static(b"abcdefgh"))
            .await
            .unwrap();
        let first = GetOptions {
            range: Some(GetRange::Bounded(0..4)),
            ..Default::default()
        };
        let second = GetOptions {
            range: Some(GetRange::Bounded(4..8)),
            ..Default::default()
        };
        let (first, second) =
            tokio::join!(store.get_opts(&path, first), store.get_opts(&path, second));
        let (first, second) = (first.unwrap(), second.unwrap());
        assert_eq!(first.range, 0..4);
        assert_eq!(second.range, 4..8);
        assert_eq!(first.bytes().await.unwrap().as_ref(), b"abcd");
        assert_eq!(second.bytes().await.unwrap().as_ref(), b"efgh");
        assert_eq!(inner.counters().0, 1);
    }

    #[tokio::test]
    async fn gaps_conditions_and_invalid_ranges_keep_individual_semantics() {
        let inner = Transport::new();
        inner.coalescing.store(true, Ordering::Relaxed);
        let store = ReadBatch::new(inner.clone());
        let path = Path::from("sst/data.sst");
        store
            .put(&path, PutPayload::from_static(b"abcdefgh"))
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            store.get_opts(&path, options(0..2)),
            store.get_opts(&path, options(4..6))
        );
        assert_eq!(first.unwrap().bytes().await.unwrap().as_ref(), b"ab");
        assert_eq!(second.unwrap().bytes().await.unwrap().as_ref(), b"ef");
        assert_eq!(inner.counters(), (2, 4, 1));

        let conditional = GetOptions {
            if_match: Some("wrong".into()),
            ..options(0..4)
        };
        let (first, second) = tokio::join!(
            store.get_opts(&path, conditional),
            store.get_opts(&path, options(4..8))
        );
        assert!(matches!(
            first,
            Err(object_store::Error::Precondition { .. })
        ));
        assert_eq!(second.unwrap().bytes().await.unwrap().as_ref(), b"efgh");

        let (first, invalid) = tokio::join!(
            store.get_opts(&path, options(0..8)),
            store.get_opts(&path, options(8..12))
        );
        assert_eq!(first.unwrap().bytes().await.unwrap().as_ref(), b"abcdefgh");
        let original = inner.get_opts(&path, options(8..12)).await.unwrap_err();
        assert_eq!(invalid.unwrap_err().to_string(), original.to_string());
    }

    #[tokio::test]
    async fn overlapping_ranges_preserve_bytes_and_metadata() {
        let inner = Transport::new();
        inner.coalescing.store(true, Ordering::Relaxed);
        let store = ReadBatch::new(inner.clone());
        let path = Path::from("sst/data.sst");
        store
            .put(&path, PutPayload::from_static(b"abcdefgh"))
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            store.get_opts(&path, options(1..6)),
            store.get_opts(&path, options(4..20))
        );
        let (first, second) = (first.unwrap(), second.unwrap());
        assert_eq!(first.meta, second.meta);
        assert_eq!(second.range, 4..8);
        assert_eq!(first.bytes().await.unwrap().as_ref(), b"bcdef");
        assert_eq!(second.bytes().await.unwrap().as_ref(), b"efgh");
        assert_eq!(inner.counters().0, 1);
    }

    #[tokio::test]
    async fn cancelled_queued_reads_are_not_dispatched() {
        let inner = Transport::new();
        inner.coalescing.store(true, Ordering::Relaxed);
        let store = ReadBatch::new(inner.clone());
        let path = Path::from("sst/absent.sst");
        let mut read = Box::pin(store.get_opts(&path, options(0..4)));
        assert!(futures::poll!(&mut read).is_pending());
        drop(read);
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        assert_eq!(inner.counters().0, 0);
    }

    #[test]
    fn merged_windows_are_bounded_and_never_cross_gaps() {
        let ranges = [
            0..600_000,
            500_000..1_000_000,
            1_000_000..1_200_000,
            1_300_000..1_400_000,
        ];
        let mut receivers = Vec::new();
        let pending = ranges
            .into_iter()
            .map(|range| {
                let (result, receiver) = tokio::sync::oneshot::channel();
                receivers.push(receiver);
                Pending { range, result }
            })
            .collect();
        let groups = partition(pending);
        assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(), [2, 1, 1]);
    }
}
