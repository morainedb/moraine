//! Locating stable row ids within one immutable data file.

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use futures::TryStreamExt;
use tracing::debug;

use crate::{
    data_file::{
        ParquetFile, RowIdSource, ScopedRows, auxiliary_cache, carries_embedded_row_ids, metrics,
        row_set::{FileRowSet, PositionedRowSet, RowOrder},
        scoped_read_entry_batches, sidecar,
    },
    error::Result,
};

/// Concurrent summary builds and per-pass table/file fan-out.
pub(crate) const PUBLISH_CONCURRENCY: usize = 4;

// Bound complete builds, including decoded row IDs and encoded sidecars, across
// catalogs.
static SUMMARY_SLOTS: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(4)));
static BACKGROUND_SUMMARY_SLOTS: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(2)));

static PUBLISH_SLOTS: LazyLock<tokio::sync::Semaphore> =
    LazyLock::new(|| tokio::sync::Semaphore::new(PUBLISH_CONCURRENCY));

type PublishKey = (crate::CacheIdentity, String, u64, u64, u64);
static PUBLISHING: LazyLock<Mutex<HashMap<PublishKey, Weak<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn publishing_file(file: &ParquetFile, table: u64, data_file: u64) -> Arc<tokio::sync::Mutex<()>> {
    let mut active = PUBLISHING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    active.retain(|_, lock| lock.strong_count() != 0);
    let key = (
        file.store.cache_identity(),
        file.path.to_string(),
        file.file_size,
        table,
        data_file,
    );
    if let Some(lock) = active.get(&key).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    active.insert(key, Arc::downgrade(&lock));
    lock
}

/// What a sidecar read asks for before it has a header to size it by,
/// chosen above the membership a million-row file publishes so the common
/// read is one request.
const SIDECAR_PREFIX_BYTES: u64 = 4 << 20;

/// What a caller needs of a summary. Every lookup tests membership; only a
/// located delete or update resolves positions, and only it pays for them.
///
/// Reads never publish. A query answers its caller and writes nothing to
/// the data path; publishing is something an embedder asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Want {
    /// Membership alone, which reads no order from a published summary.
    Membership,
    /// Membership and the order that positions it.
    Positions,
}

/// What a summary holds, which is what its caller asked for.
#[derive(Clone)]
enum Held {
    Membership(Arc<FileRowSet>),
    Positioned(Arc<PositionedRowSet>),
}

/// One file's row-id membership, and what it cost to obtain.
#[derive(Clone)]
pub(crate) struct FileSummary {
    rows: Held,
    /// Whether this call read the file's row-id column and cached the
    /// result.
    pub(crate) built: bool,
}

impl FileSummary {
    fn set(&self) -> &FileRowSet {
        match &self.rows {
            Held::Membership(rows) => rows,
            Held::Positioned(positioned) => &positioned.rows,
        }
    }

    /// Whether this summary carries the order positions are read from.
    pub(crate) fn resolves_positions(&self) -> bool {
        matches!(self.rows, Held::Positioned(_))
    }

    /// Estimated resident bytes of what this summary holds.
    pub(crate) fn estimated_bytes(&self) -> u64 {
        match &self.rows {
            Held::Membership(rows) => rows.estimated_bytes(),
            Held::Positioned(positioned) => positioned.estimated_bytes(),
        }
    }

    /// Verified dense membership, irrespective of how the file records row
    /// IDs. A permuted file's set is never a range, so the set alone
    /// settles this.
    pub(crate) fn dense_range(&self) -> Option<std::ops::Range<u64>> {
        match self.set() {
            FileRowSet::Range { start, end } => Some(*start..*end),
            _ => None,
        }
    }

    /// Which of `requested` this file holds, in request order.
    #[cfg(test)]
    pub(crate) fn matching(&self, requested: &[u64]) -> Vec<u64> {
        self.set().matching(requested)
    }

    pub(crate) fn contains(&self, row_id: u64) -> bool {
        self.set().contains(row_id)
    }

    /// The least and greatest row id this file holds, `None` when empty.
    pub(crate) fn bounds(&self) -> Option<(u64, u64)> {
        self.set().bounds()
    }

    /// File positions of `requested` rows within this file; `None` for rows
    /// this file does not hold.
    #[cfg(test)]
    pub(crate) fn positions_of(&self, requested: &[u64]) -> Vec<Option<u64>> {
        match &self.rows {
            Held::Positioned(positioned) => positioned.positions_of(requested),
            Held::Membership(_) => vec![None; requested.len()],
        }
    }

    /// Visits every physical version of `row_id`; `Ok(false)` means it is
    /// absent. A summary fetched for membership alone has no order to
    /// visit, which is a caller asking for what it did not request.
    pub(crate) fn visit_positions(&self, row_id: u64, visit: impl FnMut(u64)) -> Result<bool> {
        match &self.rows {
            Held::Positioned(positioned) => Ok(positioned.visit_positions(row_id, visit)),
            Held::Membership(_) => Err(crate::error::Error::Constraint(
                "a summary fetched for membership cannot resolve positions".to_owned(),
            )),
        }
    }
}

/// This file's row-id membership, from the cache when it is resident. A
/// file whose ids are the recorded dense range answers from its footer and
/// is remembered as that range; one carrying the reserved row-id column
/// reads only that column and is cached.
pub(crate) async fn file_summary(
    file: ParquetFile,
    table_id: u64,
    data_file_id: u64,
    row_id_start: Option<u64>,
    record_count: u64,
    want: Want,
) -> Result<FileSummary> {
    let path = file.path.clone();
    let file_size = file.file_size;
    let store = file.store.clone();
    let key = auxiliary_cache::FileSummaryKey {
        table_id,
        data_file_id,
        path: &path,
        file_size,
    };

    if let Some(rows) = auxiliary_cache::shared().summary(&store, &key).await {
        return Ok(FileSummary {
            rows: Held::Positioned(rows),
            built: false,
        });
    }

    if let Some(start) = row_id_start
        && !carries_embedded_row_ids(file.clone()).await?
    {
        let rows = Arc::new(PositionedRowSet {
            rows: FileRowSet::range(start, record_count)?.into(),
            order: RowOrder::Ascending,
        });
        auxiliary_cache::shared().insert_summary(&store, &key, &rows);

        Ok(FileSummary {
            rows: Held::Positioned(rows),
            built: false,
        })
    } else {
        let built = Arc::new(AtomicBool::new(false));
        let rows = match want {
            Want::Positions => {
                let read = {
                    let built = Arc::clone(&built);
                    move || async move {
                        let identity = identity_of(&file, table_id, data_file_id);
                        if let Some(rows) = published_summary(&file, identity).await {
                            return Ok(rows);
                        }

                        built.store(true, Ordering::Relaxed);
                        read_row_ids(file.clone(), row_id_start).await
                    }
                };
                Held::Positioned(
                    auxiliary_cache::shared()
                        .fetch_summary(&store, &key, read)
                        .await?,
                )
            }
            Want::Membership => {
                let read = {
                    let built = Arc::clone(&built);
                    let store = store.clone();
                    move || async move {
                        let identity = identity_of(&file, table_id, data_file_id);
                        if let Some(rows) = published_membership(&file, identity).await {
                            return Ok(rows);
                        }

                        built.store(true, Ordering::Relaxed);
                        let rows = read_row_ids(file.clone(), row_id_start).await?;
                        // The whole summary is in hand, so a later caller
                        // wanting positions finds it rather than reading
                        // the column over again.
                        auxiliary_cache::shared().insert_summary(
                            &store,
                            &auxiliary_cache::FileSummaryKey {
                                table_id,
                                data_file_id,
                                path: &file.path,
                                file_size: file.file_size,
                            },
                            &rows,
                        );
                        Ok(Arc::clone(&rows.rows))
                    }
                };
                let membership = auxiliary_cache::shared()
                    .fetch_membership(&store, &key, read)
                    .await?;
                // A fill that had to read the column cached the whole
                // summary as well. Prefer it: the order is already paid
                // for, and holding it spares a later position the read.
                match auxiliary_cache::shared().summary(&store, &key).await {
                    Some(positioned) => Held::Positioned(positioned),
                    None => Held::Membership(membership),
                }
            }
        };

        Ok(FileSummary {
            rows,
            built: built.load(Ordering::Relaxed),
        })
    }
}

/// Publishes `file`'s summary unless one is already published for it,
/// returning whether this call wrote one.
///
/// Neither reads nor fills the summary caches: a backfill over a lake
/// should not evict the working set of the process running it.
///
/// # Errors
///
/// Returns an error if reading row IDs, encoding, or writing the summary fails.
pub(crate) async fn publish_if_missing(
    mut file: ParquetFile,
    table_id: u64,
    data_file_id: u64,
    row_id_start: Option<u64>,
) -> Result<bool> {
    let _permit = PUBLISH_SLOTS.acquire().await.map_err(|error| {
        crate::Error::Interrupted(format!("summary publishing admission: {error}"))
    })?;
    // Take the file lock after admission, bounding the lock registry by active
    // builders.
    let _file = publishing_file(&file, table_id, data_file_id)
        .lock_owned()
        .await;
    file.store = file.store.background();
    // Settled before the store is asked anything: a dense file needs no
    // summary, so probing for one would be a guaranteed miss per file per
    // pass.
    if row_id_start.is_some() && !carries_embedded_row_ids(file.clone()).await? {
        return Ok(false);
    }

    let identity = identity_of(&file, table_id, data_file_id);
    if sidecar::is_published(&file.store, &file.path, identity).await {
        return Ok(false);
    }

    let maximum_rows = summary_memory_limit()? / 64;
    let counts = super::row_group_row_counts(&file).await?;
    let admitted = counts.into_iter().try_fold(0_usize, |total, rows| {
        usize::try_from(rows)
            .ok()
            .and_then(|rows| total.checked_add(rows))
            .filter(|&rows| rows <= maximum_rows)
    });
    if admitted.is_none() {
        debug!(path = %file.path, maximum_rows, "summary publication exceeds its build allowance");
        return Ok(false);
    }
    let rows = Box::pin(read_row_ids_limited(
        file.clone(),
        row_id_start,
        maximum_rows,
    ))
    .await?;
    publish_summary(&file, identity, &rows).await?;
    Ok(true)
}

/// The file a sidecar beside `file` must name to be believed.
fn identity_of(
    file: &ParquetFile,
    table_id: u64,
    data_file_id: u64,
) -> sidecar::SidecarIdentity<'_> {
    sidecar::SidecarIdentity {
        table_id,
        data_file_id,
        file_path: file.path.as_ref(),
        file_size: file.file_size,
    }
}

/// The membership published beside `file`, read without its order.
async fn published_membership(
    file: &ParquetFile,
    identity: sidecar::SidecarIdentity<'_>,
) -> Option<Arc<FileRowSet>> {
    let path = sidecar::path_for(&file.path).ok()?;
    let Ok(prefix) = file
        .store
        .read_prefix(
            &path,
            SIDECAR_PREFIX_BYTES.min(summary_memory_limit().ok()? as u64),
        )
        .await
    else {
        metrics::sidecar_miss();
        return None;
    };

    // The membership sits before the order, so one bounded read covers it
    // unless the set alone outruns the speculation.
    let bytes = match sidecar::layout_of(identity, &prefix) {
        Ok(layout) if layout.membership_end() <= prefix.len() => prefix,
        Ok(layout) => match continued(file, &path, prefix, layout.membership_end()).await {
            Some(bytes) => bytes,
            None => return refused_membership(&path, sidecar::Rejected::Malformed),
        },
        Err(rejection) => return refused_membership(&path, rejection),
    };

    match sidecar::membership(identity, &bytes) {
        Ok(rows) => {
            metrics::sidecar_hit();
            Some(Arc::new(rows))
        }
        Err(rejection) => refused_membership(&path, rejection),
    }
}

fn refused_membership(
    path: &object_store::path::Path,
    rejection: sidecar::Rejected,
) -> Option<Arc<FileRowSet>> {
    debug!(path = %path, ?rejection, "a published row summary was refused");
    metrics::sidecar_refused();
    metrics::sidecar_miss();
    None
}

/// The summary published beside `file`, or `None` when there is none to
/// have. Every failure is the same answer — derive it instead — so nothing
/// here returns an error to a caller that has a way of its own.
async fn published_summary(
    file: &ParquetFile,
    identity: sidecar::SidecarIdentity<'_>,
) -> Option<Arc<PositionedRowSet>> {
    let path = sidecar::path_for(&file.path).ok()?;
    let Ok(prefix) = file
        .store
        .read_prefix(
            &path,
            SIDECAR_PREFIX_BYTES.min(summary_memory_limit().ok()? as u64),
        )
        .await
    else {
        metrics::sidecar_miss();
        return None;
    };

    // One read covers the whole sidecar unless the file is large enough to
    // outrun the speculation, in which case its header says by how much.
    let bytes = match sidecar::layout_of(identity, &prefix) {
        Ok(layout) => {
            let whole = layout.membership_end().saturating_add(layout.order_len);
            if whole <= prefix.len() {
                prefix
            } else {
                match continued(file, &path, prefix, whole).await {
                    Some(bytes) => bytes,
                    None => return refused(&path, sidecar::Rejected::Malformed),
                }
            }
        }
        Err(rejection) => return refused(&path, rejection),
    };

    match sidecar::summary(identity, &bytes) {
        Ok(rows) => {
            metrics::sidecar_hit();
            Some(Arc::new(rows))
        }
        Err(rejection) => refused(&path, rejection),
    }
}

/// `prefix` extended to `end` by reading only what it is missing, so a
/// summary that outran the speculative read is not fetched twice.
async fn continued(
    file: &ParquetFile,
    path: &object_store::path::Path,
    prefix: bytes::Bytes,
    end: usize,
) -> Option<bytes::Bytes> {
    if end > summary_memory_limit().ok()? / 2 {
        return None;
    }
    let from = u64::try_from(prefix.len()).ok()?;
    let to = u64::try_from(end).ok()?;
    let rest = file.store.read_range(path, from..to).await.ok()?;
    if rest.len() != end.checked_sub(prefix.len())? {
        return None;
    }

    let mut bytes = Vec::with_capacity(end);
    bytes.extend_from_slice(&prefix);
    bytes.extend_from_slice(&rest);
    Some(bytes.into())
}

/// A sidecar that was there and would not answer: counted apart from one
/// that was never published, so a deployment can tell them apart.
fn refused(
    path: &object_store::path::Path,
    rejection: sidecar::Rejected,
) -> Option<Arc<PositionedRowSet>> {
    debug!(path = %path, ?rejection, "a published row summary was refused");
    metrics::sidecar_refused();
    metrics::sidecar_miss();
    None
}

/// Writes a summary and reports encoding or storage failures to the publishing
/// pass.
async fn publish_summary(
    file: &ParquetFile,
    identity: sidecar::SidecarIdentity<'_>,
    rows: &Arc<PositionedRowSet>,
) -> Result<()> {
    let result = async {
        let path = sidecar::path_for(&file.path)?;
        let permit = Arc::clone(&SUMMARY_SLOTS)
            .acquire_owned()
            .await
            .map_err(|error| {
                crate::Error::Interrupted(format!("summary encoding admission: {error}"))
            })?;
        let owned_file = file.clone();
        let table = identity.table_id;
        let data_file = identity.data_file_id;
        let memory = crate::MemoryReservation::reserve(
            rows.estimated_bytes()
                .saturating_mul(4)
                .saturating_add(4096),
        )?;
        let rows = Arc::clone(rows);
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelOnDrop(Arc::clone(&cancelled));
        let bytes = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let bytes = sidecar::encode_checked(
                identity_of(&owned_file, table, data_file),
                &rows,
                &mut || {
                    if cancelled.load(Ordering::Relaxed) {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "summary encoding cancelled",
                        ))
                    } else {
                        Ok(())
                    }
                },
            )?;
            memory.into_bytes(bytes.into())
        })
        .await
        .map_err(|error| crate::Error::Interrupted(format!("summary encoding: {error}")))??;
        file.store.write(&path, bytes).await.map_err(|error| {
            crate::Error::from(
                slatedb::Error::unavailable("publishing row summary".to_owned())
                    .with_source(Box::new(error)),
            )
        })
    }
    .await;
    metrics::sidecar_published(result.is_ok());
    result
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

fn summary_memory_limit() -> Result<usize> {
    match std::env::var("MORAINE_SUMMARY_MEMORY_BYTES") {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|&value| value >= 64)
            .ok_or_else(|| {
                crate::Error::Configuration(
                    "MORAINE_SUMMARY_MEMORY_BYTES must be at least 64".into(),
                )
            }),
        Err(std::env::VarError::NotPresent) => Ok(256 * 1024 * 1024),
        Err(_) => Err(crate::Error::Configuration(
            "MORAINE_SUMMARY_MEMORY_BYTES must be UTF-8".into(),
        )),
    }
}

async fn read_row_ids(
    file: ParquetFile,
    row_id_start: Option<u64>,
) -> Result<Arc<PositionedRowSet>> {
    let bytes = summary_memory_limit()?;
    Box::pin(read_row_ids_limited(file, row_id_start, bytes / 64)).await
}

pub(super) async fn read_row_ids_limited(
    file: ParquetFile,
    row_id_start: Option<u64>,
    maximum_rows: usize,
) -> Result<Arc<PositionedRowSet>> {
    let admission_error =
        |error| crate::Error::Interrupted(format!("summary build admission: {error}"));
    let background = if file.store.is_background() {
        Some(
            Arc::clone(&BACKGROUND_SUMMARY_SLOTS)
                .acquire_owned()
                .await
                .map_err(admission_error)?,
        )
    } else {
        None
    };
    let permit = Arc::clone(&SUMMARY_SLOTS)
        .acquire_owned()
        .await
        .map_err(admission_error)?;
    let too_large = || {
        crate::Error::Constraint(format!(
            "summary memory exceeds {maximum_rows} rows; raise MORAINE_SUMMARY_MEMORY_BYTES"
        ))
    };
    let counts = super::row_group_row_counts(&file).await?;
    let rows = counts.into_iter().try_fold(0_usize, |total, rows| {
        usize::try_from(rows)
            .ok()
            .and_then(|rows| total.checked_add(rows))
            .filter(|&rows| rows <= maximum_rows)
            .ok_or_else(too_large)
    })?;
    let memory = crate::MemoryReservation::reserve((rows as u64).saturating_mul(64))?;
    let mut row_ids = Vec::new();
    row_ids.try_reserve_exact(rows).map_err(|_| too_large())?;
    let mut batches = scoped_read_entry_batches(
        file,
        &[],
        ScopedRows::All,
        RowIdSource::Resolve { row_id_start },
    )
    .await?;
    while let Some(batch) = batches.try_next().await? {
        if batch.len() > rows.saturating_sub(row_ids.len()) {
            return Err(too_large());
        }
        row_ids.extend(batch.iter().map(|entry| entry.row_id));
        tokio::task::yield_now().await;
    }
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel = CancelOnDrop(Arc::clone(&cancelled));
    // Permits move with CPU work: cancelling its waiter cannot admit another
    // build while the blocking task still owns its buffers.
    tokio::task::spawn_blocking(move || {
        let (_permit, _background, _memory) = (permit, background, memory);
        PositionedRowSet::from_file_order_checked(row_ids, || {
            if cancelled.load(Ordering::Relaxed) {
                Err(crate::Error::Interrupted("summary build cancelled".into()))
            } else {
                Ok(())
            }
        })
        .map(Arc::new)
    })
    .await
    .map_err(|error| crate::Error::Interrupted(format!("summary build: {error}")))?
}

/// Resolves requested IDs in batches when a whole-file summary is unavailable.
pub(crate) async fn requested_positions(
    file: ParquetFile,
    row_id_start: Option<u64>,
    requested: &[u64],
) -> Result<(Vec<u64>, Option<u64>)> {
    let mut found: HashMap<u64, bool> = requested.iter().map(|&row| (row, false)).collect();
    let mut positions = Vec::new();
    let mut batches = scoped_read_entry_batches(
        file,
        &[],
        ScopedRows::All,
        RowIdSource::Resolve { row_id_start },
    )
    .await?;
    while let Some(batch) = batches.try_next().await? {
        for entry in batch {
            if let Some(found) = found.get_mut(&entry.row_id) {
                *found = true;
                positions.push(entry.ordinal);
            }
        }
        tokio::task::yield_now().await;
    }
    let missing = requested
        .iter()
        .copied()
        .find(|row| found.get(row) == Some(&false));
    Ok((positions, missing))
}
