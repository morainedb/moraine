//! Locating stable row ids within one immutable data file.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
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
            rows: FileRowSet::range(start, record_count)?,
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
                        Ok(Arc::new(rows.rows.clone()))
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
/// Returns a store error if the file's row-id column cannot be read.
pub(crate) async fn publish_if_missing(
    file: ParquetFile,
    table_id: u64,
    data_file_id: u64,
    row_id_start: Option<u64>,
) -> Result<bool> {
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

    let rows = read_row_ids(file.clone(), row_id_start).await?;
    publish_summary(&file, identity, &rows).await;
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
    let Ok(prefix) = file.store.read_prefix(&path, SIDECAR_PREFIX_BYTES).await else {
        metrics::sidecar_miss();
        return None;
    };

    // The membership sits before the order, so one bounded read covers it
    // unless the set alone outruns the speculation.
    let bytes = match sidecar::layout_of(identity, &prefix) {
        Ok(layout) if layout.membership_end() <= prefix.len() => prefix.to_vec(),
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
    let Ok(prefix) = file.store.read_prefix(&path, SIDECAR_PREFIX_BYTES).await else {
        metrics::sidecar_miss();
        return None;
    };

    // One read covers the whole sidecar unless the file is large enough to
    // outrun the speculation, in which case its header says by how much.
    let bytes = match sidecar::layout_of(identity, &prefix) {
        Ok(layout) => {
            let whole = layout.membership_end().saturating_add(layout.order_len);
            if whole <= prefix.len() {
                prefix.to_vec()
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
) -> Option<Vec<u8>> {
    let from = u64::try_from(prefix.len()).ok()?;
    let to = u64::try_from(end).ok()?;
    let rest = file.store.read_range(path, from..to).await.ok()?;

    let mut bytes = Vec::with_capacity(end);
    bytes.extend_from_slice(&prefix);
    bytes.extend_from_slice(&rest);
    Some(bytes)
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

/// Publishes a derived summary for whichever process asks next, awaited
/// so a pass that reports what it published has published it.
///
/// A failure is logged and otherwise ignored: the summary is already in
/// hand for the caller, and the next pass derives it again.
async fn publish_summary(
    file: &ParquetFile,
    identity: sidecar::SidecarIdentity<'_>,
    rows: &PositionedRowSet,
) {
    let published = match sidecar::path_for(&file.path).and_then(|path| {
        sidecar::encode(identity, rows).map(|bytes| (path, bytes::Bytes::from(bytes)))
    }) {
        Ok(published) => published,
        Err(error) => {
            debug!(%error, "a row summary would not encode for publishing");
            metrics::sidecar_published(false);
            return;
        }
    };

    let (path, bytes) = published;
    match file.store.write(&path, bytes).await {
        Ok(()) => metrics::sidecar_published(true),
        Err(error) => {
            debug!(path = %path, %error, "a row summary could not be published");
            metrics::sidecar_published(false);
        }
    }
}

async fn read_row_ids(
    file: ParquetFile,
    row_id_start: Option<u64>,
) -> Result<Arc<PositionedRowSet>> {
    let row_ids_in_file_order = scoped_read_entry_batches(
        file,
        &[],
        ScopedRows::All,
        RowIdSource::Resolve { row_id_start },
    )
    .await?
    .try_fold(Vec::new(), |mut row_ids, batch| async move {
        row_ids.extend(batch.iter().map(|entry| entry.row_id));
        Ok(row_ids)
    })
    .await?;

    Ok(Arc::new(PositionedRowSet::from_file_order(
        row_ids_in_file_order,
    )?))
}
