//! Index entry maintenance: turning writer-supplied entries into staged
//! `index` writes, with commit-time uniqueness enforcement. A unique
//! entry's store key is the value, so two commits inserting the same value
//! collide on write-write detection.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    ops::Bound,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, future::BoxFuture, stream};
use tracing::warn;

use crate::{
    data_file::ScopedReadTally,
    error::{Error, Result},
    store::{
        StagedBytes,
        handle::{ReadHandle, ScanOrder, ScanShape},
        index_encoding::{CanonicalKey, NullOrder, non_null_flag_key},
        key::{
            IndexKey, IndexKind, Key, encode_index_entry, index_index_prefix,
            index_multi_value_prefix, index_value_above, index_value_body, index_value_suffix,
        },
    },
};

/// One index-entry mutation accumulated during a commit closure, resolved
/// against the store when the batch is staged.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StagedIndexEntry {
    /// The index this entry belongs to.
    pub(crate) index_id: u64,
    /// Whether the index is unique.
    pub(crate) unique: bool,
    /// The final encoded SlateDB entry key.
    pub(crate) key: Bytes,
    /// The row the entry points at.
    pub(crate) row_id: u64,
    /// Whether this removes the entry rather than adds it.
    pub(crate) delete: bool,
    /// Whether the index is still building; a collision then poisons it
    /// instead of failing the commit.
    pub(crate) building: bool,
}

/// A unique entry's value is the holding row id, big-endian.
fn decode_row_id(bytes: &[u8]) -> Result<u64> {
    let array: [u8; 8] = bytes.try_into().map_err(|_| {
        Error::Corruption(format!(
            "index entry value is {} bytes, expected 8",
            bytes.len()
        ))
    })?;
    Ok(u64::from_be_bytes(array))
}

/// Uniqueness probes in flight at once; sized for a remote object store.
const UNIQUENESS_PROBE_CONCURRENCY: usize = 1024;

/// Additions derived while the deletion phase is still draining. Once full,
/// backpressure pauses addition sources without holding up deletion staging.
const ADDITION_PREFETCH: usize = 512;

/// The most entries one commit may stage. Each staged key costs roughly a
/// kilobyte in the store's write path, so this admits commits needing
/// about 8 GiB.
const MAX_INDEX_ENTRIES_PER_COMMIT: usize = 8_000_000;

/// What staging a batch's entries produced: the indexes a collision
/// poisoned, and the bytes the entries put on the batch.
pub(crate) struct StagedEntries {
    /// The entry writes to stage, deletes first, then puts: staging them in
    /// order makes a delete-then-reinsert of one unique value within a commit
    /// see the value as absent.
    pub(crate) writes: Vec<crate::transaction::commit::StagedWrite>,
    /// Definitions a duplicate poisoned, sorted and deduplicated.
    pub(crate) poisoned: Vec<u64>,
    /// Deferred definitions whose SQL additions committed without entries.
    pub(crate) deferred: Vec<u64>,
    /// Key and value bytes staged, deletes included.
    pub(crate) bytes: u64,
    /// Inline chunk-directory repairs derived along the way, handed back
    /// rather than staged so the caller can order them below the batch's
    /// own directory writes.
    pub(crate) locator_writes: Vec<crate::transaction::commit::StagedWrite>,
    pub(crate) metrics: IndexMaintenanceMetrics,
}

/// Per-commit equality-index work. Derivation and probe windows overlap.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct IndexMaintenanceMetrics {
    pub(crate) deletion_derivation: Duration,
    pub(crate) addition_derivation: Duration,
    pub(crate) probe_window: Duration,
    pub(crate) probe_service: Duration,
    pub(crate) staging: Duration,
    pub(crate) scoped_read: ScopedReadTally,
    pub(crate) additions: u64,
    pub(crate) deletions: u64,
    pub(crate) unique_probes: u64,
    pub(crate) probe_hits: u64,
    pub(crate) probe_misses: u64,
    pub(crate) probe_peak_in_flight: u64,
    pub(crate) probes_completed_during_deletions: u64,
}

/// One unique put awaiting its committed-state probe.
struct PendingProbe {
    /// The entry's encoded store key.
    key: Bytes,
    /// The row claiming the value.
    row_id: u64,
    /// The index the claim belongs to.
    index_id: u64,
    /// Whether that index is still building.
    building: bool,
}

/// Work one streamed index entry requires.
enum ProbePlan {
    /// Stage a non-unique put without a read.
    Put(Bytes),
    /// Read committed state before deciding whether to stage the put.
    Probe(PendingProbe),
    /// A repeated claim by the same row needs no write.
    Noop,
    /// Two rows in this batch claim one unique value.
    Collision { index_id: u64, building: bool },
    /// Input, limit, or derivation failure at this stream position.
    Failure(Error),
}

/// A completed plan, carrying a probe's value when it needed a read.
enum ReadyAddition {
    Put(Bytes),
    Probed(CompletedProbe),
    Poison(u64),
}

struct CompletedProbe {
    probe: PendingProbe,
    present: Option<Bytes>,
    service: Duration,
    completed: Instant,
}

/// Sequential planning state. Claims are recorded before probes start, so
/// concurrent reads cannot admit duplicate values from the same commit.
struct ProbePlanner {
    claimed: HashMap<Bytes, u64>,
}

impl ProbePlanner {
    fn plan(&mut self, entry: Result<StagedIndexEntry>, entry_count: &mut usize) -> ProbePlan {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => return ProbePlan::Failure(error),
        };
        *entry_count = entry_count.saturating_add(1);
        if *entry_count > MAX_INDEX_ENTRIES_PER_COMMIT {
            return ProbePlan::Failure(oversized_commit(*entry_count));
        }
        if entry.delete {
            return ProbePlan::Failure(Error::Corruption(
                "index put stream contains a deletion".to_owned(),
            ));
        }
        if !entry.unique {
            return ProbePlan::Put(entry.key);
        }

        if let Some(&holder) = self.claimed.get(&entry.key) {
            return if holder == entry.row_id {
                ProbePlan::Noop
            } else {
                ProbePlan::Collision {
                    index_id: entry.index_id,
                    building: entry.building,
                }
            };
        }
        self.claimed.insert(entry.key.clone(), entry.row_id);

        ProbePlan::Probe(PendingProbe {
            key: entry.key,
            row_id: entry.row_id,
            index_id: entry.index_id,
            building: entry.building,
        })
    }
}

/// A collision's verdict: fail the commit, or poison the building index.
fn collision(probe_index_id: u64, building: bool) -> Result<Option<u64>> {
    if building {
        Ok(Some(probe_index_id))
    } else {
        Err(unique_violation(probe_index_id))
    }
}

async fn resolve_probe(reader: ProbeHandle<'_>, probe: PendingProbe) -> Result<CompletedProbe> {
    let started = Instant::now();
    let present = reader.get(&probe.key).await?;

    Ok(CompletedProbe {
        probe,
        present,
        service: started.elapsed(),
        completed: Instant::now(),
    })
}

fn schedule_probe_plan<'a>(
    plan: ProbePlan,
    reader: ProbeHandle<'a>,
    probes: &mut futures::stream::FuturesUnordered<BoxFuture<'a, Result<CompletedProbe>>>,
    ready: &mut VecDeque<ReadyAddition>,
    metrics: &mut IndexMaintenanceMetrics,
    first_probe: &mut Option<Instant>,
) -> Result<()> {
    match plan {
        ProbePlan::Put(key) => ready.push_back(ReadyAddition::Put(key)),
        ProbePlan::Probe(probe) => {
            first_probe.get_or_insert_with(Instant::now);
            metrics.unique_probes = metrics.unique_probes.saturating_add(1);
            probes.push(resolve_probe(reader, probe).boxed());
            metrics.probe_peak_in_flight = metrics
                .probe_peak_in_flight
                .max(u64::try_from(probes.len()).unwrap_or(u64::MAX));
        }
        ProbePlan::Noop => {}
        ProbePlan::Collision { index_id, building } => {
            if let Some(index_id) = collision(index_id, building)? {
                ready.push_back(ReadyAddition::Poison(index_id));
            }
        }
        ProbePlan::Failure(error) => return Err(error),
    }
    Ok(())
}

/// Drops a unique deletion whose entry is held by some other row.
///
/// A unique entry's key is the value alone, so deleting it by key removes
/// whichever row holds that value — not necessarily the row being removed.
/// The stored row id is what says the two are the same, and a derivation
/// that names an already-removed row must not take the entry a later
/// insert of the same value put there.
fn guarded_deletions<'a, D>(
    reader: ProbeHandle<'a>,
    deletes: D,
) -> impl Stream<Item = Result<StagedIndexEntry>> + 'a
where
    D: Stream<Item = Result<StagedIndexEntry>> + 'a,
{
    deletes
        .map(move |entry| async move {
            let entry = entry?;
            if !entry.unique {
                return Ok(Some(entry));
            }
            let Some(bytes) = reader.get(&entry.key).await? else {
                return Ok(None);
            };
            Ok((decode_row_id(&bytes)? == entry.row_id).then_some(entry))
        })
        .buffer_unordered(UNIQUENESS_PROBE_CONCURRENCY)
        .try_filter_map(|entry| std::future::ready(Ok(entry)))
}

/// Plans one entry put: a unique entry's value is its holding row id, a
/// non-unique entry's is empty.
fn plan_probe_put(
    writes: &mut Vec<crate::transaction::commit::StagedWrite>,
    staged: &mut StagedBytes,
    key: &Bytes,
    row_id: Option<u64>,
) {
    let value = row_id.map_or_else(Vec::new, |row_id| row_id.to_be_bytes().to_vec());
    staged.add(key.len(), value.len());
    writes.push((key.to_vec(), Some(value)));
}

/// Resolves accumulated entries into planned writes, enforcing uniqueness at
/// commit against `reader` — the folded store overlaid with the unfolded
/// tail, so a value claimed by a committed but unfolded slot is seen. For each
/// unique put: present with a different row id → [`Error::Constraint`];
/// present with the same row id → no-op; absent → planned. Duplicates within
/// the commit are caught in memory.
///
/// A collision against an index still **building** poisons it instead: the id
/// comes back in `poisoned`, the claim is dropped so the live holder's entry
/// survives, and the commit proceeds. Coverage is partial until a build flips
/// ready, so failing the finder would decide by timing which party a duplicate
/// falls on.
pub(crate) async fn plan_index_entries(
    reader: ProbeHandle<'_>,
    entries: Vec<StagedIndexEntry>,
) -> Result<StagedEntries> {
    let entry_count = entries.len();
    if entry_count > MAX_INDEX_ENTRIES_PER_COMMIT {
        warn!(
            staged = entry_count,
            limit = MAX_INDEX_ENTRIES_PER_COMMIT,
            "refusing an oversized commit"
        );
        return Err(oversized_commit(entry_count));
    }

    let (deletes, puts): (Vec<_>, Vec<_>) = entries.into_iter().partition(|entry| entry.delete);
    let deletes = stream::iter(deletes.into_iter().map(Ok));
    let puts = stream::iter(puts.into_iter().map(Ok));
    plan_index_entry_stream(reader, deletes, puts, 0).await
}

/// Consumes deletion entries, then additions, as streams. Unique probes form
/// one continuously replenished window; successful reads stage in
/// completion order, and the first fatal result aborts.
#[allow(clippy::too_many_lines)]
pub(crate) async fn plan_index_entry_stream<D, S>(
    reader: ProbeHandle<'_>,
    deletes: D,
    entries: S,
    prior_entry_count: usize,
) -> Result<StagedEntries>
where
    D: Stream<Item = Result<StagedIndexEntry>>,
    S: Stream<Item = Result<StagedIndexEntry>>,
{
    let started = Instant::now();
    let mut staged = StagedBytes::default();
    let mut writes: Vec<crate::transaction::commit::StagedWrite> = Vec::new();
    let mut deleted_unique = HashSet::new();
    let mut entry_count = prior_entry_count;
    let mut deletes = std::pin::pin!(guarded_deletions(reader, deletes));
    let mut entries = std::pin::pin!(entries);
    let mut ready = VecDeque::with_capacity(ADDITION_PREFETCH);
    let mut additions_done = false;
    let mut planner = ProbePlanner {
        claimed: HashMap::new(),
    };

    let mut probes =
        futures::stream::FuturesUnordered::<BoxFuture<'_, Result<CompletedProbe>>>::new();
    let mut metrics = IndexMaintenanceMetrics::default();
    let mut first_probe = None;
    let mut last_probe_completion = None;

    loop {
        let buffered = ready.len().saturating_add(probes.len());
        let deletion = if buffered >= ADDITION_PREFETCH {
            deletes.next().await
        } else {
            tokio::select! {
                biased;
                deletion = deletes.next() => deletion,
                resolution = probes.next(), if !probes.is_empty() => {
                    if let Some(resolution) = resolution {
                        metrics.probes_completed_during_deletions = metrics
                            .probes_completed_during_deletions
                            .saturating_add(1);
                        ready.push_back(ReadyAddition::Probed(resolution?));
                    }
                    continue;
                },
                addition = entries.next(), if !additions_done => if let Some(addition) = addition {
                    metrics.additions = metrics.additions.saturating_add(1);
                    let plan = planner.plan(addition, &mut entry_count);
                    schedule_probe_plan(
                        plan,
                        reader,
                        &mut probes,
                        &mut ready,
                        &mut metrics,
                        &mut first_probe,
                    )?;
                    continue;
                } else {
                    additions_done = true;
                    metrics.addition_derivation = started.elapsed();
                        continue;
                }
            }
        };
        let Some(entry) = deletion else {
            break;
        };
        let entry = entry?;
        entry_count = entry_count.saturating_add(1);
        if entry_count > MAX_INDEX_ENTRIES_PER_COMMIT {
            return Err(oversized_commit(entry_count));
        }
        if !entry.delete {
            return Err(Error::Corruption(
                "index deletion stream contains a put".to_owned(),
            ));
        }
        if entry.unique {
            deleted_unique.insert(entry.key.clone());
        }
        metrics.deletions = metrics.deletions.saturating_add(1);
        let stage_started = Instant::now();
        staged.add(entry.key.len(), 0);
        writes.push((entry.key.to_vec(), None));
        metrics.staging = metrics.staging.saturating_add(stage_started.elapsed());
    }
    metrics.deletion_derivation = started.elapsed();

    let mut poisoned = Vec::new();
    loop {
        let resolution = if let Some(resolution) = ready.pop_front() {
            Some(resolution)
        } else if additions_done || probes.len() >= UNIQUENESS_PROBE_CONCURRENCY {
            probes.next().await.transpose()?.map(ReadyAddition::Probed)
        } else {
            tokio::select! {
                biased;
                resolution = probes.next(), if !probes.is_empty() => {
                    resolution.transpose()?.map(ReadyAddition::Probed)
                },
                addition = entries.next() => if let Some(addition) = addition {
                    metrics.additions = metrics.additions.saturating_add(1);
                    let plan = planner.plan(addition, &mut entry_count);
                    schedule_probe_plan(
                        plan,
                        reader,
                        &mut probes,
                        &mut ready,
                        &mut metrics,
                        &mut first_probe,
                    )?;
                    continue;
                } else {
                    additions_done = true;
                    metrics.addition_derivation = started.elapsed();
                    continue;
                }
            }
        };

        let Some(resolution) = resolution else {
            break;
        };
        match resolution {
            ReadyAddition::Put(key) => {
                let stage_started = Instant::now();
                plan_probe_put(&mut writes, &mut staged, &key, None);
                metrics.staging = metrics.staging.saturating_add(stage_started.elapsed());
            }
            ReadyAddition::Probed(CompletedProbe {
                probe,
                present,
                service,
                completed,
            }) => {
                metrics.probe_service = metrics.probe_service.saturating_add(service);
                last_probe_completion = Some(completed);
                if present.is_some() {
                    metrics.probe_hits = metrics.probe_hits.saturating_add(1);
                } else {
                    metrics.probe_misses = metrics.probe_misses.saturating_add(1);
                }
                if deleted_unique.contains(&probe.key) {
                    let stage_started = Instant::now();
                    plan_probe_put(&mut writes, &mut staged, &probe.key, Some(probe.row_id));
                    metrics.staging = metrics.staging.saturating_add(stage_started.elapsed());
                } else if let Some(bytes) = present {
                    match decode_row_id(&bytes) {
                        Ok(holder) if holder == probe.row_id => {}
                        Ok(_) => {
                            if let Some(index_id) = collision(probe.index_id, probe.building)? {
                                poisoned.push(index_id);
                            }
                        }
                        Err(error) => return Err(error),
                    }
                } else {
                    let stage_started = Instant::now();
                    plan_probe_put(&mut writes, &mut staged, &probe.key, Some(probe.row_id));
                    metrics.staging = metrics.staging.saturating_add(stage_started.elapsed());
                }
            }
            ReadyAddition::Poison(index_id) => poisoned.push(index_id),
        }
    }

    if let (Some(first_probe), Some(last_probe_completion)) = (first_probe, last_probe_completion) {
        metrics.probe_window = last_probe_completion.saturating_duration_since(first_probe);
    }
    poisoned.sort_unstable();
    poisoned.dedup();

    Ok(StagedEntries {
        writes,
        poisoned,
        deferred: Vec::new(),
        bytes: staged.0,
        locator_writes: Vec::new(),
        metrics,
    })
}

/// Records `poisoned` on the working state's definitions, so the commit's
/// ordinary entity diff stages the flag. Poisoning is terminal.
pub(crate) fn apply_poison(state: &mut crate::catalog::CatalogSnapshot, poisoned: &[u64]) {
    let poisoned: HashSet<&u64> = poisoned.iter().collect();
    for value in state
        .indexes
        .values_mut()
        .flat_map(|per_table| per_table.values_mut())
    {
        if poisoned.contains(&value.index_id) {
            value.poisoned = Some(true);
        }
    }
}

/// Marks ready deferred definitions as awaiting repair. A definition already
/// being repaired keeps its cursor.
pub(crate) fn apply_deferred_maintenance(
    base: &crate::catalog::CatalogSnapshot,
    state: &mut crate::catalog::CatalogSnapshot,
    deferred: &[u64],
    new_snapshot: u64,
) {
    for index_id in deferred {
        for (table_id, per_table) in &mut state.indexes {
            let Some(value) = per_table.get_mut(index_id) else {
                continue;
            };

            if value.build_state.is_none() {
                value.begin_snapshot = new_snapshot;
                value.build_state = Some("maintaining".to_owned());
                value.build_cursor_row_id = base
                    .table_stats
                    .get(table_id)
                    .and_then(|stats| stats.next_row_id.checked_sub(1));
                let tail = base
                    .data_files
                    .get(table_id)
                    .and_then(|files| files.values().max_by_key(|file| file.data_file_id));
                // File ids begin above zero: zero means the old snapshot held
                // no files.
                value.build_cursor_file = Some(tail.map_or(0, |file| file.data_file_id));
                value.build_cursor_position =
                    Some(tail.map_or(0, |file| file.record_count.saturating_sub(1)));
            }
        }
    }
}

/// The refusal for a commit staging more entries than
/// [`MAX_INDEX_ENTRIES_PER_COMMIT`]. The text must avoid DuckLake's retry
/// substrings (`conflict`, `concurrent`, `unique`, `primary key`).
fn oversized_commit(staged: usize) -> Error {
    // A kilobyte apiece, rounded to the nearest GiB.
    let gib = (staged + 512 * 1024) / (1024 * 1024);
    Error::Constraint(format!(
        "commit stages {staged} equality-index entries, above the \
         {MAX_INDEX_ENTRIES_PER_COMMIT} a single commit allows; at about a kilobyte \
         apiece in the store's write path it would need roughly {gib} GiB of \
         memory. Split the work into several smaller commits."
    ))
}

/// A uniqueness error. The text must avoid DuckLake's retry substrings
/// (`conflict`, `concurrent`, `unique`, `primary key`).
fn unique_violation(index_id: u64) -> Error {
    Error::Constraint(format!(
        "duplicate value violates equality index {index_id}"
    ))
}

/// Deletes up to `limit` orphaned entries of one dropped index inside an
/// open transaction, returning how many deletes were staged.
pub(crate) async fn reclaim_entries(
    tx: &slatedb::DbTransaction,
    index_id: u64,
    limit: usize,
    staged: &mut StagedBytes,
) -> Result<usize> {
    let mut deleted = 0;
    for kind in [IndexKind::Unique, IndexKind::Multi] {
        if deleted >= limit {
            break;
        }
        let (batch, _) =
            reclaim_entries_from(tx, kind, index_id, limit - deleted, None, staged).await?;
        deleted += batch;
    }

    Ok(deleted)
}

/// Deletes up to `limit` entries of one dropped index of one kind,
/// resuming at `start_from` when given, and returns how many were staged
/// alongside the last key deleted.
pub(crate) async fn reclaim_entries_from(
    tx: &slatedb::DbTransaction,
    kind: IndexKind,
    index_id: u64,
    limit: usize,
    start_from: Option<&[u8]>,
    staged: &mut StagedBytes,
) -> Result<(usize, Option<Vec<u8>>)> {
    let prefix = index_index_prefix(kind, index_id);
    // `scan_prefix` takes its bounds as a suffix of the prefix.
    let suffix = match start_from {
        Some(key) if key.len() >= prefix.len() => key[prefix.len()..].to_vec(),
        _ => Vec::new(),
    };

    let mut iter = ReadHandle::Tx(tx)
        .scan_prefix(prefix, suffix.., ScanShape::Bulk)
        .await?;
    let mut deleted = 0;
    let mut last: Option<Bytes> = None;
    while deleted < limit {
        match iter.next().await? {
            Some(entry) => {
                staged.add(entry.key.len(), 0);
                last = Some(entry.key.clone());
                tx.delete(entry.key)?;
                deleted += 1;
            }
            None => break,
        }
    }

    Ok((deleted, last.map(|key| key.to_vec())))
}

/// Read-side dispatch for uniqueness probes: the folded store view overlaid
/// with the writes of slots no folder has applied yet.
#[derive(Clone, Copy)]
pub(crate) enum ProbeHandle<'a> {
    /// A store view overlaid with an unfolded tail: a tail write shadows the
    /// stored value, a tail delete hides it.
    Overlaid {
        /// The folded store view.
        store: ReadHandle<'a>,
        /// The writes of slots no folder has applied yet.
        overlay: &'a moraine_wal::Overlay,
    },
}

impl<'a> ProbeHandle<'a> {
    /// The folded store view, for the scans a probe cannot answer by key.
    pub(crate) fn store(&self) -> ReadHandle<'a> {
        let Self::Overlaid { store, .. } = self;
        *store
    }

    /// The unfolded tail's writes, to overlay a scan of [`store`](Self::store).
    pub(crate) fn overlay(&self) -> &'a moraine_wal::Overlay {
        let Self::Overlaid { overlay, .. } = self;
        overlay
    }

    /// Point read of one key, an overlaid tail's write for it taking
    /// precedence over the store's.
    pub(crate) async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        match self {
            Self::Overlaid { store, overlay } => match overlay.get(key) {
                Some(write) => Ok(write.map(Bytes::copy_from_slice)),
                None => store.get(key).await.map_err(Error::from),
            },
        }
    }
}

/// A byte range under one entry prefix, as the suffix bounds a scan takes.
type Subrange = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// The row id one entry names, from its key and value.
type RowIdOf = fn(&[u8], &[u8]) -> Result<u64>;

/// A non-unique entry carries its row id in the key.
fn multi_row_id(key: &[u8], _value: &[u8]) -> Result<u64> {
    match Key::decode(key)? {
        Key::Index(IndexKey::Multi { row_id, .. }) => Ok(row_id),
        other => Err(Error::Corruption(format!(
            "non-multi key in index scan: {other:?}"
        ))),
    }
}

/// A unique entry's value is the holding row id.
fn unique_row_id(_key: &[u8], value: &[u8]) -> Result<u64> {
    decode_row_id(value)
}

/// Whether `key` falls inside `subrange`, whose bounds are suffixes of
/// `prefix`. Every key considered starts with `prefix`, and such keys compare
/// as their suffixes do, so the bounds are compared whole.
fn in_subrange(key: &[u8], prefix: &[u8], (lower, upper): &Subrange) -> bool {
    let full = |suffix: &[u8]| [prefix, suffix].concat();
    let above_lower = match lower {
        Bound::Included(suffix) => key >= full(suffix).as_slice(),
        Bound::Excluded(suffix) => key > full(suffix).as_slice(),
        Bound::Unbounded => true,
    };
    let below_upper = match upper {
        Bound::Included(suffix) => key <= full(suffix).as_slice(),
        Bound::Excluded(suffix) => key < full(suffix).as_slice(),
        Bound::Unbounded => true,
    };

    above_lower && below_upper
}

/// One entry, an unfolded tail's write for it taking precedence over the
/// store's.
async fn read_entry(
    reader: ReadHandle<'_>,
    tail: Option<&moraine_wal::Overlay>,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    if let Some(write) = tail.and_then(|tail| tail.get(key)) {
        return Ok(write.map(<[u8]>::to_vec));
    }

    Ok(reader
        .get(key)
        .await
        .map_err(Error::from)?
        .map(|bytes| bytes.to_vec()))
}

/// The row ids of the entries under `prefix` within `subrange`, in `order`,
/// with an unfolded tail's writes merged over the store's: a tail put shadows
/// the stored entry, a tail delete hides it. Both sides run in `order`, so one
/// pass over each answers the probe, and only the tail's slice of the range is
/// held in memory.
async fn merged_row_ids(
    reader: ReadHandle<'_>,
    tail: Option<&moraine_wal::Overlay>,
    prefix: Vec<u8>,
    subrange: Subrange,
    order: ScanOrder,
    row_id_of: RowIdOf,
) -> Result<Vec<u64>> {
    let mut writes: Vec<(Vec<u8>, Option<Vec<u8>>)> = tail.map_or_else(Vec::new, |tail| {
        tail.prefixed(&prefix)
            .filter(|(key, _)| in_subrange(key, &prefix, &subrange))
            .map(|(key, value)| (key.to_vec(), value.map(<[u8]>::to_vec)))
            .collect()
    });
    // `prefixed` ascends; a descending scan walks the same slice backwards so
    // both sides step in one direction.
    if order == ScanOrder::Descending {
        writes.reverse();
    }

    let mut iter = reader
        .scan_prefix_ordered(prefix, subrange, ScanShape::Probe, order)
        .await
        .map_err(Error::from)?;
    let mut stored = iter.next().await.map_err(Error::from)?;
    let mut writes = writes.into_iter();
    let mut written = writes.next();

    let mut row_ids = Vec::new();
    loop {
        match (stored.take(), written.take()) {
            (None, None) => break,
            (Some(entry), None) => {
                row_ids.push(row_id_of(&entry.key, &entry.value)?);
                stored = iter.next().await.map_err(Error::from)?;
            }
            (None, Some((key, value))) => {
                if let Some(bytes) = value {
                    row_ids.push(row_id_of(&key, &bytes)?);
                }
                written = writes.next();
            }
            (Some(entry), Some((key, value))) => {
                let ordering = match order {
                    ScanOrder::Ascending => key.as_slice().cmp(entry.key.as_ref()),
                    ScanOrder::Descending => entry.key.as_ref().cmp(key.as_slice()),
                };
                match ordering {
                    Ordering::Less => {
                        if let Some(bytes) = value {
                            row_ids.push(row_id_of(&key, &bytes)?);
                        }
                        stored = Some(entry);
                        written = writes.next();
                    }
                    // The tail wrote the stored entry's own key, so it replaces it.
                    Ordering::Equal => {
                        if let Some(bytes) = value {
                            row_ids.push(row_id_of(&key, &bytes)?);
                        }
                        stored = iter.next().await.map_err(Error::from)?;
                        written = writes.next();
                    }
                    Ordering::Greater => {
                        row_ids.push(row_id_of(&entry.key, &entry.value)?);
                        stored = iter.next().await.map_err(Error::from)?;
                        written = Some((key, value));
                    }
                }
            }
        }
    }

    Ok(row_ids)
}

/// The row ids holding one indexed value: a point-get for a unique index,
/// an ascending prefix scan for a non-unique one. An unfolded tail's writes
/// take precedence over the store's, so a value claimed by a committed but
/// unfolded slot is seen.
pub(crate) async fn lookup_row_ids(
    reader: ReadHandle<'_>,
    tail: Option<&moraine_wal::Overlay>,
    index_id: u64,
    unique: bool,
    key: &CanonicalKey,
) -> Result<Vec<u64>> {
    if unique {
        let entry_key = encode_index_entry(index_id, true, key, 0);
        return match read_entry(reader, tail, &entry_key).await? {
            Some(bytes) => Ok(vec![decode_row_id(&bytes)?]),
            None => Ok(Vec::new()),
        };
    }

    let prefix = index_multi_value_prefix(index_id, key);
    merged_row_ids(
        reader,
        tail,
        prefix,
        (Bound::Unbounded, Bound::Unbounded),
        ScanOrder::Ascending,
        multi_row_id,
    )
    .await
}

/// The row ids whose indexed value falls between `lower` and `upper`, in the
/// requested scan order. The bounds are canonical values, already encoded
/// in the columns' declared directions. An unfolded tail's writes are merged
/// over the store's.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn range_row_ids(
    reader: ReadHandle<'_>,
    tail: Option<&moraine_wal::Overlay>,
    index_id: u64,
    unique: bool,
    leading_nulls: NullOrder,
    lower: Bound<CanonicalKey>,
    upper: Bound<CanonicalKey>,
    order: ScanOrder,
) -> Result<Vec<u64>> {
    let kind = if unique {
        IndexKind::Unique
    } else {
        IndexKind::Multi
    };
    let prefix = index_index_prefix(kind, index_id);

    let suffix = |canon: &CanonicalKey| index_value_suffix(kind, index_id, canon);
    let above = |canon: &CanonicalKey| index_value_above(kind, index_id, canon);

    // A comparison never matches a NULL, so an open side stops at the
    // leading column's non-null region.
    let non_null = non_null_flag_key(leading_nulls);
    let past_non_null = above(&non_null).map_or(Bound::Unbounded, Bound::Excluded);

    let start = match lower {
        Bound::Included(canon) => Bound::Included(suffix(&canon)),
        // Skip every entry sharing the bound value: start above them all.
        Bound::Excluded(canon) => match above(&canon) {
            Some(above) => Bound::Included(above),
            None => Bound::Excluded(suffix(&canon)),
        },
        Bound::Unbounded => Bound::Included(index_value_body(kind, index_id, &non_null)),
    };
    let end = match upper {
        // Include every entry sharing the bound value: end above them all.
        Bound::Included(canon) => match above(&canon) {
            Some(above) => Bound::Excluded(above),
            None => past_non_null,
        },
        Bound::Excluded(canon) => Bound::Excluded(suffix(&canon)),
        Bound::Unbounded => past_non_null,
    };

    let row_id_of: RowIdOf = if unique { unique_row_id } else { multi_row_id };

    merged_row_ids(reader, tail, prefix, (start, end), order, row_id_of).await
}

/// The row ids whose leading indexed columns match `prefix`, a canonical
/// key over a leading run of `= value` and `IS NULL` predicates. A row with
/// any NULL indexed column is stored multi-shaped, so the `multi` subrange
/// is scanned. An unfolded tail's writes are merged over the store's.
pub(crate) async fn null_prefix_row_ids(
    reader: ReadHandle<'_>,
    tail: Option<&moraine_wal::Overlay>,
    index_id: u64,
    prefix: &CanonicalKey,
    order: ScanOrder,
) -> Result<Vec<u64>> {
    let mut scan_prefix = index_multi_value_prefix(index_id, prefix);
    // Dropping the exact-value terminator turns the bytes into a true
    // leading prefix.
    scan_prefix.pop();

    merged_row_ids(
        reader,
        tail,
        scan_prefix,
        (Bound::Unbounded, Bound::Unbounded),
        order,
        multi_row_id,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::store::{
        index_encoding::{Direction, IndexKeyValue, IntWidth, encode_ordered_values},
        open::StoreBuilder,
    };

    /// A probe over `tx` with no unfolded tail: these exercise the pipeline,
    /// not the overlay.
    fn probe<'a>(
        tx: &'a slatedb::DbTransaction,
        overlay: &'a moraine_wal::Overlay,
    ) -> ProbeHandle<'a> {
        ProbeHandle::Overlaid {
            store: ReadHandle::Tx(tx),
            overlay,
        }
    }

    /// A normal fact flush carries 250 source keys plus a handful of newly
    /// discovered reference keys. They must all enter the first probe window:
    /// leaving one or two for a second window charges a full remote-read tail
    /// for almost no work.
    #[test]
    fn normal_flush_unique_probes_share_one_window() {
        const FACTS_AND_REFERENCES: usize = 258;
        let concurrency = std::hint::black_box(UNIQUENESS_PROBE_CONCURRENCY);

        assert!(
            concurrency >= FACTS_AND_REFERENCES,
            "{FACTS_AND_REFERENCES} probes exceed the concurrency window of \
             {concurrency}"
        );
    }

    /// A stream larger than the former 1,024-entry group stays one rolling
    /// probe window rather than draining at an artificial boundary.
    #[tokio::test]
    async fn unique_probe_stream_crosses_the_old_group_boundary() {
        const ENTRIES: usize = 1_025;

        let overlay = moraine_wal::Overlay::default();
        let (db, _) = StoreBuilder::new("continuous-probes", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let entries = stream::iter((0..ENTRIES).map(|position| {
            let row_id = u64::try_from(position).unwrap();
            Ok(StagedIndexEntry {
                index_id: 1,
                unique: true,
                key: Bytes::copy_from_slice(&row_id.to_be_bytes()),
                row_id,
                delete: false,
                building: false,
            })
        }));

        let staged = plan_index_entry_stream(probe(&tx, &overlay), stream::empty(), entries, 0)
            .await
            .unwrap();

        assert_eq!(staged.bytes, u64::try_from(ENTRIES * 16).unwrap());
        tx.rollback();
        db.close().await.unwrap();
    }

    /// A unique deletion removes the entry of the row it names, and only
    /// that row's.
    ///
    /// The store key is the value alone, so a deletion derived for a row
    /// that no longer holds the value would otherwise take the entry from
    /// whichever row does.
    #[tokio::test]
    async fn a_unique_deletion_removes_only_the_named_row_s_entry() {
        let overlay = moraine_wal::Overlay::default();
        let (db, _) = StoreBuilder::new("guarded-deletions", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let key = Bytes::from_static(b"one value");

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(key.clone(), 3u64.to_be_bytes()).unwrap();
        tx.commit().await.unwrap();

        let deletion = |row_id: u64, key: Bytes| {
            stream::once(async move {
                Ok(StagedIndexEntry {
                    index_id: 1,
                    unique: true,
                    key,
                    row_id,
                    delete: true,
                    building: false,
                })
            })
        };

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let staged = plan_index_entry_stream(
            probe(&tx, &overlay),
            deletion(0, key.clone()),
            stream::empty(),
            0,
        )
        .await
        .unwrap();
        assert_eq!(staged.metrics.deletions, 0);
        crate::transaction::commit::stage_writes(&tx, &staged.writes).unwrap();
        tx.commit().await.unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(
            ReadHandle::Tx(&tx)
                .get(key.clone())
                .await
                .unwrap()
                .is_some(),
            "row 3's entry survives a deletion derived for row 0"
        );
        let staged = plan_index_entry_stream(
            probe(&tx, &overlay),
            deletion(3, key.clone()),
            stream::empty(),
            0,
        )
        .await
        .unwrap();
        assert_eq!(staged.metrics.deletions, 1);
        crate::transaction::commit::stage_writes(&tx, &staged.writes).unwrap();
        tx.commit().await.unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(ReadHandle::Tx(&tx).get(key).await.unwrap().is_none());
        tx.rollback();

        db.close().await.unwrap();
    }

    /// Addition sources may fill the bounded prefetch while a deletion is
    /// pending, but the transaction must still stage the delete first.
    #[tokio::test]
    async fn addition_prefetch_preserves_the_deletion_phase_boundary() {
        let overlay = moraine_wal::Overlay::default();
        let (db, _) = StoreBuilder::new("addition-prefetch", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let key = Bytes::from_static(b"delete-before-add");
        // The entry the deletion removes: a unique deletion is staged only
        // for the row the committed entry names.
        let seed = db.begin(IsolationLevel::Snapshot).await.unwrap();
        seed.put(key.clone(), 7_u64.to_be_bytes()).unwrap();
        seed.commit().await.unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let (release_deletion, deletion_released) = tokio::sync::oneshot::channel();
        let deletion_key = key.clone();
        let deletions = stream::once(async move {
            deletion_released.await.unwrap();
            Ok(StagedIndexEntry {
                index_id: 1,
                unique: true,
                key: deletion_key,
                row_id: 7,
                delete: true,
                building: false,
            })
        });
        let (addition_polled, observed_addition) = tokio::sync::oneshot::channel();
        let addition_key = key.clone();
        let additions = stream::once(async move {
            addition_polled.send(()).unwrap();
            Ok(StagedIndexEntry {
                index_id: 1,
                unique: true,
                key: addition_key,
                row_id: 7,
                delete: false,
                building: false,
            })
        });

        {
            let staging = plan_index_entry_stream(probe(&tx, &overlay), deletions, additions, 0);
            let mut staging = std::pin::pin!(staging);
            tokio::select! {
                _ = &mut staging => panic!("staging finished before deletion release"),
                observed = observed_addition => observed.unwrap(),
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            release_deletion.send(()).unwrap();
            let staged = staging.as_mut().await.unwrap();
            assert_eq!(staged.metrics.unique_probes, 1);
            assert_eq!(staged.metrics.probes_completed_during_deletions, 1);
            assert_eq!(staged.metrics.additions, 1);
            assert_eq!(staged.metrics.deletions, 1);
            crate::transaction::commit::stage_writes(&tx, &staged.writes).unwrap();
        }

        assert_eq!(
            tx.get(&key).await.unwrap(),
            Some(Bytes::copy_from_slice(&7_u64.to_be_bytes()))
        );
        tx.rollback();
        db.close().await.unwrap();
    }

    /// A one-column ascending key over `value`.
    fn value_key(value: u128) -> CanonicalKey {
        encode_ordered_values(
            &[Some(IndexKeyValue::UInt {
                value,
                width: IntWidth::I64,
            })],
            &[Direction::Ascending],
            &[NullOrder::Last],
        )
        .unwrap()
    }

    /// The encoded key of one non-unique entry.
    fn multi_key(index_id: u64, value: u128, row_id: u64) -> Vec<u8> {
        Key::Index(IndexKey::Multi {
            index_id,
            key: value_key(value),
            row_id,
        })
        .encode()
    }

    /// An overlay holding one envelope's writes, as tail replay builds it.
    fn tail_writing(writes: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> moraine_wal::Overlay {
        let mut overlay = moraine_wal::Overlay::default();
        overlay.absorb(&moraine_wal::Envelope {
            leader: None,
            commits: vec![moraine_wal::Commit {
                transaction_id: [1; 16],
                payload: moraine_wal::SlotPayload {
                    validated_head: 0,
                    changes_made: String::new(),
                    writes: writes
                        .into_iter()
                        .map(|(key, value)| moraine_wal::SlotWrite { key, value })
                        .collect(),
                },
            }],
        });
        overlay
    }

    /// A probe on a slot-backed store reads the unfolded tail over the store:
    /// the tail's entries join the stored ones, and the entries it deletes
    /// disappear. Without this a query would miss every commit no folder has
    /// applied.
    #[tokio::test]
    async fn a_probe_reads_the_tail_over_the_stored_entries() {
        let (db, _) = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for (value, row_id) in [(10_u128, 1_u64), (20, 2), (30, 3)] {
            tx.put(multi_key(7, value, row_id), []).unwrap();
        }
        crate::transaction::commit::commit_durably(&db, tx)
            .await
            .unwrap();

        // The tail adds value 20 for row 4 and 40 for row 5, and deletes the
        // stored entry for 30.
        let tail = tail_writing(vec![
            (multi_key(7, 20, 4), Some(Vec::new())),
            (multi_key(7, 40, 5), Some(Vec::new())),
            (multi_key(7, 30, 3), None),
        ]);

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let handle = ReadHandle::Tx(&tx);

        // A point lookup sees the tail's added row alongside the stored one.
        assert_eq!(
            lookup_row_ids(handle, Some(&tail), 7, false, &value_key(20))
                .await
                .unwrap(),
            vec![2, 4]
        );
        // A deleted entry is gone, though the store still holds it.
        assert!(
            lookup_row_ids(handle, Some(&tail), 7, false, &value_key(30))
                .await
                .unwrap()
                .is_empty()
        );
        // A whole-index range merges both sides in value order.
        assert_eq!(
            range_row_ids(
                handle,
                Some(&tail),
                7,
                false,
                NullOrder::Last,
                Bound::Unbounded,
                Bound::Unbounded,
                ScanOrder::Ascending,
            )
            .await
            .unwrap(),
            vec![1, 2, 4, 5]
        );
        // Descending walks the same merge backwards.
        assert_eq!(
            range_row_ids(
                handle,
                Some(&tail),
                7,
                false,
                NullOrder::Last,
                Bound::Unbounded,
                Bound::Unbounded,
                ScanOrder::Descending,
            )
            .await
            .unwrap(),
            vec![5, 4, 2, 1]
        );
        // Without a tail the same probes see only the store.
        assert_eq!(
            lookup_row_ids(handle, None, 7, false, &value_key(30))
                .await
                .unwrap(),
            vec![3]
        );
        tx.rollback();
        db.close().await.unwrap();
    }

    /// The oversized-commit refusal must be terminal: DuckLake re-runs a
    /// commit whose error text carries any of four substrings, and
    /// re-running this one only reaches the same answer more slowly.
    #[test]
    fn oversized_commit_refusal_avoids_ducklake_retry_substrings() {
        let text = oversized_commit(13_400_000).to_string();
        for substring in ["conflict", "concurrent", "unique", "primary key"] {
            assert!(
                !text.contains(substring),
                "{text:?} contains DuckLake's retry substring {substring:?}"
            );
        }
    }

    /// It names the count, the limit, the memory it would have needed, and
    /// the one thing the caller can do about it.
    #[test]
    fn oversized_commit_refusal_is_actionable() {
        let text = oversized_commit(13_400_000).to_string();
        assert!(text.contains("13400000"), "names the count: {text}");
        assert!(text.contains("8000000"), "names the limit: {text}");
        assert!(text.contains("13 GiB"), "names the memory: {text}");
        assert!(text.contains("smaller commits"), "names the remedy: {text}");
    }
}
