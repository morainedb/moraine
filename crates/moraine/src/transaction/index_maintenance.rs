//! Index entry maintenance: turning writer-supplied entries into staged
//! `index` writes, with commit-time uniqueness enforcement.
//!
//! Entries ride the same batch as the commit that owns them. A unique
//! entry's store key *is* the value, so staging its put arms SlateDB's
//! write-write detection: two commits inserting the same value collide
//! mechanically, and the loser re-runs and sees the winner's entry.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::Bound,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt, future::BoxFuture, stream};
use slatedb::DbTransaction;
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
    /// Whether the index is unique — selects the key shape and enforcement.
    pub(crate) unique: bool,
    /// The final encoded SlateDB entry key.
    pub(crate) key: Bytes,
    /// The row the entry points at.
    pub(crate) row_id: u64,
    /// Whether this removes the entry (`true`) or adds it (`false`).
    pub(crate) delete: bool,
    /// Whether the index is still building, so a collision poisons it
    /// instead of failing this commit.
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

/// How many uniqueness probes are in flight at once. Each probe is an
/// independent point read, so running them one after another makes a batch
/// cost one store round-trip of latency *per entry*. This covers an ordinary
/// several-hundred-row flush in one window while keeping a bulk load bounded.
const UNIQUENESS_PROBE_CONCURRENCY: usize = 512;

/// Additions derived while the deletion phase is still draining. Once full,
/// backpressure pauses addition sources without holding up deletion staging.
const ADDITION_PREFETCH: usize = 512;

/// The most entries one commit may stage.
///
/// Every staged key costs roughly a kilobyte of memory in the store's write
/// path — measured, and almost none of it moraine's: the write batch, the
/// WAL buffer's copy, the memtable's skiplist node, and the transaction's
/// write-key set each hold their own. That is inherent to putting a key in a
/// batch, so a commit's footprint is set by how many entries it stages and
/// cannot be optimized away here.
///
/// A bulk load stages one entry per indexed row per index, so an unbounded
/// load asks for memory proportional to the whole table — tens of gigabytes
/// at tens of millions of rows. Past that the process does not fail, it
/// thrashes: swapping, no progress, no error. Refusing up front turns that
/// into an immediate, actionable message.
///
/// At roughly a kilobyte apiece this admits commits needing about 8 GiB.
const MAX_INDEX_ENTRIES_PER_COMMIT: usize = 8_000_000;

/// What staging a batch's entries produced: the indexes a collision
/// poisoned, and the bytes the entries put on the batch.
pub(crate) struct StagedEntries {
    /// Definitions a duplicate poisoned, sorted and deduplicated.
    pub(crate) poisoned: Vec<u64>,
    /// Deferred definitions whose SQL additions committed without entries.
    pub(crate) deferred: Vec<u64>,
    /// Key and value bytes staged, deletes included.
    pub(crate) bytes: u64,
    /// Whether this batch wrote the inline chunk row-range directory.
    pub(crate) uses_inline_chunk_directory: bool,
    /// Work inside index upkeep. Its wall-clock fields overlap by design.
    pub(crate) metrics: IndexMaintenanceMetrics,
}

/// Per-commit equality-index work. Derivation and probe windows overlap, so
/// these durations describe phases rather than an additive decomposition.
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
    /// Whether that index is still building, so a collision poisons it
    /// rather than failing this commit.
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

async fn resolve_probe(reader: ReadHandle<'_>, probe: PendingProbe) -> Result<CompletedProbe> {
    let started = Instant::now();
    let present = reader.get(probe.key.clone()).await.map_err(Error::from)?;
    Ok(CompletedProbe {
        probe,
        present,
        service: started.elapsed(),
        completed: Instant::now(),
    })
}

fn schedule_probe_plan<'a>(
    plan: ProbePlan,
    reader: ReadHandle<'a>,
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

fn stage_probe_put(
    db_tx: &DbTransaction,
    staged: &mut StagedBytes,
    key: Bytes,
    row_id: Option<u64>,
) -> Result<()> {
    let value_bytes = row_id.map_or(0, |_| size_of::<u64>());
    staged.add(key.len(), value_bytes);
    match row_id {
        Some(row_id) => db_tx.put(key, row_id.to_be_bytes()),
        None => db_tx.put(key, []),
    }
    .map_err(Error::from)
}

/// Resolves accumulated entries onto `db_tx`, enforcing uniqueness at
/// commit. Deletes are staged first so a delete-then-reinsert of one unique
/// value within a commit sees the value as absent. For each unique put:
/// present with a **different** row id → [`Error::Constraint`]; present with
/// the **same** row id → no-op (a re-derived entry); absent → staged.
/// Duplicates within the commit are caught in memory.
///
/// A collision against an index that is still **building** poisons it
/// instead: the id is returned, the claim is dropped so the live holder's
/// entry survives, and the commit proceeds. Coverage is partial until a
/// build flips ready, so failing the finder would decide by timing which
/// party a duplicate falls on.
///
/// Entries stage onto the transaction directly rather than through the
/// caller's write list. The list is retained until the commit lands so the
/// maintained projections can fold it, and no projection reflects an index
/// entry — keeping one would hold a second copy of the batch's largest part
/// in memory for nothing. A bulk load stages one entry per indexed row, so
/// that copy is what decides whether the commit fits in RAM.
///
/// Puts may stage in probe-completion order. Every staged key is distinct —
/// an entry's kind is part of its key — so only the deletes-before-puts
/// ordering above carries meaning.
pub(crate) async fn stage_index_entries(
    db_tx: &DbTransaction,
    entries: Vec<StagedIndexEntry>,
) -> Result<StagedEntries> {
    let entry_count = entries.len();
    if entry_count > MAX_INDEX_ENTRIES_PER_COMMIT {
        // Logged as well as returned: the refusal is the guardrail firing,
        // and an operator reading logs after a failed bulk load should see
        // it without having to recover the SQL error text.
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
    stage_index_entry_stream(db_tx, deletes, puts, 0).await
}

/// Consumes deletion entries, then additions, as streams. Unique probes form
/// one continuously replenished window across every derived source group.
/// Successful reads stage in completion order, and the first fatal result
/// aborts the transaction without waiting for slower probes.
#[allow(clippy::too_many_lines)]
pub(crate) async fn stage_index_entry_stream<D, S>(
    db_tx: &DbTransaction,
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
    let mut deleted_unique = HashSet::new();
    let mut entry_count = prior_entry_count;
    let mut deletes = std::pin::pin!(deletes);
    let mut entries = std::pin::pin!(entries);
    let mut ready = VecDeque::with_capacity(ADDITION_PREFETCH);
    let mut additions_done = false;
    let mut planner = ProbePlanner {
        claimed: HashMap::new(),
    };
    let reader = ReadHandle::Tx(db_tx);
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
        db_tx.delete(entry.key).map_err(Error::from)?;
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
                stage_probe_put(db_tx, &mut staged, key, None)?;
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
                    stage_probe_put(db_tx, &mut staged, probe.key, Some(probe.row_id))?;
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
                    stage_probe_put(db_tx, &mut staged, probe.key, Some(probe.row_id))?;
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
        poisoned,
        deferred: Vec::new(),
        bytes: staged.0,
        uses_inline_chunk_directory: false,
        metrics,
    })
}

/// Records `poisoned` on the working state's definitions, so the commit's
/// ordinary entity diff stages the flag. It is terminal: a poisoned build
/// never flips ready, and its driver ends the definition.
pub(crate) fn apply_poison(state: &mut crate::catalog::CatalogSnapshot, poisoned: &[u64]) {
    for index_id in poisoned {
        for per_table in state.indexes.values_mut() {
            if let Some(value) = per_table.get_mut(index_id) {
                value.poisoned = Some(true);
            }
        }
    }
}

/// Marks ready deferred definitions as awaiting repair. A definition already
/// being repaired keeps its cursor: newly registered files have greater ids,
/// so the existing source watermark will still reach them.
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
                // File ids begin above zero. The zero sentinel means the
                // old snapshot held no files, while still selecting the
                // physical source-cursor resume path.
                value.build_cursor_file = Some(tail.map_or(0, |file| file.data_file_id));
                value.build_cursor_position =
                    Some(tail.map_or(0, |file| file.record_count.saturating_sub(1)));
            }
        }
    }
}

/// The refusal for a commit staging more entries than
/// [`MAX_INDEX_ENTRIES_PER_COMMIT`]. Names the count, the limit, and the
/// remedy, because the caller's only fix is to commit less at a time.
///
/// Like the uniqueness rejection, the text avoids DuckLake's four retry
/// substrings: this is terminal, and re-running it would only spend the
/// caller's retry budget arriving at the same answer more slowly.
fn oversized_commit(staged: usize) -> Error {
    // A kilobyte apiece, rounded to the nearest GiB — an order-of-magnitude
    // figure for the reader, so integer arithmetic is precise enough.
    let gib = (staged + 512 * 1024) / (1024 * 1024);
    Error::Constraint(format!(
        "commit stages {staged} equality-index entries, above the \
         {MAX_INDEX_ENTRIES_PER_COMMIT} a single commit allows; at about a kilobyte \
         apiece in the store's write path it would need roughly {gib} GiB of \
         memory. Split the work into several smaller commits."
    ))
}

/// A uniqueness error. The text is free of DuckLake's four retry substrings
/// (`conflict`, `concurrent`, `unique`, `primary key`) so a rejected bulk
/// INSERT surfaces at once instead of spinning DuckLake's commit loop.
fn unique_violation(index_id: u64) -> Error {
    Error::Constraint(format!(
        "duplicate value violates equality index {index_id}"
    ))
}

/// Deletes up to `limit` orphaned entries of one dropped index inside an
/// open transaction, returning how many deletes were staged. An index is
/// exclusively one kind, so only one prefix holds entries; scanning both
/// is harmless.
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
///
/// Reclaiming a whole range takes one transaction per batch, and a batch
/// that restarted at the range's beginning would first have to step over
/// every tombstone the earlier batches left — turning a large range into
/// quadratic work. Handing the last key back lets the next batch resume
/// there instead. The resume is inclusive, so it re-reads exactly one
/// tombstone rather than needing an exclusive bound.
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
    let mut last = None;
    while deleted < limit {
        match iter.next().await? {
            Some(entry) => {
                let key = entry.key.to_vec();
                staged.add(key.len(), 0);
                tx.delete(entry.key)?;
                deleted += 1;
                last = Some(key);
            }
            None => break,
        }
    }
    Ok((deleted, last))
}

/// The row ids holding one indexed value: a point-get for a unique index,
/// an ascending prefix scan for a non-unique one. The non-unique row id
/// lives in the entry key, so each scanned key is decoded to recover it.
pub(crate) async fn lookup_row_ids(
    reader: ReadHandle<'_>,
    index_id: u64,
    unique: bool,
    key: &CanonicalKey,
) -> Result<Vec<u64>> {
    if unique {
        let entry_key = encode_index_entry(index_id, true, key, 0);
        return match reader.get(entry_key).await.map_err(Error::from)? {
            Some(bytes) => Ok(vec![decode_row_id(&bytes)?]),
            None => Ok(Vec::new()),
        };
    }

    let prefix = index_multi_value_prefix(index_id, key);
    let mut iter = reader
        .scan_prefix(prefix, .., ScanShape::Probe)
        .await
        .map_err(Error::from)?;
    let mut row_ids = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        match Key::decode(&entry.key)? {
            Key::Index(IndexKey::Multi { row_id, .. }) => row_ids.push(row_id),
            other => {
                return Err(Error::Corruption(format!(
                    "non-multi key in index scan: {other:?}"
                )));
            }
        }
    }

    Ok(row_ids)
}

/// The row ids whose indexed value falls between `lower` and `upper`, in the
/// requested scan order. Ordered encoding makes byte order equal the index's
/// declared value order, so the query is a bounded sub-scan of its contiguous
/// range; descending iteration serves the exact opposite. The bounds are the
/// canonical values, already encoded in the columns' declared directions. A
/// unique entry carries its row id in the value, a non-unique one in the key.
pub(crate) async fn range_row_ids(
    reader: ReadHandle<'_>,
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

    // A comparison never matches a NULL, and a non-unique index stores
    // NULL-bearing entries beside valued ones. An open side stops at the
    // leading column's non-null region; a bound value is non-null, so a
    // closed side is already inside it.
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

    let mut iter = reader
        .scan_prefix_ordered(prefix, (start, end), ScanShape::Probe, order)
        .await
        .map_err(Error::from)?;
    let mut row_ids = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        if unique {
            row_ids.push(decode_row_id(entry.value.as_ref())?);
        } else {
            match Key::decode(&entry.key)? {
                Key::Index(IndexKey::Multi { row_id, .. }) => row_ids.push(row_id),
                other => {
                    return Err(Error::Corruption(format!(
                        "non-multi key in index range scan: {other:?}"
                    )));
                }
            }
        }
    }

    Ok(row_ids)
}

/// The row ids of live rows whose leading indexed columns match `prefix` — a
/// canonical key over a leading run of `= value` and `IS NULL` predicates.
/// A row with any NULL indexed column is stored multi-shaped, so `IS NULL`
/// queries scan the `multi` subrange; the value framing's terminator is
/// dropped from the scan prefix so it matches every key that extends the run.
/// The iterator emits the stored or exact-opposite order directly.
pub(crate) async fn null_prefix_row_ids(
    reader: ReadHandle<'_>,
    index_id: u64,
    prefix: &CanonicalKey,
    order: ScanOrder,
) -> Result<Vec<u64>> {
    let mut scan_prefix = index_multi_value_prefix(index_id, prefix);
    // `index_multi_value_prefix` frames the value and appends a terminator for an
    // exact-value scan; dropping it turns the bytes into a true leading prefix.
    scan_prefix.pop();
    let mut iter = reader
        .scan_prefix_ordered(scan_prefix, .., ScanShape::Probe, order)
        .await
        .map_err(Error::from)?;
    let mut row_ids = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        match Key::decode(&entry.key)? {
            Key::Index(IndexKey::Multi { row_id, .. }) => row_ids.push(row_id),
            other => {
                return Err(Error::Corruption(format!(
                    "non-multi key in null-prefix scan: {other:?}"
                )));
            }
        }
    }

    Ok(row_ids)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::store::open::StoreBuilder;

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

        let staged = stage_index_entry_stream(&tx, stream::empty(), entries, 0)
            .await
            .unwrap();

        assert_eq!(staged.bytes, u64::try_from(ENTRIES * 16).unwrap());
        tx.rollback();
        db.close().await.unwrap();
    }

    /// Addition sources may fill the bounded prefetch while a deletion is
    /// pending, but the transaction must still stage the delete first.
    #[tokio::test]
    async fn addition_prefetch_preserves_the_deletion_phase_boundary() {
        let (db, _) = StoreBuilder::new("addition-prefetch", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let key = Bytes::from_static(b"delete-before-add");
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
            let staging = stage_index_entry_stream(&tx, deletions, additions, 0);
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
        }

        assert_eq!(
            tx.get(&key).await.unwrap(),
            Some(Bytes::copy_from_slice(&7_u64.to_be_bytes()))
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
