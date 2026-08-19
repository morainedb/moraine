//! Index entry maintenance: turning writer-supplied entries into staged
//! `index` writes, with commit-time uniqueness enforcement.
//!
//! Entries ride the same batch as the commit that owns them. A unique
//! entry's store key *is* the value, so staging its put arms SlateDB's
//! write-write detection: two commits inserting the same value collide
//! mechanically, and the loser re-runs and sees the winner's entry.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    ops::Bound,
};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use moraine_wal::Overlay;
use tracing::warn;

use crate::{
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        index_encoding::{CanonicalKey, NullOrder, non_null_flag_key},
        key::{
            IndexKey, IndexKind, Key, index_index_prefix, index_multi_value_prefix,
            index_value_above, index_value_body, index_value_suffix,
        },
    },
    transaction::commit::StagedWrite,
};

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

impl ProbeHandle<'_> {
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

/// One index-entry mutation accumulated during a commit closure, resolved
/// against the store when the batch is staged.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StagedIndexEntry {
    /// The index this entry belongs to.
    pub(crate) index_id: u64,
    /// Whether the index is unique — selects the key shape and enforcement.
    pub(crate) unique: bool,
    /// The canonical indexed value.
    pub(crate) key: CanonicalKey,
    /// The row the entry points at.
    pub(crate) row_id: u64,
    /// Whether this removes the entry (`true`) or adds it (`false`).
    pub(crate) delete: bool,
    /// Whether the index is still building, so a collision poisons it
    /// instead of failing this commit.
    pub(crate) building: bool,
}

fn entry_key(entry: &StagedIndexEntry) -> Key {
    if entry.unique {
        Key::Index(IndexKey::Unique {
            index_id: entry.index_id,
            key: entry.key.clone(),
        })
    } else {
        Key::Index(IndexKey::Multi {
            index_id: entry.index_id,
            key: entry.key.clone(),
            row_id: entry.row_id,
        })
    }
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
/// cost one store round-trip of latency *per entry*. Bounding the fan-out
/// instead keeps a single commit from monopolizing the store's request
/// budget.
const UNIQUENESS_PROBE_CONCURRENCY: usize = 64;

/// How many probes are gathered before they are run and resolved. A batch
/// can hold millions of entries, so probes are resolved in bounded groups
/// rather than accumulated: peak memory is this many keys, not the batch's.
const UNIQUENESS_PROBE_GROUP: usize = 1024;

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

/// One unique put awaiting its probe.
struct PendingProbe {
    /// The entry's encoded store key.
    key: Vec<u8>,
    /// The row claiming the value.
    row_id: u64,
    /// The index the claim belongs to.
    index_id: u64,
    /// Whether that index is still building, so a collision poisons it
    /// rather than failing this commit.
    building: bool,
}

/// A collision's verdict: fail the commit, or poison the building index.
fn collision(probe_index_id: u64, building: bool, poisoned: &mut Vec<u64>) -> Option<Error> {
    if building {
        poisoned.push(probe_index_id);
        None
    } else {
        Some(unique_violation(probe_index_id))
    }
}

/// Probes one group of unique puts concurrently and plans the survivors'
/// puts into `writes`, draining `pending`. Present with a different row id
/// rejects the batch; present with the same row id is a re-derived entry and
/// plans nothing.
async fn resolve_probes(
    probe: &ProbeHandle<'_>,
    deleted_unique: &HashSet<Vec<u8>>,
    pending: &mut Vec<PendingProbe>,
    poisoned: &mut Vec<u64>,
    writes: &mut Vec<StagedWrite>,
) -> Result<()> {
    let held: Vec<Option<Bytes>> = stream::iter(0..pending.len())
        .map(|position| {
            let key_bytes = pending[position].key.clone();
            // A value this same batch deletes is free again, whatever the
            // store still holds, so it needs no read at all.
            let deleted = deleted_unique.contains(&key_bytes);
            async move {
                if deleted {
                    Ok(None)
                } else {
                    probe.get(&key_bytes).await
                }
            }
        })
        .buffered(UNIQUENESS_PROBE_CONCURRENCY)
        .try_collect()
        .await?;

    // Resolved in batch order, so which entry a rejection names does not
    // depend on which probe happened to finish first.
    for (probe, present) in pending.drain(..).zip(held) {
        if let Some(bytes) = present {
            if decode_row_id(&bytes)? != probe.row_id {
                match collision(probe.index_id, probe.building, poisoned) {
                    Some(error) => return Err(error),
                    // Dropping the claim leaves the live holder's entry.
                    None => continue,
                }
            }
            continue;
        }
        writes.push((probe.key, Some(probe.row_id.to_be_bytes().to_vec())));
    }
    Ok(())
}

/// Resolves accumulated entries into planned writes, enforcing uniqueness at
/// commit against `probe`. Returns the ids of poisoned building indexes and
/// the writes to stage — deletes first, then puts, so staging them in order
/// makes a delete-then-reinsert of one unique value within a commit see the
/// value as absent. For each unique put: present with a **different** row id →
/// [`Error::Constraint`]; present with the **same** row id → no-op (a
/// re-derived entry); absent → planned. Duplicates within the commit are
/// caught in memory.
///
/// A collision against an index that is still **building** poisons it
/// instead: the id is returned, the claim is dropped so the live holder's
/// entry survives, and the commit proceeds. Coverage is partial until a
/// build flips ready, so failing the finder would decide by timing which
/// party a duplicate falls on.
///
/// Unique puts plan after non-unique ones rather than in entry order. Every
/// key is distinct — an entry's kind is part of its key — so only the
/// deletes-before-puts ordering above carries meaning.
pub(crate) async fn plan_index_entries(
    probe: ProbeHandle<'_>,
    entries: &[StagedIndexEntry],
) -> Result<(Vec<u64>, Vec<StagedWrite>)> {
    if entries.len() > MAX_INDEX_ENTRIES_PER_COMMIT {
        // Logged as well as returned: the refusal is the guardrail firing,
        // and an operator reading logs after a failed bulk load should see
        // it without having to recover the SQL error text.
        warn!(
            staged = entries.len(),
            limit = MAX_INDEX_ENTRIES_PER_COMMIT,
            "refusing an oversized commit"
        );
        return Err(oversized_commit(entries.len()));
    }

    let mut writes: Vec<StagedWrite> = Vec::new();
    let mut deleted_unique: HashSet<Vec<u8>> = HashSet::new();
    for entry in entries.iter().filter(|entry| entry.delete) {
        let key_bytes = entry_key(entry).encode();
        if entry.unique {
            deleted_unique.insert(key_bytes.clone());
        }
        writes.push((key_bytes, None));
    }

    // Non-unique puts plan straight away; unique puts collapse to one probe
    // per distinct key. Two entries claiming one value for different rows
    // collide here, in memory, before any read. Presence is then resolved a
    // group of point reads at a time.
    let mut pending: Vec<PendingProbe> = Vec::new();
    let mut claimed: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut poisoned: Vec<u64> = Vec::new();
    for entry in entries.iter().filter(|entry| !entry.delete) {
        let key_bytes = entry_key(entry).encode();
        if !entry.unique {
            // The row id lives in the key; the value is empty.
            writes.push((key_bytes, Some(Vec::new())));
            continue;
        }
        match claimed.get(&key_bytes) {
            Some(&holder) if holder != entry.row_id => {
                match collision(entry.index_id, entry.building, &mut poisoned) {
                    Some(error) => return Err(error),
                    None => continue,
                }
            }
            Some(_) => continue,
            None => claimed.insert(key_bytes.clone(), entry.row_id),
        };
        pending.push(PendingProbe {
            key: key_bytes,
            row_id: entry.row_id,
            index_id: entry.index_id,
            building: entry.building,
        });
        if pending.len() >= UNIQUENESS_PROBE_GROUP {
            resolve_probes(
                &probe,
                &deleted_unique,
                &mut pending,
                &mut poisoned,
                &mut writes,
            )
            .await?;
        }
    }
    resolve_probes(
        &probe,
        &deleted_unique,
        &mut pending,
        &mut poisoned,
        &mut writes,
    )
    .await?;

    poisoned.sort_unstable();
    poisoned.dedup();
    Ok((poisoned, writes))
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
) -> Result<usize> {
    let mut deleted = 0;
    for kind in [IndexKind::Unique, IndexKind::Multi] {
        if deleted >= limit {
            break;
        }
        let (batch, _) = reclaim_entries_from(tx, kind, index_id, limit - deleted, None).await?;
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
) -> Result<(usize, Option<Vec<u8>>)> {
    let prefix = index_index_prefix(kind, index_id);
    // `scan_prefix` takes its bounds as a suffix of the prefix.
    let suffix = match start_from {
        Some(key) if key.len() >= prefix.len() => key[prefix.len()..].to_vec(),
        _ => Vec::new(),
    };

    let mut iter = ReadHandle::Tx(tx).scan_prefix(prefix, suffix..).await?;
    let mut deleted = 0;
    let mut last = None;
    while deleted < limit {
        match iter.next().await? {
            Some(entry) => {
                let key = entry.key.to_vec();
                tx.delete(entry.key)?;
                deleted += 1;
                last = Some(key);
            }
            None => break,
        }
    }
    Ok((deleted, last))
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

/// The row ids of the entries under `prefix` within `subrange`, with an
/// unfolded tail's writes merged over the store's: a tail put shadows the
/// stored entry, a tail delete hides it. Both sides ascend by key, so one pass
/// over each answers the probe, and only the tail's slice of the range is held
/// in memory.
async fn merged_row_ids(
    reader: ReadHandle<'_>,
    tail: Option<&Overlay>,
    prefix: Vec<u8>,
    subrange: Subrange,
    row_id_of: RowIdOf,
) -> Result<Vec<u64>> {
    let writes: Vec<(Vec<u8>, Option<Vec<u8>>)> = tail.map_or_else(Vec::new, |tail| {
        tail.prefixed(&prefix)
            .filter(|(key, _)| in_subrange(key, &prefix, &subrange))
            .map(|(key, value)| (key.to_vec(), value.map(<[u8]>::to_vec)))
            .collect()
    });

    let mut iter = reader
        .scan_prefix(prefix, subrange)
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
            (Some(entry), Some((key, value))) => match key.as_slice().cmp(entry.key.as_ref()) {
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
            },
        }
    }

    Ok(row_ids)
}

/// One entry, an unfolded tail's write for it taking precedence over the
/// store's.
async fn read_entry(
    reader: ReadHandle<'_>,
    tail: Option<&Overlay>,
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

/// The row ids holding one indexed value: a point-get for a unique index,
/// an ascending prefix scan for a non-unique one. `tail` carries the writes of
/// slots no folder has applied yet, when the store is slot-backed.
pub(crate) async fn lookup_row_ids(
    reader: ReadHandle<'_>,
    tail: Option<&Overlay>,
    index_id: u64,
    unique: bool,
    key: &CanonicalKey,
) -> Result<Vec<u64>> {
    if unique {
        let entry_key = Key::Index(IndexKey::Unique {
            index_id,
            key: key.clone(),
        })
        .encode();
        return match read_entry(reader, tail, &entry_key).await? {
            Some(bytes) => Ok(vec![decode_row_id(&bytes)?]),
            None => Ok(Vec::new()),
        };
    }

    merged_row_ids(
        reader,
        tail,
        index_multi_value_prefix(index_id, key),
        (Bound::Unbounded, Bound::Unbounded),
        multi_row_id,
    )
    .await
}

/// The row ids whose indexed value falls between `lower` and `upper`, in the
/// index's stored order. Ordered encoding makes byte order equal value order,
/// so the query is a bounded sub-scan of the index's contiguous range; the
/// bounds are the canonical values, already encoded in the columns' declared
/// directions. A unique entry carries its row id in the value, a non-unique
/// one in the key. `tail` is treated as in [`lookup_row_ids`].
pub(crate) async fn range_row_ids(
    reader: ReadHandle<'_>,
    tail: Option<&Overlay>,
    index_id: u64,
    unique: bool,
    leading_nulls: NullOrder,
    lower: Bound<CanonicalKey>,
    upper: Bound<CanonicalKey>,
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

    let row_id_of = if unique { unique_row_id } else { multi_row_id };

    merged_row_ids(reader, tail, prefix, (start, end), row_id_of).await
}

/// The row ids of live rows whose leading indexed columns match `prefix` — a
/// canonical key over a leading run of `= value` and `IS NULL` predicates.
/// A row with any NULL indexed column is stored multi-shaped, so `IS NULL`
/// queries scan the `multi` subrange; the value framing's terminator is
/// dropped from the scan prefix so it matches every key that extends the run.
/// `tail` is treated as in [`lookup_row_ids`].
pub(crate) async fn null_prefix_row_ids(
    reader: ReadHandle<'_>,
    tail: Option<&Overlay>,
    index_id: u64,
    prefix: &CanonicalKey,
) -> Result<Vec<u64>> {
    let mut scan_prefix = index_multi_value_prefix(index_id, prefix);
    // `index_multi_value_prefix` frames the value and appends a terminator for an
    // exact-value scan; dropping it turns the bytes into a true leading prefix.
    scan_prefix.pop();

    merged_row_ids(
        reader,
        tail,
        scan_prefix,
        (Bound::Unbounded, Bound::Unbounded),
        multi_row_id,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use moraine_wal::{Commit, Envelope, SlotPayload, SlotWrite};
    use object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;
    use crate::store::{
        index_encoding::{Direction, IndexKeyValue, IntWidth, encode_ordered_values},
        open::StoreBuilder,
    };

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
    fn tail_writing(writes: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Overlay {
        let mut overlay = Overlay::default();
        overlay.absorb(&Envelope {
            leader: None,
            commits: vec![Commit {
                transaction_id: [1; 16],
                payload: SlotPayload {
                    validated_head: 0,
                    changes_made: String::new(),
                    writes: writes
                        .into_iter()
                        .map(|(key, value)| SlotWrite { key, value })
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
        let db = StoreBuilder::new("t", Arc::new(InMemory::new()))
            .open_writer()
            .await
            .unwrap();
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for (value, row_id) in [(10_u128, 1_u64), (20, 2), (30, 3)] {
            tx.put(multi_key(7, value, row_id), []).unwrap();
        }
        crate::transaction::commit::commit_durably(tx)
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
            )
            .await
            .unwrap(),
            vec![1, 2, 4, 5]
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
