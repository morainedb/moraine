//! The multi-writer head: the folded store view with every slot above the
//! fold cursor replayed onto it.

use std::{
    cmp::Ordering,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    time::Instant,
};

use moraine_wal::{Commit, Envelope, Overlay, Race, SlotLog, SlotPayload, SlotWrite};
use slatedb::DbReader;
use tracing::warn;

use crate::{
    catalog::{CatalogSnapshot, Contention, SlotStore},
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{Key, SysKey},
        open::StoreBuilder,
        proto::HeadValue,
        read, value,
    },
    transaction::{
        commit::{self, StagedWrite, fold},
        operations::{ChangeSet, conflicts},
    },
};

/// Unfolded-tail length past which a materialized head is warned about: fold
/// lag has grown enough that every read replays a long tail and truncation
/// cannot advance. An operator signal, not a limit — commits still proceed.
pub(crate) const FOLD_STALL_THRESHOLD: u64 = 512;

/// Slot-race and retry counters a slot-backed store accumulates across every
/// commit through the handle and its clones. A lost race is the signal
/// contention-triggered forwarding acts on, so `races_lost` is a live counter,
/// not only a log line.
#[derive(Debug, Default)]
pub(crate) struct ContentionCounters {
    committed: AtomicU64,
    races_lost: AtomicU64,
    exhausted: AtomicU64,
}

impl ContentionCounters {
    /// Records one commit that won its slot after losing `races_lost` races.
    pub(crate) fn record_committed(&self, races_lost: u64) {
        self.committed.fetch_add(1, AtomicOrdering::Relaxed);
        self.races_lost
            .fetch_add(races_lost, AtomicOrdering::Relaxed);
    }

    /// Records one commit that spent its retry budget without winning.
    pub(crate) fn record_exhausted(&self) {
        self.exhausted.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Records one lost slot race the staged path surfaces to its caller — the
    /// verb path counts its own losses through [`record_committed`]. A nonzero
    /// `races_lost` is the contention proof that arms forwarding.
    ///
    /// [`record_committed`]: Self::record_committed
    pub(crate) fn record_lost(&self) {
        self.races_lost.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// A point-in-time read of the counters for a host or a forwarding client.
    pub(crate) fn snapshot(&self) -> Contention {
        Contention {
            commits: self.committed.load(AtomicOrdering::Relaxed),
            races_lost: self.races_lost.load(AtomicOrdering::Relaxed),
            exhaustions: self.exhausted.load(AtomicOrdering::Relaxed),
        }
    }
}

mod coalesce;
pub(crate) use coalesce::CommitCoalescer;

/// Per-store forwarding state shared across a handle's clones: the set of
/// leader endpoints given up on for the life of the process. Aging is local —
/// a client that cannot reach an advertised address stops trying it and commits
/// directly, without deciding anything about the endpoint for its peers.
#[cfg(feature = "leader")]
#[derive(Debug, Default)]
pub(crate) struct Forwarding {
    aged: Mutex<std::collections::HashSet<String>>,
}

#[cfg(feature = "leader")]
impl Forwarding {
    fn lock(&self) -> MutexGuard<'_, std::collections::HashSet<String>> {
        self.aged.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stops trying `endpoint` for the rest of this process's life.
    pub(crate) fn age(&self, endpoint: &str) {
        self.lock().insert(endpoint.to_string());
    }

    /// Whether `endpoint` was aged out this session.
    pub(crate) fn is_aged(&self, endpoint: &str) -> bool {
        self.lock().contains(endpoint)
    }
}

/// A reachable leader a contended staged transaction forwards to: its
/// advertised endpoint and the store-held session token.
#[cfg(feature = "leader")]
pub(crate) struct ForwardTarget {
    pub(crate) endpoint: String,
    pub(crate) secret: [u8; commit::SECRET_LEN],
}

/// The leader a staged transaction should forward to, or `None` to commit
/// directly. Forwarding is armed only by a lost race — an uncontended client
/// (`races_lost == 0`) never connects, so a live leader's advert alone never
/// pulls a solo committer onto the network. A withdrawal (endpoint-absent
/// advert) and an endpoint already aged this session both resolve to direct.
#[cfg(feature = "leader")]
pub(crate) async fn forward_target(store: &SlotStore) -> Result<Option<ForwardTarget>> {
    if store.contention.snapshot().races_lost == 0 {
        return Ok(None);
    }

    let Some(advert) = crate::leader::current_advert(store).await? else {
        return Ok(None);
    };
    let Some(endpoint) = advert.endpoint else {
        return Ok(None);
    };
    if store.forwarding.is_aged(&endpoint) {
        return Ok(None);
    }

    let Some(secret) = read_forwarding_secret(store).await? else {
        return Ok(None);
    };
    Ok(Some(ForwardTarget { endpoint, secret }))
}

/// The forwarding context a latched staged transaction carries: the leader
/// target plus the store parts a direct fallback and an ambiguous-outcome scan
/// need.
#[cfg(feature = "leader")]
pub(crate) fn forward_context(
    store: &SlotStore,
    target: ForwardTarget,
) -> crate::transaction::staged::forward::Forward {
    crate::transaction::staged::forward::Forward::new(
        target.endpoint,
        target.secret,
        Arc::clone(&store.forwarding),
        Arc::clone(&store.object_store),
        store.options.path.clone(),
        store.options.cache_dir.clone(),
        store.slots.clone(),
    )
}

/// The store-held session token, read through the handle's reader and, if that
/// has not polled it yet, a fresh one. `None` when the store holds no token —
/// the client then commits directly rather than forward unauthenticated.
#[cfg(feature = "leader")]
async fn read_forwarding_secret(store: &SlotStore) -> Result<Option<[u8; commit::SECRET_LEN]>> {
    if let Some(value) = read::read_secret(ReadHandle::Reader(&store.reader)).await? {
        return Ok(Some(secret_bytes(&value)?));
    }
    let reader = reopen_reader(store).await?;
    let value = read::read_secret(ReadHandle::Reader(&reader)).await;
    release_reader(Some(&reader)).await;
    value?.map(|value| secret_bytes(&value)).transpose()
}

/// The token bytes at the stored width, refusing a wrong one.
#[cfg(feature = "leader")]
fn secret_bytes(value: &crate::store::proto::SecretValue) -> Result<[u8; commit::SECRET_LEN]> {
    <[u8; commit::SECRET_LEN]>::try_from(value.token.as_slice()).map_err(|_| {
        Error::Corruption(format!(
            "forwarding token is {} bytes, expected {}",
            value.token.len(),
            commit::SECRET_LEN
        ))
    })
}

/// The multi-writer head: folded store state plus replay of every slot
/// past the fold cursor.
pub(crate) struct SlotHead {
    pub(crate) view: CatalogSnapshot,
    /// The tail's writes, keyed by encoded store key: read-your-tail for
    /// probes the projection does not model (index entries above all).
    pub(crate) overlay: Overlay,
    /// The next unwritten slot sequence — the one a commit races for.
    pub(crate) next_sequence: u64,
    /// Set when the view came from a reader this materialization opened rather
    /// than the handle's. Any read that must match the view — an index entry
    /// scan above all — goes through it, and [`release_reader`] closes it.
    pub(crate) reader: Option<DbReader>,
}

impl std::fmt::Debug for SlotHead {
    // `DbReader` carries no `Debug`, so the reader is reported by presence.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotHead")
            .field("view", &self.view)
            .field("overlay", &self.overlay)
            .field("next_sequence", &self.next_sequence)
            .field("has_own_reader", &self.reader.is_some())
            .finish()
    }
}

impl SlotHead {
    /// A read handle over the reader this head's view came from: the one it
    /// opened for itself, else `folded` — the handle's own.
    pub(crate) fn handle<'a>(&'a self, folded: ReadHandle<'a>) -> ReadHandle<'a> {
        match &self.reader {
            Some(reader) => ReadHandle::Reader(reader),
            None => folded,
        }
    }
}

/// The head as of now, read through the handle's shared reader: for the commit
/// cycle and catalog-view reads, which the unfolded tail keeps current without
/// a store refresh.
pub(crate) async fn materialize_slot_head(store: &SlotStore) -> Result<SlotHead> {
    slot_head(store, None, false).await
}

/// The head as of now, read through a reader opened for this materialization so
/// it reflects the latest durable store state — folder-role writes (index
/// backfills and maintenance sweeps) included — that the handle's reader has
/// not yet polled. For entry scans, which read those writes directly from the
/// store rather than through the tail.
pub(crate) async fn materialize_slot_head_fresh(store: &SlotStore) -> Result<SlotHead> {
    slot_head(store, None, true).await
}

/// How many slots one cached head may absorb incrementally before a read
/// re-materializes from the folded store. The overlay accumulates every slot
/// it absorbs, so this bounds a no-folder run from growing it without end.
const MAX_INCREMENTAL_SLOTS: u64 = 512;

/// The materialized head cached across a handle's reads. Cheap to clone —
/// every clone shares the one cache — so the commit paths that hold no
/// `SlotStore` can still update it. Within
/// [`CatalogOptions::reader_poll_interval`] a read serves the cached head
/// untouched; past it, one LIST from `next_sequence` either finds the head
/// unchanged or absorbs the new slots into the cached view.
#[derive(Clone, Default)]
pub(crate) struct HeadCache {
    entry: Arc<Mutex<Option<CachedHead>>>,
}

/// The cached logical head: enough to answer a read and to premise a commit,
/// with the freshness bookkeeping the bound uses.
struct CachedHead {
    view: CatalogSnapshot,
    overlay: Overlay,
    next_sequence: u64,
    /// The frontier at the last full materialization. Incremental absorbs past
    /// `next_sequence - anchor_sequence > MAX_INCREMENTAL_SLOTS`
    /// re-materialize.
    anchor_sequence: u64,
    refreshed_at: Instant,
}

impl HeadCache {
    fn lock(&self) -> MutexGuard<'_, Option<CachedHead>> {
        self.entry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Roughly what the cached head holds, in bytes: the decoded view plus
    /// the unfolded tail it carries. Zero when nothing is cached.
    pub(crate) fn estimated_bytes(&self) -> u64 {
        self.lock().as_ref().map_or(0, |head| {
            head.view
                .estimated_bytes()
                .saturating_add(head.overlay.estimated_bytes())
        })
    }

    /// Clears the cache, so the next read re-materializes from the folded
    /// store.
    pub(crate) fn invalidate(&self) {
        *self.lock() = None;
    }

    /// Records the head a commit produced, so a read on the same handle sees
    /// its own write regardless of the refresh window. `view` and `overlay`
    /// already carry the committed writes, and `next_sequence` is one past the
    /// sequence it won. The old anchor rides forward, so the incremental bound
    /// still forces a periodic re-materialization.
    pub(crate) fn record(&self, view: &CatalogSnapshot, overlay: &Overlay, next_sequence: u64) {
        let mut cache = self.lock();
        let anchor = cache
            .as_ref()
            .map_or(next_sequence, |cached| cached.anchor_sequence);
        *cache = Some(CachedHead {
            view: view.clone(),
            overlay: overlay.clone(),
            next_sequence,
            anchor_sequence: anchor,
            refreshed_at: Instant::now(),
        });
    }
}

impl CachedHead {
    fn to_head(&self) -> SlotHead {
        SlotHead {
            view: self.view.clone(),
            overlay: self.overlay.clone(),
            next_sequence: self.next_sequence,
            reader: None,
        }
    }
}

/// The head for a read: the cached head when the freshness bound allows, else a
/// revalidation that repopulates the cache. Within `reader_poll_interval` this
/// touches the store not at all — the bounded-staleness a read accepts in
/// exchange for the freed request.
pub(crate) async fn cached_slot_head(store: &SlotStore) -> Result<SlotHead> {
    {
        let cache = store.head_cache.lock();
        if let Some(cached) = cache.as_ref()
            && (store.pinned || cached.refreshed_at.elapsed() < store.options.reader_poll_interval)
        {
            return Ok(cached.to_head());
        }
    }
    // A pinned attach reads a fixed cut: it materializes the folded store once
    // and never revalidates against the tail committed after the checkpoint.
    if store.pinned {
        return materialize_into_cache(store).await;
    }
    revalidate_head(store).await
}

/// The head for a commit premise: always revalidates the frontier, never the
/// within-window shortcut a read may take. A commit races `next_sequence`, so a
/// stale one racing a folded-and-truncated sequence would fork history; the
/// LIST here settles the true frontier. It still skips the folded-store scan
/// when the cached view can absorb the new slots.
pub(crate) async fn revalidated_slot_head(store: &SlotStore) -> Result<SlotHead> {
    revalidate_head(store).await
}

/// Validity is one LIST from the cached `next_sequence`, never a point probe on
/// that sequence. A slot written, folded, and truncated between two reads
/// leaves nothing at `next_sequence`, so a probe would read it absent and call
/// the cache fresh — serving a head missing that commit. An empty LISTING is
/// the head unchanged, because a truncated run always leaves newer slots
/// visible. New slots absorb into the cached view; a hole, a run grown past the
/// bound, or a slot that does not chain re-materializes from the folded store.
async fn revalidate_head(store: &SlotStore) -> Result<SlotHead> {
    let carried = {
        let cache = store.head_cache.lock();
        cache.as_ref().map(|cached| {
            (
                cached.view.clone(),
                cached.overlay.clone(),
                cached.next_sequence,
                cached.anchor_sequence,
            )
        })
    };

    let Some((mut view, mut overlay, mut next_sequence, anchor)) = carried else {
        return materialize_into_cache(store).await;
    };

    let tail = store.slots.read_tail(next_sequence).await?;
    if tail.gap_at.is_some() || next_sequence.saturating_sub(anchor) > MAX_INCREMENTAL_SLOTS {
        return materialize_into_cache(store).await;
    }

    for (sequence, envelope) in &tail.slots {
        for commit in &envelope.commits {
            match admit(&view, commit, *sequence)? {
                Admission::Apply => apply(&mut view, commit, *sequence)?,
                // A cached view a new slot does not chain onto is not an
                // incremental step; the folded store settles it.
                Admission::Skip => return materialize_into_cache(store).await,
            }
        }
        overlay.absorb(envelope);
        next_sequence = sequence.saturating_add(1);
    }

    let head = SlotHead {
        view,
        overlay,
        next_sequence,
        reader: None,
    };
    store_cached_head(store, &head, anchor);
    Ok(head)
}

/// A full materialization, cached as a fresh anchor. Its own reader (opened
/// past a truncation) is not cached — only the logical view travels — so a
/// later read never reuses a reader the batch closed.
async fn materialize_into_cache(store: &SlotStore) -> Result<SlotHead> {
    let head = materialize_slot_head(store).await?;
    store_cached_head(store, &head, head.next_sequence);
    Ok(head)
}

fn store_cached_head(store: &SlotStore, head: &SlotHead, anchor_sequence: u64) {
    *store.head_cache.lock() = Some(CachedHead {
        view: head.view.clone(),
        overlay: head.overlay.clone(),
        next_sequence: head.next_sequence,
        anchor_sequence,
        refreshed_at: Instant::now(),
    });
}

/// Closes a reader a hole retry opened. A handle's own reader is left alone; a
/// close that fails is logged, never substituted for the caller's outcome.
pub(crate) async fn release_reader(reader: Option<&DbReader>) {
    if let Some(reader) = reader
        && let Err(err) = reader.close().await
    {
        warn!(error = %err, "could not close the reader opened past a truncated prefix");
    }
}

/// The view at `snapshot`: a target at or below the folded head is history the
/// store still holds; above it, only the tail carries it.
pub(crate) async fn materialize_slot_view_at(
    store: &SlotStore,
    snapshot: u64,
) -> Result<CatalogSnapshot> {
    let handle = ReadHandle::Reader(&store.reader);
    // The head pointer routes between two reads that each establish their own
    // consistency. A reader that has taken the whole log answers by itself: its
    // view holds every commit, including one whose record is backdated to at or
    // below the target. A reader short of the log's end cannot, however high its
    // head reads, so it routes to the replay below.
    let cursor = crate::transaction::folder::reader_cursor(&store.reader);
    let tail = store.slots.tail_extent(cursor.saturating_add(1)).await?;
    let reader_holds_the_log = tail.gap_at.is_none()
        && tail
            .last
            .is_none_or(|end| crate::transaction::folder::reader_reached(&store.reader, end));
    if reader_holds_the_log && snapshot <= folded_head(handle).await? {
        return commit::materialize(handle, Some(snapshot)).await;
    }

    // The whole tail is replayed, not a prefix truncated at the target: a
    // later commit's backdated record — a flush's data file effective at or
    // below the target — is in the overlay the target's view filters over.
    let head = slot_head(store, None, false).await?;
    let reached = head.view.snapshot.snapshot_id;
    let outcome =
        commit::materialize_overlaid(head.handle(handle), &head.overlay, reached, snapshot).await;
    release_reader(head.reader.as_ref()).await;

    outcome
}

/// Resolves whether `transaction_id` committed, and at which snapshot: the
/// folded snapshot records above `floor`, then the unfolded tail. A hole in
/// the scanned tail surfaces as [`Error::Corruption`] rather than a false
/// `None` — reporting a possibly-committed transaction absent is the one wrong
/// answer.
///
/// The reader is opened at the current manifest, so a just-folded snapshot
/// record the handle's shared reader has not yet polled is still seen.
pub(crate) async fn transaction_outcome(
    store: &SlotStore,
    transaction_id: [u8; 16],
    floor: u64,
) -> Result<Option<u64>> {
    let reader = reopen_reader(store).await?;
    let outcome = resolve_outcome(&reader, &store.slots, transaction_id, floor).await;
    release_reader(Some(&reader)).await;
    outcome
}

/// [`transaction_outcome`] over the parts a forwarding client holds: it opens
/// its own fresh reader (the handle's may lag a just-folded record) and scans
/// the same folded-then-tail path. Resolves an ambiguous forwarded commit —
/// the slot landed but the ack was lost — by the client's own transaction id.
#[cfg(feature = "leader")]
pub(crate) async fn transaction_outcome_from(
    object_store: &Arc<dyn object_store::ObjectStore>,
    path: &str,
    cache_dir: Option<std::path::PathBuf>,
    slots: &SlotLog,
    transaction_id: [u8; 16],
    floor: u64,
) -> Result<Option<u64>> {
    let reader = StoreBuilder::new(path, Arc::clone(object_store))
        .cache_dir(cache_dir)
        .open_reader()
        .await?;
    let (reader, _) = reader;
    let outcome = resolve_outcome(&reader, slots, transaction_id, floor).await;
    release_reader(Some(&reader)).await;
    outcome
}

/// The folded-record scan, then the tail scan, for one transaction id through
/// `reader`.
async fn resolve_outcome(
    reader: &DbReader,
    slots: &SlotLog,
    transaction_id: [u8; 16],
    floor: u64,
) -> Result<Option<u64>> {
    let handle = ReadHandle::Reader(reader);
    if let Some(snapshot_id) = read::snapshot_of_transaction(handle, floor, &transaction_id).await?
    {
        return Ok(Some(snapshot_id));
    }

    let fold = crate::transaction::folder::reader_cursor(reader);
    match slots
        .find_transaction(fold.saturating_add(1), transaction_id)
        .await?
    {
        Some(sequence) => Ok(Some(
            tail_minted_snapshot(slots, sequence, transaction_id).await?,
        )),
        None => Ok(None),
    }
}

/// The snapshot a committed transaction minted, read from the slot the tail
/// scan found it in: the carrying commit's validated head plus one.
async fn tail_minted_snapshot(
    slots: &SlotLog,
    sequence: u64,
    transaction_id: [u8; 16],
) -> Result<u64> {
    let envelope = slots.read_slot(sequence).await?.ok_or_else(|| {
        Error::Corruption(format!(
            "slot {sequence} carried a transaction on the tail scan but reads absent"
        ))
    })?;

    envelope
        .commits
        .iter()
        .find(|commit| commit.transaction_id == transaction_id)
        .map(|commit| commit.payload.validated_head.saturating_add(1))
        .ok_or_else(|| {
            Error::Corruption(format!(
                "slot {sequence} was reported to carry a transaction it does not hold"
            ))
        })
}

/// Replays the tail, telling a truncated prefix this reader is behind from a
/// destroyed slot. A hole is never served as a head: continuing past it would
/// hide committed state and let the next committer re-win that sequence.
///
/// `fresh` opens a reader for this materialization rather than reusing the
/// handle's — the entry-scan paths need it, so a folder-role write the handle's
/// reader has not yet polled is still seen.
async fn slot_head(store: &SlotStore, until: Option<u64>, fresh: bool) -> Result<SlotHead> {
    // A pinned attach reads the folded store the checkpoint captured and never
    // the tail committed after it, so its head is the folded view with no
    // overlay.
    if store.pinned {
        let handle = ReadHandle::Reader(&store.reader);
        let view = commit::materialize(handle, None).await?;
        let next_sequence =
            crate::transaction::folder::reader_cursor(&store.reader).saturating_add(1);
        return Ok(SlotHead {
            view,
            overlay: Overlay::default(),
            next_sequence,
            reader: None,
        });
    }

    if !fresh && let Replayed::Head(head) = replay(&store.reader, &store.slots, until).await? {
        return Ok(*head);
    }

    // Either an entry-scan read that must see the latest durable store state, or
    // a hole through the handle's reader that a peer may merely have truncated:
    // a live `DbReader` cannot be refreshed, so only a freshly opened one can
    // tell a stale cursor from a slot destroyed outside the protocol.
    let reader = reopen_reader(store).await?;
    match replay(&reader, &store.slots, until).await {
        // The view came from this reader, so it travels with the head: an entry
        // scan must read through it, not the handle's staler reader.
        Ok(Replayed::Head(mut head)) => {
            head.reader = Some(reader);
            Ok(*head)
        }
        Ok(Replayed::Hole { gap_at, folded }) => {
            release_reader(Some(&reader)).await;
            Err(Error::Corruption(format!(
                "slot {gap_at} is absent while higher slots are present, and the fold cursor \
                 {folded} is below it: nothing folded that commit, so no truncation could have \
                 removed it"
            )))
        }
        Err(err) => {
            release_reader(Some(&reader)).await;
            Err(err)
        }
    }
}

/// One replay's outcome. A hole never reaches a caller as a head.
enum Replayed {
    /// The tail replayed onto the folded view.
    Head(Box<SlotHead>),
    /// A sequence absent below a present one, with the fold cursor the tail
    /// was read from.
    Hole { gap_at: u64, folded: u64 },
}

/// Replays the tail onto the folded view, stopping once `until` is reached. A
/// truncated replay's overlay and `next_sequence` cover only the slots it
/// applied, so only an `until` of `None` describes the head.
async fn replay(reader: &DbReader, slots: &SlotLog, until: Option<u64>) -> Result<Replayed> {
    let handle = ReadHandle::Reader(reader);

    // The cursor is read before the view, both through this reader. A
    // `DbReader` follows the manifest on its own interval, so a refresh can
    // land between the two reads: a cursor stale against fresher data replays
    // slots the admission rule below skips, while a cursor ahead of the data
    // would drop the slots between them and serve a gapped catalog.
    let folded = crate::transaction::folder::reader_cursor(reader);
    let mut view = commit::materialize(handle, None).await?;

    // A fold cursor of `n` means slots `1..=n` are applied, so the tail starts
    // at `n + 1`.
    let from = folded.saturating_add(1);
    let tail = slots.read_tail(from).await?;
    if let Some(gap_at) = tail.gap_at {
        return Ok(Replayed::Hole { gap_at, folded });
    }

    let mut overlay = Overlay::default();
    let mut next_sequence = from;
    // Folding advances in log order under a prefix cursor, so the commits this
    // view already reflects are a leading prefix of the tail. Past the first
    // commit that applies, one that does not is a broken chain, not a fold this
    // cursor had not caught up to.
    let mut applied = false;
    // The head each commit in the tail leaves behind, checked slot by slot: the
    // next commit must have been validated against exactly that. A slot whose
    // commit was validated against anything else was substituted or reordered,
    // which the view cannot reveal — it already reflects the tail the reader
    // replayed for itself.
    //
    // Seeded with the head the tail must start from, so the first slot is held
    // to the same rule as every later one. Without the seed the view is the
    // only thing the first slot is checked against, and the view already
    // reflects it.
    let mut chain: Option<u64> = if tail.slots.is_empty() {
        None
    } else {
        tail_anchor(slots, folded).await?
    };
    'tail: for (sequence, envelope) in &tail.slots {
        for commit in &envelope.commits {
            if let Some(expected) = chain
                && commit.payload.validated_head != expected
            {
                return Err(Error::Corruption(format!(
                    "slot {sequence} was validated against snapshot {} where the slot before it \
                     leaves {expected}; its commit does not chain onto the one before it",
                    commit.payload.validated_head
                )));
            }
            chain = Some(committed_head(commit)?.unwrap_or(commit.payload.validated_head));

            if until.is_some_and(|target| view.snapshot.snapshot_id >= target) {
                break 'tail;
            }
            match admit(&view, commit, *sequence)? {
                Admission::Apply => {
                    apply(&mut view, commit, *sequence)?;
                    applied = true;
                }
                Admission::Skip if applied => {
                    return Err(Error::Corruption(format!(
                        "slot {sequence} was validated against snapshot {} after replay had \
                         already reached {}; its commit does not chain onto the one before it",
                        commit.payload.validated_head, view.snapshot.snapshot_id
                    )));
                }
                Admission::Skip => {}
            }
        }
        overlay.absorb(envelope);
        next_sequence = sequence.saturating_add(1);
    }

    Ok(Replayed::Head(Box::new(SlotHead {
        view,
        overlay,
        next_sequence,
        reader: None,
    })))
}

/// The head the slot after `folded` must have been validated against: the one
/// the last folded slot leaves, read from the log itself.
///
/// `None` when the log cannot supply it — nothing folded yet, or the folded
/// slot already truncated away. Only the store records the head in those
/// cases, and a reader replaying the log as its write-ahead log holds the
/// tail folded into that view, so it cannot read the two apart. An unanchored
/// tail is still checked slot against slot; it is the first slot that goes
/// unchecked.
async fn tail_anchor(slots: &SlotLog, folded: u64) -> Result<Option<u64>> {
    if folded == 0 {
        return Ok(None);
    }

    let Some(envelope) = slots.read_slot(folded).await? else {
        return Ok(None);
    };

    let mut head = None;
    for commit in &envelope.commits {
        head = Some(committed_head(commit)?.unwrap_or(commit.payload.validated_head));
    }

    Ok(head)
}

/// The head a commit leaves behind, read from its own `sys/head` write.
/// `None` when it writes no head — a commit that changes state without minting
/// a snapshot leaves the head where it found it.
fn committed_head(commit: &Commit) -> Result<Option<u64>> {
    let head_key = Key::Sys(SysKey::Head).encode();
    commit
        .payload
        .writes
        .iter()
        .rev()
        .find(|write| write.key == head_key)
        .and_then(|write| write.value.as_ref())
        .map(|bytes| Ok(value::decode_value::<HeadValue>(bytes)?.snapshot_id))
        .transpose()
}

/// Whether one commit's writes belong on the replaying view.
#[derive(Debug)]
enum Admission {
    /// The commit staged against exactly this head.
    Apply,
    /// The commit is already reflected in the folded view.
    Skip,
}

/// Admits one commit by its validated head: equal applies, less is already
/// folded in, and greater means the slots between the view and this commit are
/// missing — a substituted or reordered slot never applies its writes.
fn admit(view: &CatalogSnapshot, commit: &Commit, sequence: u64) -> Result<Admission> {
    let head = view.snapshot.snapshot_id;
    let validated_head = commit.payload.validated_head;
    match validated_head.cmp(&head) {
        Ordering::Equal => Ok(Admission::Apply),
        Ordering::Less => Ok(Admission::Skip),
        Ordering::Greater => Err(Error::Corruption(format!(
            "slot {sequence} was validated against snapshot {validated_head} but replay \
             reached only {head}; the slots between them are missing"
        ))),
    }
}

/// Applies one commit's staged batch. The log is authoritative, so a batch
/// that cannot replay is a broken store rather than a stale view.
fn apply(view: &mut CatalogSnapshot, commit: &Commit, sequence: u64) -> Result<()> {
    let writes: Vec<StagedWrite> = commit
        .payload
        .writes
        .iter()
        .map(|write| (write.key.clone(), write.value.clone()))
        .collect();

    fold::fold_batch(view, &writes)
        .map_err(|err| Error::Corruption(format!("slot {sequence} could not be replayed: {err}")))
}

/// Builds one commit from its arbitration-independent parts: the batch every
/// backing computes the same way, wrapped with the transaction id and the
/// validated head a replay chains on. Shared by the verb cycle's coalescer and
/// the staged-row path.
pub(crate) fn commit_from(
    transaction_id: [u8; 16],
    validated_head: u64,
    changes_made: String,
    writes: &[StagedWrite],
) -> Commit {
    Commit {
        transaction_id,
        payload: SlotPayload {
            validated_head,
            changes_made,
            writes: writes
                .iter()
                .map(|(key, value)| SlotWrite {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        },
    }
}

/// Judges a lost race by comparing this commit's change set against every
/// commit the winner carries. A parse yielding an unknown kind classifies as a
/// conflict through [`conflicts`], so an unparseable winner is never benign.
fn classify_lost_race(ours: Option<&ChangeSet>, winner: &Envelope) -> Race {
    let Some(ours) = ours else {
        return Race::Conflict;
    };
    for commit in &winner.commits {
        let theirs = ChangeSet::parse(&commit.payload.changes_made);
        if conflicts(ours, &theirs) {
            return Race::Conflict;
        }
    }

    Race::Benign
}

/// The unfolded-tail length a head at `next_sequence` materialized against, for
/// the fold-lag log line. Best-effort through the attach reader: a fold-cursor
/// read failure reports 0 rather than failing the commit it only narrates, and
/// a lagged reader over-reports the tail, which is safe for a warning signal.
pub(super) async fn unfolded_tail_at(store: &SlotStore, next_sequence: u64) -> u64 {
    let folded = crate::transaction::folder::reader_cursor(&store.reader);
    next_sequence.saturating_sub(folded.saturating_add(1))
}

/// The head snapshot id the folded store carries.
async fn folded_head(handle: ReadHandle<'_>) -> Result<u64> {
    Ok(read::read_head(handle)
        .await?
        .ok_or_else(|| Error::Corruption("store has no head pointer".to_string()))?
        .snapshot_id)
}

/// A reader at the manifest as it stands now.
async fn reopen_reader(store: &SlotStore) -> Result<DbReader> {
    StoreBuilder::new(&store.options.path, Arc::clone(&store.object_store))
        .cache_dir(store.options.cache_dir.clone())
        .open_reader()
        .await
        .map(|(reader, _)| reader)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    };

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use moraine_wal::{Commit, Envelope, SlotLog, SlotPayload, SlotWrite};
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        memory::InMemory, path::Path,
    };
    use uuid::Uuid;

    use super::*;
    use crate::{
        catalog::{Catalog, CatalogOptions, ColumnDef, SlotStore, SnapshotId},
        store::{
            key::{EntityKey, Key, SysKey},
            open::StoreBuilder,
            proto, value,
        },
        transaction::{commit, operations::ChangeSet},
    };

    fn multi_writer_options() -> CatalogOptions {
        CatalogOptions::default()
    }

    /// A bootstrapped slot-backed store, attached as the catalog attaches it:
    /// bootstrap through the writer, then read through a `DbReader`.
    async fn bootstrap(object_store: Arc<dyn ObjectStore>) -> SlotStore {
        let options = multi_writer_options();
        let (db, _, _) = commit::open_initialized(
            StoreBuilder::new(&options.path, Arc::clone(&object_store)),
            false,
            None,
        )
        .await
        .unwrap();
        db.close().await.unwrap();

        let (reader, _) = StoreBuilder::new(&options.path, Arc::clone(&object_store))
            .open_reader()
            .await
            .unwrap();
        let slots = SlotLog::new(Arc::clone(&object_store), &options.path);
        let coalescer = CommitCoalescer::new(options.commit_batch_window);
        SlotStore {
            reader: Arc::new(reader),
            slots,
            object_store,
            options,
            read_only: false,
            pinned: false,
            coalescer,
            projections: Arc::new(std::sync::RwLock::new(
                crate::catalog::projection::ProjectionCache::empty(),
            )),
            head_cache: HeadCache::default(),
            contention: Arc::new(ContentionCounters::default()),
            #[cfg(feature = "leader")]
            forwarding: Arc::new(Forwarding::default()),
        }
    }

    /// The writes a commit creating one schema stages: the schema record, the
    /// minted snapshot record, and the head advance — the shapes
    /// `stage_bootstrap` produces, under the real key and value codecs.
    fn schema_writes(schema_id: u64, name: &str, snapshot_id: u64) -> Vec<SlotWrite> {
        let mut changes = ChangeSet::default();
        changes.created_schemas.insert(name.to_string());

        vec![
            SlotWrite {
                key: Key::current(EntityKey::Schema { schema_id }).encode(),
                value: Some(value::encode_value(&proto::SchemaValue {
                    schema_id,
                    schema_uuid: Uuid::new_v4().to_string(),
                    begin_snapshot: snapshot_id,
                    end_snapshot: None,
                    schema_name: name.to_string(),
                    path: format!("{name}/"),
                    path_is_relative: true,
                })),
            },
            SlotWrite {
                key: Key::Snapshot { snapshot_id }.encode(),
                value: Some(value::encode_value(&proto::SnapshotValue {
                    snapshot_id,
                    schema_version: snapshot_id,
                    next_catalog_id: schema_id + 1,
                    changes_made: changes.to_changes_made(),
                    ..proto::SnapshotValue::default()
                })),
            },
            SlotWrite {
                key: Key::Sys(SysKey::Head).encode(),
                value: Some(value::encode_value(&proto::HeadValue {
                    snapshot_id,
                    batch_seq: 0,
                })),
            },
        ]
    }

    /// One commit that creates a schema, minting the snapshot above
    /// `validated_head`.
    fn schema_commit(
        transaction_id: u8,
        schema_id: u64,
        name: &str,
        validated_head: u64,
    ) -> Commit {
        Commit {
            transaction_id: [transaction_id; 16],
            payload: SlotPayload {
                validated_head,
                changes_made: format!("created_schema:\"{name}\""),
                writes: schema_writes(schema_id, name, validated_head + 1),
            },
        }
    }

    /// One envelope holding one commit that creates a schema.
    fn schema_slot(
        transaction_id: u8,
        schema_id: u64,
        name: &str,
        validated_head: u64,
    ) -> Envelope {
        Envelope {
            leader: None,
            commits: vec![schema_commit(
                transaction_id,
                schema_id,
                name,
                validated_head,
            )],
        }
    }

    /// A commit carrying only a classification string, for judging a race
    /// without staging any writes.
    fn classified(changes_made: &str) -> Commit {
        Commit {
            transaction_id: [0; 16],
            payload: SlotPayload {
                validated_head: 0,
                changes_made: changes_made.to_string(),
                writes: vec![],
            },
        }
    }

    /// A lost race is judged against every commit a winner carries, so a
    /// conflict a second commit introduces is not missed; an unparseable
    /// classification is an unknown change and never benign; and an absent
    /// change set cannot be judged benign at all. Multi-commit winners first
    /// exist with the coalescer, so these branches get their own test rather
    /// than only correct-by-construction cover.
    #[test]
    fn classify_lost_race_scans_all_commits_and_refuses_the_unparseable() {
        let mut ours = ChangeSet::default();
        ours.altered_tables.insert(5);

        // Two commits, neither touching table 5: nothing to rebase away from.
        let benign = Envelope {
            leader: None,
            commits: vec![classified("altered_table:7"), classified("altered_table:9")],
        };
        assert_eq!(classify_lost_race(Some(&ours), &benign), Race::Benign);

        // The conflict is the winner's *second* commit; a scan that stopped at
        // the first would call this benign and apply over a real conflict.
        let conflicting = Envelope {
            leader: None,
            commits: vec![classified("altered_table:7"), classified("altered_table:5")],
        };
        assert_eq!(
            classify_lost_race(Some(&ours), &conflicting),
            Race::Conflict
        );

        // An unparseable classification parses to an unknown change, which
        // conflicts with anything.
        let unparseable = Envelope {
            leader: None,
            commits: vec![classified("not a change list at all")],
        };
        assert_eq!(
            classify_lost_race(Some(&ours), &unparseable),
            Race::Conflict
        );

        // No change set to compare cannot be called benign.
        assert_eq!(classify_lost_race(None, &benign), Race::Conflict);
    }

    /// Bootstrap mints the 32-byte forwarding token, readable straight back
    /// through the store — the leader authenticates forwarded sessions against
    /// it, and a store predating it mints it lazily on first bind.
    #[tokio::test]
    async fn bootstrap_mints_a_readable_secret() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        let secret = read::read_secret(ReadHandle::Reader(&store.reader))
            .await
            .unwrap()
            .expect("bootstrap mints a token");
        assert_eq!(secret.token.len(), 32);
    }

    /// The head is the folded store plus the tail: a slot no folder has
    /// applied is already visible, and its writes are readable as an overlay.
    #[tokio::test]
    async fn the_head_replays_the_tail_over_the_folded_view() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        let envelope = schema_slot(1, 1, "sales", 0);
        store.slots.put_slot(1, &envelope).await.unwrap();

        let head = materialize_slot_head(&store).await.unwrap();

        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(1));
        assert!(head.view.schema_by_name("sales").is_some());
        assert_eq!(head.next_sequence, 2);
        for write in &envelope.commits[0].payload.writes {
            assert_eq!(head.overlay.get(&write.key), Some(write.value.as_deref()));
        }
    }

    /// Slots apply in order, and every process replays the same order: a
    /// second slot staged against the first's result advances the head again.
    #[tokio::test]
    async fn the_tail_replays_in_slot_order() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();
        store
            .slots
            .put_slot(2, &schema_slot(2, 2, "staging", 1))
            .await
            .unwrap();

        let head = materialize_slot_head(&store).await.unwrap();

        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(2));
        assert_eq!(head.view.schemas().len(), 3);
        assert_eq!(head.next_sequence, 3);
    }

    /// The anchor the first unfolded slot is held to is the head the last
    /// folded slot leaves, taken from the log. It is absent exactly when the
    /// log cannot supply it: nothing folded, or that slot truncated away.
    #[tokio::test]
    async fn the_tail_anchors_on_the_head_the_folded_slot_leaves() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();

        // Slot 1 leaves head 1, so a slot following it owes exactly that.
        assert_eq!(tail_anchor(&store.slots, 1).await.unwrap(), Some(1));
        // Nothing folded, and a truncated slot, leave the store the only
        // record of the head.
        assert_eq!(tail_anchor(&store.slots, 0).await.unwrap(), None);
        assert_eq!(tail_anchor(&store.slots, 9).await.unwrap(), None);
    }

    /// A commit validated above the replayed head means the slots between them
    /// are missing, so its writes never apply.
    ///
    /// Refusing it needs the head the tail must start from, and here nothing
    /// is folded, so the log cannot supply it — see `tail_anchor`. Whether the
    /// bootstrap fold has reached this reader's manifest decides whether the
    /// anchor exists at all, which makes the outcome a race rather than a
    /// result. Reading the folded head apart from the replayed tail is what
    /// this needs.
    #[ignore = "the first slot is unanchored when nothing is folded; needs a folded-only head read"]
    #[tokio::test]
    async fn a_commit_validated_above_the_view_refuses() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        // Bootstrap leaves head 0, so a slot staged against head 7 has seven
        // predecessors nothing in this log holds.
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 7))
            .await
            .unwrap();

        let err = materialize_slot_head(&store).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        assert!(err.to_string().contains("slot 1"), "{err}");
    }

    /// A slot that does not chain onto the one before it is refused, not
    /// skipped: once a commit has applied in this replay, a following `less`
    /// is a broken chain, never a stale cursor. Skipping it would silently
    /// drop a committer's writes while reporting success.
    #[tokio::test]
    async fn a_slot_that_does_not_chain_after_an_apply_refuses() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();
        // A second slot staged against head 0 as well: whatever assembled it
        // did not stage it against slot 1's result.
        store
            .slots
            .put_slot(2, &schema_slot(2, 2, "staging", 0))
            .await
            .unwrap();

        let err = materialize_slot_head(&store).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        assert!(err.to_string().contains("slot 2"), "{err}");
    }

    /// The legitimate `less` case the skip rule exists for: a commit already
    /// reflected in the view is skipped rather than applied twice, while one
    /// staged against the view's own head applies and one staged above it is
    /// damage. The store's writes and its cursor now advance as one manifest,
    /// so the lagging cursor this rule guards against is a reader's, and the
    /// rule is pinned here rather than staged through a store.
    #[test]
    fn a_commit_already_reflected_in_the_view_is_skipped() {
        let view = view_at_head(2);

        let folded = commit_validated_against(1);
        assert!(matches!(admit(&view, &folded, 5).unwrap(), Admission::Skip));

        let current = commit_validated_against(2);
        assert!(matches!(
            admit(&view, &current, 5).unwrap(),
            Admission::Apply
        ));

        let ahead = commit_validated_against(3);
        let err = admit(&view, &ahead, 5).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        assert!(err.to_string().contains("slot 5"), "{err}");
    }

    /// A well-chained multi-commit envelope replays commit by commit to
    /// head + 2. Nothing produces these yet; the per-commit apply is what makes
    /// accepting them safe in advance.
    #[tokio::test]
    async fn a_chained_multi_commit_envelope_reaches_head_plus_two() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        let envelope = Envelope {
            leader: None,
            commits: vec![
                schema_commit(1, 1, "sales", 0),
                schema_commit(2, 2, "staging", 1),
            ],
        };
        store.slots.put_slot(1, &envelope).await.unwrap();

        let head = materialize_slot_head(&store).await.unwrap();

        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(2));
        assert!(head.view.schema_by_name("sales").is_some());
        assert!(head.view.schema_by_name("staging").is_some());
        assert_eq!(head.next_sequence, 2);
    }

    /// A multi-commit envelope whose second commit did not stage against the
    /// first's result fails its own replay under the latch, rather than
    /// applying the first and silently dropping the second.
    #[tokio::test]
    async fn a_badly_chained_multi_commit_envelope_refuses() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        let envelope = Envelope {
            leader: None,
            commits: vec![
                schema_commit(1, 1, "sales", 0),
                // Staged against head 0 as well: it does not chain onto commit 1.
                schema_commit(2, 2, "staging", 0),
            ],
        };
        store.slots.put_slot(1, &envelope).await.unwrap();

        let err = materialize_slot_head(&store).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        assert!(err.to_string().contains("slot 1"), "{err}");
    }

    /// A hole the fold cursor is still below is a slot destroyed outside the
    /// protocol: the prefix is never served as the head.
    #[tokio::test]
    async fn a_hole_below_the_fold_cursor_is_corruption() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();
        // Slot 2 was destroyed: 3 exists above it and nothing folded it.
        store
            .slots
            .put_slot(3, &schema_slot(3, 3, "archive", 2))
            .await
            .unwrap();

        let err = materialize_slot_head(&store).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        assert!(err.to_string().contains("slot 2"), "{err}");
    }

    /// The other cause of a hole: a peer folded past it and truncated it, so
    /// this reader is merely behind. A freshly opened reader sees the advanced
    /// cursor, and the slots above it still serve.
    #[tokio::test]
    async fn a_hole_a_peer_truncated_replays_against_the_fresher_cursor() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        let first = schema_slot(1, 1, "sales", 0);
        store.slots.put_slot(1, &first).await.unwrap();
        store
            .slots
            .put_slot(2, &schema_slot(2, 2, "staging", 1))
            .await
            .unwrap();

        // A peer folds slot 1 into the store, advances the cursor, and
        // truncates the slot it no longer needs.
        fold_more(&object_store, &store.options, 1).await;
        store.slots.truncate_through(1).await.unwrap();

        // Whether or not this handle's reader has caught up, the head is the
        // fold plus what remains of the tail.
        let head = materialize_slot_head(&store).await.unwrap();

        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(2));
        assert!(head.view.schema_by_name("sales").is_some());
        assert!(head.view.schema_by_name("staging").is_some());
        assert_eq!(head.next_sequence, 3);
    }

    /// A handle whose reader was open before a peer folded and truncated still
    /// materializes the head those slots reached, and the head's own probe
    /// handle reads the records they left in the store — whether the reader
    /// caught up on its own or the materialization adopted a fresh one.
    #[tokio::test]
    async fn the_head_reader_travels_past_a_truncation() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;

        let slots = [
            schema_slot(1, 1, "sales", 0),
            schema_slot(2, 2, "staging", 1),
            schema_slot(3, 3, "archive", 2),
            schema_slot(4, 4, "analytics", 3),
        ];
        for (offset, slot) in slots.iter().enumerate() {
            let sequence = offset as u64 + 1;
            store.slots.put_slot(sequence, slot).await.unwrap();
        }

        // A peer folds slots 1..=3 into the store, advancing the cursor, then
        // truncates them. This handle's reader still sees the bootstrap
        // manifest — cursor 0, none of those writes.
        fold_more(&object_store, &store.options, 3).await;
        store.slots.truncate_through(3).await.unwrap();

        let head = materialize_slot_head(&store).await.unwrap();
        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(4));
        assert!(head.view.schema_by_name("sales").is_some());

        // A probe reads through the head's own handle, which is the reader the
        // view came from rather than whichever one the caller holds.
        let sales_key = Key::current(EntityKey::Schema { schema_id: 1 }).encode();
        let handle = ReadHandle::Reader(&store.reader);
        assert!(
            head.handle(handle).get(&sales_key).await.unwrap().is_some(),
            "the head's handle sees the folded record"
        );

        release_reader(head.reader.as_ref()).await;
    }

    /// Time travel below the folded head reads history from the store; above
    /// it, the tail is replayed up to the target and no further.
    #[tokio::test]
    async fn time_travel_spans_the_folded_head_and_the_tail() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();
        store
            .slots
            .put_slot(2, &schema_slot(2, 2, "staging", 1))
            .await
            .unwrap();

        let bootstrapped = materialize_slot_view_at(&store, 0).await.unwrap();
        assert_eq!(bootstrapped.schemas().len(), 1);

        let first = materialize_slot_view_at(&store, 1).await.unwrap();
        assert_eq!(first.current_snapshot().id, SnapshotId::new(1));
        assert!(first.schema_by_name("sales").is_some());
        assert!(first.schema_by_name("staging").is_none());

        let err = materialize_slot_view_at(&store, 3).await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err}");
    }

    /// The wired read path: a handle attached to the same bucket serves a slot
    /// no folder has applied, and one opened before the slot landed serves it
    /// too.
    #[tokio::test]
    async fn a_catalog_handle_serves_the_tail_without_a_folder() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = Catalog::open(Arc::clone(&object_store), multi_writer_options())
            .await
            .unwrap();

        let slots = SlotLog::new(Arc::clone(&object_store), "");
        slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();

        let second = Catalog::open(Arc::clone(&object_store), multi_writer_options())
            .await
            .unwrap();
        let view = second.snapshot().await.unwrap();
        assert_eq!(view.current_snapshot().id, SnapshotId::new(1));
        assert!(view.schema_by_name("sales").is_some());

        assert!(
            first
                .snapshot()
                .await
                .unwrap()
                .schema_by_name("sales")
                .is_some()
        );
        let bootstrapped = second.snapshot_at(SnapshotId::new(0)).await.unwrap();
        assert_eq!(bootstrapped.schemas().len(), 1);
    }

    /// A committing transaction resolves to the snapshot it minted straight
    /// from the unfolded tail — no fold has run — and one that never raced a
    /// slot is definitively absent.
    #[tokio::test]
    async fn transaction_outcome_resolves_an_unfolded_tail_hit() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        store
            .slots
            .put_slot(1, &schema_slot(7, 1, "sales", 0))
            .await
            .unwrap();

        let landed = transaction_outcome(&store, [7; 16], 0).await.unwrap();
        assert_eq!(landed, Some(1));

        let absent = transaction_outcome(&store, [9; 16], 0).await.unwrap();
        assert_eq!(absent, None);
    }

    /// A committed-and-folded transaction resolves from its snapshot record
    /// above the floor. A floor at the transaction's own snapshot excludes it
    /// — the false-safe `None` a caller avoids by passing a floor at or below
    /// the head the attempt validated against.
    #[tokio::test]
    async fn transaction_outcome_resolves_a_folded_hit_above_the_floor() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        seed_folded_snapshot(&object_store, &store.options, 3, [5; 16]).await;

        assert_eq!(
            transaction_outcome(&store, [5; 16], 2).await.unwrap(),
            Some(3)
        );
        assert_eq!(transaction_outcome(&store, [5; 16], 3).await.unwrap(), None);
    }

    /// A hole below the searched-for transaction cannot rule it out, so the
    /// scan surfaces [`Error::Corruption`] rather than a false `None`.
    #[tokio::test]
    async fn transaction_outcome_surfaces_a_tail_hole_as_corruption() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();
        // Slot 2 was destroyed; slot 3 exists above the hole.
        store
            .slots
            .put_slot(3, &schema_slot(3, 3, "archive", 2))
            .await
            .unwrap();

        let err = transaction_outcome(&store, [3; 16], 0).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }

    /// Writes one snapshot record carrying `transaction_id` straight into the
    /// store, the state a completed fold leaves behind.
    async fn seed_folded_snapshot(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
        snapshot_id: u64,
        transaction_id: [u8; 16],
    ) {
        let (db, _) = StoreBuilder::new(&options.path, Arc::clone(object_store))
            .open_writer()
            .await
            .unwrap();
        let tx = db.begin(slatedb::IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            Key::Snapshot { snapshot_id }.encode(),
            value::encode_value(&proto::SnapshotValue {
                snapshot_id,
                transaction_id: Some(transaction_id.to_vec()),
                ..proto::SnapshotValue::default()
            }),
        )
        .unwrap();
        tx.put(
            Key::Sys(SysKey::Head).encode(),
            value::encode_value(&proto::HeadValue {
                snapshot_id,
                batch_seq: 0,
            }),
        )
        .unwrap();
        commit::commit_durably(&db, tx).await.unwrap();
        db.close().await.unwrap();
    }

    /// A view whose head snapshot is `head`, carrying nothing else: the
    /// admission rule reads the head and nothing more.
    fn view_at_head(head: u64) -> CatalogSnapshot {
        CatalogSnapshot::build(
            proto::SnapshotValue {
                snapshot_id: head,
                ..proto::SnapshotValue::default()
            },
            &[],
            &[],
            None,
        )
    }

    /// A commit that writes nothing, staged against `validated_head`.
    fn commit_validated_against(validated_head: u64) -> Commit {
        Commit {
            transaction_id: [9; 16],
            payload: SlotPayload {
                validated_head,
                changes_made: String::new(),
                writes: Vec::new(),
            },
        }
    }

    /// Folds `slots` more of the log into the store the way a peer's sprint
    /// does: opening the writer replays them, and the flush makes the fold —
    /// and the cursor it advances — durable.
    async fn fold_more(object_store: &Arc<dyn ObjectStore>, options: &CatalogOptions, slots: u64) {
        let (db, _) = StoreBuilder::new(&options.path, Arc::clone(object_store))
            .replay_limit(Some(slots))
            .open_writer()
            .await
            .unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();
    }

    /// Wraps an [`InMemory`] store and, on the first conditional create of a
    /// slot object, plants a competing commit at that slot first — so the real
    /// committer's create loses to a genuine winner it must read back and
    /// rebase onto. A deterministic two-writer race with a fixed loser.
    #[derive(Debug)]
    struct RaceOnceStore {
        inner: InMemory,
        competitor: Bytes,
        planted: AtomicBool,
    }

    impl RaceOnceStore {
        fn new(competitor: Bytes) -> Self {
            Self {
                inner: InMemory::new(),
                competitor,
                planted: AtomicBool::new(false),
            }
        }
    }

    impl std::fmt::Display for RaceOnceStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RaceOnceStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RaceOnceStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            let races_slot = location.as_ref().starts_with("commits/")
                && matches!(opts.mode, PutMode::Create)
                && !self.planted.swap(true, AtomicOrdering::SeqCst);
            if races_slot {
                self.inner
                    .put_opts(
                        location,
                        self.competitor.clone().into(),
                        PutOptions {
                            mode: PutMode::Create,
                            ..Default::default()
                        },
                    )
                    .await?;
            }
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
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[std::ops::Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
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

    /// The one column every fixture table needs.
    fn one_column() -> Vec<ColumnDef> {
        vec![ColumnDef {
            name: "id".to_string(),
            column_type: "BIGINT".to_string(),
            nulls_allowed: false,
            default_value: None,
            children: Vec::new(),
        }]
    }

    /// A forced two-writer race: the first slot create loses to a planted
    /// competitor, so the driver rebases and wins the next slot — a
    /// `Committed { attempts: 2, races_lost: 1 }` the handle surfaces on its
    /// contention counter, not merely a log line. Asserting the counter (the
    /// driver's return, narrated) rather than scraping logs.
    #[tokio::test]
    async fn a_forced_two_writer_race_rebases_and_counts_the_lost_race() {
        // Build a real competing commit — a table created in `main` — by
        // committing it on a scratch store, then lift the slot's bytes: an
        // envelope another writer could have raced, never hand-encoded.
        let scratch: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let scratch_catalog = Catalog::open(Arc::clone(&scratch), multi_writer_options())
            .await
            .unwrap();
        scratch_catalog
            .commit(|tx| {
                let main = tx
                    .state()
                    .schema_by_name("main")
                    .expect("bootstrap mints main")
                    .id;
                tx.create_table(main, "planted", &one_column()).map(|_| ())
            })
            .await
            .unwrap();
        let competitor = scratch
            .get(&Path::from("commits/00000000000000000001"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        // The real store races the planted competitor for its first slot.
        let race_store: Arc<dyn ObjectStore> = Arc::new(RaceOnceStore::new(competitor));
        let catalog = Catalog::open(Arc::clone(&race_store), multi_writer_options())
            .await
            .unwrap();

        let snapshot = catalog
            .commit(|tx| {
                let main = tx
                    .state()
                    .schema_by_name("main")
                    .expect("bootstrap mints main")
                    .id;
                tx.create_table(main, "mine", &one_column()).map(|_| ())
            })
            .await
            .unwrap();

        // The competitor won slot 1 (snapshot 1); this commit rebased onto it
        // and won slot 2 (snapshot 2).
        assert_eq!(snapshot, SnapshotId::new(2));

        // The lost race is a live counter, so contention-triggered forwarding
        // can act on it — attempts > 1 shown through the driver's return.
        let contention = catalog.contention();
        assert!(
            contention.races_lost >= 1,
            "a rebased commit records the race it lost: {contention:?}"
        );
        assert_eq!(contention.commits, 1);
        assert_eq!(contention.exhaustions, 0);

        // Both tables landed: the rebase applied the competitor, then this
        // commit.
        let view = catalog.snapshot().await.unwrap();
        let main = view.schema_by_name("main").unwrap().id;
        assert!(view.table_by_name(main, "planted").is_some());
        assert!(view.table_by_name(main, "mine").is_some());
    }
}
