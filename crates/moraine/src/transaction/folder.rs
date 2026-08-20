//! The folder-role session: the fenced writer that derived-state maintenance
//! and folding run under, so a slot-backed store keeps exactly one direct
//! writer.
//!
//! With commits in the log, nothing may write the store directly except the
//! one fenced writer, or replay and the store diverge. Opening that writer is
//! the whole license: concurrent sessions fence each other, the newest winning,
//! so a loser surfaces [`Error::Fenced`](crate::Error::Fenced) rather than
//! corrupting anything.
//!
//! Folding drains the slot log into that store so the store is an accurate
//! derived index of the log — and the store does it itself. The log is the
//! store's write-ahead log, so opening the writer replays every slot past its
//! fold cursor into the memtable, and the flush that follows is what makes the
//! fold durable. The cursor is the store's own replay point: nothing here
//! applies a slot or advances a cursor by hand.

use std::{sync::Arc, time::Duration};

use moraine_wal::{FoldReport, Jitter};
use slatedb::{Checkpoint, Db, DbReader};
use uuid::Uuid;

use crate::{
    catalog::SlotStore,
    error::{Error, Result},
    store::open::StoreBuilder,
    transaction::slot_commit::FOLD_STALL_THRESHOLD,
};

/// Opens the fenced writer over `store`, runs `body` against it, and closes it.
/// The writer is the single direct writer of the slot-backed store; a second
/// session opened concurrently fences this one, which surfaces the fencing as
/// an error from `body` rather than a corrupt store.
///
/// Opening folds: the session starts from a store that has replayed every slot
/// the log holds, so derived-state work reads the same state a reader does.
pub(crate) async fn with_folder<T, F>(store: &SlotStore, body: F) -> Result<T>
where
    F: AsyncFnOnce(&Db) -> Result<T>,
{
    let db = open_folder_writer(store, None).await?;

    let outcome = body(&db).await;

    // A close failure surfaces only when the body itself succeeded; a body
    // error is the primary cause and keeps precedence.
    match db.close().await {
        Ok(()) => outcome,
        Err(err) => outcome.and(Err(Error::from(err))),
    }
}

/// Opens the fenced writer `Db` over the store's retained builder shape,
/// folding at most `limit` slots as it opens.
async fn open_folder_writer(store: &SlotStore, limit: Option<u64>) -> Result<Db> {
    StoreBuilder::new(&store.options.path, Arc::clone(&store.object_store))
        .replay_limit(limit)
        .cache_dir(store.options.cache_dir.clone())
        .cache_size(store.options.cache_size)
        .cache_preload(store.options.cache_preload)
        .cache_puts(store.options.cache_puts)
        .open_writer()
        .await
        .map(|(db, _)| db)
}

/// The fold cursor a writer stands at: the last slot its own manifest records
/// as folded, which is where its next open resumes.
fn writer_cursor(db: &Db) -> u64 {
    db.status().current_manifest.replay_after_wal_id()
}

/// The fold cursor a reader sees: the last slot the store had durably folded
/// as of the manifest this reader follows.
pub(crate) fn reader_cursor(reader: &DbReader) -> u64 {
    reader.manifest().replay_after_wal_id()
}

/// Whether `reader` has taken the log through `sequence` — folded or replayed
/// for itself, which its view does not distinguish. A reader replays the slots
/// past its cursor on its own cadence, so its view can hold commits its cursor
/// has not reached; the sequence numbers it has applied are what say how far
/// it has actually come.
pub(crate) fn reader_reached(reader: &DbReader, sequence: u64) -> bool {
    reader.status().durable_seq >= moraine_wal::slot_sequence(sequence)
}

/// One bounded fold pass: opens the fenced writer, which replays up to `limit`
/// unfolded slots into the memtable, flushes them to a durable L0 SST, and
/// closes. The attach's reader is undisturbed.
///
/// The flush is the fold's only durability barrier — no journaling stands
/// behind it, since the log is the journal. A sprint killed before it costs no
/// correctness: the memtable folds are lost, the log still holds those slots,
/// and the successor re-folds them. The cursor never advances past what is
/// durable, so no slot is ever double-applied.
pub(crate) async fn fold_sprint(store: &SlotStore, limit: u64) -> Result<FoldReport> {
    let db = open_folder_writer(store, Some(limit)).await?;
    let folded_from = writer_cursor(&db);

    let flushed = db.flush().await.map_err(Error::from);
    let folded_through = writer_cursor(&db);

    let closed = db.close().await.map_err(Error::from);
    flushed.and(closed)?;

    let tail_remaining = store
        .slots
        .tail_length(folded_through.saturating_add(1))
        .await
        .map_err(Error::from)?;
    let report = FoldReport {
        slots_folded: folded_through.saturating_sub(folded_from),
        folded_through,
        tail_remaining,
    };
    narrate_fold(&report);

    Ok(report)
}

/// Narrates one fold pass's [`FoldReport`]: the counts a host reads back from
/// [`fold_sprint`], at `debug`, with a `warn` when the unfolded tail is still
/// past the fold-stall threshold after the pass. `moraine_maintenance_status`
/// surfaces the same counts from SQL — `tail_remaining` alongside
/// `slots_folded` and truncation's `slots_removed`.
fn narrate_fold(report: &FoldReport) {
    tracing::debug!(
        slots_folded = report.slots_folded,
        folded_through = report.folded_through,
        tail_remaining = report.tail_remaining,
        "fold sprint applied slots"
    );
    if report.tail_remaining > FOLD_STALL_THRESHOLD {
        tracing::warn!(
            tail_remaining = report.tail_remaining,
            threshold = FOLD_STALL_THRESHOLD,
            "unfolded slot tail still exceeds the fold-stall threshold after a sprint"
        );
    }
}

/// Slots committed but not yet folded — the clock-free staleness signal. One
/// `tail_length` from the fold cursor: existence probes, no slot bodies
/// fetched. The cursor is read through a reader opened at the current manifest,
/// so a sprint this handle just ran is reflected rather than lagged behind the
/// attach reader's poll interval.
pub(crate) async fn unfolded_tail(store: &SlotStore) -> Result<u64> {
    let (reader, _) = StoreBuilder::new(&store.options.path, Arc::clone(&store.object_store))
        .cache_dir(store.options.cache_dir.clone())
        .cache_size(store.options.cache_size)
        .cache_preload(store.options.cache_preload)
        .cache_puts(store.options.cache_puts)
        .open_reader()
        .await?;
    let unfolded = store
        .slots
        .tail_length(reader_cursor(&reader).saturating_add(1))
        .await
        .map_err(Error::from);

    if let Err(err) = reader.close().await {
        tracing::warn!(error = %err, "could not close the reader opened for the unfolded-tail probe");
    }
    unfolded
}

/// Slots kept below the horizon regardless of the two bounds, so a reader whose
/// checkpoint advances between the manifest read and the delete still finds the
/// run it is about to replay. Mirrors the min-age SlateDB's own WAL GC keeps
/// alongside checkpoint references; slot envelopes are small, so over-retention
/// costs storage while under-retention costs a committed slot.
const TRUNCATION_RETENTION_MARGIN: u64 = 64;

/// Deletes durably folded slots the fleet no longer needs, oldest first, and
/// returns how many objects were removed. The horizon is the lower of two
/// bounds, held back by [`TRUNCATION_RETENTION_MARGIN`]: what is durably folded
/// (a reader cannot see a memtable, so an unfolded slot — the only copy of its
/// commit — is never past it), and the fold cursor the oldest live reader still
/// sits at (or its next materialization finds the slots it must replay
/// deleted).
pub(crate) async fn truncate_folded_slots(store: &SlotStore) -> Result<u64> {
    let reader = reopen_reader(store).await?;

    // One fresh reader answers both bounds: its own fold cursor is the true
    // durable frontier (not the attach reader's lagged poll), and its manifest
    // lists every live reader checkpoint.
    let horizon = async {
        let durable = reader_cursor(&reader);
        let reader_floor = oldest_reader_fold_cursor(store, &reader, durable).await?;
        Ok::<u64, Error>(
            durable
                .min(reader_floor)
                .saturating_sub(TRUNCATION_RETENTION_MARGIN),
        )
    }
    .await;

    if let Err(err) = reader.close().await {
        tracing::warn!(error = %err, "could not close the reader opened for the truncation horizon");
    }

    let removed = store
        .slots
        .truncate_through(horizon?)
        .await
        .map_err(Error::from)?;
    tracing::debug!(slots_removed = removed, "truncated durably folded slots");
    Ok(removed)
}

/// The fold cursor as of the oldest live reader checkpoint in
/// `manifest_reader`'s manifest, or `durable` when no live checkpoint
/// constrains truncation. Every live reader pins its view as a manifest
/// checkpoint — the mechanism SlateDB's own GC leans on — so the oldest
/// checkpoint is the reader furthest behind, and the fold cursor read through
/// it is the deepest a truncation may reach without deleting a slot that reader
/// still replays. An expired checkpoint (a crashed reader's lapsed pin) does
/// not hold truncation back.
async fn oldest_reader_fold_cursor(
    store: &SlotStore,
    manifest_reader: &DbReader,
    durable: u64,
) -> Result<u64> {
    let Some(checkpoint_id) = oldest_live_checkpoint(manifest_reader.manifest().checkpoints())
    else {
        // No live reader pins anything, so only the durable frontier bounds us.
        return Ok(durable);
    };

    match fold_cursor_as_of(store, checkpoint_id).await {
        Ok(cursor) => Ok(cursor),
        // A reader churning its checkpoint can delete the one just listed before
        // it is opened, and a checkpoint can expire mid-open. Either way this
        // round retains rather than truncate past a reader it could not inspect;
        // the next pass re-reads the manifest and makes progress.
        Err(err) => {
            tracing::warn!(
                error = %err,
                "could not read the oldest reader checkpoint; retaining all slots this pass"
            );
            Ok(0)
        }
    }
}

/// The id of the live checkpoint referencing the oldest manifest, if any.
fn oldest_live_checkpoint(checkpoints: &[Checkpoint]) -> Option<Uuid> {
    let now = unix_seconds_now();
    checkpoints
        .iter()
        .filter(|checkpoint| checkpoint_is_live(checkpoint, now))
        .min_by_key(|checkpoint| checkpoint.manifest_id)
        .map(|checkpoint| checkpoint.id)
}

/// Whether `checkpoint`'s pin still holds: an absent expiry never lapses, and a
/// future one has not yet.
fn checkpoint_is_live(checkpoint: &Checkpoint, now_seconds: i64) -> bool {
    match checkpoint.expire_time {
        Some(expire) => expire.timestamp() > now_seconds,
        None => true,
    }
}

/// Seconds since the Unix epoch; 0 if the clock is unreadable, which retains
/// rather than truncates.
fn unix_seconds_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
        .unwrap_or(0)
}

/// The fold cursor read through a reader pinned to `checkpoint_id`: the store
/// view the checkpoint's owner currently holds.
async fn fold_cursor_as_of(store: &SlotStore, checkpoint_id: Uuid) -> Result<u64> {
    let reader = StoreBuilder::new(&store.options.path, Arc::clone(&store.object_store))
        .cache_dir(store.options.cache_dir.clone())
        .cache_size(store.options.cache_size)
        .cache_preload(store.options.cache_preload)
        .cache_puts(store.options.cache_puts)
        .open_reader_at(checkpoint_id)
        .await?;
    let cursor = reader_cursor(&reader);

    if let Err(err) = reader.close().await {
        tracing::warn!(error = %err, "could not close the checkpoint-pinned reader");
    }
    Ok(cursor)
}

/// A reader at the manifest as it stands now, for discovering live checkpoints.
async fn reopen_reader(store: &SlotStore) -> Result<DbReader> {
    StoreBuilder::new(&store.options.path, Arc::clone(&store.object_store))
        .cache_dir(store.options.cache_dir.clone())
        .cache_size(store.options.cache_size)
        .cache_preload(store.options.cache_preload)
        .cache_puts(store.options.cache_puts)
        .open_reader()
        .await
        .map(|(reader, _)| reader)
}

/// The self-appointment rule: if the unfolded tail exceeds `threshold`, wait
/// `delay` plus jitter, re-read the cursor, and sprint only if no other folder
/// advanced it. The wait is what keeps a fleet from stampeding one stalled
/// tail — every process draws its own jitter, so the first to wake folds and
/// the rest stand down on the cursor it moved.
pub(crate) async fn fold_if_stalled(
    store: &SlotStore,
    threshold: u64,
    delay: Duration,
    limit: u64,
) -> Result<Option<FoldReport>> {
    let cursor = reader_cursor(&store.reader);
    let unfolded = store
        .slots
        .tail_length(cursor.saturating_add(1))
        .await
        .map_err(Error::from)?;
    if unfolded <= threshold {
        return Ok(None);
    }

    let jitter = Jitter::from_entropy();
    tokio::time::sleep(delay.saturating_add(jitter.draw(delay))).await;

    // The attach's reader follows the manifest on its own cadence, so a fresh
    // one is what tells whether a peer's fold has landed since.
    let peer = reopen_reader(store).await?;
    let advanced = reader_cursor(&peer) > cursor;
    if let Err(err) = peer.close().await {
        tracing::warn!(error = %err, "could not close the reader opened for the fold appointment");
    }
    if advanced {
        return Ok(None);
    }

    fold_sprint(store, limit).await.map(Some)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::{ObjectStore, memory::InMemory};

    use super::StoreBuilder;
    use crate::catalog::{Catalog, CatalogOptions};

    #[allow(clippy::unwrap_used)]
    async fn open_catalog(store: &Arc<InMemory>) -> Catalog {
        Catalog::open(
            store.clone() as Arc<dyn ObjectStore>,
            CatalogOptions::default(),
        )
        .await
        .unwrap()
    }

    #[allow(clippy::unwrap_used)]
    async fn commit_schemas(catalog: &Catalog, names: &[&str]) {
        for name in names {
            catalog
                .commit(|tx| tx.create_schema(name).map(|_| ()))
                .await
                .unwrap();
        }
    }

    /// The head snapshot id plus the sorted schema names, read through a fresh
    /// attach so the folded store — not a lagging reader — is what it
    /// reconstructs.
    #[allow(clippy::unwrap_used)]
    async fn logical_state(store: &Arc<InMemory>) -> (u64, Vec<String>) {
        let catalog = open_catalog(store).await;
        let snapshot = catalog.snapshot().await.unwrap();
        let mut schemas: Vec<String> = snapshot
            .schemas()
            .into_iter()
            .map(|schema| schema.name)
            .collect();
        schemas.sort();
        (snapshot.current_snapshot().id.get(), schemas)
    }

    /// A sprint killed before its flush loses the fold it held in the
    /// memtable: nothing journals it, since the log is the journal. The
    /// reopened store therefore still counts every slot unfolded, a full
    /// sprint re-folds them, and the result matches a fold that was never
    /// interrupted — the cursor never advances past what is durable, so no
    /// slot is applied twice.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn a_killed_sprint_loses_its_memtable_fold_and_reconverges() {
        let store = Arc::new(InMemory::new());
        let object_store = store.clone() as Arc<dyn ObjectStore>;
        let names = ["a", "b", "c", "d"];

        let catalog = open_catalog(&store).await;
        commit_schemas(&catalog, &names).await;
        assert_eq!(catalog.unfolded_tail().await.unwrap(), 4);

        // A killed sprint: open the fold writer, which replays the tail into
        // the memtable, then drop it without the flush or the close that make
        // the fold durable — a process kill.
        {
            let db = StoreBuilder::new("", object_store.clone())
                .open_writer()
                .await
                .unwrap();
            drop(db);
        }

        let reopened = open_catalog(&store).await;
        assert_eq!(
            reopened.unfolded_tail().await.unwrap(),
            4,
            "a killed sprint's memtable fold is lost, not recovered"
        );

        // A full sprint re-folds every slot from the log and drains the tail.
        let report = reopened.fold_sprint(u64::MAX).await.unwrap();
        assert_eq!(report.slots_folded, 4, "every slot re-folded from the log");
        assert_eq!(reopened.unfolded_tail().await.unwrap(), 0);

        // Convergence: the killed-then-refolded store matches a
        // never-interrupted fold of the identical workload.
        let reference_store = Arc::new(InMemory::new());
        let reference = open_catalog(&reference_store).await;
        commit_schemas(&reference, &names).await;
        reference.fold_sprint(u64::MAX).await.unwrap();

        assert_eq!(
            logical_state(&store).await,
            logical_state(&reference_store).await,
            "a killed-then-refolded store converges to the never-interrupted state"
        );
    }

    /// A bounded sprint folds only as far as its limit and the next one
    /// resumes from the cursor it left: the bound is on the replay, so the
    /// store's own cursor is what carries the sprint boundary.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn a_bounded_sprint_folds_to_its_limit_and_the_next_resumes() {
        let store = Arc::new(InMemory::new());
        let catalog = open_catalog(&store).await;
        commit_schemas(&catalog, &["a", "b", "c"]).await;

        let first = catalog.fold_sprint(2).await.unwrap();
        assert_eq!(first.slots_folded, 2);
        assert_eq!(first.folded_through, 2);
        assert_eq!(first.tail_remaining, 1);

        let second = catalog.fold_sprint(2).await.unwrap();
        assert_eq!(second.slots_folded, 1, "the sprint resumes at the cursor");
        assert_eq!(second.folded_through, 3);
        assert_eq!(second.tail_remaining, 0);
    }
}
