//! Opening, bootstrap, and snapshot materialization. The commit cycle
//! itself builds on these.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{StreamExt, TryStreamExt, stream};
use slatedb::{Db, DbReader, DbTransaction, IsolationLevel, config::WriteOptions};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    catalog::{
        CatalogSnapshot, SnapshotId, Timestamp,
        projection::{
            ProjectionCache, cache_epoch, cached_head_view, fold_committed_batch, format_floor,
            held_head_view, install_head_view, install_head_view_at, invalidate_head_view,
            migration_clear_at, note_migration_clear, raise_format_floor,
        },
    },
    error::{Error, Result},
    store::{
        StagedBytes,
        cache::CacheCounters,
        handle::ReadHandle,
        key::{EntityKey, Key, SysKey},
        open::StoreBuilder,
        proto,
        read::{self, EntityRecord},
        value,
    },
    transaction::{
        index_maintenance, inline,
        operations::{ChangeSet, Operation},
        verbs::{Transaction, TransactionParts},
    },
};

/// Structural layout version a fresh store bootstraps at.
pub(crate) const FORMAT_VERSION: u64 = 1;
/// Format stamped the first time an equality index exists: adds the
/// `index` subspace and kind.
pub(crate) const FORMAT_WITH_INDEX: u64 = 2;
/// Format stamped the first time a staged (multi-commit) index build
/// exists: a `building` definition serves no lookups.
pub(crate) const FORMAT_WITH_STAGED_INDEX: u64 = 3;
/// Format stamped the first time an index defers SQL additions.
pub(crate) const FORMAT_WITH_DEFERRED_INDEX: u64 = 4;
/// Format stamped the first time durable maintenance status is recorded.
pub(crate) const FORMAT_WITH_MAINTENANCE_STATUS: u64 = 5;
/// Format stamped the first time an inline chunk row-range locator is
/// written.
pub(crate) const FORMAT_WITH_INLINE_CHUNK_DIRECTORY: u64 = 6;
/// A deregistered inline schema may be a reference to another version of
/// the same table rather than the Arrow bytes themselves, which a reader
/// that predates the field would decode as a schema with no columns.
pub(crate) const FORMAT_WITH_INLINE_SCHEMA_REFERENCE: u64 = 7;
/// An inline chunk locator carries the chunk's identity, not its range end
/// alone. A reader that predates it looks for the superseded key and finds
/// no directory at all, so it must be locked out rather than left to serve
/// reads from a directory it cannot see.
pub(crate) const FORMAT_WITH_INLINE_CHUNK_IDENTITY: u64 = 8;
/// The highest format this binary understands. It opens any store in
/// `MIN_FORMAT_VERSION..=MAX_FORMAT_VERSION` and refuses a newer one.
pub(crate) const MAX_FORMAT_VERSION: u64 = FORMAT_WITH_INLINE_CHUNK_IDENTITY;
/// The lowest format this binary reads directly; a store below it must be
/// migrated up first. Rises only when a format rewrites the keyspace.
pub(crate) const MIN_FORMAT_VERSION: u64 = FORMAT_VERSION;
/// Bounded internal retries before a benign race is reported as a
/// conflict.
pub(crate) const MAX_COMMIT_ATTEMPTS: usize = 10;

/// Delay before the second commit attempt, in microseconds; each further
/// retry doubles it, up to [`RETRY_BACKOFF_MAX_MICROS`].
const RETRY_BACKOFF_BASE_MICROS: u64 = 2_000;
/// Ceiling on one retry's delay.
const RETRY_BACKOFF_MAX_MICROS: u64 = 50_000;

/// Intervening snapshot records read concurrently after a lost head race.
const INTERVENING_READ_CONCURRENCY: usize = 64;

/// Changelog and current-record reads kept in flight by an incremental head
/// refresh; sized for a remote object store.
const REFRESH_READ_CONCURRENCY: usize = 256;

/// How long to wait before re-running `attempt` (0-based; the first attempt
/// never waits): exponential to the cap, plus jitter of up to the base
/// delay.
pub(crate) fn retry_backoff(attempt: usize) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }
    let doublings = u32::try_from(attempt - 1).unwrap_or(u32::MAX).min(31);
    let step = RETRY_BACKOFF_BASE_MICROS
        .saturating_mul(1_u64 << doublings)
        .min(RETRY_BACKOFF_MAX_MICROS);
    let jitter = now_micros().unsigned_abs() % RETRY_BACKOFF_BASE_MICROS;
    Duration::from_micros(step.saturating_add(jitter))
}

/// Current time in microseconds since the Unix epoch, the width the
/// snapshot record stores.
pub(crate) fn now_micros() -> i64 {
    Timestamp::now().as_micros()
}

pub(crate) fn durable() -> WriteOptions {
    WriteOptions {
        await_durable: true,
        ..Default::default()
    }
}

/// How long a durable commit may wait before the wait itself is reported,
/// and how often it is reported thereafter.
const STALL_INTERVAL: Duration = Duration::from_secs(10);

/// How a durable commit's bytes reach object storage.
#[derive(Clone, Default)]
pub(crate) enum CommitDurability {
    /// Hand the batch to the store and wait for the flush timer to carry
    /// it out, so the commit costs up to one flush interval.
    #[default]
    OnFlushInterval,
    /// Force the write-ahead log out as part of the commit, so the commit
    /// costs one object-store PUT whatever the flush interval is.
    Immediate(Db),
}

/// The route commits through `db` take, given whether the catalog was
/// opened to flush on commit.
pub(crate) fn writer_durability(db: &Db, flush_on_commit: bool) -> CommitDurability {
    if flush_on_commit {
        CommitDurability::Immediate(db.clone())
    } else {
        CommitDurability::OnFlushInterval
    }
}

/// Commits `tx` and waits for the batch to reach object storage, by the
/// route `durability` names. The wait is unbounded: the store retries the
/// write indefinitely, and a deadline could not retract a batch that still
/// lands.
pub(crate) async fn commit_durable(
    tx: DbTransaction,
    operation: &'static str,
    staged: StagedBytes,
    durability: &CommitDurability,
) -> std::result::Result<Option<slatedb::WriteHandle>, slatedb::Error> {
    match durability {
        CommitDurability::OnFlushInterval => {
            reporting_stalls(operation, staged, tx.commit_with_options(&durable())).await
        }
        // The commit returns as soon as the batch is visible; the flush
        // that follows is what puts it in object storage, so both are
        // waited on before this reports the write durable.
        CommitDurability::Immediate(db) => {
            let handle = tx.commit_with_options(&non_durable()).await?;
            reporting_stalls(operation, staged, db.flush()).await?;
            Ok(handle)
        }
    }
}

/// Awaits `write`, naming `operation` and the batch's `staged` size in the
/// log every [`STALL_INTERVAL`] the wait runs long.
async fn reporting_stalls<T>(
    operation: &'static str,
    staged: StagedBytes,
    write: impl Future<Output = std::result::Result<T, slatedb::Error>>,
) -> std::result::Result<T, slatedb::Error> {
    let mut write = Box::pin(write);
    let mut waited = Duration::ZERO;
    loop {
        if let Ok(outcome) = tokio::time::timeout(STALL_INTERVAL, &mut write).await {
            return outcome;
        }
        waited = waited.saturating_add(STALL_INTERVAL);
        warn!(
            operation,
            waited_seconds = waited.as_secs(),
            staged_bytes = staged.0,
            "still waiting for object storage to accept a durable write; the batch goes as one \
             request and is retried indefinitely, so it will not fail on its own"
        );
    }
}

/// A commit that returns without waiting for the write to reach object
/// storage. The write is still atomic and visible to this handle at once.
/// Only for writes whose loss is self-correcting.
pub(crate) fn non_durable() -> WriteOptions {
    WriteOptions {
        await_durable: false,
        ..Default::default()
    }
}

/// Refuses a store this binary must not touch: mid-migration, or a
/// format newer/older than it understands. `None` format means the store
/// is empty and needs bootstrap.
async fn validate_format(tx: ReadHandle<'_>) -> Result<Option<proto::FormatValue>> {
    let (migration, format) = futures::try_join!(read::read_migration(tx), read::read_format(tx))?;
    if migration.is_some() {
        return Err(Error::Migration(
            "store is mid-migration; refusing to open — Catalog::migrate resumes it from the \
             durable cursor, and takes a store path rather than an open catalog, so it runs \
             against a store no attach will touch"
                .to_string(),
        ));
    }
    match format {
        Some(format) if format.format_version > MAX_FORMAT_VERSION => {
            Err(Error::Migration(format!(
                "store format {} is newer than this binary understands (max {MAX_FORMAT_VERSION}); \
             upgrade the binary",
                format.format_version
            )))
        }
        Some(format) if format.format_version < MIN_FORMAT_VERSION => {
            Err(Error::Migration(format!(
                "store format {} predates this binary's minimum ({MIN_FORMAT_VERSION}); \
             migrate it up with Catalog::migrate, which takes a store path rather than an \
             open catalog and so runs on this same binary against the store it refuses to \
             open",
                format.format_version
            )))
        }
        Some(format) => Ok(Some(format)),
        None => Ok(None),
    }
}

/// Stages the initial state of an empty store into `tx`: format stamp,
/// snapshot 0, the `main` schema record, the global option record
/// (`encrypted` as `"true"`/`"false"`, plus `data_path` when given), and
/// the head pointer.
fn stage_bootstrap(
    tx: &DbTransaction,
    encrypted: bool,
    data_path: Option<&str>,
) -> Result<StagedBytes> {
    let mut writes: Vec<StagedWrite> = Vec::with_capacity(5);
    let mut stage = |key: Key, bytes: Vec<u8>| writes.push((key.encode(), Some(bytes)));
    stage(
        Key::Sys(SysKey::Format),
        value::encode_value(&proto::FormatValue {
            format_version: FORMAT_VERSION,
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    );
    // Bootstrap's snapshot records minting `main`.
    let mut bootstrap_changes = ChangeSet::default();
    bootstrap_changes.created_schemas.insert("main".to_string());
    stage(
        Key::Snapshot { snapshot_id: 0 },
        value::encode_value(&proto::SnapshotValue {
            snapshot_id: 0,
            snapshot_time_micros: now_micros(),
            schema_version: 0,
            next_catalog_id: 1,
            next_file_id: 0,
            changes_made: bootstrap_changes.to_changes_made(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
            deleted_data_file_ids: Vec::new(),
        }),
    );
    stage(
        Key::current(EntityKey::Schema { schema_id: 0 }),
        value::encode_value(&proto::SchemaValue {
            schema_id: 0,
            schema_uuid: Uuid::new_v4().to_string(),
            begin_snapshot: 0,
            end_snapshot: None,
            schema_name: "main".to_string(),
            path: "main/".to_string(),
            path_is_relative: true,
        }),
    );
    let mut options = std::collections::HashMap::from([(
        "encrypted".to_string(),
        if encrypted { "true" } else { "false" }.to_string(),
    )]);
    if let Some(path) = data_path {
        options.insert("data_path".to_string(), path.to_string());
    }
    stage(
        Key::current(EntityKey::Option {
            scope_kind: 0,
            scope_id: 0,
        }),
        value::encode_value(&proto::OptionScopeValue { options }),
    );
    stage(
        Key::Sys(SysKey::Head),
        value::encode_value(&proto::HeadValue {
            snapshot_id: 0,
            batch_seq: 0,
        }),
    );

    stage_writes(tx, &writes)
}

/// How many attempts an open that is fenced while creating the catalog
/// gets before the fence reaches the caller. Every attempt takes the
/// writer epoch in turn, so the count stays small.
const GENESIS_ATTEMPTS: u32 = 3;

/// Why an open attempt returned no catalog.
enum OpenFailure {
    /// Fenced while creating the catalog; nothing was written.
    FencedAtGenesis(Error),
    /// Anything else: opening again would reach the same answer.
    Fatal(Error),
}

/// Opens the store, bootstrapping an empty one in one atomic batch under
/// conflict detection; a lost bootstrap race re-validates instead of
/// double-initializing. Every exit that does not commit rolls back. Only a
/// genesis displaced by another initializer re-attempts, up to
/// [`GENESIS_ATTEMPTS`]; a fence anywhere else is the caller's to handle.
/// Returns the format version the store stands at, already validated.
pub(crate) async fn open_initialized(
    store: StoreBuilder<'_>,
    encrypted: bool,
    data_path: Option<&str>,
    flush_on_commit: bool,
) -> Result<(Db, Arc<CacheCounters>, u64)> {
    let mut attempt = 1;
    loop {
        match open_attempt(&store, encrypted, data_path, flush_on_commit).await {
            Ok(opened) => return Ok(opened),
            Err(OpenFailure::Fatal(err)) => return Err(err),
            Err(OpenFailure::FencedAtGenesis(err)) => {
                if attempt == GENESIS_ATTEMPTS {
                    return Err(err);
                }
                warn!(
                    attempt,
                    "another writer took the store over while this open was creating the \
                     catalog; nothing was written, so this open re-attempts"
                );
                attempt += 1;
            }
        }
    }
}

/// One attempt at [`open_initialized`], separating the fence that a second
/// attempt can get past from the failures it cannot.
async fn open_attempt(
    store: &StoreBuilder<'_>,
    encrypted: bool,
    data_path: Option<&str>,
    flush_on_commit: bool,
) -> std::result::Result<(Db, Arc<CacheCounters>, u64), OpenFailure> {
    let started = Instant::now();
    let (db, counters) = store.open_writer().await.map_err(OpenFailure::Fatal)?;
    info!(
        writer_open_ms = crate::telemetry::milliseconds(started.elapsed()),
        "opened the store read-write"
    );
    let tx = begin_snapshot(&db).await?;

    match validate_format(ReadHandle::Tx(&tx)).await {
        Ok(Some(format)) => {
            tx.rollback();
            return Ok((db, counters, format.format_version));
        }
        Ok(None) => {}
        Err(err) => {
            tx.rollback();
            return Err(OpenFailure::Fatal(err));
        }
    }

    let staged = match stage_bootstrap(&tx, encrypted, data_path) {
        Ok(staged) => staged,
        Err(err) => {
            tx.rollback();
            return Err(OpenFailure::Fatal(err));
        }
    };

    let durability = writer_durability(&db, flush_on_commit);
    match commit_durable(tx, "bootstrap", staged, &durability).await {
        Ok(_) => {
            info!(encrypted, data_path, "bootstrapped a fresh catalog store");
            Ok((db, counters, FORMAT_VERSION))
        }
        Err(err) if err.kind() == slatedb::ErrorKind::Transaction => {
            // Lost the bootstrap race: someone initialized concurrently.
            let tx = begin_snapshot(&db).await?;
            let validated = validate_format(ReadHandle::Tx(&tx)).await;
            tx.rollback();
            match validated {
                Ok(Some(format)) => Ok((db, counters, format.format_version)),
                Ok(None) => Err(OpenFailure::Fatal(Error::Corruption(
                    "bootstrap race left the store uninitialized".to_string(),
                ))),
                Err(err) => Err(OpenFailure::Fatal(err)),
            }
        }
        Err(err) if err.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced) => {
            Err(OpenFailure::FencedAtGenesis(err.into()))
        }
        Err(err) => Err(OpenFailure::Fatal(err.into())),
    }
}

/// Begins a snapshot-isolated transaction for an open attempt. A fence
/// caught here is fatal: nothing has been staged yet.
async fn begin_snapshot(db: &Db) -> std::result::Result<DbTransaction, OpenFailure> {
    db.begin(IsolationLevel::Snapshot)
        .await
        .map_err(|err| OpenFailure::Fatal(Error::from(err)))
}

/// Opens the store read-only as a [`DbReader`], validating the format it
/// finds and returning it. Never fences a live writer and never
/// bootstraps: an uninitialized store is refused.
pub(crate) async fn open_reader_initialized(
    store: StoreBuilder<'_>,
) -> Result<(DbReader, Arc<CacheCounters>, u64)> {
    let started = Instant::now();
    let (reader, counters) = store.open_reader().await?;
    let opened = started.elapsed();

    let started = Instant::now();
    let format = validate_format(ReadHandle::Reader(&reader)).await?;
    info!(
        reader_open_ms = crate::telemetry::milliseconds(opened),
        validate_ms = crate::telemetry::milliseconds(started.elapsed()),
        "opened the store read-only"
    );

    match format {
        Some(format) => Ok((reader, counters, format.format_version)),
        None => Err(Error::Corruption(
            "store is not an initialized moraine catalog; a read-only attach \
             needs a writer to have created it first"
                .to_string(),
        )),
    }
}

/// As [`refuse_mid_migration`], skipping the read when the marker was
/// already observed absent at `head`. A migration starts by stamping the
/// head, so a batch cannot begin one without moving `head` off the stamp.
pub(crate) async fn refuse_mid_migration_at(
    tx: ReadHandle<'_>,
    projections: &std::sync::RwLock<ProjectionCache>,
    head: &proto::HeadValue,
) -> Result<()> {
    if migration_clear_at(projections, head) {
        return Ok(());
    }
    refuse_mid_migration(tx).await?;
    note_migration_clear(projections, *head);

    Ok(())
}

/// Refuses a store whose keyspace is mid-migration: any scan of it may be
/// partial.
pub(crate) async fn refuse_mid_migration(tx: ReadHandle<'_>) -> Result<()> {
    match read::read_migration(tx).await? {
        Some(marker) => Err(Error::Migration(format!(
            "store is migrating from format {} to {}; reads are unavailable until it completes",
            marker.from_format, marker.to_format
        ))),
        None => Ok(()),
    }
}

/// The head record; an initialized store always has one.
pub(crate) async fn read_head_value(tx: ReadHandle<'_>) -> Result<proto::HeadValue> {
    read::read_head(tx)
        .await?
        .ok_or_else(|| Error::Corruption("store has no head pointer".to_string()))
}

/// The latest committed snapshot id.
pub(crate) async fn read_head_id(tx: ReadHandle<'_>) -> Result<u64> {
    Ok(read_head_value(tx).await?.snapshot_id)
}

/// The head write every batch carries: the snapshot id it leaves at head
/// (unchanged for a maintenance batch) and a batch count one above the one
/// standing. `sys/head` is the single conflict anchor.
pub(crate) fn head_stamp(snapshot_id: u64, standing_batch_seq: u64) -> StagedWrite {
    (
        Key::Sys(SysKey::Head).encode(),
        Some(value::encode_value(&proto::HeadValue {
            snapshot_id,
            batch_seq: standing_batch_seq.saturating_add(1),
        })),
    )
}

/// Resolves a requested read point to the snapshot it reads at and that
/// snapshot's record. `at: None` resolves to head.
pub(crate) async fn resolve_read_snapshot(
    tx: ReadHandle<'_>,
    at: Option<u64>,
) -> Result<(u64, proto::SnapshotValue)> {
    let head = read_head_id(tx).await?;
    resolve_below(tx, at, head).await
}

/// As [`resolve_read_snapshot`], for a caller that has already read head.
async fn resolve_below(
    tx: ReadHandle<'_>,
    at: Option<u64>,
    head: u64,
) -> Result<(u64, proto::SnapshotValue)> {
    let target = match at {
        Some(requested) if requested > head => {
            return Err(Error::NotFound(format!(
                "snapshot {requested} (head is {head})"
            )));
        }
        Some(requested) => requested,
        None => head,
    };
    // Ids are sequential to head, so a missing record at or below it is an
    // expired snapshot.
    let snapshot = read::read_snapshot(tx, target).await?.ok_or_else(|| {
        Error::SnapshotExpired(format!(
            "snapshot {target} is below the retention horizon (head is {head}); \
             re-resolve from head"
        ))
    })?;

    Ok((target, snapshot))
}

/// Materializes a catalog view through an open transaction. `at: None`
/// reads the head (`current` only); `at: Some(s)` also scans `history` to
/// reconstruct the entities live at `s`.
pub(crate) async fn materialize(tx: ReadHandle<'_>, at: Option<u64>) -> Result<CatalogSnapshot> {
    match at {
        None => Ok(materialize_capturing(tx).await?.0),
        Some(_) => {
            read::consistent(tx, || async move {
                let ((), head) = futures::try_join!(refuse_mid_migration(tx), read_head_value(tx))?;
                let ((target, snapshot), current, history) = futures::try_join!(
                    resolve_below(tx, at, head.snapshot_id),
                    read::scan_current_entities(tx),
                    read::scan_history_entities(tx),
                )?;

                Ok(CatalogSnapshot::build(
                    snapshot,
                    &current,
                    &history,
                    Some(target),
                ))
            })
            .await
        }
    }
}

/// As [`materialize`] at head, also handing back the scanned `current`
/// records from the same consistent cut.
pub(crate) async fn materialize_capturing(
    tx: ReadHandle<'_>,
) -> Result<(CatalogSnapshot, Arc<Vec<EntityRecord>>)> {
    read::consistent(tx, || async move {
        let ((), head) = futures::try_join!(refuse_mid_migration(tx), read_head_value(tx))?;
        let (_, snapshot) = resolve_below(tx, None, head.snapshot_id).await?;

        let started = Instant::now();
        let current = read::scan_current_entities(tx).await?;
        let scanned = started.elapsed();
        info!(
            records = current.len(),
            scan_ms = crate::telemetry::milliseconds(scanned),
            "scanned `current`"
        );

        let mut view = CatalogSnapshot::build(snapshot, &current, &[], None);
        // A head view stands at the store state the head record names.
        view.batch_seq = head.batch_seq;

        Ok((view, Arc::new(current)))
    })
    .await
}

/// Builds a head view from the shared `current` records instead of
/// scanning, or reports `None` when the store no longer stands at
/// `expected`.
pub(crate) async fn materialize_from(
    tx: ReadHandle<'_>,
    expected: &proto::HeadValue,
    current: &[EntityRecord],
) -> Result<Option<CatalogSnapshot>> {
    read::consistent(tx, || async move {
        let ((), head) = futures::try_join!(refuse_mid_migration(tx), read_head_value(tx))?;
        if head.snapshot_id != expected.snapshot_id || head.batch_seq != expected.batch_seq {
            return Ok(None);
        }
        let (_, snapshot) = resolve_below(tx, None, head.snapshot_id).await?;

        let mut view = CatalogSnapshot::build(snapshot, current, &[], None);
        view.batch_seq = head.batch_seq;

        Ok(Some(view))
    })
    .await
}

/// The largest changelog a commit records; a batch past it records nothing
/// and readers rescan. Sized so an ordinary bulk write still leaves a
/// replayable trail — a batch that records nothing forces every reader
/// behind it to rematerialize, which costs far more than the changelog it
/// declined to write. [`REFRESH_CHURN_SHARE`] still bounds what a replay
/// will accept, so this cap governs recording alone.
const MAX_REFRESH_KEYS: usize = 4_096;

/// A refresher rescans rather than replays a gap whose churn exceeds this
/// share of the live catalog.
const REFRESH_CHURN_SHARE: usize = 2;

/// The `current` keys a batch wrote, sorted and deduplicated. The flag is
/// false — and the list empty — for a batch that wrote more than
/// [`MAX_REFRESH_KEYS`] of them or holds a key this binary cannot decode.
fn refresh_keys_of(writes: &[StagedWrite]) -> (Vec<Vec<u8>>, bool) {
    let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();
    for (encoded, _) in writes {
        match Key::decode(encoded) {
            Ok(Key::Current(_)) => {
                keys.insert(encoded.clone());
                if keys.len() > MAX_REFRESH_KEYS {
                    return (Vec::new(), false);
                }
            }
            Ok(_) => {}
            Err(_) => return (Vec::new(), false),
        }
    }

    (keys.into_iter().collect(), true)
}

/// How many commits' changelogs the store keeps: each commit deletes the
/// one this many snapshots back.
const CHANGELOG_WINDOW: u64 = 64;

/// The changelog writes a batch minting `snapshot_id` carries: its own
/// record of the `current` keys `writes` names, and the deletion of the
/// record `CHANGELOG_WINDOW` snapshots back. A batch with no usable
/// changelog writes only the deletion; the absent record tells a reader to
/// rematerialize.
pub(crate) fn changelog_writes(snapshot_id: u64, writes: &[StagedWrite]) -> Vec<StagedWrite> {
    let (keys, complete) = refresh_keys_of(writes);
    let mut out = Vec::with_capacity(2);
    if complete {
        out.push((
            Key::Changelog { snapshot_id }.encode(),
            Some(value::encode_value(&proto::ChangelogValue { keys })),
        ));
    }
    if let Some(expired) = snapshot_id.checked_sub(CHANGELOG_WINDOW) {
        out.push((
            Key::Changelog {
                snapshot_id: expired,
            }
            .encode(),
            None,
        ));
    }

    out
}

/// Advances `base` to head by replaying the changelog of every snapshot in
/// the gap, re-reading exactly the `current` keys those commits wrote.
///
/// Returns `None`, and the caller rematerializes, when head has not moved
/// forward, a batch in the gap minted no snapshot, a snapshot record or
/// changelog in the gap is missing, or the churn exceeds
/// [`REFRESH_CHURN_SHARE`] of the live catalog.
pub(crate) async fn refresh(
    tx: ReadHandle<'_>,
    base: &CatalogSnapshot,
) -> Result<Option<CatalogSnapshot>> {
    read::consistent(tx, || async move {
        let head = read_head_value(tx).await?;
        let churn_limit = base.live_entity_count() / REFRESH_CHURN_SHARE;
        replay(tx, base, &head, churn_limit).await
    })
    .await
}

/// One pass of [`refresh`], against the state `head` names, declining a gap
/// whose churn passes `churn_limit`.
async fn replay(
    tx: ReadHandle<'_>,
    base: &CatalogSnapshot,
    head: &proto::HeadValue,
    churn_limit: usize,
) -> Result<Option<CatalogSnapshot>> {
    let from = base.snapshot.snapshot_id;
    let Some(minted) = head.snapshot_id.checked_sub(from).filter(|gap| *gap > 0) else {
        return Ok(None);
    };
    // Every commit moves the count by one, minting or not, so a gap whose
    // count outruns the snapshots it minted contains a batch that recorded
    // no changelog.
    if head.batch_seq.saturating_sub(base.batch_seq) != minted {
        return Ok(None);
    }

    let walk_changelogs = async {
        let changelogs = stream::iter(
            ((from + 1)..=head.snapshot_id)
                .map(|snapshot_id| read::read_changelog(tx, snapshot_id)),
        )
        .buffer_unordered(REFRESH_READ_CONCURRENCY);
        let mut changelogs = std::pin::pin!(changelogs);
        let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        while let Some(changelog) = changelogs.try_next().await? {
            let Some(changelog) = changelog else {
                return Ok(None);
            };
            keys.extend(changelog.keys);
            if keys.len() > churn_limit {
                return Ok(None);
            }
        }
        Ok::<_, Error>(Some(keys))
    };
    let (keys, latest) =
        futures::try_join!(walk_changelogs, read::read_snapshot(tx, head.snapshot_id))?;
    let (Some(keys), Some(latest)) = (keys, latest) else {
        return Ok(None);
    };

    // Each key's current value is its post-gap state; an absent one was
    // ended or reclaimed. Reads complete in key order, so entity parents
    // fold before their children.
    let writes: Vec<StagedWrite> = stream::iter(keys.into_iter().map(|key| async move {
        let value = tx.get(&key).await.map_err(Error::from)?;
        Ok::<_, Error>((key, value.map(|bytes| bytes.to_vec())))
    }))
    .buffered(REFRESH_READ_CONCURRENCY)
    .try_collect()
    .await?;

    let mut view = base.clone();
    fold::fold_batch(&mut view, &writes)?;
    view.snapshot = latest;
    view.batch_seq = head.batch_seq;

    Ok(Some(view))
}

/// One staged write: `Some` puts, `None` deletes.
pub(crate) type StagedWrite = (Vec<u8>, Option<Vec<u8>>);

mod diff;
mod fold;
mod group;
use diff::diff_options;
pub(crate) use diff::{Touched, diff_touched, diff_writes};
pub(crate) use group::Coalescer;
use group::Outcome;

/// What a batch's durable write did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Landed {
    /// The batch is durable, with its write and projection costs separated.
    Committed(CommitTimings),
    /// A concurrent commit advanced the head first; nothing was written.
    LostRace,
}

/// Timings from the shared durable landing path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommitTimings {
    /// Time waiting for the store to make the batch durable.
    pub(crate) durable: Duration,
    /// Time folding the landed batch into the maintained projections.
    pub(crate) projection: Duration,
}

/// The result of one attempt at a group.
enum CommitOutcome {
    /// The attempt committed; carries one snapshot id per member, in
    /// member order.
    Committed(Vec<SnapshotId>),
    /// Lost the head race: what each member tried to change, and the head
    /// the batch's premise was read at.
    LostRace {
        ours: Vec<ChangeSet>,
        head_before: u64,
    },
    /// Nothing landed, for a reason the attempt cannot classify.
    Nothing,
}

/// Runs one attempt: stages every member onto whichever batch the store is
/// forming, and takes that batch's fate as this attempt's. Members stage
/// one at a time, each against the state the members before it left.
async fn attempt_group<F>(
    db: &Db,
    members: &[F],
    coalescer: &Arc<Coalescer>,
) -> Result<CommitOutcome>
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    let staged = coalescer.stage(db, members).await?;

    // A caller that staged nothing is not riding the batch and does not
    // wait on it.
    if !staged.contributed {
        return Ok(CommitOutcome::Committed(staged.ids));
    }

    match group::await_outcome(staged.outcome).await {
        Outcome::Committed => Ok(CommitOutcome::Committed(staged.ids)),
        Outcome::LostRace => Ok(CommitOutcome::LostRace {
            ours: staged.ours,
            head_before: staged.head_before,
        }),
        Outcome::Nothing => Ok(CommitOutcome::Nothing),
    }
}

/// The view a commit attempt stages against: the cached one when it
/// already matches head, one refreshed across the gap when it has fallen
/// behind, else a fresh materialization. Every read runs through `db_tx`,
/// so the view and the conflict-detection window share one start
/// sequence; the install is compare-and-set against an epoch captured
/// first.
pub(crate) async fn head_view_for(
    db_tx: &DbTransaction,
    projections: &std::sync::RwLock<ProjectionCache>,
) -> Result<Arc<CatalogSnapshot>> {
    let epoch = cache_epoch(projections);
    let handle = ReadHandle::Tx(db_tx);
    // Checked here because a warm cache returns before `materialize`'s
    // own check. Head comes first because it keys the migration stamp; a
    // stamped head spends no read at all, an unstamped one spends the same
    // two this always did.
    let head = read_head_value(handle).await?;
    refuse_mid_migration_at(handle, projections, &head).await?;
    if let Some(view) = cached_head_view(projections, &head) {
        return Ok(view);
    }

    let view = match held_head_view(projections) {
        Some(behind) => refresh(handle, &behind).await?,
        None => None,
    };
    let view = match view {
        Some(refreshed) => Arc::new(refreshed),
        None => Arc::new(materialize(handle, None).await?),
    };
    install_head_view_at(projections, epoch, Arc::clone(&view));

    Ok(view)
}

/// Folds a just-committed head-advancing batch into `base` and installs the
/// result as the new head view, or clears the cache when the fold fails.
/// Head-preserving commits instead clear the cache before they commit
/// ([`invalidate_head_view`]).
pub(crate) fn refresh_head_view(
    projections: &std::sync::RwLock<ProjectionCache>,
    base: &CatalogSnapshot,
    writes: &[StagedWrite],
) {
    let mut view = base.clone();
    match fold::fold_batch(&mut view, writes) {
        Ok(()) => install_head_view(projections, Arc::new(view)),
        Err(err) => {
            debug!(error = %err, "head view fold failed; clearing the cached view");
            invalidate_head_view(projections);
        }
    }
}

/// Applies direct writes not represented by staged-row translation.
pub(crate) fn finish_translated_head_view(
    view: &mut CatalogSnapshot,
    writes: &[StagedWrite],
) -> Result<()> {
    fold::fold_batch(view, writes)
}

/// How a successful head-advancing commit obtains its cached head view.
pub(crate) enum HeadViewUpdate {
    /// Derive the view from the commit's base and writes.
    Rebuild(Arc<CatalogSnapshot>),
    /// Install the view staged-row translation already produced.
    Prepared(Arc<CatalogSnapshot>),
}

fn install_committed_head_view(
    projections: &std::sync::RwLock<ProjectionCache>,
    update: HeadViewUpdate,
    writes: &[StagedWrite],
) {
    match update {
        HeadViewUpdate::Rebuild(base) => refresh_head_view(projections, &base, writes),
        HeadViewUpdate::Prepared(view) => install_head_view(projections, view),
    }
}

/// What one attempt staged onto its transaction.
enum Prepared {
    /// The closure changed nothing; the head is unchanged.
    Nothing {
        /// The head snapshot id the attempt read.
        head: u64,
    },
    /// Writes are staged and ready to commit.
    Staged {
        /// This member's change set, empty for an options-only commit.
        ours: Box<ChangeSet>,
        /// The snapshot id a successful commit reports.
        commits: u64,
        writes: Vec<StagedWrite>,
        /// The batch's size, index entries included.
        staged_bytes: StagedBytes,
    },
}

/// The inline shapes a batch wrote. Each carries its own format floor, and
/// neither subsumes the other.
#[derive(Clone, Copy, Default)]
pub(crate) struct InlineShapes {
    /// A locator carrying its chunk's identity.
    pub(crate) chunk_directory: bool,
    /// A schema recorded as a reference to another version.
    pub(crate) schema_reference: bool,
}

/// The format floor `state`'s index records require: deferred upkeep
/// implies [`FORMAT_WITH_DEFERRED_INDEX`], a `building` index
/// [`FORMAT_WITH_STAGED_INDEX`], and any other index [`FORMAT_WITH_INDEX`].
/// The floor is the highest term that applies, never the first.
pub(crate) fn index_format(state: &CatalogSnapshot) -> u64 {
    let indexes = || state.indexes.values().flat_map(BTreeMap::values);

    [
        indexes()
            .any(|index| index.deferred_maintenance == Some(true))
            .then_some(FORMAT_WITH_DEFERRED_INDEX),
        indexes()
            .any(|index| index.build_state.is_some())
            .then_some(FORMAT_WITH_STAGED_INDEX),
        state
            .indexes
            .values()
            .any(|per_table| !per_table.is_empty())
            .then_some(FORMAT_WITH_INDEX),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(FORMAT_VERSION)
}

/// The format floor the inline shapes `wrote` require. Neither shape
/// subsumes the other, so a batch writing both owes the higher.
pub(crate) fn inline_format(wrote: InlineShapes) -> u64 {
    [
        wrote
            .schema_reference
            .then_some(FORMAT_WITH_INLINE_SCHEMA_REFERENCE),
        wrote
            .chunk_directory
            .then_some(FORMAT_WITH_INLINE_CHUNK_IDENTITY),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(FORMAT_VERSION)
}

/// The store format `state` requires once `wrote` is folded in.
pub(crate) fn target_format(state: &CatalogSnapshot, wrote: InlineShapes) -> u64 {
    index_format(state).max(inline_format(wrote))
}

/// The format-stamp write this commit owes, if any. The stamp is forward-only.
async fn format_stamp(
    db_tx: &DbTransaction,
    projections: &std::sync::RwLock<ProjectionCache>,
    state: &CatalogSnapshot,
    wrote: InlineShapes,
) -> Result<Option<StagedWrite>> {
    format_stamp_to(db_tx, projections, target_format(state, wrote)).await
}

/// The forward-only format stamp write required to reach `target_format`.
/// A target at or below the highest version this handle has seen owes no
/// write and costs no read: the stamp only ever rises, so an observed
/// version stays a valid floor.
pub(crate) async fn format_stamp_to(
    db_tx: &DbTransaction,
    projections: &std::sync::RwLock<ProjectionCache>,
    target_format: u64,
) -> Result<Option<StagedWrite>> {
    if target_format <= FORMAT_VERSION || format_floor(projections) >= target_format {
        return Ok(None);
    }
    let current = read::read_format(ReadHandle::Tx(db_tx))
        .await?
        .map_or(FORMAT_VERSION, |format| format.format_version);
    raise_format_floor(projections, current);
    if current >= target_format {
        return Ok(None);
    }

    info!(
        from = current,
        to = target_format,
        "upgrading the store format stamp"
    );

    Ok(Some((
        Key::Sys(SysKey::Format).encode(),
        Some(value::encode_value(&proto::FormatValue {
            format_version: target_format,
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
        })),
    )))
}

/// The `ducklake_schema_versions` record a schema-changing commit owes for
/// one table. It outlives the snapshot record: an expired snapshot's data
/// files still resolve their schema version through it.
pub(crate) fn schema_version_write(
    table_id: u64,
    begin_snapshot: u64,
    schema_version: u64,
) -> StagedWrite {
    (
        Key::SchemaVersion {
            table_id,
            begin_snapshot,
        }
        .encode(),
        Some(value::encode_value(&proto::SchemaVersionValue {
            schema_version,
        })),
    )
}

/// Runs the closure against `base` and stages the resulting writes.
/// Options-only commits stage no snapshot record and no head advance.
async fn prepare_and_stage<F>(
    db_tx: &DbTransaction,
    projections: &std::sync::RwLock<ProjectionCache>,
    f: &F,
    base: &CatalogSnapshot,
) -> Result<Prepared>
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    let head = base.snapshot.snapshot_id;
    let new_id = head + 1;
    let mut tx = Transaction::new(base.clone(), new_id);
    f(&mut tx)?;
    let TransactionParts {
        operations,
        index_entries,
        inline_ops,
        mut state,
        next_catalog_id,
        next_file_id,
    } = tx.into_parts();

    if operations.is_empty() {
        let mut writes = Vec::new();
        diff_options(&mut writes, diff::Scope::All, base, &state);
        if writes.is_empty() {
            return Ok(Prepared::Nothing { head });
        }
        // The unchanged head is re-put as the conflict anchor.
        writes.push(head_stamp(head, base.batch_seq));
        let staged_bytes = stage_writes(db_tx, &writes)?;
        return Ok(Prepared::Staged {
            ours: Box::default(),
            commits: head,
            writes,
            staged_bytes,
        });
    }

    // Entries stage before the entity diff so a poisoned definition rides
    // that diff. The three reads touch disjoint subspaces, so they run
    // together.
    let index_entry_count = index_entries.len();
    // This path records every schema whole; only the staged one writes a
    // schema as a reference to another version.
    let wrote_inline = InlineShapes {
        chunk_directory: inline_ops
            .iter()
            .any(|operation| matches!(operation, inline::InlineStage::Insert { .. })),
        schema_reference: false,
    };
    let (entries, inline_writes, format_write) = futures::try_join!(
        index_maintenance::stage_index_entries(db_tx, index_entries),
        inline::stage_inline_writes(db_tx, projections, &inline_ops),
        format_stamp(db_tx, projections, &state, wrote_inline),
    )?;
    let poisoned = entries.poisoned;
    index_maintenance::apply_poison(&mut state, &poisoned);

    let mut writes = diff_writes(base, &state, new_id);
    writes.extend(inline_writes);
    writes.extend(format_write);
    let schema_changed = operations.iter().any(Operation::is_schema_changing);
    let schema_changed_table_ids: Vec<u64> = operations
        .iter()
        .filter_map(Operation::schema_changed_table_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let schema_version = base.snapshot.schema_version + u64::from(schema_changed);
    for table_id in &schema_changed_table_ids {
        writes.push(schema_version_write(*table_id, new_id, schema_version));
    }
    let ours = ChangeSet::from_operations(&operations);
    let changelog = changelog_writes(new_id, &writes);

    let snapshot = proto::SnapshotValue {
        snapshot_id: new_id,
        snapshot_time_micros: now_micros(),
        schema_version,
        next_catalog_id,
        next_file_id,
        changes_made: ours.to_changes_made(),
        author: None,
        commit_message: None,
        commit_extra_info: None,
        schema_changed_table_ids,
        deleted_data_file_ids: ours.deleted_data_file_ids.iter().copied().collect(),
    };
    writes.extend(changelog);
    writes.push((
        Key::Snapshot {
            snapshot_id: new_id,
        }
        .encode(),
        Some(value::encode_value(&snapshot)),
    ));
    writes.push(head_stamp(new_id, base.batch_seq));
    let mut staged_bytes = stage_writes(db_tx, &writes)?;
    staged_bytes.0 = staged_bytes.0.saturating_add(entries.bytes);
    tracing::debug!(
        snapshot = new_id,
        index_entries = index_entry_count,
        inline_ops = inline_ops.len(),
        catalog_writes = writes.len(),
        poisoned_indexes = poisoned.len(),
        staged_bytes = staged_bytes.0,
        "commit staged"
    );

    Ok(Prepared::Staged {
        ours: Box::new(ours),
        commits: new_id,
        writes,
        staged_bytes,
    })
}

pub(crate) fn stage_writes(db_tx: &DbTransaction, writes: &[StagedWrite]) -> Result<StagedBytes> {
    let mut staged = StagedBytes::default();
    for (key, write) in writes {
        staged.add(key.len(), write.as_ref().map_or(0, Vec::len));
        match write {
            Some(bytes) => db_tx.put(key, bytes),
            None => db_tx.delete(key),
        }
        .map_err(Error::from)?;
    }
    Ok(staged)
}

/// The head value a batch's own `sys/head` write leaves standing, if it
/// carries one.
fn staged_head_value(writes: &[StagedWrite]) -> Option<proto::HeadValue> {
    staged_sys_value::<proto::HeadValue>(writes, SysKey::Head)
}

/// The format version a batch's own stamp leaves standing, if it carries
/// one.
fn staged_format_version(writes: &[StagedWrite]) -> Option<u64> {
    staged_sys_value::<proto::FormatValue>(writes, SysKey::Format)
        .map(|format| format.format_version)
}

fn staged_sys_value<M: prost::Message + Default>(writes: &[StagedWrite], key: SysKey) -> Option<M> {
    let encoded = Key::Sys(key).encode();
    writes
        .iter()
        .rev()
        .find(|(key, _)| *key == encoded)
        .and_then(|(_, write)| write.as_ref())
        .and_then(|bytes| value::decode_value::<M>(bytes).ok())
}

/// The head pointer a batch moves: the id it commits against, and the id
/// it leaves standing. A head-preserving commit repeats the same id.
#[derive(Clone, Copy)]
pub(crate) struct HeadTransition {
    pub(crate) before: u64,
    pub(crate) after: u64,
}

/// Commits one staged batch and folds the result into the maintained
/// projections. The one place a catalog batch reaches the store.
pub(crate) async fn commit_batch(
    db_tx: DbTransaction,
    heads: HeadTransition,
    writes: &[StagedWrite],
    staged_bytes: StagedBytes,
    head_view_update: HeadViewUpdate,
    projections: &std::sync::RwLock<ProjectionCache>,
    durability: &CommitDurability,
) -> Result<Landed> {
    let head = heads.after;
    // A head-preserving commit reuses the head id with new content, so the
    // cache is dropped before the write is visible.
    let head_advanced = head > heads.before;
    if !head_advanced {
        invalidate_head_view(projections);
    }
    let durable_started = Instant::now();
    match commit_durable(db_tx, "commit", staged_bytes, durability).await {
        Ok(_) => {
            let durable = durable_started.elapsed();
            let projection_started = Instant::now();
            // `sys/head` is the conflict anchor, so winning it proves no
            // batch landed between the base's migration check and this
            // write. A migration start stamps the head, so none began.
            if let Some(head_after) = staged_head_value(writes) {
                note_migration_clear(projections, head_after);
            }
            // A format stamp is true once its batch lands, not when read.
            if let Some(stamped) = staged_format_version(writes) {
                raise_format_floor(projections, stamped);
            }
            fold_committed_batch(projections, writes, head);
            if head_advanced {
                install_committed_head_view(projections, head_view_update, writes);
            }
            let projection = projection_started.elapsed();
            debug!(
                operation = "commit",
                snapshot = head,
                staged_bytes = staged_bytes.0,
                elapsed_ms = crate::telemetry::milliseconds(durable),
                projection_ms = crate::telemetry::milliseconds(projection),
                "durable commit landed"
            );
            Ok(Landed::Committed(CommitTimings {
                durable,
                projection,
            }))
        }
        Err(err) if err.kind() == slatedb::ErrorKind::Transaction => Ok(Landed::LostRace),
        Err(err) => {
            // The write's fate is unknown, so the held view may be stale.
            invalidate_head_view(projections);
            Err(err.into())
        }
    }
}

/// Runs [`commit_batch`] on a task of its own and waits for it, putting
/// the durable write out of reach of the caller's cancellation. A task
/// that never reports leaves the write's fate unknown and surfaces as
/// [`Error::Interrupted`]; the caller must re-resolve head rather than
/// re-drive.
pub(crate) async fn commit_batch_off_task(
    db_tx: DbTransaction,
    heads: HeadTransition,
    writes: Vec<StagedWrite>,
    staged_bytes: StagedBytes,
    head_view_update: HeadViewUpdate,
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
    durability: CommitDurability,
) -> Result<Landed> {
    let task = {
        let projections = Arc::clone(&projections);
        tokio::spawn(async move {
            commit_batch(
                db_tx,
                heads,
                &writes,
                staged_bytes,
                head_view_update,
                &projections,
                &durability,
            )
            .await
        })
    };

    match task.await {
        Ok(landed) => landed,
        Err(err) => {
            // The task may have died between the durable write and the fold.
            invalidate_head_view(&projections);
            warn!(error = %err, "the durable write did not report back; its outcome is unknown");
            Err(Error::Interrupted(format!(
                "the durable write did not report back ({err}); it may or may not have \
                 landed — re-resolve head before re-driving"
            )))
        }
    }
}

/// Commits a group of closures as one batch, retrying benign races with a
/// full re-run. A true conflict surfaces as [`Error::CommitConflict`],
/// which the caller may retry; an exhausted budget surfaces as
/// [`Error::RetryBudgetExhausted`], which it may not.
///
/// Returns one snapshot id per member, in member order. A group of one is
/// the ordinary commit path.
pub(crate) async fn commit_cycle<F>(
    db: &Db,
    members: &[F],
    coalescer: &Arc<Coalescer>,
) -> Result<Vec<SnapshotId>>
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    let started = std::time::Instant::now();
    let mut first_head = None;
    let mut last_intervening = Vec::new();

    for attempt in 0..MAX_COMMIT_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(retry_backoff(attempt)).await;
        }
        match attempt_group(db, members, coalescer).await? {
            CommitOutcome::Nothing => {
                tracing::debug!(attempt, "commit's batch wrote nothing; retrying");
            }
            CommitOutcome::Committed(ids) => {
                tracing::debug!(
                    snapshot = ids.last().map(|id| id.get()),
                    members = members.len(),
                    attempt,
                    elapsed_ms = crate::telemetry::milliseconds(started.elapsed()),
                    "commit landed"
                );
                return Ok(ids);
            }
            CommitOutcome::LostRace { ours, head_before } => {
                first_head.get_or_insert(head_before);
                // An options-only loser is last-write-wins: always benign.
                if ours.iter().all(ChangeSet::is_empty) {
                    tracing::debug!(
                        attempt,
                        head_before,
                        "commit lost the head race with nothing to conflict over; retrying"
                    );
                    continue;
                }
                let intervening = classify_intervening_changes(db, head_before, &ours).await?;
                if let Some(snapshot_id) = intervening.conflict {
                    tracing::debug!(
                        attempt,
                        head_before,
                        winner = snapshot_id,
                        "commit conflicts with an intervening commit; surfacing"
                    );
                    return Err(Error::CommitConflict(format!(
                        "concurrent commit {snapshot_id} touched the same state"
                    )));
                }
                last_intervening = intervening.snapshot_ids;
                tracing::debug!(
                    attempt,
                    head_before,
                    intervening = ?last_intervening,
                    "commit lost the head race to disjoint commits; retrying"
                );
            }
        }
    }

    let head_before = first_head.unwrap_or_default();
    tracing::warn!(
        attempts = MAX_COMMIT_ATTEMPTS,
        head_before,
        intervening = ?last_intervening,
        "commit exhausted its retry budget; reporting a terminal error"
    );
    Err(Error::RetryBudgetExhausted(format!(
        "spent {MAX_COMMIT_ATTEMPTS} attempts from head snapshot {head_before} without \
         settling; commits above it: {last_intervening:?}"
    )))
}

struct InterveningClassification {
    conflict: Option<u64>,
    snapshot_ids: Vec<u64>,
}

async fn read_head_snapshot_id(db: &Db) -> Result<u64> {
    let head_bytes = db
        .get(Key::Sys(SysKey::Head).encode())
        .await
        .map_err(Error::from)?
        .ok_or_else(|| Error::Corruption("store has no head pointer".to_string()))?;
    let head: proto::HeadValue = value::decode_value(&head_bytes)?;
    Ok(head.snapshot_id)
}

async fn read_intervening_change(db: &Db, snapshot_id: u64) -> Result<(u64, ChangeSet)> {
    let change_set = match db
        .get(Key::Snapshot { snapshot_id }.encode())
        .await
        .map_err(Error::from)?
    {
        Some(bytes) => {
            let snapshot: proto::SnapshotValue = value::decode_value(&bytes)?;
            let mut change_set = ChangeSet::parse(&snapshot.changes_made);
            // `changes_made` names the tables deleted from; the record names
            // the files, which classifies deletes at file grain.
            if !snapshot.deleted_data_file_ids.is_empty() {
                change_set.deleted_data_file_ids =
                    snapshot.deleted_data_file_ids.iter().copied().collect();
                change_set.deletes_untargeted_files = false;
            }
            change_set
        }
        None => ChangeSet {
            has_unknown: true,
            ..ChangeSet::default()
        },
    };
    Ok((snapshot_id, change_set))
}

fn intervening_change_stream(
    db: &Db,
    first: u64,
    last: u64,
) -> impl futures::Stream<Item = Result<(u64, ChangeSet)>> + '_ {
    stream::iter(
        (first..=last)
            .map(move |snapshot_id| async move { read_intervening_change(db, snapshot_id).await }),
    )
    .buffered(INTERVENING_READ_CONCURRENCY)
}

/// What a staged commit lost its race to: which intervening snapshot
/// conflicts with `ours`, every snapshot that landed above `head_before`,
/// and whether those account for the whole gap.
///
/// A head-preserving batch mints no snapshot id and states no changes, so
/// walking the ids never visits it. Every landed batch moves the batch
/// count by one, so a gap wider than the walk saw means something landed
/// that this cannot describe — and a race is only benign when nothing is
/// unaccounted for.
///
/// Exposed for the staged path, which cannot re-run its caller and so
/// reports the conflict rather than retrying past it.
pub(crate) async fn conflicting_intervening_change(
    db: &Db,
    head_before: &proto::HeadValue,
    ours: &ChangeSet,
) -> Result<(Option<u64>, Vec<u64>, bool)> {
    let classified =
        classify_intervening_changes(db, head_before.snapshot_id, std::slice::from_ref(ours))
            .await?;
    let batches = u64::try_from(classified.snapshot_ids.len()).unwrap_or(u64::MAX);
    let accounted = read_head_stamp(db)
        .await?
        .is_some_and(|head| head.batch_seq.saturating_sub(head_before.batch_seq) == batches);

    Ok((classified.conflict, classified.snapshot_ids, accounted))
}

/// The whole head stamp, or `None` when the store carries no head yet.
async fn read_head_stamp(db: &Db) -> Result<Option<proto::HeadValue>> {
    db.get(Key::Sys(SysKey::Head).encode())
        .await
        .map_err(Error::from)?
        .map(|bytes| value::decode_value(&bytes))
        .transpose()
}

async fn classify_intervening_changes(
    db: &Db,
    head_before: u64,
    ours: &[ChangeSet],
) -> Result<InterveningClassification> {
    let head = read_head_snapshot_id(db).await?;
    if head_before >= head {
        return Ok(InterveningClassification {
            conflict: None,
            snapshot_ids: Vec::new(),
        });
    }
    let mut changes = std::pin::pin!(intervening_change_stream(
        db,
        head_before.saturating_add(1),
        head,
    ));
    let mut snapshot_ids = Vec::new();
    while let Some((snapshot_id, theirs)) = changes.try_next().await? {
        snapshot_ids.push(snapshot_id);
        // A group conflicts if any member does. Reads complete in snapshot
        // order, so the lowest conflicting snapshot ends the scan.
        if ours
            .iter()
            .any(|mine| crate::transaction::operations::conflicts(mine, &theirs))
        {
            return Ok(InterveningClassification {
                conflict: Some(snapshot_id),
                snapshot_ids,
            });
        }
    }
    Ok(InterveningClassification {
        conflict: None,
        snapshot_ids,
    })
}

/// The change sets of every commit above `head_before`. An expired record
/// classifies as an unknowable change, never as corruption.
#[cfg(test)]
async fn intervening_changes(db: &Db, head_before: u64) -> Result<Vec<(u64, ChangeSet)>> {
    let head = read_head_snapshot_id(db).await?;
    if head_before >= head {
        return Ok(Vec::new());
    }
    intervening_change_stream(db, head_before.saturating_add(1), head)
        .try_collect()
        .await
}

#[cfg(test)]
mod tests;
