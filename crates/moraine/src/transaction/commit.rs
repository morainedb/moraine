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
            ProjectionCache, cache_epoch, cached_head_view, fold_committed_batch, held_head_view,
            install_head_view, install_head_view_at, invalidate_head_view,
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

/// Structural layout version a fresh store bootstraps at — format 1 plus
/// nothing. Index-free stores stay here, byte-identical to pre-index
/// stores and readable by older binaries.
pub(crate) const FORMAT_VERSION: u64 = 1;
/// Format stamped lazily the first time an equality index exists: format 1
/// plus the `index` subspace and `index` kind. Older binaries, which
/// maintain no entries, refuse it.
pub(crate) const FORMAT_WITH_INDEX: u64 = 2;
/// Format stamped the first time a staged (multi-commit) index build
/// exists. A format-2 binary would read a `building` definition as a ready
/// index and serve from an under-covered entry set, so it must refuse this.
pub(crate) const FORMAT_WITH_STAGED_INDEX: u64 = 3;
/// Format stamped the first time an index defers SQL additions. A format-3
/// binary does not understand the deferred marker and could expose partial
/// coverage as ready after rewriting the definition.
pub(crate) const FORMAT_WITH_DEFERRED_INDEX: u64 = 4;
/// Format stamped the first time durable maintenance status is recorded.
/// Older binaries keep status only in process memory and would silently
/// omit the durable history, so they must refuse this store.
pub(crate) const FORMAT_WITH_MAINTENANCE_STATUS: u64 = 5;
/// Format stamped the first time an inline chunk row-range locator is
/// written. Older binaries do not remove these locators with their chunks.
pub(crate) const FORMAT_WITH_INLINE_CHUNK_DIRECTORY: u64 = 6;
/// The highest format this binary understands. It opens any store in
/// `MIN_FORMAT_VERSION..=MAX_FORMAT_VERSION` and refuses a newer one.
pub(crate) const MAX_FORMAT_VERSION: u64 = FORMAT_WITH_INLINE_CHUNK_DIRECTORY;
/// The lowest structural format this binary reads directly. A store below
/// this floor must be migrated up before an ordinary attach can use it.
/// Every format so far is additive — each adds a subspace without moving an
/// existing key — so the floor sits at the base format and no store in the
/// world is below it. It rises only when a format rewrites the keyspace,
/// which is what makes an old store unreadable rather than merely older.
pub(crate) const MIN_FORMAT_VERSION: u64 = FORMAT_VERSION;
/// Bounded internal retries before a benign race is reported as a
/// conflict.
pub(crate) const MAX_COMMIT_ATTEMPTS: usize = 10;

/// Delay before the second commit attempt, in microseconds; each further
/// retry doubles it, up to [`RETRY_BACKOFF_MAX_MICROS`]. Without a pause the
/// budget is ten immediate re-runs, which under real contention is a spin
/// that burns the budget faster than the contention can clear.
const RETRY_BACKOFF_BASE_MICROS: u64 = 2_000;
/// Ceiling on one retry's delay. The whole budget then spans a few hundred
/// milliseconds — enough for a competing commit to land, short enough that a
/// caller waiting on a conflict is not left hanging.
const RETRY_BACKOFF_MAX_MICROS: u64 = 50_000;

/// Intervening snapshot records read concurrently after a lost head race.
const INTERVENING_READ_CONCURRENCY: usize = 64;

/// How long to wait before re-running `attempt` (0-based; the first attempt
/// never waits). Exponential to the cap, plus jitter of up to the base delay
/// so two writers that just collided do not back off in lockstep and collide
/// again.
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

/// Commits `tx` and waits for the batch to reach object storage, naming
/// `operation` and the batch's `staged` size in the log if the wait runs
/// long.
///
/// The wait is unbounded on purpose. A failed object-store write is retried
/// beneath us indefinitely, so a write that never succeeds stalls here
/// rather than failing. Giving up on a deadline would not undo the staged
/// batch: the flush continues, so the deadline would report failure for a
/// commit that still lands, and a caller re-driving it would apply it
/// twice. A stall that says so in the log is the half of that trade worth
/// having.
pub(crate) async fn commit_durable(
    tx: DbTransaction,
    operation: &'static str,
    staged: StagedBytes,
) -> std::result::Result<Option<slatedb::WriteHandle>, slatedb::Error> {
    let options = durable();
    let mut commit = Box::pin(tx.commit_with_options(&options));
    let mut waited = Duration::ZERO;
    loop {
        if let Ok(outcome) = tokio::time::timeout(STALL_INTERVAL, &mut commit).await {
            return outcome;
        }
        waited = waited.saturating_add(STALL_INTERVAL);
        // No error accompanies this, and none can: the failure is retried
        // below us, so there is nothing here to report but the wait. The
        // batch's size is the one fact that separates the two causes — a
        // batch too large to transfer inside the store client's request
        // timeout can never land, however healthy the credentials are.
        warn!(
            operation,
            waited_seconds = waited.as_secs(),
            staged_bytes = staged.0,
            "still waiting for object storage to accept a durable write; the batch goes as one \
             request and is retried indefinitely, so it will not fail on its own — check that a \
             batch this size fits the store client's request timeout, then credentials and \
             bucket policy"
        );
    }
}

/// A commit that returns without waiting for the write to reach object
/// storage. The write is still atomic and visible to this handle at once;
/// only the durability wait — a flush-cadence tick — is skipped. Use it
/// where a lost write is self-correcting, never where a caller treats the
/// return as a durable fact.
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
    if read::read_migration(tx).await?.is_some() {
        return Err(Error::Migration(
            "store is mid-migration; refusing to open — Catalog::migrate resumes it from the \
             durable cursor, and takes a store path rather than an open catalog, so it runs \
             against a store no attach will touch"
                .to_string(),
        ));
    }
    match read::read_format(tx).await? {
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
/// snapshot 0 (carrying the default `main` schema, counters advanced past
/// its id), the `main` schema record itself, the global `encrypted`
/// option, and head pointer — the same starting catalog shape a fresh
/// DuckLake metadata store carries.
///
/// `encrypted` is recorded explicitly (as `"true"`/`"false"`) and only
/// here: whether data files are encrypted is fixed when the catalog is
/// created, exactly as DuckLake fixes it when initializing a metadata
/// store.
fn stage_bootstrap(
    tx: &DbTransaction,
    encrypted: bool,
    data_path: Option<&str>,
) -> Result<StagedBytes> {
    let staged = std::cell::Cell::new(StagedBytes::default());
    let stage = |key: Key, bytes: Vec<u8>| {
        let key = key.encode();
        let mut running = staged.get();
        running.add(key.len(), bytes.len());
        staged.set(running);
        tx.put(key, bytes).map_err(Error::from)
    };
    stage(
        Key::Sys(SysKey::Format),
        value::encode_value(&proto::FormatValue {
            format_version: FORMAT_VERSION,
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    )?;
    // Bootstrap's snapshot records minting `main`, byte-identical to the
    // `created_schema:"main"` DuckLake's own initialization writes.
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
    )?;
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
    )?;
    // The global option record carries `encrypted` and, when the lake was
    // given a data root, `data_path` — so a later open reads the root back
    // without being told it again.
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
    )?;
    stage(
        Key::Sys(SysKey::Head),
        value::encode_value(&proto::HeadValue {
            snapshot_id: 0,
            batch_seq: 0,
        }),
    )?;
    Ok(staged.get())
}

/// How many attempts an open that is fenced while creating the catalog
/// gets before the fence reaches the caller.
///
/// Two are enough for the ordinary case — the initializer that displaced
/// the first has finished by then, so the second adopts the catalog it
/// left. The third is headroom for initializers that keep arriving. The
/// count stays small because every attempt takes the writer epoch in turn,
/// so two openers that keep displacing each other would otherwise loop
/// rather than fail.
const GENESIS_ATTEMPTS: u32 = 3;

/// Why an open attempt returned no catalog.
enum OpenFailure {
    /// Fenced while creating the catalog. The staged genesis never landed
    /// and the attempt wrote nothing else, so opening again can neither
    /// double-initialize nor find a half-made store.
    FencedAtGenesis(Error),
    /// Anything else: opening again would reach the same answer.
    Fatal(Error),
}

/// Opens the store, bootstrapping an empty one in one atomic batch under
/// conflict detection — a lost bootstrap race re-validates instead of
/// double-initializing. Every exit that does not commit rolls back.
///
/// A genesis displaced by another initializer re-attempts, up to
/// [`GENESIS_ATTEMPTS`]. Only genesis does: an open that finds a catalog
/// returns before it stages anything, so a fence from anywhere else means
/// a live writer took the store over, and re-taking it is the caller's
/// decision rather than this function's. Re-attempting genesis takes the
/// writer epoch in turn and so may displace an initializer that had just
/// won — which is what opening read-write does anyway, and what the
/// attempt this retry replaces would have done had it been a moment
/// slower.
pub(crate) async fn open_initialized(
    store: StoreBuilder<'_>,
    encrypted: bool,
    data_path: Option<&str>,
) -> Result<(Db, Arc<CacheCounters>)> {
    let mut attempt = 1;
    loop {
        match open_attempt(&store, encrypted, data_path).await {
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
) -> std::result::Result<(Db, Arc<CacheCounters>), OpenFailure> {
    // Timed for the same reason the read-only open is: a writer open reads
    // the manifest and replays the log before any catalog work begins.
    let started = Instant::now();
    let (db, counters) = store.open_writer().await.map_err(OpenFailure::Fatal)?;
    info!(
        writer_open_ms = started.elapsed().as_secs_f64() * 1_000.0,
        "opened the store read-write"
    );
    let tx = begin_snapshot(&db).await?;

    match validate_format(ReadHandle::Tx(&tx)).await {
        Ok(Some(_)) => {
            tx.rollback();
            return Ok((db, counters));
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

    match commit_durable(tx, "bootstrap", staged).await {
        Ok(_) => {
            // Once per store, ever: the commit that created the catalog.
            info!(encrypted, data_path, "bootstrapped a fresh catalog store");
            Ok((db, counters))
        }
        Err(err) if err.kind() == slatedb::ErrorKind::Transaction => {
            // Lost the bootstrap race: someone initialized concurrently.
            let tx = begin_snapshot(&db).await?;
            let validated = validate_format(ReadHandle::Tx(&tx)).await;
            tx.rollback();
            match validated {
                Ok(Some(_)) => Ok((db, counters)),
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
/// caught here ends the attempt: the format has not been read yet, so
/// nothing tells a displaced genesis apart from a live writer taking the
/// store over.
async fn begin_snapshot(db: &Db) -> std::result::Result<DbTransaction, OpenFailure> {
    db.begin(IsolationLevel::Snapshot)
        .await
        .map_err(|err| OpenFailure::Fatal(Error::from(err)))
}

/// Opens the store read-only as a [`DbReader`], validating the format it
/// finds. Never opens a `Db`, so it never fences a live writer, and never
/// bootstraps — a read-only attach against an uninitialized store is refused
/// (there is nothing committed to read).
pub(crate) async fn open_reader_initialized(
    store: StoreBuilder<'_>,
) -> Result<(DbReader, Arc<CacheCounters>)> {
    // The substrate open is timed on its own: it reads the manifest, takes
    // a checkpoint, and — unless pinned to one — replays the write-ahead
    // log, none of which any per-subspace measurement can see.
    let started = Instant::now();
    let (reader, counters) = store.open_reader().await?;
    let opened = started.elapsed();

    let started = Instant::now();
    let format = validate_format(ReadHandle::Reader(&reader)).await?;
    info!(
        reader_open_ms = opened.as_secs_f64() * 1_000.0,
        validate_ms = started.elapsed().as_secs_f64() * 1_000.0,
        "opened the store read-only"
    );

    match format {
        Some(_) => Ok((reader, counters)),
        None => Err(Error::Corruption(
            "store is not an initialized moraine catalog; a read-only attach \
             needs a writer to have created it first"
                .to_string(),
        )),
    }
}

/// Refuses a store whose keyspace is mid-move. A structural migration
/// rewrites keys in place, so any scan of it may be missing records that
/// have not arrived yet; failing is the only way to avoid returning a
/// silently partial catalog.
pub(crate) async fn refuse_mid_migration(tx: ReadHandle<'_>) -> Result<()> {
    match read::read_migration(tx).await? {
        Some(marker) => Err(Error::Migration(format!(
            "store is migrating from format {} to {}; reads are unavailable until it completes",
            marker.from_format, marker.to_format
        ))),
        None => Ok(()),
    }
}

/// The head record. An initialized store always has one, so its absence is
/// corruption rather than an empty catalog.
pub(crate) async fn read_head_value(tx: ReadHandle<'_>) -> Result<proto::HeadValue> {
    read::read_head(tx)
        .await?
        .ok_or_else(|| Error::Corruption("store has no head pointer".to_string()))
}

/// The latest committed snapshot id.
pub(crate) async fn read_head_id(tx: ReadHandle<'_>) -> Result<u64> {
    Ok(read_head_value(tx).await?.snapshot_id)
}

/// The head write every batch carries: the snapshot id it leaves at head —
/// unchanged for a maintenance batch — and a batch count one above the one
/// standing. Writing it unconditionally is what makes `sys/head` both the
/// single conflict anchor and a stamp that moves whenever committed state
/// does.
pub(crate) async fn head_write(db_tx: &DbTransaction, snapshot_id: u64) -> Result<StagedWrite> {
    let standing = read_head_value(ReadHandle::Tx(db_tx)).await?;

    Ok((
        Key::Sys(SysKey::Head).encode(),
        Some(value::encode_value(&proto::HeadValue {
            snapshot_id,
            batch_seq: standing.batch_seq.saturating_add(1),
        })),
    ))
}

/// Resolves a requested read point to the snapshot it reads at and that
/// snapshot's record. `at: None` resolves to head.
///
/// Every read that takes a snapshot id resolves it here, so catalog views
/// and inlined-row reads refuse the same ids for the same reasons.
pub(crate) async fn resolve_read_snapshot(
    tx: ReadHandle<'_>,
    at: Option<u64>,
) -> Result<(u64, proto::SnapshotValue)> {
    let head = read_head_id(tx).await?;
    resolve_below(tx, at, head).await
}

/// As [`resolve_read_snapshot`], for a caller that has already read head
/// and would otherwise pay for it twice.
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
    // A missing record at or below head is an expired snapshot, not a
    // missing one: ids are sequential to head, so `target` was minted, and
    // expiry deletes snapshot records without renumbering. The reader
    // re-resolves from head rather than dereference reclaimed files.
    let snapshot = read::read_snapshot(tx, target).await?.ok_or_else(|| {
        Error::SnapshotExpired(format!(
            "snapshot {target} is below the retention horizon (head is {head}); \
             re-resolve from head"
        ))
    })?;

    Ok((target, snapshot))
}

/// Materializes a catalog view through an open transaction, so the view
/// and any staged writes share one read point. `at: None` reads the head
/// (`current` only); `at: Some(s)` also scans `history` to reconstruct the
/// entities live at `s`.
pub(crate) async fn materialize(tx: ReadHandle<'_>, at: Option<u64>) -> Result<CatalogSnapshot> {
    match at {
        None => Ok(materialize_capturing(tx).await?.0),
        Some(_) => {
            read::consistent(tx, || async move {
                refuse_mid_migration(tx).await?;
                let head = read_head_value(tx).await?;
                let (target, snapshot) = resolve_below(tx, at, head.snapshot_id).await?;

                let current = read::scan_current_entities(tx).await?;
                let history = read::scan_history_entities(tx).await?;

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
/// records so the caller can install them as the shared record set — the
/// view and the records come from one consistent cut and stand at one
/// stamp by construction.
pub(crate) async fn materialize_capturing(
    tx: ReadHandle<'_>,
) -> Result<(CatalogSnapshot, Arc<Vec<EntityRecord>>)> {
    read::consistent(tx, || async move {
        refuse_mid_migration(tx).await?;
        let head = read_head_value(tx).await?;
        let (_, snapshot) = resolve_below(tx, None, head.snapshot_id).await?;

        // Timed apart from the decode that follows: a materialization that
        // is slow is either fetching `current` or building from it, and the
        // remedies differ entirely.
        let started = Instant::now();
        let current = read::scan_current_entities(tx).await?;
        let scanned = started.elapsed();
        info!(
            records = current.len(),
            scan_ms = scanned.as_secs_f64() * 1_000.0,
            "scanned `current`"
        );

        let mut view = CatalogSnapshot::build(snapshot, &current, &[], None);
        // A head view stands at the store state the head record names.
        view.batch_seq = head.batch_seq;

        Ok((view, Arc::new(current)))
    })
    .await
}

/// Builds a head view from the shared `current` half instead of scanning,
/// or reports `None` when the store no longer stands at `expected` — the
/// records would not match, and the caller falls back to a scan. The
/// migration marker outranks the shared records exactly as it outranks
/// every other cache.
pub(crate) async fn materialize_from(
    tx: ReadHandle<'_>,
    expected: &proto::HeadValue,
    current: &[EntityRecord],
) -> Result<Option<CatalogSnapshot>> {
    read::consistent(tx, || async move {
        refuse_mid_migration(tx).await?;
        let head = read_head_value(tx).await?;
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

/// The largest changelog a commit records. The list rides in the snapshot
/// record, which DuckLake re-reads every transaction, so it is kept small
/// deliberately: a DuckLake-sized commit writes on the order of ten keys
/// and adds a few hundred bytes, while a bulk one blows past the cap,
/// records nothing, and leaves readers to rescan — which is what a
/// refresher would choose for that much churn anyway.
const MAX_REFRESH_KEYS: usize = 256;

/// Above this share of the live catalog, replaying a gap's changelog costs
/// more than one `current` scan and a refresher rescans instead. Full
/// materialization is roughly linear in live entities; a replay pays one
/// point read per changed key plus one per snapshot record in the gap, and
/// a point read is the dearer of the two, so the crossover sits below
/// parity rather than at it.
const REFRESH_CHURN_SHARE: usize = 2;

/// The `current` keys a batch wrote: the changelog a reader replays to
/// advance a held view across this commit, sorted and deduplicated.
///
/// The flag is false — and the list empty — for a batch that wrote more
/// than [`MAX_REFRESH_KEYS`] of them, or that holds a key this binary
/// cannot decode. Either way a reader crossing the commit rematerializes.
fn refresh_keys_of(writes: &[StagedWrite]) -> (Vec<Vec<u8>>, bool) {
    let mut keys = Vec::new();
    for (encoded, _) in writes {
        match Key::decode(encoded) {
            Ok(Key::Current(_)) => keys.push(encoded.clone()),
            Ok(_) => {}
            Err(_) => return (Vec::new(), false),
        }
    }
    keys.sort_unstable();
    keys.dedup();
    if keys.len() > MAX_REFRESH_KEYS {
        return (Vec::new(), false);
    }

    (keys, true)
}

/// How many commits' changelogs the store keeps. Each commit writes its
/// own and deletes the one `CHANGELOG_WINDOW` snapshots back, so the
/// subspace is bounded by the window and nothing else has to reclaim it —
/// not expiry, not a maintenance sweep. A reader further behind than this
/// rematerializes, which a gap that long would cost it anyway.
const CHANGELOG_WINDOW: u64 = 64;

/// The changelog writes a batch minting `snapshot_id` carries: its own
/// record of the `current` keys `writes` names, and the deletion of the
/// record `CHANGELOG_WINDOW` snapshots back. A batch that recorded no
/// usable changelog writes nothing but the deletion, so the absent record
/// is what tells a reader to rematerialize.
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

/// Advances `base` to the store state `head` names by replaying the
/// changelog of every snapshot in the gap: each commit recorded the
/// `current` keys it wrote, so the refresh re-reads exactly those and drops
/// the ones now absent. Cost is proportional to churn across the gap, not
/// to catalog size.
///
/// Returns `None` when the gap cannot — or should not — be replayed, and
/// the caller rematerializes:
///
/// - head has not moved forward, so there is no changelog to walk;
/// - a batch landed that minted no snapshot (maintenance reclaims state under a
///   reused snapshot id), leaving part of the gap unrecorded;
/// - a snapshot record in the gap has been reclaimed — `base` has fallen below
///   the retention horizon and its changelog is gone;
/// - a commit in the gap declined to record its changelog;
/// - the churn is large enough that one `current` scan is cheaper.
///
/// The replay reads head itself rather than being handed it, and runs
/// under the same consistent cut a materialization does, so the view it
/// returns is stamped with the state its reads actually observed.
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
    // Every batch moves the count by one, so a gap whose count outruns the
    // snapshots it minted contains a batch that recorded no changelog.
    if head.batch_seq.saturating_sub(base.batch_seq) != minted {
        return Ok(None);
    }

    let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();
    for snapshot_id in (from + 1)..=head.snapshot_id {
        let Some(changelog) = read::read_changelog(tx, snapshot_id).await? else {
            return Ok(None);
        };
        keys.extend(changelog.keys);
        if keys.len() > churn_limit {
            return Ok(None);
        }
    }

    // The gap replays, so the view lands at head and takes head's metadata.
    let Some(latest) = read::read_snapshot(tx, head.snapshot_id).await? else {
        return Ok(None);
    };

    // Each key's current value is its post-gap state; an absent one was
    // ended or reclaimed. That is exactly the write set a fold applies, so
    // the replay reuses the fold rather than restating every kind's rules.
    let mut writes: Vec<StagedWrite> = Vec::with_capacity(keys.len());
    for key in keys {
        let value = tx.get(&key).await.map_err(Error::from)?;
        writes.push((key, value.map(|bytes| bytes.to_vec())));
    }

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
pub(crate) use diff::diff_writes;
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
    /// the batch's premise was read at — classification reads the commits
    /// above it.
    LostRace {
        ours: Vec<ChangeSet>,
        head_before: u64,
    },
    /// Nothing landed, for a reason the attempt cannot classify. Retrying
    /// is the only way to learn more: an attempt that meets the same
    /// failure without a batch to share surfaces it typed.
    Nothing,
}

/// Runs one attempt: stage every member onto whichever batch the store is
/// forming, and take that batch's fate as this attempt's.
///
/// Members are staged one caller at a time, so each stages against the
/// state the members before it left — a batch can never conflict with
/// itself, only fail on a premise an earlier member invalidated.
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
    // wait on it: it reports the head standing at its turn, exactly as a
    // commit of the same closure alone would.
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
/// behind, else a fresh materialization. Staging against this is what lets
/// a commit skip the full `current` rescan.
///
/// Every read runs through `db_tx`, so the premise view and the
/// conflict-detection window share one start sequence and no commit can land
/// between them unnoticed. The install is compare-and-set against an epoch
/// captured first, so a head-preserving commit (id reused, content changed)
/// that invalidates mid-read cannot have its invalidation undone here.
pub(crate) async fn head_view_for(
    db_tx: &DbTransaction,
    projections: &std::sync::RwLock<ProjectionCache>,
) -> Result<Arc<CatalogSnapshot>> {
    let epoch = cache_epoch(projections);
    let handle = ReadHandle::Tx(db_tx);
    // The commit path's own gate. `materialize` below carries one too, but
    // a warm cache returns before reaching it, and a commit staged against
    // a view of a keyspace mid-rewrite is the writer-side form of the
    // partial read the marker exists to forbid.
    refuse_mid_migration(handle).await?;
    let head = read_head_value(handle).await?;
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
/// result as the new head view, or clears the cache when the fold cannot be
/// applied faithfully. Only for head-advancing commits: their minted id is
/// unique, so a concurrent attempt reading the old id never mistakes this
/// view for its own. Head-preserving commits reuse the id and are handled
/// by clearing the cache before they commit ([`invalidate_head_view`]).
pub(crate) fn refresh_head_view(
    projections: &std::sync::RwLock<ProjectionCache>,
    base: &CatalogSnapshot,
    writes: &[StagedWrite],
) {
    let mut view = base.clone();
    match fold::fold_batch(&mut view, writes) {
        Ok(()) => install_head_view(projections, Arc::new(view)),
        Err(err) => {
            // The committed batch could not be folded into the cached view;
            // dropping the cache is safe (the next commit rescans and
            // reinstalls) but the cause is worth a trace — a batch this
            // writer just built should always fold.
            debug!(error = %err, "head view fold failed; clearing the cached view");
            invalidate_head_view(projections);
        }
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
        /// The staged batch, kept so a successful commit can fold it into
        /// the maintained projections.
        writes: Vec<StagedWrite>,
        /// What the batch weighs, index entries included — the size of the
        /// object-store request the durable commit becomes.
        staged_bytes: StagedBytes,
    },
}

/// The store format the staged state requires: deferred upkeep implies
/// [`FORMAT_WITH_DEFERRED_INDEX`], a `building` index
/// [`FORMAT_WITH_STAGED_INDEX`], any other index [`FORMAT_WITH_INDEX`], and
/// inline chunk locators [`FORMAT_WITH_INLINE_CHUNK_DIRECTORY`].
fn target_format(state: &CatalogSnapshot, uses_inline_chunk_directory: bool) -> u64 {
    let index_format = if state
        .indexes
        .values()
        .flat_map(BTreeMap::values)
        .any(|index| index.deferred_maintenance == Some(true))
    {
        FORMAT_WITH_DEFERRED_INDEX
    } else if state
        .indexes
        .values()
        .flat_map(BTreeMap::values)
        .any(|index| index.build_state.is_some())
    {
        FORMAT_WITH_STAGED_INDEX
    } else if state
        .indexes
        .values()
        .any(|per_table| !per_table.is_empty())
    {
        FORMAT_WITH_INDEX
    } else {
        FORMAT_VERSION
    };

    if uses_inline_chunk_directory {
        index_format.max(FORMAT_WITH_INLINE_CHUNK_DIRECTORY)
    } else {
        index_format
    }
}

/// Materializes, runs the closure, and stages the resulting writes.
/// Options-only commits stage no snapshot record and no head advance.
/// The format-stamp write this commit owes, if any. The stamp is lazy and
/// forward-only: a completed or dropped build never downgrades it.
async fn format_stamp(
    db_tx: &DbTransaction,
    state: &CatalogSnapshot,
    uses_inline_chunk_directory: bool,
) -> Result<Option<StagedWrite>> {
    format_stamp_to(db_tx, target_format(state, uses_inline_chunk_directory)).await
}

/// The forward-only format stamp write required to reach `target_format`.
pub(crate) async fn format_stamp_to(
    db_tx: &DbTransaction,
    target_format: u64,
) -> Result<Option<StagedWrite>> {
    if target_format <= FORMAT_VERSION {
        return Ok(None);
    }
    let current = read::read_format(ReadHandle::Tx(db_tx))
        .await?
        .map_or(FORMAT_VERSION, |format| format.format_version);
    if current >= target_format {
        return Ok(None);
    }

    // The stamp decides which binaries can open the store from here on.
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
/// one table. Written by both commit paths and outliving the snapshot
/// record that names the same table: expiry deletes snapshots, and a data
/// file older than every surviving snapshot still has to resolve its
/// schema version.
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

async fn prepare_and_stage<F>(
    db_tx: &DbTransaction,
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
        diff_options(&mut writes, base, &state);
        if writes.is_empty() {
            return Ok(Prepared::Nothing { head });
        }
        // Re-put the unchanged head as a conflict anchor: every batch
        // writes it, so a racing drop of this option's scope forces a
        // re-run that re-validates the scope against the winner's state
        // instead of committing blind.
        writes.push(head_write(db_tx, head).await?);
        let staged_bytes = stage_writes(db_tx, &writes)?;
        return Ok(Prepared::Staged {
            ours: Box::default(),
            commits: head,
            writes,
            staged_bytes,
        });
    }

    // Entries stage before the entity diff so a poisoned definition rides
    // that diff rather than needing a write of its own.
    let index_entry_count = index_entries.len();
    let entries = index_maintenance::stage_index_entries(db_tx, index_entries).await?;
    let poisoned = entries.poisoned;
    index_maintenance::apply_poison(&mut state, &poisoned);

    let mut writes = diff_writes(base, &state, new_id);
    // Inline records live outside the entity model, so they are translated
    // rather than diffed — and translated against `db_tx`'s pre-commit
    // state, before any of this batch's writes are staged onto it.
    writes.extend(inline::stage_inline_writes(db_tx, &inline_ops).await?);
    let uses_inline_chunk_directory = inline_ops
        .iter()
        .any(|operation| matches!(operation, inline::InlineStage::Insert { .. }));
    writes.extend(format_stamp(db_tx, &state, uses_inline_chunk_directory).await?);
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
    // Derived before the snapshot and head writes join the batch: the
    // changelog names `current` keys, and those two are not.
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
        // Recorded so a later loser can classify a delete against this
        // commit at file grain; `changes_made` carries only the table.
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
    writes.push(head_write(db_tx, new_id).await?);
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
            Some(bytes) => db_tx.put(key.clone(), bytes.clone()),
            None => db_tx.delete(key.clone()),
        }
        .map_err(Error::from)?;
    }
    Ok(staged)
}

/// Commits one staged batch — one commit's or a whole batch of them — and
/// folds the result into the maintained projections. `head` is the id the
/// batch leaves at the head pointer, its last snapshot-minting member's.
///
/// The one place a catalog batch reaches the store: both front doors land
/// here, so the durable write, the head-race classification, and the
/// projection bookkeeping that must accompany them are written once.
pub(crate) async fn commit_batch(
    db_tx: DbTransaction,
    head_before: u64,
    head: u64,
    writes: &[StagedWrite],
    staged_bytes: StagedBytes,
    base: &CatalogSnapshot,
    projections: &std::sync::RwLock<ProjectionCache>,
) -> Result<Landed> {
    // A head-preserving commit reuses the head id with new content; drop the
    // cache before the write is visible so no concurrent attempt reads a
    // stale view that still matches by id.
    let head_advanced = head > head_before;
    if !head_advanced {
        invalidate_head_view(projections);
    }
    let durable_started = Instant::now();
    match commit_durable(db_tx, "commit", staged_bytes).await {
        Ok(_) => {
            let durable = durable_started.elapsed();
            let projection_started = Instant::now();
            fold_committed_batch(projections, writes, head);
            if head_advanced {
                refresh_head_view(projections, base, writes);
            }
            let projection = projection_started.elapsed();
            debug!(
                operation = "commit",
                snapshot = head,
                staged_bytes = staged_bytes.0,
                elapsed_ms = durable.as_secs_f64() * 1_000.0,
                projection_ms = projection.as_secs_f64() * 1_000.0,
                "durable commit landed"
            );
            Ok(Landed::Committed(CommitTimings {
                durable,
                projection,
            }))
        }
        Err(err) if err.kind() == slatedb::ErrorKind::Transaction => Ok(Landed::LostRace),
        Err(err) => {
            // A lost race is a batch that provably did not land. Any other
            // failure leaves that open, and a read-write handle serves its
            // held view as head without re-reading `sys/head` — so a view
            // kept across an unresolved write could answer from a state the
            // store has already moved past.
            invalidate_head_view(projections);
            Err(err.into())
        }
    }
}

/// Runs [`commit_batch`] on a task of its own and waits for it, putting
/// the durable write out of reach of the caller's cancellation.
///
/// Everything before this call is staged in memory and freely droppable;
/// the write is the point of no return, and dropping a future parked
/// inside it cannot retract a batch already issued. Spawning moves it off
/// the cancellable future, so an interrupt races the *wait*, never the
/// write.
///
/// A task that never reports — cancelled with the runtime, or lost to a
/// panic — leaves the write's fate unknown, which is the one answer a
/// caller must not read as "nothing landed". It surfaces as
/// [`Error::Interrupted`] so the caller re-resolves head instead of
/// re-driving a commit that may already be durable.
pub(crate) async fn commit_batch_off_task(
    db_tx: DbTransaction,
    head_before: u64,
    head: u64,
    writes: Vec<StagedWrite>,
    staged_bytes: StagedBytes,
    base: Arc<CatalogSnapshot>,
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
) -> Result<Landed> {
    let task = {
        let projections = Arc::clone(&projections);
        tokio::spawn(async move {
            commit_batch(
                db_tx,
                head_before,
                head,
                &writes,
                staged_bytes,
                &base,
                &projections,
            )
            .await
        })
    };

    match task.await {
        Ok(landed) => landed,
        Err(err) => {
            // The task may have died between the durable write and the fold
            // that accompanies it, leaving the cache claiming a state the
            // store has left. A read-write handle serves that view as head
            // without re-reading `sys/head`, so re-resolving head is not
            // enough — the view has to go too.
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
/// full re-run — fresh snapshot, closures, ids — so every member's premise
/// re-validates against the state that won. A true conflict surfaces as
/// [`Error::CommitConflict`], which the caller may retry; an exhausted
/// budget surfaces as [`Error::RetryBudgetExhausted`], which it may not.
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
    // Kept across attempts so an exhausted budget can report where the
    // premise started and what it kept losing to.
    let started = std::time::Instant::now();
    let mut first_head = None;
    let mut last_intervening = Vec::new();

    for attempt in 0..MAX_COMMIT_ATTEMPTS {
        // Every path into this loop past the first is a lost race, so the
        // wait belongs here rather than at each `continue`.
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
                    elapsed_ms = started.elapsed().as_millis(),
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
    // The only record of an exhausted budget: without it a caller sees a
    // slow commit and no reason for it.
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
            // The grammar named the tables this commit deleted from; the
            // record names the files. Together they classify deletes at file
            // grain rather than conservatively at table grain.
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
        // A group conflicts if any member does: they share one batch and
        // therefore one fate. Ordered delivery makes the first verdict
        // stable while pending later reads are dropped immediately.
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

/// The change sets of every commit above `head_before`, read outside any
/// transaction (the loser's is dead). A record that has already been
/// expired by a racing maintenance commit classifies as an unknowable
/// change (forcing the conflict path), never as corruption — the caller
/// re-drives against the new head.
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
