//! Opening, bootstrap, and snapshot materialization. The commit cycle
//! itself builds on these.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use slatedb::{Db, DbReader, DbTransaction, IsolationLevel, WriteHandle};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    catalog::{
        CatalogSnapshot, Timestamp,
        projection::{ProjectionCache, format_floor, raise_format_floor},
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
        index_maintenance::{self, ProbeHandle},
        inline,
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
/// Format stamped only at bootstrap, for a store whose commits ride the
/// commit-slot log rather than direct writer transactions. Never reached by
/// the lazy format-advance path — a store does not drift into this
/// topology, so a raise stops below it.
pub(crate) const FORMAT_MULTI_WRITER: u64 = 8;
/// The highest format a stamp alone can reach: past it the formats describe
/// a topology rather than a record shape, and nothing drifts into one.
pub(crate) const MAX_ADDITIVE_FORMAT: u64 = FORMAT_WITH_INLINE_SCHEMA_REFERENCE;
/// The highest format this binary understands. It opens any store in
/// `MIN_FORMAT_VERSION..=MAX_FORMAT_VERSION` and refuses a newer one.
pub(crate) const MAX_FORMAT_VERSION: u64 = FORMAT_MULTI_WRITER;
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

/// Commits `tx` into `db` and flushes, which is what makes the write durable:
/// the store journals nothing of its own — the slot log is its journal — so a
/// batch sits in the memtable until a flush lands it in an L0 SST. An empty
/// transaction writes nothing, so it has nothing to flush.
pub(crate) async fn commit_durably(
    db: &Db,
    tx: DbTransaction,
) -> std::result::Result<Option<WriteHandle>, slatedb::Error> {
    let handle = tx.commit().await?;
    let Some(handle) = handle else {
        return Ok(None);
    };
    db.flush().await?;
    refuse_a_write_past_the_next_slot(db, &handle)?;

    Ok(Some(handle))
}

/// Refuses a store write that has run into the sequence number the next
/// unfolded slot will take.
///
/// The store's own writes share a sequence space with the log's slots and take
/// the numbers between the slot last folded and the one after it, so they
/// order after everything folded and before everything still to fold. Writing
/// past that ceiling would put a store write at a slot's own number, and the
/// fold would then skip that slot as already covered. The interval is a
/// million writes wide, so reaching it means a session wrote without ever
/// folding; the fix is to fold, not to widen it.
fn refuse_a_write_past_the_next_slot(
    db: &Db,
    handle: &WriteHandle,
) -> std::result::Result<(), slatedb::Error> {
    let folded = db.status().current_manifest.replay_after_wal_id();
    let ceiling = moraine_wal::slot_sequence(folded.saturating_add(1));
    if handle.seqnum() >= ceiling {
        return Err(slatedb::Error::invalid(format!(
            "this store write took sequence {}, at or past the {ceiling} the slot after the \
             fold cursor {folded} will take; fold the log before writing more",
            handle.seqnum()
        )));
    }

    Ok(())
}

/// The width of the store-held forwarding token.
pub(crate) const SECRET_LEN: usize = 32;

/// Mints a fresh forwarding token from two random UUIDs — 256 token bits over
/// the process's `getrandom`-backed UUID source, no extra dependency.
pub(crate) fn mint_secret() -> [u8; SECRET_LEN] {
    let mut token = [0u8; SECRET_LEN];
    token[..16].copy_from_slice(&Uuid::new_v4().into_bytes());
    token[16..].copy_from_slice(&Uuid::new_v4().into_bytes());
    token
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
/// snapshot 0, the `main` schema record, the forwarding token, the global
/// option record (`encrypted` as `"true"`/`"false"`, plus `data_path` when
/// given), and the head pointer. Stamps [`FORMAT_MULTI_WRITER`]: a store does
/// not drift into that topology, so bootstrap is the only place it is set.
fn stage_bootstrap(
    tx: &DbTransaction,
    encrypted: bool,
    data_path: Option<&str>,
) -> Result<StagedBytes> {
    let mut writes: Vec<StagedWrite> = Vec::with_capacity(6);
    let mut stage = |key: Key, bytes: Vec<u8>| writes.push((key.encode(), Some(bytes)));
    stage(
        Key::Sys(SysKey::Format),
        value::encode_value(&proto::FormatValue {
            format_version: FORMAT_MULTI_WRITER,
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    );
    stage(
        Key::Sys(SysKey::Secret),
        value::encode_value(&proto::SecretValue {
            token: mint_secret().to_vec(),
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
            transaction_id: None,
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
) -> Result<(Db, Arc<CacheCounters>, u64)> {
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

    match commit_durable(&db, tx, "bootstrap", staged).await {
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

/// Converts a format 1–3 store to the slot-log topology in one atomic batch:
/// [`FORMAT_MULTI_WRITER`] and a zero fold cursor, the existing store already
/// being the folded state and the slot log starting empty. Opening the writer
/// fences any incumbent old-binary writer.
///
/// Idempotent by the format check: a store already stamped
/// [`FORMAT_MULTI_WRITER`] (a racing migration won) migrates nothing and
/// succeeds. A migration whose own write is fenced by a competing migration
/// surfaces [`Error::Fenced`], and the caller re-probes.
pub(crate) async fn migrate_to_slot_log(store: StoreBuilder<'_>) -> Result<()> {
    let (db, _) = store.open_writer().await?;
    let outcome = migrate_stamp(&db).await;
    match db.close().await {
        Ok(()) => outcome,
        Err(err) => outcome.and(Err(Error::from(err))),
    }
}

/// Stamps the slot-log format through the fenced writer. Re-reads the format
/// under the fence, so a store a racing migration already converted stamps
/// nothing.
async fn migrate_stamp(db: &Db) -> Result<()> {
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;

    let from_format = match validate_format(ReadHandle::Tx(&tx)).await {
        Ok(Some(format)) if format.format_version >= FORMAT_MULTI_WRITER => {
            tx.rollback();
            return Ok(());
        }
        Ok(Some(format)) => format.format_version,
        Ok(None) => {
            tx.rollback();
            return Err(Error::Corruption(
                "store lost its format stamp before migration could run".to_string(),
            ));
        }
        Err(err) => {
            tx.rollback();
            return Err(err);
        }
    };

    let stamp = |key: Key, bytes: Vec<u8>| tx.put(key.encode(), bytes).map_err(Error::from);
    if let Err(err) = stamp(
        Key::Sys(SysKey::Format),
        value::encode_value(&proto::FormatValue {
            format_version: FORMAT_MULTI_WRITER,
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    ) {
        tx.rollback();
        return Err(err);
    }

    match commit_durably(db, tx).await {
        Ok(_) => {
            warn!(
                from_format,
                to_format = FORMAT_MULTI_WRITER,
                "migrated the catalog store to the slot-log format, fencing any old-binary writer"
            );
            Ok(())
        }
        // A racing migration committed first: it stamped the same format, so
        // the store is already converted.
        Err(err) if err.kind() == slatedb::ErrorKind::Transaction => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// [`materialize`] over the folded store with the unfolded tail overlaid, for a
/// time-travel target no folder has applied. The whole tail is overlaid, not a
/// prefix truncated at the target, so a later commit's backdated record — a
/// flush's data file effective at or below `at` — is present and the snapshot
/// filter admits it. `head` is the tail's own head, the ceiling `at` may name.
pub(crate) async fn materialize_overlaid(
    tx: ReadHandle<'_>,
    overlay: &moraine_wal::Overlay,
    head: u64,
    at: u64,
) -> Result<CatalogSnapshot> {
    if at > head {
        return Err(Error::NotFound(format!("snapshot {at} (head is {head})")));
    }
    let snapshot = read::read_snapshot_overlaid(tx, overlay, at)
        .await?
        .ok_or_else(|| Error::NotFound(format!("snapshot {at} (expired or never minted)")))?;
    let current = read::scan_current_entities_overlaid(tx, Some(overlay)).await?;
    let history = read::scan_history_entities_overlaid(tx, Some(overlay)).await?;

    Ok(CatalogSnapshot::build(
        snapshot,
        &current,
        &history,
        Some(at),
    ))
}

/// How long a durable commit may wait before the wait itself is reported,
/// and how often it is reported thereafter.
const STALL_INTERVAL: Duration = Duration::from_secs(10);

/// Commits `tx` and waits for the batch to reach object storage, naming
/// `operation` in the log if the wait runs long.
///
/// The wait is unbounded on purpose. A failed object-store write is retried
/// beneath us indefinitely, so a permanent refusal — expired credentials, a
/// revoked bucket policy — stalls here rather than failing. Giving up on a
/// deadline would not undo the staged batch: the flush continues, so the
/// deadline would report failure for a commit that still lands, and a
/// caller re-driving it would apply it twice. A stall that says so in the
/// log is the half of that trade worth having.
pub(crate) async fn commit_durable(
    db: &Db,
    tx: DbTransaction,
    operation: &'static str,
    staged: StagedBytes,
) -> std::result::Result<Option<WriteHandle>, slatedb::Error> {
    let Some(handle) = tx.commit().await? else {
        return Ok(None);
    };

    let mut durable = Box::pin(db.flush());
    let mut waited = Duration::ZERO;
    loop {
        if let Ok(outcome) = tokio::time::timeout(STALL_INTERVAL, &mut durable).await {
            drop(durable);
            outcome?;
            refuse_a_write_past_the_next_slot(db, &handle)?;
            return Ok(Some(handle));
        }
        waited = waited.saturating_add(STALL_INTERVAL);
        warn!(
            operation,
            waited_seconds = waited.as_secs(),
            staged_bytes = staged.0,
            "still waiting for object storage to accept a durable write; writes are retried \
             indefinitely, so check credentials and bucket policy"
        );
    }
}

/// A commit that returns without flushing, so the batch stays in the memtable
/// until a later flush lands it. The write is still atomic and visible to this
/// handle at once. Only for writes whose loss is self-correcting.
///
/// Durability is the caller's explicit `flush`, not a per-write flag: these
/// options simply decline to ask for one.
pub(crate) fn non_durable() -> slatedb::config::WriteOptions {
    slatedb::config::WriteOptions::default()
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

/// The `ducklake_schema_versions` record a schema-changing commit owes for
/// one table. Outlives the snapshot record that names the same table: expiry
/// deletes snapshots, and a data file older than every surviving snapshot
/// still has to resolve its schema version.
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

/// The largest changelog a commit records; a batch past it records nothing
/// and readers rescan. Sized so an ordinary bulk write still leaves a
/// replayable trail — a batch that records nothing forces every reader
/// behind it to rematerialize, which costs far more than the changelog it
/// declined to write. [`REFRESH_CHURN_SHARE`] still bounds what a replay
/// will accept, so this cap governs recording alone.
const MAX_REFRESH_KEYS: usize = 4_096;

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

/// One staged write: `Some` puts, `None` deletes.
pub(crate) type StagedWrite = (Vec<u8>, Option<Vec<u8>>);

mod diff;
pub(crate) mod fold;
use diff::diff_options;
pub(crate) use diff::{Touched, diff_touched, diff_writes};

/// Everything one commit attempt assembles, independent of where the batch is
/// arbitrated.
pub(crate) struct Assembled {
    /// This attempt's change set, empty for an options-only commit.
    pub(crate) ours: Box<ChangeSet>,
    /// The head snapshot id the attempt's premise was read at.
    pub(crate) head_before: u64,
    /// The snapshot id a successful commit reports.
    pub(crate) commits: u64,
    /// The full batch to stage: index entries, entity diff, the format stamp
    /// this commit owes, the minted snapshot, and the head advance. A
    /// successful commit also folds it into the maintained projections.
    pub(crate) writes: Vec<StagedWrite>,
}

/// What one commit attempt computes, independent of where it is arbitrated.
pub(crate) enum Prepared {
    /// The closure changed nothing; the head is unchanged.
    Nothing,
    /// A staged batch ready to commit.
    Staged(Assembled),
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

/// The format-stamp write this commit owes, if any. The stamp is lazy and
/// forward-only: a completed or dropped build never downgrades it.
/// `format_current` is the store's current stamp; `None` skips the stamp,
/// for a topology whose format never advances lazily.
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

fn format_stamp(
    format_current: Option<u64>,
    state: &CatalogSnapshot,
    uses_inline_chunk_directory: bool,
) -> Option<StagedWrite> {
    let target_format = target_format(state, uses_inline_chunk_directory);
    if target_format <= FORMAT_VERSION {
        return None;
    }
    let current = format_current?;
    if current >= target_format {
        return None;
    }

    info!(
        from = current,
        to = target_format,
        "upgrading the store format stamp"
    );

    Some((
        Key::Sys(SysKey::Format).encode(),
        Some(value::encode_value(&proto::FormatValue {
            format_version: target_format,
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
        })),
    ))
}

/// Materializes the closure against `base` and assembles the full write batch,
/// reading uniqueness probes through `probe`. Options-only commits assemble no
/// snapshot record and no head advance. `format_current` feeds the lazy format
/// stamp. `transaction_id`, when set, is stamped into the minted snapshot
/// record so a slot-backed commit's id survives folding and the tail scan can
/// find it; the single-writer path passes `None`.
#[allow(clippy::too_many_lines)]
pub(crate) async fn assemble_commit<F>(
    probe: ProbeHandle<'_>,
    f: &F,
    base: &CatalogSnapshot,
    projections: &std::sync::RwLock<ProjectionCache>,
    format_current: Option<u64>,
    transaction_id: Option<[u8; 16]>,
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

    let ProbeHandle::Overlaid { store, overlay } = probe;
    let inline_writes =
        inline::stage_inline_writes(store, Some(overlay), projections, &inline_ops).await?;

    if operations.is_empty() && inline_writes.is_empty() {
        let mut writes = Vec::new();
        diff_options(&mut writes, diff::Scope::All, base, &state);
        if writes.is_empty() {
            return Ok(Prepared::Nothing);
        }
        // Re-put the unchanged head as a conflict anchor: every
        // snapshot-minting commit writes it, so a racing drop of this
        // option's scope forces a re-run that re-validates the scope
        // against the winner's state instead of committing blind.
        writes.push(head_stamp(head, base.batch_seq));
        return Ok(Prepared::Staged(Assembled {
            ours: Box::default(),
            head_before: head,
            commits: head,
            writes,
        }));
    }

    // Entries plan before the entity diff so a poisoned definition rides that
    // diff rather than needing a write of its own.
    let index_entry_count = index_entries.len();
    let index_maintenance::StagedEntries {
        mut writes,
        poisoned,
        deferred,
        locator_writes,
        ..
    } = index_maintenance::plan_index_entries(probe, index_entries).await?;
    // The verb path diffs the whole catalog, so nothing here consumes the
    // record of which definitions those two rewrote.
    let mut touched = Touched::default();
    index_maintenance::apply_poison(&mut state, &poisoned, &mut touched);
    index_maintenance::apply_deferred_maintenance(
        base,
        &mut state,
        &deferred,
        new_id,
        &mut touched,
    );
    let uses_inline_chunk_directory = !locator_writes.is_empty();
    writes.extend(locator_writes);

    writes.extend(diff_writes(base, &state, new_id));
    writes.extend(inline_writes);
    writes.extend(format_stamp(
        format_current,
        &state,
        uses_inline_chunk_directory,
    ));
    tracing::debug!(
        snapshot = new_id,
        index_entries = index_entry_count,
        batch_writes = writes.len(),
        poisoned_indexes = poisoned.len(),
        "commit assembled"
    );

    let schema_changed = operations.iter().any(Operation::is_schema_changing);
    let schema_changed_table_ids: Vec<u64> = operations
        .iter()
        .filter_map(Operation::schema_changed_table_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let _ours = ChangeSet::from_operations(&operations);

    let schema_version = base.snapshot.schema_version + u64::from(schema_changed);

    // The schema-version rows this commit staged, as records of their own:
    // `snapshot` carries them too, but only until expiry deletes it, and the
    // files they describe outlive that.
    for table_id in &schema_changed_table_ids {
        writes.push(schema_version_write(*table_id, new_id, schema_version));
    }
    let ours = ChangeSet::from_operations(&operations);
    let _changelog = changelog_writes(new_id, &writes);

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
        transaction_id: transaction_id.map(|id| id.to_vec()),
        deleted_data_file_ids: ours.deleted_data_file_ids.iter().copied().collect(),
    };
    writes.push((
        Key::Snapshot {
            snapshot_id: new_id,
        }
        .encode(),
        Some(value::encode_value(&snapshot)),
    ));
    writes.push(head_stamp(new_id, base.batch_seq));

    Ok(Prepared::Staged(Assembled {
        ours: Box::new(ours),
        head_before: head,
        commits: new_id,
        writes,
    }))
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

#[cfg(test)]
mod tests;
