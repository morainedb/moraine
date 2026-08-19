//! Opening, bootstrap, and snapshot materialization. The commit cycle
//! itself builds on these.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use slatedb::{Db, DbReader, DbTransaction, IsolationLevel, WriteHandle};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    catalog::CatalogSnapshot,
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{EntityKey, Key, SysKey},
        open::StoreBuilder,
        proto, read, value,
    },
    transaction::{
        index_maintenance::{self, ProbeHandle},
        inline,
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
/// Format stamped only at bootstrap, for a store whose commits ride the
/// commit-slot log rather than direct writer transactions. Never reached by
/// the lazy format-advance path — a store does not drift into this
/// topology.
pub(crate) const FORMAT_MULTI_WRITER: u64 = 4;
/// The highest format this binary understands. It opens any store in
/// `MIN_FORMAT_VERSION..=MAX_FORMAT_VERSION` and refuses a newer one.
pub(crate) const MAX_FORMAT_VERSION: u64 = FORMAT_MULTI_WRITER;
/// The lowest structural format this binary reads directly. Every format so
/// far is additive — each adds a subspace without moving an existing key — so
/// the floor sits at the base format and no store in the world is below it. It
/// rises only when a format rewrites the keyspace.
pub(crate) const MIN_FORMAT_VERSION: u64 = FORMAT_VERSION;

/// Current time in microseconds since the Unix epoch. Clamped, never
/// panicking: a clock before the epoch stamps 0.
pub(crate) fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
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
///
/// Stamps [`FORMAT_MULTI_WRITER`] and a zero fold cursor, bootstrap being the
/// degenerate first fold.
fn stage_bootstrap(tx: &DbTransaction, encrypted: bool, data_path: Option<&str>) -> Result<()> {
    let stage = |key: Key, bytes: Vec<u8>| tx.put(key.encode(), bytes).map_err(Error::from);
    stage(
        Key::Sys(SysKey::Format),
        value::encode_value(&proto::FormatValue {
            format_version: FORMAT_MULTI_WRITER,
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    )?;
    stage(
        Key::Sys(SysKey::Secret),
        value::encode_value(&proto::SecretValue {
            token: mint_secret().to_vec(),
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
            transaction_id: None,
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
    )
}

/// Opens the store, bootstrapping an empty one in one atomic batch under
/// conflict detection — a lost bootstrap race re-validates instead of
/// double-initializing. Every exit that does not commit rolls back.
pub(crate) async fn open_initialized(
    store: StoreBuilder<'_>,
    encrypted: bool,
    data_path: Option<&str>,
) -> Result<Db> {
    let db = store.open_writer().await?;
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;

    match validate_format(ReadHandle::Tx(&tx)).await {
        Ok(Some(_)) => {
            tx.rollback();
            return Ok(db);
        }
        Ok(None) => {}
        Err(err) => {
            tx.rollback();
            return Err(err);
        }
    }

    if let Err(err) = stage_bootstrap(&tx, encrypted, data_path) {
        tx.rollback();
        return Err(err);
    }

    match commit_durably(&db, tx).await {
        Ok(_) => {
            // Once per store, ever: the commit that created the catalog.
            info!(encrypted, data_path, "bootstrapped a fresh catalog store");
            Ok(db)
        }
        Err(err) if err.kind() == slatedb::ErrorKind::Transaction => {
            // Lost the bootstrap race: someone initialized concurrently.
            let tx = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            let validated = validate_format(ReadHandle::Tx(&tx)).await;
            tx.rollback();
            match validated? {
                Some(_) => Ok(db),
                None => Err(Error::Corruption(
                    "bootstrap race left the store uninitialized".to_string(),
                )),
            }
        }
        Err(err) => Err(err.into()),
    }
}

/// Opens the store read-only as a [`DbReader`], returning it with the
/// structural format it is stamped with. Never opens a `Db`, so it never
/// fences a live writer, and never bootstraps. Returns `None` when the reader
/// opens onto a store carrying no format stamp yet (a writer began creating it
/// but has not finished); a store with no manifest at all fails to open at all,
/// and that failure propagates as an error rather than `None`.
pub(crate) async fn open_reader_initialized(
    store: StoreBuilder<'_>,
) -> Result<Option<(DbReader, u64)>> {
    let reader = store.open_reader().await?;
    match validate_format(ReadHandle::Reader(&reader)).await? {
        Some(format) => Ok(Some((reader, format.format_version))),
        None => Ok(None),
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
    let db = store.open_writer().await?;
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

/// Materializes a catalog view through an open transaction, so the view
/// and any staged writes share one read point. `at: None` reads the head
/// (`current` only); `at: Some(s)` also scans `history` to reconstruct the
/// entities live at `s`.
pub(crate) async fn materialize(tx: ReadHandle<'_>, at: Option<u64>) -> Result<CatalogSnapshot> {
    let head = read::read_head(tx)
        .await?
        .ok_or_else(|| Error::Corruption("store has no head pointer".to_string()))?
        .snapshot_id;
    let target = match at {
        Some(requested) if requested > head => {
            return Err(Error::NotFound(format!(
                "snapshot {requested} (head is {head})"
            )));
        }
        Some(requested) => requested,
        None => head,
    };
    // A missing record at or below head is an expired snapshot, not
    // corruption: expiry deletes snapshot records without renumbering.
    // The caller re-resolves from head.
    let snapshot = read::read_snapshot(tx, target)
        .await?
        .ok_or_else(|| Error::NotFound(format!("snapshot {target} (expired or never minted)")))?;
    let current = read::scan_current_entities(tx).await?;
    let history = match at {
        Some(_) => read::scan_history_entities(tx).await?,
        None => Vec::new(),
    };

    Ok(CatalogSnapshot::build(
        snapshot,
        current,
        history,
        at.map(|_| target),
    ))
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
    let current = read::scan_current_entities_overlaid(tx, overlay).await?;
    let history = read::scan_history_entities_overlaid(tx, overlay).await?;

    Ok(CatalogSnapshot::build(snapshot, current, history, Some(at)))
}

/// One staged write: `Some` puts, `None` deletes.
pub(crate) type StagedWrite = (Vec<u8>, Option<Vec<u8>>);

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
) -> std::result::Result<Option<WriteHandle>, slatedb::Error> {
    let Some(handle) = tx.commit().await? else {
        return Ok(None);
    };

    let mut durable = Box::pin(db.flush());
    let mut waited = Duration::ZERO;
    loop {
        if let Ok(outcome) = tokio::time::timeout(STALL_INTERVAL, &mut durable).await {
            drop(durable);
            return outcome.map(|()| Some(handle));
        }
        waited = waited.saturating_add(STALL_INTERVAL);
        warn!(
            operation,
            waited_seconds = waited.as_secs(),
            "still waiting for object storage to accept a durable write; writes are retried \
             indefinitely, so check credentials and bucket policy"
        );
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

mod diff;
pub(crate) mod fold;
use diff::diff_options;
pub(crate) use diff::diff_writes;

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

/// The store format the staged state requires: a `building` index implies
/// [`FORMAT_WITH_STAGED_INDEX`], any other index [`FORMAT_WITH_INDEX`],
/// else the base [`FORMAT_VERSION`].
fn target_format(state: &CatalogSnapshot) -> u64 {
    if state
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
    }
}

/// The format-stamp write this commit owes, if any. The stamp is lazy and
/// forward-only: a completed or dropped build never downgrades it.
/// `format_current` is the store's current stamp; `None` skips the stamp,
/// for a topology whose format never advances lazily.
fn format_stamp(format_current: Option<u64>, state: &CatalogSnapshot) -> Option<StagedWrite> {
    let target_format = target_format(state);
    if target_format <= FORMAT_VERSION {
        return None;
    }
    let current = format_current?;
    if current >= target_format {
        return None;
    }

    // The stamp decides which binaries can open the store from here on.
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
pub(crate) async fn assemble_commit<F>(
    probe: ProbeHandle<'_>,
    f: &F,
    base: &CatalogSnapshot,
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
    let inline_writes = inline::stage_inline_writes(store, Some(overlay), &inline_ops).await?;

    if operations.is_empty() && inline_writes.is_empty() {
        let mut writes = Vec::new();
        diff_options(&mut writes, base, &state);
        if writes.is_empty() {
            return Ok(Prepared::Nothing);
        }
        // Re-put the unchanged head as a conflict anchor: every
        // snapshot-minting commit writes it, so a racing drop of this
        // option's scope forces a re-run that re-validates the scope
        // against the winner's state instead of committing blind.
        writes.push((
            Key::Sys(SysKey::Head).encode(),
            Some(value::encode_value(&proto::HeadValue {
                snapshot_id: head,
                batch_seq: 0,
            })),
        ));
        return Ok(Prepared::Staged(Assembled {
            ours: Box::default(),
            head_before: head,
            commits: head,
            writes,
        }));
    }

    // Entries plan before the entity diff so a poisoned definition rides that
    // diff rather than needing a write of its own.
    let (poisoned, mut writes) =
        index_maintenance::plan_index_entries(probe, &index_entries).await?;
    index_maintenance::apply_poison(&mut state, &poisoned);

    writes.extend(diff_writes(base, &state, new_id));
    writes.extend(inline_writes);
    writes.extend(format_stamp(format_current, &state));
    tracing::debug!(
        snapshot = new_id,
        index_entries = index_entries.len(),
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
    let ours = ChangeSet::from_operations(&operations);

    let schema_version = base.snapshot.schema_version + u64::from(schema_changed);

    // The schema-version rows this commit staged, as records of their own:
    // `snapshot` carries them too, but only until expiry deletes it, and the
    // files they describe outlive that.
    for table_id in &schema_changed_table_ids {
        writes.push(schema_version_write(*table_id, new_id, schema_version));
    }

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
        deleted_data_file_ids: Vec::new(),
    };
    writes.push((
        Key::Snapshot {
            snapshot_id: new_id,
        }
        .encode(),
        Some(value::encode_value(&snapshot)),
    ));
    writes.push((
        Key::Sys(SysKey::Head).encode(),
        Some(value::encode_value(&proto::HeadValue {
            snapshot_id: new_id,
            batch_seq: 0,
        })),
    ));

    Ok(Prepared::Staged(Assembled {
        ours: Box::new(ours),
        head_before: head,
        commits: new_id,
        writes,
    }))
}

pub(crate) fn stage_writes(db_tx: &DbTransaction, writes: &[StagedWrite]) -> Result<()> {
    for (key, write) in writes {
        match write {
            Some(bytes) => db_tx.put(key.clone(), bytes.clone()),
            None => db_tx.delete(key.clone()),
        }
        .map_err(Error::from)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
