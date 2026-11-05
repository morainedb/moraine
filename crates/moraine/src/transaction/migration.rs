//! Structural format migration: the one-way, crash-resumable rewrite that
//! carries a store from one `sys/format` version to the next.
//!
//! A migration runs as start, steps, finish. The start batch writes the
//! `sys/migration` marker, which makes every reader refuse the store. Each
//! step batch stages one bounded piece of the rewrite together with the
//! cursor that records it. The finish batch flips `sys/format` and clears
//! the marker together. A multi-version jump composes `v_n → v_{n+1}`
//! units, each running its own start, steps, and finish.

use futures::future::BoxFuture;
use slatedb::{Db, IsolationLevel};
use tracing::info;

use super::session::CommitSession;

mod inline_tombstones;
mod names;

use crate::{
    error::{Error, Result},
    fault::{CrashPoint, crash_seam},
    store::{
        StagedBytes,
        handle::ReadHandle,
        key::{Key, SysKey},
        proto, read, value,
    },
    transaction::commit::{self, CommitDurability, MAX_COMMIT_ATTEMPTS, commit_durable},
};

/// Where a step left off: the cursor the next step resumes at and what it
/// put on the batch, or `None` when the walk is done and the step staged
/// nothing.
pub(crate) type StepOutcome = Option<StepProgress>;

/// One step's contribution to the batch the driver commits.
pub(crate) struct StepProgress {
    /// The cursor the next step resumes at.
    pub(crate) cursor: Vec<u8>,
    /// Key and value bytes the step staged onto the transaction.
    pub(crate) staged: StagedBytes,
}

/// A unit's step: stages one bounded batch of its rewrite into `tx`,
/// resuming at `cursor` (empty at the start of the walk). A step must be
/// idempotent, must write new-format keys before deleting the old-format
/// keys they supersede, and commits nothing itself.
type StepFn = for<'a> fn(&'a CommitSession, &'a [u8]) -> BoxFuture<'a, Result<StepOutcome>>;

/// One `v_n → v_{n+1}` structural rewrite.
pub(crate) struct MigrationUnit {
    /// Stable name, carried into logs and the report.
    pub(crate) name: &'static str,
    /// The format this unit reads.
    pub(crate) from_format: u64,
    /// The format it writes.
    pub(crate) to_format: u64,
    /// Rewrites one bounded batch of the unit's work.
    pub(crate) step: StepFn,
}

/// Every structural migration this binary ships, in ascending order.
pub(crate) const MIGRATIONS: &[MigrationUnit] = &[
    inline_tombstones::MIGRATION,
    names::NATIVE,
    names::COMMIT_LOG,
];

/// What one raise call did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FormatRaise {
    /// The format the store carried when the call began.
    pub from_format: u64,
    /// The format it carries now. Equal to `from_format` when there was
    /// nothing to raise.
    pub to_format: u64,
}

/// What one catalog-version raise did.
///
/// A separate axis from [`FormatRaise`]: the store format is how moraine
/// writes bytes, while the catalog version is the shape it serves
/// DuckLake. They move on their own schedules.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CatalogVersionRaise {
    /// The catalog version the store recorded when the call began.
    pub from: String,
    /// The newest version this build serves. A dry run reports the move
    /// it would make without recording it; equal to `from` when there was
    /// nothing to raise.
    pub to: String,
}

/// The highest format `current` can reach without a structural rewrite:
/// this binary's newest, stopped below the first format some migration
/// unit reads. Past that one the keys have moved, and only that unit puts
/// them where the new format says they are.
fn additive_ceiling(current: u64) -> u64 {
    registry()
        .into_iter()
        .map(|unit| unit.from_format)
        .filter(|from| *from >= current)
        .min()
        .map_or(commit::MAX_FORMAT_VERSION, |rewrite_at| {
            rewrite_at.min(commit::MAX_FORMAT_VERSION)
        })
}

/// Raises `sys/format` to the additive ceiling without crossing a structural
/// migration, refusing stores mid-migration. Name directories require explicit
/// backfill, so ordinary format raises stop before that migration.
///
/// `dry_run` reports the result without writing; ordinary commits do not
/// raise the format.
pub(crate) async fn raise_format(
    db: &Db,
    durability: &CommitDurability,
    dry_run: bool,
) -> Result<FormatRaise> {
    let tx = db.begin(IsolationLevel::Snapshot).map_err(Error::from)?;

    let read = async {
        let format = read::read_format(ReadHandle::Tx(&tx))
            .await?
            .map_or(commit::FORMAT_VERSION, |stamp| stamp.format_version);
        let marker = read::read_migration(ReadHandle::Tx(&tx)).await?;
        Ok::<_, Error>((format, marker))
    }
    .await;
    let (from_format, marker) = match read {
        Ok(read) => read,
        Err(error) => {
            tx.rollback();
            return Err(error);
        }
    };

    if let Some(marker) = marker {
        tx.rollback();
        return Err(Error::Migration(format!(
            "store is mid-migration from format {} to {}; finish that before raising the format",
            marker.from_format, marker.to_format
        )));
    }

    let to_format = additive_ceiling(from_format);
    if dry_run {
        tx.rollback();
        return Ok(FormatRaise {
            from_format,
            to_format: to_format.max(from_format),
        });
    }
    if to_format <= from_format {
        tx.rollback();
        return Ok(FormatRaise {
            from_format,
            to_format: from_format,
        });
    }

    let key = Key::Sys(SysKey::Format).encode();
    let value = value::encode_value(&proto::FormatValue {
        format_version: to_format,
        writer_version: env!("CARGO_PKG_VERSION").to_string(),
        commit_log_batch_origin: None,
    });
    let mut staged = StagedBytes::default();
    staged.add(key.len(), value.len());
    if let Err(error) = tx.put(key, value) {
        tx.rollback();
        return Err(Error::from(error));
    }
    commit_durable(tx, "format raise", staged, durability).await?;

    info!(from_format, to_format, "raised the store format");
    Ok(FormatRaise {
        from_format,
        to_format,
    })
}

/// The units a call plans over: everything this binary ships, plus whatever
/// a fault-injection build installed.
fn registry() -> Vec<&'static MigrationUnit> {
    MIGRATIONS
        .iter()
        .chain(crate::fault::installed_migrations().iter().copied())
        .collect()
}

/// What one migrate call did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MigrationReport {
    /// The format the store carried when the call began.
    pub from_format: u64,
    /// The format it carries now. Equal to `from_format` when there was
    /// nothing to run.
    pub to_format: u64,
    /// The units run, in the order they ran.
    pub units_run: Vec<String>,
    /// Whether the call resumed a migration a previous run left partly
    /// applied.
    pub resumed: bool,
}

/// The units a call must run, and where the first of them resumes.
struct Plan {
    units: Vec<&'static MigrationUnit>,
    /// The durable cursor of an interrupted unit, when resuming one.
    resume: Option<Vec<u8>>,
}

/// The chain of units carrying `format` as far as this binary can take it.
fn chain_from(format: u64) -> Vec<&'static MigrationUnit> {
    let registry = registry();
    let mut units = Vec::new();
    let mut format = format;
    while let Some(unit) = registry.iter().find(|unit| unit.from_format == format) {
        units.push(*unit);
        format = unit.to_format;
    }
    units
}

/// Resolves what to run from the two durable facts a reopen can read: the
/// format stamp and the marker.
fn plan(format: u64, marker: Option<&proto::MigrationValue>) -> Result<Plan> {
    let Some(marker) = marker else {
        return Ok(Plan {
            units: chain_from(format),
            resume: None,
        });
    };

    // The finish batch flips the format and clears the marker together, so
    // no protocol step can produce a marker naming another source format.
    if marker.from_format != format {
        return Err(Error::Corruption(format!(
            "store is stamped format {format} but carries a migration marker from format {} \
             to {}; the flip and the clear land in one batch, so this pairing cannot arise",
            marker.from_format, marker.to_format
        )));
    }

    let interrupted = registry()
        .into_iter()
        .find(|unit| unit.from_format == marker.from_format && unit.to_format == marker.to_format)
        .ok_or_else(|| {
            Error::Migration(format!(
                "store is mid-migration from format {} to {}, which this binary does not carry; \
                 finish it with the binary that started it",
                marker.from_format, marker.to_format
            ))
        })?;

    let mut units = vec![interrupted];
    units.extend(chain_from(interrupted.to_format));

    Ok(Plan {
        units,
        resume: Some(marker.cursor.clone()),
    })
}

/// Stages the marker recording `unit` and its progress.
fn stage_marker(tx: &CommitSession, unit: &MigrationUnit, cursor: &[u8]) -> Result<StagedBytes> {
    let key = Key::Sys(SysKey::Migration).encode();
    let value = value::encode_value(&proto::MigrationValue {
        from_format: unit.from_format,
        to_format: unit.to_format,
        cursor: cursor.to_vec(),
    });
    let mut staged = StagedBytes::default();
    staged.add(key.len(), value.len());
    tx.put(key, value)?;
    Ok(staged)
}

/// Commits progress and its head stamp through the store's commit authority.
async fn land(
    session: CommitSession,
    mut staged: StagedBytes,
    log: Option<&crate::commit_log::LogCommitter>,
) -> Result<commit::Landed> {
    let head = commit::read_head_value(session.handle()).await?;
    let (key, value) = commit::head_stamp(head.snapshot_id, head.batch_seq);
    let value = value.unwrap_or_default();
    staged.add(key.len(), value.len());
    session.put(key, value)?;
    let projections = std::sync::RwLock::default();
    commit::commit_batch(
        std::sync::Arc::new(session),
        commit::HeadTransition {
            before: head.snapshot_id,
            after: head.snapshot_id,
        },
        &[],
        staged,
        commit::Landing {
            projections: &projections,
            log,
        },
        &CommitDurability::OnFlushInterval,
    )
    .await
}

async fn check_progress(
    session: &CommitSession,
    unit: &MigrationUnit,
    cursor: Option<&[u8]>,
) -> Result<()> {
    let format = read::read_format(session.handle()).await?;
    let marker = read::read_migration(session.handle()).await?;
    let same_marker = match (cursor, marker.as_ref()) {
        (None, None) => true,
        (Some(cursor), Some(marker)) => {
            marker.from_format == unit.from_format
                && marker.to_format == unit.to_format
                && marker.cursor == cursor
        }
        _ => false,
    };
    if format.is_none_or(|format| format.format_version != unit.from_format) || !same_marker {
        return Err(Error::Migration(
            "migration progress changed; reopen to resume the durable cursor".into(),
        ));
    }
    Ok(())
}

async fn start(
    db: &Db,
    unit: &MigrationUnit,
    log: Option<&crate::commit_log::LogCommitter>,
) -> Result<()> {
    for attempt in 0..MAX_COMMIT_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(commit::retry_backoff(attempt)).await;
        }
        let session =
            CommitSession::folder(db.begin(IsolationLevel::Snapshot).map_err(Error::from)?);
        check_progress(&session, unit, None).await?;
        let staged = stage_marker(&session, unit, &[])?;
        if matches!(
            land(session, staged, log).await?,
            commit::Landed::Committed(_)
        ) {
            return Ok(());
        }
    }
    Err(Error::RetryBudgetExhausted(
        "migration start lost every head race".into(),
    ))
}

async fn finish(
    db: &Db,
    unit: &MigrationUnit,
    cursor: &[u8],
    log: Option<&crate::commit_log::LogCommitter>,
) -> Result<()> {
    let session = CommitSession::folder(db.begin(IsolationLevel::Snapshot).map_err(Error::from)?);
    check_progress(&session, unit, Some(cursor)).await?;
    let old = read::read_format(session.handle())
        .await?
        .ok_or_else(|| Error::Corruption("migration lost its format stamp".into()))?;
    let format = Key::Sys(SysKey::Format).encode();
    let stamp = value::encode_value(&proto::FormatValue {
        format_version: unit.to_format,
        writer_version: env!("CARGO_PKG_VERSION").to_string(),
        commit_log_batch_origin: old.commit_log_batch_origin,
    });
    let marker = Key::Sys(SysKey::Migration).encode();
    let mut staged = StagedBytes::default();
    staged.add(format.len(), stamp.len());
    staged.add(marker.len(), 0);
    session.put(format, stamp)?;
    session.delete(marker)?;
    require_landed(land(session, staged, log).await?)
}

fn require_landed(landed: commit::Landed) -> Result<()> {
    match landed {
        commit::Landed::Committed(_) => Ok(()),
        commit::Landed::LostRace => Err(Error::Migration(
            "migration lost a head race; reopen to resume the durable cursor".into(),
        )),
    }
}

/// Advances only after each rewrite and its cursor land in one commit.
async fn run_unit(
    db: &Db,
    unit: &MigrationUnit,
    resume: Option<Vec<u8>>,
    log: Option<&crate::commit_log::LogCommitter>,
) -> Result<()> {
    let mut cursor = if let Some(cursor) = resume {
        cursor
    } else {
        start(db, unit, log).await?;
        crash_seam(CrashPoint::AfterStart)?;
        Vec::new()
    };
    loop {
        let session =
            CommitSession::folder(db.begin(IsolationLevel::Snapshot).map_err(Error::from)?);
        check_progress(&session, unit, Some(&cursor)).await?;
        let Some(StepProgress {
            cursor: next,
            staged: rewritten,
        }) = (unit.step)(&session, &cursor).await?
        else {
            session.rollback();
            break;
        };
        let mut staged = stage_marker(&session, unit, &next)?;
        staged.0 = staged.0.saturating_add(rewritten.0);
        require_landed(land(session, staged, log).await?)?;
        crash_seam(CrashPoint::AfterStep)?;
        cursor = next;
    }
    crash_seam(CrashPoint::BeforeFinish)?;
    finish(db, unit, &cursor, log).await?;
    crash_seam(CrashPoint::AfterFinish)?;
    Ok(())
}

/// Migrates `db` as far as this binary can carry it, resuming an interrupted
/// migration if the marker says one is in flight. A store already at the
/// newest format is left untouched and reported as such. Epoch fencing on
/// the writer means exactly one migrator runs.
pub(crate) async fn run(
    db: &Db,
    log: Option<&crate::commit_log::LogCommitter>,
) -> Result<MigrationReport> {
    let tx = db.begin(IsolationLevel::Snapshot).map_err(Error::from)?;
    let (format, marker) = futures::join!(
        read::read_format(ReadHandle::Tx(&tx)),
        read::read_migration(ReadHandle::Tx(&tx)),
    );
    tx.rollback();

    let format = format?.ok_or_else(|| {
        Error::Corruption(
            "store is not an initialized moraine catalog; there is nothing to migrate".to_string(),
        )
    })?;
    let from_format = format.format_version;
    let marker = marker?;

    // Older additive stamps can reach the first rewrite's source without
    // moving keys. An interrupted rewrite must resume at its recorded stamp.
    let planning_format = if marker.is_none() && from_format < commit::MIN_FORMAT_VERSION {
        raise_format(db, &CommitDurability::OnFlushInterval, false)
            .await?
            .to_format
    } else {
        from_format
    };
    let Plan { units, mut resume } = plan(planning_format, marker.as_ref())?;
    let resumed = resume.is_some();

    if units.is_empty() {
        return Ok(MigrationReport {
            from_format,
            to_format: planning_format,
            units_run: Vec::new(),
            resumed,
        });
    }

    let mut units_run = Vec::with_capacity(units.len());
    let mut to_format = planning_format;

    for unit in units {
        info!(
            unit = unit.name,
            from = unit.from_format,
            to = unit.to_format,
            resuming = resume.is_some(),
            "migrating the store format"
        );

        run_unit(db, unit, resume.take(), log).await?;

        units_run.push(unit.name.to_string());
        to_format = unit.to_format;
    }

    info!(from = from_format, to = to_format, "migration complete");

    Ok(MigrationReport {
        from_format,
        to_format,
        units_run,
        resumed,
    })
}

#[cfg(any(test, feature = "fault-injection"))]
pub(crate) mod synthetic;

#[cfg(test)]
mod tests;
