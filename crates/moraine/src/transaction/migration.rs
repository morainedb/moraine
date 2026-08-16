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
use slatedb::{Db, DbTransaction, IsolationLevel};
use tracing::info;

use crate::{
    error::{Error, Result},
    fault::{CrashPoint, crash_seam},
    store::{
        StagedBytes,
        handle::ReadHandle,
        key::{Key, SysKey},
        proto, read, value,
    },
    transaction::commit::commit_durable,
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
type StepFn = for<'a> fn(&'a DbTransaction, &'a [u8]) -> BoxFuture<'a, Result<StepOutcome>>;

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
/// Empty: every format to date is additive, so no keyspace is rewritten.
pub(crate) const MIGRATIONS: &[MigrationUnit] = &[];

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
fn stage_marker(tx: &DbTransaction, unit: &MigrationUnit, cursor: &[u8]) -> Result<StagedBytes> {
    let key = Key::Sys(SysKey::Migration).encode();
    let value = value::encode_value(&proto::MigrationValue {
        from_format: unit.from_format,
        to_format: unit.to_format,
        cursor: cursor.to_vec(),
    });
    let mut staged = StagedBytes::default();
    staged.add(key.len(), value.len());
    tx.put(key, value).map_err(Error::from)?;
    Ok(staged)
}

/// The start batch: the marker exists after it, or it does not.
async fn start(db: &Db, unit: &MigrationUnit) -> Result<()> {
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;
    let staged = match stage_marker(&tx, unit, &[]) {
        Ok(staged) => staged,
        Err(error) => {
            tx.rollback();
            return Err(error);
        }
    };
    commit_durable(tx, "migration start", staged)
        .await
        .map_err(Error::from)?;
    Ok(())
}

/// The finish batch: the format flip and the marker clear, together.
async fn finish(db: &Db, unit: &MigrationUnit) -> Result<()> {
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;

    let format = Key::Sys(SysKey::Format).encode();
    let stamp = value::encode_value(&proto::FormatValue {
        format_version: unit.to_format,
        writer_version: env!("CARGO_PKG_VERSION").to_string(),
    });
    let marker = Key::Sys(SysKey::Migration).encode();
    let mut staged = StagedBytes::default();
    staged.add(format.len(), stamp.len());
    staged.add(marker.len(), 0);
    if let Err(error) = tx.put(format, stamp).and_then(|()| tx.delete(marker)) {
        tx.rollback();
        return Err(Error::from(error));
    }

    commit_durable(tx, "migration finish", staged)
        .await
        .map_err(Error::from)?;
    Ok(())
}

/// Walks one unit to completion: start (unless resuming), then step batches
/// until the walk is done, then finish.
async fn run_unit(db: &Db, unit: &MigrationUnit, resume: Option<Vec<u8>>) -> Result<()> {
    let mut cursor = if let Some(cursor) = resume {
        cursor
    } else {
        start(db, unit).await?;
        crash_seam(CrashPoint::AfterStart)?;
        Vec::new()
    };

    loop {
        let tx = db
            .begin(IsolationLevel::Snapshot)
            .await
            .map_err(Error::from)?;

        let outcome = match (unit.step)(&tx, &cursor).await {
            Ok(outcome) => outcome,
            Err(error) => {
                tx.rollback();
                return Err(error);
            }
        };

        let Some(StepProgress {
            cursor: next,
            staged: rewritten,
        }) = outcome
        else {
            tx.rollback();
            break;
        };

        // The cursor advances in the same batch as the rewrite it records.
        let mut staged = match stage_marker(&tx, unit, &next) {
            Ok(staged) => staged,
            Err(error) => {
                tx.rollback();
                return Err(error);
            }
        };
        staged.0 = staged.0.saturating_add(rewritten.0);
        commit_durable(tx, "migration step", staged)
            .await
            .map_err(Error::from)?;
        crash_seam(CrashPoint::AfterStep)?;

        cursor = next;
    }

    crash_seam(CrashPoint::BeforeFinish)?;
    finish(db, unit).await?;
    crash_seam(CrashPoint::AfterFinish)?;

    Ok(())
}

/// Migrates `db` as far as this binary can carry it, resuming an interrupted
/// migration if the marker says one is in flight. A store already at the
/// newest format is left untouched and reported as such. Epoch fencing on
/// the writer means exactly one migrator runs.
pub(crate) async fn run(db: &Db) -> Result<MigrationReport> {
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;
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

    let Plan { units, mut resume } = plan(from_format, marker.as_ref())?;
    let resumed = resume.is_some();

    if units.is_empty() {
        return Ok(MigrationReport {
            from_format,
            to_format: from_format,
            units_run: Vec::new(),
            resumed,
        });
    }

    let mut units_run = Vec::with_capacity(units.len());
    let mut to_format = from_format;

    for unit in units {
        info!(
            unit = unit.name,
            from = unit.from_format,
            to = unit.to_format,
            resuming = resume.is_some(),
            "migrating the store format"
        );

        run_unit(db, unit, resume.take()).await?;

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
