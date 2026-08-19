//! Structural format migration: the one-way, crash-resumable rewrite that
//! carries a store from one `sys/format` version to the next.
//!
//! A migration runs as start, steps, finish. The start batch writes the
//! `sys/migration` marker, which makes every reader refuse the store while
//! its keyspace is in motion. Each step batch stages one bounded piece of
//! the rewrite together with the cursor that records it, so a crash resumes
//! from exactly what is durable and never from more. The finish batch flips
//! `sys/format` and clears the marker together, so no reopen can see the new
//! format with the marker still present.
//!
//! Migrations are named `v_n → v_{n+1}` units. A multi-version jump is their
//! composition, each link running its own start, steps, and finish, so there
//! is no bespoke multi-version path to write or test.

use futures::future::BoxFuture;
use slatedb::{Db, DbTransaction, IsolationLevel};
use tracing::info;

use crate::{
    error::{Error, Result},
    fault::{CrashPoint, crash_seam},
    store::{
        handle::ReadHandle,
        key::{Key, SysKey},
        proto, read, value,
    },
    transaction::commit::commit_durable,
};

/// Where a step left off: the cursor the next step resumes at, or `None`
/// when the walk is done and the step staged nothing.
pub(crate) type StepOutcome = Option<Vec<u8>>;

/// A unit's step: stages one bounded batch of its rewrite into `tx`,
/// resuming at `cursor` (empty at the start of the walk).
///
/// A step must write the new-format keys it produces **before** deleting the
/// old-format keys they supersede, and must be idempotent — re-running it at
/// or before the cursor converges rather than duplicating. The driver commits
/// the batch and advances the durable cursor; the step commits nothing
/// itself.
type StepFn = for<'a> fn(&'a DbTransaction, &'a [u8]) -> BoxFuture<'a, Result<StepOutcome>>;

/// One `v_n → v_{n+1}` structural rewrite: a named unit with its own step
/// logic and its own tests, composed with its neighbours for a longer jump.
pub(crate) struct MigrationUnit {
    /// Stable name, carried into logs and into the report.
    pub(crate) name: &'static str,
    /// The format this unit reads.
    pub(crate) from_format: u64,
    /// The format it writes.
    pub(crate) to_format: u64,
    /// Rewrites one bounded batch of the unit's work.
    pub(crate) step: StepFn,
}

/// Every structural migration this binary ships, in ascending order.
///
/// Empty, and correctly so: every format to date is additive — it adds a
/// subspace without moving a key that already exists — so the lazy stamp
/// carries it and there is no keyspace to rewrite. The first format that
/// moves an existing key adds the first entry here, and inherits the
/// start/step/finish protocol below already built and tested.
pub(crate) const MIGRATIONS: &[MigrationUnit] = &[];

/// The units a call plans over: everything this binary ships, plus whatever
/// a fault-injection build installed.
///
/// One seam, consulted by the planner itself, so a test with a unit to drive
/// exercises the shipped planner rather than a parallel copy of it. A
/// production build's `installed_migrations` is the empty slice, so this is
/// [`MIGRATIONS`] and nothing else.
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
    /// applied, rather than starting from a clean store.
    pub resumed: bool,
}

/// The units a call must run, and where the first of them resumes.
struct Plan {
    units: Vec<&'static MigrationUnit>,
    /// The durable cursor of an interrupted unit, when resuming one.
    resume: Option<Vec<u8>>,
}

/// The unit that reads `format`, if this binary carries one.
fn unit_from(format: u64) -> Option<&'static MigrationUnit> {
    registry()
        .into_iter()
        .find(|unit| unit.from_format == format)
}

/// The chain of units carrying `format` as far as this binary can take it.
/// Stops at the first version no unit reads, which is the newest this binary
/// migrates to.
fn chain_from(format: u64) -> Vec<&'static MigrationUnit> {
    let mut units = Vec::new();
    let mut format = format;
    while let Some(unit) = unit_from(format) {
        units.push(unit);
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

    // The finish batch flips the format and clears the marker together, so a
    // marker naming any other source format is torn state no step of the
    // protocol can produce.
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
fn stage_marker(tx: &DbTransaction, unit: &MigrationUnit, cursor: &[u8]) -> Result<()> {
    tx.put(
        Key::Sys(SysKey::Migration).encode(),
        value::encode_value(&proto::MigrationValue {
            from_format: unit.from_format,
            to_format: unit.to_format,
            cursor: cursor.to_vec(),
        }),
    )
    .map_err(Error::from)
}

/// The start batch: the marker exists after it, or it does not.
async fn start(db: &Db, unit: &MigrationUnit) -> Result<()> {
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;
    if let Err(error) = stage_marker(&tx, unit, &[]) {
        tx.rollback();
        return Err(error);
    }
    commit_durable(db, tx, "migration start")
        .await
        .map_err(Error::from)?;
    Ok(())
}

/// The finish batch: the format flip and the marker clear, together. Until
/// it lands the store is coherently old; after it lands, coherently new.
async fn finish(db: &Db, unit: &MigrationUnit) -> Result<()> {
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;

    let staged = tx
        .put(
            Key::Sys(SysKey::Format).encode(),
            value::encode_value(&proto::FormatValue {
                format_version: unit.to_format,
                writer_version: env!("CARGO_PKG_VERSION").to_string(),
            }),
        )
        .and_then(|()| tx.delete(Key::Sys(SysKey::Migration).encode()));
    if let Err(error) = staged {
        tx.rollback();
        return Err(Error::from(error));
    }

    commit_durable(db, tx, "migration finish")
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

        let Some(next) = outcome else {
            tx.rollback();
            break;
        };

        // The cursor advances in the same batch as the rewrite it records,
        // so it never claims more progress than is durable.
        if let Err(error) = stage_marker(&tx, unit, &next) {
            tx.rollback();
            return Err(error);
        }
        commit_durable(db, tx, "migration step")
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
/// newest format this binary knows is left untouched and reported as such.
///
/// The caller owns the writer: SlateDB's epoch fencing means exactly one
/// migrator runs, by the same guarantee that makes a second catalog writer
/// safe.
pub(crate) async fn run(db: &Db) -> Result<MigrationReport> {
    let tx = db
        .begin(IsolationLevel::Snapshot)
        .await
        .map_err(Error::from)?;
    let format = read::read_format(ReadHandle::Tx(&tx)).await;
    let marker = read::read_migration(ReadHandle::Tx(&tx)).await;
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
