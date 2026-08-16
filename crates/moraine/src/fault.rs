//! Fault injection: the crash seams the format-migration driver consults
//! between its durable batches, the synthetic units that give that driver
//! something to run, and the enumeration of crash cases the suites drive.
//!
//! The driver calls a seam unconditionally; the arming machinery behind it
//! compiles only under `test` or the `fault-injection` feature, so a
//! production build carries an empty function and no fault surface.

/// A named seam the migration driver consults between durable batches.
/// Reachable outside the crate only under the `fault-injection` feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    /// After the start batch (marker + initial cursor) is durable.
    AfterStart,
    /// After a step batch (data rewrite + cursor advance) is durable.
    AfterStep,
    /// After the last step, before the finish flip.
    BeforeFinish,
    /// After the finish batch (format flip + marker clear) is durable.
    AfterFinish,
}

/// Where a crash can interrupt a state-changing operation. Each variant
/// names what the process was doing when it died; only
/// [`MigrationInterrupted`](Self::MigrationInterrupted) is driven from
/// inside the library, through [`CrashPoint`].
#[cfg(any(test, feature = "fault-injection"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashCase {
    /// Batch staged, its WAL flush never reached object storage.
    CommitNotDurable,
    /// Flush landed; the process died before the caller was told.
    CommitDurableNotAcknowledged,
    /// A drop ending many records at once, crashed at every WAL boundary.
    MultiTombstoneDrop,
    /// Several catalog commits staged into one batch.
    GroupCommit,
    /// A writer died mid-commit and another took the store over.
    TakeoverMidCommit,
    /// The displaced writer woke and tried to commit anyway.
    FencedWriterResumes,
    /// An initializer died partway through creating the catalog.
    GenesisInterrupted,
    /// Two processes created the same empty catalog at once.
    ConcurrentGenesis,
    /// A staged index build died between two of its steps.
    StagedBuildInterrupted,
    /// A batched entry reclamation died between two of its batches.
    ReclamationInterrupted,
    /// A structural format migration died between two of its batches.
    MigrationInterrupted,
}

#[cfg(any(test, feature = "fault-injection"))]
impl CrashCase {
    /// Every case, in the order the suite drives them.
    pub const ALL: [Self; 11] = [
        Self::CommitNotDurable,
        Self::CommitDurableNotAcknowledged,
        Self::MultiTombstoneDrop,
        Self::GroupCommit,
        Self::TakeoverMidCommit,
        Self::FencedWriterResumes,
        Self::GenesisInterrupted,
        Self::ConcurrentGenesis,
        Self::StagedBuildInterrupted,
        Self::ReclamationInterrupted,
        Self::MigrationInterrupted,
    ];

    /// The seams that crash this case from inside the library, empty for a
    /// case crashed from outside the operation.
    #[must_use]
    pub fn seams(self) -> &'static [CrashPoint] {
        match self {
            Self::MigrationInterrupted => &[
                CrashPoint::AfterStart,
                CrashPoint::AfterStep,
                CrashPoint::BeforeFinish,
                CrashPoint::AfterFinish,
            ],
            _ => &[],
        }
    }
}

/// Which synthetic migration units a fault-injection build installs into the
/// driver's registry. The shipped registry is empty (every shipped format is
/// additive), so a unit is what gives the driver something to run.
#[cfg(any(test, feature = "fault-injection"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyntheticMigration {
    /// Nothing installed: the registry is exactly what ships.
    #[default]
    None,
    /// One rewriting unit that moves the catalog's option records from
    /// schema scope to table scope, one record per batch, resumable from its
    /// cursor. It lands the store on the newest format this binary reads.
    MoveOptionScope,
    /// The rewriting unit followed by a second link that walks nothing and
    /// targets a format past the newest this binary reads.
    MoveOptionScopeThenLink,
}

#[cfg(any(test, feature = "fault-injection"))]
mod armed {
    use std::cell::Cell;

    use super::{CrashPoint, SyntheticMigration};
    use crate::{
        error::{Error, Result},
        transaction::migration::{MigrationUnit, synthetic},
    };

    thread_local! {
        static ARMED: Cell<Option<CrashPoint>> = const { Cell::new(None) };
        static INSTALLED: Cell<SyntheticMigration> = const { Cell::new(SyntheticMigration::None) };
    }

    /// Arms (or disarms with `None`) the seam that trips on its next pass.
    /// Thread-local, so tests arming different seams do not collide.
    pub fn inject_crash(point: Option<CrashPoint>) {
        ARMED.with(|armed| armed.set(point));
    }

    /// Installs the synthetic units the migration driver's registry carries.
    /// Thread-local: the migration must be driven from the installing thread,
    /// on a current-thread runtime.
    pub fn install_migration(units: SyntheticMigration) {
        INSTALLED.with(|installed| installed.set(units));
    }

    /// The installed units, for the driver's registry to append.
    pub(crate) fn installed_migrations() -> &'static [&'static MigrationUnit] {
        match INSTALLED.with(Cell::get) {
            SyntheticMigration::None => &[],
            SyntheticMigration::MoveOptionScope => synthetic::REWRITE,
            SyntheticMigration::MoveOptionScopeThenLink => synthetic::REWRITE_THEN_LINK,
        }
    }

    /// Errors the first time it is called at the armed point, then disarms.
    pub(crate) fn crash_seam(point: CrashPoint) -> Result<()> {
        let tripped = ARMED.with(|armed| {
            if armed.get() == Some(point) {
                armed.set(None);
                true
            } else {
                false
            }
        });
        if tripped {
            return Err(Error::Migration(
                "fault-injection: simulated crash at a migration seam".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "fault-injection"))]
pub(crate) use armed::{crash_seam, installed_migrations};
#[cfg(any(test, feature = "fault-injection"))]
pub use armed::{inject_crash, install_migration};

/// The seam with fault injection compiled out; the `Result` keeps call
/// sites identical to the armed build's.
#[cfg(not(any(test, feature = "fault-injection")))]
#[inline]
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn crash_seam(_point: CrashPoint) -> crate::error::Result<()> {
    Ok(())
}

/// The installed-unit list with fault injection compiled out: always empty.
#[cfg(not(any(test, feature = "fault-injection")))]
#[inline]
pub(crate) fn installed_migrations()
-> &'static [&'static crate::transaction::migration::MigrationUnit] {
    &[]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn armed_point_trips_its_seam_once() {
        inject_crash(Some(CrashPoint::AfterStart));
        assert!(crash_seam(CrashPoint::AfterStart).is_err());
        // Disarmed after firing: a second pass proceeds.
        assert!(crash_seam(CrashPoint::AfterStart).is_ok());
    }

    #[test]
    fn unarmed_seam_is_a_noop() {
        inject_crash(None);
        assert!(crash_seam(CrashPoint::AfterFinish).is_ok());
    }

    #[test]
    fn other_points_do_not_trip() {
        inject_crash(Some(CrashPoint::BeforeFinish));
        assert!(crash_seam(CrashPoint::AfterStep).is_ok());
    }

    /// Exactly one case is crashed from inside the library, and it names
    /// every seam the driver has.
    #[test]
    fn one_case_owns_the_driver_seams_and_the_rest_own_none() {
        let seam_driven: Vec<CrashCase> = CrashCase::ALL
            .into_iter()
            .filter(|case| !case.seams().is_empty())
            .collect();
        assert_eq!(seam_driven, vec![CrashCase::MigrationInterrupted]);
        assert_eq!(
            CrashCase::MigrationInterrupted.seams(),
            [
                CrashPoint::AfterStart,
                CrashPoint::AfterStep,
                CrashPoint::BeforeFinish,
                CrashPoint::AfterFinish
            ]
        );
    }

    /// Installing a registry is reversible.
    #[test]
    fn installed_units_replace_and_clear() {
        assert!(installed_migrations().is_empty());
        install_migration(SyntheticMigration::MoveOptionScopeThenLink);
        assert_eq!(installed_migrations().len(), 2);
        install_migration(SyntheticMigration::MoveOptionScope);
        assert_eq!(installed_migrations().len(), 1);
        install_migration(SyntheticMigration::None);
        assert!(installed_migrations().is_empty());
    }
}
