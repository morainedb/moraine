//! Paces write-ahead-log flushes: at most one object-store PUT per spacing,
//! and no commit waiting longer than the spacing for its bytes to land.

use std::{
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use slatedb::{Db, WriteHandle};
use tokio::time::Instant;

/// One writer's flush pacing. A commit whose spacing has elapsed since the
/// last flush flushes at once; one inside the spacing joins a single flush
/// deferred to the moment it elapses.
pub(crate) struct FlushPacer {
    db: Db,
    spacing: Duration,
    state: Mutex<PacerState>,
    /// Serializes the flushes themselves, so a deferred one never overlaps
    /// an immediate one still in the air.
    flushes: tokio::sync::Mutex<()>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PacerState {
    last_started: Option<Instant>,
    /// A deferred flush is waiting for its moment; commits until then ride
    /// on it.
    scheduled: bool,
    /// A flush is in the air; a commit landing now is not in it.
    in_flight: bool,
}

/// What a commit does for its durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Claim {
    /// Flush at once: nothing flushed within the spacing.
    FlushNow,
    /// Flush at this instant, carrying every commit until then.
    FlushAt(Instant),
    /// A flush already scheduled carries this commit.
    Await,
}

impl PacerState {
    fn claim(&mut self, spacing: Duration, now: Instant) -> Claim {
        if self.scheduled {
            return Claim::Await;
        }
        let due = self.last_started.map_or(now, |started| started + spacing);
        if !self.in_flight && due <= now {
            self.last_started = Some(now);
            self.in_flight = true;
            return Claim::FlushNow;
        }
        self.scheduled = true;
        Claim::FlushAt(due.max(now))
    }
}

impl FlushPacer {
    pub(crate) fn new(db: Db, spacing: Duration) -> Arc<Self> {
        Arc::new(Self {
            db,
            spacing,
            state: Mutex::new(PacerState::default()),
            flushes: tokio::sync::Mutex::new(()),
        })
    }

    fn state(&self) -> MutexGuard<'_, PacerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits until `handle`'s already-committed batch is in object storage,
    /// flushing now, later, or not at all per the pacing.
    pub(crate) async fn await_durable(&self, handle: WriteHandle) -> Result<(), slatedb::Error> {
        let claim = self.state().claim(self.spacing, Instant::now());
        match claim {
            Claim::FlushNow => {
                let in_flight = InFlight(self);
                let _serialized = self.flushes.lock().await;
                self.flush(in_flight).await
            }
            Claim::FlushAt(deadline) => {
                let mut scheduled = Scheduled {
                    pacer: self,
                    armed: true,
                };
                tokio::time::sleep_until(deadline).await;
                let _serialized = self.flushes.lock().await;
                let in_flight = {
                    let mut state = self.state();
                    state.scheduled = false;
                    state.in_flight = true;
                    state.last_started = Some(Instant::now());
                    InFlight(self)
                };
                scheduled.armed = false;
                self.flush(in_flight).await
            }
            Claim::Await => handle.await_durable().await,
        }
    }

    /// Performs one flush; the caller holds the serializing lock.
    async fn flush(&self, in_flight: InFlight<'_>) -> Result<(), slatedb::Error> {
        let outcome = self.db.flush().await;
        drop(in_flight);
        outcome
    }
}

/// Clears `scheduled` if the deferred flush is abandoned before it starts,
/// so later commits do not wait on a flush nobody will perform.
struct Scheduled<'a> {
    pacer: &'a FlushPacer,
    armed: bool,
}

impl Drop for Scheduled<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.pacer.state().scheduled = false;
        }
    }
}

/// Clears `in_flight` once a flush ends, however it ends.
struct InFlight<'a>(&'a FlushPacer);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.state().in_flight = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPACING: Duration = Duration::from_millis(100);

    fn at(base: Instant, millis: u64) -> Instant {
        base + Duration::from_millis(millis)
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_commit_flushes_at_once() {
        let mut state = PacerState::default();
        let now = Instant::now();

        assert_eq!(state.claim(SPACING, now), Claim::FlushNow);
        assert!(state.in_flight);
        assert_eq!(state.last_started, Some(now));
    }

    #[tokio::test(start_paused = true)]
    async fn a_commit_after_the_spacing_flushes_at_once() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.claim(SPACING, base);
        state.in_flight = false;

        assert_eq!(state.claim(SPACING, at(base, 100)), Claim::FlushNow);
    }

    #[tokio::test(start_paused = true)]
    async fn a_commit_inside_the_spacing_schedules_the_flush_for_when_it_elapses() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.claim(SPACING, base);
        state.in_flight = false;

        assert_eq!(
            state.claim(SPACING, at(base, 30)),
            Claim::FlushAt(at(base, 100))
        );
        assert!(state.scheduled);
    }

    #[tokio::test(start_paused = true)]
    async fn commits_behind_a_scheduled_flush_ride_on_it() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.claim(SPACING, base);
        state.in_flight = false;
        state.claim(SPACING, at(base, 30));

        assert_eq!(state.claim(SPACING, at(base, 60)), Claim::Await);
        assert_eq!(state.claim(SPACING, at(base, 150)), Claim::Await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_commit_during_a_flush_in_the_air_schedules_the_next_slot() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.claim(SPACING, base);

        // Still in flight past the spacing: the next flush follows it
        // rather than overlapping it.
        assert_eq!(
            state.claim(SPACING, at(base, 120)),
            Claim::FlushAt(at(base, 120))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn zero_spacing_flushes_every_commit_that_finds_no_flush_in_the_air() {
        let mut state = PacerState::default();
        let base = Instant::now();

        assert_eq!(state.claim(Duration::ZERO, base), Claim::FlushNow);
        state.in_flight = false;
        assert_eq!(state.claim(Duration::ZERO, base), Claim::FlushNow);
        assert_eq!(state.claim(Duration::ZERO, base), Claim::FlushAt(base));
    }
}
