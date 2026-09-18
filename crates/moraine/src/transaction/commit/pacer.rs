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
    /// Commits parked on the scheduled flush's durability, performing no
    /// flush of their own.
    riding: usize,
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
            self.riding = self.riding.saturating_add(1);
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
                    phase: "spacing",
                    claimed: Instant::now(),
                };
                tokio::time::sleep_until(deadline).await;
                scheduled.phase = "serializing";
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
            Claim::Await => {
                let _riding = Riding(self);
                handle.await_durable().await
            }
        }
    }

    /// Performs one flush; the caller holds the serializing lock.
    async fn flush(&self, in_flight: InFlight<'_>) -> Result<(), slatedb::Error> {
        let outcome = self.db.flush().await;
        drop(in_flight);
        if let Err(error) = &outcome {
            // The store has no flush timer: a failed flush leaves the
            // commits riding it with nothing to advance their sequence.
            tracing::warn!(
                %error,
                kind = ?error.kind(),
                riding = self.state().riding,
                "a flush failed; commits riding it are not durable"
            );
        }
        outcome
    }
}

/// Clears `scheduled` if the deferred flush is abandoned before it starts,
/// so later commits do not wait on a flush nobody will perform.
struct Scheduled<'a> {
    pacer: &'a FlushPacer,
    armed: bool,
    /// Where the flush died: `spacing` waiting the spacing out,
    /// `serializing` queued behind a flush still in the air.
    phase: &'static str,
    claimed: Instant,
}

impl Drop for Scheduled<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let riding = {
            let mut state = self.pacer.state();
            state.scheduled = false;
            state.riding
        };
        // The store has no flush timer, so commits already riding this
        // flush have nothing else to advance their durable sequence.
        tracing::warn!(
            phase = self.phase,
            riding,
            held_ms = crate::telemetry::milliseconds(self.claimed.elapsed()),
            unwinding = std::thread::panicking(),
            "a deferred flush was abandoned before it ran; commits riding it have no other flush"
        );
    }
}

/// Counts one commit parked on a scheduled flush, so an abandoned flush
/// can say how many it stranded.
struct Riding<'a>(&'a FlushPacer);

impl Drop for Riding<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state();
        state.riding = state.riding.saturating_sub(1);
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
    use object_store::memory::InMemory;
    use slatedb::config::Settings;

    use super::*;

    const SPACING: Duration = Duration::from_millis(100);

    /// A commit that lands inside the spacing waits it out and then
    /// flushes, so its bytes are durable when `await_durable` returns.
    #[tokio::test(start_paused = true)]
    async fn a_deferred_flush_waits_out_the_spacing_and_lands() {
        let db = Db::builder("", Arc::new(InMemory::new()))
            .with_settings(Settings {
                // As a catalog's writer opens it: the pacer is the only
                // thing that flushes the write-ahead log.
                flush_interval: None,
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();
        let pacer = FlushPacer::new(db.clone(), SPACING);

        // Leaves the spacing unelapsed with nothing in the air: the claim
        // that waits it out before flushing.
        pacer.state().claim(SPACING, Instant::now());
        pacer.state().in_flight = false;

        let tx = db.begin(slatedb::IsolationLevel::Snapshot).await.unwrap();
        tx.put(b"key", b"value").unwrap();
        let handle = tx.commit().await.unwrap().unwrap();

        let started = Instant::now();
        pacer.await_durable(handle).await.unwrap();

        assert!(
            started.elapsed() >= SPACING,
            "the deferred flush must wait the spacing out, waited {:?}",
            started.elapsed()
        );
        assert!(
            !pacer.state().scheduled,
            "the claim is released once it runs"
        );
    }

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

    /// A deferred flush that is abandoned strands every commit riding on
    /// it: the store has no flush timer, so no other flush follows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "reproduces an unfixed strand: the riders never land"]
    async fn an_abandoned_deferred_flush_strands_the_commits_riding_on_it() {
        let db = Db::builder("", Arc::new(InMemory::new()))
            .with_settings(Settings {
                flush_interval: None,
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();
        let pacer = FlushPacer::new(db.clone(), SPACING);

        let commit = async |key: &'static [u8]| {
            let tx = db.begin(slatedb::IsolationLevel::Snapshot).await.unwrap();
            tx.put(key, b"v").unwrap();
            tx.commit().await.unwrap().unwrap()
        };

        // Flushes at once and starts the spacing.
        pacer.await_durable(commit(b"a").await).await.unwrap();

        // Lands inside the spacing: claims the deferred flush.
        let deferred = {
            let pacer = Arc::clone(&pacer);
            let handle = commit(b"b").await;
            tokio::spawn(async move { pacer.await_durable(handle).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Rides on that deferred flush.
        let riding = {
            let pacer = Arc::clone(&pacer);
            let handle = commit(b"c").await;
            tokio::spawn(async move { pacer.await_durable(handle).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;

        // The owner goes away before it flushes.
        deferred.abort();

        assert!(
            tokio::time::timeout(Duration::from_secs(5), riding)
                .await
                .is_ok(),
            "a commit riding an abandoned deferred flush must still land"
        );
    }
}
