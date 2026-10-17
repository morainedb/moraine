//! Paces write-ahead-log flushes: at most one object-store PUT per spacing,
//! and no commit waiting longer than the spacing for its bytes to land.

use std::{
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use slatedb::{Db, WriteHandle};
use tokio::time::Instant;

/// One writer's flush pacing. A commit whose spacing has elapsed since the
/// last flush flushes at once. One behind flushes that carried no other
/// commit waits only the floor, a tenth of the spacing: a writer without
/// siblings pays no spacing, and still one flush per commit. One inside
/// the spacing behind a flush that carried riders joins a single flush
/// deferred to the moment it elapses, or to the moment as many commits
/// have joined it as the most a recent flush carried, whichever comes
/// first, and never before the floor: the known population has arrived,
/// and waiting longer coalesces nothing.
pub(crate) struct FlushPacer {
    db: Db,
    spacing: Duration,
    state: Mutex<PacerState>,
    /// Serializes the flushes themselves, so a deferred one never overlaps
    /// an immediate one still in the air.
    flushes: tokio::sync::Mutex<()>,
    /// Bumped when a scheduled flush ends without carrying its riders out.
    /// The store has no flush timer, so nothing else would ever wake them.
    stranded: tokio::sync::watch::Sender<u64>,
    /// Wakes the scheduled flush before its deadline once the population
    /// the last flush carried has joined it.
    population: tokio::sync::Notify,
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
    /// flush of their own: the members of every batch riding, since a
    /// batch is one handle carrying many commits.
    riding: usize,
    /// The commits of the batch that scheduled the pending flush.
    scheduler: usize,
    /// How many commits each of the last few flushes carried. The most of
    /// them is the population mark: a flush that closed early carried
    /// fewer than there are, so one flush cannot lower it, and one means
    /// the writer has no sibling.
    carried: [usize; RECENT_FLUSHES],
    /// The slot the next flush's count goes in.
    next_carried: usize,
    /// When a flush last waited the whole spacing out, which is the only
    /// kind that sees the whole population.
    last_measured: Option<Instant>,
}

/// How many flushes the population mark is taken over.
const RECENT_FLUSHES: usize = 3;

/// The least time between two flushes' starts, as a fraction of the
/// spacing: the rate cap a lone writer or an early close still honours.
const FLOOR_FRACTION: u32 = 10;

/// How many spacings a measurement stays good for. A flush that waits
/// the spacing out carries every writer and so measures the population;
/// early closes carry only the mark and measure nothing, so without a
/// fresh measurement the mark could never grow.
const MEASUREMENT_SPACINGS: u32 = 10;

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
    /// What a batch of `commits` does for its durability.
    fn claim(&mut self, spacing: Duration, now: Instant, commits: usize) -> Claim {
        if self.scheduled {
            self.riding = self.riding.saturating_add(commits);
            return Claim::Await;
        }
        // Alone, the horizon is the floor; among siblings, the spacing.
        let horizon = if self.mark() <= 1 {
            spacing / FLOOR_FRACTION
        } else {
            spacing
        };
        let due = self.last_started.map_or(now, |started| started + horizon);
        if !self.in_flight && due <= now {
            // A whole spacing passed with nobody: that is a measurement.
            // The first flush ever passed nothing.
            if horizon == spacing && self.last_started.is_some() {
                self.last_measured = Some(now);
            }
            self.last_started = Some(now);
            self.in_flight = true;
            self.record_carried(commits);
            return Claim::FlushNow;
        }
        self.scheduled = true;
        self.scheduler = commits;
        Claim::FlushAt(due.max(now))
    }

    /// The earliest the next flush may start.
    fn floor(&self, spacing: Duration) -> Option<Instant> {
        self.last_started
            .map(|started| started + spacing / FLOOR_FRACTION)
    }

    /// The population mark: the most a recent flush carried.
    fn mark(&self) -> usize {
        self.carried.iter().copied().max().unwrap_or(0)
    }

    fn record_carried(&mut self, count: usize) {
        self.carried[self.next_carried] = count;
        self.next_carried = (self.next_carried + 1) % RECENT_FLUSHES;
    }

    /// Whether the scheduled flush already carries the population: the
    /// scheduler plus its riders against a mark measured recently enough
    /// to trust.
    fn population_reached(&self, now: Instant, spacing: Duration) -> bool {
        let measured = self
            .last_measured
            .is_some_and(|at| now.duration_since(at) <= spacing * MEASUREMENT_SPACINGS);
        measured && self.riding.saturating_add(self.scheduler) >= self.mark()
    }

    /// The scheduled flush starts, carrying the commit that scheduled it
    /// and every rider parked on it. One that waited its whole deadline
    /// out saw every writer, and is the measurement the mark rests on.
    fn start_scheduled(&mut self, now: Instant, waited_out: bool) {
        self.scheduled = false;
        self.in_flight = true;
        self.last_started = Some(now);
        self.record_carried(self.riding.saturating_add(self.scheduler));
        self.scheduler = 0;
        if waited_out {
            self.last_measured = Some(now);
        }
    }
}

impl FlushPacer {
    pub(crate) fn new(db: Db, spacing: Duration) -> Arc<Self> {
        Arc::new(Self {
            db,
            spacing,
            state: Mutex::new(PacerState::default()),
            flushes: tokio::sync::Mutex::new(()),
            stranded: tokio::sync::watch::Sender::new(0),
            population: tokio::sync::Notify::new(),
        })
    }

    fn state(&self) -> MutexGuard<'_, PacerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits until `handle`'s already-committed batch is in object storage,
    /// flushing now, later, or riding the flush already scheduled.
    ///
    /// A rider whose flush is abandoned claims again rather than waiting on
    /// one that will not run.
    pub(crate) async fn await_durable(
        &self,
        handle: WriteHandle,
        commits: usize,
    ) -> Result<(), slatedb::Error> {
        loop {
            // Subscribed before the claim, so a flush stranded between the
            // two is seen as a change rather than missed.
            let mut stranded = self.stranded.subscribe();
            // Bound, not matched on directly: the guard would otherwise be
            // held across the awaits below it.
            let claim = self.state().claim(self.spacing, Instant::now(), commits);
            match claim {
                Claim::FlushNow => {
                    let in_flight = InFlight(self);
                    let _serialized = self.flushes.lock().await;
                    return self.flush(in_flight).await;
                }
                Claim::FlushAt(deadline) => {
                    let mut scheduled = Scheduled {
                        pacer: self,
                        armed: true,
                        phase: "spacing",
                        claimed: Instant::now(),
                    };
                    // The floor first, which no early close crosses; then
                    // the spacing, or the population arriving. The waiter
                    // is registered before the population is checked, so a
                    // rider's wake between the two is caught rather than
                    // lost; and only a registered waiter is woken, so a wake
                    // from an earlier window cannot fire this one.
                    let floor = self.state().floor(self.spacing);
                    if let Some(floor) = floor {
                        tokio::time::sleep_until(floor.min(deadline)).await;
                    }
                    let population = self.population.notified();
                    tokio::pin!(population);
                    population.as_mut().enable();
                    let mut waited_out = true;
                    if self
                        .state()
                        .population_reached(Instant::now(), self.spacing)
                    {
                        waited_out = false;
                    } else {
                        tokio::select! {
                            () = tokio::time::sleep_until(deadline) => {}
                            () = &mut population => waited_out = false,
                        }
                    }
                    scheduled.phase = "serializing";
                    let _serialized = self.flushes.lock().await;
                    let in_flight = {
                        self.state().start_scheduled(Instant::now(), waited_out);
                        InFlight(self)
                    };
                    scheduled.armed = false;
                    return self.flush(in_flight).await;
                }
                Claim::Await => {
                    let _riding = Riding(self, commits);
                    if self
                        .state()
                        .population_reached(Instant::now(), self.spacing)
                    {
                        self.population.notify_waiters();
                    }
                    tokio::select! {
                        durable = handle.await_durable() => return durable,
                        // Nothing is going to carry these bytes out now, so
                        // claim again rather than wait on a flush that left.
                        _ = stranded.changed() => {}
                    }
                }
            }
        }
    }

    /// Wakes every commit riding a scheduled flush that will not carry
    /// them out, so each claims again instead of waiting on it.
    fn strand_riders(&self) {
        self.stranded.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }

    /// Performs one flush; the caller holds the serializing lock.
    async fn flush(&self, in_flight: InFlight<'_>) -> Result<(), slatedb::Error> {
        {
            let state = self.state();
            tracing::debug!(
                carried = state.carried[(state.next_carried + RECENT_FLUSHES - 1) % RECENT_FLUSHES],
                mark = state.mark(),
                riding = state.riding,
                measured = state.last_measured.is_some(),
                scheduled = state.scheduled,
                "flush starting"
            );
        }
        let outcome = self.db.flush().await;
        drop(in_flight);
        if let Err(error) = &outcome {
            self.strand_riders();
            tracing::warn!(
                %error,
                kind = ?error.kind(),
                riding = self.state().riding,
                "a flush failed; its riders claim again"
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
        self.pacer.strand_riders();
        tracing::warn!(
            phase = self.phase,
            riding,
            held_ms = crate::telemetry::milliseconds(self.claimed.elapsed()),
            unwinding = std::thread::panicking(),
            "a deferred flush was abandoned before it ran; its riders claim again"
        );
    }
}

/// Counts one commit parked on a scheduled flush, so an abandoned flush
/// can say how many it stranded.
struct Riding<'a>(&'a FlushPacer, usize);

impl Drop for Riding<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state();
        state.riding = state.riding.saturating_sub(self.1);
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
        pacer.state().claim(SPACING, Instant::now(), 1);
        pacer.state().in_flight = false;
        // Behind a flush that carried a sibling: the claim that waits.
        pacer.state().record_carried(2);

        let tx = db.begin(slatedb::IsolationLevel::Snapshot).unwrap();
        tx.put(b"key", b"value").unwrap();
        let handle = tx.commit().await.unwrap().unwrap();

        let started = Instant::now();
        pacer.await_durable(handle, 1).await.unwrap();

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

        assert_eq!(state.claim(SPACING, now, 1), Claim::FlushNow);
        assert!(state.in_flight);
        assert_eq!(state.last_started, Some(now));
    }

    /// Nothing rode the last flush, so nobody is coming: waiting the
    /// spacing out would buy no coalescing.
    #[tokio::test(start_paused = true)]
    async fn a_lone_writers_commit_inside_the_spacing_waits_only_the_floor() {
        let mut state = PacerState::default();
        let base = Instant::now();
        assert_eq!(state.claim(SPACING, base, 1), Claim::FlushNow);
        state.in_flight = false;
        assert_eq!(
            state.claim(SPACING, at(base, 5), 1),
            Claim::FlushAt(at(base, 10)),
            "inside the floor: the flush waits for it"
        );
        state.start_scheduled(at(base, 10), false);
        state.in_flight = false;
        assert_eq!(
            state.claim(SPACING, at(base, 20), 1),
            Claim::FlushNow,
            "past the floor: at once"
        );
        state.in_flight = false;
        assert_eq!(state.claim(SPACING, at(base, 30), 1), Claim::FlushNow);
    }

    /// A flush that carried riders means siblings: the next commit keeps
    /// deferring into the spacing, where they coalesce.
    #[tokio::test(start_paused = true)]
    async fn a_commit_behind_a_flush_that_carried_riders_waits_the_spacing_out() {
        let mut state = PacerState::default();
        let base = Instant::now();
        assert_eq!(state.claim(SPACING, base, 1), Claim::FlushNow);
        // A sibling lands while that flush is in the air and schedules the
        // next, at the floor since the writer was alone until now; the
        // first writer's next commit rides it.
        //
        // next; the first writer's next commit rides it.
        assert_eq!(
            state.claim(SPACING, at(base, 5), 1),
            Claim::FlushAt(at(base, 10))
        );
        state.in_flight = false;
        assert_eq!(state.claim(SPACING, at(base, 20), 1), Claim::Await);
        state.start_scheduled(at(base, 100), true);
        assert_eq!(state.mark(), 2);
        state.in_flight = false;
        state.riding = 0;
        assert_eq!(
            state.claim(SPACING, at(base, 110), 1),
            Claim::FlushAt(at(base, 200)),
            "two writers are coalescing; the spacing stays"
        );
    }

    /// As many commits have joined the deferred flush as the last one
    /// carried: it closes there, well before the spacing.
    #[tokio::test(start_paused = true)]
    async fn a_deferred_flush_closes_once_the_last_flushs_population_has_joined() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.claim(SPACING, base, 1);
        state.in_flight = false;
        state.record_carried(3);
        state.last_measured = Some(base);
        assert_eq!(
            state.claim(SPACING, at(base, 10), 1),
            Claim::FlushAt(at(base, 100))
        );
        assert!(
            !state.population_reached(at(base, 10), SPACING),
            "one of three"
        );
        assert_eq!(state.claim(SPACING, at(base, 12), 1), Claim::Await);
        assert!(
            !state.population_reached(at(base, 12), SPACING),
            "two of three"
        );
        assert_eq!(state.claim(SPACING, at(base, 14), 1), Claim::Await);
        assert!(
            state.population_reached(at(base, 14), SPACING),
            "three of three"
        );
    }

    /// Driven end to end: the second of two writers joining lands the
    /// flush at once instead of at the spacing.
    #[tokio::test(start_paused = true)]
    async fn the_populations_arrival_lands_the_deferred_flush_early() {
        let db = Db::builder("", Arc::new(InMemory::new()))
            .with_settings(Settings {
                flush_interval: None,
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();
        let pacer = FlushPacer::new(db.clone(), SPACING);
        pacer.state().claim(SPACING, Instant::now(), 1);
        pacer.state().in_flight = false;
        pacer.state().record_carried(2);
        pacer.state().last_measured = Some(Instant::now());

        let handles = {
            let mut handles = Vec::new();
            for key in [b"a", b"b"] {
                let tx = db.begin(slatedb::IsolationLevel::Snapshot).unwrap();
                tx.put(key, b"value").unwrap();
                handles.push(tx.commit().await.unwrap().unwrap());
            }
            handles
        };
        let started = Instant::now();
        let [first, second]: [_; 2] = handles.try_into().ok().unwrap();
        let (scheduler, rider) = tokio::join!(pacer.await_durable(first, 1), async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            pacer.await_durable(second, 1).await
        });
        scheduler.unwrap();
        rider.unwrap();
        assert!(
            started.elapsed() < SPACING / 2,
            "two is the population; the flush must not wait the spacing out: {:?}",
            started.elapsed()
        );
    }

    /// A flush that closed early carried fewer than there are; the mark
    /// is the most a recent flush carried, so it survives that flush and
    /// the lone-writer rule stays off while siblings were seen.
    #[tokio::test(start_paused = true)]
    async fn an_early_close_does_not_lower_the_population_mark() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.record_carried(8);
        state.last_measured = Some(base);
        state.claim(SPACING, base, 1);
        state.in_flight = false;
        // Closed early with three aboard.
        state.riding = 2;
        state.start_scheduled(at(base, 20), false);
        state.in_flight = false;
        state.riding = 0;
        assert_eq!(state.mark(), 8);
        assert_eq!(
            state.claim(SPACING, at(base, 30), 1),
            Claim::FlushAt(at(base, 120)),
            "eight were seen; three aboard is not alone"
        );
        state.riding = 6;
        assert!(
            !state.population_reached(at(base, 40), SPACING),
            "seven of eight"
        );
        state.riding = 7;
        assert!(state.population_reached(at(base, 40), SPACING));
    }

    /// Only flushes that waited the spacing out and still carried less
    /// lower the mark, a few of them in a row.
    #[tokio::test(start_paused = true)]
    async fn the_mark_falls_once_the_population_has_stayed_away() {
        let mut state = PacerState::default();
        state.record_carried(8);
        for _ in 0..RECENT_FLUSHES {
            state.record_carried(1);
        }
        assert_eq!(state.mark(), 1, "the writer is alone again");
    }

    /// Without a recent measurement the mark is not trusted: the flush
    /// waits the spacing out and carries every writer, which is the
    /// measurement.
    #[tokio::test(start_paused = true)]
    async fn an_unmeasured_population_waits_the_spacing_out() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.record_carried(3);
        state.claim(SPACING, base, 1);
        state.in_flight = false;
        state.claim(SPACING, at(base, 10), 1);
        state.riding = 5;
        assert!(
            !state.population_reached(at(base, 20), SPACING),
            "more than the mark aboard, but nothing says the mark is right"
        );
        state.start_scheduled(at(base, 100), true);
        assert_eq!(state.mark(), 6);
        state.in_flight = false;
        state.riding = 0;
        state.claim(SPACING, at(base, 110), 1);
        state.riding = 5;
        assert!(
            state.population_reached(at(base, 120), SPACING),
            "measured: six are the population"
        );
        assert!(
            !state.population_reached(at(base, 100 + 100 * 11), SPACING),
            "a measurement goes stale"
        );
    }

    /// A batch is one handle carrying many commits; each of them counts
    /// toward the population, since that is what the mark counts.
    #[tokio::test(start_paused = true)]
    async fn a_batch_counts_each_member_toward_the_population() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.record_carried(16);
        state.last_measured = Some(base);
        state.claim(SPACING, base, 1);
        state.in_flight = false;
        assert_eq!(
            state.claim(SPACING, at(base, 10), 10),
            Claim::FlushAt(at(base, 100))
        );
        assert!(
            !state.population_reached(at(base, 10), SPACING),
            "ten of sixteen"
        );
        assert_eq!(state.claim(SPACING, at(base, 12), 6), Claim::Await);
        assert!(
            state.population_reached(at(base, 12), SPACING),
            "sixteen of sixteen"
        );
        state.start_scheduled(at(base, 12), false);
        assert_eq!(state.mark(), 16);
    }

    #[tokio::test(start_paused = true)]
    async fn a_commit_after_the_spacing_flushes_at_once() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.claim(SPACING, base, 1);
        state.in_flight = false;

        assert_eq!(state.claim(SPACING, at(base, 100), 1), Claim::FlushNow);
    }

    #[tokio::test(start_paused = true)]
    async fn a_commit_inside_the_spacing_schedules_the_flush_for_when_it_elapses() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.claim(SPACING, base, 1);
        state.in_flight = false;
        // The flush carried a sibling: the spacing is worth waiting out.
        state.record_carried(2);

        assert_eq!(
            state.claim(SPACING, at(base, 30), 1),
            Claim::FlushAt(at(base, 100))
        );
        assert!(state.scheduled);
    }

    #[tokio::test(start_paused = true)]
    async fn commits_behind_a_scheduled_flush_ride_on_it() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.claim(SPACING, base, 1);
        state.in_flight = false;
        state.record_carried(2);
        state.claim(SPACING, at(base, 30), 1);

        assert_eq!(state.claim(SPACING, at(base, 60), 1), Claim::Await);
        assert_eq!(state.claim(SPACING, at(base, 150), 1), Claim::Await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_commit_during_a_flush_in_the_air_schedules_the_next_slot() {
        let mut state = PacerState::default();
        let base = Instant::now();
        state.claim(SPACING, base, 1);

        // Still in flight past the spacing: the next flush follows it
        // rather than overlapping it.
        assert_eq!(
            state.claim(SPACING, at(base, 120), 1),
            Claim::FlushAt(at(base, 120))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn zero_spacing_flushes_every_commit_that_finds_no_flush_in_the_air() {
        let mut state = PacerState::default();
        let base = Instant::now();

        assert_eq!(state.claim(Duration::ZERO, base, 1), Claim::FlushNow);
        state.in_flight = false;
        assert_eq!(state.claim(Duration::ZERO, base, 1), Claim::FlushNow);
        assert_eq!(state.claim(Duration::ZERO, base, 1), Claim::FlushAt(base));
    }

    /// A commit riding a deferred flush still lands when that flush is
    /// abandoned: nothing else would carry its bytes out, so it claims
    /// again rather than waiting on one that left.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_rider_lands_when_the_flush_it_rides_is_abandoned() {
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
            let tx = db.begin(slatedb::IsolationLevel::Snapshot).unwrap();
            tx.put(key, b"v").unwrap();
            tx.commit().await.unwrap().unwrap()
        };

        // Flushes at once and starts the spacing.
        pacer.await_durable(commit(b"a").await, 1).await.unwrap();

        // Lands inside the spacing: claims the deferred flush.
        let deferred = {
            let pacer = Arc::clone(&pacer);
            let handle = commit(b"b").await;
            tokio::spawn(async move { pacer.await_durable(handle, 1).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Rides on that deferred flush.
        let riding = {
            let pacer = Arc::clone(&pacer);
            let handle = commit(b"c").await;
            tokio::spawn(async move { pacer.await_durable(handle, 1).await })
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
