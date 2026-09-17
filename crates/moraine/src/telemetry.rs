use std::{future::Future, time::Duration};

use tracing::warn;

/// Timer ticks any attached runtime has completed since the process began.
///
/// A heartbeat that reports its own ticks cannot say whether a gap means the
/// timer stopped or the record was lost, because the report travels the path
/// under suspicion. This counter rides records that are known to arrive
/// instead, so the two answers separate.
pub(crate) static RUNTIME_TICKS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Records one completed timer tick on some attached runtime.
pub fn note_runtime_tick() {
    RUNTIME_TICKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Timer ticks completed process-wide, for a record that is known to reach
/// its reader.
pub(crate) fn runtime_ticks() -> u64 {
    RUNTIME_TICKS.load(std::sync::atomic::Ordering::Relaxed)
}

/// How long a wait runs before it is worth reporting, and how often it is
/// reported thereafter.
pub(crate) const STALL_INTERVAL: Duration = Duration::from_secs(10);

/// Awaits `work`, naming `phase` in the log every [`STALL_INTERVAL`] the
/// wait runs long, and never giving up on it.
///
/// A task parked mid-phase cannot be read from a thread dump: a parked
/// `block_on` leaves its future's state machine on the heap, so the await
/// it is suspended at is absent from every thread's stack. These records
/// are the only thing that names the phase, so a stalled caller says which
/// one it is stuck in rather than only that it is stuck.
pub(crate) async fn reporting_phase<T>(phase: &'static str, work: impl Future<Output = T>) -> T {
    let mut work = Box::pin(work);
    let mut waited = Duration::ZERO;
    loop {
        if let Ok(done) = tokio::time::timeout(STALL_INTERVAL, &mut work).await {
            return done;
        }
        waited = waited.saturating_add(STALL_INTERVAL);
        warn!(
            phase,
            waited_seconds = waited.as_secs(),
            "a caller is still waiting in this phase"
        );
    }
}

/// Converts a duration for a saturating nanosecond accumulator.
pub(crate) fn nanoseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Rounds a measured duration to the nearest millisecond for telemetry.
pub(crate) fn milliseconds(duration: Duration) -> u64 {
    let rounded = duration.as_nanos().saturating_add(500_000) / 1_000_000;
    u64::try_from(rounded).unwrap_or(u64::MAX)
}

/// The level of every event `emit` records, in order.
#[cfg(test)]
pub(crate) fn recorded_levels(emit: impl FnOnce()) -> Vec<tracing::Level> {
    use std::sync::{Arc, Mutex};

    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::{Layer, layer::SubscriberExt};

    #[derive(Clone, Default)]
    struct Levels(Arc<Mutex<Vec<Level>>>);

    impl<S: Subscriber> Layer<S> for Levels {
        fn on_event(&self, event: &Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            self.0.lock().unwrap().push(*event.metadata().level());
        }
    }

    let levels = Levels::default();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(levels.clone()), emit);
    levels.0.lock().unwrap().clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reported phase outlives its own reports: the reporter names a slow
    /// wait, it never cuts one short.
    #[tokio::test(start_paused = true)]
    async fn a_reported_phase_is_never_abandoned() {
        let slow = async {
            tokio::time::sleep(STALL_INTERVAL * 4).await;
            "landed"
        };
        assert_eq!(reporting_phase("test", slow).await, "landed");
    }

    #[test]
    fn milliseconds_rounds_to_the_nearest_integer() {
        assert_eq!(milliseconds(Duration::from_nanos(499_999)), 0);
        assert_eq!(milliseconds(Duration::from_micros(500)), 1);
        assert_eq!(milliseconds(Duration::from_nanos(1_499_999)), 1);
        assert_eq!(milliseconds(Duration::from_micros(1_500)), 2);
    }

    #[test]
    fn nanoseconds_saturates_at_the_counter_width() {
        assert_eq!(nanoseconds(Duration::from_nanos(42)), 42);
        assert_eq!(nanoseconds(Duration::MAX), u64::MAX);
    }
}

#[cfg(test)]
mod block_on_tests {
    use super::*;

    /// The shape the extension runs: a phase awaited inside `block_on` on a
    /// multi-threaded runtime, where the calling thread parks and a worker
    /// must drive the timer. A phase that never reports here reports nowhere
    /// in production.
    #[test]
    fn a_phase_reports_from_inside_block_on() {
        use tracing_subscriber::layer::SubscriberExt;

        struct Count(std::sync::Arc<std::sync::atomic::AtomicUsize>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Count {
            fn on_event(
                &self,
                _: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let fired = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = std::sync::Arc::clone(&fired);

        let subscriber = tracing_subscriber::registry().with(Count(counter));
        tracing::subscriber::with_default(subscriber, || {
            runtime.block_on(async {
                let slow = tokio::time::sleep(STALL_INTERVAL + Duration::from_millis(500));
                reporting_phase("block-on", slow).await;
            });
        });

        assert!(
            fired.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "a phase held past the interval inside block_on must report"
        );
    }
}
