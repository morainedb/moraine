use std::{future::Future, time::Duration};

use tracing::warn;

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
