use std::time::Duration;

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
