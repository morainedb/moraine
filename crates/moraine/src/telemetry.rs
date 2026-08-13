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
