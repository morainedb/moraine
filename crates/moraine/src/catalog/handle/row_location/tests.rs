use super::*;

/// A resolution's record rises to INFO once it crosses the slow threshold, so
/// a host running at the default level sees the ones worth explaining.
#[test]
fn a_slow_resolution_records_above_the_debug_level() {
    let levels = crate::telemetry::recorded_levels(|| {
        for locate in [
            SLOW_RESOLVE.saturating_sub(Duration::from_millis(1)),
            SLOW_RESOLVE,
        ] {
            log_located(TableId::new(1), 1, &[], 0, 0, 0.0, locate);
        }
    });

    assert_eq!(levels, [tracing::Level::DEBUG, tracing::Level::INFO]);
}
