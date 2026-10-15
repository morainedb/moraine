//! Parsing the ClickBench driver's log into a result document.
//!
//! `lib/benchmark-common.sh` prints one `[t1,t2,t3],` line per query
//! between a `Load time:` and a `Data size:` line, with `null` for a try
//! that failed. Everything else on the stream is progress noise.

use anyhow::{Context, bail, ensure};

use super::run::Tries;

/// What a driver log carries: the measurements, without the metadata that
/// `template.json` and the run's machine supply.
#[derive(Debug, Default, PartialEq)]
pub struct Measurements {
    /// Seconds the load took.
    pub load_time: Option<f64>,
    /// Bytes the loaded dataset occupies.
    pub data_size: Option<u64>,
    /// Successful queries per second under concurrent load, where the
    /// driver ran that test.
    pub concurrent_qps: Option<f64>,
    /// Share of the concurrent test's queries that errored.
    pub concurrent_error_ratio: Option<f64>,
    /// Seconds per query, in `queries.sql` order.
    pub result: Vec<Option<Tries>>,
}

/// Reads a driver log. Fails when the load never finished or no query ran,
/// which are the shapes a crashed run leaves behind.
pub fn parse(log: &str) -> anyhow::Result<Measurements> {
    let mut measurements = Measurements::default();

    for line in log.lines().map(str::trim) {
        if let Some(value) = line.strip_prefix("Load time:") {
            measurements.load_time = optional_number(value)?;
        } else if let Some(value) = line.strip_prefix("Data size:") {
            measurements.data_size = optional_bytes(value)?;
        } else if let Some(value) = line.strip_prefix("Concurrent QPS:") {
            measurements.concurrent_qps = optional_number(value)?;
        } else if let Some(value) = line.strip_prefix("Concurrent error ratio:") {
            measurements.concurrent_error_ratio = optional_number(value)?;
        } else if line.starts_with('[') {
            measurements.result.push(Some(tries(line)?));
        }
    }

    ensure!(
        measurements.load_time.is_some(),
        "the log carries no `Load time` line: the load did not finish"
    );
    ensure!(
        !measurements.result.is_empty(),
        "the log carries no query timings: the run stopped before the queries"
    );
    Ok(measurements)
}

/// A `null` or a number, as every value line in the log may be.
fn optional_number(value: &str) -> anyhow::Result<Option<f64>> {
    let value = value.trim();
    if value == "null" {
        return Ok(None);
    }
    Ok(Some(value.parse().with_context(|| {
        format!("parsing `{value}` as a measurement")
    })?))
}

/// A byte count, read as an integer rather than through a float so a size
/// past 2^53 survives.
fn optional_bytes(value: &str) -> anyhow::Result<Option<u64>> {
    let value = value.trim();
    if value == "null" {
        return Ok(None);
    }
    Ok(Some(value.parse().with_context(|| {
        format!("parsing `{value}` as a byte count")
    })?))
}

/// One `[t1,t2,t3],` line.
fn tries(line: &str) -> anyhow::Result<Tries> {
    let body = line
        .trim_end_matches(',')
        .trim_start_matches('[')
        .trim_end_matches(']');
    let mut timings = [None; 3];
    let mut count = 0usize;
    for field in body.split(',') {
        if count == timings.len() {
            bail!("`{line}` carries more than three tries per query");
        }
        timings[count] = optional_number(field)?;
        count += 1;
    }
    if count != timings.len() {
        bail!("`{line}` does not carry three tries per query");
    }
    Ok(timings)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOG: &str = "\
bench: waited 5s for ext4lazyinit to finish before the load
Load time: 412.500
[0.008,0.005,0.005],
[0.17,null,0.038],
null
[1.5,1.1,1.2],
Data size: 25024802816
Concurrent QPS: null
Concurrent error ratio: null
";

    #[test]
    fn the_driver_log_yields_load_size_and_every_query() {
        let parsed = parse(LOG).unwrap();
        assert_eq!(parsed.load_time, Some(412.5));
        assert_eq!(parsed.data_size, Some(25_024_802_816));
        assert_eq!(parsed.concurrent_qps, None);
        assert_eq!(
            parsed.result,
            vec![
                Some([Some(0.008), Some(0.005), Some(0.005)]),
                Some([Some(0.17), None, Some(0.038)]),
                Some([Some(1.5), Some(1.1), Some(1.2)]),
            ]
        );
    }

    #[test]
    fn a_byte_count_past_a_floats_exact_range_survives() {
        let parsed = parse("Load time: 1.0\n[1,1,1],\nData size: 9007199254740993\n").unwrap();
        assert_eq!(parsed.data_size, Some(9_007_199_254_740_993));
    }

    #[test]
    fn a_concurrency_measurement_is_read_when_the_driver_made_one() {
        let parsed = parse("Load time: 1.0\n[1,1,1],\nData size: 10\nConcurrent QPS: 12.5\nConcurrent error ratio: 0.25\n").unwrap();
        assert_eq!(parsed.concurrent_qps, Some(12.5));
        assert_eq!(parsed.concurrent_error_ratio, Some(0.25));
    }

    #[test]
    fn a_log_without_a_load_time_is_a_failed_run_not_an_empty_one() {
        let error = parse("[1,1,1],\nData size: 10\n").unwrap_err().to_string();
        assert!(error.contains("Load time"), "{error}");
    }

    #[test]
    fn a_log_with_no_query_lines_is_refused() {
        let error = parse("Load time: 1.0\nData size: 10\n")
            .unwrap_err()
            .to_string();
        assert!(error.contains("no query timings"), "{error}");
    }

    #[test]
    fn a_try_count_other_than_three_is_refused() {
        let error = parse("Load time: 1.0\n[1,1],\nData size: 10\n")
            .unwrap_err()
            .to_string();
        assert!(error.contains("three tries"), "{error}");
    }
}
