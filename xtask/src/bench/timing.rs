//! Parsing of the DuckDB CLI's `.timer on` output and per-phase
//! aggregation. With the timer on, the CLI prints one
//! `Run Time (s): real R user U sys S` line after every SQL statement, in
//! statement order (dot-commands print none) — so run times zip with the
//! statement list by index.

use anyhow::ensure;

/// One SQL statement in a measured session. `phase: None` marks setup:
/// executed and timed by the CLI, but excluded from the report.
pub struct Statement {
    pub sql: String,
    pub phase: Option<&'static str>,
}

impl Statement {
    pub fn setup(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            phase: None,
        }
    }

    pub fn measured(phase: &'static str, sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            phase: Some(phase),
        }
    }
}

/// Extracts the `real` seconds from every `Run Time (s)` line, in order.
/// A warning the CLI prints in colour ends with a reset sequence on the
/// timer's line, so terminal escapes are stripped before matching.
pub fn parse_run_times(stdout: &str) -> Vec<f64> {
    stdout
        .lines()
        .filter_map(|line| {
            let plain = strip_terminal_escapes(line);
            let rest = plain.strip_prefix("Run Time (s): real ")?;
            let real = rest.split_whitespace().next()?;
            real.parse().ok()
        })
        .collect()
}

/// `line` without its `ESC [ ... m` colour sequences.
pub fn strip_terminal_escapes(line: &str) -> String {
    let mut plain = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(start) = rest.find("\x1b[") {
        plain.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find('m') {
            Some(end) => rest = &after[end + 1..],
            None => return plain,
        }
    }
    plain.push_str(rest);
    plain
}

/// Zips `stdout`'s run-time lines with `statements` by index and sums the
/// `real` seconds per phase, preserving first-appearance phase order.
pub fn phase_seconds(
    statements: &[Statement],
    stdout: &str,
) -> anyhow::Result<Vec<(&'static str, f64)>> {
    let run_times = parse_run_times(stdout);
    ensure!(
        run_times.len() == statements.len(),
        "expected {} `Run Time (s)` lines, found {}; session output:\n{stdout}",
        statements.len(),
        run_times.len()
    );

    let mut totals: Vec<(&'static str, f64)> = Vec::new();
    for (statement, seconds) in statements.iter().zip(run_times) {
        let Some(phase) = statement.phase else {
            continue;
        };
        match totals.iter_mut().find(|(name, _)| *name == phase) {
            Some((_, total)) => *total += seconds,
            None => totals.push((phase, seconds)),
        }
    }
    Ok(totals)
}

/// Median of `samples`; sorts in place. At least one sample is required.
pub fn median(samples: &mut [f64]) -> anyhow::Result<f64> {
    ensure!(!samples.is_empty(), "median of zero samples");
    samples.sort_by(f64::total_cmp);
    let mid = samples.len() / 2;
    Ok(if samples.len() % 2 == 1 {
        samples[mid]
    } else {
        f64::midpoint(samples[mid - 1], samples[mid])
    })
}

/// `(min, max)` of `samples`; same non-empty contract as [`median`].
pub fn spread(samples: &[f64]) -> anyhow::Result<(f64, f64)> {
    ensure!(!samples.is_empty(), "spread of zero samples");
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for &sample in samples {
        min = min.min(sample);
        max = max.max(sample);
    }
    Ok((min, max))
}

#[cfg(test)]
#[allow(clippy::float_cmp)] // asserting on exactly-representable literals
mod tests {
    use super::*;

    #[test]
    fn parses_run_time_lines_in_order() {
        let out = "1\nRun Time (s): real 0.316 user 0.001339 sys 0.004215\ncount_star()\n1000\n\
                   Run Time (s): real 0.003 user 0.000679 sys 0.000906\n";
        assert_eq!(parse_run_times(out), vec![0.316, 0.003]);
    }

    #[test]
    fn a_colour_reset_before_the_timer_line_does_not_hide_it() {
        let out = "\x1b[90mWARNING:\n\x1b[00m\x1b[90msome warning text\n\n\
                   \x1b[00mRun Time (s): real 0.339 user 0.036945 sys 0.020743\n\
                   Run Time (s): real 0.279 user 0.155977 sys 0.021373\n";
        assert_eq!(parse_run_times(out), vec![0.339, 0.279]);
    }

    #[test]
    fn ignores_result_rows_that_mention_run_time_elsewhere() {
        let out = "some Run Time (s): real 1.0 not at line start is still matched only when \
                   the line starts with the prefix\nRun Time (s): real 2.5 user 0 sys 0\n";
        assert_eq!(parse_run_times(out), vec![2.5]);
    }

    #[test]
    fn phase_seconds_sums_by_phase_in_first_seen_order() {
        let statements = [
            Statement::setup("SELECT 1;"),
            Statement::measured("attach", "ATTACH 'x';"),
            Statement::measured("inserts", "INSERT 1;"),
            Statement::measured("inserts", "INSERT 2;"),
        ];
        let out = "Run Time (s): real 0.100 user 0 sys 0\n\
                   Run Time (s): real 0.200 user 0 sys 0\n\
                   Run Time (s): real 0.300 user 0 sys 0\n\
                   Run Time (s): real 0.400 user 0 sys 0\n";
        let phases = phase_seconds(&statements, out).unwrap();
        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0].0, "attach");
        assert!((phases[0].1 - 0.2).abs() < 1e-9);
        assert_eq!(phases[1].0, "inserts");
        assert!((phases[1].1 - 0.7).abs() < 1e-9);
    }

    #[test]
    fn phase_seconds_rejects_count_mismatch() {
        let statements = [Statement::measured("attach", "ATTACH 'x';")];
        assert!(phase_seconds(&statements, "no timer lines here\n").is_err());
    }

    #[test]
    fn median_of_odd_and_even_sample_counts() {
        assert_eq!(median(&mut [3.0, 1.0, 2.0]).unwrap(), 2.0);
        assert_eq!(median(&mut [4.0, 1.0, 2.0, 3.0]).unwrap(), 2.5);
        assert!(median(&mut []).is_err());
    }

    #[test]
    fn spread_returns_min_and_max() {
        assert_eq!(spread(&[0.2, 0.1, 0.3]).unwrap(), (0.1, 0.3));
        assert!(spread(&[]).is_err());
    }
}
