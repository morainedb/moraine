//! Bench report shaping: an aligned text table for stdout and a JSON
//! document for `target/bench/report.json`, both computed from raw
//! per-repeat phase timings.

use std::fmt::Write as _;

use super::{
    endpoints::Endpoint,
    timing::{median, spread},
};

/// Per-repeat seconds for one (workload, phase, backend) cell, with the
/// commits each repeat performed where the workload counts them.
pub struct Row {
    pub workload: String,
    pub phase: String,
    pub backend: String,
    pub seconds: Vec<f64>,
    pub commits: Vec<u64>,
}

/// A full bench run: the knobs it ran with, the latency of every endpoint
/// it talked to, and every timed cell.
pub struct Report {
    pub scale: String,
    pub repeat: usize,
    pub endpoints: Vec<Endpoint>,
    pub rows: Vec<Row>,
}

/// Renders `seconds` with an adaptive unit: µs below a millisecond, ms
/// below a second, seconds above.
pub fn format_duration(seconds: f64) -> String {
    if seconds < 0.001 {
        format!("{:.1}µs", seconds * 1_000_000.0)
    } else if seconds < 1.0 {
        format!("{:.1}ms", seconds * 1000.0)
    } else {
        format!("{seconds:.2}s")
    }
}

/// The median commits per second over a row's repeats, for a workload
/// that counts its commits.
fn cell_rate(row: &Row) -> Option<f64> {
    if row.commits.len() != row.seconds.len() || row.commits.is_empty() {
        return None;
    }

    #[allow(clippy::cast_precision_loss)]
    let mut rates: Vec<f64> = row
        .commits
        .iter()
        .zip(&row.seconds)
        .filter(|&(_, &seconds)| seconds > 0.0)
        .map(|(&commits, &seconds)| commits as f64 / seconds)
        .collect();
    median(&mut rates).ok()
}

/// One rendered cell: `median (min…max)` over repeats, and the commit
/// rate where the workload has one, or `skipped` when the backend didn't
/// run this workload.
fn cell(report: &Report, workload: &str, phase: &str, backend: &str) -> anyhow::Result<String> {
    let Some(row) = report
        .rows
        .iter()
        .find(|row| row.workload == workload && row.phase == phase && row.backend == backend)
    else {
        return Ok("skipped".to_owned());
    };
    let mut samples = row.seconds.clone();
    let mid = median(&mut samples)?;
    let (min, max) = spread(&samples)?;
    let rate = match cell_rate(row) {
        Some(rate) => format!(" {rate:.0}/s"),
        None => String::new(),
    };
    Ok(format!(
        "{} ({}…{}){rate}",
        format_duration(mid),
        format_duration(min),
        format_duration(max)
    ))
}

/// How much slower `backend` was than moraine on this cell. A workload
/// that counts commits is compared by rate: under a window every backend
/// runs for the same time by construction, so its wall clock says
/// nothing.
fn cell_ratio(report: &Report, workload: &str, phase: &str, backend: &str) -> Option<f64> {
    let find = |backend: &str| {
        report
            .rows
            .iter()
            .find(|row| row.workload == workload && row.phase == phase && row.backend == backend)
    };
    let moraine = find("moraine")?;
    let other = find(backend)?;

    if let (Some(moraine_rate), Some(other_rate)) = (cell_rate(moraine), cell_rate(other)) {
        return (other_rate > 0.0).then_some(moraine_rate / other_rate);
    }

    let moraine = median(&mut moraine.seconds.clone()).ok()?;
    let other = median(&mut other.seconds.clone()).ok()?;
    (moraine > 0.0).then_some(other / moraine)
}

/// Renders one line per probed endpoint. A table of catalog timings is
/// only readable beside the latency of what served it.
pub fn render_endpoints(report: &Report) -> String {
    let mut rendered = String::new();
    for endpoint in &report.endpoints {
        // write! to a String cannot fail.
        let _ = writeln!(
            rendered,
            "{}: {} to {}, one TCP connect",
            endpoint.name,
            format_duration(endpoint.seconds),
            endpoint.target
        );
    }
    rendered
}

/// Renders the aligned comparison table: one row per `workload/phase`,
/// one column per backend (`median (min…max)`), and — when moraine is a
/// column — one `<backend>/moraine` ratio column per other backend.
pub fn render_table(report: &Report, backends: &[String]) -> anyhow::Result<String> {
    let has_moraine = backends.iter().any(|backend| backend == "moraine");

    let mut header = vec!["workload/phase".to_owned()];
    header.extend(backends.iter().cloned());
    if has_moraine {
        for backend in backends {
            if backend != "moraine" {
                header.push(format!("{backend}/moraine"));
            }
        }
    }

    // Phases appear in run order and each (workload, phase) is unique per
    // backend, so deduplicating the first backend column's keys preserves
    // the report's row order.
    let mut keys: Vec<(String, String)> = Vec::new();
    for row in &report.rows {
        let key = (row.workload.clone(), row.phase.clone());
        if !keys.contains(&key) {
            keys.push(key);
        }
    }

    let mut table = vec![header];
    for (workload, phase) in &keys {
        let mut line = vec![format!("{workload}/{phase}")];
        for backend in backends {
            line.push(cell(report, workload, phase, backend)?);
        }
        if has_moraine {
            for backend in backends {
                if backend == "moraine" {
                    continue;
                }
                line.push(match cell_ratio(report, workload, phase, backend) {
                    Some(ratio) => format!("{ratio:.2}×"),
                    None => "—".to_owned(),
                });
            }
        }
        table.push(line);
    }

    let columns = table[0].len();
    let mut widths = vec![0usize; columns];
    for line in &table {
        for (index, text) in line.iter().enumerate() {
            widths[index] = widths[index].max(text.chars().count());
        }
    }

    let mut rendered = String::new();
    for line in &table {
        for (index, text) in line.iter().enumerate() {
            if index > 0 {
                rendered.push_str("  ");
            }
            rendered.push_str(text);
            let pad = widths[index] - text.chars().count();
            if index + 1 < columns {
                rendered.push_str(&" ".repeat(pad));
            }
        }
        rendered.push('\n');
    }
    Ok(rendered)
}

fn json_escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\t' => escaped.push_str("\\t"),
            '\r' => escaped.push_str("\\r"),
            control if (control as u32) < 0x20 => {
                // write! to a String cannot fail.
                let _ = write!(escaped, "\\u{:04x}", control as u32);
            }
            other => escaped.push(other),
        }
    }
    escaped
}

/// Serializes the report as JSON for diffing across checkouts.
pub fn render_json(report: &Report) -> String {
    let mut results = String::new();
    for (index, row) in report.rows.iter().enumerate() {
        if index > 0 {
            results.push_str(",\n");
        }
        let seconds: Vec<String> = row.seconds.iter().map(ToString::to_string).collect();
        let commits: Vec<String> = row.commits.iter().map(ToString::to_string).collect();
        let commits = if commits.is_empty() {
            String::new()
        } else {
            format!(", \"commits\": [{}]", commits.join(", "))
        };
        // write! to a String cannot fail.
        let _ = write!(
            results,
            "    {{\"workload\": \"{}\", \"phase\": \"{}\", \"backend\": \"{}\", \
             \"seconds\": [{}]{commits}}}",
            json_escape(&row.workload),
            json_escape(&row.phase),
            json_escape(&row.backend),
            seconds.join(", ")
        );
    }

    let mut endpoints = String::new();
    for (index, endpoint) in report.endpoints.iter().enumerate() {
        if index > 0 {
            endpoints.push_str(",\n");
        }
        // write! to a String cannot fail.
        let _ = write!(
            endpoints,
            "    {{\"name\": \"{}\", \"target\": \"{}\", \"connect_seconds\": {}}}",
            json_escape(&endpoint.name),
            json_escape(&endpoint.target),
            endpoint.seconds
        );
    }

    format!(
        "{{\n  \"scale\": \"{}\",\n  \"repeat\": {},\n  \"endpoints\": [\n{}\n  ],\n  \
         \"results\": [\n{}\n  ]\n}}\n",
        json_escape(&report.scale),
        report.repeat,
        endpoints,
        results
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_units_adapt() {
        assert_eq!(format_duration(0.000_000_5), "0.5µs");
        assert_eq!(format_duration(0.0123), "12.3ms");
        assert_eq!(format_duration(1.5), "1.50s");
    }

    fn sample_report() -> Report {
        Report {
            scale: "small".to_owned(),
            repeat: 2,
            endpoints: vec![Endpoint {
                name: "s3".to_owned(),
                target: "s3.us-east-1.amazonaws.com:443".to_owned(),
                seconds: 0.0025,
            }],
            rows: vec![
                Row {
                    workload: "small_commits".to_owned(),
                    phase: "inserts".to_owned(),
                    backend: "moraine".to_owned(),
                    seconds: vec![0.010, 0.020],
                    commits: Vec::new(),
                },
                Row {
                    workload: "small_commits".to_owned(),
                    phase: "inserts".to_owned(),
                    backend: "duckdb".to_owned(),
                    seconds: vec![0.030, 0.030],
                    commits: Vec::new(),
                },
                Row {
                    workload: "concurrent_connections".to_owned(),
                    phase: "writers_4".to_owned(),
                    backend: "moraine".to_owned(),
                    seconds: vec![0.5, 1.0],
                    commits: vec![128, 128],
                },
            ],
        }
    }

    #[test]
    fn table_has_backend_columns_ratio_and_skipped_cells() {
        let backends = vec![
            "moraine".to_owned(),
            "duckdb".to_owned(),
            "postgres".to_owned(),
        ];
        let table = render_table(&sample_report(), &backends).unwrap();
        let lines: Vec<&str> = table.lines().collect();
        assert!(lines[0].contains("workload/phase"));
        assert!(lines[0].contains("duckdb/moraine"));
        assert!(lines[0].contains("postgres/moraine"));
        assert!(lines[1].starts_with("small_commits/inserts"));
        assert!(lines[1].contains("15.0ms (10.0ms…20.0ms)"));
        assert!(lines[1].contains("30.0ms (30.0ms…30.0ms)"));
        assert!(lines[1].contains("2.00×"));
        assert!(lines[1].contains("skipped"));
    }

    #[test]
    fn a_counted_workload_is_compared_by_rate_not_by_its_window() {
        let report = Report {
            scale: "small".to_owned(),
            repeat: 1,
            endpoints: Vec::new(),
            rows: vec![
                Row {
                    workload: "concurrent_connections".to_owned(),
                    phase: "writers_1".to_owned(),
                    backend: "moraine".to_owned(),
                    seconds: vec![5.0],
                    commits: vec![55],
                },
                Row {
                    workload: "concurrent_connections".to_owned(),
                    phase: "writers_1".to_owned(),
                    backend: "duckdb".to_owned(),
                    seconds: vec![5.0],
                    commits: vec![680],
                },
            ],
        };

        // Both ran for the same five seconds, so a wall-clock ratio would
        // read as parity where one committed twelve times as much.
        let table = render_table(&report, &["moraine".to_owned(), "duckdb".to_owned()]).unwrap();
        let line = table
            .lines()
            .find(|line| line.starts_with("concurrent_connections/writers_1"))
            .unwrap();
        assert!(line.contains("0.08×"), "{line}");
        assert!(!line.contains("1.00×"), "{line}");
    }

    #[test]
    fn a_counted_workload_cell_carries_its_commit_rate() {
        let backends = vec!["moraine".to_owned()];
        let table = render_table(&sample_report(), &backends).unwrap();
        let line = table
            .lines()
            .find(|line| line.starts_with("concurrent_connections/writers_4"))
            .unwrap();
        // 128 commits over the median of 0.5 s and 1.0 s.
        assert!(line.contains("192/s"), "{line}");

        let latency = table
            .lines()
            .find(|line| line.starts_with("small_commits/inserts"))
            .unwrap();
        assert!(!latency.contains("/s"), "{latency}");
    }

    #[test]
    fn endpoint_latency_is_rendered_above_the_table() {
        let rendered = render_endpoints(&sample_report());
        assert_eq!(
            rendered,
            "s3: 2.5ms to s3.us-east-1.amazonaws.com:443, one TCP connect\n"
        );
    }

    #[test]
    fn json_carries_metadata_endpoints_and_per_repeat_seconds() {
        let json = render_json(&sample_report());
        assert!(json.contains("\"scale\": \"small\""));
        assert!(json.contains("\"repeat\": 2"));
        assert!(json.contains(
            "{\"name\": \"s3\", \"target\": \"s3.us-east-1.amazonaws.com:443\", \
             \"connect_seconds\": 0.0025}"
        ));
        assert!(json.contains("\"commits\": [128, 128]"));
        assert!(json.contains(
            "{\"workload\": \"small_commits\", \"phase\": \"inserts\", \
             \"backend\": \"moraine\", \"seconds\": [0.01, 0.02]}"
        ));
    }
}
