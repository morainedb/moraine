//! Rendering a field of ClickBench runs as an aligned comparison table.
//!
//! Scores are relative: 1.00 is the best run in the field on that metric,
//! and a run barred from a metric prints as a dash rather than a number it
//! has no basis for.

use std::fmt::Write as _;

use anyhow::{Context, ensure};

use super::{
    run::Run,
    score::{self, Metric},
};

/// Every metric a comparison column can carry, in the order they print.
const COLUMNS: [(&str, Metric); 5] = [
    ("combined", Metric::Combined),
    ("cold", Metric::Cold),
    ("hot", Metric::Hot),
    ("load", Metric::Load),
    ("size", Metric::Size),
];

/// One run's place in the field.
struct Ranked<'a> {
    run: &'a Run,
    scores: [Option<f64>; COLUMNS.len()],
    sort_key: Option<f64>,
}

/// Ranks `runs` by `metric` and renders at most `limit` rows, keeping the
/// row for `highlight` whatever it ranks. The highlighted row is marked
/// with a leading `>`.
pub fn render(
    runs: &[Run],
    metric: Metric,
    highlight: Option<&str>,
    limit: usize,
) -> anyhow::Result<String> {
    ensure!(!runs.is_empty(), "no runs to compare");
    let field: Vec<&Run> = runs.iter().collect();
    let baseline = score::baseline(&field);

    let mut ranked: Vec<Ranked<'_>> = runs
        .iter()
        .map(|run| Ranked {
            run,
            scores: COLUMNS.map(|(_, column)| score::score(&baseline, run, &field, column)),
            sort_key: score::score(&baseline, run, &field, metric),
        })
        .collect();
    // A run the metric cannot score has no place in its ranking, so it
    // sorts last rather than first.
    ranked.sort_by(|left, right| {
        let key = |entry: &Ranked<'_>| entry.sort_key.unwrap_or(f64::INFINITY);
        key(left)
            .total_cmp(&key(right))
            .then_with(|| left.run.system.cmp(&right.run.system))
    });

    let shown = select_rows(&ranked, highlight, limit);

    let mut header = vec![
        String::new(),
        "#".to_owned(),
        "system".to_owned(),
        "machine".to_owned(),
        "nodes".to_owned(),
    ];
    for (name, column) in COLUMNS {
        header.push(if column == metric {
            format!("{name}*")
        } else {
            name.to_owned()
        });
    }

    let mut table = vec![header];
    for &index in &shown {
        let entry = &ranked[index];
        let mut row = vec![
            if highlight == Some(entry.run.system.as_str()) {
                ">".to_owned()
            } else {
                String::new()
            },
            (index + 1).to_string(),
            entry.run.system.clone(),
            entry.run.machine.clone(),
            entry.run.cluster_size_label(),
        ];
        for score in entry.scores {
            row.push(match score {
                Some(ratio) => format!("{ratio:.2}"),
                None => "—".to_owned(),
            });
        }
        table.push(row);
    }

    Ok(align(&table))
}

/// The rows to print: the first `limit` by rank, with the highlighted run
/// taking the last slot when it ranks past it.
fn select_rows(ranked: &[Ranked<'_>], highlight: Option<&str>, limit: usize) -> Vec<usize> {
    let mut shown: Vec<usize> = (0..ranked.len()).take(limit).collect();
    let Some(system) = highlight else {
        return shown;
    };
    let Some(place) = ranked.iter().position(|entry| entry.run.system == system) else {
        return shown;
    };
    if !shown.contains(&place) {
        shown.pop();
        shown.push(place);
    }
    shown
}

/// Per-query detail for `system`: its own time, the best in the field, and
/// the ratio between them under `metric`.
pub fn render_queries(runs: &[Run], system: &str, metric: Metric) -> anyhow::Result<String> {
    let field: Vec<&Run> = runs.iter().collect();
    let subject = field
        .iter()
        .find(|run| run.system == system)
        .with_context(|| format!("no run in the field is `{system}`"))?;
    let baseline = score::baseline(&field);

    let mut table = vec![vec![
        "query".to_owned(),
        system.to_owned(),
        "best".to_owned(),
        "ratio".to_owned(),
        "set by".to_owned(),
    ]];
    for (query, baseline_tries) in baseline.iter().enumerate() {
        let best = score::select_run(baseline_tries.as_ref(), metric);
        let timing = subject
            .result
            .get(query)
            .and_then(|tries| score::select_run(tries.as_ref(), metric));
        table.push(vec![
            format!("Q{}", query + 1),
            seconds(timing),
            seconds(best),
            match (timing, best) {
                (Some(timing), Some(best)) => format!("{:.2}", score::ratio(timing, best)),
                _ => "—".to_owned(),
            },
            best.map_or_else(
                || "—".to_owned(),
                |best| fastest_system(&field, query, metric, best),
            ),
        ]);
    }

    Ok(align(&table))
}

/// The system that set the best time on `query`.
fn fastest_system(field: &[&Run], query: usize, metric: Metric, best: f64) -> String {
    field
        .iter()
        .filter(|run| !score::metric_excludes(run, metric))
        .find(|run| {
            run.result
                .get(query)
                .and_then(|tries| score::select_run(tries.as_ref(), metric))
                .is_some_and(|timing| (timing - best).abs() < f64::EPSILON)
        })
        .map_or_else(|| "—".to_owned(), |run| run.system.clone())
}

/// A timing, or a dash where there is none.
fn seconds(timing: Option<f64>) -> String {
    timing.map_or_else(|| "—".to_owned(), |timing| format!("{timing:.3}"))
}

/// Pads every column to its widest cell, two spaces between.
fn align(table: &[Vec<String>]) -> String {
    let columns = table.iter().map(Vec::len).max().unwrap_or(0);
    let mut widths = vec![0usize; columns];
    for row in table {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.chars().count());
        }
    }

    let mut rendered = String::new();
    for row in table {
        let mut line = String::new();
        for (index, cell) in row.iter().enumerate() {
            if index > 0 {
                line.push_str("  ");
            }
            line.push_str(cell);
            let pad = widths[index] - cell.chars().count();
            line.push_str(&" ".repeat(pad));
        }
        // write! to a String cannot fail.
        let _ = writeln!(rendered, "{}", line.trim_end());
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickbench::run::Run;

    fn run(system: &str, hot: f64, load: f64, size: u64) -> Run {
        Run {
            system: system.to_owned(),
            machine: "c6a.4xlarge".to_owned(),
            cluster_size: serde_json::json!(1),
            load_time: Some(load),
            data_size: Some(size),
            result: vec![Some([Some(hot * 4.0), Some(hot), Some(hot)])],
            ..Run::default()
        }
    }

    #[test]
    fn the_table_ranks_by_the_chosen_metric_and_the_best_scores_one() {
        let runs = vec![
            run("Slow", 4.0, 400.0, 4_000_000_000),
            run("Quick", 1.0, 100.0, 2_000_000_000),
        ];
        let table = render(&runs, Metric::Hot, None, usize::MAX).unwrap();
        assert!(
            table
                .lines()
                .next()
                .is_some_and(|header| header.contains("hot*"))
        );
        assert_eq!(systems(&table), ["Quick", "Slow"], "{table}");
        assert!(
            table
                .lines()
                .nth(1)
                .is_some_and(|best| best.contains("1.00")),
            "{table}"
        );
    }

    /// The system named on each row after the header, in rank order. The
    /// marker column shifts the fields, so the system is whatever follows
    /// the rank.
    fn systems(table: &str) -> Vec<String> {
        table
            .lines()
            .skip(1)
            .filter_map(|line| {
                let mut fields = line
                    .split_whitespace()
                    .skip_while(|field| field.parse::<usize>().is_err());
                fields.next();
                fields.next().map(str::to_owned)
            })
            .collect()
    }

    #[test]
    fn the_highlighted_system_is_marked_in_place() {
        let runs = vec![
            run("Quick", 1.0, 100.0, 2_000_000_000),
            run("Ours", 4.0, 100.0, 2_000_000_000),
        ];
        let table = render(&runs, Metric::Hot, Some("Ours"), usize::MAX).unwrap();
        let marked: Vec<&str> = table.lines().filter(|line| line.starts_with('>')).collect();
        assert_eq!(marked.len(), 1, "{table}");
        assert!(marked[0].contains("Ours"), "{table}");
    }

    #[test]
    fn a_limit_keeps_the_highlighted_system_even_when_it_ranks_past_it() {
        let runs = vec![
            run("A", 1.0, 100.0, 2_000_000_000),
            run("B", 2.0, 100.0, 2_000_000_000),
            run("Ours", 9.0, 100.0, 2_000_000_000),
        ];
        let table = render(&runs, Metric::Hot, Some("Ours"), 2).unwrap();
        assert_eq!(systems(&table), ["A", "Ours"], "{table}");
    }

    #[test]
    fn a_metric_a_system_is_barred_from_prints_as_a_dash() {
        let mut stateless = run("Stateless", 1.0, 1.0, 10);
        stateless.tags = vec!["stateless".to_owned()];
        let table = render(&[stateless], Metric::Load, None, usize::MAX).unwrap();
        assert!(table.contains('—'), "{table}");
    }

    #[test]
    fn an_empty_field_is_refused_rather_than_rendered_blank() {
        assert!(render(&[], Metric::Hot, None, usize::MAX).is_err());
    }

    #[test]
    fn per_query_rows_name_the_system_that_set_each_baseline() {
        let runs = vec![
            run("Quick", 1.0, 100.0, 2_000_000_000),
            run("Ours", 3.0, 100.0, 2_000_000_000),
        ];
        let table = render_queries(&runs, "Ours", Metric::Hot).unwrap();
        assert!(table.contains("Quick"), "{table}");
        assert!(table.contains("3.000"), "{table}");
        assert!(table.contains("2.98"), "{table}");
    }

    #[test]
    fn per_query_rows_need_the_named_system_present() {
        let runs = vec![run("Quick", 1.0, 100.0, 2_000_000_000)];
        assert!(render_queries(&runs, "Absent", Metric::Hot).is_err());
    }
}
