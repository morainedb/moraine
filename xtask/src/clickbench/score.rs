//! ClickBench's published scoring, reimplemented so a local run can be
//! ranked against the results on <https://benchmark.clickhouse.com>.
//!
//! Every constant and rule here mirrors the dashboard's `index.html`: the
//! numbers only mean something next to upstream's if they are computed the
//! same way.

use super::run::{Run, Tries};

/// Added to both sides of every ratio, so a query that takes a
/// millisecond on one system and two on another does not read as a 2x
/// difference.
const CONSTANT_TIME_ADD: f64 = 0.01;

/// What an unanswered query is charged, as a multiple of the system's own
/// worst timing.
const MISSING_RESULT_PENALTY: f64 = 2.0;

/// The floor that penalty applies to, in seconds.
const MISSING_RESULT_TIME: f64 = 300.0;

const LOAD_TIME_SHARE: f64 = 0.1;
const DATA_SIZE_SHARE: f64 = 0.1;
const COLD_SHARE: f64 = 0.2;
const HOT_SHARE: f64 = 0.6;

/// A way of ranking systems. The three query metrics differ only in which
/// of a query's three tries they read; the other two rank a single number
/// the run reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// The first try, against dropped caches.
    Cold,
    /// The better of the two tries after it.
    Hot,
    /// Cold and hot weighted together, plus load time and data size.
    Combined,
    /// Seconds to load the dataset.
    Load,
    /// Bytes the loaded dataset occupies.
    Size,
}

/// The timing `metric` reads out of one query's tries. `None` is a query
/// the metric cannot score: an absent cold try, or either hot try missing.
pub fn select_run(tries: Option<&Tries>, metric: Metric) -> Option<f64> {
    let tries = tries?;
    let cold = tries[0];
    let hot = match (tries[1], tries[2]) {
        (Some(first), Some(second)) => Some(first.min(second)),
        _ => None,
    };

    match metric {
        Metric::Cold => cold,
        Metric::Hot => hot,
        _ => Some((hot? * HOT_SHARE + cold? * COLD_SHARE) / (HOT_SHARE + COLD_SHARE)),
    }
}

/// Whether `run` is barred from `metric`.
///
/// The exclusions are what keep incomparable runs out of a ranking: an
/// in-memory engine has no cold read to measure, a `no-cold` entry never
/// flushed its own caches, and a system that reports no real load or a
/// database under a gigabyte has nothing to rank on those.
pub fn metric_excludes(run: &Run, metric: Metric) -> bool {
    match metric {
        Metric::Size => run.data_size.is_none_or(|size| size < 1_000_000_000),
        Metric::Load => {
            !run.load_time.is_some_and(|seconds| seconds >= 5.0)
                || run.tagged("stateless")
                || run.tagged("in-memory")
        }
        Metric::Cold => run.tagged("in-memory") || run.tagged("no-cold"),
        Metric::Combined => run.tagged("in-memory"),
        Metric::Hot => false,
    }
}

/// The per-query best time across `runs`, as a synthetic set of tries that
/// `select_run` reads like any other system's.
///
/// Each try slot draws from the systems eligible for the metric that reads
/// it — slot 0 from those with a true cold run, slots 1 and 2 from
/// everything — so a lukewarm entry cannot depress the cold baseline and
/// inflate every honest system's cold ratio.
pub fn baseline(runs: &[&Run]) -> Vec<Option<Tries>> {
    let queries = runs.iter().map(|run| run.result.len()).max().unwrap_or(0);
    let cold_candidates: Vec<&&Run> = runs
        .iter()
        .filter(|run| !metric_excludes(run, Metric::Cold))
        .collect();
    let hot_candidates: Vec<&&Run> = runs
        .iter()
        .filter(|run| !metric_excludes(run, Metric::Hot))
        .collect();

    (0..queries)
        .map(|query| {
            let best = |candidates: &[&&Run], slot: usize| -> Option<f64> {
                candidates
                    .iter()
                    .filter_map(|run| run.result.get(query)?.as_ref()?[slot])
                    .fold(None, |best: Option<f64>, timing| {
                        Some(best.map_or(timing, |best| best.min(timing)))
                    })
            };
            let tries = [
                best(&cold_candidates, 0),
                best(&hot_candidates, 1),
                best(&hot_candidates, 2),
            ];
            tries.iter().any(Option::is_some).then_some(tries)
        })
        .collect()
}

/// One timing against another, both offset so a sub-millisecond query
/// cannot read as a large multiple.
pub fn ratio(timing: f64, baseline: f64) -> f64 {
    (CONSTANT_TIME_ADD + timing) / (CONSTANT_TIME_ADD + baseline)
}

/// `run`'s geometric-mean query time relative to `baseline`, under
/// `metric`. Lower is better and 1.0 is the best time on every query.
///
/// A query `run` did not answer is charged twice its own worst timing,
/// floored at five minutes. Queries no eligible system answered are left
/// out of the mean entirely rather than collapsing it to zero.
pub fn relative_query_time(baseline: &[Option<Tries>], run: &Run, metric: Metric) -> Option<f64> {
    let worst = run
        .result
        .iter()
        .filter_map(|tries| select_run(tries.as_ref(), metric))
        .fold(MISSING_RESULT_TIME, f64::max);
    let fallback = MISSING_RESULT_PENALTY * worst;

    let mut logarithms = 0.0;
    let mut scored = 0usize;
    for (query, baseline_tries) in baseline.iter().enumerate() {
        let Some(baseline_timing) = select_run(baseline_tries.as_ref(), metric) else {
            continue;
        };
        if !baseline_timing.is_finite() {
            continue;
        }
        let timing = run
            .result
            .get(query)
            .and_then(|tries| select_run(tries.as_ref(), metric))
            .unwrap_or(fallback);
        logarithms += ratio(timing, baseline_timing).ln();
        scored += 1;
    }

    #[allow(clippy::cast_precision_loss)]
    let scored_queries = scored as f64;
    (scored > 0).then(|| (logarithms / scored_queries).exp())
}

/// `run`'s combined score against `peers`: hot and cold query time
/// weighted 60/20, load time and data size 10 each.
///
/// A component `run` is barred from is dropped and the remaining weights
/// are scaled back up, so a lukewarm or stateless entry is ranked on what
/// it does report instead of being penalized for what it cannot.
pub fn combined_score(baseline: &[Option<Tries>], run: &Run, peers: &[&Run]) -> Option<f64> {
    let exclude_cold = metric_excludes(run, Metric::Cold);
    let exclude_load = metric_excludes(run, Metric::Load);
    let exclude_size = metric_excludes(run, Metric::Size);

    let hot_share = if exclude_cold {
        COLD_SHARE + HOT_SHARE
    } else {
        HOT_SHARE
    };
    let mut logarithms = hot_share * relative_query_time(baseline, run, Metric::Hot)?.ln();

    if !exclude_cold {
        logarithms += COLD_SHARE * relative_query_time(baseline, run, Metric::Cold)?.ln();
    }
    if !exclude_load {
        logarithms += LOAD_TIME_SHARE * (run.load_time? / best_load_time(peers)?).ln();
    }
    if !exclude_size {
        #[allow(clippy::cast_precision_loss)]
        let size = run.data_size? as f64;
        #[allow(clippy::cast_precision_loss)]
        let best = best_data_size(peers)? as f64;
        logarithms += DATA_SIZE_SHARE * (size / best).ln();
    }

    let correction = 1.0
        - if exclude_load { LOAD_TIME_SHARE } else { 0.0 }
        - if exclude_size { DATA_SIZE_SHARE } else { 0.0 };
    Some((logarithms / correction).exp())
}

/// The quickest load among the runs eligible to be ranked on it.
pub fn best_load_time(runs: &[&Run]) -> Option<f64> {
    runs.iter()
        .filter(|run| !metric_excludes(run, Metric::Load))
        .filter_map(|run| run.load_time)
        .fold(None, |best: Option<f64>, seconds| {
            Some(best.map_or(seconds, |best| best.min(seconds)))
        })
}

/// The smallest database among the runs eligible to be ranked on it.
pub fn best_data_size(runs: &[&Run]) -> Option<u64> {
    runs.iter()
        .filter(|run| !metric_excludes(run, Metric::Size))
        .filter_map(|run| run.data_size)
        .min()
}

/// `run`'s score under `metric`, against `peers` as the field it is being
/// ranked in. `None` where the run is barred from the metric, which the
/// dashboard shows by dropping the row from that ranking.
pub fn score(baseline: &[Option<Tries>], run: &Run, peers: &[&Run], metric: Metric) -> Option<f64> {
    if metric_excludes(run, metric) {
        return None;
    }
    match metric {
        Metric::Combined => combined_score(baseline, run, peers),
        Metric::Load => Some(run.load_time? / best_load_time(peers)?),
        Metric::Size => {
            #[allow(clippy::cast_precision_loss)]
            let ratio = run.data_size? as f64 / best_data_size(peers)? as f64;
            Some(ratio)
        }
        query_metric => relative_query_time(baseline, run, query_metric),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Three tries, all answered — the shape most of these cases need.
    fn tries(cold: f64, first: f64, second: f64) -> Tries {
        [Some(cold), Some(first), Some(second)]
    }

    #[test]
    fn cold_reads_the_first_try_and_hot_the_better_of_the_rest() {
        let sample = Some(tries(1.0, 0.4, 0.3));
        assert_eq!(select_run(sample.as_ref(), Metric::Cold), Some(1.0));
        assert_eq!(select_run(sample.as_ref(), Metric::Hot), Some(0.3));
    }

    #[test]
    fn combined_weights_hot_three_times_cold() {
        let sample = Some(tries(1.0, 0.5, 0.5));
        let expected = (0.5 * 0.6 + 1.0 * 0.2) / 0.8;
        assert_eq!(
            select_run(sample.as_ref(), Metric::Combined),
            Some(expected)
        );
    }

    #[test]
    fn a_missing_hot_try_leaves_no_hot_timing() {
        let partial = Some([Some(1.0), Some(0.4), None]);
        assert_eq!(select_run(partial.as_ref(), Metric::Hot), None);
        assert_eq!(select_run(partial.as_ref(), Metric::Cold), Some(1.0));
        assert_eq!(select_run(None, Metric::Cold), None);
    }

    #[test]
    fn the_baseline_takes_each_run_slot_from_its_own_eligible_systems() {
        // The in-memory system is quickest everywhere but is barred from
        // the cold slot, so the cold baseline is the other system's.
        let fast = Run {
            tags: vec!["in-memory".to_owned()],
            result: vec![Some(tries(0.1, 0.1, 0.1))],
            ..Run::default()
        };
        let durable = Run {
            result: vec![Some(tries(2.0, 0.5, 0.6))],
            ..Run::default()
        };
        let baseline = baseline(&[&fast, &durable]);
        assert_eq!(baseline[0], Some([Some(2.0), Some(0.1), Some(0.1)]));
    }

    #[test]
    fn a_system_matching_the_baseline_everywhere_scores_one() {
        let run = Run {
            result: vec![Some(tries(2.0, 0.5, 0.5)), Some(tries(1.0, 0.2, 0.2))],
            ..Run::default()
        };
        let baseline = baseline(&[&run]);
        assert_eq!(relative_query_time(&baseline, &run, Metric::Hot), Some(1.0));
    }

    #[test]
    fn the_ten_millisecond_offset_keeps_fast_queries_from_dominating() {
        let baseline = vec![Some([Some(1.0), Some(0.001), Some(0.001)])];
        let run = Run {
            result: vec![Some(tries(1.0, 0.002, 0.002))],
            ..Run::default()
        };
        // Without the offset this would be 2x; with it, 12ms over 11ms.
        let ratio = relative_query_time(&baseline, &run, Metric::Hot).unwrap();
        assert!((ratio - 0.012 / 0.011).abs() < 1e-12);
    }

    #[test]
    fn a_geometric_mean_averages_the_per_query_ratios() {
        let baseline = vec![
            Some([Some(1.0), Some(1.0), Some(1.0)]),
            Some([Some(1.0), Some(1.0), Some(1.0)]),
        ];
        let run = Run {
            // Ratios of about 4 and 1/4 about the baseline: a geometric
            // mean returns to 1 where an arithmetic one would not.
            result: vec![
                Some(tries(1.0, 4.03, 4.03)),
                Some(tries(1.0, 0.2425, 0.2425)),
            ],
            ..Run::default()
        };
        let ratio = relative_query_time(&baseline, &run, Metric::Hot).unwrap();
        assert!((ratio - 1.0).abs() < 1e-3, "{ratio}");
    }

    #[test]
    fn a_query_the_system_never_answered_costs_twice_the_floor() {
        let baseline = vec![
            Some([Some(1.0), Some(1.0), Some(1.0)]),
            Some([Some(1.0), Some(1.0), Some(1.0)]),
        ];
        let run = Run {
            result: vec![Some(tries(1.0, 1.0, 1.0)), None],
            ..Run::default()
        };
        // The second query falls back to 2 x max(300, every timing) = 600.
        let expected = f64::midpoint(f64::ln(1.0), f64::ln(600.01 / 1.01)).exp();
        let ratio = relative_query_time(&baseline, &run, Metric::Hot).unwrap();
        assert!((ratio - expected).abs() < 1e-9, "{ratio} != {expected}");
    }

    #[test]
    fn queries_no_eligible_system_answered_leave_the_mean_alone() {
        let baseline = vec![Some([Some(1.0), Some(1.0), Some(1.0)]), None];
        let run = Run {
            result: vec![Some(tries(1.0, 2.0, 2.0)), None],
            ..Run::default()
        };
        let ratio = relative_query_time(&baseline, &run, Metric::Hot).unwrap();
        assert!((ratio - 2.01 / 1.01).abs() < 1e-12);
    }

    #[test]
    fn a_barred_run_has_no_score_under_that_metric() {
        let stateless = Run {
            tags: vec!["stateless".to_owned()],
            load_time: Some(0.5),
            data_size: Some(2_000_000_000),
            result: vec![Some(tries(1.0, 0.5, 0.5))],
            ..Run::default()
        };
        let field = [&stateless];
        let baseline = baseline(&field);
        assert_eq!(score(&baseline, &stateless, &field, Metric::Load), None);
        assert_eq!(
            score(&baseline, &stateless, &field, Metric::Size),
            Some(1.0)
        );
    }

    #[test]
    fn a_trivial_load_or_a_small_database_is_barred_from_those_metrics() {
        let stateless = Run {
            load_time: Some(1.0),
            data_size: Some(2_000_000_000),
            ..Run::default()
        };
        assert!(metric_excludes(&stateless, Metric::Load));
        assert!(!metric_excludes(&stateless, Metric::Size));

        let tiny = Run {
            load_time: Some(400.0),
            data_size: Some(10_000),
            ..Run::default()
        };
        assert!(!metric_excludes(&tiny, Metric::Load));
        assert!(metric_excludes(&tiny, Metric::Size));
    }

    #[test]
    fn a_lukewarm_run_is_barred_from_cold_but_still_scores_combined() {
        let lukewarm = Run {
            tags: vec!["no-cold".to_owned()],
            ..Run::default()
        };
        assert!(metric_excludes(&lukewarm, Metric::Cold));
        assert!(!metric_excludes(&lukewarm, Metric::Combined));

        let memory = Run {
            tags: vec!["in-memory".to_owned()],
            ..Run::default()
        };
        assert!(metric_excludes(&memory, Metric::Cold));
        assert!(metric_excludes(&memory, Metric::Combined));
    }

    #[test]
    fn combined_folds_a_barred_cold_weight_into_hot() {
        // One system, so every ratio is 1 and the score is 1 whichever
        // weights apply — what this pins is that a barred component does
        // not leave the exponent short and drag the score below 1.
        let lukewarm = Run {
            tags: vec!["no-cold".to_owned()],
            load_time: Some(100.0),
            data_size: Some(2_000_000_000),
            result: vec![Some(tries(1.0, 0.5, 0.5))],
            ..Run::default()
        };
        let baseline = baseline(&[&lukewarm]);
        let score = combined_score(&baseline, &lukewarm, &[&lukewarm]).unwrap();
        assert!((score - 1.0).abs() < 1e-12, "{score}");
    }

    #[test]
    fn combined_reweights_when_load_and_size_are_both_barred() {
        let stateless = Run {
            load_time: Some(0.5),
            data_size: Some(1),
            result: vec![Some(tries(1.0, 0.5, 0.5))],
            ..Run::default()
        };
        let baseline = baseline(&[&stateless]);
        let score = combined_score(&baseline, &stateless, &[&stateless]).unwrap();
        assert!((score - 1.0).abs() < 1e-12, "{score}");
    }
}
