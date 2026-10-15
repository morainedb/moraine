//! A ClickBench result document: the `results/<date>/<machine>.json` shape
//! every system in the benchmark publishes, and the loader that reads a
//! directory of them.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

/// The three tries ClickBench runs per query: the first cold, the other
/// two hot. `None` in a slot is a query the system could not answer.
pub type Tries = [Option<f64>; 3];

/// One system's run on one machine. Field names are the published ones, so
/// a document round-trips through this struct unchanged.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Run {
    /// Display name, e.g. `DuckDB` or `DuckLake (moraine)`.
    pub system: String,
    /// Run date, `YYYY-MM-DD`.
    pub date: String,
    /// Machine the run happened on, e.g. `c6a.4xlarge`. Comparisons are
    /// only meaningful within one value of this.
    pub machine: String,
    /// Node count: a number for most systems, a string for the services
    /// that size themselves differently, null where it does not apply.
    pub cluster_size: serde_json::Value,
    /// `yes` for closed-source systems.
    pub proprietary: String,
    /// `cpu` or `gpu`.
    pub hardware: String,
    /// `yes` where the entry tunes the system beyond its defaults.
    pub tuned: String,
    /// Dashboard filter vocabulary — `in-memory` and `no-cold` also change
    /// how the system is scored.
    pub tags: Vec<String>,
    /// Storage class, stated explicitly rather than inferred from the name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<String>,
    /// Seconds to load the dataset.
    pub load_time: Option<f64>,
    /// Bytes the loaded dataset occupies, indexes and logs included.
    pub data_size: Option<u64>,
    /// Successful queries per second under concurrent load.
    pub concurrent_qps: Option<f64>,
    /// Share of the concurrent test's queries that errored.
    pub concurrent_error_ratio: Option<f64>,
    /// Seconds per query, in `queries.sql` order.
    pub result: Vec<Option<Tries>>,
    /// Set in place of measurements when a run failed. Such a document
    /// stands for "the newest attempt did not finish", so the system drops
    /// out of the comparison rather than ranking on older numbers.
    #[serde(skip_serializing)]
    pub error: Option<String>,
    /// The system directory the document came from. Not part of the
    /// published shape; the loader fills it in.
    #[serde(skip)]
    pub source: String,
}

impl Run {
    /// Whether `tags` carries `tag`.
    pub fn tagged(&self, tag: &str) -> bool {
        self.tags.iter().any(|carried| carried == tag)
    }

    /// Cluster size as the dashboard prints it.
    pub fn cluster_size_label(&self) -> String {
        match &self.cluster_size {
            serde_json::Value::Null => "—".to_owned(),
            serde_json::Value::String(text) => text.clone(),
            other => other.to_string(),
        }
    }
}

/// Reads one result document, tagging it with the system directory its
/// path names.
pub fn read(path: &Path) -> anyhow::Result<Run> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut run: Run = serde_json::from_str(&text)
        .with_context(|| format!("parsing {} as a ClickBench result", path.display()))?;
    run.source = system_directory(path);
    Ok(run)
}

/// The system directory in `<system>/results/<date>/<machine>.json`, or an
/// empty string for a path that does not have that shape.
fn system_directory(path: &Path) -> String {
    path.ancestors()
        .nth(3)
        .and_then(Path::file_name)
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// The directories under a ClickBench checkout that hold something other
/// than system results.
const NOT_SYSTEMS: [&str; 3] = ["hardware", "versions", "gravitons"];

/// The newest run per (system, machine) under `root`, which is the field
/// the dashboard compares.
///
/// Newest is by the `YYYYMMDD` directory rather than the document's own
/// `date`, which some runs leave empty. Archival (`historical`) entries
/// and documents standing in for a failed run are left out, and a
/// document that will not parse is skipped rather than stopping the
/// comparison.
pub fn read_newest_per_system(root: &Path) -> anyhow::Result<Vec<Run>> {
    let mut newest: HashMap<(String, String), (String, PathBuf)> = HashMap::new();
    for path in result_paths(root)? {
        let Some(dated) = date_directory(&path) else {
            continue;
        };
        let Some(key) = system_and_machine(&path) else {
            continue;
        };
        let supersedes = newest
            .get(&key)
            .is_none_or(|(existing, _)| *existing <= dated);
        if supersedes {
            newest.insert(key, (dated, path));
        }
    }

    let mut runs: Vec<Run> = Vec::new();
    for (dated, path) in newest.into_values() {
        let Ok(mut run) = read(&path) else { continue };
        if run.error.is_some() || run.tagged("historical") {
            continue;
        }
        if run.date.is_empty() {
            run.date = iso_date(&dated);
        }
        runs.push(run);
    }

    runs.sort_by(|left, right| (&left.system, &left.machine).cmp(&(&right.system, &right.machine)));
    Ok(runs)
}

/// The `YYYYMMDD` directory in `<system>/results/<date>/<machine>.json`.
fn date_directory(path: &Path) -> Option<String> {
    Some(path.parent()?.file_name()?.to_string_lossy().into_owned())
}

/// `YYYYMMDD` as `YYYY-MM-DD`, or the input unchanged if it is not eight
/// digits.
fn iso_date(dated: &str) -> String {
    if dated.len() != 8 || !dated.bytes().all(|byte| byte.is_ascii_digit()) {
        return dated.to_owned();
    }
    format!("{}-{}-{}", &dated[0..4], &dated[4..6], &dated[6..8])
}

/// The dedup key: the system directory and the result's file name. The
/// file name rather than the document's `machine` field, so two documents
/// that disagree cannot both survive.
fn system_and_machine(path: &Path) -> Option<(String, String)> {
    let machine = path.file_name()?.to_string_lossy().into_owned();
    Some((system_directory(path), machine))
}

/// Collects `<root>/*/results/*/*.json`, skipping the directories that
/// hold something other than system results.
fn result_paths(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let systems = fs::read_dir(root).with_context(|| format!("reading {}", root.display()))?;
    for system in systems {
        let directory = system?.path();
        let skip = directory
            .file_name()
            .is_some_and(|name| NOT_SYSTEMS.iter().any(|known| *known == name));
        if skip {
            continue;
        }
        let Ok(dates) = fs::read_dir(directory.join("results")) else {
            continue;
        };
        for date in dates {
            let Ok(machines) = fs::read_dir(date?.path()) else {
                continue;
            };
            for machine in machines {
                let path = machine?.path();
                if path
                    .extension()
                    .is_some_and(|extension| extension == "json")
                {
                    paths.push(path);
                }
            }
        }
    }
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_published_document_parses_into_its_fields() {
        let run: Run = serde_json::from_str(
            r#"{"system": "DuckDB", "date": "2025-07-11", "machine": "c6a.4xlarge",
                "cluster_size": 1, "tags": ["embedded"], "load_time": 416.5,
                "data_size": 25024802816,
                "result": [[0.008, 0.005, 0.005], null, [1.0, null, 0.5]]}"#,
        )
        .unwrap();
        assert_eq!(run.system, "DuckDB");
        assert_eq!(run.load_time, Some(416.5));
        assert_eq!(run.data_size, Some(25_024_802_816));
        assert!(run.tagged("embedded"));
        assert_eq!(run.result[0], Some([Some(0.008), Some(0.005), Some(0.005)]));
        assert_eq!(run.result[1], None);
        assert_eq!(run.result[2], Some([Some(1.0), None, Some(0.5)]));
    }

    #[test]
    fn absent_optional_fields_stay_empty_rather_than_failing() {
        let run: Run = serde_json::from_str(r#"{"system": "Minimal"}"#).unwrap();
        assert_eq!(run.load_time, None);
        assert_eq!(run.data_size, None);
        assert!(run.result.is_empty());
        assert_eq!(run.cluster_size_label(), "—");
    }

    #[test]
    fn cluster_size_prints_numbers_and_service_sizes_alike() {
        let sized = |json: &str| -> String {
            serde_json::from_str::<Run>(json)
                .unwrap()
                .cluster_size_label()
        };
        assert_eq!(sized(r#"{"cluster_size": 16}"#), "16");
        assert_eq!(sized(r#"{"cluster_size": "jumbo"}"#), "jumbo");
    }

    #[test]
    fn the_system_directory_comes_from_the_results_path() {
        assert_eq!(
            system_directory(Path::new("/c/duckdb/results/20250711/c6a.4xlarge.json")),
            "duckdb"
        );
        assert_eq!(system_directory(Path::new("shallow.json")), "");
    }

    #[test]
    fn the_dedup_key_is_the_directory_and_the_file_name() {
        assert_eq!(
            system_and_machine(Path::new("/c/duckdb/results/20250711/c6a.4xlarge.json")),
            Some(("duckdb".to_owned(), "c6a.4xlarge.json".to_owned()))
        );
    }

    #[test]
    fn the_date_directory_fills_in_for_a_document_that_omits_one() {
        assert_eq!(
            date_directory(Path::new("/c/duckdb/results/20250711/m.json")).as_deref(),
            Some("20250711")
        );
        assert_eq!(iso_date("20250711"), "2025-07-11");
        assert_eq!(iso_date("latest"), "latest");
    }

    #[test]
    fn a_failed_run_parses_into_its_error_and_writes_none_back_out() {
        let run: Run = serde_json::from_str(r#"{"error": "OOM during load"}"#).unwrap();
        assert_eq!(run.error.as_deref(), Some("OOM during load"));
        // `concurrent_error_ratio` is not the field under test.
        assert!(!serde_json::to_string(&run).unwrap().contains("\"error\""));
    }

    /// Covers all four selection rules at once: newest date directory
    /// wins, a failed run removes its system, an archival one is ignored,
    /// and a non-system directory is never read.
    #[test]
    fn the_field_is_the_newest_live_run_per_system_and_machine() {
        let root = std::env::temp_dir().join(format!("clickbench-field-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let write = |system: &str, date: &str, machine: &str, body: &str| {
            let directory = root.join(system).join("results").join(date);
            fs::create_dir_all(&directory).unwrap();
            fs::write(directory.join(format!("{machine}.json")), body).unwrap();
        };

        write(
            "alpha",
            "20250101",
            "c6a.4xlarge",
            r#"{"system": "Alpha", "machine": "c6a.4xlarge", "load_time": 1}"#,
        );
        write(
            "alpha",
            "20250202",
            "c6a.4xlarge",
            r#"{"system": "Alpha", "machine": "c6a.4xlarge", "load_time": 2}"#,
        );
        write(
            "beta",
            "20250101",
            "c6a.4xlarge",
            r#"{"system": "Beta", "machine": "c6a.4xlarge"}"#,
        );
        write("beta", "20250202", "c6a.4xlarge", r#"{"error": "crashed"}"#);
        write(
            "museum",
            "20250101",
            "c6a.4xlarge",
            r#"{"system": "Museum", "tags": ["historical"]}"#,
        );
        write("broken", "20250101", "c6a.4xlarge", "{not json");
        write(
            "versions",
            "20250101",
            "c6a.4xlarge",
            r#"{"system": "Versions"}"#,
        );

        let field = read_newest_per_system(&root).unwrap();
        let names: Vec<&str> = field.iter().map(|run| run.system.as_str()).collect();
        assert_eq!(names, ["Alpha"]);
        assert_eq!(field[0].load_time, Some(2.0));
        assert_eq!(field[0].date, "2025-02-02");

        fs::remove_dir_all(&root).unwrap();
    }
}
