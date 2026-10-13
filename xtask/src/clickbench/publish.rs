//! Turning a driver log into the `results/<date>/<machine>.json` document
//! ClickBench publishes, and writing it where the dashboard looks.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Context;

use super::{log::Measurements, run::Run};

/// Combines the entry's `template.json` metadata with one run's
/// measurements. `date` is the UTC day the run happened, `YYYY-MM-DD`.
pub fn document(template: &Run, measurements: Measurements, machine: &str, date: &str) -> Run {
    Run {
        date: date.to_owned(),
        machine: machine.to_owned(),
        load_time: measurements.load_time,
        data_size: measurements.data_size,
        concurrent_qps: measurements.concurrent_qps,
        concurrent_error_ratio: measurements.concurrent_error_ratio,
        result: measurements.result,
        ..template.clone()
    }
}

/// Serializes at upstream's four-space indent, so a document added to a
/// ClickBench checkout reads like the ones already there.
pub fn to_json(run: &Run) -> anyhow::Result<String> {
    let mut buffer = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
    let mut serializer = serde_json::Serializer::with_formatter(&mut buffer, formatter);
    serde::Serialize::serialize(run, &mut serializer).context("serializing the result")?;
    let mut json = String::from_utf8(buffer).context("the serialized result is not UTF-8")?;
    json.push('\n');
    Ok(json)
}

/// `<entry>/results/<YYYYMMDD>/<machine>.json`, the path ClickBench reads.
pub fn result_path(entry: &Path, run: &Run) -> PathBuf {
    entry
        .join("results")
        .join(run.date.replace('-', ""))
        .join(format!("{}.json", run.machine))
}

/// Writes the document to `result_path`, creating the date directory.
pub fn write(entry: &Path, run: &Run) -> anyhow::Result<PathBuf> {
    let path = result_path(entry, run);
    let directory = path
        .parent()
        .context("the result path has no date directory")?;
    fs::create_dir_all(directory).with_context(|| format!("creating {}", directory.display()))?;
    fs::write(&path, to_json(run)?).with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn template() -> Run {
        Run {
            system: "DuckLake (moraine)".to_owned(),
            cluster_size: serde_json::json!(1),
            tags: vec!["Rust".to_owned()],
            storage: Some("datalake".to_owned()),
            ..Run::default()
        }
    }

    fn measurements() -> Measurements {
        Measurements {
            load_time: Some(412.5),
            data_size: Some(25_024_802_816),
            concurrent_qps: None,
            concurrent_error_ratio: None,
            result: vec![Some([Some(1.0), Some(0.5), Some(0.4)])],
        }
    }

    #[test]
    fn the_document_keeps_the_templates_metadata_and_the_runs_numbers() {
        let run = document(&template(), measurements(), "c6a.4xlarge", "2026-10-13");
        assert_eq!(run.system, "DuckLake (moraine)");
        assert_eq!(run.storage.as_deref(), Some("datalake"));
        assert_eq!(run.machine, "c6a.4xlarge");
        assert_eq!(run.date, "2026-10-13");
        assert_eq!(run.load_time, Some(412.5));
        assert_eq!(run.result.len(), 1);
    }

    #[test]
    fn the_json_carries_the_published_field_order_and_indent() {
        let json = to_json(&document(
            &template(),
            measurements(),
            "c6a.4xlarge",
            "2026-10-13",
        ))
        .unwrap();
        // A key is a quoted token followed by a colon, which an array
        // element on its own line is not.
        let keys: Vec<&str> = json
            .lines()
            .filter_map(|line| line.trim().strip_prefix('"'))
            .filter_map(|rest| rest.split_once("\":"))
            .map(|(key, _)| key)
            .collect();
        assert_eq!(
            keys,
            [
                "system",
                "date",
                "machine",
                "cluster_size",
                "proprietary",
                "hardware",
                "tuned",
                "tags",
                "storage",
                "load_time",
                "data_size",
                "concurrent_qps",
                "concurrent_error_ratio",
                "result",
            ]
        );
        assert!(json.contains("\n    \"system\""), "{json}");
        assert!(json.ends_with("}\n"), "{json}");
    }

    #[test]
    fn a_template_without_a_storage_class_omits_the_field() {
        let bare = Run {
            system: "Bare".to_owned(),
            ..Run::default()
        };
        let json = to_json(&document(
            &bare,
            measurements(),
            "c6a.4xlarge",
            "2026-10-13",
        ))
        .unwrap();
        assert!(!json.contains("storage"), "{json}");
    }

    #[test]
    fn the_result_path_strips_the_dashes_out_of_the_date() {
        let run = document(&template(), measurements(), "c6a.4xlarge", "2026-10-13");
        assert_eq!(
            result_path(Path::new("benchmark/clickbench"), &run),
            Path::new("benchmark/clickbench/results/20261013/c6a.4xlarge.json")
        );
    }
}
