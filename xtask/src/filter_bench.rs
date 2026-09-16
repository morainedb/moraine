//! Compares indexed scan strategies on identical SQL and local Parquet data.

use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail, ensure};

use crate::duckdb;

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn execute(cli: &Path, preamble: &str, sql: &str) -> anyhow::Result<String> {
    let output = Command::new(cli)
        .args(["-unsigned", "-csv", "-bail", "-c", preamble, "-c", sql])
        .output()?;
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

pub fn run(arguments: &[String]) -> anyhow::Result<()> {
    ensure!(
        arguments.len() >= 2,
        "usage: cargo xtask filter-bench <extension> <results-directory> [--embedded | --fixture <existing-results-directory>] [--baseline <extension>] [--parallel]"
    );
    let options = Options::parse(&arguments[2..])?;
    let embedded = options.embedded;
    let parallel = options.parallel;
    let extension = fs::canonicalize(&arguments[0])?;
    let baseline = options.baseline.map(fs::canonicalize).transpose()?;
    let output = Path::new(&arguments[1]);
    fs::create_dir(output)?;
    let output = fs::canonicalize(output)?;
    let cli = duckdb::ensure_duckdb_cli()?;
    let fixture = if let Some(fixture) = &options.fixture {
        fs::canonicalize(fixture)?
    } else {
        output.clone()
    };
    let store = fixture.join("store");
    let data = fixture.join("data");
    let preamble = format!(
        "LOAD {}; SET threads=1; ATTACH {} AS lake (DATA_PATH {}, META_DATA_PATH {}, DATA_INLINING_ROW_LIMIT {}, META_CACHE_PUTS false);",
        quoted(&extension.display().to_string()),
        quoted(&format!("ducklake:moraine:{}", store.display())),
        quoted(&data.display().to_string()),
        quoted(&data.display().to_string()),
        if embedded { 1_048_576 } else { 0 }
    );
    if options.fixture.is_none() {
        let partitions = if parallel { 4 } else { 1 };
        for partition in 0..partitions {
            let command = if partition == 0 {
                "CREATE TABLE lake.main.t AS"
            } else {
                "INSERT INTO lake.main.t"
            };
            execute(&cli, &preamble, &format!(
                "CALL ducklake_set_option('lake','parquet_row_group_size','2048');
                 {command} SELECT i // 17 AS a, md5(i::VARCHAR) AS payload FROM range(524288) t(i) WHERE i % {partitions}={partition};"))?;
            if embedded {
                execute(&cli, &preamble, "CALL ducklake_flush_inlined_data('lake');")?;
            }
        }
        execute(
            &cli,
            &preamble,
            "CALL moraine_index_create('lake','main','t','by_a',['a'],false);",
        )?;
    }
    let preamble = if parallel {
        format!("{preamble} SET threads=4;")
    } else {
        preamble
    };
    let mut builds = vec![("candidate", preamble.clone())];
    if let Some(baseline) = baseline {
        let baseline_preamble = preamble.replacen(
            &format!("LOAD {};", quoted(&extension.display().to_string())),
            &format!("LOAD {};", quoted(&baseline.display().to_string())),
            1,
        );
        builds.push(("baseline", baseline_preamble));
    }
    measure(&cli, &builds, &output, parallel)
}

#[derive(Default)]
struct Options {
    embedded: bool,
    parallel: bool,
    fixture: Option<PathBuf>,
    baseline: Option<PathBuf>,
}

impl Options {
    fn parse(arguments: &[String]) -> anyhow::Result<Self> {
        let mut options = Self::default();
        let mut arguments = arguments.iter();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--embedded" => options.embedded = true,
                "--parallel" => options.parallel = true,
                "--fixture" => {
                    options.fixture =
                        Some(arguments.next().context("--fixture needs a path")?.into());
                }
                "--baseline" => {
                    options.baseline = Some(
                        arguments
                            .next()
                            .context("--baseline needs an extension")?
                            .into(),
                    );
                }
                _ => bail!("unknown filter-bench option: {argument}"),
            }
        }
        ensure!(
            !options.embedded || options.fixture.is_none(),
            "--embedded and --fixture are mutually exclusive"
        );
        Ok(options)
    }
}

fn measure(
    cli: &Path,
    builds: &[(&str, String)],
    output: &Path,
    parallel: bool,
) -> anyhow::Result<()> {
    let cases = [
        ("clustered_3264", "range(192)"),
        ("holes_3264", "list_transform(range(192), x -> x * 4)"),
        ("split_3264", "list_concat(range(96),range(30000,30096))"),
        ("scattered_4080", "list_transform(range(240), x -> x * 127)"),
        ("over_cap_4097", "range(241)"),
        ("broad_278528", "range(16384)"),
    ];
    println!(
        "case,equality,build,read_threads,scan_path,first_profile_seconds,warm_median_seconds"
    );
    for (case_index, (name, keys)) in cases.into_iter().enumerate() {
        for equality in ["=", "IS NOT DISTINCT FROM"] {
            let query = format!(
                "SELECT count(*),sum(length(data.payload)) FROM lake.main.t data
                 JOIN moraine_index_in('lake','main','t','by_a',{keys}) hits
                 ON data.rowid {equality} hits.row_id
                 AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id"
            );
            let mut order: Vec<_> = builds.iter().collect();
            if case_index % 2 != 0 {
                order.reverse();
            }
            for (label, preamble) in order {
                let mut sql = String::new();
                let mut modes = Vec::new();
                for repetition in 0..6 {
                    let order = if parallel {
                        if repetition % 2 == 0 {
                            vec![1, 2]
                        } else {
                            vec![2, 1]
                        }
                    } else {
                        vec![1]
                    };
                    for threads in order {
                        modes.push(threads);
                        if parallel {
                            writeln!(sql, "SET moraine_summary_scan_threads={threads};")?;
                        }
                        writeln!(sql, "EXPLAIN ANALYZE {query}; {query};")?;
                    }
                }
                let text = execute(cli, preamble, &sql)?;
                let rows = name
                    .split('_')
                    .next_back()
                    .context("case count missing")?
                    .parse::<u64>()?;
                ensure!(
                    text.lines()
                        .filter(|line| *line == format!("{rows},{}", rows * 32))
                        .count()
                        == modes.len(),
                    "result changed for {name}/{equality}"
                );
                let times = profile_times(&text)?;
                ensure!(
                    times.len() == modes.len(),
                    "unexpected profile count: {}",
                    times.len()
                );
                let suffix = if equality == "=" {
                    "equal"
                } else {
                    "null_safe"
                };
                fs::write(output.join(format!("{name}-{suffix}-{label}.log")), &text)?;
                let path = if text.contains("MORAINE_SUMMARY_SCAN") {
                    "selective"
                } else {
                    "ordinary"
                };
                for threads in [1, 2] {
                    let selected: Vec<_> = times
                        .iter()
                        .zip(&modes)
                        .filter_map(|(&time, &mode)| (mode == threads).then_some(time))
                        .collect();
                    if selected.is_empty() {
                        continue;
                    }
                    let mut warm = selected[1..].to_vec();
                    warm.sort_by(f64::total_cmp);
                    println!(
                        "{name},{suffix},{label},{threads},{path},{:.6},{:.6}",
                        selected[0],
                        warm[warm.len() / 2]
                    );
                }
            }
        }
    }
    Ok(())
}

fn profile_times(text: &str) -> anyhow::Result<Vec<f64>> {
    text.split("Total Time: ")
        .skip(1)
        .map(|part| {
            part.split('s')
                .next()
                .context("profile duration missing")?
                .trim()
                .parse()
                .context("profile duration invalid")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparison_uses_explicit_builds_not_runtime_switches() {
        let arguments = [
            "--baseline",
            "reference/moraine.duckdb_extension",
            "--fixture",
            "fixture",
            "--parallel",
        ]
        .map(str::to_owned);
        let options = Options::parse(&arguments).unwrap();
        assert_eq!(
            options.baseline,
            Some(PathBuf::from("reference/moraine.duckdb_extension"))
        );
        assert_eq!(options.fixture, Some(PathBuf::from("fixture")));
        assert!(options.parallel);
        for retired in ["--ordinary", "--compare"] {
            assert!(Options::parse(&[retired.to_owned()]).is_err());
        }
        assert!(Options::parse(&["--baseline".to_owned()]).is_err());
        assert!(
            Options::parse(&[
                "--embedded".to_owned(),
                "--fixture".to_owned(),
                "fixture".to_owned()
            ])
            .is_err()
        );
    }
}
