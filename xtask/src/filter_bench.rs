//! Compares indexed scan strategies on identical SQL and local Parquet data.

use std::{
    fmt::Write as _,
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, bail, ensure};

use crate::{catalog_target::sql_literal, duckdb};

fn execute(cli: &Path, preamble: &str, sql: &str) -> anyhow::Result<String> {
    let child = Command::new(cli)
        .args(["-unsigned", "-csv", "-bail"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let output = communicate(child, &format!("{preamble}\n{sql}\n"))?;
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

fn communicate(mut child: std::process::Child, sql: &str) -> anyhow::Result<std::process::Output> {
    let mut input = child.stdin.take().context("DuckDB stdin is unavailable")?;
    std::thread::scope(|scope| {
        let writer = scope.spawn(move || input.write_all(sql.as_bytes()));
        let output = child.wait_with_output();
        writer
            .join()
            .map_err(|_| anyhow::anyhow!("DuckDB stdin writer panicked"))??;
        Ok(output?)
    })
}

#[cfg(test)]
mod pipe_tests {
    use super::*;

    #[test]
    fn large_input_and_both_output_pipes_are_drained_together() {
        let child = Command::new("tee")
            .arg("/dev/stderr")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let sql = "x".repeat(1024 * 1024);
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || sender.send(communicate(child, &sql)).unwrap());
        let output = receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout.len(), 1024 * 1024);
        assert_eq!(output.stderr, output.stdout);
    }
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
        sql_literal(&extension.display().to_string()),
        sql_literal(&format!("ducklake:moraine:{}", store.display())),
        sql_literal(&data.display().to_string()),
        sql_literal(&data.display().to_string()),
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
        execute(&cli, &preamble, "CREATE INDEX by_a ON lake.main.t(a);")?;
    }
    let preamble = if parallel {
        format!("{preamble} SET threads=4;")
    } else {
        preamble
    };
    let mut builds = vec![("candidate", preamble.clone())];
    if let Some(baseline) = baseline {
        let baseline_preamble = preamble.replacen(
            &format!("LOAD {};", sql_literal(&extension.display().to_string())),
            &format!("LOAD {};", sql_literal(&baseline.display().to_string())),
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

fn cases() -> [(&'static str, Vec<i64>); 6] {
    [
        ("clustered_3264", (0..192).collect()),
        ("holes_3264", (0..192).map(|x| x * 4).collect()),
        ("split_3264", (0..96).chain(30000..30096).collect()),
        ("scattered_4080", (0..240).map(|x| x * 127).collect()),
        ("over_cap_4097", (0..241).collect()),
        ("broad_278528", (0..16384).collect()),
    ]
}

fn measure(
    cli: &Path,
    builds: &[(&str, String)],
    output: &Path,
    parallel: bool,
) -> anyhow::Result<()> {
    println!("case,query,build,read_threads,scan_path,first_profile_seconds,warm_median_seconds");
    for (case_index, (name, keys)) in cases().into_iter().enumerate() {
        let keys = keys
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        for (suffix, query) in [
            (
                "predicate",
                format!(
                    "SELECT count(*),sum(length(payload)) FROM lake.main.t WHERE a IN ({keys})"
                ),
            ),
            (
                "lookup",
                format!(
                    "SELECT count(*),sum(length(data.payload)) FROM lake.main.t data
                 JOIN moraine_index_in('lake','main','t','by_a',[{keys}]) hits
                 ON data.rowid = hits.row_id
                 AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id"
                ),
            ),
        ] {
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
                    "result changed for {name}/{suffix}"
                );
                let times = profile_times(&text)?;
                ensure!(
                    times.len() == modes.len(),
                    "unexpected profile count: {}",
                    times.len()
                );
                fs::write(output.join(format!("{name}-{suffix}-{label}.log")), &text)?;
                let path = if text.contains("Coverage costing ms") {
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
