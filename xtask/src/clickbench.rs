//! The `clickbench` task: runs ClickBench against a moraine-backed
//! DuckLake and ranks the result against the published numbers on
//! <https://benchmark.clickhouse.com>.
//!
//! The suite itself is upstream's. `benchmark/clickbench` is a ClickBench
//! system entry — the `install`/`load`/`query` contract every system in
//! that benchmark implements — and `run` drives it through upstream's own
//! `lib/benchmark-common.sh`, so moraine's numbers are produced by the
//! same driver as the ones they are compared against. What lives here is
//! only what upstream does not provide: building the extension the entry
//! loads, turning the driver's log into a result document, and the
//! comparison.

pub mod log;
pub mod publish;
pub mod report;
pub mod run;
pub mod score;

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail, ensure};

use crate::duckdb;

/// The ClickBench system entry, relative to the workspace root.
const ENTRY: &str = "benchmark/clickbench";

/// The directory name the entry takes inside a ClickBench checkout.
const SYSTEM_DIRECTORY: &str = "ducklake-moraine";

/// The upstream entry the suite is taken from. ClickBench is CC BY-NC-SA
/// and must not land in this tree, so the DuckLake table reuses the DuckDB
/// entry's column list and queries from the checkout instead.
const SUITE_SOURCE: &str = "duckdb";

/// The suite files staged from `SUITE_SOURCE`.
const SUITE_FILES: [&str; 2] = ["create.sql", "queries.sql"];

/// The machine every comparison defaults to: the instance type most
/// upstream entries publish, and the one the dashboard selects.
const REFERENCE_MACHINE: &str = "c6a.4xlarge";

/// Dispatches `cargo xtask clickbench <subcommand>`.
pub fn run(arguments: &[String]) -> anyhow::Result<()> {
    match arguments.first().map(String::as_str) {
        Some("prepare") => prepare(&arguments[1..]),
        Some("run") => run_suite(&arguments[1..]),
        Some("publish") => publish_log(&arguments[1..]),
        Some("report") => report(&arguments[1..]),
        Some(other) => {
            bail!("unknown clickbench subcommand `{other}`; valid: prepare, run, publish, report")
        }
        None => bail!(
            "usage: cargo xtask clickbench <prepare|run|publish|report>\n\
             \n\
             prepare  build the extension and CLI the entry's `install` step uses\n\
             run      drive the suite through a ClickBench checkout and write a result\n\
             publish  turn a driver log into a result document\n\
             report   rank a result against the published numbers"
        ),
    }
}

/// Builds the extension and the CLI that loads it, writing a shell
/// snippet naming both to `--env-file`. The build writes to stdout
/// itself, so the paths cannot go there.
///
/// The CLI is the one built beside the extension: nothing else carries the
/// version string the extension's footer demands.
fn prepare(arguments: &[String]) -> anyhow::Result<()> {
    let mut destination = None;
    let mut iterator = arguments.iter();
    while let Some(flag) = iterator.next() {
        match flag.as_str() {
            "--env-file" => {
                destination = Some(PathBuf::from(
                    iterator.next().context("`--env-file` needs a path")?,
                ));
            }
            other => bail!("unknown flag `{other}`; valid: --env-file"),
        }
    }

    let bundled = crate::ducklake_patch::prepare()?;
    let extension = duckdb::build_and_package_extension(&bundled)?;
    let cli = duckdb::ensure_duckdb_cli()?;
    let snippet = format!(
        "export MORAINE_EXTENSION={}\nexport DUCKDB_CLI={}\n",
        shell_single_quote(&extension.to_string_lossy()),
        shell_single_quote(&cli.to_string_lossy())
    );

    match destination {
        Some(path) => {
            fs::write(&path, snippet).with_context(|| format!("writing {}", path.display()))?;
            println!("wrote {}", path.display());
        }
        None => print!("{snippet}"),
    }
    Ok(())
}

/// Parsed `clickbench run` arguments.
#[derive(Debug)]
struct RunOptions {
    clickbench: PathBuf,
    machine: String,
}

fn parse_run_options(arguments: &[String]) -> anyhow::Result<RunOptions> {
    let mut clickbench = None;
    let mut machine = None;

    let mut iterator = arguments.iter();
    while let Some(flag) = iterator.next() {
        let mut value = || {
            iterator
                .next()
                .with_context(|| format!("flag `{flag}` needs a value"))
        };
        match flag.as_str() {
            "--clickbench" => clickbench = Some(PathBuf::from(value()?)),
            "--machine" => machine = Some(value()?.clone()),
            other => bail!("unknown flag `{other}`; valid: --clickbench, --machine"),
        }
    }

    Ok(RunOptions {
        clickbench: clickbench.context(
            "--clickbench <dir> is required: a checkout of \
             https://github.com/ClickHouse/ClickBench, whose lib/benchmark-common.sh \
             drives the run",
        )?,
        // No autodetect: a result labelled with the wrong machine is
        // worse than no result, because the dashboard compares within one
        // machine and would silently rank it against the wrong field.
        machine: machine.context(
            "--machine <name> is required: the instance type this run is on, e.g. \
             c6a.4xlarge. Comparisons only hold within one machine, so the label has \
             to be the real one",
        )?,
    })
}

/// Copies the entry into a ClickBench checkout, runs upstream's driver,
/// and writes the result document the comparison reads.
fn run_suite(arguments: &[String]) -> anyhow::Result<()> {
    let options = parse_run_options(arguments)?;
    let root = duckdb::workspace_root();
    let entry = root.join(ENTRY);

    let driver = options.clickbench.join("lib/benchmark-common.sh");
    ensure!(
        driver.exists(),
        "{} does not look like a ClickBench checkout: {} is missing",
        options.clickbench.display(),
        driver.display()
    );

    let staged = options.clickbench.join(SYSTEM_DIRECTORY);
    stage_entry(&entry, &options.clickbench, &staged)?;

    let log_path = root.join("target/clickbench/benchmark.log");
    let log = capture_driver(&staged, &log_path)?;

    let written = document_from_log(&entry, &log, &options.machine, &entry)
        .with_context(|| format!("reading the driver log at {}", log_path.display()))?;
    println!("wrote {}", written.display());
    Ok(())
}

/// Turns one driver log into a result document under `out`, taking the
/// metadata from `entry`'s `template.json`.
fn document_from_log(
    entry: &Path,
    log: &str,
    machine: &str,
    out: &Path,
) -> anyhow::Result<PathBuf> {
    let template =
        run::read(&entry.join("template.json")).context("reading the entry's template.json")?;
    let measurements = log::parse(log)?;
    let document = publish::document(&template, measurements, machine, &today()?);
    publish::write(out, &document)
}

/// Parsed `clickbench publish` arguments.
#[derive(Debug)]
struct PublishOptions {
    log: PathBuf,
    machine: String,
    out: Option<PathBuf>,
}

fn parse_publish_options(arguments: &[String]) -> anyhow::Result<PublishOptions> {
    let mut log = None;
    let mut machine = None;
    let mut out = None;

    let mut iterator = arguments.iter();
    while let Some(flag) = iterator.next() {
        let mut value = || {
            iterator
                .next()
                .with_context(|| format!("flag `{flag}` needs a value"))
        };
        match flag.as_str() {
            "--log" => log = Some(PathBuf::from(value()?)),
            "--machine" => machine = Some(value()?.clone()),
            "--out" => out = Some(PathBuf::from(value()?)),
            other => bail!("unknown flag `{other}`; valid: --log, --machine, --out"),
        }
    }

    Ok(PublishOptions {
        log: log.context("--log <path> is required: the driver's stdout for one run")?,
        machine: machine.context(
            "--machine <name> is required: the instance type the run is on, e.g. c6a.4xlarge",
        )?,
        out,
    })
}

/// Writes the result document for a log a driver already produced — a run
/// done by hand, or one whose log came back from elsewhere.
fn publish_log(arguments: &[String]) -> anyhow::Result<()> {
    let options = parse_publish_options(arguments)?;
    let entry = duckdb::workspace_root().join(ENTRY);
    let log = fs::read_to_string(&options.log)
        .with_context(|| format!("reading {}", options.log.display()))?;
    let out = options.out.unwrap_or_else(|| entry.clone());
    let written = document_from_log(&entry, &log, &options.machine, &out)?;
    println!("wrote {}", written.display());
    Ok(())
}

/// Mirrors the entry's files into `staged` inside `clickbench`, which is
/// where upstream's driver runs from, and adds the suite out of the
/// checkout's own `duckdb` entry.
///
/// The `results` directory stays behind: it is this repository's, not the
/// checkout's.
fn stage_entry(entry: &Path, clickbench: &Path, staged: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(staged).with_context(|| format!("creating {}", staged.display()))?;

    for file in fs::read_dir(entry).with_context(|| format!("reading {}", entry.display()))? {
        let source = file?.path();
        if source.is_dir() {
            continue;
        }
        stage_file(&source, staged)?;
    }

    for name in SUITE_FILES {
        let source = clickbench.join(SUITE_SOURCE).join(name);
        ensure!(
            source.exists(),
            "{} is missing from the checkout, so there is no query suite to run",
            source.display()
        );
        stage_file(&source, staged)?;
    }
    Ok(())
}

/// Copies one file into `staged`, keeping the executable bit on the
/// driver's contract scripts — the extensionless ones and `benchmark.sh`.
fn stage_file(source: &Path, staged: &Path) -> anyhow::Result<()> {
    let name = source
        .file_name()
        .with_context(|| format!("{} has no file name", source.display()))?;
    let destination = staged.join(name);
    fs::copy(source, &destination)
        .with_context(|| format!("copying {} to {}", source.display(), destination.display()))?;

    let executable = match source.extension() {
        None => true,
        Some(extension) => extension == "sh",
    };
    if executable {
        duckdb::make_executable(&destination)?;
    }
    Ok(())
}

/// Runs `./benchmark.sh` in `staged`, teeing its stdout to `log_path` so a
/// failed run leaves the log that explains it.
fn capture_driver(staged: &Path, log_path: &Path) -> anyhow::Result<String> {
    let directory = log_path
        .parent()
        .context("the log path has no parent directory")?;
    fs::create_dir_all(directory).with_context(|| format!("creating {}", directory.display()))?;

    // The driver's own progress goes to stderr and is left inherited, so
    // a long run is visible while it happens.
    let script = format!(
        "./benchmark.sh | tee {}",
        shell_single_quote(&log_path.to_string_lossy())
    );
    duckdb::run(
        Command::new("bash")
            .args(["-o", "pipefail", "-c", &script])
            .env("MORAINE_REPO", duckdb::workspace_root())
            .current_dir(staged),
    )?;
    fs::read_to_string(log_path).with_context(|| format!("reading {}", log_path.display()))
}

/// Wraps `value` in single quotes for `bash -c`.
fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

/// Today in UTC, `YYYY-MM-DD`, from `date` — the same clock upstream's
/// result directories are named after.
fn today() -> anyhow::Result<String> {
    let output = duckdb::command_output(Command::new("date").args(["-u", "+%Y-%m-%d"]))?;
    let date = output.trim().to_owned();
    ensure!(date.len() == 10, "`date -u` printed `{date}`");
    Ok(date)
}

/// Parsed `clickbench report` arguments.
#[derive(Debug)]
struct ReportOptions {
    clickbench: PathBuf,
    result: Option<PathBuf>,
    machine: String,
    metric: score::Metric,
    limit: usize,
    queries: bool,
}

fn parse_metric(name: &str) -> anyhow::Result<score::Metric> {
    match name {
        "combined" => Ok(score::Metric::Combined),
        "cold" => Ok(score::Metric::Cold),
        "hot" => Ok(score::Metric::Hot),
        "load" => Ok(score::Metric::Load),
        "size" => Ok(score::Metric::Size),
        other => bail!("unknown metric `{other}`; valid: combined, cold, hot, load, size"),
    }
}

fn parse_report_options(arguments: &[String]) -> anyhow::Result<ReportOptions> {
    let mut clickbench = None;
    let mut result = None;
    let mut machine = REFERENCE_MACHINE.to_owned();
    let mut metric = score::Metric::Combined;
    let mut limit = 30usize;
    let mut queries = false;

    let mut iterator = arguments.iter();
    while let Some(flag) = iterator.next() {
        let mut value = || {
            iterator
                .next()
                .with_context(|| format!("flag `{flag}` needs a value"))
        };
        match flag.as_str() {
            "--clickbench" => clickbench = Some(PathBuf::from(value()?)),
            "--result" => result = Some(PathBuf::from(value()?)),
            "--machine" => machine.clone_from(value()?),
            "--metric" => metric = parse_metric(value()?)?,
            "--limit" => {
                limit = value()?.parse().context("--limit takes a number of rows")?;
                ensure!(limit >= 1, "--limit must be at least 1");
            }
            "--queries" => queries = true,
            other => bail!(
                "unknown flag `{other}`; valid: --clickbench, --result, --machine, \
                 --metric, --limit, --queries"
            ),
        }
    }

    Ok(ReportOptions {
        clickbench: clickbench.context(
            "--clickbench <dir> is required: the checkout whose published results \
             supply the field to compare against",
        )?,
        result,
        machine,
        metric,
        limit,
        queries,
    })
}

/// Ranks a result against the published field for one machine.
fn report(arguments: &[String]) -> anyhow::Result<()> {
    let options = parse_report_options(arguments)?;
    let root = duckdb::workspace_root();

    let mut field = run::read_newest_per_system(&options.clickbench)?;
    let ours = match &options.result {
        Some(path) => Some(run::read(path)?),
        None => newest_own_result(&root.join(ENTRY), &options.machine)?,
    };

    // A run only belongs in the field if it is on the same machine:
    // upstream's dashboard compares within one instance type, and mixing
    // them would rank hardware rather than systems.
    if let Some(ours) = ours.clone() {
        ensure!(
            ours.machine == options.machine,
            "the result is labelled `{}` but the field is `{}`; pass \
             --machine {} to compare against that field instead",
            ours.machine,
            options.machine,
            ours.machine
        );
        field.retain(|run| run.system != ours.system);
        field.push(ours);
    }
    field.retain(|run| run.machine == options.machine);
    ensure!(
        !field.is_empty(),
        "no published results on `{}`; pass --machine with an instance type the \
         checkout carries, e.g. {REFERENCE_MACHINE}",
        options.machine
    );

    let highlight = ours.as_ref().map(|run| run.system.as_str());
    println!(
        "ClickBench, {} — relative {} (lower is better, 1.00 is the best in the field)\n",
        options.machine,
        metric_label(options.metric)
    );
    print!(
        "{}",
        report::render(&field, options.metric, highlight, options.limit)?
    );

    if options.queries {
        let system = highlight.context("--queries needs a result of ours to detail")?;
        println!("\nPer query, {system} against the best in the field:\n");
        print!(
            "{}",
            report::render_queries(&field, system, options.metric)?
        );
    }
    Ok(())
}

/// What the metric measures, for the table's caption.
fn metric_label(metric: score::Metric) -> &'static str {
    match metric {
        score::Metric::Combined => "time and data size",
        score::Metric::Cold => "cold-run time",
        score::Metric::Hot => "hot-run time",
        score::Metric::Load => "load time",
        score::Metric::Size => "data size",
    }
}

/// The newest result the entry has published for `machine`, if any.
fn newest_own_result(entry: &Path, machine: &str) -> anyhow::Result<Option<run::Run>> {
    if !entry.join("results").is_dir() {
        return Ok(None);
    }
    let runs = run::read_newest_per_system(
        entry
            .parent()
            .context("the entry has no parent directory")?,
    )?;
    Ok(runs.into_iter().find(|run| run.machine == machine))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_report_defaults_to_the_reference_machine_and_the_combined_metric() {
        let options = parse_report_options(&["--clickbench".to_owned(), "/c".to_owned()]).unwrap();
        assert_eq!(options.machine, REFERENCE_MACHINE);
        assert_eq!(options.metric, score::Metric::Combined);
        assert_eq!(options.limit, 30);
        assert!(!options.queries);
        assert!(options.result.is_none());
    }

    #[test]
    fn a_report_without_a_checkout_names_what_it_needs() {
        let error = parse_report_options(&[]).unwrap_err().to_string();
        assert!(error.contains("--clickbench"), "{error}");
    }

    #[test]
    fn metrics_parse_by_name_and_anything_else_is_refused() {
        assert_eq!(parse_metric("cold").unwrap(), score::Metric::Cold);
        assert_eq!(parse_metric("size").unwrap(), score::Metric::Size);
        assert!(parse_metric("warm").is_err());
    }

    #[test]
    fn a_run_insists_on_both_a_checkout_and_a_machine_label() {
        let error = parse_run_options(&["--clickbench".to_owned(), "/c".to_owned()])
            .unwrap_err()
            .to_string();
        assert!(error.contains("--machine"), "{error}");

        let error = parse_run_options(&["--machine".to_owned(), "c6a.4xlarge".to_owned()])
            .unwrap_err()
            .to_string();
        assert!(error.contains("--clickbench"), "{error}");

        let options = parse_run_options(&[
            "--clickbench".to_owned(),
            "/c".to_owned(),
            "--machine".to_owned(),
            "c6a.4xlarge".to_owned(),
        ])
        .unwrap();
        assert_eq!(options.clickbench, Path::new("/c"));
        assert_eq!(options.machine, "c6a.4xlarge");
    }

    #[test]
    fn publishing_insists_on_a_log_and_a_machine_label() {
        let error = parse_publish_options(&["--log".to_owned(), "/l".to_owned()])
            .unwrap_err()
            .to_string();
        assert!(error.contains("--machine"), "{error}");

        let options = parse_publish_options(&[
            "--log".to_owned(),
            "/l".to_owned(),
            "--machine".to_owned(),
            "c6a.4xlarge".to_owned(),
        ])
        .unwrap();
        assert_eq!(options.log, Path::new("/l"));
        assert!(options.out.is_none());
    }

    #[test]
    fn a_zero_limit_is_refused() {
        assert!(
            parse_report_options(&[
                "--clickbench".to_owned(),
                "/c".to_owned(),
                "--limit".to_owned(),
                "0".to_owned(),
            ])
            .is_err()
        );
    }

    /// The staged directory has to be what upstream's driver expects: the
    /// entry's files, the suite out of the checkout, the contract scripts
    /// executable, and this repository's own results left behind.
    #[test]
    fn staging_assembles_the_entry_and_the_suite_from_the_checkout() {
        let root = std::env::temp_dir().join(format!("clickbench-stage-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let entry = root.join("entry");
        let clickbench = root.join("ClickBench");
        fs::create_dir_all(entry.join("results/20260101")).unwrap();
        fs::create_dir_all(clickbench.join(SUITE_SOURCE)).unwrap();

        fs::write(entry.join("load"), "#!/bin/bash\n").unwrap();
        fs::write(entry.join("benchmark.sh"), "#!/bin/bash\n").unwrap();
        fs::write(entry.join("template.json"), "{}").unwrap();
        fs::write(entry.join("results/20260101/m.json"), "{}").unwrap();
        fs::write(clickbench.join(SUITE_SOURCE).join("create.sql"), "CREATE").unwrap();
        fs::write(clickbench.join(SUITE_SOURCE).join("queries.sql"), "SELECT").unwrap();

        let staged = clickbench.join(SYSTEM_DIRECTORY);
        stage_entry(&entry, &clickbench, &staged).unwrap();

        let mut staged_names: Vec<String> = fs::read_dir(&staged)
            .unwrap()
            .map(|file| file.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        staged_names.sort();
        assert_eq!(
            staged_names,
            [
                "benchmark.sh",
                "create.sql",
                "load",
                "queries.sql",
                "template.json"
            ]
        );

        let mode = |name: &str| {
            use std::os::unix::fs::PermissionsExt;
            fs::metadata(staged.join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o111
        };
        assert_ne!(mode("load"), 0, "contract scripts must be executable");
        assert_ne!(mode("benchmark.sh"), 0);
        assert_eq!(mode("template.json"), 0);

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn staging_without_the_suite_says_so_rather_than_running_nothing() {
        let root = std::env::temp_dir().join(format!("clickbench-nosuite-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let entry = root.join("entry");
        fs::create_dir_all(&entry).unwrap();
        fs::create_dir_all(root.join("ClickBench")).unwrap();

        let error = stage_entry(&entry, &root.join("ClickBench"), &root.join("staged"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("query suite"), "{error}");

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_path_with_a_quote_survives_the_shell() {
        assert_eq!(shell_single_quote("/tmp/a b"), "'/tmp/a b'");
        assert_eq!(shell_single_quote("it's"), r"'it'\''s'");
    }
}
