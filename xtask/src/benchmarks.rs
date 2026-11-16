//! Runs every benchmark behind a published number, one at a time.
//!
//! Each harness is a child process with a log of its own, so one failing
//! does not end a run that takes hours and a crash leaves its output
//! behind. They run in sequence because they contend for the machine and
//! would otherwise measure each other.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::Context;

use crate::duckdb::workspace_root;

/// Every backend a networked run compares.
const ALL_BACKENDS: &str = "moraine,moraine_express,duckdb,postgres,quack";

/// The store variables a local-disk run must not see. The published
/// "by the numbers" table is a local claim, and the harness provisions
/// its own Postgres and Quack when the environment names none.
const STORE_VARIABLES: [&str; 7] = [
    "MORAINE_S3_BUCKET",
    "MORAINE_S3_EXPRESS_BUCKET",
    "MORAINE_S3_CREDENTIAL_CHAIN",
    "MORAINE_BENCH_POSTGRES",
    "MORAINE_BENCH_QUACK",
    "MORAINE_BENCH_QUACK_TOKEN",
    "MORAINE_BENCH_QUACK_CA_CERT",
];

/// One harness run: what to call it, what to run, and whether it may see
/// the store environment.
struct Step {
    name: &'static str,
    /// `None` runs this xtask; `Some(program)` runs that instead.
    program: Option<&'static str>,
    arguments: Vec<String>,
    local_only: bool,
}

impl Step {
    fn xtask(name: &'static str, arguments: &[&str]) -> Self {
        Self {
            name,
            program: None,
            arguments: arguments
                .iter()
                .map(|argument| (*argument).to_owned())
                .collect(),
            local_only: false,
        }
    }

    fn cargo(name: &'static str, arguments: &[&str]) -> Self {
        Self {
            program: Some("cargo"),
            ..Self::xtask(name, arguments)
        }
    }

    fn local(mut self) -> Self {
        self.local_only = true;
        self
    }
}

/// Every harness, in the order it runs.
fn steps(output: &Path, clickbench: Option<&Path>) -> Vec<Step> {
    let mut steps = networked_steps(output);
    steps.extend(local_steps());
    steps.extend(single_purpose_steps(output));
    steps.extend(core_steps());
    steps.extend(clickbench_steps(clickbench));
    steps
}

/// The five-column tables and the moraine-only sections, on whatever
/// store the environment names.
fn networked_steps(output: &Path) -> Vec<Step> {
    let cache = output.join("bench-cache").display().to_string();

    vec![
        Step::xtask(
            "catalogs-windowed",
            &[
                "bench",
                "--backends",
                ALL_BACKENDS,
                "--seconds",
                "5",
                "--concurrency",
                "1,2,4,8,16",
            ],
        ),
        Step::xtask(
            "catalogs-counted",
            &[
                "bench",
                "--backends",
                ALL_BACKENDS,
                "--workloads",
                "concurrent_connections",
                "--commits",
                "128",
                "--concurrency",
                "1,4,8,16",
                "--repeat",
                "3",
            ],
        ),
        Step::xtask(
            "catalogs-warm-attach",
            &[
                "bench",
                "--backends",
                ALL_BACKENDS,
                "--workloads",
                "warm_attach",
                "--repeat",
                "3",
            ],
        ),
        Step::xtask(
            "catalogs-cached",
            &[
                "bench",
                "--backends",
                ALL_BACKENDS,
                "--seconds",
                "5",
                "--concurrency",
                "1,2,4,8,16",
                "--cache-dir",
                &cache,
            ],
        ),
        Step::xtask(
            "moraine-latency",
            &[
                "bench",
                "--backends",
                "moraine,moraine_express",
                "--workloads",
                "small_commits,wide_small_commits,first_inline_write",
                "--repeat",
                "9",
            ],
        ),
        Step::xtask(
            "moraine-ceiling",
            &[
                "bench",
                "--backends",
                "moraine,moraine_express",
                "--workloads",
                "concurrent_connections",
                "--seconds",
                "5",
                "--concurrency",
                "1,2,4,8,16,32,64",
            ],
        ),
    ]
}

/// The local-disk table the site publishes.
fn local_steps() -> Vec<Step> {
    vec![
        Step::xtask(
            "local-latency",
            &[
                "bench",
                "--backends",
                "moraine,duckdb,postgres",
                "--workloads",
                "bulk_load,small_commits,warm_attach",
                "--repeat",
                "3",
            ],
        )
        .local(),
        Step::xtask(
            "local-concurrency",
            &[
                "bench",
                "--backends",
                "moraine,duckdb,postgres",
                "--workloads",
                "concurrent_connections",
                "--commits",
                "128",
                "--concurrency",
                "1,4,8,16",
                "--repeat",
                "3",
            ],
        )
        .local(),
    ]
}

/// The harnesses that answer one question each.
fn single_purpose_steps(output: &Path) -> Vec<Step> {
    let filter_results = output.join("filter").display().to_string();
    let reader_cache = output.join("reader-cache").display().to_string();
    let extension = workspace_root()
        .join("build/release/extension/moraine/moraine.duckdb_extension")
        .display()
        .to_string();

    vec![
        Step::xtask(
            "commit-bench",
            &["commit-bench", "--files", "16,128", "--commits", "7"],
        ),
        Step::xtask(
            "reader-bench",
            &[
                "reader-bench",
                "--files",
                "2000,20000",
                "--repeat",
                "5",
                "--cache-dir",
                &reader_cache,
            ],
        ),
        Step::xtask("locate-bench", &["locate-bench"]),
        Step::xtask(
            "filter-bench",
            &["filter-bench", &extension, &filter_results],
        ),
        Step::xtask("core-endpoint", &["s3"]),
        // The same suite the `Real S3 benchmark` workflow runs, against
        // whatever bucket the environment names rather than a loopback
        // server. Skipped by its own harness when none is named.
        Step::cargo(
            "core-endpoint-aws",
            &[
                "test",
                "-p",
                "moraine",
                "--test",
                "object_storage",
                "--release",
                "--locked",
                "--",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ],
        ),
    ]
}

/// What the core measures of itself, through its own ignored tests.
fn core_steps() -> Vec<Step> {
    vec![
        Step::cargo(
            "core-integration",
            &[
                "test",
                "-p",
                "moraine",
                "--test",
                "it",
                "--release",
                "--locked",
                "--",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
                "measure_",
            ],
        ),
        Step::cargo(
            "core-internal",
            &[
                "test",
                "-p",
                "moraine",
                "--lib",
                "--release",
                "--locked",
                "--",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
                "measure_",
            ],
        ),
        Step::cargo(
            "staged-build-memory",
            &[
                "test",
                "-p",
                "moraine",
                "--test",
                "staged_build_memory",
                "--release",
                "--locked",
                "--",
                "--ignored",
                "--nocapture",
            ],
        ),
        Step::cargo(
            "conflict-check",
            &[
                "test",
                "-p",
                "moraine-duckdb",
                "--test",
                "ducklake_load",
                "--release",
                "--locked",
                "--",
                "--include-ignored",
                "--nocapture",
                "measure_staged_conflict_check_by_retained_snapshots",
            ],
        ),
    ]
}

/// ClickBench, only when a checkout names itself: it needs tens of
/// gigabytes and hours, which the rest of this does not.
fn clickbench_steps(checkout: Option<&Path>) -> Vec<Step> {
    let Some(checkout) = checkout else {
        return Vec::new();
    };
    let checkout = checkout.display().to_string();

    vec![
        Step::xtask(
            "clickbench-run",
            &[
                "clickbench",
                "run",
                "--clickbench",
                &checkout,
                "--machine",
                "c6i.2xlarge",
            ],
        ),
        Step::xtask(
            "clickbench-report",
            &["clickbench", "report", "--clickbench", &checkout],
        ),
    ]
}

/// Where a run records the process holding the output directory.
fn lock_path(output: &Path) -> PathBuf {
    output.join("running.pid")
}

/// Refuses to start beside a run already using this output directory.
/// Two runs on one machine share its cores and measure each other, and
/// their logs land on each other's.
fn claim(output: &Path) -> anyhow::Result<()> {
    let lock = lock_path(output);
    if let Ok(holder) = fs::read_to_string(&lock) {
        let holder = holder.trim();
        if holder.parse::<u32>().is_ok_and(is_running) {
            anyhow::bail!(
                "a run is already going as process {holder}; wait for it, or stop it and \
                 delete {}",
                lock.display()
            );
        }
    }

    fs::write(&lock, std::process::id().to_string())
        .with_context(|| format!("claiming {}", lock.display()))
}

/// Whether a process by that id is alive. Asked of `/proc` where there
/// is one, and of `ps` elsewhere: `kill -0` reads an out-of-range id as
/// every process and answers yes.
fn is_running(pid: u32) -> bool {
    let processes = Path::new("/proc");
    if processes.is_dir() {
        return processes.join(pid.to_string()).exists();
    }

    Command::new("ps")
        .args(["-p", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// `benchmarks [--out <dir>] [--clickbench <checkout>]`.
pub fn run(arguments: &[String]) -> anyhow::Result<()> {
    let (output, clickbench) = parse_options(arguments)?;

    fs::create_dir_all(&output).with_context(|| format!("creating {}", output.display()))?;
    claim(&output)?;
    let xtask = std::env::current_exe().context("finding this xtask binary")?;
    let steps = steps(&output, clickbench.as_deref());
    println!(
        "ok: {} harnesses, one at a time, logs under {}",
        steps.len(),
        output.display()
    );

    let mut failed: Vec<&str> = Vec::new();
    for step in &steps {
        if run_step(step, &xtask, &output)? {
            println!("ok: {}", step.name);
        } else {
            println!("notice: {} failed; see {}.log", step.name, step.name);
            failed.push(step.name);
        }
    }

    let _ = fs::remove_file(lock_path(&output));
    if failed.is_empty() {
        println!("ok: every harness ran; logs under {}", output.display());
    } else {
        println!(
            "notice: {} of {} harnesses failed: {}",
            failed.len(),
            steps.len(),
            failed.join(", ")
        );
    }
    Ok(())
}

fn parse_options(arguments: &[String]) -> anyhow::Result<(PathBuf, Option<PathBuf>)> {
    let mut output = workspace_root().join("target/benchmarks");
    let mut clickbench = None;

    let mut iterator = arguments.iter();
    while let Some(argument) = iterator.next() {
        let mut value = || {
            iterator
                .next()
                .cloned()
                .with_context(|| format!("{argument} needs a value"))
        };
        match argument.as_str() {
            "--out" => output = PathBuf::from(value()?),
            "--clickbench" => clickbench = Some(PathBuf::from(value()?)),
            other => anyhow::bail!("unknown argument `{other}`; valid: --out, --clickbench"),
        }
    }
    Ok((output, clickbench))
}

/// Runs one harness to its own log, keeping whatever report it wrote.
/// `Ok(false)` is a harness that failed, which the run survives.
fn run_step(step: &Step, xtask: &Path, output: &Path) -> anyhow::Result<bool> {
    let log = output.join(format!("{}.log", step.name));
    let file = fs::File::create(&log).with_context(|| format!("creating {}", log.display()))?;
    let errors = file.try_clone().context("cloning a log handle")?;

    let mut command = match step.program {
        Some(program) => Command::new(program),
        None => Command::new(xtask),
    };
    command.args(&step.arguments);
    if step.local_only {
        for variable in STORE_VARIABLES {
            command.env_remove(variable);
        }
    }

    let status = command
        .current_dir(workspace_root())
        .stdout(file)
        .stderr(errors)
        .status()
        .with_context(|| format!("running {}", step.name))?;

    // Every bench invocation overwrites one report, so it is kept under
    // this step's name before the next one lands on it.
    let report = workspace_root().join("target/bench/report.json");
    if report.exists() {
        let kept = output.join(format!("{}.json", step.name));
        fs::rename(&report, &kept).or_else(|_| fs::copy(&report, &kept).map(|_| ()))?;
    }
    Ok(status.success())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_step_is_named_once_and_carries_a_command() {
        let steps = steps(Path::new("/tmp/out"), None);
        let mut names: Vec<&str> = steps.iter().map(|step| step.name).collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "two steps share a name and a log");
        assert!(steps.iter().all(|step| !step.arguments.is_empty()));
    }

    #[test]
    fn only_the_published_local_table_hides_the_store_environment() {
        let steps = steps(Path::new("/tmp/out"), None);
        let local: Vec<&str> = steps
            .iter()
            .filter(|step| step.local_only)
            .map(|step| step.name)
            .collect();
        assert_eq!(local, ["local-latency", "local-concurrency"]);
    }

    #[test]
    fn a_clickbench_checkout_adds_its_two_steps_last() {
        let without = steps(Path::new("/tmp/out"), None).len();
        let steps = steps(Path::new("/tmp/out"), Some(Path::new("/tmp/ClickBench")));
        assert_eq!(steps.len(), without + 2);
        assert_eq!(steps[steps.len() - 2].name, "clickbench-run");
        assert_eq!(steps[steps.len() - 1].name, "clickbench-report");
    }

    #[test]
    fn a_live_run_keeps_a_second_one_out_and_a_dead_one_does_not() {
        let output = std::env::temp_dir().join(format!("benchmarks-lock-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        fs::create_dir_all(&output).unwrap();

        claim(&output).unwrap();
        let error = claim(&output).unwrap_err().to_string();
        assert!(error.contains("already going"), "{error}");

        // A run killed mid-flight leaves its id behind; the next one
        // takes the directory rather than refusing forever.
        fs::write(lock_path(&output), u32::MAX.to_string()).unwrap();
        assert!(!is_running(u32::MAX));
        claim(&output).unwrap();

        fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn options_default_to_the_workspace_and_no_clickbench() {
        let (output, clickbench) = parse_options(&[]).unwrap();
        assert!(
            output.ends_with("target/benchmarks"),
            "{}",
            output.display()
        );
        assert!(clickbench.is_none());

        let arguments = ["--out", "/tmp/here", "--clickbench", "/tmp/ClickBench"]
            .map(str::to_owned)
            .to_vec();
        let (output, clickbench) = parse_options(&arguments).unwrap();
        assert_eq!(output, Path::new("/tmp/here"));
        assert_eq!(clickbench.as_deref(), Some(Path::new("/tmp/ClickBench")));
    }
}
