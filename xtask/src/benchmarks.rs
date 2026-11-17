//! Runs every benchmark behind a published number, one at a time.
//!
//! Each harness is a child process with a log of its own, so one failing
//! does not end a run that takes hours and a crash leaves its output
//! behind. They run in sequence because they contend for the machine and
//! would otherwise measure each other.

use std::{
    fs,
    os::unix::process::CommandExt as _,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use anyhow::Context;

use crate::duckdb::workspace_root;

/// Every backend a networked run compares unless `--backends` narrows
/// it, which is how a run proceeds while one server is down.
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

/// How long a harness may run before it is stopped and reported failed.
/// Generous: the point is to bound a wedged server, not to hurry a sweep.
const STEP_TIMEOUT: Duration = Duration::from_mins(90);

/// ClickBench loads tens of gigabytes before it measures anything.
const CLICKBENCH_TIMEOUT: Duration = Duration::from_hours(10);

/// How often a waiting run checks on the harness it started.
const POLL: Duration = Duration::from_secs(1);

/// One harness run: what to call it, what to run, whether it may see the
/// store environment, and how long it may take.
struct Step {
    name: &'static str,
    /// `None` runs this xtask; `Some(program)` runs that instead.
    program: Option<&'static str>,
    arguments: Vec<String>,
    local_only: bool,
    timeout: Duration,
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
            timeout: STEP_TIMEOUT,
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

    fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// Every harness, in the order it runs.
fn steps(output: &Path, clickbench: Option<&Path>, backends: &str) -> Vec<Step> {
    let mut steps = networked_steps(output, backends);
    steps.extend(local_steps());
    steps.extend(single_purpose_steps(output));
    steps.extend(core_steps());
    steps.extend(clickbench_steps(clickbench));
    steps
}

/// The five-column tables and the moraine-only sections, on whatever
/// store the environment names.
fn networked_steps(output: &Path, backends: &str) -> Vec<Step> {
    let cache = output.join("bench-cache").display().to_string();

    vec![
        Step::xtask(
            "catalogs-windowed",
            &[
                "bench",
                "--backends",
                backends,
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
                backends,
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
                backends,
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
                backends,
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
        )
        .with_timeout(CLICKBENCH_TIMEOUT),
        Step::xtask(
            "clickbench-report",
            &["clickbench", "report", "--clickbench", &checkout],
        ),
    ]
}

/// Where a run records itself, for an operator looking in.
fn lock_path(output: &Path) -> PathBuf {
    output.join("running.pid")
}

/// Refuses to start beside another run. Two on one machine share its
/// cores and measure each other, and their logs land on each other's.
///
/// The running processes are the authority, not a file: wiping the output
/// directory is the first thing anyone does when restarting a run, and a
/// lock that lives in it would go with it.
fn claim(output: &Path) -> anyhow::Result<()> {
    if let Some(pid) = other_run() {
        anyhow::bail!(
            "a run is already going as process {pid}; wait for it, or stop it with \
             `pkill -f 'xtask benchmarks'`"
        );
    }

    let lock = lock_path(output);
    fs::write(&lock, std::process::id().to_string())
        .with_context(|| format!("recording this run in {}", lock.display()))
}

/// Another `benchmarks` run on this machine, if one is going.
fn other_run() -> Option<u32> {
    let mine = std::process::id();
    let entries = fs::read_dir("/proc").ok()?;

    entries.flatten().find_map(|entry| {
        let pid: u32 = entry.file_name().to_str()?.parse().ok()?;
        if pid == mine {
            return None;
        }
        let command = fs::read(entry.path().join("cmdline")).ok()?;
        is_benchmarks_run(&command).then_some(pid)
    })
}

/// Whether a `/proc` command line is a `benchmarks` run of this xtask.
/// The arguments are NUL-separated, so the check is on the parts.
fn is_benchmarks_run(cmdline: &[u8]) -> bool {
    let mut parts = cmdline.split(|byte| *byte == 0);
    let (Some(program), Some(command)) = (parts.next(), parts.next()) else {
        return false;
    };
    program.ends_with(b"xtask") && command == b"benchmarks"
}

/// `benchmarks [--out <dir>] [--clickbench <checkout>]`.
pub fn run(arguments: &[String]) -> anyhow::Result<()> {
    let Options {
        output,
        clickbench,
        backends,
    } = parse_options(arguments)?;

    fs::create_dir_all(&output).with_context(|| format!("creating {}", output.display()))?;
    claim(&output)?;
    let xtask = std::env::current_exe().context("finding this xtask binary")?;
    let steps = steps(&output, clickbench.as_deref(), &backends);
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

/// What a run was asked for.
struct Options {
    output: PathBuf,
    clickbench: Option<PathBuf>,
    backends: String,
}

fn parse_options(arguments: &[String]) -> anyhow::Result<Options> {
    let mut output = workspace_root().join("target/benchmarks");
    let mut clickbench = None;
    let mut backends = ALL_BACKENDS.to_owned();

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
            "--backends" => backends = value()?,
            other => {
                anyhow::bail!("unknown argument `{other}`; valid: --out, --clickbench, --backends")
            }
        }
    }
    Ok(Options {
        output,
        clickbench,
        backends,
    })
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

    // Its own process group, so a harness that has to be stopped takes
    // the servers and sessions it started with it.
    let mut child = command
        .current_dir(workspace_root())
        .stdout(file)
        .stderr(errors)
        .process_group(0)
        .spawn()
        .with_context(|| format!("running {}", step.name))?;
    let status = wait_for(&mut child, step)?;

    // Every bench invocation overwrites one report, so it is kept under
    // this step's name before the next one lands on it.
    let report = workspace_root().join("target/bench/report.json");
    if report.exists() {
        let kept = output.join(format!("{}.json", step.name));
        fs::rename(&report, &kept).or_else(|_| fs::copy(&report, &kept).map(|_| ()))?;
    }
    Ok(status)
}

/// Waits for a harness, stopping it once its timeout passes. A wedged
/// server is the reason this exists: one harness waiting on a statement
/// that never answers would otherwise hold a run of hours.
fn wait_for(child: &mut std::process::Child, step: &Step) -> anyhow::Result<bool> {
    let deadline = Instant::now() + step.timeout;
    loop {
        if let Some(status) = child.try_wait().context("polling a harness")? {
            return Ok(status.success());
        }
        if Instant::now() >= deadline {
            println!(
                "notice: {} passed {} minutes and was stopped",
                step.name,
                step.timeout.as_secs() / 60
            );
            signal_group(child.id(), "TERM");
            std::thread::sleep(Duration::from_secs(5));
            signal_group(child.id(), "KILL");
            let _ = child.wait();
            return Ok(false);
        }
        std::thread::sleep(POLL);
    }
}

/// Signals a harness's whole process group, which is where the DuckDB
/// sessions and servers it started live.
fn signal_group(pid: u32, signal: &str) {
    let _ = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(format!("-{pid}"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_step_is_named_once_and_carries_a_command() {
        let steps = steps(Path::new("/tmp/out"), None, ALL_BACKENDS);
        let mut names: Vec<&str> = steps.iter().map(|step| step.name).collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "two steps share a name and a log");
        assert!(steps.iter().all(|step| !step.arguments.is_empty()));
    }

    #[test]
    fn only_the_published_local_table_hides_the_store_environment() {
        let steps = steps(Path::new("/tmp/out"), None, ALL_BACKENDS);
        let local: Vec<&str> = steps
            .iter()
            .filter(|step| step.local_only)
            .map(|step| step.name)
            .collect();
        assert_eq!(local, ["local-latency", "local-concurrency"]);
    }

    #[test]
    fn a_clickbench_checkout_adds_its_two_steps_last() {
        let without = steps(Path::new("/tmp/out"), None, ALL_BACKENDS).len();
        let steps = steps(
            Path::new("/tmp/out"),
            Some(Path::new("/tmp/ClickBench")),
            ALL_BACKENDS,
        );
        assert_eq!(steps.len(), without + 2);
        assert_eq!(steps[steps.len() - 2].name, "clickbench-run");
        assert_eq!(steps[steps.len() - 1].name, "clickbench-report");
    }

    #[test]
    fn clickbench_alone_gets_longer_than_the_others() {
        let steps = steps(
            Path::new("/tmp/out"),
            Some(Path::new("/tmp/ClickBench")),
            ALL_BACKENDS,
        );
        let long: Vec<&str> = steps
            .iter()
            .filter(|step| step.timeout > STEP_TIMEOUT)
            .map(|step| step.name)
            .collect();
        assert_eq!(long, ["clickbench-run"]);
    }

    #[test]
    fn a_benchmarks_command_line_is_told_from_its_neighbours() {
        assert!(is_benchmarks_run(b"target/debug/xtask\0benchmarks\0"));
        assert!(is_benchmarks_run(
            b"/home/me/moraine/target/debug/xtask\0benchmarks\0--out\0/t\0"
        ));

        // Neither a harness a run starts nor a test naming both words.
        assert!(!is_benchmarks_run(b"cargo\0test\0-p\0xtask\0benchmarks\0"));
        assert!(!is_benchmarks_run(
            b"target/debug/xtask\0bench\0--backends\0moraine\0"
        ));
        assert!(!is_benchmarks_run(b"cargo\0test\0-p\0moraine\0"));
        assert!(!is_benchmarks_run(b"xtask\0"));
    }

    #[test]
    fn a_run_records_itself_where_an_operator_will_look() {
        let output = std::env::temp_dir().join(format!("benchmarks-lock-{}", std::process::id()));
        let _ = fs::remove_dir_all(&output);
        fs::create_dir_all(&output).unwrap();

        claim(&output).unwrap();
        let recorded = fs::read_to_string(lock_path(&output)).unwrap();
        assert_eq!(recorded, std::process::id().to_string());

        fs::remove_dir_all(&output).unwrap();
    }

    #[test]
    fn options_default_to_every_backend_and_no_clickbench() {
        let options = parse_options(&[]).unwrap();
        assert!(options.output.ends_with("target/benchmarks"));
        assert!(options.clickbench.is_none());
        assert_eq!(options.backends, ALL_BACKENDS);
    }

    #[test]
    fn narrowing_the_backends_reaches_every_networked_harness() {
        let options =
            parse_options(&["--backends".to_owned(), "moraine,duckdb".to_owned()]).unwrap();
        assert_eq!(options.backends, "moraine,duckdb");

        let steps = steps(Path::new("/tmp/out"), None, &options.backends);
        let networked: Vec<&Step> = steps
            .iter()
            .filter(|step| step.name.starts_with("catalogs-"))
            .collect();
        assert!(!networked.is_empty());
        for step in networked {
            assert!(
                step.arguments
                    .iter()
                    .any(|argument| argument == "moraine,duckdb"),
                "{} kept every backend",
                step.name
            );
        }
    }
}
