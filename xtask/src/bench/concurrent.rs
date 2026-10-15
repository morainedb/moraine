//! Concurrent commit bursts: what a catalog does when several writers
//! commit at once.
//!
//! Every other workload here is one connection issuing one statement at a
//! time, so it measures commit *latency*. This one measures what the
//! catalog does under contention: N operating-system processes, each
//! attached to the same lake, released together and committing as fast as
//! they can. Separate processes rather than threads because that is what
//! a catalog shared between hosts actually faces, and because it is the
//! only form the other backends can be put under at all.

use std::{
    io::Write as _,
    path::Path,
    process::{Child, Command, Stdio},
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

use anyhow::{Context, bail, ensure};

/// One commit burst: `writers` processes committing `commits_each` times
/// apiece, each into a table of its own so the measurement is of
/// concurrent commits rather than of row-level conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Burst {
    pub writers: usize,
    pub commits_each: u64,
    /// Whether each writer's table carries an equality index, so every
    /// commit also maintains index entries.
    pub indexed: bool,
}

impl Burst {
    /// Splits `commits` over `writers`. Refuses an uneven split: two
    /// concurrency levels that did different amounts of work could not be
    /// compared against each other.
    pub fn plan(commits: u64, writers: usize) -> anyhow::Result<Self> {
        ensure!(writers > 0, "a burst needs at least one writer");
        let count = writers as u64;
        ensure!(
            commits.is_multiple_of(count),
            "{writers} writers do not divide {commits} commits evenly; pick a commit \
             count every concurrency level divides"
        );
        Ok(Self {
            writers,
            commits_each: commits / count,
            indexed: false,
        })
    }

    /// The same burst over indexed tables.
    pub fn with_index(self, indexed: bool) -> Self {
        Self { indexed, ..self }
    }

    /// Commits the whole burst performs.
    pub fn commits(self) -> u64 {
        self.commits_each * self.writers as u64
    }

    /// The table each writer appends to, created before any of them start.
    pub fn setup_sql(self) -> Vec<String> {
        (0..self.writers)
            .flat_map(|writer| {
                let table = format!("CREATE TABLE lake.main.writer_{writer} (id BIGINT);");
                let index = self.indexed.then(|| {
                    format!("CREATE INDEX writer_{writer}_by_id ON lake.main.writer_{writer}(id);")
                });
                std::iter::once(table).chain(index)
            })
            .collect()
    }

    /// One writer's statements. Each is its own autocommitted insert, so
    /// each is one catalog commit.
    pub fn writer_sql(self, writer: usize) -> Vec<String> {
        (0..self.commits_each)
            .map(|row| format!("INSERT INTO lake.main.writer_{writer} VALUES ({row});"))
            .collect()
    }

    /// Commits per second over `elapsed`.
    #[allow(clippy::cast_precision_loss)]
    pub fn commits_per_second(self, elapsed: Duration) -> f64 {
        self.commits() as f64 / elapsed.as_secs_f64()
    }

    /// The report phase this burst fills.
    pub fn phase(self) -> String {
        if self.indexed {
            format!("writers_{}_indexed", self.writers)
        } else {
            format!("writers_{}", self.writers)
        }
    }
}

/// Parses `--concurrency 1,2,4,8` into sorted, distinct writer counts.
pub fn parse_sweep(value: &str) -> anyhow::Result<Vec<usize>> {
    let mut counts: Vec<usize> = Vec::new();
    for field in value.split(',') {
        let count: usize = field
            .trim()
            .parse()
            .with_context(|| format!("`{field}` is not a writer count"))?;
        ensure!(count > 0, "a writer count must be at least one");
        if !counts.contains(&count) {
            counts.push(count);
        }
    }
    ensure!(!counts.is_empty(), "--concurrency needs at least one count");
    counts.sort_unstable();
    Ok(counts)
}

/// What a burst needs to start a writer: the CLI, the extension it
/// loads, and the `ATTACH` that puts it on the lake.
pub struct WriterContext<'a> {
    pub cli: &'a Path,
    pub extension: &'a Path,
    pub attach: &'a str,
    pub scratch: &'a Path,
}

/// How long a writer is given to attach before the burst is called off.
const READY_TIMEOUT: Duration = Duration::from_mins(2);

/// How often a writer checks whether it has finished attaching.
const READY_POLL: Duration = Duration::from_millis(1);

/// Runs one burst and returns the wall clock of the released window —
/// from the instant every writer is attached and waiting to the instant
/// the last one finishes committing.
///
/// Attaching, extension loading and table creation all happen before the
/// clock starts, so the measurement is of commits under contention and
/// nothing else.
pub fn run(context: &WriterContext<'_>, burst: Burst) -> anyhow::Result<Duration> {
    let barrier = Arc::new(Barrier::new(burst.writers + 1));
    let mut elapsed = Duration::ZERO;

    let outcomes = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..burst.writers)
            .map(|writer| {
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || commit_burst(context, burst, writer, &barrier))
            })
            .collect();

        // Returns once every writer is attached, or has failed trying —
        // a writer that cannot attach still releases the barrier, so one
        // backend refusing a second connection fails the burst instead of
        // hanging it.
        barrier.wait();
        let started = Instant::now();

        let outcomes: Vec<anyhow::Result<()>> = handles
            .into_iter()
            .map(|handle| match handle.join() {
                Ok(outcome) => outcome,
                Err(_) => Err(anyhow::anyhow!("a writer thread panicked")),
            })
            .collect();
        elapsed = started.elapsed();
        outcomes
    });

    for outcome in outcomes {
        outcome?;
    }
    Ok(elapsed)
}

/// One writer: attach, wait for the others, then commit.
fn commit_burst(
    context: &WriterContext<'_>,
    burst: Burst,
    writer: usize,
    barrier: &Barrier,
) -> anyhow::Result<()> {
    let prepared = attach_writer(context, writer);
    // Unconditional, so a writer that failed to attach does not strand
    // the ones that did.
    barrier.wait();

    let (mut child, log) = prepared?;
    let outcome = commit(&mut child, &burst.writer_sql(writer));
    let status = child.wait().context("waiting for a writer")?;
    outcome?;
    ensure!(
        status.success(),
        "a writer exited with {status}: {}",
        first_error(&log)
    );
    Ok(())
}

/// How much of a writer's log an error quotes. A writer that loses its
/// catalog mid-burst fails every remaining statement, so the first line
/// is the diagnosis and the rest is the same line again.
const LOG_EXCERPT_LINES: usize = 2;

/// The opening lines of a writer's log, flattened onto one line.
fn first_error(log: &Path) -> String {
    let text = std::fs::read_to_string(log).unwrap_or_default();
    let excerpt: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(LOG_EXCERPT_LINES)
        .collect();
    if excerpt.is_empty() {
        return "no diagnostics".to_owned();
    }
    excerpt.join(" / ")
}

/// Starts a writer and blocks until it has attached and is waiting on
/// stdin. Readiness is a file the writer writes, not a line on its
/// stdout: the CLI block-buffers a piped stdout, so a marker printed
/// there might not arrive until the process exits.
fn attach_writer(
    context: &WriterContext<'_>,
    writer: usize,
) -> anyhow::Result<(Child, std::path::PathBuf)> {
    let ready = context.scratch.join(format!("ready-{writer}"));
    let log = context.scratch.join(format!("writer-{writer}.log"));
    let _ = std::fs::remove_file(&ready);

    let errors =
        std::fs::File::create(&log).with_context(|| format!("creating {}", log.display()))?;
    let mut child = Command::new(context.cli)
        .args(["-unsigned", "-batch", "-noheader", "-list"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::from(errors))
        .spawn()
        .context("spawning a writer")?;

    let preamble = format!(
        "SET extension_directory='{}';\nINSTALL postgres;\nLOAD postgres;\nLOAD '{}';\n{}\n         COPY (SELECT 1) TO '{}' (FORMAT csv);\n",
        crate::duckdb::extension_install_directory().display(),
        context.extension.display(),
        context.attach,
        ready.display()
    );
    let stdin = child.stdin.as_mut().context("opening a writer's stdin")?;
    stdin
        .write_all(preamble.as_bytes())
        .context("writing a writer's preamble")?;
    stdin.flush().context("flushing a writer's preamble")?;

    let deadline = Instant::now() + READY_TIMEOUT;
    while !ready.exists() {
        if let Some(status) = child.try_wait().context("polling a writer")? {
            bail!(
                "a writer exited with {status} before it attached: {}",
                first_error(&log)
            );
        }
        ensure!(
            Instant::now() < deadline,
            "a writer did not attach within {READY_TIMEOUT:?}"
        );
        std::thread::sleep(READY_POLL);
    }
    Ok((child, log))
}

/// Feeds a released writer its statements and closes its stdin, which is
/// what makes the CLI run them and exit.
fn commit(child: &mut Child, statements: &[String]) -> anyhow::Result<()> {
    let mut stdin = child.stdin.take().context("taking a writer's stdin")?;
    for statement in statements {
        stdin
            .write_all(statement.as_bytes())
            .context("writing a commit")?;
        stdin.write_all(b"\n").context("writing a commit")?;
    }
    Ok(())
}

/// Creates the tables a burst's writers append to, in one session before
/// any writer starts.
pub fn create_tables(context: &WriterContext<'_>, burst: Burst) -> anyhow::Result<()> {
    let mut script = format!(
        "SET extension_directory='{}';\nINSTALL postgres;\nLOAD postgres;\nLOAD '{}';\n{}\n",
        crate::duckdb::extension_install_directory().display(),
        context.extension.display(),
        context.attach
    );
    for statement in burst.setup_sql() {
        script.push_str(&statement);
        script.push('\n');
    }

    let mut child = Command::new(context.cli)
        .args(["-unsigned", "-batch", "-noheader", "-list"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning the burst's setup session")?;
    child
        .stdin
        .take()
        .context("opening the setup session's stdin")?
        .write_all(script.as_bytes())
        .context("writing the setup script")?;
    let output = child
        .wait_with_output()
        .context("waiting for the setup session")?;
    ensure!(
        output.status.success(),
        "the burst's setup session failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_burst_splits_its_commits_evenly_across_writers() {
        let burst = Burst::plan(128, 8).unwrap();
        assert_eq!(burst.writers, 8);
        assert_eq!(burst.commits_each, 16);
        assert_eq!(burst.commits(), 128);
    }

    #[test]
    fn a_burst_that_would_not_divide_evenly_is_refused() {
        // Rounding would make two concurrency levels measure different
        // amounts of work and the ratio between them meaningless.
        let error = Burst::plan(100, 8).unwrap_err().to_string();
        assert!(error.contains("divide"), "{error}");
        assert!(Burst::plan(128, 0).is_err());
    }

    #[test]
    fn each_writer_gets_a_table_of_its_own() {
        let setup = Burst::plan(4, 2).unwrap().setup_sql();
        assert_eq!(setup.len(), 2);
        assert!(setup[0].contains("writer_0"), "{setup:?}");
        assert!(setup[1].contains("writer_1"), "{setup:?}");
    }

    #[test]
    fn a_writer_commits_one_row_at_a_time_into_its_own_table() {
        let statements = Burst::plan(4, 2).unwrap().writer_sql(1);
        assert_eq!(statements.len(), 2);
        assert!(
            statements[0].starts_with("INSERT INTO lake.main.writer_1"),
            "{statements:?}"
        );
        assert!(statements[0].contains("(0)"), "{statements:?}");
        assert!(statements[1].contains("(1)"), "{statements:?}");
    }

    #[test]
    fn throughput_is_the_whole_burst_over_the_wall_clock() {
        let burst = Burst::plan(128, 8).unwrap();
        let rate = burst.commits_per_second(Duration::from_secs(2));
        assert!((rate - 64.0).abs() < f64::EPSILON, "{rate}");
        assert!(burst.commits_per_second(Duration::ZERO).is_infinite());
    }

    #[test]
    fn the_phase_names_the_writer_count_so_the_report_can_sweep_it() {
        assert_eq!(Burst::plan(8, 8).unwrap().phase(), "writers_8");
        assert_eq!(Burst::plan(8, 1).unwrap().phase(), "writers_1");
    }

    #[test]
    fn an_error_quotes_the_opening_of_a_writers_log_and_nothing_more() {
        let log = std::env::temp_dir().join(format!("burst-log-{}", std::process::id()));
        std::fs::write(&log, "first\n\nsecond\nthird\nfourth\n").unwrap();
        assert_eq!(first_error(&log), "first / second");

        std::fs::write(&log, "").unwrap();
        assert_eq!(first_error(&log), "no diagnostics");
        std::fs::remove_file(&log).unwrap();
    }

    #[test]
    fn a_sweep_parses_as_a_sorted_list_of_distinct_counts() {
        assert_eq!(parse_sweep("8,1,4,1").unwrap(), vec![1, 4, 8]);
        assert!(parse_sweep("").is_err());
        assert!(parse_sweep("2,x").is_err());
        assert!(parse_sweep("0").is_err());
    }
}
