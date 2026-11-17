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
    sync::{Arc, Barrier, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, bail, ensure};

/// What bounds a burst's work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bound {
    /// Each writer commits this many times and the burst ends with the
    /// last of them.
    Commits(u64),
    /// Every writer commits until the window closes, and the burst
    /// reports what landed inside it. The measurement a scaling curve
    /// wants: start-up and the first flush are a fixed share of a window,
    /// where in a short count they are most of it.
    Window(Duration),
}

/// One commit burst: `writers` processes committing under `bound`, each
/// into a table of its own so the measurement is of concurrent commits
/// rather than of row-level conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Burst {
    pub writers: usize,
    pub bound: Bound,
    /// Whether each writer's table carries an equality index, so every
    /// commit also maintains index entries.
    pub indexed: bool,
    /// Whether every writer commits to one table instead of its own.
    /// Separate tables measure catalog contention alone; one table adds
    /// the conflict a catalog's own protocol has to resolve.
    pub shared: bool,
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
            bound: Bound::Commits(commits / count),
            indexed: false,
            shared: false,
        })
    }

    /// Every writer commits for `window`. Any writer count works, since
    /// nothing has to divide evenly.
    pub fn windowed(window: Duration, writers: usize) -> anyhow::Result<Self> {
        ensure!(writers > 0, "a burst needs at least one writer");
        ensure!(!window.is_zero(), "a burst window must be longer than zero");
        Ok(Self {
            writers,
            bound: Bound::Window(window),
            indexed: false,
            shared: false,
        })
    }

    /// The same burst over indexed tables.
    pub fn with_index(self, indexed: bool) -> Self {
        Self { indexed, ..self }
    }

    /// The same burst with every writer on one table.
    pub fn with_shared(self, shared: bool) -> Self {
        Self { shared, ..self }
    }

    /// The table a writer commits to.
    fn table(self, writer: usize) -> String {
        if self.shared {
            "lake.main.shared".to_owned()
        } else {
            format!("lake.main.writer_{writer}")
        }
    }

    /// Commits the burst is planned to perform, or `None` when the window
    /// decides and only the run can say.
    pub fn planned_commits(self) -> Option<u64> {
        match self.bound {
            Bound::Commits(each) => Some(each * self.writers as u64),
            Bound::Window(_) => None,
        }
    }

    /// The table each writer appends to, created before any of them start.
    pub fn setup_sql(self) -> Vec<String> {
        if self.shared {
            // The writer column is what lets a shared table still say who
            // committed what, which is how a starved writer shows up.
            let mut statements =
                vec!["CREATE TABLE lake.main.shared (writer BIGINT, id BIGINT);".to_owned()];
            if self.indexed {
                statements.push("CREATE INDEX shared_by_id ON lake.main.shared(id);".to_owned());
            }
            return statements;
        }

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

    /// One writer's statements under a commit count. Each is its own
    /// autocommitted insert, so each is one catalog commit.
    pub fn writer_sql(self, writer: usize) -> Vec<String> {
        let Bound::Commits(each) = self.bound else {
            return Vec::new();
        };
        (0..each).map(|row| self.insert_sql(writer, row)).collect()
    }

    /// The statement a writer repeats, with `row` as its value.
    pub fn insert_sql(self, writer: usize, row: u64) -> String {
        if self.shared {
            format!("INSERT INTO lake.main.shared VALUES ({writer}, {row});")
        } else {
            format!("INSERT INTO {} VALUES ({row});", self.table(writer))
        }
    }

    /// One row per writer's table, in writer order, for the session that
    /// counts what landed. Per writer rather than summed: a writer that
    /// committed nothing is a refusal to report, not a zero to absorb.
    pub fn count_sql(self) -> String {
        let counts: Vec<String> = (0..self.writers)
            .map(|writer| {
                if self.shared {
                    format!("SELECT count(*) FROM lake.main.shared WHERE writer = {writer}")
                } else {
                    format!("SELECT count(*) FROM {}", self.table(writer))
                }
            })
            .collect();
        format!("{};", counts.join(" UNION ALL "))
    }

    /// The report phase this burst fills.
    pub fn phase(self) -> String {
        let mut phase = format!("writers_{}", self.writers);
        if self.shared {
            phase.push_str("_shared");
        }
        if self.indexed {
            phase.push_str("_indexed");
        }
        phase
    }
}

/// Commits per second, from what a run committed and how long its window
/// was open.
#[allow(clippy::cast_precision_loss)]
pub fn commits_per_second(commits: u64, elapsed: Duration) -> f64 {
    commits as f64 / elapsed.as_secs_f64()
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
    /// Untimed statements the store needs before `attach` (none locally).
    pub preamble: &'a str,
    pub attach: &'a str,
    pub scratch: &'a Path,
}

/// How long a writer has to exit on its own before it is killed. Long
/// enough for the CLI to flush what it was writing, short beside a
/// window.
const TERM_GRACE: Duration = Duration::from_millis(500);

/// Signals every running writer. `kill(1)` rather than a crate: the only
/// signal `std` sends is `SIGKILL`, which takes the diagnostics with it.
fn signal_writers(running: &[Mutex<Option<Child>>], signal: &str) {
    for slot in running {
        if let Ok(slot) = slot.lock()
            && let Some(child) = slot.as_ref()
        {
            let _ = Command::new("kill")
                .arg(format!("-{signal}"))
                .arg(child.id().to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

/// How long a writer is given to attach before the burst is called off.
const READY_TIMEOUT: Duration = Duration::from_mins(2);

/// How often a writer checks whether it has finished attaching.
const READY_POLL: Duration = Duration::from_millis(1);

/// What a burst did: the wall clock of the released window and the
/// commits that landed inside it.
#[derive(Debug, Clone, Copy)]
pub struct Outcome {
    pub elapsed: Duration,
    pub commits: u64,
}

/// Runs one burst and reports the released window — from the instant
/// every writer is attached and waiting to the instant the last one
/// stops committing.
///
/// Attaching, extension loading and table creation all happen before the
/// clock starts, so the measurement is of commits under contention and
/// nothing else. Under a window, the writers are stopped where they
/// stand and the catalog is asked what landed.
pub fn run(context: &WriterContext<'_>, burst: Burst) -> anyhow::Result<Outcome> {
    let barrier = Arc::new(Barrier::new(burst.writers + 1));
    let running: Vec<Mutex<Option<Child>>> = (0..burst.writers).map(|_| Mutex::new(None)).collect();
    let mut elapsed = Duration::ZERO;

    let outcomes = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..burst.writers)
            .map(|writer| {
                let barrier = Arc::clone(&barrier);
                let running = &running;
                scope.spawn(move || commit_burst(context, burst, writer, &barrier, running))
            })
            .collect();

        // Returns once every writer is attached, or has failed trying —
        // a writer that cannot attach still releases the barrier, so one
        // backend refusing a second connection fails the burst instead of
        // hanging it.
        barrier.wait();
        let started = Instant::now();

        // Closing the window stops the writers, which is also what
        // unblocks a feeder stalled on a full pipe: the CLI acknowledges
        // nothing, so the pipe is the only back-pressure. They are asked
        // first and killed after, because a writer killed outright loses
        // the buffered diagnostics that say why it committed nothing.
        if let Bound::Window(window) = burst.bound {
            std::thread::sleep(window);
            elapsed = started.elapsed();
            signal_writers(&running, "TERM");
            std::thread::sleep(TERM_GRACE);
            signal_writers(&running, "KILL");
        }

        let outcomes: Vec<anyhow::Result<()>> = handles
            .into_iter()
            .map(|handle| match handle.join() {
                Ok(outcome) => outcome,
                Err(_) => Err(anyhow::anyhow!("a writer thread panicked")),
            })
            .collect();
        if matches!(burst.bound, Bound::Commits(_)) {
            elapsed = started.elapsed();
        }
        outcomes
    });

    for outcome in outcomes {
        outcome?;
    }

    if let Some(commits) = burst.planned_commits() {
        return Ok(Outcome { elapsed, commits });
    }

    let landed = count_commits(context, burst)?;
    if let Some(writer) = landed.iter().position(|&commits| commits == 0) {
        let spread = landed
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let log = context.scratch.join(format!("writer-{writer}.log"));

        // A writer that complained is one the catalog would not take: a
        // killed writer leaves no exit status, so its log is the only
        // evidence. One that said nothing while its competitors committed
        // was starved, which is a property of the catalog under load and
        // not a reason to throw the burst's throughput away.
        if let Some(error) = first_error(&log) {
            bail!(
                "writer {writer} committed nothing in the window \
                 (per writer: {spread}): {error}"
            );
        }
        println!(
            "notice: writer {writer} was starved, committing nothing while the others ran \
             (per writer: {spread}); the rate reported is the whole burst's"
        );
    }
    Ok(Outcome {
        elapsed,
        commits: landed.iter().sum(),
    })
}

/// One writer: attach, wait for the others, then commit until its
/// statements run out or the window closes under it.
fn commit_burst(
    context: &WriterContext<'_>,
    burst: Burst,
    writer: usize,
    barrier: &Barrier,
    running: &[Mutex<Option<Child>>],
) -> anyhow::Result<()> {
    let prepared = attach_writer(context, writer);
    // Unconditional, so a writer that failed to attach does not strand
    // the ones that did.
    barrier.wait();

    let (mut child, log) = prepared?;
    match burst.bound {
        Bound::Commits(_) => {
            let outcome = commit(&mut child, &burst.writer_sql(writer));
            let status = child.wait().context("waiting for a writer")?;
            outcome?;
            ensure!(
                status.success(),
                "a writer exited with {status}: {}",
                first_error(&log).unwrap_or_else(no_diagnostics)
            );
        }
        Bound::Window(_) => {
            let mut stdin = child.stdin.take().context("taking a writer's stdin")?;
            let slot = running
                .get(writer)
                .context("a writer with no slot to run in")?;
            slot.lock()
                .map_err(|_| anyhow::anyhow!("a writer slot was poisoned"))?
                .replace(child);

            // Writes stop with the kill that closes the window, so the
            // first failure is the end of the run, not an error in it.
            for row in 0.. {
                if stdin
                    .write_all(burst.insert_sql(writer, row).as_bytes())
                    .and_then(|()| stdin.write_all(b"\n"))
                    .is_err()
                {
                    break;
                }
            }

            let mut slot = slot
                .lock()
                .map_err(|_| anyhow::anyhow!("a writer slot was poisoned"))?;
            if let Some(child) = slot.as_mut() {
                let _ = child.wait();
            }
        }
    }
    Ok(())
}

/// Asks the catalog how many rows each of the burst's writers committed.
/// Each insert is one commit, so a row count is a commit count.
fn count_commits(context: &WriterContext<'_>, burst: Burst) -> anyhow::Result<Vec<u64>> {
    let script = format!(
        "SET extension_directory='{}';\nINSTALL postgres;\nLOAD postgres;\nLOAD '{}';\n{}{}\n{}\n",
        crate::duckdb::extension_install_directory().display(),
        context.extension.display(),
        context.preamble,
        context.attach,
        burst.count_sql()
    );

    let mut child = Command::new(context.cli)
        .args(["-unsigned", "-batch", "-noheader", "-list"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning the session that counts a burst's commits")?;
    child
        .stdin
        .take()
        .context("opening the counting session's stdin")?
        .write_all(script.as_bytes())
        .context("writing the counting script")?;
    let output = child
        .wait_with_output()
        .context("waiting for the counting session")?;
    ensure!(
        output.status.success(),
        "counting a burst's commits failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let counts = parse_counts(&stdout);
    ensure!(
        counts.len() == burst.writers,
        "the counting session reported {} of {} writers: {stdout}",
        counts.len(),
        burst.writers
    );
    Ok(counts)
}

/// The counts in a counting session's output, in writer order. The
/// catalog may print a coloured warning first, and its reset sequence
/// lands on the line the first count is on, so the escapes come off
/// before the parse.
fn parse_counts(stdout: &str) -> Vec<u64> {
    stdout
        .lines()
        .filter_map(|line| {
            super::timing::strip_terminal_escapes(line)
                .trim()
                .parse()
                .ok()
        })
        .collect()
}

/// How much of a writer's log an error quotes. A writer that loses its
/// catalog mid-burst fails every remaining statement, so the first line
/// is the diagnosis and the rest is the same line again.
const LOG_EXCERPT_LINES: usize = 2;

/// What an error says about a writer that logged nothing at all.
fn no_diagnostics() -> String {
    "no diagnostics".to_owned()
}

/// The opening lines of a writer's log, flattened onto one line, or
/// `None` when it wrote nothing: a writer that committed nothing and
/// complained about nothing was starved, not refused.
fn first_error(log: &Path) -> Option<String> {
    let text = std::fs::read_to_string(log).unwrap_or_default();
    let excerpt: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(LOG_EXCERPT_LINES)
        .collect();
    (!excerpt.is_empty()).then(|| excerpt.join(" / "))
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
        "SET extension_directory='{}';\nINSTALL postgres;\nLOAD postgres;\nLOAD '{}';\n{}{}\n         COPY (SELECT 1) TO '{}' (FORMAT csv);\n",
        crate::duckdb::extension_install_directory().display(),
        context.extension.display(),
        context.preamble,
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
                first_error(&log).unwrap_or_else(no_diagnostics)
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
        "SET extension_directory='{}';\nINSTALL postgres;\nLOAD postgres;\nLOAD '{}';\n{}{}\n",
        crate::duckdb::extension_install_directory().display(),
        context.extension.display(),
        context.preamble,
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
        assert_eq!(burst.bound, Bound::Commits(16));
        assert_eq!(burst.planned_commits(), Some(128));
    }

    #[test]
    fn a_windowed_burst_takes_any_writer_count_and_plans_no_commits() {
        let burst = Burst::windowed(Duration::from_secs(5), 7).unwrap();
        assert_eq!(burst.bound, Bound::Window(Duration::from_secs(5)));
        // Nothing has to divide evenly, which is what lets a sweep run
        // until a backend stops improving.
        assert_eq!(burst.planned_commits(), None);
        assert!(burst.writer_sql(0).is_empty());

        assert!(Burst::windowed(Duration::ZERO, 1).is_err());
        assert!(Burst::windowed(Duration::from_secs(5), 0).is_err());
    }

    #[test]
    fn a_colour_reset_on_the_first_count_line_does_not_hide_it() {
        // Exactly what a deprecation warning ahead of the counts looks
        // like: the reset sequence is on the first count's own line.
        let stdout = concat!(
            "\u{1b}[90mWARNING:\n\u{1b}[00m\u{1b}[90mdeprecated.\n\n",
            "\u{1b}[00m266\n71\n"
        );
        assert_eq!(parse_counts(stdout), vec![266, 71]);
        assert_eq!(parse_counts("266\n"), vec![266]);
        assert!(parse_counts("writer 0: boom\n").is_empty());
    }

    #[test]
    fn a_shared_burst_puts_every_writer_on_one_table() {
        let burst = Burst::plan(4, 2).unwrap().with_shared(true);
        assert_eq!(
            burst.setup_sql(),
            ["CREATE TABLE lake.main.shared (writer BIGINT, id BIGINT);"]
        );
        assert_eq!(
            burst.insert_sql(1, 7),
            "INSERT INTO lake.main.shared VALUES (1, 7);"
        );

        // The writer column keeps a starved writer visible.
        assert_eq!(
            burst.count_sql(),
            "SELECT count(*) FROM lake.main.shared WHERE writer = 0 UNION ALL \
             SELECT count(*) FROM lake.main.shared WHERE writer = 1;"
        );
        assert_eq!(burst.phase(), "writers_2_shared");
        assert_eq!(
            burst.with_index(true).phase(),
            "writers_2_shared_indexed",
            "a report must tell the four variants apart"
        );
    }

    #[test]
    fn a_windowed_burst_counts_each_writer_separately() {
        let sql = Burst::windowed(Duration::from_secs(1), 2)
            .unwrap()
            .count_sql();
        assert_eq!(
            sql,
            "SELECT count(*) FROM lake.main.writer_0 UNION ALL \
             SELECT count(*) FROM lake.main.writer_1;"
        );
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
    fn throughput_is_what_landed_over_the_wall_clock() {
        let rate = commits_per_second(128, Duration::from_secs(2));
        assert!((rate - 64.0).abs() < f64::EPSILON, "{rate}");
        assert!(commits_per_second(128, Duration::ZERO).is_infinite());
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
        assert_eq!(first_error(&log).as_deref(), Some("first / second"));

        // An empty log is what tells a starved writer from a refused one.
        std::fs::write(&log, "").unwrap();
        assert_eq!(first_error(&log), None);
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
