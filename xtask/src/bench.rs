//! The `bench` task: runs identical DuckLake workloads against moraine's
//! SlateDB catalog, a stock DuckDB-file catalog, and a Postgres catalog
//! through the same pinned DuckDB CLI, and reports per-phase wall-clock
//! timings side by side. Only the metadata catalog varies — the data
//! layer is Parquet under a local `DATA_PATH` everywhere — so differences
//! isolate metadata-path cost.
//!
//! Timing comes from the CLI's `.timer on` output inside one session per
//! (backend, workload, repeat): process start-up, extension loading, and
//! seeding are excluded, and `ATTACH` is itself a timed phase.

pub mod backends;
pub mod concurrent;
mod endpoints;
pub mod in_process;
pub mod report;
pub mod support;
pub mod timing;
pub mod workloads;

use std::{
    fmt::Write as _,
    fs,
    io::Write as _,
    path::PathBuf,
    process::{Command, Stdio},
};

use anyhow::{Context, bail, ensure};
use backends::{BackendKind, PostgresHandle, QuackHandle, RemoteCatalog, SessionPaths, attach_sql};
use report::{Report, Row};
use timing::{Statement, phase_seconds};

use crate::{catalog_target::CatalogTarget, duckdb, ducklake_patch};

/// The WAL flush spacing the latency workloads pin moraine to, so they
/// measure catalog work rather than the wait for the next flush.
const LATENCY_FLUSH_INTERVAL_MS: u64 = 1;

/// How concurrent writers are arranged. Neither is a
/// [`workloads::Workload`] — both run several writers rather than one
/// statement stream — but both fill the same report rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BurstMode {
    /// Separate operating-system processes, each attaching the lake.
    Processes,
    /// Separate connections over one DuckDB instance and one attach.
    Connections,
}

/// One cell of a concurrency sweep.
#[derive(Debug, Clone, Copy)]
struct BurstRun {
    burst: concurrent::Burst,
    repeat: usize,
    mode: BurstMode,
    flush_interval_ms: u64,
}

impl BurstMode {
    /// The report workload this mode fills.
    fn workload(self) -> &'static str {
        match self {
            Self::Processes => "concurrent_processes",
            Self::Connections => "concurrent_connections",
        }
    }

    fn all() -> [Self; 2] {
        [Self::Processes, Self::Connections]
    }
}

/// Parsed `cargo xtask bench` arguments.
struct Options {
    backends: Vec<BackendKind>,
    workloads: Option<Vec<String>>,
    scale: workloads::Scale,
    repeat: usize,
    concurrency: Vec<usize>,
    commits: u64,
    /// A window every writer commits into, instead of a commit count.
    seconds: Option<u64>,
    /// Where moraine keeps what it read, shared across the whole run.
    cache_dir: Option<PathBuf>,
    flush_interval_ms: u64,
    /// Whether the concurrency workloads write to indexed tables.
    indexed: bool,
    /// Whether the concurrency workloads share one table.
    shared: bool,
}

impl Options {
    /// The burst one concurrency level runs: a window when `--seconds`
    /// asked for one, else the commit count split across the writers.
    fn burst(&self, writers: usize) -> anyhow::Result<concurrent::Burst> {
        let burst = match self.seconds {
            Some(seconds) => {
                concurrent::Burst::windowed(std::time::Duration::from_secs(seconds), writers)?
            }
            None => concurrent::Burst::plan(self.commits, writers)?,
        };
        Ok(burst.with_index(self.indexed).with_shared(self.shared))
    }
}

fn parse_options(arguments: &[String]) -> anyhow::Result<Options> {
    let mut backends: Option<Vec<BackendKind>> = None;
    let mut selected_workloads: Option<Vec<String>> = None;
    let mut scale = "small".to_owned();
    let mut repeat = 3usize;
    let mut concurrency = vec![1usize, 2, 4, 8];
    let mut commits = 128u64;
    let mut seconds: Option<u64> = None;
    let mut cache_dir: Option<PathBuf> = None;
    // The default spacing, not the pinned one the latency workloads use:
    // coalescing is what the concurrency workloads are there to measure,
    // and a 1 ms spacing leaves nothing to coalesce.
    let mut flush_interval_ms = 100u64;
    let mut indexed = false;
    let mut shared = false;

    let mut iterator = arguments.iter();
    while let Some(flag) = iterator.next() {
        let mut value = || {
            iterator
                .next()
                .with_context(|| format!("flag `{flag}` needs a value"))
        };
        match flag.as_str() {
            "--backends" => {
                backends = Some(
                    value()?
                        .split(',')
                        .map(BackendKind::parse)
                        .collect::<anyhow::Result<_>>()?,
                );
            }
            "--workloads" => {
                selected_workloads = Some(value()?.split(',').map(str::to_owned).collect());
            }
            "--scale" => value()?.clone_into(&mut scale),
            "--concurrency" => concurrency = concurrent::parse_sweep(value()?)?,
            "--flush-interval-ms" => {
                flush_interval_ms = value()?
                    .parse()
                    .with_context(|| "parsing --flush-interval-ms".to_owned())?;
            }
            "--indexed" => indexed = true,
            "--shared" => shared = true,
            "--commits" => {
                commits = value()?
                    .parse()
                    .with_context(|| "parsing --commits".to_owned())?;
            }
            "--cache-dir" => cache_dir = Some(PathBuf::from(value()?)),
            "--seconds" => {
                seconds = Some(
                    value()?
                        .parse()
                        .with_context(|| "parsing --seconds".to_owned())?,
                );
            }
            "--repeat" => {
                repeat = value()?
                    .parse()
                    .with_context(|| "parsing --repeat".to_owned())?;
                ensure!(repeat >= 1, "--repeat must be at least 1");
            }
            other => {
                bail!(
                    "unknown flag `{other}`; valid: --backends, --workloads, --scale, \
                     --repeat, --concurrency, --commits, --flush-interval-ms"
                )
            }
        }
    }

    Ok(Options {
        backends: backends.unwrap_or_else(|| BackendKind::all().to_vec()),
        workloads: selected_workloads,
        scale: workloads::Scale::parse(&scale)?,
        repeat,
        concurrency,
        commits,
        seconds,
        cache_dir,
        flush_interval_ms,
        indexed,
        shared,
    })
}

/// Everything a session needs besides its statements.
struct SessionContext {
    cli: PathBuf,
    extension: PathBuf,
    store: CatalogTarget,
    /// The directory bucket, when one is named, so a run compares the
    /// two S3 stores under identical conditions.
    express_store: Option<CatalogTarget>,
    preamble: String,
    cache_dir: Option<PathBuf>,
}

impl SessionContext {
    /// Builds and packages the extension, finds the CLI, and reads the
    /// store from the environment; `local_root` holds local catalogs.
    fn prepare(local_root: PathBuf, cache_dir: Option<PathBuf>) -> anyhow::Result<Self> {
        let extension = duckdb::build_and_package_extension(&ducklake_patch::prepare()?)?;
        println!("ok: packaged {}", extension.display());
        let cli = duckdb::ensure_duckdb_cli()?;
        println!("ok: duckdb CLI at {}", cli.display());

        let store = CatalogTarget::from_environment(local_root);
        if store.is_remote() {
            println!("ok: moraine catalogs on {}", store.description());
        }
        let express_store = CatalogTarget::express_from_environment();
        if let Some(cache_dir) = &cache_dir {
            fs::create_dir_all(cache_dir)
                .with_context(|| format!("creating {}", cache_dir.display()))?;
            println!(
                "ok: moraine keeps what it reads under {}",
                cache_dir.display()
            );
        }
        let preamble = store_preamble(&store)?;

        Ok(Self {
            cli,
            extension,
            store,
            express_store,
            preamble,
            cache_dir,
        })
    }

    /// Moraine's catalog location for one run: a URI under the S3 prefix,
    /// or `catalog_dir` itself.
    fn moraine_catalog(
        &self,
        backend: BackendKind,
        catalog_dir: &std::path::Path,
        leaf: &str,
    ) -> anyhow::Result<String> {
        let store = self.store_for(backend);
        if store.is_remote() {
            store.catalog_uri(leaf)
        } else {
            Ok(catalog_dir.display().to_string())
        }
    }

    /// The store a backend's catalog lives on: the directory bucket for
    /// `moraine_express`, the standard store for everything else.
    fn store_for(&self, backend: BackendKind) -> &CatalogTarget {
        match (backend, &self.express_store) {
            (BackendKind::MoraineExpress, Some(store)) => store,
            _ => &self.store,
        }
    }

    /// Where a backend's write-ahead log goes, for the one backend that
    /// puts it somewhere other than its catalog's store.
    fn wal_path(&self, backend: BackendKind, leaf: &str) -> anyhow::Result<Option<String>> {
        if backend != BackendKind::MoraineWalExpress {
            return Ok(None);
        }
        let Some(express) = &self.express_store else {
            return Ok(None);
        };
        express.catalog_uri(&format!("{leaf}-wal")).map(Some)
    }

    /// Runs one CLI session: an untimed preamble (extension setup, and
    /// `INSTALL`/`LOAD postgres` so every backend's session is
    /// byte-identical, plus the store's own), then `.timer on`, then
    /// `statements` — each of which produces exactly one `Run Time (s)`
    /// line, verified by the caller through [`phase_seconds`].
    fn run(&self, statements: &[Statement]) -> anyhow::Result<String> {
        let mut script = String::new();
        // write! to a String cannot fail; ignoring the Result avoids a
        // fabricated error path.
        let _ = writeln!(
            script,
            "SET extension_directory='{}';",
            duckdb::extension_install_directory().display()
        );
        script.push_str("INSTALL postgres;\nLOAD postgres;\n");
        // moraine bundles DuckLake, which serves every catalog under test.
        let _ = writeln!(script, "LOAD '{}';", self.extension.display());
        script.push_str(&self.preamble);
        script.push_str(".timer on\n");
        for statement in statements {
            script.push_str(&statement.sql);
            script.push('\n');
        }

        let mut child = Command::new(&self.cli)
            .arg("-unsigned")
            .arg("-batch")
            .arg("-csv")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("spawning the DuckDB CLI")?;
        child
            .stdin
            .take()
            .context("opening the CLI's stdin")?
            .write_all(script.as_bytes())
            .context("writing the session script")?;
        let output = child
            .wait_with_output()
            .context("waiting for the DuckDB CLI")?;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        ensure!(
            output.status.success(),
            "DuckDB session failed:\n--- script ---\n{script}\n--- stdout ---\n{stdout}\n\
             --- stderr ---\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(stdout)
    }
}

/// Prepends the per-session boilerplate: a warm-up statement absorbing
/// one-time initialization, then the backend's `ATTACH` as the `attach`
/// phase (or untimed setup for seeding sessions).
fn with_session_prefix(
    attach: &str,
    attach_is_measured: bool,
    rest: impl IntoIterator<Item = Statement>,
) -> Vec<Statement> {
    let attach_statement = if attach_is_measured {
        Statement::measured("attach", attach)
    } else {
        Statement::setup(attach)
    };
    let mut statements = vec![Statement::setup("SELECT 1;"), attach_statement];
    // A workload that re-attaches writes `{attach}` rather than knowing
    // the backend's own attach, which only the runner can build.
    statements.extend(rest.into_iter().map(|statement| Statement {
        sql: statement.sql.replace("{attach}", attach),
        ..statement
    }));
    statements
}

/// One (workload, backend, repeat) run in fresh directories (and, for
/// Postgres, a fresh database): an untimed seeding session when the
/// workload has one, then the measured session. Returns per-phase
/// seconds.
fn run_once(
    context: &SessionContext,
    work_root: &std::path::Path,
    workload: &workloads::Workload,
    backend: BackendKind,
    servers: &Servers,
    repeat: usize,
) -> anyhow::Result<Vec<(&'static str, f64)>> {
    let leaf = format!("{}-{}-{repeat}", workload.name, backend.name());
    let run_dir = work_root.join(&leaf);
    let catalog_dir = run_dir.join("catalog");
    let paths = SessionPaths {
        moraine_catalog: context.moraine_catalog(backend, &catalog_dir, &leaf)?,
        catalog_dir,
        wal_path: context.wal_path(backend, &leaf)?,
        cache_dir: context.cache_dir.clone(),
        data_dir: run_dir.join("data"),
    };
    fs::create_dir_all(&paths.catalog_dir)
        .with_context(|| format!("creating {}", paths.catalog_dir.display()))?;
    fs::create_dir_all(&paths.data_dir)
        .with_context(|| format!("creating {}", paths.data_dir.display()))?;

    let scratch_name = scratch_name(&format!("{}_{repeat}", workload.name));
    let remote = servers.create(backend, &scratch_name)?;
    let attach = attach_sql(backend, &paths, remote.as_ref(), LATENCY_FLUSH_INTERVAL_MS)?;

    if !workload.seed.is_empty() {
        let seed_statements =
            with_session_prefix(&attach, false, workload.seed.iter().map(Statement::setup));
        let stdout = context.run(&seed_statements)?;
        phase_seconds(&seed_statements, &stdout)?;
    }

    let measured = with_session_prefix(
        &attach,
        true,
        workload.measured.iter().map(|statement| Statement {
            sql: statement.sql.clone(),
            phase: statement.phase,
        }),
    );
    let stdout = context.run(&measured)?;
    let phases = phase_seconds(&measured, &stdout)?;

    servers.drop(backend, &scratch_name)?;
    fs::remove_dir_all(&run_dir).with_context(|| format!("removing {}", run_dir.display()))?;

    Ok(phases)
}

/// The catalog servers a bench run talks to, when their backends are
/// selected. Each run gets a fresh database or schema on them.
struct Servers {
    postgres: Option<PostgresHandle>,
    quack: Option<QuackHandle>,
}

impl Servers {
    /// Creates `backend`'s per-run catalog named `name`, if it has one.
    fn create(&self, backend: BackendKind, name: &str) -> anyhow::Result<Option<RemoteCatalog>> {
        Ok(match (backend, &self.postgres, &self.quack) {
            (BackendKind::Postgres, Some(handle), _) => {
                handle.create_database(name)?;
                Some(RemoteCatalog::Postgres {
                    dsn: handle.dsn(name),
                })
            }
            (BackendKind::Quack, _, Some(handle)) => {
                handle.create_schema(name)?;
                Some(RemoteCatalog::Quack {
                    server: handle.server().to_owned(),
                    schema: name.to_owned(),
                })
            }
            _ => None,
        })
    }

    /// Drops what [`create`](Self::create) made for `backend` and `name`.
    fn drop(&self, backend: BackendKind, name: &str) -> anyhow::Result<()> {
        match (backend, &self.postgres, &self.quack) {
            (BackendKind::Postgres, Some(handle), _) => handle.drop_database(name),
            (BackendKind::Quack, _, Some(handle)) => handle.drop_schema(name),
            _ => Ok(()),
        }
    }
}

/// A server-side name for one run, carrying this process's id. A run
/// killed mid-flight leaves its schemas and databases behind, and a
/// deterministic name would collide with them on the next attempt.
fn scratch_name(leaf: &str) -> String {
    format!("bench_{}_{leaf}", std::process::id())
}

/// The statements a moraine attach needs before it runs: `httpfs` and the
/// store's secret for S3, nothing for a local directory.
fn store_preamble(store: &CatalogTarget) -> anyhow::Result<String> {
    let mut preamble = store.extensions_sql();
    if let Some(secret) = store.secret_sql()? {
        preamble.push_str(&secret);
        preamble.push('\n');
    }
    Ok(preamble)
}

/// Whether the concurrent burst was asked for: by default, or by name.
fn burst_selected(names: Option<&Vec<String>>, mode: BurstMode) -> bool {
    names.is_none_or(|names| names.iter().any(|name| name == mode.workload()))
}

/// One (backend, burst, repeat) run in fresh directories: create the
/// writers' tables, then time the released window.
fn run_burst_once(
    context: &SessionContext,
    work_root: &std::path::Path,
    backend: BackendKind,
    servers: &Servers,
    run: BurstRun,
) -> anyhow::Result<concurrent::Outcome> {
    let BurstRun {
        burst,
        repeat,
        mode,
        flush_interval_ms,
    } = run;
    let leaf = format!(
        "{}-{}-{}-{repeat}",
        mode.workload(),
        burst.writers,
        backend.name()
    );
    let run_dir = work_root.join(&leaf);
    let catalog_dir = run_dir.join("catalog");
    let paths = SessionPaths {
        moraine_catalog: context.moraine_catalog(backend, &catalog_dir, &leaf)?,
        catalog_dir,
        wal_path: context.wal_path(backend, &leaf)?,
        cache_dir: context.cache_dir.clone(),
        data_dir: run_dir.join("data"),
    };
    let scratch = run_dir.join("scratch");
    for directory in [&paths.catalog_dir, &paths.data_dir, &scratch] {
        fs::create_dir_all(directory)
            .with_context(|| format!("creating {}", directory.display()))?;
    }

    let scratch_name = scratch_name(&format!("burst_{}_{repeat}", burst.writers));
    let remote = servers.create(backend, &scratch_name)?;
    let attach = attach_sql(backend, &paths, remote.as_ref(), flush_interval_ms)?;
    let writer_context = concurrent::WriterContext {
        cli: &context.cli,
        extension: &context.extension,
        preamble: &context.preamble,
        attach: &attach,
        scratch: &scratch,
    };

    let outcome = match mode {
        BurstMode::Processes => concurrent::create_tables(&writer_context, burst)
            .and_then(|()| concurrent::run(&writer_context, burst)),
        // The driver does its own setup: the tables have to be created on
        // the instance the writers then share.
        BurstMode::Connections => in_process::driver().and_then(|driver| {
            in_process::run(
                &driver,
                &context.preamble,
                &attach,
                burst.writers,
                burst.bound,
                burst.indexed,
                burst.shared,
            )
        }),
    };

    // What the burst cost in objects: every write-ahead log object the
    // catalog put. The setup commits (one table per writer, the attach) are
    // in the count too, so read it against `commits + writers + 1`. A remote
    // store is not listed.
    if backend.is_moraine() && !context.store_for(backend).is_remote() {
        let wal_objects = fs::read_dir(paths.catalog_dir.join("wal"))
            .map_or(0, |entries| entries.filter_map(Result::ok).count());
        let commits = outcome
            .as_ref()
            .map_or(0, |outcome: &concurrent::Outcome| outcome.commits);
        println!(
            "ok: {wal_objects} wal objects for {commits} commits plus setup with {} writers \
             on moraine",
            burst.writers
        );
    }
    servers.drop(backend, &scratch_name)?;
    let _ = fs::remove_dir_all(&run_dir);

    outcome
}

/// Selects workloads by `--workloads`, rejecting unknown names.
fn select_workloads<'workloads>(
    all: &'workloads [workloads::Workload],
    names: Option<&Vec<String>>,
) -> anyhow::Result<Vec<&'workloads workloads::Workload>> {
    let Some(names) = names else {
        return Ok(all.iter().collect());
    };
    for name in names {
        ensure!(
            BurstMode::all().iter().any(|mode| mode.workload() == name)
                || all.iter().any(|workload| workload.name == name),
            "unknown workload `{name}`; valid: {}, {}",
            all.iter()
                .map(|workload| workload.name)
                .collect::<Vec<_>>()
                .join(", "),
            BurstMode::all()
                .iter()
                .map(|mode| mode.workload())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(all
        .iter()
        .filter(|workload| names.iter().any(|name| name == workload.name))
        .collect())
}

/// The concurrent burst at every requested writer count, for every
/// backend, appending its rows to `rows`.
///
/// A backend that cannot take a given number of concurrent writers leaves
/// that cell empty rather than failing the suite: refusing the second
/// writer is a property of the catalog, and the report should say so.
fn sweep_concurrency(
    context: &SessionContext,
    work_root: &std::path::Path,
    backends: &[BackendKind],
    servers: &Servers,
    options: &Options,
    mode: BurstMode,
    rows: &mut Vec<Row>,
) -> anyhow::Result<()> {
    for &backend in backends {
        if options.indexed && !backend.is_moraine() {
            println!(
                "notice: {} skipped on {} — an equality index is moraine's own",
                backend.name(),
                mode.workload()
            );
            continue;
        }
        let mut one_writer_worked = false;
        for &writers in &options.concurrency {
            let burst = options.burst(writers)?;
            let mut seconds: Vec<f64> = Vec::new();
            let mut commits: Vec<u64> = Vec::new();

            for repeat in 0..options.repeat {
                let run = BurstRun {
                    burst,
                    repeat,
                    mode,
                    flush_interval_ms: options.flush_interval_ms,
                };
                match run_burst_once(context, work_root, backend, servers, run) {
                    Ok(outcome) => {
                        println!(
                            "ok: {} with {writers} writers on {} \
                             ({} commits, {:.1} commits/s, repeat {}/{})",
                            mode.workload(),
                            backend.name(),
                            outcome.commits,
                            concurrent::commits_per_second(outcome.commits, outcome.elapsed),
                            repeat + 1,
                            options.repeat
                        );
                        seconds.push(outcome.elapsed.as_secs_f64());
                        commits.push(outcome.commits);
                    }
                    Err(error) => {
                        println!(
                            "notice: {}",
                            burst_refusal(
                                backend.name(),
                                mode.workload(),
                                writers,
                                one_writer_worked,
                                &error
                            )
                        );
                        seconds.clear();
                        commits.clear();
                        break;
                    }
                }
            }

            if seconds.is_empty() {
                // Higher counts cannot succeed where this one failed, and
                // the cells stay empty: refusing concurrent writers is a
                // property of the catalog, which the report should show
                // rather than hide behind an aborted run.
                break;
            }
            if writers == 1 {
                one_writer_worked = true;
            }
            rows.push(Row {
                workload: mode.workload().to_owned(),
                phase: burst.phase(),
                backend: backend.name().to_owned(),
                seconds,
                commits,
            });
        }
    }

    Ok(())
}

/// How to describe a burst a backend would not run.
///
/// A catalog that takes one writer process but not two is single-writer,
/// which is a design property worth naming; anything else is reported as
/// the failure it is.
fn burst_refusal(
    backend: &str,
    workload: &str,
    writers: usize,
    one_writer_worked: bool,
    error: &anyhow::Error,
) -> String {
    if one_writer_worked && writers > 1 {
        format!(
            "{backend} takes one writer but refused {writers} on {workload}, so its cells \
             there stay empty — {error:#}"
        )
    } else {
        format!("{backend} could not run {writers} writers on {workload} — {error:#}")
    }
}

/// The endpoints whose latency a run should record beside its numbers:
/// the catalog store when it is remote, and any server not on a socket of
/// its own making.
fn probe_targets(context: &SessionContext, servers: &Servers) -> Vec<(String, String)> {
    let mut targets = Vec::new();
    if let Some(target) = context.store.probe_target() {
        targets.push(("store".to_owned(), target));
    }
    if let Some(target) = servers
        .postgres
        .as_ref()
        .and_then(PostgresHandle::probe_target)
    {
        targets.push(("postgres".to_owned(), target));
    }
    if let Some(target) = servers.quack.as_ref().and_then(QuackHandle::probe_target) {
        targets.push(("quack".to_owned(), target));
    }
    targets
}

/// The requested backends this machine can actually run, saying which it
/// dropped and why.
fn available_backends(
    options: &Options,
    context: &SessionContext,
    servers: &Servers,
) -> Vec<BackendKind> {
    let needs_express = [BackendKind::MoraineExpress, BackendKind::MoraineWalExpress];
    if options
        .backends
        .iter()
        .any(|kind| needs_express.contains(kind))
        && context.express_store.is_none()
    {
        println!(
            "notice: the express backends are skipped — MORAINE_S3_EXPRESS_BUCKET names no \
             directory bucket"
        );
    }

    options
        .backends
        .iter()
        .copied()
        .filter(|kind| *kind != BackendKind::Postgres || servers.postgres.is_some())
        .filter(|kind| !needs_express.contains(kind) || context.express_store.is_some())
        .collect()
}

/// Runs the whole suite and prints/writes the report.
pub fn bench(arguments: &[String]) -> anyhow::Result<()> {
    let options = parse_options(arguments)?;

    let work_root = std::env::temp_dir().join(format!("moraine-bench-{}", std::process::id()));
    fs::create_dir_all(&work_root).with_context(|| format!("creating {}", work_root.display()))?;
    let mut context = SessionContext::prepare(work_root.clone(), options.cache_dir.clone())?;

    let postgres = if options.backends.contains(&BackendKind::Postgres) {
        let handle = PostgresHandle::provision(&work_root)?;
        if handle.is_none() {
            println!(
                "notice: postgres backend skipped — no Postgres binaries found on $PATH or \
                 under /opt/homebrew/opt, and MORAINE_BENCH_POSTGRES is unset"
            );
        }
        handle
    } else {
        None
    };
    let quack = if options.backends.contains(&BackendKind::Quack) {
        let handle = QuackHandle::provision(&context.cli, &work_root)?;
        println!("ok: quack server at {}", handle.server());
        context.preamble.push_str(&handle.preamble());
        Some(handle)
    } else {
        None
    };
    let servers = Servers { postgres, quack };
    let backends = available_backends(&options, &context, &servers);

    let all_workloads = workloads::workloads(&options.scale);
    let selected = select_workloads(&all_workloads, options.workloads.as_ref())?;

    let mut rows: Vec<Row> = Vec::new();
    for workload in &selected {
        for &backend in &backends {
            if workload.moraine_only && !backend.is_moraine() {
                continue;
            }
            for repeat in 0..options.repeat {
                let phases = run_once(&context, &work_root, workload, backend, &servers, repeat)?;
                for (phase, seconds) in phases {
                    match rows.iter_mut().find(|row| {
                        row.workload == workload.name
                            && row.phase == phase
                            && row.backend == backend.name()
                    }) {
                        Some(row) => row.seconds.push(seconds),
                        None => rows.push(Row {
                            workload: workload.name.to_owned(),
                            phase: phase.to_owned(),
                            backend: backend.name().to_owned(),
                            seconds: vec![seconds],
                            commits: Vec::new(),
                        }),
                    }
                }
                println!(
                    "ok: {} on {} (repeat {}/{})",
                    workload.name,
                    backend.name(),
                    repeat + 1,
                    options.repeat
                );
            }
        }
    }

    for mode in BurstMode::all() {
        if burst_selected(options.workloads.as_ref(), mode) {
            sweep_concurrency(
                &context, &work_root, &backends, &servers, &options, mode, &mut rows,
            )?;
        }
    }

    let report = Report {
        scale: options.scale.name.to_owned(),
        repeat: options.repeat,
        endpoints: endpoints::probe(&probe_targets(&context, &servers)),
        rows,
    };
    let backend_names: Vec<String> = backends.iter().map(|kind| kind.name().to_owned()).collect();

    println!();
    print!("{}", report::render_endpoints(&report));
    print!("{}", report::render_table(&report, &backend_names)?);

    let report_dir = duckdb::workspace_root().join("target/bench");
    fs::create_dir_all(&report_dir)
        .with_context(|| format!("creating {}", report_dir.display()))?;
    let report_path = report_dir.join("report.json");
    fs::write(&report_path, report::render_json(&report))
        .with_context(|| format!("writing {}", report_path.display()))?;
    println!("\nok: wrote {}", report_path.display());

    // Owned servers live under `work_root`; stop them before deleting the
    // tree.
    drop(servers);
    fs::remove_dir_all(&work_root).with_context(|| format!("removing {}", work_root.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_default_to_all_backends_small_scale_three_repeats() {
        let options = parse_options(&[]).unwrap();
        assert_eq!(options.backends.len(), 6);
        assert_eq!(options.scale.name, "small");
        assert_eq!(options.repeat, 3);
        assert!(options.workloads.is_none());
    }

    #[test]
    fn options_parse_every_flag() {
        let arguments: Vec<String> = [
            "--backends",
            "moraine,duckdb",
            "--workloads",
            "scan,bulk_load",
            "--scale",
            "medium",
            "--repeat",
            "5",
        ]
        .iter()
        .map(|&argument| argument.to_owned())
        .collect();
        let options = parse_options(&arguments).unwrap();
        assert_eq!(options.backends.len(), 2);
        assert_eq!(
            options.workloads,
            Some(vec!["scan".to_owned(), "bulk_load".to_owned()])
        );
        assert_eq!(options.scale.name, "medium");
        assert_eq!(options.repeat, 5);
    }

    #[test]
    fn options_reject_unknown_flags_backends_and_zero_repeat() {
        let to_arguments = |slice: &[&str]| -> Vec<String> {
            slice.iter().map(|&argument| argument.to_owned()).collect()
        };
        assert!(parse_options(&to_arguments(&["--frobnicate"])).is_err());
        assert!(parse_options(&to_arguments(&["--backends", "sqlite"])).is_err());
        assert!(parse_options(&to_arguments(&["--repeat", "0"])).is_err());
        assert!(parse_options(&to_arguments(&["--scale"])).is_err());
    }

    #[test]
    fn workload_selection_filters_and_rejects_unknown_names() {
        let scale = workloads::Scale::parse("small").unwrap();
        let all = workloads::workloads(&scale);
        let picked =
            select_workloads(&all, Some(&vec!["scan".to_owned(), "bulk_load".to_owned()])).unwrap();
        let names: Vec<&str> = picked.iter().map(|workload| workload.name).collect();
        assert_eq!(names, ["bulk_load", "scan"]);
        assert!(select_workloads(&all, Some(&vec!["nope".to_owned()])).is_err());
    }

    #[test]
    fn the_burst_sweeps_four_concurrency_levels_by_default() {
        let options = parse_options(&[]).unwrap();
        assert_eq!(options.concurrency, [1, 2, 4, 8]);
        assert_eq!(options.commits, 128);
        // The default spacing, not the pinned one: concurrency workloads
        // measure what commits coalesce into.
        assert_eq!(options.flush_interval_ms, 100);
        assert_ne!(options.flush_interval_ms, LATENCY_FLUSH_INTERVAL_MS);
    }

    #[test]
    fn the_burst_runs_by_default_and_whenever_it_is_named() {
        for mode in BurstMode::all() {
            assert!(burst_selected(None, mode));
            assert!(burst_selected(
                Some(&vec![mode.workload().to_owned()]),
                mode
            ));
            assert!(!burst_selected(Some(&vec!["bulk_load".to_owned()]), mode));
        }
        // Naming one burst does not select the other.
        assert!(!burst_selected(
            Some(&vec![BurstMode::Processes.workload().to_owned()]),
            BurstMode::Connections
        ));
    }

    /// The burst is not a statement workload, so naming it selects none
    /// of them — but it is still a name the suite accepts.
    #[test]
    fn the_burst_is_a_selectable_workload_name() {
        let scale = workloads::Scale::parse("small").unwrap();
        let all = workloads::workloads(&scale);
        for mode in BurstMode::all() {
            assert!(
                select_workloads(&all, Some(&vec![mode.workload().to_owned()]))
                    .unwrap()
                    .is_empty()
            );
        }
        assert!(select_workloads(&all, Some(&vec!["nonsense".to_owned()])).is_err());
    }

    #[test]
    fn refusing_a_second_writer_is_reported_as_a_property_not_a_crash() {
        let error = anyhow::anyhow!("writer fenced");
        let workload = BurstMode::Processes.workload();
        let single = burst_refusal("moraine", workload, 2, true, &error);
        assert!(
            single.contains("takes one writer but refused 2"),
            "{single}"
        );
        assert!(single.contains(workload), "{single}");
        assert!(single.contains("writer fenced"), "{single}");

        // One writer failing is not a concurrency property.
        let broken = burst_refusal("moraine", workload, 1, false, &error);
        assert!(!broken.contains("takes one writer"), "{broken}");
    }

    #[test]
    fn a_commit_count_no_concurrency_level_divides_is_rejected_up_front() {
        let to_arguments = |slice: &[&str]| -> Vec<String> {
            slice.iter().map(|&argument| argument.to_owned()).collect()
        };
        let options =
            parse_options(&to_arguments(&["--concurrency", "3", "--commits", "9"])).unwrap();
        assert!(concurrent::Burst::plan(options.commits, 3).is_ok());
        assert!(
            parse_options(&to_arguments(&["--indexed"]))
                .unwrap()
                .indexed
        );
        assert!(!options.indexed);
        assert!(parse_options(&to_arguments(&["--concurrency", "0"])).is_err());
        assert!(parse_options(&to_arguments(&["--commits", "many"])).is_err());
    }
}
