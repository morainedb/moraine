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
pub mod report;
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
use backends::{BackendKind, PostgresHandle, SessionPaths, attach_sql};
use report::{Report, Row};
use timing::{Statement, phase_seconds};

use crate::duckdb;

/// Parsed `cargo xtask bench` arguments.
struct Options {
    backends: Vec<BackendKind>,
    workloads: Option<Vec<String>>,
    scale: workloads::Scale,
    repeat: usize,
}

fn parse_options(arguments: &[String]) -> anyhow::Result<Options> {
    let mut backends: Option<Vec<BackendKind>> = None;
    let mut selected_workloads: Option<Vec<String>> = None;
    let mut scale = "small".to_owned();
    let mut repeat = 3usize;

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
            "--repeat" => {
                repeat = value()?
                    .parse()
                    .with_context(|| "parsing --repeat".to_owned())?;
                ensure!(repeat >= 1, "--repeat must be at least 1");
            }
            other => {
                bail!("unknown flag `{other}`; valid: --backends, --workloads, --scale, --repeat")
            }
        }
    }

    Ok(Options {
        backends: backends.unwrap_or_else(|| BackendKind::all().to_vec()),
        workloads: selected_workloads,
        scale: workloads::Scale::parse(&scale)?,
        repeat,
    })
}

/// Everything a session needs besides its statements.
struct SessionContext {
    cli: PathBuf,
    extension: PathBuf,
}

impl SessionContext {
    /// Runs one CLI session: an untimed preamble (extension setup, and
    /// `INSTALL`/`LOAD postgres` so every backend's session is
    /// byte-identical), then `.timer on`, then `statements` — each of
    /// which produces exactly one `Run Time (s)` line, verified by the
    /// caller through [`phase_seconds`].
    fn run(&self, statements: &[Statement]) -> anyhow::Result<String> {
        let mut script = String::new();
        // write! to a String cannot fail; ignoring the Result avoids a
        // fabricated error path.
        let _ = writeln!(
            script,
            "SET extension_directory='{}';",
            duckdb::extension_install_directory().display()
        );
        script.push_str("INSTALL ducklake;\nLOAD ducklake;\n");
        script.push_str("INSTALL postgres;\nLOAD postgres;\n");
        let _ = writeln!(script, "LOAD '{}';", self.extension.display());
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
    statements.extend(rest);
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
    postgres: Option<&PostgresHandle>,
    repeat: usize,
) -> anyhow::Result<Vec<(&'static str, f64)>> {
    let run_dir = work_root.join(format!("{}-{}-{repeat}", workload.name, backend.name()));
    let paths = SessionPaths {
        catalog_dir: run_dir.join("catalog"),
        data_dir: run_dir.join("data"),
    };
    fs::create_dir_all(&paths.catalog_dir)
        .with_context(|| format!("creating {}", paths.catalog_dir.display()))?;
    fs::create_dir_all(&paths.data_dir)
        .with_context(|| format!("creating {}", paths.data_dir.display()))?;

    let database = format!("bench_{}_{repeat}", workload.name);
    let dsn = match (backend, postgres) {
        (BackendKind::Postgres, Some(handle)) => {
            handle.create_database(&database)?;
            Some(handle.dsn(&database))
        }
        _ => None,
    };
    let attach = attach_sql(backend, &paths, dsn.as_deref())?;

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

    if let (BackendKind::Postgres, Some(handle)) = (backend, postgres) {
        handle.drop_database(&database)?;
    }
    fs::remove_dir_all(&run_dir).with_context(|| format!("removing {}", run_dir.display()))?;

    Ok(phases)
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
            all.iter().any(|workload| workload.name == name),
            "unknown workload `{name}`; valid: {}",
            all.iter()
                .map(|workload| workload.name)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(all
        .iter()
        .filter(|workload| names.iter().any(|name| name == workload.name))
        .collect())
}

/// Runs the whole suite and prints/writes the report.
pub fn bench(arguments: &[String]) -> anyhow::Result<()> {
    let options = parse_options(arguments)?;

    let cli = duckdb::ensure_duckdb_cli()?;
    println!("ok: duckdb CLI at {}", cli.display());
    let extension = duckdb::build_and_package_extension()?;
    println!("ok: packaged {}", extension.display());
    let context = SessionContext { cli, extension };

    let work_root = std::env::temp_dir().join(format!("moraine-bench-{}", std::process::id()));
    fs::create_dir_all(&work_root).with_context(|| format!("creating {}", work_root.display()))?;

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
    let backends: Vec<BackendKind> = options
        .backends
        .iter()
        .copied()
        .filter(|kind| *kind != BackendKind::Postgres || postgres.is_some())
        .collect();

    let all_workloads = workloads::workloads(&options.scale);
    let selected = select_workloads(&all_workloads, options.workloads.as_ref())?;

    let mut rows: Vec<Row> = Vec::new();
    for workload in &selected {
        for &backend in &backends {
            if workload.moraine_only && backend != BackendKind::Moraine {
                continue;
            }
            for repeat in 0..options.repeat {
                let phases = run_once(
                    &context,
                    &work_root,
                    workload,
                    backend,
                    postgres.as_ref(),
                    repeat,
                )?;
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

    let report = Report {
        scale: options.scale.name.to_owned(),
        repeat: options.repeat,
        rows,
    };
    let backend_names: Vec<String> = backends.iter().map(|kind| kind.name().to_owned()).collect();

    println!();
    print!("{}", report::render_table(&report, &backend_names)?);

    let report_dir = duckdb::workspace_root().join("target/bench");
    fs::create_dir_all(&report_dir)
        .with_context(|| format!("creating {}", report_dir.display()))?;
    let report_path = report_dir.join("report.json");
    fs::write(&report_path, report::render_json(&report))
        .with_context(|| format!("writing {}", report_path.display()))?;
    println!("\nok: wrote {}", report_path.display());

    // The owned Postgres cluster (if any) lives under `work_root`; stop
    // it before deleting the tree.
    drop(postgres);
    fs::remove_dir_all(&work_root).with_context(|| format!("removing {}", work_root.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_default_to_all_backends_small_scale_three_repeats() {
        let options = parse_options(&[]).unwrap();
        assert_eq!(options.backends.len(), 3);
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
}
