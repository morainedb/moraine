//! The catalog backends under comparison, as `ATTACH` recipes over fresh
//! per-run directories, plus ephemeral Postgres provisioning. Everything
//! outside the `ATTACH` statement is identical across backends.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail, ensure};

/// A DuckLake metadata catalog backend.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    /// moraine's SlateDB store over the local filesystem.
    Moraine,
    /// Stock DuckLake: a DuckDB database file.
    DuckdbFile,
    /// Stock DuckLake over a Postgres database.
    Postgres,
}

impl BackendKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Moraine => "moraine",
            Self::DuckdbFile => "duckdb",
            Self::Postgres => "postgres",
        }
    }

    pub fn parse(name: &str) -> anyhow::Result<Self> {
        match name {
            "moraine" => Ok(Self::Moraine),
            "duckdb" => Ok(Self::DuckdbFile),
            "postgres" => Ok(Self::Postgres),
            other => bail!("unknown backend `{other}`; valid: moraine, duckdb, postgres"),
        }
    }

    pub fn all() -> [Self; 3] {
        [Self::Moraine, Self::DuckdbFile, Self::Postgres]
    }
}

/// Fresh directories for one (backend, workload, repeat) run: the catalog
/// location (ignored by Postgres) and the Parquet `DATA_PATH`.
pub struct SessionPaths {
    pub catalog_dir: PathBuf,
    pub data_dir: PathBuf,
}

/// The backend's `ATTACH` statement. `postgres_dsn` must be provided for
/// (and only for) the Postgres backend.
///
/// moraine attaches with `META_DATA_PATH` so equality-index workloads can
/// scoped-read their data files. It is inert without an index: the delete
/// workloads time the same with and without it.
pub fn attach_sql(
    kind: BackendKind,
    paths: &SessionPaths,
    postgres_dsn: Option<&str>,
) -> anyhow::Result<String> {
    let data = paths.data_dir.display();
    Ok(match kind {
        BackendKind::Moraine => format!(
            "ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{data}', META_DATA_PATH '{data}', META_FLUSH_INTERVAL_MS 1);",
            paths.catalog_dir.display()
        ),
        BackendKind::DuckdbFile => format!(
            "ATTACH 'ducklake:{}' AS lake (DATA_PATH '{data}');",
            paths.catalog_dir.join("meta.ducklake").display()
        ),
        BackendKind::Postgres => {
            let dsn = postgres_dsn.context("postgres backend needs a DSN")?;
            format!("ATTACH 'ducklake:postgres:{dsn}' AS lake (DATA_PATH '{data}');")
        }
    })
}

/// Picks the highest-versioned `postgresql@N` entry, for discovering the
/// newest Homebrew Postgres when none is on `$PATH`.
fn newest_postgres_entry(entries: &[String]) -> Option<(u32, &str)> {
    entries
        .iter()
        .filter_map(|entry| {
            let version: u32 = entry.strip_prefix("postgresql@")?.parse().ok()?;
            Some((version, entry.as_str()))
        })
        .max_by_key(|(version, _)| *version)
}

/// Where the Postgres client/server binaries live: `$PATH` if `pg_ctl`
/// resolves there, otherwise the newest Homebrew `postgresql@N` keg.
/// `None` when no Postgres is installed.
fn discover_binary_dir() -> Option<PathBuf> {
    let on_path = Command::new("pg_ctl")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success());
    if on_path {
        // An empty dir means "resolve via $PATH" when joined onto a
        // binary name.
        return Some(PathBuf::new());
    }

    let opt = Path::new("/opt/homebrew/opt");
    let entries: Vec<String> = fs::read_dir(opt)
        .ok()?
        .filter_map(|entry| Some(entry.ok()?.file_name().to_string_lossy().into_owned()))
        .collect();
    let (_, name) = newest_postgres_entry(&entries)?;
    let bin = opt.join(name).join("bin");
    bin.join("pg_ctl").exists().then_some(bin)
}

/// How the harness reaches Postgres: a cluster it started itself, or an
/// external server named by `MORAINE_BENCH_POSTGRES` (a libpq DSN without
/// `dbname`, e.g. `host=localhost port=5432 user=me`).
enum PostgresMode {
    Owned {
        data_dir: PathBuf,
        socket_dir: PathBuf,
        port: u16,
    },
    External {
        base_dsn: String,
    },
}

/// A reachable Postgres plus the binaries to talk to it. Owned clusters
/// are stopped and deleted on drop.
pub struct PostgresHandle {
    binary_dir: PathBuf,
    mode: PostgresMode,
}

impl PostgresHandle {
    /// Provisions Postgres for a bench run: the `MORAINE_BENCH_POSTGRES`
    /// DSN if set, otherwise an ephemeral single-user cluster under
    /// `work_dir` listening only on a Unix socket. `Ok(None)` means no
    /// Postgres is available on this machine.
    pub fn provision(work_dir: &Path) -> anyhow::Result<Option<Self>> {
        let Some(binary_dir) = discover_binary_dir() else {
            return Ok(None);
        };

        if let Ok(base_dsn) = std::env::var("MORAINE_BENCH_POSTGRES") {
            return Ok(Some(Self {
                binary_dir,
                mode: PostgresMode::External { base_dsn },
            }));
        }

        let data_dir = work_dir.join("pgdata");
        let socket_dir = work_dir.join("pgsocket");
        fs::create_dir_all(&socket_dir)
            .with_context(|| format!("creating {}", socket_dir.display()))?;
        // Only the socket filename carries the port; offsetting by pid
        // keeps concurrent bench runs on one machine from colliding.
        let port = 20_000 + (std::process::id() % 10_000) as u16;

        let handle = Self {
            binary_dir,
            mode: PostgresMode::Owned {
                data_dir: data_dir.clone(),
                socket_dir: socket_dir.clone(),
                port,
            },
        };

        handle.run_tool(
            "initdb",
            &[
                "--pgdata",
                &data_dir.display().to_string(),
                "--username",
                "bench",
                "--auth",
                "trust",
                "--no-sync",
            ],
        )?;

        let options = format!("-k {} -p {port} -c listen_addresses=", socket_dir.display());
        handle.run_tool(
            "pg_ctl",
            &[
                "start",
                "--pgdata",
                &data_dir.display().to_string(),
                "--log",
                &work_dir.join("postgres.log").display().to_string(),
                "--wait",
                "-o",
                &options,
            ],
        )?;

        Ok(Some(handle))
    }

    /// The libpq DSN for `database`, for both `ATTACH` and `psql`.
    pub fn dsn(&self, database: &str) -> String {
        match &self.mode {
            PostgresMode::Owned {
                socket_dir, port, ..
            } => format!(
                "dbname={database} host={} port={port} user=bench",
                socket_dir.display()
            ),
            PostgresMode::External { base_dsn } => format!("{base_dsn} dbname={database}"),
        }
    }

    /// The maintenance-database DSN used to create/drop scratch databases.
    fn admin_dsn(&self) -> String {
        self.dsn("postgres")
    }

    pub fn create_database(&self, database: &str) -> anyhow::Result<()> {
        self.psql(&format!("CREATE DATABASE {database};"))
    }

    pub fn drop_database(&self, database: &str) -> anyhow::Result<()> {
        self.psql(&format!("DROP DATABASE IF EXISTS {database};"))
    }

    fn psql(&self, sql: &str) -> anyhow::Result<()> {
        let output = Command::new(self.binary_dir.join("psql"))
            .arg(self.admin_dsn())
            .args(["-v", "ON_ERROR_STOP=1", "-c", sql])
            .output()
            .context("spawning psql")?;
        ensure!(
            output.status.success(),
            "psql failed for `{sql}`:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    fn run_tool(&self, tool: &str, arguments: &[&str]) -> anyhow::Result<()> {
        // Without a valid locale the macOS postmaster aborts with
        // "postmaster became multithreaded during startup".
        let output = Command::new(self.binary_dir.join(tool))
            .env("LC_ALL", "C")
            .args(arguments)
            .output()
            .with_context(|| format!("spawning {tool}"))?;
        ensure!(
            output.status.success(),
            "{tool} {arguments:?} failed:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }
}

impl Drop for PostgresHandle {
    fn drop(&mut self) {
        if let PostgresMode::Owned { data_dir, .. } = &self.mode {
            let _ = Command::new(self.binary_dir.join("pg_ctl"))
                .args(["stop", "--pgdata"])
                .arg(data_dir)
                .args(["--mode", "immediate", "--wait"])
                .output();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths() -> SessionPaths {
        SessionPaths {
            catalog_dir: PathBuf::from("/tmp/cat"),
            data_dir: PathBuf::from("/tmp/data"),
        }
    }

    #[test]
    fn moraine_attach_uses_the_nested_prefix() {
        assert_eq!(
            attach_sql(BackendKind::Moraine, &paths(), None).unwrap(),
            "ATTACH 'ducklake:moraine:/tmp/cat' AS lake \
             (DATA_PATH '/tmp/data', META_DATA_PATH '/tmp/data', META_FLUSH_INTERVAL_MS 1);"
        );
    }

    #[test]
    fn duckdb_attach_points_at_a_catalog_file() {
        assert_eq!(
            attach_sql(BackendKind::DuckdbFile, &paths(), None).unwrap(),
            "ATTACH 'ducklake:/tmp/cat/meta.ducklake' AS lake (DATA_PATH '/tmp/data');"
        );
    }

    #[test]
    fn postgres_attach_embeds_the_dsn_and_requires_one() {
        assert_eq!(
            attach_sql(BackendKind::Postgres, &paths(), Some("dbname=b host=/s")).unwrap(),
            "ATTACH 'ducklake:postgres:dbname=b host=/s' AS lake (DATA_PATH '/tmp/data');"
        );
        assert!(attach_sql(BackendKind::Postgres, &paths(), None).is_err());
    }

    #[test]
    fn newest_postgres_entry_picks_the_highest_version() {
        let entries = vec![
            "postgresql@16".to_owned(),
            "libpq".to_owned(),
            "postgresql@17".to_owned(),
            "postgresql@9".to_owned(),
        ];
        assert_eq!(newest_postgres_entry(&entries), Some((17, "postgresql@17")));
        assert_eq!(newest_postgres_entry(&[]), None);
    }
}
