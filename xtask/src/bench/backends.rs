//! The catalog backends under comparison, as `ATTACH` recipes over fresh
//! per-run directories, plus ephemeral Postgres and Quack provisioning.
//! Everything outside the `ATTACH` statement is identical across backends.

use std::{
    env, fs,
    io::Write as _,
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, bail, ensure};

/// A DuckLake metadata catalog backend.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    /// moraine's SlateDB store over the local filesystem.
    Moraine,
    /// The same, on the S3 Express directory bucket named by
    /// `MORAINE_S3_EXPRESS_BUCKET`, so one run compares the two stores.
    MoraineExpress,
    /// Stock DuckLake: a DuckDB database file.
    DuckdbFile,
    /// Stock DuckLake over a Postgres database.
    Postgres,
    /// Stock DuckLake over a DuckDB served through the Quack protocol.
    Quack,
}

impl BackendKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Moraine => "moraine",
            Self::MoraineExpress => "moraine_express",
            Self::DuckdbFile => "duckdb",
            Self::Postgres => "postgres",
            Self::Quack => "quack",
        }
    }

    pub fn parse(name: &str) -> anyhow::Result<Self> {
        match name {
            "moraine" => Ok(Self::Moraine),
            "moraine_express" => Ok(Self::MoraineExpress),
            "duckdb" => Ok(Self::DuckdbFile),
            "postgres" => Ok(Self::Postgres),
            "quack" => Ok(Self::Quack),
            other => bail!(
                "unknown backend `{other}`; valid: moraine, moraine_express, duckdb, \
                 postgres, quack"
            ),
        }
    }

    pub fn all() -> [Self; 5] {
        [
            Self::Moraine,
            Self::MoraineExpress,
            Self::DuckdbFile,
            Self::Postgres,
            Self::Quack,
        ]
    }

    /// Whether this backend is moraine, on whichever store.
    pub fn is_moraine(self) -> bool {
        matches!(self, Self::Moraine | Self::MoraineExpress)
    }
}

/// The server-side catalog one run attaches, created for it and dropped
/// after it.
pub enum RemoteCatalog {
    /// A Postgres database, by libpq DSN.
    Postgres { dsn: String },
    /// A metadata schema on a Quack server (`host:port`).
    Quack { server: String, schema: String },
}

/// Fresh locations for one (backend, workload, repeat) run: the catalog
/// directory (ignored by Postgres), where moraine's catalog lives (that
/// directory, or an S3 URI when the store is remote), and the Parquet
/// `DATA_PATH`.
pub struct SessionPaths {
    pub catalog_dir: PathBuf,
    pub moraine_catalog: String,
    /// Where moraine keeps what it read, when a run asked for one. Shared
    /// across a sweep, so only the first attach pays for a cold store.
    pub cache_dir: Option<PathBuf>,
    pub data_dir: PathBuf,
}

/// The backend's `ATTACH` statement. `remote` must be the matching
/// [`RemoteCatalog`] for the Postgres and Quack backends, and is ignored
/// by the embedded ones.
///
/// `flush_interval_ms` is moraine's WAL flush spacing, and the other
/// backends ignore it. The suite pins it low so a latency workload
/// measures catalog work rather than the wait for the next flush; the
/// concurrency workloads raise it, because that spacing is what commits
/// coalesce into.
pub fn attach_sql(
    kind: BackendKind,
    paths: &SessionPaths,
    remote: Option<&RemoteCatalog>,
    flush_interval_ms: u64,
) -> anyhow::Result<String> {
    let data = paths.data_dir.display();
    Ok(match kind {
        BackendKind::Moraine | BackendKind::MoraineExpress => format!(
            "ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{data}', \
             META_FLUSH_INTERVAL_MS {flush_interval_ms}{});",
            paths.moraine_catalog,
            paths
                .cache_dir
                .as_ref()
                .map(|dir| {
                    // `META_`-prefixed: the attach is DuckLake's, which
                    // passes the option through to the moraine catalog
                    // under it and refuses it unprefixed.
                    format!(
                        ", META_CACHE_DIR {}",
                        crate::catalog_target::sql_literal(&dir.display().to_string())
                    )
                })
                .unwrap_or_default()
        ),
        BackendKind::DuckdbFile => format!(
            "ATTACH 'ducklake:{}' AS lake (DATA_PATH '{data}');",
            paths.catalog_dir.join("meta.ducklake").display()
        ),
        BackendKind::Postgres => match remote {
            Some(RemoteCatalog::Postgres { dsn }) => {
                format!("ATTACH 'ducklake:postgres:{dsn}' AS lake (DATA_PATH '{data}');")
            }
            _ => bail!("postgres backend needs a DSN"),
        },
        BackendKind::Quack => match remote {
            Some(RemoteCatalog::Quack { server, schema }) => format!(
                "ATTACH 'ducklake:quack:{server}' AS lake (DATA_PATH '{data}', METADATA_SCHEMA '{schema}');"
            ),
            _ => bail!("quack backend needs a server and schema"),
        },
    })
}

/// A DuckDB serving a DuckLake catalog over Quack: a CLI the harness starts
/// over a fresh database file, or an external server named by
/// `MORAINE_BENCH_QUACK` (`host:port`) with `MORAINE_BENCH_QUACK_TOKEN`, and
/// `MORAINE_BENCH_QUACK_CA_CERT` naming the certificate a remote server
/// presents. Every run gets its own metadata schema. An owned server is
/// killed on drop.
pub struct QuackHandle {
    cli: PathBuf,
    server: String,
    token: String,
    ca_cert: Option<String>,
    owned: Option<OwnedQuackServer>,
}

/// The serving CLI. Its stdin stays open, which is what keeps it, and the
/// server it started, alive.
struct OwnedQuackServer {
    process: Child,
    _stdin: ChildStdin,
}

/// How long an owned server is given to start listening.
const QUACK_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// The setting that makes a client accept a Quack server's certificate.
/// `quack_serve` speaks TLS to anything but the loopback, so a server on
/// another host needs its issuer named here. It must be global: DuckLake's
/// metadata connection does not see a session-local value, and the attach
/// fails verification.
fn ca_cert_setting(ca_cert: Option<&str>) -> String {
    match ca_cert {
        Some(path) => format!("SET GLOBAL ca_cert_file='{path}';\n"),
        None => String::new(),
    }
}

impl QuackHandle {
    pub fn provision(cli: &Path, work_dir: &Path) -> anyhow::Result<Self> {
        if let Ok(server) = env::var("MORAINE_BENCH_QUACK") {
            let token = env::var("MORAINE_BENCH_QUACK_TOKEN")
                .context("MORAINE_BENCH_QUACK_TOKEN must accompany MORAINE_BENCH_QUACK")?;
            return Ok(Self {
                cli: cli.to_owned(),
                server,
                token,
                ca_cert: env::var("MORAINE_BENCH_QUACK_CA_CERT").ok(),
                owned: None,
            });
        }

        // Offsetting by pid keeps concurrent bench runs on one machine from
        // colliding; Postgres uses the 20,000 range the same way.
        let port = 30_000 + (std::process::id() % 10_000) as u16;
        let server = format!("localhost:{port}");
        let token = format!("bench-{}", std::process::id());
        let log = fs::File::create(work_dir.join("quack.log"))
            .with_context(|| format!("creating a server log under {}", work_dir.display()))?;
        let mut process = Command::new(cli)
            .args(["-unsigned", "-batch", "-csv"])
            .arg(work_dir.join("quack.duckdb"))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .context("spawning the Quack server CLI")?;
        let mut stdin = process.stdin.take().context("opening the server's stdin")?;
        write!(
            stdin,
            "SET extension_directory='{}';\nINSTALL quack;\nLOAD quack;\n\
             CALL quack_serve('quack:{server}', token => '{token}');\n",
            crate::duckdb::extension_install_directory().display()
        )
        .context("writing the serve script")?;
        stdin.flush().context("flushing the serve script")?;

        let deadline = Instant::now() + QUACK_READY_TIMEOUT;
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            if let Some(status) = process.try_wait().context("polling the Quack server")? {
                bail!(
                    "the Quack server exited with {status} before listening; see {}",
                    work_dir.join("quack.log").display()
                );
            }
            ensure!(
                Instant::now() < deadline,
                "the Quack server did not listen on {server} within {QUACK_READY_TIMEOUT:?}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }

        Ok(Self {
            cli: cli.to_owned(),
            server,
            token,
            ca_cert: None,
            owned: Some(OwnedQuackServer {
                process,
                _stdin: stdin,
            }),
        })
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    /// The `host:port` to probe for a round trip, or `None` for a server
    /// this harness started on the loopback.
    pub fn probe_target(&self) -> Option<String> {
        self.owned.is_none().then(|| self.server.clone())
    }

    /// The untimed statements every session needs to reach the server:
    /// the extension, the token secret, and any certificate to trust.
    pub fn preamble(&self) -> String {
        format!(
            "INSTALL quack;\nLOAD quack;\n{}CREATE SECRET quack_bench (TYPE quack, TOKEN '{}');\n",
            ca_cert_setting(self.ca_cert.as_deref()),
            self.token
        )
    }

    pub fn create_schema(&self, schema: &str) -> anyhow::Result<()> {
        self.remote_sql(&format!("CREATE SCHEMA {schema}"))
    }

    pub fn drop_schema(&self, schema: &str) -> anyhow::Result<()> {
        self.remote_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
    }

    /// Runs `sql` on the server through `quack_query` in a fresh CLI.
    fn remote_sql(&self, sql: &str) -> anyhow::Result<()> {
        let script = format!(
            "SET extension_directory='{}';\nINSTALL quack;\nLOAD quack;\n{}\
             SELECT * FROM quack_query('quack:{}', '{}', token => '{}');",
            crate::duckdb::extension_install_directory().display(),
            ca_cert_setting(self.ca_cert.as_deref()),
            self.server,
            sql.replace('\'', "''"),
            self.token
        );
        let output = Command::new(&self.cli)
            .args(["-unsigned", "-batch", "-csv", "-c", &script])
            .output()
            .context("spawning the CLI for a Quack statement")?;
        ensure!(
            output.status.success(),
            "quack statement `{sql}` failed:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }
}

impl Drop for QuackHandle {
    fn drop(&mut self) {
        if let Some(owned) = &mut self.owned {
            let _ = owned.process.kill();
            let _ = owned.process.wait();
        }
    }
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

    /// The `host:port` to probe for a round trip, or `None` for a cluster
    /// of this harness's own making, which answers over a Unix socket.
    pub fn probe_target(&self) -> Option<String> {
        match &self.mode {
            PostgresMode::Owned { .. } => None,
            PostgresMode::External { base_dsn } => super::endpoints::postgres_target(base_dsn),
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
            moraine_catalog: "/tmp/cat".to_owned(),
            cache_dir: None,
            data_dir: PathBuf::from("/tmp/data"),
        }
    }

    #[test]
    fn moraine_attach_takes_a_remote_catalog_uri() {
        let paths = SessionPaths {
            moraine_catalog: "s3://bucket/prefix/run".to_owned(),
            ..paths()
        };
        assert_eq!(
            attach_sql(BackendKind::Moraine, &paths, None, 1).unwrap(),
            "ATTACH 'ducklake:moraine:s3://bucket/prefix/run' AS lake \
             (DATA_PATH '/tmp/data', META_FLUSH_INTERVAL_MS 1);"
        );
    }

    #[test]
    fn moraine_attach_uses_the_nested_prefix() {
        assert_eq!(
            attach_sql(BackendKind::Moraine, &paths(), None, 1).unwrap(),
            "ATTACH 'ducklake:moraine:/tmp/cat' AS lake \
             (DATA_PATH '/tmp/data', META_FLUSH_INTERVAL_MS 1);"
        );
    }

    #[test]
    fn a_cache_directory_reaches_moraines_attach_and_no_others() {
        let paths = SessionPaths {
            catalog_dir: PathBuf::from("/tmp/cat"),
            moraine_catalog: "/tmp/cat".to_owned(),
            cache_dir: Some(PathBuf::from("/tmp/cache")),
            data_dir: PathBuf::from("/tmp/data"),
        };
        let moraine = attach_sql(BackendKind::Moraine, &paths, None, 1).unwrap();
        assert!(moraine.contains("META_CACHE_DIR '/tmp/cache'"), "{moraine}");

        let express = attach_sql(BackendKind::MoraineExpress, &paths, None, 1).unwrap();
        assert_eq!(express, moraine);

        let duckdb = attach_sql(BackendKind::DuckdbFile, &paths, None, 1).unwrap();
        assert!(!duckdb.contains("CACHE_DIR"), "{duckdb}");
    }

    #[test]
    fn duckdb_attach_points_at_a_catalog_file() {
        assert_eq!(
            attach_sql(BackendKind::DuckdbFile, &paths(), None, 1).unwrap(),
            "ATTACH 'ducklake:/tmp/cat/meta.ducklake' AS lake (DATA_PATH '/tmp/data');"
        );
    }

    #[test]
    fn postgres_attach_embeds_the_dsn_and_requires_one() {
        let remote = RemoteCatalog::Postgres {
            dsn: "dbname=b host=/s".to_owned(),
        };
        assert_eq!(
            attach_sql(BackendKind::Postgres, &paths(), Some(&remote), 1).unwrap(),
            "ATTACH 'ducklake:postgres:dbname=b host=/s' AS lake (DATA_PATH '/tmp/data');"
        );
        assert!(attach_sql(BackendKind::Postgres, &paths(), None, 1).is_err());
    }

    #[test]
    fn a_quack_ca_certificate_is_trusted_only_when_one_is_named() {
        assert_eq!(
            ca_cert_setting(Some("/tmp/quack-ca.pem")),
            "SET GLOBAL ca_cert_file='/tmp/quack-ca.pem';\n"
        );
        assert_eq!(ca_cert_setting(None), "");
    }

    #[test]
    fn quack_attach_names_the_server_and_the_run_schema() {
        let remote = RemoteCatalog::Quack {
            server: "localhost:9494".to_owned(),
            schema: "bench_a".to_owned(),
        };
        assert_eq!(
            attach_sql(BackendKind::Quack, &paths(), Some(&remote), 1).unwrap(),
            "ATTACH 'ducklake:quack:localhost:9494' AS lake \
             (DATA_PATH '/tmp/data', METADATA_SCHEMA 'bench_a');"
        );
        assert!(attach_sql(BackendKind::Quack, &paths(), None, 1).is_err());
    }

    #[test]
    fn the_flush_spacing_reaches_moraines_attach_and_no_others() {
        let moraine = attach_sql(BackendKind::Moraine, &paths(), None, 100).unwrap();
        assert!(moraine.contains("META_FLUSH_INTERVAL_MS 100"), "{moraine}");

        let duckdb = attach_sql(BackendKind::DuckdbFile, &paths(), None, 100).unwrap();
        assert!(!duckdb.contains("FLUSH"), "{duckdb}");
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
