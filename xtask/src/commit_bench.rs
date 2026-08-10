//! DuckLake commit breakdown against an S3-backed Moraine catalog.
//!
//! Parquet stays local so every measured object-store request belongs to
//! metadata. DuckLake's own metadata logger supplies statement counts and
//! time; Moraine's tracing event supplies core commit time; SlateDB's
//! recorder supplies the physical request count and request-latency sum.

use std::{
    env,
    fmt::Write as _,
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, bail, ensure};

use crate::{
    bench::timing::{median, parse_run_times, spread},
    duckdb, ducklake_patch,
};

const DEFAULT_FILES: &[usize] = &[16];
const DEFAULT_COMMITS: usize = 7;
const MARKER: &str = "__MORAINE_COMMIT_BENCH__";

struct Options {
    files: Vec<usize>,
    commits: usize,
    flush_milliseconds: u64,
}

fn parse_options(arguments: &[String]) -> anyhow::Result<Options> {
    let mut files = DEFAULT_FILES.to_vec();
    let mut commits = DEFAULT_COMMITS;
    let mut flush_milliseconds = 25;
    let mut arguments = arguments.iter();
    while let Some(flag) = arguments.next() {
        let value = arguments
            .next()
            .with_context(|| format!("flag `{flag}` needs a value"))?;
        match flag.as_str() {
            "--files" => {
                files = value
                    .split(',')
                    .map(|value| value.parse().context("parsing --files"))
                    .collect::<anyhow::Result<_>>()?;
                ensure!(
                    !files.is_empty() && files.iter().all(|&count| count > 0),
                    "--files needs positive comma-separated counts"
                );
            }
            "--commits" => {
                commits = value.parse().context("parsing --commits")?;
                ensure!(commits > 0, "--commits must be positive");
            }
            "--flush-ms" => {
                flush_milliseconds = value.parse().context("parsing --flush-ms")?;
            }
            other => bail!("unknown flag `{other}`; valid: --files, --commits, --flush-ms"),
        }
    }
    Ok(Options {
        files,
        commits,
        flush_milliseconds,
    })
}

struct S3Target {
    bucket: String,
    prefix: String,
    endpoint: Option<String>,
    region: String,
}

impl S3Target {
    fn from_environment() -> anyhow::Result<Self> {
        Ok(Self {
            bucket: env::var("MORAINE_S3_BUCKET").context("MORAINE_S3_BUCKET must be set")?,
            prefix: env::var("MORAINE_S3_PREFIX").unwrap_or_default(),
            endpoint: env::var("MORAINE_S3_ENDPOINT").ok(),
            region: env::var("AWS_REGION")
                .or_else(|_| env::var("AWS_DEFAULT_REGION"))
                .unwrap_or_else(|_| "us-east-1".to_owned()),
        })
    }

    fn description(&self) -> String {
        self.endpoint
            .clone()
            .unwrap_or_else(|| format!("AWS S3 in {}", self.region))
    }

    fn secret_sql(&self) -> anyhow::Result<String> {
        let region = sql_literal(&self.region);
        let Some(endpoint) = &self.endpoint else {
            return Ok(format!(
                "CREATE SECRET moraine_commit_bench (TYPE s3, PROVIDER credential_chain, REGION {region});"
            ));
        };

        let key = env::var("AWS_ACCESS_KEY_ID")
            .context("AWS_ACCESS_KEY_ID must be set for an explicit S3 endpoint")?;
        let secret = env::var("AWS_SECRET_ACCESS_KEY")
            .context("AWS_SECRET_ACCESS_KEY must be set for an explicit S3 endpoint")?;
        let use_ssl = endpoint.starts_with("https://");
        let token = env::var("AWS_SESSION_TOKEN")
            .ok()
            .map(|token| format!(", SESSION_TOKEN {}", sql_literal(&token)))
            .unwrap_or_default();
        Ok(format!(
            "CREATE SECRET moraine_commit_bench (TYPE s3, KEY_ID {}, SECRET {}, REGION {region}, \
             ENDPOINT {}, URL_STYLE 'path', USE_SSL {use_ssl}{token});",
            sql_literal(&key),
            sql_literal(&secret),
            sql_literal(endpoint),
        ))
    }

    fn catalog_uri(&self, files: usize) -> anyhow::Result<String> {
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_millis();
        let leaf = format!("commit-breakdown-{files}-{}-{epoch}", std::process::id());
        let prefix = self.prefix.trim_matches('/');
        let path = if prefix.is_empty() {
            leaf
        } else {
            format!("{prefix}/{leaf}")
        };
        Ok(format!("s3://{}/{path}", self.bucket))
    }
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(files: usize) -> anyhow::Result<Self> {
        let path = env::temp_dir().join(format!(
            "moraine-commit-bench-{}-{files}",
            std::process::id()
        ));
        if path.exists() {
            fs::remove_dir_all(&path).with_context(|| format!("clearing {}", path.display()))?;
        }
        fs::create_dir_all(path.join("data"))
            .with_context(|| format!("creating {}", path.display()))?;
        Ok(Self(path))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, PartialEq)]
struct Breakdown {
    metadata_statements: u64,
    metadata_milliseconds: f64,
    committed_scans: u64,
    committed_scan_milliseconds: f64,
    moraine_commits: u64,
    moraine_commit_milliseconds: f64,
    staged_bytes: u64,
    durable_commits: u64,
    durable_milliseconds: f64,
    main_gets: u64,
    main_get_milliseconds: f64,
    main_puts: u64,
    main_put_milliseconds: f64,
    wal_gets: u64,
    wal_get_milliseconds: f64,
    wal_puts: u64,
    wal_put_milliseconds: f64,
    errors: u64,
}

fn parse_field<T>(fields: &[&str], index: usize, name: &str) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    fields
        .get(index)
        .with_context(|| format!("missing `{name}`"))?
        .parse()
        .with_context(|| format!("parsing `{name}`"))
}

fn parse_breakdown(stdout: &str) -> anyhow::Result<Breakdown> {
    let row = stdout
        .lines()
        .find(|line| line.starts_with(MARKER))
        .with_context(|| format!("benchmark result row is missing from CLI output:\n{stdout}"))?;
    let fields: Vec<&str> = row.split(',').collect();
    ensure!(
        fields.len() == 19,
        "benchmark result has {} fields",
        fields.len()
    );
    Ok(Breakdown {
        metadata_statements: parse_field(&fields, 1, "metadata_statements")?,
        metadata_milliseconds: parse_field(&fields, 2, "metadata_ms")?,
        committed_scans: parse_field(&fields, 3, "committed_scans")?,
        committed_scan_milliseconds: parse_field(&fields, 4, "committed_scan_ms")?,
        moraine_commits: parse_field(&fields, 5, "moraine_commits")?,
        moraine_commit_milliseconds: parse_field(&fields, 6, "moraine_commit_ms")?,
        staged_bytes: parse_field(&fields, 7, "staged_bytes")?,
        durable_commits: parse_field(&fields, 8, "durable_commits")?,
        durable_milliseconds: parse_field(&fields, 9, "durable_ms")?,
        main_gets: parse_field(&fields, 10, "main_gets")?,
        main_get_milliseconds: parse_field(&fields, 11, "main_get_ms")?,
        main_puts: parse_field(&fields, 12, "main_puts")?,
        main_put_milliseconds: parse_field(&fields, 13, "main_put_ms")?,
        wal_gets: parse_field(&fields, 14, "wal_gets")?,
        wal_get_milliseconds: parse_field(&fields, 15, "wal_get_ms")?,
        wal_puts: parse_field(&fields, 16, "wal_puts")?,
        wal_put_milliseconds: parse_field(&fields, 17, "wal_put_ms")?,
        errors: parse_field(&fields, 18, "errors")?,
    })
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn report_sql() -> &'static str {
    "WITH metadata AS (\n\
         SELECT count(*) AS statements, coalesce(sum(elapsed_ms), 0) AS elapsed_ms\n\
         FROM duckdb_logs_parsed('DuckLakeMetadata') WHERE catalog = 'lake'\n\
     ), core AS (\n\
         SELECT count(*) AS commits,\n\
                coalesce(sum(try_cast(regexp_extract(message, 'elapsed_ms=([0-9]+)', 1) AS UBIGINT)), 0) AS elapsed_ms,\n\
                coalesce(sum(try_cast(regexp_extract(message, 'staged_bytes=([0-9]+)', 1) AS UBIGINT)), 0) AS staged_bytes\n\
         FROM duckdb_logs\n\
         WHERE type = 'moraine' AND contains(message, 'staged commit landed')\n\
     ), durable AS (\n\
         SELECT count(*) AS commits,\n\
                coalesce(sum(try_cast(regexp_extract(message, 'elapsed_ms=([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)), 0) AS elapsed_ms\n\
         FROM duckdb_logs\n\
         WHERE type = 'moraine' AND contains(message, 'durable commit landed')\n\
     ), committed_scan AS (\n\
         SELECT count(*) AS scans,\n\
                coalesce(sum(try_cast(regexp_extract(message, 'elapsed_ms=([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)), 0) AS elapsed_ms\n\
         FROM duckdb_logs\n\
         WHERE type = 'moraine'\n\
           AND contains(message, 'scanned committed entities for staged transaction')\n\
     ), after AS (SELECT * FROM moraine_object_store_tally('lake'))\n\
     SELECT '__MORAINE_COMMIT_BENCH__', metadata.statements, metadata.elapsed_ms,\n\
            committed_scan.scans, committed_scan.elapsed_ms,\n\
            core.commits, core.elapsed_ms, core.staged_bytes,\n\
            durable.commits, durable.elapsed_ms,\n\
            after.main_gets - before.main_gets, after.main_get_ms - before.main_get_ms,\n\
            after.main_puts - before.main_puts, after.main_put_ms - before.main_put_ms,\n\
            after.wal_gets - before.wal_gets, after.wal_get_ms - before.wal_get_ms,\n\
            after.wal_puts - before.wal_puts, after.wal_put_ms - before.wal_put_ms,\n\
            after.errors - before.errors\n\
     FROM metadata, committed_scan, core, durable, after, io_before AS before;"
}

struct Artifacts<'a> {
    cli: &'a Path,
    moraine: &'a Path,
    ducklake: &'a Path,
}

fn run_once(
    artifacts: &Artifacts<'_>,
    target: &S3Target,
    files: usize,
    commits: usize,
    flush_milliseconds: u64,
) -> anyhow::Result<(Vec<f64>, Breakdown)> {
    let temp = TempDir::new(files)?;
    let data_path = temp.0.join("data");
    let catalog_uri = target.catalog_uri(files)?;

    let mut script = String::new();
    let _ = writeln!(script, "SET threads=1;");
    let _ = writeln!(
        script,
        "SET extension_directory={};",
        sql_literal(&duckdb::extension_install_directory().display().to_string())
    );
    script.push_str("INSTALL httpfs;\nLOAD httpfs;\n");
    let _ = writeln!(
        script,
        "LOAD {};",
        sql_literal(&artifacts.ducklake.display().to_string())
    );
    let _ = writeln!(
        script,
        "LOAD {};",
        sql_literal(&artifacts.moraine.display().to_string())
    );
    let _ = writeln!(script, "{}", target.secret_sql()?);
    let _ = writeln!(
        script,
        "ATTACH {} AS lake (DATA_PATH {}, META_DATA_PATH {}, META_FLUSH_INTERVAL_MS {flush_milliseconds}, READ_WRITE);",
        sql_literal(&format!("ducklake:moraine:{catalog_uri}")),
        sql_literal(&data_path.display().to_string()),
        sql_literal(&data_path.display().to_string())
    );
    script.push_str("CREATE TABLE lake.main.items(id BIGINT, payload BIGINT);\n");
    for file in 0..files {
        let start = file.saturating_mul(100);
        let _ = writeln!(
            script,
            "INSERT INTO lake.main.items SELECT range, range FROM range({start}, {});",
            start.saturating_add(100)
        );
    }
    script.push_str(
        "CREATE TEMP TABLE io_before AS SELECT * FROM moraine_object_store_tally('lake');\n",
    );
    script.push_str("CALL enable_logging(level => 'debug', storage => 'memory');\n");
    script.push_str(".timer on\n");
    for commit in 0..commits {
        let id = commit % files * 100;
        let _ = writeln!(
            script,
            "UPDATE lake.main.items SET payload = payload + 1 WHERE id = {id};"
        );
    }
    script.push_str(".timer off\n");
    script.push_str(report_sql());
    script.push('\n');

    let mut child = Command::new(artifacts.cli)
        .arg("-unsigned")
        .arg("-batch")
        .arg("-csv")
        .env("MORAINE_LOG", "debug")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning DuckDB for the commit benchmark")?;
    child
        .stdin
        .take()
        .context("opening DuckDB stdin")?
        .write_all(script.as_bytes())
        .context("writing the commit benchmark")?;
    let output = child.wait_with_output().context("waiting for DuckDB")?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    ensure!(
        output.status.success(),
        "commit benchmark failed:\n--- script ---\n{script}\n--- stdout ---\n{stdout}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let times = parse_run_times(&stdout);
    ensure!(
        times.len() == commits,
        "expected {commits} timed commits, found {}; output:\n{stdout}",
        times.len()
    );
    let breakdown = parse_breakdown(&stdout)?;
    ensure!(
        breakdown.moraine_commits == u64::try_from(commits)?,
        "expected {commits} Moraine commits, observed {}",
        breakdown.moraine_commits
    );
    ensure!(
        breakdown.durable_commits == u64::try_from(commits)?,
        "expected {commits} durable commits, observed {}",
        breakdown.durable_commits
    );
    Ok((times, breakdown))
}

#[expect(
    clippy::cast_precision_loss,
    reason = "benchmark counts are tiny and the report is rounded to one decimal place"
)]
fn per_commit(value: f64, commits: usize) -> f64 {
    value / commits as f64
}

#[expect(
    clippy::cast_precision_loss,
    reason = "benchmark counts are tiny and the report is rounded to one decimal place"
)]
fn count_per_commit(value: u64, commits: usize) -> f64 {
    per_commit(value as f64, commits)
}

/// Builds patched DuckLake and measures the S3-backed metadata commit path.
pub fn run(arguments: &[String]) -> anyhow::Result<()> {
    let options = parse_options(arguments)?;
    let target = S3Target::from_environment()?;
    let cli = duckdb::ensure_duckdb_cli()?;
    let moraine = duckdb::build_and_package_extension()?;
    let ducklake = ducklake_patch::build_artifact(&[])?;
    let artifacts = Artifacts {
        cli: &cli,
        moraine: &moraine,
        ducklake: &ducklake,
    };

    println!(
        "\n# DuckLake commit breakdown against {}",
        target.description()
    );
    println!(
        "# local Parquet; {} one-row UPDATE commits; flush={}ms; request durations are summed and may overlap\n",
        options.commits, options.flush_milliseconds
    );
    println!(
        "{:>7}  {:>10}  {:>9}  {:>9}  {:>10}  {:>10}  {:>9}  {:>9}  {:>10}  {:>10}  {:>10}  {:>9}",
        "files",
        "commit_med",
        "min_ms",
        "max_ms",
        "meta_stmt",
        "meta_ms",
        "scan_ms",
        "core_ms",
        "bytes",
        "durable_ms",
        "requests",
        "req_ms"
    );

    for files in options.files {
        let (mut samples, breakdown) = run_once(
            &artifacts,
            &target,
            files,
            options.commits,
            options.flush_milliseconds,
        )?;
        let (minimum, maximum) = spread(&samples)?;
        let commit_median = median(&mut samples)?;
        let request_count =
            breakdown.main_gets + breakdown.main_puts + breakdown.wal_gets + breakdown.wal_puts;
        let request_milliseconds = breakdown.main_get_milliseconds
            + breakdown.main_put_milliseconds
            + breakdown.wal_get_milliseconds
            + breakdown.wal_put_milliseconds;
        println!(
            "{files:>7}  {:>10.1}  {:>9.1}  {:>9.1}  {:>10.1}  {:>10.1}  {:>9.1}  {:>9.1}  {:>10.1}  {:>10.1}  {:>10.1}  {:>9.1}",
            commit_median * 1_000.0,
            minimum * 1_000.0,
            maximum * 1_000.0,
            count_per_commit(breakdown.metadata_statements, options.commits),
            per_commit(breakdown.metadata_milliseconds, options.commits),
            per_commit(breakdown.committed_scan_milliseconds, options.commits),
            per_commit(breakdown.moraine_commit_milliseconds, options.commits),
            count_per_commit(breakdown.staged_bytes, options.commits),
            per_commit(breakdown.durable_milliseconds, options.commits),
            count_per_commit(request_count, options.commits),
            per_commit(request_milliseconds, options.commits),
        );
        println!(
            "# {files} files: {:.1} committed scans/commit; I/O per commit: \
             main GET {:.1}/{:.1}ms, main PUT {:.1}/{:.1}ms, WAL GET {:.1}/{:.1}ms, \
             WAL PUT {:.1}/{:.1}ms, errors {:.1}",
            count_per_commit(breakdown.committed_scans, options.commits),
            count_per_commit(breakdown.main_gets, options.commits),
            per_commit(breakdown.main_get_milliseconds, options.commits),
            count_per_commit(breakdown.main_puts, options.commits),
            per_commit(breakdown.main_put_milliseconds, options.commits),
            count_per_commit(breakdown.wal_gets, options.commits),
            per_commit(breakdown.wal_get_milliseconds, options.commits),
            count_per_commit(breakdown.wal_puts, options.commits),
            per_commit(breakdown.wal_put_milliseconds, options.commits),
            count_per_commit(breakdown.errors, options.commits),
        );
    }
    println!();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_tagged_breakdown_among_timer_output() {
        let output = "Run Time (s): real 0.100 user 0 sys 0\n\
                      __MORAINE_COMMIT_BENCH__,140,560,7,175.5,7,350,7000,7,280,14,280,7,140,0,0,7,210,0\n";
        assert_eq!(
            parse_breakdown(output).unwrap(),
            Breakdown {
                metadata_statements: 140,
                metadata_milliseconds: 560.0,
                committed_scans: 7,
                committed_scan_milliseconds: 175.5,
                moraine_commits: 7,
                moraine_commit_milliseconds: 350.0,
                staged_bytes: 7_000,
                durable_commits: 7,
                durable_milliseconds: 280.0,
                main_gets: 14,
                main_get_milliseconds: 280.0,
                main_puts: 7,
                main_put_milliseconds: 140.0,
                wal_gets: 0,
                wal_get_milliseconds: 0.0,
                wal_puts: 7,
                wal_put_milliseconds: 210.0,
                errors: 0,
            }
        );
    }

    #[test]
    fn options_accept_a_file_sweep() {
        let arguments = ["--files", "16,128", "--commits", "5", "--flush-ms", "1"]
            .map(str::to_owned)
            .to_vec();
        let options = parse_options(&arguments).unwrap();
        assert_eq!(options.files, [16, 128]);
        assert_eq!(options.commits, 5);
        assert_eq!(options.flush_milliseconds, 1);
    }
}
