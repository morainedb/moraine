//! Cold read-only attach over a large catalog: what a fresh reader process
//! pays to open the store, list every data file, and plan one query, and
//! what a warm repeat of that query still pays. The attach is split into
//! the store open and the first materialization by moraine's own log
//! events, since the two have different levers.
//!
//! Parquet stays local so every measured object-store request belongs to
//! metadata. Each repeat is a new DuckDB process, so nothing survives from
//! the one before except what `--cache-dir` keeps on disk.
//!
//! `--compaction` keeps that process attached while a second one commits
//! and compacts the store, then plans twice more: what a long-lived reader
//! pays on the first plan over the rewritten SSTs, and whether the plan
//! after that is warm again.

use std::{
    env,
    fmt::Write as _,
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, bail, ensure};

use crate::{
    bench::{
        support::TempDir,
        timing::{median, parse_run_times},
    },
    catalog_target::{CatalogTarget, sql_literal},
    duckdb, ducklake_patch,
};

const DEFAULT_FILES: &[usize] = &[2_000, 20_000];
const DEFAULT_REPEAT: usize = 5;

/// Tables the files are spread over. One by default, so a statement's cost
/// is the whole catalog's; more says what a many-table lake pays per
/// statement for a query that addresses one of them.
const DEFAULT_TABLES: usize = 1;

/// Percentage of the seeded files that carry a delete file. None by
/// default: a lake that has never deleted holds no delete-file rows, so
/// the kind costs a statement nothing.
const DEFAULT_DELETES: usize = 0;

/// Files one seeding commit registers: one per partition value.
const FILES_PER_COMMIT: usize = 1_000;

/// Rows in each seeded file.
const ROWS_PER_FILE: usize = 10;

const MARKER: &str = "__MORAINE_READER_BENCH__";

struct Options {
    files: Vec<usize>,
    tables: usize,
    deletes: usize,
    repeat: usize,
    cache_dir: Option<PathBuf>,
    compaction: bool,
}

fn parse_options(arguments: &[String]) -> anyhow::Result<Options> {
    let mut options = Options {
        files: DEFAULT_FILES.to_vec(),
        tables: DEFAULT_TABLES,
        deletes: DEFAULT_DELETES,
        repeat: DEFAULT_REPEAT,
        cache_dir: None,
        compaction: false,
    };
    let mut arguments = arguments.iter();
    while let Some(flag) = arguments.next() {
        if flag == "--compaction" {
            options.compaction = true;
            continue;
        }
        let value = arguments
            .next()
            .with_context(|| format!("flag `{flag}` needs a value"))?;
        match flag.as_str() {
            "--files" => {
                options.files = value
                    .split(',')
                    .map(|value| value.parse().context("parsing --files"))
                    .collect::<anyhow::Result<_>>()?;
                ensure!(
                    !options.files.is_empty() && options.files.iter().all(|&count| count > 0),
                    "--files needs positive comma-separated counts"
                );
            }
            "--repeat" => {
                options.repeat = value.parse().context("parsing --repeat")?;
                ensure!(options.repeat > 0, "--repeat must be positive");
            }
            "--tables" => {
                options.tables = value.parse().context("parsing --tables")?;
                ensure!(options.tables > 0, "--tables must be positive");
            }
            "--deletes" => {
                options.deletes = value.parse().context("parsing --deletes")?;
                ensure!(options.deletes <= 100, "--deletes is a percentage");
            }
            "--cache-dir" => options.cache_dir = Some(PathBuf::from(value)),
            other => bail!(
                "unknown flag `{other}`; valid: --files, --tables, --deletes, --repeat, \
                 --cache-dir, --compaction"
            ),
        }
    }
    Ok(options)
}

/// How a catalog of one size is seeded: `commits` inserts into a table
/// partitioned `partitions` ways, each registering one file per partition.
#[derive(Debug, PartialEq, Eq)]
struct SeedPlan {
    partitions: usize,
    commits: usize,
}

fn seed_plan(files: usize) -> SeedPlan {
    let partitions = files.clamp(1, FILES_PER_COMMIT);
    SeedPlan {
        partitions,
        commits: files.div_ceil(partitions),
    }
}

/// One cold process's measurements.
#[derive(Debug, PartialEq)]
struct Sample {
    attach_ms: f64,
    open_ms: f64,
    files_ms: f64,
    plan_ms: f64,
    warm_ms: f64,
    cold_requests: u64,
    cold_request_ms: f64,
    warm_requests: u64,
    warm_request_ms: f64,
    errors: u64,
    compacted: Option<Compacted>,
}

/// The same process's plans after another process committed and compacted
/// the store: the first over the rewritten SSTs, and the one after it.
#[derive(Debug, PartialEq)]
struct Compacted {
    plan_ms: f64,
    requests: u64,
    request_ms: f64,
    settled_ms: f64,
    settled_requests: u64,
    settled_request_ms: f64,
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

fn parse_sample(stdout: &str, compaction: bool) -> anyhow::Result<Sample> {
    let times = parse_run_times(stdout);
    let statements = if compaction { 6 } else { 4 };
    ensure!(
        times.len() == statements,
        "expected {statements} timed statements, found {}; output:\n{stdout}",
        times.len()
    );
    let row = stdout
        .lines()
        .find(|line| line.starts_with(MARKER))
        .with_context(|| format!("benchmark result row is missing from CLI output:\n{stdout}"))?;
    let fields: Vec<&str> = row.split(',').collect();
    ensure!(
        fields.len() == if compaction { 11 } else { 7 },
        "benchmark result has {} fields",
        fields.len()
    );
    let compacted = compaction
        .then(|| {
            Ok::<_, anyhow::Error>(Compacted {
                plan_ms: times[4] * 1_000.0,
                requests: parse_field(&fields, 7, "compacted_requests")?,
                request_ms: parse_field(&fields, 8, "compacted_request_ms")?,
                settled_ms: times[5] * 1_000.0,
                settled_requests: parse_field(&fields, 9, "settled_requests")?,
                settled_request_ms: parse_field(&fields, 10, "settled_request_ms")?,
            })
        })
        .transpose()?;
    Ok(Sample {
        attach_ms: times[0] * 1_000.0,
        open_ms: parse_field(&fields, 6, "open_ms")?,
        files_ms: times[1] * 1_000.0,
        plan_ms: times[2] * 1_000.0,
        warm_ms: times[3] * 1_000.0,
        cold_requests: parse_field(&fields, 1, "cold_requests")?,
        cold_request_ms: parse_field(&fields, 2, "cold_request_ms")?,
        warm_requests: parse_field(&fields, 3, "warm_requests")?,
        warm_request_ms: parse_field(&fields, 4, "warm_request_ms")?,
        errors: parse_field(&fields, 5, "errors")?,
        compacted,
    })
}

/// The request count and summed request time one tally holds.
fn tally_sum(alias: &str) -> [String; 2] {
    [
        format!(
            "{alias}.main_gets + {alias}.main_heads + {alias}.main_lists + {alias}.wal_gets \
             + {alias}.wal_heads + {alias}.wal_lists"
        ),
        format!(
            "{alias}.main_get_ms + {alias}.main_head_ms + {alias}.wal_get_ms + {alias}.wal_head_ms"
        ),
    ]
}

struct Artifacts<'a> {
    cli: &'a Path,
    moraine: &'a Path,
}

/// The statements every script opens with: one thread, the extension
/// directory, `httpfs` for a remote target, the extension, the secret.
fn preamble(artifacts: &Artifacts<'_>, target: &CatalogTarget) -> anyhow::Result<String> {
    let mut script = String::new();
    let _ = writeln!(script, "SET threads=1;");
    let _ = writeln!(
        script,
        "SET extension_directory={};",
        sql_literal(&duckdb::extension_install_directory().display().to_string())
    );
    script.push_str(&target.extensions_sql());
    let _ = writeln!(
        script,
        "LOAD {};",
        sql_literal(&artifacts.moraine.display().to_string())
    );
    if let Some(secret) = target.secret_sql()? {
        let _ = writeln!(script, "{secret}");
    }
    Ok(script)
}

fn run_script(artifacts: &Artifacts<'_>, script: &str, what: &str) -> anyhow::Result<String> {
    let mut child = Command::new(artifacts.cli)
        .arg("-unsigned")
        .arg("-batch")
        .arg("-csv")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawning DuckDB for {what}"))?;
    child
        .stdin
        .take()
        .context("opening DuckDB stdin")?
        .write_all(script.as_bytes())
        .with_context(|| format!("writing {what}"))?;
    let output = child.wait_with_output().context("waiting for DuckDB")?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    ensure!(
        output.status.success(),
        "{what} failed:\n--- script ---\n{script}\n--- stdout ---\n{stdout}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(stdout)
}

/// The table a measured query addresses.
fn table_name(index: usize) -> String {
    format!("items{index}")
}

/// The `DELETE` that gives `deletes` percent of one table's files a delete
/// file: one row out of each, chosen so no file is covered whole (which
/// would drop the file instead of writing a deletion beside it). The seed
/// attach turns inlining off, or a deletion this small is recorded in the
/// store rather than as a delete file.
///
/// A file holds the rows whose ids are `start + k + j * partitions` for
/// `j` in `0..ROWS_PER_FILE`, so `id // partitions` names the row within
/// its file and `k` names the file within its commit.
fn delete_statement(name: &str, partitions: usize, deletes: usize) -> String {
    format!(
        "DELETE FROM lake.main.{name} WHERE (id // {partitions}) % {ROWS_PER_FILE} = 0 \
         AND k % 100 < {deletes};"
    )
}

/// Seeds `files` data files spread evenly over `tables` partitioned
/// tables, each filled by inserts that write one file per partition value,
/// then gives `deletes` percent of them a delete file.
fn seed(
    artifacts: &Artifacts<'_>,
    target: &CatalogTarget,
    data_path: &Path,
    catalog_uri: &str,
    files: usize,
    tables: usize,
    deletes: usize,
) -> anyhow::Result<()> {
    let plan = seed_plan(files.div_ceil(tables));
    let mut script = preamble(artifacts, target)?;
    let _ = writeln!(
        script,
        "ATTACH {} AS lake (DATA_PATH {}, META_FLUSH_INTERVAL_MS 1, \
         DATA_INLINING_ROW_LIMIT 0, READ_WRITE);",
        sql_literal(&format!("ducklake:moraine:{catalog_uri}")),
        sql_literal(&data_path.display().to_string())
    );
    let rows_per_commit = plan.partitions.saturating_mul(ROWS_PER_FILE);
    for table in 0..tables {
        let name = table_name(table);
        let _ = writeln!(
            script,
            "CREATE TABLE lake.main.{name}(id BIGINT, k BIGINT);"
        );
        let _ = writeln!(
            script,
            "ALTER TABLE lake.main.{name} SET PARTITIONED BY (k);"
        );
        for commit in 0..plan.commits {
            let start = commit.saturating_mul(rows_per_commit);
            let _ = writeln!(
                script,
                "INSERT INTO lake.main.{name} SELECT i, i % {} FROM range({start}, {}) t(i);",
                plan.partitions,
                start.saturating_add(rows_per_commit)
            );
        }
        if deletes > 0 {
            let _ = writeln!(
                script,
                "{}",
                delete_statement(&name, plan.partitions, deletes)
            );
        }
    }
    let _ = writeln!(
        script,
        "SELECT '{MARKER}', \
         (SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file \
          WHERE end_snapshot IS NULL), \
         (SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file \
          WHERE end_snapshot IS NULL);"
    );

    let stdout = run_script(artifacts, &script, "seeding the reader benchmark")?;
    let row = stdout
        .lines()
        .find(|line| line.starts_with(MARKER))
        .with_context(|| format!("file count is missing from CLI output:\n{stdout}"))?;
    let fields: Vec<&str> = row.split(',').collect();
    let seeded: usize = parse_field(&fields, 1, "files")?;
    let expected = plan
        .partitions
        .saturating_mul(plan.commits)
        .saturating_mul(tables);
    ensure!(
        seeded == expected,
        "seeding registered {seeded} files, expected {expected}"
    );

    // A delete that covered a file whole would drop it rather than write a
    // deletion beside it, so the count is what says the shape is right.
    let with_deletions: usize = parse_field(&fields, 2, "delete files")?;
    let owed = expected.saturating_mul(deletes.min(100)) / 100;
    ensure!(
        with_deletions == owed,
        "seeding registered {with_deletions} delete files, expected {owed}"
    );
    Ok(())
}

/// The store open within an attach, read back from the event moraine logs
/// as it opens. Milliseconds, and NULL if the event never fired.
///
/// There was a second half once, the first materialization of the
/// catalog. Scopes load on demand now, so no attach materializes
/// anything and there is nothing there to time.
const STORE_OPEN_SQL: &str = "\
    (SELECT CAST(regexp_extract(message, 'elapsed_ms=([0-9]+)', 1) AS BIGINT) FROM duckdb_logs \
     WHERE type = 'moraine' AND message LIKE '%opened catalog read-only%' LIMIT 1)";

/// One cold process: attach read-only, list every data file, plan a query
/// pruned to one file, then plan another with everything warm.
fn measure(
    artifacts: &Artifacts<'_>,
    target: &CatalogTarget,
    data_path: &Path,
    catalog_uri: &str,
    cache_dir: Option<&Path>,
    writer_script: Option<&Path>,
) -> anyhow::Result<Sample> {
    let mut script = preamble(artifacts, target)?;
    // `META_`-prefixed: the attach is DuckLake's, which passes the option
    // through to the moraine catalog under it and refuses it unprefixed.
    let cache = cache_dir
        .map(|dir| {
            format!(
                ", META_CACHE_DIR {}",
                sql_literal(&dir.display().to_string())
            )
        })
        .unwrap_or_default();
    // A reader that must notice the other process's commit polls for it
    // on a short window: the statements before the commit run inside one
    // window, and a pause after it puts the next plan outside.
    let poll = if writer_script.is_some() {
        ", META_READER_POLL_INTERVAL_MS 2000"
    } else {
        ""
    };
    script.push_str("CALL enable_logging(level => 'info', storage => 'memory');\n");
    script.push_str(".timer on\n");
    let _ = writeln!(
        script,
        "ATTACH {} AS lake (DATA_PATH {}, READ_ONLY{cache}{poll});",
        sql_literal(&format!("ducklake:moraine:{catalog_uri}")),
        sql_literal(&data_path.display().to_string())
    );
    script.push_str(
        "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file WHERE end_snapshot IS NULL;\n",
    );
    let _ = writeln!(
        script,
        "SELECT count(*) FROM lake.main.{} WHERE k = 7 AND id = 7;",
        table_name(0)
    );
    script.push_str(".timer off\n");
    script.push_str(
        "CREATE TEMP TABLE io_cold AS SELECT * FROM moraine_object_store_tally('lake');\n",
    );
    script.push_str(".timer on\n");
    let _ = writeln!(
        script,
        "SELECT count(*) FROM lake.main.{} WHERE k = 8 AND id = 8;",
        table_name(0)
    );
    script.push_str(".timer off\n");
    script.push_str(
        "CREATE TEMP TABLE io_warm AS SELECT * FROM moraine_object_store_tally('lake');\n",
    );
    let [cold_requests, cold_ms] = tally_sum("cold");
    let [warm_requests, warm_ms] = tally_sum("warm");
    let mut columns = format!(
        "{cold_requests}, {cold_ms}, {warm_requests} - ({cold_requests}), {warm_ms} - ({cold_ms}), \
         after.errors, {STORE_OPEN_SQL}"
    );
    let mut tables = String::from("io_cold AS cold, io_warm AS warm");
    if let Some(writer_script) = writer_script {
        // The other process commits one row and compacts the store while
        // this one stays attached; the two plans after it are timed. The
        // shell escape re-tokenizes its line, so the command lives in a
        // wrapper beside the script, and its output beside that.
        let _ = writeln!(
            script,
            ".system sh {}",
            writer_wrapper(writer_script).display()
        );
        script.push_str(".timer on\n");
        let _ = writeln!(
            script,
            "SELECT count(*) FROM lake.main.{} WHERE k = 9 AND id = 9;",
            table_name(0)
        );
        script.push_str(".timer off\n");
        script.push_str(
            "CREATE TEMP TABLE io_compacted AS SELECT * FROM moraine_object_store_tally('lake');\n",
        );
        script.push_str(".timer on\n");
        let _ = writeln!(
            script,
            "SELECT count(*) FROM lake.main.{} WHERE k = 10 AND id = 10;",
            table_name(0)
        );
        script.push_str(".timer off\n");
        let [compacted_requests, compacted_ms] = tally_sum("compacted");
        let [after_requests, after_ms] = tally_sum("after");
        let _ = write!(
            columns,
            ", {compacted_requests} - ({warm_requests}), {compacted_ms} - ({warm_ms}), \
             {after_requests} - ({compacted_requests}), {after_ms} - ({compacted_ms})"
        );
        tables.push_str(", io_compacted AS compacted");
    }
    let _ = writeln!(
        script,
        "SELECT '{MARKER}', {columns} FROM moraine_object_store_tally('lake') AS after, {tables};"
    );

    let stdout = run_script(artifacts, &script, "the reader benchmark")?;
    if let Some(writer_script) = writer_script {
        let output = writer_output(writer_script);
        let written =
            fs::read_to_string(&output).with_context(|| format!("reading {}", output.display()))?;
        ensure!(
            written.lines().any(|line| line.starts_with(WRITER_MARKER)),
            "the committing process did not report its compaction:\n{written}"
        );
    }
    parse_sample(&stdout, writer_script.is_some())
}

const WRITER_MARKER: &str = "__MORAINE_READER_BENCH_WRITER__";

/// Where the committing process's output lands, beside its script.
fn writer_output(writer_script: &Path) -> PathBuf {
    writer_script.with_extension("out")
}

/// The shell wrapper the reader's `.system` runs, beside the script.
fn writer_wrapper(writer_script: &Path) -> PathBuf {
    writer_script.with_extension("sh")
}

/// Writes the committing process's script and the wrapper that runs it:
/// the CLI over the script, output captured, then a pause longer than the
/// reader's poll window so its next plan looks for the commit.
fn write_writer_scripts(
    artifacts: &Artifacts<'_>,
    target: &CatalogTarget,
    data_path: &Path,
    catalog_uri: &str,
    writer_script: &Path,
) -> anyhow::Result<()> {
    fs::write(
        writer_script,
        writer_sql(artifacts, target, data_path, catalog_uri)?,
    )
    .with_context(|| format!("writing {}", writer_script.display()))?;
    let wrapper = format!(
        "{} -unsigned -batch -csv < {} > {} 2>&1\nsleep 3\n",
        artifacts.cli.display(),
        writer_script.display(),
        writer_output(writer_script).display()
    );
    let path = writer_wrapper(writer_script);
    fs::write(&path, wrapper).with_context(|| format!("writing {}", path.display()))
}

/// The script the second process runs under `--compaction`: one small
/// commit, so the compaction has new work, then a compaction it waits for.
fn writer_sql(
    artifacts: &Artifacts<'_>,
    target: &CatalogTarget,
    data_path: &Path,
    catalog_uri: &str,
) -> anyhow::Result<String> {
    let mut script = preamble(artifacts, target)?;
    let _ = writeln!(
        script,
        "ATTACH {} AS lake (DATA_PATH {}, META_FLUSH_INTERVAL_MS 1, READ_WRITE);",
        sql_literal(&format!("ducklake:moraine:{catalog_uri}")),
        sql_literal(&data_path.display().to_string())
    );
    let _ = writeln!(
        script,
        "INSERT INTO lake.main.{} VALUES (1000000000, 0);",
        table_name(0)
    );
    let _ = writeln!(
        script,
        "SELECT '{WRITER_MARKER}', count(*) FROM moraine_compact_store('lake', timeout => 600, \
         require_completed => true);"
    );
    Ok(script)
}

fn median_of(samples: &[Sample], pick: impl Fn(&Sample) -> f64) -> anyhow::Result<f64> {
    let mut values: Vec<f64> = samples.iter().map(pick).collect();
    median(&mut values)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "request counts are small and the report is rounded to one decimal place"
)]
fn count(value: u64) -> f64 {
    value as f64
}

/// One size's row of medians, and its request-error count when non-zero.
fn print_row(files: usize, samples: &[Sample], compaction: bool) -> anyhow::Result<()> {
    print!(
        "{files:>7}  {:>9.1}  {:>7.1}  {:>9.1}  {:>9.1}  {:>9.1}  {:>9.1}  {:>11.1}  \
         {:>9.1}  {:>11.1}",
        median_of(samples, |sample| sample.attach_ms)?,
        median_of(samples, |sample| sample.open_ms)?,
        median_of(samples, |sample| sample.files_ms)?,
        median_of(samples, |sample| sample.plan_ms)?,
        median_of(samples, |sample| sample.warm_ms)?,
        median_of(samples, |sample| count(sample.cold_requests))?,
        median_of(samples, |sample| sample.cold_request_ms)?,
        median_of(samples, |sample| count(sample.warm_requests))?,
        median_of(samples, |sample| sample.warm_request_ms)?,
    );
    if compaction {
        let compacted = |pick: fn(&Compacted) -> f64| {
            median_of(samples, |sample| {
                sample.compacted.as_ref().map_or(f64::NAN, pick)
            })
        };
        print!(
            "  {:>12.1}  {:>18.1}  {:>10.1}  {:>16.1}",
            compacted(|compacted| compacted.plan_ms)?,
            compacted(|compacted| count(compacted.requests))?,
            compacted(|compacted| compacted.settled_ms)?,
            compacted(|compacted| count(compacted.settled_requests))?,
        );
    }
    println!();
    let errors: u64 = samples.iter().map(|sample| sample.errors).sum();
    if errors > 0 {
        println!(
            "# {files} files: {errors} object-store request errors across all processes, \
             missing-object probes included"
        );
    }
    Ok(())
}

/// Builds the extension and measures cold read-only attaches over one
/// catalog per requested size.
pub fn run(arguments: &[String]) -> anyhow::Result<()> {
    let options = parse_options(arguments)?;
    let local_root = env::temp_dir().join(format!("moraine-reader-bench-{}", std::process::id()));
    let target = CatalogTarget::from_environment(local_root.clone());
    if !target.is_remote() {
        fs::create_dir_all(&local_root)
            .with_context(|| format!("creating {}", local_root.display()))?;
    }
    let moraine = duckdb::build_and_package_extension(&ducklake_patch::prepare()?)?;
    let cli = duckdb::ensure_duckdb_cli()?;
    let artifacts = Artifacts {
        cli: &cli,
        moraine: &moraine,
    };

    println!("\n# Cold read-only attach against {}", target.description());
    println!(
        "# local Parquet; {} tables; {}% of files carrying a delete file; {} cold processes per \
         size; cache dir: {}; request durations are summed and may overlap\n",
        options.tables,
        options.deletes,
        options.repeat,
        options
            .cache_dir
            .as_ref()
            .map_or_else(|| "none".to_owned(), |dir| dir.display().to_string())
    );
    print!(
        "{:>7}  {:>9}  {:>7}  {:>9}  {:>9}  {:>9}  {:>9}  {:>11}  {:>9}  {:>11}",
        "files",
        "attach_ms",
        "open_ms",
        "files_ms",
        "plan_ms",
        "warm_ms",
        "cold_requests",
        "cold_request_ms",
        "warm_requests",
        "warm_request_ms"
    );
    if options.compaction {
        print!(
            "  {:>12}  {:>18}  {:>10}  {:>16}",
            "compacted_ms", "compacted_requests", "settled_ms", "settled_requests"
        );
    }
    println!();

    for files in options.files {
        let temp = TempDir::new(env::temp_dir().join(format!(
            "moraine-reader-bench-{}-{files}",
            std::process::id()
        )))?;
        let data_path = temp.data();
        let catalog_uri = target.catalog_uri(&format!("reader-{files}"))?;
        seed(
            &artifacts,
            &target,
            &data_path,
            &catalog_uri,
            files,
            options.tables,
            options.deletes,
        )?;

        let writer = options
            .compaction
            .then(|| {
                let path = temp.data().with_file_name("writer.sql");
                write_writer_scripts(&artifacts, &target, &data_path, &catalog_uri, &path)?;
                Ok::<_, anyhow::Error>(path)
            })
            .transpose()?;

        let mut samples = Vec::with_capacity(options.repeat);
        for _ in 0..options.repeat {
            samples.push(measure(
                &artifacts,
                &target,
                &data_path,
                &catalog_uri,
                options.cache_dir.as_deref(),
                writer.as_deref(),
            )?);
        }
        print_row(files, &samples, options.compaction)?;
    }
    println!();
    if !target.is_remote() {
        let _ = fs::remove_dir_all(&local_root);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_accept_a_file_sweep_and_a_cache_dir() {
        let arguments = [
            "--files",
            "2000,20000",
            "--tables",
            "4",
            "--deletes",
            "50",
            "--repeat",
            "3",
            "--cache-dir",
            "/tmp/c",
        ]
        .map(str::to_owned)
        .to_vec();
        let options = parse_options(&arguments).unwrap();
        assert_eq!(options.files, [2_000, 20_000]);
        assert_eq!(options.tables, 4);
        assert_eq!(options.deletes, 50);
        assert_eq!(options.repeat, 3);
        assert_eq!(options.cache_dir.as_deref(), Some(Path::new("/tmp/c")));
        assert!(!options.compaction);
        assert!(
            parse_options(&[
                "--compaction".to_owned(),
                "--repeat".to_owned(),
                "2".to_owned()
            ])
            .unwrap()
            .compaction
        );
        assert!(parse_options(&["--repeat".to_owned(), "0".to_owned()]).is_err());
        assert!(parse_options(&["--files".to_owned(), "0".to_owned()]).is_err());
        assert!(parse_options(&["--tables".to_owned(), "0".to_owned()]).is_err());
        assert!(parse_options(&["--deletes".to_owned(), "101".to_owned()]).is_err());
    }

    #[test]
    fn a_seed_plan_registers_exactly_the_requested_files_per_table() {
        assert_eq!(
            seed_plan(200),
            SeedPlan {
                partitions: 200,
                commits: 1
            }
        );
        assert_eq!(
            seed_plan(20_000),
            SeedPlan {
                partitions: 1_000,
                commits: 20
            }
        );
        assert_eq!(
            seed_plan(2_500).partitions * seed_plan(2_500).commits,
            3_000
        );
    }

    /// The seeded delete takes one row from each file of the chosen
    /// partitions — never a whole file, which would drop it instead.
    #[test]
    fn the_delete_takes_one_row_from_each_chosen_file() {
        let statement = delete_statement("items0", 1_000, 25);
        assert!(statement.contains("(id // 1000) % 10 = 0"), "{statement}");
        assert!(statement.contains("k % 100 < 25"), "{statement}");
    }

    #[test]
    fn parses_four_timings_and_the_tagged_row() {
        // Seven fields: the row lost its materialization column with the
        // materialization itself.
        let output = "Run Time (s): real 0.250 user 0 sys 0\n\
                      Run Time (s): real 0.150 user 0 sys 0\n\
                      Run Time (s): real 0.040 user 0 sys 0\n\
                      Run Time (s): real 0.002 user 0 sys 0\n\
                      __MORAINE_READER_BENCH__,4,86.5,0,0.0,0,90\n";
        assert_eq!(
            parse_sample(output, false).unwrap(),
            Sample {
                attach_ms: 250.0,
                open_ms: 90.0,
                files_ms: 150.0,
                plan_ms: 40.0,
                warm_ms: 2.0,
                cold_requests: 4,
                cold_request_ms: 86.5,
                warm_requests: 0,
                warm_request_ms: 0.0,
                errors: 0,
                compacted: None,
            }
        );
        assert!(parse_sample(output, true).is_err());
    }

    /// Under `--compaction` two more statements are timed and the row
    /// carries the two plans' requests after the warm ones.
    #[test]
    fn parses_the_compaction_timings_and_fields() {
        let output = "Run Time (s): real 0.250 user 0 sys 0\n\
                      Run Time (s): real 0.150 user 0 sys 0\n\
                      Run Time (s): real 0.040 user 0 sys 0\n\
                      Run Time (s): real 0.002 user 0 sys 0\n\
                      Run Time (s): real 0.300 user 0 sys 0\n\
                      Run Time (s): real 0.003 user 0 sys 0\n\
                      __MORAINE_READER_BENCH__,4,86.5,0,0.0,0,90,12,210.5,0,0.0\n";
        let sample = parse_sample(output, true).unwrap();
        assert_eq!(
            sample.compacted,
            Some(Compacted {
                plan_ms: 300.0,
                requests: 12,
                request_ms: 210.5,
                settled_ms: 3.0,
                settled_requests: 0,
                settled_request_ms: 0.0,
            })
        );
    }
}
