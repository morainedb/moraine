//! What file-located index lookups cost and save.
//!
//! Reports the four numbers the design rests on: the bytes the summary cache
//! holds, the time to build a sparse file's summary, the time a located
//! lookup takes cold and warm, and the files a DuckLake scan reads with and
//! without the located join.

use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};

use anyhow::{Context, bail, ensure};

use crate::{duckdb, ducklake_patch};

/// Files the lake is built with. Enough that a scan reading one instead of
/// all of them is unambiguous, few enough to stay a minute-scale run.
const DEFAULT_FILES: usize = 16;

/// Rows per file.
const DEFAULT_ROWS_PER_FILE: usize = 2_000;

/// Timed repetitions of each lookup.
const REPETITIONS: usize = 5;

struct Options {
    files: usize,
    rows_per_file: usize,
}

fn parse_options(arguments: &[String]) -> anyhow::Result<Options> {
    let mut options = Options {
        files: DEFAULT_FILES,
        rows_per_file: DEFAULT_ROWS_PER_FILE,
    };
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--files" => {
                index += 1;
                options.files = arguments
                    .get(index)
                    .context("`--files` requires a count")?
                    .parse()
                    .context("`--files` must be a number")?;
            }
            "--rows-per-file" => {
                index += 1;
                options.rows_per_file = arguments
                    .get(index)
                    .context("`--rows-per-file` requires a count")?
                    .parse()
                    .context("`--rows-per-file` must be a number")?;
            }
            unknown => bail!(
                "unknown argument `{unknown}`; usage: locate-bench [--files N] \
                 [--rows-per-file N]"
            ),
        }
        index += 1;
    }
    ensure!(options.files > 0, "`--files` must be greater than zero");
    ensure!(
        options.rows_per_file > 0,
        "`--rows-per-file` must be greater than zero"
    );
    Ok(options)
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// A working directory that removes itself, so a run leaves no lake behind.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> anyhow::Result<Self> {
        let root = duckdb::workspace_root().join("target/locate-bench");
        fs::create_dir_all(&root).with_context(|| format!("creating {}", root.display()))?;
        let path = root.join(name);
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(path.join("store"))
            .with_context(|| format!("creating {}", path.display()))?;
        fs::create_dir_all(path.join("data"))
            .with_context(|| format!("creating {}", path.display()))?;
        Ok(Self(path))
    }

    fn store(&self) -> PathBuf {
        self.0.join("store")
    }

    fn data(&self) -> PathBuf {
        self.0.join("data")
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The preamble every session needs: both extensions, then the lake.
fn preamble(moraine: &Path, ducklake: &Path, temp: &TempDir) -> String {
    let mut script = String::new();
    let _ = writeln!(script, "SET threads=1;");
    let _ = writeln!(
        script,
        "SET extension_directory={};",
        sql_literal(&duckdb::extension_install_directory().display().to_string())
    );
    let _ = writeln!(
        script,
        "LOAD {};",
        sql_literal(&ducklake.display().to_string())
    );
    let _ = writeln!(
        script,
        "LOAD {};",
        sql_literal(&moraine.display().to_string())
    );
    let _ = writeln!(
        script,
        "ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH {}, META_DATA_PATH {}, \
         DATA_INLINING_ROW_LIMIT 0);",
        temp.store().display(),
        sql_literal(&temp.data().display().to_string()),
        sql_literal(&temp.data().display().to_string()),
    );
    script
}

/// Runs `script` through the CLI and returns its stdout.
fn run_sql(cli: &Path, script: &str) -> anyhow::Result<String> {
    let output = Command::new(cli)
        .args(["-unsigned", "-noheader", "-list", "-c", script])
        .output()
        .with_context(|| format!("spawning {}", cli.display()))?;
    ensure!(
        output.status.success(),
        "duckdb failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// The `Total Files Read` an `EXPLAIN ANALYZE` reported, if it named one. A
/// plan that opened no Parquet at all reports nothing.
///
/// The count follows the label rather than ending the line: the profile
/// renders side-by-side boxes, so one line can carry another operator's
/// timing after it.
const FILES_READ_LABEL: &str = "Total Files Read:";

fn total_files_read(plan: &str) -> Option<u64> {
    plan.lines().find_map(|line| {
        let tail = line.split_once(FILES_READ_LABEL)?.1;
        let digits: String = tail
            .trim_start()
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        digits.parse().ok()
    })
}

/// The last whole number on the line carrying `marker`. The CLI's list mode
/// separates with `|`; its CSV mode with `,`.
fn marked_number(stdout: &str, marker: &str) -> anyhow::Result<u64> {
    let line = stdout
        .lines()
        .find(|line| line.contains(marker))
        .with_context(|| format!("no `{marker}` line in the benchmark output"))?;
    line.rsplit(['|', ','])
        .next()
        .and_then(|tail| tail.trim().parse().ok())
        .with_context(|| format!("`{marker}` line carried no number: {line}"))
}

/// Milliseconds, to one decimal.
fn milliseconds(elapsed: std::time::Duration) -> f64 {
    (elapsed.as_secs_f64() * 10_000.0).round() / 10.0
}

/// Builds the lake once: `files` data files, each carrying `rows_per_file`
/// rows, with a unique index over the key column.
fn build_lake(cli: &Path, preamble: &str, options: &Options) -> anyhow::Result<()> {
    let mut script = preamble.to_owned();
    let _ = writeln!(script, "CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    for file in 0..options.files {
        let start = file * options.rows_per_file;
        let _ = writeln!(
            script,
            "INSERT INTO lake.main.t SELECT i, 'v' FROM range({start}, {}) t(i);",
            start + options.rows_per_file
        );
    }
    let _ = writeln!(
        script,
        "CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);"
    );
    run_sql(cli, &script)?;
    Ok(())
}

/// One timed lookup in a fresh session, so nothing is resident.
fn cold_lookup(cli: &Path, preamble: &str, key: usize) -> anyhow::Result<(f64, u64)> {
    let mut script = preamble.to_owned();
    let _ = writeln!(
        script,
        "SELECT 'cache', (SELECT auxiliary_metadata_occupancy_bytes FROM moraine_cache_status());"
    );
    let _ = writeln!(
        script,
        "SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', {key});"
    );
    let _ = writeln!(
        script,
        "SELECT 'cache_after', (SELECT auxiliary_metadata_occupancy_bytes FROM \
         moraine_cache_status());"
    );
    let started = Instant::now();
    let stdout = run_sql(cli, &script)?;
    let elapsed = milliseconds(started.elapsed());
    let cache_after = marked_number(&stdout, "cache_after")?;
    Ok((elapsed, cache_after))
}

/// Repeated lookups in one session: the first builds every summary, the rest
/// find them resident.
fn warm_lookups(cli: &Path, preamble: &str, options: &Options) -> anyhow::Result<(f64, f64)> {
    let mut script = preamble.to_owned();
    let key = options.rows_per_file / 2;
    let _ = writeln!(
        script,
        "SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', {key});"
    );
    let first = Instant::now();
    run_sql(cli, &script)?;
    let first_elapsed = milliseconds(first.elapsed());

    // The same session, now with every summary resident.
    let mut warm = preamble.to_owned();
    for repetition in 0..REPETITIONS {
        let key = repetition * options.rows_per_file + 1;
        let _ = writeln!(
            warm,
            "SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', {key});"
        );
    }
    let started = Instant::now();
    run_sql(cli, &warm)?;
    let repetitions = u32::try_from(REPETITIONS).unwrap_or(1);
    let per_lookup =
        (milliseconds(started.elapsed()) - first_elapsed).max(0.0) / f64::from(repetitions);
    Ok((first_elapsed, per_lookup))
}

/// Files a scan reads when the located id is already a constant.
///
/// This is the ceiling the join is aiming at: the same predicate, known
/// before DuckLake builds its file list rather than during the join.
fn files_read_static(cli: &Path, preamble: &str, key: usize) -> anyhow::Result<Option<u64>> {
    let located = run_sql(
        cli,
        &format!(
            "{preamble}SELECT row_id, data_file_id FROM \
             moraine_index_lookup('lake', 'main', 't', 'by_a', {key});\n"
        ),
    )?;
    let Some((row_id, data_file_id)) = located
        .lines()
        .find_map(|line| line.split_once('|'))
        .and_then(|(row, file)| Some((row.trim().parse::<u64>().ok()?, file.trim().to_owned())))
    else {
        return Ok(None);
    };
    if data_file_id.is_empty() {
        return Ok(None);
    }
    let plan = run_sql(
        cli,
        &format!(
            "{preamble}EXPLAIN ANALYZE SELECT b FROM lake.main.t \
             WHERE rowid = {row_id} AND data_file_id = {data_file_id};\n"
        ),
    )?;
    Ok(total_files_read(&plan))
}

/// Makes the files' row-id ranges overlap, the way an UPDATE does.
///
/// Updating one row per file writes a single file holding preserved ids
/// drawn from every source file, so its row-id range spans the table and
/// min/max statistics can no longer exclude it. That is the shape where
/// locating has something left to prune.
fn introduce_overlap(cli: &Path, preamble: &str, options: &Options) -> anyhow::Result<()> {
    let mut script = preamble.to_owned();
    let _ = writeln!(
        script,
        "UPDATE lake.main.t SET b = 'updated' WHERE a % {} = 1;",
        options.rows_per_file
    );
    run_sql(cli, &script)?;
    Ok(())
}

/// Files a scan reads when it joins on the row id alone, and when it also
/// takes the located file id.
fn files_read(
    cli: &Path,
    preamble: &str,
    key: usize,
) -> anyhow::Result<(Option<u64>, Option<u64>)> {
    let row_id_only = format!(
        "{preamble}EXPLAIN ANALYZE SELECT data.b FROM lake.main.t data \
         JOIN moraine_index_lookup('lake', 'main', 't', 'by_a', {key}) hits \
           ON data.rowid = hits.row_id;\n"
    );
    let located = format!(
        "{preamble}EXPLAIN ANALYZE SELECT data.b FROM lake.main.t data \
         JOIN moraine_index_lookup('lake', 'main', 't', 'by_a', {key}) hits \
           ON data.rowid = hits.row_id \
          AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id;\n"
    );
    Ok((
        total_files_read(&run_sql(cli, &row_id_only)?),
        total_files_read(&run_sql(cli, &located)?),
    ))
}

/// Builds a lake, then reports what locating its rows costs and saves.
pub fn run(arguments: &[String]) -> anyhow::Result<()> {
    let options = parse_options(arguments)?;
    let cli = duckdb::ensure_duckdb_cli()?;
    let moraine = duckdb::build_and_package_extension()?;
    let ducklake = ducklake_patch::build_artifact(&[])?;

    let temp = TempDir::new("lake")?;
    let preamble = preamble(&moraine, &ducklake, &temp);

    println!(
        "building {} files of {} rows",
        options.files, options.rows_per_file
    );
    build_lake(&cli, &preamble, &options)?;

    let key = options.rows_per_file / 2;
    let (cold_ms, cache_bytes) = cold_lookup(&cli, &preamble, key)?;
    let (first_ms, warm_ms) = warm_lookups(&cli, &preamble, &options)?;
    let (row_id_files, located_files) = files_read(&cli, &preamble, key)?;

    // Then the same measurement once the row-id ranges overlap.
    introduce_overlap(&cli, &preamble, &options)?;
    let (overlap_row_id_files, overlap_located_files) = files_read(&cli, &preamble, key)?;
    let overlap_static_files = files_read_static(&cli, &preamble, key)?;

    let report = |name: &str, value: String| println!("{name:<32}{value}");
    println!();
    report("files", options.files.to_string());
    report("rows per file", options.rows_per_file.to_string());
    report("summary cache bytes", cache_bytes.to_string());
    report("cold lookup ms", format!("{cold_ms:.1}"));
    report("first lookup ms (builds)", format!("{first_ms:.1}"));
    report("warm lookup ms", format!("{warm_ms:.1}"));
    report(
        "files read, row id only",
        row_id_files.map_or_else(|| "none reported".to_owned(), |files| files.to_string()),
    );
    report(
        "files read, located",
        located_files.map_or_else(|| "none reported".to_owned(), |files| files.to_string()),
    );
    println!("\nafter an update makes the row-id ranges overlap:");
    report(
        "files read, row id only",
        overlap_row_id_files.map_or_else(|| "none reported".to_owned(), |files| files.to_string()),
    );
    report(
        "files read, located",
        overlap_located_files.map_or_else(|| "none reported".to_owned(), |files| files.to_string()),
    );
    report(
        "files read, id as a constant",
        overlap_static_files.map_or_else(|| "none reported".to_owned(), |files| files.to_string()),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_plan_line_yields_the_file_count() {
        let plan = "│    Total Files Read: 3    │";
        assert_eq!(total_files_read(plan), Some(3));
    }

    #[test]
    fn a_neighbouring_box_on_the_same_line_does_not_hide_the_count() {
        let plan = "│    Total Files Read: 1    ││           0.00s           │";
        assert_eq!(total_files_read(plan), Some(1));
    }

    #[test]
    fn a_plan_that_read_nothing_reports_no_count() {
        assert_eq!(total_files_read("│  Query Profiling Graph  │"), None);
    }

    #[test]
    fn a_marked_line_yields_its_trailing_number() {
        let comma = "cache,0\ncache_after,4096\n";
        assert_eq!(marked_number(comma, "cache_after").unwrap(), 4096);
        let pipe = "cache|0\ncache_after|15208\n";
        assert_eq!(marked_number(pipe, "cache_after").unwrap(), 15_208);
    }

    #[test]
    fn defaults_survive_no_arguments() {
        let options = parse_options(&[]).unwrap();
        assert_eq!(options.files, DEFAULT_FILES);
        assert_eq!(options.rows_per_file, DEFAULT_ROWS_PER_FILE);
    }

    #[test]
    fn a_zero_scale_is_refused() {
        let arguments = vec!["--files".to_owned(), "0".to_owned()];
        assert!(parse_options(&arguments).is_err());
    }
}
