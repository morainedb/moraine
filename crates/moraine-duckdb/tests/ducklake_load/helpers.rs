//! Shared fixtures and CLI-session runners for the `ducklake_load`
//! suite.

use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use moraine::{Catalog, CatalogOptions, ColumnDef, DataFile};
use object_store::local::LocalFileSystem;

pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "moraine-ducklake-load-{tag}-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("test setup: create temp dir");
        Self(dir)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn cli_path() -> PathBuf {
    PathBuf::from(
        env::var("MORAINE_DUCKDB_CLI")
            .expect("MORAINE_DUCKDB_CLI must be set (see this module's doc comment)"),
    )
}

pub fn ext_path() -> PathBuf {
    PathBuf::from(
        env::var("MORAINE_DUCKDB_EXT")
            .expect("MORAINE_DUCKDB_EXT must be set (see this module's doc comment)"),
    )
}

pub fn ducklake_ext_path() -> PathBuf {
    PathBuf::from(
        env::var("MORAINE_DUCKLAKE_EXT")
            .expect("MORAINE_DUCKLAKE_EXT must be set (see this module's doc comment)"),
    )
}

pub fn ducklake_load_statement(path: &Path) -> String {
    format!("LOAD '{}';", path.display().to_string().replace('\'', "''"))
}

pub const ROW_COUNT: u64 = 5;

/// Seeds a store via the `moraine` API: `main` from bootstrap (no
/// explicit `create_schema` call), one table `t` with a relative-path
/// data file, then a rename to give the table row history depth (two
/// `ducklake_table` versions). `file_size_bytes`/`footer_size` must be
/// the real Parquet file's stats: DuckLake's own reader uses the
/// registered `footer_size` to seek to the file's metadata footer, so a
/// placeholder `0` throws `Invalid Input Error: Invalid footer length`.
pub fn seed(dir: &Path, file_size_bytes: u64, footer_size: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test setup: build tokio runtime");

    rt.block_on(async {
        let store =
            Arc::new(LocalFileSystem::new_with_prefix(dir).expect("test setup: open local store"));
        let catalog = Catalog::open(store, CatalogOptions::default())
            .await
            .expect("test setup: open catalog");

        catalog
            .commit(|tx| {
                let main = tx.schema_by_name("main").expect("bootstrap mints main").id;

                let t = tx.create_table(
                    main,
                    "t_old",
                    &[
                        ColumnDef {
                            name: "id".into(),
                            column_type: "BIGINT".into(),
                            nulls_allowed: false,
                            default_value: None,
                            children: Vec::new(),
                        },
                        ColumnDef {
                            name: "amount".into(),
                            column_type: "DOUBLE".into(),
                            nulls_allowed: true,
                            default_value: None,
                            children: Vec::new(),
                        },
                    ],
                )?;
                tx.register_data_file(
                    t,
                    DataFile {
                        path: "data.parquet".into(),
                        path_is_relative: true,
                        file_format: "parquet".into(),
                        record_count: ROW_COUNT,
                        file_size_bytes,
                        footer_size,
                        encryption_key: None,
                        partition_values: vec![],
                        column_stats: vec![],
                    },
                    &[],
                )?;
                // History depth: `t_old`'s current row ends, a history row is
                // written, and a new current row for `t` begins.
                tx.rename_table(t, "t")?;

                Ok(())
            })
            .await
            .expect("test setup: commit fixtures");

        catalog.close().await.expect("test setup: close catalog");
    });
}

/// Writes `<data_path>/main/t_old/data.parquet`. Two path facts drive
/// this:
///
/// - DuckLake resolves a relative data-file path against `<DATA_PATH from
///   ATTACH>/<schema.path>/<table.path>/`, never against the metadata store's
///   own directory.
/// - `table.path` is fixed at `CREATE TABLE` time (here, `t_old/`) and is
///   untouched by a later rename.
///
/// Returns `(file_size_bytes, footer_size)` for the written file, per
/// the Parquet spec's fixed trailer: the last 4 bytes are the magic
/// `PAR1`, and the 4 bytes before that are the footer's thrift-encoded
/// length as a little-endian `u32` — the same `footer_size` DuckLake
/// registers when it authors a data file.
pub fn write_parquet(data_path: &Path) -> (u64, u64) {
    let table_dir = data_path.join("main").join("t_old");
    std::fs::create_dir_all(&table_dir).expect("test setup: create table dir");
    let file = table_dir.join("data.parquet");

    let status = Command::new(cli_path())
        .arg("-c")
        .arg(format!(
            "COPY (SELECT i::BIGINT AS id, (i * 1.5)::DOUBLE AS amount FROM range({ROW_COUNT}) t(i)) \
             TO '{}' (FORMAT PARQUET);",
            file.display()
        ))
        .status()
        .expect("failed to spawn duckdb CLI to write the fixture Parquet file");
    assert!(status.success(), "writing fixture Parquet file failed");

    let bytes = std::fs::read(&file).expect("test setup: read back fixture Parquet file");
    let file_size_bytes = u64::try_from(bytes.len()).expect("test setup: file size fits u64");
    assert!(
        bytes.ends_with(b"PAR1"),
        "fixture Parquet file missing trailing PAR1 magic"
    );
    let footer_len_offset = bytes.len() - 8;
    let footer_size = u64::from(u32::from_le_bytes(
        bytes[footer_len_offset..footer_len_offset + 4]
            .try_into()
            .expect("test setup: 4-byte footer length slice"),
    ));
    (file_size_bytes, footer_size)
}

/// One CLI session's catalog chain and attach mode.
pub enum Attach<'a> {
    /// `ducklake:moraine:<store>` with `DATA_PATH`, optional extra attach
    /// options (e.g. `", ENCRYPTED"`), and optional `READ_ONLY`.
    Moraine {
        store_dir: &'a Path,
        data_path: &'a Path,
        options: &'a str,
        read_only: bool,
    },
    /// `ducklake:moraine:<store>` with no data-path option at all —
    /// DuckLake must read the data root from the metadata moraine serves.
    MoraineBare { store_dir: &'a Path },
    /// A stock DuckLake catalog (`<meta_dir>/meta.ducklake`), no moraine
    /// in the chain — the reference oracle: an identical statement stream
    /// mints identical snapshot ids and rowids on both catalogs, so
    /// outputs are comparable row-for-row.
    Reference {
        meta_dir: &'a Path,
        data_path: &'a Path,
    },
    /// The metadata-only `moraine:<store>` attach: reads the store
    /// through this crate's own metadata-table scan, not DuckLake's
    /// reader — the independent verification surface for staged writes.
    Standalone {
        store_dir: &'a Path,
        read_only: bool,
    },
}

/// Spawns one `-csv` CLI session — extension setup per the attach shape,
/// the attach, then `sql` — returning the raw process output.
///
/// Pinned single-threaded: DuckLake's catalog re-read after a rename is
/// racy under multiple threads — a fresh attach sometimes returns an
/// empty table list. The race reproduces with no moraine in the chain,
/// so it is upstream; one thread closes it so these tests exercise
/// moraine's translation, not DuckLake's cache concurrency.
pub fn run_session(attach: &Attach, sql: &str) -> std::process::Output {
    let mut command = Command::new(cli_path());
    let loads_moraine = !matches!(attach, Attach::Reference { .. });
    command.arg("-unsigned").arg("-csv");
    // The standalone attach needs no ducklake extension.
    if !matches!(attach, Attach::Standalone { .. }) {
        command
            .arg("-c")
            .arg("SET threads=1;")
            .arg("-c")
            .arg(ducklake_load_statement(&ducklake_ext_path()));
    }
    if loads_moraine {
        command
            .arg("-c")
            .arg(format!("LOAD '{}';", ext_path().display()));
    }
    let attach_sql = match attach {
        Attach::Moraine {
            store_dir,
            data_path,
            options,
            read_only,
        } => format!(
            "ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}'{options}{});",
            store_dir.display(),
            data_path.display(),
            if *read_only { ", READ_ONLY" } else { "" },
        ),
        Attach::MoraineBare { store_dir } => {
            format!("ATTACH 'ducklake:moraine:{}' AS lake;", store_dir.display())
        }
        Attach::Reference {
            meta_dir,
            data_path,
        } => format!(
            "ATTACH 'ducklake:{}' AS lake (DATA_PATH '{}');",
            meta_dir.join("meta.ducklake").display(),
            data_path.display()
        ),
        Attach::Standalone {
            store_dir,
            read_only,
        } => format!(
            "ATTACH 'moraine:{}' AS m{};",
            store_dir.display(),
            if *read_only { " (READ_ONLY)" } else { "" },
        ),
    };
    command
        .arg("-c")
        .arg(attach_sql)
        .arg("-c")
        .arg(sql)
        .output()
        .expect("failed to spawn duckdb CLI")
}

/// Runs `sql` in an open session, waits `settle`, then kills the process
/// outright — no commit, no detach, no close.
///
/// This is the one crash a moraine-side test cannot stage: the interesting
/// instant is between DuckLake writing a Parquet file and committing the
/// metadata that references it, and only a session running both halves
/// reaches it. `sql` is expected to leave a transaction open; `settle`
/// gives the write time to land before the kill.
pub fn kill_ducklake_session_after(
    store_dir: &Path,
    data_path: &Path,
    sql: &str,
    settle: std::time::Duration,
) {
    use std::io::Write;

    let mut child = Command::new(cli_path())
        .arg("-unsigned")
        .arg("-csv")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn duckdb CLI");

    let preamble = format!(
        "SET threads=1;\n\
         {}\n\
         LOAD '{}';\n\
         ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}');\n",
        ducklake_load_statement(&ducklake_ext_path()),
        ext_path().display(),
        store_dir.display(),
        data_path.display(),
    );

    // Held, not dropped: closing stdin would give the CLI an EOF and let
    // it exit cleanly, which is the opposite of the crash under test.
    let mut stdin = child.stdin.take().expect("piped stdin");
    stdin
        .write_all(preamble.as_bytes())
        .expect("write the preamble");
    stdin
        .write_all(sql.as_bytes())
        .expect("write the statements");
    stdin.flush().expect("flush the statements");

    std::thread::sleep(settle);

    child.kill().expect("kill duckdb CLI");
    child.wait().expect("reap duckdb CLI");
}

/// Runs `before`, waits `pause`, then runs `after` — all in **one**
/// session, by feeding the CLI through stdin rather than `-c`.
///
/// The scheduler's retained passes live in memory per attach, so a
/// second session would observe a fresh, empty one. Watching a timer do
/// anything therefore requires holding a session open across real
/// wall-clock time, which is what this does.
pub fn run_ducklake_sql_with_pause(
    store_dir: &Path,
    data_path: &Path,
    attach_options: &str,
    before: &str,
    pause: std::time::Duration,
    after: &str,
) -> String {
    use std::io::Write;

    // Everything goes through stdin: the CLI runs its `-c` arguments and
    // exits without ever reading stdin, so a session that must outlive a
    // pause cannot use them.
    let mut child = Command::new(cli_path())
        .arg("-unsigned")
        .arg("-csv")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn duckdb CLI");

    let preamble = format!(
        "SET threads=1;\n\
         {}\n\
         LOAD '{}';\n\
         ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}'{attach_options});\n",
        ducklake_load_statement(&ducklake_ext_path()),
        ext_path().display(),
        store_dir.display(),
        data_path.display(),
    );

    {
        // Taking stdin means dropping it here closes the pipe, so the CLI
        // sees EOF and exits rather than waiting for more input.
        let mut stdin = child.stdin.take().expect("piped stdin");
        stdin
            .write_all(preamble.as_bytes())
            .expect("write the preamble");
        stdin
            .write_all(before.as_bytes())
            .expect("write the first statements");
        stdin.flush().expect("flush the first statements");
        // The session stays open while the scheduler's timer runs.
        std::thread::sleep(pause);
        stdin
            .write_all(after.as_bytes())
            .expect("write the second statements");
    }

    // These sessions exercise paths that can legitimately block — a
    // detach waits out a pass already running — so a bug there would
    // otherwise wedge the whole suite. Bound the wait and fail loudly.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        match child.try_wait().expect("poll duckdb CLI") {
            Some(_) => break,
            None if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                let output = child.wait_with_output().expect("reap duckdb CLI");
                panic!(
                    "duckdb CLI did not exit within the deadline — the session is stuck.\n\
                     stdout: {}\nstderr: {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            None => std::thread::sleep(std::time::Duration::from_millis(25)),
        }
    }
    let output = child.wait_with_output().expect("await duckdb CLI");
    assert_session_ok(output, "paused ducklake session", after)
}

/// Runs `before` in a held-open session, waits `settle`, runs `during` —
/// a *second* process against the same store — then runs `after` in the
/// first session and returns its raw output.
///
/// The one shape that answers a question about what a second attach does to
/// a live writer. Two `-c` sessions cannot: the first has already exited
/// and dropped its epoch by the time the second opens. `settle` gives
/// `before` time to execute, since the CLI reads stdin on its own schedule.
///
/// Success is not asserted — `after` failing is a legitimate answer, and
/// the assertion is the caller's.
pub fn run_ducklake_sql_around(
    store_dir: &Path,
    data_path: &Path,
    before: &str,
    settle: std::time::Duration,
    during: impl FnOnce(),
    after: &str,
) -> std::process::Output {
    use std::io::Write;

    let mut child = Command::new(cli_path())
        .arg("-unsigned")
        .arg("-csv")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn duckdb CLI");

    let preamble = format!(
        "SET threads=1;\n\
         {}\n\
         LOAD '{}';\n\
         ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}');\n",
        ducklake_load_statement(&ducklake_ext_path()),
        ext_path().display(),
        store_dir.display(),
        data_path.display(),
    );

    {
        // Dropping stdin at the end of this block closes the pipe, so the
        // CLI sees EOF and exits rather than waiting for more input.
        let mut stdin = child.stdin.take().expect("piped stdin");
        stdin
            .write_all(preamble.as_bytes())
            .expect("write the preamble");
        stdin
            .write_all(before.as_bytes())
            .expect("write the first statements");
        stdin.flush().expect("flush the first statements");
        std::thread::sleep(settle);

        during();

        stdin
            .write_all(after.as_bytes())
            .expect("write the second statements");
    }

    child.wait_with_output().expect("await duckdb CLI")
}

/// Asserts the session succeeded and returns its stdout.
pub fn assert_session_ok(output: std::process::Output, context: &str, sql: &str) -> String {
    assert!(
        output.status.success(),
        "{context} failed for `{sql}`:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("duckdb CLI stdout is not UTF-8")
}

/// Combined stdout+stderr, for error-text assertions.
pub fn combined_output(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Runs `sql` through the nested `ducklake:moraine:` chain, asserting
/// success and returning stdout.
pub fn run_ducklake_sql(store_dir: &Path, data_path: &Path, sql: &str) -> String {
    run_ducklake_sql_with_options(store_dir, data_path, "", sql)
}

/// Runs `sql` against a fresh CLI + attach, returning the raw process
/// output without asserting success — for statements expected to fail.
pub fn run_ducklake_sql_output(
    store_dir: &Path,
    data_path: &Path,
    attach_options: &str,
    sql: &str,
) -> std::process::Output {
    run_session(
        &Attach::Moraine {
            store_dir,
            data_path,
            options: attach_options,
            read_only: false,
        },
        sql,
    )
}

/// As [`run_ducklake_sql`], with extra ATTACH options appended after
/// `DATA_PATH` (e.g. `", ENCRYPTED, META_ENCRYPTED true"`).
pub fn run_ducklake_sql_with_options(
    store_dir: &Path,
    data_path: &Path,
    attach_options: &str,
    sql: &str,
) -> String {
    let output = run_ducklake_sql_output(store_dir, data_path, attach_options, sql);
    assert_session_ok(output, "duckdb CLI", sql)
}

/// As [`run_ducklake_sql`], but against a stock DuckLake catalog — see
/// [`Attach::Reference`].
pub fn run_reference_ducklake_sql(meta_dir: &Path, data_path: &Path, sql: &str) -> String {
    let output = run_session(
        &Attach::Reference {
            meta_dir,
            data_path,
        },
        sql,
    );
    assert_session_ok(output, "reference duckdb CLI", sql)
}

/// As [`run_ducklake_sql`], but for a statement that must fail:
/// returns the CLI's combined output for the caller to assert on. The
/// CLI exits nonzero when any statement errors; a success here means
/// the statement unexpectedly worked.
pub fn run_ducklake_sql_expect_err(store_dir: &Path, data_path: &Path, sql: &str) -> String {
    let output = run_session(
        &Attach::Moraine {
            store_dir,
            data_path,
            options: "",
            read_only: false,
        },
        sql,
    );
    assert!(
        !output.status.success(),
        "`{sql}` unexpectedly succeeded:\nstdout: {}",
        String::from_utf8_lossy(&output.stdout),
    );
    combined_output(&output)
}

/// As [`run_ducklake_sql_expect_err`], but with extra attach options —
/// for refusals that happen at attach rather than at the statement, so
/// the failing option can be asserted on.
pub fn run_ducklake_sql_expect_err_with_options(
    store_dir: &Path,
    data_path: &Path,
    attach_options: &str,
    sql: &str,
) -> String {
    let output = run_session(
        &Attach::Moraine {
            store_dir,
            data_path,
            options: attach_options,
            read_only: false,
        },
        sql,
    );
    assert!(
        !output.status.success(),
        "`{sql}` with options `{attach_options}` unexpectedly succeeded:\nstdout: {}",
        String::from_utf8_lossy(&output.stdout),
    );
    combined_output(&output)
}

/// As [`run_reference_ducklake_sql`], but for a statement that must
/// fail — the reference twin of [`run_ducklake_sql_expect_err`], so a
/// refusal can be asserted on both catalogs.
pub fn run_reference_ducklake_sql_expect_err(meta_dir: &Path, data_path: &Path, sql: &str) {
    let output = run_session(
        &Attach::Reference {
            meta_dir,
            data_path,
        },
        sql,
    );
    assert!(
        !output.status.success(),
        "reference: `{sql}` unexpectedly succeeded:\nstdout: {}",
        String::from_utf8_lossy(&output.stdout),
    );
}

pub fn csv_rows(output: &str) -> Vec<Vec<String>> {
    output
        .lines()
        .skip(1)
        .filter(|line| !line.is_empty())
        .map(|line| line.split(',').map(str::to_owned).collect())
        .collect()
}

/// Runs `sql` through the standalone metadata-only attach — see
/// [`Attach::Standalone`].
pub fn run_standalone_sql(store_dir: &Path, sql: &str) -> String {
    let output = run_session(
        &Attach::Standalone {
            store_dir,
            read_only: false,
        },
        sql,
    );
    assert_session_ok(output, "standalone moraine attach", sql)
}

/// Like [`run_standalone_sql`] but attaches `moraine:` **read-only**
/// (`READ_ONLY`), so moraine opens a `DbReader` rather than the writer
/// `Db`.
pub fn run_standalone_read_only_sql(store_dir: &Path, sql: &str) -> String {
    let output = run_session(
        &Attach::Standalone {
            store_dir,
            read_only: true,
        },
        sql,
    );
    assert_session_ok(output, "standalone read-only moraine attach", sql)
}

/// Like [`run_ducklake_sql`] but attaches the DuckLake chain **read-only**
/// (`READ_ONLY` on the outer attach).
pub fn run_ducklake_read_only_sql(store_dir: &Path, data_path: &Path, sql: &str) -> String {
    let output = run_session(
        &Attach::Moraine {
            store_dir,
            data_path,
            options: "",
            read_only: true,
        },
        sql,
    );
    assert_session_ok(output, "read-only ducklake attach", sql)
}

/// As [`run_ducklake_read_only_sql`], but for a statement that must fail
/// on a reader: returns the CLI's combined output for the caller to assert
/// on.
pub fn run_ducklake_read_only_sql_expect_err(
    store_dir: &Path,
    data_path: &Path,
    sql: &str,
) -> String {
    let output = run_session(
        &Attach::Moraine {
            store_dir,
            data_path,
            options: "",
            read_only: true,
        },
        sql,
    );
    assert!(
        !output.status.success(),
        "`{sql}` unexpectedly succeeded on a read-only attach:\nstdout: {}",
        String::from_utf8_lossy(&output.stdout),
    );
    combined_output(&output)
}

/// As [`run_ducklake_sql`], but returns combined stdout+stderr without
/// asserting success — for statements expected to raise a moraine error.
pub fn run_ducklake_sql_capturing(store_dir: &Path, data_path: &Path, sql: &str) -> String {
    let output = run_session(
        &Attach::Moraine {
            store_dir,
            data_path,
            options: "",
            read_only: false,
        },
        sql,
    );
    combined_output(&output)
}

/// Every `.parquet` file under `dir`, recursively.
pub fn parquet_files_under(dir: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).expect("read data dir") {
            let path = entry.expect("read dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "parquet") {
                result.push(path);
            }
        }
    }
    result
}

/// Runs `sql` with **no** data-path option at all — see
/// [`Attach::MoraineBare`]. Returns the raw output.
pub fn run_ducklake_sql_bare(store_dir: &Path, sql: &str) -> std::process::Output {
    run_session(&Attach::MoraineBare { store_dir }, sql)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::ducklake_load_statement;

    #[test]
    fn patched_ducklake_is_loaded_by_escaped_artifact_path() {
        assert_eq!(
            ducklake_load_statement(Path::new("/tmp/patched'ducklake.duckdb_extension")),
            "LOAD '/tmp/patched''ducklake.duckdb_extension';"
        );
    }
}
