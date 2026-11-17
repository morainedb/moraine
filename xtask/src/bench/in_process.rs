//! Concurrent DuckLake transactions inside one DuckDB instance.
//!
//! The CLI holds one connection, so the statement workloads can never make
//! two DuckLake transactions overlap: the second would be serialized behind
//! the first. This drives `xtask/cpp/concurrent_writers.c` instead, which
//! opens one instance, gives each writer a connection of its own and
//! releases them together — the only shape in which DuckLake's commit loop
//! and conflict check actually run.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use anyhow::{Context, bail, ensure};

use super::concurrent::{Bound, Outcome};
use crate::duckdb;

/// The driver's source, relative to the workspace root.
const DRIVER_SOURCE: &str = "xtask/cpp/concurrent_writers.c";

/// Where the compiled driver is cached.
const DRIVER_BINARY: &str = "target/bench/concurrent_writers";

/// Builds the driver if it is missing or out of date, and returns it.
///
/// It links the DuckDB the extension was built against, by path: an
/// extension only loads into the exact version stamped in its footer, so
/// any other `libduckdb` would refuse it.
pub fn driver() -> anyhow::Result<PathBuf> {
    let root = duckdb::workspace_root();
    let source = root.join(DRIVER_SOURCE);
    let binary = root.join(DRIVER_BINARY);
    if !needs_build(&source, &binary) {
        return Ok(binary);
    }

    let library = root.join("build/release/src");
    ensure!(
        library.join("libduckdb.so").exists() || library.join("libduckdb.dylib").exists(),
        "no DuckDB shared library at {}; build the extension first",
        library.display()
    );
    let parent = binary.parent().context("the driver has no directory")?;
    fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;

    let compiler = duckdb::cpp_compilers()?.map_or("cc", |compilers| compilers.c);
    let mut build = Command::new(compiler);
    build
        .args(["-O2", "-Wall", "-Wextra", "-I"])
        .arg(root.join("duckdb/src/include"))
        .arg(&source)
        .arg("-o")
        .arg(&binary)
        .arg("-L")
        .arg(&library)
        .args(["-lduckdb", "-lpthread"])
        .arg(format!("-Wl,-rpath,{}", library.display()));
    duckdb::run(&mut build).context("compiling the concurrent-writer driver")?;
    Ok(binary)
}

/// Whether the driver has to be compiled: no binary yet, or a source that
/// has moved ahead of it.
fn needs_build(source: &Path, binary: &Path) -> bool {
    let modified = |path: &Path| fs::metadata(path).and_then(|data| data.modified()).ok();
    match (modified(source), modified(binary)) {
        (Some(source), Some(binary)) => source > binary,
        _ => true,
    }
}

/// Runs one burst of `writers` connections against `attach` under
/// `bound`, and returns the window the driver timed with the commits that
/// landed in it. `preamble` runs ahead of the attach in the same setup
/// query, which the driver executes as one multi-statement string.
pub fn run(
    driver: &Path,
    preamble: &str,
    attach: &str,
    writers: usize,
    bound: Bound,
    indexed: bool,
    shared: bool,
) -> anyhow::Result<Outcome> {
    let output = Command::new(driver)
        .arg(duckdb::extension_install_directory())
        .arg(extension_path()?)
        .arg(format!("{preamble}{attach}"))
        .arg(writers.to_string())
        .arg(driver_bound(bound))
        .args(indexed.then_some("indexed"))
        .args(shared.then_some("shared"))
        .output()
        .context("running the concurrent-writer driver")?;
    ensure!(
        output.status.success(),
        "the driver exited with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(Outcome {
        elapsed: Duration::from_secs_f64(parse_elapsed(&stdout)?),
        commits: parse_commits(&stdout)?,
    })
}

/// How the driver is told what bounds a writer: a commit count, or
/// seconds with a trailing `s`.
fn driver_bound(bound: Bound) -> String {
    match bound {
        Bound::Commits(each) => each.to_string(),
        Bound::Window(window) => format!("{}s", window.as_secs_f64()),
    }
}

/// The packaged extension the driver loads.
fn extension_path() -> anyhow::Result<PathBuf> {
    let path =
        duckdb::workspace_root().join("build/release/extension/moraine/moraine.duckdb_extension");
    ensure!(
        path.exists(),
        "no packaged extension at {}; build it first",
        path.display()
    );
    Ok(path)
}

/// Reads the driver's `commits=` line.
fn parse_commits(stdout: &str) -> anyhow::Result<u64> {
    let Some(value) = stdout
        .lines()
        .rev()
        .find_map(|line| line.trim().strip_prefix("commits="))
    else {
        bail!("the driver printed no `commits` line:\n{stdout}");
    };
    value
        .parse()
        .with_context(|| format!("parsing `{value}` as a commit count"))
}

/// Reads the driver's `elapsed_seconds=` line.
fn parse_elapsed(stdout: &str) -> anyhow::Result<f64> {
    let Some(value) = stdout
        .lines()
        .rev()
        .find_map(|line| line.trim().strip_prefix("elapsed_seconds="))
    else {
        bail!("the driver printed no `elapsed_seconds` line:\n{stdout}");
    };
    value
        .parse()
        .with_context(|| format!("parsing `{value}` as the timed window"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_driver_reports_the_window_it_timed() {
        let close = |text: &str, expected: f64| {
            (parse_elapsed(text).unwrap() - expected).abs() < f64::EPSILON
        };
        assert!(close("elapsed_seconds=1.250000\n", 1.25));
        assert!(close("note: warming up\nelapsed_seconds=0.5\n", 0.5));
    }

    #[test]
    fn the_driver_reports_what_it_committed() {
        assert_eq!(
            parse_commits("elapsed_seconds=1.0\ncommits=4096\n").unwrap(),
            4096
        );
        assert!(parse_commits("elapsed_seconds=1.0\n").is_err());
    }

    #[test]
    fn a_bound_is_a_count_or_seconds_with_a_suffix() {
        assert_eq!(driver_bound(Bound::Commits(16)), "16");
        assert_eq!(driver_bound(Bound::Window(Duration::from_secs(5))), "5s");
    }

    #[test]
    fn a_driver_that_printed_no_timing_is_a_failure_not_a_zero() {
        let error = parse_elapsed("writer 0: boom\n").unwrap_err().to_string();
        assert!(error.contains("elapsed_seconds"), "{error}");
        assert!(parse_elapsed("elapsed_seconds=soon").is_err());
    }

    #[test]
    fn the_driver_is_rebuilt_only_when_its_source_is_newer() {
        let root = std::env::temp_dir().join(format!("in-process-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let source = root.join("driver.c");
        let binary = root.join("driver");

        fs::write(&source, "int main(void){return 0;}").unwrap();
        assert!(needs_build(&source, &binary), "no binary yet");

        fs::write(&binary, "stale").unwrap();
        assert!(
            !needs_build(&source, &binary),
            "binary is newer than source"
        );

        filetime_touch(&source);
        assert!(needs_build(&source, &binary), "source moved ahead");

        fs::remove_dir_all(&root).unwrap();
    }

    /// Moves `path`'s modification time past anything already on disk.
    fn filetime_touch(path: &std::path::Path) {
        let later = std::time::SystemTime::now() + Duration::from_mins(1);
        fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(later)
            .unwrap();
    }
}
