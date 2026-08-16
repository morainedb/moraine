//! The `e2e` task: downloads/caches the pinned DuckDB CLI, builds and
//! packages the extension, and runs
//! `crates/moraine-duckdb/tests/duckdb_load.rs`,
//! `crates/moraine-duckdb/tests/ducklake_load.rs`, and the `test/sql`
//! sqllogictest files un-ignored against the real binaries and repository
//! DuckLake patch as a hard assertion.

use std::{path::Path, process::Command};

use anyhow::{Context, ensure};

use crate::{duckdb, ducklake_patch};

/// The single `#[ignore]`d integration test `e2e` un-ignores and runs by
/// exact name. Deleting or renaming it fails `e2e` instead of silently
/// matching zero tests.
const DUCKDB_LOAD_TEST_NAME: &str = "attach_lists_and_scans_through_real_duckdb";

/// Every `#[ignore]`d test in `ducklake_load.rs`, run together (not
/// `--exact`, since there are many). Adding a test there means bumping this
/// count, or `e2e` fails — deliberate, so a silently-filtered test can never
/// pass.
const DUCKLAKE_LOAD_TEST_COUNT: &str = "115 passed";

/// Every file under `test/sql`, run together through DuckDB's own
/// sqllogictest runner. Adding one means bumping this count, exactly as for
/// the suites above — and a file that *skips* (its `require-env` unmet, or
/// an extension download failing) reports no passing case at all, so a
/// silent skip fails the gate instead of looking like a pass.
const SQLLOGIC_TEST_COUNT: &str = "4 test cases";

/// Downloads/caches the pinned DuckDB CLI, builds Moraine and patched
/// DuckLake, then runs every repository test against those artifacts.
pub fn e2e() -> anyhow::Result<()> {
    let cli = duckdb::ensure_duckdb_cli()?;
    println!("ok: duckdb CLI at {}", cli.display());

    let extension = duckdb::build_and_package_extension()?;
    println!("ok: packaged {}", extension.display());

    let ducklake_extension = ducklake_patch::build_artifact(&[])?;
    println!(
        "ok: built and validated patched DuckLake at {}",
        ducklake_extension.display()
    );

    duckdb::run(Command::new("cargo").args(["test", "-p", "moraine-duckdb", "--release"]))?;

    let envs: &[(&str, &std::ffi::OsStr)] = &[
        ("MORAINE_DUCKDB_CLI", cli.as_os_str()),
        ("MORAINE_DUCKDB_EXT", extension.as_os_str()),
        ("MORAINE_DUCKLAKE_EXT", ducklake_extension.as_os_str()),
    ];

    duckdb::run_ignored_suite(
        "moraine-duckdb",
        "duckdb_load",
        true,
        &["--exact", DUCKDB_LOAD_TEST_NAME],
        envs,
        "1 passed",
    )?;
    println!("ok: real DuckDB loaded moraine_duckdb and drove attach/listing/scan");

    duckdb::run_ignored_suite(
        "moraine-duckdb",
        "ducklake_load",
        true,
        &[],
        envs,
        DUCKLAKE_LOAD_TEST_COUNT,
    )?;
    println!(
        "ok: real DuckDB + ducklake attached through moraine:'s metadata catalog and read the lake"
    );

    run_sqllogictests(&extension, &ducklake_extension)?;
    println!(
        "ok: over several connections — two DuckLake transactions raced over one lake, a commit \
         landed under an open reader without moving what it reads, and maintenance contended \
         with a held write transaction instead of deadlocking behind it"
    );

    Ok(())
}

/// Runs `test/sql/*.test` through DuckDB's own sqllogictest runner, built
/// alongside the extension.
///
/// This is the one harness that gives **several connections over one DuckDB
/// instance**, which the CLI cannot: two overlapping DuckLake transactions
/// against one attached lake are the only way to make DuckLake's commit
/// retry loop run against moraine. The runner loads both extensions by path,
/// so it exercises the same artifacts the CLI suites do.
fn run_sqllogictests(extension: &Path, ducklake_extension: &Path) -> anyhow::Result<()> {
    let root = duckdb::workspace_root();
    let runner = root.join("build/release/test/unittest");
    ensure!(
        runner.exists(),
        "the sqllogictest runner is missing at {}; `make release` builds it \
         alongside the extension",
        runner.display()
    );

    let extension_dir = root.join("target/duckdb-extensions");
    let output = Command::new(&runner)
        .arg("--test-dir")
        .arg(&root)
        .arg("test/sql/*")
        .env("MORAINE_DUCKDB_EXT", extension.as_os_str())
        .env("MORAINE_DUCKLAKE_EXT", ducklake_extension.as_os_str())
        .env("MORAINE_EXTENSION_DIR", extension_dir.as_os_str())
        .output()
        .with_context(|| format!("spawning {}", runner.display()))?;

    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    ensure!(output.status.success(), "the sqllogictest suite failed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    ensure!(
        stdout.contains("All tests passed") && stdout.contains(SQLLOGIC_TEST_COUNT),
        "expected the sqllogictest suite to report `All tests passed` over \
         `{SQLLOGIC_TEST_COUNT}`; a file may have been added, deleted, or silently \
         skipped. Got:\n{stdout}"
    );
    Ok(())
}
