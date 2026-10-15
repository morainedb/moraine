//! Drives a real, pinned DuckDB CLI against a store pre-seeded through the
//! `moraine` API: `LOAD`s the packaged extension, `ATTACH`es the store, and
//! exercises listing and `DESCRIBE` through actual SQL. A data scan through
//! this standalone attach must raise a redirect error at execution time
//! (user-table data is served only through DuckLake); this suite asserts
//! that too.
//!
//! Ignored by default: needs the built DuckDB CLI and the packaged
//! `.duckdb_extension`, which only `cargo xtask e2e` produces (it sets
//! `MORAINE_DUCKDB_CLI`/`MORAINE_DUCKDB_EXT` and runs this test un-ignored).
//!
//! Run manually after `cargo xtask e2e` has produced the artifacts once:
//!
//! ```text
//! MORAINE_DUCKDB_CLI=build/release/duckdb \
//! MORAINE_DUCKDB_EXT=build/release/extension/moraine/moraine.duckdb_extension \
//! cargo test -p moraine-duckdb --release --test duckdb_load -- --ignored
//! ```

// The tests-exempt lints (`clippy.toml`) reach `#[test]` functions and
// `#[cfg(test)]` modules, not an integration crate's plain helper
// functions — exempted here instead, crate-wide, as tests.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

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

/// A directory under the OS temp dir, unique per call (pid + a
/// monotonic counter), removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "moraine-duckdb-load-{tag}-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("test setup: create temp dir");
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn cli_path() -> PathBuf {
    PathBuf::from(
        env::var("MORAINE_DUCKDB_CLI")
            .expect("MORAINE_DUCKDB_CLI must be set (see this module's doc comment)"),
    )
}

fn ext_path() -> PathBuf {
    PathBuf::from(
        env::var("MORAINE_DUCKDB_EXT")
            .expect("MORAINE_DUCKDB_EXT must be set (see this module's doc comment)"),
    )
}

/// Rows registered for `s.t`'s data file; must match the Parquet file
/// written by [`write_parquet`] (`range(ROW_COUNT)`).
const ROW_COUNT: u64 = 5;

/// Seeds a store via the `moraine` API: schema `s`, table `empty`
/// (one column, no data files), table `t` (two columns, a
/// relative-path data file registered but not yet written — see
/// [`write_parquet`]), and a view over `t`.
fn seed(dir: &Path) {
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
                let schema = tx.create_schema("s")?;

                tx.create_table(
                    schema,
                    "empty",
                    &[ColumnDef {
                        name: "id".into(),
                        column_type: "BIGINT".into(),
                        nulls_allowed: false,
                        default_value: None,
                        children: Vec::new(),
                    }],
                )?;

                let t = tx.create_table(
                    schema,
                    "t",
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
                        file_size_bytes: 0,
                        footer_size: 0,
                        encryption_key: None,
                        partition_values: vec![],
                        column_stats: vec![],
                    },
                    &[],
                )?;

                tx.create_view(schema, "t_v", "duckdb", "select * from t")?;

                Ok(())
            })
            .await
            .expect("test setup: commit fixtures");

        catalog.close().await.expect("test setup: close catalog");
    });
}

/// Writes `s/t/data.parquet` under the store directory via the
/// DuckDB CLI's own `COPY ... TO`; the file's bytes come from
/// DuckDB, only its catalog registration comes from the `moraine`
/// API.
fn write_parquet(store_dir: &Path) {
    let table_dir = store_dir.join("s").join("t");
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
}

/// Runs `LOAD`, `ATTACH`, then `sql` against the seeded store through
/// the pinned CLI in CSV mode, returning stdout. Each call is a
/// fresh CLI invocation, so each query's CSV output is exactly one
/// header line plus its rows, nothing else interleaved.
fn run_sql(store_dir: &Path, sql: &str) -> String {
    let output = Command::new(cli_path())
        .arg("-unsigned")
        .arg("-csv")
        .arg("-c")
        .arg(format!("LOAD '{}';", ext_path().display()))
        .arg("-c")
        .arg(format!(
            "ATTACH '{}' AS m (TYPE moraine);",
            store_dir.display()
        ))
        .arg("-c")
        .arg(sql)
        .output()
        .expect("failed to spawn duckdb CLI");
    assert!(
        output.status.success(),
        "duckdb CLI failed for `{sql}`:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("duckdb CLI stdout is not UTF-8")
}

/// Like [`run_sql`], but asserts `sql` fails and returns stderr instead
/// of asserting success. Used for the standalone attach's data-scan
/// redirect, which must raise at execution time.
fn run_sql_expect_failure(store_dir: &Path, sql: &str) -> String {
    let output = Command::new(cli_path())
        .arg("-unsigned")
        .arg("-csv")
        .arg("-c")
        .arg(format!("LOAD '{}';", ext_path().display()))
        .arg("-c")
        .arg(format!(
            "ATTACH '{}' AS m (TYPE moraine);",
            store_dir.display()
        ))
        .arg("-c")
        .arg(sql)
        .output()
        .expect("failed to spawn duckdb CLI");
    assert!(
        !output.status.success(),
        "expected `{sql}` to fail, but it succeeded:\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    String::from_utf8(output.stderr).expect("duckdb CLI stderr is not UTF-8")
}

/// Splits CSV output (header line + data lines) into rows of fields,
/// dropping the header. Assumes no embedded commas or quoting, true
/// for this test's data.
fn csv_rows(output: &str) -> Vec<Vec<String>> {
    output
        .lines()
        .skip(1)
        .filter(|line| !line.is_empty())
        .map(|line| line.split(',').map(str::to_owned).collect())
        .collect()
}

/// `LOAD`s the extension, `ATTACH`es the seeded store as a standalone
/// moraine catalog, and asserts listing, `DESCRIBE`, and scans
/// (including an empty table and a real Parquet-backed table)
/// round-trip through real DuckDB SQL.
///
/// Run via `cargo xtask e2e`, or manually — see this module's doc
/// comment.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged extension; run via `cargo xtask e2e`"]
fn attach_lists_and_scans_through_real_duckdb() {
    let dir = TempDir::new("attach");
    seed(dir.path());
    write_parquet(dir.path());
    let store = dir.path();

    let databases = csv_rows(&run_sql(
        store,
        "SELECT database_name FROM duckdb_databases() WHERE database_name = 'm';",
    ));
    assert_eq!(databases, vec![vec!["m".to_string()]]);

    // Scoped to schema `s`: `main` also carries the synthesized
    // `ducklake_*` metadata tables this shim serves, which would
    // otherwise leak into this listing.
    let tables = csv_rows(&run_sql(
        store,
        "SELECT table_name FROM duckdb_tables() WHERE database_name = 'm' AND schema_name = 's' \
         ORDER BY table_name;",
    ));
    assert_eq!(
        tables,
        vec![vec!["empty".to_string()], vec!["t".to_string()]]
    );

    // `nulls_allowed` maps to DuckDB's NOT NULL constraint; DESCRIBE's
    // `null` column reflects it (`id` non-nullable, `amount` nullable).
    let describe_t = csv_rows(&run_sql(store, "DESCRIBE m.s.t;"));
    assert_eq!(
        describe_t,
        vec![
            vec![
                "id".to_string(),
                "BIGINT".to_string(),
                "NO".to_string(),
                "NULL".to_string(),
                "NULL".to_string(),
                "NULL".to_string()
            ],
            vec![
                "amount".to_string(),
                "DOUBLE".to_string(),
                "YES".to_string(),
                "NULL".to_string(),
                "NULL".to_string(),
                "NULL".to_string()
            ],
        ]
    );

    let describe_empty = csv_rows(&run_sql(store, "DESCRIBE m.s.empty;"));
    assert_eq!(
        describe_empty,
        vec![vec![
            "id".to_string(),
            "BIGINT".to_string(),
            "NO".to_string(),
            "NULL".to_string(),
            "NULL".to_string(),
            "NULL".to_string()
        ]]
    );

    // The scan binds but raises a redirect error at execution time,
    // naming the table and the `ducklake:moraine:` attach to use
    // instead.
    let stderr = run_sql_expect_failure(store, "SELECT * FROM m.s.t;");
    assert!(
        stderr.contains("s.t"),
        "expected the redirect error to name the table; got: {stderr}"
    );
    assert!(
        stderr.contains("ducklake:moraine:"),
        "expected the redirect error to name the ducklake:moraine: attach form; got: {stderr}"
    );
    assert!(
        stderr.contains(&store.display().to_string()),
        "expected the redirect error to include the real store path; got: {stderr}"
    );
    // DuckLake's `RetryOnError` treats these substrings as retryable;
    // the redirect is not a commit race and must never be retried.
    for substring in ["conflict", "unique", "primary key", "concurrent"] {
        assert!(
            !stderr.to_lowercase().contains(substring),
            "redirect error must not contain DuckLake's retry substring \"{substring}\": {stderr}"
        );
    }

    let empty_scan_stderr = run_sql_expect_failure(store, "SELECT * FROM m.s.empty;");
    assert!(
        empty_scan_stderr.contains("s.empty"),
        "expected the redirect error to name the empty table; got: {empty_scan_stderr}"
    );

    let views = csv_rows(&run_sql(
        store,
        "SELECT database_name, schema_name, view_name FROM duckdb_views() WHERE database_name = 'm';",
    ));
    assert_eq!(
        views,
        vec![vec!["m".to_string(), "s".to_string(), "t_v".to_string()]]
    );
}
