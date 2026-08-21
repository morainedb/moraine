//! `moraine_migrate`: the operator's SQL surface for a structural format
//! migration.
//!
//! Every other function here runs against an attached catalog. This one
//! must not, and that is the property under test: a store below the
//! binary's format floor, or one an interrupted run left carrying a
//! migration marker, is refused by ATTACH — that refusal is what keeps
//! readers off a keyspace in motion. So the stores the verb exists to
//! repair are exactly the ones no session can attach, and a surface
//! reachable only through an attached catalog would be reachable only for
//! stores that never needed it.

use std::process::Command;

use crate::helpers::*;

/// Runs `sql` in a session that loads the extension and attaches nothing.
fn run_unattached(sql: &str) -> std::process::Output {
    Command::new(cli_path())
        .arg("-unsigned")
        .arg("-csv")
        .arg("-c")
        .arg(format!("LOAD '{}';", ext_path().display()))
        .arg("-c")
        .arg(sql)
        .output()
        .expect("failed to spawn duckdb CLI")
}

/// The verb reaches a store no session has attached, reports what it did,
/// and re-runs as a no-op. Every format to date is additive, so there is
/// nothing to rewrite and the store reports the version it already
/// carries — the dormant case, which is the one an operator hits until the
/// first rewriting format exists.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extension"]
fn migrate_reaches_a_store_without_attaching_it() {
    let store = TempDir::new("migrate-store");
    let data = TempDir::new("migrate-data");

    // Create the store through an ordinary attach, then leave it alone.
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);",
    );

    let sql = format!(
        "SELECT from_format, to_format, resumed, units_run FROM moraine_migrate('{}');",
        store.path().display()
    );
    let first = assert_session_ok(run_unattached(&sql), "migrate", &sql);
    let rows = csv_rows(&first);
    assert_eq!(
        rows,
        vec![vec![
            // The newest format this binary reads: a bootstrapped store
            // already carries it, so the migration rewrites nothing.
            moraine::MAX_FORMAT_VERSION.to_string(),
            moraine::MAX_FORMAT_VERSION.to_string(),
            "false".to_string(),
            String::new(),
        ]],
        "a store already at the newest format reports it and rewrites nothing"
    );

    // Running it again changes nothing: there is no marker to resume and
    // the format is already the target.
    let second = assert_session_ok(run_unattached(&sql), "migrate re-run", &sql);
    assert_eq!(csv_rows(&second), rows, "the verb is idempotent");

    // The store is still perfectly attachable afterwards.
    let after = run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake';",
    );
    assert!(
        after.contains('t'),
        "migrate left the store attachable: {after}"
    );
}

/// The migration commits through its own writer, so running it inside an
/// explicit transaction would deadlock against the caller's. It refuses
/// instead, the way maintenance does.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extension"]
fn migrate_refuses_an_explicit_transaction() {
    let store = TempDir::new("migrate-txn-store");
    let data = TempDir::new("migrate-txn-data");
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);",
    );

    let output = run_unattached(&format!(
        "BEGIN; SELECT * FROM moraine_migrate('{}');",
        store.path().display()
    ));
    let combined = combined_output(&output);
    assert!(
        combined.contains("cannot run inside an explicit transaction"),
        "expected the transaction refusal, got: {combined}"
    );
}
