//! `moraine_move_wal`: moving an existing lake's write-ahead log to
//! another object store.
//!
//! Like `moraine_migrate` it takes a store path and no attached catalog:
//! it opens the writer twice, once to drain the log where it stands and
//! once to record where it now lives.

use crate::helpers::*;

/// A lake created with its log in the catalog store moves it out, reads
/// back through the new store, and from then on is held to it.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn move_wal_adopts_a_log_store_for_an_existing_lake() {
    let store = TempDir::new("move-wal-store");
    let data = TempDir::new("move-wal-data");
    let log = TempDir::new("move-wal-log");

    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(id BIGINT); \
         INSERT INTO lake.main.t VALUES (1), (2);",
    );

    let sql = format!(
        "SELECT from_wal_path, to_wal_path, moved FROM moraine_move_wal('{}', wal_path => '{}');",
        store.path().display(),
        log.path().display()
    );
    let moved = csv_rows(&assert_session_ok(run_unattached(&sql), "move_wal", &sql));
    assert_eq!(
        moved,
        vec![vec![
            // The log was in the catalog store, which is no store of its own.
            "NULL".to_string(),
            log.path().display().to_string(),
            "true".to_string(),
        ]],
        "the log moved from the catalog store to the one named"
    );
    assert!(
        log.path().join("wal").is_dir(),
        "expected the write-ahead log under {:?}",
        log.path()
    );

    // Every attach from here names it, and the rows are all still there.
    let attach_options = format!(", META_WAL_PATH '{}'", log.path().display());
    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &attach_options,
            "INSERT INTO lake.main.t VALUES (3); SELECT count(*) FROM lake.main.t;",
        )),
        vec![vec!["3".to_string()]]
    );

    let refused = run_ducklake_sql_expect_err(
        store.path(),
        data.path(),
        "SELECT count(*) FROM lake.main.t;",
    );
    assert!(
        refused.contains("write-ahead log"),
        "an attach omitting the moved log store must be refused, got: {refused}"
    );

    // And the move is reversible: back in the catalog store, the plain
    // attach works again.
    let back = format!(
        "SELECT moved FROM moraine_move_wal('{}', from_wal_path => '{}', wal_path => NULL);",
        store.path().display(),
        log.path().display()
    );
    assert_eq!(
        csv_rows(&assert_session_ok(
            run_unattached(&back),
            "move_wal back",
            &back
        )),
        vec![vec!["true".to_string()]]
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store.path(),
            data.path(),
            "SELECT count(*) FROM lake.main.t;",
        )),
        vec![vec!["3".to_string()]]
    );
}

/// The move drains the log it leaves behind, so a lake whose last commits
/// are still in the log keeps them — proven by moving the log of a lake
/// that was never cleanly detached.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn move_wal_keeps_what_the_old_log_still_held() {
    let store = TempDir::new("move-wal-drain-store");
    let data = TempDir::new("move-wal-drain-data");
    let log = TempDir::new("move-wal-drain-log");

    // A CLI session that is killed rather than detached leaves its last
    // commits in the log alone.
    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        ", META_FLUSH_INTERVAL_MS 60000",
        "CREATE TABLE lake.main.t(id BIGINT); \
         INSERT INTO lake.main.t VALUES (1), (2), (3);",
    );

    let sql = format!(
        "SELECT moved FROM moraine_move_wal('{}', wal_path => '{}');",
        store.path().display(),
        log.path().display()
    );
    assert_eq!(
        csv_rows(&assert_session_ok(run_unattached(&sql), "move_wal", &sql)),
        vec![vec!["true".to_string()]]
    );

    let attach_options = format!(", META_WAL_PATH '{}'", log.path().display());
    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &attach_options,
            "SELECT count(*) FROM lake.main.t;",
        )),
        vec![vec!["3".to_string()]],
        "the move must carry the old log's commits into the catalog store"
    );
}

/// The move commits through its own writer, so running it inside an
/// explicit transaction would deadlock against the caller's.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn move_wal_refuses_an_explicit_transaction() {
    let store = TempDir::new("move-wal-txn-store");
    let data = TempDir::new("move-wal-txn-data");
    let log = TempDir::new("move-wal-txn-log");
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);",
    );

    let output = run_unattached(&format!(
        "BEGIN; SELECT * FROM moraine_move_wal('{}', wal_path => '{}');",
        store.path().display(),
        log.path().display()
    ));
    let combined = combined_output(&output);
    assert!(
        combined.contains("cannot run inside an explicit transaction"),
        "expected the transaction refusal, got: {combined}"
    );
}
