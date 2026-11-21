//! A commit whose outcome the catalog cannot report keeps the Parquet it
//! wrote.
//!
//! DuckLake deletes a transaction's data files when its metadata commit
//! fails, which is right for a commit that did not happen and wrong for one
//! that may have: the snapshot it landed references those files. The
//! bundled DuckLake recognises moraine's unknown-outcome report, stops
//! retrying, and leaves the files for reconciliation.

use crate::helpers::{
    TempDir, csv_rows, parquet_files_under, run_ducklake_sql, run_ducklake_sql_expect_err,
};

/// The acknowledgement of a landed commit is dropped: the statement fails
/// naming the retained files, the Parquet stays on disk, and a fresh
/// attach reads the rows through it.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_unknown_commit_outcome_keeps_the_written_parquet() {
    let store = TempDir::new("unknown-outcome-store");
    let data = TempDir::new("unknown-outcome-data");
    run_ducklake_sql(
        store.path(),
        data.path(),
        "SET ducklake_default_data_inlining_row_limit = 0; CREATE TABLE lake.main.t(a BIGINT);",
    );
    assert!(parquet_files_under(data.path()).is_empty());

    let output = run_ducklake_sql_expect_err(
        store.path(),
        data.path(),
        "SET ducklake_default_data_inlining_row_limit = 0; \
         CALL moraine_fault_drop_next_commit_ack(); \
         INSERT INTO lake.main.t SELECT range FROM range(1000);",
    );
    assert!(
        output.contains("commit outcome unknown"),
        "the statement must report the outcome as unknown: {output}"
    );
    assert!(
        output.contains("files retained for reconciliation"),
        "DuckLake must say it kept the files: {output}"
    );

    let files = parquet_files_under(data.path());
    assert_eq!(
        files.len(),
        1,
        "the Parquet the insert wrote must survive the unknown outcome: {files:?}"
    );

    // The commit landed, so the retained file is the only copy of the rows.
    let rows = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT count(*) FROM lake.main.t;",
    ));
    assert_eq!(rows, vec![vec!["1000".to_string()]]);
}
