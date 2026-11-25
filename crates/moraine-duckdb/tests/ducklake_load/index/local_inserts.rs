//! Inside a transaction that has inserted rows, an indexed predicate still
//! reads committed rows by position. The transaction's own files are read by
//! DuckLake's scan over just those files, and its inlined rows by the located
//! scan itself, so the predicate sees every row the transaction does.

use crate::helpers::*;

/// Two files of permuted keys with a certified unique index on `a`.
fn certified_table(run: &dyn Fn(&str) -> String) {
    run("CREATE TABLE lake.main.t AS SELECT (i*7919)%100000 AS a,
             repeat(md5(i::VARCHAR),6) AS b FROM range(50000) r(i);
         INSERT INTO lake.main.t SELECT (i*7919)%100000,
             repeat(md5(i::VARCHAR),6) FROM range(50000,100000) r(i);
         CREATE UNIQUE INDEX by_a ON lake.main.t(a);");
    let certified = run("SELECT * FROM moraine_index_verify('lake','main','t','by_a');");
    assert!(
        certified
            .lines()
            .any(|line| line.starts_with("100000,100000,0,0,0,")),
        "the index did not verify clean: {certified}"
    );
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_predicate_in_a_writing_transaction_reads_committed_and_local_rows() {
    let store = TempDir::new("local-inserts-store");
    let data = TempDir::new("local-inserts-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);

    let result = run("BEGIN;
        INSERT INTO lake.main.t VALUES (100001, 'local');
        EXPLAIN ANALYZE SELECT b FROM lake.main.t WHERE a = 30001;
        EXPLAIN SELECT b FROM lake.main.t WHERE a = 100001;
        SELECT 'committed', count(*) FROM lake.main.t WHERE a = 30001;
        SELECT 'local', b FROM lake.main.t WHERE a = 100001;
        SELECT 'both', count(*) FROM lake.main.t WHERE a IN (30001, 100001);
        COMMIT;
        SELECT 'after', count(*) FROM lake.main.t WHERE a IN (30001, 100001);");
    assert!(
        result.contains("Coverage costing ms"),
        "committed rows were not read by position: {result}"
    );
    assert!(
        !result.contains("EMPTY_RESULT"),
        "a key no committed row holds still has to reach the local rows: {result}"
    );
    for expected in ["committed,1", "local,local", "both,2", "after,2"] {
        assert!(has_line(&result, expected), "{result}");
    }
}

/// With inlining on, the transaction's rows live in memory rather than a
/// file; the located scan emits them itself, and a delete of one in the same
/// transaction removes it.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_predicate_in_a_writing_transaction_reads_local_inlined_rows() {
    let store = TempDir::new("local-inlined-store");
    let data = TempDir::new("local-inlined-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 10",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);

    let result = run("BEGIN;
        INSERT INTO lake.main.t VALUES (100001, 'local'), (100002, 'other');
        EXPLAIN ANALYZE SELECT b FROM lake.main.t WHERE a = 100001;
        SELECT 'local', b FROM lake.main.t WHERE a = 100001;
        SELECT 'committed', count(*) FROM lake.main.t WHERE a = 30001;
        DELETE FROM lake.main.t WHERE a = 100001;
        SELECT 'deleted', count(*) FROM lake.main.t WHERE a = 100001;
        SELECT 'kept', b FROM lake.main.t WHERE a = 100002;
        COMMIT;
        SELECT 'after', count(*) FROM lake.main.t WHERE a IN (100001, 100002);");
    assert!(
        result.contains("Coverage costing ms"),
        "the predicate did not plan as a located read: {result}"
    );
    for expected in [
        "local,local",
        "committed,1",
        "deleted,0",
        "kept,other",
        "after,1",
    ] {
        assert!(has_line(&result, expected), "{result}");
    }
}

/// An UPDATE earlier in the transaction deletes the committed copy and writes
/// the new one to a local file; a predicate sees the new version once.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_predicate_after_a_local_update_sees_the_new_version_once() {
    let store = TempDir::new("local-update-store");
    let data = TempDir::new("local-update-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);

    let result = run("BEGIN;
        UPDATE lake.main.t SET b = 'u' WHERE a = 30001;
        EXPLAIN ANALYZE SELECT b FROM lake.main.t WHERE a = 30001;
        SELECT 'changed', b FROM lake.main.t WHERE a = 30001;
        COMMIT;
        SELECT 'verified', missing, dead, mismatched FROM moraine_index_verify('lake','main','t','by_a');");
    assert!(result.contains("Coverage costing ms"), "{result}");
    assert_eq!(
        csv_rows(&result)
            .into_iter()
            .filter(|row| row.first().is_some_and(|phase| phase == "changed"))
            .collect::<Vec<_>>(),
        vec![vec!["changed".to_string(), "u".to_string()]],
        "{result}"
    );
    assert!(has_line(&result, "verified,0,0,0"), "{result}");
}
