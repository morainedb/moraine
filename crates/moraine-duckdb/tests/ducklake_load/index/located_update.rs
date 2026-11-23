//! An `UPDATE` whose predicate a certified index answers reads its rows by
//! position: the located scan feeds DuckLake's own update operator, which
//! deletes the old versions by file position and writes the new ones under
//! their row ids. Anything the located scan cannot serve leaves the plan as
//! written.

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
fn an_indexed_update_reads_its_rows_by_position() {
    let store = TempDir::new("located-update-store");
    let data = TempDir::new("located-update-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);
    let before = csv_rows(&run("SELECT rowid, b FROM lake.main.t WHERE a = 30001;"));
    assert_eq!(before.len(), 1, "{before:?}");

    let plan = run("EXPLAIN ANALYZE UPDATE lake.main.t SET b = 'z' WHERE a = 30001;");
    assert!(
        plan.contains("Coverage costing ms"),
        "the update's rows were not read by position: {plan}"
    );
    assert!(
        plan.lines()
            .any(|line| line.contains("Total Files Read: 1")),
        "the located update opened more than the row's file: {plan}"
    );

    let after = csv_rows(&run("SELECT rowid, b FROM lake.main.t WHERE a = 30001;"));
    assert_eq!(
        after,
        vec![vec![before[0][0].clone(), "z".to_string()]],
        "the row keeps its id and carries the new value"
    );
    let result = run("SELECT 'rows', count(*) FROM lake.main.t;
        SELECT 'verified', missing, dead, mismatched FROM moraine_index_verify('lake','main','t','by_a');");
    assert!(has_line(&result, "rows,100000"), "{result}");
    assert!(has_line(&result, "verified,0,0,0"), "{result}");

    // The rewritten row now lives in the update's output file; a second
    // located update finds it there.
    let again = run("EXPLAIN ANALYZE UPDATE lake.main.t SET b = 'zz' WHERE a = 30001;");
    assert!(again.contains("Coverage costing ms"), "{again}");
    assert_eq!(
        csv_rows(&run("SELECT rowid, b FROM lake.main.t WHERE a = 30001;")),
        vec![vec![before[0][0].clone(), "zz".to_string()]]
    );
}

/// A row the index places in inlined data has no file position, so the
/// update keeps DuckLake's scan and still lands.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_update_of_an_inlined_row_keeps_the_scan() {
    let store = TempDir::new("located-update-inline-store");
    let data = TempDir::new("located-update-inline-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    run("SET ducklake_default_data_inlining_row_limit = 0;
         CREATE TABLE lake.main.t AS SELECT i AS a, 'v' || i::VARCHAR AS b FROM range(1000) r(i);
         CREATE UNIQUE INDEX by_a ON lake.main.t(a);");
    // Below the inlining limit, so this row is inlined rather than written.
    run("INSERT INTO lake.main.t VALUES (5000, 'inlined');");
    let certified = run("SELECT * FROM moraine_index_verify('lake','main','t','by_a');");
    assert!(certified.contains(",0,0,0,"), "{certified}");

    let plan = run("EXPLAIN ANALYZE UPDATE lake.main.t SET b = 'z' WHERE a = 5000;");
    assert!(
        !plan.contains("Coverage costing ms"),
        "an inlined row has no position to update by: {plan}"
    );
    assert_eq!(
        csv_rows(&run("SELECT b FROM lake.main.t WHERE a = 5000;")),
        vec![vec!["z"]]
    );
}

/// Rows inserted earlier in the same transaction have no committed
/// position, so the update keeps DuckLake's scan.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_update_after_a_local_insert_keeps_the_scan() {
    let store = TempDir::new("located-update-local-store");
    let data = TempDir::new("located-update-local-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);

    let result = run("BEGIN;
        INSERT INTO lake.main.t VALUES (100001, 'local');
        EXPLAIN ANALYZE UPDATE lake.main.t SET b = 'z' WHERE a = 30001;
        SELECT 'changed', b FROM lake.main.t WHERE a = 30001;
        SELECT 'local', b FROM lake.main.t WHERE a = 100001;
        COMMIT;");
    assert!(
        !result.contains("Coverage costing ms"),
        "a transaction with local inserts must keep the scan: {result}"
    );
    assert!(has_line(&result, "changed,z"), "{result}");
    assert!(has_line(&result, "local,local"), "{result}");
}

/// Keys `7919` and `15838` are the first file's second and third rows, so a
/// delete of one and an update of the other land in the same file.
const DELETED_KEY: u64 = 7919;
const UPDATED_KEY: u64 = 15838;

/// The table's row counts and live deletion files after a delete of one key
/// and an update of another: every row but the deleted one, the updated row
/// once, and a single deletion file for the file holding both.
fn deletion_state(run: &dyn Fn(&str) -> String) -> String {
    run(&format!(
        "SELECT 'rows', count(*), count(*) FILTER (WHERE a = {DELETED_KEY}),
                count(*) FILTER (WHERE a = {UPDATED_KEY} AND b = 'z') FROM lake.main.t;
         SELECT 'deletion_files', count(*) FROM __ducklake_metadata_lake.ducklake_delete_file
         WHERE end_snapshot IS NULL;
         SELECT 'verified', missing, dead, mismatched FROM moraine_index_verify('lake','main','t','by_a');"
    ))
}

/// A located update of a row in a file that already carries a deletion file
/// replaces that file rather than leaving two in force.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_update_in_a_file_with_deletions_replaces_its_deletion_file() {
    let store = TempDir::new("located-update-deletions-store");
    let data = TempDir::new("located-update-deletions-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);
    run(&format!("DELETE FROM lake.main.t WHERE a = {DELETED_KEY};"));

    let plan = run(&format!(
        "EXPLAIN ANALYZE UPDATE lake.main.t SET b = 'z' WHERE a = {UPDATED_KEY};"
    ));
    assert!(plan.contains("Coverage costing ms"), "{plan}");
    let state = deletion_state(&run);
    assert!(has_line(&state, "rows,99999,0,1"), "{state}");
    assert!(has_line(&state, "deletion_files,1"), "{state}");
    assert!(has_line(&state, "verified,0,0,0"), "{state}");
}

/// A located update after a delete in the same transaction merges with that
/// pending deletion file.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_update_after_a_local_delete_merges_its_deletions() {
    let store = TempDir::new("located-update-local-delete-store");
    let data = TempDir::new("located-update-local-delete-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);

    let result = run(&format!(
        "BEGIN;
         DELETE FROM lake.main.t WHERE a = {DELETED_KEY};
         EXPLAIN ANALYZE UPDATE lake.main.t SET b = 'z' WHERE a = {UPDATED_KEY};
         COMMIT;"
    ));
    assert!(result.contains("Coverage costing ms"), "{result}");
    let state = deletion_state(&run);
    assert!(has_line(&state, "rows,99999,0,1"), "{state}");
    assert!(has_line(&state, "deletion_files,1"), "{state}");
    assert!(has_line(&state, "verified,0,0,0"), "{state}");
}

/// With deletions inlined into the catalog, a located update in a file that
/// already has some adds to them.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_update_in_a_file_with_inlined_deletions_adds_to_them() {
    let store = TempDir::new("located-update-inlined-deletions-store");
    let data = TempDir::new("located-update-inlined-deletions-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 10",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);
    run(&format!("DELETE FROM lake.main.t WHERE a = {DELETED_KEY};"));

    let plan = run(&format!(
        "EXPLAIN ANALYZE UPDATE lake.main.t SET b = 'z' WHERE a = {UPDATED_KEY};"
    ));
    assert!(plan.contains("Coverage costing ms"), "{plan}");
    let state = deletion_state(&run);
    assert!(has_line(&state, "rows,99999,0,1"), "{state}");
    assert!(has_line(&state, "deletion_files,0"), "{state}");
    assert!(has_line(&state, "verified,0,0,0"), "{state}");
}
