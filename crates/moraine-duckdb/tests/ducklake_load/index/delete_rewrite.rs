use crate::helpers::*;

/// 100,000 rows in two files, keys permuted so column statistics prune
/// nothing, with a unique index over the key. Past the probe's size gate,
/// which every trigger here needs.
fn indexed_table(run: &dyn Fn(&str) -> String) {
    run("CREATE TABLE lake.main.t AS SELECT (i*7919)%100000 AS a,
             repeat(md5(i::VARCHAR),6) AS b FROM range(50000) r(i);
         INSERT INTO lake.main.t SELECT (i*7919)%100000,
             repeat(md5(i::VARCHAR),6) FROM range(50000,100000) r(i);");
    run("CREATE UNIQUE INDEX by_a ON lake.main.t(a);");
}

/// The same, with the index certified — the state the rewrite requires.
fn certified_table(run: &dyn Fn(&str) -> String) {
    indexed_table(run);
    let certified = run("SELECT * FROM moraine_index_verify('lake','main','t','by_a');");
    assert!(
        certified
            .lines()
            .any(|line| line.starts_with("100000,100000,0,0,0,")),
        "the index did not verify clean: {certified}"
    );
}

/// A `DELETE` whose whole predicate a certified index answers plans as the
/// positional deletion of the rows it reports: no scan survives, so nothing
/// is read.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_certified_equality_delete_reads_nothing() {
    let store = TempDir::new("delete-rewrite-store");
    let data = TempDir::new("delete-rewrite-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);

    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FROM duckdb_functions() WHERE function_name IN
            ('moraine_rows_at', 'moraine_delete_located', 'moraine_update',
             'ducklake_update_positions');"
        )),
        vec![vec!["0"]],
        "the automatic rewrite must not restore public locator functions"
    );

    let plan = run("EXPLAIN ANALYZE DELETE FROM lake.main.t WHERE a = 30001;");
    assert!(
        plan.contains("DUCKLAKE_DELETE_POSITIONS"),
        "the delete was not rewritten: {plan}"
    );
    assert!(
        !plan.contains("DUCKLAKE_SCAN") && !plan.contains("ducklake_scan"),
        "a scan survived the rewrite: {plan}"
    );

    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FILTER (WHERE a = 30001), count(*) FROM lake.main.t;"
        )),
        vec![vec!["0".to_string(), "99999".to_string()]],
        "the rewrite removed its row and only its row"
    );
}

/// The rewrite deletes the rows it was asked for and no others, including
/// when the value no row holds. A DuckLake `DELETE` prints no count, so the
/// table is counted instead.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn the_rewrite_deletes_exactly_its_rows() {
    let store = TempDir::new("delete-rewrite-count-store");
    let data = TempDir::new("delete-rewrite-count-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);

    assert_eq!(
        csv_rows(&run(
            "DELETE FROM lake.main.t WHERE a = 30001;
             DELETE FROM lake.main.t WHERE a = 999999;
             SELECT count(*) FILTER (WHERE a = 30001), count(*) FROM lake.main.t;"
        ))
        .last()
        .cloned()
        .unwrap_or_default(),
        vec!["0".to_string(), "99999".to_string()],
        "a value no row holds must delete nothing, and the key exactly one row"
    );

    // Repeating it is a no-op: the row is already gone.
    assert_eq!(
        csv_rows(&run("DELETE FROM lake.main.t WHERE a = 30001;
             SELECT count(*) FROM lake.main.t;"))
        .last()
        .cloned()
        .unwrap_or_default(),
        vec!["99999".to_string()],
        "deleting an already-dead row changed the table"
    );
}

/// Without a certificate the scan stays: the rewrite replaces predicate
/// evaluation, so it will not run on an index nothing has checked.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_uncertified_index_keeps_the_scan() {
    let store = TempDir::new("delete-rewrite-uncertified-store");
    let data = TempDir::new("delete-rewrite-uncertified-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    indexed_table(&run);

    let result = run_session_with_env(
        &Attach::Moraine {
            store_dir: store.path(),
            data_path: data.path(),
            options: &meta,
            read_only: false,
        },
        "CALL enable_logging(level => 'debug', storage => 'memory');
         EXPLAIN DELETE FROM lake.main.t WHERE a = 30001;
         SELECT 'placements', count(*) FROM duckdb_logs
         WHERE type = 'moraine' AND message LIKE '%located row ids%';",
        &[("MORAINE_LOG", "debug")],
    );
    let plan = combined_output(&result);
    assert!(result.status.success(), "{plan}");
    assert!(
        csv_rows(&plan).contains(&vec!["placements".into(), "1".into()]),
        "a declined rewrite must reuse its lookup for scan pruning: {plan}"
    );
    assert!(
        !plan.contains("DUCKLAKE_DELETE_POSITIONS"),
        "an unchecked index must not answer a delete: {plan}"
    );

    // Certifying it, and only that, lets the rewrite fire.
    run("SELECT * FROM moraine_index_verify('lake','main','t','by_a');");
    let plan = run("EXPLAIN DELETE FROM lake.main.t WHERE a = 30001;");
    assert!(
        plan.contains("DUCKLAKE_DELETE_POSITIONS"),
        "a certified index should answer it: {plan}"
    );
}

/// A residual predicate is not something the index answered, so a delete
/// carrying one keeps its scan.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_second_predicate_keeps_the_scan() {
    let store = TempDir::new("delete-rewrite-residual-store");
    let data = TempDir::new("delete-rewrite-residual-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);

    let plan = run("EXPLAIN DELETE FROM lake.main.t WHERE a = 30001 AND b = 'nothing';");
    assert!(
        !plan.contains("DUCKLAKE_DELETE_POSITIONS"),
        "a residual predicate must keep the scan: {plan}"
    );
    assert_eq!(
        csv_rows(&run(
            "DELETE FROM lake.main.t WHERE a = 30001 AND b = 'other';
             SELECT count(*) FROM lake.main.t;"
        ))
        .last()
        .cloned()
        .unwrap_or_default(),
        vec!["100000".to_string()],
        "the residual predicate still decides"
    );
}

/// Plain UPDATE and an explicit index join prune to one file and read equal
/// bytes.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_update_matches_explicit_lookup_pruning() {
    let read_bytes = |plan: &str| -> String {
        assert!(
            plan.lines().any(|line| line.contains("Total Files Read:")
                && line
                    .split_once("Total Files Read:")
                    .unwrap()
                    .1
                    .trim_end_matches(['│', ' '])
                    .trim()
                    == "1"),
            "{plan}"
        );
        plan.lines()
            .find_map(|line| {
                let tail = line.split_once("Data Read:")?.1;
                Some(tail.trim_end_matches(['│', ' ']).trim().to_owned())
            })
            .unwrap_or_else(|| panic!("no Data Read line in {plan}"))
    };

    let predicate = {
        let store = TempDir::new("update-floor-predicate-store");
        let data = TempDir::new("update-floor-predicate-data");
        let meta = format!(
            ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
            data.path().display()
        );
        let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
        certified_table(&run);
        let plan = run("EXPLAIN ANALYZE UPDATE lake.main.t SET b = 'z' WHERE a = 30001;");
        assert!(
            !plan.contains("DUCKLAKE_DELETE_POSITIONS"),
            "an UPDATE must keep its scan: {plan}"
        );
        let result = run("SELECT 'changed', count(*) FROM lake.main.t WHERE a = 30001 AND b = 'z';
             SELECT 'verified', missing, dead, mismatched FROM moraine_index_verify('lake','main','t','by_a');");
        assert!(result.lines().any(|line| line == "changed,1"), "{result}");
        assert!(
            result.lines().any(|line| line == "verified,0,0,0"),
            "{result}"
        );
        read_bytes(&plan)
    };

    let named = {
        let store = TempDir::new("update-floor-named-store");
        let data = TempDir::new("update-floor-named-data");
        let meta = format!(
            ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
            data.path().display()
        );
        let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
        certified_table(&run);
        read_bytes(&run("EXPLAIN ANALYZE UPDATE lake.main.t SET b = 'z' \
             FROM moraine_index_lookup('lake','main','t','by_a',30001) hits \
             WHERE t.rowid = hits.row_id \
               AND t.data_file_id IS NOT DISTINCT FROM hits.data_file_id;"))
    };

    assert_eq!(
        predicate, named,
        "naming the rows exactly read a different amount than the predicate did"
    );
}

/// A prepared delete rebinds after writes and rollback, including pending
/// deletes.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn prepared_deletes_obey_transaction_boundaries() {
    let store = TempDir::new("delete-rewrite-prepared-store");
    let data = TempDir::new("delete-rewrite-prepared-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);
    let result = run("PREPARE remove AS DELETE FROM lake.main.t WHERE a = 30001;
        BEGIN;
        EXECUTE remove;
        EXECUTE remove;
        SELECT 'pending', count(*) FROM lake.main.t WHERE a = 30001;
        ROLLBACK;
        SELECT 'rolled_back', count(*) FROM lake.main.t WHERE a = 30001;
        EXECUTE remove;
        INSERT INTO lake.main.t VALUES (30001, 'replacement');
        EXECUTE remove;
        SELECT 'final', count(*) FROM lake.main.t WHERE a = 30001;
        SELECT 'verified', missing, dead, mismatched FROM moraine_index_verify('lake','main','t','by_a');");
    for expected in ["pending,0", "rolled_back,1", "final,0", "verified,0,0,0"] {
        assert!(result.lines().any(|line| line == expected), "{result}");
    }
}

/// Rewritten deletes preserve committed delete files and transaction-local
/// inserts.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn deletes_preserve_existing_masks_and_local_rows() {
    let store = TempDir::new("delete-rewrite-masks-store");
    let data = TempDir::new("delete-rewrite-masks-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);
    run("DELETE FROM lake.main.t WHERE a < 1000;");
    let plan = run("EXPLAIN ANALYZE DELETE FROM lake.main.t WHERE a = 30001;");
    assert!(plan.contains("DUCKLAKE_DELETE_POSITIONS"), "{plan}");
    let result = run("BEGIN;
        INSERT INTO lake.main.t VALUES (30001, 'local');
        DELETE FROM lake.main.t WHERE a = 30001;
        COMMIT;
        SELECT 'remaining', count(*) FROM lake.main.t;
        SELECT 'verified', missing, dead, mismatched FROM moraine_index_verify('lake','main','t','by_a');");
    assert!(
        result.lines().any(|line| line == "remaining,98999"),
        "{result}"
    );
    assert!(
        result.lines().any(|line| line == "verified,0,0,0"),
        "{result}"
    );
}

/// Deletion-vector tables retain DuckLake's ordinary DELETE operator.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn deletion_vectors_keep_the_scan() {
    let store = TempDir::new("delete-rewrite-vectors-store");
    let data = TempDir::new("delete-rewrite-vectors-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);
    run("CALL ducklake_set_option('lake', 'write_deletion_vectors', true);");
    let plan = run("EXPLAIN DELETE FROM lake.main.t WHERE a = 30001;");
    assert!(!plan.contains("DUCKLAKE_DELETE_POSITIONS"), "{plan}");
    // Index upkeep currently decodes Parquet delete files, so exercise
    // the fallback before committing a deletion vector.
    assert_eq!(
        csv_rows(&run("BEGIN; DELETE FROM lake.main.t WHERE a = 30001;
        SELECT count(*) FROM lake.main.t WHERE a = 30001; ROLLBACK;")),
        vec![vec!["0"]]
    );
}

/// Updating an indexed key preserves row ids and maintains subsequent probes.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_indexed_update_maintains_keys_and_row_ids() {
    let store = TempDir::new("update-key-store");
    let data = TempDir::new("update-key-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    certified_table(&run);
    let result = run("CREATE TEMP TABLE original AS SELECT rowid AS id FROM lake.main.t WHERE a = 30001;
        UPDATE lake.main.t SET a = 200000, b = upper(b) WHERE a = 30001;
        SELECT 'old', count(*) FROM lake.main.t WHERE a = 30001;
        SELECT 'new', count(*) FROM lake.main.t WHERE a = 200000 AND rowid = (SELECT id FROM original);
        SELECT 'verified', missing, dead, mismatched FROM moraine_index_verify('lake','main','t','by_a');");
    for expected in ["old,0", "new,1", "verified,0,0,0"] {
        assert!(result.lines().any(|line| line == expected), "{result}");
    }
}

/// A non-unique equality deletes every matching row across multiple files.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_non_unique_delete_spans_files() {
    let store = TempDir::new("delete-rewrite-multiple-store");
    let data = TempDir::new("delete-rewrite-multiple-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    run("CREATE TABLE lake.main.t AS SELECT (i*7919)%1000 AS a,
             repeat(md5(i::VARCHAR),6) AS b FROM range(50000) r(i);
         INSERT INTO lake.main.t SELECT (i*7919)%1000,
             repeat(md5(i::VARCHAR),6) FROM range(50000,100000) r(i);
         CREATE INDEX by_a ON lake.main.t(a);
         SELECT * FROM moraine_index_verify('lake','main','t','by_a');");
    assert_eq!(
        csv_rows(&run("SELECT count(*), count(DISTINCT data_file_id)
        FROM lake.main.t WHERE a = 1;")),
        vec![vec!["100", "2"]]
    );
    let plan = run("EXPLAIN ANALYZE DELETE FROM lake.main.t WHERE a = 1;");
    assert!(plan.contains("DUCKLAKE_DELETE_POSITIONS"), "{plan}");
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*), count(*) FILTER (WHERE a = 1) FROM lake.main.t;"
        )),
        vec![vec!["99900", "0"]]
    );
    assert_eq!(
        csv_rows(&run("SELECT stored, expected, missing, dead, mismatched
        FROM moraine_index_verify('lake','main','t','by_a');")),
        vec![vec!["99900", "99900", "0", "0", "0"]]
    );
}
