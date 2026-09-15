use crate::helpers::*;

/// Exact indexed joins scan projected positions and retain their residual
/// predicates.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn summary_driven_scan_keeps_join_and_filters() {
    let store = TempDir::new("selective-store");
    let data = TempDir::new("selective-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let query = "SELECT sum(data.b) FROM lake.main.t data
        JOIN moraine_index_in('lake','main','t','by_a',[1,3,7]) hits
        ON data.rowid = hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id
        WHERE data.b % 2 = 1 AND data.b > 10";
    let result = run_ducklake_sql_with_options(store.path(), data.path(), &options, &format!(
        "CREATE TABLE lake.main.t AS SELECT i % 10 a, i b, repeat('wide',1000) unused FROM range(10000) r(i);
         CALL moraine_index_create('lake','main','t','by_a',['a'],false);
         EXPLAIN {query}; {query};"));
    assert!(
        result.contains("MORAINE_SUMMARY_SCAN"),
        "no selective scan: {result}"
    );
    assert!(
        result.contains("HASH_JOIN"),
        "the join was removed: {result}"
    );
    assert!(
        result.lines().any(|line| line == "14995989"),
        "wrong residual result: {result}"
    );
}

/// Prepared selective scans follow commits and project evolved schemas on files
/// and inline chunks.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn summary_driven_scan_handles_visibility_and_schema_changes() {
    for inline in [0, 1000] {
        let store = TempDir::new("selective-visibility-store");
        let data = TempDir::new("selective-visibility-data");
        let options = format!(
            ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {inline}",
            data.path().display()
        );
        let result = run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            "CREATE TABLE lake.main.t(a INTEGER, b INTEGER);
             INSERT INTO lake.main.t VALUES (1,10),(1,20),(2,30);
             CALL moraine_index_create('lake','main','t','by_a',['a'],false);
             ALTER TABLE lake.main.t ADD COLUMN c BIGINT DEFAULT 7;
             ALTER TABLE lake.main.t RENAME COLUMN b TO renamed;
             PREPARE p AS SELECT 'answer', sum(data.renamed + data.c) FROM lake.main.t data
             JOIN moraine_index_lookup('lake','main','t','by_a',1) hits
             ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id;
             EXECUTE p;
             DELETE FROM lake.main.t WHERE renamed=10;
             EXECUTE p;
             INSERT INTO lake.main.t VALUES(1,40,8);
             EXECUTE p;
             DEALLOCATE p;",
        );
        let answers: Vec<_> = csv_rows(&result)
            .into_iter()
            .filter(|row| row[0] == "answer")
            .collect();
        assert_eq!(
            answers,
            vec![
                vec!["answer", "44"],
                vec!["answer", "27"],
                vec!["answer", "75"]
            ],
            "inline={inline}: {result}"
        );
    }
}

/// Joins missing location identity and unsupported projections keep DuckLake's
/// scan.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn summary_driven_scan_declines_unsafe_rewrites() {
    let store = TempDir::new("selective-fallback-store");
    let data = TempDir::new("selective-fallback-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let result = run_ducklake_sql_with_options(store.path(), data.path(), &options,
        "CREATE TABLE lake.main.t AS SELECT 1::BIGINT a, 2::DECIMAL(10,2) b;
         CREATE TABLE lake.main.other AS SELECT 1::BIGINT a;
         CALL moraine_index_create('lake','main','t','by_a',['a'],true);
         EXPLAIN SELECT data.a FROM lake.main.t data JOIN moraine_index_lookup('lake','main','t','by_a',1) hits ON data.rowid=hits.row_id;
         EXPLAIN SELECT data.a FROM lake.main.other data JOIN moraine_index_lookup('lake','main','t','by_a',1) hits
         ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id;
         EXPLAIN SELECT data.b FROM lake.main.t data JOIN moraine_index_lookup('lake','main','t','by_a',1) hits
         ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id;");
    assert!(
        !result.contains("MORAINE_SUMMARY_SCAN"),
        "unsafe replacement: {result}"
    );
}

/// PREPARE opens no selective cursor; each execution opens one projected
/// reader.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn summary_scan_prepare_defers_projected_reads() {
    let store = TempDir::new("selective-prepare-store");
    let data = TempDir::new("selective-prepare-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let output = run_session_with_env(&Attach::Moraine { store_dir: store.path(), data_path: data.path(), options: &options, read_only: false },
        "CREATE TABLE lake.main.t AS SELECT i a, i * 2 b FROM range(1000) r(i);
         CALL moraine_index_create('lake','main','t','by_a',['a'],true);
         CALL enable_logging(level => 'debug', storage => 'memory');
         PREPARE p AS SELECT data.b FROM lake.main.t data JOIN moraine_index_lookup('lake','main','t','by_a',2) hits
         ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id;
         SELECT 'opened', count(*) FROM duckdb_logs WHERE type='moraine' AND message LIKE '%summary scan opened%';
         EXECUTE p;
         SELECT 'opened', count(*) FROM duckdb_logs WHERE type='moraine' AND message LIKE '%summary scan opened%columns=1';
         EXECUTE p;
         SELECT 'opened', count(*) FROM duckdb_logs WHERE type='moraine' AND message LIKE '%summary scan opened%columns=1';
         DEALLOCATE p;", &[("MORAINE_LOG", "debug")]);
    let result = combined_output(&output);
    assert!(output.status.success(), "{result}");
    let counts: Vec<_> = csv_rows(&result)
        .into_iter()
        .filter(|row| row[0] == "opened")
        .collect();
    assert_eq!(
        counts,
        vec![
            vec!["opened", "0"],
            vec!["opened", "1"],
            vec!["opened", "2"]
        ],
        "{result}"
    );
}
