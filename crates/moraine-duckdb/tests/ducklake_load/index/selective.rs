use std::fmt::Write as _;

use crate::helpers::*;

/// Scan selection is automatic and prepared reads need no public on/off
/// control.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn summary_scan_is_automatic_without_a_public_switch() {
    let store = TempDir::new("selective-control-store");
    let data = TempDir::new("selective-control-data");
    let query = "SELECT sum(data.b) FROM lake.main.t data
        JOIN moraine_index_in('lake','main','t','by_a',[1,3,7]) hits
        ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id";
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let result = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        &format!(
            "CREATE TABLE lake.main.t AS SELECT i a, i * 2 b FROM range(10000) r(i);
         CALL moraine_index_create('lake','main','t','by_a',['a'],true);
         SELECT 'switches',count(*) FROM duckdb_settings() WHERE name='moraine_summary_scan';
         EXPLAIN {query}; {query};
         SELECT sum(b) FROM lake.main.t WHERE a IN (1,3,7);
         PREPARE p AS {query}; EXECUTE p; EXECUTE p;"
        ),
    );
    assert!(result.lines().any(|line| line == "switches,0"), "{result}");
    assert_eq!(
        result.matches("MORAINE_SUMMARY_SCAN").count(),
        2,
        "{result}"
    );
    assert_eq!(
        result.lines().filter(|line| *line == "22").count(),
        4,
        "{result}"
    );
}

/// Sparse rows spread over almost every row group keep the ordinary reader.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn summary_scan_uses_physical_coverage() {
    let store = TempDir::new("selective-coverage-store");
    let data = TempDir::new("selective-coverage-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let result = run_ducklake_sql_with_options(store.path(), data.path(), &options,
        "CALL ducklake_set_option('lake','parquet_row_group_size','2048');
         CREATE TABLE lake.main.t AS SELECT i a, md5(i::VARCHAR) payload FROM range(262144) r(i);
         CALL moraine_index_create('lake','main','t','by_a',['a'],true);
         EXPLAIN SELECT sum(length(data.payload)) FROM lake.main.t data
         JOIN moraine_index_in('lake','main','t','by_a',list_transform(range(128),x -> x * 2048)) hits
         ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id;");
    assert!(!result.contains("MORAINE_SUMMARY_SCAN"), "{result}");
}

/// Parallel cursors preserve nullable strings and early-stop ownership.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn summary_scan_parallel_files_match_serial_and_ordinary() {
    let store = TempDir::new("selective-parallel-store");
    let data = TempDir::new("selective-parallel-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let query = "SELECT 'answer',count(*),sum(length(data.payload)) FROM lake.main.t data
        JOIN moraine_index_lookup('lake','main','t','by_a',0) hits
        ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id";
    let mut sql = "CALL ducklake_set_option('lake','parquet_row_group_size','2048');
        CREATE TABLE lake.main.t(a BIGINT, payload VARCHAR);"
        .to_owned();
    for _ in 0..4 {
        sql.push_str("INSERT INTO lake.main.t SELECT i // 1024, CASE WHEN i % 2=0 THEN repeat('x',100) END FROM range(65536) r(i);");
    }
    sql.push_str("CALL moraine_index_create('lake','main','t','by_a',['a'],false); SET threads=4;");
    sql.push_str("SELECT 'answer',count(*),sum(length(payload)) FROM lake.main.t WHERE a=0;");
    for setting in [
        "SET moraine_summary_scan_threads=1",
        "SET moraine_summary_scan_threads=2",
    ] {
        write!(sql, "{setting}; EXPLAIN ANALYZE {query}; {query};").unwrap();
    }
    for _ in 0..8 {
        sql.push_str("SELECT data.payload FROM lake.main.t data JOIN moraine_index_lookup('lake','main','t','by_a',0) hits
            ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id LIMIT 1;");
    }
    let result = run_ducklake_sql_with_options(store.path(), data.path(), &options, &sql);
    let answers: Vec<_> = csv_rows(&result)
        .into_iter()
        .filter(|row| row[0] == "answer")
        .collect();
    assert_eq!(
        answers,
        vec![vec!["answer", "4096", "204800"]; 3],
        "{result}"
    );
    assert!(result.contains("Peak read workers"), "{result}");
}

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

/// A transaction with staged writes keeps the selective path, and its own
/// deletes vanish from it: pending delete files, inlined file deletions and
/// inlined-data deletions alike.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn summary_scan_runs_inside_a_writing_transaction_minus_its_deletes() {
    let query = "SELECT 'sum', sum(data.b) FROM lake.main.t data
        JOIN moraine_index_in('lake','main','t','by_a',[1,3,7]) hits
        ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id";
    for (limit, table) in [
        (
            0,
            "CREATE TABLE lake.main.t AS SELECT i a, i * 2 b FROM range(10000) r(i);",
        ),
        (
            10,
            "CREATE TABLE lake.main.t AS SELECT i a, i * 2 b FROM range(10000) r(i);",
        ),
        (
            10,
            "CREATE TABLE lake.main.t(a BIGINT, b BIGINT); INSERT INTO lake.main.t VALUES (1,2),(3,6),(7,14);",
        ),
    ] {
        let store = TempDir::new("selective-writing-tx-store");
        let data = TempDir::new("selective-writing-tx-data");
        let options = format!(
            ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {limit}",
            data.path().display()
        );
        let result = run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            &format!(
                "{table}
                 CALL moraine_index_create('lake','main','t','by_a',['a'],true);
                 BEGIN;
                 INSERT INTO lake.main.t VALUES (20000, 1);
                 EXPLAIN {query}; {query};
                 DELETE FROM lake.main.t WHERE a = 3;
                 EXPLAIN {query}; {query};
                 DELETE FROM lake.main.t WHERE a = 7;
                 EXPLAIN {query}; {query};
                 COMMIT;
                 {query};"
            ),
        );
        assert_eq!(
            result.matches("MORAINE_SUMMARY_SCAN").count(),
            6,
            "limit={limit}: {result}"
        );
        let sums: Vec<_> = csv_rows(&result)
            .into_iter()
            .filter(|row| row[0] == "sum")
            .map(|row| row[1].clone())
            .collect();
        assert_eq!(sums, vec!["22", "16", "2", "2"], "limit={limit}: {result}");
    }
}

/// A `UUID` column the query filters and projects keeps the selective path.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn summary_scan_reads_uuid_columns() {
    let store = TempDir::new("selective-uuid-store");
    let data = TempDir::new("selective-uuid-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let owner = "11111111-1111-1111-1111-111111111111";
    let query = format!(
        "SELECT 'rows', count(*), min(data.owner_id::VARCHAR) FROM lake.main.t data
         JOIN moraine_index_in('lake','main','t','by_a',[1,3,7]) hits
         ON data.rowid = hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id
         WHERE data.owner_id = '{owner}'::UUID"
    );
    let result = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        &format!(
            "CREATE TABLE lake.main.t AS
               SELECT i a, '{owner}'::UUID owner_id FROM range(10000) r(i);
             CALL moraine_index_create('lake','main','t','by_a',['a'],true);
             EXPLAIN {query}; {query};"
        ),
    );
    assert!(
        result.contains("MORAINE_SUMMARY_SCAN"),
        "no selective scan: {result}"
    );
    assert!(
        result.lines().any(|line| line == format!("rows,3,{owner}")),
        "wrong rows: {result}"
    );
}
