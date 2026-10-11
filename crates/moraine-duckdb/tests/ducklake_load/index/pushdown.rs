//! An ordinary `WHERE key = 'x'` is answered through the index: the
//! optimizer resolves the equality once and reads the resolved rows by
//! position — or, where that is not the cheaper read, pushes their ids and
//! files into the scan — with no lookup call and no join in the query.
//!
//! The fixture scatters keys across both files so DuckLake's own column
//! statistics cannot prune — every file's min/max spans every key — and
//! makes the files large enough that the probe pays for itself.

use crate::helpers::*;

/// Keys permuted over two files of incompressible rows, ~2.4 MB each: the
/// first file holds the first half of the permutation, the second the rest,
/// and both span the whole key range. `c` repeats `a` unindexed.
fn scattered_lake(store: &std::path::Path, data: &std::path::Path, options: &str) {
    run_ducklake_sql_with_options(
        store,
        data,
        options,
        "CALL ducklake_set_option('lake','parquet_row_group_size','20000');
         CREATE TABLE lake.main.t AS SELECT (i*7919)%100000 AS a, (i*7919)%100000 AS c,
             repeat(md5(i::VARCHAR),6) AS b FROM range(50000) r(i);
         INSERT INTO lake.main.t SELECT (i*7919)%100000, (i*7919)%100000,
             repeat(md5(i::VARCHAR),6) FROM range(50000,100000) r(i);",
    );
}

/// `30000` is `(70000*7919) % 100000`, so it lives in the second file.
const KEY_IN_SECOND_FILE: &str = "30000";

fn files_read(output: &str) -> Vec<&str> {
    output
        .lines()
        .filter_map(|line| line.split("Total Files Read:").nth(1))
        .map(|count| count.trim_end_matches([' ', '│']).trim())
        .collect()
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_indexed_equality_reads_only_the_rows_holding_its_value() {
    let store = TempDir::new("pushdown-store");
    let data = TempDir::new("pushdown-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    scattered_lake(store.path(), data.path(), &options);
    let query = format!("SELECT count(*) FROM lake.main.t WHERE a = {KEY_IN_SECOND_FILE};");

    // Without an index the filter prunes nothing: both files' statistics
    // admit the key, which is what makes the reading below meaningful.
    let before = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        &format!("EXPLAIN ANALYZE {query}"),
    );
    assert_eq!(files_read(&before), vec!["2"], "{before}");

    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CALL moraine_index_create('lake','main','t','by_a',['a'],false);",
    );
    let after = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        &format!("EXPLAIN ANALYZE {query}"),
    );
    assert_eq!(files_read(&after), vec!["1"], "{after}");
    // The resolved row is read by position — its pages, not its row group —
    // with the predicate evaluated over what that read returns.
    // The summary scan renders as a table scan carrying its own costing
    // line, which no other scan prints.
    assert!(
        after.contains("Coverage costing ms"),
        "the resolved row was not read by position: {after}"
    );

    // The answer is the filter's, not the index's.
    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            &format!("SELECT a, c FROM lake.main.t WHERE a = {KEY_IN_SECOND_FILE};"),
        )),
        vec![vec![KEY_IN_SECOND_FILE, KEY_IN_SECOND_FILE]]
    );
    let missing = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "EXPLAIN ANALYZE SELECT count(*) FROM lake.main.t WHERE a = 7777777;",
    );
    // A value no row holds reads no file: the scan either reports none or
    // is planned away altogether.
    assert!(
        files_read(&missing).iter().all(|count| *count == "0"),
        "a value no row holds read a file: {missing}"
    );
}

/// What the rule declines. Each reads every file — or, for a table too
/// small to be worth a probe, never asks the index at all.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn equalities_the_index_cannot_answer_cheaply_leave_the_scan_alone() {
    let store = TempDir::new("pushdown-decline-store");
    let data = TempDir::new("pushdown-decline-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    scattered_lake(store.path(), data.path(), &options);
    // Both files exist at this version; the index arrives in the next one.
    let loaded_version = csv_rows(&run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "SELECT max(snapshot_id) FROM ducklake_snapshots('lake');",
    ))
    .pop()
    .and_then(|row| row.into_iter().next())
    .expect("the lake reports a snapshot");
    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CALL moraine_index_create('lake','main','t','by_a',['a'],false);",
    );

    let declines = [
        // An unindexed column, scattered so statistics prune nothing either.
        format!("SELECT count(*) FROM lake.main.t WHERE c = {KEY_IN_SECOND_FILE};"),
        // A range, which an index answers through a different probe.
        "SELECT count(*) FROM lake.main.t WHERE a > 50000;".to_owned(),
        // A time-travelled scan is a different cut than the index resolves
        // against, even where it holds the same files.
        format!(
            "SELECT count(*) FROM lake.main.t AT (VERSION => {loaded_version}) WHERE a = {KEY_IN_SECOND_FILE};"
        ),
    ];
    for query in &declines {
        let output = run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            &format!("EXPLAIN ANALYZE {query}"),
        );
        assert_eq!(files_read(&output), vec!["2"], "{query}: {output}");
    }

    // A table below the probe's worth is scanned without asking the index:
    // the lookup count stays at what the index's own functions made.
    let small_store = TempDir::new("pushdown-small-store");
    let small_data = TempDir::new("pushdown-small-data");
    let small_options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        small_data.path().display()
    );
    let result = run_session_with_env(
        &Attach::Moraine {
            store_dir: small_store.path(),
            data_path: small_data.path(),
            options: &small_options,
            read_only: false,
        },
        "CREATE TABLE lake.main.t(a BIGINT);
         INSERT INTO lake.main.t VALUES (1),(2),(3),(4);
         CALL moraine_index_create('lake','main','t','by_a',['a'],false);
         CALL enable_logging(level => 'debug', storage => 'memory');
         SELECT count(*) FROM lake.main.t WHERE a = 2;
         SELECT 'lookups', count(*) FROM duckdb_logs WHERE type='moraine' AND message LIKE '%index lookup resolved%';",
        &[("MORAINE_LOG", "debug")],
    );
    let output = combined_output(&result);
    assert!(result.status.success(), "{output}");
    assert!(
        csv_rows(&output).contains(&vec!["lookups".into(), "0".into()]),
        "a four-row table paid for a probe: {output}"
    );
}
