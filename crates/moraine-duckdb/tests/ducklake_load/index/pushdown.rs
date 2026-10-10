//! An ordinary `WHERE key = 'x'` reads only the files an index places the
//! matching rows in.
//!
//! The filter reaches moraine while DuckLake builds its file list, so the
//! narrowing happens before any reader opens a file, with no lookup call
//! and no join in the query. The fixture scatters keys across both files so
//! that DuckLake's own column statistics cannot prune: every file's
//! min/max spans every key, and a file that is read anyway was read because
//! the index was not consulted.

use crate::helpers::*;

/// Two files, keys interleaved: file one holds the even values, file two the
/// odd ones, both spanning the whole range. `c` carries the same values as
/// `a` but goes unindexed, so a filter on it is one no index can answer
/// while DuckLake's own statistics prune no better than for `a`.
fn scattered_lake(store: &std::path::Path, data: &std::path::Path, options: &str) {
    run_ducklake_sql_with_options(
        store,
        data,
        options,
        "CREATE TABLE lake.main.t AS SELECT i*2 AS a, i*2 AS c, repeat('x',20) AS b FROM range(1000) r(i);
         INSERT INTO lake.main.t SELECT i*2+1, i*2+1, repeat('y',20) FROM range(1000) r(i);",
    );
}

fn files_read(output: &str) -> Vec<&str> {
    output
        .lines()
        .filter_map(|line| line.split("Total Files Read:").nth(1))
        .map(|count| count.trim_end_matches([' ', '│']).trim())
        .collect()
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_indexed_equality_filter_reads_only_the_files_holding_its_rows() {
    let store = TempDir::new("pushdown-store");
    let data = TempDir::new("pushdown-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    scattered_lake(store.path(), data.path(), &options);

    // Without an index the filter prunes nothing: both files' statistics
    // admit the key, which is what makes the reading below meaningful.
    let before = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "EXPLAIN ANALYZE SELECT count(*) FROM lake.main.t WHERE a = 1501;",
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
        "EXPLAIN ANALYZE SELECT count(*) FROM lake.main.t WHERE a = 1501;",
    );
    assert_eq!(files_read(&after), vec!["1"], "{after}");

    // The answer is the filter's, not the index's: pruning only drops files
    // that cannot hold a match.
    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            "SELECT a, b FROM lake.main.t WHERE a = 1501;",
        )),
        vec![vec!["1501".to_owned(), "y".repeat(20)]]
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            "SELECT count(*) FROM lake.main.t WHERE a = 7777777;",
        )),
        vec![vec!["0"]],
        "a key no file holds prunes every file"
    );
}

/// What the narrowing declines to answer. Each of these reads every file,
/// because the index cannot establish that the others hold nothing.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn filters_an_index_cannot_answer_leave_the_file_list_whole() {
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
        "SELECT count(*) FROM lake.main.t WHERE c = 1501;".to_owned(),
        // A range, which an index answers through a different probe.
        "SELECT count(*) FROM lake.main.t WHERE a > 1500;".to_owned(),
        // A time-travelled list is a different cut than the index read, even
        // where it holds the same files: this version is the one before the
        // index itself was created.
        format!(
            "SELECT count(*) FROM lake.main.t AT (VERSION => {loaded_version}) WHERE a = 1501;"
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
}
