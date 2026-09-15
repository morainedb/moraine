use crate::helpers::*;

/// Paired locations above 256 rows use the exact scan; row-only joins retain
/// pruning.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn a_wide_sparse_probe_derives_row_pruning() {
    let store = TempDir::new("wide-row-pruning-store");
    let data = TempDir::new("wide-row-pruning-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let query = "SELECT count(*) FROM lake.main.t data
        JOIN moraine_index_in('lake','main','t','by_a',list_concat(range(96),range(1000,1096))) hits
        ON data.rowid IS NOT DISTINCT FROM hits.row_id
        AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id";
    let result = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        &format!(
            "CREATE TABLE lake.main.t AS SELECT i // 17 AS a FROM range(32768) t(i);
         CALL moraine_index_create('lake','main','t','by_a',['a'],false);
         EXPLAIN {query}; {query};"
        ),
    );
    assert!(
        result.contains("MORAINE_SUMMARY_SCAN"),
        "the wide probe lost its exact scan: {result}"
    );
    assert!(
        !result.contains("rowid IN"),
        "the pruning list became a per-row residual: {result}"
    );
    assert!(
        result.lines().any(|line| line == "3264"),
        "the probe result changed: {result}"
    );

    // The derived pruning list describes the unfiltered index read, not
    // the rows surviving this additional predicate on its output.
    let filtered = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "SELECT count(*) FROM lake.main.t data
         JOIN (SELECT * FROM moraine_index_in('lake','main','t','by_a',
               list_concat(range(96),range(1000,1096))) WHERE row_id % 2 = 0) hits
         ON data.rowid IS NOT DISTINCT FROM hits.row_id
         AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id;",
    );
    assert_eq!(csv_rows(&filtered), vec![vec!["1632"]]);

    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.boundary AS SELECT i AS a FROM range(32768) t(i);
         CALL moraine_index_create('lake','main','boundary','by_a',['a'],true);",
    );
    for (end, expected) in [(12048, "4096"), (12049, "4097")] {
        let query = format!("SELECT count(*) FROM lake.main.boundary data
            JOIN moraine_index_in('lake','main','boundary','by_a',list_concat(range(2048),range(10000,{end}))) hits
            ON data.rowid IS NOT DISTINCT FROM hits.row_id
            AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id");
        let result = run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            &format!("EXPLAIN {query}; {query};"),
        );
        assert!(
            result.contains("MORAINE_SUMMARY_SCAN"),
            "cap boundary {expected}: {result}"
        );
        assert!(
            result.lines().any(|line| line == expected),
            "cap boundary changed results: {result}"
        );
    }

    let sparse = run_ducklake_sql_with_options(
        store.path(), data.path(), &options,
        "EXPLAIN SELECT count(*) FROM lake.main.boundary data
         JOIN moraine_index_in('lake','main','boundary','by_a',list_transform(range(4096), x -> x * 7)) hits
         ON data.rowid IS NOT DISTINCT FROM hits.row_id;
         SELECT count(*) FROM lake.main.boundary data
         JOIN moraine_index_in('lake','main','boundary','by_a',list_transform(range(4096), x -> x * 7)) hits
         ON data.rowid IS NOT DISTINCT FROM hits.row_id;",
    );
    assert!(sparse.contains("optional: rowid>="));
    assert!(
        sparse.matches("rowid>=").count() <= 16,
        "too many metadata predicates: {sparse}"
    );
    assert!(
        sparse.lines().any(|line| line == "4096"),
        "coalesced gaps changed the result: {sparse}"
    );
}

/// Embedded row IDs resolve exact rows rather than whole row-group candidates.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn a_wide_probe_skips_embedded_row_groups() {
    let store = TempDir::new("wide-embedded-store");
    let data = TempDir::new("wide-embedded-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 65536",
        data.path().display()
    );
    let result = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CALL ducklake_set_option('lake','parquet_row_group_size','2048');
         CREATE TABLE lake.main.t AS SELECT i // 17 AS a FROM range(32768) t(i);
         CALL ducklake_flush_inlined_data('lake');
         CALL moraine_index_create('lake','main','t','by_a',['a'],false);
         EXPLAIN ANALYZE SELECT count(*) FROM lake.main.t data
         JOIN moraine_index_in('lake','main','t','by_a',range(192)) hits
         ON data.rowid IS NOT DISTINCT FROM hits.row_id
         AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id;",
    );
    assert!(
        result.contains("MORAINE_SUMMARY_SCAN") && !result.contains("4,096 rows"),
        "the scan did not use exact positions: {result}"
    );
    assert!(
        result.contains("3,264 rows"),
        "the exact join result changed: {result}"
    );
    assert!(
        !result.contains("32,768 rows"),
        "the scan decoded the whole file: {result}"
    );
}
