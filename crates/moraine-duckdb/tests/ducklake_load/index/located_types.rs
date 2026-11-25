//! The located scan reads every column type DuckLake writes, so an indexed
//! equality on a wide table plans by position rather than falling back to
//! DuckLake's scan over the whole file list.

use crate::helpers::*;

/// A table whose indexed key `k` sits beside a column of every type the
/// located reader must carry, written as two files so the lookup has a
/// file to skip, and padded past the size below which no index is probed.
fn typed_table(run: &dyn Fn(&str) -> String) {
    run("CREATE TABLE lake.main.t AS SELECT
             i::BIGINT AS k,
             repeat(md5(i::VARCHAR), 4) AS pad,
             (i * 1.5)::DECIMAL(18,3) AS price,
             (i * 100000000000)::DECIMAL(38,6) AS wide,
             i::HUGEINT * 1000000000000000000 AS huge,
             i::UHUGEINT AS uhuge,
             (TIME '08:00:00' + INTERVAL (i) SECOND)::TIME AS at_time,
             INTERVAL (i) MINUTE AS span,
             {'a': i}::VARIANT AS var,
             ('x' || i::VARCHAR)::BLOB AS raw,
             uuid() AS id,
             [i, i + 1, i + 2] AS trio,
             {'x': i::INTEGER, 'y': 'v' || i::VARCHAR} AS point,
             MAP {'count': i::INTEGER} AS tally,
             i::FLOAT AS ratio,
             (DATE '2024-01-01' + INTERVAL (i) DAY)::DATE AS day,
             TIMESTAMP '2024-01-01' + INTERVAL (i) SECOND AS at_ts,
             TIMESTAMPTZ '2024-01-01 00:00:00+00' + INTERVAL (i) SECOND AS at_tz,
             (i % 30000)::SMALLINT AS small,
             i % 2 = 0 AS flag,
             i::DOUBLE AS dbl
         FROM range(0, 50000) r(i);
         INSERT INTO lake.main.t SELECT
             i::BIGINT, repeat(md5(i::VARCHAR), 4), (i * 1.5)::DECIMAL(18,3), (i * 100000000000)::DECIMAL(38,6),
             i::HUGEINT * 1000000000000000000, i::UHUGEINT,
             (TIME '08:00:00' + INTERVAL (i) SECOND)::TIME, INTERVAL (i) MINUTE, {'a': i}::VARIANT,
             ('x' || i::VARCHAR)::BLOB,
             uuid(), [i, i + 1, i + 2], {'x': i::INTEGER, 'y': 'v' || i::VARCHAR},
             MAP {'count': i::INTEGER}, i::FLOAT,
             (DATE '2024-01-01' + INTERVAL (i) DAY)::DATE,
             TIMESTAMP '2024-01-01' + INTERVAL (i) SECOND,
             TIMESTAMPTZ '2024-01-01 00:00:00+00' + INTERVAL (i) SECOND,
             (i % 30000)::SMALLINT, i % 2 = 0, i::DOUBLE
         FROM range(50000, 100000) r(i);");
}

/// The rows an unindexed scan returns for a key, as the located scan must
/// return them. `id` is random per load; `span` and `var` are the columns
/// the located reader declines.
const LOOKUP: &str = "SELECT * EXCLUDE (id, span, var) FROM lake.main.t WHERE k = 7777;";

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_indexed_lookup_locates_rows_of_every_column_type() {
    let store = TempDir::new("located-types-store");
    let data = TempDir::new("located-types-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);
    typed_table(&run);
    let expected = csv_rows(&run(LOOKUP));
    assert_eq!(expected.len(), 1, "the key names one row: {expected:?}");

    run("CREATE UNIQUE INDEX by_k ON lake.main.t(k);");
    let plan = run(&format!("EXPLAIN ANALYZE {LOOKUP}"));
    assert!(
        plan.contains("Coverage costing ms"),
        "a wide table's lookup was not read by position: {plan}"
    );
    assert!(
        plan.lines()
            .any(|line| line.contains("Total Files Read: 1")),
        "the located read opened more than the row's file: {plan}"
    );
    assert_eq!(
        csv_rows(&run(LOOKUP)),
        expected,
        "the located scan must return exactly what the scan returned"
    );
}

/// An INTERVAL cannot be decoded from Parquet without the writer's Arrow
/// schema, and a VARIANT comes back as its shredded struct, so a lookup
/// projecting either keeps DuckLake's scan and still answers.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_lookup_projecting_an_undecodable_type_keeps_the_scan() {
    let store = TempDir::new("located-undecodable-store");
    let data = TempDir::new("located-undecodable-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);
    typed_table(&run);
    let queries = [
        "SELECT k, span FROM lake.main.t WHERE k = 7777;",
        "SELECT k, var FROM lake.main.t WHERE k = 7777;",
    ];
    let expected: Vec<_> = queries.iter().map(|query| csv_rows(&run(query))).collect();
    run("CREATE UNIQUE INDEX by_k ON lake.main.t(k);");

    for (query, expected) in queries.iter().zip(expected) {
        let plan = run(&format!("EXPLAIN ANALYZE {query}"));
        assert!(
            !plan.contains("Coverage costing ms"),
            "`{query}` must keep DuckLake's scan: {plan}"
        );
        assert_eq!(csv_rows(&run(query)), expected, "{query}");
    }
}
