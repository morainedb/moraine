use crate::helpers::*;

/// Time travel through DuckLake's `AT (VERSION => N)`: a query at a past
/// snapshot sees exactly that snapshot's data *and* schema. moraine adds
/// no time-travel logic — it serves every `ducklake_*` row (current and
/// history) row-faithfully with begin/end snapshots, and DuckLake filters
/// by version in its own SQL, reconstructing the past schema from the
/// `ducklake_column` versions moraine hands it. Each commit is one
/// snapshot: 1 = CREATE, 2 = first INSERT, 3 = ADD COLUMN, 4 = second
/// INSERT.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_time_travel_reads_past_data_and_schema() {
    let dir = TempDir::new("tt-store");
    let data_dir = TempDir::new("tt-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a BIGINT);");
    run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (1);");
    run_ducklake_sql(
        store,
        data_path,
        "ALTER TABLE lake.main.t ADD COLUMN b VARCHAR;",
    );
    run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (2, 'x');");

    // Present: both columns, both rows.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT * FROM lake.main.t ORDER BY a;"
        )),
        vec![vec!["1", "NULL"], vec!["2", "x"]]
    );
    // At v2 (after the first insert, before ADD COLUMN): schema is just
    // `a`, and only the first row exists.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT column_name FROM (DESCRIBE SELECT * FROM lake.main.t AT (VERSION => 2));",
        )),
        vec![vec!["a"]]
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT * FROM lake.main.t AT (VERSION => 2) ORDER BY a;",
        )),
        vec![vec!["1"]]
    );
    // At v3 (after ADD COLUMN, before the second insert): both columns,
    // the pre-existing row back-filled with a NULL `b`.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT * FROM lake.main.t AT (VERSION => 3) ORDER BY a;",
        )),
        vec![vec!["1", "NULL"]]
    );
}

/// Time travel survives flush: rows inlined before a flush read back at a
/// pre-flush version from the **backdated** Parquet file DuckLake writes
/// (its `ducklake_data_file` record carries the minimum per-row snapshot),
/// so a past-snapshot scan is served the Parquet with a per-row filter —
/// never double-counted, never lost.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_time_travel_survives_flush() {
    let dir = TempDir::new("ttf-store");
    let data_dir = TempDir::new("ttf-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a BIGINT);");
    run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (10);"); // v2
    run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (20);"); // v3
    run_ducklake_sql(
        store,
        data_path,
        "CALL ducklake_flush_inlined_data('lake');",
    );

    // Present: both rows, now served from Parquet.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT a FROM lake.main.t ORDER BY a;"
        )),
        vec![vec!["10"], vec!["20"]]
    );
    // Pre-flush versions still read the right subset, from the backdated file.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT a FROM lake.main.t AT (VERSION => 2) ORDER BY a;",
        )),
        vec![vec!["10"]]
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT a FROM lake.main.t AT (VERSION => 3) ORDER BY a;",
        )),
        vec![vec!["10"], vec!["20"]]
    );
}
