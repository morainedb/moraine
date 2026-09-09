use crate::helpers::*;

/// Repeated updates preserve history and change feeds across schema changes and
/// flush.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn repeated_inline_updates_preserve_time_travel_and_change_feeds() {
    let store = TempDir::new("inline-history-store");
    let data = TempDir::new("inline-history-data");
    let reference = TempDir::new("inline-history-reference");
    let reference_data = TempDir::new("inline-history-reference-data");
    let apply = |sql: &str| {
        run_ducklake_sql(store.path(), data.path(), sql);
        run_reference_ducklake_sql(reference.path(), reference_data.path(), sql);
    };
    let probe = |sql: &str| {
        let actual = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
        let expected = csv_rows(&run_reference_ducklake_sql(
            reference.path(),
            reference_data.path(),
            sql,
        ));
        assert_eq!(actual, expected, "{sql}");
    };
    apply(
        "CREATE TABLE lake.main.t(a BIGINT, b VARCHAR); \
           INSERT INTO lake.main.t VALUES (1, 'first'); \
           UPDATE lake.main.t SET b = 'second'; \
           ALTER TABLE lake.main.t ADD COLUMN c BIGINT; \
           UPDATE lake.main.t SET b = 'third'; \
           UPDATE lake.main.t SET b = 'fourth'; \
           DELETE FROM lake.main.t;",
    );

    for flushed in [false, true] {
        if flushed {
            apply("CALL ducklake_flush_inlined_data('lake');");
        }
        for snapshot in 2..=7 {
            probe(&format!(
                "SELECT rowid, a, b FROM lake.main.t AT (VERSION => {snapshot}) ORDER BY rowid;"
            ));
            probe(&format!(
                "SELECT snapshot_id, rowid, change_type, a, b \
                 FROM ducklake_table_changes('lake', 'main', 't', {snapshot}, {snapshot}) \
                 ORDER BY snapshot_id, rowid, change_type;"
            ));
        }
        probe(
            "SELECT snapshot_id, rowid, change_type, a, b \
               FROM ducklake_table_changes('lake', 'main', 't', 0, 7) \
               ORDER BY snapshot_id, rowid, change_type;",
        );
    }
}

/// Inline lifetimes preserve versions that moved through Parquet.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn inline_history_survives_a_version_written_to_parquet() {
    let store = TempDir::new("mixed-history-store");
    let data = TempDir::new("mixed-history-data");
    let reference = TempDir::new("mixed-history-reference");
    let reference_data = TempDir::new("mixed-history-reference-data");
    let apply = |sql: &str| {
        run_ducklake_sql(store.path(), data.path(), sql);
        run_reference_ducklake_sql(reference.path(), reference_data.path(), sql);
    };
    apply(
        "CREATE TABLE lake.main.t(a BIGINT, b VARCHAR); INSERT INTO lake.main.t VALUES (1, 'first');",
    );
    apply("CALL lake.set_option('data_inlining_row_limit', 0); UPDATE lake.main.t SET b = 'file';");
    apply(
        "CALL lake.set_option('data_inlining_row_limit', 10); UPDATE lake.main.t SET b = 'inline-again';",
    );
    apply("UPDATE lake.main.t SET b = 'last';");

    for flushed in [false, true] {
        if flushed {
            apply("CALL ducklake_flush_inlined_data('lake');");
        }
        for snapshot in 2..=5 {
            let sql = format!(
                "SELECT rowid, a, b FROM lake.main.t AT (VERSION => {snapshot}) ORDER BY rowid;"
            );
            assert_eq!(
                csv_rows(&run_ducklake_sql(store.path(), data.path(), &sql)),
                csv_rows(&run_reference_ducklake_sql(
                    reference.path(),
                    reference_data.path(),
                    &sql
                )),
                "{sql} (flushed={flushed})"
            );
        }
    }
}

/// Time travel through DuckLake's `AT (VERSION => N)`: a query at a past
/// snapshot sees exactly that snapshot's data *and* schema. moraine adds
/// no time-travel logic — it serves every `ducklake_*` row (current and
/// history) row-faithfully with begin/end snapshots, and DuckLake filters
/// by version in its own SQL, reconstructing the past schema from the
/// `ducklake_column` versions moraine hands it. Each commit is one
/// snapshot: 1 = CREATE, 2 = first INSERT, 3 = ADD COLUMN, 4 = second
/// INSERT.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
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
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
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
