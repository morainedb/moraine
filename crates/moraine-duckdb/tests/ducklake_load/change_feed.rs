use crate::helpers::*;

/// The change data feed over inlined (unflushed) data, differential
/// against a stock DuckLake catalog fed the identical statements:
/// insert attribution to the minting snapshot with stable rowids,
/// inline deletes, `UPDATE` pairing `update_preimage`/`update_postimage`
/// on one rowid, sub-range and full-range queries, agreement of
/// `ducklake_table_insertions`/`_deletions` with `ducklake_table_changes`,
/// and the `TIMESTAMPTZ` bound form agreeing with the version form.
/// moraine adds no feed logic — DuckLake computes everything from the
/// served projections; the reference catalog is the semantic oracle.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
#[allow(clippy::too_many_lines)]
fn ducklake_change_feed_attributes_inline_changes() {
    let store = TempDir::new("cdf-store");
    let data = TempDir::new("cdf-data");
    let reference_meta = TempDir::new("cdf-ref-meta");
    let reference_data = TempDir::new("cdf-ref-data");

    // Runs the same statements on both catalogs, discarding output.
    let apply = |sql: &str| {
        run_ducklake_sql(store.path(), data.path(), sql);
        run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
    };
    // Runs the same probe on both catalogs, asserts moraine matches
    // the reference, and returns the shared rows.
    let probe = |sql: &str| -> Vec<Vec<String>> {
        let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
        let reference_rows = csv_rows(&run_reference_ducklake_sql(
            reference_meta.path(),
            reference_data.path(),
            sql,
        ));
        assert_eq!(
            moraine_rows, reference_rows,
            "moraine diverges from stock DuckLake for `{sql}`"
        );
        moraine_rows
    };

    // One snapshot per autocommitted statement: 1 create, 2 insert
    // rowids 0-1, 3 insert rowid 2, 4 delete rowid 1, 5 update rowid 0.
    apply(
        "CREATE TABLE lake.main.t (i BIGINT, v VARCHAR);\
         INSERT INTO lake.main.t VALUES (1, 'a'), (2, 'b');\
         INSERT INTO lake.main.t VALUES (3, 'c');\
         DELETE FROM lake.main.t WHERE i = 2;\
         UPDATE lake.main.t SET v = 'z' WHERE i = 1;",
    );

    let changes = |start: &str, end: &str| {
        format!(
            "SELECT snapshot_id, rowid, change_type, i, v \
             FROM ducklake_table_changes('lake', 'main', 't', {start}, {end}) \
             ORDER BY snapshot_id, rowid, change_type;"
        )
    };

    // Insert-only sub-range: each row attributed to its minting
    // snapshot, rowids stable.
    assert_eq!(
        probe(&changes("2", "3")),
        vec![
            vec!["2", "0", "insert", "1", "a"],
            vec!["2", "1", "insert", "2", "b"],
            vec!["3", "2", "insert", "3", "c"],
        ]
    );

    // Delete + update sub-range: the delete carries the deleted row's
    // values; the update pairs pre/postimage on one rowid in one
    // snapshot.
    assert_eq!(
        probe(&changes("4", "5")),
        vec![
            vec!["4", "1", "delete", "2", "b"],
            vec!["5", "0", "update_postimage", "1", "z"],
            vec!["5", "0", "update_preimage", "1", "a"],
        ]
    );

    // Full range: the update pair coexists with rowid 0's original
    // insert — separate events, not merged.
    let full_range = probe(&changes("0", "5"));
    assert_eq!(
        full_range,
        vec![
            vec!["2", "0", "insert", "1", "a"],
            vec!["2", "1", "insert", "2", "b"],
            vec!["3", "2", "insert", "3", "c"],
            vec!["4", "1", "delete", "2", "b"],
            vec!["5", "0", "update_postimage", "1", "z"],
            vec!["5", "0", "update_preimage", "1", "a"],
        ]
    );

    // The convenience functions are exactly the corresponding subsets.
    assert_eq!(
        probe(
            "SELECT snapshot_id, rowid, i, v \
             FROM ducklake_table_insertions('lake', 'main', 't', 2, 3) \
             ORDER BY snapshot_id, rowid;"
        ),
        vec![
            vec!["2", "0", "1", "a"],
            vec!["2", "1", "2", "b"],
            vec!["3", "2", "3", "c"],
        ]
    );
    assert_eq!(
        probe(
            "SELECT snapshot_id, rowid, i, v \
             FROM ducklake_table_deletions('lake', 'main', 't', 4, 4) \
             ORDER BY snapshot_id, rowid;"
        ),
        vec![vec!["4", "1", "2", "b"]]
    );

    // Timestamp bounds resolve through each catalog's own served
    // snapshot times (wall clocks differ, so this is per catalog, not
    // differential): the full time window equals the full version
    // range. Inclusive bounds make the window unambiguous even if
    // neighboring snapshots share a timestamp.
    for run in [
        &(|sql: &str| run_ducklake_sql(store.path(), data.path(), sql)) as &dyn Fn(&str) -> String,
        &(|sql: &str| {
            run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql)
        }),
    ] {
        let window = csv_rows(&run("SET TimeZone='UTC'; \
             SELECT min(snapshot_time)::VARCHAR, max(snapshot_time)::VARCHAR \
             FROM ducklake_snapshots('lake');"));
        let by_time = csv_rows(&run(&format!(
            "SET TimeZone='UTC'; \
             SELECT snapshot_id, rowid, change_type, i, v \
             FROM ducklake_table_changes('lake', 'main', 't', \
             TIMESTAMPTZ '{}', TIMESTAMPTZ '{}') \
             ORDER BY snapshot_id, rowid, change_type;",
            window[0][0], window[0][1]
        )));
        assert_eq!(by_time, full_range);
    }
}

/// The change data feed across `ducklake_flush_inlined_data` and over
/// file-based deletes, differential against a stock DuckLake catalog:
/// a range spanning the flush answers identically before and after it
/// (flushed files are backdated; `partial_max` filters per row), the
/// flush snapshot itself contributes nothing, and post-flush the three
/// delete flavors — delete file, a later delete superseding it (the
/// previous-delete subtraction), and update pairing — attribute
/// correctly.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
#[allow(clippy::too_many_lines)]
fn ducklake_change_feed_survives_flush_and_file_deletes() {
    let store = TempDir::new("cdf-flush-store");
    let data = TempDir::new("cdf-flush-data");
    let reference_meta = TempDir::new("cdf-flush-ref-meta");
    let reference_data = TempDir::new("cdf-flush-ref-data");

    let apply = |sql: &str| {
        run_ducklake_sql(store.path(), data.path(), sql);
        run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
    };
    let probe = |sql: &str| -> Vec<Vec<String>> {
        let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
        let reference_rows = csv_rows(&run_reference_ducklake_sql(
            reference_meta.path(),
            reference_data.path(),
            sql,
        ));
        assert_eq!(
            moraine_rows, reference_rows,
            "moraine diverges from stock DuckLake for `{sql}`"
        );
        moraine_rows
    };
    let changes = |start: &str, end: &str| {
        format!(
            "SELECT snapshot_id, rowid, change_type, i, v \
             FROM ducklake_table_changes('lake', 'main', 't', {start}, {end}) \
             ORDER BY snapshot_id, rowid, change_type;"
        )
    };

    // 1 create, 2 insert rowids 0-1, 3 insert rowids 2-3, 4 delete
    // rowid 1 — all inlined.
    apply(
        "CREATE TABLE lake.main.t (i BIGINT, v VARCHAR);\
         INSERT INTO lake.main.t VALUES (1, 'a'), (2, 'b');\
         INSERT INTO lake.main.t VALUES (3, 'c'), (4, 'd');\
         DELETE FROM lake.main.t WHERE i = 2;",
    );

    // Owned so it can be compared against post-flush output and
    // extended below; the literal pins the expected content.
    let pre_flush_history = probe(&changes("1", "4"));
    assert_eq!(
        pre_flush_history,
        vec![
            vec!["2", "0", "insert", "1", "a"],
            vec!["2", "1", "insert", "2", "b"],
            vec!["3", "2", "insert", "3", "c"],
            vec!["3", "3", "insert", "4", "d"],
            vec!["4", "1", "delete", "2", "b"],
        ]
    );
    let pre_flush_middle = probe(&changes("3", "3"));
    assert_eq!(
        pre_flush_middle,
        vec![
            vec!["3", "2", "insert", "3", "c"],
            vec!["3", "3", "insert", "4", "d"],
        ]
    );

    // Flush mints snapshot 5; the same ranges must answer identically
    // (files backdated, per-row filtering through partial_max), and
    // the flush snapshot contributes no rows.
    apply("CALL ducklake_flush_inlined_data('lake');");
    assert_eq!(probe(&changes("1", "4")), pre_flush_history);
    assert_eq!(probe(&changes("3", "3")), pre_flush_middle);
    assert_eq!(probe(&changes("5", "5")), Vec::<Vec<String>>::new());

    // Post-flush file operations: 6 update rowid 2, 7 delete rowid 3,
    // 8 delete the remaining rowids 0 and 2. Snapshot 8's delete state
    // supersedes 7's on the same data file, so [8, 8] must not
    // re-report rowid 3.
    apply(
        "UPDATE lake.main.t SET v = 'z' WHERE i = 3;\
         DELETE FROM lake.main.t WHERE i = 4;\
         DELETE FROM lake.main.t;",
    );

    assert_eq!(
        probe(&changes("6", "6")),
        vec![
            vec!["6", "2", "update_postimage", "3", "z"],
            vec!["6", "2", "update_preimage", "3", "c"],
        ]
    );
    assert_eq!(
        probe(&changes("7", "7")),
        vec![vec!["7", "3", "delete", "4", "d"]]
    );
    assert_eq!(
        probe(&changes("8", "8")),
        vec![
            vec!["8", "0", "delete", "1", "a"],
            vec!["8", "2", "delete", "3", "z"],
        ]
    );

    // Full history stays coherent across the flush boundary, and the
    // deletions function agrees with the feed's delete subset.
    let mut full = pre_flush_history.clone();
    full.extend(probe(&changes("6", "8")));
    assert_eq!(probe(&changes("1", "8")), full);
    assert_eq!(
        probe(
            "SELECT snapshot_id, rowid, i, v \
             FROM ducklake_table_deletions('lake', 'main', 't', 6, 8) \
             ORDER BY snapshot_id, rowid;"
        ),
        vec![
            vec!["6", "2", "3", "c"],
            vec!["7", "3", "4", "d"],
            vec!["8", "0", "1", "a"],
            vec!["8", "2", "3", "z"],
        ]
    );
}
