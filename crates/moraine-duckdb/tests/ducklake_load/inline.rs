use crate::helpers::*;

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_inline_data_round_trip_through_flush() {
    let dir = TempDir::new("inline-store");
    let data_dir = TempDir::new("inline-data");
    // No fixture seed: bootstrap alone (an empty attach mints `main`)
    // is enough for a CREATE TABLE; row inlining is on at DuckLake's
    // compiled default of ten rows, so these small inserts inline.
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.t (i BIGINT, s VARCHAR, d DOUBLE, b BOOLEAN);",
    );
    run_ducklake_sql(
        store,
        data_path,
        "INSERT INTO lake.main.t VALUES (1, 'a', 1.5, true), (2, NULL, NULL, false), \
         (3, 'c', 3.25, NULL);",
    );
    // A second statement is a second chunk: proves multi-chunk decode.
    run_ducklake_sql(
        store,
        data_path,
        "INSERT INTO lake.main.t VALUES (4, 'd', 4.5, true), (5, 'e', 5.5, false);",
    );

    let select = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "SELECT * FROM lake.main.t ORDER BY i;",
    ));
    assert_eq!(
        select,
        vec![
            vec!["1", "a", "1.5", "true"],
            vec!["2", "NULL", "NULL", "false"],
            vec!["3", "c", "3.25", "NULL"],
            vec!["4", "d", "4.5", "true"],
            vec!["5", "e", "5.5", "false"],
        ]
    );
    // Every inlined row is served through the dynamic
    // `ducklake_inlined_data_<t>_<v>` entry, not a real materialized
    // table: no Parquet file is registered yet.
    let pre_flush_files = csv_rows(&run_standalone_sql(
        store,
        "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
    ));
    assert_eq!(pre_flush_files, vec![vec!["0".to_string()]]);

    run_ducklake_sql(store, data_path, "DELETE FROM lake.main.t WHERE i = 3;");
    let after_delete = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "SELECT i FROM lake.main.t ORDER BY i;",
    ));
    assert_eq!(
        after_delete,
        vec![vec!["1"], vec!["2"], vec!["4"], vec!["5"]]
    );

    run_ducklake_sql(
        store,
        data_path,
        "CALL ducklake_flush_inlined_data('lake');",
    );
    let after_flush = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "SELECT * FROM lake.main.t ORDER BY i;",
    ));
    assert_eq!(
        after_flush,
        vec![
            vec!["1", "a", "1.5", "true"],
            vec!["2", "NULL", "NULL", "false"],
            vec!["4", "d", "4.5", "true"],
            vec!["5", "e", "5.5", "false"],
        ]
    );

    // Row-faithful check through the standalone surface: the `t`
    // table's inline entry is drained (the flush's `inline/*` deletes
    // landed) and exactly one live `ducklake_data_file` now backs it.
    let table_id = csv_rows(&run_standalone_sql(
        store,
        "SELECT table_id FROM m.ducklake_table WHERE table_name = 't' AND end_snapshot IS NULL;",
    ));
    assert_eq!(table_id, vec![vec!["1".to_string()]]);
    let remaining_inline_rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT count(*) FROM m.ducklake_inlined_data_1_1;",
    ));
    assert_eq!(remaining_inline_rows, vec![vec!["0".to_string()]]);
    let post_flush_files = csv_rows(&run_standalone_sql(
        store,
        "SELECT count(*), sum(record_count) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
    ));
    assert_eq!(
        post_flush_files,
        vec![vec!["1".to_string(), "5".to_string()]]
    );
}

/// Nested user-column types (`LIST`, `STRUCT`, `MAP`) create, inline, and
/// round-trip end to end. DuckLake stores a nested column as a marker
/// row (`list`/`struct`/`map`) plus child `ducklake_column` rows; moraine
/// stores those verbatim and passes the marker through its metadata
/// projection, and the Arrow IPC inline path carries the values. Read
/// back through scalar extractors so the comma-splitting `csv_rows`
/// never sees a nested value's internal commas.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_inline_nested_types_round_trip_through_flush() {
    let dir = TempDir::new("inline-nested-store");
    let data_dir = TempDir::new("inline-nested-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.n \
         (id BIGINT, tags BIGINT[], pt STRUCT(x INTEGER, y INTEGER), mp MAP(VARCHAR, INTEGER));",
    );
    run_ducklake_sql(
        store,
        data_path,
        "INSERT INTO lake.main.n VALUES \
         (1, [10, 20, 30], {'x': 1, 'y': 2}, MAP {'a': 7}), \
         (2, [], {'x': 3, 'y': 4}, MAP {}), \
         (3, NULL, NULL, NULL);",
    );

    let extracted = "SELECT id, len(tags), tags[1], pt.x, pt.y, map_extract(mp, 'a')[1], cardinality(mp) \
                     FROM lake.main.n ORDER BY id;";
    let want = vec![
        vec!["1", "3", "10", "1", "2", "7", "1"],
        vec!["2", "0", "NULL", "3", "4", "NULL", "0"],
        vec!["3", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"],
    ];
    assert_eq!(
        csv_rows(&run_ducklake_sql(store, data_path, extracted)),
        want
    );

    // Served through the inline entry, not a materialized Parquet file.
    let pre_flush_files = csv_rows(&run_standalone_sql(
        store,
        "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
    ));
    assert_eq!(pre_flush_files, vec![vec!["0".to_string()]]);

    // Flush transcodes the inlined nested rows through the shim's decode
    // into Parquet; the read afterward is DuckLake's Parquet reader.
    run_ducklake_sql(
        store,
        data_path,
        "CALL ducklake_flush_inlined_data('lake');",
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(store, data_path, extracted)),
        want
    );

    let remaining_inline_rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT count(*) FROM m.ducklake_inlined_data_1_1;",
    ));
    assert_eq!(remaining_inline_rows, vec![vec!["0".to_string()]]);
}

/// Flushing a table that carries **inlined file-deletions** — deletes
/// against rows living in a real Parquet file, rather than against
/// inlined rows.
///
/// This is the shape `ducklake_inline_data_round_trip_through_flush`
/// above does not reach: there, every row is inlined, so a DELETE writes
/// an `inline/inline_delete` tombstone. Once an insert overflows the
/// inlining limit and lands as a Parquet file, a DELETE against it
/// records an `inline/file_delete` instead, and the flush must
/// materialize those into a real delete file and then clear them —
/// DuckLake issues an unqualified `DELETE FROM
/// ducklake_inlined_delete_<table_id>` to do it.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_flush_clears_inlined_file_deletions() {
    let dir = TempDir::new("idel-flush-store");
    let data_dir = TempDir::new("idel-flush-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (i BIGINT);");
    // 100 rows overflows the inlining limit, so this lands as a Parquet
    // data file rather than an inlined chunk.
    run_ducklake_sql(
        store,
        data_path,
        "INSERT INTO lake.main.t SELECT range FROM range(100);",
    );
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
        )),
        vec![vec!["1"]],
        "the overflowing insert must land as a real data file"
    );

    // A delete against a Parquet-resident row is inlined, not written as
    // a delete file — that is what the flush below has to resolve.
    run_ducklake_sql(
        store,
        data_path,
        "DELETE FROM lake.main.t WHERE i IN (5, 7);",
    );
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_delete_file WHERE end_snapshot IS NULL;",
        )),
        vec![vec!["0"]],
        "the delete is inlined, so no delete file exists yet"
    );

    run_ducklake_sql(
        store,
        data_path,
        "CALL ducklake_flush_inlined_data('lake');",
    );

    // The flush wrote a real delete file and cleared the inlined
    // deletions it materialized.
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_delete_file WHERE end_snapshot IS NULL;",
        )),
        vec![vec!["1"]],
        "the flush must register a delete file for the inlined deletions"
    );

    // The rows stay deleted, and are now deleted exactly once — a
    // surviving inlined deletion alongside the delete file would be the
    // silent double-count this clearing exists to prevent.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*), sum(i) FROM lake.main.t;",
        )),
        vec![vec!["98", "4938"]],
        "5 and 7 stay deleted after the flush"
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT i FROM lake.main.t WHERE i IN (5, 7);",
        )),
        Vec::<Vec<String>>::new()
    );

    // A second flush is a no-op rather than a re-run: nothing inlined is
    // left to materialize.
    run_ducklake_sql(
        store,
        data_path,
        "CALL ducklake_flush_inlined_data('lake');",
    );
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_delete_file WHERE end_snapshot IS NULL;",
        )),
        vec![vec!["1"]],
        "a second flush must not write another delete file"
    );

    // The emptied table must still exist. DuckLake caches "this inlined
    // deletion table exists" for the life of the catalog and never
    // re-probes, so anything it runs after the flush *in the same
    // session* queries a table it believes is there. One session, flush
    // then maintenance then read — the sequence that catches an existence
    // derived from whether any deletion is currently recorded.
    let after = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "CALL ducklake_flush_inlined_data('lake'); \
         CALL ducklake_merge_adjacent_files('lake'); \
         SELECT count(*) FROM lake.main.t;",
    ));
    assert_eq!(
        after.last().expect("a count row"),
        &vec!["98".to_string()],
        "the emptied inlined-deletion table must still exist for the rest of the session"
    );
}

/// Rows for `count` parents' worth of data in one insert, and the number
/// of Parquet files the catalog holds afterwards. Inlining writes none.
fn data_files_after_inserting(
    store: &std::path::Path,
    data_path: &std::path::Path,
    attach_options: &str,
    rows: u64,
) -> String {
    run_ducklake_sql_with_options(
        store,
        data_path,
        attach_options,
        &format!(
            "CREATE TABLE lake.main.t (i BIGINT);\n\
             INSERT INTO lake.main.t SELECT range FROM range({rows});"
        ),
    );
    csv_rows(&run_standalone_sql(
        store,
        "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
    ))
    .into_iter()
    .next()
    .and_then(|row| row.into_iter().next())
    .expect("a count row")
}

/// DuckLake's compiled default bounds an insert at ten rows, and moraine
/// serves no option row of its own to displace it: a small insert inlines,
/// a large one writes a file.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_inlining_defaults_to_ten_rows() {
    let small = TempDir::new("limit-small-store");
    let small_data = TempDir::new("limit-small-data");
    assert_eq!(
        data_files_after_inserting(small.path(), small_data.path(), "", 5),
        "0",
        "five rows are under the default, so they inline"
    );

    let large = TempDir::new("limit-large-store");
    let large_data = TempDir::new("limit-large-data");
    assert_eq!(
        data_files_after_inserting(large.path(), large_data.path(), "", 400),
        "1",
        "four hundred are over it, so they land as a data file"
    );
}

/// `ATTACH ... (DATA_INLINING_ROW_LIMIT n)` raises the limit.
///
/// DuckLake resolves the limit from its config options first and only
/// falls back to the setting and its compiled default, and an ATTACH
/// option and a `ducklake_metadata` row land in the same map — so any row
/// moraine serves for this key silently outranks what the attach asked
/// for. Serving none is what keeps the option meaningful.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_attach_option_raises_the_inlining_row_limit() {
    let dir = TempDir::new("limit-attach-store");
    let data_dir = TempDir::new("limit-attach-data");
    assert_eq!(
        data_files_after_inserting(
            dir.path(),
            data_dir.path(),
            ", DATA_INLINING_ROW_LIMIT 1000",
            400
        ),
        "0",
        "the attach option must take effect, so 400 rows inline"
    );
}

/// `SET ducklake_default_data_inlining_row_limit` raises it too — the
/// same shadowing question, reached through DuckDB's setting rather than
/// through an ATTACH option.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_session_setting_raises_the_inlining_row_limit() {
    let dir = TempDir::new("limit-setting-store");
    let data_dir = TempDir::new("limit-setting-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "SET ducklake_default_data_inlining_row_limit = 1000;\n\
         CREATE TABLE lake.main.t (i BIGINT);\n\
         INSERT INTO lake.main.t SELECT range FROM range(400);",
    );
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
        )),
        vec![vec!["0"]],
        "the session setting must take effect, so 400 rows inline"
    );
}

/// A stored option raises the limit as well, which is the durable form of
/// the two knobs above: `set_option` records a `ducklake_metadata` row,
/// and a stored row is what DuckLake resolves before either of them.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_stored_option_raises_the_inlining_row_limit() {
    let dir = TempDir::new("limit-stored-store");
    let data_dir = TempDir::new("limit-stored-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(
        store,
        data_path,
        "CALL lake.set_option('data_inlining_row_limit', 1000);",
    );
    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.t (i BIGINT);\n\
         INSERT INTO lake.main.t SELECT range FROM range(400);",
    );
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
        )),
        vec![vec!["0"]],
        "the stored option must take effect, so 400 rows inline"
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM lake.main.t;",
        )),
        vec![vec!["400"]]
    );
}

/// A single-row delete costs one pass over the table, not two.
///
/// Evaluating the predicate scans, and that is inherent. What is not is a
/// second materialization to invert the rowid the scan emitted: the scan
/// emits the row's own id, so the tombstoning sink stages it directly. The
/// delete therefore costs what the equivalent read costs.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn deleting_one_inlined_row_costs_one_pass_over_the_table() {
    let dir = TempDir::new("inline-delete-cost");
    let data_dir = TempDir::new("inline-delete-cost-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (i BIGINT);");
    for chunk in 0..40 {
        run_ducklake_sql(
            store,
            data_path,
            &format!("INSERT INTO lake.main.t VALUES ({chunk});"),
        );
    }

    // Both tallies and the statement between them share one session: the
    // counters belong to the attach, so a second process restarts them.
    let lookups = |statement: &str| -> u64 {
        let tally = "SELECT metadata_hits + metadata_misses + block_hits + block_misses \
                       FROM moraine_cache_tally('lake');";
        let rows = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            &format!("{tally}\n{statement}\n{tally}"),
        ));
        let read = |row: &Vec<String>| row[0].parse::<u64>().expect("a tally is a number");
        read(rows.last().expect("a closing tally")) - read(rows.first().expect("an opening tally"))
    };

    let scanned = lookups("SELECT count(*) FROM lake.main.t WHERE i = 39;");
    let deleted = lookups("DELETE FROM lake.main.t WHERE i = 0;");
    assert!(
        deleted <= scanned + scanned / 10,
        "the delete cost {deleted} lookups against the {scanned} its scan alone costs: the sink \
         is reading the table a second time to invert the rowid"
    );
}
