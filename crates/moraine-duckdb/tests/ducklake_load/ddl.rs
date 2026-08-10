use crate::helpers::*;

/// The staged-row write path, driven end to end by DuckLake's own SQL:
///
/// - `CREATE TABLE` **completes** — its metadata INSERT batch translates
///   through `PlanInsert` and lands as one atomic staged commit. Row inlining
///   is on (the synthesized `ducklake_metadata` serves `data_inlining_row_limit
///   = 10`, DuckLake's default), so `CREATE TABLE` also provisions the dynamic
///   `ducklake_inlined_data_<t>_<v>` entry this shim recognizes and routes into
///   the `inline/*` keyspace rather than materializing.
/// - `ALTER TABLE ... RENAME TO` drives DuckLake's `UPDATE ducklake_table SET
///   end_snapshot ... WHERE end_snapshot IS NULL AND table_id IN (...)` — the
///   old version must land in history, the renamed one in current.
/// - `DROP TABLE` drives the same UPDATE convention for the drop.
///
/// Every step is verified through two independent surfaces: DuckLake's
/// own catalog in a fresh CLI session, and the standalone `moraine:`
/// attach's row-faithful projections.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_create_rename_drop_round_trip_through_staged_writes() {
    let dir = TempDir::new("write-store");
    let data_dir = TempDir::new("write-data");
    // No fixture seed: bootstrap alone (an empty attach mints `main`)
    // is enough for a CREATE TABLE.
    let store = dir.path();
    let data_path = data_dir.path();

    // CREATE TABLE completes.
    run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.x (i BIGINT);");

    // A fresh session (fresh DuckLake attach re-reading the store) sees
    // the table DuckLake itself believes it created.
    let tables = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "SELECT name FROM (SHOW ALL TABLES) WHERE database = 'lake';",
    ));
    assert_eq!(tables, vec![vec!["x".to_string()]]);

    // Row-faithful check through the other surface: exactly one live
    // ducklake_table row, name `x`, its column typed in DuckLake's own
    // vocabulary.
    let rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT table_name, column_name, column_type FROM m.ducklake_table t \
         JOIN m.ducklake_column c USING (table_id) \
         WHERE t.end_snapshot IS NULL AND c.end_snapshot IS NULL;",
    ));
    assert_eq!(
        rows,
        vec![vec!["x".to_string(), "i".to_string(), "int64".to_string()]]
    );

    // RENAME: DuckLake ends the live ducklake_table row (UPDATE ... SET
    // end_snapshot) and inserts the renamed version.
    run_ducklake_sql(store, data_path, "ALTER TABLE lake.main.x RENAME TO y;");

    let tables = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "SELECT name FROM (SHOW ALL TABLES) WHERE database = 'lake';",
    ));
    assert_eq!(tables, vec![vec!["y".to_string()]]);

    // Lifecycle stitching, row-faithfully: one history row `x` whose
    // end_snapshot equals the current row `y`'s begin_snapshot, same
    // table_id.
    let rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT h.table_name, c.table_name, CAST(h.end_snapshot = c.begin_snapshot AS VARCHAR), \
                CAST(h.table_id = c.table_id AS VARCHAR) \
         FROM m.ducklake_table h, m.ducklake_table c \
         WHERE h.end_snapshot IS NOT NULL AND c.end_snapshot IS NULL;",
    ));
    assert_eq!(
        rows,
        vec![vec![
            "x".to_string(),
            "y".to_string(),
            "true".to_string(),
            "true".to_string()
        ]]
    );

    // DROP: ends the remaining live version.
    run_ducklake_sql(store, data_path, "DROP TABLE lake.main.y;");

    let tables = csv_rows(&run_ducklake_sql(
        store,
        data_path,
        "SELECT name FROM (SHOW ALL TABLES) WHERE database = 'lake';",
    ));
    assert!(tables.is_empty(), "dropped table still listed: {tables:?}");

    let rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT count(*) FROM m.ducklake_table WHERE end_snapshot IS NULL;",
    ));
    assert_eq!(rows, vec![vec!["0".to_string()]]);
}

/// Column/name mapping end to end: a Parquet file written by plain
/// DuckDB `COPY` (no DuckLake field ids), one column short and sitting
/// under a hive path, registers through `ducklake_add_data_files` with
/// `hive_partitioning`. DuckLake writes the `ducklake_column_mapping` /
/// `ducklake_name_mapping` rows (folded into one mapping record) and
/// the file row carries its `mapping_id`; reads resolve the body
/// column by name and the partition column from the path, time travel
/// included; the standalone attach serves the row-faithful
/// projections.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_add_data_files_maps_foreign_parquet() {
    let dir = TempDir::new("mapping-store");
    let data_dir = TempDir::new("mapping-data");
    let store = dir.path();
    let data_path = data_dir.path();

    let foreign_dir = data_path.join("foreign").join("region=east");
    std::fs::create_dir_all(&foreign_dir).expect("test setup: create hive dir");
    let foreign_file = foreign_dir.join("part.parquet");

    // Create, write the foreign file, register it.
    run_ducklake_sql(
        store,
        data_path,
        &format!(
            "CREATE TABLE lake.main.t (id BIGINT, region VARCHAR);\
             COPY (SELECT range AS id FROM range(3)) TO '{f}' (FORMAT parquet);\
             CALL ducklake_add_data_files('lake', 't', '{f}', hive_partitioning => true);",
            f = foreign_file.display(),
        ),
    );

    // A fresh attach re-reads the mapping from the store: the body
    // column resolves by name, the partition column from the path.
    let out = run_ducklake_sql(
        store,
        data_path,
        "SELECT id, region FROM lake.main.t ORDER BY id;",
    );
    assert_eq!(
        csv_rows(&out),
        vec![
            vec!["0".to_string(), "east".to_string()],
            vec!["1".to_string(), "east".to_string()],
            vec!["2".to_string(), "east".to_string()],
        ]
    );

    // A later write advances the head; the registered snapshot stays
    // readable through the mapping.
    let out = run_ducklake_sql(
        store,
        data_path,
        "USE lake;\
         SELECT CAST(max(snapshot_id) AS VARCHAR) FROM snapshots();",
    );
    let registered_snapshot = csv_rows(&out)[0][0].clone();
    run_ducklake_sql(
        store,
        data_path,
        "INSERT INTO lake.main.t VALUES (9, 'west');",
    );
    let out = run_ducklake_sql(
        store,
        data_path,
        &format!(
            "SELECT count(*), max(id) FROM lake.main.t AT (VERSION => {registered_snapshot});"
        ),
    );
    assert_eq!(csv_rows(&out), vec![vec!["3".to_string(), "2".to_string()]]);

    // Row-faithful projections through the standalone attach: one
    // mapping, map_by_name.
    let rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT mapping_id, table_id, type FROM m.ducklake_column_mapping;",
    ));
    assert_eq!(rows.len(), 1);
    let mapping_id = rows[0][0].clone();
    assert_eq!(rows[0][2], "map_by_name");

    // The name rows: `id` resolved from the file body, `region` a
    // hive-path virtual column; roots carry no parent.
    let rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT column_id, source_name, \
                CAST(parent_column IS NULL AS VARCHAR), \
                CAST(is_partition AS VARCHAR) \
         FROM m.ducklake_name_mapping ORDER BY column_id;",
    ));
    let sources: Vec<(&str, &str)> = rows
        .iter()
        .map(|r| (r[1].as_str(), r[3].as_str()))
        .collect();
    assert!(sources.contains(&("id", "false")), "{rows:?}");
    assert!(sources.contains(&("region", "true")), "{rows:?}");
    assert!(rows.iter().all(|r| r[2] == "true"), "roots only: {rows:?}");

    // The registered file row carries the mapping id; nothing else
    // does (the inlined INSERT wrote no file).
    let rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT CAST(mapping_id AS VARCHAR) FROM m.ducklake_data_file \
         WHERE mapping_id IS NOT NULL;",
    ));
    assert_eq!(rows, vec![vec![mapping_id]]);
}

/// Scalar and table macros end to end, driven entirely through
/// DuckLake's own SQL: `CREATE MACRO` (arity overloads, a defaulted
/// parameter, a table macro) folds its `ducklake_macro_impl` /
/// `ducklake_macro_parameters` inserts into one macro record;
/// `CREATE OR REPLACE` re-binds under a fresh `macro_id`;
/// `DROP MACRO` ends the row; a `SNAPSHOT_VERSION` attach still calls
/// the dropped definition; and the standalone attach serves the
/// row-faithful `ducklake_macro*` projections.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
#[allow(clippy::too_many_lines)]
fn ducklake_macros_round_trip_through_staged_writes() {
    let dir = TempDir::new("macro-store");
    let data_dir = TempDir::new("macro-data");
    let store = dir.path();
    let data_path = data_dir.path();

    // Create all three macros, then call every shape in one SELECT.
    let out = run_ducklake_sql(
        store,
        data_path,
        "USE lake;\
         CREATE MACRO add_num(a) AS a + 1, (a, b) AS a + b;\
         CREATE MACRO defaulted(a, b := 5) AS a + b;\
         CREATE MACRO pick(x) AS TABLE SELECT x AS v;\
         SELECT add_num(1), add_num(1, 2), defaulted(1), (SELECT v FROM pick(7));",
    );
    assert_eq!(
        csv_rows(&out),
        vec![vec![
            "2".to_string(),
            "3".to_string(),
            "6".to_string(),
            "7".to_string()
        ]]
    );

    // A fresh session re-reads the macros from the store and captures
    // the pre-replace snapshot for the time-travel attach below.
    let out = run_ducklake_sql(
        store,
        data_path,
        "USE lake;\
         SELECT CAST(max(snapshot_id) AS VARCHAR) || ':' || \
                CAST(add_num(1, 2) AS VARCHAR) FROM snapshots();",
    );
    let combined = csv_rows(&out);
    let (pre_replace_snapshot, add_result) = combined[0][0]
        .trim_matches('"')
        .split_once(':')
        .expect("snapshot:result pair");
    assert_eq!(add_result, "3");
    let pre_replace_snapshot = pre_replace_snapshot.to_owned();

    // Replace re-binds under a fresh macro_id; drop removes the
    // defaulted macro from the live catalog set.
    let out = run_ducklake_sql(
        store,
        data_path,
        "USE lake;\
         CREATE OR REPLACE MACRO add_num(a) AS a + 10;\
         DROP MACRO defaulted;\
         SELECT add_num(1), \
                (SELECT count(*) FROM duckdb_functions() \
                 WHERE database_name = 'lake' AND function_name = 'defaulted');",
    );
    assert_eq!(
        csv_rows(&out),
        vec![vec!["11".to_string(), "0".to_string()]]
    );

    // Time travel: at the pre-replace snapshot the old overloads and
    // the since-dropped macro both still bind and compute.
    let out = run_ducklake_sql_with_options(
        store,
        data_path,
        &format!(", SNAPSHOT_VERSION {pre_replace_snapshot}"),
        "USE lake; SELECT add_num(1), add_num(1, 2), defaulted(1);",
    );
    assert_eq!(
        csv_rows(&out),
        vec![vec!["2".to_string(), "3".to_string(), "6".to_string()]]
    );

    // Row-faithful projections through the standalone attach. Four
    // macro rows: the replaced and dropped ones ended, the replacement
    // and the table macro live.
    let rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT macro_name, CAST(end_snapshot IS NULL AS VARCHAR) \
         FROM m.ducklake_macro ORDER BY macro_id;",
    ));
    assert_eq!(
        rows,
        vec![
            vec!["add_num".to_string(), "false".to_string()],
            vec!["defaulted".to_string(), "false".to_string()],
            vec!["pick".to_string(), "true".to_string()],
            vec!["add_num".to_string(), "true".to_string()],
        ]
    );

    // Impl rows keep serving for ended macros (time travel reads
    // them); ordinals and types are verbatim.
    let rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT i.macro_id = m.macro_id, i.impl_id, i.type \
         FROM m.ducklake_macro_impl i \
         JOIN m.ducklake_macro m USING (macro_id) \
         WHERE m.macro_name = 'add_num' AND m.end_snapshot IS NOT NULL \
         ORDER BY i.impl_id;",
    ));
    assert_eq!(
        rows,
        vec![
            vec!["true".to_string(), "0".to_string(), "scalar".to_string()],
            vec!["true".to_string(), "1".to_string(), "scalar".to_string()],
        ]
    );
    let rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT i.type FROM m.ducklake_macro_impl i \
         JOIN m.ducklake_macro m USING (macro_id) \
         WHERE m.macro_name = 'pick';",
    ));
    assert_eq!(rows, vec![vec!["table".to_string()]]);

    // The defaulted parameter row carries the default verbatim.
    let rows = csv_rows(&run_standalone_sql(
        store,
        "SELECT p.column_id, p.parameter_name, p.default_value, p.default_value_type \
         FROM m.ducklake_macro_parameters p \
         JOIN m.ducklake_macro m USING (macro_id) \
         WHERE m.macro_name = 'defaulted' ORDER BY p.column_id;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![
                "0".to_string(),
                "a".to_string(),
                "NULL".to_string(),
                "unknown".to_string()
            ],
            vec![
                "1".to_string(),
                "b".to_string(),
                "5".to_string(),
                "int32".to_string()
            ],
        ]
    );
}

/// Column ids and positions match stock DuckLake exactly, differential
/// against a reference catalog fed the identical DDL. Existing coverage
/// asserts names and types ordered *by* `column_order` and so would pass
/// under any monotonic numbering; this pins the values themselves — ids
/// allocated per-table and never reused, positions numbered from 1 with
/// the gap a drop leaves preserved rather than renumbered.
///
/// This exercises the staged path, where moraine carries DuckLake's own
/// values verbatim. The verb path allocates them itself and is pinned by
/// the core unit tests instead; a divergence there is invisible here.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_column_ids_and_positions_match_stock_ducklake() {
    let store = TempDir::new("colid-store");
    let data = TempDir::new("colid-data");
    let reference_meta = TempDir::new("colid-ref-meta");
    let reference_data = TempDir::new("colid-ref-data");

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

    let live = "SELECT column_name, column_id, column_order \
                FROM __ducklake_metadata_lake.ducklake_column \
                WHERE end_snapshot IS NULL ORDER BY column_order;";

    apply("CREATE TABLE lake.main.t (a BIGINT, b VARCHAR, c DOUBLE);");
    assert_eq!(
        probe(live),
        vec![
            vec!["a".to_string(), "1".to_string(), "1".to_string()],
            vec!["b".to_string(), "2".to_string(), "2".to_string()],
            vec!["c".to_string(), "3".to_string(), "3".to_string()],
        ],
        "positions are numbered from 1, matching field ids on a fresh table"
    );

    apply("ALTER TABLE lake.main.t ADD COLUMN d INTEGER;");
    apply("ALTER TABLE lake.main.t DROP COLUMN b;");

    // The drop leaves a gap at 2 in both id and position: survivors keep
    // what they had, and nothing is renumbered to close it.
    assert_eq!(
        probe(live),
        vec![
            vec!["a".to_string(), "1".to_string(), "1".to_string()],
            vec!["c".to_string(), "3".to_string(), "3".to_string()],
            vec!["d".to_string(), "4".to_string(), "4".to_string()],
        ],
        "a drop must leave a gap rather than renumber the survivors"
    );

    // A later add continues past the highest, never reusing the dropped id.
    apply("ALTER TABLE lake.main.t ADD COLUMN e INTEGER;");
    assert_eq!(
        probe(live).last().expect("at least one column"),
        &vec!["e".to_string(), "5".to_string(), "5".to_string()],
        "the re-add must allocate above the maximum, not fill the gap"
    );

    // A rename and a widening promotion, so the history the sweep below
    // walks carries every column-version transition, not only add and drop.
    apply("ALTER TABLE lake.main.t RENAME COLUMN c TO c2;");
    apply("ALTER TABLE lake.main.t ALTER COLUMN d TYPE BIGINT;");
    nested_columns_match_stock_ducklake(&apply, &probe);

    // Head agreement is one row of the table. The reconstruction is the
    // whole of it: every version between the two catalogs must carry the
    // same live column set, since a divergence that cancels out by head is
    // exactly what a head-only probe cannot see. One statement, evaluated
    // per version, rather than a session per version — a CLI round trip
    // each would cost minutes.
    let history = "SELECT s.snapshot_id, c.column_name, c.column_id, c.column_order, c.column_type \
                   FROM __ducklake_metadata_lake.ducklake_snapshot s \
                   JOIN __ducklake_metadata_lake.ducklake_column c \
                     ON c.begin_snapshot <= s.snapshot_id \
                    AND (c.end_snapshot IS NULL OR c.end_snapshot > s.snapshot_id) \
                   ORDER BY s.snapshot_id, c.column_order;";
    let rebuilt = probe(history);
    assert!(
        !rebuilt.is_empty(),
        "the history sweep matched no rows; the join or the metadata schema moved"
    );
    // The last version's rows are the live set the head probe just pinned,
    // so the sweep is anchored rather than merely self-consistent.
    let last_version = rebuilt.last().expect("at least one row")[0].clone();
    let at_head: Vec<Vec<String>> = rebuilt
        .iter()
        .filter(|row| row[0] == last_version)
        .map(|row| row[1..4].to_vec())
        .collect();
    assert_eq!(
        at_head,
        vec![
            vec!["a".to_string(), "1".to_string(), "1".to_string()],
            vec!["c2".to_string(), "3".to_string(), "3".to_string()],
            vec!["d".to_string(), "4".to_string(), "4".to_string()],
            vec!["e".to_string(), "5".to_string(), "5".to_string()],
            vec!["s".to_string(), "6".to_string(), "6".to_string()],
            vec!["x".to_string(), "7".to_string(), "7".to_string()],
            vec!["y".to_string(), "8".to_string(), "8".to_string()],
        ],
        "the newest version of the sweep must be the live set"
    );
}

/// The nested half of the differential above, split out to keep each
/// readable: a parent plus one row per field, allocated in pre-order out of
/// the same counter the top-level columns draw from, each field naming its
/// parent. The core pins the verb path against this same layout; here the
/// staged path is held to it against stock DuckLake.
fn nested_columns_match_stock_ducklake(
    apply: &dyn Fn(&str),
    probe: &dyn Fn(&str) -> Vec<Vec<String>>,
) {
    apply("ALTER TABLE lake.main.t ADD COLUMN s STRUCT(x BIGINT, y VARCHAR);");
    assert_eq!(
        probe(
            "SELECT column_name, column_id, column_order, column_type, parent_column \
             FROM __ducklake_metadata_lake.ducklake_column \
             WHERE end_snapshot IS NULL AND column_id >= 6 ORDER BY column_id;"
        ),
        vec![
            vec![
                "s".to_string(),
                "6".to_string(),
                "6".to_string(),
                "struct".to_string(),
                "NULL".to_string(),
            ],
            vec![
                "x".to_string(),
                "7".to_string(),
                "7".to_string(),
                "int64".to_string(),
                "6".to_string(),
            ],
            vec![
                "y".to_string(),
                "8".to_string(),
                "8".to_string(),
                "varchar".to_string(),
                "6".to_string(),
            ],
        ],
        "a nested column's fields take the ids after their parent, in pre-order"
    );
}

/// Column-level schema evolution through DuckLake's own `ALTER TABLE`:
/// ADD / RENAME / DROP COLUMN. DuckLake expresses each as
/// `ducklake_column` version transitions over the staged-write path,
/// so this exercises no dedicated schema-mutation path in
/// moraine — the generic staged commit carries it. Verified through the
/// standalone row-faithful projection (live columns, ordered) and
/// DuckLake's own reflection in a fresh session.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_column_schema_evolution_through_staged_writes() {
    let dir = TempDir::new("evolve-store");
    let data_dir = TempDir::new("evolve-data");
    let store = dir.path();
    let data_path = data_dir.path();

    let live_columns = "SELECT column_name, column_type FROM m.ducklake_column \
                        WHERE end_snapshot IS NULL ORDER BY column_order;";

    run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a BIGINT);");
    run_ducklake_sql(
        store,
        data_path,
        "ALTER TABLE lake.main.t ADD COLUMN b VARCHAR;",
    );
    assert_eq!(
        csv_rows(&run_standalone_sql(store, live_columns)),
        vec![vec!["a", "int64"], vec!["b", "varchar"]]
    );
    // DuckLake's own reflection in a fresh attach agrees.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT column_name FROM (DESCRIBE lake.main.t) ORDER BY column_name;",
        )),
        vec![vec!["a"], vec!["b"]]
    );

    run_ducklake_sql(
        store,
        data_path,
        "ALTER TABLE lake.main.t RENAME COLUMN b TO c;",
    );
    assert_eq!(
        csv_rows(&run_standalone_sql(store, live_columns)),
        vec![vec!["a", "int64"], vec!["c", "varchar"]]
    );

    run_ducklake_sql(store, data_path, "ALTER TABLE lake.main.t DROP COLUMN c;");
    assert_eq!(
        csv_rows(&run_standalone_sql(store, live_columns)),
        vec![vec!["a", "int64"]]
    );

    // The evolved schema is functional: a fresh session inserts and reads.
    run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (42);");
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT a FROM lake.main.t;"
        )),
        vec![vec!["42"]]
    );
}

/// Type promotion through DuckLake's `ALTER TABLE ... ALTER COLUMN ...
/// TYPE` — the remaining column-level op. The load-bearing
/// case is data that predates the change: rows inlined under the old type
/// live in their own schema version's chunk (decoded against that
/// version's `inline/schema`), and must still read back — coerced to the
/// new type by DuckLake — after the column is retyped and newer rows
/// inline under the new version.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_column_type_promotion_over_inlined_data() {
    let dir = TempDir::new("promote-store");
    let data_dir = TempDir::new("promote-data");
    let store = dir.path();
    let data_path = data_dir.path();

    run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a INTEGER);");
    // Inlined under the INTEGER (int32) schema version.
    run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (1), (2);");

    run_ducklake_sql(
        store,
        data_path,
        "ALTER TABLE lake.main.t ALTER COLUMN a TYPE BIGINT;",
    );
    // The retyped column is int64 in the live catalog projection.
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT column_type FROM m.ducklake_column WHERE end_snapshot IS NULL;",
        )),
        vec![vec!["int64"]]
    );
    // Rows inlined before the change still read, coerced to the new type.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT a FROM lake.main.t ORDER BY a;"
        )),
        vec![vec!["1"], vec!["2"]]
    );
    // New rows inline under the new version and coexist with the old.
    run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (3);");
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT a FROM lake.main.t ORDER BY a;"
        )),
        vec![vec!["1"], vec!["2"], vec!["3"]]
    );
}

/// Multi-statement, cross-table ACID transactions through DuckLake's own
/// `BEGIN`/`COMMIT`/`ROLLBACK`, driven end to end.
///
/// Every write a DuckDB transaction makes stages into one moraine
/// staged tx (opened lazily on the first write, reused across every
/// statement), committed atomically at `COMMIT` — so a transaction that
/// writes two different tables mints exactly one `ducklake_snapshot`, and
/// both tables' rows appear together or not at all.
///
/// - `BEGIN; INSERT a; INSERT b; COMMIT;` across two tables lands both rows and
///   advances the snapshot by exactly one (not one per statement) — the
///   batching proof.
/// - `BEGIN; INSERT; ROLLBACK;` discards the write and mints no snapshot.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_multi_statement_transaction_commits_atomically() {
    let dir = TempDir::new("tx-store");
    let data_dir = TempDir::new("tx-data");
    let store = dir.path();
    let data_path = data_dir.path();

    // Two tables to span in one transaction.
    run_ducklake_sql(
        store,
        data_path,
        "CREATE TABLE lake.main.accounts (id BIGINT); \
         CREATE TABLE lake.main.ledger (amount BIGINT);",
    );

    // The two CREATEs above minted snapshots 1 and 2 (bootstrap is 0):
    // head is now 2.
    let head_before = csv_rows(&run_standalone_sql(
        store,
        "SELECT max(snapshot_id) FROM m.ducklake_snapshot;",
    ));
    assert_eq!(head_before, vec![vec!["2".to_string()]]);

    // One transaction, two tables, two writes — committed atomically.
    run_ducklake_sql(
        store,
        data_path,
        "BEGIN; \
         INSERT INTO lake.main.accounts VALUES (1); \
         INSERT INTO lake.main.ledger VALUES (100); \
         COMMIT;",
    );

    // Both rows are visible: the transaction landed as a whole.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT (SELECT count(*) FROM lake.main.accounts), \
                    (SELECT count(*) FROM lake.main.ledger);",
        )),
        vec![vec!["1".to_string(), "1".to_string()]]
    );

    // The two writes advanced the head by exactly one snapshot: they
    // shared one moraine staged tx, not one per statement.
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT max(snapshot_id) FROM m.ducklake_snapshot;",
        )),
        vec![vec!["3".to_string()]]
    );

    // ROLLBACK discards the write and mints no snapshot.
    run_ducklake_sql(
        store,
        data_path,
        "BEGIN; \
         INSERT INTO lake.main.accounts VALUES (2); \
         ROLLBACK;",
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM lake.main.accounts;",
        )),
        vec![vec!["1".to_string()]]
    );
    assert_eq!(
        csv_rows(&run_standalone_sql(
            store,
            "SELECT max(snapshot_id) FROM m.ducklake_snapshot;",
        )),
        vec![vec!["3".to_string()]]
    );
}

/// Table and column comments, differential against a stock DuckLake
/// catalog fed the identical statements: `COMMENT ON` lands
/// `ducklake_tag` / `ducklake_column_tag` rows through the staged-row
/// path, DuckLake reads them back, a re-comment ends the old entry
/// and inserts the new one, the served rows carry both lifecycles
/// row-for-row identical to the reference, and comments survive a
/// column rename.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn ducklake_table_and_column_comments_round_trip() {
    let store = TempDir::new("tags-store");
    let data = TempDir::new("tags-data");
    let reference_meta = TempDir::new("tags-ref-meta");
    let reference_data = TempDir::new("tags-ref-data");

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

    apply(
        "CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);\
         COMMENT ON TABLE lake.main.t IS 'first';\
         COMMENT ON COLUMN lake.main.t.a IS 'col comment';",
    );
    assert_eq!(
        probe("SELECT comment FROM duckdb_tables() WHERE table_name = 't';"),
        vec![vec!["first".to_string()]]
    );
    assert_eq!(
        probe(
            "SELECT comment FROM duckdb_columns() \
             WHERE table_name = 't' AND column_name = 'a';"
        ),
        vec![vec!["col comment".to_string()]]
    );

    // A re-comment is a set-end + insert pair: the old entry ends,
    // the new one lands live, both rows lifecycle-identical to stock.
    apply("COMMENT ON TABLE lake.main.t IS 'second';");
    assert_eq!(
        probe("SELECT comment FROM duckdb_tables() WHERE table_name = 't';"),
        vec![vec!["second".to_string()]]
    );
    assert_eq!(
        probe(
            "SELECT object_id, begin_snapshot, end_snapshot, key, value \
             FROM __ducklake_metadata_lake.ducklake_tag ORDER BY begin_snapshot;"
        ),
        vec![
            vec![
                "1".to_string(),
                "2".to_string(),
                "4".to_string(),
                "comment".to_string(),
                "first".to_string(),
            ],
            vec![
                "1".to_string(),
                "4".to_string(),
                "NULL".to_string(),
                "comment".to_string(),
                "second".to_string(),
            ],
        ]
    );
    assert_eq!(
        probe(
            "SELECT table_id, column_id, begin_snapshot, end_snapshot, key, value \
             FROM __ducklake_metadata_lake.ducklake_column_tag;"
        ),
        vec![vec![
            "1".to_string(),
            "1".to_string(),
            "3".to_string(),
            "NULL".to_string(),
            "comment".to_string(),
            "col comment".to_string(),
        ]]
    );

    // Column comments survive a column rename (a version transition
    // carries the entries forward), identically to stock.
    apply("ALTER TABLE lake.main.t RENAME COLUMN a TO a2;");
    assert_eq!(
        probe(
            "SELECT comment FROM duckdb_columns() \
             WHERE table_name = 't' AND column_name = 'a2';"
        ),
        vec![vec!["col comment".to_string()]]
    );
}
