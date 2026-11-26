use crate::helpers::*;

const INSTANCE: &str = "'00000000-0000-0000-0000-000000000001'::UUID";

/// 100,000 rows in two files, every one under the same `instance_id`, with
/// a certified unique index over `(instance_id, source_key)`. DuckDB's
/// statistics pass proves an equality on `instance_id` always true and
/// removes it before the planner sees the predicate.
fn single_instance_table(run: &dyn Fn(&str) -> String) {
    run(&format!(
        "CREATE TABLE lake.main.t AS SELECT uuid() AS id, {INSTANCE} AS instance_id,
             'k' || i AS source_key, repeat(md5(i::VARCHAR),6) AS payload FROM range(50000) r(i);
         INSERT INTO lake.main.t SELECT uuid(), {INSTANCE}, 'k' || i,
             repeat(md5(i::VARCHAR),6) FROM range(50000,100000) r(i);
         CREATE UNIQUE INDEX t_identity ON lake.main.t(instance_id, source_key);"
    ));
    let certified = run("SELECT * FROM moraine_index_verify('lake','main','t','t_identity');");
    assert!(
        certified
            .lines()
            .any(|line| line.starts_with("100000,100000,0,0,0,")),
        "the index did not verify clean: {certified}"
    );
}

fn positional(plan: &str) {
    assert!(
        plan.contains("MORAINE_DELETE_POSITIONS"),
        "the delete was not rewritten: {plan}"
    );
    assert!(
        !plan.contains("DUCKLAKE_SCAN") && !plan.contains("Ducklake Scan"),
        "a scan survived the rewrite: {plan}"
    );
}

/// The leading equality DuckDB's statistics removed is restored from those
/// same statistics: the delete plans positionally and the read is located.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_removed_leading_equality_still_completes_the_key() {
    let store = TempDir::new("implied-removed-store");
    let data = TempDir::new("implied-removed-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    single_instance_table(&run);

    let plan = run(&format!(
        "EXPLAIN ANALYZE SELECT count(*) FROM lake.main.t WHERE instance_id = {INSTANCE} AND source_key = 'k7';"
    ));
    assert!(
        plan.contains("Coverage costing ms"),
        "the read was not located: {plan}"
    );
    positional(&run(&format!(
        "EXPLAIN ANALYZE DELETE FROM lake.main.t WHERE instance_id = {INSTANCE} AND source_key = 'k8';"
    )));
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FILTER (WHERE source_key = 'k8'), count(*) FROM lake.main.t;"
        )),
        vec![vec!["0".to_string(), "99999".to_string()]]
    );
}

/// A constant column need not be named at all: the statistics complete the
/// key, for one key and for a key set alike.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_constant_column_need_not_be_named() {
    let store = TempDir::new("implied-unnamed-store");
    let data = TempDir::new("implied-unnamed-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    single_instance_table(&run);

    positional(&run(
        "EXPLAIN ANALYZE DELETE FROM lake.main.t WHERE source_key = 'k9';",
    ));
    positional(&run(
        "EXPLAIN ANALYZE DELETE FROM lake.main.t WHERE source_key IN ('k10', 'k12', 'k15');",
    ));
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FILTER (WHERE source_key IN ('k9', 'k10', 'k12', 'k15')), count(*) FROM lake.main.t;"
        )),
        vec![vec!["0".to_string(), "99996".to_string()]]
    );
    assert_eq!(
        csv_rows(&run(
            "SELECT missing, dead, mismatched FROM moraine_index_verify('lake','main','t','t_identity');"
        )),
        vec![vec!["0", "0", "0"]]
    );
}

/// A column some row holds NULL in is not constant, so nothing is implied
/// and the predicate still decides over every row.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_column_with_nulls_implies_nothing() {
    let store = TempDir::new("implied-nulls-store");
    let data = TempDir::new("implied-nulls-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    single_instance_table(&run);
    run("INSERT INTO lake.main.t VALUES (uuid(), NULL, 'k20', 'orphan');");

    let plan = run("EXPLAIN DELETE FROM lake.main.t WHERE source_key = 'k20';");
    assert!(
        !plan.contains("MORAINE_DELETE_POSITIONS"),
        "a nullable column must not complete the key: {plan}"
    );
    assert_eq!(
        csv_rows(&run(
            "DELETE FROM lake.main.t WHERE source_key = 'k20';
             SELECT count(*) FILTER (WHERE source_key = 'k20'), count(*) FROM lake.main.t;"
        ))
        .last()
        .cloned()
        .unwrap_or_default(),
        vec!["0".to_string(), "99999".to_string()],
        "both rows under the key go, the constant one and the NULL one"
    );
}

/// Once a second value appears the column implies nothing, and the explicit
/// equality DuckDB now keeps carries the key as before.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_second_value_restores_the_explicit_path() {
    let store = TempDir::new("implied-second-store");
    let data = TempDir::new("implied-second-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    single_instance_table(&run);
    run(
        "INSERT INTO lake.main.t SELECT uuid(), '00000000-0000-0000-0000-000000000002'::UUID,
             'k' || i, 'other' FROM range(1000) r(i);",
    );

    let plan = run("EXPLAIN DELETE FROM lake.main.t WHERE source_key = 'k30';");
    assert!(
        !plan.contains("MORAINE_DELETE_POSITIONS"),
        "two instances share the key, so nothing is implied: {plan}"
    );
    positional(&run(&format!(
        "EXPLAIN ANALYZE DELETE FROM lake.main.t WHERE instance_id = {INSTANCE} AND source_key = 'k30';"
    )));
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FILTER (WHERE source_key = 'k30'), count(*) FROM lake.main.t;"
        )),
        vec![vec!["1".to_string(), "100999".to_string()]],
        "the other instance's row under the key survives"
    );
}
