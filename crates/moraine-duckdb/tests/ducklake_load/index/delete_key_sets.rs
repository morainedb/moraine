use crate::helpers::*;

/// 100,000 rows in two files where every `(c, a)` pair with `c` in 0..4
/// exists, so a cross product of key columns always names rows the keys
/// do not. A certified unique index covers `(c, a)`.
fn keyed_table(run: &dyn Fn(&str) -> String) {
    run("CREATE TABLE lake.main.t AS SELECT i%4 AS c, i//4 AS a,
             repeat(md5(i::VARCHAR),6) AS payload FROM range(50000) r(i);
         INSERT INTO lake.main.t SELECT i%4, i//4,
             repeat(md5(i::VARCHAR),6) FROM range(50000,100000) r(i);
         CREATE UNIQUE INDEX by_ca ON lake.main.t(c, a);");
    let certified = run("SELECT * FROM moraine_index_verify('lake','main','t','by_ca');");
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

fn counts(run: &dyn Fn(&str) -> String, predicate: &str) -> Vec<Vec<String>> {
    csv_rows(&run(&format!(
        "SELECT count(*) FILTER (WHERE {predicate}), count(*) FROM lake.main.t;"
    )))
}

/// An IN list beside a leading equality is the keys' cross product, which
/// the index answers in one probe: the delete plans positionally and reads
/// nothing, whether DuckDB keeps the list as a filter or, from five
/// constants, plans it as a join. The keys are not consecutive, since DuckDB
/// folds a consecutive integer list into a range before the rule sees it.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_in_list_beside_an_equality_reads_nothing() {
    let store = TempDir::new("key-set-in-store");
    let data = TempDir::new("key-set-in-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    keyed_table(&run);

    positional(&run(
        "EXPLAIN ANALYZE DELETE FROM lake.main.t WHERE c = 1 AND a IN (10, 12, 15);",
    ));
    assert_eq!(
        counts(&run, "c = 1 AND a IN (10, 12, 15)"),
        vec![vec!["0".to_string(), "99997".to_string()]]
    );

    let fifty = (0..50)
        .map(|i| (100 + 3 * i).to_string())
        .collect::<Vec<_>>()
        .join(",");
    positional(&run(&format!(
        "EXPLAIN ANALYZE DELETE FROM lake.main.t WHERE c = 2 AND a IN ({fifty});"
    )));
    assert_eq!(
        counts(&run, &format!("c = 2 AND a IN ({fifty})")),
        vec![vec!["0".to_string(), "99947".to_string()]]
    );
    assert_eq!(
        csv_rows(&run(
            "SELECT missing, dead, mismatched FROM moraine_index_verify('lake','main','t','by_ca');"
        )),
        vec![vec!["0", "0", "0"]]
    );
}

/// A tuple list names whole keys. The rewrite deletes those keys and none of
/// the rows their cross product would add.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_tuple_list_deletes_only_its_keys() {
    let store = TempDir::new("key-set-tuple-store");
    let data = TempDir::new("key-set-tuple-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    keyed_table(&run);

    positional(&run(
        "EXPLAIN ANALYZE DELETE FROM lake.main.t WHERE (c, a) IN ((0, 10), (1, 13));",
    ));
    assert_eq!(
        counts(&run, "(c, a) IN ((0, 10), (1, 13))"),
        vec![vec!["0".to_string(), "99998".to_string()]]
    );
    assert_eq!(
        counts(&run, "(c, a) IN ((0, 13), (1, 10))"),
        vec![vec!["2".to_string(), "99998".to_string()]],
        "the cross product's other rows survive"
    );
}

/// A disjunction of conjunctive equalities is a key list too, and keeps the
/// same exactness.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn an_or_of_conjunctions_deletes_only_its_keys() {
    let store = TempDir::new("key-set-or-store");
    let data = TempDir::new("key-set-or-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    keyed_table(&run);

    positional(&run("EXPLAIN ANALYZE DELETE FROM lake.main.t
         WHERE (c = 0 AND a = 20) OR (a = 23 AND c = 1);"));
    assert_eq!(
        counts(&run, "(c, a) IN ((0, 20), (1, 23))"),
        vec![vec!["0".to_string(), "99998".to_string()]]
    );
    assert_eq!(
        counts(&run, "(c, a) IN ((0, 23), (1, 20))"),
        vec![vec!["2".to_string(), "99998".to_string()]]
    );
}

/// A predicate on a column the index does not cover is residual, so the
/// scan stays and the residual still decides.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn a_key_set_with_a_residual_predicate_keeps_the_scan() {
    let store = TempDir::new("key-set-residual-store");
    let data = TempDir::new("key-set-residual-data");
    let meta = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    keyed_table(&run);

    let plan = run(
        "EXPLAIN DELETE FROM lake.main.t WHERE c = 0 AND a IN (30, 31) AND payload = 'nothing';",
    );
    assert!(
        !plan.contains("MORAINE_DELETE_POSITIONS"),
        "a residual predicate must keep the scan: {plan}"
    );
    assert_eq!(
        csv_rows(&run(
            "DELETE FROM lake.main.t WHERE c = 0 AND a IN (30, 31) AND payload = 'nothing';
             SELECT count(*) FROM lake.main.t;"
        ))
        .last()
        .cloned()
        .unwrap_or_default(),
        vec!["100000".to_string()]
    );
}
