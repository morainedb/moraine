use crate::helpers::*;

/// Ordinary predicates use the same selective reads as explicit lookups.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn ordinary_predicates_probe_single_and_composite_indexes() {
    let wide = format!(
        "a IN ({})",
        (70_000..70_064)
            .map(|row| ((row * 7919) % 100_000).to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    for (columns, predicates) in [
        (
            "['a']",
            vec![
                "a IN (30000,37919,45838)",
                "a IN (30000,30000,NULL,7777777)",
                "a BETWEEN 30000 AND 30002",
                "a > 99995",
                "a < 4",
                "a >= 99998",
                "a <= 2",
                "a IS NULL",
                &wide,
            ],
        ),
        (
            "['c','a']",
            vec![
                "c = 0 AND a = 30000",
                "a = 30000 AND c = 0",
                "c = 0 AND a IN (30000,37919,45838)",
                "c IN (0,2) AND a IN (30000,37919,45838)",
                "c = 0 AND a BETWEEN 30000 AND 30002",
                "c = 0 AND a IS NULL",
                "(c,a) IN ((0,30000),(1,37919))",
            ],
        ),
    ] {
        let store = TempDir::new("predicate-store");
        let data = TempDir::new("predicate-data");
        let options = format!(
            ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
            data.path().display()
        );
        let run =
            |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);
        fixture(&run);
        let expected: Vec<_> = predicates
            .iter()
            .map(|predicate| {
                csv_rows(&run(&format!(
                    "SELECT count(*),sum(c) FROM lake.main.t WHERE {predicate};"
                )))
            })
            .collect();
        run(&format!(
            "CALL moraine_index_create('lake','main','t','by_key',{columns},false);"
        ));
        for (predicate, expected) in predicates.iter().zip(expected) {
            let query = format!("SELECT count(*),sum(c) FROM lake.main.t WHERE {predicate};");
            assert_eq!(csv_rows(&run(&query)), expected, "{predicate}");
            let plan = run(&format!("EXPLAIN ANALYZE {query}"));
            assert!(
                plan.contains("Coverage costing ms")
                    || plan.contains("rowid")
                    || plan.contains("data_file_id"),
                "no indexed read for {predicate}: {plan}"
            );
        }
    }
}

fn fixture(run: &dyn Fn(&str) -> String) {
    run("CREATE TABLE lake.main.t AS SELECT CASE WHEN i >= 99990 THEN NULL ELSE (i*7919)%100000 END AS a,
         i%10 AS c, repeat(md5(i::VARCHAR),6) AS payload FROM range(50000) r(i);
         INSERT INTO lake.main.t SELECT CASE WHEN i >= 99990 THEN NULL ELSE (i*7919)%100000 END,
         i%10, repeat(md5(i::VARCHAR),6) FROM range(50000,100000) r(i);");
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn automatic_predicates_preserve_nulls_ordering_and_residuals() {
    let store = TempDir::new("predicate-orders-store");
    let data = TempDir::new("predicate-orders-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);
    fixture(&run);
    let predicates = [
        "a IN (NULL,30000)",
        "a IN (NULL)",
        "a NOT IN (30000,NULL)",
        "a IS NULL",
        "a IS NOT NULL AND c = 1",
        "a > 99998",
        "a < 2",
        "a BETWEEN 30000 AND 30002",
        "a = 30000 OR c = 2",
        "c = 0 AND a BETWEEN 30000 AND 30002",
        "c = 0 AND a > 99995",
        "(c,a) IN ((0,NULL),(1,37919))",
        "(c,a) = (0,NULL)",
        "(c,a) IN ((0,30000),(1,37919)) AND c+a > 30000",
    ];
    let expected: Vec<_> = predicates
        .iter()
        .map(|p| {
            csv_rows(&run(&format!(
                "SELECT count(*),sum(c) FROM lake.main.t WHERE {p};"
            )))
        })
        .collect();
    for (columns, directions) in [("['a']", "['desc']"), ("['c','a']", "['asc','desc']")] {
        run(&format!(
            "CALL moraine_index_create('lake','main','t','by_key',{columns},false,directions := {directions},nulls := {});",
            if columns == "['a']" {
                "['first']"
            } else {
                "['last','first']"
            }
        ));
        for (predicate, expected) in predicates.iter().zip(&expected) {
            assert_eq!(
                &csv_rows(&run(&format!(
                    "SELECT count(*),sum(c) FROM lake.main.t WHERE {predicate};"
                ))),
                expected,
                "{columns}: {predicate}"
            );
        }
        run("CALL moraine_index_drop('lake','main','t','by_key');");
    }
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn prepared_predicates_refresh_and_writes_keep_residuals() {
    let store = TempDir::new("predicate-writes-store");
    let data = TempDir::new("predicate-writes-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);
    fixture(&run);
    run(
        "CALL moraine_index_create('lake','main','t','by_a',['a'],false);
         SELECT * FROM moraine_index_verify('lake','main','t','by_a');",
    );
    let output = run("PREPARE points AS SELECT 'points',count(*) FROM lake.main.t WHERE a IN ($1,$2);
         PREPARE bounds AS SELECT 'bounds',count(*) FROM lake.main.t WHERE a BETWEEN $1 AND $2;
         EXECUTE points(30000,37919); EXECUTE bounds(30000,30002);
         BEGIN; INSERT INTO lake.main.t VALUES (30000,0,'new');
         EXECUTE points(30000,37919); EXECUTE bounds(30000,30002); ROLLBACK;
         EXECUTE points(30000,37919);
         UPDATE lake.main.t SET c=99 WHERE a IN (30000,37919) AND c=0;
         SELECT 'updated',count(*) FROM lake.main.t WHERE c=99;
         EXPLAIN DELETE FROM lake.main.t WHERE a IN (30000,37919) AND c=99;
         DELETE FROM lake.main.t WHERE a IN (30000,37919) AND c=99;
         SELECT 'survivor',count(*) FROM lake.main.t WHERE a=37919;
         EXECUTE points(30000,37919);
         SELECT 'verified',missing,dead,mismatched FROM moraine_index_verify('lake','main','t','by_a');");
    let rows = csv_rows(&output);
    for expected in [
        vec!["points", "2"],
        vec!["points", "3"],
        vec!["points", "1"],
        vec!["bounds", "3"],
        vec!["bounds", "4"],
        vec!["updated", "1"],
        vec!["survivor", "1"],
        vec!["verified", "0", "0", "0"],
    ] {
        assert!(
            rows.contains(&expected.iter().map(ToString::to_string).collect()),
            "{expected:?}: {output}"
        );
    }
    assert!(
        !output.contains("DUCKLAKE_DELETE_POSITIONS"),
        "residual predicate was discarded: {output}"
    );
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn certified_composite_equalities_delete_without_a_scan() {
    let store = TempDir::new("predicate-composite-delete-store");
    let data = TempDir::new("predicate-composite-delete-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);
    fixture(&run);
    run(
        "CALL moraine_index_create('lake','main','t','by_key',['c','a'],false);
         SELECT * FROM moraine_index_verify('lake','main','t','by_key');",
    );
    let plan = run("EXPLAIN DELETE FROM lake.main.t WHERE a=37919 AND c=1;");
    assert!(plan.contains("DUCKLAKE_DELETE_POSITIONS"), "{plan}");
    run("DELETE FROM lake.main.t WHERE a=37919 AND c=1;");
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FROM lake.main.t WHERE a IN (30000,37919,45838);"
        )),
        vec![vec!["2"]]
    );
    assert_eq!(
        csv_rows(&run(
            "SELECT missing,dead,mismatched FROM moraine_index_verify('lake','main','t','by_key');"
        )),
        vec![vec!["0", "0", "0"]]
    );
}
