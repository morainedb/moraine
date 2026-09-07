use crate::helpers::*;

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extensions"]
fn indexes_follow_field_identity_after_dropping_a_preceding_column() {
    for limit in [0, 10] {
        for staged in [false, true] {
            let store = TempDir::new("index-schema-drop");
            let data = TempDir::new("index-schema-drop-data");
            let options = format!(
                ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {limit}",
                data.path().display()
            );
            let run =
                |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);
            run(
                "CREATE TABLE lake.main.t(a BIGINT, b BIGINT); INSERT INTO lake.main.t VALUES (10,20); ALTER TABLE lake.main.t DROP COLUMN a;",
            );
            run(&format!(
                "CALL moraine_index_create('lake','main','t','by_b',['b'],true, staged := {staged});"
            ));
            assert_eq!(
                csv_rows(&run(
                    "SELECT row_id FROM moraine_index_lookup('lake','main','t','by_b',20);"
                )),
                vec![vec!["0"]],
                "limit={limit}, staged={staged}"
            );
            let duplicate = run_ducklake_sql_output(
                store.path(),
                data.path(),
                &options,
                "INSERT INTO lake.main.t VALUES (20);",
            );
            assert!(!duplicate.status.success(), "a duplicate must be refused");
            run("UPDATE lake.main.t SET b=30;");
            assert_eq!(
                csv_rows(&run(
                    "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_b',20);"
                )),
                vec![vec!["0"]]
            );
            assert_eq!(
                csv_rows(&run(
                    "SELECT row_id FROM moraine_index_lookup('lake','main','t','by_b',30);"
                )),
                vec![vec!["0"]]
            );
            run("INSERT INTO lake.main.t VALUES (20); CALL ducklake_flush_inlined_data('lake');");
            assert_eq!(
                csv_rows(&run(
                    "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_b',20);"
                )),
                vec![vec!["1"]]
            );
        }
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extensions"]
fn indexes_materialize_added_columns_initial_defaults() {
    for limit in [0, 10] {
        for staged in [false, true] {
            let store = TempDir::new("index-schema-default");
            let data = TempDir::new("index-schema-default-data");
            let options = format!(
                ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {limit}",
                data.path().display()
            );
            let run =
                |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);
            run(
                "CREATE TABLE lake.main.t(a BIGINT); INSERT INTO lake.main.t VALUES (10); ALTER TABLE lake.main.t ADD COLUMN b BIGINT DEFAULT 20; ALTER TABLE lake.main.t ALTER COLUMN b SET DEFAULT 99;",
            );
            run(&format!(
                "CALL moraine_index_create('lake','main','t','by_b',['b'],true, staged := {staged});"
            ));
            let location = csv_rows(&run(
                "SELECT row_id, data_file_id FROM moraine_index_lookup('lake','main','t','by_b',20);",
            ));
            assert_eq!(location.len(), 1, "limit={limit}, staged={staged}");
            let file = if location[0][1].is_empty() {
                "NULL"
            } else {
                &location[0][1]
            };
            run(&format!(
                "SELECT * FROM moraine_delete_located('lake','main','t',[{{row_id: {}, data_file_id: {file}}}]);",
                location[0][0]
            ));
            assert_eq!(
                csv_rows(&run(
                    "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_b',20);"
                )),
                vec![vec!["0"]]
            );
            run("INSERT INTO lake.main.t VALUES (11,20);");
        }
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extensions"]
fn indexes_follow_renamed_and_reused_column_names() {
    for limit in [0, 10] {
        let store = TempDir::new("index-schema-rename");
        let data = TempDir::new("index-schema-rename-data");
        let options = format!(
            ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {limit}",
            data.path().display()
        );
        let run =
            |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);
        run(
            "CREATE TABLE lake.main.t(a INTEGER, b BIGINT); INSERT INTO lake.main.t VALUES (10,20); ALTER TABLE lake.main.t RENAME COLUMN a TO c; ALTER TABLE lake.main.t ADD COLUMN a BIGINT DEFAULT 30; ALTER TABLE lake.main.t ALTER COLUMN c TYPE BIGINT;",
        );
        run("CALL moraine_index_create('lake','main','t','by_values',['a','c'],true);");
        assert_eq!(
            csv_rows(&run(
                "SELECT row_id FROM moraine_index_lookup('lake','main','t','by_values',30,10);"
            )),
            vec![vec!["0"]]
        );
        run(
            "BEGIN; ALTER TABLE lake.main.t RENAME COLUMN c TO d; UPDATE lake.main.t SET d=11; COMMIT;",
        );
        assert_eq!(
            csv_rows(&run(
                "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_values',30,10);"
            )),
            vec![vec!["0"]]
        );
        assert_eq!(
            csv_rows(&run(
                "SELECT row_id FROM moraine_index_lookup('lake','main','t','by_values',30,11);"
            )),
            vec![vec!["0"]]
        );
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extensions"]
fn indexes_follow_foreign_file_name_mappings() {
    let store = TempDir::new("index-schema-mapping");
    let data = TempDir::new("index-schema-mapping-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);
    let directory = data.path().join("region=east%20west");
    std::fs::create_dir_all(&directory).unwrap();
    let file = directory.join("foreign.parquet");
    run(&format!(
        "CREATE TABLE lake.main.t(a BIGINT, b BIGINT, region VARCHAR); COPY (SELECT 20::BIGINT b, 10::BIGINT a) TO '{}' (FORMAT parquet); CALL ducklake_add_data_files('lake','t','{}', hive_partitioning := true); ALTER TABLE lake.main.t RENAME COLUMN b TO c; CALL moraine_index_create('lake','main','t','by_c',['c','region'],true);",
        file.display(),
        file.display()
    ));
    assert_eq!(
        csv_rows(&run(
            "SELECT row_id FROM moraine_index_lookup('lake','main','t','by_c',20,'east west');"
        )),
        vec![vec!["0"]]
    );
    run("DELETE FROM lake.main.t WHERE c=20; INSERT INTO lake.main.t VALUES (11,20,'east west');");
    let directory = data.path().join("region=north");
    std::fs::create_dir_all(&directory).unwrap();
    let second = directory.join("second.parquet");
    run(&format!(
        "COPY (SELECT 30::BIGINT c, 12::BIGINT a) TO '{}' (FORMAT parquet); CALL ducklake_add_data_files('lake','t','{}', hive_partitioning := true);",
        second.display(),
        second.display()
    ));
    assert_eq!(
        csv_rows(&run(
            "SELECT count(*) FROM moraine_index_lookup('lake','main','t','by_c',30,'north');"
        )),
        vec![vec!["1"]]
    );
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extensions"]
fn indexes_ignore_nested_fields_before_a_scalar_column() {
    for limit in [0, 10] {
        let store = TempDir::new("index-schema-nested");
        let data = TempDir::new("index-schema-nested-data");
        let options = format!(
            ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {limit}",
            data.path().display()
        );
        let run =
            |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &options, sql);
        run(
            "CREATE TABLE lake.main.t(a STRUCT(x BIGINT,y BIGINT), b BIGINT); INSERT INTO lake.main.t VALUES ({x:10,y:11},20); CALL moraine_index_create('lake','main','t','by_b',['b'],true);",
        );
        assert_eq!(
            csv_rows(&run(
                "SELECT row_id FROM moraine_index_lookup('lake','main','t','by_b',20);"
            )),
            vec![vec!["0"]]
        );
    }
}
