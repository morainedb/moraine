use crate::helpers::*;

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn native_index_ddl_creates_probes_and_drops() {
    let store = TempDir::new("native-index-store");
    let data = TempDir::new("native-index-data");
    let options = format!(
        ", META_DATA_PATH '{}', METADATA_CATALOG 'native_metadata'",
        data.path().display()
    );
    let result = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);
         INSERT INTO lake.main.t VALUES (1, 'a'), (2, 'b');
         CREATE UNIQUE INDEX by_a ON lake.main.t(a);
         CREATE INDEX IF NOT EXISTS by_a ON lake.main.t(b);
         SELECT 'wrappers', count(*) FROM duckdb_functions()
         WHERE function_name IN ('moraine_index_create','moraine_index_drop');
         SELECT index_name, is_unique FROM moraine_indexes('lake','main','t');
         SELECT 'hit', row_id FROM moraine_index_lookup('lake','main','t','by_a',2);
         DROP INDEX IF EXISTS lake.main.by_a;
         SELECT 'remaining', count(*) FROM moraine_indexes('lake','main','t');
         DROP INDEX IF EXISTS lake.main.by_a;
         CREATE INDEX by_b ON lake.main.t(b) WITH (staged=true, step_entries=1);
         DROP INDEX IF EXISTS lake.missing_schema.by_a;
         DROP INDEX lake.main.by_b;",
    );
    for expected in ["wrappers,0", "by_a,true", "hit,1", "remaining,0"] {
        assert!(result.lines().any(|line| line == expected), "{result}");
    }
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn native_index_names_follow_schema_and_identifier_rules() {
    let store = TempDir::new("native-names-store");
    let data = TempDir::new("native-names-data");
    let result = run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE SCHEMA lake.other;
         CREATE TABLE lake.other.\"Some Table\"(\"Some Column\" BIGINT);
         CREATE TABLE lake.other.second(a BIGINT);
         USE lake.other;
         CREATE INDEX \"Some Index\" ON \"Some Table\"(\"some column\");
         CREATE INDEX IF NOT EXISTS \"some index\" ON second(a);
         SELECT index_name FROM moraine_indexes('lake','other','Some Table');
         DROP INDEX \"SOME INDEX\";
         CREATE INDEX same ON second(a);
         CREATE TABLE lake.main.t(a BIGINT);
         CREATE INDEX same ON lake.main.t(a);
         DROP INDEX same;
         DROP INDEX lake.main.same;
         CREATE TABLE memory.main.local_table(a INTEGER);
         CREATE INDEX local_index ON memory.main.local_table(a);
         DROP INDEX IF EXISTS memory.main.local_index;",
    );
    assert!(result.contains("Some Index"), "{result}");
    let error = run_ducklake_sql_expect_err(
        store.path(),
        data.path(),
        "CREATE INDEX by_a ON lake.main.t(a);
         CREATE TABLE lake.main.second(a BIGINT);
         CREATE INDEX BY_A ON lake.main.second(a);",
    );
    assert!(error.contains("already exists"), "{error}");
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn native_index_ddl_is_execution_only_and_requires_autocommit() {
    let store = TempDir::new("native-tx-store");
    let data = TempDir::new("native-tx-data");
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);",
    );
    let explained = run_ducklake_sql(
        store.path(),
        data.path(),
        "EXPLAIN CREATE INDEX by_a ON lake.main.t(a);
         SELECT 'remaining', count(*) FROM moraine_indexes('lake','main','t');",
    );
    assert!(
        explained.lines().any(|line| line == "remaining,0"),
        "{explained}"
    );
    for sql in [
        "BEGIN; CREATE INDEX by_a ON lake.main.t(a);",
        "BEGIN; DROP INDEX IF EXISTS lake.main.by_a;",
    ] {
        let error = run_ducklake_sql_expect_err(store.path(), data.path(), sql);
        assert!(error.contains("autocommit"), "{error}");
    }
    let result = run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE INDEX by_a ON lake.main.t(a);
         EXPLAIN DROP INDEX lake.main.by_a;
         SELECT 'remaining', count(*) FROM moraine_indexes('lake','main','t');",
    );
    assert!(result.lines().any(|line| line == "remaining,1"), "{result}");
    let error = run_ducklake_sql_expect_err(
        store.path(),
        data.path(),
        "BEGIN; DROP INDEX lake.main.by_a;",
    );
    assert!(error.contains("autocommit"), "{error}");
    run_ducklake_sql(store.path(), data.path(), "DROP INDEX lake.main.by_a;");
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn native_indexes_validate_definitions_and_enforce_uniqueness() {
    let store = TempDir::new("native-validation-store");
    let data = TempDir::new("native-validation-data");
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT, b BIGINT); INSERT INTO lake.main.t VALUES (1,2);",
    );
    for (sql, expected) in [
        ("CREATE INDEX bad ON lake.main.t((a + 1));", "stored column"),
        (
            "CREATE INDEX bad ON lake.main.t(a) WITH (unknown=true);",
            "Unknown Moraine index option",
        ),
        (
            "CREATE INDEX bad ON lake.main.t(a) WITH (staged=true, step_entries=0);",
            "greater than zero",
        ),
        (
            "CREATE UNIQUE INDEX bad ON lake.main.t(a) WITH (maintenance='deferred');",
            "non-unique",
        ),
        (
            "CREATE INDEX bad ON lake.main.t(a,b) WITH (directions='[desc]');",
            "one entry per column",
        ),
        (
            "CREATE INDEX bad ON lake.main.t(a) WITH (directions='[sideways]');",
            "entries must be",
        ),
        (
            "CREATE INDEX bad ON lake.main.t(a) WITH (nulls='[NULL]');",
            "cannot be NULL",
        ),
        (
            "CREATE INDEX bad ON lake.main.t(a) WITH (nulls='[]');",
            "one entry per column",
        ),
        ("CREATE INDEX bad ON lake.main.t(missing);", "missing"),
        ("DROP INDEX lake.main.missing;", "missing"),
    ] {
        let error = run_ducklake_sql_expect_err(store.path(), data.path(), sql);
        assert!(error.contains(expected), "{sql}: {error}");
    }
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE UNIQUE INDEX by_ab ON lake.main.t(a,b);",
    );
    let error = run_ducklake_sql_expect_err(
        store.path(),
        data.path(),
        "INSERT INTO lake.main.t VALUES (1,2);",
    );
    assert!(error.contains("duplicate value"), "{error}");
    let count = run_ducklake_sql(
        store.path(),
        data.path(),
        "SELECT count(*) FROM lake.main.t;",
    );
    assert_eq!(csv_rows(&count), vec![vec!["1"]]);
    for sql in [
        "CREATE INDEX other ON lake.main.t(a);",
        "DROP INDEX IF EXISTS lake.main.by_ab;",
    ] {
        let error = run_ducklake_read_only_sql_expect_err(store.path(), data.path(), sql);
        assert!(error.to_ascii_lowercase().contains("read-only"), "{error}");
    }
}
