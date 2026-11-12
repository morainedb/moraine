use crate::helpers::*;

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn rewrite_data_path_reopens_copied_parquet_and_preserves_history() {
    let catalog = TempDir::new("rewrite-catalog");
    let source_catalog = TempDir::new("rewrite-source-catalog");
    let source = TempDir::new("rewrite-source");
    let destination = TempDir::new("rewrite-destination");
    let inserted = csv_rows(&run_ducklake_sql(
        source_catalog.path(),
        source.path(),
        "CALL ducklake_set_option('lake', 'data_inlining_row_limit', 0);
         CREATE TABLE lake.main.t(id BIGINT);
         INSERT INTO lake.main.t VALUES (1), (2);
         SELECT max(snapshot_id) FROM ducklake_snapshots('lake');
         DELETE FROM lake.main.t WHERE id = 2;",
    ));
    let snapshot = &inserted[0][0];
    copy_directory(source_catalog.path(), catalog.path());
    copy_directory(source.path(), destination.path());
    std::fs::remove_dir_all(source.path()).unwrap();
    let sql = format!(
        "CALL moraine_rewrite_data_path('{}', '{}', '{}');",
        catalog.path().display(),
        source.path().display(),
        destination.path().display()
    );
    assert_session_ok(run_unattached(&sql), "rewrite", &sql);
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            catalog.path(),
            destination.path(),
            "SELECT id FROM lake.main.t;"
        )),
        vec![vec!["1"]]
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            catalog.path(),
            destination.path(),
            &format!("SELECT id FROM lake.main.t AT (VERSION => {snapshot}) ORDER BY id;")
        )),
        vec![vec!["1"], vec!["2"]]
    );
    run_ducklake_sql(
        catalog.path(),
        destination.path(),
        "INSERT INTO lake.main.t VALUES (3);",
    );
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            catalog.path(),
            destination.path(),
            "SELECT id FROM lake.main.t ORDER BY id;"
        )),
        vec![vec!["1"], vec!["3"]]
    );
    let refused = run_unattached(&sql);
    assert!(!refused.status.success());
}

fn copy_directory(source: &std::path::Path, destination: &std::path::Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_directory(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn rewrite_data_path_rejects_invalid_requests() {
    let catalog = TempDir::new("rewrite-invalid-catalog");
    let data = TempDir::new("rewrite-invalid-data");
    run_ducklake_sql(
        catalog.path(),
        data.path(),
        "CREATE TABLE lake.main.t(id BIGINT);",
    );
    for (before, old, new) in [
        ("", "NULL".to_owned(), "'/new'".to_owned()),
        ("", "''".to_owned(), "'/new'".to_owned()),
        ("", "'/wrong'".to_owned(), "'/new'".to_owned()),
        (
            "",
            format!("'{}'", data.path().display()),
            format!("'{}'", catalog.path().display()),
        ),
        (
            "BEGIN;",
            format!("'{}'", data.path().display()),
            "'/new'".to_owned(),
        ),
    ] {
        let sql = format!(
            "{before} CALL moraine_rewrite_data_path('{}', {old}, {new});",
            catalog.path().display()
        );
        let output = run_unattached(&sql);
        assert!(!output.status.success(), "{sql}");
    }
    run_ducklake_sql(catalog.path(), data.path(), "SELECT * FROM lake.main.t;");
}
