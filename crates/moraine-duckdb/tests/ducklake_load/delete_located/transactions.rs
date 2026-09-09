use std::process::Command;

use crate::helpers::*;

struct Fixture {
    store: TempDir,
    data: TempDir,
    options: String,
}

impl Fixture {
    fn new(inline_limit: u64) -> Self {
        let store = TempDir::new("located-transaction-store");
        let data = TempDir::new("located-transaction-data");
        let options = format!(
            ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {inline_limit}",
            data.path().display()
        );
        let fixture = Self {
            store,
            data,
            options,
        };
        fixture.run(
            "CREATE TABLE lake.main.t(a BIGINT NOT NULL);
             INSERT INTO lake.main.t VALUES (1), (2), (3);
             CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);",
        );
        fixture
    }

    fn run(&self, sql: &str) -> String {
        run_ducklake_sql_with_options(self.store.path(), self.data.path(), &self.options, sql)
    }

    fn snapshot(&self) -> u64 {
        csv_rows(&self.run("SELECT max(snapshot_id) FROM ducklake_snapshots('lake');"))[0][0]
            .parse()
            .unwrap()
    }

    fn deletion(&self, value: i64) -> String {
        let rows = csv_rows(&self.run(&format!(
            "SELECT row_id, data_file_id FROM moraine_index_lookup('lake', 'main', 't', 'by_a', {value});"
        )));
        assert_eq!(rows.len(), 1);
        let file = if rows[0][1].is_empty() {
            "NULL"
        } else {
            &rows[0][1]
        };
        format!(
            "CALL moraine_delete_located('lake', 'main', 't', [{{row_id: {}::BIGINT, data_file_id: {file}::UBIGINT}}]);",
            rows[0][0]
        )
    }

    fn assert_original_rows(&self) {
        assert_eq!(
            csv_rows(&self.run("SELECT a FROM lake.main.t ORDER BY a;")),
            vec![vec!["1"], vec!["2"], vec!["3"]],
        );
        assert_eq!(
            csv_rows(&self.run(
                "SELECT count(DISTINCT row_id) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 1);"
            )),
            vec![vec!["1"]],
        );
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn located_deletions_roll_back_with_the_outer_transaction() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        let before = fixture.snapshot();
        let deletion = fixture.deletion(1);
        let files_before = parquet_files_under(fixture.data.path());
        fixture.run(&format!("BEGIN; {deletion} ROLLBACK;"));
        assert_eq!(parquet_files_under(fixture.data.path()), files_before);
        fixture.assert_original_rows();
        assert_eq!(fixture.snapshot(), before);
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn failed_replacements_preserve_the_located_rows() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        let before = fixture.snapshot();
        let deletion = fixture.deletion(1);
        let output = Command::new(cli_path())
            .args(["-unsigned", "-csv", "-c"])
            .arg(format!(
                "{} ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}'{});
                 BEGIN; {deletion}",
                load_statement(),
                fixture.store.path().display(),
                fixture.data.path().display(),
                fixture.options,
            ))
            .args([
                "-c",
                "INSERT INTO lake.main.t VALUES (NULL);",
                "-c",
                "ROLLBACK;",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("NOT NULL constraint failed"), "{error}");
        fixture.assert_original_rows();
        assert_eq!(fixture.snapshot(), before);
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn located_deletions_and_replacements_commit_together() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        let before = fixture.snapshot();
        let first = fixture.deletion(1);
        let second = fixture.deletion(2);
        fixture.run(&format!(
            "BEGIN; {first} {first} {second}
             SELECT CASE WHEN count(*)=1 THEN true ELSE error('pending deletions are not visible') END FROM lake.main.t;
             INSERT INTO lake.main.t VALUES (10), (20);
             COMMIT;"
        ));
        assert_eq!(
            csv_rows(&fixture.run("SELECT a FROM lake.main.t ORDER BY a;")),
            vec![vec!["3"], vec!["10"], vec!["20"]],
        );
        assert_eq!(fixture.snapshot(), before + 1);
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn prepared_located_deletions_follow_the_execution_transaction() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        let deletion = fixture.deletion(1).replacen("CALL ", "SELECT * FROM ", 1);
        fixture.run(&format!(
            "PREPARE remove_row AS {deletion} BEGIN; EXECUTE remove_row; ROLLBACK;"
        ));
        fixture.assert_original_rows();
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn located_and_sql_deletions_share_pending_changes() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        let first = fixture.deletion(1);
        let second = fixture.deletion(2);
        fixture.run(&format!(
            "BEGIN; DELETE FROM lake.main.t WHERE a = 1; {first} {second}
             INSERT INTO lake.main.t VALUES (1), (2); COMMIT;"
        ));
        fixture.assert_original_rows();
        fixture.run(&format!(
            "BEGIN; {} DELETE FROM lake.main.t WHERE a = 2; ROLLBACK;",
            fixture.deletion(1),
        ));
        fixture.assert_original_rows();
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn located_deletions_preserve_committed_deletes_and_inline_schema_versions() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        fixture.run(&fixture.deletion(1));
        fixture.run("ALTER TABLE lake.main.t ADD COLUMN b BIGINT DEFAULT 7; INSERT INTO lake.main.t(a) VALUES (4);");
        let before = fixture.snapshot();
        fixture.run(&format!(
            "BEGIN; {} {} INSERT INTO lake.main.t(a) VALUES (2), (4); COMMIT;",
            fixture.deletion(2),
            fixture.deletion(4),
        ));
        assert_eq!(
            csv_rows(&fixture.run("SELECT a,b FROM lake.main.t ORDER BY a;")),
            vec![vec!["2", "7"], vec!["3", "7"], vec!["4", "7"]]
        );
        assert_eq!(fixture.snapshot(), before + 1);
        assert_eq!(csv_rows(&fixture.run("SELECT count(DISTINCT row_id) FROM moraine_index_lookup('lake','main','t','by_a',2);")), vec![vec!["1"]]);
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn located_file_deletes_do_not_duplicate_inline_deletion_records() {
    for commit_first in [false, true] {
        let mut fixture = Fixture::new(0);
        fixture.options = fixture
            .options
            .replace("DATA_INLINING_ROW_LIMIT 0", "DATA_INLINING_ROW_LIMIT 1024");
        let first = fixture.deletion(1);
        let second = fixture.deletion(2);
        if commit_first {
            fixture.run("DELETE FROM lake.main.t WHERE a=1;");
            fixture.run(&format!("BEGIN; {first} {second} COMMIT;"));
        } else {
            fixture.run(&format!(
                "BEGIN; DELETE FROM lake.main.t WHERE a=1; {first} {second} COMMIT;"
            ));
        }
        assert_eq!(
            csv_rows(&fixture.run("SELECT count(*) FROM lake.main.t;")),
            vec![vec!["1"]]
        );
        assert_eq!(
            csv_rows(&fixture.run("SELECT a FROM lake.main.t ORDER BY a;")),
            vec![vec!["3"]]
        );
    }
}
