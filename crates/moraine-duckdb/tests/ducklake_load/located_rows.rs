//! `moraine_rows_at` and the located update recipe it completes.

use crate::helpers::*;

struct Fixture {
    store: TempDir,
    data: TempDir,
    options: String,
}

impl Fixture {
    fn new(inline_limit: u64) -> Self {
        let store = TempDir::new("located-rows-store");
        let data = TempDir::new("located-rows-data");
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
            "CREATE TABLE lake.main.t(a BIGINT NOT NULL, b VARCHAR);
             INSERT INTO lake.main.t VALUES (1, 'x'), (2, 'y'), (3, 'z');
             CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);",
        );
        fixture
    }

    fn run(&self, sql: &str) -> String {
        run_ducklake_sql_with_options(self.store.path(), self.data.path(), &self.options, sql)
    }

    /// The `SET VARIABLE` naming the located rows of `values`.
    fn locate(values: &str) -> String {
        format!(
            "SET VARIABLE located = (SELECT list({{row_id: row_id, data_file_id: data_file_id}}) \
             FROM moraine_index_in('lake', 'main', 't', 'by_a', [{values}]));"
        )
    }

    fn rows(&self) -> Vec<Vec<String>> {
        csv_rows(&self.run("SELECT a, b FROM lake.main.t ORDER BY a;"))
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn located_rows_read_back_whole_with_their_locators() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        let rows = csv_rows(&fixture.run(&format!(
            "{} SELECT a, b, row_id, data_file_id IS NULL FROM \
             moraine_rows_at('lake', 'main', 't', getvariable('located')) ORDER BY a;",
            Fixture::locate("1, 3")
        )));
        let inlined = if inline_limit > 0 { "true" } else { "false" };
        assert_eq!(
            rows,
            vec![vec!["1", "x", "0", inlined], vec!["3", "z", "2", inlined]]
        );
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn a_partial_update_recipe_commits_as_one_transaction() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        fixture.run(&format!(
            "{} BEGIN;
             CALL moraine_delete_located('lake', 'main', 't', getvariable('located'));
             INSERT INTO lake.main.t SELECT a, b || '!' FROM \
             moraine_rows_at('lake', 'main', 't', getvariable('located'));
             COMMIT;",
            Fixture::locate("1, 3")
        ));
        assert_eq!(
            fixture.rows(),
            vec![vec!["1", "x!"], vec!["2", "y"], vec!["3", "z!"]]
        );
        assert_eq!(
            csv_rows(&fixture.run(
                "SELECT count(DISTINCT row_id) FROM moraine_index_in('lake', 'main', 't', 'by_a', [1, 3]);"
            )),
            vec![vec!["2"]],
        );
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn a_rolled_back_recipe_leaves_the_rows_untouched() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        fixture.run(&format!(
            "{} BEGIN;
             CALL moraine_delete_located('lake', 'main', 't', getvariable('located'));
             INSERT INTO lake.main.t SELECT a, b || '!' FROM \
             moraine_rows_at('lake', 'main', 't', getvariable('located'));
             ROLLBACK;",
            Fixture::locate("1, 3")
        ));
        assert_eq!(
            fixture.rows(),
            vec![vec!["1", "x"], vec!["2", "y"], vec!["3", "z"]]
        );
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn a_column_added_after_the_rows_were_written_reads_null() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        let rows = csv_rows(&fixture.run(&format!(
            "ALTER TABLE lake.main.t ADD COLUMN c DOUBLE;
             {} SELECT a, b, c IS NULL FROM \
             moraine_rows_at('lake', 'main', 't', getvariable('located')) ORDER BY a;",
            Fixture::locate("2")
        )));
        assert_eq!(rows, vec![vec!["2", "y", "true"]]);
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn a_located_update_applies_its_assignments_in_one_statement() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        let ids_before = csv_rows(&fixture.run("SELECT rowid, a FROM lake.main.t ORDER BY a;"));
        let counts = csv_rows(&fixture.run(&format!(
            "{} SELECT file_rows_deleted + inline_rows_deleted, rows_inserted FROM \
             moraine_update('lake', 'main', 't', getvariable('located'), 'b = b || ''!''');",
            Fixture::locate("1, 3")
        )));
        assert_eq!(counts, vec![vec!["2", "2"]]);
        assert_eq!(
            fixture.rows(),
            vec![vec!["1", "x!"], vec!["2", "y"], vec!["3", "z!"]]
        );
        // The rows keep their ids, as an UPDATE's would.
        assert_eq!(
            csv_rows(&fixture.run("SELECT rowid, a FROM lake.main.t ORDER BY a;")),
            ids_before
        );
        assert_eq!(
            csv_rows(&fixture.run(
                "SELECT count(DISTINCT row_id) FROM moraine_index_in('lake', 'main', 't', 'by_a', [1, 3]);"
            )),
            vec![vec!["2"]],
        );
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn a_located_update_rolls_back_with_its_transaction() {
    for inline_limit in [0, 1024] {
        let fixture = Fixture::new(inline_limit);
        fixture.run(&format!(
            "{} BEGIN;
             CALL moraine_update('lake', 'main', 't', getvariable('located'), 'b = b || ''!''');
             ROLLBACK;",
            Fixture::locate("1, 3")
        ));
        assert_eq!(
            fixture.rows(),
            vec![vec!["1", "x"], vec!["2", "y"], vec!["3", "z"]]
        );
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine and patched DuckLake extensions"]
fn an_assignment_to_an_unknown_column_is_refused() {
    let fixture = Fixture::new(0);
    let output = run_ducklake_sql_output(
        fixture.store.path(),
        fixture.data.path(),
        &fixture.options,
        &format!(
            "{} CALL moraine_update('lake', 'main', 't', getvariable('located'), 'zz = 1');",
            Fixture::locate("1")
        ),
    );
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("has no column \"zz\""), "{error}");
    assert_eq!(
        fixture.rows(),
        vec![vec!["1", "x"], vec!["2", "y"], vec!["3", "z"]]
    );
}
