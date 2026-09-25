//! `moraine_rows_at` and the located update recipe it completes.

use std::fmt::Write as _;

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
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn located_rows_prepare_defers_strict_position_checks() {
    let fixture = Fixture::new(0);
    let prepare = "PREPARE located AS SELECT * FROM moraine_rows_at('lake','main','t',
        [{row_id: 999999::BIGINT, data_file_id: NULL::UBIGINT}]);";
    fixture.run(prepare);
    let output = run_session(
        &Attach::Moraine {
            store_dir: fixture.store.path(),
            data_path: fixture.data.path(),
            options: &fixture.options,
            read_only: false,
        },
        &format!("{prepare} EXECUTE located;"),
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not a live inlined row"));
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn located_rows_and_summary_scans_parallelize_one_files_row_groups() {
    for inline_limit in [0, 100_000] {
        let fixture = Fixture::new(inline_limit);
        let query = "SELECT 'answer',count(*),sum(length(payload)),sum(row_id),count(DISTINCT row_id) FROM moraine_rows_at(
        'lake','main','t',getvariable('located'))";
        let join = "SELECT 'answer',count(*),sum(length(data.payload)),sum(data.rowid),count(DISTINCT data.rowid) FROM lake.main.t data
        JOIN moraine_index_lookup('lake','main','t','by_a',0) hits
        ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id";
        let mut sql = "DROP TABLE lake.main.t;
        CALL ducklake_set_option('lake','parquet_row_group_size','2048');
        CREATE TABLE lake.main.t AS SELECT i // 8192 a,
            CASE WHEN i % 2=0 THEN repeat('x',100) END payload FROM range(65536) r(i);
        CALL ducklake_flush_inlined_data('lake');
        CALL moraine_index_create('lake','main','t','by_a',['a'],false);
        SET VARIABLE located = (SELECT list({row_id:row_id,data_file_id:data_file_id})
            FROM moraine_index_lookup('lake','main','t','by_a',0));
        SET threads=4;"
            .to_owned();
        for threads in [1, 2] {
            write!(
                sql,
                "SET moraine_summary_scan_threads={threads};
            EXPLAIN ANALYZE {query}; {query}; EXPLAIN ANALYZE {join}; {join};"
            )
            .unwrap();
        }
        sql.push_str("SELECT 'answer',count(*),sum(length(payload)),sum(rowid),count(DISTINCT rowid) FROM lake.main.t WHERE a=0;");
        for _ in 0..8 {
            sql.push_str("SELECT payload FROM moraine_rows_at('lake','main','t',getvariable('located')) LIMIT 1;");
        }
        let result = fixture.run(&sql);
        let answers: Vec<_> = csv_rows(&result)
            .into_iter()
            .filter(|row| row[0] == "answer")
            .collect();
        assert_eq!(
            answers,
            vec![vec!["answer", "8192", "409600", "33550336", "8192"]; 5]
        );
        if std::thread::available_parallelism().is_ok_and(|cores| cores.get() >= 4) {
            assert!(
                result.contains("Peak read workers: 1") || result.contains("Peak read workers: 2"),
                "{result}"
            );
        }
        assert!(result.contains("Total Files Read: 1"), "{result}");
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
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
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
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
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
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
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
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
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
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
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
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
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
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

/// Locating a deletion's positions reports its phases and the deletion
/// backlog it had to read back, which grows with the table's history rather
/// than with the rows being deleted. At `info`, so a caller reads it without
/// turning on the per-request chatter `debug` carries.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn locating_positions_reports_its_phases_and_backlog() {
    let fixture = Fixture::new(0);
    fixture.run(&format!(
        "{} CALL moraine_delete_located('lake', 'main', 't', getvariable('located'));",
        Fixture::locate("1")
    ));
    let output = run_session_with_env(
        &Attach::Moraine {
            store_dir: fixture.store.path(),
            data_path: fixture.data.path(),
            options: &fixture.options,
            read_only: false,
        },
        &format!(
            "CALL enable_logging(level => 'info', storage => 'memory');
             {} CALL moraine_delete_located('lake', 'main', 't', getvariable('located'));
             SELECT 'split', count(*) FROM duckdb_logs WHERE type='moraine'
               AND message LIKE '%located row positions%pairs=1%existing_positions=1%positioning_ms=%existing_ms=%';",
            Fixture::locate("3")
        ),
        &[("MORAINE_LOG", "info")],
    );
    let result = combined_output(&output);
    assert!(output.status.success(), "{result}");
    assert!(
        csv_rows(&result).contains(&vec!["split".into(), "1".into()]),
        "{result}"
    );
}
