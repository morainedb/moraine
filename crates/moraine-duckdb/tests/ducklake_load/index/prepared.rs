use crate::helpers::*;

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extensions"]
fn prepared_index_reads_reuse_scoped_probes() {
    for parameterized in [false, true] {
        for joined in [false, true] {
            for present in [false, true] {
                for (kind, read, arguments) in reads(parameterized) {
                    let store = TempDir::new("index-prepared-reuse");
                    let data = TempDir::new("index-prepared-reuse-data");
                    let options = format!(
                        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 10",
                        data.path().display()
                    );
                    let from = if joined {
                        format!(
                            "lake.main.t data JOIN {read} hits ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id"
                        )
                    } else {
                        read
                    };
                    let insert = if present {
                        "INSERT INTO lake.main.t VALUES (2),(NULL);"
                    } else {
                        ""
                    };
                    let result = run_session_with_env(&Attach::Moraine { store_dir: store.path(), data_path: data.path(), options: &options, read_only: false }, &format!(
            "CREATE TABLE lake.main.t(a BIGINT);
             {insert}
             CALL moraine_index_create('lake','main','t','by_a',['a'],false);
             CALL enable_logging(level => 'debug', storage => 'memory');
             PREPARE probe AS SELECT 'hits', count(*) FROM {from};
             SELECT 'resolutions', count(*) FROM duckdb_logs WHERE type='moraine' AND message LIKE '%index probe resolved%';
             EXECUTE probe{arguments};
             SELECT 'resolutions', count(*) FROM duckdb_logs WHERE type='moraine' AND message LIKE '%index probe resolved%';
             EXECUTE probe{arguments};
             SELECT 'resolutions', count(*) FROM duckdb_logs WHERE type='moraine' AND message LIKE '%index probe resolved%';
             SELECT 'binds', count(*) FROM duckdb_logs WHERE type='moraine' AND message LIKE '%index bind resolved%';
             DEALLOCATE probe;"
        ), &[("MORAINE_LOG", "debug")]);
                    let output = combined_output(&result);
                    assert!(result.status.success(), "{output}");
                    let counts: Vec<_> = csv_rows(&output)
                        .into_iter()
                        .filter(|row| row[0] == "resolutions")
                        .collect();
                    assert_eq!(
                        counts,
                        vec![
                            vec!["resolutions", if parameterized { "0" } else { "1" }],
                            vec!["resolutions", "1"],
                            vec!["resolutions", "1"]
                        ],
                        "{kind}, joined={joined}, present={present}, parameterized={parameterized}: {output}"
                    );
                    if !joined && !parameterized {
                        assert!(
                            csv_rows(&output).contains(&vec!["binds".into(), "1".into()]),
                            "{output}"
                        );
                    }
                    let hits: Vec<_> = csv_rows(&output)
                        .into_iter()
                        .filter(|row| row[0] == "hits")
                        .collect();
                    assert_eq!(
                        hits,
                        vec![vec!["hits", if present { "1" } else { "0" }]; 2],
                        "{kind}: {output}"
                    );
                }
            }
        }
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extensions"]
fn prepared_index_arguments_depending_on_session_state_are_rebound() {
    let store = TempDir::new("index-prepared-dynamic");
    let data = TempDir::new("index-prepared-dynamic-data");
    let options = format!(", META_DATA_PATH '{}'", data.path().display());
    let output = run_ducklake_sql_with_options(store.path(), data.path(), &options,
        "CREATE TABLE lake.main.t(a BIGINT);
         INSERT INTO lake.main.t VALUES (1),(2);
         CALL moraine_index_create('lake','main','t','by_a',['a'],false);
         SET VARIABLE key=1;
         CREATE MACRO current_key() AS getvariable('key');
         CREATE VIEW dynamic_hits AS SELECT * FROM moraine_index_lookup('lake','main','t','by_a',getvariable('key'));
         PREPARE variable_probe AS SELECT 'variable', row_id FROM moraine_index_lookup('lake','main','t','by_a',getvariable('key'));
         PREPARE macro_probe AS SELECT 'macro', row_id FROM moraine_index_lookup('lake','main','t','by_a',current_key());
         PREPARE view_probe AS SELECT 'view', row_id FROM dynamic_hits;
         EXECUTE variable_probe;
         EXECUTE macro_probe;
         EXECUTE view_probe;
         SET VARIABLE key=2;
         EXECUTE variable_probe;
         EXECUTE macro_probe;
         EXECUTE view_probe;
         DEALLOCATE variable_probe;
         DEALLOCATE macro_probe;
         DEALLOCATE view_probe;");
    for phase in ["variable", "macro", "view"] {
        let rows: Vec<_> = csv_rows(&output)
            .into_iter()
            .filter(|row| row[0] == phase)
            .collect();
        assert_eq!(rows, vec![vec![phase, "0"], vec![phase, "1"]], "{output}");
    }
}

/// Replacement attachments invalidate plans; manifest readers conservatively
/// rebind.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extensions"]
fn prepared_index_reads_invalidate_on_reattach_and_refuse_reader_caching() {
    let store = TempDir::new("index-prepared-reattach");
    let data = TempDir::new("index-prepared-reattach-data");
    let options = format!(", META_DATA_PATH '{}'", data.path().display());
    let output = run_ducklake_sql_with_options(store.path(), data.path(), &options, &format!(
        "CREATE TABLE lake.main.t(a BIGINT);
         INSERT INTO lake.main.t VALUES (2);
         CALL moraine_index_create('lake','main','t','by_a',['a'],false);
         PREPARE probe AS SELECT 'hits', count(*) FROM moraine_index_lookup('lake','main','t','by_a',2);
         EXECUTE probe;
         DETACH lake;
         ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}', META_DATA_PATH '{}');
         INSERT INTO lake.main.t VALUES (2);
         EXECUTE probe;
         DEALLOCATE probe;", store.path().display(), data.path().display(), data.path().display()));
    let hits: Vec<_> = csv_rows(&output)
        .into_iter()
        .filter(|row| row[0] == "hits")
        .collect();
    assert_eq!(hits, vec![vec!["hits", "1"], vec!["hits", "2"]], "{output}");

    let output = run_session_with_env(&Attach::Moraine { store_dir: store.path(), data_path: data.path(), options: &options, read_only: true },
        "CALL enable_logging(level => 'debug', storage => 'memory');
         PREPARE probe AS SELECT 'hits', count(*) FROM moraine_index_lookup('lake','main','t','by_a',2);
         EXECUTE probe;
         EXECUTE probe;
         SELECT 'probes', count(*) FROM duckdb_logs WHERE type='moraine' AND message LIKE '%index probe resolved%';",
         &[("MORAINE_LOG", "debug")]);
    assert!(output.status.success(), "{}", combined_output(&output));
    let rows = csv_rows(&combined_output(&output));
    assert!(
        rows.contains(&vec!["probes".into(), "3".into()]),
        "{rows:?}"
    );
    assert_eq!(rows.iter().filter(|row| row[0] == "hits").count(), 2);
    assert!(
        rows.iter()
            .filter(|row| row[0] == "hits")
            .all(|row| row[1] == "2")
    );
}

/// Each read uses a literal or the same parameter values on every execution.
fn reads(parameterized: bool) -> [(&'static str, String, &'static str); 4] {
    let key = if parameterized { "$1" } else { "2" };
    let keys = if parameterized { "$1" } else { "[2]" };
    let null = if parameterized { "$1" } else { "NULL" };
    [
        (
            "point",
            format!("moraine_index_lookup('lake','main','t','by_a',{key})"),
            if parameterized { "(2)" } else { "" },
        ),
        (
            "in",
            format!("moraine_index_in('lake','main','t','by_a',{keys})"),
            if parameterized { "([2])" } else { "" },
        ),
        (
            "range",
            format!("moraine_index_range('lake','main','t','by_a',{key},{key},true,true)"),
            if parameterized { "(2)" } else { "" },
        ),
        (
            "nulls",
            format!("moraine_index_nulls('lake','main','t','by_a',{null})"),
            if parameterized { "(NULL)" } else { "" },
        ),
    ]
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extensions"]
fn prepared_index_reads_refresh_after_inserts_updates_and_deletes() {
    for limit in [0, 10] {
        for parameterized in [false, true] {
            for (kind, read, arguments) in reads(parameterized) {
                let store = TempDir::new("index-prepared-mutations");
                let data = TempDir::new("index-prepared-mutations-data");
                let options = format!(
                    ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {limit}",
                    data.path().display()
                );
                let result = run_ducklake_sql_with_options(
                    store.path(),
                    data.path(),
                    &options,
                    &format!(
                        "CREATE TABLE lake.main.t(a BIGINT);
                         INSERT INTO lake.main.t VALUES (1);
                         CALL moraine_index_create('lake','main','t','by_a',['a'],false);
                         PREPARE probe AS SELECT 'probe' AS phase, count(*) AS hits FROM {read};
                         EXECUTE probe{arguments};
                         INSERT INTO lake.main.t VALUES (2),(NULL);
                         EXECUTE probe{arguments};
                         PREPARE present_probe AS SELECT 'present' AS phase, count(*) AS hits FROM {read};
                         EXECUTE present_probe{arguments};
                         UPDATE lake.main.t SET a=3 WHERE a=2 OR a IS NULL;
                         EXECUTE present_probe{arguments};
                         EXECUTE probe{arguments};
                         INSERT INTO lake.main.t VALUES (2),(NULL);
                         EXECUTE present_probe{arguments};
                         EXECUTE probe{arguments};
                         DELETE FROM lake.main.t WHERE a=2 OR a IS NULL;
                         EXECUTE present_probe{arguments};
                         EXECUTE probe{arguments};"
                    ),
                );
                let counts: Vec<_> = csv_rows(&result)
                    .into_iter()
                    .filter(|row| row.first().is_some_and(|phase| phase == "probe"))
                    .map(|row| row.into_iter().skip(1).collect::<Vec<_>>())
                    .collect();
                assert_eq!(
                    counts,
                    vec![vec!["0"], vec!["1"], vec!["0"], vec!["1"], vec!["0"]],
                    "{kind}, limit={limit}, parameterized={parameterized}"
                );
                let present: Vec<_> = csv_rows(&result)
                    .into_iter()
                    .filter(|row| row.first().is_some_and(|phase| phase == "present"))
                    .map(|row| row.into_iter().skip(1).collect::<Vec<_>>())
                    .collect();
                assert_eq!(
                    present,
                    vec![vec!["1"], vec!["0"], vec!["1"], vec!["0"]],
                    "{kind}, limit={limit}, parameterized={parameterized}"
                );
            }
        }
    }
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extensions"]
fn prepared_located_joins_refresh_rows_and_file_filters() {
    for limit in [0, 10] {
        for parameterized in [false, true] {
            for (kind, read, arguments) in reads(parameterized) {
                let store = TempDir::new("index-prepared-locations");
                let data = TempDir::new("index-prepared-locations-data");
                let options = format!(
                    ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {limit}",
                    data.path().display()
                );
                let result = run_ducklake_sql_with_options(
                    store.path(),
                    data.path(),
                    &options,
                    &format!(
                        "CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);
                         INSERT INTO lake.main.t VALUES (1,'unrelated');
                         CALL moraine_index_create('lake','main','t','by_a',['a'],false);
                         PREPARE located AS
                           SELECT 'probe' AS phase, coalesce(string_agg(data.b,',' ORDER BY data.b),'missing') AS matched
                           FROM lake.main.t data JOIN {read} hits
                             ON data.rowid=hits.row_id
                            AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id;
                         EXECUTE located{arguments};
                         INSERT INTO lake.main.t VALUES (2,'before'),(NULL,'before');
                         EXECUTE located{arguments};
                         PREPARE holding_file AS
                           SELECT 'file' AS phase, coalesce(data_file_id::VARCHAR,'inline') AS file
                           FROM {read};
                         EXECUTE holding_file{arguments};
                         UPDATE lake.main.t SET b='updated' WHERE a=2 OR a IS NULL;
                         EXECUTE located{arguments};
                         CALL ducklake_flush_inlined_data('lake');
                         EXECUTE located{arguments};
                         EXECUTE holding_file{arguments};
                         DELETE FROM lake.main.t WHERE a=1;
                         CALL ducklake_rewrite_data_files('lake', delete_threshold => 0.01);
                         CALL ducklake_merge_adjacent_files('lake');
                         EXECUTE located{arguments};
                         DELETE FROM lake.main.t WHERE a=2 OR a IS NULL;
                         EXECUTE located{arguments};"
                    ),
                );
                let matched: Vec<_> = csv_rows(&result)
                    .into_iter()
                    .filter(|row| row.first().is_some_and(|phase| phase == "probe"))
                    .map(|row| row.into_iter().skip(1).collect::<Vec<_>>())
                    .collect();
                assert_eq!(
                    matched,
                    vec![
                        vec!["missing"],
                        vec!["before"],
                        vec!["updated"],
                        vec!["updated"],
                        vec!["updated"],
                        vec!["missing"]
                    ],
                    "{kind}, limit={limit}, parameterized={parameterized}"
                );
                let files: Vec<_> = csv_rows(&result)
                    .into_iter()
                    .filter(|row| row.first().is_some_and(|phase| phase == "file"))
                    .collect();
                assert_eq!(
                    files.len(),
                    2,
                    "{kind}, limit={limit}, parameterized={parameterized}"
                );
                assert_ne!(
                    files[0][1], files[1][1],
                    "the prepared read retained its old file: {kind}, limit={limit}, parameterized={parameterized}"
                );
            }
        }
    }
}

/// A transaction that has already written keeps its index probes pinned at
/// the revision it started on, so prepared reads reuse them until commit.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged extensions"]
fn prepared_index_reads_reuse_probes_inside_a_writing_transaction() {
    for limit in [0, 10] {
        let store = TempDir::new("index-prepared-writing-tx");
        let data = TempDir::new("index-prepared-writing-tx-data");
        let options = format!(
            ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {limit}",
            data.path().display()
        );
        let joined = "lake.main.t data JOIN moraine_index_in('lake','main','t','by_a',[1,2,3]) hits \
             ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id";
        let counts = "SELECT 'lookups', count(*) FROM duckdb_logs WHERE type='moraine' AND message LIKE '%index lookup resolved%';";
        let result = run_session_with_env(
            &Attach::Moraine {
                store_dir: store.path(),
                data_path: data.path(),
                options: &options,
                read_only: false,
            },
            &format!(
                "CREATE TABLE lake.main.t(a BIGINT);
                 INSERT INTO lake.main.t VALUES (1),(2),(3);
                 CALL moraine_index_create('lake','main','t','by_a',['a'],false);
                 CALL enable_logging(level => 'debug', storage => 'memory');
                 BEGIN;
                 INSERT INTO lake.main.t VALUES (9);
                 PREPARE probe AS SELECT 'hits', count(*) FROM {joined};
                 {counts}
                 EXECUTE probe;
                 {counts}
                 DELETE FROM lake.main.t WHERE a = 2;
                 EXECUTE probe;
                 {counts}
                 COMMIT;
                 EXECUTE probe;
                 {counts}"
            ),
            &[("MORAINE_LOG", "debug")],
        );
        let output = combined_output(&result);
        assert!(result.status.success(), "{output}");
        let rows = csv_rows(&output);
        let lookups: Vec<_> = rows
            .iter()
            .filter(|row| row[0] == "lookups")
            .map(|row| row[1].as_str())
            .collect();
        assert_eq!(lookups, vec!["1", "1", "1", "2"], "limit={limit}: {output}");
        let hits: Vec<_> = rows
            .iter()
            .filter(|row| row[0] == "hits")
            .map(|row| row[1].as_str())
            .collect();
        assert_eq!(hits, vec!["3", "2", "2"], "limit={limit}: {output}");
    }
}
