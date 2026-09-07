use crate::helpers::*;

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
