use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, OptionScope};
use object_store::local::LocalFileSystem;

use crate::helpers::*;

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn options_stream_with_fixed_facts_under_a_small_memory_budget() {
    let directory = TempDir::new("paged-options");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let catalog = Catalog::open(
            Arc::new(LocalFileSystem::new_with_prefix(directory.path()).unwrap()),
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let payload = "x".repeat(4096);
        catalog
            .commit(async |tx| {
                let schema = tx.schema_by_name("main").await.unwrap().unwrap().id;
                tx.set_option(OptionScope::Global, "created_by", "stored-shadow")
                    .await?;
                for id in 0..512 {
                    let table = tx
                        .create_table(
                            schema,
                            &format!("t_{id}"),
                            &[ColumnDef {
                                name: "i".into(),
                                column_type: "BIGINT".into(),
                                nulls_allowed: true,
                                default_value: None,
                                children: Vec::new(),
                            }],
                        )
                        .await?;
                    for option in 0..8 {
                        tx.set_option(
                            OptionScope::Table(table),
                            &format!("measure_{option}"),
                            &payload,
                        )
                        .await?;
                    }
                    if id == 0 {
                        tx.set_option(OptionScope::Table(table), "version", "table-version")
                            .await?;
                    }
                }
                Ok(())
            })
            .await
            .unwrap();
        catalog.close().await.unwrap();
    });
    let output = run_session_with_env(
        &Attach::Standalone {
            store_dir: directory.path(),
            read_only: true,
        },
        "SELECT count(*) FROM m.ducklake_metadata WHERE key LIKE 'measure_%'; SELECT value FROM m.ducklake_metadata WHERE key = 'created_by' AND scope IS NULL; SELECT value FROM m.ducklake_metadata WHERE key = 'version' AND scope = 'table';",
        &[("MORAINE_WORKING_MEMORY_BYTES", "16777216")],
    );
    assert!(output.status.success(), "{}", combined_output(&output));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("4096"), "{stdout}");
    assert!(stdout.contains("moraine"), "{stdout}");
    assert!(stdout.contains("table-version"), "{stdout}");
    assert!(!stdout.contains("stored-shadow"), "{stdout}");
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn staged_statistics_scan_stays_bounded_after_a_delete() {
    let directory = TempDir::new("paged-staged-statistics");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let first_file = std::cell::Cell::new(None);
    runtime.block_on(async {
        let catalog = Catalog::open(
            Arc::new(LocalFileSystem::new_with_prefix(directory.path()).unwrap()),
            CatalogOptions::default(),
        )
        .await
        .unwrap();
        let payload = "x".repeat(4096);
        catalog
            .commit(async |tx| {
                let schema = tx.schema_by_name("main").await.unwrap().unwrap().id;
                let table = tx
                    .create_table(
                        schema,
                        "files",
                        &[ColumnDef {
                            name: "i".into(),
                            column_type: "VARCHAR".into(),
                            nulls_allowed: true,
                            default_value: None,
                            children: Vec::new(),
                        }],
                    )
                    .await?;
                let column = tx.columns_of(table).await.unwrap()[0].id;
                for id in 0..2048 {
                    let file = tx
                        .register_data_file(
                            table,
                            moraine::DataFile {
                                path: format!("file_{id}.parquet"),
                                path_is_relative: true,
                                file_format: "parquet".into(),
                                record_count: 1,
                                file_size_bytes: 1024,
                                footer_size: 128,
                                encryption_key: None,
                                partition_values: Vec::new(),
                                column_stats: vec![moraine::FileColumnStats {
                                    column_id: column,
                                    column_size_bytes: 1024,
                                    value_count: Some(1),
                                    null_count: Some(0),
                                    min_value: Some(payload.clone()),
                                    max_value: Some(payload.clone()),
                                    contains_nan: None,
                                    extra_stats: None,
                                }],
                            },
                            &[],
                        )
                        .await?;
                    if id == 0 {
                        first_file.set(Some(file.get()));
                    }
                }
                Ok(())
            })
            .await
            .unwrap();
        catalog.close().await.unwrap();
    });
    let output = run_session_with_env(
        &Attach::Standalone {
            store_dir: directory.path(),
            read_only: false,
        },
        &format!(
            "BEGIN; DELETE FROM m.ducklake_file_column_stats WHERE data_file_id = {}; SELECT count(*) FROM m.ducklake_file_column_stats; ROLLBACK;",
            first_file.get().unwrap()
        ),
        &[("MORAINE_WORKING_MEMORY_BYTES", "16777216")],
    );
    assert!(output.status.success(), "{}", combined_output(&output));
    assert!(String::from_utf8(output.stdout).unwrap().contains("2047"));
}

/// Seeds `count` DuckLake commits whose snapshot rows each carry `extra_bytes`
/// of commit info, so the retained snapshots weigh enough to notice.
fn seed_heavy_snapshots(
    store_dir: &std::path::Path,
    data_path: &std::path::Path,
    count: usize,
    extra_bytes: usize,
) {
    use std::io::Write;

    // Fed through stdin: thousands of statements overflow an argument.
    let mut sql = format!(
        "{}\nATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}');\nCREATE TABLE lake.t (i BIGINT);\n",
        load_statement(),
        store_dir.display(),
        data_path.display()
    );
    for i in 0..count {
        sql.push_str(&format!(
            "BEGIN; CALL ducklake_set_commit_message('lake', 'seed', 'commit {i}', extra_info => repeat('x', {extra_bytes})); INSERT INTO lake.t VALUES ({i}); COMMIT;\n"
        ));
    }
    let mut child = std::process::Command::new(cli_path())
        .arg("-unsigned")
        .arg("-csv")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn the seeding duckdb CLI");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(sql.as_bytes())
        .expect("seed sql");
    let output = child.wait_with_output().expect("duckdb CLI");
    assert!(output.status.success(), "{}", combined_output(&output));
}

/// The two snapshot tables page from the pushed `snapshot_id` bound inside a
/// transaction that has staged a write, so the commit-time conflict check
/// reads the few snapshots since the transaction began rather than dumping
/// every retained one.
#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn staged_snapshot_scans_page_from_the_bound_under_a_small_budget() {
    let directory = TempDir::new("paged-staged-snapshots");
    let data = TempDir::new("paged-staged-snapshots-data");
    // 256 snapshots of 128 KiB each: a dump of them all is twice the budget.
    seed_heavy_snapshots(directory.path(), data.path(), 256, 128 * 1024);

    let queries = "SELECT count(*) FROM m.ducklake_snapshot_changes WHERE snapshot_id > 250; \
                   SELECT count(*) FROM m.ducklake_snapshot WHERE snapshot_id >= 255; \
                   SELECT string_agg(changes_made, ',') FROM m.ducklake_snapshot_changes WHERE snapshot_id > 250;";
    let committed = run_standalone_sql(directory.path(), queries);
    let staged = run_session_with_env(
        &Attach::Standalone {
            store_dir: directory.path(),
            read_only: false,
        },
        &format!(
            "BEGIN; INSERT INTO m.ducklake_tag (object_id, begin_snapshot, end_snapshot, key, value) \
             VALUES (1, 1, NULL, 'probe', 'staged'); {queries} ROLLBACK;"
        ),
        &[("MORAINE_WORKING_MEMORY_BYTES", "16777216")],
    );
    assert!(staged.status.success(), "{}", combined_output(&staged));
    let staged = String::from_utf8(staged.stdout).unwrap();
    assert_eq!(csv_rows(&staged), csv_rows(&committed));
    // The table's creation took snapshot 1 and the 256 inserts 2 through 257.
    assert_eq!(csv_rows(&committed)[0], vec!["7".to_string()]);
}

/// What a commit-time conflict check costs against the retained snapshot
/// count, inside a transaction that has staged a write. Prints the check's
/// time, amortized over twenty repeats against a session that runs none,
/// and the object-store GETs the repeats issued.
#[test]
#[ignore = "a measurement, not an assertion; needs the built DuckDB CLI and packaged Moraine extension"]
fn measure_staged_conflict_check_by_retained_snapshots() {
    const REPEATS: usize = 20;
    println!("\n# staged conflict check by retained snapshots, 4 KiB of commit info each");
    println!("{:>9} {:>12} {:>8}", "retained", "ms_per_check", "gets");
    for retained in [128_usize, 512, 2048] {
        let directory = TempDir::new("measure-staged-snapshots");
        let data = TempDir::new("measure-staged-snapshots-data");
        seed_heavy_snapshots(directory.path(), data.path(), retained, 4096);
        let check = format!(
            "SELECT COALESCE((SELECT string_agg(changes_made, ',') FROM m.ducklake_snapshot_changes c \
             WHERE c.snapshot_id > {retained}), '');"
        );
        let session = |checks: usize| {
            let sql = format!(
                "BEGIN; INSERT INTO m.ducklake_tag (object_id, begin_snapshot, end_snapshot, key, value) \
                 VALUES (1, 1, NULL, 'probe', 'staged'); \
                 SELECT main_gets FROM moraine_object_store_tally('m'); {} \
                 SELECT main_gets FROM moraine_object_store_tally('m'); ROLLBACK;",
                check.repeat(checks)
            );
            let started = std::time::Instant::now();
            let output = run_session(
                &Attach::Standalone {
                    store_dir: directory.path(),
                    read_only: false,
                },
                &sql,
            );
            assert!(output.status.success(), "{}", combined_output(&output));
            let gets: Vec<u64> = csv_rows(&String::from_utf8_lossy(&output.stdout))
                .iter()
                .filter_map(|row| row.first()?.parse().ok())
                .collect();
            let issued = gets
                .last()
                .copied()
                .unwrap_or(0)
                .saturating_sub(gets.first().copied().unwrap_or(0));
            (started.elapsed(), issued)
        };
        let (baseline, _) = session(0);
        let (with_checks, gets) = session(REPEATS);
        let per_check_ms =
            with_checks.saturating_sub(baseline).as_secs_f64() * 1000.0 / REPEATS as f64;
        println!("{retained:>9} {per_check_ms:>12.2} {gets:>8}");
    }
}
