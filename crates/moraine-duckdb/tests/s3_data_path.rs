//! Indexed Parquet mutations against an S3 emulator. Set the three
//! `MORAINE_DUCK*` artifact variables used by `ducklake_load`, plus
//! `MORAINE_S3_ENDPOINT` and `MORAINE_S3_BUCKET`, then run this suite with
//! `--ignored`. The endpoint must accept the test credentials below.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::{env, process::Command};

#[allow(dead_code)]
#[path = "ducklake_load/helpers.rs"]
mod helpers;

use helpers::{TempDir, cli_path, csv_rows, load_statement};

#[test]
#[ignore = "needs the packaged extensions, DuckDB CLI with httpfs, and an S3 emulator"]
fn indexed_parquet_mutations_reopen_roots_with_trailing_separators() {
    let endpoint = env::var("MORAINE_S3_ENDPOINT").unwrap();
    let endpoint = endpoint
        .strip_prefix("http://")
        .expect("HTTP emulator endpoint");
    let bucket = env::var("MORAINE_S3_BUCKET").unwrap();

    for (case, suffix) in ["", "/", "///"].into_iter().enumerate() {
        let catalog = TempDir::new("s3-data-root");
        let root = format!("s3://{bucket}/data-root-{}-{case}", std::process::id());
        let recorded = format!("{root}{suffix}");
        let run = |data_root: Option<&str>, sql: &str| {
            let options = data_root.map_or_else(String::new, |root| {
                format!(", DATA_PATH '{root}', META_DATA_PATH '{root}'")
            });
            let output = Command::new(cli_path())
                .env("AWS_ACCESS_KEY_ID", "minioadmin")
                .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
                .env("AWS_REGION", "us-east-1")
                .env("AWS_ENDPOINT", format!("http://{endpoint}"))
                .env("AWS_ALLOW_HTTP", "true")
                .args(["-unsigned", "-csv", "-batch", "-bail", "-c"])
                .arg(format!(
                    "LOAD httpfs; {}
                     CREATE SECRET emulator (TYPE s3, KEY_ID 'minioadmin',
                         SECRET 'minioadmin', REGION 'us-east-1', ENDPOINT '{endpoint}',
                         USE_SSL false, URL_STYLE 'path');
                     ATTACH 'ducklake:moraine:{}' AS lake (META_FLUSH_INTERVAL_MS 1{options});
                     SELECT 'moraine_result_start';
                     {sql}",
                    load_statement(),
                    catalog.path().display(),
                ))
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "root {recorded:?}, attach {data_root:?}:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            let output = String::from_utf8(output.stdout).unwrap();
            let (_, result) = output.rsplit_once("moraine_result_start\n").unwrap();
            csv_rows(result)
        };

        run(
            Some(&recorded),
            "CALL ducklake_set_option('lake', 'data_inlining_row_limit', 0);
             CREATE TABLE lake.main.probe(a BIGINT);
             CALL moraine_index_create('lake', 'main', 'probe', 'by_a', ['a'], true);
             INSERT INTO lake.main.probe SELECT i FROM range(10) t(i);",
        );
        assert_eq!(
            run(Some(&root), "SELECT count(*) FROM lake.main.probe;"),
            vec![vec!["10"]],
        );

        // Reopening without an override must also use the recorded root.
        run(None, "UPDATE lake.main.probe SET a=107 WHERE a=7;");
        run(Some(&root), "DELETE FROM lake.main.probe WHERE a=2;");
        assert_eq!(
            run(None, "SELECT count(*), sum(a) FROM lake.main.probe;"),
            vec![vec!["9", "143"]],
        );
        for (key, expected) in [(7, "0"), (2, "0"), (107, "1"), (3, "1")] {
            assert_eq!(
                run(
                    Some(&root),
                    &format!(
                        "SELECT count(DISTINCT row_id) FROM moraine_index_lookup(
                            'lake', 'main', 'probe', 'by_a', {key});"
                    ),
                ),
                vec![vec![expected]],
                "index entry for {key} after update and delete",
            );
        }
    }
}
