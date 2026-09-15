//! Compares derived row-id filter limits on identical local Parquet data.

use std::{fmt::Write as _, fs, path::Path, process::Command};

use anyhow::{Context, ensure};

use crate::duckdb;

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn execute(cli: &Path, preamble: &str, sql: &str) -> anyhow::Result<String> {
    let output = Command::new(cli)
        .args(["-unsigned", "-csv", "-bail", "-c", preamble, "-c", sql])
        .output()?;
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

pub fn run(arguments: &[String]) -> anyhow::Result<()> {
    let ordinary = arguments.last().is_some_and(|value| value == "--ordinary");
    let arguments = if ordinary {
        &arguments[..arguments.len() - 1]
    } else {
        arguments
    };
    ensure!(
        arguments.len() == 2
            || (arguments.len() == 3 && arguments[2] == "--embedded")
            || (arguments.len() == 4 && arguments[2] == "--fixture"),
        "usage: cargo xtask filter-bench <extension> <results-directory> [--embedded | --fixture <existing-results-directory>] [--ordinary]"
    );
    let embedded = arguments.len() == 3;
    let extension = fs::canonicalize(&arguments[0])?;
    let output = Path::new(&arguments[1]);
    fs::create_dir(output)?;
    let output = fs::canonicalize(output)?;
    let cli = duckdb::ensure_duckdb_cli()?;
    let fixture = if arguments.len() == 4 {
        fs::canonicalize(&arguments[3])?
    } else {
        output.clone()
    };
    let store = fixture.join("store");
    let data = fixture.join("data");
    let preamble = format!(
        "LOAD {}; SET threads=1; ATTACH {} AS lake (DATA_PATH {}, META_DATA_PATH {}, DATA_INLINING_ROW_LIMIT {}, META_CACHE_PUTS false);",
        quoted(&extension.display().to_string()),
        quoted(&format!("ducklake:moraine:{}", store.display())),
        quoted(&data.display().to_string()),
        quoted(&data.display().to_string()),
        if embedded { 1_048_576 } else { 0 }
    );
    if arguments.len() != 4 {
        execute(&cli, &preamble,
        "CALL ducklake_set_option('lake','parquet_row_group_size','2048');
         CREATE TABLE lake.main.t AS SELECT i // 17 AS a, md5(i::VARCHAR) AS payload FROM range(524288) t(i);")?;
        if embedded {
            execute(&cli, &preamble, "CALL ducklake_flush_inlined_data('lake');")?;
        }
        execute(
            &cli,
            &preamble,
            "CALL moraine_index_create('lake','main','t','by_a',['a'],false);",
        )?;
    }
    measure(&cli, &preamble, &output, ordinary)
}

fn measure(cli: &Path, preamble: &str, output: &Path, ordinary: bool) -> anyhow::Result<()> {
    let cases = [
        ("clustered_3264", "range(192)"),
        ("holes_3264", "list_transform(range(192), x -> x * 4)"),
        ("split_3264", "list_concat(range(96),range(30000,30096))"),
        ("scattered_4080", "list_transform(range(240), x -> x * 127)"),
        ("over_cap_4097", "range(241)"),
        ("broad_278528", "range(16384)"),
    ];
    println!("case,equality,first_profile_seconds,warm_median_seconds");
    for (name, keys) in cases {
        for equality in ["=", "IS NOT DISTINCT FROM"] {
            // This fixture has unique row IDs; a row-only join returns the
            // same rows while retaining the ordinary DuckLake scan.
            let file_condition = if ordinary {
                ""
            } else {
                " AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id"
            };
            let query = format!(
                "SELECT count(*),sum(length(data.payload)) FROM lake.main.t data
                 JOIN moraine_index_in('lake','main','t','by_a',{keys}) hits
                 ON data.rowid {equality} hits.row_id{file_condition}"
            );
            let mut sql = String::new();
            for _ in 0..6 {
                writeln!(sql, "EXPLAIN ANALYZE {query};")?;
            }
            writeln!(sql, "{query};")?;
            let text = execute(cli, preamble, &sql)?;
            let rows = name
                .split('_')
                .next_back()
                .context("case count missing")?
                .parse::<u64>()?;
            ensure!(
                text.lines()
                    .any(|line| line == format!("{rows},{}", rows * 32)),
                "result changed for {name}/{equality}"
            );
            let times: Vec<f64> = text
                .split("Total Time: ")
                .skip(1)
                .map(|part| {
                    part.split('s')
                        .next()
                        .context("profile duration missing")?
                        .trim()
                        .parse()
                        .context("profile duration invalid")
                })
                .collect::<anyhow::Result<_>>()?;
            ensure!(
                times.len() == 6,
                "expected six profiles, got {}",
                times.len()
            );
            let mut warm = times[1..].to_vec();
            warm.sort_by(f64::total_cmp);
            let suffix = if equality == "=" {
                "equal"
            } else {
                "null_safe"
            };
            fs::write(output.join(format!("{name}-{suffix}.log")), &text)?;
            println!("{name},{suffix},{:.6},{:.6}", times[0], warm[2]);
        }
    }
    Ok(())
}
