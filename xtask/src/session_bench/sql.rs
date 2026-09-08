//! SQL workloads against the exact revision artifacts.

use std::{
    fmt::Write as _,
    fs,
    io::Write as _,
    path::Path,
    process::{Command, Stdio},
};

use anyhow::{Context, ensure};

use super::{Options, REVISIONS, revision_order};
use crate::bench::timing::Statement;

#[allow(clippy::too_many_lines)]
pub(super) fn run(options: &Options) -> anyhow::Result<()> {
    let cli = fs::read_to_string(options.root.join("cli-path.txt"))?;
    let prefix = if options.phase == "sql-files" {
        "sql-files"
    } else {
        "sql"
    };
    let mut results = fs::File::create(options.root.join(format!("{prefix}.csv")))?;
    writeln!(
        results,
        "label,round,workload,tables,flush_ms,phase,sequence,seconds,cpu_seconds"
    )?;
    let mut failures = fs::File::create(options.root.join(format!("{prefix}-failures.csv")))?;
    writeln!(failures, "label,round,workload,tables,flush_ms,log")?;
    let mut totals = fs::File::create(options.root.join(format!("{prefix}-totals.csv")))?;
    writeln!(totals, "label,round,workload,tables,flush_ms,seconds")?;
    let cases = if options.phase == "sql-files" {
        vec![("mixed_256_files", 0, 0)]
    } else {
        vec![
            ("mixed", 0, 0),
            ("mixed", 128, 0),
            ("mixed", 128, 10),
            ("bulk", 0, 0),
            ("mixed_256_files", 0, 0),
        ]
    };
    for round in 0..options.repeat {
        for &(workload, tables, flush) in &cases {
            for index in revision_order(round) {
                let (label, _) = REVISIONS[index];
                let name = format!("{label}-{round}-{workload}-{tables}-{flush}");
                println!("Measuring SQL {name}");
                let root = options.root.join(format!("fixture-{name}"));
                fs::create_dir(&root).context(
                    "SQL fixtures must be fresh; remove a failed run's fixture before rerunning",
                )?;
                let statements = match workload {
                    "mixed" => mixed(tables, 16),
                    "mixed_256_files" => mixed(tables, 256),
                    _ => bulk(),
                };
                let script = script(&options.root, &root, label, flush, &statements)?;
                let input = options.root.join("raw").join(format!("{name}.sql"));
                fs::write(&input, &script)?;
                let output = Command::new(cli.trim())
                    .args(["-unsigned", "-batch", "-bail", "-csv"])
                    .stdin(fs::File::open(input)?)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .output()?;
                let stdout = String::from_utf8(output.stdout)?;
                let stderr = String::from_utf8_lossy(&output.stderr);
                let log = options.root.join("raw").join(format!("{name}.log"));
                fs::write(&log, format!("{stdout}\n{stderr}"))?;
                let parsed = parse(&stdout);
                let measured: Vec<_> = statements
                    .iter()
                    .filter_map(|statement| statement.phase)
                    .collect();
                if !output.status.success()
                    || !stdout.lines().any(|line| line == "campaign_verified")
                    || !parsed
                        .as_ref()
                        .is_ok_and(|timings| timings.len() == measured.len())
                {
                    writeln!(
                        failures,
                        "{label},{round},{workload},{tables},{flush},{}",
                        log.display()
                    )?;
                    println!("SQL case failed validation: {}", log.display());
                } else {
                    let total = block_seconds(&stdout)?;
                    writeln!(
                        totals,
                        "{label},{round},{workload},{tables},{flush},{total:.9}"
                    )?;
                    for (sequence, (phase, (seconds, cpu))) in
                        measured.into_iter().zip(parsed?).enumerate()
                    {
                        writeln!(
                            results,
                            "{label},{round},{workload},{tables},{flush},{phase},{sequence},{seconds:.9},{cpu:.9}"
                        )?;
                    }
                }
                results.flush()?;
                totals.flush()?;
                failures.flush()?;
                fs::remove_dir_all(root)?;
            }
        }
    }
    Ok(())
}

fn literal(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

fn script(
    artifacts: &Path,
    root: &Path,
    label: &str,
    flush: usize,
    statements: &[Statement],
) -> anyhow::Result<String> {
    let data = root.join("data");
    let mut script = format!(
        "SET threads=2;\nLOAD '{}';\nLOAD '{}';\nATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}', META_DATA_PATH '{}', META_FLUSH_INTERVAL_MS {flush}, META_CACHE_MEMORY 67108864);\n",
        literal(&artifacts.join("ducklake.duckdb_extension")),
        literal(&artifacts.join(label).join("moraine.duckdb_extension")),
        literal(&root.join("store")),
        literal(&data),
        literal(&data),
    );
    let mut measuring = false;
    for statement in statements {
        if statement.phase.is_some() != measuring {
            script.push_str(".timer off\n");
            let marker = if measuring {
                "campaign_end"
            } else {
                "campaign_start"
            };
            writeln!(
                script,
                "SELECT '{marker}' AS marker, epoch_us(get_current_timestamp()) AS micros;"
            )?;
            measuring = !measuring;
        }
        writeln!(
            script,
            ".timer {}\n{}",
            if statement.phase.is_some() {
                "on"
            } else {
                "off"
            },
            statement.sql
        )?;
    }
    ensure!(!measuring, "a workload must end with untimed verification");
    Ok(script)
}

fn block_seconds(stdout: &str) -> anyhow::Result<f64> {
    let mut start = None;
    let mut total = 0;
    let mut blocks = 0;
    for line in stdout.lines() {
        if let Some(value) = line.strip_prefix("campaign_start,") {
            ensure!(start.is_none(), "nested measurement blocks");
            start = Some(value.parse::<u64>()?);
        } else if let Some(value) = line.strip_prefix("campaign_end,") {
            let end = value.parse::<u64>()?;
            total += end
                .checked_sub(start.take().context("end without start")?)
                .context("measurement clock moved backwards")?;
            blocks += 1;
        }
    }
    ensure!(
        start.is_none() && blocks > 0,
        "missing measurement boundary"
    );
    Ok(std::time::Duration::from_micros(total).as_secs_f64())
}

fn mixed(tables: usize, files: usize) -> Vec<Statement> {
    let rows = files * 128;
    let mut setup = String::from(
        "BEGIN;\nCREATE TABLE lake.main.items(id BIGINT, value BIGINT);\nCREATE TABLE lake.main.events(id BIGINT);\n",
    );
    for file in 0..files {
        writeln!(
            setup,
            "INSERT INTO lake.main.items SELECT i, 0 FROM range({}, {}) t(i);",
            file * 128,
            (file + 1) * 128
        )
        .ok();
    }
    for table in 0..tables {
        writeln!(setup, "CREATE TABLE lake.main.unrelated_{table}(a BIGINT);").ok();
        for _ in 0..2 {
            writeln!(
                setup,
                "INSERT INTO lake.main.unrelated_{table} SELECT i FROM range(32) t(i);"
            )
            .ok();
        }
    }
    setup.push_str(
        "COMMIT;\nCALL moraine_index_create('lake','main','items','by_id',['id'],true);\n",
    );
    writeln!(setup, "SELECT CASE WHEN sum(file_count)={} THEN 'fixture_verified' ELSE error('unexpected seeded file count') END FROM ducklake_table_info('lake');", files + tables * 2).ok();
    let mut statements = vec![Statement::setup(setup)];
    // Record the first lookup separately from subsequent reads.
    statements.push(Statement::measured("first_read", point_read(rows / 2)));
    for cycle in 0..100 {
        for offset in 0..8 {
            statements.push(Statement::measured(
                "read",
                point_read(
                    ((if files > 16 { rows / 2 } else { 0 }) + cycle * 17 + offset * 31) % rows,
                ),
            ));
        }
        statements.push(Statement::measured(
            "update",
            format!("UPDATE lake.main.items SET value=1 WHERE id={cycle};"),
        ));
        statements.push(Statement::measured(
            "insert",
            format!("INSERT INTO lake.main.events VALUES ({cycle});"),
        ));
        statements.push(Statement::measured(
            "delete",
            format!("DELETE FROM lake.main.events WHERE id={cycle};"),
        ));
    }
    statements.push(Statement::setup(format!("SELECT CASE WHEN (SELECT count(*) FROM lake.main.items)={rows} AND (SELECT sum(value) FROM lake.main.items)=100 AND (SELECT count(*) FROM lake.main.events)=0 THEN 'campaign_verified' ELSE error('incorrect mixed workload result') END AS check_result;")));
    statements
}

fn point_read(id: usize) -> String {
    format!(
        "SELECT CASE WHEN count(*)=1 AND min(data.id)={id} THEN {id} ELSE error('incorrect point lookup') END FROM lake.main.items data JOIN moraine_index_lookup('lake','main','items','by_id',{id}) hits ON data.rowid=hits.row_id AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id;"
    )
}

fn bulk() -> Vec<Statement> {
    let mut statements = vec![
        Statement::setup("CREATE TABLE lake.main.items(id BIGINT);"),
        Statement::measured(
            "bulk_insert",
            "INSERT INTO lake.main.items SELECT i FROM range(1000000) t(i);",
        ),
    ];
    for _ in 0..10 {
        statements.push(Statement::measured("scan", "SELECT CASE WHEN sum(id)=499999500000 THEN 1 ELSE error('incorrect bulk scan') END FROM lake.main.items;"));
    }
    statements.push(Statement::setup(
        "CREATE TABLE lake.main.fragments(id BIGINT);",
    ));
    for file in 0..16 {
        statements.push(Statement::setup(format!(
            "INSERT INTO lake.main.fragments SELECT i FROM range({}, {}) t(i);",
            file * 1000,
            (file + 1) * 1000
        )));
    }
    statements.push(Statement::measured(
        "merge",
        "CALL ducklake_merge_adjacent_files('lake');",
    ));
    statements.push(Statement::measured(
        "expire",
        "CALL ducklake_expire_snapshots('lake', older_than => now());",
    ));
    statements.push(Statement::measured(
        "cleanup",
        "CALL ducklake_cleanup_old_files('lake', cleanup_all => true);",
    ));
    statements.push(Statement::setup("SELECT CASE WHEN (SELECT count(*) FROM lake.main.items)=1000000 AND (SELECT count(*) FROM lake.main.fragments)=16000 THEN 'campaign_verified' ELSE error('incorrect maintenance result') END AS check_result;"));
    statements
}

fn parse(stdout: &str) -> anyhow::Result<Vec<(f64, f64)>> {
    stdout
        .lines()
        .filter_map(|line| line.strip_prefix("Run Time (s): real "))
        .map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            ensure!(
                fields.len() == 5 && fields[1] == "user" && fields[3] == "sys",
                "unexpected timer output: {line}"
            );
            Ok((
                fields[0].parse()?,
                fields[2].parse::<f64>()? + fields[4].parse::<f64>()?,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_parser_preserves_individual_operation_cpu_and_wall_time() {
        assert_eq!(
            parse("header\nRun Time (s): real 0.100 user 0.040 sys 0.020\nrow").unwrap(),
            vec![(0.1, 0.06)]
        );
        assert!(parse("Run Time (s): real 0.1").is_err());
    }

    #[test]
    fn high_resolution_totals_exclude_gaps_between_measured_blocks() {
        assert!((block_seconds("campaign_start,1000000\ncampaign_end,1100123\ncampaign_start,3000000\ncampaign_end,3200000").unwrap() - 0.300_123).abs() < 1e-12);
        assert!(block_seconds("campaign_start,1000000").is_err());
        assert!(block_seconds("campaign_end,1000000").is_err());
    }

    #[test]
    fn mixed_work_has_constant_hits_and_mutations_at_every_catalog_size() {
        for (tables, files) in [(0, 16), (128, 16), (0, 256)] {
            let statements = mixed(tables, files);
            assert_eq!(
                statements
                    .iter()
                    .filter(|statement| statement.phase.is_some())
                    .count(),
                1101
            );
            assert_eq!(
                statements
                    .iter()
                    .filter(|statement| statement.phase == Some("read"))
                    .count(),
                800
            );
            assert!(
                statements
                    .last()
                    .unwrap()
                    .sql
                    .contains("sum(value) FROM lake.main.items)=100")
            );
        }
    }
}
