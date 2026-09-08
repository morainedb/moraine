//! A fixed-revision performance comparison using identical benchmark sources.

mod build;
mod measure;
mod sql;

use std::{fs, path::PathBuf};

use anyhow::{Context, bail, ensure};

const REVISIONS: [(&str, &str); 3] = [
    ("before", "b8953e99ab3d51f9a4407046999e3e84e304a7fa"),
    ("review", "e892ff7cb0ca08cbd42582e301d00a6a2d7e644e"),
    ("slatedb16", "75100261773d7a66fb55069f3437e47ff1786765"),
];

struct Options {
    root: PathBuf,
    phase: String,
    repeat: usize,
}

impl Options {
    fn parse(arguments: &[String]) -> anyhow::Result<Self> {
        let mut options = Self {
            root: crate::duckdb::workspace_root().join("target/session-bench"),
            phase: "all".into(),
            repeat: 7,
        };
        let mut arguments = arguments.iter();
        while let Some(flag) = arguments.next() {
            let value = arguments
                .next()
                .with_context(|| format!("{flag} needs a value"))?;
            match flag.as_str() {
                "--root" => options.root = PathBuf::from(value),
                "--phase" => options.phase.clone_from(value),
                "--repeat" => options.repeat = value.parse()?,
                _ => bail!("unknown flag {flag}; use --root, --phase, or --repeat"),
            }
        }
        ensure!(options.repeat > 0, "repeat must be positive");
        ensure!(
            ["all", "build", "core", "sql", "sql-files", "build-time"]
                .contains(&options.phase.as_str()),
            "phase must be all, build, core, sql, sql-files, or build-time"
        );
        Ok(options)
    }
}

fn revision_order(round: usize) -> [usize; 3] {
    [round % 3, (round + 1) % 3, (round + 2) % 3]
}

pub fn run(arguments: &[String]) -> anyhow::Result<()> {
    let mut options = Options::parse(arguments)?;
    fs::create_dir_all(&options.root)?;
    options.root = fs::canonicalize(options.root)?;
    fs::create_dir_all(options.root.join("raw"))?;
    if options.phase == "build-time" {
        build::latency(&options)?;
        measure::build_latency(&options)?;
    }
    if ["all", "build"].contains(&options.phase.as_str()) {
        build::run(&options)?;
    }
    if ["all", "core"].contains(&options.phase.as_str()) {
        measure::run(&options)?;
    }
    if ["all", "sql", "sql-files"].contains(&options.phase.as_str()) {
        sql::run(&options)?;
    }
    println!("Campaign output: {}", options.root.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_campaign_options_are_rejected_before_building() {
        for arguments in [
            vec!["--repeat", "0"],
            vec!["--phase", "unknown"],
            vec!["--root"],
        ] {
            let arguments = arguments.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert!(Options::parse(&arguments).is_err());
        }
    }

    #[test]
    fn rotation_balances_revision_order_across_rounds() {
        assert_eq!(revision_order(0), [0, 1, 2]);
        assert_eq!(revision_order(1), [1, 2, 0]);
        assert_eq!(revision_order(2), [2, 0, 1]);
    }
}
