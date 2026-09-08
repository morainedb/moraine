//! Sequential core measurements, rotating revision order for each repetition.

use std::{fs, process::Command};

use super::{Options, REVISIONS, build::logged, revision_order};

pub(super) fn run(options: &Options) -> anyhow::Result<()> {
    let mut cases: Vec<(String, Vec<String>)> = Vec::new();
    for instrumentation in ["", "_alloc"] {
        for tables in [0, 128, 1024, 4096] {
            cases.push((
                format!("campaign_verb{instrumentation}"),
                vec![
                    tables.to_string(),
                    "8".into(),
                    "60".into(),
                    "1".into(),
                    "0".into(),
                ],
            ));
        }
        for tables in [0, 4096] {
            cases.push((
                format!("campaign_verb{instrumentation}"),
                vec![
                    tables.to_string(),
                    "8".into(),
                    "60".into(),
                    "1".into(),
                    "10".into(),
                ],
            ));
        }
        for mode in ["files", "inline", "recent"] {
            for count in [16, 128, 1024] {
                cases.push((
                    format!("campaign_row{instrumentation}"),
                    vec![mode.into(), count.to_string()],
                ));
            }
        }
        for count in [16, 1024] {
            cases.push((
                format!("campaign_row{instrumentation}"),
                vec!["recent".into(), count.to_string(), "reader".into()],
            ));
        }
        for existing in [1024, 65536] {
            for batch in [1, 1024] {
                cases.push((
                    format!("campaign_index{instrumentation}"),
                    vec![existing.to_string(), batch.to_string()],
                ));
            }
        }
    }
    for (chunks, rows, step) in [
        (16, 128, 128),
        (128, 128, 128),
        (128, 128, 16),
        (16, 1024, 128),
    ] {
        cases.push((
            "campaign_memory".into(),
            vec![
                chunks.to_string(),
                rows.to_string(),
                step.to_string(),
                "1024".into(),
            ],
        ));
    }
    for round in 0..options.repeat {
        for (binary, arguments) in &cases {
            for index in revision_order(round) {
                let (label, _) = REVISIONS[index];
                let name = format!("{label}-{round}-{binary}-{}", arguments.join("-"));
                let output = options.root.join("raw").join(format!("{name}.csv"));
                println!("Measuring {name}");
                logged(
                    Command::new(options.root.join(label).join(binary))
                        .args(arguments)
                        .current_dir(&options.root),
                    &output,
                )?;
            }
        }
    }
    build_latency(options)?;
    fs::write(
        options.root.join("core-complete.txt"),
        format!(
            "{} rounds, {} cases, {} revisions\n",
            options.repeat,
            cases.len() + 4,
            REVISIONS.len()
        ),
    )?;
    Ok(())
}

pub(super) fn build_latency(options: &Options) -> anyhow::Result<()> {
    for round in 0..options.repeat {
        for (chunks, rows, step) in [
            (16, 128, 128),
            (128, 128, 128),
            (128, 128, 16),
            (16, 1024, 128),
        ] {
            let arguments = [
                chunks.to_string(),
                rows.to_string(),
                step.to_string(),
                "1024".into(),
            ];
            for index in revision_order(round) {
                let (label, _) = REVISIONS[index];
                let name = format!("{label}-{round}-campaign_build-{}", arguments.join("-"));
                println!("Measuring {name}");
                logged(
                    Command::new(options.root.join(label).join("campaign_build"))
                        .args(&arguments)
                        .current_dir(&options.root),
                    &options.root.join("raw").join(format!("{name}.csv")),
                )?;
            }
        }
    }
    Ok(())
}
