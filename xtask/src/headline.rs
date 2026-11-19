//! The `headline` task: the published "by the numbers" digest. Its cells
//! are read out of the AWS tables in `docs/benchmarks.md` and written
//! between the markers in `README.md` and the site's landing page.
//! `--check` fails when either copy has drifted from that source.

use std::fs;

use anyhow::{Context, bail, ensure};

use crate::duckdb::workspace_root;

/// The measurement the digest is cut from.
const SOURCE: &str = "docs/benchmarks.md";

/// The line above the per-catalog table of serial phases.
const SERIAL_TABLE: &str = "Serial phases, medians:";

/// The line above the commits-per-second table.
const RATE_TABLE: &str = "Commits per second, writers sharing one process:";

/// The line above the attach, detach, attach again table.
const WARM_TABLE: &str = "attached again in one process, each attach timed.";

/// The catalogs the digest publishes, as the source tables head them. The
/// source also measures Quack, which the digest leaves out.
const CATALOGS: [&str; 4] = ["moraine", "log on Express", "duckdb", "postgres"];

/// What the digest heads them with.
const COLUMNS: [&str; 4] = [
    "Moraine",
    "Moraine, log on Express",
    "DuckDB file",
    "Postgres",
];

/// The serial row the per-commit cost in the closing paragraph divides,
/// and whose leading count is the divisor.
const COMMITS_ROW: &str = "20 single-row commits";

/// Which source table a digest row comes from.
enum SourceTable {
    Serial,
    Rate,
    Warm,
}

/// Every digest row: what the digest calls it, which table holds it, and
/// that table's own label for it.
const ROWS: [(&str, SourceTable, &str); 6] = [
    (
        "20 single-row commits, one connection",
        SourceTable::Serial,
        COMMITS_ROW,
    ),
    (
        "commits per second, one process, 1 connection",
        SourceTable::Rate,
        "1",
    ),
    (
        "commits per second, one process, 8 connections",
        SourceTable::Rate,
        "8",
    ),
    ("attach, a fresh process", SourceTable::Serial, "attach"),
    ("full scan, cold cache", SourceTable::Serial, "full scan"),
    (
        "read a count, already attached",
        SourceTable::Warm,
        "count after it",
    ),
];

/// A published copy of the digest, with the comment syntax its format
/// takes: Markdown an HTML comment, MDX a JSX expression.
struct Target {
    path: &'static str,
    start: &'static str,
    end: &'static str,
}

const TARGETS: [Target; 2] = [
    Target {
        path: "README.md",
        start: "<!-- headline:start -->",
        end: "<!-- headline:end -->",
    },
    Target {
        path: "site/src/content/docs/index.mdx",
        start: "{/* headline:start */}",
        end: "{/* headline:end */}",
    },
];

/// Writes the digest into every published copy, or reports the copies
/// that have drifted.
pub fn run(arguments: &[String]) -> anyhow::Result<()> {
    let check = match arguments {
        [] => false,
        [flag] if flag == "--check" => true,
        _ => bail!("usage: cargo xtask headline [--check]"),
    };

    let root = workspace_root();
    let source =
        fs::read_to_string(root.join(SOURCE)).with_context(|| format!("reading {SOURCE}"))?;
    let digest = digest(&source)?;

    let mut stale = Vec::new();
    let mut written = 0;
    for target in &TARGETS {
        let path = root.join(target.path);
        let contents =
            fs::read_to_string(&path).with_context(|| format!("reading {}", target.path))?;
        let updated = splice(&contents, target, &digest)?;
        if updated == contents {
            continue;
        }
        if check {
            stale.push(target.path);
            continue;
        }
        fs::write(&path, &updated).with_context(|| format!("writing {}", target.path))?;
        println!("{}: digest updated from {SOURCE}", target.path);
        written += 1;
    }

    if !check && written == 0 {
        println!("every copy already carries the digest in {SOURCE}");
    }

    ensure!(
        stale.is_empty(),
        "{} no longer carries the digest in {SOURCE}; run `cargo xtask headline`",
        stale.join(" and ")
    );
    Ok(())
}

/// The digest: the table and the paragraph that reads it.
fn digest(source: &str) -> anyhow::Result<String> {
    let serial = table_after(source, SERIAL_TABLE)?;
    let rate = table_after(source, RATE_TABLE)?;
    let warm = table_after(source, WARM_TABLE)?;

    let mut table = vec![
        format!("| | {} |", COLUMNS.join(" | ")),
        "|---|---:|---:|---:|---:|".to_owned(),
    ];
    for (label, from, source_label) in &ROWS {
        let values = match from {
            SourceTable::Serial => &serial,
            SourceTable::Rate => &rate,
            SourceTable::Warm => &warm,
        }
        .row(source_label)?;
        table.push(format!("| {label} | {} |", values.join(" | ")));
    }

    let commits = serial.row(COMMITS_ROW)?;
    let standard = cell(commits, 0)?;
    let express = cell(commits, 1)?;

    Ok([table.join("\n"), paragraph(standard, express)].join("\n\n"))
}

/// One cell of a row, by position among the published catalogs.
fn cell(values: &[String], index: usize) -> anyhow::Result<&String> {
    values
        .get(index)
        .with_context(|| format!("{SOURCE}: a row is missing column {}", index + 1))
}

/// The closing paragraph, whose two figures are the commits row's own
/// cells, so a reader checks them against the table above rather than
/// dividing. Wrapped here with room for a four-character cell, so a
/// slower run stays inside the line limit.
fn paragraph(standard: &str, express: &str) -> String {
    format!(
        "A commit is one object write, the write-ahead-log entry that makes it\n\
         durable, so its cost is that round trip: the twenty above take {standard}\n\
         on S3 and {express} with the log on S3 Express. That is why one connection\n\
         is slower here than a catalog on a local file or a nearby Postgres, and\n\
         why there is no server to run. Attach is a start-up cost, paid once by a\n\
         process that stays attached. It and the scan are a fresh process with no\n\
         cache directory, so the scan reads cold; a shared cache directory serves\n\
         those reads instead. Moraine is single-writer across processes — a second\n\
         writer process is fenced — and readers are not limited that way.\n\
         Connections within one process share a flush, so when they overlap,\n\
         throughput rises where the others' falls."
    )
}

/// Replaces what a target carries between its markers.
fn splice(contents: &str, target: &Target, digest: &str) -> anyhow::Result<String> {
    let (before, rest) = contents
        .split_once(target.start)
        .with_context(|| format!("{}: no `{}` marker", target.path, target.start))?;
    let (_, after) = rest
        .split_once(target.end)
        .with_context(|| format!("{}: no `{}` marker", target.path, target.end))?;

    Ok(format!(
        "{before}{}\n\n{digest}\n\n{}{after}",
        target.start, target.end
    ))
}

/// A markdown table, its rows labelled by their first cell.
struct Table {
    header: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    /// The values of one row, past its label.
    fn row(&self, label: &str) -> anyhow::Result<&[String]> {
        let row = self
            .rows
            .iter()
            .find(|row| row.first().is_some_and(|first| first == label))
            .with_context(|| format!("{SOURCE}: no `{label}` row under its table"))?;
        let values = row.get(1..).unwrap_or_default();
        values.get(..CATALOGS.len()).with_context(|| {
            format!(
                "{SOURCE}: the `{label}` row carries {} columns, not {}",
                values.len(),
                CATALOGS.len()
            )
        })
    }
}

/// The first table after `anchor`, which must head the catalogs the
/// digest publishes in that order.
fn table_after(source: &str, anchor: &str) -> anyhow::Result<Table> {
    let (_, after) = source
        .split_once(anchor)
        .with_context(|| format!("{SOURCE}: nothing reads `{anchor}`"))?;

    let mut lines = after.lines().skip_while(|line| !line.starts_with('|'));
    let header = cells(
        lines
            .next()
            .with_context(|| format!("{SOURCE}: no table after `{anchor}`"))?,
    );
    let divider = lines
        .next()
        .with_context(|| format!("{SOURCE}: the table after `{anchor}` has no divider"))?;
    ensure!(
        cells(divider)
            .iter()
            .all(|cell| cell.trim_matches(':').chars().all(|char| char == '-')),
        "{SOURCE}: the table after `{anchor}` has no divider"
    );

    let table = Table {
        header,
        rows: lines
            .take_while(|line| line.starts_with('|'))
            .map(cells)
            .collect(),
    };

    for (index, catalog) in CATALOGS.iter().enumerate() {
        let heading = table
            .header
            .get(index + 1)
            .with_context(|| format!("{SOURCE}: the table after `{anchor}` is too narrow"))?;
        ensure!(
            heading == catalog,
            "{SOURCE}: the table after `{anchor}` heads column {} `{heading}`, not `{catalog}`",
            index + 1
        );
    }
    Ok(table)
}

/// The cells of one table line, trimmed, without the bounding pipes.
fn cells(line: &str) -> Vec<String> {
    line.trim()
        .trim_matches('|')
        .split('|')
        .map(|cell| cell.trim().to_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A source holding one of each table the digest reads.
    const FIXTURE: &str = "\
### Five catalogs on AWS

2026-11-19 at `7eaff76`; a `c6i.2xlarge` runner in us-east-1 `us-east-1c`,
with the standard bucket. Three repeats.

Serial phases, medians:

| phase | moraine | log on Express | duckdb | postgres | quack |
|---|---:|---:|---:|---:|---:|
| attach | 543 ms | 574 ms | 20 ms | 194 ms | 165 ms |
| 20 single-row commits | 637 ms | 277 ms | 209 ms | 273 ms | 525 ms |
| full scan | 143 ms | 131 ms | 37 ms | 78 ms | 73 ms |

Commits per second, writers sharing one process:

| writers | moraine | log on Express | duckdb | postgres | quack |
|---:|---:|---:|---:|---:|---:|
| 1 | 22 | 85 | 128 | 96 | 34 |
| 8 | 145 | 430 | 77 | 47 | 4 |
| 16 | 303 | 582 | 85 | 23 | 2 |

What a second attach costs: the catalog is attached, detached, and
attached again in one process, each attach timed.

| phase | moraine | log on Express | duckdb | postgres | quack |
|---|---:|---:|---:|---:|---:|
| attach | 875 ms | 754 ms | 4 ms | 55 ms | 94 ms |
| count after it | 26 ms | 26 ms | 29 ms | 70 ms | 61 ms |
";

    /// Every digest row carries the cells of the source row it names, and
    /// only the published catalogs.
    #[test]
    fn the_digest_takes_its_cells_from_the_source_tables() {
        let digest = digest(FIXTURE).expect("the fixture carries every table");
        assert!(
            digest.contains("| attach, a fresh process | 543 ms | 574 ms | 20 ms | 194 ms |\n")
        );
        assert!(digest.contains(
            "| 20 single-row commits, one connection | 637 ms | 277 ms | 209 ms | 273 ms |"
        ));
        assert!(
            digest.contains(
                "| commits per second, one process, 8 connections | 145 | 430 | 77 | 47 |"
            )
        );
        assert!(
            digest.contains("| read a count, already attached | 26 ms | 26 ms | 29 ms | 70 ms |")
        );
        assert!(!digest.contains("165 ms"), "Quack stays out of the digest");
        assert!(
            !digest.contains("16 connections"),
            "the sixteen-connection row stays out of the digest"
        );
    }

    /// The paragraph quotes the commits row's own cells, so a reader can
    /// check it against the table above.
    #[test]
    fn the_paragraph_quotes_the_commits_row() {
        let digest = digest(FIXTURE).expect("the fixture carries every table");
        assert!(digest.contains("the twenty above take 637 ms"));
        assert!(digest.contains("277 ms with the log on S3 Express"));
    }

    /// A reworded source says which anchor stopped matching.
    #[test]
    fn a_missing_table_names_the_anchor_it_looked_for() {
        let without = FIXTURE.replace(RATE_TABLE, "Commits per second:");
        let error = digest(&without).expect_err("the rate table is gone");
        assert!(
            error.to_string().contains(RATE_TABLE),
            "names the anchor: {error}"
        );
    }

    /// A reordered source is refused rather than published scrambled.
    #[test]
    fn a_table_heading_other_catalogs_is_refused() {
        let swapped = FIXTURE.replace(
            "| phase | moraine | log on Express | duckdb | postgres | quack |",
            "| phase | moraine | duckdb | log on Express | postgres | quack |",
        );
        let error = digest(&swapped).expect_err("the columns moved");
        assert!(
            error.to_string().contains("log on Express"),
            "names the column: {error}"
        );
    }

    /// Splicing touches only what lies between the markers.
    #[test]
    fn splicing_replaces_only_the_marked_block() {
        let target = Target {
            path: "page.mdx",
            start: "{/* headline:start */}",
            end: "{/* headline:end */}",
        };
        let page = "intro\n\n{/* headline:start */}\n\nstale\n\n{/* headline:end */}\n\noutro\n";
        assert_eq!(
            splice(page, &target, "fresh").expect("the markers are there"),
            "intro\n\n{/* headline:start */}\n\nfresh\n\n{/* headline:end */}\n\noutro\n"
        );
    }

    /// A copy that lost its markers says so instead of going unwritten.
    #[test]
    fn a_target_without_markers_is_an_error() {
        let target = Target {
            path: "page.md",
            start: "<!-- headline:start -->",
            end: "<!-- headline:end -->",
        };
        let error = splice("no markers here\n", &target, "fresh").expect_err("no markers");
        assert!(error.to_string().contains("page.md"), "names the copy");
    }

    /// The published copies carry what the source says today.
    #[test]
    fn the_published_copies_carry_the_current_digest() {
        run(&["--check".to_owned()]).expect("the digest is current in every copy");
    }
}
