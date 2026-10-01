//! The `bump-duckdb` task: moves the primary DuckDB pin to a new release.
//!
//! Everything the bump touches beyond the manifest is derivable from the
//! version, and every derived place is one `check-pins` already enforces —
//! so doing them by hand is transcription, and the failure mode is an
//! artifact that builds and then will not load. This writes them, and
//! leaves the judgement to the operator: what the new DuckDB changed, and
//! whether the DuckLake commit it drags along still behaves.

use std::{fs, process::Command};

use anyhow::{Context, bail, ensure};

use crate::{
    duckdb::{duckdb_pin, workspace_root},
    pins::{DUCKLAKE_CONFIG, DUCKLAKE_RELEASE_PINS, declared_ducklake_commit},
};

/// The submodules the primary entry pins, and the ref namespace each
/// release lives in upstream. DuckDB tags its releases; extension-ci-tools
/// carries no tags at all and keeps a branch per DuckDB version, updated
/// in place.
const SUBMODULES: [(&str, &str); 2] = [
    ("duckdb", "refs/tags"),
    ("extension-ci-tools", "refs/heads"),
];

/// Files naming the primary DuckDB version, its abbreviated commit, or
/// the DuckLake commit that rides with it. Every one of them is checked
/// by `check-pins`, which is what makes rewriting them mechanical.
const PINNED_FILES: [&str; 7] = [
    ".gitmodules",
    ".github/workflows/extension.yml",
    ".github/workflows/release.yml",
    ".github/workflows/debug-build.yml",
    "crates/moraine-duckdb/README.md",
    "docs/rfcs/0006-extension-surface.md",
    "xtask/src/ducklake_patch.rs",
];

/// The wire-contract suite, which carries the DuckLake commit but no
/// DuckDB version.
const WIRE_CONTRACT: &str = "crates/moraine-duckdb/tests/ducklake_load/wire_contract.rs";

const MANIFEST: &str = ".github/duckdb-versions";

/// Moves both submodules to `version`, rewrites the manifest around it,
/// and carries every derived reference along.
///
/// Leaves the result uncommitted and unverified: `check-pins` names
/// anything still stale, and `e2e` is what actually proves the new pair.
pub fn bump_duckdb(arguments: &[String]) -> anyhow::Result<()> {
    let Some(version) = arguments.first() else {
        bail!("usage: cargo xtask bump-duckdb <version>, e.g. `cargo xtask bump-duckdb v1.5.6`");
    };
    ensure!(
        version.starts_with('v') && version.split('.').count() == 3,
        "`{version}` is not a `vMAJOR.MINOR.PATCH` DuckDB release"
    );

    let previous = duckdb_pin().to_owned();
    let previous_duckdb = submodule_head("duckdb")?;
    let previous_ducklake = current_ducklake_commit()?;

    let mut pins = Vec::new();
    for (path, namespace) in SUBMODULES {
        let commit = resolve(path, &format!("{namespace}/{version}"))?;
        println!("{path}: {version} is {commit}");
        checkout(path, &commit)?;
        pins.push((path, commit));
    }

    let ducklake = current_ducklake_commit()?;

    write(
        MANIFEST,
        &rewrite_manifest(&read(MANIFEST)?, version, &pins),
    )?;
    write(
        DUCKLAKE_RELEASE_PINS,
        &rewrite_source_pins(&read(DUCKLAKE_RELEASE_PINS)?, version, &ducklake),
    )?;

    let duckdb_commit = pins
        .iter()
        .find(|(path, _)| *path == "duckdb")
        .map(|(_, commit)| commit.clone())
        .unwrap_or_default();
    for file in PINNED_FILES {
        let contents = read(file)?
            .replace(&previous, version)
            .replace(&previous_ducklake, &ducklake);
        write(
            file,
            &abbreviations(&contents, &previous_duckdb, &duckdb_commit),
        )?;
    }
    let contract = read(WIRE_CONTRACT)?.replace(&previous, version);
    write(
        WIRE_CONTRACT,
        &abbreviations(&contract, &previous_ducklake, &ducklake),
    )?;

    println!("\nbumped {previous} -> {version}");
    if ducklake == previous_ducklake {
        println!("DuckLake commit unchanged ({ducklake})");
    } else {
        println!(
            "DuckLake moved {previous_ducklake} -> {ducklake}, because {version} declares it in \
             {DUCKLAKE_CONFIG}.\n  Read what changed before trusting the wire-contract pins: \
             https://github.com/duckdb/ducklake/compare/{previous_ducklake}...{ducklake}\n  \
             The patch series is located by context, so a hunk the new source moved out from \
             under fails its apply rather than landing quietly in the wrong place — \
             `cargo xtask check-patch-pins` applies it to every pinned source."
        );
    }
    println!(
        "\nNot done for you: the codename in the README pin table, and whether an older \
         release should now leave the manifest.\nNext: `cargo xtask check-pins`, \
         `cargo xtask check-patch-pins`, then `cargo xtask e2e`."
    );
    Ok(())
}

/// The manifest with `version` as the primary entry carrying `pins`, and
/// every previous entry demoted to a bare version — only the first line
/// carries submodule commits, since CI checks the rest out by tag.
///
/// Comments and blank lines before the first entry are kept as they are;
/// the entries themselves are rewritten wholesale.
fn rewrite_manifest(current: &str, version: &str, pins: &[(&str, String)]) -> String {
    let is_entry = |line: &str| {
        let trimmed = line.trim();
        !trimmed.is_empty() && !trimmed.starts_with('#')
    };

    let header: Vec<&str> = current.lines().take_while(|line| !is_entry(line)).collect();
    let kept = current
        .lines()
        .filter(|line| is_entry(line))
        .filter_map(|entry| entry.split_whitespace().next())
        .filter(|entry| *entry != version);

    let pinned = pins
        .iter()
        .map(|(path, commit)| format!("{path}={commit}"))
        .collect::<Vec<_>>()
        .join(" ");

    let mut lines = header;
    let primary = format!("{version} {pinned}");
    lines.push(&primary);
    let demoted: Vec<&str> = kept.collect();
    lines.extend(demoted);
    format!("{}\n", lines.join("\n"))
}

/// The DuckLake source pins with `version` mapped to `commit`, newest
/// first. Every other release keeps its own pin: it is still built, and
/// still fetches the DuckLake its own DuckDB declares.
fn rewrite_source_pins(current: &str, version: &str, commit: &str) -> String {
    let is_pin = |line: &str| {
        let trimmed = line.trim();
        !trimmed.is_empty() && !trimmed.starts_with('#')
    };
    let names_version = |line: &str| line.split_whitespace().next() == Some(version);

    let pin = format!("{version} {commit}");
    let mut lines = Vec::new();
    let mut placed = false;
    for line in current.lines() {
        if !is_pin(line) {
            lines.push(line.to_owned());
            continue;
        }
        if names_version(line) {
            lines.push(pin.clone());
            placed = true;
            continue;
        }
        if !placed {
            lines.push(pin.clone());
            placed = true;
        }
        lines.push(line.to_owned());
    }
    if !placed {
        lines.push(pin);
    }
    format!("{}\n", lines.join("\n"))
}

/// `contents` with every abbreviation of `previous` replaced by the same
/// length of `commit`.
///
/// A pin table writes a commit short, so a whole-commit replacement never
/// reaches it. Lengths are tried longest-first so a 10-character hash is
/// not left half-rewritten by an 8-character pass.
fn abbreviations(contents: &str, previous: &str, commit: &str) -> String {
    let mut out = contents.to_owned();
    for length in (6..=previous.len().min(commit.len())).rev() {
        let (Some(from), Some(to)) = (previous.get(..length), commit.get(..length)) else {
            continue;
        };
        out = out.replace(from, to);
    }
    out
}

/// The commit `reference` names in the submodule's upstream.
fn resolve(path: &str, reference: &str) -> anyhow::Result<String> {
    // Peeled first: an annotated tag's own object is not the commit.
    let peeled = format!("{reference}^{{}}");
    let output = Command::new("git")
        .args(["-C", path, "ls-remote", "origin", &peeled, reference])
        .current_dir(workspace_root())
        .output()
        .with_context(|| format!("running git ls-remote in {path}"))?;
    ensure!(
        output.status.success(),
        "git ls-remote in {path} exited with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );

    let listing = String::from_utf8_lossy(&output.stdout);
    let mut lines: Vec<&str> = listing.lines().collect();
    lines.sort_by_key(|line| !line.ends_with("^{}"));
    let commit = lines
        .first()
        .and_then(|line| line.split_whitespace().next())
        .map(str::to_owned);
    commit.with_context(|| format!("{path} upstream has no `{reference}`"))
}

fn checkout(path: &str, commit: &str) -> anyhow::Result<()> {
    let root = workspace_root();
    for arguments in [
        vec!["-C", path, "fetch", "--depth", "1", "origin", commit],
        vec!["-C", path, "checkout", "--detach", commit],
    ] {
        let status = Command::new("git")
            .args(&arguments)
            .current_dir(&root)
            .status()
            .with_context(|| format!("running git {} in {path}", arguments[2]))?;
        ensure!(status.success(), "git {} in {path} failed", arguments[2]);
    }
    Ok(())
}

/// The commit the submodule at `path` is checked out on.
fn submodule_head(path: &str) -> anyhow::Result<String> {
    let output = Command::new("git")
        .args(["-C", path, "rev-parse", "HEAD"])
        .current_dir(workspace_root())
        .output()
        .with_context(|| format!("running git rev-parse in {path}"))?;
    ensure!(output.status.success(), "git rev-parse in {path} failed");
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// The DuckLake commit the checked-out DuckDB declares.
fn current_ducklake_commit() -> anyhow::Result<String> {
    let config = read(DUCKLAKE_CONFIG)?;
    declared_ducklake_commit(&config)
        .map(str::to_owned)
        .with_context(|| format!("{DUCKLAKE_CONFIG} names no `GIT_TAG` commit"))
}

fn read(file: &str) -> anyhow::Result<String> {
    fs::read_to_string(workspace_root().join(file)).with_context(|| format!("reading {file}"))
}

fn write(file: &str, contents: &str) -> anyhow::Result<()> {
    fs::write(workspace_root().join(file), contents).with_context(|| format!("writing {file}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pins() -> Vec<(&'static str, String)> {
        vec![
            ("duckdb", "aaaa1111".repeat(5)),
            ("extension-ci-tools", "bbbb2222".repeat(5)),
        ]
    }

    #[test]
    fn the_new_release_leads_and_the_old_one_keeps_its_place_unpinned() {
        let manifest = "# a comment\n\nv1.5.4 duckdb=old extension-ci-tools=older\nv1.5.3\n";
        let bumped = rewrite_manifest(manifest, "v1.5.5", &pins());
        let lines: Vec<&str> = bumped.lines().collect();
        assert_eq!(lines[0], "# a comment");
        assert_eq!(lines[1], "");
        assert_eq!(
            lines[2],
            format!(
                "v1.5.5 duckdb={} extension-ci-tools={}",
                "aaaa1111".repeat(5),
                "bbbb2222".repeat(5)
            )
        );
        assert_eq!(lines[3], "v1.5.4");
        assert_eq!(lines[4], "v1.5.3");
    }

    /// Re-running a bump is a no-op rather than a duplicated entry.
    #[test]
    fn bumping_to_the_release_already_primary_does_not_repeat_it() {
        let manifest = "# head\nv1.5.5 duckdb=x extension-ci-tools=y\nv1.5.4\n";
        let bumped = rewrite_manifest(manifest, "v1.5.5", &pins());
        assert_eq!(
            bumped
                .lines()
                .filter(|line| line.contains("v1.5.5"))
                .count(),
            1
        );
        assert!(bumped.ends_with("v1.5.4\n"));
    }

    #[test]
    fn the_new_release_leads_the_source_pins_and_the_old_ones_keep_theirs() {
        let pins = "# DuckDB version DuckLake commit\nv1.5.5 aaaa\nv1.5.4 bbbb\n";
        assert_eq!(
            rewrite_source_pins(pins, "v1.5.6", "cccc"),
            "# DuckDB version DuckLake commit\nv1.5.6 cccc\nv1.5.5 aaaa\nv1.5.4 bbbb\n"
        );
    }

    /// Re-running a bump re-pins that release in place rather than listing
    /// it twice, which `check-pins` would then read as one pin too many.
    #[test]
    fn bumping_to_a_release_already_pinned_repins_it_where_it_is() {
        let pins = "# head\nv1.5.6 stale\nv1.5.5 aaaa\n";
        assert_eq!(
            rewrite_source_pins(pins, "v1.5.6", "cccc"),
            "# head\nv1.5.6 cccc\nv1.5.5 aaaa\n"
        );
    }

    #[test]
    fn a_source_pin_file_with_only_comments_gains_the_first_pin() {
        assert_eq!(
            rewrite_source_pins("# only a comment\n", "v1.5.6", "cccc"),
            "# only a comment\nv1.5.6 cccc\n"
        );
    }

    #[test]
    fn abbreviations_of_every_length_move_together() {
        let previous = "d318a545571d7d46eb751fa2aa5f6f4389285d3c";
        let commit = "d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4";
        let text = format!(
            "full {previous}, short {}, table {}",
            &previous[..8],
            &previous[..10]
        );
        let moved = abbreviations(&text, previous, commit);
        assert_eq!(
            moved,
            format!(
                "full {commit}, short {}, table {}",
                &commit[..8],
                &commit[..10]
            )
        );
    }

    /// An unrelated hex run of the same shape is left alone: only
    /// prefixes of the commit being replaced are touched.
    #[test]
    fn abbreviations_leave_other_commits_alone() {
        let text = "ours d318a545, theirs 0badc0de";
        let moved = abbreviations(text, "d318a545571d", "d8a1881e2251");
        assert_eq!(moved, "ours d8a1881e, theirs 0badc0de");
    }
}
