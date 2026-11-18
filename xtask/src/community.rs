//! The community descriptor: `pin-descriptor` stamps the root
//! `description.yml` with a release, and `community-pr` carries that file
//! into a pull request against DuckDB's community extension repository.

use std::{fs, process::Command};

use anyhow::{Context, bail, ensure};

use crate::duckdb::{run, workspace_root};

const DESCRIPTOR: &str = "description.yml";
const UPSTREAM: &str = "duckdb/community-extensions";
const UPSTREAM_PATH: &str = "extensions/moraine/description.yml";
const TAG_PREFIX: &str = "ext-v";

/// Rewrites the descriptor's `version` and `ref` for the release `tag`
/// points at. Leaves the result uncommitted.
pub fn pin_descriptor(arguments: &[String]) -> anyhow::Result<()> {
    let [tag, sha] = arguments else {
        bail!("usage: cargo xtask pin-descriptor <ext-v<version>[-build.N]> <commit sha>");
    };
    let version = version_from_tag(tag)?;
    ensure!(
        sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit()),
        "`{sha}` is not a full commit sha"
    );

    let path = workspace_root().join(DESCRIPTOR);
    let contents = fs::read_to_string(&path).with_context(|| format!("reading {DESCRIPTOR}"))?;
    fs::write(&path, pinned(&contents, version, sha)?)
        .with_context(|| format!("writing {DESCRIPTOR}"))?;
    Ok(())
}

/// Opens, or refreshes, the pull request that moves the community
/// repository's copy of the descriptor to the release the root one names.
/// Needs `git` and an authenticated `gh` for the account that owns the fork.
pub fn community_pr(arguments: &[String]) -> anyhow::Result<()> {
    ensure!(
        arguments.is_empty(),
        "usage: cargo xtask community-pr (takes no arguments)"
    );

    let root = workspace_root();
    let descriptor = fs::read_to_string(root.join(DESCRIPTOR))
        .with_context(|| format!("reading {DESCRIPTOR}"))?;
    let version = field(&descriptor, "version")?;
    let sha = field(&descriptor, "ref")?;
    let tag = format!("{TAG_PREFIX}{version}");
    let tagged = capture(
        Command::new("git")
            .args(["ls-remote", "--tags", "origin", &tag])
            .current_dir(&root),
    )?;
    ensure!(
        tagged.starts_with(sha),
        "{DESCRIPTOR} names version {version} at {sha}, but `{tag}` is at {}; release first, or pull main",
        tagged.split_whitespace().next().unwrap_or("no commit")
    );

    let login = capture(Command::new("gh").args(["api", "user", "--jq", ".login"]))?
        .trim()
        .to_owned();
    let fork = format!("{login}/community-extensions");
    // Creating a fork that exists is a no-op upstream, so this is the one
    // call that both prepares a first run and costs nothing afterwards.
    run(Command::new("gh").args(["repo", "fork", UPSTREAM, "--clone=false"]))?;

    let checkout = std::env::temp_dir().join(format!("moraine-community-{version}"));
    if checkout.exists() {
        fs::remove_dir_all(&checkout)
            .with_context(|| format!("clearing {}", checkout.display()))?;
    }
    run(Command::new("gh").args([
        "repo",
        "clone",
        &fork,
        &checkout.to_string_lossy(),
        "--",
        "--depth",
        "1",
    ]))?;
    let git = |args: &[&str]| run(Command::new("git").args(args).current_dir(&checkout));
    git(&[
        "remote",
        "add",
        "upstream",
        &format!("https://github.com/{UPSTREAM}.git"),
    ])?;
    git(&["fetch", "--depth", "1", "upstream", "main"])?;
    let branch = format!("moraine-v{version}");
    git(&["checkout", "-B", &branch, "upstream/main"])?;

    let target = checkout.join(UPSTREAM_PATH);
    let first = !target.exists();
    fs::create_dir_all(target.parent().unwrap_or(&checkout))?;
    fs::write(&target, &descriptor).with_context(|| format!("writing {UPSTREAM_PATH}"))?;
    let title = pr_title(first, version);
    git(&["add", UPSTREAM_PATH])?;
    git(&["commit", "-m", &title])?;
    git(&["push", "--force", "origin", &branch])?;

    let head = format!("{login}:{branch}");
    let existing = capture(Command::new("gh").args([
        "pr", "list", "--repo", UPSTREAM, "--head", &head, "--json", "url", "--jq", ".[].url",
    ]))?;
    if !existing.trim().is_empty() {
        println!("refreshed {}", existing.trim());
        return Ok(());
    }
    run(Command::new("gh")
        .args([
            "pr", "create", "--repo", UPSTREAM, "--head", &head, "--title", &title, "--body",
        ])
        .arg(pr_body(first, version, sha))
        .current_dir(&checkout))?;
    Ok(())
}

/// The release version an `ext-v<version>[-build.N]` tag names.
fn version_from_tag(tag: &str) -> anyhow::Result<&str> {
    let Some(body) = tag.strip_prefix(TAG_PREFIX) else {
        bail!("`{tag}` does not start with `{TAG_PREFIX}`");
    };
    let version = body
        .split_once("-build.")
        .map_or(body, |(version, _)| version);
    ensure!(
        !version.is_empty()
            && version
                .split('.')
                .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit())),
        "`{tag}` does not carry a dotted numeric version"
    );
    Ok(version)
}

/// `contents` with its `version:` and `ref:` lines replaced; every other
/// byte is kept, so the file's comments and ordering survive.
fn pinned(contents: &str, version: &str, sha: &str) -> anyhow::Result<String> {
    let mut saw_version = false;
    let mut saw_ref = false;
    let lines = contents.split_inclusive('\n').map(|line| {
        let indent = &line[..line.len() - line.trim_start().len()];
        if let Some(rest) = line.trim_start().strip_prefix("version:") {
            saw_version = true;
            format!("{indent}version: {version}{}", line_ending(rest))
        } else if let Some(rest) = line.trim_start().strip_prefix("ref:") {
            saw_ref = true;
            format!("{indent}ref: {sha}{}", line_ending(rest))
        } else {
            line.to_owned()
        }
    });
    let pinned: String = lines.collect();
    ensure!(
        saw_version && saw_ref,
        "{DESCRIPTOR} has no `version:` and `ref:` lines to pin"
    );
    Ok(pinned)
}

fn line_ending(rest: &str) -> &str {
    if rest.ends_with('\n') { "\n" } else { "" }
}

/// The value of the first `key:` line in `contents`.
fn field<'a>(contents: &'a str, key: &str) -> anyhow::Result<&'a str> {
    contents
        .lines()
        .find_map(|line| line.trim_start().strip_prefix(key)?.strip_prefix(':'))
        .map(|value| value.trim().trim_matches(|c| c == '\'' || c == '"'))
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{DESCRIPTOR} has no `{key}:` value"))
}

fn pr_title(first: bool, version: &str) -> String {
    if first {
        "Add moraine extension".to_owned()
    } else {
        format!("Update moraine to v{version}")
    }
}

fn pr_body(first: bool, version: &str, sha: &str) -> String {
    let what = if first {
        "Moraine is a DuckLake catalog backend that stores the catalog in object storage on SlateDB, \
         so a lake needs no catalog database. Rust core with a C++ shim, built with cmake through \
         extension-ci-tools with the Rust toolchain enabled."
    } else {
        "Moves the descriptor to the latest Moraine release."
    };
    format!(
        "{what}\n\nRelease: https://github.com/morainedb/moraine/releases/tag/{TAG_PREFIX}{version} at `{sha}`.\n\nI'm the maintainer."
    )
}

fn capture(cmd: &mut Command) -> anyhow::Result<String> {
    let output = cmd.output().with_context(|| format!("spawning {cmd:?}"))?;
    ensure!(
        output.status.success(),
        "{cmd:?} exited with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).context("non-UTF-8 command output")
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "extension:\n  name: moraine\n  version: 0.8.1\n  language: Rust & C++\n\nrepo:\n  github: morainedb/moraine\n  ref: a64f1570d4f18255bdf52f9f955cf4f6e68fc355\n\ndocs:\n  hello_world: |\n    INSTALL moraine FROM community;\n";

    #[test]
    fn pinning_rewrites_version_and_ref_and_nothing_else() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let pinned = pinned(SAMPLE, "0.9.3", sha).unwrap();
        assert!(pinned.contains("  version: 0.9.3\n"));
        assert!(pinned.contains(&format!("  ref: {sha}\n")));
        assert_eq!(pinned.lines().count(), SAMPLE.lines().count());
        assert!(pinned.contains("  github: morainedb/moraine\n"));
        assert!(pinned.contains("    INSTALL moraine FROM community;\n"));
    }

    #[test]
    fn pinning_refuses_a_descriptor_missing_either_line() {
        assert!(pinned("extension:\n  version: 1\n", "2", "x").is_err());
        assert!(pinned("repo:\n  ref: a\n", "2", "x").is_err());
    }

    #[test]
    fn a_rebuild_tag_names_the_same_version() {
        assert_eq!(version_from_tag("ext-v0.9.3").unwrap(), "0.9.3");
        assert_eq!(version_from_tag("ext-v0.9.3-build.2").unwrap(), "0.9.3");
        assert!(version_from_tag("v0.9.3").is_err());
        assert!(version_from_tag("ext-v").is_err());
        assert!(version_from_tag("ext-vmain").is_err());
    }

    #[test]
    fn fields_read_the_values_the_pin_wrote() {
        let pinned = pinned(SAMPLE, "0.9.3", "0123456789abcdef0123456789abcdef01234567").unwrap();
        assert_eq!(field(&pinned, "version").unwrap(), "0.9.3");
        assert_eq!(
            field(&pinned, "ref").unwrap(),
            "0123456789abcdef0123456789abcdef01234567"
        );
        assert!(field(&pinned, "andium").is_err());
    }

    #[test]
    fn the_first_pull_request_adds_and_later_ones_update() {
        assert_eq!(pr_title(true, "0.9.3"), "Add moraine extension");
        assert_eq!(pr_title(false, "0.9.3"), "Update moraine to v0.9.3");
    }
}
