//! The `check-pins` task: every place that names a DuckDB version must
//! name the one `.github/duckdb-versions` calls primary, and every place
//! that names a DuckLake commit must name the one DuckDB declares.
//!
//! A C++-ABI extension is refused by any DuckDB whose version string
//! differs from the one in its metadata footer, so the pin is not a
//! preference — it decides which engine can load the artifact at all. It
//! is also named in six places that cannot be derived from one another
//! (two git submodules, a Rust constant, two workflow files, a README
//! table), and a bump that misses one produces an artifact that builds,
//! passes CI, and then fails to load. This makes that a build failure
//! instead.
//!
//! The DuckLake commit rides along because it is not moraine's to pick:
//! DuckDB hard-codes it, so a DuckDB bump moves it silently. Left
//! unchecked it surfaces only in `e2e`, a whole extension build later.

use std::process::Command;

use anyhow::{Context, bail, ensure};

use crate::duckdb::{
    duckdb_pin, primary_submodule_pins, supported_duckdb_versions, workspace_root,
};

/// Where DuckDB names the DuckLake commit `INSTALL ducklake` resolves to.
pub const DUCKLAKE_CONFIG: &str = "duckdb/.github/config/extensions/ducklake.cmake";

/// The wire-contract suite, which pins that commit as the version the
/// behaviour it describes was observed against.
const WIRE_CONTRACT: &str = "crates/moraine-duckdb/tests/ducklake_load/wire_contract.rs";

/// The crate README, whose pin table names the version and its commit.
const README: &str = "crates/moraine-duckdb/README.md";

/// Prose that names the pinned DuckLake commit.
const DUCKLAKE_PROSE: [&str; 2] = [README, "docs/rfcs/0006-extension-surface.md"];

/// How much of a commit `duckdb_extensions()` reports as
/// `extension_version`, and so how much the pins carry.
const SHORT_COMMIT: usize = 8;

/// Checks every pinned DuckDB version reference against the primary entry
/// in `.github/duckdb-versions`, reporting all mismatches at once rather
/// than the first.
pub fn check_pins() -> anyhow::Result<()> {
    let pin = duckdb_pin();
    let supported = supported_duckdb_versions();
    ensure!(
        !pin.is_empty(),
        ".github/duckdb-versions lists no DuckDB version"
    );
    println!("primary pin: {pin}");
    println!("supported:   {}", supported.join(", "));

    let mut problems = Vec::new();
    // Compared by commit rather than tag: a submodule is routinely a
    // shallow clone with no tags fetched, where `git describe` has nothing
    // to say.
    let pinned_submodules = primary_submodule_pins();
    ensure!(
        pinned_submodules.len() == 2,
        "the primary entry in .github/duckdb-versions must pin both submodules \
         (`duckdb=<commit> extension-ci-tools=<commit>`); it pins {}",
        pinned_submodules.len()
    );

    for (path, expected) in &pinned_submodules {
        match submodule_commit(path) {
            Ok(commit) if &commit == expected => {}
            Ok(commit) => problems.push(format!(
                "the `{path}` submodule is at `{commit}`, but .github/duckdb-versions \
                 pins `{expected}` for {pin} — `git -C {path} checkout {expected}`"
            )),
            Err(error) => problems.push(format!(
                "could not read the `{path}` submodule's commit: {error} \
                 (run `git submodule update --init`)"
            )),
        }
    }

    // Two levels of check, because the files differ in kind. In a workflow
    // every DuckDB version is a pin, so an unlisted one is a mistake; in
    // prose a version is often an example, so only the primary's presence
    // is required.
    for file in [
        ".github/workflows/extension.yml",
        ".github/workflows/release.yml",
    ] {
        let contents = read(file)?;
        if !contents.contains(pin) {
            problems.push(format!("{file} never names the primary pin `{pin}`"));
        }
        for stale in stale_versions(&contents, &supported) {
            problems.push(format!(
                "{file} pins DuckDB `{stale}`, which is not in .github/duckdb-versions"
            ));
        }
        problems.extend(tools_ref_problem(file, &contents));
    }

    let readme = read(README)?;
    if !readme.contains(pin) {
        problems.push(format!(
            "{README}'s pin table never names the primary pin `{pin}`"
        ));
    }

    // The pin table is the one place a version and its commit are written
    // side by side, so a bump that moves one and not the other reads as
    // agreeing with itself. Abbreviated to whatever length the table uses.
    if let Some(expected) = pinned_submodules
        .iter()
        .find(|(path, _)| path == "duckdb")
        .map(|(_, commit)| commit)
    {
        match backticked_after(&readme, "git hash ") {
            Some(abbreviated) if expected.starts_with(abbreviated) => {}
            Some(abbreviated) => problems.push(format!(
                "{README}'s pin table gives git hash `{abbreviated}`, which is not a prefix \
                 of the `duckdb` commit .github/duckdb-versions pins (`{expected}`)"
            )),
            None => problems.push(format!("{README}'s pin table gives no `git hash`")),
        }
    }

    let ducklake = pinned_ducklake_commit();
    match &ducklake {
        Ok(commit) => problems.extend(ducklake_problems(commit)?),
        Err(problem) => problems.push(problem.clone()),
    }

    ensure!(
        problems.is_empty(),
        "the DuckDB pin is inconsistent:\n  - {}",
        problems.join("\n  - ")
    );
    println!("ok: every DuckDB version reference matches .github/duckdb-versions");
    if let Ok(commit) = &ducklake {
        println!("ok: every DuckLake commit reference matches the one {pin} declares ({commit})");
    }
    Ok(())
}

/// The DuckLake commit the pinned DuckDB declares, or the problem that
/// stopped it being read.
fn pinned_ducklake_commit() -> Result<String, String> {
    let config = read(DUCKLAKE_CONFIG).map_err(|error| {
        format!("could not read {DUCKLAKE_CONFIG}: {error} (run `git submodule update --init`)")
    })?;
    declared_ducklake_commit(&config)
        .map(str::to_owned)
        .ok_or_else(|| format!("{DUCKLAKE_CONFIG} names no `GIT_TAG` commit"))
}

/// The DuckLake commit DuckDB's extension config declares — the one
/// `INSTALL ducklake` against the pinned CLI resolves to.
pub fn declared_ducklake_commit(config: &str) -> Option<&str> {
    config
        .split_whitespace()
        .skip_while(|word| *word != "GIT_TAG")
        .nth(1)
        .map(|commit| commit.trim_end_matches(')'))
        .filter(|commit| !commit.is_empty())
}

/// What the first pair of backticks after `marker` encloses.
fn backticked_after<'a>(contents: &'a str, marker: &str) -> Option<&'a str> {
    let after = contents.find(marker)? + marker.len();
    let rest = contents.get(after..)?;
    let open = rest.find('`')? + 1;
    let quoted = rest.get(open..)?;
    quoted.find('`').and_then(|end| quoted.get(..end))
}

/// The value of a `const NAME: &str = "…";` declaration.
fn string_constant<'a>(contents: &'a str, name: &str) -> Option<&'a str> {
    let opening = format!("const {name}: &str = \"");
    let start = contents.find(&opening)? + opening.len();
    let rest = contents.get(start..)?;
    rest.find('"').and_then(|end| rest.get(..end))
}

/// Every place naming a DuckLake commit that disagrees with `commit`.
///
/// The suite's constant must match exactly; prose only has to carry it,
/// since the same file quotes the commit in more than one form.
fn ducklake_problems(commit: &str) -> anyhow::Result<Vec<String>> {
    let short = commit.get(..SHORT_COMMIT).unwrap_or(commit);
    let mut problems = Vec::new();

    match string_constant(&read(WIRE_CONTRACT)?, "DUCKLAKE_SOURCE_COMMIT") {
        Some(pinned) if pinned == short => {}
        Some(pinned) => problems.push(format!(
            "{WIRE_CONTRACT} pins DuckLake `{pinned}`, but {DUCKLAKE_CONFIG} declares \
             `{short}` — re-verify every pin in that file against the new DuckLake"
        )),
        None => problems.push(format!(
            "{WIRE_CONTRACT} declares no `DUCKLAKE_SOURCE_COMMIT`"
        )),
    }

    for file in DUCKLAKE_PROSE {
        if !read(file)?.contains(short) {
            problems.push(format!(
                "{file} never names the DuckLake commit `{short}` that {DUCKLAKE_CONFIG} declares"
            ));
        }
    }

    Ok(problems)
}

/// The complaint that a workflow calls the reusable extension build at one
/// extension-ci-tools ref and asks it to generate the build matrix from
/// another.
///
/// The two are one artifact: the distribution matrix a `ci_tools_version`
/// checkout supplies is read by the workflow body at the `uses:` ref, so an
/// older matrix omits fields a newer body requires. That drops the platforms
/// it cannot describe silently — no job, no failure, an incomplete release.
fn tools_ref_problem(file: &str, contents: &str) -> Option<String> {
    let called = word_after(contents, "_extension_distribution.yml@")?;
    let generated_from = word_after(contents, "ci_tools_version: ")?;
    (called != generated_from).then(|| {
        format!(
            "{file} calls extension-ci-tools `{called}` but generates its build matrix from \
             `{generated_from}`; both must name the same ref, or the platforms the older \
             matrix cannot describe are dropped without failing"
        )
    })
}

/// The run of non-whitespace immediately after `marker`.
fn word_after<'a>(contents: &'a str, marker: &str) -> Option<&'a str> {
    let after = contents.find(marker)? + marker.len();
    let rest = contents.get(after..)?;
    let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    rest.get(..end).filter(|word| !word.is_empty())
}

/// A repo-relative file, read whole.
fn read(file: &str) -> anyhow::Result<String> {
    std::fs::read_to_string(workspace_root().join(file)).with_context(|| format!("reading {file}"))
}

/// The commit the submodule at `path` is checked out on.
fn submodule_commit(path: &str) -> anyhow::Result<String> {
    let output = Command::new("git")
        .args(["-C", path, "rev-parse", "HEAD"])
        .current_dir(workspace_root())
        .output()
        .with_context(|| format!("running git rev-parse in {path}"))?;
    if !output.status.success() {
        bail!(
            "git rev-parse exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Every `vN.N.N`-shaped DuckDB version in `contents` that the manifest
/// does not list. Scoped to the `v1.` prefix so a Rust crate version, an
/// action's `@v4`, or a DuckLake branch name is not mistaken for one.
fn stale_versions(contents: &str, supported: &[String]) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    for (index, _) in contents.match_indices("v1.") {
        let rest = &contents[index..];
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == 'v'))
            .unwrap_or(rest.len());
        let candidate = rest[..end].trim_end_matches('.');
        // Three components: a DuckDB release, not a `v1.5` series name.
        if candidate.split('.').count() != 3 {
            continue;
        }
        if !supported.iter().any(|version| version == candidate)
            && !found.iter().any(|seen| seen == candidate)
        {
            found.push(candidate.to_string());
        }
    }
    found
}

/// The versions manifest as the JSON array the release workflows feed to
/// their build matrix, printed to stdout.
pub fn print_version_matrix() {
    let entries: Vec<String> = supported_duckdb_versions()
        .iter()
        .map(|version| format!("\"{version}\""))
        .collect();
    println!("[{}]", entries.join(","));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The manifest parses to at least one version, and the primary is the
    /// first of them.
    #[test]
    fn the_manifest_parses_and_leads_with_the_primary_pin() {
        let supported = supported_duckdb_versions();
        assert!(!supported.is_empty(), "the manifest lists no versions");
        assert_eq!(supported[0], duckdb_pin());
        for version in &supported {
            assert!(
                version.starts_with('v') && version.split('.').count() == 3,
                "`{version}` is not a `vMAJOR.MINOR.PATCH` DuckDB release"
            );
        }
    }

    /// The declaration this parses is DuckDB's, in the exact shape its
    /// extension config writes.
    #[test]
    fn the_ducklake_commit_is_read_out_of_duckdbs_extension_config() {
        let config = "duckdb_extension_load(ducklake\n    \
             GIT_URL https://github.com/duckdb/ducklake\n    \
             GIT_TAG d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4\n)\n";
        assert_eq!(
            declared_ducklake_commit(config),
            Some("d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4")
        );
        // A one-line form closes the call on the same word.
        assert_eq!(
            declared_ducklake_commit("duckdb_extension_load(ducklake GIT_TAG abc123)"),
            Some("abc123")
        );
        assert_eq!(declared_ducklake_commit("GIT_URL only\n"), None);
    }

    #[test]
    fn a_backticked_value_is_read_after_its_marker() {
        let table = "| DuckDB | **v1.5.5** (git hash `d8cdaa33fd`, codename Variegata) |";
        assert_eq!(backticked_after(table, "git hash "), Some("d8cdaa33fd"));
        assert_eq!(backticked_after(table, "no such marker"), None);
    }

    #[test]
    fn a_string_constant_is_read_by_name() {
        let source = "const OTHER: &str = \"no\";\nconst WANTED: &str = \"yes\";\n";
        assert_eq!(string_constant(source, "WANTED"), Some("yes"));
        assert_eq!(string_constant(source, "MISSING"), None);
    }

    /// The checked-in tree agrees with the submodule it is pinned to —
    /// the same assertion `check-pins` makes, run without the submodule
    /// present being a hard requirement.
    #[test]
    fn the_wire_contract_pins_the_commit_duckdb_declares() {
        let Ok(config) = read(DUCKLAKE_CONFIG) else {
            return;
        };
        let Some(commit) = declared_ducklake_commit(&config) else {
            panic!("{DUCKLAKE_CONFIG} names no GIT_TAG commit");
        };
        assert_eq!(
            ducklake_problems(commit).expect("reading the pinned files"),
            Vec::<String>::new()
        );
    }

    /// The mismatch that shipped a release with no macOS builds for one of
    /// its DuckDB versions.
    #[test]
    fn a_build_matrix_generated_from_another_tools_ref_is_a_problem() {
        let workflow = |generated_from| {
            format!(
                "    uses: duckdb/extension-ci-tools/.github/workflows/\
                 _extension_distribution.yml@v1.5.5\n      \
                 ci_tools_version: {generated_from}\n"
            )
        };
        assert_eq!(tools_ref_problem("w.yml", &workflow("v1.5.5")), None);
        let problem = tools_ref_problem("w.yml", &workflow("${{ matrix.duckdb_version }}"))
            .expect("a differing ref is a problem");
        assert!(problem.contains("`v1.5.5`"), "{problem}");
    }

    #[test]
    fn stale_versions_ignores_series_names_and_known_releases() {
        let supported = vec!["v1.5.4".to_string()];
        assert!(stale_versions("duckdb_version: v1.5.4", &supported).is_empty());
        // A DuckLake branch name is a series, not a release.
        assert!(stale_versions("branch v1.5-variegata", &supported).is_empty());
        assert_eq!(
            stale_versions("duckdb_version: v1.5.3", &supported),
            vec!["v1.5.3".to_string()]
        );
    }
}
