//! Prepares the repository's DuckLake patch series for bundling into the
//! moraine extension.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail, ensure};

use crate::duckdb::{self, command_output, optional_command_output};

const DUCKLAKE_URL: &str = "https://github.com/duckdb/ducklake.git";
const DUCKLAKE_REVISION: &str = "417a9117f590e8141742ac0579d02f6a90162e37";
const SUPPORTED_DUCKDB_PIN: &str = "v2.0.0";
/// DuckDB's own DuckLake patches, which it applies to the commit it pins
/// before building the extension. The series here targets that patched
/// source, so every application composes theirs first.
const UPSTREAM_PATCH_DIRECTORY: &str = "duckdb/.github/patches/extensions/ducklake";
/// How much DuckLake history to fetch. Deep enough that DuckDB's patches
/// find the blobs their three-way merge needs: they are maintained against
/// a moving branch, and at depth one git has nothing to merge against and
/// refuses the hunks whose region has moved.
const DUCKLAKE_FETCH_DEPTH: &str = "50";
/// vcpkg is fetched for one revision's ports, with nothing to merge.
const SHALLOW: &str = "1";
const VCPKG_URL: &str = "https://github.com/microsoft/vcpkg.git";
const VCPKG_REVISION: &str = "ea1a7396b05637a53bf23c078647ecc0edee4b80";
pub(crate) const PATCH_PATHS: [&str; 8] = [
    "patches/ducklake/0001-perf-prune-DuckLake-files-by-row-id.patch",
    "patches/ducklake/0002-feat-expose-DuckLake-data-file-ids-to-scans.patch",
    "patches/ducklake/0003-feat-share-the-DuckLake-inlined-table-cache.patch",
    "patches/ducklake/0004-fix-retain-files-after-unknown-commit-outcomes.patch",
    "patches/ducklake/0005-feat-change-DuckLake-rows-by-position.patch",
    "patches/ducklake/0006-perf-name-the-table-a-dropped-file-belongs-to.patch",
    "patches/ducklake/0007-fix-keep-the-row-ids-a-merged-file-carries.patch",
    "patches/ducklake/0008-fix-bound-compaction-input-files.patch",
];
pub(crate) const CONFIG_PATH: &str = "patches/ducklake/ducklake.cmake";
/// The patched-behaviour sqllogictests, run against the built artifact.
const REGRESSION_TEST_PATHS: [&str; 2] = [
    "test/sql/rowid/ducklake_row_id_file_pruning.test",
    "test/sql/rowid/ducklake_data_file_id.test",
];

/// The patched DuckLake checkout the moraine build bundles, and the vcpkg
/// it resolves DuckLake's `roaring` dependency through.
#[derive(Debug)]
pub struct PatchedDuckLake {
    root: PathBuf,
    /// The pinned DuckLake source with the patch series applied.
    pub source: PathBuf,
    vcpkg: PathBuf,
}

impl PatchedDuckLake {
    fn at(root: PathBuf) -> Self {
        Self {
            source: root.join("source"),
            vcpkg: root.join("vcpkg"),
            root,
        }
    }

    /// The toolchain file the build resolves `roaring` through.
    pub fn vcpkg_toolchain(&self) -> PathBuf {
        self.vcpkg.join("scripts/buildsystems/vcpkg.cmake")
    }
}

/// Fetches the pinned DuckLake source and vcpkg under `target/` and applies
/// the tracked patch series, ready for the moraine build to bundle.
pub fn prepare() -> anyhow::Result<PatchedDuckLake> {
    let workspace = duckdb::workspace_root();
    let root = workspace.join("target/patched-ducklake");
    prepare_at(&workspace, root)
}

/// Applies the series to every pinned DuckLake source without building
/// anything, fetching the sources the cache has not seen.
pub fn check_pins() -> anyhow::Result<()> {
    let workspace = duckdb::workspace_root();
    let source = workspace.join("target/patched-ducklake/source");
    fs::create_dir_all(&source).with_context(|| format!("creating {}", source.display()))?;
    prepare_checkout(
        &source,
        DUCKLAKE_URL,
        DUCKLAKE_REVISION,
        DUCKLAKE_FETCH_DEPTH,
        "DuckLake",
    )?;
    check_pin_sources(&workspace, &source)
}

fn prepare_at(workspace: &Path, root: PathBuf) -> anyhow::Result<PatchedDuckLake> {
    ensure!(
        duckdb::duckdb_pin() == SUPPORTED_DUCKDB_PIN,
        "the DuckLake patch targets DuckDB {SUPPORTED_DUCKDB_PIN}, but moraine's primary pin is {}",
        duckdb::duckdb_pin()
    );
    for patch in PATCH_PATHS {
        ensure!(
            workspace.join(patch).exists(),
            "the DuckLake patch is missing at {}",
            workspace.join(patch).display()
        );
    }

    let patched = PatchedDuckLake::at(root);
    fs::create_dir_all(&patched.root)
        .with_context(|| format!("creating {}", patched.root.display()))?;
    prepare_checkout(
        &patched.source,
        DUCKLAKE_URL,
        DUCKLAKE_REVISION,
        DUCKLAKE_FETCH_DEPTH,
        "DuckLake",
    )?;
    apply_patches(workspace, &patched.source)?;
    prepare_checkout(&patched.vcpkg, VCPKG_URL, VCPKG_REVISION, SHALLOW, "vcpkg")?;
    bootstrap_vcpkg(&patched.vcpkg)?;
    Ok(patched)
}

fn prepare_checkout(
    path: &Path,
    url: &str,
    revision: &str,
    depth: &str,
    name: &str,
) -> anyhow::Result<()> {
    let git_directory = path.join(".git");
    if git_directory.is_dir() && !git_directory.join("HEAD").exists() {
        println!(
            "repairing cached {name} checkout with stripped Git metadata at {}",
            path.display()
        );
        fs::remove_dir_all(path)
            .with_context(|| format!("removing incomplete cached {name} at {}", path.display()))?;
    }

    if !path.join(".git").exists() {
        ensure_directory_is_empty(path, name)?;
        fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
        duckdb::run(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(path),
        )?;
        duckdb::run(
            Command::new("git")
                .args(["remote", "add", "origin", url])
                .current_dir(path),
        )?;
    }

    let head = optional_command_output(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(path),
    )?;
    if head.is_none() {
        duckdb::run(
            Command::new("git")
                .args(["fetch", "--depth", depth, "origin", revision])
                .current_dir(path),
        )?;
        duckdb::run(
            Command::new("git")
                .args(["switch", "--quiet", "--detach", "FETCH_HEAD"])
                .current_dir(path),
        )?;
    }

    let head = command_output(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(path),
    )?;
    ensure!(
        head.trim() == revision,
        "cached {name} checkout at {} is {}, expected {}; choose another `--root` or remove the cache",
        path.display(),
        head.trim(),
        revision
    );
    Ok(())
}

fn ensure_directory_is_empty(path: &Path, name: &str) -> anyhow::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(path).with_context(|| format!("reading {}", path.display()))?;
    ensure!(
        entries.next().is_none(),
        "{} exists but is not a {name} checkout; choose another `--root` or remove the cache",
        path.display()
    );
    Ok(())
}

fn apply_patches(workspace: &Path, source: &Path) -> anyhow::Result<()> {
    let upstream = upstream_patches(workspace)?;
    let patches: Vec<PathBuf> = PATCH_PATHS
        .iter()
        .map(|patch| workspace.join(patch))
        .collect();
    if checkout_matches_patches(source, &upstream, &patches)? {
        return Ok(());
    }

    ensure_clean_checkout(source, "cached DuckLake checkout")?;

    // DuckDB's own patches first, three-way: they are maintained against a
    // moving branch and DuckDB applies them with `patch`, which tolerates the
    // same drift.
    duckdb::run(
        Command::new("git")
            .args(["apply", "--3way"])
            .args(&upstream)
            .current_dir(source),
    )?;
    // One invocation for the whole series: git threads each patch's result
    // into the next, and a later patch edits files an earlier one creates.
    // The apply is all-or-nothing; `--check` cannot stand in for it because it
    // does not see files an earlier patch in the same run creates.
    duckdb::run(
        Command::new("git")
            .args(["apply"])
            .args(&patches)
            .current_dir(source),
    )?;
    ensure!(
        checkout_matches_patches(source, &upstream, &patches)?,
        "applying the DuckLake patch series did not produce its exact tracked diff"
    );
    Ok(())
}

/// DuckDB's DuckLake patches, in the order DuckDB itself applies them.
fn upstream_patches(workspace: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let directory = workspace.join(UPSTREAM_PATCH_DIRECTORY);
    let entries = fs::read_dir(&directory).with_context(|| {
        format!(
            "reading {} (run `git submodule update --init duckdb`)",
            directory.display()
        )
    })?;
    let mut patches: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "patch")
        })
        .collect();
    patches.sort();
    ensure!(
        !patches.is_empty(),
        "{} holds no patches; DuckDB stopped patching DuckLake, so the series should \
         target the pinned commit directly",
        directory.display()
    );
    Ok(patches)
}

fn checkout_matches_patches(
    source: &Path,
    upstream: &[PathBuf],
    patches: &[PathBuf],
) -> anyhow::Result<bool> {
    let parent = source
        .parent()
        .context("the DuckLake source checkout has no parent")?;
    let indexes = TemporaryDirectory::create(parent, "patch-validation")?;
    let expected_index = indexes.path().join("expected.index");
    let actual_index = indexes.path().join("actual.index");

    initialize_temporary_index(source, &expected_index)?;
    apply_series_to_index(source, &expected_index, upstream, patches)?;
    let expected_tree = write_temporary_tree(source, &expected_index)?;

    initialize_temporary_index(source, &actual_index)?;
    command_output(
        Command::new("git")
            .args(["add", "--all", "--", "."])
            .env("GIT_INDEX_FILE", &actual_index)
            .current_dir(source),
    )?;
    let actual_tree = write_temporary_tree(source, &actual_index)?;

    Ok(expected_tree == actual_tree)
}

/// Applies DuckDB's patches and then the series to `index`, composing them
/// the way a build does: theirs three-way, ours strictly. Returns the tree
/// their application leaves, which is the base the series is tracked
/// against.
fn apply_series_to_index(
    source: &Path,
    index: &Path,
    upstream: &[PathBuf],
    patches: &[PathBuf],
) -> anyhow::Result<String> {
    command_output(
        Command::new("git")
            .args(["apply", "--cached", "--3way"])
            .args(upstream)
            .env("GIT_INDEX_FILE", index)
            .current_dir(source),
    )
    .context("applying DuckDB's own DuckLake patches")?;
    let base = write_temporary_tree(source, index)?;
    command_output(
        Command::new("git")
            .args(["apply", "--cached"])
            .args(patches)
            .env("GIT_INDEX_FILE", index)
            .current_dir(source),
    )
    .context("applying the DuckLake patch series")?;
    Ok(base)
}

fn initialize_temporary_index(source: &Path, index: &Path) -> anyhow::Result<()> {
    command_output(
        Command::new("git")
            .args(["read-tree", "HEAD"])
            .env("GIT_INDEX_FILE", index)
            .current_dir(source),
    )?;
    Ok(())
}

fn write_temporary_tree(source: &Path, index: &Path) -> anyhow::Result<String> {
    command_output(
        Command::new("git")
            .arg("write-tree")
            .env("GIT_INDEX_FILE", index)
            .current_dir(source),
    )
    .map(|tree| tree.trim().to_string())
}

/// Applies the series to the DuckLake source every `source-pins` entry
/// names, so a hunk one of those sources has moved out from under fails
/// here instead of in that release's build. Only the primary pin is built
/// locally, and a release build is the first thing that would otherwise
/// touch the rest.
pub fn check_pin_sources(workspace: &Path, source: &Path) -> anyhow::Result<()> {
    let upstream = upstream_patches(workspace)?;
    let patches: Vec<PathBuf> = PATCH_PATHS
        .iter()
        .map(|patch| workspace.join(patch))
        .collect();
    let pins = crate::pins::ducklake_source_pins()?;
    let (primary, rest) = pins
        .split_first()
        .context("patches/ducklake/source-pins names no DuckLake source")?;

    let indexes = TemporaryDirectory::create(
        source
            .parent()
            .context("the DuckLake source checkout has no parent")?,
        "pin-validation",
    )?;

    let primary_paths = series_paths(source, indexes.path(), primary, &upstream, &patches)?;
    println!(
        "ok: the series applies to the DuckLake DuckDB {} selects, touching {} files",
        primary.0,
        primary_paths.len()
    );
    for pin in rest {
        let paths = series_paths(source, indexes.path(), pin, &upstream, &patches)?;
        if let Some(problem) = pin_coverage_problem((&primary.0, &primary_paths), (&pin.0, &paths))
        {
            bail!("{problem}");
        }
        println!(
            "ok: the series applies to the DuckLake DuckDB {} selects, touching the same files",
            pin.0
        );
    }
    Ok(())
}

/// The paths the series changes in one pinned source, applied through a
/// throwaway index so no checkout is disturbed.
fn series_paths(
    source: &Path,
    indexes: &Path,
    pin: &(String, String),
    upstream: &[PathBuf],
    patches: &[PathBuf],
) -> anyhow::Result<Vec<String>> {
    let (version, commit) = pin;
    fetch_revision(source, commit)
        .with_context(|| format!("fetching the DuckLake DuckDB {version} selects"))?;

    let index = indexes.join(format!("{version}.index"));
    command_output(
        Command::new("git")
            .args(["read-tree", commit])
            .env("GIT_INDEX_FILE", &index)
            .current_dir(source),
    )?;
    let base = apply_series_to_index(source, &index, upstream, patches)
        .with_context(|| format!("the DuckLake {commit} that DuckDB {version} selects"))?;

    let tree = write_temporary_tree(source, &index)?;
    let names = command_output(
        Command::new("git")
            .args(["diff", "--name-only", &base, &tree])
            .current_dir(source),
    )?;
    Ok(names.lines().map(str::to_owned).collect())
}

/// Fetches `revision` into `source` unless it is already there, so a pin
/// the cached clone has never seen can still be read.
fn fetch_revision(source: &Path, revision: &str) -> anyhow::Result<()> {
    let present = optional_command_output(
        Command::new("git")
            .args(["cat-file", "-e", &format!("{revision}^{{commit}}")])
            .current_dir(source),
    )?;
    if present.is_some() {
        return Ok(());
    }
    duckdb::run(
        Command::new("git")
            .args(["fetch", "--depth", DUCKLAKE_FETCH_DEPTH, "origin", revision])
            .current_dir(source),
    )
}

/// States how `pin`'s series coverage differs from `primary`'s, or `None`
/// when they cover the same files.
fn pin_coverage_problem(primary: (&str, &[String]), pin: (&str, &[String])) -> Option<String> {
    let (primary_version, primary_paths) = primary;
    let (version, paths) = pin;
    let missing: Vec<&String> = primary_paths
        .iter()
        .filter(|path| !paths.contains(path))
        .collect();
    let extra: Vec<&String> = paths
        .iter()
        .filter(|path| !primary_paths.contains(path))
        .collect();
    if missing.is_empty() && extra.is_empty() {
        return None;
    }
    Some(format!(
        "the series covers different files in the DuckLake DuckDB {version} selects than in \
         {primary_version}'s; missing {missing:?}, unexpected {extra:?}"
    ))
}

fn ensure_clean_checkout(source: &Path, name: &str) -> anyhow::Result<()> {
    let status = command_output(
        Command::new("git")
            .args(["status", "--porcelain=v1", "--untracked-files=all"])
            .current_dir(source),
    )?;
    ensure_clean_checkout_status(name, &status)
}

fn ensure_clean_checkout_status(name: &str, status: &str) -> anyhow::Result<()> {
    ensure!(
        status.trim().is_empty(),
        "{name} is dirty; commit, stash, or remove these changes before building:\n{status}"
    );
    Ok(())
}

fn bootstrap_vcpkg(vcpkg: &Path) -> anyhow::Result<()> {
    let executable = if cfg!(windows) {
        vcpkg.join("vcpkg.exe")
    } else {
        vcpkg.join("vcpkg")
    };
    if executable.exists() {
        return Ok(());
    }

    if cfg!(windows) {
        duckdb::run(
            Command::new("cmd")
                .args(["/C", "bootstrap-vcpkg.bat", "-disableMetrics"])
                .current_dir(vcpkg),
        )
    } else {
        duckdb::run(
            Command::new("sh")
                .args(["bootstrap-vcpkg.sh", "-disableMetrics"])
                .current_dir(vcpkg),
        )
    }
}

fn preload_ducklake_test(test_path: &str, script: &str) -> anyhow::Result<String> {
    const REQUIREMENT: &str = "require ducklake\n";
    ensure!(
        script.matches(REQUIREMENT).count() == 1,
        "{test_path} must contain exactly one `{}` directive",
        REQUIREMENT.trim()
    );
    Ok(script.replacen(
        REQUIREMENT,
        "# ducklake is preloaded from the artifact under test\n",
        1,
    ))
}

/// Runs the series' own row-ID write, pruning and file-merge sqllogictests
/// with `artifact` preloaded in place of `require ducklake`.
pub fn run_row_id_regression(patched: &PatchedDuckLake, artifact: &Path) -> anyhow::Result<()> {
    let runner = duckdb::workspace_root().join("build/release/test/unittest");
    ensure!(
        runner.exists(),
        "the sqllogictest runner is missing at {}; `cargo xtask e2e` builds it",
        runner.display()
    );

    for test_path in REGRESSION_TEST_PATHS {
        let source_test = patched.source.join(test_path);
        let script = fs::read_to_string(&source_test)
            .with_context(|| format!("reading patched test {}", source_test.display()))?;
        let script = preload_ducklake_test(test_path, &script)?;
        let test_root = TemporaryDirectory::create(&patched.root, "ducklake-regression")?;
        let copied_test = test_root.path().join(test_path);
        let parent = copied_test
            .parent()
            .context("the regression test path has no parent")?;
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        fs::write(&copied_test, script)
            .with_context(|| format!("writing {}", copied_test.display()))?;

        let artifact_literal = artifact.display().to_string().replace('\'', "''");
        let output = Command::new(&runner)
            .args(["--test-dir"])
            .arg(test_root.path())
            .arg(test_path)
            .env("DUCKDB_TEST_ON_INIT", format!("LOAD '{artifact_literal}';"))
            .output()
            .with_context(|| format!("spawning {}", runner.display()))?;
        print!("{}", String::from_utf8_lossy(&output.stdout));
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        ensure!(
            output.status.success(),
            "the patched DuckLake sqllogictest {test_path} failed against {}",
            artifact.display()
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        ensure!(
            stdout.contains("All tests passed") && stdout.contains("1 test case"),
            "the patched DuckLake sqllogictest {test_path} did not report one passing case"
        );
    }
    Ok(())
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn create(parent: &Path, name: &str) -> anyhow::Result<Self> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system time is before the Unix epoch")?
            .as_nanos();
        let path = parent.join(format!(".{name}-{}-{nonce}", std::process::id()));
        fs::create_dir(&path).with_context(|| format!("creating {}", path.display()))?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    /// Same files in a different order is the same coverage.
    #[test]
    fn pins_covering_the_same_files_are_no_problem() {
        let primary = vec!["src/a.cpp".to_owned(), "src/b.cpp".to_owned()];
        let pin = vec!["src/b.cpp".to_owned(), "src/a.cpp".to_owned()];
        assert_eq!(
            pin_coverage_problem(("v1.5.6", &primary), ("v1.5.5", &pin)),
            None
        );
    }

    /// A file the series reaches in one source and not the other is named
    /// on the side it is missing from.
    #[test]
    fn a_pin_the_series_covers_differently_is_a_problem() {
        let primary = vec!["src/a.cpp".to_owned(), "src/b.cpp".to_owned()];
        let pin = vec!["src/a.cpp".to_owned(), "src/c.cpp".to_owned()];
        let problem = pin_coverage_problem(("v1.5.6", &primary), ("v1.5.5", &pin))
            .expect("differing coverage is a problem");
        assert!(problem.contains("v1.5.5"), "{problem}");
        assert!(problem.contains("src/b.cpp"), "{problem}");
        assert!(problem.contains("src/c.cpp"), "{problem}");
    }

    fn commit_fixture(repository: &Path, name: &str, contents: &str) -> String {
        fs::write(repository.join(name), contents).expect("fixture file");
        command_output(
            Command::new("git")
                .args(["add", "."])
                .current_dir(repository),
        )
        .expect("stage fixture");
        command_output(
            Command::new("git")
                .args([
                    "-c",
                    "user.name=Moraine Test",
                    "-c",
                    "user.email=test@example.com",
                    "commit",
                    "-m",
                    "fixture",
                ])
                .current_dir(repository),
        )
        .expect("commit fixture");
        command_output(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repository),
        )
        .expect("fixture revision")
        .trim()
        .to_owned()
    }

    #[test]
    fn cache_with_stripped_nested_git_metadata_is_rebuilt() {
        let root = TemporaryDirectory::create(&std::env::temp_dir(), "stripped-git-test")
            .expect("temporary root");
        let origin = root.path().join("origin");
        fs::create_dir(&origin).expect("origin directory");
        command_output(Command::new("git").arg("init").current_dir(&origin))
            .expect("initialize origin");
        let revision = commit_fixture(&origin, "pinned.txt", "pinned\n");

        // Rust cache restoration can retain the nested `.git` directory but
        // strip its contents. Git then walks up to the workspace repository
        // and reports the PR merge commit as this checkout's HEAD.
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).expect("workspace directory");
        command_output(Command::new("git").arg("init").current_dir(&workspace))
            .expect("initialize workspace");
        let workspace_revision = commit_fixture(&workspace, "workspace.txt", "workspace\n");
        let source = workspace.join("target/patched-ducklake/source");
        fs::create_dir_all(source.join(".git")).expect("stripped git marker");
        fs::write(source.join("stale.txt"), "stale\n").expect("stale cache file");
        let escaped = command_output(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&source),
        )
        .expect("parent revision escapes into nested cache");
        assert_eq!(escaped.trim(), workspace_revision);

        prepare_checkout(
            &source,
            origin.to_str().expect("UTF-8 origin path"),
            &revision,
            SHALLOW,
            "fixture",
        )
        .expect("repair stripped cache");

        let head = command_output(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&source),
        )
        .expect("repaired revision");
        assert_eq!(head.trim(), revision);
        assert!(source.join("pinned.txt").exists());
        assert!(!source.join("stale.txt").exists());

        let later_revision = commit_fixture(&origin, "later.txt", "later\n");
        let error = prepare_checkout(
            &source,
            origin.to_str().expect("UTF-8 origin path"),
            &later_revision,
            SHALLOW,
            "fixture",
        )
        .expect_err("a real checkout at another revision stays protected");
        assert!(error.to_string().contains("cached fixture checkout"));
        let preserved = command_output(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&source),
        )
        .expect("preserved revision");
        assert_eq!(preserved.trim(), revision);
    }

    /// The cached checkout is reused only when its tree is exactly what
    /// DuckDB's patches and then the series produce.
    #[test]
    fn cached_patch_series_requires_the_exact_resulting_tree() {
        let root = TemporaryDirectory::create(&std::env::temp_dir(), "patch-tree-test")
            .expect("temporary root");
        let source = root.path().join("source");
        fs::create_dir(&source).expect("source directory");
        command_output(Command::new("git").arg("init").current_dir(&source))
            .expect("initialize repository");
        command_output(
            Command::new("git")
                .args(["config", "diff.noprefix", "true"])
                .current_dir(&source),
        )
        .expect("configure alternate diff rendering");
        fs::write(source.join("source.cpp"), "old\n").expect("source file");
        fs::write(source.join("sibling.cpp"), "unchanged\n").expect("sibling file");
        command_output(Command::new("git").args(["add", "."]).current_dir(&source))
            .expect("stage baseline");
        command_output(
            Command::new("git")
                .args([
                    "-c",
                    "user.name=Moraine Test",
                    "-c",
                    "user.email=test@example.com",
                    "commit",
                    "-m",
                    "baseline",
                ])
                .current_dir(&source),
        )
        .expect("commit baseline");

        let first = root.path().join("0001-change.patch");
        fs::write(
            &first,
            "diff --git a/source.cpp b/source.cpp\n\
             --- a/source.cpp\n\
             +++ b/source.cpp\n\
             @@ -1 +1 @@\n\
             -old\n\
             +mid\n",
        )
        .expect("first patch file");
        let second = root.path().join("0002-change.patch");
        fs::write(
            &second,
            "diff --git a/source.cpp b/source.cpp\n\
             --- a/source.cpp\n\
             +++ b/source.cpp\n\
             @@ -1 +1 @@\n\
             -mid\n\
             +new\n",
        )
        .expect("second patch file");
        // The first stands in for one of DuckDB's own patches, the second
        // for the series: the comparison composes them in that order.
        let upstream = [first];
        let patches = [second];
        fs::write(source.join("source.cpp"), "new\n").expect("apply expected edit");

        assert!(checkout_matches_patches(&source, &upstream, &patches).expect("matching tree"));

        fs::write(source.join("sibling.cpp"), "extra edit\n").expect("extra tracked edit");
        assert!(
            !checkout_matches_patches(&source, &upstream, &patches).expect("extra tracked edit")
        );

        fs::write(source.join("sibling.cpp"), "unchanged\n").expect("restore sibling");
        fs::write(source.join("untracked.cpp"), "extra\n").expect("extra untracked file");
        assert!(
            !checkout_matches_patches(&source, &upstream, &patches).expect("extra untracked file")
        );
    }

    #[test]
    fn dirty_pinned_checkout_is_rejected() {
        let error = ensure_clean_checkout_status("DuckDB submodule", " M CMakeLists.txt\n")
            .expect_err("tracked edits must fail");

        assert!(error.to_string().contains("DuckDB submodule is dirty"));
        assert!(ensure_clean_checkout_status("DuckDB submodule", "").is_ok());
    }

    #[test]
    fn patched_regression_preloads_only_ducklake() {
        let script = "require ducklake\n\nrequire parquet\n\nstatement ok\nSELECT 1\n";
        let transformed =
            preload_ducklake_test("test/sql/example.test", script).expect("DuckLake requirement");

        assert!(!transformed.contains("require ducklake"));
        assert!(transformed.contains("require parquet"));
        assert!(transformed.contains("statement ok\nSELECT 1"));
    }
}
